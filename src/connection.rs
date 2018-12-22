// Copyright 2018 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

use crate::{
    Config,
    DEFAULT_CREDIT,
    WindowUpdateMode,
    error::ConnectionError,
    frame::{
        codec::FrameCodec,
        header::{ACK, ECODE_INTERNAL, ECODE_PROTO, FIN, Header, RST, SYN, Type},
        Data,
        Frame,
        GoAway,
        Ping,
        RawFrame,
        WindowUpdate
    },
    stream::{self, State, Stream, Window, CONNECTION_ID}
};
use futures::{
    executor,
    future::{self, Either},
    prelude::*,
    stream::{Fuse, Stream as FutStream},
    sync::{mpsc, oneshot},
    task::Task,
    try_ready
};
use log::{debug, error, trace};
use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    io,
    sync::Arc,
    u32,
    usize
};
use tokio_codec::Framed;
use tokio_io::{AsyncRead, AsyncWrite};

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum Mode { Client, Server }

enum Command {
    OpenStream(oneshot::Sender<Stream>)
}

struct StreamHandle {
    window: Arc<Window>,
    sender: mpsc::UnboundedSender<stream::Item>,
    ack: bool
}

pub struct Connection<T> {
    mode: Mode,
    is_dead: bool,
    config: Arc<Config>,
    resource: Fuse<Framed<T, FrameCodec>>,
    streams: BTreeMap<stream::Id, StreamHandle>,
    streams_rx: Fuse<mpsc::UnboundedReceiver<(stream::Id, stream::Item)>>,
    streams_tx: mpsc::UnboundedSender<(stream::Id, stream::Item)>,
    outgoing: VecDeque<RawFrame>,
    commands_rx: Fuse<mpsc::UnboundedReceiver<Command>>,
    commands_tx: mpsc::UnboundedSender<Command>,
    tasks: Option<Task>,
    next_id: u32
}

impl<T> fmt::Debug for Connection<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Connection")
            .field("mode", &self.mode)
            .field("streams", &self.streams.len())
            .field("outgoing", &self.outgoing.len())
            .field("next_id", &self.next_id)
            .finish()
    }
}

impl<T> Connection<T>
where
    T: AsyncRead + AsyncWrite
{
    pub fn new(resource: T, config: Config, mode: Mode) -> Self {
		let (streams_tx, streams_rx) = mpsc::unbounded();
		let (commands_tx, commands_rx) = mpsc::unbounded();
        let framed = Framed::new(resource, FrameCodec::new(&config));
        Connection {
            mode,
            is_dead: false,
            config: Arc::new(config),
            streams: BTreeMap::new(),
			streams_tx,
            streams_rx: streams_rx.fuse(),
            resource: framed.fuse(),
            outgoing: VecDeque::new(),
            commands_rx: commands_rx.fuse(),
            commands_tx,
            tasks: None,
            next_id: match mode {
                Mode::Client => 1,
                Mode::Server => 2
            }
        }
    }

    /// Open a new outbound stream which is multiplexed over the existing connection.
    ///
    /// This may fail if the underlying connection is already dead (in which case `None` is
    /// returned), or for other reasons, e.g. if the (configurable) maximum number of streams is
    /// already open.
    pub fn open_stream(&mut self) -> impl Future<Item = Stream, Error = ConnectionError> {
        trace!("open stream");
        if self.is_dead {
            return Either::A(future::err(ConnectionError::Closed))
        }
        let (tx, rx) = oneshot::channel();
        self.commands_tx.unbounded_send(Command::OpenStream(tx)).unwrap();
        if let Some(t) = self.tasks.as_ref() {
            trace!("> notify current: {}", t.will_notify_current());
        } else {
            futures::task::current().notify()
        }
        Either::B(rx.map_err(|_| ConnectionError::Closed))
    }

    pub fn flush(&mut self) -> Poll<(), io::Error> {
        trace!("flush");
        self.resource.poll_complete().map_err(|e| { self.terminate(); e })
    }

    pub fn close(&mut self) -> Poll<(), io::Error> {
        self.is_dead = true;
        self.resource.close().map_err(|e| { self.terminate(); e })
    }

    fn new_stream(&mut self, id: stream::Id, ack: bool, window: u32, credit: u32) -> Stream {
        let window = Arc::new(stream::Window::new(window));
        let (tx, rx) = mpsc::unbounded();
        let stream = StreamHandle {
            window: window.clone(),
            sender: tx,
            ack
        };
        self.streams.insert(id, stream);
        let rx = executor::spawn(rx.fuse());
        Stream::new(id, self.config.clone(), window, credit, self.streams_tx.clone(), rx)
    }

    fn next_stream_id(&mut self) -> Result<stream::Id, ConnectionError> {
        let proposed = stream::Id::new(self.next_id);
        self.next_id = self.next_id.checked_add(2).ok_or(ConnectionError::NoMoreStreamIds)?;
        match self.mode {
            Mode::Client => assert!(proposed.is_client()),
            Mode::Server => assert!(proposed.is_server())
        }
        Ok(proposed)
    }

    fn is_valid_remote_id(&self, id: stream::Id, ty: Type) -> bool {
        match ty {
            Type::Ping | Type::GoAway => return id.is_session(),
            _ => {}
        }
        match self.mode {
            Mode::Client => id.is_server(),
            Mode::Server => id.is_client()
        }
    }

    fn on_data(&mut self, frame: Frame<Data>) -> Result<Option<Stream>, Frame<GoAway>> {
        let stream_id = frame.header().id();

        if frame.header().flags().contains(RST) { // stream reset
            debug!("received reset for stream {}", stream_id);
            if let Some(s) = self.streams.remove(&stream_id) {
                let _ = s.sender.unbounded_send(stream::Item::Reset);
            }
            return Ok(None)
        }

        let is_finish = frame.header().flags().contains(FIN); // half-close

        if frame.header().flags().contains(SYN) { // new stream
            if !self.is_valid_remote_id(stream_id, Type::Data) {
                error!("invalid stream id {}", stream_id);
                return Err(Frame::go_away(ECODE_PROTO))
            }
            if frame.body().len() > DEFAULT_CREDIT as usize {
                error!("initial data exceeds default credit");
                return Err(Frame::go_away(ECODE_PROTO))
            }
            if self.streams.contains_key(&stream_id) {
                error!("stream {} already exists", stream_id);
                return Err(Frame::go_away(ECODE_PROTO))
            }
            if self.streams.len() == self.config.max_num_streams {
                error!("maximum number of streams reached");
                return Err(Frame::go_away(ECODE_INTERNAL))
            }
            let mut stream = self.new_stream(stream_id, true, DEFAULT_CREDIT, DEFAULT_CREDIT);
            if is_finish {
                stream.update_state(State::RecvClosed)
            }
            stream.setup(frame.into_body());
            return Ok(Some(stream))
        }

        if let Some(stream) = self.streams.get_mut(&stream_id) {
            if frame.body().len() > stream.window.get() {
                error!("frame body larger than window of stream {}", stream_id);
                return Err(Frame::go_away(ECODE_PROTO))
            }
            if is_finish {
                let _ = stream.sender.unbounded_send(stream::Item::Finish);
            }
            if stream.window.get() == 0 && self.config.window_update_mode == WindowUpdateMode::OnReceive {
                trace!("{:?}: stream {}: sending window update", self.mode, stream_id);
                let frame = Frame::window_update(stream_id, self.config.receive_window);
                self.outgoing.push_back(frame.into_raw());
                stream.window.set(self.config.receive_window as usize)
            }
            let _ = stream.sender.unbounded_send(stream::Item::Data(frame.into_body()));
        }

        Ok(None)
    }

    fn on_window_update(&mut self, frame: Frame<WindowUpdate>) -> Result<Option<Stream>, Frame<GoAway>> {
        let stream_id = frame.header().id();

        if frame.header().flags().contains(RST) { // stream reset
            debug!("received reset for stream {}", stream_id);
            if let Some(s) = self.streams.remove(&stream_id) {
                let _ = s.sender.unbounded_send(stream::Item::Reset);
            }
            return Ok(None)
        }

        let is_finish = frame.header().flags().contains(FIN); // half-close

        if frame.header().flags().contains(SYN) { // new stream
            if !self.is_valid_remote_id(stream_id, Type::WindowUpdate) {
                error!("invalid stream id {}", stream_id);
                return Err(Frame::go_away(ECODE_PROTO))
            }
            if self.streams.contains_key(&stream_id) {
                error!("stream {} already exists", stream_id);
                return Err(Frame::go_away(ECODE_PROTO))
            }
            if self.streams.len() == self.config.max_num_streams {
                error!("maximum number of streams reached");
                return Err(Frame::go_away(ECODE_INTERNAL))
            }
            let mut stream = self.new_stream(stream_id, true, DEFAULT_CREDIT, frame.header().credit());
            if is_finish {
                stream.update_state(State::RecvClosed)
            }
            return Ok(Some(stream))
        }

        if let Some(stream) = self.streams.get_mut(&stream_id) {
            if is_finish {
                let _ = stream.sender.unbounded_send(stream::Item::Finish);
            }
            let _ = stream.sender.unbounded_send(stream::Item::WindowUpdate(frame.header().credit()));
        }

        Ok(None)
    }

    fn on_ping(&mut self, frame: Frame<Ping>) -> Option<Frame<Ping>> {
        let stream_id = frame.header().id();

        if frame.header().flags().contains(ACK) { // pong
            return None
        }

        if stream_id == CONNECTION_ID || self.streams.contains_key(&stream_id) {
            let mut hdr = Header::ping(frame.header().nonce());
            hdr.ack();
            return Some(Frame::new(hdr))
        }

        debug!("received ping for unknown stream {}", stream_id);
        None
    }

    fn send(&mut self, frame: RawFrame, end: End) -> Poll<(), ConnectionError> {
        trace!("send: {:?}", frame);
        match self.resource.start_send(frame) {
            Ok(AsyncSink::Ready) => Ok(Async::Ready(())),
            Ok(AsyncSink::NotReady(frame)) => {
                match end {
                    End::Front => self.outgoing.push_front(frame),
                    End::Back => self.outgoing.push_back(frame)
                }
                Ok(Async::NotReady)
            }
            Err(e) => {
                self.terminate();
                Err(e.into())
            }
        }
    }

    fn terminate(&mut self) {
        debug!("terminating connection");
        self.is_dead = true;
        self.streams.clear()
    }
}

enum End { Front, Back }

impl<T> futures::Stream for Connection<T>
where
    T: AsyncRead + AsyncWrite
{
    type Item = Stream;
    type Error = ConnectionError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        trace!("1 poll: {}", self.is_dead);
        if self.is_dead {
            return Ok(Async::Ready(None))
        }

        trace!("2 poll");
        // First, check for outgoing frames we need to send.
        while let Some(frame) = self.outgoing.pop_front() {
            trace!("outgoing frame: {:?}", frame);
            try_ready!(self.send(frame, End::Front))
        }

        trace!("3 poll");
        // Check for control commands.
        while let Ok(Async::Ready(Some(Command::OpenStream(reply)))) = self.commands_rx.poll() {
            trace!("command");
            match self.next_stream_id() {
                Ok(id) => {
                    let s = self.new_stream(id, false, self.config.receive_window, DEFAULT_CREDIT);
                    let mut f = Frame::window_update(id, self.config.receive_window);
                    f.header_mut().syn();
                    let _ = reply.send(s);
                    try_ready!(self.send(f.into_raw(), End::Back))
                }
                Err(e) => {
                    self.terminate();
                    return Err(e)
                }
            }
        }

        trace!("4 poll");
        // Check for items from streams.
        while let Ok(Async::Ready(Some(item))) = self.streams_rx.poll() {
            let frame = match item.1 {
                stream::Item::Data(body) => {
                    let mut frame = Frame::data(item.0, body);
                    if let Some(stream) = self.streams.get_mut(&item.0) {
                        if stream.ack {
                            stream.ack = false;
                            frame.header_mut().ack()
                        }
                    }
                    frame.into_raw()
                }
                stream::Item::WindowUpdate(n) => {
                    let mut frame = Frame::window_update(item.0, n);
                    if let Some(stream) = self.streams.get_mut(&item.0) {
                        if stream.ack {
                            stream.ack = false;
                            frame.header_mut().ack()
                        }
                    }
                    frame.into_raw()
                }
                stream::Item::Reset => {
                    self.streams.remove(&item.0);
                    let mut header = Header::data(item.0, 0);
                    header.rst();
                    Frame::new(header).into_raw()
                }
                stream::Item::Finish => {
                    let mut header = Header::data(item.0, 0);
                    header.fin();
                    Frame::new(header).into_raw()
                }
            };
            try_ready!(self.send(frame, End::Back))
        }

        trace!("5 poll");
        // Finally, check for incoming data from remote.
        loop {
            try_ready!(self.flush());
            match self.resource.poll() {
                Ok(Async::Ready(Some(frame))) => {
                    trace!("frame: {:?}", frame);
                    match frame.dyn_type() {
                        Type::Data => {
                            match self.on_data(Frame::assert(frame)) {
                                Ok(None) => {}
                                Ok(Some(stream)) => return Ok(Async::Ready(Some(stream))),
                                Err(frame) => try_ready!(self.send(frame.into_raw(), End::Back))
                            }
                        }
                        Type::WindowUpdate => {
                            match self.on_window_update(Frame::assert(frame)) {
                                Ok(None) => {}
                                Ok(Some(stream)) => return Ok(Async::Ready(Some(stream))),
                                Err(frame) => try_ready!(self.send(frame.into_raw(), End::Back))
                            }
                        }
                        Type::Ping => {
                            if let Some(pong) = self.on_ping(Frame::assert(frame)) {
                                try_ready!(self.send(pong.into_raw(), End::Back))
                            }
                        }
                        Type::GoAway => {
                            self.terminate();
                            return Ok(Async::Ready(None))
                        }
                    }
                }
                Ok(Async::Ready(None)) => {
                    trace!("resource eof");
                    self.terminate();
                    return Ok(Async::Ready(None))
                }
                Ok(Async::NotReady) => {
                    if let Some(t) = self.tasks.as_ref() {
                        trace!("notify current: {}", t.will_notify_current());
                    } else {
                        self.tasks = Some(futures::task::current());
                    }
                    trace!("resource not ready");
                    return Ok(Async::NotReady)
                }
                Err(e) => {
                    trace!("resource error: {}", e);
                    self.terminate();
                    return Err(e.into())
                }
            }
        }
    }
}

