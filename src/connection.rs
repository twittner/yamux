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
    try_ready
};
use log::{debug, error, trace};
use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    sync::Arc,
    u32,
    usize
};
use tokio;
use tokio_codec::Framed;
use tokio_io::{AsyncRead, AsyncWrite};

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum Mode { Client, Server }

/// Remote control
pub struct Remote {
    tx: mpsc::UnboundedSender<Command>,
    rx: Fuse<mpsc::UnboundedReceiver<Stream>>
}

impl Drop for Remote {
    fn drop(&mut self) {
        trace!("dropping remote");
        let _ = self.tx.unbounded_send(Command::Close);
    }
}

impl Remote {
    pub fn open_stream(&mut self) -> impl Future<Item = Stream, Error = ConnectionError> {
        let (tx, rx) = oneshot::channel();
        if self.tx.unbounded_send(Command::OpenStream(tx)).is_err() {
            Either::A(future::err(ConnectionError::Closed))
        } else {
            Either::B(rx.map_err(|_| ConnectionError::Closed))
        }
    }
}

impl futures::Stream for Remote {
    type Item = Stream;
    type Error = ConnectionError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.rx.poll().map_err(|_| ConnectionError::Closed)
    }
}

enum Command {
    OpenStream(oneshot::Sender<Stream>),
    // TODO: Flush,
    Close
}

struct Ports {
    rx_command: Fuse<mpsc::UnboundedReceiver<Command>>,
    tx_stream: mpsc::UnboundedSender<(stream::Id, stream::Item)>,
    rx_stream: Fuse<mpsc::UnboundedReceiver<(stream::Id, stream::Item)>>,
    tx_remote: mpsc::UnboundedSender<Stream>
}

struct StreamHandle {
    window: Arc<Window>,
    sender: mpsc::UnboundedSender<stream::Item>,
    ack: bool
}

enum End { Front, Back }

pub struct Connection<T> {
    mode: Mode,
    is_dead: bool,
    config: Arc<Config>,
    resource: Fuse<Framed<T, FrameCodec>>,
    streams: BTreeMap<stream::Id, StreamHandle>,
    ports: Ports,
    pending: VecDeque<RawFrame>,
    next_id: u32
}

impl<T> fmt::Debug for Connection<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Connection")
            .field("mode", &self.mode)
            .field("open", &!self.is_dead)
            .field("streams", &self.streams.len())
            .field("next_id", &self.next_id)
            .finish()
    }
}

impl<T> Connection<T>
where
    T: AsyncRead + AsyncWrite + Send + 'static
{
    pub fn new(resource: T, config: Config, mode: Mode) -> Remote {
        let (ports, remote) = {
            let (tx_command, rx_command) = mpsc::unbounded();
            let (tx_stream, rx_stream) = mpsc::unbounded();
            let (tx_remote, rx_remote) = mpsc::unbounded();
            let remote = Remote {
                tx: tx_command,
                rx: rx_remote.fuse()
            };
            let ports = Ports {
                rx_command: rx_command.fuse(),
                tx_stream,
                rx_stream: rx_stream.fuse(),
                tx_remote
            };
            (ports, remote)
        };
        let framed = Framed::new(resource, FrameCodec::new(&config));
        let connection = Connection {
            mode,
            is_dead: false,
            config: Arc::new(config),
            resource: framed.fuse(),
            streams: BTreeMap::new(),
            ports,
            pending: VecDeque::new(),
            next_id: match mode {
                Mode::Client => 1,
                Mode::Server => 2
            }
        };
        tokio::spawn(connection.run().map_err(|e| { error!("{}", e); () }));
        remote
    }

    fn flush(&mut self) -> Poll<(), ConnectionError> {
        self.resource.poll_complete().map_err(|e| { self.terminate(); e.into() })
    }

    fn close(&mut self) -> Poll<(), ConnectionError> {
        self.is_dead = true;
        self.resource.close().map_err(|e| { self.terminate(); e.into() })
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
        Stream::new(id, self.config.clone(), window, credit, self.ports.tx_stream.clone(), rx)
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
                self.pending.push_back(frame.into_raw());
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
                    End::Front => self.pending.push_front(frame),
                    End::Back => self.pending.push_back(frame)
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

    fn run(mut self) -> impl Future<Item = (), Error = ConnectionError> {
        future::poll_fn(move || {
            if self.is_dead {
                return Ok(Async::Ready(()))
            }

            // First, check for outgoing frames we need to send.
            while let Some(frame) = self.pending.pop_front() {
                try_ready!(self.send(frame, End::Front))
            }

            // Check for control commands.
            while let Ok(Async::Ready(Some(cmd))) = self.ports.rx_command.poll() {
                match cmd {
                    Command::OpenStream(reply) => {
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
                    Command::Close => {
                        self.is_dead = true;
                        try_ready!(self.close());
                        return Ok(Async::Ready(()))
                    }
                }
            }

            // Check for items from streams.
            while let Ok(Async::Ready(Some(item))) = self.ports.rx_stream.poll() {
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

            // Finally, check for incoming data from remote.
            loop {
                try_ready!(self.flush());
                match self.resource.poll() {
                    Ok(Async::Ready(Some(frame))) => {
                        match frame.dyn_type() {
                            Type::Data => {
                                match self.on_data(Frame::assert(frame)) {
                                    Ok(None) => {}
                                    Ok(Some(stream)) => self.ports.tx_remote.unbounded_send(stream).unwrap(),
                                    Err(frame) => try_ready!(self.send(frame.into_raw(), End::Back))
                                }
                            }
                            Type::WindowUpdate => {
                                match self.on_window_update(Frame::assert(frame)) {
                                    Ok(None) => {}
                                    Ok(Some(stream)) => self.ports.tx_remote.unbounded_send(stream).unwrap(),
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
                                return Ok(Async::Ready(()))
                            }
                        }
                    }
                    Ok(Async::Ready(None)) => {
                        self.terminate();
                        return Ok(Async::Ready(()))
                    }
                    Ok(Async::NotReady) => {
                        return Ok(Async::NotReady)
                    }
                    Err(e) => {
                        self.terminate();
                        return Err(e.into())
                    }
                }
            }
        })
    }
}

