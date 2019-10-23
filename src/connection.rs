// Copyright (c) 2018-2019 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

mod stream;

use crate::{
    Config,
    DEFAULT_CREDIT,
    WindowUpdateMode,
    compat,
    error::ConnectionError,
    frame::{
        self,
        Frame,
        header::{
            ACK,
            CONNECTION_ID,
            Data,
            FIN,
            GoAway,
            Header,
            Ping,
            RST,
            SYN,
            StreamId,
            Tag,
            WindowUpdate
        }
    }
};
use either::Either;
use futures::{channel::{mpsc, oneshot}, prelude::*, stream::Fuse};
use nohash_hasher::IntMap;
use std::fmt;
use tokio_codec::Framed;

pub use stream::{State, Stream};

type Result<T> = std::result::Result<T, ConnectionError>;

/// How the connection is used.
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum Mode {
    /// Client to server connection.
    Client,
    /// Server to client connection.
    Server
}

/// The connection ID.
///
/// Randomly generated, this is mainly intended to improve log output.
#[derive(Clone, Copy)]
struct Id(u32);

impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:08x}", self.0)
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:08x}", self.0)
    }
}

/// Controller of [`Connection`].
#[derive(Clone)]
pub struct RemoteControl(mpsc::Sender<Command>);

impl RemoteControl {
    /// Open a new stream to the remote.
    pub async fn open_stream(&mut self) -> Result<Stream> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::Open(tx)).await?;
        Ok(rx.await?)
    }

    /// Close the connection.
    pub async fn close(&mut self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::Close(Either::Right(tx))).await?;
        Ok(rx.await?)
    }
}

/// A yamux connection object.
pub struct Connection<T> {
    id: Id,
    mode: Mode,
    config: Config,
    socket: Fuse<Framed<compat::AioCompat<T>, frame::Codec>>,
    next_id: u32,
    streams: IntMap<u32, Stream>,
    sender: mpsc::Sender<Command>,
    receiver: mpsc::Receiver<Command>
}

/// Connection commands.
#[derive(Debug)]
pub(crate) enum Command {
    /// Open a new stream to remote.
    Open(oneshot::Sender<Stream>),
    /// A new frame should be sent to remote.
    Send(Frame<Data>),
    /// Flush the socket.
    Flush,
    /// Close the stream or the connection.
    Close(Either<StreamId, oneshot::Sender<()>>)
}

/// Possible actions as a result of incoming frame handling.
#[derive(Debug)]
enum Action {
    /// Nothing to be done.
    None,
    /// A new stream has been opened by the remote.
    New(Stream),
    /// A window update should be sent to the remote.
    Update(Frame<WindowUpdate>),
    /// A ping should be answered.
    Ping(Frame<Ping>),
    /// A stream should be reset.
    Reset(Frame<Data>),
    /// The connection should be terminated.
    Terminate(Frame<GoAway>)
}

impl<T> fmt::Debug for Connection<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Connection")
            .field("id", &self.id)
            .field("mode", &self.mode)
            .field("streams", &self.streams.len())
            .field("next_id", &self.next_id)
            .finish()
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Connection<T> {
    pub fn new(socket: T, cfg: Config, mode: Mode) -> Self {
        let id = Id(rand::random());
        log::debug!("new connection: id = {}, mode = {:?}", id, mode);
        let (sender, receiver) = mpsc::channel(cfg.max_pending_frames);
        let socket = compat::AioCompat(socket);
        let socket = Framed::new(socket, frame::Codec::new(cfg.max_buffer_size)).fuse();
        Connection {
            id,
            mode,
            config: cfg,
            socket,
            streams: IntMap::default(),
            sender,
            receiver,
            next_id: match mode {
                Mode::Client => 1,
                Mode::Server => 2
            }
        }
    }

    /// Get a controller to control this connection.
    pub fn remote_control(&self) -> RemoteControl {
        RemoteControl(self.sender.clone())
    }

    /// Get the next incoming stream, opened by the remote.
    ///
    /// This must be called continously in order to make progress.
    /// Once `Ok(None)` is returned the connection is closed and
    /// no further invocations of this method should be attempted.
    pub async fn next(&mut self) -> Result<Option<Stream>> {
        loop {
            futures::select! {
                command = self.receiver.next() => match command {
                    Some(Command::Open(reply)) => {}
                    Some(Command::Send(frame)) => self.send(frame).await?,
                    Some(Command::Flush) => self.flush().await?,
                    Some(Command::Close(Either::Left(id))) =>
                        if self.streams.contains_key(&id.val()) {
                            let mut header = Header::data(id, 0);
                            header.fin();
                            self.send(Frame::new(header)).await?
                        }
                    Some(Command::Close(Either::Right(reply))) => {}
                    None => break
                },
                frame = self.socket.try_next() => {
                    match frame {
                        Ok(Some(frame)) => {
                            match self.on_frame(frame) {
                                Action::None => continue,
                                Action::New(stream) => return Ok(Some(stream)),
                                Action::Update(frame) => self.send(frame).await?,
                                Action::Ping(frame) => self.send(frame).await?,
                                Action::Reset(frame) => self.send(frame).await?,
                                Action::Terminate(frame) => self.send(frame).await?
                            }
                            self.flush().await?
                        }
                        Ok(None) => break,
                        Err(e) => return Err(e.into())
                    }
                    // Remove stale streams.
                    let conn_id = self.id;
                    self.streams.retain(|id, stream| {
                        if stream.strong_count() == 1 {
                            log::trace!("{}: removing dropped stream {}", conn_id, id);
                            return false
                        }
                        true
                    })
                }
                complete => break
            }
        }
        Ok(None)
    }

    async fn send<A>(&mut self, f: Frame<A>) -> Result<()> {
        if let Err(e) = self.socket.send(f.cast()).await {
            log::debug!("{}: error sending frame: {}", self.id, e);
            self.kill();
            return Err(e.into())
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        if let Err(e) = self.socket.flush().await {
            log::debug!("{}: error flushing socket: {}", self.id, e);
            self.kill();
            return Err(e.into())
        }
        Ok(())
    }

    fn kill(&mut self) {
        log::debug!("{}: destroying connection", self.id);
        for s in self.streams.values_mut() {
            let mut shared = s.shared();
            shared.update_state(State::Closed);
            if let Some(w) = shared.reader.take() {
                w.wake()
            }
            if let Some(w) = shared.writer.take() {
                w.wake()
            }
        }
    }

    fn next_stream_id(&mut self) -> Result<StreamId> {
        let proposed = StreamId::new(self.next_id);
        self.next_id = self.next_id.checked_add(2).ok_or(ConnectionError::NoMoreStreamIds)?;
        match self.mode {
            Mode::Client => assert!(proposed.is_client()),
            Mode::Server => assert!(proposed.is_server())
        }
        Ok(proposed)
    }

    fn is_valid_remote_id(&self, id: StreamId, tag: Tag) -> bool {
        if tag == Tag::Ping || tag == Tag::GoAway {
            return id.is_session()
        }
        match self.mode {
            Mode::Client => id.is_server(),
            Mode::Server => id.is_client()
        }
    }

    fn on_frame<A>(&mut self, frame: Frame<A>) -> Action {
        match frame.header().tag() {
            Tag::Data => self.on_data(frame.cast()),
            Tag::WindowUpdate => self.on_window_update(&frame.cast()),
            Tag::Ping => self.on_ping(&frame.cast()),
            Tag::GoAway => {
                self.kill();
                Action::None
            }
        }
    }

    fn on_data(&mut self, frame: Frame<Data>) -> Action {
        let stream_id = frame.header().stream_id();

        if frame.header().flags().contains(RST) { // stream reset
            log::debug!("{}: {}: received reset for stream", self.id, stream_id);
            if let Some(s) = self.streams.get_mut(&stream_id.val()) {
                s.shared().update_state(State::Closed)
            }
            return Action::None
        }

        let is_finish = frame.header().flags().contains(FIN); // half-close

        if frame.header().flags().contains(SYN) { // new stream
            if !self.is_valid_remote_id(stream_id, Tag::Data) {
                log::error!("{}: {}: invalid stream id", self.id, stream_id);
                return Action::Terminate(Frame::protocol_error())
            }
            if frame.body().len() > crate::u32_as_usize(DEFAULT_CREDIT) {
                log::error!("{}: {}: first stream body exceeds default credit", self.id, stream_id);
                return Action::Terminate(Frame::protocol_error())
            }
            if self.streams.contains_key(&stream_id.val()) {
                log::error!("{}: {}: stream already exists", self.id, stream_id);
                return Action::Terminate(Frame::protocol_error())
            }
            if self.streams.len() == self.config.max_num_streams {
                log::error!("{}: maximum number of streams reached", self.id);
                return Action::Terminate(Frame::internal_error())
            }
            let stream = Stream::new(stream_id, DEFAULT_CREDIT, DEFAULT_CREDIT, self.sender.clone());
            {
                let mut shared = stream.shared();
                if is_finish {
                    shared.update_state(State::RecvClosed)
                }
                shared.window = shared.window.saturating_sub(frame.body().len() as u32);
                shared.buffer.push(frame.into_body());
            }
            self.streams.insert(stream_id.val(), stream.clone());
            return Action::New(stream)
        }

        if let Some(stream) = self.streams.get_mut(&stream_id.val()) {
            let mut shared = stream.shared();
            if frame.body().len() > crate::u32_as_usize(shared.window) {
                log::error!("{}: {}: frame body larger than window of stream", self.id, stream_id);
                return Action::Terminate(Frame::protocol_error())
            }
            if is_finish {
                shared.update_state(State::RecvClosed)
            }
            let max_buffer_size = self.config.max_buffer_size;
            if shared.buffer.len().map(move |n| n >= max_buffer_size).unwrap_or(true) {
                log::error!("{}: {}: buffer of stream grows beyond limit", self.id, stream_id);
                let mut header = Header::data(stream_id, 0);
                header.rst();
                return Action::Reset(Frame::new(header))
            }
            shared.window = shared.window.saturating_sub(frame.body().len() as u32);
            shared.buffer.push(frame.into_body());
            if !is_finish
                && shared.window == 0
                && self.config.window_update_mode == WindowUpdateMode::OnReceive
            {
                log::trace!("{}: {}: sending window update", self.id, stream_id);
                shared.window = self.config.receive_window;
                let frame = Frame::window_update(stream_id, self.config.receive_window);
                return Action::Update(frame)
            }
        }

        Action::None
    }

    fn on_window_update(&mut self, frame: &Frame<WindowUpdate>) -> Action {
        let stream_id = frame.header().stream_id();

        if frame.header().flags().contains(RST) { // stream reset
            log::debug!("{}: {}: received reset for stream", self.id, stream_id);
            if let Some(s) = self.streams.get_mut(&stream_id.val()) {
                s.shared().update_state(State::Closed)
            }
            return Action::None
        }

        let is_finish = frame.header().flags().contains(FIN); // half-close

        if frame.header().flags().contains(SYN) { // new stream
            if !self.is_valid_remote_id(stream_id, Tag::WindowUpdate) {
                log::error!("{}: {}: invalid stream id", self.id, stream_id);
                return Action::Terminate(Frame::protocol_error())
            }
            if self.streams.contains_key(&stream_id.val()) {
                log::error!("{}: {}: stream already exists", self.id, stream_id);
                return Action::Terminate(Frame::protocol_error())
            }
            if self.streams.len() == self.config.max_num_streams {
                log::error!("{}: maximum number of streams reached", self.id);
                return Action::Terminate(Frame::protocol_error())
            }
            let credit = frame.header().credit();
            let stream = Stream::new(stream_id, DEFAULT_CREDIT, credit, self.sender.clone());
            if is_finish {
                stream.shared().update_state(State::RecvClosed)
            }
            self.streams.insert(stream_id.val(), stream.clone());
            return Action::New(stream)
        }

        if let Some(stream) = self.streams.get_mut(&stream_id.val()) {
            let mut shared = stream.shared();
            shared.credit += frame.header().credit();
            if is_finish {
                shared.update_state(State::RecvClosed)
            }
            if let Some(w) = shared.writer.take() {
                w.wake()
            }
        }

        Action::None
    }

    fn on_ping(&mut self, frame: &Frame<Ping>) -> Action {
        let stream_id = frame.header().stream_id();
        if frame.header().flags().contains(ACK) { // pong
            return Action::None
        }
        if stream_id == CONNECTION_ID || self.streams.contains_key(&stream_id.val()) {
            let mut hdr = Header::ping(frame.header().nonce());
            hdr.ack();
            return Action::Ping(Frame::new(hdr))
        }
        log::debug!("{}: {}: received ping for unknown stream", self.id, stream_id);
        Action::None
    }
}

