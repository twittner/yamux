// Copyright (c) 2018-2019 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

mod control;
mod stream;

use crate::{
    Config,
    DEFAULT_CREDIT,
    WindowUpdateMode,
    compat,
    error::ConnectionError,
    frame::{self, Frame},
    frame::header::{self, CONNECTION_ID, Data, GoAway, Header, Ping, StreamId, Tag, WindowUpdate}
};
use either::Either;
use futures::{channel::{mpsc, oneshot}, prelude::*, stream::Fuse};
use nohash_hasher::IntMap;
use std::{fmt, sync::Arc};
use tokio_codec::Framed;

pub use control::RemoteControl;
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

/// The connection identifier.
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

/// A Yamux connection object.
///
/// Wraps the underlying I/O resource and makes progress via its
/// [`Connection::next_stream`] method which must be called repeatedly
/// until `Ok(None)` signals EOF or an error is encountered.
pub struct Connection<T> {
    id: Id,
    mode: Mode,
    config: Arc<Config>,
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
    Open(oneshot::Sender<Result<Stream>>),
    /// A new frame should be sent to remote.
    Send(Frame<()>),
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
    /// Create a new `Connection` from the given I/O resource.
    pub fn new(socket: T, cfg: Config, mode: Mode) -> Self {
        let id = Id(rand::random());
        log::debug!("new connection: id = {}, mode = {:?}", id, mode);
        let (sender, receiver) = mpsc::channel(cfg.max_pending_frames);
        let socket = compat::AioCompat(socket);
        let socket = Framed::new(socket, frame::Codec::new(cfg.max_buffer_size)).fuse();
        Connection {
            id,
            mode,
            config: Arc::new(cfg),
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

    /// Get a controller for this connection.
    pub fn remote_control(&self) -> RemoteControl {
        RemoteControl::new(self.sender.clone())
    }

    /// Get the next incoming stream, opened by the remote.
    ///
    /// This must be called repeatedly in order to make progress.
    /// Once `Ok(None)` or `Err(_)` is returned the connection is
    /// considered closed and no further invocations of this method
    /// must be attempted.
    pub async fn next_stream(&mut self) -> Result<Option<Stream>> {
        let result = self.next().await;

        match &result {
            Ok(Some(_)) => return result,
            Ok(None) => log::debug!("{}: eof", self.id),
            Err(e) => log::error!("{}: connection error: {}", self.id, e)
        }

        // At this point we are either at EOF or encountered an error.
        // We close all streams and wake up the associated tasks before
        // closing the socket. The connection is then considered closed.

        for (_, s) in self.streams.drain() {
            let mut shared = s.shared();
            shared.update_state(State::Closed);
            if let Some(w) = shared.waker.take() {
                w.wake()
            }
        }

        self.socket.close().await?;
        result
    }

    /// The actual implementation of `next_stream`.
    ///
    /// It is wrapped in order to guarantee proper closing in case of an
    /// error or at EOF.
    async fn next(&mut self) -> Result<Option<Stream>> {
        loop {
            // Remove stale streams on each iteration.
            // If we ever get async. destructors we can replace this with
            // streams sending a close command on drop.
            let conn_id = self.id;
            self.streams.retain(|id, stream| {
                if stream.strong_count() == 1 {
                    log::trace!("{}: removing dropped stream {}", conn_id, id);
                    return false
                }
                true
            });
            futures::select! {
                // Handle commands from remote control or our own streams.
                command = self.receiver.next() => match command {
                    // Open a new stream.
                    Some(Command::Open(reply)) => {
                        if self.streams.len() >= self.config.max_num_streams {
                            log::error!("{}: maximum number of streams reached", self.id);
                            let _ = reply.send(Err(ConnectionError::TooManyStreams));
                            continue
                        }
                        let id = self.next_stream_id()?;
                        let mut frame = Frame::window_update(id, self.config.receive_window);
                        frame.header_mut().syn();
                        self.socket.send(frame.cast()).await?;
                        let sender = self.sender.clone();
                        let config = self.config.clone();
                        let window = self.config.receive_window;
                        let stream = Stream::new(id, config, window, DEFAULT_CREDIT, sender);
                        self.streams.insert(id.val(), stream.clone());
                        log::debug!("{}: {}: outgoing stream of {:?}", self.id, id, self);
                        let _ = reply.send(Ok(stream));
                    }
                    // Send a frame from one of our own streams.
                    Some(Command::Send(frame)) => self.socket.send(frame).await?,
                    // Flush the connection.
                    Some(Command::Flush) => self.socket.flush().await?,
                    // Close a stream.
                    Some(Command::Close(Either::Left(id))) =>
                        if self.streams.contains_key(&id.val()) {
                            let mut header = Header::data(id, 0);
                            header.fin();
                            self.socket.send(Frame::new(header).cast()).await?
                        }
                    // Close the whole connection.
                    Some(Command::Close(Either::Right(reply))) => {
                        self.socket.send(Frame::term().cast()).await?;
                        let _ = reply.send(());
                        break
                    }
                    // The remote control and all our streams are gone, so terminate.
                    None => {
                        self.socket.send(Frame::term().cast()).await?;
                        break
                    }
                },

                // Handle incoming frames from the remote.
                frame = self.socket.try_next() => match frame {
                    Ok(Some(frame)) => {
                        if frame.header().tag() == Tag::GoAway {
                            break
                        }
                        match self.on_frame(frame) {
                            Action::None => continue,
                            Action::New(stream) => return Ok(Some(stream)),
                            Action::Update(frame) => self.socket.send(frame.cast()).await?,
                            Action::Ping(frame) => self.socket.send(frame.cast()).await?,
                            Action::Reset(frame) => self.socket.send(frame.cast()).await?,
                            Action::Terminate(frame) => self.socket.send(frame.cast()).await?
                        }
                        self.socket.flush().await?
                    }
                    Ok(None) => break,
                    Err(e) => return Err(e.into())
                }
            }
        }

        Ok(None)
    }

    /// Dispatch frame handling based on the frame's runtime type [`Tag`].
    fn on_frame<A>(&mut self, frame: Frame<A>) -> Action {
        match frame.header().tag() {
            Tag::Data => self.on_data(frame.cast()),
            Tag::WindowUpdate => self.on_window_update(&frame.cast()),
            Tag::Ping => self.on_ping(&frame.cast()),
            Tag::GoAway => Action::None
        }
    }

    fn on_data(&mut self, frame: Frame<Data>) -> Action {
        let stream_id = frame.header().stream_id();

        if frame.header().flags().contains(header::RST) { // stream reset
            log::debug!("{}: {}: received reset for stream", self.id, stream_id);
            if let Some(s) = self.streams.get_mut(&stream_id.val()) {
                s.shared().update_state(State::Closed)
            }
            return Action::None
        }

        let is_finish = frame.header().flags().contains(header::FIN); // half-close

        if frame.header().flags().contains(header::SYN) { // new stream
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
            let sender = self.sender.clone();
            let config = self.config.clone();
            let stream = Stream::new(stream_id, config, DEFAULT_CREDIT, DEFAULT_CREDIT, sender);
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
            if let Some(w) = shared.waker.take() {
                w.wake()
            }
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

        if frame.header().flags().contains(header::RST) { // stream reset
            log::debug!("{}: {}: received reset for stream", self.id, stream_id);
            if let Some(s) = self.streams.get_mut(&stream_id.val()) {
                s.shared().update_state(State::Closed)
            }
            return Action::None
        }

        let is_finish = frame.header().flags().contains(header::FIN); // half-close

        if frame.header().flags().contains(header::SYN) { // new stream
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
            let config = self.config.clone();
            let stream = Stream::new(stream_id, config, DEFAULT_CREDIT, credit, self.sender.clone());
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
            if let Some(w) = shared.waker.take() {
                w.wake()
            }
        }

        Action::None
    }

    fn on_ping(&mut self, frame: &Frame<Ping>) -> Action {
        let stream_id = frame.header().stream_id();
        if frame.header().flags().contains(header::ACK) { // pong
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

    // Get the next valid stream ID.
    fn next_stream_id(&mut self) -> Result<StreamId> {
        let proposed = StreamId::new(self.next_id);
        self.next_id = self.next_id.checked_add(2).ok_or(ConnectionError::NoMoreStreamIds)?;
        match self.mode {
            Mode::Client => assert!(proposed.is_client()),
            Mode::Server => assert!(proposed.is_server())
        }
        Ok(proposed)
    }

    // Check if the given stream ID is valid w.r.t. the provided tag and our connection mode.
    fn is_valid_remote_id(&self, id: StreamId, tag: Tag) -> bool {
        if tag == Tag::Ping || tag == Tag::GoAway {
            return id.is_session()
        }
        match self.mode {
            Mode::Client => id.is_server(),
            Mode::Server => id.is_client()
        }
    }
}

/// Turn a Yamux [`Connection`] into a [`futures::Stream`].
pub fn into_stream<T>(c: Connection<T>) -> impl futures::stream::Stream<Item = Result<Stream>>
where
    T: AsyncRead + AsyncWrite + Unpin
{
    futures::stream::unfold(c, |mut c| async move {
        match c.next_stream().await {
            Ok(None) => None,
            Ok(Some(stream)) => Some((Ok(stream), c)),
            Err(e) => Some((Err(e), c))
        }
    })
}

