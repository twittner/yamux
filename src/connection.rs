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
        self.0.send(Command::Close(tx)).await?;
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
    Send(Frame<()>),
    /// Flush the socket.
    Flush,
    /// Close the connection.
    Close(oneshot::Sender<()>)
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
                    Some(Command::Close(reply)) => {}
                    None => break
                },
                frame = self.socket.try_next() => {
                    match frame {
                        Ok(Some(frame)) => {
                            match self.on_frame(frame).await {
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
            self.kill().await;
            return Err(e.into())
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        if let Err(e) = self.socket.flush().await {
            log::debug!("{}: error flushing socket: {}", self.id, e);
            self.kill().await;
            return Err(e.into())
        }
        Ok(())
    }

    async fn kill(&mut self) {
        log::debug!("{}: destroying connection", self.id);
        for s in self.streams.values_mut() {
            s.shared().await.update_state(State::Closed)
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

    async fn on_frame<A>(&mut self, frame: Frame<A>) -> Action {
        match frame.header().tag() {
            Tag::Data => self.on_data(frame.cast()).await,
            Tag::WindowUpdate => self.on_window_update(&frame.cast()).await,
            Tag::Ping => self.on_ping(&frame.cast()),
            Tag::GoAway => {
                self.kill().await;
                Action::None
            }
        }
    }

    async fn on_data(&mut self, frame: Frame<Data>) -> Action {
        let stream_id = frame.header().stream_id();

        if frame.header().flags().contains(RST) { // stream reset
            log::debug!("{}: {}: received reset for stream", self.id, stream_id);
            if let Some(s) = self.streams.get_mut(&stream_id.val()) {
                s.shared().await.update_state(State::Closed)
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
                log::error!("{}: {}: initial data for stream exceeds default credit", self.id, stream_id);
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
            let mut stream = Stream::new(stream_id, DEFAULT_CREDIT, DEFAULT_CREDIT, self.sender.clone());
            {
                let mut shared = stream.shared().await;
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
            let mut shared = stream.shared().await;
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

    async fn on_window_update(&mut self, frame: &Frame<WindowUpdate>) -> Action {
        let stream_id = frame.header().stream_id();

        if frame.header().flags().contains(RST) { // stream reset
            log::debug!("{}: {}: received reset for stream", self.id, stream_id);
            if let Some(s) = self.streams.get_mut(&stream_id.val()) {
                s.shared().await.update_state(State::Closed)
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
            let mut stream = Stream::new(stream_id, DEFAULT_CREDIT, frame.header().credit(), self.sender.clone());
            if is_finish {
                stream.shared().await.update_state(State::RecvClosed)
            }
            self.streams.insert(stream_id.val(), stream.clone());
            return Action::New(stream)
        }

        if let Some(stream) = self.streams.get_mut(&stream_id.val()) {
            let mut shared = stream.shared().await;
            shared.credit += frame.header().credit();
            if is_finish {
                shared.update_state(State::RecvClosed)
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

//    async fn finish(&mut self, id: StreamId) -> Result<()> {
//        let frame =
//            if let Some(stream) = self.streams.get_mut(&id.val()) {
//                let mut shared = stream.shared().await;
//                if shared.state().can_write() {
//                    log::debug!("{}: {}: finish stream", self.id, id);
//                    shared.update_state(State::SendClosed);
//                    let mut header = Header::data(id, 0);
//                    header.fin();
//                    Frame::new(header)
//                } else {
//                    return Ok(())
//                }
//            } else {
//                return Ok(())
//            };
//        self.send(frame).await?;
//        Ok(())
//    }
}

//impl<T> io::Read for StreamHandle<T>
//where
//    T: AsyncRead + AsyncWrite
//{
//    fn read(&mut self, buf: &mut[u8]) -> io::Result<usize> {
//        let mut inner = Use::with(self.connection.inner.lock(), Action::Destroy);
//        if !inner.config.read_after_close && inner.status != ConnStatus::Open {
//            return Ok(0)
//        }
//        loop {
//            {
//                let mut n = 0;
//                let mut buffer = self.buffer.lock();
//                while let Some(chunk) = buffer.front_mut() {
//                    if chunk.is_empty() {
//                        buffer.pop();
//                        continue
//                    }
//                    let k = min(chunk.len(), buf.len() - n);
//                    (&mut buf[n .. n + k]).copy_from_slice(&chunk[.. k]);
//                    n += k;
//                    chunk.advance(k);
//                    inner.on_drop(Action::None);
//                    if n == buf.len() {
//                        break
//                    }
//                }
//                if n > 0 {
//                    return Ok(n)
//                }
//                let can_read = inner.streams.get(&self.id).map(|s| s.state().can_read());
//                if !can_read.unwrap_or(false) {
//                    log::debug!("{}: {}: can no longer read", inner.id, self.id);
//                    inner.on_drop(Action::None);
//                    return Ok(0) // stream has been reset
//                }
//            }
//
//            if inner.status != ConnStatus::Open {
//                return Ok(0)
//            }
//
//            if inner.config.window_update_mode == WindowUpdateMode::OnRead {
//                let inner = &mut *inner;
//                let frame =
//                    if let Some(stream) = inner.streams.get_mut(&self.id) {
//                        if stream.window == 0 {
//                            log::trace!("{}: {}: read: sending window update", inner.id, self.id);
//                            stream.window = inner.config.receive_window;
//                            Some(Frame::window_update(self.id, inner.config.receive_window))
//                        } else {
//                            None
//                        }
//                    } else {
//                        None
//                    };
//                if let Some(frame) = frame {
//                    inner.add_pending(frame.into_raw())
//                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
//                }
//            }
//
//            match inner.process_incoming() {
//                Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
//                Ok(Async::NotReady) => {
//                    if !self.buffer.lock().is_empty() {
//                        continue
//                    }
//                    inner.on_drop(Action::None);
//                    let can_read = inner.streams.get(&self.id).map(|s| s.state().can_read());
//                    if can_read.unwrap_or(false) {
//                        return Err(io::ErrorKind::WouldBlock.into())
//                    } else {
//                        log::debug!("{}: {}: can no longer read", inner.id, self.id);
//                        return Ok(0) // stream has been reset
//                    }
//                }
//                Ok(Async::Ready(())) => {
//                    assert!(inner.status != ConnStatus::Open);
//                    if !inner.config.read_after_close || self.buffer.lock().is_empty() {
//                        inner.on_drop(Action::None);
//                        return Ok(0)
//                    }
//                }
//            }
//        }
//    }
//}
//
//impl<T> AsyncRead for StreamHandle<T> where T: AsyncRead + AsyncWrite {}
//
//impl<T> io::Write for StreamHandle<T>
//where
//    T: AsyncRead + AsyncWrite
//{
//    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
//        let mut inner = Use::with(self.connection.inner.lock(), Action::Destroy);
//        if inner.status != ConnStatus::Open {
//            return Err(io::Error::new(io::ErrorKind::WriteZero, "connection is closed"))
//        }
//        match inner.process_incoming() {
//            Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
//            Ok(Async::NotReady) => {}
//            Ok(Async::Ready(())) => {
//                assert!(inner.status != ConnStatus::Open);
//                return Err(io::Error::new(io::ErrorKind::WriteZero, "connection is closed"))
//            }
//        }
//        inner.on_drop(Action::None);
//        let frame = match inner.streams.get_mut(&self.id) {
//            Some(stream) => {
//                if !stream.state().can_write() {
//                    log::debug!("{}: {}: can no longer write", inner.id, self.id);
//                    return Err(io::Error::new(io::ErrorKind::WriteZero, "stream is closed"))
//                }
//                if stream.credit == 0 {
//                    inner.tasks.insert_current();
//                    return Err(io::ErrorKind::WouldBlock.into())
//                }
//                let k = min(stream.credit as usize, buf.len());
//                let b = (&buf[0..k]).into();
//                stream.credit = stream.credit.saturating_sub(k as u32);
//                Frame::data(self.id, b).into_raw()
//            }
//            None => {
//                log::debug!("{}: {}: stream is gone, cannot write", inner.id, self.id);
//                return Err(io::Error::new(io::ErrorKind::WriteZero, "stream is closed"))
//            }
//        };
//        let n = frame.body.len();
//        inner.add_pending(frame).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
//        Ok(n)
//    }
//
//    fn flush(&mut self) -> io::Result<()> {
//        let mut inner = Use::with(self.connection.inner.lock(), Action::Destroy);
//        if inner.status != ConnStatus::Open {
//            return Ok(())
//        }
//        match inner.flush_pending() {
//            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
//            Ok(Async::NotReady) => {
//                inner.on_drop(Action::None);
//                Err(io::ErrorKind::WouldBlock.into())
//            }
//            Ok(Async::Ready(())) => {
//                inner.on_drop(Action::None);
//                Ok(())
//            }
//        }
//    }
//}
//
//impl<T> AsyncWrite for StreamHandle<T>
//where
//    T: AsyncRead + AsyncWrite
//{
//    fn shutdown(&mut self) -> Poll<(), io::Error> {
//        let mut connection = Use::with(self.connection.inner.lock(), Action::Destroy);
//        if connection.status != ConnStatus::Open {
//            return Ok(Async::Ready(()))
//        }
//        connection.finish(self.id).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
//        match connection.flush_pending() {
//            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
//            Ok(Async::NotReady) => {
//                connection.on_drop(Action::None);
//                Ok(Async::NotReady)
//            }
//            Ok(Async::Ready(())) => {
//                connection.on_drop(Action::None);
//                Ok(Async::Ready(()))
//            }
//        }
//    }
//}
