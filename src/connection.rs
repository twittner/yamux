// Copyright (c) 2018-2019 Parity Technologies (UK) Ltd.
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
    chunks::Chunks,
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
    },
    stream::{State, StreamEntry}
};
use either::Either;
use fehler::{throw, throws};
use futures::{channel::{mpsc, oneshot}, lock::Mutex, prelude::*, stream::{self, SplitSink}};
use nohash_hasher::IntMap;
use std::{fmt, pin::Pin, sync::Arc};
use tokio_codec::Framed;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum Mode {
    Client,
    Server
}

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

#[derive(Debug)]
enum Command {
    Inbound(Result<Frame<()>, frame::DecodeError>),
    Open(oneshot::Sender<StreamHandle>),
    Outbound(Frame<()>),
    Flush,
    Close(oneshot::Sender<()>)
}

pub struct Control(mpsc::Sender<Command>);

impl Control {
    #[throws(ConnectionError)]
    pub async fn open_stream(&mut self) -> StreamHandle {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::Open(tx)).await?;
        rx.await?
    }

    #[throws(ConnectionError)]
    pub async fn close(&mut self) {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::Close(tx)).await?;
        rx.await?
    }
}

pub struct Connection<T> {
    id: Id,
    mode: Mode,
    config: Config,
    outgoing: SplitSink<Framed<crate::compat::AioCompat<T>, frame::Codec>, Frame<()>>,
    incoming: Pin<Box<dyn Stream<Item = Result<Command, frame::DecodeError>>>>,
    next_id: u32,
    streams: IntMap<u32, StreamEntry>,
    cmd_sender: mpsc::Sender<Command>
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

impl<T> Connection<T>
where
    T: AsyncRead + AsyncWrite + Unpin + 'static
{
    pub fn new(socket: T, cfg: Config, mode: Mode) -> (Self, Control) {
        let id = Id(rand::random());
        log::debug!("new connection: id = {}, mode = {:?}", id, mode);

        let socket = crate::compat::AioCompat(socket);
        let (cmd_sender, cmd_receiver) = mpsc::channel(cfg.max_pending_frames);

        let (outgoing, incoming) = {
            let (sink, stream) = Framed::new(socket, frame::Codec::new(cfg.max_buffer_size)).split();
            let stream = stream.map_ok(|f| Command::Inbound(Ok(f)));
            (sink, Box::pin(stream::select(cmd_receiver.map(Ok), stream).fuse()))
        };

        let conn = Connection {
            id,
            mode,
            config: cfg,
            outgoing,
            incoming,
            streams: IntMap::default(),
            cmd_sender: cmd_sender.clone(),
            next_id: match mode {
                Mode::Client => 1,
                Mode::Server => 2
            }
        };

        (conn, Control(cmd_sender))
    }

    #[throws(ConnectionError)]
    pub async fn incoming_stream(&mut self) -> Option<StreamHandle> {
        while let Some(command) = self.incoming.try_next().await? {
            match command {
                Command::Inbound(Err(e)) => throw!(e),
                Command::Inbound(Ok(frame)) => {
                    match self.on_frame(frame).await? {
                        Either::Left(None) => (),
                        Either::Left(Some(response)) => self.send_outgoing(response).await?,
                        Either::Right(id) => {
                            let buffer = self.streams.get(&id.val()).unwrap().buffer.clone();
                            return Some(StreamHandle::new(id, buffer, self.cmd_sender.clone()))
                        }
                    }
                }
                Command::Open(reply) => {}
                Command::Outbound(frame) => self.send_outgoing(frame).await?,
                Command::Flush => {}
                Command::Close(reply) => {}
            }
        }
        None
    }

    #[throws(ConnectionError)]
    async fn send_outgoing<A>(&mut self, f: Frame<A>) {
        if let Err(e) = self.outgoing.send(f.cast()).await {
            log::debug!("{}: connection closed", self.id);
            self.kill();
            throw!(e)
        }
    }

    fn kill(&mut self) {
        log::debug!("{}: destroying connection", self.id);
        for s in self.streams.values_mut() {
            s.update_state(State::Closed)
        }
    }

    #[throws(ConnectionError)]
    fn next_stream_id(&mut self) -> StreamId {
        let proposed = StreamId::new(self.next_id);
        self.next_id = self.next_id.checked_add(2).ok_or(ConnectionError::NoMoreStreamIds)?;
        match self.mode {
            Mode::Client => assert!(proposed.is_client()),
            Mode::Server => assert!(proposed.is_server())
        }
        proposed
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

    #[throws(ConnectionError)]
    async fn on_frame(&mut self, frame: Frame<()>) -> Either<Option<Frame<()>>, StreamId> {
        match frame.header().tag() {
            Tag::Data =>
                self.on_data(frame.cast()).await?.map_left(|o| o.map(Frame::cast)),
            Tag::WindowUpdate =>
                self.on_window_update(&frame.cast()).await?.map_left(|o| o.map(Frame::cast)),
            Tag::Ping =>
                Either::Left(self.on_ping(&frame.cast()).map(Frame::cast)),
            Tag::GoAway => {
                self.kill();
                Either::Left(None)
            }
        }
    }

    #[throws(ConnectionError)]
    async fn on_data(&mut self, frame: Frame<Data>) -> Either<Option<Frame<GoAway>>, StreamId> {
        let stream_id = frame.header().stream_id();

        if frame.header().flags().contains(RST) { // stream reset
            log::debug!("{}: {}: received reset for stream", self.id, stream_id);
            if let Some(s) = self.streams.get_mut(&stream_id.val()) {
                s.update_state(State::Closed)
            }
            return Either::Left(None)
        }

        let is_finish = frame.header().flags().contains(FIN); // half-close

        if frame.header().flags().contains(SYN) { // new stream
            if !self.is_valid_remote_id(stream_id, Tag::Data) {
                log::error!("{}: {}: invalid stream id", self.id, stream_id);
                return Either::Left(Some(Frame::protocol_error()))
            }
            if frame.body().len() > crate::u32_as_usize(DEFAULT_CREDIT) {
                log::error!("{}: {}: initial data for stream exceeds default credit", self.id, stream_id);
                return Either::Left(Some(Frame::protocol_error()))
            }
            if self.streams.contains_key(&stream_id.val()) {
                log::error!("{}: {}: stream already exists", self.id, stream_id);
                return Either::Left(Some(Frame::protocol_error()))
            }
            if self.streams.len() == self.config.max_num_streams {
                log::error!("{}: maximum number of streams reached", self.id);
                return Either::Left(Some(Frame::internal_error()))
            }
            let mut stream = StreamEntry::new(DEFAULT_CREDIT, DEFAULT_CREDIT);
            if is_finish {
                stream.update_state(State::RecvClosed)
            }
            stream.window = stream.window.saturating_sub(frame.body().len() as u32);
            stream.buffer.lock().await.push(frame.into_body());
            self.streams.insert(stream_id.val(), stream);
            return Either::Right(stream_id)
        }

        let frame =
            if let Some(stream) = self.streams.get_mut(&stream_id.val()) {
                if frame.body().len() > crate::u32_as_usize(stream.window) {
                    log::error!("{}: {}: frame body larger than window of stream", self.id, stream_id);
                    return Either::Left(Some(Frame::protocol_error()))
                }
                if is_finish {
                    stream.update_state(State::RecvClosed)
                }
                let max_buffer_size = self.config.max_buffer_size;
                let mut stream_buffer = stream.buffer.lock().await;
                if stream_buffer.len().map(move |n| n >= max_buffer_size).unwrap_or(true) {
                    log::error!("{}: {}: buffer of stream grows beyond limit", self.id, stream_id);
                    // TODO: self.reset(stream_id).await?;
                    return Either::Left(None)
                }
                stream.window = stream.window.saturating_sub(frame.body().len() as u32);
                stream_buffer.push(frame.into_body());
                if !is_finish
                    && stream.window == 0
                    && self.config.window_update_mode == WindowUpdateMode::OnReceive
                {
                    log::trace!("{}: {}: sending window update", self.id, stream_id);
                    stream.window = self.config.receive_window;
                    Frame::window_update(stream_id, self.config.receive_window)
                } else {
                    return Either::Left(None)
                }
            } else {
                return Either::Left(None)
            };

        self.send_outgoing(frame).await?;
        Either::Left(None)
    }

    #[throws(ConnectionError)]
    async fn on_window_update(&mut self, frame: &Frame<WindowUpdate>) -> Either<Option<Frame<GoAway>>, StreamId> {
        let stream_id = frame.header().stream_id();

        if frame.header().flags().contains(RST) { // stream reset
            log::debug!("{}: {}: received reset for stream", self.id, stream_id);
            if let Some(s) = self.streams.get_mut(&stream_id.val()) {
                s.update_state(State::Closed)
            }
            return Either::Left(None)
        }

        let is_finish = frame.header().flags().contains(FIN); // half-close

        if frame.header().flags().contains(SYN) { // new stream
            if !self.is_valid_remote_id(stream_id, Tag::WindowUpdate) {
                log::error!("{}: {}: invalid stream id", self.id, stream_id);
                return Either::Left(Some(Frame::protocol_error()))
            }
            if self.streams.contains_key(&stream_id.val()) {
                log::error!("{}: {}: stream already exists", self.id, stream_id);
                return Either::Left(Some(Frame::protocol_error()))
            }
            if self.streams.len() == self.config.max_num_streams {
                log::error!("{}: maximum number of streams reached", self.id);
                return Either::Left(Some(Frame::protocol_error()))
            }
            let mut stream = StreamEntry::new(DEFAULT_CREDIT, frame.header().credit());
            if is_finish {
                stream.update_state(State::RecvClosed)
            }
            self.streams.insert(stream_id.val(), stream);
            return Either::Right(stream_id)
        }

        if let Some(stream) = self.streams.get_mut(&stream_id.val()) {
            stream.credit += frame.header().credit();
            if is_finish {
                stream.update_state(State::RecvClosed)
            }
        }

        Either::Left(None)
    }

    fn on_ping(&mut self, frame: &Frame<Ping>) -> Option<Frame<Ping>> {
        let stream_id = frame.header().stream_id();

        if frame.header().flags().contains(ACK) { // pong
            return None
        }

        if stream_id == CONNECTION_ID || self.streams.contains_key(&stream_id.val()) {
            let mut hdr = Header::ping(frame.header().nonce());
            hdr.ack();
            return Some(Frame::new(hdr))
        }

        log::debug!("{}: {}: received ping for unknown stream", self.id, stream_id);
        None
    }

    #[throws(ConnectionError)]
    async fn reset(&mut self, id: StreamId) {
        if let Some(stream) = self.streams.remove(&id.val()) {
            if stream.state() == State::Closed {
                return
            }
        } else {
            return
        }
        log::debug!("{}: {}: resetting stream of {:?}", self.id, id, self);
        let mut header = Header::data(id, 0);
        header.rst();
        self.send_outgoing(Frame::new(header)).await?
    }

    #[throws(ConnectionError)]
    async fn finish(&mut self, id: StreamId) {
        let frame =
            if let Some(stream) = self.streams.get_mut(&id.val()) {
                if stream.state().can_write() {
                    log::debug!("{}: {}: finish stream", self.id, id);
                    stream.update_state(State::SendClosed);
                    let mut header = Header::data(id, 0);
                    header.fin();
                    Frame::new(header)
                } else {
                    return
                }
            } else {
                return
            };
        self.send_outgoing(frame).await?
    }
}

/// A handle to a multiplexed stream.
#[derive(Debug)]
pub struct StreamHandle {
    id: StreamId,
    buffer: Arc<Mutex<Chunks>>,
    outgoing: mpsc::Sender<Command>
}

impl StreamHandle {
    fn new(id: StreamId, buffer: Arc<Mutex<Chunks>>, out: mpsc::Sender<Command>) -> Self {
        StreamHandle { id, buffer, outgoing: out }
    }

//    /// Report how much sending credit this stream has available.
//    pub fn credit(&self) -> Option<u32> {
//        self.connection.inner.lock().streams.get(&self.id).map(|s| s.credit)
//    }
}
//
//impl<T> Drop for StreamHandle<T>
//where
//    T: AsyncRead + AsyncWrite
//{
//    fn drop(&mut self) {
//        let mut inner = self.connection.inner.lock();
//        log::debug!("{}: {}: dropping stream", inner.id, self.id);
//        let _ = inner.reset(self.id);
//    }
//}
//
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
