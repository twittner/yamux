// Copyright 2018-2019 Parity Technologies (UK) Ltd.
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
use fehler::{throw, throws};
use futures::{channel::mpsc, lock::Mutex, prelude::*, ready};
use std::{collections::BTreeMap, fmt, pin::Pin, sync::Arc, task::{Context, Poll}};
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

pub struct Connection {
    id: Id,
    mode: Mode,
    config: Config,
    streams: BTreeMap<StreamId, StreamEntry>,
    io_task: Pin<Box<dyn Future<Output = Result<(), ConnectionError>>>>,
    incoming: mpsc::Receiver<Result<Frame<()>, frame::DecodeError>>,
    outgoing: mpsc::Sender<Frame<()>>,
    pending: Option<Pin<Box<dyn Future<Output = Result<(), ConnectionError>>>>>,
    next_id: u32
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Connection")
            .field("id", &self.id)
            .field("mode", &self.mode)
            .field("streams", &self.streams.len())
            .field("next_id", &self.next_id)
            .finish()
    }
}

impl Stream for Connection {
    type Item = Result<StreamHandle, ConnectionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        loop {
            if let Poll::Ready(result) = Pin::new(&mut self.io_task).poll(cx) {
                if let Err(e) = result {
                    return Poll::Ready(Some(Err(e.into())))
                }
                return Poll::Ready(None)
            }
            while let Some(item) = ready!(Pin::new(&mut self.incoming).poll_next(cx)) {
                match item {
                    Ok(frame) => {
                        let mut fut = async {
                            if let Some(response) = self.on_frame(frame).await? {
                                self.send_outgoing(frame).await?
                            }
                            Ok(())
                        };
                        if Pin::new_unchecked(&mut fut).poll(cx).is_pending() {
                            self.pending = Some(Box::pin(fut));
                            return Poll::Pending
                        }
                    }
                    Err(e) => {
                        log::debug!("{}: decode error: {}", self.id, e);
                        return Poll::Ready(Some(Err(e.into())))
                    }
                }
            }
        }
    }
}

impl Connection {
    pub fn new<T>(io: T, cfg: Config, mode: Mode) -> Self
    where
        T: AsyncRead + AsyncWrite + Unpin + 'static
    {
        let id = Id(rand::random());
        log::debug!("new connection: id = {}, mode = {:?}", id, mode);
        let io = crate::compat::AioCompat(io);
        let mut resource = Framed::new(io, frame::Codec::new(cfg.max_buffer_size)).fuse();
        let (io_tx, io_rx) = mpsc::channel(cfg.max_pending_frames);
        let (mut co_tx, co_rx) = mpsc::channel(cfg.max_pending_frames);
        let mut io_rx = io_rx.fuse();
        let io_task = async move {
            loop {
                futures::select! {
                    item = resource.next() =>
                        if let Some(item) = item {
                            co_tx.send(item).await?
                        } else {
                            break
                        },
                    item = io_rx.next() =>
                        if let Some(item) = item {
                            resource.send(item).await?
                        } else {
                            break
                        },
                    complete => break
                }
            }
            Ok(())
        };
        Connection {
            id,
            mode,
            config: cfg,
            streams: BTreeMap::new(),
            io_task: Box::pin(io_task),
            incoming: co_rx,
            outgoing: io_tx,
            pending: None,
            next_id: match mode {
                Mode::Client => 1,
                Mode::Server => 2
            }
        }
    }

    #[throws(ConnectionError)]
    pub async fn open_stream(&mut self) -> StreamHandle {
        if self.streams.len() >= self.config.max_num_streams {
            log::error!("{}: maximum number of streams reached", self.id);
            throw!(ConnectionError::TooManyStreams)
        }
        let id = self.next_stream_id()?;
        let mut frame = Frame::window_update(id, self.config.receive_window);
        frame.header_mut().syn();
        self.send_outgoing(frame).await?;
        let stream = StreamEntry::new(self.config.receive_window, DEFAULT_CREDIT);
        let buffer = stream.buffer.clone();
        self.streams.insert(id, stream);
        log::debug!("{}: {}: outgoing stream of {:?}", self.id, id, self);
        StreamHandle::new(id, buffer, self.outgoing.clone())
    }

    #[throws(ConnectionError)]
    pub async fn close(&mut self) {
        self.send_outgoing(Frame::term()).await?
    }

    #[throws(ConnectionError)]
    async fn send_outgoing<T>(&mut self, f: Frame<T>) {
        if self.outgoing.send(f.unchecked_cast()).await.is_err() {
            log::debug!("{}: connection closed", self.id);
            self.kill()
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
    async fn on_frame(&mut self, frame: Frame<()>) -> Option<Frame<()>> {
        match frame.header().tag() {
            Tag::Data =>
                self.on_data(frame.unchecked_cast()).await?.map(Frame::unchecked_cast),
            Tag::WindowUpdate =>
                self.on_window_update(&frame.unchecked_cast()).await?.map(Frame::unchecked_cast),
            Tag::Ping =>
                self.on_ping(&frame.unchecked_cast()).map(Frame::unchecked_cast),
            Tag::GoAway => {
                self.kill();
                None
            }
        }
    }

    #[throws(ConnectionError)]
    async fn on_data(&mut self, frame: Frame<Data>) -> Option<Frame<GoAway>> {
        let stream_id = frame.header().stream_id();

        if frame.header().flags().contains(RST) { // stream reset
            log::debug!("{}: {}: received reset for stream", self.id, stream_id);
            if let Some(s) = self.streams.get_mut(&stream_id) {
                s.update_state(State::Closed)
            }
            return None
        }

        let is_finish = frame.header().flags().contains(FIN); // half-close

        if frame.header().flags().contains(SYN) { // new stream
            if !self.is_valid_remote_id(stream_id, Tag::Data) {
                log::error!("{}: {}: invalid stream id", self.id, stream_id);
                return Some(Frame::protocol_error())
            }
            if frame.body().len() > crate::u32_as_usize(DEFAULT_CREDIT) {
                log::error!("{}: {}: initial data for stream exceeds default credit", self.id, stream_id);
                return Some(Frame::protocol_error())
            }
            if self.streams.contains_key(&stream_id) {
                log::error!("{}: {}: stream already exists", self.id, stream_id);
                return Some(Frame::protocol_error())
            }
            if self.streams.len() == self.config.max_num_streams {
                log::error!("{}: maximum number of streams reached", self.id);
                return Some(Frame::internal_error())
            }
            let mut stream = StreamEntry::new(DEFAULT_CREDIT, DEFAULT_CREDIT);
            if is_finish {
                stream.update_state(State::RecvClosed)
            }
            stream.window = stream.window.saturating_sub(frame.body().len() as u32);
            stream.buffer.lock().await.push(frame.into_body());
            self.streams.insert(stream_id, stream);
            // TODO: self.incoming.push_back(stream_id);
            return None
        }

        let frame =
            if let Some(stream) = self.streams.get_mut(&stream_id) {
                if frame.body().len() > crate::u32_as_usize(stream.window) {
                    log::error!("{}: {}: frame body larger than window of stream", self.id, stream_id);
                    return Some(Frame::protocol_error())
                }
                if is_finish {
                    stream.update_state(State::RecvClosed)
                }
                let max_buffer_size = self.config.max_buffer_size;
                let mut stream_buffer = stream.buffer.lock().await;
                if stream_buffer.len().map(move |n| n >= max_buffer_size).unwrap_or(true) {
                    log::error!("{}: {}: buffer of stream grows beyond limit", self.id, stream_id);
                    // TODO: self.reset(stream_id).await?;
                    return None
                }
                stream.window = stream.window.saturating_sub(frame.body().len() as u32);
                stream_buffer.push(frame.into_body());
                if !is_finish && stream.window == 0 && self.config.window_update_mode == WindowUpdateMode::OnReceive {
                    log::trace!("{}: {}: sending window update", self.id, stream_id);
                    stream.window = self.config.receive_window;
                    Frame::window_update(stream_id, self.config.receive_window)
                } else {
                    return None
                }
            } else {
                return None
            };

        self.send_outgoing(frame).await?;
        None
    }

    #[throws(ConnectionError)]
    async fn on_window_update(&mut self, frame: &Frame<WindowUpdate>) -> Option<Frame<GoAway>> {
        let stream_id = frame.header().stream_id();

        if frame.header().flags().contains(RST) { // stream reset
            log::debug!("{}: {}: received reset for stream", self.id, stream_id);
            if let Some(s) = self.streams.get_mut(&stream_id) {
                s.update_state(State::Closed)
            }
            return None
        }

        let is_finish = frame.header().flags().contains(FIN); // half-close

        if frame.header().flags().contains(SYN) { // new stream
            if !self.is_valid_remote_id(stream_id, Tag::WindowUpdate) {
                log::error!("{}: {}: invalid stream id", self.id, stream_id);
                return Some(Frame::protocol_error())
            }
            if self.streams.contains_key(&stream_id) {
                log::error!("{}: {}: stream already exists", self.id, stream_id);
                return Some(Frame::protocol_error())
            }
            if self.streams.len() == self.config.max_num_streams {
                log::error!("{}: maximum number of streams reached", self.id);
                return Some(Frame::protocol_error())
            }
            let mut stream = StreamEntry::new(DEFAULT_CREDIT, frame.header().credit());
            if is_finish {
                stream.update_state(State::RecvClosed)
            }
            self.streams.insert(stream_id, stream);
            // TODO: self.incoming.push_back(stream_id);
            return None
        }

        if let Some(stream) = self.streams.get_mut(&stream_id) {
            stream.credit += frame.header().credit();
            if is_finish {
                stream.update_state(State::RecvClosed)
            }
        }

        None
    }

    fn on_ping(&mut self, frame: &Frame<Ping>) -> Option<Frame<Ping>> {
        let stream_id = frame.header().stream_id();

        if frame.header().flags().contains(ACK) { // pong
            return None
        }

        if stream_id == CONNECTION_ID || self.streams.contains_key(&stream_id) {
            let mut hdr = Header::ping(frame.header().nonce());
            hdr.ack();
            return Some(Frame::new(hdr))
        }

        log::debug!("{}: {}: received ping for unknown stream", self.id, stream_id);
        None
    }

    #[throws(ConnectionError)]
    async fn reset(&mut self, id: StreamId) {
        if let Some(stream) = self.streams.remove(&id) {
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
            if let Some(stream) = self.streams.get_mut(&id) {
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
    outgoing: mpsc::Sender<Frame<()>>
}

impl StreamHandle {
    fn new(id: StreamId, buffer: Arc<Mutex<Chunks>>, out: mpsc::Sender<Frame<()>>) -> Self {
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
