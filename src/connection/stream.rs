// Copyright (c) 2018-2019 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

use bytes::BytesMut;
use crate::{chunks::Chunks, frame::{Frame, header::StreamId}};
use either::Either;
use futures::{ready, channel::mpsc, io::{AsyncRead, AsyncWrite}};
use parking_lot::{Mutex, MutexGuard};
use std::{io, pin::Pin, sync::Arc, task::{Context, Poll, Waker}};
use super::Command;

/// The state of a stream.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum State {
    /// Open bidirectionally.
    Open,
    /// Open for incoming messages.
    SendClosed,
    /// Open for outgoing messages.
    RecvClosed,
    /// Closed.
    Closed
}

impl State {
    /// Can we receive messages over this stream?
    pub fn can_read(self) -> bool {
        if let State::RecvClosed | State::Closed = self {
            false
        } else {
            true
        }
    }

    /// Can we send messages over this stream?
    pub fn can_write(self) -> bool {
        if let State::SendClosed | State::Closed = self {
            false
        } else {
            true
        }
    }
}

/// A yamux stream.
#[derive(Clone, Debug)]
pub struct Stream {
    id: StreamId,
    sender: mpsc::Sender<Command>,
    shared: Arc<Mutex<Shared>>
}

impl Stream {
    pub(crate) fn new(id: StreamId, window: u32, credit: u32, sender: mpsc::Sender<Command>) -> Self {
        Stream {
            id,
            sender,
            shared: Arc::new(Mutex::new(Shared::new(window, credit))),
        }
    }

    pub fn id(&self) -> StreamId {
        self.id
    }

    pub(crate) fn strong_count(&self) -> usize {
        Arc::strong_count(&self.shared)
    }

    pub(crate) fn shared(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock()
    }

    fn send(mut self: Pin<&mut Self>, cx: &mut Context, cmd: Command) -> Poll<io::Result<()>> {
        if let Err(e) = ready!(self.sender.poll_ready(cx)) {
            log::debug!("{}: channel error: {}", self.id, e);
            let e = io::Error::new(io::ErrorKind::WriteZero, "connection is closed");
            return Poll::Ready(Err(e))
        }
        if let Err(e) = self.sender.start_send(cmd) {
            log::debug!("{}: channel error: {}", self.id, e);
            let e = io::Error::new(io::ErrorKind::WriteZero, "connection is closed");
            return Poll::Ready(Err(e))
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for Stream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        let mut shared = self.shared();
        let mut n = 0;
        while let Some(chunk) = shared.buffer.front_mut() {
            if chunk.is_empty() {
                shared.buffer.pop();
                continue
            }
            let k = std::cmp::min(chunk.len(), buf.len() - n);
            (&mut buf[n .. n + k]).copy_from_slice(&chunk[.. k]);
            n += k;
            chunk.advance(k);
            if n == buf.len() {
                break
            }
        }
        if n > 0 {
            return Poll::Ready(Ok(n))
        }
        if !shared.state().can_read() {
            log::debug!("{}: can no longer read", self.id);
            return Poll::Ready(Ok(0)) // stream has been reset
        }
        shared.reader = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl AsyncWrite for Stream {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        if let Err(e) = ready!(self.sender.poll_ready(cx)) {
            log::debug!("{}: channel error: {}", self.id, e);
            let e = io::Error::new(io::ErrorKind::WriteZero, "connection is closed");
            return Poll::Ready(Err(e))
        }

        let body = {
            let mut shared = self.shared();
            if !shared.state().can_write() {
                log::debug!("{}: can no longer write", self.id);
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::WriteZero, "stream is closed")))
            }
            if shared.credit == 0 {
                shared.writer = Some(cx.waker().clone());
                return Poll::Pending
            }
            let k = std::cmp::min(crate::u32_as_usize(shared.credit), buf.len());
            shared.credit = shared.credit.saturating_sub(k as u32);
            BytesMut::from(&buf[.. k])
        };

        let n = body.len();
        let frame = Frame::data(self.id, body).expect("body <= u32::MAX");

        if let Err(e) = self.sender.start_send(Command::Send(frame)) {
            log::debug!("{}: channel error: {}", self.id, e);
            let e = io::Error::new(io::ErrorKind::WriteZero, "connection is closed");
            return Poll::Ready(Err(e))
        }

        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        self.send(cx, Command::Flush)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        let cmd = Command::Close(Either::Left(self.id));
        if let Err(e) = ready!(self.as_mut().send(cx, cmd)) {
            return Poll::Ready(Err(e))
        }
        self.shared().update_state(State::SendClosed);
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
pub(crate) struct Shared {
    state: State,
    pub(crate) window: u32,
    pub(crate) credit: u32,
    pub(crate) buffer: Chunks,
    pub(crate) reader: Option<Waker>,
    pub(crate) writer: Option<Waker>
}

impl Shared {
    fn new(window: u32, credit: u32) -> Self {
        Shared {
            state: State::Open,
            window,
            credit,
            buffer: Chunks::new(),
            reader: None,
            writer: None
        }
    }

    pub(crate) fn state(&self) -> State {
        self.state
    }

    pub(crate) fn update_state(&mut self, next: State) {
        use self::State::*;

        let current = self.state;

        match (current, next) {
            (Closed,              _) => {}
            (Open,                _) => self.state = next,
            (RecvClosed,     Closed) => self.state = Closed,
            (RecvClosed,       Open) => {}
            (RecvClosed, RecvClosed) => {}
            (RecvClosed, SendClosed) => self.state = Closed,
            (SendClosed,     Closed) => self.state = Closed,
            (SendClosed,       Open) => {}
            (SendClosed, RecvClosed) => self.state = Closed,
            (SendClosed, SendClosed) => {}
        }
    }
}

