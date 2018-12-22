// Copyright 2018 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

use bytes::{Bytes, BytesMut};
use crate::{error::StreamError, Config, notify::Notifier, WindowUpdateMode};
use futures::{executor::Spawn, prelude::*, stream::Fuse, sync::mpsc};
use log::{debug, trace};
use parking_lot::Mutex;
use std::{fmt, io, sync::{atomic::{AtomicUsize, Ordering}, Arc}, u32};
use tokio_io::{AsyncRead, AsyncWrite};

pub(crate) const CONNECTION_ID: Id = Id(0);

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Id(u32);

impl Id {
    pub(crate) fn new(id: u32) -> Id {
        Id(id)
    }

    pub fn is_server(self) -> bool {
        self.0 % 2 == 0
    }

    pub fn is_client(self) -> bool {
        !self.is_server()
    }

    pub fn is_session(self) -> bool {
        self.0 == 0
    }

    pub fn as_u32(self) -> u32 {
        self.0
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum State {
    Open,
    SendClosed,
    RecvClosed,
    Closed
}

impl State {
    pub fn can_read(self) -> bool {
        match self {
            State::RecvClosed | State::Closed => false,
            _ => true
        }
    }

    pub fn can_write(self) -> bool {
        match self {
            State::SendClosed | State::Closed => false,
            _ => true
        }
    }
}

#[derive(Debug)]
pub struct Window(AtomicUsize);

impl Window {
    pub fn new(n: u32) -> Window {
        Window(AtomicUsize::new(n as usize))
    }

    pub fn decrement(&self, amount: usize) -> usize {
        loop {
            let prev = self.0.load(Ordering::SeqCst);
            let next = prev.checked_sub(amount).unwrap_or(0);
            if self.0.compare_and_swap(prev, next, Ordering::SeqCst) == prev {
                return next
            }
        }
    }

    pub fn get(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }

    pub fn set(&self, val: usize) {
        self.0.store(val, Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub enum Item {
	Data(Bytes),
	WindowUpdate(u32),
	Reset,
	Finish
}


pub type Outbound = mpsc::UnboundedSender<(Id, Item)>;
pub type Inbound = Spawn<Fuse<mpsc::UnboundedReceiver<Item>>>;

pub struct Stream {
    id: Id,
    state: State,
    config: Arc<Config>,
    window: Arc<Window>,
    credit: u32,
	inbound: Mutex<Inbound>,
	outbound: Outbound,
    buffer: BytesMut,
    tasks: Arc<Notifier>
}

impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Stream")
            .field("id", &self.id)
            .field("state", &self.state)
            .field("window", &self.window.get())
            .field("credit", &self.credit)
            .field("buffer", &self.buffer.len())
            .finish()
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        if self.state.can_write() {
            let _ = self.send_item(Item::Reset);
        }
    }
}

impl Stream {
    pub fn new(id: Id, c: Arc<Config>, w: Arc<Window>, credit: u32, tx: Outbound, rx: Inbound) -> Self {
        Stream {
            id,
            state: State::Open,
            config: c,
            window: w,
            credit,
            inbound: Mutex::new(rx),
            outbound: tx,
            buffer: BytesMut::new(),
			tasks: Arc::new(Notifier::new())
        }
    }

    pub fn setup(&mut self, b: Bytes) {
        self.window.decrement(b.len());
        self.buffer.extend_from_slice(&b[..])
    }

    pub fn id(&self) -> Id {
        self.id
    }

    pub fn state(&self) -> State {
        self.state
    }

    pub fn update_state(&mut self, next: State) {
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

        trace!("[{}] {:?} -> {:?}", self.id, current, next);
    }

    fn send_item(&mut self, item: Item) -> Result<(), StreamError> {
        trace!("[{}] send item: {:?}", self.id, item);
        if self.outbound.unbounded_send((self.id, item)).is_err() {
            self.update_state(State::SendClosed);
            return Err(StreamError::StreamClosed(self.id))
        }
        Ok(())
    }

    fn poll(&mut self) -> Async<()> {
        loop {
            let result = {
                self.inbound.lock().poll_stream_notify(&self.tasks, 0)
            };
            match result {
                Err(()) => {
                    self.update_state(State::RecvClosed);
                    return Async::Ready(())
                }
                Ok(Async::NotReady) => {
                    return Async::NotReady
                }
                Ok(Async::Ready(item)) => match item {
                    Some(Item::Data(body)) => {
                        trace!("[{}] received data: len = {}", self.id, body.len());
                        let remaining = self.window.decrement(body.len());
                        self.buffer.extend(body);
                        if remaining == 0 && self.config.window_update_mode == WindowUpdateMode::OnRead {
                            trace!("[{}] receive window exhausted", self.id);
                            let item = Item::WindowUpdate(self.config.receive_window);
                            self.window.set(self.config.receive_window as usize);
                            if self.send_item(item).is_err() {
                                debug!("[{}] failed to send window update", self.id)
                            }
                        }
                    }
                    Some(Item::WindowUpdate(n)) => {
                        trace!("[{}] received window update: {}", self.id, n);
                        self.credit = self.credit.saturating_add(n);
                        self.tasks.notify_all();
                    }
                    Some(Item::Finish) => {
                        trace!("[{}] received finish", self.id);
                        self.update_state(State::RecvClosed);
                    }
                    Some(Item::Reset) => {
                        trace!("[{}] received reset", self.id);
                        self.update_state(State::Closed);
                        return Async::Ready(())
                    }
                    None => {
                        trace!("[{}] receiver returned None", self.id);
                        self.update_state(State::RecvClosed);
                        return Async::Ready(())
                    }
                }
            }
        }
    }

    fn copy_from_buffer(&mut self, buf: &mut [u8]) -> usize {
        let n = std::cmp::min(buf.len(), self.buffer.len());
        (&mut buf[.. n]).copy_from_slice(&self.buffer.split_to(n));
        n
    }

}

impl io::Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let k = self.copy_from_buffer(buf);
        if !self.state.can_read() {
            return Ok(k)
        }
        match self.poll() {
            Async::NotReady => {
                if k == 0 && self.buffer.is_empty() {
                    return Err(io::ErrorKind::WouldBlock.into())
                }
                Ok(k + self.copy_from_buffer(&mut buf[k ..]))
            }
            Async::Ready(()) => {
                Ok(k + self.copy_from_buffer(&mut buf[k ..]))
            }
        }
    }
}

impl AsyncRead for Stream { }

impl io::Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		self.poll();

		if !self.state.can_write() {
			return Err(io::ErrorKind::WriteZero.into())
		}

        if self.credit == 0 {
            trace!("[{}] write: no credit ({} tasks already waiting)", self.id, self.tasks.len());
            self.tasks.insert_current();
            return Err(io::ErrorKind::WouldBlock.into())
        }

        let len = std::cmp::min(buf.len(), self.credit as usize);
        self.credit -= len as u32;
        let body = (&buf[.. len]).into();

        trace!("[{}] writing {} bytes", self.id, len);
        if self.send_item(Item::Data(body)).is_err() {
            return Err(io::ErrorKind::WriteZero.into())
        }

        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl AsyncWrite for Stream {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        if self.state.can_write() {
            let _ = self.send_item(Item::Finish);
            self.update_state(State::SendClosed)
        }
        Ok(Async::Ready(()))
    }
}

