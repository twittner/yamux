// Copyright (c) 2018-2019 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

use crate::{chunks::Chunks, frame::header::StreamId};
use futures::{channel::mpsc, lock::{Mutex, MutexGuard}};
use std::sync::Arc;
use super::Event;

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
    sender: mpsc::Sender<Event>,
    shared: Arc<Mutex<Shared>>
}

impl Stream {
    pub(crate) fn new(id: StreamId, window: u32, credit: u32, sender: mpsc::Sender<Event>) -> Self {
        Stream {
            id,
            shared: Arc::new(Mutex::new(Shared::new(window, credit))),
            sender
        }
    }

    pub(crate) async fn shared(&mut self) -> MutexGuard<'_, Shared> {
        self.shared.lock().await
    }

    pub(crate) fn strong_count(&self) -> usize {
        Arc::strong_count(&self.shared)
    }

    pub(crate) fn id(&self) -> StreamId {
        self.id
    }
}

#[derive(Debug)]
pub(crate) struct Shared {
    state: State,
    pub(crate) window: u32,
    pub(crate) credit: u32,
    pub(crate) buffer: Chunks
}

impl Shared {
    fn new(window: u32, credit: u32) -> Self {
        Shared {
            state: State::Open,
            window,
            credit,
            buffer: Chunks::new()
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

