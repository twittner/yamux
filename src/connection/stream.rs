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
use futures::{channel::mpsc, lock::Mutex};
use std::sync::Arc;

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

#[derive(Clone, Debug)]
pub struct Stream(Arc<Mutex<Inner>>);

impl Stream {
    pub fn new(id: StreamId, window: u32, credit: u32, sender: mpsc::Sender<super::Command>) -> Self {
        let inner = Inner::new(id, window, credit, sender);
        Stream(Arc::new(Mutex::new(inner)))
    }

    pub(crate) async fn inner(&mut self) -> &mut Inner {
        self.0.lock().await
    }

}

#[derive(Debug)]
pub(crate) struct Inner {
    id: StreamId,
    state: State,
    sender: mpsc::Sender<super::Command>,
    pub(crate) window: u32,
    pub(crate) credit: u32,
    pub(crate) buffer: Chunks
}

impl Inner {
    fn new(id: StreamId, window: u32, credit: u32, sender: mpsc::Sender<super::Command>) -> Self {
        Inner {
            id,
            state: State::Open,
            sender,
            window,
            credit,
            buffer: Chunks::new()
        }
    }

    fn state(&self) -> State {
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

