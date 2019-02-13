// Copyright 2018 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

use crate::stream;
use futures::task;
use nohash_hasher::IntMap;

/// A notifier maintains a collection of tasks which should be notified at some point.
pub struct Notifier {
    tasks: IntMap<u32, task::Task>
}

impl Notifier {
    pub fn new() -> Self {
        Notifier { tasks: IntMap::default() }
    }

    pub fn insert_current(&mut self, id: stream::Id) {
        self.tasks.insert(id.as_u32(), task::current());
    }

    pub fn notify(&mut self, id: stream::Id) {
        if let Some(t) = self.tasks.remove(&id.as_u32()) {
            t.notify()
        }
    }

    pub fn notify_all(&mut self) {
        for (_, t) in self.tasks.drain() {
            t.notify();
        }
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }
}

