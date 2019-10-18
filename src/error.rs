// Copyright (c) 2018-2019 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

use crate::{frame, frame::header::StreamId};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    #[error("decode error: {0}")]
    Decode(#[from] frame::DecodeError),

    #[error("number of stream ids has been exhausted")]
    NoMoreStreamIds,

    #[error("connection is closed")]
    Closed,

    #[error("stream {0} not found")]
    StreamNotFound(StreamId),

    #[error("maximum number of streams reached")]
    TooManyStreams,

    #[error("maximum number of pending frames reached")]
    TooManyPendingFrames,

    #[doc(hidden)]
    #[error("__Nonexhaustive")]
    __Nonexhaustive
}

impl From<futures::channel::mpsc::SendError> for ConnectionError {
    fn from(_: futures::channel::mpsc::SendError) -> Self {
        ConnectionError::Closed
    }
}

impl From<futures::channel::oneshot::Canceled> for ConnectionError {
    fn from(_: futures::channel::oneshot::Canceled) -> Self {
        ConnectionError::Closed
    }
}
