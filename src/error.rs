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
use std::{fmt, io};

/// Possible errors while decoding a Yamux frame
#[derive(Debug)]
pub enum DecodeError {
    /// An I/O error occurred.
    Io(io::Error),
    /// An unknown frame type.
    Type(u8),
    /// The frame body length to too large.
    FrameTooLarge(usize),

    #[doc(hidden)]
    __Nonexhaustive
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DecodeError::Io(e) => write!(f, "i/o error: {}", e),
            DecodeError::Type(t) => write!(f, "unkown frame type: {}", t),
            DecodeError::FrameTooLarge(n) => write!(f, "frame body is too large ({})", n),
            DecodeError::__Nonexhaustive => f.write_str("__Nonexhaustive")
        }
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DecodeError::Io(e) => Some(e),
            DecodeError::Type(_)
            | DecodeError::FrameTooLarge(_)
            | DecodeError::__Nonexhaustive => None
        }
    }
}

impl From<io::Error> for DecodeError {
    fn from(e: io::Error) -> Self {
        DecodeError::Io(e)
    }
}

#[derive(Debug)]
pub enum ConnectionError {
    Io(io::Error),
    Decode(DecodeError),
    NoMoreStreamIds,
    Closed,
    StreamNotFound(stream::Id),
    TooManyStreams,
    TooManyPendingFrames,

    #[doc(hidden)]
    __Nonexhaustive
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConnectionError::Io(e) => write!(f, "i/o error: {}", e),
            ConnectionError::Decode(e) => write!(f, "decode error: {}", e),
            ConnectionError::NoMoreStreamIds =>
                f.write_str("number of stream ids has been exhausted"),
            ConnectionError::Closed => f.write_str("connection is closed"),
            ConnectionError::StreamNotFound(id) => write!(f, "stream {} not found", id),
            ConnectionError::TooManyStreams => f.write_str("maximum number of streams exhausted"),
            ConnectionError::TooManyPendingFrames =>
                f.write_str("maximum number of pending frames reached"),
            ConnectionError::__Nonexhaustive => f.write_str("___Nonexhaustive")
        }
    }
}

impl std::error::Error for ConnectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConnectionError::Io(e) => Some(e),
            ConnectionError::Decode(e) => Some(e),
            ConnectionError::NoMoreStreamIds
            | ConnectionError::Closed
            | ConnectionError::StreamNotFound(_)
            | ConnectionError::TooManyStreams
            | ConnectionError::TooManyPendingFrames
            | ConnectionError::__Nonexhaustive => None
        }
    }
}

