// Copyright (c) 2018-2019 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

use fehler::{throw, throws};
use std::fmt;
use thiserror::Error;

/// The message frame header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header<T> {
    version: Version,
    tag: Tag,
    flags: Flags,
    stream_id: StreamId,
    length: Len,
    _marker: std::marker::PhantomData<T>
}

impl<T> Header<T> {
    pub fn version(&self) -> Version {
        self.version
    }

    pub fn tag(&self) -> Tag {
        self.tag
    }

    pub fn flags(&self) -> Flags {
        self.flags
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub fn len(&self) -> Len {
        self.length
    }

    #[cfg(test)]
    pub fn set_len(&mut self, len: u32) {
        self.length = Len(len)
    }

    pub(crate) fn cast<U>(self) -> Header<U> {
        Header {
            version: self.version,
            tag: self.tag,
            flags: self.flags,
            stream_id: self.stream_id,
            length: self.length,
            _marker: std::marker::PhantomData
        }
    }
}

impl<T: HasSyn> Header<T> {
    /// Set the [`SYN`] flag.
    pub fn syn(&mut self) {
        self.flags.0 |= SYN.0
    }
}

impl<T: HasAck> Header<T> {
    /// Set the [`ACK`] flag.
    pub fn ack(&mut self) {
        self.flags.0 |= ACK.0
    }
}

impl<T: HasFin> Header<T> {
    /// Set the [`FIN`] flag.
    pub fn fin(&mut self) {
        self.flags.0 |= FIN.0
    }
}

impl<T: HasRst> Header<T> {
    /// Set the [`RST`] flag.
    pub fn rst(&mut self) {
        self.flags.0 |= RST.0
    }
}

impl Header<Data> {
    /// Create a new data frame header.
    pub fn data(id: StreamId, len: u32) -> Self {
        Header {
            version: Version(0),
            tag: Tag::Data,
            flags: Flags(0),
            stream_id: id,
            length: Len(len),
            _marker: std::marker::PhantomData
        }
    }
}

impl Header<WindowUpdate> {
    /// Create a new window update frame header.
    pub fn window_update(id: StreamId, credit: u32) -> Self {
        Header {
            version: Version(0),
            tag: Tag::WindowUpdate,
            flags: Flags(0),
            stream_id: id,
            length: Len(credit),
            _marker: std::marker::PhantomData
        }
    }

    /// The credit this window update grants to the remote.
    pub fn credit(&self) -> u32 {
        self.length.0
    }
}

impl Header<Ping> {
    /// Create a new ping frame header.
    pub fn ping(nonce: u32) -> Self {
        Header {
            version: Version(0),
            tag: Tag::Ping,
            flags: Flags(0),
            stream_id: StreamId(0),
            length: Len(nonce),
            _marker: std::marker::PhantomData
        }
    }

    /// The nonce of this ping.
    pub fn nonce(&self) -> u32 {
        self.length.0
    }
}

impl Header<GoAway> {
    /// Terminate the session without indicating an error to the remote.
    pub fn term() -> Self {
        Self::go_away(0)
    }

    /// Terminate the session indicating a protocol error to the remote.
    pub fn protocol_error() -> Self {
        Self::go_away(1)
    }

    /// Terminate the session indicating an internal error to the remote.
    pub fn internal_error() -> Self {
        Self::go_away(2)
    }

    fn go_away(code: u32) -> Self {
        Header {
            version: Version(0),
            tag: Tag::GoAway,
            flags: Flags(0),
            stream_id: StreamId(0),
            length: Len(code),
            _marker: std::marker::PhantomData
        }
    }

    /// Get the termination code.
    pub fn code(&self) -> u32 {
        self.length.0
    }
}

/// Data message type.
#[derive(Debug)]
pub enum Data {}

/// Window update message type.
#[derive(Debug)]
pub enum WindowUpdate {}

/// Ping message type.
#[derive(Debug)]
pub enum Ping {}

/// Go Away message type.
#[derive(Debug)]
pub enum GoAway {}

/// Types which have a `syn` method.
pub trait HasSyn: private::Sealed {}
impl HasSyn for Data {}
impl HasSyn for WindowUpdate {}
impl HasSyn for Ping {}

/// Types which have an `ack` method.
pub trait HasAck: private::Sealed {}
impl HasAck for Data {}
impl HasAck for WindowUpdate {}
impl HasAck for Ping {}

/// Types which have a `fin` method.
pub trait HasFin: private::Sealed {}
impl HasFin for Data {}
impl HasFin for WindowUpdate {}

/// Types which have a `rst` method.
pub trait HasRst: private::Sealed {}
impl HasRst for Data {}
impl HasRst for WindowUpdate {}

mod private {
    pub trait Sealed {}

    impl Sealed for super::Data {}
    impl Sealed for super::WindowUpdate {}
    impl Sealed for super::Ping {}
}

/// A tag is the runtime representation of a message type.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Tag {
    Data,
    WindowUpdate,
    Ping,
    GoAway
}

/// The protocol version a message corresponds to.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Version(u8);

impl Version {
    pub fn val(self) -> u8 {
        self.0
    }
}

/// The message length.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Len(u32);

impl Len {
    pub fn val(self) -> u32 {
        self.0
    }
}

pub const CONNECTION_ID: StreamId = StreamId(0);

/// The stream ID of a message.
/// The value 0 denotes no particular stream but the whole session.
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId(u32);

impl StreamId {
    pub fn new(val: u32) -> Self {
        StreamId(val)
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

    pub fn val(self) -> u32 {
        self.0
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Possible flags set on a message.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Flags(u16);

impl Flags {
    pub fn contains(self, other: Flags) -> bool {
        self.0 & other.0 == other.0
    }

    pub fn and(self, other: Flags) -> Flags {
        Flags(self.0 | other.0)
    }
}

const MAX_FLAG_VAL: u16 = 8;

/// Indicates the start of a new stream.
pub const SYN: Flags = Flags(1);

/// Acknowledges the start of a new stream.
pub const ACK: Flags = Flags(2);

/// Indicates the half-closing of a stream.
pub const FIN: Flags = Flags(4);

/// Indicates an immediate stream reset.
pub const RST: Flags = Flags(8);

/// The serialised header size in bytes.
pub const HEADER_SIZE: usize = 12;

/// An encoder/decoder of [`Header`] values.
#[derive(Debug)]
pub struct Codec {
    max_body_len: usize
}

impl Codec {
    /// Create a new codec which accepts frames up to the given max. body length.
    pub fn new(max_body_len: usize) -> Self {
        Codec { max_body_len }
    }

    pub fn encode<T>(&self, hdr: &Header<T>) -> [u8; HEADER_SIZE] {
        let mut buf = [0; HEADER_SIZE];
        buf[0] = hdr.version.0;
        buf[1] = hdr.tag as u8;
        buf[2 .. 4].copy_from_slice(&hdr.flags.0.to_be_bytes());
        buf[4 .. 8].copy_from_slice(&hdr.stream_id.0.to_be_bytes());
        buf[8 .. HEADER_SIZE].copy_from_slice(&hdr.length.0.to_be_bytes());
        buf
    }

    #[throws(DecodeError)]
    pub fn decode<T>(&self, buf: [u8; HEADER_SIZE]) -> Header<T> {
        if buf[0] != 0 {
            throw!(DecodeError::Version(buf[0]))
        }

        let hdr = Header {
            version: Version(buf[0]),
            tag: match buf[1] {
                0 => Tag::Data,
                1 => Tag::WindowUpdate,
                2 => Tag::Ping,
                3 => Tag::GoAway,
                t => throw!(DecodeError::Type(t))
            },
            flags: Flags(u16::from_be_bytes([buf[2], buf[3]])),
            stream_id: StreamId(u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]])),
            length: Len(u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]])),
            _marker: std::marker::PhantomData
        };

        if Tag::Data == hdr.tag {
            let len = crate::u32_as_usize(hdr.length.0);
            if len > self.max_body_len {
                throw!(DecodeError::FrameTooLarge(len))
            }
        }

        if hdr.flags.0 > MAX_FLAG_VAL {
            throw!(DecodeError::Flags(hdr.flags.0))
        }

        hdr
    }
}

/// Possible errors while decoding a message frame header.
#[derive(Debug, Error)]
pub enum DecodeError {
    /// Unknown version.
    #[error("unknown version: {0}")]
    Version(u8),

    /// An unknown frame type.
    #[error("unknown frame type: {0}")]
    Type(u8),

    /// Unknown flags.
    #[error("unknown flags type: {:0x}", .0)]
    Flags(u16),

    /// The frame body length larger than the configured maximum.
    #[error("frame body is too large ({0})")]
    FrameTooLarge(usize),

    #[doc(hidden)]
    #[error("__Nonexhaustive")]
    __Nonexhaustive
}

#[cfg(test)]
mod tests {
    use quickcheck::{Arbitrary, Gen, quickcheck};
    use rand::{Rng, seq::SliceRandom};
    use super::*;

    impl Arbitrary for Header<()> {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let tag = [Tag::Data, Tag::WindowUpdate, Tag::Ping, Tag::GoAway]
                .choose(g)
                .unwrap()
                .clone();

            Header {
                version: Version(0),
                tag,
                flags: Flags(std::cmp::min(g.gen(), MAX_FLAG_VAL)),
                stream_id: StreamId(g.gen()),
                length: Len(g.gen()),
                _marker: std::marker::PhantomData
            }
        }
    }

    #[test]
    fn encode_decode_identity() {
        fn property(mut hdr: Header<()>, len: u32) -> bool {
            let mut codec = Codec::new(crate::u32_as_usize(len));
            hdr.length = Len(std::cmp::min(hdr.length.0, len));
            let bytes = codec.encode(&hdr);
            match codec.decode(bytes) {
                Ok(x) => x == hdr,
                Err(e) => {
                    eprintln!("decode error: {}", e);
                    false
                }
            }
        }
        quickcheck(property as fn(Header<()>, u32) -> bool)
    }
}

