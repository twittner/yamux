// Copyright (c) 2018-2019 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

use crate::{Stream, error::ConnectionError};
use futures::{ready, channel::{mpsc, oneshot}, prelude::*};
use std::{pin::Pin, task::{Context, Poll}};
use super::Command;

type Result<T> = std::result::Result<T, ConnectionError>;

/// The Yamux `Connection` controller.
///
/// While a Yamux connection makes progress via its `next_stream` method,
/// this controller can be used to asynchronously direct the connection,
/// e.g. to open a new stream to the remote or to close the connection.
///
/// The possible operations are implemented as async methods and redundantly
/// as poll-based variants which may be useful inside of other poll based
/// environments such as certain trait implementations.
#[derive(Debug)]
pub struct Control {
    sender: mpsc::Sender<Command>,
    state: RemoteControlState
}

impl Clone for Control {
    fn clone(&self) -> Self {
        Control {
            sender: self.sender.clone(),
            state: RemoteControlState::Idle
        }
    }
}

/// Internal state used for the implementation of the `poll_*` methods.
#[derive(Debug)]
enum RemoteControlState {
    /// Nothing to wait for.
    Idle,
    /// We have sent a [`Command::OpenStream`] and await the result.
    AwaitOpen(oneshot::Receiver<Result<Stream>>),
    /// We have sent a [`Command::CloseConnection`] and await the result.
    AwaitClose(oneshot::Receiver<()>),
}

impl Control {
    pub(crate) fn new(sender: mpsc::Sender<Command>) -> Self {
        Control {
            sender,
            state: RemoteControlState::Idle
        }
    }

    /// Open a new stream to the remote.
    pub async fn open_stream(&mut self) -> Result<Stream> {
        let (tx, rx) = oneshot::channel();
        self.sender.send(Command::OpenStream(tx)).await?;
        rx.await?
    }

    /// Close the connection.
    pub async fn close(&mut self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender.send(Command::CloseConnection(tx)).await?;
        Ok(rx.await?)
    }

    /// [`Poll`] based alternative to [`Control::open_stream`].
    pub fn poll_open_stream(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Stream>> {
        ready!(self.sender.poll_ready(cx)?);
        loop {
            match std::mem::replace(&mut self.state, RemoteControlState::Idle) {
                RemoteControlState::Idle => {
                    let (tx, rx) = oneshot::channel();
                    self.sender.start_send(Command::OpenStream(tx))?;
                    self.state = RemoteControlState::AwaitOpen(rx)
                }
                RemoteControlState::AwaitOpen(mut rx) =>
                    match Pin::new(&mut rx).poll(cx)? {
                        Poll::Ready(result) => return Poll::Ready(result),
                        Poll::Pending => {
                            self.state = RemoteControlState::AwaitOpen(rx);
                            return Poll::Pending
                        }
                    }
                RemoteControlState::AwaitClose(_) => panic!("invalid remote control state")
            }
        }
    }

    /// [`Poll`] based alternative to [`Control::close`].
    pub fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        ready!(self.sender.poll_ready(cx)?);
        loop {
            match std::mem::replace(&mut self.state, RemoteControlState::Idle) {
                RemoteControlState::Idle => {
                    let (tx, rx) = oneshot::channel();
                    self.sender.start_send(Command::CloseConnection(tx))?;
                    self.state = RemoteControlState::AwaitClose(rx)
                }
                RemoteControlState::AwaitClose(mut rx) =>
                    match Pin::new(&mut rx).poll(cx)? {
                        Poll::Ready(()) => return Poll::Ready(Ok(())),
                        Poll::Pending => {
                            self.state = RemoteControlState::AwaitClose(rx);
                            return Poll::Pending
                        }
                    }
                RemoteControlState::AwaitOpen(_) => panic!("invalid remote control state")
            }
        }
    }
}

