// Copyright (c) 2018-2019 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

use futures::prelude::*;
use std::{pin::Pin, task::{Context, Poll}};

/// Wrapper to perform sends without implicit flush.
pub(crate) struct Socket<T, A> {
    inner: T,
    _mark: std::marker::PhantomData<A>
}

impl<T: Sink<A> + Unpin, A> Socket<T, A> {
    pub(crate) fn new(inner: T) -> Self {
        Socket { inner, _mark: std::marker::PhantomData }
    }

    pub(crate) fn inner(&mut self) -> &mut T {
        &mut self.inner
    }

    /// Like `SinkExt::send` but without an implicit flush.
    pub(crate) async fn send(&mut self, item: A) -> Result<(), T::Error> {
        SendOnly { sink: &mut self.inner, item: Some(item) }.await
    }

    pub(crate) async fn flush(&mut self) -> Result<(), T::Error> {
        self.inner.flush().await
    }

    pub(crate) async fn close(&mut self) -> Result<(), T::Error> {
        self.inner.close().await
    }
}

struct SendOnly<'a, T, A> {
    sink: &'a mut T,
    item: Option<A>
}

impl<T: Sink<A> + Unpin, A> Unpin for SendOnly<'_, T, A> {}

impl<T: Sink<A> + Unpin, A> Future for SendOnly<'_, T, A> {
    type Output = Result<(), T::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if let Some(item) = self.as_mut().item.take() {
            let mut this = self.as_mut();
            let mut sink = Pin::new(&mut this.sink);
            match sink.as_mut().poll_ready(cx)? {
                Poll::Ready(()) => sink.as_mut().start_send(item)?,
                Poll::Pending => {
                    this.item = Some(item);
                    return Poll::Pending
                }
            }
        }
        Poll::Ready(Ok(()))
    }
}

