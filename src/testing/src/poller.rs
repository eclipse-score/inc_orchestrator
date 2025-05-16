// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use crate::waker::noop_waker;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

///
/// Helper struct for testing futures
///
pub struct TestingFuturePoller<OutType> {
    future: Pin<Box<dyn Future<Output = OutType> + 'static + Send>>,
}

impl<OutType> TestingFuturePoller<OutType> {
    pub fn new(future: impl Future<Output = OutType> + 'static + Send) -> TestingFuturePoller<OutType> {
        Self { future: Box::pin(future) }
    }

    pub fn from_boxed(boxed_future: Pin<Box<dyn Future<Output = OutType> + 'static + Send>>) -> TestingFuturePoller<OutType> {
        Self { future: boxed_future }
    }

    ///
    /// Poll the internal future once with a specified waker
    /// This will add the waker into the set of registered wakers, if not done already
    ///
    pub fn poll_with_waker(&mut self, waker: &Waker) -> Poll<OutType> {
        let mut cx = Context::from_waker(waker);
        self.future.as_mut().poll(&mut cx)
    }

    ///
    /// Poll the internal future repeatedly for n times with a specified waker
    /// This will add the waker into the set of registered wakers, if not done already
    ///
    pub fn poll_n_with_waker(&mut self, n: usize, waker: &Waker) -> Poll<OutType> {
        let mut cx = Context::from_waker(waker);
        let mut result: Poll<OutType> = Poll::Pending;

        for _ in 0..n {
            result = self.future.as_mut().poll(&mut cx)
        }
        result
    }

    ///
    /// Poll the internal future once with a default (noop) waker
    ///
    pub fn poll(&mut self) -> Poll<OutType> {
        self.poll_with_waker(&noop_waker())
    }

    ///
    /// Poll the internal future repeatedly for n times with a default (noop) waker
    ///
    pub fn poll_n(&mut self, n: usize) -> Poll<OutType> {
        self.poll_n_with_waker(n, &noop_waker())
    }
}
