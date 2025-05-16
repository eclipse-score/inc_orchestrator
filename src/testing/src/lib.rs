// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Wake, Waker};

///
/// Helper waker that does not do anything
///
pub fn noop_waker() -> Waker {
    static NOOP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(noop_clone, noop, noop, noop);

    fn noop(_data: *const ()) {}

    fn noop_clone(_: *const ()) -> RawWaker {
        noop_raw_waker()
    }

    fn noop_raw_waker() -> RawWaker {
        RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE)
    }

    unsafe { Waker::from_raw(noop_raw_waker()) }
}

///
/// Helper waker whose ref count can be tracked
///
pub struct TrackableWaker {
    inner: Arc<InnerTrackableWaker>,
}

impl Default for TrackableWaker {
    fn default() -> Self {
        Self::new()
    }
}

impl TrackableWaker {
    pub fn new() -> TrackableWaker {
        Self {
            inner: Arc::new(InnerTrackableWaker {
                was_waked: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }

    pub fn was_waked(&self) -> bool {
        self.inner.was_waked.load(std::sync::atomic::Ordering::Relaxed)
    }

    ///
    /// Return the strong reference count of this waker
    ///
    pub fn get_waker_ref_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }

    ///
    /// Get the waker object out of this TrackableWaker
    ///
    pub fn get_waker(&self) -> Waker {
        Waker::from(self.inner.clone())
    }
}

struct InnerTrackableWaker {
    was_waked: std::sync::atomic::AtomicBool,
}

impl Wake for InnerTrackableWaker {
    fn wake(self: Arc<Self>) {
        // do nothing
        self.was_waked.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

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
