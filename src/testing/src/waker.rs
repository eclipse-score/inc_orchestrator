// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use std::sync::Arc;
use std::task::{RawWaker, RawWakerVTable, Wake, Waker};

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
        self.was_waked.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}
