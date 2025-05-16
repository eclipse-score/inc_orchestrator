// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use std::{
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use crate::prelude::*;

pub struct ThreadWaitBarrier {
    mtx: std::sync::Mutex<u32>,
    cv: std::sync::Condvar,
    ntf_cnt: FoundationAtomicU32,
    expected_cnt: u32,
}

pub struct ThreadReadyNotifier {
    inner: Arc<ThreadWaitBarrier>,
}

impl ThreadReadyNotifier {
    pub fn ready(self) {
        {
            let mut value = self.inner.mtx.lock().unwrap();
            *value -= 1;
        }
        self.inner.cv.notify_one();
    }
}

impl ThreadWaitBarrier {
    pub fn new(thread_to_wait: u32) -> Self {
        Self {
            mtx: Mutex::new(thread_to_wait),
            cv: Condvar::new(),
            ntf_cnt: FoundationAtomicU32::new(0),
            expected_cnt: thread_to_wait,
        }
    }

    pub fn get_notifier(self: &Arc<Self>) -> Option<ThreadReadyNotifier> {
        let new_cnt = self.ntf_cnt.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if new_cnt > self.expected_cnt {
            None
        } else {
            Some(ThreadReadyNotifier { inner: self.clone() })
        }
    }

    pub fn wait_for_all(&self, dur: Duration) -> Result<(), CommonErrors> {
        let res = self.cv.wait_timeout_while(self.mtx.lock().unwrap(), dur, |cond| *cond > 0).unwrap();

        if res.1.timed_out() {
            Err(CommonErrors::Timeout)
        } else {
            Ok(())
        }
    }
}
