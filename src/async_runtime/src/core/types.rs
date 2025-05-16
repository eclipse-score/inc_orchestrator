// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use std::boxed::Box;
use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

// Used to Box Futures
pub(crate) type BoxCustom<T> = Box<T>; // TODO: We shall replace Global allocator with our own. Since Allocator API is not stable, we shall provide own Box impl (only for internal purpose handling)

pub type FutureBox<T> = Pin<BoxCustom<dyn Future<Output = T> + Send>>;

pub fn box_future<T: Send, U: Future<Output = T> + 'static + Send>(fut: U) -> FutureBox<T> {
    BoxCustom::pin(fut)
}

// Both used internally, that may allocate at runtime from a previously bounded allocator.
pub(crate) type BoxInternal<T> = Box<T>; // TODO: Use mempool allocator, for now we keep default impl
pub(crate) type ArcInternal<T> = Arc<T>; // TODO: Use mempool allocator, for now we keep default impl

///
/// TaskId encodes the worker on which it was created and it's number local to the worker.
/// This id cannot be used to infer task order creation or anything like that, it's only for identification purpose.
///
#[derive(Copy, Clone, Debug)]
pub(crate) struct TaskId(pub(crate) u32);

thread_local! {
    static TASK_COUNTER: Cell<u32> = const {Cell::new(0)};
}

#[allow(dead_code)]
impl TaskId {
    pub(crate) fn new(worker_id: u8) -> Self {
        let val = (TASK_COUNTER.get()) % 0x00FFFFFF; //TODO: Fix it later or change algo
        TASK_COUNTER.set(val + 1);

        Self((val << 8) | worker_id as u32)
    }

    pub(crate) fn worker(&self) -> u8 {
        (self.0 & 0xFF_u32) as u8
    }
}
