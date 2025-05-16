// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

//! # Async Runtime Crate
//!
//! `async_runtime` is a library that provides a customizable `async/await` runtime for concurrent programming.
//!
//! # Details
//!
//! ## Engines
//! The runtime can consist of multiple execution engines. Each engine can be added using [`crate::runtime::async_runtime::AsyncRuntimeBuilder::with_engine`] and configured through the [`crate::scheduler::execution_engine::ExecutionEngineBuilder`] interface.
//!
//! Engines allow you to separate different aspects of your application, such as running different components with different priorities.
//!
//! Each engine provides the following features:
//! - Configuration of `thread priority` for all async workers
//! - Configuration of `affinity` for all async workers
//! - Creation of dedicated workers that can run arbitrary code (including blocking operations) without impacting the main async worker pool
//!
//!
//! ### Async Workers
//! Async workers operate within a work-stealing model to balance load efficiently. This means that a `task`
//! spawned on worker N may complete execution on worker M, ensuring optimal resource utilization and throughput.
//!
//!
//! ### Dedicated Workers
//! These are separate, uniquely tagged workers that can be targeted for specific workloads. The key characteristic
//! of dedicated workers is task locality - when you spawn a `task` on such a worker, it will always execute on that
//! specific worker, even if the task needs to yield and be woken up later.
//!
//! This guarantees that when a task is resumed via its `Waker`, it will always continue execution on the same thread
//! where it was originally spawned, providing strong execution locality guarantees when needed.

use core::types::{box_future, FutureBox};
use std::future::Future;

use foundation::not_recoverable_error;
use foundation::prelude::*;
use futures::reusable_box_future::ReusableBoxFuture;
use scheduler::{
    context::ctx_get_handler,
    join_handle::JoinHandle,
    task::async_task::{AsyncTask, TaskRef},
    workers::worker_types::UniqueWorkerId,
};

pub mod core;
pub mod futures;
pub mod runtime;
pub mod scheduler;

#[cfg(test)]
mod testing;

///
/// Spawns a given `future` into runtime and let it execute on any of configured workers
/// This function allocates a `future` dynamically using [`Box`]
///
pub fn spawn<T>(future: T) -> JoinHandle<T::Output>
where
    T: Future + 'static + Send,
    T::Output: Send + 'static,
{
    let boxed = box_future(future);
    spawn_from_boxed(boxed)
}

///
/// Same as [`spawn`], but from already boxed future. No allocation is done for a future using this API.
///
pub fn spawn_from_boxed<T>(boxed: FutureBox<T>) -> JoinHandle<T>
where
    T: Send + 'static,
{
    if let Some(handler) = ctx_get_handler() {
        handler.spawn(boxed)
    } else {
        not_recoverable_error!("For now we don't allow runtime API to be called outside of runtime")
    }
}

///
/// Same as [`spawn`], but from reusable future. No allocation is done for a future using this API.
///
pub fn spawn_from_reusable<T>(reusable: ReusableBoxFuture<T>) -> JoinHandle<T>
where
    T: Send + 'static,
{
    if let Some(handler) = ctx_get_handler() {
        handler.spawn_reusable(reusable)
    } else {
        not_recoverable_error!("For now we don't allow runtime API to be called outside of runtime")
    }
}

///
/// Spawns a given `future` into runtime and let it execute on dedicated worker using `worker_id`.
/// This function allocates a `future` dynamically using [`Box`]
///
pub fn spawn_on_dedicated<T>(future: T, worker_id: UniqueWorkerId) -> JoinHandle<T::Output>
where
    T: Future + 'static + Send,
    T::Output: Send + 'static,
{
    let boxed = box_future(future);
    spawn_from_boxed_on_dedicated(boxed, worker_id)
}

///
/// Same as [`spawn_on_dedicated`], but from already boxed future. No allocation is done for a future using this API.
///
pub fn spawn_from_boxed_on_dedicated<T>(boxed: FutureBox<T>, worker_id: UniqueWorkerId) -> JoinHandle<T>
where
    T: Send + 'static,
{
    if let Some(handler) = ctx_get_handler() {
        handler.spawn_on_dedicated(boxed, worker_id)
    } else {
        not_recoverable_error!("For now we don't allow runtime API to be called outside of runtime")
    }
}

///
/// Same as [`spawn_on_dedicated`], but from reusable future. No allocation is done for a future using this API.
///
pub fn spawn_from_reusable_on_dedicated<T>(reusable: ReusableBoxFuture<T>, worker_id: UniqueWorkerId) -> JoinHandle<T>
where
    T: Send + 'static,
{
    if let Some(handler) = ctx_get_handler() {
        handler.spawn_reusable_on_dedicated(reusable, worker_id)
    } else {
        not_recoverable_error!("For now we don't allow runtime API to be called outside of runtime")
    }
}
