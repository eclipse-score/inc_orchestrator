//
// Copyright (c) 2025 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache License Version 2.0 which is available at
// <https://www.apache.org/licenses/LICENSE-2.0>
//
// SPDX-License-Identifier: Apache-2.0
//

#![allow(dead_code)]

use crate::core::types::{box_future, FutureBox, UniqueWorkerId};
use crate::scheduler::waker::create_waker;

use ::core::cell::RefCell;
use ::core::future::Future;

use ::core::task::Context;
use std::sync::Arc;

use crate::futures::reusable_box_future::ReusableBoxFuture;

use crate::scheduler::{join_handle::JoinHandle, task::async_task::TaskRef};
use std::collections::VecDeque;

use crate::testing::*;

///
/// Mock runtime API with possibility to poll tasks manually.
/// ATTENTION: Instance of runtime is SINGLETON but in TLS so one for each thread.
///
///
pub struct MockRuntime {
    tasks: VecDeque<TaskRef>,
    sched: Arc<SchedulerMock>,
}

impl MockRuntime {
    ///
    /// Removes all tasks from runtime. Shall be called before each test to ensure that no tasks are left from previous tests.
    ///
    pub fn reset(&mut self) {
        self.tasks.clear();
    }

    ///
    /// Poll each task once to advance it's state. When task returns Ready, it will be removed from the queue.
    ///
    pub fn advance_tasks(&mut self) {
        //let mut t = VecDeque::<TaskRef>::new();
        //t.reserve(self.tasks.len());

        while let Some(task) = self.tasks.pop_front() {
            let waker = create_waker(task.clone());
            let mut ctx = Context::from_waker(&waker);

            match task.poll(&mut ctx) {
                _ => {
                    if !task.is_done() {
                        self.tasks.push_back(task);
                    }
                }
            }
        }

        //self.tasks = t;
    }

    ///
    /// Number of tasks waiting for poll
    ///
    pub fn remaining_tasks(&self) -> usize {
        self.tasks.len()
    }
}

thread_local! {
    static RUNTIME_MOCK_INSTANCE: RefCell<Option<MockRuntime>> = RefCell::new(None);
}

static DEV_CODE_ALLOW_ROUTING_OVER_MOCK: ::core::sync::atomic::AtomicBool = ::core::sync::atomic::AtomicBool::new(false);

pub fn init_runtime_mock() {
    RUNTIME_MOCK_INSTANCE.set(Some(MockRuntime {
        tasks: VecDeque::new(),
        sched: Arc::new(SchedulerMock::default()),
    }));
}

///
/// Allows to run development process with RUNTIME MOCK compiled in so it will route to real runtime.
/// THIS IS ONLY ALLOWED IN DEV CODE, NOT IN PRODUCTION.
pub unsafe fn allow_routing_over_mock() {
    DEV_CODE_ALLOW_ROUTING_OVER_MOCK.store(true, ::core::sync::atomic::Ordering::Relaxed);
    println!(
        "\n{bar}\n\
     {warn:^80}\n\
     {msg:^80}\n\
     {bar}\n",
        bar = "═".repeat(80),
        warn = "⚠️  UNSAFE RUN DETECTED! ⚠️",
        msg =
            "This build include RUNTIME API MOCK AND SHALL NOT BE USED IN ANY PRODUCTION! THIS WORKS BECAUSE YOU EXPLICITLY ALLOWED IT IN DEV CODE.",
    );
}
///
/// Reset runtime
///
pub fn runtime_reset() {
    RUNTIME_MOCK_INSTANCE.with(|instance| {
        let mut instance = instance.borrow_mut();

        instance
            .as_mut()
            .expect("Runtime mock instance is not initialized. Did you call `init_runtime_mock`?")
            .tasks
            .clear();
    });
}

///
/// Get access to mock instance within closure `c`
///
pub fn runtime_instance<C: FnMut(&mut MockRuntime) -> T, T>(mut c: C) -> T {
    RUNTIME_MOCK_INSTANCE.with(|instance| {
        c(instance
            .borrow_mut()
            .as_mut()
            .expect("Runtime mock instance is not initialized. Did you call `init_runtime_mock`?"))
    })
}

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
    if DEV_CODE_ALLOW_ROUTING_OVER_MOCK.load(::core::sync::atomic::Ordering::Relaxed) {
        crate::spawn_from_boxed(boxed)
    } else {
        RUNTIME_MOCK_INSTANCE.with(|instance| {
            let mut instance = instance.borrow_mut();

            let runtime = instance
                .as_mut()
                .expect("Runtime mock instance is not initialized. Did you call `init_runtime_mock`?");

            let task = Arc::new(AsyncTask::new(boxed, 0, runtime.sched.clone()));
            let task_ref = TaskRef::new(task);
            runtime.tasks.push_back(task_ref.clone());
            JoinHandle::new(task_ref)
        })
    }
}

///
/// Same as [`spawn`], but from reusable future. No allocation is done for a future using this API.
///
pub fn spawn_from_reusable<T>(reusable: ReusableBoxFuture<T>) -> JoinHandle<T>
where
    T: Send + 'static,
{
    if DEV_CODE_ALLOW_ROUTING_OVER_MOCK.load(::core::sync::atomic::Ordering::Relaxed) {
        crate::spawn_from_reusable(reusable)
    } else {
        RUNTIME_MOCK_INSTANCE.with(|instance| {
            let mut instance = instance.borrow_mut();

            let runtime = instance
                .as_mut()
                .expect("Runtime mock instance is not initialized. Did you call `init_runtime_mock`?");

            let task = Arc::new(AsyncTask::new(reusable.into_pin(), 0, runtime.sched.clone()));
            let task_ref = TaskRef::new(task);
            runtime.tasks.push_back(task_ref.clone());
            JoinHandle::new(task_ref)
        })
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
pub fn spawn_from_boxed_on_dedicated<T>(boxed: FutureBox<T>, _worker_id: UniqueWorkerId) -> JoinHandle<T>
where
    T: Send + 'static,
{
    if DEV_CODE_ALLOW_ROUTING_OVER_MOCK.load(::core::sync::atomic::Ordering::Relaxed) {
        crate::spawn_from_boxed_on_dedicated(boxed, _worker_id)
    } else {
        RUNTIME_MOCK_INSTANCE.with(|instance| {
            let mut instance = instance.borrow_mut();

            let runtime = instance
                .as_mut()
                .expect("Runtime mock instance is not initialized. Did you call `init_runtime_mock`?");

            let task = Arc::new(AsyncTask::new(boxed, 0, runtime.sched.clone()));
            let task_ref = TaskRef::new(task);
            runtime.tasks.push_back(task_ref.clone());
            JoinHandle::new(task_ref)
        })
    }
}

///
/// Same as [`spawn_on_dedicated`], but from reusable future. No allocation is done for a future using this API.
///
pub fn spawn_from_reusable_on_dedicated<T>(reusable: ReusableBoxFuture<T>, _worker_id: UniqueWorkerId) -> JoinHandle<T>
where
    T: Send + 'static,
{
    if DEV_CODE_ALLOW_ROUTING_OVER_MOCK.load(::core::sync::atomic::Ordering::Relaxed) {
        crate::spawn_from_reusable_on_dedicated(reusable, _worker_id)
    } else {
        RUNTIME_MOCK_INSTANCE.with(|instance| {
            let mut instance = instance.borrow_mut();

            let runtime = instance
                .as_mut()
                .expect("Runtime mock instance is not initialized. Did you call `init_runtime_mock`?");

            let task = Arc::new(AsyncTask::new(reusable.into_pin(), 0, runtime.sched.clone()));
            let task_ref = TaskRef::new(task);
            runtime.tasks.push_back(task_ref.clone());
            JoinHandle::new(task_ref)
        })
    }
}

pub mod safety {
    use super::*;

    use ::core::future::Future;

    use crate::{
        core::types::{box_future, FutureBox},
        futures::reusable_box_future::ReusableBoxFuture,
        scheduler::join_handle::JoinHandle,
    };

    use crate::safety::SafetyResult;

    pub fn ensure_safety_enabled() {}

    pub fn spawn<F, T, E>(future: F) -> JoinHandle<F::Output>
    where
        F: Future<Output = SafetyResult<T, E>> + 'static + Send,
        F::Output: Send + 'static,
        E: Send + 'static,
        T: Send + 'static,
    {
        let boxed = box_future(future);
        spawn_from_boxed(boxed)
    }

    pub fn spawn_from_boxed<T, E>(boxed: FutureBox<SafetyResult<T, E>>) -> JoinHandle<SafetyResult<T, E>>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        if DEV_CODE_ALLOW_ROUTING_OVER_MOCK.load(::core::sync::atomic::Ordering::Relaxed) {
            crate::safety::spawn_from_boxed(boxed)
        } else {
            RUNTIME_MOCK_INSTANCE.with(|instance| {
                let mut instance = instance.borrow_mut();

                let runtime = instance
                    .as_mut()
                    .expect("Runtime mock instance is not initialized. Did you call `init_runtime_mock`?");

                let task = Arc::new(AsyncTask::new_safety(true, boxed, 0, runtime.sched.clone()));
                let task_ref = TaskRef::new(task);
                runtime.tasks.push_back(task_ref.clone());
                JoinHandle::new(task_ref)
            })
        }
    }

    pub fn spawn_from_reusable<T, E>(reusable: ReusableBoxFuture<SafetyResult<T, E>>) -> JoinHandle<SafetyResult<T, E>>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        if DEV_CODE_ALLOW_ROUTING_OVER_MOCK.load(::core::sync::atomic::Ordering::Relaxed) {
            crate::safety::spawn_from_reusable(reusable)
        } else {
            RUNTIME_MOCK_INSTANCE.with(|instance| {
                let mut instance = instance.borrow_mut();

                let runtime = instance
                    .as_mut()
                    .expect("Runtime mock instance is not initialized. Did you call `init_runtime_mock`?");

                let task = Arc::new(AsyncTask::new_safety(true, reusable.into_pin(), 0, runtime.sched.clone()));
                let task_ref = TaskRef::new(task);
                runtime.tasks.push_back(task_ref.clone());
                JoinHandle::new(task_ref)
            })
        }
    }

    pub fn spawn_on_dedicated<F, T, E>(future: F, worker_id: UniqueWorkerId) -> JoinHandle<F::Output>
    where
        F: Future<Output = SafetyResult<T, E>> + 'static + Send,
        F::Output: Send + 'static,
        E: Send + 'static,
        T: Send + 'static,
    {
        let boxed = box_future(future);
        spawn_from_boxed_on_dedicated(boxed, worker_id)
    }

    pub fn spawn_from_boxed_on_dedicated<T, E>(boxed: FutureBox<SafetyResult<T, E>>, _worker_id: UniqueWorkerId) -> JoinHandle<SafetyResult<T, E>>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        if DEV_CODE_ALLOW_ROUTING_OVER_MOCK.load(::core::sync::atomic::Ordering::Relaxed) {
            crate::safety::spawn_from_boxed_on_dedicated(boxed, _worker_id)
        } else {
            RUNTIME_MOCK_INSTANCE.with(|instance| {
                let mut instance = instance.borrow_mut();

                let runtime = instance
                    .as_mut()
                    .expect("Runtime mock instance is not initialized. Did you call `init_runtime_mock`?");

                let task = Arc::new(AsyncTask::new_safety(true, boxed, 0, runtime.sched.clone()));
                let task_ref = TaskRef::new(task);
                runtime.tasks.push_back(task_ref.clone());
                JoinHandle::new(task_ref)
            })
        }
    }

    pub fn spawn_from_reusable_on_dedicated<T, E>(
        reusable: ReusableBoxFuture<SafetyResult<T, E>>,
        _worker_id: UniqueWorkerId,
    ) -> JoinHandle<SafetyResult<T, E>>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        if DEV_CODE_ALLOW_ROUTING_OVER_MOCK.load(::core::sync::atomic::Ordering::Relaxed) {
            crate::safety::spawn_from_reusable_on_dedicated(reusable, _worker_id)
        } else {
            RUNTIME_MOCK_INSTANCE.with(|instance| {
                let mut instance = instance.borrow_mut();

                let runtime = instance
                    .as_mut()
                    .expect("Runtime mock instance is not initialized. Did you call `init_runtime_mock`?");

                let task = Arc::new(AsyncTask::new_safety(true, reusable.into_pin(), 0, runtime.sched.clone()));
                let task_ref = TaskRef::new(task);
                runtime.tasks.push_back(task_ref.clone());
                JoinHandle::new(task_ref)
            })
        }
    }
}
