// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use super::task_state::*;
use crate::core::types::*;
use crate::scheduler::scheduler_mt::SchedulerTrait;
use core::{ptr::NonNull, task::Context, task::Waker};
use foundation::cell::UnsafeCell;
use foundation::not_recoverable_error;
use foundation::prelude::*;
use std::future::Future;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;

///
/// Table of pointers to access API of generic `AsyncTask` without a need to know it's type along a way
///
struct TaskVTable {
    ///
    /// Drops one ref count from associated ArcInternal<AsyncTask<T>>
    ///
    drop: unsafe fn(NonNull<TaskHeader>),

    ///
    /// Adds one ref count from associated ArcInternal<AsyncTask<T>>
    ///
    clone: unsafe fn(NonNull<TaskHeader>),

    ///
    /// Reschedules task into runtime
    ///
    schedule: unsafe fn(NonNull<TaskHeader>, TaskRef),

    poll: unsafe fn(this: NonNull<TaskHeader>, ctx: &mut Context) -> TaskPollResult,

    ///
    /// Sets connected waker into task. Return true if setting waker succeeded, otherwise false meaning that either task is finished or cancelled
    ///
    set_join_handle_waker: unsafe fn(NonNull<TaskHeader>, Waker) -> bool,

    ///
    /// Fetches return value from task
    ///
    get_return_val: unsafe fn(NonNull<TaskHeader>, return_storage: *mut u8),

    ///
    /// Aborts task
    ///
    abort: unsafe fn(NonNull<TaskHeader>) -> bool,
}

///
/// Task stage in which we are in
///
pub(crate) enum TaskStage<T, ResultType> {
    Extracted,             // No data available anymore
    InProgress(T),         // Holds "a thing" to execute (could be future, could be fn)
    Completed(ResultType), // Future finished and return value is stored in here
}

///
/// TODO: WIP
///
pub(crate) struct TaskHeader {
    pub(in crate::scheduler) state: TaskState,
    #[allow(dead_code)]
    id: TaskId,

    vtable: &'static TaskVTable, // API entrypoint to typed task
}

impl TaskHeader {
    pub(crate) fn new<T: 'static, AllocatedFuture, SchedulerType>(worker_id: u8) -> Self
    where
        AllocatedFuture: Deref + DerefMut,
        AllocatedFuture::Target: Future<Output = T> + Send + 'static,
        SchedulerType: Deref,
        SchedulerType::Target: SchedulerTrait,
    {
        Self {
            state: TaskState::new(),
            id: TaskId::new(worker_id),
            vtable: create_task_vtable::<T, AllocatedFuture, SchedulerType>(),
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum TaskPollResult {
    Done,
    Notified,
}

///
/// Represents async task with a future. Keep in mind that this can only be accessed via `Task`` trait after construction
///
/// Safety:
///  - has to be repr(C) to make TaskRef work
///
/// Design decision
///    We took a decision for `T` being a `Future Return Type`` and not the Future itself (AllocatedFuture is a somehow dynamically allocated Future). This is because each Future has different size and `AsyncTask` need to be somehow dynamically allocated. Assuming
///    Future is Boxed (dynamically allocated), `AsyncTask` will have predictable sizes which are easier to map into some simple allocators like MemPool. If we some day device to abandon this, there will be not so much change
///    around here or we can even implement new Task class out of current building blocks and it will fit to whole system seamlessly.
#[repr(C)]
pub(crate) struct AsyncTask<T: 'static, AllocatedFuture, SchedulerType>
where
    AllocatedFuture: Deref + DerefMut,
    AllocatedFuture::Target: Future<Output = T> + Send + 'static,
    SchedulerType: Deref,
    SchedulerType::Target: SchedulerTrait,
{
    pub(in crate::scheduler) header: TaskHeader,
    stage: UnsafeCell<TaskStage<Pin<AllocatedFuture>, T>>, // Describe which stage we are in (progress, done, etc)

    handle_waker: UnsafeCell<Option<Waker>>, // Waker for the one that hold the JoinHandle, for now Option, but potentially needs a wrapper for sharing too
    scheduler: SchedulerType,
}

// We protect *Cells by task state, making sure there is no concurrency on those
unsafe impl<T: 'static, AllocatedFuture, SchedulerType> Sync for AsyncTask<T, AllocatedFuture, SchedulerType>
where
    AllocatedFuture: Deref + DerefMut,
    AllocatedFuture::Target: Future<Output = T> + Send + 'static,
    SchedulerType: Deref,
    SchedulerType::Target: SchedulerTrait,
{
}

impl<T, AllocatedFuture, SchedulerType> AsyncTask<T, AllocatedFuture, SchedulerType>
where
    AllocatedFuture: Deref + DerefMut,
    AllocatedFuture::Target: Future<Output = T> + Send + 'static,
    SchedulerType: Deref,
    SchedulerType::Target: SchedulerTrait,
{
    pub(crate) fn new(future: Pin<AllocatedFuture>, worker_id: u8, scheduler: SchedulerType) -> Self {
        Self {
            header: TaskHeader::new::<T, AllocatedFuture, SchedulerType>(worker_id),
            stage: UnsafeCell::new(TaskStage::InProgress(future)),
            handle_waker: UnsafeCell::new(None),
            scheduler,
        }
    }

    pub(crate) fn set_waker(&self, waker: Waker) -> bool {
        unsafe {
            self.handle_waker.with_mut(|ptr| {
                *ptr = Some(waker);
            })
        }

        self.header.state.set_waker() // Safety: makes sure storing waker is not reordered behind this operation
    }

    ///
    /// Mark the task as aborted. There are three cases:
    ///  - task is in idle state, outside of any queue, then it's marked aborted and will never be executed again, possibly it will got it's reference count to 0 before landing in any queue and then it will simply drop
    ///  - task is running, it's marked aborted and will be aborted once after current execution finish, no matter if it still Pending or Ready
    ///  - task is notified (in some queue), then it's get drooped once it's taken from the queue
    ///
    ///  Returns
    ///     - true if the task will never execute again, so for user it's aborted
    ///     - false if it was aborted but it is currently executing, so it's not aborted yet
    ///
    pub(crate) fn abort(&self) -> bool {
        match self.header.state.transition_to_canceled() {
            TransitionToCanceled::Done => {
                // We moved the task to aborted, this means no one ever will access stage anymore, we can do it.
                // Since the abort cannot happen while awaiting on Task (we don't have AbortHandle as of now with would allow remote abort), we don't have any racing on stage due to `get_future_ret`
                self.clear_stage();
                true
            }
            TransitionToCanceled::AlreadyDone => false, // We don't know whether first caller finished with Done or DoneWhileRunning
            TransitionToCanceled::DoneWhileRunning => false,
        }
    }

    pub(crate) fn poll(&self, cx: &mut Context) -> TaskPollResult {
        match self.header.state.transition_to_running() {
            TransitionToRunning::Done => {
                // means we are the only one who could be now running this future
                self.poll_core(cx)
            }
            TransitionToRunning::Aborted => {
                // if we were aborted before, it means someone else cleared up everything and we are simply done here
                TaskPollResult::Done
            }
        }
    }

    ///
    /// Safety: outer caller needs to ensure that the stage is synchronized by task state
    ///
    fn poll_core(&self, ctx: &mut Context) -> TaskPollResult {
        // Once we are here, we are the only one who can be running this future and/or stage itself
        let res = self.stage.with_mut(|ptr| {
            let ref_to_stage = unsafe { &mut *ptr };

            match ref_to_stage {
                TaskStage::Extracted => {
                    not_recoverable_error!("We shall never be in TaskStage::Extracted in poll as we handle that before (task state == completed)")
                }
                TaskStage::InProgress(ref mut future) => {
                    // Lets poll future
                    match future.as_mut().poll(ctx) {
                        std::task::Poll::Ready(ret) => {
                            // Store result, drops the future
                            *ref_to_stage = TaskStage::Completed(ret);
                            None // Finish state transition outside cell access
                        }
                        std::task::Poll::Pending => {
                            match self.header.state.transition_to_idle() {
                                TransitionToIdle::Done => Some(TaskPollResult::Done),
                                TransitionToIdle::Notified => Some(TaskPollResult::Notified),
                                TransitionToIdle::Aborted => {
                                    // We are done, we can simply clear the stage and we don't need to notify via handle_waker as of now as no remote abort supported
                                    self.clear_stage();
                                    Some(TaskPollResult::Done)
                                }
                            }
                        }
                    }
                }
                TaskStage::Completed(_) => {
                    not_recoverable_error!("We shall never be in TaskStage::Completed in poll as we handle that before (task state == completed)")
                }
            }
        });

        res.unwrap_or_else(|| {
            // Finish when future was done, but not under opened writable cell that can cause bugs if someone reorder some instructions, here we are sure
            let status = self.header.state.transition_to_completed();
            match status {
                TransitionToCompleted::Done => {}
                TransitionToCompleted::HadConnectedJoinHandle => {
                    self.handle_waker.with_mut(|ptr: *mut Option<Waker>| match unsafe { (*ptr).take() } {
                        Some(v) => {
                            v.wake_by_ref();
                        }
                        None => not_recoverable_error!("We shall never be here if we have HadConnectedJoinHandle set!"),
                    })
                }
                TransitionToCompleted::Aborted => {
                    // We are done, we can simply clear the stage and we don't need to notify via handle_waker as of now as no remote abort supported
                    // We also do not guarantee ordering on this, but state is correct so no one except us will access stage
                    self.clear_stage();
                }
            }

            TaskPollResult::Done
        })
    }

    ///
    /// SAFETY: Caller needs to make sure this API is called only from single thread at a time.
    ///
    pub fn get_future_ret(&self, storage: &mut Result<T, CommonErrors>) {
        let current_state = self.header.state.get();

        if current_state.is_canceled() {
            *storage = Err(CommonErrors::OperationAborted);

            return;
        }

        if !current_state.is_completed() {
            *storage = Err(CommonErrors::NoData);
            return;
        }

        self.stage.with_mut(|ptr| {
            let ref_to_stage = unsafe { &mut *ptr };
            // Can only be accessed if `get` and above conditions are met, as they form a guard

            let should_take = match ref_to_stage {
                TaskStage::Extracted => false,
                TaskStage::InProgress(_) => false,
                TaskStage::Completed(_) => true,
            };

            let prev = mem::replace(&mut *ref_to_stage, TaskStage::Extracted);

            let result = if should_take {
                if let TaskStage::Completed(v) = prev {
                    Ok(v)
                } else {
                    Err(CommonErrors::AlreadyDone)
                }
            } else {
                Err(CommonErrors::AlreadyDone)
            };

            *storage = result;
        })
    }

    fn schedule(&self, task: TaskRef) {
        // TODO: For now we do simple reschedule (even not in instance, for now not needed, but later will be needed) without:
        // - no spawn into certain place (certain worker)
        // - ...

        match self.header.state.set_notified() {
            TransitionToNotified::Done => {
                // We need to reschedule on our own
                self.scheduler.respawn(task);
            }
            TransitionToNotified::AlreadyNotified => {
                // Do nothing as someone already did it
            }
            TransitionToNotified::Running => {
                // Do nothing was we will be rescheduled by poll itself, still notification was marked
            }
        }
    }

    ///
    /// SAFETY: Caller must ensure that only the caller makes modifications to the stage
    ///
    fn clear_stage(&self) {
        self.stage.with_mut(|ptr| {
            unsafe { *ptr = TaskStage::Extracted };
        })
    }

    /// instance unbounded functions that can still bind a correct T so we can extract real instance from their arg. They forward calls to bounded API
    fn drop_vtable(this: NonNull<TaskHeader>) {
        let instance = this.as_ptr().cast::<AsyncTask<T, AllocatedFuture, SchedulerType>>();
        unsafe { drop(ArcInternal::from_raw(instance)) };
    }

    fn clone_vtable(this: NonNull<TaskHeader>) {
        let instance = this.as_ptr().cast::<AsyncTask<T, AllocatedFuture, SchedulerType>>();
        unsafe { ArcInternal::increment_strong_count(instance) };
    }

    fn schedule_vtable(this: NonNull<TaskHeader>, task: TaskRef) {
        let instance = this.as_ptr().cast::<AsyncTask<T, AllocatedFuture, SchedulerType>>();
        unsafe { (*instance).schedule(task) }
    }

    fn poll_vtable(this: NonNull<TaskHeader>, ctx: &mut Context) -> TaskPollResult {
        let instance = this.as_ptr().cast::<AsyncTask<T, AllocatedFuture, SchedulerType>>();
        unsafe { (*instance).poll(ctx) }
    }

    fn set_join_handle_waker_vtable(this: NonNull<TaskHeader>, waker: Waker) -> bool {
        let instance = this.as_ptr().cast::<AsyncTask<T, AllocatedFuture, SchedulerType>>();
        unsafe { (*instance).set_waker(waker) }
    }

    fn get_return_val_vtable(this: NonNull<TaskHeader>, return_storage: *mut u8) {
        let instance = this.as_ptr().cast::<AsyncTask<T, AllocatedFuture, SchedulerType>>();
        let storage = return_storage.cast::<Result<T, CommonErrors>>();

        unsafe { (*instance).get_future_ret(&mut *storage) }
    }

    fn abort_vtable(this: NonNull<TaskHeader>) -> bool {
        let instance = this.as_ptr().cast::<AsyncTask<T, AllocatedFuture, SchedulerType>>();
        unsafe { (*instance).abort() }
    }
}

///
/// Creates static reference to VTable for specific `T` using `const promotion` of Rust
///
fn create_task_vtable<T: 'static, AllocatedFuture, SchedulerType>() -> &'static TaskVTable
where
    AllocatedFuture: Deref + DerefMut,
    AllocatedFuture::Target: Future<Output = T> + Send + 'static,
    SchedulerType: Deref,
    SchedulerType::Target: SchedulerTrait,
{
    &TaskVTable {
        drop: AsyncTask::<T, AllocatedFuture, SchedulerType>::drop_vtable,
        clone: AsyncTask::<T, AllocatedFuture, SchedulerType>::clone_vtable,
        schedule: AsyncTask::<T, AllocatedFuture, SchedulerType>::schedule_vtable,
        poll: AsyncTask::<T, AllocatedFuture, SchedulerType>::poll_vtable,
        set_join_handle_waker: AsyncTask::<T, AllocatedFuture, SchedulerType>::set_join_handle_waker_vtable,
        get_return_val: AsyncTask::<T, AllocatedFuture, SchedulerType>::get_return_val_vtable,
        abort: AsyncTask::<T, AllocatedFuture, SchedulerType>::abort_vtable,
    }
}

///
/// Task reference type that can track real `AsyncTask<T>` without knowing it's type. It make sure the ArcInternal<> holding a task has correct ref count
///
pub(crate) struct TaskRef {
    header: NonNull<TaskHeader>,
}

unsafe impl Send for TaskRef {}

impl TaskRef {
    pub(crate) fn new<Task>(arc_task: ArcInternal<Task>) -> Self {
        // Take raw pointer without any borrows
        let val = NonNull::new(ArcInternal::as_ptr(&arc_task) as *mut TaskHeader).unwrap();
        std::mem::forget(arc_task); // we took over ref count from arg into ourself
        Self { header: val }
    }

    pub(crate) fn clone(&self) -> Self {
        unsafe {
            (self.header.as_ref().vtable.clone)(self.header);
        }

        Self { header: self.header }
    }

    ///
    /// Release pointer and does not decrement ref count
    ///
    pub(crate) fn into_raw(this: TaskRef) -> *const TaskHeader {
        let ptr = this.header.as_ptr();
        std::mem::forget(this);
        ptr
    }

    ///
    /// Binds pointer and does not increment ref count. This call has to be paired with `into_raw` - same as ArcInternal<> calls.
    ///
    pub(crate) unsafe fn from_raw(ptr: *const TaskHeader) -> TaskRef {
        Self {
            header: NonNull::new(ptr as *mut TaskHeader).unwrap(),
        }
    }

    ///
    /// Proxy to `AsyncTask<T>::schedule`
    ///
    pub(crate) fn schedule(&self) {
        unsafe {
            (self.header.as_ref().vtable.schedule)(self.header, self.clone());
        }
    }

    ///
    /// Proxy to `AsyncTask<T>::poll`
    ///
    pub(crate) fn poll(&self, ctx: &mut Context) -> TaskPollResult {
        unsafe { (self.header.as_ref().vtable.poll)(self.header, ctx) }
    }

    ///
    /// Sets waker for join handle task and return true if this was set, otherwise false, meaning task is already done/aborted
    ///
    pub(crate) fn set_join_handle_waker(&self, waker: Waker) -> bool {
        unsafe { (self.header.as_ref().vtable.set_join_handle_waker)(self.header, waker) }
    }

    ///
    /// Returns the current state of task, erasing underlying types.
    /// Safety:
    ///     - Caller has to ensure that `return_storage` is valid by whole time of this fn call and also it's mutable
    ///     - Caller needs to ensure that this is called only from ONE thread at a time. Due to this, this API shall be only used from JoinHandle
    ///         than cannot be copied or cloned which makes this assumption valid.
    ///
    pub(crate) fn get_return_val(&self, return_storage: *mut u8) {
        unsafe { (self.header.as_ref().vtable.get_return_val)(self.header, return_storage) }
    }

    pub(crate) fn abort(&self) -> bool {
        unsafe { (self.header.as_ref().vtable.abort)(self.header) }
    }
}

impl Drop for TaskRef {
    fn drop(&mut self) {
        unsafe {
            (self.header.as_ref().vtable.drop)(self.header);
        }
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {

    use std::{
        future::Future,
        ops::{Deref, DerefMut},
    };

    use crate::{
        core::types::{box_future, ArcInternal},
        scheduler::{
            execution_engine::ExecutionEngineBuilder,
            scheduler_mt::SchedulerTrait,
            task::async_task::{TaskId, TaskRef},
        },
    };

    use super::AsyncTask;

    impl<T, AllocatedFuture, SchedulerType> AsyncTask<T, AllocatedFuture, SchedulerType>
    where
        AllocatedFuture: Deref + DerefMut,
        AllocatedFuture::Target: Future<Output = T> + Send + 'static,
        SchedulerType: Deref,
        SchedulerType::Target: SchedulerTrait,
    {
        pub fn id(&self) -> TaskId {
            self.header.id
        }
    }

    async fn dummy() {
        println!("test123");
    }

    async fn dummy_ret() -> Result<bool, String> {
        println!("test1234");

        Ok(true)
    }

    #[test]
    fn test_task_ctor() {
        // This code only proves to compile different Future constructs
        {
            let boxed = box_future(dummy());
            let scheduler = ExecutionEngineBuilder::new().build().get_async_scheduler();
            let task = ArcInternal::new(AsyncTask::new(boxed, 1, scheduler));
            let id = task.id();
            assert_eq!(id.0 & 0xFF, 1); // Test some internals
        }

        {
            let boxed = box_future(dummy_ret());
            let scheduler = ExecutionEngineBuilder::new().build().get_async_scheduler();
            let task = AsyncTask::new(boxed, 2, scheduler);
            let id = task.id();
            assert_eq!(id.0 & 0xFF, 2); // Test some internals
            drop(task);
        }

        {
            let boxed = box_future(async {
                println!("some test");
            });
            let scheduler = ExecutionEngineBuilder::new().build().get_async_scheduler();
            let task = AsyncTask::new(boxed, 2, scheduler);
            let id = task.id();
            assert_eq!(id.0 & 0xFF, 2); // Test some internals

            drop(task);
        }

        {
            let v = vec![0, 1, 2];

            let boxed = box_future(async move {
                println!("some test 1 {:?}", v);
            });
            let scheduler = ExecutionEngineBuilder::new().build().get_async_scheduler();
            let task = AsyncTask::new(boxed, 2, scheduler);
            let id = task.id();
            assert_eq!(id.0 & 0xFF, 2); // Test some internals

            drop(task);
        }
    }

    #[test]
    fn test_taskref_counting() {
        let boxed = box_future(dummy());
        let scheduler = ExecutionEngineBuilder::new().build().get_async_scheduler();
        let task = ArcInternal::new(AsyncTask::new(boxed, 1, scheduler));
        let id = task.id();
        assert_eq!(id.0 & 0xFF, 1); // Test some internals

        let task_ref = TaskRef::new(task.clone());

        assert_eq!(ArcInternal::strong_count(&task), 2);
        drop(task_ref);

        assert_eq!(ArcInternal::strong_count(&task), 1);

        {
            let task_ref = TaskRef::new(task.clone());
            let task_ref2 = task_ref.clone();
            let _task_ref3 = task_ref2.clone();

            assert_eq!(ArcInternal::strong_count(&task), 4);
        }

        assert_eq!(ArcInternal::strong_count(&task), 1);
    }

    // #[test]
    // fn test_taskref_calling_task_api() {
    //     let boxed = box_future(dummy());
    //     let task = ArcInternal::new(AsyncTask::new(boxed, 1));
    //     let id = task.id();
    //     assert_eq!(id.0 & 0xFF, 1); // Test some internals

    //     let task_ref = TaskRef::new(task.clone());

    //     task_ref.schedule(); TODO: Fix a need for context in UT
    // }

    #[test]
    fn task_taskref_can_be_send_to_another_thread() {
        let boxed = box_future(dummy());
        let scheduler = ExecutionEngineBuilder::new().build().get_async_scheduler();
        let task = ArcInternal::new(AsyncTask::new(boxed, 1, scheduler));
        let id = task.id();
        assert_eq!(id.0 & 0xFF, 1); // Test some internals

        let task_ref = TaskRef::new(task.clone());

        let handle = {
            let cln: TaskRef = task_ref.clone();
            assert_eq!(ArcInternal::strong_count(&task), 3);
            std::thread::spawn(|| {
                drop(cln);
            })
        };

        handle.join().unwrap();
        assert_eq!(ArcInternal::strong_count(&task), 2);
    }
}

#[cfg(test)]
#[cfg(loom)]
mod tests {
    use std::task::{RawWaker, RawWakerVTable};

    use super::*;
    use foundation::sync::foundation_atomic::FoundationAtomicU16;
    use loom::model::Builder;
    use std::sync::atomic::Ordering::Relaxed;

    use crate::{
        core::types::{box_future, ArcInternal},
        scheduler::scheduler_mt::SchedulerTrait,
    };

    async fn dummy() -> u32 {
        0
    }

    async fn dummy_pending() -> u32 {
        NeverReady {}.await;
        0
    }

    pub struct NeverReady;
    impl Future for NeverReady {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> std::task::Poll<Self::Output> {
            std::task::Poll::Pending
        }
    }

    fn noop_waker() -> Waker {
        fn clone(_: *const ()) -> RawWaker {
            noop_raw_waker()
        }

        fn wake(_: *const ()) {}

        fn wake_by_ref(_: *const ()) {}

        fn drop(_: *const ()) {}

        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

        fn noop_raw_waker() -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }

        unsafe { Waker::from_raw(noop_raw_waker()) }
    }

    pub struct SchedulerMock {
        pub spawn_count: FoundationAtomicU16,
    }

    impl SchedulerTrait for SchedulerMock {
        fn respawn(&self, _: TaskRef) {
            self.spawn_count.fetch_add(1, Relaxed);
        }
    }

    #[test]
    fn task_reschedule_is_never_done_twice_when_no_poll_happens() {
        loom::model(|| {
            let boxed = box_future(dummy());
            let scheduler = ArcInternal::new(SchedulerMock {
                spawn_count: FoundationAtomicU16::new(0),
            });

            let task = ArcInternal::new(AsyncTask::new(boxed, 1, scheduler.clone()));
            let handle1;
            {
                let task_ref = TaskRef::new(task.clone());
                handle1 = loom::thread::spawn(move || {
                    task_ref.schedule();
                });
            }

            let handle2;
            {
                let task_ref = TaskRef::new(task.clone());
                handle2 = loom::thread::spawn(move || {
                    task_ref.schedule();
                });
            }

            let handle3;
            {
                let task_ref = TaskRef::new(task.clone());
                handle3 = loom::thread::spawn(move || {
                    task_ref.schedule();
                });
            }

            TaskRef::new(task.clone()).schedule();

            handle2.join().unwrap();
            handle3.join().unwrap();
            handle1.join().unwrap();

            assert_eq!(1, scheduler.spawn_count.load(Relaxed));
        });
    }

    #[test]
    fn task_reschedule_never_happens_when_running() {
        let mut builder = Builder::new();
        builder.preemption_bound = Some(3);

        builder.check(|| {
            let boxed = box_future(dummy_pending());
            let scheduler = ArcInternal::new(SchedulerMock {
                spawn_count: FoundationAtomicU16::new(0),
            });
            let task = ArcInternal::new(AsyncTask::new(boxed, 1, scheduler.clone()));

            let handle1;
            {
                let task_ref = TaskRef::new(task.clone());
                handle1 = loom::thread::spawn(move || {
                    task_ref.schedule();
                });
            }

            let handle2;
            {
                let task_ref = TaskRef::new(task.clone());
                handle2 = loom::thread::spawn(move || {
                    task_ref.schedule();
                });
            }

            let handle3;
            {
                let task_ref = TaskRef::new(task.clone());
                handle3 = loom::thread::spawn(move || {
                    task_ref.schedule();
                });
            }

            let waker = noop_waker();
            let mut cx = Context::from_waker(&waker);

            let ret = task.poll(&mut cx);

            handle2.join().unwrap();
            handle1.join().unwrap();
            handle3.join().unwrap();

            match ret {
                TaskPollResult::Done => {
                    let val = scheduler.spawn_count.load(Relaxed);
                    assert!(val == 1 || val == 2); // Either the schedule happen before pool or after. Loom will explore all interleaving
                }
                TaskPollResult::Notified => {
                    let val = scheduler.spawn_count.load(Relaxed);

                    assert!(val == 0 || val == 1); // 0 - one threads calls when we are in running, 1 - one threads call before we running
                }
            }
        });
    }
}
