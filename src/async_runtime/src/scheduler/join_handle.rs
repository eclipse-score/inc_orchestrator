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
use foundation::prelude::*;
use foundation::{not_recoverable_error, prelude::CommonErrors};

use crate::{
    futures::{FutureInternalReturn, FutureState},
    TaskRef,
};
use core::task::Poll;
use std::{future::Future, marker::PhantomData};

pub type JoinResult<T> = Result<T, CommonErrors>;

pub struct JoinHandle<T: Send + 'static> {
    state: FutureState,
    for_task: TaskRef,
    _phantom: PhantomData<T>,
}

impl<T: Send + 'static> JoinHandle<T> {
    pub(crate) fn new(task: TaskRef) -> Self {
        Self {
            state: FutureState::New,
            for_task: task,
            _phantom: PhantomData,
        }
    }
}

impl<T: Send + 'static> JoinHandle<T> {
    ///
    /// Aborts the task. This does `cooperative` abort, meaning that if running task does not return control to runtime, it cannot be aborted.
    /// Currently we don't support `remote` aborts.
    ///
    /// Returns
    ///     - true when the connected future was dropped in place and user can consider task as done
    ///     - false when the connected future was not YET dropped and drop will be done ASAP (ie. task is running yet)
    ///
    pub fn abort(&self) -> bool {
        self.for_task.abort()
    }
}

impl<T: Send + 'static> Future for JoinHandle<T> {
    type Output = JoinResult<T>;

    ///
    /// Pools a future.
    ///
    /// Returns
    ///     - T if there is a result available
    ///     - `CommonErrors::OperationAborted` when abort() was called
    ///     - `CommonErrors::Panicked` if future panicked. Currently not yet supported!
    ///
    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let res: FutureInternalReturn<JoinResult<T>> = match self.state {
            FutureState::New => {
                let waker = cx.waker();

                // Set the waker, return values tells what have happen and took care about correct synchronization
                let was_set = self.for_task.set_join_handle_waker(waker.clone());

                if was_set {
                    FutureInternalReturn::default()
                } else {
                    let mut ret: Result<T, CommonErrors> = Err(CommonErrors::NoData);
                    let ret_as_ptr = &mut ret as *mut _;
                    self.for_task.get_return_val(ret_as_ptr as *mut u8);

                    match ret {
                        Ok(v) => FutureInternalReturn::ready(Ok(v)),
                        Err(CommonErrors::OperationAborted) => FutureInternalReturn::ready(Err(CommonErrors::OperationAborted)),
                        Err(e) => {
                            not_recoverable_error!(with e, "There has been an error in a task that is not recoverable ({})!");
                        }
                    }
                }
            }
            FutureState::Polled => {
                // Safety belows forms AqrRel so waker is really written before we do marking
                let mut ret: Result<T, CommonErrors> = Err(CommonErrors::NoData);
                let ret_as_ptr = &mut ret as *mut _;
                self.for_task.get_return_val(ret_as_ptr as *mut u8);

                match ret {
                    Ok(v) => FutureInternalReturn::ready(Ok(v)),
                    Err(CommonErrors::NoData) => FutureInternalReturn::polled(),
                    Err(CommonErrors::OperationAborted) => FutureInternalReturn::ready(Err(CommonErrors::OperationAborted)),
                    Err(e) => {
                        not_recoverable_error!(with e, "There has been an error in a task that is not recoverable ({})!");
                    }
                }
            }
            FutureState::Finished => {
                not_recoverable_error!("Future polled after it finished!");
            }
        };

        self.get_mut().state.assign_and_propagate(res)
    }
}

// We don't depend on any states that cannot be unpinned since we keep all inside itself
impl<T: Send + 'static> Unpin for JoinHandle<T> {}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use std::task::Context;

    use crate::{
        core::types::{box_future, ArcInternal},
        AsyncTask,
    };

    use super::*;
    use crate::testing::*;

    use testing::prelude::*;

    #[test]
    fn test_join_handler_ready_task_get_correct_result_to_handle() {
        let scheduler = create_mock_scheduler();

        {
            // Data is present after first poll of join handle
            let task = ArcInternal::new(AsyncTask::new(box_future(test_function::<u32>()), 1, scheduler.clone()));

            let handle = JoinHandle::<u32>::new(TaskRef::new(task.clone()));

            let mut poller = TestingFuturePoller::new(handle);

            let mut res = poller.poll();
            assert_eq!(res, std::task::Poll::Pending);

            {
                let waker = noop_waker();

                let mut cx = Context::from_waker(&waker);
                task.poll(&mut cx);
            }

            res = poller.poll();
            assert_eq!(res, std::task::Poll::Ready(Ok(0)));
        }

        {
            // Data is present before first poll of join handle
            let task = ArcInternal::new(AsyncTask::new(box_future(test_function_ret::<u32>(1234)), 1, scheduler.clone()));

            let handle = JoinHandle::<u32>::new(TaskRef::new(task.clone()));

            let mut poller = TestingFuturePoller::new(handle);

            {
                let waker = noop_waker();

                let mut cx = Context::from_waker(&waker);
                task.poll(&mut cx);
            }

            assert_eq!(poller.poll(), std::task::Poll::Ready(Ok(1234)));
        }
    }

    #[test]
    fn test_join_handler_aborted_task_produce_handle_abort_result() {
        let scheduler = create_mock_scheduler();

        {
            let task = ArcInternal::new(AsyncTask::new(box_future(test_function_ret::<u32>(1234)), 1, scheduler.clone()));

            let handle = JoinHandle::<u32>::new(TaskRef::new(task.clone()));

            let mut poller = TestingFuturePoller::new(handle);

            let mut res = poller.poll();
            assert_eq!(res, std::task::Poll::Pending);

            assert!(task.abort());

            res = poller.poll();
            assert_eq!(res, std::task::Poll::Ready(Err(CommonErrors::OperationAborted)));
        }

        {
            let task = ArcInternal::new(AsyncTask::new(box_future(test_function::<u32>()), 1, scheduler.clone()));
            let handle = JoinHandle::<u32>::new(TaskRef::new(task.clone()));
            let mut poller = TestingFuturePoller::new(handle);

            assert!(task.abort());
            assert_eq!(poller.poll(), std::task::Poll::Ready(Err(CommonErrors::OperationAborted)));
        }
    }

    #[test]
    #[should_panic]
    fn test_join_handler_panics_when_fetched_data_and_repolled() {
        let scheduler = create_mock_scheduler();

        {
            // Data is present before first poll of join handle
            let task = ArcInternal::new(AsyncTask::new(box_future(test_function::<u32>()), 1, scheduler.clone()));

            let handle = JoinHandle::<u32>::new(TaskRef::new(task.clone()));

            let mut poller = TestingFuturePoller::new(handle);

            {
                let waker = noop_waker();

                let mut cx = Context::from_waker(&waker);
                task.poll(&mut cx);
            }

            assert_eq!(poller.poll(), std::task::Poll::Ready(Ok(0)));

            let _ = poller.poll(); // this shall fail
        }
    }

    #[test]
    fn test_join_handler_is_waked_when_task_done() {
        let scheduler = create_mock_scheduler();

        {
            // Data is present before first poll of join handle
            let task = ArcInternal::new(AsyncTask::new(box_future(test_function::<u32>()), 1, scheduler.clone()));

            let handle = JoinHandle::<u32>::new(TaskRef::new(task.clone()));

            let mut poller = TestingFuturePoller::new(handle);

            let waker_mock = TrackableWaker::new();
            let waker = waker_mock.get_waker();
            let _ = poller.poll_with_waker(&waker);

            {
                let waker = noop_waker();
                let mut cx = Context::from_waker(&waker);
                task.poll(&mut cx); // task done
            }

            assert!(waker_mock.was_waked());
            assert_eq!(poller.poll(), std::task::Poll::Ready(Ok(0)));
        }
    }
}

#[cfg(test)]
#[cfg(loom)]
mod tests {
    use std::task::Context;

    use crate::{
        core::types::{box_future, ArcInternal},
        AsyncTask,
    };

    use super::*;
    use crate::testing::*;

    use testing::prelude::*;

    use loom::model::Builder;

    #[test]
    fn test_join_handler_mt_get_result() {
        let builder = Builder::new();

        builder.check(|| {
            let scheduler = create_mock_scheduler();

            {
                // Data is present after first poll of join handle
                let task = ArcInternal::new(AsyncTask::new(box_future(test_function_ret::<u32>(1234)), 1, scheduler.clone()));

                let handle = JoinHandle::<u32>::new(TaskRef::new(task.clone()));

                let mut poller = TestingFuturePoller::new(handle);

                let handle = loom::thread::spawn(move || {
                    let waker = noop_waker();

                    let mut cx = Context::from_waker(&waker);
                    task.poll(&mut cx);
                });

                let waker_mock = TrackableWaker::new();
                let waker = waker_mock.get_waker();
                let mut was_pending = false;

                loop {
                    match poller.poll_with_waker(&waker) {
                        Poll::Ready(v) => {
                            assert_eq!(v, Ok(1234));

                            if was_pending {
                                assert!(waker_mock.was_waked());
                            }

                            break;
                        }
                        Poll::Pending => {
                            was_pending = true;
                        }
                    }
                    loom::hint::spin_loop();
                }

                let _ = handle.join();
            }
        });
    }

    #[test]
    fn test_join_handler_mt_abort_and_poll() {
        let builder = Builder::new();

        builder.check(|| {
            let scheduler = create_mock_scheduler();

            {
                // Data is present after first poll of join handle
                let task = ArcInternal::new(AsyncTask::new(box_future(test_function_ret::<u32>(1234)), 1, scheduler.clone()));

                let join_handle = JoinHandle::<u32>::new(TaskRef::new(task.clone()));

                let handle = loom::thread::spawn(move || {
                    let waker = noop_waker();

                    let mut cx = Context::from_waker(&waker);
                    task.poll(&mut cx);
                });

                let abort_res = join_handle.abort();

                let mut poller = TestingFuturePoller::new(join_handle);

                let waker_mock = TrackableWaker::new();
                let waker = waker_mock.get_waker();

                let res = loop {
                    match poller.poll_with_waker(&waker) {
                        Poll::Ready(v) => {
                            break v;
                        }
                        Poll::Pending => {}
                    }
                    loom::hint::spin_loop();
                };

                // Since loom permutate options on thread, if abort returned true, means there must be OperationAborted, otherwise it may be Ready or aborted depending on which stage was poll
                if abort_res {
                    assert_eq!(res, Err(CommonErrors::OperationAborted));
                    assert!(!waker_mock.was_waked());
                } else {
                    match res {
                        Ok(v) => {
                            assert_eq!(v, 1234);
                            assert!(!waker_mock.was_waked());
                        }
                        Err(err) => {
                            assert_eq!(err, CommonErrors::OperationAborted);
                            assert!(!waker_mock.was_waked());
                        }
                    }
                }

                let _ = handle.join();
            }
        });
    }
}
