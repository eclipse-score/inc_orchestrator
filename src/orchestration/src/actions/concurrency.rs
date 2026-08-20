// *******************************************************************************
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache License Version 2.0 which is available at
// <https://www.apache.org/licenses/LICENSE-2.0>
//
// SPDX-License-Identifier: Apache-2.0
// *******************************************************************************

use super::action::{ActionBaseMeta, ActionMeta, ActionResult, ActionTrait, ReusableBoxFutureResult};
use crate::actions::action::ActionExecError;
use crate::api::design::Design;
use crate::common::tag::Tag;
use ::core::future::Future;
use ::core::pin::Pin;
use ::core::task::{Context, Poll};
use kyron::futures::reusable_box_future::ReusableBoxFuturePool;
use kyron::futures::{FutureInternalReturn, FutureState};
#[cfg(any(test, feature = "runtime-api-mock"))]
use kyron::testing::mock::*;
#[cfg(not(any(test, feature = "runtime-api-mock")))]
use kyron::*;
use kyron_foundation::containers::growable_vec::GrowableVec;
use kyron_foundation::containers::reusable_objects::ReusableObject;
use kyron_foundation::containers::reusable_vec_pool::ReusableVecPool;
use kyron_foundation::not_recoverable_error;
use kyron_foundation::prelude::vector_extension::VectorExtension;
use kyron_foundation::prelude::*;

/// Builder for constructing a concurrency group of actions to be executed concurrently.
/// Allows adding multiple branches (actions) and finalizing into a [`Concurrency`] object.
/// Requires at least one branch to be added before building.
pub struct ConcurrencyBuilder {
    actions: Option<GrowableVec<Box<dyn ActionTrait>>>,
}

/// Final concurrency object, ready for execution.
/// The concurrency object is reusable and can be executed multiple times.
/// Holds the actions to be executed concurrently and manages their execution and result collection.
/// All actions are spawned as tasks and their results are awaited concurrently.
/// The result of the concurrency execution is either `Ok(())` if all branches succeed,
/// or an `ActionExecError` if any branch fails. The error returned is the last failing branch's error in the registration order of concurrency.
/// If any branch fails, the other branches are still awaited to completion (without aborting them).
pub struct Concurrency {
    base: ActionBaseMeta,
    actions: Vec<Box<dyn ActionTrait>>,
    futures_vec_pool: ReusableVecPool<ActionMeta>,
}

impl ConcurrencyBuilder {
    /// Create a new concurrency builder.
    pub fn new() -> Self {
        Self { actions: None }
    }

    /// Add a new branch (concurrent action).
    /// Returns a mutable reference to self for chaining.
    pub fn with_branch(&mut self, action: Box<dyn ActionTrait>) -> &mut Self {
        self.actions.get_or_insert(GrowableVec::new(2)).push(action);
        self
    }

    /// Finalize and return the concurrency object ready for execution.
    ///
    /// # Panics
    /// Panics if no branch is added.
    pub fn build(&mut self, design: &Design) -> Box<Concurrency> {
        let mut actions = self.actions.take().expect("Concurrency requires at least one branch.");
        actions.lock();
        let length = actions.len();

        Box::new(Concurrency {
            base: ActionBaseMeta {
                tag: super::action::next_unique_action_tag("orch::internal::concurrency"),
                reusable_future_pool: Concurrency::create_reusable_future_pool(
                    design.config.max_concurrent_action_executions,
                ),
            },
            actions: actions.into(),
            futures_vec_pool: ReusableVecPool::<ActionMeta>::new(
                design.config.max_concurrent_action_executions,
                |_| Vec::new_in_global(length),
            ),
        })
    }
}

impl Default for ConcurrencyBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Concurrency {
    /// Internal async execution logic for concurrent actions.
    ///
    /// Spawns all actions as tasks, waits for all to complete.
    async fn execute_impl(meta: Tag, mut futures_vec: ReusableObject<Vec<ActionMeta>>) -> ActionResult {
        for fut in futures_vec.iter_mut() {
            if let Some(future) = fut.take_future() {
                fut.assign_handle(safety::spawn_from_reusable(future));
            }
        }

        tracing_adapter!(concurrent = ?meta, "Before joining branches");

        let joined = ConcurrencyJoin::new(futures_vec);
        let res = joined.await;

        tracing_adapter!(concurrent = ?meta, ?res, "After joining branches");
        res
    }

    /// Creates a reusable future pool
    fn create_reusable_future_pool(pool_size: usize) -> ReusableBoxFuturePool<ActionResult> {
        let mut vec_pool = ReusableVecPool::<ActionMeta>::new(pool_size, |_| Vec::new_in_global(1));
        let vec = vec_pool.next_object().unwrap();
        ReusableBoxFuturePool::<ActionResult>::for_value(pool_size, Self::execute_impl("dummy".into(), vec))
    }
}

impl ActionTrait for Concurrency {
    /// Attempts to execute all branches concurrently, returning a reusable boxed future.
    fn try_execute(&mut self) -> ReusableBoxFutureResult {
        let mut futures_vec = self.futures_vec_pool.next_object()?;

        for action in self.actions.iter_mut() {
            // Each action is executed and its future is collected for concurrent execution.
            futures_vec.push(ActionMeta::new(action.try_execute()?));
        }

        self.base
            .reusable_future_pool
            .next(Self::execute_impl(self.base.tag, futures_vec))
    }

    fn name(&self) -> &'static str {
        "Concurrency"
    }

    fn dbg_fmt(&self, nest: usize, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
        let indent = " ".repeat(nest);
        writeln!(f, "{}|-{} - {:?}", indent, self.name(), self.base)?;
        self.actions.iter().try_for_each(|x| {
            writeln!(f, "{} |branch", indent)?;
            x.dbg_fmt(nest + 1, f)
        })
    }
}

/// Future that waits for multiple [`JoinHandle`]s to complete.
/// Returns `Ready` once all are done. Uses FutureState to track polling state.
struct ConcurrencyJoin {
    handles: ReusableObject<Vec<ActionMeta>>,
    state: FutureState,
    action_execution_result: (usize, ActionResult),
}

impl ConcurrencyJoin {
    /// Create a new `ConcurrencyJoin` for the given handles.
    fn new(handles: ReusableObject<Vec<ActionMeta>>) -> Self {
        Self {
            handles,
            state: FutureState::New,
            action_execution_result: (0, ActionResult::Ok(())),
        }
    }

    /// Handles polling all join handles. Returns Ready if all are done, Pending otherwise.
    /// Returns the error of last failing branch in case of any failure,
    /// or `Ok(())` if all branches succeed.
    fn join_result(&mut self, cx: &mut Context<'_>) -> Poll<ActionResult> {
        let result = match self.state {
            FutureState::New | FutureState::Polled => {
                // Poll all handles and collect results.
                let mut is_done = true;

                for hnd in self.handles.iter_mut().enumerate() {
                    match hnd.1 {
                        ActionMeta::Handle(handle) => {
                            let res = Pin::new(handle).poll(cx);
                            match res {
                                Poll::Ready(action_result) => {
                                    hnd.1.clear(); // Clear the handle after polling
                                    let execution_result = match action_result {
                                        Ok(Ok(_)) => continue,
                                        Ok(Err(err)) => Err(err),

                                        // This a JoinResult error, not the future error
                                        Err(_) => Err(ActionExecError::Internal),
                                    };

                                    // Store the error of the last failed branch in the registration order of concurrency.
                                    if execution_result.is_err() && hnd.0 >= self.action_execution_result.0 {
                                        self.action_execution_result = (hnd.0, execution_result);
                                    }
                                },
                                Poll::Pending => {
                                    is_done = false; // At least one handle is still pending
                                    if self.state == FutureState::Polled {
                                        // Exit loop, no need to poll others now since aborting is not required
                                        break;
                                    }
                                },
                            }
                        },
                        ActionMeta::Future(_) => {
                            not_recoverable_error!("Join handle not available for the spawned future!");
                        },
                        ActionMeta::Empty => {
                            if self.state == FutureState::Polled {
                                continue; // Already polled.
                            }
                            not_recoverable_error!("Join handle not available for the spawned future!");
                        },
                    }
                }

                if is_done {
                    FutureInternalReturn::ready(self.action_execution_result.1)
                } else {
                    FutureInternalReturn::polled()
                }
            },
            FutureState::Finished => {
                not_recoverable_error!("Future polled after it finished!")
            },
        };
        self.state.assign_and_propagate(result)
    }
}

impl Future for ConcurrencyJoin {
    type Output = ActionResult;

    /// Polls the `ConcurrencyJoin` future.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Poll the join result and return the appropriate Poll state.
        self.join_result(cx)
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use crate::common::DesignConfig;
    use crate::testing::MockActionBuilder;
    use crate::testing::OrchTestingPoller;
    use ::core::task::Poll;
    use kyron::testing::mock;
    use kyron_testing_macros::ensure_clear_mock_runtime;

    #[test]
    fn concurrency_builder_using_new() {
        let mock1 = MockActionBuilder::<()>::new().build();
        let mock2 = MockActionBuilder::<()>::new().build();
        // Create a concurrency builder using new() and add two branches.
        let design = Design::new("Design".into(), DesignConfig::default());
        let mut concurrency_builder = ConcurrencyBuilder::new();
        concurrency_builder
            .with_branch(Box::new(mock1))
            .with_branch(Box::new(mock2));
        let concurrency = concurrency_builder.build(&design);
        assert_eq!(concurrency.actions.len(), 2);
        assert_eq!(concurrency.name(), "Concurrency");
    }

    #[test]
    fn concurrency_builder_using_default() {
        let mock1 = MockActionBuilder::<()>::new().build();
        // Create a concurrency builder using default() and add one branch.
        let design = Design::new("Design".into(), DesignConfig::default());
        let mut concurrency_builder = ConcurrencyBuilder::default();
        concurrency_builder.with_branch(Box::new(mock1));
        let concurrency = concurrency_builder.build(&design);
        assert_eq!(concurrency.actions.len(), 1);
        assert_eq!(concurrency.name(), "Concurrency");
    }

    #[test]
    #[should_panic(expected = "Concurrency requires at least one branch.")]
    fn concurrency_builder_panics_with_no_branch() {
        let design = Design::new("Design".into(), DesignConfig::default());
        let mut concurrency_builder = ConcurrencyBuilder::new();
        let _ = concurrency_builder.build(&design);
    }

    #[test]
    #[ensure_clear_mock_runtime]
    fn concurrency_execute_ok_actions() {
        let mock1 = MockActionBuilder::<()>::new().will_once_return(Ok(())).build();
        let mock2 = MockActionBuilder::<()>::new().will_once_return(Ok(())).build();

        let design = Design::new("Design".into(), DesignConfig::default());
        let mut concurrency_builder = ConcurrencyBuilder::new();
        concurrency_builder
            .with_branch(Box::new(mock1))
            .with_branch(Box::new(mock2));
        let mut concurrency = concurrency_builder.build(&design);

        let mut poller = OrchTestingPoller::new(concurrency.try_execute().unwrap());
        // Call the poll function to spawn all the actions to execute concurrently and wait for them to complete.
        // The mock runtime will handle the execution of these actions.
        let _ = poller.poll();

        // Use the mock runtime to execute all spawned concurrent actions.
        assert!(mock::runtime::remaining_tasks() > 0);
        mock::runtime::step();
        assert_eq!(mock::runtime::remaining_tasks(), 0);

        // Get the result
        let result = poller.poll();
        assert_eq!(result, Poll::Ready(Ok(())));
    }

    #[test]
    #[ensure_clear_mock_runtime]
    fn concurrency_execute_err_action() {
        let mock1 = MockActionBuilder::<()>::new()
            .will_once_return(Err(ActionExecError::NonRecoverableFailure))
            .build();

        let design = Design::new("Design".into(), DesignConfig::default());
        let mut concurrency_builder = ConcurrencyBuilder::new();
        concurrency_builder.with_branch(Box::new(mock1));
        let mut concurrency = concurrency_builder.build(&design);

        let mut poller = OrchTestingPoller::new(concurrency.try_execute().unwrap());
        // Call the poll function to spawn all the actions to execute concurrently and wait for them to complete.
        // The mock runtime will handle the execution of these actions.
        let _ = poller.poll();

        // Use the mock runtime to execute all spawned concurrent actions.
        assert!(mock::runtime::remaining_tasks() > 0);
        mock::runtime::step();
        assert_eq!(mock::runtime::remaining_tasks(), 0);

        // Get the result
        let result = poller.poll();
        assert_eq!(result, Poll::Ready(Err(ActionExecError::NonRecoverableFailure)));
    }

    #[test]
    #[ensure_clear_mock_runtime]
    fn concurrency_execute_ok_and_err_actions() {
        let mock1 = MockActionBuilder::<()>::new().will_once_return(Ok(())).build();
        let mock2 = MockActionBuilder::<()>::new()
            .will_once_return(Err(ActionExecError::Internal))
            .build();
        let mock3 = MockActionBuilder::<()>::new().will_once_return(Ok(())).build();
        let mock4 = MockActionBuilder::<()>::new()
            .will_once_return(Err(ActionExecError::NonRecoverableFailure))
            .build();
        let mock5 = MockActionBuilder::<()>::new().will_once_return(Ok(())).build();

        let design = Design::new("Design".into(), DesignConfig::default());
        let mut concurrency_builder = ConcurrencyBuilder::new();
        concurrency_builder
            .with_branch(Box::new(mock1))
            .with_branch(Box::new(mock2))
            .with_branch(Box::new(mock3))
            .with_branch(Box::new(mock4))
            .with_branch(Box::new(mock5));
        let mut concurrency = concurrency_builder.build(&design);

        let mut poller = OrchTestingPoller::new(concurrency.try_execute().unwrap());
        // Call the poll function to spawn all the actions to execute concurrently and wait for them to complete.
        // The mock runtime will handle the execution of these actions.
        let _ = poller.poll();

        // Use the mock runtime to execute all spawned concurrent actions.
        assert!(mock::runtime::remaining_tasks() > 0);
        mock::runtime::step();
        assert_eq!(mock::runtime::remaining_tasks(), 0);

        // Get the result
        let result = poller.poll();
        assert_eq!(result, Poll::Ready(Err(ActionExecError::NonRecoverableFailure)));
    }

    #[test]
    #[ensure_clear_mock_runtime]
    fn concurrency_polled_multiple_times_before_runtime_advances() {
        let mock1 = MockActionBuilder::<()>::new().will_once_return(Ok(())).build();
        let mock2 = MockActionBuilder::<()>::new().will_once_return(Ok(())).build();

        let design = Design::new("Design".into(), DesignConfig::default());
        let mut concurrency_builder = ConcurrencyBuilder::new();
        concurrency_builder
            .with_branch(Box::new(mock1))
            .with_branch(Box::new(mock2));
        let mut concurrency = concurrency_builder.build(&design);

        let mut poller = OrchTestingPoller::new(concurrency.try_execute().unwrap());
        // Call the poll function to spawn all the actions to execute concurrently and wait for them to complete.
        // The mock runtime will handle the execution of these actions.
        let _ = poller.poll();

        // Use the mock runtime to execute all spawned concurrent actions.
        assert!(mock::runtime::remaining_tasks() > 0);
        let _ = poller.poll(); // Poll again before advancing tasks
        let result = poller.poll();
        assert_eq!(result, Poll::Pending); // Should still be pending since tasks are not advanced yet
        mock::runtime::step();
        assert_eq!(mock::runtime::remaining_tasks(), 0);

        // Get the result
        let result = poller.poll();
        assert_eq!(result, Poll::Ready(Ok(())));
    }

    #[test]
    #[ensure_clear_mock_runtime]
    #[should_panic]
    fn concurrency_panics_if_polled_after_future_reported_ready() {
        let mock1 = MockActionBuilder::<()>::new().will_once_return(Ok(())).build();
        let mock2 = MockActionBuilder::<()>::new().will_once_return(Ok(())).build();

        let design = Design::new("Design".into(), DesignConfig::default());
        let mut concurrency_builder = ConcurrencyBuilder::new();
        concurrency_builder
            .with_branch(Box::new(mock1))
            .with_branch(Box::new(mock2));
        let mut concurrency = concurrency_builder.build(&design);

        let mut poller = OrchTestingPoller::new(concurrency.try_execute().unwrap());
        // Call the poll function to spawn all the actions to execute concurrently and wait for them to complete.
        // The mock runtime will handle the execution of these actions.
        let _ = poller.poll();

        // Use the mock runtime to execute all spawned concurrent actions.
        assert!(mock::runtime::remaining_tasks() > 0);
        mock::runtime::step();
        assert_eq!(mock::runtime::remaining_tasks(), 0);

        // Get the result
        let result = poller.poll();
        assert_eq!(result, Poll::Ready(Ok(())));

        // Poll again after the future has reported ready, this causes a panic.
        let _ = poller.poll();
    }

    #[test]
    #[ensure_clear_mock_runtime]
    fn concurrency_executed_twice() {
        let mock1 = MockActionBuilder::<()>::new().times(2).build();
        let mock2 = MockActionBuilder::<()>::new().times(2).build();

        let design = Design::new("Design".into(), DesignConfig::default());
        let mut concurrency_builder = ConcurrencyBuilder::new();
        concurrency_builder
            .with_branch(Box::new(mock1))
            .with_branch(Box::new(mock2));
        let mut concurrency = concurrency_builder.build(&design);

        // Execute the concurrency twice to ensure it can handle multiple executions correctly.
        // This is to test that the futures are reset and can be reused.
        for _ in 0..2 {
            let mut poller = OrchTestingPoller::new(concurrency.try_execute().unwrap());
            // Call the poll function to spawn all the actions to execute concurrently and wait for them to complete.
            // The mock runtime will handle the execution of these actions.
            let _ = poller.poll();

            // Use the mock runtime to execute all spawned concurrent actions.
            assert!(mock::runtime::remaining_tasks() > 0);
            mock::runtime::step();
            assert_eq!(mock::runtime::remaining_tasks(), 0);

            // Get the result
            let result = poller.poll();
            assert_eq!(result, Poll::Ready(Ok(())));
        }
    }

    #[test]
    #[ensure_clear_mock_runtime]
    fn concurrency_fails_first_time_and_succeeds_second_time() {
        let mock1 = MockActionBuilder::<()>::new().times(2).build();
        let mock2 = MockActionBuilder::<()>::new()
            .will_once_return(Err(ActionExecError::Internal))
            .will_once_return(Ok(()))
            .build();

        let design = Design::new("Design".into(), DesignConfig::default());
        let mut concurrency_builder = ConcurrencyBuilder::new();
        concurrency_builder
            .with_branch(Box::new(mock1))
            .with_branch(Box::new(mock2));
        let mut concurrency = concurrency_builder.build(&design);

        // Execute the concurrency twice to ensure it can handle multiple executions correctly.
        // This is to test that the futures are reset and can be reused.
        for count in 0..2 {
            let mut poller = OrchTestingPoller::new(concurrency.try_execute().unwrap());
            // Call the poll function to spawn all the actions to execute concurrently and wait for them to complete.
            // The mock runtime will handle the execution of these actions.
            let _ = poller.poll();

            // Use the mock runtime to execute all spawned concurrent actions.
            assert!(mock::runtime::remaining_tasks() > 0);
            mock::runtime::step();
            assert_eq!(mock::runtime::remaining_tasks(), 0);

            // Get the result
            let result = poller.poll();
            if count == 0 {
                // First execution should fail since mock2 returns Err on the first call
                assert_eq!(result, Poll::Ready(Err(ActionExecError::Internal)));
            } else {
                // Second execution should succeed since mock2 returns Ok on the second call
                assert_eq!(result, Poll::Ready(Ok(())));
            }
        }
    }
}
