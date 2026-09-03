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

#![allow(dead_code)]
use ::core::ops::BitOr;
use std::sync::{Arc, Mutex};

use kyron::futures::reusable_box_future::*;

use kyron_foundation::not_recoverable_error;
use kyron_foundation::prelude::*;

use crate::api::design::Design;

use super::action::*;

// Error that will be propagated to user handler
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum HandlerErrors {
    UserErr(UserErrValue),
    Timeout,
}

/// Filter for which catch action shall react. This supports bitwise-or `|`.
#[derive(Debug, Clone, PartialEq)]
pub enum ErrorFilter {
    // Values shall be powers of 2, so we can use bitwise operations
    /// Catch action will handle user errors
    UserErrors = 0x1,

    /// Catch action will handle timeouts that are monitored by [`Timeout`] action
    Timeouts = 0x2,
}

/// Use [`ErrorFilter`] with bitwise-or (or .into()) to create a set of filters
#[derive(Debug, Clone, Copy)]
pub struct ErrorFilters(u64);
impl ErrorFilters {
    fn is_filter_enabled(&self, filter: ErrorFilter) -> bool {
        self.0 & (filter as u64) != 0
    }
}

/// `Catch` is an action that wraps another action and intercepts errors during its execution.
///
/// It uses filters to determine which errors should be handled and provides mechanisms to attach custom handler for those errors.
/// The `Catch` action ensures that errors are either handled or propagated further down the chain.
///
/// # Key Features
/// - Supports filtering specific error types using `ErrorFilter`.
/// - Allows attaching recoverable and non-recoverable error handlers.
/// - Propagates unhandled errors to the next action in the chain.
///
pub struct Catch {
    base: ActionBaseMeta,

    filters: ErrorFilters,
    action: Box<dyn ActionTrait>,
    handler: HandlerType,
}

/// `CatchBuilder` is a builder for creating a `Catch` action.
pub struct CatchBuilder {
    filters: ErrorFilters,
    action: Option<Box<dyn ActionTrait>>,
    handler: HandlerType,
}

impl CatchBuilder {
    /// Creates a new `CatchBuilder` instance.
    ///
    /// # Parameters
    /// - `filters`: Specifies the error filters that determine which errors the `Catch` action will handle.
    /// - `action`: The action to be wrapped by the `Catch` action.
    ///
    /// # Returns
    /// A new instance of `CatchBuilder`.
    ///
    pub fn new(filters: ErrorFilters, action: Box<dyn ActionTrait>) -> Self {
        Self {
            filters,
            action: Some(action),
            handler: HandlerType::None,
        }
    }

    /// Attaches a non-recoverable error handler to the `CatchBuilder`.
    ///
    /// # Parameters
    /// - `handler`: A closure that takes a `HandlerErrors` parameter and handles non-recoverable errors.
    ///
    /// # Returns
    /// A mutable reference to the `CatchBuilder` instance.
    ///
    /// # Panics
    /// Panics if a handler is already attached.
    ///
    pub fn catch<H>(mut self, mut handler: H) -> Self
    where
        H: FnMut(HandlerErrors) + Send + 'static,
    {
        assert!(
            self.handler.is_none(),
            "Catch: Cannot set handler multiple times, this will cause an error in execution."
        );

        let c = move |e| {
            handler(e);
            false
        };

        self.handler = HandlerType::NonRecoverable(Arc::new(Mutex::new(c)));
        self
    }

    /// Attaches a recoverable error handler to the `CatchBuilder`.
    ///
    /// # Parameters
    /// - `handler`: A closure that takes a `HandlerErrors` parameter and returns a `bool`. `true` when task chain shall continue execution from `Catch` point as normal,
    ///   `false` to propagate error down the chain.
    ///
    /// # Returns
    /// A mutable reference to the `CatchBuilder` instance.
    ///
    /// # Panics
    /// Panics if a handler is already attached.
    ///
    pub fn catch_recoverable<H>(mut self, handler: H) -> Self
    where
        H: FnMut(HandlerErrors) -> bool + Send + 'static,
    {
        assert!(
            self.handler.is_none(),
            "Catch: Cannot set handler multiple times, this will cause an error in execution."
        );

        self.handler = HandlerType::Recoverable(Arc::new(Mutex::new(handler)));
        self
    }

    /// Builds the `Catch` action.
    ///
    /// # Returns
    /// A `Box<Catch>` instance that wraps the specified action and handles errors based on the configured filters and handlers.
    ///
    /// # Panics
    /// Panics if no handler is attached.
    ///
    pub fn build(mut self, design: &Design) -> Box<Catch> {
        assert!(
            !self.handler.is_none(),
            "Catch: No handler provided, this will cause an error in execution."
        );

        let mut lp = ReusableBoxFuturePool::for_value(1, async move { Ok(()) });
        let action = lp.next(async { Ok(()) }).unwrap();

        Box::new(Catch {
            base: ActionBaseMeta {
                tag: crate::actions::action::next_unique_action_tag("orch::internal::catch_action"),
                reusable_future_pool: ReusableBoxFuturePool::for_value(
                    design.config.max_concurrent_action_executions,
                    Catch::execute_impl(action, HandlerType::None, self.filters),
                ),
            },
            filters: self.filters,
            action: self
                .action
                .take()
                .expect("CatchBuilder: Action must be set before building"),
            handler: self.handler.clone(),
        })
    }
}

#[derive(Clone)]
enum HandlerType {
    None,
    Recoverable(Arc<Mutex<dyn FnMut(HandlerErrors) -> bool + Send>>),
    NonRecoverable(Arc<Mutex<dyn FnMut(HandlerErrors) -> bool + Send>>), // Consider sth else than mutex
}

unsafe impl Send for HandlerType {} // underlying type is send so this can also be send

impl HandlerType {
    fn is_none(&self) -> bool {
        matches!(self, HandlerType::None)
    }
}
impl From<HandlerErrors> for ActionExecError {
    fn from(e: HandlerErrors) -> Self {
        match e {
            HandlerErrors::UserErr(user_err) => ActionExecError::UserError(user_err),
            HandlerErrors::Timeout => ActionExecError::Timeout,
        }
    }
}

impl BitOr for ErrorFilter {
    type Output = ErrorFilters;

    fn bitor(self, rhs: Self) -> Self::Output {
        ErrorFilters(self as u64 | rhs as u64)
    }
}

#[allow(clippy::from_over_into)]
impl Into<ErrorFilters> for ErrorFilter {
    fn into(self) -> ErrorFilters {
        ErrorFilters(self as u64)
    }
}

impl Catch {
    async fn execute_impl(
        action: ReusableBoxFuture<ActionResult>,
        handler: HandlerType,
        filters: ErrorFilters,
    ) -> ActionResult {
        // How does it work:
        // There are two cases for error source: Return error from user Invoke or Timeout from `Timeout` action..
        //
        // 1. Error from user Invoke: When error happens im user action, the connected Task is bring into safety worker and actions are run there.
        // Even if there was multiple tasks along the way between error source and `Catch` action, each of tasks is spawned as safety so on error it will be waken into safety worker
        // which eventually leads to `Catch` action execution.
        //
        // 2. Timeout from `Timeout` action: Timeout actions is always spawning a new task for connected action to be sure it cannot be blocked by any `synchronous` action (like Sequence with Invoke).
        // When timeout is detected, task in which `Timeout` was created will be bring back into safety worker, return error as Timeout and then `Catch` action will be executed eventually (as above)
        //

        let res = action.into_pin().await;

        // Checks errors from actions, this action acts as error filter to call reaction
        match res {
            Ok(_) => Ok(()),
            Err(ActionExecError::UserError(user_error)) if filters.is_filter_enabled(ErrorFilter::UserErrors) => {
                Self::handle_user_action(handler, HandlerErrors::UserErr(user_error))
            },
            Err(ActionExecError::Timeout) if filters.is_filter_enabled(ErrorFilter::Timeouts) => {
                Self::handle_user_action(handler, HandlerErrors::Timeout)
            },
            Err(e) => {
                error!("Catch: Not filtered error in action execution: {:?}, propagating.", e);
                Err(e)
            },
        }
    }

    fn handle_user_action(mut handler: HandlerType, e: HandlerErrors) -> ActionResult {
        match handler {
            HandlerType::None => not_recoverable_error!("Catch: Cannot be here, we assured this during builder phase."),
            HandlerType::Recoverable(ref mut user_handler) => {
                let mut handler = user_handler.lock().unwrap();
                if handler(e) {
                    Ok(())
                } else {
                    Err(ActionExecError::from(e)) // Keep  the error as is, maybe someone below can handle it
                }
            },
            HandlerType::NonRecoverable(ref mut user_handler) => {
                let mut handler = user_handler.lock().unwrap();
                handler(e);
                Err(ActionExecError::NonRecoverableFailure)
            },
        }
    }
}

impl ActionTrait for Catch {
    fn try_execute(&mut self) -> ReusableBoxFutureResult {
        let action = self.action.try_execute()?;

        self.base
            .reusable_future_pool
            .next(Self::execute_impl(action, self.handler.clone(), self.filters))
    }

    fn name(&self) -> &'static str {
        "Catch"
    }

    fn dbg_fmt(&self, _nest: usize, _f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
        todo!()
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {

    use super::*;
    use core::{
        sync::atomic::{AtomicBool, Ordering},
        task::Poll,
    };

    use crate::{
        common::DesignConfig,
        testing::{MockAction, MockActionBuilder, OrchTestingPoller},
    };

    #[test]
    fn non_recoverable_handler_not_called_before_execution() {
        let design = Design::new("Design".into(), DesignConfig::default());
        let action = Box::new(MockAction::<()>::default());
        let builder = CatchBuilder::new(ErrorFilter::UserErrors.into(), action);

        let handler_called = Arc::new(AtomicBool::new(false));
        let handler_called_clone = Arc::clone(&handler_called);

        let _catch = builder
            .catch(move |_err| {
                handler_called_clone.store(true, Ordering::SeqCst);
            })
            .build(&design);
        assert!(!handler_called.load(Ordering::SeqCst)); // Handler should not be called during build
    }

    #[test]
    fn recoverable_handler_not_called_before_execution() {
        let design = Design::new("Design".into(), DesignConfig::default());
        let action = Box::new(MockAction::<()>::default());
        let builder = CatchBuilder::new(ErrorFilter::UserErrors.into(), action);

        let handler_called = Arc::new(AtomicBool::new(false));
        let handler_called_clone = Arc::clone(&handler_called);

        let _catch = builder
            .catch_recoverable(move |_err| {
                handler_called_clone.store(true, Ordering::SeqCst);
                true
            })
            .build(&design);
        assert!(!handler_called.load(Ordering::SeqCst)); // Handler should not be called during build
    }

    #[test]
    #[should_panic(expected = "Catch: Cannot set handler multiple times, this will cause an error in execution.")]
    fn multiple_handlers_panic() {
        let action = Box::new(MockAction::<()>::default());
        let mut builder = CatchBuilder::new(ErrorFilter::UserErrors.into(), action);

        builder = builder.catch(|_err| {});
        builder.catch(|_err| {}); // This should panic
    }

    #[test]
    #[should_panic(expected = "Catch: No handler provided, this will cause an error in execution.")]
    fn no_handler_panic() {
        let design = Design::new("Design".into(), DesignConfig::default());
        let action = Box::new(MockAction::<()>::default());
        let builder = CatchBuilder::new(ErrorFilter::UserErrors.into(), action);

        builder.build(&design); // This should panic
    }

    #[test]
    fn when_user_action_finished_without_error_catch_returns_ok() {
        let design = Design::new("Design".into(), DesignConfig::default());
        let action = Box::new(MockAction::<()>::default());
        let builder = CatchBuilder::new(ErrorFilter::UserErrors.into(), action);

        let mut handler_mock = kyron_testing::mock_fn::MockFnBuilder::<(), bool>::new()
            .times(0)
            .build();

        let mut catch = builder
            .catch(move |_err| {
                handler_mock.call(());
            })
            .build(&design);

        let f = catch.try_execute().unwrap();

        let mut poller = OrchTestingPoller::new(f);

        assert_eq!(poller.poll(), Poll::Ready(Ok(()))); // Task, so the action, shall be ready
    }

    #[test]
    fn when_user_action_finished_with_filtered_error_catch_calls_handler() {
        let design = Design::new("Design".into(), DesignConfig::default());
        let action = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Err(UserErrValue::from(64).into()))
                .build(),
        );
        let builder = CatchBuilder::new(ErrorFilter::UserErrors.into(), action);

        let mut handler_mock = kyron_testing::mock_fn::MockFnBuilder::<(), bool>::new()
            .times(1)
            .build();

        let mut catch = builder
            .catch(move |_err| {
                handler_mock.call(());
            })
            .build(&design);

        let f = catch.try_execute().unwrap();

        let mut poller = OrchTestingPoller::new(f);

        assert_eq!(poller.poll(), Poll::Ready(Err(ActionExecError::NonRecoverableFailure)));
    }

    #[test]
    fn when_user_action_finished_with_not_filtered_error_catch_does_not_call_handler() {
        {
            let design = Design::new("Design".into(), DesignConfig::default());
            let action = Box::new(
                MockActionBuilder::<()>::new()
                    .will_once_return(Err(ActionExecError::Timeout))
                    .build(),
            );
            let builder = CatchBuilder::new(ErrorFilter::UserErrors.into(), action);

            let mut handler_mock = kyron_testing::mock_fn::MockFnBuilder::<(), bool>::new()
                .times(0)
                .build();

            let mut catch = builder
                .catch(move |_err| {
                    handler_mock.call(());
                })
                .build(&design);

            let f = catch.try_execute().unwrap();

            let mut poller = OrchTestingPoller::new(f);

            assert_eq!(poller.poll(), Poll::Ready(Err(ActionExecError::Timeout)));
        }

        {
            let design = Design::new("Design".into(), DesignConfig::default());
            let action = Box::new(
                MockActionBuilder::<()>::new()
                    .will_once_return(Err(UserErrValue::from(64).into()))
                    .build(),
            );
            let builder = CatchBuilder::new(ErrorFilter::Timeouts.into(), action);

            let mut handler_mock = kyron_testing::mock_fn::MockFnBuilder::<(), bool>::new()
                .times(0)
                .build();

            let mut catch = builder
                .catch(move |_err| {
                    handler_mock.call(());
                })
                .build(&design);

            let f = catch.try_execute().unwrap();

            let mut poller = OrchTestingPoller::new(f);

            assert_eq!(
                poller.poll(),
                Poll::Ready(Err(ActionExecError::UserError(UserErrValue::from(64))))
            );
        }
    }

    #[test]
    fn when_action_finished_with_internal_err_error_is_propagated() {
        let design = Design::new("Design".into(), DesignConfig::default());
        let action = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Err(ActionExecError::Internal))
                .build(),
        );
        let builder = CatchBuilder::new(ErrorFilter::UserErrors.into(), action);

        let mut handler_mock = kyron_testing::mock_fn::MockFnBuilder::<(), bool>::new()
            .times(0)
            .build();

        let mut catch = builder
            .catch(move |_err| {
                handler_mock.call(());
            })
            .build(&design);

        let f = catch.try_execute().unwrap();

        let mut poller = OrchTestingPoller::new(f);

        assert_eq!(poller.poll(), Poll::Ready(Err(ActionExecError::Internal)));
    }

    #[test]
    fn when_user_action_finished_with_filtered_error_catch_calls_handler_and_continue_execution() {
        let design = Design::new("Design".into(), DesignConfig::default());
        let action = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Err(UserErrValue::from(64).into()))
                .build(),
        );
        let builder = CatchBuilder::new(ErrorFilter::UserErrors.into(), action);

        let mut handler_mock = kyron_testing::mock_fn::MockFnBuilder::<(), bool>::new()
            .times(1)
            .build();

        let mut catch = builder
            .catch_recoverable(move |_err| {
                handler_mock.call(());
                true
            })
            .build(&design);

        let f = catch.try_execute().unwrap();

        let mut poller = OrchTestingPoller::new(f);

        assert_eq!(poller.poll(), Poll::Ready(Ok(())));
    }

    #[test]
    fn when_user_action_finished_with_filtered_error_catch_calls_handler_and_returns_err() {
        let design = Design::new("Design".into(), DesignConfig::default());
        let action = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Err(UserErrValue::from(64).into()))
                .build(),
        );
        let builder = CatchBuilder::new(ErrorFilter::UserErrors.into(), action);

        let mut handler_mock = kyron_testing::mock_fn::MockFnBuilder::<(), bool>::new()
            .times(1)
            .build();

        let mut catch = builder
            .catch_recoverable(move |_err| {
                handler_mock.call(());
                false
            })
            .build(&design);

        let f = catch.try_execute().unwrap();

        let mut poller = OrchTestingPoller::new(f);

        assert_eq!(
            poller.poll(),
            Poll::Ready(Err(ActionExecError::UserError(UserErrValue::from(64))))
        );
    }
}
