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

use super::action::{ActionBaseMeta, ActionResult, ActionTrait, ReusableBoxFutureResult};
use crate::api::design::Design;
use ::core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use kyron::futures::reusable_box_future::{ReusableBoxFuture, ReusableBoxFuturePool};
use kyron_foundation::{
    base::fast_rand::FastRand,
    containers::{
        growable_vec::GrowableVec,
        reusable_objects::{ReusableObject, ReusableObjectTrait, ReusableObjects},
    },
    prelude::{vector_extension::VectorExtension, *},
};
use std::time::{SystemTime, UNIX_EPOCH};

/// Object used to construct the [`Select`] action which can run multiple async actions
/// concurrently and return the result of the first action that completes.
pub struct SelectBuilder {
    cases: Option<GrowableVec<Box<dyn ActionTrait>>>,
}

impl SelectBuilder {
    /// Create the builder.
    pub fn new() -> Self {
        Self { cases: None }
    }

    /// Add an action as one of the cases.
    pub fn with_case(&mut self, action: Box<dyn ActionTrait>) -> &mut Self {
        self.cases.get_or_insert(GrowableVec::new(1)).push(action);
        self
    }

    /// Build a `Select` action out of the added cases.
    pub fn build(&mut self, design: &Design) -> Box<Select> {
        let cases = self.cases.take().expect("Select requires at least one case.");
        let cases_len = cases.len();
        let mut reusable_case_pins = ReusableObjects::<Vec<Pin<ReusableBoxFuture<ActionResult>>>>::new(
            design.config.max_concurrent_action_executions,
            |_| Vec::new_in_global(cases_len),
        );

        Box::new(Select {
            base: ActionBaseMeta {
                tag: super::action::next_unique_action_tag("orch::internal::select"),
                reusable_future_pool: ReusableBoxFuturePool::<ActionResult>::for_value(
                    design.config.max_concurrent_action_executions,
                    SelectFuture::new(
                        reusable_case_pins
                            .next_object()
                            .expect("Not enough reusable case handles to build the Select action."),
                    ),
                ),
            },
            cases: cases.into(),
            reusable_case_pins,
        })
    }
}

impl Default for SelectBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// `Select` will concurrently execute async case actions until any one finishes. The result of
/// the `Select` action will be the result of the first case action that finishes. The remaining
/// case actions will be cancelled. The order of case actions matters. `Select` polls case actions
/// in random order. `Select` can be executed multiple times, also concurrently depending on the configuration.
/// # Notes
/// 1. If multiple case actions finish at the same time, the result of the first polled action will be returned.
/// 2. Sync actions are not supposed to be used as case actions. If used, they will be executed to completion when polled
///    and their result will be returned immediately.
pub struct Select {
    base: ActionBaseMeta,
    cases: Vec<Box<dyn ActionTrait>>,
    reusable_case_pins: ReusableObjects<Vec<Pin<ReusableBoxFuture<ActionResult>>>>,
}

impl ActionTrait for Select {
    fn try_execute(&mut self) -> ReusableBoxFutureResult {
        let mut case_pins = self.reusable_case_pins.next_object()?;

        for case in self.cases.iter_mut() {
            case_pins.push(case.try_execute()?.into_pin());
        }

        self.base.reusable_future_pool.next(SelectFuture::new(case_pins))
    }

    fn name(&self) -> &'static str {
        "Select"
    }

    fn dbg_fmt(&self, nest: usize, formatter: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
        let indent = " ".repeat(nest);

        writeln!(formatter, "{}|-{} - {:?}", indent, self.name(), self.base)?;
        self.cases.iter().try_for_each(|case| {
            writeln!(formatter, "{} |case", indent)?;
            case.dbg_fmt(nest + 1, formatter)
        })
    }
}

struct SelectFuture {
    case_pins: ReusableObject<Vec<Pin<ReusableBoxFuture<ActionResult>>>>,
    rand: FastRand,
}

impl SelectFuture {
    fn new(case_pins: ReusableObject<Vec<Pin<ReusableBoxFuture<ActionResult>>>>) -> Self {
        let seed = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(since) => since.as_millis() as u64,
            Err(_) => {
                warn!("Failed to seed the SelectFuture random generator.");

                0xA491_3C75_E1F8_B2D6
            },
        };

        Self {
            case_pins,
            rand: FastRand::new(seed),
        }
    }
}

impl Future for SelectFuture {
    type Output = ActionResult;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        debug_assert!(
            !self.case_pins.is_empty(),
            "Should not be possible with the current SelectBuilder implementation."
        );

        let mut loop_result = None;
        let start_i = self.rand.next() as usize;
        let case_count = self.case_pins.len();

        for i in 0..case_count {
            let case_pin = &mut self.case_pins[(start_i + i) % case_count];

            match case_pin.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    loop_result = Some(result);
                    break;
                },
                Poll::Pending => (),
            }
        }

        if let Some(result) = loop_result {
            // The idea is to try to cancel the remaining cases.
            self.case_pins.clear();

            Poll::Ready(result)
        } else {
            Poll::Pending
        }
    }
}

impl ReusableObjectTrait for SelectFuture {
    fn reusable_clear(&mut self) {
        self.case_pins.clear();
    }
}

#[cfg(test)]
#[cfg(not(miri))]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use crate::{
        common::DesignConfig,
        prelude::ActionExecError,
        testing::{MockActionBuilder, OrchTestingPoller, TestAsyncAction},
    };
    use core::future;
    use kyron::futures::yield_now::yield_now;
    use kyron_testing_macros::ensure_clear_mock_runtime;
    async fn async_fn_with_await() -> ActionResult {
        yield_now().await;
        Ok(())
    }
    #[test]
    #[ensure_clear_mock_runtime]
    fn first_action_returns_value() {
        let mock1 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Err(ActionExecError::UserError(0xdeadbeef.into())))
                .build(),
        );
        let mock2 = Box::new(TestAsyncAction::new(future::pending));
        let mock3 = Box::new(TestAsyncAction::new(future::pending));

        let design = Design::new("Design".into(), DesignConfig::default());
        let mut select = SelectBuilder::new()
            .with_case(mock1)
            .with_case(mock2)
            .with_case(mock3)
            .build(&design);

        let mut poller = OrchTestingPoller::new(select.try_execute().unwrap());
        assert_eq!(
            poller.poll(),
            Poll::Ready(Err(ActionExecError::UserError(0xdeadbeef.into())))
        );
    }

    #[test]
    #[ensure_clear_mock_runtime]
    fn middle_action_returns_value() {
        let mock1 = Box::new(TestAsyncAction::new(future::pending));
        let mock2 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Err(ActionExecError::UserError(0xdeadbeef.into())))
                .build(),
        );
        let mock3 = Box::new(TestAsyncAction::new(future::pending));

        let design = Design::new("Design".into(), DesignConfig::default());
        let mut select = SelectBuilder::new()
            .with_case(mock1)
            .with_case(mock2)
            .with_case(mock3)
            .build(&design);

        let mut poller = OrchTestingPoller::new(select.try_execute().unwrap());
        assert_eq!(
            poller.poll(),
            Poll::Ready(Err(ActionExecError::UserError(0xdeadbeef.into())))
        );
    }

    #[test]
    #[ensure_clear_mock_runtime]
    fn last_action_returns_value() {
        let mock1 = Box::new(TestAsyncAction::new(future::pending));
        let mock2 = Box::new(TestAsyncAction::new(future::pending));
        let mock3 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Err(ActionExecError::UserError(0xdeadbeef.into())))
                .build(),
        );

        let design = Design::new("Design".into(), DesignConfig::default());
        let mut select = SelectBuilder::new()
            .with_case(mock1)
            .with_case(mock2)
            .with_case(mock3)
            .build(&design);

        let mut poller = OrchTestingPoller::new(select.try_execute().unwrap());
        assert_eq!(
            poller.poll(),
            Poll::Ready(Err(ActionExecError::UserError(0xdeadbeef.into())))
        );
    }

    #[test]
    #[ensure_clear_mock_runtime]
    fn verify_ordering_with_first_pending() {
        // Since the execution of select cases is random, we need actions which complete at different times to verify the ordering.
        // Otherwise, the test may pass or fail randomly.
        let mock1 = Box::new(TestAsyncAction::new(future::pending));
        let mock2 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Err(ActionExecError::UserError(0xdeadbeef.into())))
                .build(),
        );
        let mock3 = Box::new(TestAsyncAction::new(async_fn_with_await));

        let design = Design::new("Design".into(), DesignConfig::default());
        let mut select = SelectBuilder::new()
            .with_case(mock1)
            .with_case(mock2)
            .with_case(mock3)
            .build(&design);

        let mut poller = OrchTestingPoller::new(select.try_execute().unwrap());
        assert_eq!(
            poller.poll(),
            Poll::Ready(Err(ActionExecError::UserError(0xdeadbeef.into())))
        );
    }

    #[test]
    #[ensure_clear_mock_runtime]
    fn verify_ordering_with_middle_pending() {
        // Since the execution of select cases is random, we need actions which complete at different times to verify the ordering.
        // Otherwise, the test may pass or fail randomly.
        let mock1 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Err(ActionExecError::UserError(0xdeadbeef.into())))
                .build(),
        );
        let mock2 = Box::new(TestAsyncAction::new(future::pending));
        let mock3 = Box::new(TestAsyncAction::new(async_fn_with_await));

        let design = Design::new("Design".into(), DesignConfig::default());
        let mut select = SelectBuilder::new()
            .with_case(mock1)
            .with_case(mock2)
            .with_case(mock3)
            .build(&design);

        let mut poller = OrchTestingPoller::new(select.try_execute().unwrap());
        assert_eq!(
            poller.poll(),
            Poll::Ready(Err(ActionExecError::UserError(0xdeadbeef.into())))
        );
    }

    #[test]
    #[ensure_clear_mock_runtime]
    fn verify_ordering_with_last_pending() {
        // Since the execution of select cases is random, we need actions which complete at different times to verify the ordering.
        // Otherwise, the test may pass or fail randomly.
        let mock1 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Err(ActionExecError::UserError(0xdeadbeef.into())))
                .build(),
        );
        let mock2 = Box::new(TestAsyncAction::new(async_fn_with_await));
        let mock3 = Box::new(TestAsyncAction::new(future::pending));

        let design = Design::new("Design".into(), DesignConfig::default());
        let mut select = SelectBuilder::new()
            .with_case(mock1)
            .with_case(mock2)
            .with_case(mock3)
            .build(&design);

        let mut poller = OrchTestingPoller::new(select.try_execute().unwrap());
        assert_eq!(
            poller.poll(),
            Poll::Ready(Err(ActionExecError::UserError(0xdeadbeef.into())))
        );
    }

    #[test]
    #[ensure_clear_mock_runtime]
    fn verify_ordering_ok_first() {
        // Since the execution of select cases is random, we need actions which complete at different times to verify the ordering.
        // Otherwise, the test may pass or fail randomly.
        let mock1 = Box::new(TestAsyncAction::new(future::pending));
        let mock2 = Box::new(MockActionBuilder::<()>::new().will_once_return(Ok(())).build());
        let mock3 = Box::new(TestAsyncAction::new(async_fn_with_await));

        let design = Design::new("Design".into(), DesignConfig::default());
        let mut select = SelectBuilder::new()
            .with_case(mock1)
            .with_case(mock2)
            .with_case(mock3)
            .build(&design);

        let mut poller = OrchTestingPoller::new(select.try_execute().unwrap());
        assert_eq!(poller.poll(), Poll::Ready(Ok(())));
    }

    #[test]
    #[ensure_clear_mock_runtime]
    fn verify_ordering_err_first() {
        // Since the execution of select cases is random, we need actions which complete at different times to verify the ordering.
        // Otherwise, the test may pass or fail randomly.
        let mock1 = Box::new(TestAsyncAction::new(future::pending));
        let mock2 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Err(ActionExecError::UserError(0x1234abcd.into())))
                .build(),
        );
        let mock3 = Box::new(TestAsyncAction::new(async_fn_with_await));

        let design = Design::new("Design".into(), DesignConfig::default());
        let mut select = SelectBuilder::new()
            .with_case(mock1)
            .with_case(mock2)
            .with_case(mock3)
            .build(&design);

        let mut poller = OrchTestingPoller::new(select.try_execute().unwrap());
        assert_eq!(
            poller.poll(),
            Poll::Ready(Err(ActionExecError::UserError(0x1234abcd.into())))
        );
    }
}
