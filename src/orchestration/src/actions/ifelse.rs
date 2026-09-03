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

use crate::{
    api::design::Design,
    common::{orch_tag::OrchestrationTag, DesignConfig},
    prelude::{ActionBaseMeta, ActionResult, ActionTrait, ReusableBoxFutureResult},
};
use core::future::Future;
use kyron::futures::reusable_box_future::{ReusableBoxFuture, ReusableBoxFuturePool};
use std::sync::{Arc, Mutex};

/// The trait that needs to be implemented by the IfElse condition object provided by the user.
/// The compute method result determines which branch action is executed by the IfElse action.
pub trait IfElseCondition {
    fn compute(&self) -> bool;
}

/// An orchestration action that executes either branch action depending on the result of the user-provided condition object.
pub struct IfElse {}

impl IfElse {
    /// Create an if-else action out of an orchestration tag.
    pub fn from_tag(
        tag: &OrchestrationTag,
        true_branch: Box<dyn ActionTrait>,
        false_branch: Box<dyn ActionTrait>,
        config: &DesignConfig,
    ) -> Box<dyn ActionTrait> {
        tag.action_provider()
            .borrow_mut()
            .provide_if_else(*tag.tag(), true_branch, false_branch, config)
            .unwrap()
    }

    /// Create an if-else action out of a design.
    pub fn from_design(
        name: &str,
        true_branch: Box<dyn ActionTrait>,
        false_branch: Box<dyn ActionTrait>,
        design: &Design,
    ) -> Box<dyn ActionTrait> {
        let tag = design.get_orchestration_tag(name.into());
        assert!(tag.is_ok(), "Failed to create ifelse with name \"{}\"", name);

        Self::from_tag(&tag.unwrap(), true_branch, false_branch, design.config())
    }

    pub(crate) fn from_arc_condition<C>(
        condition: Arc<C>,
        true_branch: Box<dyn ActionTrait>,
        false_branch: Box<dyn ActionTrait>,
        config: &DesignConfig,
    ) -> Box<dyn ActionTrait>
    where
        C: IfElseCondition + Send + Sync + 'static,
    {
        const TAG: &str = "orch::internal::ifelse:arc";

        Box::new(IfElseArc {
            base: ActionBaseMeta {
                tag: crate::actions::action::next_unique_action_tag(TAG),
                reusable_future_pool: IfElseArc::<C>::create_future_pool(
                    IfElseArc::<C>::choose_branch,
                    config.max_concurrent_action_executions,
                ),
            },
            condition,
            true_branch,
            false_branch,
        })
    }

    pub(crate) fn from_arc_mutex_condition<C>(
        condition: Arc<Mutex<C>>,
        true_branch: Box<dyn ActionTrait>,
        false_branch: Box<dyn ActionTrait>,
        config: &DesignConfig,
    ) -> Box<dyn ActionTrait>
    where
        C: IfElseCondition + Send + 'static,
    {
        const TAG: &str = "orch::internal::ifelse:arcmutex";

        Box::new(IfElseArcMutex {
            base: ActionBaseMeta {
                tag: crate::actions::action::next_unique_action_tag(TAG),
                reusable_future_pool: IfElseArcMutex::<C>::create_future_pool(
                    IfElseArcMutex::<C>::choose_branch,
                    config.max_concurrent_action_executions,
                ),
            },
            condition,
            true_branch,
            false_branch,
        })
    }
}

struct IfElseArc<C: IfElseCondition + Send + Sync + 'static> {
    base: ActionBaseMeta,
    condition: Arc<C>,
    true_branch: Box<dyn ActionTrait>,
    false_branch: Box<dyn ActionTrait>,
}

impl<C: IfElseCondition + Send + Sync + 'static> IfElseArc<C> {
    fn create_future_pool<F, T>(_: F, size: usize) -> ReusableBoxFuturePool<ActionResult>
    where
        F: Fn(Arc<C>, ReusableBoxFuture<ActionResult>, ReusableBoxFuture<ActionResult>) -> T,
        T: Future<Output = ActionResult> + Send + 'static,
    {
        ReusableBoxFuturePool::for_type::<T>(size)
    }

    async fn choose_branch(
        condition: Arc<C>,
        true_future: ReusableBoxFuture<ActionResult>,
        false_future: ReusableBoxFuture<ActionResult>,
    ) -> ActionResult {
        if condition.compute() {
            true_future.into_pin().await
        } else {
            false_future.into_pin().await
        }
    }
}

impl<C: IfElseCondition + Send + Sync + 'static> ActionTrait for IfElseArc<C> {
    fn try_execute(&mut self) -> ReusableBoxFutureResult {
        self.base.reusable_future_pool.next(Self::choose_branch(
            Arc::clone(&self.condition),
            self.true_branch.try_execute()?,
            self.false_branch.try_execute()?,
        ))
    }

    fn name(&self) -> &'static str {
        "IfElse"
    }

    fn dbg_fmt(&self, _nest: usize, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
        write!(
            f,
            "IfElseArc {{ true_branch: {:?}, false_branch: {:?} }}",
            self.true_branch.name(),
            self.false_branch.name()
        )
    }
}

struct IfElseArcMutex<C: IfElseCondition + Send + 'static> {
    base: ActionBaseMeta,
    condition: Arc<Mutex<C>>,
    true_branch: Box<dyn ActionTrait>,
    false_branch: Box<dyn ActionTrait>,
}

impl<C: IfElseCondition + Send + 'static> IfElseArcMutex<C> {
    fn create_future_pool<F, T>(_: F, size: usize) -> ReusableBoxFuturePool<ActionResult>
    where
        F: Fn(Arc<Mutex<C>>, ReusableBoxFuture<ActionResult>, ReusableBoxFuture<ActionResult>) -> T,
        T: Future<Output = ActionResult> + Send + 'static,
    {
        ReusableBoxFuturePool::for_type::<T>(size)
    }

    async fn choose_branch(
        condition: Arc<Mutex<C>>,
        true_future: ReusableBoxFuture<ActionResult>,
        false_future: ReusableBoxFuture<ActionResult>,
    ) -> ActionResult {
        let condition = condition.lock().unwrap().compute();

        if condition {
            true_future.into_pin().await
        } else {
            false_future.into_pin().await
        }
    }
}

impl<C: IfElseCondition + Send + 'static> ActionTrait for IfElseArcMutex<C> {
    fn try_execute(&mut self) -> ReusableBoxFutureResult {
        self.base.reusable_future_pool.next(Self::choose_branch(
            Arc::clone(&self.condition),
            self.true_branch.try_execute()?,
            self.false_branch.try_execute()?,
        ))
    }

    fn name(&self) -> &'static str {
        "IfElse"
    }

    fn dbg_fmt(&self, _nest: usize, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
        write!(
            f,
            "IfElseIfElseArcMutexArc {{ true_branch: {:?}, false_branch: {:?} }}",
            self.true_branch.name(),
            self.false_branch.name()
        )
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use crate::{
        prelude::ActionExecError,
        testing::{MockActionBuilder, OrchTestingPoller},
    };
    use core::task::Poll;

    #[test]
    fn test_true_branch() {
        let config = DesignConfig::default();

        struct TestCond {}

        impl IfElseCondition for TestCond {
            fn compute(&self) -> bool {
                true
            }
        }

        let true_branch = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Err(ActionExecError::UserError(0xcafe_u64.into())))
                .build(),
        );
        let false_branch = Box::new(MockActionBuilder::<()>::new().times(0).build());
        let mut ifelse = IfElse::from_arc_condition(Arc::new(TestCond {}), true_branch, false_branch, &config);

        let mut mock = OrchTestingPoller::new(ifelse.try_execute().unwrap());
        assert_eq!(
            Poll::Ready(Err(ActionExecError::UserError(0xcafe_u64.into()))),
            mock.poll()
        );
    }

    #[test]
    fn test_false_branch() {
        let config = DesignConfig::default();

        struct TestCond {}

        impl IfElseCondition for TestCond {
            fn compute(&self) -> bool {
                false
            }
        }

        let true_branch = Box::new(MockActionBuilder::<()>::new().times(0).build());
        let false_branch = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Err(ActionExecError::UserError(0xbeef_u64.into())))
                .build(),
        );
        let mut ifelse = IfElse::from_arc_condition(Arc::new(TestCond {}), true_branch, false_branch, &config);

        let mut mock = OrchTestingPoller::new(ifelse.try_execute().unwrap());
        assert_eq!(
            Poll::Ready(Err(ActionExecError::UserError(0xbeef_u64.into()))),
            mock.poll()
        );
    }
}
