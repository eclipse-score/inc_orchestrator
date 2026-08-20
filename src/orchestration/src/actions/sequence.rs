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
use crate::common::tag::Tag;

use kyron::futures::reusable_box_future::{ReusableBoxFuture, ReusableBoxFuturePool};
use kyron_foundation::{
    containers::{growable_vec::GrowableVec, reusable_objects::ReusableObject, reusable_vec_pool::ReusableVecPool},
    prelude::{vector_extension::VectorExtension, *},
};

const REUSABLE_FUTURE_POOL_SIZE: usize = 2;
const REUSABLE_VEC_POOL_SIZE: usize = 2;
const DEFAULT_TAG: &str = "orch::internal::sequence";

///
/// Construct a `SequenceBuilder` for creating a `Sequence` action
///
pub struct SequenceBuilder {
    actions: GrowableVec<Box<dyn ActionTrait>>,
}

impl Default for SequenceBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SequenceBuilder {
    ///
    /// Construct a `SequenceBuilder`
    ///
    pub fn new() -> SequenceBuilder {
        const REUSABLE_VEC_SIZE: usize = 4;
        Self {
            actions: GrowableVec::new(REUSABLE_VEC_SIZE),
        }
    }

    ///
    /// Add an action to the `Sequence`
    ///
    pub fn with_step(&mut self, action: Box<dyn ActionTrait>) -> &mut Self {
        self.actions.push(action);
        self
    }

    ///
    /// Build the `Sequence` action
    ///
    /// # Errors
    ///
    /// Returns ``Err(NoData)` if no reusable futures collection for storing the actions' futures is available
    ///
    /// # Panics
    ///
    /// Panics if the `Sequence` does not contain any actions
    ///
    pub fn build(&mut self) -> Box<Sequence> {
        assert!(!self.actions.is_empty(), "Sequence must contain at least one action!");

        // No more actions may be added beyond this point
        self.actions.lock();

        // Create pools
        let (futures_vec_pool, reusable_future_pool) = SequenceBuilder::create_pools(self.actions.len());

        // Move the actions from Builder's GrowableVec to Sequence's fixed-sized Vec
        // Here we also reverse the order, so that the actions become already in the correct order,
        // when they are popped out in the execute_impl() later on
        let mut actions = Vec::<Box<dyn ActionTrait>>::new_in_global(self.actions.len());
        while let Some(action) = self.actions.pop() {
            actions
                .push(action)
                .expect("Unable to transfer action from Builder to Sequence");
        }

        // Finally, return the `Sequence` action
        Box::new(Sequence {
            actions,
            base: ActionBaseMeta {
                tag: super::action::next_unique_action_tag(DEFAULT_TAG),
                reusable_future_pool,
            },
            futures_vec_pool,
        })
    }

    ///
    /// Create pools of reusable futures vec and reusable future
    ///
    fn create_pools(
        futures_size: usize,
    ) -> (
        ReusableVecPool<ReusableBoxFuture<ActionResult>>,
        ReusableBoxFuturePool<ActionResult>,
    ) {
        let mut futures_vec_pool =
            ReusableVecPool::<ReusableBoxFuture<ActionResult>>::new(REUSABLE_VEC_POOL_SIZE, |_| {
                Vec::new_in_global(futures_size)
            });
        let futures_vec = futures_vec_pool.next_object().unwrap();

        // Populate the futures' collection to initialize the reusable future pool's layout
        let reusable_future_pool = ReusableBoxFuturePool::<ActionResult>::for_value(
            REUSABLE_FUTURE_POOL_SIZE,
            Sequence::execute_impl(Tag::from_str_static(DEFAULT_TAG), futures_vec),
        );

        (futures_vec_pool, reusable_future_pool)
    }
}

///
/// An orchestration action that invokes subsequent actions specified via `with_step()` in a FIFO
/// manner.
///
/// If any action encounters an error, the `Sequence` execution will terminate immediately,
/// preventing the execution of any remaining actions.
///
pub struct Sequence {
    actions: Vec<Box<dyn ActionTrait>>,
    base: ActionBaseMeta,
    futures_vec_pool: ReusableVecPool<ReusableBoxFuture<ActionResult>>,
}

impl Sequence {
    async fn execute_impl(tag: Tag, mut futures: ReusableObject<Vec<ReusableBoxFuture<ActionResult>>>) -> ActionResult {
        // Execute all futures in the collection, but terminates immediately upon error
        // We can directly pop() without reversing the order here, because the reversion already took place
        // during elements transfer from Builder's GrowableVec to Sequence's Vec
        while let Some(future) = futures.pop() {
            tracing_adapter!(step = ?tag, "Before awaiting step");
            let result = future.into_pin().await;
            if result.is_err() {
                // Terminate sequence and propagate the error
                error!("Error in sequence step {:?}", tag);
                return result;
            }
            tracing_adapter!(step = ?tag, "After awaiting step");
        }

        Ok(())
    }
}

impl ActionTrait for Sequence {
    fn try_execute(&mut self) -> ReusableBoxFutureResult {
        // Get a fresh reusable futures collection and re-populate it with actions' futures
        let mut futures_vec_pool = self.futures_vec_pool.next_object()?;
        self.actions.iter_mut().try_for_each(|action| {
            // Return error in (unlikely) case that no more future can be added to the reusable collection
            if !futures_vec_pool.push(action.try_execute()?) {
                error!("Unable to add a future to the reusable future vec in {:?}", self.base);
                return Err(CommonErrors::NoSpaceLeft);
            }
            Ok(())
        })?;

        // Get a future from the reusable future pool and execute it
        self.base
            .reusable_future_pool
            .next(Sequence::execute_impl(self.base.tag, futures_vec_pool))
    }

    fn name(&self) -> &'static str {
        "Sequence"
    }

    fn dbg_fmt(&self, nest: usize, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
        let indent = " ".repeat(nest);
        writeln!(f, "{}|-{} - {:?}", indent, self.name(), self.base)?;
        self.actions.iter().try_for_each(|action| {
            writeln!(f, "{} |step", indent)?;
            action.dbg_fmt(nest + 1, f)
        })
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use crate::actions::action::{ActionExecError, UserErrValue};
    use crate::testing::{MockActionBuilder, OrchTestingPoller};

    use ::core::task::Poll;

    #[test]
    #[should_panic]
    fn build_on_empty_sequence_should_panic() {
        // Create an empty sequence
        let mut seq = SequenceBuilder::new().build();

        // Execute the sequence
        let mut mock = OrchTestingPoller::new(seq.try_execute().unwrap());
        assert_eq!(Poll::Ready(Ok(())), mock.poll());
    }

    #[test]
    fn all_subsequent_steps_are_called() {
        let seq = kyron_testing::prelude::Sequence::new();
        // Create a sequence with no nested branch
        let mock_1 = Box::new(MockActionBuilder::<()>::new().times(1).in_sequence(&seq).build());
        let mock_2 = Box::new(MockActionBuilder::<()>::new().times(1).in_sequence(&seq).build());
        let mut seq = SequenceBuilder::new().with_step(mock_1).with_step(mock_2).build();

        // Execute the sequence
        let mut mock = OrchTestingPoller::new(seq.try_execute().unwrap());
        assert_eq!(Poll::Ready(Ok(())), mock.poll());
    }

    #[test]
    fn all_steps_within_nested_steps_seq_are_called() {
        let seq = kyron_testing::prelude::Sequence::new();
        // Create a sequence with a nested branch
        let mock_1 = Box::new(MockActionBuilder::<()>::new().times(1).in_sequence(&seq).build());
        let mock_nested_a = Box::new(MockActionBuilder::<()>::new().times(1).in_sequence(&seq).build());
        let mock_nested_b = Box::new(MockActionBuilder::<()>::new().times(1).in_sequence(&seq).build());
        let mock_2 = Box::new(MockActionBuilder::<()>::new().times(1).in_sequence(&seq).build());
        let mut seq = SequenceBuilder::new()
            .with_step(mock_1)
            .with_step(
                SequenceBuilder::new()
                    .with_step(mock_nested_a)
                    .with_step(mock_nested_b)
                    .build(),
            )
            .with_step(mock_2)
            .build();

        // Execute the sequence
        let mut mock = OrchTestingPoller::new(seq.try_execute().unwrap());
        assert_eq!(Poll::Ready(Ok(())), mock.poll());
    }

    #[test]
    fn all_steps_within_double_nested_steps_seq_are_called() {
        let seq = kyron_testing::prelude::Sequence::new();
        // Create a sequence with double nested branches
        let mock_1 = Box::new(MockActionBuilder::<()>::new().times(1).in_sequence(&seq).build());
        let mock_2 = Box::new(MockActionBuilder::<()>::new().times(1).in_sequence(&seq).build());
        let mock_nested_2a = Box::new(MockActionBuilder::<()>::new().times(1).in_sequence(&seq).build());
        let mock_double_nested_2aa = Box::new(MockActionBuilder::<()>::new().times(1).in_sequence(&seq).build());
        let mock_double_nested_2ab = Box::new(MockActionBuilder::<()>::new().times(1).in_sequence(&seq).build());
        let mock_nested_2b = Box::new(MockActionBuilder::<()>::new().times(1).in_sequence(&seq).build());
        let mut seq = SequenceBuilder::new()
            .with_step(mock_1)
            .with_step(mock_2)
            .with_step(
                SequenceBuilder::new()
                    .with_step(mock_nested_2a)
                    .with_step(
                        SequenceBuilder::new()
                            .with_step(mock_double_nested_2aa)
                            .with_step(mock_double_nested_2ab)
                            .build(),
                    )
                    .with_step(mock_nested_2b)
                    .build(),
            )
            .build();

        // Execute the sequence
        let mut mock = OrchTestingPoller::new(seq.try_execute().unwrap());
        assert_eq!(Poll::Ready(Ok(())), mock.poll());
    }

    #[test]
    fn step_with_err_terminates_immediately() {
        let seq = kyron_testing::prelude::Sequence::new();
        // Create a sequence that contains an error
        let mock_ok = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq)
                .build(),
        );
        let user_err = ActionExecError::UserError(UserErrValue::from(42));
        let mock_err_1 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Err(user_err))
                .in_sequence(&seq)
                .build(),
        );
        let mock_err_2 = Box::new(MockActionBuilder::<()>::new().times(0).in_sequence(&seq).build());
        let mut seq = SequenceBuilder::new()
            .with_step(mock_ok)
            .with_step(mock_err_1)
            .with_step(mock_err_2)
            .build();

        // Execute the sequence
        let mut mock = OrchTestingPoller::new(seq.try_execute().unwrap());
        assert_eq!(Poll::Ready(Err(user_err)), mock.poll());
    }

    #[test]
    fn step_with_err_in_nested_branch_terminates_immediately() {
        let seq = kyron_testing::prelude::Sequence::new();
        // Create a sequence that contains an error step within a nested branch
        let mock_1 = Box::new(MockActionBuilder::<()>::new().times(1).in_sequence(&seq).build());
        let mock_2 = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq)
                .build(),
        );
        let mock_nested_2a = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Ok(()))
                .in_sequence(&seq)
                .build(),
        );
        let mock_nested_err = Box::new(
            MockActionBuilder::<()>::new()
                .will_once_return(Err(ActionExecError::NonRecoverableFailure))
                .in_sequence(&seq)
                .build(),
        );
        let mock_nested_2b = Box::new(MockActionBuilder::<()>::new().times(0).in_sequence(&seq).build());
        let mut seq = SequenceBuilder::new()
            .with_step(mock_1)
            .with_step(mock_2)
            .with_step(
                SequenceBuilder::new()
                    .with_step(mock_nested_2a)
                    .with_step(mock_nested_err)
                    .with_step(mock_nested_2b)
                    .build(),
            )
            .build();

        // Execute the sequence
        let mut mock = OrchTestingPoller::new(seq.try_execute().unwrap());
        assert_eq!(Poll::Ready(Err(ActionExecError::NonRecoverableFailure)), mock.poll());
    }

    #[test]
    fn test_unique_tags_for_actions() {
        let seq = kyron_testing::prelude::Sequence::new();
        let mock1 = Box::new(MockActionBuilder::<()>::new().in_sequence(&seq).build());
        let mock2 = Box::new(MockActionBuilder::<()>::new().in_sequence(&seq).build());

        let seq1 = SequenceBuilder::new().with_step(mock1).build();
        let seq2 = SequenceBuilder::new().with_step(mock2).build();

        assert_ne!(seq1.base.tag, seq2.base.tag);
    }
}
