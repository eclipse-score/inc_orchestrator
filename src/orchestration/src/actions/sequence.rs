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

use async_runtime::core::types::box_future;
use logging_tracing::prelude::*;

use super::action::{ActionBaseMeta, ActionFuture, ActionResult, ActionTrait, NamedId};

pub struct Sequence {
    base: ActionBaseMeta,
    actions: Vec<Box<dyn ActionTrait>>,
}

impl Sequence {
    /// Create a new sequence action
    pub fn new() -> Box<Sequence> {
        Self::new_internal(NamedId::default())
    }

    pub fn new_with_id(id: NamedId) -> Box<Sequence> {
        Self::new_internal(id)
    }

    fn new_internal(named_id: NamedId) -> Box<Sequence> {
        Box::new(Self {
            actions: Vec::new(),
            base: ActionBaseMeta {
                named_id,
                runtime: Default::default(),
            },
        })
    }

    /// Add sequence step
    pub fn with_step(mut self: Box<Self>, action: Box<dyn ActionTrait>) -> Box<Self> {
        self.actions.push(action);
        self
    }

    /// Execute a futures collection and terminates immediately upon error
    async fn execute_impl(meta: ActionBaseMeta, mut futures: Vec<ActionFuture>) -> ActionResult {
        let mut result = Ok(());

        trace!(sequence = ?meta, "Before joining steps");

        for future in futures.iter_mut() {
            result = future.await;
            if result.is_err() {
                // terminate sequence and propagate the error
                error!("error in step!");
                break;
            }
        }

        trace!(sequence = ?meta, "After joining steps");
        result
    }
}

impl ActionTrait for Sequence {
    /// Will be called on each sequence step
    fn execute(&mut self) -> ActionFuture {
        // first collect all futures from the steps
        let futures: std::vec::Vec<_> = self.actions.iter_mut().map(|action| action.execute()).collect();

        // and execute them
        box_future(Sequence::execute_impl(self.base, futures))
    }

    fn name(&self) -> &'static str {
        "Sequence"
    }

    fn dbg_fmt(&self, nest: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let indent = " ".repeat(nest);
        writeln!(f, "{}|-{} - {:?}", indent, self.name(), self.base)?;
        self.actions.iter().try_for_each(|x| {
            writeln!(f, "{} |step", indent)?;
            x.dbg_fmt(nest + 1, f)
        })
    }

    fn fill_runtime_info(&mut self, p: &mut super::action::ActionRuntimeInfoProvider) {
        self.base.runtime = p.next();
        self.actions.iter_mut().for_each(|item| item.fill_runtime_info(p));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::invoke::Invoke;
    use std::task::Poll;
    use testing::prelude::*;

    async fn add_async<const N: usize>() -> ActionResult {
        Ok(())
    }

    fn add_sync<const N: usize>() -> ActionResult {
        Ok(())
    }

    #[test]
    fn test_with_future() {
        let mut seq = Sequence::new()
            .with_step(Invoke::from_async(add_async::<42>))
            .with_step(Invoke::from_fn(add_sync::<2>));

        // create mock with the resulting future
        let mut mock = TestingFuturePoller::new(seq.execute());
        assert_eq!(Poll::Ready(Ok(())), mock.poll());
    }

    #[test]
    fn test_with_boxed_future() {
        let mut seq = Sequence::new()
            .with_step(Invoke::from_async(add_async::<1729>))
            .with_step(Invoke::from_fn(add_sync::<711>));

        // create mock with the resulting future as boxed future
        let mut mock = TestingFuturePoller::new(Box::pin(seq.execute()));
        assert_eq!(Poll::Ready(Ok(())), mock.poll());
    }

    #[test]
    fn test_nested() {
        let mut seq = Sequence::new()
            .with_step(Invoke::from_async(add_async::<42>))
            .with_step(
                Sequence::new()
                    .with_step(Invoke::from_async(add_async::<1729>))
                    .with_step(
                        Sequence::new()
                            .with_step(Invoke::from_fn(add_sync::<4242>))
                            .with_step(Invoke::from_async(add_async::<2424>)),
                    )
                    .with_step(Invoke::from_fn(add_sync::<1881>)),
            )
            .with_step(Invoke::from_async(add_async::<2>))
            .with_step(Invoke::from_fn(add_sync::<711>));

        // execute the action and check the results
        let mut mock = TestingFuturePoller::new(seq.execute());
        assert_eq!(Poll::Ready(Ok(())), mock.poll());
    }

    #[test]
    fn test_with_trackable_waker() {
        let mut seq = Sequence::new().with_step(Invoke::from_fn(add_sync::<42>));

        // use trackable waker for polling
        let waker = TrackableWaker::new();

        // execute the action and check the results
        let mut mock = TestingFuturePoller::new(seq.execute());
        assert_eq!(Poll::Ready(Ok(())), mock.poll_with_waker(&waker.get_waker()));
        assert_eq!(1, waker.get_waker_ref_count());
    }
}
