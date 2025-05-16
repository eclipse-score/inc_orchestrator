// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use std::{future::Future, pin::Pin};

use super::action::{ActionBaseMeta, ActionFuture, ActionResult, ActionTrait, NamedId};

use async_runtime::{core::types::*, scheduler::join_handle::JoinHandle};
use logging_tracing::prelude::*;

type ActionMeta = (Option<ActionFuture>, Option<JoinHandle<ActionResult>>);
type ActionsVectorType = Vec<ActionMeta>;

///
/// Branches execution flow into separate, independent paths using a [`async_runtime::spawn`] which will issue an concurrent execution depending on runtime workers configuration
///
pub struct Concurrency {
    base: ActionBaseMeta,
    actions: Vec<Box<dyn ActionTrait>>,
}

///
/// Internal future that can wait for multiple [`JoinHandle`] and return Ready once all are done or any fails canceling all other handles.
///
struct ConcurrencyJoin<'a> {
    handles: &'a mut [ActionMeta],
}

impl<'a> ConcurrencyJoin<'a> {
    fn new(actions: &'a mut ActionsVectorType) -> Self {
        Self { handles: actions }
    }
}

impl Future for ConcurrencyJoin<'_> {
    type Output = ActionResult;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let mut is_done = true;

        for (_, handle_opt) in &mut *self.handles {
            match handle_opt {
                Some(handle) => {
                    let p = Pin::new(handle);
                    let res = p.poll(cx);

                    match res {
                        std::task::Poll::Ready(_) => {
                            *handle_opt = None; // Clear out handle that is already done

                            // TODO: Missing abort logic & error propagation for now
                        }
                        std::task::Poll::Pending => {
                            is_done = false;
                        }
                    }
                }
                None => {
                    //Nothing to do, this handle was already consumed
                }
            }
        }

        if is_done {
            std::task::Poll::Ready(Ok(()))
        } else {
            std::task::Poll::Pending
        }
    }
}

impl Concurrency {
    pub fn new() -> Box<Concurrency> {
        Self::new_internal(NamedId::default())
    }

    pub fn new_with_id(id: NamedId) -> Box<Concurrency> {
        Self::new_internal(id)
    }

    fn new_internal(named_id: NamedId) -> Box<Concurrency> {
        Box::new(Self {
            actions: Vec::new(),
            base: ActionBaseMeta {
                named_id,
                runtime: Default::default(),
            },
        })
    }

    ///
    /// Adds new branch to flow execution
    ///
    pub fn with_branch(mut self: Box<Self>, action: Box<dyn ActionTrait>) -> Box<Self> {
        self.actions.push(action);
        self
    }

    //
    // PRIVATE
    //

    async fn internal_future(meta: ActionBaseMeta, mut futures_collection: ActionsVectorType) -> ActionResult {
        for (fut_opt, handle_opt) in &mut futures_collection {
            match fut_opt.take() {
                Some(fut) => {
                    *handle_opt = Some(async_runtime::spawn_from_boxed(fut));
                }
                None => {
                    panic!("We shall never be here since we always create Some(fut) at beginning")
                }
            }
        }

        trace!(concurrent = ?meta, "Before joining branches");

        let joined = ConcurrencyJoin::new(&mut futures_collection);
        let res = joined.await;

        trace!(concurrent = ?meta, "After joining branches");
        res
    }
}

impl ActionTrait for Concurrency {
    fn execute(&mut self) -> ActionFuture {
        //TODO: Here we have allocation, discuss how we can avoid it, but it does not influence a TaskFlow, it only influence each run of it (since allocation is done before each program iteration)
        let collected_futures: ActionsVectorType = self.actions.iter_mut().map(|action| (Some(action.execute()), None)).collect();

        box_future(Self::internal_future(self.base, collected_futures))
    }

    fn name(&self) -> &'static str {
        "Concurrency"
    }

    fn dbg_fmt(&self, nest: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let indent = " ".repeat(nest);
        writeln!(f, "{}|-{} - {:?}", indent, self.name(), self.base)?;
        self.actions.iter().try_for_each(|x| {
            writeln!(f, "{} |branch", indent)?;
            x.dbg_fmt(nest + 1, f)
        })
    }

    fn fill_runtime_info(&mut self, p: &mut super::action::ActionRuntimeInfoProvider) {
        self.base.runtime = p.next();
        self.actions.iter_mut().for_each(|item| item.fill_runtime_info(p));
    }
}
