// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use super::action::{ActionBaseMeta, ActionFuture, ActionResult, ActionTrait, NamedId};
use super::event::Event;
use async_runtime::core::types::box_future;
use logging_tracing::prelude::*;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Copy, Clone)]
/// SyncInner is needed to implement Future and get Waker which will be triggered from event polling thread.
struct SyncInner {
    event_id: usize,
}
impl Future for SyncInner {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let waker_clone = cx.waker().clone();
        let event_received = Event::get_instance().lock().unwrap().wake_on_event(self.event_id, waker_clone);
        if event_received {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

/// Sync action
pub struct Sync {
    base: ActionBaseMeta,
    event_id: usize,
}

impl Sync {
    pub fn new(event_name: &str) -> Box<Self> {
        Self::new_internal(event_name, NamedId::default())
    }

    pub fn new_with_id(event_name: &str, id: NamedId) -> Box<Self> {
        Self::new_internal(event_name, id)
    }

    fn new_internal(event_name: &str, named_id: NamedId) -> Box<Self> {
        Box::new(Self {
            event_id: Event::get_instance().lock().unwrap().create_listener(event_name),
            base: ActionBaseMeta {
                named_id,
                runtime: Default::default(),
            },
        })
    }

    async fn execute_impl(meta: ActionBaseMeta, event_id: usize) -> ActionResult {
        trace!(sync = ?meta, "Awaiting sync event");
        SyncInner { event_id }.await;
        trace!(sync = ?meta, "Awaited sync event");

        Ok(())
    }
}

impl ActionTrait for Sync {
    fn execute(&mut self) -> ActionFuture {
        box_future(Sync::execute_impl(self.base, self.event_id))
    }

    fn name(&self) -> &'static str {
        "Sync"
    }

    fn dbg_fmt(&self, nest: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let indent = " ".repeat(nest);
        writeln!(f, "{}|-{} - {:?} event_id({})", indent, self.name(), self.base, self.event_id)
    }

    fn fill_runtime_info(&mut self, p: &mut super::action::ActionRuntimeInfoProvider) {
        self.base.runtime = p.next();
    }
}
