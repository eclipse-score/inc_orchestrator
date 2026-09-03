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

use super::action::{ActionBaseMeta, ReusableBoxFutureResult};
use crate::events::event_traits::ListenerTrait;
use crate::{
    actions::action::ActionTrait,
    api::design::Design,
    common::{orch_tag::OrchestrationTag, DesignConfig},
    events::events_provider::EventActionType,
};
use kyron::futures::reusable_box_future::*;

pub struct SyncBuilder;

///
/// Builder for creating `Sync` action. `Sync` action is used to synchronize the execution of an orchestration with corresponding events (`Trigger`).
///
impl SyncBuilder {
    /// Creates a new `Sync` action based on the provided orchestration tag.
    pub fn from_tag(tag: &OrchestrationTag, config: &DesignConfig) -> Box<dyn ActionTrait> {
        let sync = tag
            .action_provider()
            .borrow_mut()
            .provide_event(*tag.tag(), EventActionType::Sync, config);
        assert!(
            sync.is_some(),
            "Failed to create Sync Action with tag {:?}, design/deployment errors where not handled properly before or You passing wrong tag.",
            tag,
        );

        sync.unwrap()
    }

    /// Creates a new `Sync` action based on the provided name and design. Useful when you don't have tag that was returned from [`Design`] `register_*` API
    pub fn from_design(name: &str, design: &Design) -> Box<dyn ActionTrait> {
        let tag = design.get_orchestration_tag(name.into());
        assert!(
            tag.is_ok(),
            "Failed to create Sync Action with name '{}', design({:?})/deployment errors where not handled properly before or You passing wrong name.",
            name, design
        );

        Self::from_tag(&tag.unwrap(), design.config())
    }
}

///
/// This action is used to synchronize the execution of an orchestration with corresponding events.
///
pub(crate) struct Sync<T: ListenerTrait + Send + 'static> {
    base: ActionBaseMeta,
    listener: T,
}

impl<T: ListenerTrait + Send> Sync<T> {
    pub(crate) fn new(mut listener: T, future_pool_size: usize) -> Box<Self> {
        const DEFAULT_TAG: &str = "orch::internal::sync";

        Box::new(Self {
            base: ActionBaseMeta {
                tag: super::action::next_unique_action_tag(DEFAULT_TAG),
                reusable_future_pool: ReusableBoxFuturePool::for_value(future_pool_size, listener.next()),
            },
            listener,
        })
    }
}
impl<T: ListenerTrait + Send> ActionTrait for Sync<T> {
    fn try_execute(&mut self) -> ReusableBoxFutureResult {
        let fut = self.listener.next();
        self.base.reusable_future_pool.next(fut)
    }

    fn name(&self) -> &'static str {
        "Sync"
    }

    fn dbg_fmt(&self, nest: usize, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
        writeln!(f, "{}|-{}", " ".repeat(nest), self.name())
    }
}
