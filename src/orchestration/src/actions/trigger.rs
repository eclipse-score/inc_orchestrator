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

use crate::{
    actions::action::ActionTrait,
    api::design::Design,
    common::orch_tag::OrchestrationTag,
    events::{event_traits::NotifierTrait, events_provider::EventActionType},
};

use super::action::{ActionBaseMeta, ReusableBoxFutureResult};
use crate::common::tag::Tag;

use async_runtime::futures::reusable_box_future::*;

pub struct TriggerBuilder;

///
/// Builder for creating `Trigger` action. `Trigger` action is used to trigger the execution of an orchestration with corresponding events (`Sync`).
///
impl TriggerBuilder {
    /// Creates a new `Trigger` action based on the provided orchestration tag.
    pub fn from_tag(tag: &OrchestrationTag) -> Box<dyn ActionTrait> {
        let trigger = tag.action_provider().borrow_mut().provide_event(*tag.key(), EventActionType::Trigger);
        assert!(
            trigger.is_some(),
            "Failed to create Trigger Action with tag {:?}, design/deployment errors where not handled properly before or You passing wrong tag.",
            tag,
        );

        trigger.unwrap()
    }

    /// Creates a new `Trigger` action based on the provided name and design. Useful when you don't have tag that was returned from [`Design`] `register_*` API
    pub fn from_design(name: &str, design: &Design) -> Box<dyn ActionTrait> {
        let tag = design.get_orchestration_tag(name.into());
        assert!(
            tag.is_ok(),
            "Failed to create Trigger Action with name '{}', design/deployment errors where not handled properly before or You passing wrong name.",
            name
        );

        Self::from_tag(&tag.unwrap())
    }
}

///
/// This action is used to send events(notifications) to corresponding `Sync` actions
///
pub(crate) struct Trigger<T: NotifierTrait + Send + 'static> {
    base: ActionBaseMeta,
    notifier: T,
}

impl<T: NotifierTrait + Send> Trigger<T> {
    pub(crate) fn new(notifier: T) -> Box<Self> {
        const DEFAULT_TAG: &str = "orch::internal::trigger";

        Box::new(Self {
            base: ActionBaseMeta {
                tag: Tag::from_str_static(DEFAULT_TAG),
                reusable_future_pool: ReusableBoxFuturePool::new(1, notifier.notify(0)),
            },
            notifier,
        })
    }
}
impl<T: NotifierTrait + Send> ActionTrait for Trigger<T> {
    fn try_execute(&mut self) -> ReusableBoxFutureResult {
        let fut = self.notifier.notify(0);
        self.base.reusable_future_pool.next(fut)
    }

    fn name(&self) -> &'static str {
        "Trigger"
    }

    fn dbg_fmt(&self, nest: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}|-{}", " ".repeat(nest), self.name())
    }
}
