// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use async_runtime::core::types::box_future;
use foundation::prelude::CommonErrors;
use logging_tracing::prelude::*;

use super::action::{ActionBaseMeta, ActionFuture, ActionResult, ActionTrait, NamedId};
use super::event::Event;

pub struct Trigger {
    base: ActionBaseMeta,
    event_name: String,
}

impl Trigger {
    /// Create a trigger action for triggering a single event
    pub fn new(event_name: &str) -> Box<Self> {
        Self::new_internal(event_name, NamedId::default())
    }

    pub fn new_with_id(event_name: &str, id: NamedId) -> Box<Self> {
        Self::new_internal(event_name, id)
    }

    fn new_internal(event_name: &str, named_id: NamedId) -> Box<Self> {
        // create the new notifier in the singleton object
        Event::get_instance().lock().unwrap().create_notifier(event_name);

        Box::new(Self {
            event_name: event_name.to_string(),
            base: ActionBaseMeta {
                named_id,
                runtime: Default::default(),
            },
        })
    }

    /// Execute the trigger future
    async fn execute_impl(meta: ActionBaseMeta, event_name: String) -> ActionResult {
        // we cannot call this earlier because Notifier<ipc::Service> is not Send
        match Event::get_instance().lock().unwrap().trigger_event(&event_name) {
            Ok(_) => {
                trace!(trigger= ?meta, "Triggered event {}", event_name);
                Ok(())
            }
            Err(_) => {
                error!(trigger= ?meta, "error: triggering event id: {}", event_name);
                // return GenericError since we do not have a direct 1:1 mapping
                Err(CommonErrors::GenericError)
            }
        }
    }
}

impl ActionTrait for Trigger {
    /// Will be called on each trigger action
    fn execute(&mut self) -> ActionFuture {
        box_future(Trigger::execute_impl(self.base, self.event_name.clone()))
    }

    fn name(&self) -> &'static str {
        "Trigger"
    }

    fn dbg_fmt(&self, nest: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let indent = " ".repeat(nest);
        writeln!(f, "{}|-{} - {:?} event_name({})", indent, self.name(), self.base, self.event_name)
    }

    fn fill_runtime_info(&mut self, p: &mut super::action::ActionRuntimeInfoProvider) {
        self.base.runtime = p.next();
    }
}
