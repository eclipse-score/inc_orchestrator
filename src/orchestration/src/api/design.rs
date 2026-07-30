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
    actions::{ifelse::IfElseCondition, invoke},
    api::ShutdownEvent,
    common::{orch_tag::OrchestrationTag, tag::Tag, DesignConfig},
    prelude::InvokeResult,
    program::{Program, ProgramBuilder},
    program_database::ProgramDatabase,
};
use ::core::fmt::Debug;
use ::core::future::Future;
use kyron_foundation::{containers::growable_vec::GrowableVec, prelude::CommonErrors};
use std::sync::{Arc, Mutex};

pub type ProgramTag = Tag;
pub type DesignTag = Tag;

///
/// Design is a container for Application developer to register all it's components (functions, events, conditions, etc.)
/// and orchestrations (programs) in `config-by-code` approach.  If `config-by-file` is used, user does not need to use
/// [`Design::add_program`] since it will be loaded from the file. Read more in [`crate::api::Orchestration`].
///
pub struct Design {
    id: DesignTag,
    pub(crate) config: DesignConfig,
    pub(crate) db: ProgramDatabase,
    programs: GrowableVec<ProgramData>,
}

impl Debug for Design {
    fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
        f.debug_struct("Design").field("id", &self.id).finish()
    }
}

impl Design {
    /// Creates a new `Design` instance with the given identifier and configuration `parameters`.
    pub fn new(id: DesignTag, config: DesignConfig) -> Self {
        const DEFAULT_PROGRAMS_CNT: usize = 1;
        Design {
            id,
            config,
            db: ProgramDatabase::new(config),
            programs: GrowableVec::new(DEFAULT_PROGRAMS_CNT),
        }
    }

    /// Returns the unique identifier for this design.
    pub fn id(&self) -> Tag {
        self.id
    }

    /// Returns the configuration parameters for this design.
    pub fn config(&self) -> &DesignConfig {
        &self.config
    }

    /// Registers a function as an invoke action.
    pub fn register_invoke_fn(
        &self,
        tag: Tag,
        action: invoke::InvokeFunctionType,
    ) -> Result<OrchestrationTag, CommonErrors> {
        self.db.register_invoke_fn(tag, action)
    }

    /// Registers an async function as an invoke action
    pub fn register_invoke_async<A, F>(&self, tag: Tag, action: A) -> Result<OrchestrationTag, CommonErrors>
    where
        A: Fn() -> F + 'static + Send + Clone,
        F: Future<Output = InvokeResult> + 'static + Send,
    {
        self.db.register_invoke_async(tag, action)
    }

    /// Registers a method on an object as an invoke action.
    pub fn register_invoke_method<T: 'static + Send>(
        &self,
        tag: Tag,
        object: Arc<Mutex<T>>,
        method: fn(&mut T) -> InvokeResult,
    ) -> Result<OrchestrationTag, CommonErrors> {
        self.db.register_invoke_method(tag, object, method)
    }

    /// Registers an async method on an object as an invoke action.
    pub fn register_invoke_method_async<T, M, F>(
        &self,
        tag: Tag,
        object: Arc<Mutex<T>>,
        method: M,
    ) -> Result<OrchestrationTag, CommonErrors>
    where
        T: 'static + Send,
        M: Fn(Arc<Mutex<T>>) -> F + 'static + Send + Clone,
        F: Future<Output = InvokeResult> + 'static + Send,
    {
        self.db.register_invoke_method_async(tag, object, method)
    }

    /// Registers an event in the design and returns an [`OrchestrationTag`] that can be used to reference this event in programs.
    pub fn register_event(&self, tag: Tag) -> Result<OrchestrationTag, CommonErrors> {
        self.db.register_event(tag)
    }

    /// Registers a condition for an IfElse action.
    pub fn register_if_else_condition<C>(&mut self, tag: Tag, condition: C) -> Result<OrchestrationTag, CommonErrors>
    where
        C: IfElseCondition + Send + Sync + 'static,
    {
        self.db.register_if_else_arc_condition(tag, Arc::new(condition))
    }

    /// Registers an arc condition for an IfElse action.
    pub fn register_if_else_arc_condition<C>(
        &mut self,
        tag: Tag,
        condition: Arc<C>,
    ) -> Result<OrchestrationTag, CommonErrors>
    where
        C: IfElseCondition + Send + Sync + 'static,
    {
        self.db.register_if_else_arc_condition(tag, condition)
    }

    /// Registers an arc mutex condition for an IfElse action.
    pub fn register_if_else_arc_mutex_condition<C>(
        &mut self,
        tag: Tag,
        condition: Arc<Mutex<C>>,
    ) -> Result<OrchestrationTag, CommonErrors>
    where
        C: IfElseCondition + Send + 'static,
    {
        self.db.register_if_else_arc_mutex_condition(tag, condition)
    }

    /// Fetches an [`OrchestrationTag`] for a given tag, which can be used to reference the orchestration in programs.
    pub fn get_orchestration_tag(&self, tag: Tag) -> Result<OrchestrationTag, CommonErrors> {
        self.db.get_orchestration_tag(tag)
    }

    /// Adds a program to the design. The program is created using the provided closure, which receives a mutable reference to the design.
    pub fn add_program<F>(&mut self, name: &'static str, program_creator: F)
    where
        F: FnOnce(&mut Self, &mut ProgramBuilder) -> Result<(), CommonErrors> + 'static,
    {
        self.programs.push(ProgramData::new(name, Box::new(program_creator)));
    }

    pub(crate) fn has_any_programs(&self) -> bool {
        !self.programs.is_empty()
    }

    pub(super) fn into_programs(
        mut self,
        shutdown_events: &GrowableVec<ShutdownEvent>,
        container: &mut GrowableVec<Program>,
    ) -> Result<(), CommonErrors> {
        while let Some(program_data) = self.programs.remove(0) {
            let mut builder = ProgramBuilder::new(program_data.0);
            (program_data.1)(&mut self, &mut builder)?;
            container.push(builder.build(shutdown_events, self.config())?);
        }

        Ok(())
    }
}

type ProgramBuilderFn = Box<dyn FnOnce(&mut Design, &mut ProgramBuilder) -> Result<(), CommonErrors>>;

#[allow(dead_code)]
pub(super) struct ProgramData(&'static str, ProgramBuilderFn);

impl ProgramData {
    pub(super) fn new(name: &'static str, program: ProgramBuilderFn) -> Self {
        Self(name, program)
    }
}

#[cfg(test)]
mod tests {
    // Tests are disabled in Miri due to limitations of using OS calls that are done in Iceroxy2 backend.
    // Currently we do not have any constructor that can inject IPC provider (subject to change in the near future).

    use crate::actions::action::UserErrValue;

    use super::*;

    #[test]
    fn design_creation() {
        let id = Tag::from_str_static("design1");
        let config = DesignConfig::default();

        let design = Design::new(id, config);

        assert_eq!(design.id(), id);
        assert_eq!(*design.config(), config);
    }

    fn action() -> Result<(), UserErrValue> {
        Ok(())
    }

    #[test]
    fn register_invoke_fn_success() {
        let id = Tag::from_str_static("design1");
        let config = DesignConfig::default();
        let design = Design::new(id, config);

        let tag = Tag::from_str_static("invoke_fn");

        let result = design.register_invoke_fn(tag, action);

        assert!(result.is_ok());
        let orchestration_tag = result.unwrap();
        assert_eq!(*orchestration_tag.tag(), tag);
    }

    #[test]
    fn register_invoke_fn_duplicate() {
        let id = Tag::from_str_static("design1");
        let config = DesignConfig::default();
        let design = Design::new(id, config);

        let tag = Tag::from_str_static("invoke_fn");

        // Register the function once
        let result = design.register_invoke_fn(tag, action);
        assert!(result.is_ok());

        // Attempt to register the same function again
        let duplicate_result = design.register_invoke_fn(tag, action);
        assert!(duplicate_result.is_err());
        assert_eq!(duplicate_result.unwrap_err(), CommonErrors::AlreadyDone);
    }

    #[test]
    fn get_orchestration_tag_success() {
        let id = Tag::from_str_static("design1");
        let config = DesignConfig::default();
        let design = Design::new(id, config);

        let tag = Tag::from_str_static("orchestration_tag");

        // Register the function
        let _ = design.register_invoke_fn(tag, action);

        // Retrieve the orchestration tag
        let orchestration_tag = design.get_orchestration_tag(tag);
        assert!(orchestration_tag.is_ok());
        assert_eq!(*orchestration_tag.unwrap().tag(), tag);
    }

    #[test]
    fn get_orchestration_tag_not_found() {
        let id = Tag::from_str_static("design1");
        let config = DesignConfig::default();
        let design = Design::new(id, config);

        let tag = Tag::from_str_static("non_existent_tag");

        // Attempt to retrieve a non-existent orchestration tag
        let orchestration_tag = design.get_orchestration_tag(tag);
        assert!(orchestration_tag.is_err());
    }

    #[test]
    fn into_programs_preserves_insertion_order() {
        use crate::prelude::Invoke;

        let id = Tag::from_str_static("design1");
        let config = DesignConfig::default();
        let mut design = Design::new(id, config);

        let run_tag = design.register_invoke_fn(Tag::from_str_static("run_action"), action).unwrap();

        let tag = run_tag.clone();
        design.add_program("first", move |design, builder| {
            builder.with_run_action(Invoke::from_tag(&tag, design.config()));
            Ok(())
        });
        let tag = run_tag.clone();
        design.add_program("second", move |design, builder| {
            builder.with_run_action(Invoke::from_tag(&tag, design.config()));
            Ok(())
        });
        design.add_program("third", move |design, builder| {
            builder.with_run_action(Invoke::from_tag(&run_tag, design.config()));
            Ok(())
        });

        let mut container = GrowableVec::default();
        design.into_programs(&GrowableVec::default(), &mut container).unwrap();

        assert_eq!(container.len(), 3);
        assert_eq!(container[0].name(), "first");
        assert_eq!(container[1].name(), "second");
        assert_eq!(container[2].name(), "third");
    }

    // TODO add more tests once new Program skeleton is created
}
