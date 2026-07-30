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

//! # Orchestration API Module
//!
//! This module implements the **Orchestration API**, which provides a structured way to manage
//! **designs**, **programs**, and their **deployment** in an orchestrated system. The API is split
//! into three key components:
//!
//! 1. **Design**:
//!    - Provides a way to register all application callables (functions, async functions, objects etc)
//!    - Allow to create an application task flow in `config-by-code` case
//!
//! 2. **Deployment**:
//!    - Provides a way to bind specific application actions to some concrete implementation in current system:
//!      - binding events to Local/Remote/Timers
//!      - configuring certain thread for callables
//!      - ...
//!
//! 3. **Orchestration**:
//!    - Acts as the central API for managing designs and transitioning them into a deployment-ready state.
//!    - Handles the creation of programs and their orchestration.
//!
//! ## Purpose of Orchestration, Design, and Deployment Split
//!
//! The split between **Orchestration**, **Design**, and **Deployment** is intentional and reflects
//! the separation of concerns in the orchestration process:
//!
//! - **Design**: Focuses on the **definition** of the system's components, encapsulating configuration
//!   and metadata for specific parts of the system.
//! - **Deployment**: Focuses on the **execution** of the designs, handling specific details of given system
//! - **Orchestration**: Acts as the **entry point** for managing designs and transitioning them into
//!   deployment, bridging the gap between the design phase and the deployment phase.
//!
//! This separation ensures that each phase of the orchestration process is modular, testable, and maintainable.
//!

use crate::common::tag::{AsTagTrait, Tag};
use crate::events::events_provider::{EventCreator, EventsProvider, ShutdownNotifier};
use crate::{
    api::{deployment::Deployment, design::Design},
    program::Program,
};
use ::core::marker::PhantomData;
use kyron_foundation::prelude::vector_extension::VectorExtension;
use kyron_foundation::prelude::{Vec, Vector};
use kyron_foundation::{containers::growable_vec::GrowableVec, prelude::CommonErrors};
use std::path::Path;
use std::rc::Rc;

pub mod deployment;
pub mod design;

///
/// The main entry point for the Orchestration API.
///
pub type Orchestration<'a> = OrchestrationApi<_EmptyTag>;

pub struct OrchestrationApi<T> {
    designs: GrowableVec<Design>,
    events: EventsProvider,
    shutdown_events: GrowableVec<ShutdownEvent>,
    _p: PhantomData<T>,
}

impl Default for OrchestrationApi<_EmptyTag> {
    fn default() -> Self {
        Self::new()
    }
}

impl OrchestrationApi<_EmptyTag> {
    /// Creates a new instance of the Orchestration API.
    pub fn new() -> OrchestrationApi<_EmptyTag> {
        OrchestrationApi {
            _p: PhantomData,
            designs: GrowableVec::default(),
            events: EventsProvider::default(),
            shutdown_events: GrowableVec::default(),
        }
    }

    ///
    /// Adds a design to the orchestration API.
    ///
    /// # Panics
    ///
    /// Panics if a design with the same ID already exists in the API.
    ///
    /// # Arguments
    ///
    /// * `design` - The design to be added.
    ///
    /// # Returns
    ///
    /// Returns the updated `OrchestrationApi` instance with the new design added.
    pub fn add_design(mut self, design: Design) -> Self {
        assert!(
            !self.designs.iter().any(|d| d.id() == design.id()),
            "Cannot insert same design again"
        );

        self.designs.push(design);
        self
    }

    ///
    /// Finalizes the design phase and transitions the API to a state where it can be used for deployment.
    ///
    /// # Returns
    ///
    /// Returns an `OrchestrationApi` instance with a `_DesignTag` marker, indicating that the design phase is complete.
    ///
    pub fn design_done(self) -> OrchestrationApi<_DesignTag> {
        //TODO: This is temporary and will be removed once iceoryx IPC integration is modified.
        #[cfg(feature = "iceoryx2-ipc")]
        {
            use crate::events::iceoryx::event::Event;
            // Start the event handling thread for Iceoryx IPC
            Event::get_instance().lock().unwrap().create_polling_thread();
        }

        OrchestrationApi {
            _p: PhantomData,
            designs: self.designs,
            events: self.events,
            shutdown_events: GrowableVec::default(),
        }
    }
}

impl OrchestrationApi<_DesignTag> {
    ///
    /// # Returns
    ///
    /// Returns a `Deployment` instance that provides methods to manage the deployment of programs.
    pub fn get_deployment_mut(&mut self) -> Deployment<'_> {
        Deployment::new(self)
    }

    ///
    /// Loads config for orchestration from file
    ///
    pub fn use_config(&mut self, _path: &Path) -> Result<(), CommonErrors> {
        todo!()
    }

    /// Creates programs based on the designs added to the orchestration API.
    ///
    /// # Returns
    ///
    /// Returns an `OrchProgramManager` containing the created programs.
    ///
    /// # Errors
    ///
    /// Returns an error if there is an issue while creating the programs, such as a design not being valid.
    pub fn into_program_manager(mut self) -> Result<OrchProgramManager, CommonErrors> {
        let mut programs = GrowableVec::default();
        while let Some(design) = self.designs.remove(0) {
            design.into_programs(&self.shutdown_events, &mut programs)?
        }

        Ok(OrchProgramManager {
            programs: programs.into(),
            shutdown_events: self.shutdown_events.into(),
        })
    }

    pub(crate) fn register_shutdown_event(&mut self, tag: Tag, creator: EventCreator) -> Result<(), CommonErrors> {
        if tag.find_in_collection(self.shutdown_events.iter()).is_some() {
            Err(CommonErrors::AlreadyDone)
        } else if self.shutdown_events.push(ShutdownEvent { tag, creator }) {
            Ok(())
        } else {
            Err(CommonErrors::NoSpaceLeft)
        }
    }
}

pub struct OrchProgramManager {
    programs: Vec<Program>,
    shutdown_events: Vec<ShutdownEvent>,
}

impl OrchProgramManager {
    /// Moves all programs out of the manager and returns them.
    pub fn get_programs(&mut self) -> Vec<Program> {
        let empty = Vec::new_in_global(0);
        core::mem::replace(&mut self.programs, empty)
    }

    /// Moves the named program out of the manager and returns it.
    pub fn get_program(&mut self, name: &str) -> Option<Program> {
        if let Some((index, _)) = self
            .programs
            .iter()
            .enumerate()
            .find(|(_, program)| program.name == name)
        {
            self.programs.remove(index)
        } else {
            None
        }
    }

    /// Retrieve a shutdown notifier for the given event.
    pub fn get_shutdown_notifier(&self, shutdown_event_tag: Tag) -> Result<Box<dyn ShutdownNotifier>, CommonErrors> {
        if let Some(shutdown_event) = shutdown_event_tag.find_in_collection(self.shutdown_events.iter()) {
            if let Some(shutdown_notifier) = shutdown_event.creator().borrow_mut().create_shutdown_notifier() {
                return Ok(shutdown_notifier);
            } else {
                return Err(CommonErrors::GenericError);
            }
        }

        Err(CommonErrors::NotFound)
    }

    /// Retrieve a shutdown notifier for all shutdown events.
    pub fn get_shutdown_all_notifier(&self) -> Result<Box<dyn ShutdownNotifier>, CommonErrors> {
        let mut shutdown_notifiers = Vec::new_in_global(self.shutdown_events.len());

        for event in self.shutdown_events.iter() {
            if let Some(notifier) = event.creator().borrow_mut().create_shutdown_notifier() {
                let _ = shutdown_notifiers.push(notifier);
            } else {
                return Err(CommonErrors::GenericError);
            }
        }

        Ok(Box::new(ShutdownAllNotifierImpl { shutdown_notifiers }))
    }
}

pub(crate) struct ShutdownEvent {
    tag: Tag,
    creator: EventCreator,
}

impl ShutdownEvent {
    pub fn creator(&self) -> EventCreator {
        Rc::clone(&self.creator)
    }
}

impl AsTagTrait for ShutdownEvent {
    fn as_tag(&self) -> &Tag {
        &self.tag
    }
}

impl AsTagTrait for &ShutdownEvent {
    fn as_tag(&self) -> &Tag {
        &self.tag
    }
}

impl AsTagTrait for &mut ShutdownEvent {
    fn as_tag(&self) -> &Tag {
        &self.tag
    }
}

struct ShutdownAllNotifierImpl {
    shutdown_notifiers: Vec<Box<dyn ShutdownNotifier>>,
}

impl ShutdownNotifier for ShutdownAllNotifierImpl {
    fn shutdown(&mut self) -> crate::prelude::ActionResult {
        for notifier in self.shutdown_notifiers.iter_mut() {
            notifier.shutdown()?
        }

        Ok(())
    }
}

#[doc(hidden)]
pub struct _EmptyTag {}

#[doc(hidden)]
pub struct _DesignTag {}
