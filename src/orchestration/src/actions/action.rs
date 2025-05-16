// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use async_runtime::core::types::FutureBox;
use foundation::prelude::*;
use std::fmt::{Debug, Formatter};

///
/// Result to indicate the given action status. [`Ok(())`] if everything went fine, Err(_) to mark error in execution.
///
pub type ActionResult = Result<(), CommonErrors>;

///
/// Action future type alias
///
pub type ActionFuture = FutureBox<ActionResult>;

///
/// Describes action interface that let us build task chain from program. Each action should store it's actions as [`Box<dyn ActionTrait>`] for now
///
pub trait ActionTrait: Send {
    ///
    /// Will be called on each `Program` iteration.
    ///
    /// Key assumptions:
    ///     - should avoid allocation except creation of boxed future
    ///     - each action shall propagate ActionResult down the chain in Future and should immediately stop it's work once Err(_) is reached, propagating it down.
    ///
    fn execute(&mut self) -> ActionFuture;

    ///
    /// Provide debug name of action
    ///
    fn name(&self) -> &'static str;

    ///
    /// Since we store actions behind dyn ActionTrait, we need an API that we can call from program to print constructed representation
    ///
    fn dbg_fmt(&self, nest: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result;

    fn fill_runtime_info(&mut self, p: &mut ActionRuntimeInfoProvider);
}

#[derive(Default)]
pub struct ActionRuntimeInfoProvider {
    id: usize,
}

#[derive(Clone, Copy, Default)]
pub struct ActionRuntimeInfo(usize);

impl Debug for ActionRuntimeInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[allow(clippy::should_implement_trait)]
impl ActionRuntimeInfoProvider {
    pub fn next(&mut self) -> ActionRuntimeInfo {
        self.id += 1;
        ActionRuntimeInfo(self.id)
    }
}

#[derive(Copy, Clone)]
enum NamedIdInner {
    Static(&'static str),
    Empty,
}

#[derive(Copy, Clone)]
pub struct NamedId(NamedIdInner);

impl Default for NamedId {
    fn default() -> Self {
        Self(NamedIdInner::Empty)
    }
}

impl NamedId {
    pub fn new_static(data: &'static str) -> Self {
        Self(NamedIdInner::Static(data))
    }
}

impl Debug for NamedId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            NamedIdInner::Static(arg0) => write!(f, "{:?}", arg0),
            NamedIdInner::Empty => write!(f, "Empty"),
        }
    }
}

#[derive(Clone, Copy)]
pub struct ActionBaseMeta {
    pub named_id: NamedId, // Consider support dynamic string. This has a problem that each iteration we would clone it (sick!), otherwise we can only do unsafe ptr magic as we don't bind action into async
    pub runtime: ActionRuntimeInfo,
}

impl Debug for ActionBaseMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "named_id({:?}), runtime_id({:?})", self.named_id, self.runtime)
    }
}
