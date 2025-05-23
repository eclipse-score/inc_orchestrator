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

use crate::common::tag::Tag;

use async_runtime::futures::reusable_box_future::ReusableBoxFuture;
use foundation::prelude::CommonErrors;

use std::fmt::{Debug, Formatter};

///
/// Result to indicate the given action status. [`Ok(())`] if everything went fine, Err(CommonErrors) to mark error in execution.
///
pub type ActionResult = Result<(), CommonErrors>;

///
/// Result to indicate the acquisition status of the reusable (boxed) future. [`Ok(ReusableBoxFuture<ActionResult>)`] if everything went fine, Err(CommonErrors) to mark error in execution.
///
pub type ReusableBoxFutureResult = Result<ReusableBoxFuture<ActionResult>, CommonErrors>;

///
/// Describes action interface that let us build task chain from program.
///
pub trait ActionTrait: Send {
    ///
    /// Will be called on each `Program` iteration.
    ///
    /// Key assumptions:
    ///     - should avoid allocation due to the usage of reusable boxed future
    ///     - each action shall propagate ActionResult down the chain in Future and should immediately stop it's work once Err(_) is reached, propagating it down.
    ///
    fn execute(&mut self) -> ReusableBoxFutureResult;

    ///
    /// Provide name of the action
    ///
    fn name(&self) -> &'static str;

    ///
    /// Since we store actions behind dyn ActionTrait, we need an API that we can call from program to print constructed representation
    ///
    fn dbg_fmt(&self, nest: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result;
}

#[derive(Clone, Copy)]
pub struct ActionBaseMeta {
    pub tag: Tag,
}

impl Debug for ActionBaseMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.tag)
    }
}
