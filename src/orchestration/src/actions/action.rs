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

use crate::{actions::invoke::InvokeResult, common::tag::Tag};

use kyron::futures::reusable_box_future::{ReusableBoxFuture, ReusableBoxFuturePool};
use kyron::JoinHandle;
use kyron_foundation::prelude::CommonErrors;

use ::core::{
    fmt::{Debug, Formatter},
    ops::Deref,
};

/// Represents a user-defined error value that can be propagated through the action execution chain.
/// This allows user code to signal specific errors that can be handled or logged by the orchestrator.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct UserErrValue(u64);

impl Deref for UserErrValue {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<u64> for UserErrValue {
    fn from(value: u64) -> Self {
        UserErrValue(value)
    }
}

impl From<UserErrValue> for InvokeResult {
    fn from(value: UserErrValue) -> Self {
        Err(value)
    }
}

#[allow(clippy::from_over_into)]
impl Into<ActionExecError> for UserErrValue {
    fn into(self) -> ActionExecError {
        ActionExecError::UserError(self)
    }
}

/// Enum representing possible errors that can occur during action execution.
///
/// Variants:
/// - `UserError(UserErrValue)`: Indicates an error returned by user code, allowing it to propagate through the chain. It means signature to `Invoke` needs to capture Futures/functions with Result<(), UserErrValue>
/// - `NonRecoverableFailure`: Represents a failure that cannot be recovered from.
/// - `Internal`: Placeholder for internal errors, with potential for expansion as needed.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ActionExecError {
    UserError(UserErrValue),
    NonRecoverableFailure,
    Timeout,
    Internal, // TODO add more errors if needed
}

///
/// Result to indicate the given action status. [`Ok(())`] if everything went fine, Err(ActionExecError) to mark error in execution.
///
pub type ActionResult = Result<(), ActionExecError>;

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
    fn try_execute(&mut self) -> ReusableBoxFutureResult;

    ///
    /// Provide name of the action
    ///
    fn name(&self) -> &'static str;

    ///
    /// Since we store actions behind dyn ActionTrait, we need an API that we can call from program to print constructed representation
    ///
    fn dbg_fmt(&self, nest: usize, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result;
}

pub struct ActionBaseMeta {
    pub tag: Tag,
    pub reusable_future_pool: ReusableBoxFuturePool<ActionResult>,
}

impl Debug for ActionBaseMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> ::core::fmt::Result {
        write!(f, "{:?}", self.tag)
    }
}

/// Represents the state of an action's execution.
/// Can be empty, a future, or a running handle.
pub enum ActionMeta {
    Empty,
    Future(ReusableBoxFuture<ActionResult>),
    Handle(JoinHandle<ActionResult>),
}

impl ActionMeta {
    /// Wraps a future in an ActionMeta.
    pub fn new(fut: ReusableBoxFuture<ActionResult>) -> Self {
        ActionMeta::Future(fut)
    }

    /// Takes the future out of the ActionMeta, leaving it empty.
    pub fn take_future(&mut self) -> Option<ReusableBoxFuture<ActionResult>> {
        match ::core::mem::replace(self, ActionMeta::Empty) {
            ActionMeta::Future(fut) => Some(fut),
            other => {
                *self = other;
                None
            },
        }
    }

    /// Assigns a handle to the ActionMeta, replacing its current state.
    pub fn assign_handle(&mut self, handle: JoinHandle<ActionResult>) {
        *self = ActionMeta::Handle(handle);
    }

    /// Clears the ActionMeta, setting it to Empty.
    pub fn clear(&mut self) {
        *self = ActionMeta::Empty;
    }
}

pub(crate) fn next_unique_action_tag(prefix: &str) -> Tag {
    use ::core::sync::atomic::{AtomicUsize, Ordering};
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}::{}", prefix, id).into()
}
