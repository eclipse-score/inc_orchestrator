// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use crate::core::runtime_seq_acc::RuntimeSequentialAccess;

use super::action::{ActionResult, ActionTrait};
use async_runtime::core::types::box_future;

type FunctionType = fn() -> ActionResult;

///
/// TODO: Capturing object (owned, arced, closure) seems to be tricky from runtime perspective. So in theory, we will never call it in parallel, but if there is a timeout and user resumes program, we can come back to the same Invoke while other was not yet aborted (clogged, whatever).
/// I have a feeling that we shall build handling mechanism for that in Orchestration. The same issue will happen with Computation and we need common strategy. Options:
///  - panic on such case
///  - trigger global error handler and stop program without resume possibility
///  - block to wait until previous call did not finished - this may retrigger error handler again and again  ?
///
pub struct Invoke {} // Dummy struct to create fake namespace Invoke:: to hide types for impl details

impl Invoke {
    ///
    /// Creates Invoke action from plain function pointer
    ///
    pub fn from_fn(func: FunctionType) -> Box<dyn ActionTrait> {
        Box::new(InvokeFn { action: func })
    }

    ///
    /// Creates Invoke action from plain async function
    ///
    pub fn from_async<F, Fut>(function: F) -> Box<dyn ActionTrait>
    where
        F: FnMut() -> Fut + 'static + Send,
        Fut: Future<Output = ActionResult> + 'static + Send,
    {
        Box::new(InvokeAsyncFn { action: function })
    }

    pub fn from_not_owned<T: 'static>(
        obj: Arc<RuntimeSequentialAccess<T>>,
        method: fn(&mut Arc<RuntimeSequentialAccess<T>>) -> ActionResult,
    ) -> Box<dyn ActionTrait> {
        Box::new(InvokeOwnedObject {
            object: Arc::new(RuntimeSequentialAccess::new(obj)),
            method: FnType::Mut(method),
        })
    }

    pub fn from_arc<T: 'static + Send>(obj: Arc<Mutex<T>>, method: fn(&mut T) -> ActionResult) -> Box<dyn ActionTrait> {
        Box::new(InvokeArc {
            object: obj,
            method: FnType::Mut(method),
        })
    }

    pub fn from_arc_mtx<T: 'static + Send, F, Fut>(obj: Arc<Mutex<T>>, method: F) -> Box<dyn ActionTrait>
    where
        F: FnMut(Arc<Mutex<T>>) -> Fut + 'static + Send,
        Fut: Future<Output = ActionResult> + 'static + Send,
    {
        Box::new(InvokeArcMtx { object: obj, method })
    }

    /// # Safety
    ///
    /// TODO: This is marked as unsafe right now (as precaution indicator). User need to know restrictions before doing this. Check [`InvokeOwnedObject`].
    pub unsafe fn from_owned<T: 'static>(obj: T, method: fn(&T) -> ActionResult) -> Box<dyn ActionTrait> {
        Box::new(InvokeOwnedObject {
            object: Arc::new(RuntimeSequentialAccess::new(obj)),
            method: FnType::Immut(method),
        })
    }

    /// # Safety
    ///
    /// TODO: This is marked as unsafe right now (as precaution indicator). User need to know restrictions before doing this. Check [`InvokeOwnedObject`].
    pub unsafe fn from_owned_mut<T: 'static>(obj: T, method: fn(&mut T) -> ActionResult) -> Box<dyn ActionTrait> {
        Box::new(InvokeOwnedObject {
            object: Arc::new(RuntimeSequentialAccess::new(obj)),
            method: FnType::Mut(method),
        })
    }
}

struct InvokeFn {
    action: FunctionType,
}

impl InvokeFn {
    async fn internal_future(action: FunctionType) -> ActionResult {
        action()
    }
}

impl ActionTrait for InvokeFn {
    fn execute(&mut self) -> super::action::ActionFuture {
        box_future(Self::internal_future(self.action))
    }

    fn name(&self) -> &'static str {
        "Invoke"
    }

    fn dbg_fmt(&self, nest: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let indent = " ".repeat(nest);
        writeln!(f, "{}|-{}", indent, self.name())
    }

    fn fill_runtime_info(&mut self, _p: &mut super::action::ActionRuntimeInfoProvider) {}
}

struct InvokeAsyncFn<T, Fut>
where
    T: FnMut() -> Fut + 'static + Send,
    Fut: Future<Output = ActionResult> + 'static + Send,
{
    action: T,
}

impl<T, Fut> ActionTrait for InvokeAsyncFn<T, Fut>
where
    T: FnMut() -> Fut + 'static + Send,
    Fut: Future<Output = ActionResult> + 'static + Send,
{
    fn execute(&mut self) -> super::action::ActionFuture {
        let fut = (self.action)();
        box_future(fut)
    }

    fn name(&self) -> &'static str {
        "Invoke"
    }

    fn dbg_fmt(&self, nest: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let indent = " ".repeat(nest);
        writeln!(f, "{}|-{}", indent, self.name())
    }

    fn fill_runtime_info(&mut self, _p: &mut super::action::ActionRuntimeInfoProvider) {}
}

enum FnType<T> {
    Mut(fn(&mut T) -> ActionResult),
    Immut(fn(&T) -> ActionResult),
}

impl<T> Clone for FnType<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Mut(arg0) => Self::Mut(*arg0),
            Self::Immut(arg0) => Self::Immut(*arg0),
        }
    }
}

///
/// TODO: Decide if we are going for it....
/// If we do own object, the Orchestration assures that it will never be called in parallel, so we can take over immutable leash from Arc.
/// Safety:
///     - The T implementer must ensure that provides method will not block. Otherwise next attempt for this execution will panic!
///
struct InvokeOwnedObject<T: 'static> {
    object: Arc<RuntimeSequentialAccess<T>>,
    method: FnType<T>,
}

impl<T> InvokeOwnedObject<T> {
    async fn internal_future(object: Arc<RuntimeSequentialAccess<T>>, method: FnType<T>) -> ActionResult {
        let mut guard = object.lock();

        match method {
            FnType::Mut(callable) => (callable)(&mut guard),
            FnType::Immut(callable) => (callable)(&guard),
        }
    }
}

impl<T> ActionTrait for InvokeOwnedObject<T> {
    fn execute(&mut self) -> super::action::ActionFuture {
        // TODO: We need to panic here if we have it timeout on this method and program returned back here. This is bad implementation on user side.
        if self.object.is_locked() {
            panic!("This action is still running, we cannot let it run again. This clearly indicate bug int user application layer")
        }

        box_future(Self::internal_future(self.object.clone(), self.method.clone()))
    }

    fn name(&self) -> &'static str {
        "Invoke"
    }

    fn dbg_fmt(&self, nest: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let indent = " ".repeat(nest);
        writeln!(f, "{}|-{}", indent, self.name())
    }

    fn fill_runtime_info(&mut self, _p: &mut super::action::ActionRuntimeInfoProvider) {}
}

struct InvokeArc<T: 'static> {
    object: Arc<Mutex<T>>,
    method: FnType<T>,
}

impl<T: 'static> InvokeArc<T> {
    async fn internal_future(object: Arc<Mutex<T>>, method: FnType<T>) -> ActionResult {
        let mut guard = object.lock().unwrap();

        match method {
            FnType::Mut(callable) => (callable)(&mut guard),
            FnType::Immut(callable) => (callable)(&guard),
        }
    }
}

impl<T: 'static + Send> ActionTrait for InvokeArc<T> {
    fn execute(&mut self) -> super::action::ActionFuture {
        box_future(Self::internal_future(self.object.clone(), self.method.clone()))
    }

    fn name(&self) -> &'static str {
        "Invoke"
    }

    fn dbg_fmt(&self, nest: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let indent = " ".repeat(nest);
        writeln!(f, "{}|-{}", indent, self.name())
    }

    fn fill_runtime_info(&mut self, _p: &mut super::action::ActionRuntimeInfoProvider) {}
}

struct InvokeArcMtx<T: 'static, F, Fut>
where
    F: FnMut(Arc<Mutex<T>>) -> Fut + 'static + Send,
    Fut: Future<Output = ActionResult> + 'static + Send,
{
    object: Arc<Mutex<T>>,
    method: F,
}

impl<T: 'static + Send, F, Fut> ActionTrait for InvokeArcMtx<T, F, Fut>
where
    F: FnMut(Arc<Mutex<T>>) -> Fut + 'static + Send,
    Fut: Future<Output = ActionResult> + 'static + Send,
{
    fn execute(&mut self) -> super::action::ActionFuture {
        box_future((self.method)(self.object.clone()))
    }

    fn name(&self) -> &'static str {
        "Invoke"
    }

    fn dbg_fmt(&self, nest: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let indent = " ".repeat(nest);
        writeln!(f, "{}|-{}", indent, self.name())
    }

    fn fill_runtime_info(&mut self, _p: &mut super::action::ActionRuntimeInfoProvider) {}
}
