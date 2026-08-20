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

use super::action::{
    ActionBaseMeta, ActionExecError, ActionResult, ActionTrait, ReusableBoxFutureResult, UserErrValue,
};
use crate::{
    api::design::Design,
    common::{orch_tag::OrchestrationTag, tag::Tag, DesignConfig},
};
use ::core::future::Future;

use kyron::{
    common::types::UniqueWorkerId, futures::reusable_box_future::ReusableBoxFuture,
    futures::reusable_box_future::ReusableBoxFuturePool,
};
use kyron_foundation::prelude::CommonErrors;
use std::sync::{Arc, Mutex};

#[cfg(not(any(test, feature = "runtime-api-mock")))]
use kyron::safety::spawn_from_reusable_on_dedicated;
#[cfg(any(test, feature = "runtime-api-mock"))]
use kyron::testing::mock::spawn_from_reusable_on_dedicated;

/// A result of an invoke action.
pub type InvokeResult = Result<(), UserErrValue>;
pub(crate) type InvokeFunctionType = fn() -> InvokeResult;

pub struct Invoke {}

impl Invoke {
    /// Create an invoke action out of an orchestration tag.
    pub fn from_tag(tag: &OrchestrationTag, config: &DesignConfig) -> Box<dyn ActionTrait> {
        tag.action_provider()
            .borrow_mut()
            .provide_invoke(*tag.tag(), config)
            .unwrap()
    }

    pub fn from_design(name: &str, design: &Design) -> Box<dyn ActionTrait> {
        let tag = design.get_orchestration_tag(name.into());
        assert!(
            tag.is_ok(),
            "Failed to create invoke with name '{}', design/deployment errors where not handled properly before or You passing wrong name. ({:?})",
            name,
            tag
        );

        Self::from_tag(&tag.unwrap(), design.config())
    }

    pub(crate) fn from_fn(
        tag: Tag,
        action: InvokeFunctionType,
        worker_id: Option<UniqueWorkerId>,
        config: &DesignConfig,
    ) -> Box<dyn ActionTrait> {
        Box::new(InvokeFn {
            action,
            action_future_pool: ReusableBoxFuturePool::for_value(
                config.max_concurrent_action_executions,
                InvokeFn::action_future(action),
            ),
            worker_id,
            base: ActionBaseMeta {
                tag,
                reusable_future_pool: ReusableBoxFuturePool::for_value(
                    config.max_concurrent_action_executions,
                    InvokeFn::spawn_action(InstantOrSpawn::None),
                ),
            },
        })
    }

    pub(crate) fn from_async<A, F>(
        tag: Tag,
        action: A,
        worker_id: Option<UniqueWorkerId>,
        config: &DesignConfig,
    ) -> Box<dyn ActionTrait>
    where
        A: Fn() -> F + 'static + Send,
        F: Future<Output = InvokeResult> + 'static + Send,
    {
        let future = action();

        Box::new(InvokeAsync {
            action,
            action_future_pool: ReusableBoxFuturePool::for_value(
                config.max_concurrent_action_executions,
                InvokeAsync::<A, F>::action_future(future),
            ),
            worker_id,
            base: ActionBaseMeta {
                tag,
                reusable_future_pool: ReusableBoxFuturePool::for_value(
                    config.max_concurrent_action_executions,
                    InvokeAsync::<A, F>::spawn_action(InstantOrSpawn::None),
                ),
            },
        })
    }

    pub(crate) fn from_method<T: 'static + Send>(
        tag: Tag,
        object: Arc<Mutex<T>>,
        method: fn(&mut T) -> InvokeResult,
        worker_id: Option<UniqueWorkerId>,
        config: &DesignConfig,
    ) -> Box<dyn ActionTrait> {
        Box::new(InvokeMethod {
            object: Arc::clone(&object),
            method,
            action_future_pool: ReusableBoxFuturePool::for_value(
                config.max_concurrent_action_executions,
                InvokeMethod::<T>::action_future(Arc::clone(&object), method),
            ),
            worker_id,
            base: ActionBaseMeta {
                tag,
                reusable_future_pool: ReusableBoxFuturePool::for_value(
                    config.max_concurrent_action_executions,
                    InvokeMethod::<T>::spawn_action(InstantOrSpawn::None),
                ),
            },
        })
    }

    pub(crate) fn from_method_async<T, M, F>(
        tag: Tag,
        object: Arc<Mutex<T>>,
        method: M,
        worker_id: Option<UniqueWorkerId>,
        config: &DesignConfig,
    ) -> Box<dyn ActionTrait>
    where
        T: 'static + Send,
        M: Fn(Arc<Mutex<T>>) -> F + 'static + Send,
        F: Future<Output = InvokeResult> + 'static + Send,
    {
        let future = (method)(Arc::clone(&object));

        Box::new(InvokeMethodAsync {
            object,
            method,
            action_future_pool: ReusableBoxFuturePool::for_value(
                config.max_concurrent_action_executions,
                InvokeMethodAsync::<T, M, F>::action_future(future),
            ),
            worker_id,
            base: ActionBaseMeta {
                tag,
                reusable_future_pool: ReusableBoxFuturePool::for_value(
                    config.max_concurrent_action_executions,
                    InvokeMethodAsync::<T, M, F>::spawn_action(InstantOrSpawn::None),
                ),
            },
        })
    }
}

fn invoke_result_into_action_result(result: InvokeResult) -> ActionResult {
    result.map_err(|err| err.into())
}

enum InstantOrSpawn<I> {
    None,
    Instant(I),
    Spawn(ReusableBoxFuture<ActionResult>, UniqueWorkerId),
}

struct InvokeFn {
    action: InvokeFunctionType,
    action_future_pool: ReusableBoxFuturePool<ActionResult>,
    worker_id: Option<UniqueWorkerId>,
    base: ActionBaseMeta,
}

impl InvokeFn {
    async fn action_future(action: InvokeFunctionType) -> ActionResult {
        invoke_result_into_action_result(action())
    }

    async fn spawn_action(instant_or_spawn: InstantOrSpawn<InvokeFunctionType>) -> ActionResult {
        match instant_or_spawn {
            InstantOrSpawn::None => Ok(()),
            InstantOrSpawn::Instant(action) => invoke_result_into_action_result(action()),
            InstantOrSpawn::Spawn(future, worker_id) => {
                match spawn_from_reusable_on_dedicated(future, worker_id).await {
                    Ok(result) => result,
                    Err(_) => Err(ActionExecError::Internal),
                }
            },
        }
    }
}

impl ActionTrait for InvokeFn {
    fn try_execute(&mut self) -> ReusableBoxFutureResult {
        if let Some(worker_id) = self.worker_id {
            match self.action_future_pool.next(InvokeFn::action_future(self.action)) {
                Ok(future) => self
                    .base
                    .reusable_future_pool
                    .next(InvokeFn::spawn_action(InstantOrSpawn::Spawn(future, worker_id))),
                Err(_) => Err(CommonErrors::GenericError),
            }
        } else {
            self.base
                .reusable_future_pool
                .next(InvokeFn::spawn_action(InstantOrSpawn::Instant(self.action)))
        }
    }

    fn name(&self) -> &'static str {
        "Invoke"
    }

    fn dbg_fmt(&self, nest: usize, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
        writeln!(f, "{}|-{}", " ".repeat(nest), self.name())
    }
}

struct InvokeAsync<A, F>
where
    A: Fn() -> F + 'static + Send,
    F: Future<Output = InvokeResult> + 'static + Send,
{
    action: A,
    action_future_pool: ReusableBoxFuturePool<ActionResult>,
    worker_id: Option<UniqueWorkerId>,
    base: ActionBaseMeta,
}

impl<A, F> InvokeAsync<A, F>
where
    A: Fn() -> F + 'static + Send,
    F: Future<Output = InvokeResult> + 'static + Send,
{
    async fn action_future(future: F) -> ActionResult {
        invoke_result_into_action_result(future.await)
    }

    async fn spawn_action(instant_or_spawn: InstantOrSpawn<F>) -> ActionResult {
        match instant_or_spawn {
            InstantOrSpawn::None => Ok(()),
            InstantOrSpawn::Instant(action) => invoke_result_into_action_result(action.await),
            InstantOrSpawn::Spawn(future, worker_id) => {
                match spawn_from_reusable_on_dedicated(future, worker_id).await {
                    Ok(result) => result,
                    Err(_) => Err(ActionExecError::Internal),
                }
            },
        }
    }
}

impl<A, F> ActionTrait for InvokeAsync<A, F>
where
    A: Fn() -> F + 'static + Send,
    F: Future<Output = InvokeResult> + 'static + Send,
{
    fn try_execute(&mut self) -> ReusableBoxFutureResult {
        if let Some(worker_id) = self.worker_id {
            match self
                .action_future_pool
                .next(InvokeAsync::<A, F>::action_future((self.action)()))
            {
                Ok(future) => {
                    self.base
                        .reusable_future_pool
                        .next(InvokeAsync::<A, F>::spawn_action(InstantOrSpawn::Spawn(
                            future, worker_id,
                        )))
                },
                Err(_) => Err(CommonErrors::GenericError),
            }
        } else {
            self.base
                .reusable_future_pool
                .next(InvokeAsync::<A, F>::spawn_action(InstantOrSpawn::Instant((self
                    .action)(
                ))))
        }
    }

    fn name(&self) -> &'static str {
        "InvokeAsync"
    }

    fn dbg_fmt(&self, nest: usize, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
        writeln!(f, "{}|-{}", " ".repeat(nest), self.name())
    }
}

type InvokeMethodType<T> = fn(&mut T) -> InvokeResult;

struct InvokeMethod<T: 'static + Send> {
    object: Arc<Mutex<T>>,
    method: InvokeMethodType<T>,
    action_future_pool: ReusableBoxFuturePool<ActionResult>,
    worker_id: Option<UniqueWorkerId>,
    base: ActionBaseMeta,
}

impl<T: 'static + Send> InvokeMethod<T> {
    async fn action_future(object: Arc<Mutex<T>>, method: InvokeMethodType<T>) -> ActionResult {
        let mut object = object.lock().unwrap();
        invoke_result_into_action_result(method(&mut object))
    }

    async fn spawn_action(instant_or_spawn: InstantOrSpawn<(Arc<Mutex<T>>, InvokeMethodType<T>)>) -> ActionResult {
        match instant_or_spawn {
            InstantOrSpawn::None => Ok(()),
            InstantOrSpawn::Instant((object, method)) => {
                let mut object = object.lock().unwrap();
                invoke_result_into_action_result(method(&mut object))
            },
            InstantOrSpawn::Spawn(future, worker_id) => {
                match spawn_from_reusable_on_dedicated(future, worker_id).await {
                    Ok(result) => result,
                    Err(_) => Err(ActionExecError::Internal),
                }
            },
        }
    }
}

impl<T: 'static + Send> ActionTrait for InvokeMethod<T> {
    fn try_execute(&mut self) -> ReusableBoxFutureResult {
        if let Some(worker_id) = self.worker_id {
            match self
                .action_future_pool
                .next(InvokeMethod::<T>::action_future(Arc::clone(&self.object), self.method))
            {
                Ok(future) => {
                    self.base
                        .reusable_future_pool
                        .next(InvokeMethod::<T>::spawn_action(InstantOrSpawn::Spawn(
                            future, worker_id,
                        )))
                },
                Err(_) => Err(CommonErrors::GenericError),
            }
        } else {
            self.base
                .reusable_future_pool
                .next(InvokeMethod::<T>::spawn_action(InstantOrSpawn::Instant((
                    Arc::clone(&self.object),
                    self.method,
                ))))
        }
    }

    fn name(&self) -> &'static str {
        "InvokeAsync"
    }

    fn dbg_fmt(&self, nest: usize, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
        writeln!(f, "{}|-{}", " ".repeat(nest), self.name())
    }
}

struct InvokeMethodAsync<T, M, F>
where
    T: 'static + Send,
    M: FnMut(Arc<Mutex<T>>) -> F + 'static + Send,
    F: Future<Output = InvokeResult> + 'static + Send,
{
    object: Arc<Mutex<T>>,
    method: M,
    action_future_pool: ReusableBoxFuturePool<ActionResult>,
    worker_id: Option<UniqueWorkerId>,
    base: ActionBaseMeta,
}

impl<T, M, F> InvokeMethodAsync<T, M, F>
where
    T: 'static + Send,
    M: FnMut(Arc<Mutex<T>>) -> F + 'static + Send,
    F: Future<Output = InvokeResult> + 'static + Send,
{
    async fn action_future(future: F) -> ActionResult {
        invoke_result_into_action_result(future.await)
    }

    async fn spawn_action(instant_or_spawn: InstantOrSpawn<F>) -> ActionResult {
        match instant_or_spawn {
            InstantOrSpawn::None => Ok(()),
            InstantOrSpawn::Instant(future) => invoke_result_into_action_result(future.await),
            InstantOrSpawn::Spawn(future, worker_id) => {
                match spawn_from_reusable_on_dedicated(future, worker_id).await {
                    Ok(result) => result,
                    Err(_) => Err(ActionExecError::Internal),
                }
            },
        }
    }
}

impl<T, M, F> ActionTrait for InvokeMethodAsync<T, M, F>
where
    T: 'static + Send,
    M: FnMut(Arc<Mutex<T>>) -> F + 'static + Send,
    F: Future<Output = InvokeResult> + 'static + Send,
{
    fn try_execute(&mut self) -> ReusableBoxFutureResult {
        if let Some(worker_id) = self.worker_id {
            match self
                .action_future_pool
                .next(InvokeMethodAsync::<T, M, F>::action_future((self.method)(Arc::clone(
                    &self.object,
                )))) {
                Ok(future) => self
                    .base
                    .reusable_future_pool
                    .next(InvokeMethodAsync::<T, M, F>::spawn_action(InstantOrSpawn::Spawn(
                        future, worker_id,
                    ))),
                Err(_) => Err(CommonErrors::GenericError),
            }
        } else {
            self.base
                .reusable_future_pool
                .next(InvokeMethodAsync::<T, M, F>::spawn_action(InstantOrSpawn::Instant(
                    (self.method)(Arc::clone(&self.object)),
                )))
        }
    }
    fn name(&self) -> &'static str {
        "InvokeAsync"
    }

    fn dbg_fmt(&self, nest: usize, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
        writeln!(f, "{}|-{}", " ".repeat(nest), self.name())
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use crate::common::DesignConfig;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_fn() {
        let config = DesignConfig::default();

        fn test() -> super::InvokeResult {
            Ok(())
        }

        // Capture the same action multiple times.
        let mut action1 = super::Invoke::from_fn("tag".into(), test, None, &config);
        let mut action2 = super::Invoke::from_fn("tag".into(), test, None, &config);
        // Execute the same invoke multiple times.
        assert!(action1.try_execute().is_ok());
        assert!(action1.try_execute().is_ok());
        assert!(action2.try_execute().is_ok());
        assert!(action2.try_execute().is_ok());
    }

    #[test]
    fn test_async() {
        let config = DesignConfig::default();

        async fn test() -> super::InvokeResult {
            Ok(())
        }

        // Capture the same action multiple times.
        let mut action1 = super::Invoke::from_async("tag".into(), test, None, &config);
        let mut action2 = super::Invoke::from_async("tag".into(), test, None, &config);
        // Execute the same invoke multiple times.
        assert!(action1.try_execute().is_ok());
        assert!(action1.try_execute().is_ok());
        assert!(action2.try_execute().is_ok());
        assert!(action2.try_execute().is_ok());
    }

    #[test]
    fn test_method() {
        let config = DesignConfig::default();

        struct TestObject {}

        impl TestObject {
            fn test_method(&mut self) -> super::InvokeResult {
                Err(0xcafe_u64.into())
            }
        }

        let object = Arc::new(Mutex::new(TestObject {}));

        // Capture the same action multiple times.
        let mut action1 = super::Invoke::from_method(
            "tag".into(),
            Arc::clone(&object),
            TestObject::test_method,
            None,
            &config,
        );
        let mut action2 = super::Invoke::from_method(
            "tag".into(),
            Arc::clone(&object),
            TestObject::test_method,
            None,
            &config,
        );
        // Execute the same invoke multiple times.
        assert!(action1.try_execute().is_ok());
        assert!(action1.try_execute().is_ok());
        assert!(action2.try_execute().is_ok());
        assert!(action2.try_execute().is_ok());
    }

    #[test]
    fn test_method_async() {
        let config = DesignConfig::default();

        struct TestObject {}

        async fn test_method(object: Arc<Mutex<TestObject>>) -> super::InvokeResult {
            let _guard = object.lock().unwrap();

            Err(0xcafe_u64.into())
        }

        let object = Arc::new(Mutex::new(TestObject {}));

        // Capture the same action multiple times.
        let mut action1 =
            super::Invoke::from_method_async("tag".into(), Arc::clone(&object), test_method, None, &config);
        let mut action2 =
            super::Invoke::from_method_async("tag".into(), Arc::clone(&object), test_method, None, &config);
        // Execute the same invoke multiple times.
        assert!(action1.try_execute().is_ok());
        assert!(action1.try_execute().is_ok());
        assert!(action2.try_execute().is_ok());
        assert!(action2.try_execute().is_ok());
    }
}
