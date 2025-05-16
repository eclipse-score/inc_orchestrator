// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use std::{future::Future, sync::Arc};

use crate::{
    box_future,
    scheduler::execution_engine::{ExecutionEngine, ExecutionEngineBuilder},
    AsyncTask, TaskRef,
};

pub struct AsyncRuntimeBuilder {
    engine_builder: ExecutionEngineBuilder,
}

impl Default for AsyncRuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncRuntimeBuilder {
    pub fn new() -> Self {
        Self {
            engine_builder: ExecutionEngineBuilder::new(),
        }
    }

    ///
    /// Adds engine to the runtime using `builder` configured by caller.
    /// >ATTENTION: Currently only single engine supported, feature is WIP
    ///
    pub fn with_engine(mut self, builder: ExecutionEngineBuilder) -> Self {
        self.engine_builder = builder;
        self
    }

    #[allow(clippy::result_unit_err)]
    pub fn build(self) -> Result<AsyncRuntime, ()> {
        Ok(AsyncRuntime {
            engine: self.engine_builder.build(),
        })
    }
}

/// TODO: For now entire file is mockup to build runtime, and let us work. It will evolve and stabilize once we add features need in runtime.
pub struct AsyncRuntime {
    engine: ExecutionEngine,
}

impl AsyncRuntime {
    #[allow(clippy::result_unit_err)]
    pub fn enter_engine<Ret: 'static + Send, T: Future<Output = Ret> + 'static + Send>(&mut self, future: T) -> Result<(), ()> {
        let boxed = box_future(future);
        let scheduler = self.engine.get_async_scheduler();
        let task = Arc::new(AsyncTask::new(boxed, 0, scheduler)); // TODO: worker_id fill
        let task_ref = TaskRef::new(task.clone());

        self.engine.start(task_ref);

        //TODO: followup
        Err(())
    }
}
