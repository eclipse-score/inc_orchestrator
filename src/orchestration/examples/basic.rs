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

use async_runtime::{runtime::async_runtime::AsyncRuntimeBuilder, scheduler::execution_engine::*};
use foundation::prelude::*;
use logging_tracing::{TraceScope, TracingLibraryBuilder};
use orchestration::{
    api::{design::Design, Orchestration},
    common::DesignConfig,
    prelude::*,
};

mod common;
use common::register_all_common_into_design;

fn example_component_design() -> Result<Design, CommonErrors> {
    let mut design = Design::new("ExampleDesign".into(), DesignConfig::default());

    register_all_common_into_design(&mut design)?; // Register our common functions, events, etc

    // Create a program with some actions
    design.add_program("ExampleDesignProgram", move |design_instance, builder| {
        builder.with_run_action(
            SequenceBuilder::new()
                .with_step(Invoke::from_design("test1_sync_func", &design_instance))
                .with_step(Invoke::from_design("test2_sync_func", &design_instance))
                .with_step(
                    ConcurrencyBuilder::new()
                        .with_branch(Invoke::from_design("test3_sync_func", &design_instance))
                        .with_branch(Invoke::from_design("test4_sync_func", &design_instance))
                        .build(),
                )
                .with_step(Invoke::from_design("test4_async_func", &design_instance))
                .build(),
        );

        Ok(())
    });

    Ok(design)
}

fn main() {
    // Setup any logging framework you want to use.
    let mut logger = TracingLibraryBuilder::new()
        .global_log_level(Level::TRACE)
        .enable_tracing(TraceScope::AppScope)
        .enable_logging(true)
        .build();

    logger.init_log_trace();

    // Create runtime
    let (builder, _engine_id) = AsyncRuntimeBuilder::new().with_engine(
        ExecutionEngineBuilder::new()
            .task_queue_size(256)
            .workers(2)
            .with_dedicated_worker("dedicated_worker1".into()),
    );

    let mut runtime = builder.build().unwrap();

    // Build Orchestration

    let mut orch = Orchestration::new()
        .add_design(example_component_design().expect("Failed to create design"))
        .design_done();

    // Deployment part - specify event details
    let mut deployment = orch.get_deployment_mut();

    // Bind a invoke action to a dedicated worker
    deployment
        .bind_invoke_to_worker("test1_sync_func".into(), "dedicated_worker1".into())
        .expect("Failed to bind invoke action to worker");

    // Create programs
    let mut programs = orch.create_programs().unwrap();

    // Put programs into runtime and run them
    let _ = runtime.block_on(async move {
        let _ = programs.programs.pop().unwrap().run_n(1).await;
        info!("Program finished running.");
        Ok(0)
    });

    info!("Exit.");
}
