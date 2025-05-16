// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use std::{thread, time::Duration};

use async_runtime::{runtime::async_runtime::AsyncRuntimeBuilder, scheduler::execution_engine::*};
use foundation::prelude::*;
use orchestration::prelude::*;

async fn wait_ends() -> ActionResult {
    info!("Test_Event_1 triggered");
    Ok(())
}

async fn wait_ends2() -> ActionResult {
    info!("Test_Event_2 triggered");
    Ok(())
}

async fn wait_ends3() -> ActionResult {
    info!("Test_Event_3 triggered");
    Ok(())
}

// This depepnds on app 'basic'. This receives the events from app 'basic'.
fn main() {
    tracing_subscriber::fmt()
        // .with_span_events(FmtSpan::FULL) // Ensures span open/close events are logged
        .with_target(false) // Optional: Remove module path
        .with_max_level(Level::DEBUG)
        .init();

    let mut runtime = AsyncRuntimeBuilder::new()
        .with_engine(ExecutionEngineBuilder::new().task_queue_size(256).workers(3))
        .build()
        .unwrap();

    {
        // Start the event handling thread.
        // This can be moved inside engine.
        Event::get_instance().lock().unwrap().create_polling_thread();
    }

    let _ = runtime.enter_engine(async {
        let event_name: &str = "Test_Event_1";
        let event_name2: &str = "Test_Event_2";
        let event_name3: &str = "Test_Event_3";
        let mut c = Concurrency::new()
            .with_branch(Sequence::new().with_step(Sync::new(event_name)).with_step(Invoke::from_async(wait_ends)))
            .with_branch(
                Sequence::new()
                    .with_step(Sync::new(event_name2))
                    .with_step(Invoke::from_async(wait_ends2)),
            )
            .with_branch(
                Sequence::new()
                    .with_step(Sync::new(event_name3))
                    .with_step(Invoke::from_async(wait_ends3)),
            );

        let res = c.execute().await;

        println!("Done {:?}", res);
    });

    // wait for some time to allow the engine finishes the last action
    thread::sleep(Duration::new(50, 0));
    println!("Exit.");
}
