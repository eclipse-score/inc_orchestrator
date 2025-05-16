// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use async_runtime::{runtime::async_runtime::AsyncRuntimeBuilder, scheduler::execution_engine::*, spawn};
use foundation::prelude::*;
use std::{thread, time::Duration};

//
// This is a test program and the following should happen:
// * create an AsyncRuntime with 1 worker and a queue size of 8.
// * spawn a task on the runtime that spawns 14 tasks on the same worker again
// * tasks 0 to 7 are added to the workers local queue
// * when task 8 is added, there is no more room in the local queue, resulting in half of the queue
//   being pushed to the global queue, namely tasks 0, 1, 2, 3
// * tasks 9, 10, 11 are pushed to the local queue
// * when task 12 is added, tasks 4, 5, 6, 7 are again pushed to global queue
// * task 13 is pushed to the local queue
// * tasks 8, 9, 10, 11, 12, 13 are now in the local queue and executed in this order
// * worker is beginning to search for work: tasks 0 to 7 are taken from global queue to the
//   workers local queue
// * tasks 0, 1, 2, 3, 4, 5, 6, 7 are executed in this order
// * worker is again searching for work but it does not find anything this time
// * worker is going to sleep
//
fn main() {
    tracing_subscriber::fmt()
        // .with_span_events(FmtSpan::FULL) // Ensures span open/close events are logged
        .with_target(false) // Optional: Remove module path
        .with_max_level(Level::DEBUG)
        .init();

    let mut runtime = AsyncRuntimeBuilder::new()
        .with_engine(ExecutionEngineBuilder::new().task_queue_size(8).workers(1))
        .build()
        .unwrap();

    let _ = runtime.enter_engine(async {
        info!("We have just entered runtime.");

        for i in 0..14 {
            let _handle = spawn(async move {
                info!("Message from task {}", i);
                i
            });
        }
    });

    thread::sleep(Duration::new(2, 0));
}
