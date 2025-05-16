// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use async_runtime::{runtime::async_runtime::AsyncRuntimeBuilder, scheduler::execution_engine::*, spawn, spawn_on_dedicated};
use foundation::prelude::*;
use std::{future::Future, thread, time::Duration};

pub struct X {}

impl Future for X {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        cx.waker().wake_by_ref();

        cx.waker().wake_by_ref();

        cx.waker().wake_by_ref();

        cx.waker().wake_by_ref();

        cx.waker().wake_by_ref();

        cx.waker().wake_by_ref();

        cx.waker().wake_by_ref();

        std::task::Poll::Ready(())
    }
}

fn main() {
    tracing_subscriber::fmt()
        // .with_span_events(FmtSpan::FULL) // Ensures span open/close events are logged
        .with_target(false) // Optional: Remove module path
        .with_max_level(Level::DEBUG)
        .with_thread_ids(true)
        .with_thread_names(true)
        .init();

    let mut runtime = AsyncRuntimeBuilder::new()
        .with_engine(
            ExecutionEngineBuilder::new()
                .task_queue_size(256)
                .workers(3)
                .with_dedicated_worker("dedicated".into()),
        )
        .build()
        .unwrap();

    let _ = runtime.enter_engine(async {
        error!("We do have first enter into runtime ;)");

        let handle = spawn(async {
            error!("And again from one we are in another ;)");

            spawn(async {
                error!("And again from one nested ;)");

                1
            })
            .await
            .unwrap();

            spawn_on_dedicated(
                async {
                    error!("I AM DEDICATED  ;)");

                    spawn(async {
                        error!("And again from one nested from dedicated ;)");

                        1
                    })
                    .await
                    .unwrap();

                    error!("I AM DEDICATED RESTORED  ;)");
                    1
                },
                "dedicated".into(),
            )
            .await
            .unwrap();

            1
        });

        let res = handle.await;
        error!("After await res is {}", res.unwrap());

        let x = X {};

        x.await;
        error!("After multi waker");
    });

    thread::sleep(Duration::new(20, 0));
}
