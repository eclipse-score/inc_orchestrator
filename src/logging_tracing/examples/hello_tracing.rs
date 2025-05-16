// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use logging_tracing::prelude::*;
use std::thread;

/// The Hello World routine
#[instrument]
fn hello_world() {
    trace!("Trace level test");
    debug!("Trace level test");
    info!("{:?}:inside test_function!", thread::current().id());
    warn!("Trace level test");
    error!("Trace level test");
    println!("Hello World!");
}

fn trace_example() {
    //Switch to TraceMode and initialize

    let mut tracer = TracingLibraryBuilder::new()
        .global_log_level(Level::TRACE)
        .enable_tracing(TraceScope::AppScope)
        .enable_logging(true)
        .build();

    tracer.init_log_trace();

    let _span = tracer.create_span();

    // Run the example trace
    hello_world();
    // You can trace messages using info! or debug! here (in TraceMode)
    info!("This is a traced message");
}

fn main() {
    //log_example();

    // OR
    trace_example();
}
