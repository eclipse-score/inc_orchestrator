// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

///
/// The calling async task gives up for other queued tasks to run and the self is immediately put into notifid state to run.
/// User shall not assume any order of scheduling based on yield.
///
pub async fn yield_now() {
    // Yield implementation
    struct Yield {
        yielded: bool,
    }

    // Future implementation for yield.
    impl Future for Yield {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.yielded {
                return Poll::Ready(());
            }

            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    Yield { yielded: false }.await;
}
