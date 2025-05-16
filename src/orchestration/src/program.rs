// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use crate::actions::action::*;
use std::{
    fmt::Debug,
    future::Future,
    pin::Pin,
    time::{Duration, Instant},
};

use async_runtime::scheduler::join_handle::JoinHandle;
use foundation::prelude::CommonErrors;
use logging_tracing::prelude::*;

///
/// Whole description to Task Chain is delivered via this instance. It shall hold all actions that build as Task Chain
///
pub struct Program {
    name: String,

    action: Option<Box<dyn ActionTrait>>,
    startup_hook: Option<Box<dyn ActionTrait>>,
    shutdown_hook: Option<Box<dyn ActionTrait>>,
    shutdown_ntf: Option<Box<dyn ActionTrait>>,

    interval: Option<Duration>,
}

impl Debug for Program {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Program - {}", self.name)?;

        if let Some(hook) = self.startup_hook.as_ref() {
            writeln!(f, "Startup hook:")?;
            hook.dbg_fmt(1, f)?;
        }

        writeln!(f, "Body:")?;
        self.action.as_ref().unwrap().dbg_fmt(1, f)
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct ProgramStats<'a> {
    iteration: usize,
    prog_name: &'a str,
    iteration_time: Duration,
}
pub struct ProgramBuilder(Program);

impl ProgramBuilder {
    pub fn new(name: &str) -> Self {
        Self(Program {
            name: name.to_string(),
            action: None,
            startup_hook: None,
            shutdown_hook: None,
            shutdown_ntf: None,
            interval: None,
        })
    }

    pub fn with_body(mut self, action: Box<dyn ActionTrait>) -> Self {
        self.0.action = Some(action);
        self
    }

    pub fn with_startup_hook(mut self, action: Box<dyn ActionTrait>) -> Self {
        // TODO: Should this be only async fn and not actions ?
        self.0.startup_hook = Some(action);
        self
    }

    pub fn with_shutdown_hook(mut self, action: Box<dyn ActionTrait>) -> Self {
        // TODO: Should this be only async fn and not actions ?
        self.0.shutdown_hook = Some(action);
        self
    }

    pub fn with_shutdown_notification(mut self, shutdown: Box<dyn ActionTrait>) -> Self {
        // TODO: Should this be only async fn and not actions ?
        self.0.shutdown_ntf = Some(shutdown);
        self
    }

    ///
    /// When set, each program iteration will align to this `interval`.
    /// ATTENTION: Currently setting this will BLOCK a thread where the program is `run*` due to blocking sleep usage. Work to resolve this is in progress.
    ///
    pub fn with_cycle_time(mut self, interval: Duration) -> Self {
        self.0.interval = Some(interval);
        self
    }

    pub fn build(mut self) -> Program {
        let mut provider = ActionRuntimeInfoProvider::default();

        if let Some(hook) = self.0.startup_hook.as_mut() {
            hook.fill_runtime_info(&mut provider);
        }

        self.0
            .action
            .as_mut()
            .expect("Body must be set for program!")
            .fill_runtime_info(&mut provider);

        assert!(self.0.shutdown_ntf.is_some(), "Shutdown notification has to be provided!");
        self.0
    }
}

impl Program {
    ///
    /// Shall start running a task chain in a `loop`. This means that once TaskChain finishes, it will start from beginning until requested to stop.
    ///
    pub async fn run(&mut self) {
        self.run_internal(None).await;
    }

    ///
    /// Shall start running a task chain `N` times
    ///
    pub async fn run_n(&mut self, iter_cnt: usize) {
        self.run_internal(Some(iter_cnt)).await;
    }

    ///
    /// Should notify program to stop executing as soon as possible.
    ///
    pub fn stop() {
        todo!()
    }

    async fn run_internal(&mut self, times: Option<usize>) {
        let mut ntf = async_runtime::spawn_from_boxed(self.shutdown_ntf.as_mut().unwrap().execute());

        if let Some(ref mut hook) = self.startup_hook {
            let _ = hook.execute().await;
        }

        let requested_iteration_cnt = times.unwrap_or_default();

        let mut iteration = 0_usize;

        while times.is_none() || iteration < requested_iteration_cnt {
            let mut diff = Duration::new(0, 0);

            let stats = ProgramStats {
                iteration,
                prog_name: self.name.as_str(),
                iteration_time: diff,
            };

            trace!(meta = ?stats, "Iteration started");

            let start_time = Instant::now();

            let task_chain = self.action.as_mut().unwrap().execute();
            let mut handle = async_runtime::spawn_from_boxed(task_chain);

            let wait_any = AnyHandle {
                fut1: &mut handle, // 0
                fut2: &mut ntf,    // 1
            };
            let res = wait_any.await;

            match res {
                Ok(v) => {
                    if v == 1 {
                        info!("Received shutdown request, stopping iterations");
                        break;
                    }
                }
                Err(_) => todo!("We currently do not handler errors from a chain, this is wip!"),
            }

            let end_time = Instant::now();

            diff = end_time.duration_since(start_time);
            iteration += 1;

            let stats = ProgramStats {
                iteration,
                prog_name: self.name.as_str(),
                iteration_time: diff,
            };

            trace!( meta = ?stats, "Iteration completed");

            self.align_to_cycle_time(diff).await;
        }

        if let Some(ref mut hook) = self.shutdown_hook {
            let _ = hook.execute().await;
        }
    }

    async fn align_to_cycle_time(&mut self, iteration_time: Duration) {
        if let Some(ref interval) = self.interval {
            if iteration_time < *interval {
                std::thread::sleep(iteration_time.abs_diff(*interval)); // TODO: Change once non blocking sleep is available
            }
        }
    }
}

pub struct AnyHandle<'a> {
    fut1: &'a mut JoinHandle<ActionResult>,
    fut2: &'a mut JoinHandle<ActionResult>,
}

impl Future for AnyHandle<'_> {
    type Output = Result<usize, CommonErrors>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        // we can do unchecking pin because we borrow futures for our whole lifetime so we are sure they will not be gone anywhere once we are using them.

        let mut pined1 = unsafe { Pin::new_unchecked(&mut self.fut1) };

        match pined1.as_mut().poll(cx) {
            std::task::Poll::Ready(ret) => return std::task::Poll::Ready(ret.map_or_else(|_| Err(CommonErrors::GenericError), |_| Ok(0))),
            std::task::Poll::Pending => {}
        }

        let mut pined2 = unsafe { Pin::new_unchecked(&mut self.fut2) };
        match pined2.as_mut().poll(cx) {
            std::task::Poll::Ready(ret) => return std::task::Poll::Ready(ret.map_or_else(|_| Err(CommonErrors::GenericError), |_| Ok(1))),
            std::task::Poll::Pending => {}
        }

        std::task::Poll::Pending
    }
}

// Additional remarks:
//
// - Actions shall follow API aka build pattern (more or less like in Nico code) for construction
// - First we need tree actions: Sequence, Concurrent and Invoke
// - Invoke shall be able to take from user - a function, an async function and object + method for a moment.
//
