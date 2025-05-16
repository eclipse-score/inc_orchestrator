// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use std::ops::Deref;
use std::sync::{self, Arc};

use foundation::containers::spmc_queue::*;
use foundation::prelude::*;

use crate::scheduler::task::async_task::TaskRef;

pub type TaskStealQueue = Arc<SpmcStealQueue<TaskRef>>;
pub type StealHandle = TaskStealQueue;

pub fn create_steal_queue(size: usize) -> TaskStealQueue {
    Arc::new(SpmcStealQueue::new(size as u32))
}

pub(super) const WORKER_STATE_SLEEPING_CV: u8 = 0b00000000;
pub(super) const WORKER_STATE_NOTIFIED: u8 = 0b00000001; // Was asked to wake-up
pub(super) const WORKER_STATE_EXECUTING: u8 = 0b00000011;

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct UniqueWorkerId(u64);

#[allow(clippy::from_over_into)]
impl Into<UniqueWorkerId> for &str {
    fn into(self) -> UniqueWorkerId {
        // TODO: for now use DJB2 hash
        let mut hash: u64 = 5381;
        for byte in self.bytes() {
            hash = (hash.wrapping_shl(5)).wrapping_add(hash).wrapping_add(byte as u64);
        }

        UniqueWorkerId(hash)
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub(crate) struct WorkerId {
    unique_id: UniqueWorkerId,
    engine_id: u8,   // in which engine worker is working
    worker_id: u8,   // whats the worker id in this engine
    typ: WorkerType, // whats the type
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub(crate) enum WorkerType {
    Async,
    Dedicated,
}

impl WorkerId {
    pub(crate) fn new(unique_id: UniqueWorkerId, engine_id: u8, worker_id: u8, typ: WorkerType) -> Self {
        Self {
            unique_id,
            engine_id,
            worker_id,
            typ,
        }
    }

    // Getters for all fields
    pub(crate) fn unique_id(&self) -> UniqueWorkerId {
        self.unique_id
    }

    #[allow(dead_code)]
    pub(crate) fn engine_id(&self) -> u8 {
        self.engine_id
    }

    pub(crate) fn worker_id(&self) -> u8 {
        self.worker_id
    }

    #[allow(dead_code)]
    pub(crate) fn typ(&self) -> WorkerType {
        self.typ
    }
}

#[derive(Clone)]
pub(crate) struct WorkerInteractor {
    inner: Arc<WorkerInteractorInnner>,
}

impl Deref for WorkerInteractor {
    type Target = WorkerInteractorInnner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

unsafe impl Send for WorkerInteractor {}

pub(crate) struct WorkerState(pub FoundationAtomicU8);

impl WorkerState {
    pub(crate) fn new(val: u8) -> Self {
        Self(FoundationAtomicU8::new(val))
    }
}

pub(crate) struct WorkerInteractorInnner {
    pub(crate) steal_handle: StealHandle,

    pub(super) state: WorkerState,
    pub(super) mtx: std::sync::Mutex<()>,
    pub(super) cv: std::sync::Condvar,
}

impl WorkerInteractor {
    pub fn new(handle: StealHandle) -> Self {
        Self {
            inner: Arc::new(WorkerInteractorInnner {
                mtx: std::sync::Mutex::new(()),
                cv: std::sync::Condvar::new(),
                steal_handle: handle,
                state: WorkerState::new(WORKER_STATE_EXECUTING),
            }),
        }
    }

    pub(crate) fn unpark(&self) {
        match self.state.0.swap(WORKER_STATE_NOTIFIED, sync::atomic::Ordering::SeqCst) {
            WORKER_STATE_NOTIFIED => {
                //Nothing to do, already someone did if for us
            }
            WORKER_STATE_SLEEPING_CV => {
                drop(self.mtx.lock().unwrap()); // Synchronize so worker does not lose the notification in before it goes into a wait
                self.cv.notify_one(); // notify without lock in case we get preempted by woken thread
            }
            WORKER_STATE_EXECUTING => {
                //Nothing to do, looks like we already running
            }
            _ => {
                panic!("Inconsistent/not handled state when unparking worker!")
            }
        };
    }
}
