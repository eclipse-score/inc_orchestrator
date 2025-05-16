// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::scheduler_mt::*;
use super::task::async_task::TaskRef;
use super::workers::dedicated_worker::DedicatedWorker;
use super::workers::worker::Worker;
use super::workers::worker_types::*;

use foundation::containers::growable_vec::GrowableVec;
use foundation::containers::mpmc_queue::MpmcQueue;
use foundation::containers::trigger_queue::TriggerQueue;
use foundation::prelude::*;
use foundation::threading::thread_wait_barrier::ThreadWaitBarrier;

pub struct ExecutionEngine {
    async_workers: Vec<Worker>,
    async_queues: Vec<TaskStealQueue>,
    async_scheduler: Arc<AsyncScheduler>,

    dedicated_workers: Vec<DedicatedWorker>,
    dedicated_scheduler: Arc<DedicatedScheduler>,
}

impl ExecutionEngine {
    pub(crate) fn start(&mut self, entry_task: TaskRef) {
        {
            //TODO: Total hack, injecting task before we run any async_workers so they will pick it
            let pc = self.async_queues[0].get_local().unwrap();
            pc.push(entry_task, &self.async_scheduler.global_queue)
                .unwrap_or_else(|_| panic!("Failed to enter runtime while pushing init task"));
        }

        let start_barrier = Arc::new(ThreadWaitBarrier::new(
            self.async_workers.len() as u32 + self.dedicated_workers.len() as u32,
        ));

        self.async_workers.iter_mut().for_each(|w| {
            w.start(
                self.async_scheduler.clone(),
                self.dedicated_scheduler.clone(),
                start_barrier.get_notifier().unwrap(),
            );
        });

        self.dedicated_workers.iter_mut().for_each(|w| {
            w.start(
                self.async_scheduler.clone(),
                self.dedicated_scheduler.clone(),
                start_barrier.get_notifier().unwrap(),
            );
        });

        debug!("Engine starts waiting for workers to be ready");

        let res = start_barrier.wait_for_all(Duration::new(5, 0));
        match res {
            Ok(_) => {
                debug!("Workers ready, continue...");
            }
            Err(_) => {
                panic!("Timeout on starting engine, not all workers reported ready, stopping...");
            }
        }
    }

    pub(crate) fn get_async_scheduler(&self) -> Arc<AsyncScheduler> {
        self.async_scheduler.clone()
    }
}

pub struct ExecutionEngineBuilder {
    async_workers_cnt: usize,
    queue_size: usize,
    priority: Option<u16>,

    dedicated_workers_ids: GrowableVec<UniqueWorkerId>,
}

impl Default for ExecutionEngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionEngineBuilder {
    pub fn new() -> Self {
        Self {
            async_workers_cnt: 1,
            queue_size: 256,
            priority: None,
            dedicated_workers_ids: GrowableVec::new(2),
        }
    }

    ///
    /// Will create runtime with `cnt` async workers
    ///
    pub fn workers(mut self, cnt: usize) -> Self {
        self.async_workers_cnt = cnt;
        self
    }

    ///
    /// Configure queue size with `size` for each async worker.
    /// >ATTENTION: `size` has to be power of two
    ///
    pub fn task_queue_size(mut self, size: usize) -> Self {
        assert!(size.is_power_of_two(), "Task queue size must be power of two");
        self.queue_size = size;
        self
    }

    pub fn priority(mut self, prio: u16) -> Self {
        self.priority = Some(prio);
        todo!()
    }

    ///
    /// Adds new dedicated worker to the engine identified by `id`
    ///
    #[allow(dead_code)]
    pub fn with_dedicated_worker(mut self, id: UniqueWorkerId) -> Self {
        assert!(
            !self.dedicated_workers_ids.contains(&id),
            "Cannot register same unique worker multiple times!"
        );

        self.dedicated_workers_ids.push(id);
        debug!("Registered worker {:?}", id);
        self
    }

    pub(crate) fn build(self) -> ExecutionEngine {
        // Create async workers part
        let mut worker_interactors = Box::<[WorkerInteractor]>::new_uninit_slice(self.async_workers_cnt);
        let mut async_queues: Vec<TaskStealQueue> = Vec::new(self.async_workers_cnt);

        for i in 0..self.async_workers_cnt {
            async_queues.push(create_steal_queue(self.queue_size));

            unsafe {
                worker_interactors[i].as_mut_ptr().write(WorkerInteractor::new(async_queues[i].clone()));
            }
        }

        let global_queue = MpmcQueue::new(32);
        let async_scheduler = Arc::new(AsyncScheduler {
            worker_access: unsafe { worker_interactors.assume_init() },
            num_of_searching_workers: FoundationAtomicU8::new(0),
            parked_workers_indexes: Mutex::new(vec![]),
            global_queue,
        });

        let mut async_workers = Vec::new(self.async_workers_cnt);

        for i in 0..self.async_workers_cnt {
            async_workers.push(Worker::new(
                self.priority,
                WorkerId::new(format!("arunner{}", i).as_str().into(), 0, i as u8, WorkerType::Async),
            ));
        }

        // Create dedicated workers part
        let mut dedicated_workers = Vec::new(self.dedicated_workers_ids.len());
        let mut dedicated_queues = Box::<[(WorkerId, Arc<TriggerQueue<TaskRef>>)]>::new_uninit_slice(self.dedicated_workers_ids.len());

        for i in 0..self.dedicated_workers_ids.len() {
            let id = self.dedicated_workers_ids[i];
            let real_id = WorkerId::new(id, 0, i as u8, WorkerType::Dedicated);

            dedicated_workers.push(DedicatedWorker::new(real_id));
            unsafe {
                dedicated_queues[i]
                    .as_mut_ptr()
                    .write((real_id, Arc::new(TriggerQueue::new(self.queue_size))));
            }
        }

        let dedicated_scheduler = Arc::new(DedicatedScheduler {
            dedicated_queues: unsafe { dedicated_queues.assume_init() },
        });

        ExecutionEngine {
            async_workers,
            async_queues,
            async_scheduler,
            dedicated_workers,
            dedicated_scheduler,
        }
    }
}
