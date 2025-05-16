use std::{sync::Arc, task::Context};

use foundation::{containers::trigger_queue::TriggerQueueConsumer, threading::thread_wait_barrier::ThreadReadyNotifier};

use crate::{
    scheduler::{
        context::{ctx_initialize, ContextBuilder},
        scheduler_mt::{AsyncScheduler, DedicatedScheduler},
        task::async_task::TaskPollResult,
        waker::create_waker,
    },
    TaskRef,
};
use foundation::prelude::*;

use super::worker_types::WorkerId;

///
/// This is a factor which we will divide worker queue size to obtain size of local storage that is used to pop multiple items under single lock.
///
const LOCAL_STORAGE_SIZE_REDUCTION: usize = 8;

pub(crate) struct DedicatedWorker {
    thread_handle: Option<std::thread::JoinHandle<()>>,
    id: WorkerId,
}

impl DedicatedWorker {
    pub(crate) fn new(id: WorkerId) -> Self {
        DedicatedWorker { id, thread_handle: None }
    }

    pub(crate) fn start(
        &mut self,
        scheduler: Arc<AsyncScheduler>,
        dedicated_scheduler: Arc<DedicatedScheduler>,
        ready_notifier: ThreadReadyNotifier,
    ) {
        self.thread_handle = {
            let queue = self.get_queue(&dedicated_scheduler);
            let id = self.id;
            let local_size = queue.capacity() / LOCAL_STORAGE_SIZE_REDUCTION;

            Some(
                std::thread::Builder::new()
                    .name(format!("dworker{}", self.id.worker_id()))
                    .spawn(move || {
                        let internal = WorkerInner {
                            dedicated_scheduler,
                            consumer: queue,
                            local_storage: Vec::new(local_size),
                            id,
                        };

                        Self::run_internal(internal, scheduler, ready_notifier);
                    })
                    .unwrap(),
            )
        };
    }

    fn get_queue(&self, dedicated_scheduler: &Arc<DedicatedScheduler>) -> TriggerQueueConsumer<TaskRef> {
        dedicated_scheduler
            .dedicated_queues
            .iter()
            .find(|(id, _)| *id == self.id)
            .expect("The queue for the worker has to be provided")
            .1
            .get_consumer()
            .expect("There shall be consumer available as only we shall pick it")
    }

    fn run_internal(mut worker: WorkerInner, scheduler: Arc<AsyncScheduler>, ready_notifier: ThreadReadyNotifier) {
        worker.pre_run(scheduler);

        // Let the engine know what we are ready to handle tasks
        ready_notifier.ready();

        debug!("Dedicated worker {:?} started", worker.id.unique_id());
        worker.run();
    }
}

struct WorkerInner {
    dedicated_scheduler: Arc<DedicatedScheduler>,
    consumer: TriggerQueueConsumer<TaskRef>,
    local_storage: Vec<TaskRef>,
    id: WorkerId,
}

impl WorkerInner {
    fn pre_run(&mut self, scheduler: Arc<AsyncScheduler>) {
        let builder = ContextBuilder::new()
            .thread_id(0)
            .with_dedicated_handle(scheduler, self.dedicated_scheduler.clone())
            .with_worker_id(self.id);

        // Setup context
        ctx_initialize(builder);
    }

    fn run(&mut self) {
        loop {
            while !self.local_storage.is_empty() {
                let task = self.local_storage.pop().unwrap(); // Since it was not empty, value must be there.
                self.run_task(task);
            }

            self.consumer.pop_into_vec(&mut self.local_storage);

            if !self.local_storage.is_empty() {
                // If we have new data available, continue processing
                continue;
            }

            match self.consumer.pop_blocking_with_timeout(std::time::Duration::from_millis(100)) {
                Ok(task_ref) => {
                    self.local_storage.push(task_ref);
                }
                Err(CommonErrors::Timeout) => {
                    continue;
                }
                Err(_) => todo!(),
            }
        }
    }

    fn run_task(&mut self, task: TaskRef) {
        let waker = create_waker(task.clone());
        let mut ctx = Context::from_waker(&waker);
        match task.poll(&mut ctx) {
            TaskPollResult::Done => {
                // Literally nothing to do ;)
            }
            TaskPollResult::Notified => {
                // For now stupid respawn
                self.dedicated_scheduler.spawn(task, self.id.unique_id());
            }
        }
    }
}
