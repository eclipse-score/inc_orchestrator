use foundation::prelude::FoundationAtomicU16;

use crate::{core::types::ArcInternal, scheduler::scheduler_mt::SchedulerTrait, TaskRef};

pub struct SchedulerMock {
    pub spawn_count: FoundationAtomicU16,
}

impl SchedulerTrait for SchedulerMock {
    fn respawn(&self, _: TaskRef) {
        self.spawn_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

//Creators

pub fn create_mock_scheduler() -> ArcInternal<SchedulerMock> {
    ArcInternal::new(SchedulerMock {
        spawn_count: FoundationAtomicU16::new(0),
    })
}

// Dummy stub functions
pub async fn test_function<T: Default>() -> T {
    T::default()
}

pub async fn test_function_ret<T>(ret: T) -> T {
    ret
}
