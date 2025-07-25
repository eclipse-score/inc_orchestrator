use crate::internals::scenario::{ScenarioGroup, ScenarioGroupImpl};
use orchestration_concurrency::{MultipleConcurrency, NestedConcurrency, SingleConcurrency};
use orchestration_sequence::{AwaitSequence, NestedSequence, SingleSequence};
use orchestration_sleep::SleepUnderLoad;
use orchestration_trigger_sync::{
    OneTriggerOneSyncTwoPrograms, OneTriggerTwoSyncsThreePrograms, TriggerAndSyncInNestedBranches, TriggerSyncOneAfterAnother,
};

use async_runtime::futures::reusable_box_future::ReusableBoxFuturePool;
use orchestration::{common::tag::Tag, prelude::*};

use tracing::info;

macro_rules! generic_test_func {
    ($name:expr) => {
        || generic_test_sync_func($name)
    };
}
#[macro_use]
pub mod orchestration_concurrency;
pub mod orchestration_sequence;
pub mod orchestration_sleep;
pub mod orchestration_trigger_sync;

pub struct OrchestrationScenarioGroup {
    group: ScenarioGroupImpl,
}

impl OrchestrationScenarioGroup {
    pub fn new() -> Self {
        OrchestrationScenarioGroup {
            group: ScenarioGroupImpl::new("orchestration"),
        }
    }
}

impl ScenarioGroup for OrchestrationScenarioGroup {
    fn get_group_impl(&mut self) -> &mut ScenarioGroupImpl {
        &mut self.group
    }

    fn init(&mut self) -> () {
        // Sequence scenarios
        self.group.add_scenario(Box::new(SingleSequence));
        self.group.add_scenario(Box::new(NestedSequence));
        self.group.add_scenario(Box::new(AwaitSequence));
        // Concurrency scenarios
        self.group.add_scenario(Box::new(SingleConcurrency));
        self.group.add_scenario(Box::new(MultipleConcurrency));
        self.group.add_scenario(Box::new(NestedConcurrency));
        // Trigger and sync scenarios
        self.group.add_scenario(Box::new(OneTriggerOneSyncTwoPrograms));
        self.group.add_scenario(Box::new(OneTriggerTwoSyncsThreePrograms));
        self.group.add_scenario(Box::new(TriggerAndSyncInNestedBranches));
        self.group.add_scenario(Box::new(TriggerSyncOneAfterAnother));
        // Sleep scenarios
        self.group.add_scenario(Box::new(SleepUnderLoad));
    }
}

pub struct JustLogAction {
    base: ActionBaseMeta,
    name: String,
}

impl JustLogAction {
    fn new(name: impl Into<String>) -> Box<JustLogAction> {
        const DEFAULT_TAG: &str = "integration::tests::just_log_action";

        Box::new(Self {
            base: ActionBaseMeta {
                tag: Tag::from_str_static(DEFAULT_TAG),
                reusable_future_pool: ReusableBoxFuturePool::new(1, Self::execute_impl("JustLogAction".into())),
            },
            name: name.into(),
        })
    }
    async fn execute_impl(name: String) -> ActionResult {
        info!("{name} was executed");
        Ok(())
    }
}

impl ActionTrait for JustLogAction {
    fn name(&self) -> &'static str {
        "JustLogAction"
    }
    fn dbg_fmt(&self, _nest: usize, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
    fn try_execute(&mut self) -> ReusableBoxFutureResult {
        self.base.reusable_future_pool.next(JustLogAction::execute_impl(self.name.clone()))
    }
}

/// emulate some computing
fn busy_sleep() -> ActionResult {
    info!("Start sleeping");
    let mut ctr = 1_000_000;
    while ctr > 0 {
        ctr -= 1;
    }
    info!("End sleeping");
    Ok(())
}

fn generic_test_sync_func(name: &'static str) -> InvokeResult {
    info!("Start of '{}' function", name);
    // Spend some time to simulate work
    let _ = busy_sleep();
    info!("End of '{}' function", name);
    Ok(())
}
