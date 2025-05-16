# Detailed Design for Component: Orchestration

[WIP] - as there is no explanation of actions and so on, this is like a start document for a process that should let us understand, review and code design.

## Description
The `Orchestration` component provides the framework to build **Task Chains with deterministic execution flow** 

### Design Decisions
TBD

> Design should be split from deployment

> The orchestration should be able to consume C++ functions as input into Invoke (over FFI) so C++ code can be called.
> NOTE: This is one direction integration meaning that C++ code will not use Orchestration API at all, it will only be called from RUST.

### Design Constraints
TBD

## Rationale Behind Decomposition into Units
As the `Orchestration` component is providing simple and unified API to the users, internally to achieve serration of concerns it is split into multiple units:

- **OrchestrationAPI** - unit responsible for providing single, simplified end user API. This splits the `design` and `deployment` concerns for end user.
- **Actions** - unit holding implementation of all actions provided by `Orchestration`
- **Common** - unit holding internal code that is used in other units 
- **Program** - unit providing means to configure & construct **Task chain** 
- **Configuration** - unit taking care about `config by file` aspects


## Static Diagrams for Unit Interactions

#### Orchestration decomposition diagram
![Orch Static Diagram](images/orch_static_view.drawio.svg)


#### Orchestration class diagram

```plantuml

package Orchestration {

    class OrchestrationAPI {
        db: ProgramDatabase,
        parser: ConfigParser,
        programs: Vec<> // user may provide multiple programs
        ...
        ---
        + fn enable_programs_debug_info()
        + ...
        + fn as_design_configurator() -> OrchestrationDesignConfigurator
        + fn as_deployment_configurator() -> OrchestrationDeploymentConfigurator
        + run() -> Result<>
    }

    class OrchestrationDesignConfigurator {

        + register_invocable(name: &str, ...) -> OrchestrationTag
        + register_simple_condition(name: &str,...) -> OrchestrationTag
        + register_complex_condition(name: &str, ...) -> OrchestrationTag
        + register_event(name: &str) -> OrchestrationTag

        + use_program(program_creator: Fn() -> ProgramBuilder) -> Result<> // Config by code, clouser let us delay construction 
        to right moment, ie in .run() when all registers are already done
    }

    note bottom of OrchestrationDesignConfigurator
    User register only "event name" as on design level user does not care
    whether it's global or local ie. It has to receive **blanket** OrchestrationTag that is able to later resolve to real event based on
    OrchestrationDesignConfigurator.register_event
    end note

    class OrchestrationDeploymentConfigurator {
        Here later we will put more things, like affinities, num of supported events etc.
        ---

        + fn register_event(name: &str, type: EventType) -> OrchestrationId
        + fn register_timer(name: &str, ...) -> OrchestrationId

        + use_program_from_file(path) -> Result<> // Config by file
        + use_program(program: Fn() -> ProgramBuilder) -> Result<> // Config by code
    }

    OrchestrationAPI *-- OrchestrationDesignConfigurator
    OrchestrationAPI *-- OrchestrationDeploymentConfigurator
}

note top of Orchestration
Let split design and deployment actions and provide high level access API to deploy task chain.
Further it also can hide things like
- runtime
- logging and tracing
- etc
if we find it make sense
end note

package Common {
    class Tag {
        id: u64,
        tracing: Option<&'static str>
    }
    note top of Tag
    Orchestration provides **Into<Tag>** for **String** and **&str**
    which most likely is a Hash
    end note

    class OrchestrationTag {
        id: Tag,
        global_config: GlobalConfig, // Keeps some global infos
        tracking: Option<&'static str>, // Db will leak provided names when asked converting them into static  refs. This can be done under some option provided to db
        provider: &ActionProvider

        ---
    }

    OrchestrationTag o-- ActionDataProvider: uses

    interface SimpleConditionProvider {
        fn compute_condition_result(&self) -> bool;
    }

    note top of SimpleConditionProvider
    IfElse require from user to implement this type as condition provider
    end note

    interface ComplexConditionProvider {
        fn try_from_value(value: SwitchValue) -> Result<u64, Err>;
        fn compute_condition_result(&self) -> u64;
    }

    note top of ComplexConditionProvider
    This is internal type we will use to deliver into **Switch**.
    The *try_** API will help switch to pass user info into real impl and get mapped value
    so associated action can be stored
    end note

    enum SwitchValue {
        Bool(bool),
        Int(u32),
        Object(?),
        String(String)
    }


    interface ComplexConditionProviderTyped<<ComplexConditionProvider>> {
        type CustomType where  Self::CustomType: Into<u64>;
        fn compute_condition_result(&self) -> Self::CustomType

        fn try_from_value(value: SwitchValue) -> Result<Self::C, Err>;



        Internally we will put this into another impl that uses .into() to get results into switch
    }

    note top of ComplexConditionProviderTyped
    Switch require from user to implement this type as condition provider.
    Overall handling of switch is more complex since it may be of any type.
    Thats why we internally will wrap it into **ComplexConditionProvider** which will translate custom types
    to simple u32 values which **Switch** will use
    end note

}


package Config {
    class ConfigParser {

    + fn load_config(file: Path);
    + fn build_program(db: &ProgramDatabase) -> Result<Program, Err>;

    }
}

package Program {
    enum EventType {
        Local,
        Global,
        Timer
    }

    class ProgramDatabase<<T: CommTrait>> {
        comm_provider: T, // Abstraction of Com
        action_provider: ActionDataProvider,
        event_builders: Map<Tag, EventBuilder>, // Try use SlotMap from Iceoryx2, otherwise Vec ?
        simple_conditions: Map<Tag, XYZ>,
        ...

        ---

        + register_event(name, type) -> OrchestrationTag
        + register_invocable(name, invocable_fn_etc) -> OrchestrationTag
        + register_condition(name, SimpleConditionProvider) -> OrchestrationTag
        + register_condition_complex(name, ComplexConditionProviderTyped) -> OrchestrationTag
        + register_*

    }

    ProgramDatabase *-- ActionDataProvider
    ProgramDatabase ..> OrchestrationTag: creates
    ConfigParser ..> ProgramDatabase

    class ActionDataProvider {
            database: &ProgramDatabase
        ---

        + provide_event_listener(t: Tag) ->  EvtListener;
        + provide_invocable(t: Tag) ->  InvocableInternal;
        + provide_condition(t: Tag) -> SimpleConditionProvider
        + ...

    }
}

package Actions {
    class TriggerAction {
        notifier: EvtNotifier
    ---
        fn from_tag(tag: OrchestrationTag) -> Box<TriggerAction>
    }

    class InvokeAction{
        invocable: InvocableInternal
    ---
        fn from_tag(tag: OrchestrationTag) -> Box<InvokeAction>
    }

    class IfElse{
        cond: SimpleConditionProvider
    ---
        fn from_tag(tag: OrchestrationTag) -> Box<IfElse>
    }



    class Switch{
        cond: ComplexConditionProvider
    ---
        fn from_tag(tag: OrchestrationTag) -> Box<Switch>

        fn with_case(value: SwitchValue, Box<dyn ActionTrait>)
    }

    Switch ..> OrchestrationTag
    IfElse ..> OrchestrationTag
    InvokeAction ..> OrchestrationTag
    TriggerAction ..> OrchestrationTag
}



OrchestrationAPI *-- ProgramDatabase
OrchestrationAPI *-- ConfigParser

```


#### Sample code when using config file only (using lower layers)
```rust


let db = ProgramDatabase::new()
db.register_invocable("some_name", some_function);
db.register_event("some_name", EventType::Local);
...

let program = ConfigParser::load_config("/user/set/cfg.json").build_program(db).unwrap();


```

#### Sample code when using config in code (using lower layers)
```rust


let db = ProgramDatabase::new()
let some_fn_id = db.register_invocable("some_name", some_function);
let some_evt_id = db.register_event("some_name", EventType::Local);
...

let program = ProgramBuilder::new()
    .with_body(Sequence::new()
        .with_step(Trigger::from_tag(some_evt_id))
        .with_step(Invoke::from_tag(some_fn_id)))
    .build().unwrap();

```

## Dynamic Diagrams for Unit Interactions

TBD



# Known problems and decisions record

- Actions can be currently orphaned (can be wrongly configured, ie no step function, no if or else condition etc). Proposed actions:
    - Use builder pattern instead with drawback that each `Action::new().whatever()` will need to finish with `build()` where we can do checks ✅
    - Also try to use `typestate` pattern that allows to present some API only once you provided needed things. Example:
```rust

pub struct ConcBuilderInner {
    // keep real data for building
}

pub struct ConcBuilderNotRequired {
    inner: ConcBuilderInner,
}
pub struct ConcBuilderRequiredProvided {
    inner: ConcBuilderInner,
}

type ConcBuilder = ConcBuilderNotRequired;

impl ConcBuilderNotRequired {
    // New only in non required state
    fn new() -> Self {
        Self { inner: ConcBuilderInner {} }
    }

    fn with_opt_param(self) -> Self {
        self
    }

    // Once required thing is provided, transition to next "State"
    fn with_step(self) -> ConcBuilderRequiredProvided {
        ConcBuilderRequiredProvided { inner: self.inner }
    }
}

impl ConcBuilderRequiredProvided {
    fn with_step(&mut self) -> &mut Self {
        self
    }

    fn build(self) {} // build only visible in final state
}

Keep in mind that original pattern uses ClassName<State> since you may have multiple needed things to be provided.
For us it could be that this only works for Concurrency and Sequence so just have it in mind if it makes sense to use it.
```

- When no config file is given we don't know how much actions we need to store. Proposed actions:
    - Use builder pattern for actions instead direct object creation. This way action will know it's Vec size ✅
    - The builder will have same issue. Proposal for this too:
        - Use `GrowableVec` from foundation ✅


- Futures are dynamically allocated during runtime. Proposed actions:
    - Replace with ReusableFuturePool which allocates only in startup phase ✅

- When in `execute` we currently are using `Vec` to store Futures from other actions. Proposed action:
    - We already know vector size so we can preallocate in constructor, but we still would need a pool of it (same as future pool). Maybe use same pattern here ?

- Builders should rather return &mut self to be more useful, currently we return Self which leads to need to assign each time you do something with builders (ie create actions in loops, etc) ✅

- When user register Conditions, should it register Condition, Arc<Condition> or Builder<Condition>
    - first means it can be only used ONCE or has to be COPY
    - second means it is shared
    - this means no Copy but can be produced N times using user code to do it.
> Decision: Try go with Arc<Condition> and user need to take care of interior mutability ✅


- In CPP we were providing sometimes "context" into Invocables. Do we want to do it here too (context is really the Condition or N Conditions or ?)?
    - no context -> user must make sure it previously share Condition with any code that can modify it before register to us
    - context -> how user need to tell us which Invocable gets which context ? Would it be it needs to say 

```
{
    invocable: "function_abc",
    context: "some_ctx"
}
```
....
    This will also have a problem that we can probably only give user a UnTyped context so it will need to recast types on his own.

> Decision: Do not provide context at current implementation ✅



- Can we use `String` at init phase or we need to handle it differently ?

> Decision: Try not to use `String` even in `register_` API but use directly `Tag` and provide `Into<Tag>` for `String` and `&str` also handling internally debug option (to keep leaked String into &'static str for tracing'). User will be then able to also do any other naming of its function and implement `Into<Tag>` for its own type if needed. ✅

- Need to use Mutex for object that are shared via Arc<> (means basically all objects because ) -> should we create simple "guard" that let N accesses wait for a guard once this is held ? This is something like reversed semaphore where you need to say how many users of locks is there at max. 

> Decision: Use A "OrchestrationMutex" that will use atomic to check if something is in progress and if yes, it will use yield ;) ✅

- Should `Program` support cycle time or shall it be modeled by user as `Sync`

> Decision: For now use `Sync` modelling by user, if we find better to use it on program level, we will add it and it will not be any breaking change. ✅

- Should `Program` support separate `shutdown_notification` or shall user include it in Task chain ?

> Decision: Use separate `shutdown_notification` otherwise user will need to know how to exactly model this which will be confusing at the end compared to explicit configuration for this. ✅
