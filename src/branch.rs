//! Run-time branch creation -- the Rust counterpart of the Java client's `Step.create` /
//! `Branch`. A spawning step's handler opens branches with [`Step::create`] and chains steps onto
//! them by function; the branches ride the step's report and the engine runs each in isolation,
//! handing their results to the combine after the step.
//!
//! [`Step::create`]: crate::Step::create

use std::future::Future;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::error::StepError;
use crate::pb;
use crate::retry::RetryPolicy;
use crate::typed::fn_name;
use crate::value::to_value;
use crate::worker::{
    compensation_handler, effect_handler, encode, gate_handler, task_handler, Combine, Compensation, Handler, Kind,
    Outcome,
};

/// The steps one branch (or one nested chain of it) has recorded.
#[derive(Default)]
pub(crate) struct BranchBuf {
    input: Value,
    key: Option<String>,
    steps: Vec<pb::BranchStep>,
    error: Option<String>,
    /// The last step has no name yet: a closure awaiting `named(..)`.
    unnamed: Option<String>,
    /// This chain is an arm of `then_one_of`, opened (or not yet) by its guard.
    choice_arm: bool,
    guard: Option<String>,
    otherwise: bool,
}

impl BranchBuf {
    fn fail(&mut self, msg: impl Into<String>) {
        if self.error.is_none() {
            self.error = Some(msg.into());
        }
    }

    /// Settles a pending name before anything else is recorded.
    fn settle_name(&mut self) {
        if let Some(e) = self.unnamed.take() {
            self.fail(e);
        }
    }

    fn push(&mut self, step: pb::BranchStep) {
        self.settle_name();
        self.steps.push(step);
    }

    pub(crate) fn build(&mut self) -> Result<pb::CreatedBranch, String> {
        self.settle_name();
        if let Some(e) = &self.error {
            return Err(e.clone());
        }
        if self.steps.is_empty() {
            return Err("a created branch has no steps; chain at least one onto Step::create(..)".into());
        }
        Ok(pb::CreatedBranch {
            input: Some(to_value(&self.input)),
            key: self.key.clone().unwrap_or_default(),
            steps: self.steps.clone(),
        })
    }
}

fn step(name: String, kind: &str) -> pb::BranchStep {
    pb::BranchStep { name, kind: kind.to_string(), ..Default::default() }
}

/// A branch being chained, whose context is an `I` at this point. Steps are named after the
/// functions passed (nothing is invoked here); the worker that runs them binds the same functions
/// with [`Worker::branch_steps`](crate::Worker::branch_steps).
pub struct Branch<I> {
    buf: Arc<Mutex<BranchBuf>>,
    _t: PhantomData<fn() -> I>,
}

impl<I> Branch<I> {
    pub(crate) fn open(input: Value, key: Option<String>) -> (Branch<I>, Arc<Mutex<BranchBuf>>) {
        let buf = Arc::new(Mutex::new(BranchBuf { input, key, ..Default::default() }));
        (Branch { buf: buf.clone(), _t: PhantomData }, buf)
    }

    fn detached(choice_arm: bool) -> Branch<I> {
        let buf = BranchBuf { choice_arm, ..Default::default() };
        Branch { buf: Arc::new(Mutex::new(buf)), _t: PhantomData }
    }

    fn cast<R>(self) -> Branch<R> {
        Branch { buf: self.buf, _t: PhantomData }
    }

    fn edit(&self, f: impl FnOnce(&mut BranchBuf)) {
        f(&mut self.buf.lock().expect("branch lock"));
    }

    fn add<R, F>(self, kind: &str) -> Branch<R> {
        let kind = kind.to_string();
        self.edit(|b| {
            let (name, unnamed) = match fn_name::<F>() {
                Ok(n) => (n, None),
                Err(e) => (String::new(), Some(e)),
            };
            b.push(step(name, &kind));
            b.unnamed = unnamed;
        });
        self.cast()
    }

    /// Runs a nested chain on a fresh buffer and returns its steps; its error becomes this branch's.
    fn chain<X>(&self, body: impl FnOnce(Branch<I>) -> Branch<X>, what: &str) -> Vec<pb::BranchStep> {
        let sub = body(Branch::detached(false));
        let mut sub = sub.buf.lock().expect("branch lock");
        sub.settle_name();
        let (steps, error) = (std::mem::take(&mut sub.steps), sub.error.take());
        drop(sub);
        self.edit(|b| {
            if let Some(e) = error {
                b.fail(e);
            } else if steps.is_empty() {
                b.fail(format!("{what} has no steps"));
            }
        });
        steps
    }

    fn last_step(&self, what: &str, f: impl FnOnce(&mut pb::BranchStep)) {
        self.edit(|b| match b.steps.last_mut() {
            Some(s) if s.kind == "TASK" || s.kind == "PREDICATE" => f(s),
            _ => b.fail(format!("{what} must directly follow then_apply, then_accept or then_filter")),
        });
    }

    /// Names the step just added -- needed for a closure, or to match a step name of another client.
    pub fn named(self, name: impl Into<String>) -> Self {
        let name = name.into();
        self.edit(|b| b.unnamed = None);
        self.last_step("named()", |s| s.name = name);
        self
    }

    /// The queue the step just added runs on (default: the creating step's). The workers serving
    /// it must list it in [`WorkerOptions::with_queues`](crate::WorkerOptions::with_queues).
    pub fn queue(self, queue: impl Into<String>) -> Self {
        let queue = queue.into();
        self.last_step("queue()", |s| s.queue = queue);
        self
    }

    /// The retry policy of the step just added (default: the creating step's).
    pub fn retry(self, retry: RetryPolicy) -> Self {
        self.last_step("retry()", |s| s.retry = Some(to_value(&retry.to_json())));
        self
    }

    /// A server-side timer.
    pub fn then_sleep(self, duration: Duration) -> Branch<I> {
        self.edit(|b| b.push(pb::BranchStep { sleep_millis: duration.as_millis() as i64, ..step(String::new(), "SLEEP") }));
        self
    }

    /// Waits for the named signal.
    pub fn then_await(self, signal: impl Into<String>) -> Branch<I> {
        let signal = signal.into();
        self.edit(|b| b.push(step(signal, "SIGNAL")));
        self
    }

    /// [`then_await`](Self::then_await) that fails the instance when `timeout` passes first.
    pub fn then_await_timeout(self, signal: impl Into<String>, timeout: Duration) -> Branch<I> {
        let signal = signal.into();
        self.edit(|b| b.push(pb::BranchStep { sleep_millis: timeout.as_millis() as i64, ..step(signal, "SIGNAL") }));
        self
    }

    /// [`then_await`](Self::then_await) that runs `escalation` and goes on when `timeout` passes first.
    pub fn then_await_or<X>(
        self,
        signal: impl Into<String>,
        timeout: Duration,
        escalation: impl FnOnce(Branch<I>) -> Branch<X>,
    ) -> Branch<I> {
        let signal = signal.into();
        if timeout.is_zero() {
            self.edit(|b| b.fail(format!("the wait for '{signal}' needs a positive timeout")));
            return self;
        }
        let esc = self.chain(escalation, &format!("the escalation of '{signal}'"));
        self.edit(|b| {
            b.push(pb::BranchStep { sleep_millis: timeout.as_millis() as i64, escalation: esc, ..step(signal, "SIGNAL") })
        });
        self
    }

    /// Runs the registered workflow `workflow` as a child on the branch's context (node `node`).
    pub fn then_sub_flow<R>(self, node: impl Into<String>, workflow: impl Into<String>) -> Branch<R> {
        let (node, workflow) = (node.into(), workflow.into());
        self.edit(|b| b.push(pb::BranchStep { workflow, ..step(node, "SUB_WORKFLOW") }));
        self.cast()
    }

    /// Fans the branch out into parallel arms, each on its own copy of the branch's context; the
    /// returned stage requires a combine. An arm is named after its last step.
    pub fn then_all_of(self, arms: impl FnOnce(AllOfArms<I>) -> AllOfArms<I>) -> BranchFork<I> {
        let arms = arms(AllOfArms { arms: Vec::new(), errors: Vec::new(), _t: PhantomData });
        self.edit(|b| {
            if let Some(e) = arms.errors.into_iter().next() {
                b.fail(e);
            } else if arms.arms.len() < 2 {
                b.fail("then_all_of needs at least two arms");
            }
        });
        BranchFork { branch: self, arms: arms.arms }
    }

    /// Exactly one arm runs: the first whose [`when`](Self::when) guard holds, or the
    /// [`otherwise`](Self::otherwise) arm (last); with no otherwise and no match the branch goes
    /// on past the choice.
    pub fn then_one_of<R>(self, arms: impl FnOnce(OneOfArms<I, R>) -> OneOfArms<I, R>) -> Branch<R> {
        let arms = arms(OneOfArms { cases: Vec::new(), error: None, _t: PhantomData });
        self.edit(|b| {
            if let Some(e) = arms.error {
                b.fail(e);
            } else if arms.cases.first().map_or(true, |c| c.guard.is_empty()) {
                b.fail("then_one_of needs an arm opened with when(..)");
            } else {
                b.push(pb::BranchStep { cases: arms.cases, ..step(String::new(), "PREDICATE") });
            }
        });
        self.cast()
    }

    /// Opens an arm of [`then_one_of`](Self::then_one_of): `otherwise` arms run when no guard held.
    pub fn otherwise(self) -> Branch<I> {
        self.edit(|b| {
            if !b.choice_arm || !b.steps.is_empty() || b.guard.is_some() || b.otherwise {
                b.fail("otherwise() opens an arm of then_one_of, so it comes first in one");
            }
            b.otherwise = true;
        });
        self
    }
}

impl<I: DeserializeOwned> Branch<I> {
    /// A task: `step(I) -> R`.
    pub fn then_apply<R, F, Fut>(self, _step: F) -> Branch<R>
    where
        F: Fn(I) -> Fut,
        Fut: Future<Output = Result<R, StepError>>,
    {
        self.add::<R, F>("TASK")
    }

    /// A compensable task: `step(I) -> R`. If the instance later fails, its compensator (bound
    /// with [`BranchSteps::compensation`]) runs in the reverse pass.
    pub fn then_apply_compensable<R, F, Fut>(self, _step: F) -> Branch<R>
    where
        F: Fn(I) -> Fut,
        Fut: Future<Output = Result<R, StepError>>,
    {
        let next = self.add::<R, F>("TASK");
        next.edit(|b| {
            if let Some(s) = b.steps.last_mut() {
                s.compensable = true;
            }
        });
        next
    }

    /// A side effect: `effect(I)`; the context is unchanged.
    pub fn then_accept<F, Fut>(self, _effect: F) -> Branch<I>
    where
        F: Fn(I) -> Fut,
        Fut: Future<Output = Result<(), StepError>>,
    {
        self.add::<I, F>("TASK")
    }

    /// A gate: a false result ends the branch (it goes straight to the join).
    pub fn then_filter<F, Fut>(self, _gate: F) -> Branch<I>
    where
        F: Fn(I) -> Fut,
        Fut: Future<Output = Result<bool, StepError>>,
    {
        self.add::<I, F>("PREDICATE")
    }

    /// Makes the task just added a spawning step of its own: its handler calls `Step::create`, and
    /// `combine` receives those branches' results and continues this branch with its return.
    pub fn combine<R, F, Fut>(self, _combine: F) -> Branch<R>
    where
        F: Fn(Combine) -> Fut,
        Fut: Future<Output = Result<R, StepError>>,
    {
        let name = fn_name::<F>();
        self.edit(|b| {
            b.settle_name();
            match (b.steps.last_mut(), name) {
                (Some(s), Ok(name)) if s.kind == "TASK" && s.combine.is_empty() => s.combine = name,
                (_, Err(e)) => b.fail(e),
                _ => b.fail("combine() must directly follow the step that creates the branches it combines: \
                             a then_apply or then_accept"),
            }
        });
        self.cast()
    }

    /// Opens an arm of [`then_one_of`](Self::then_one_of), taken when `guard` holds.
    pub fn when<F, Fut>(self, _guard: F) -> Branch<I>
    where
        F: Fn(I) -> Fut,
        Fut: Future<Output = Result<bool, StepError>>,
    {
        let name = fn_name::<F>();
        self.edit(|b| {
            if !b.choice_arm || !b.steps.is_empty() || b.guard.is_some() || b.otherwise {
                b.fail("when(..) opens an arm of then_one_of, so it comes first in one");
            }
            match name {
                Ok(n) => b.guard = Some(n),
                Err(e) => b.fail(e),
            }
        });
        self
    }

    /// Runs `body`, then again while `condition` holds (engine-default budget).
    pub fn repeat_while<F, Fut>(self, condition: F, body: impl FnOnce(Branch<I>) -> Branch<I>) -> Branch<I>
    where
        F: Fn(I) -> Fut,
        Fut: Future<Output = Result<bool, StepError>>,
    {
        self.repeat(condition, 0, body)
    }

    /// [`repeat_while`](Self::repeat_while) with an explicit iteration budget.
    pub fn repeat_while_max<F, Fut>(self, condition: F, max_iterations: u32, body: impl FnOnce(Branch<I>) -> Branch<I>) -> Branch<I>
    where
        F: Fn(I) -> Fut,
        Fut: Future<Output = Result<bool, StepError>>,
    {
        if max_iterations == 0 {
            self.edit(|b| b.fail("repeat_while_max: max_iterations must be positive"));
            return self;
        }
        self.repeat(condition, max_iterations as i32, body)
    }

    fn repeat<F>(self, _condition: F, budget: i32, body: impl FnOnce(Branch<I>) -> Branch<I>) -> Branch<I> {
        let name = fn_name::<F>();
        let what = format!("the body of repeat_while({})", name.as_deref().unwrap_or("?"));
        let body = self.chain(body, &what);
        self.edit(|b| match name {
            Ok(name) => b.push(pb::BranchStep { body, loop_budget: budget, ..step(name, "PREDICATE") }),
            Err(e) => b.fail(e),
        });
        self
    }
}

/// The arms of [`Branch::then_all_of`].
pub struct AllOfArms<I> {
    arms: Vec<pb::BranchArm>,
    errors: Vec<String>,
    _t: PhantomData<fn() -> I>,
}

impl<I> AllOfArms<I> {
    /// Adds an arm, chained on a copy of the branch's context.
    pub fn arm<X>(mut self, arm: impl FnOnce(Branch<I>) -> Branch<X>) -> Self {
        let sub = arm(Branch::detached(false));
        let mut sub = sub.buf.lock().expect("branch lock");
        sub.settle_name();
        if let Some(e) = sub.error.take() {
            self.errors.push(e);
        } else if sub.steps.is_empty() {
            self.errors.push(format!("arm {} of then_all_of has no steps", self.arms.len()));
        }
        self.arms.push(pb::BranchArm { steps: std::mem::take(&mut sub.steps) });
        drop(sub);
        self
    }
}

/// The mandatory combine stage of [`Branch::then_all_of`].
#[must_use = "then_all_of needs combine(..) to rejoin its arms"]
pub struct BranchFork<I> {
    branch: Branch<I>,
    arms: Vec<pb::BranchArm>,
}

impl<I> BranchFork<I> {
    /// Rejoins the arms at `combine`, which receives each arm's result by arm name.
    pub fn combine<R, F, Fut>(self, _combine: F) -> Branch<R>
    where
        F: Fn(Combine) -> Fut,
        Fut: Future<Output = Result<R, StepError>>,
    {
        let name = fn_name::<F>();
        let arms = self.arms;
        self.branch.edit(|b| match name {
            Ok(combine) => b.push(pb::BranchStep { arms, combine, ..step(String::new(), "FORK") }),
            Err(e) => b.fail(e),
        });
        self.branch.cast()
    }
}

/// The arms of [`Branch::then_one_of`]; every arm ends at an `R`.
pub struct OneOfArms<I, R> {
    cases: Vec<pb::BranchCase>,
    error: Option<String>,
    _t: PhantomData<fn() -> (I, R)>,
}

impl<I, R> OneOfArms<I, R> {
    /// Adds an arm, which must open with [`Branch::when`] or [`Branch::otherwise`].
    pub fn arm(mut self, arm: impl FnOnce(Branch<I>) -> Branch<R>) -> Self {
        let sub = arm(Branch::detached(true));
        let mut sub = sub.buf.lock().expect("branch lock");
        sub.settle_name();
        let n = self.cases.len();
        let problem = if let Some(e) = sub.error.take() {
            Some(e)
        } else if sub.guard.is_none() && !sub.otherwise {
            Some(format!("arm {n} of then_one_of must open with when(..) or otherwise()"))
        } else if self.cases.last().is_some_and(|c| c.guard.is_empty()) {
            Some("otherwise() must be the last arm of then_one_of".to_string())
        } else {
            None
        };
        if let (Some(e), None) = (problem, &self.error) {
            self.error = Some(e);
        }
        self.cases.push(pb::BranchCase { guard: sub.guard.take().unwrap_or_default(), steps: std::mem::take(&mut sub.steps) });
        drop(sub);
        self
    }
}

// ---- the worker side: handlers for created steps ----

pub(crate) struct BranchBinding {
    pub(crate) step: String,
    pub(crate) kind: Kind,
    pub(crate) handler: Handler,
}

/// Handlers for the steps a workflow's branches may run -- steps that are in no graph, so a worker
/// binds them by name, as the Java worker binds a created node's `@ForFlow` method on first
/// dispatch. Each is named after its function, which is what [`Branch`] names the step it chains,
/// so the two agree by construction; [`named`](Self::named) overrides it.
///
/// ```ignore
/// worker.branch_steps("orders", BranchSteps::new()
///     .handle(reserve)
///     .handle(ship)
///     .effect(deliver_link)
///     .compensation(reserve, release))
/// ```
#[derive(Default)]
pub struct BranchSteps {
    entries: Vec<(Result<String, String>, Kind, Handler)>,
}

impl BranchSteps {
    pub fn new() -> Self {
        BranchSteps::default()
    }

    fn add<F>(mut self, kind: Kind, handler: Handler) -> Self {
        self.entries.push((fn_name::<F>(), kind, handler));
        self
    }

    /// A task chained with [`Branch::then_apply`].
    pub fn handle<I, O, F, Fut>(self, f: F) -> Self
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, StepError>> + Send + 'static,
    {
        self.add::<F>(Kind::Task, task_handler(f))
    }

    /// A side effect chained with [`Branch::then_accept`].
    pub fn effect<I, F, Fut>(self, f: F) -> Self
    where
        I: DeserializeOwned + Send + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), StepError>> + Send + 'static,
    {
        self.add::<F>(Kind::Task, effect_handler(f))
    }

    /// A gate, guard or loop condition ([`Branch::then_filter`], [`Branch::when`],
    /// [`Branch::repeat_while`]).
    pub fn gate<I, F, Fut>(self, f: F) -> Self
    where
        I: DeserializeOwned + Send + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<bool, StepError>> + Send + 'static,
    {
        self.add::<F>(Kind::Gate, gate_handler(f))
    }

    /// A combine of a branch's fork or nested spawn ([`BranchFork::combine`], [`Branch::combine`]).
    pub fn combine<O, F, Fut>(self, f: F) -> Self
    where
        O: Serialize + 'static,
        F: Fn(Combine) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, StepError>> + Send + 'static,
    {
        let f = Arc::new(f);
        let handler: Handler = Arc::new(move |task| {
            let f = f.clone();
            Box::pin(async move { Ok(Outcome::Merge(encode(f(Combine::of(&task)).await?)?)) })
        });
        self.add::<F>(Kind::Combine, handler)
    }

    /// The compensator of the step `step` (chained with [`Branch::then_apply_compensable`]): named after `step`.
    pub fn compensation<S, F, Fut>(mut self, _step: S, undo: F) -> Self
    where
        F: Fn(Compensation) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), StepError>> + Send + 'static,
    {
        self.entries.push((fn_name::<S>(), Kind::Compensate, compensation_handler(undo)));
        self
    }

    /// Names the handler just added (for a closure, or a step another client created).
    pub fn named(mut self, name: impl Into<String>) -> Self {
        if let Some(last) = self.entries.last_mut() {
            last.0 = Ok(name.into());
        }
        self
    }

    pub(crate) fn into_bindings(self) -> Result<Vec<BranchBinding>, String> {
        self.entries
            .into_iter()
            .map(|(name, kind, handler)| name.map(|step| BranchBinding { step, kind, handler }))
            .collect()
    }
}
