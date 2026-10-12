//! The worker: binds handlers to steps by name, reconciles them against the registered graph, then
//! pulls tasks it has capacity for, runs the matching handler and reports the result. It holds no
//! durable state: a crash loses at most the in-flight steps, which the server re-leases.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{Map, Value};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::client::{Client, StepReport, Task};
use crate::error::{Error, Result, StepError};
use crate::step::Scope;

const COMPENSATE_SUFFIX: &str = "#compensate";

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

pub(crate) enum Outcome {
    /// A task's complete next context; `None` leaves the context unchanged.
    Merge(Option<Value>),
    Predicate(bool),
}

pub(crate) type Handler = Arc<dyn Fn(Task) -> BoxFuture<std::result::Result<Outcome, StepError>> + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Task,
    Gate,
    Combine,
    Compensate,
}

impl Kind {
    fn verb(self) -> &'static str {
        match self {
            Kind::Task => "handle()/effect()",
            Kind::Gate => "gate()",
            Kind::Combine => "combine()",
            Kind::Compensate => "compensation()",
        }
    }
}

struct Binding {
    workflow: String,
    step: String,
    kind: Kind,
    handler: Handler,
}

impl Binding {
    fn activity(&self) -> String {
        let base = format!("{}#{}", self.workflow, self.step);
        if self.kind == Kind::Compensate {
            base + COMPENSATE_SUFFIX
        } else {
            base
        }
    }
}

/// Tuning for a [`Worker`].
#[derive(Debug, Clone)]
pub struct WorkerOptions {
    concurrency: usize,
    lease: Duration,
    long_poll_wait: Duration,
    queues: Option<Vec<String>>,
    await_registration: Duration,
    idle_backoff: Duration,
    error_backoff: Duration,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        WorkerOptions {
            concurrency: std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4),
            lease: Duration::from_secs(30),
            long_poll_wait: Duration::from_secs(10),
            queues: None,
            await_registration: Duration::ZERO,
            idle_backoff: Duration::from_millis(200),
            error_backoff: Duration::from_secs(2),
        }
    }
}

impl WorkerOptions {
    /// The most steps in flight at once (default: the number of CPUs).
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        self
    }

    /// How long a claimed step may run before the server reclaims it (default 30s). A running
    /// step's lease is extended by heartbeats every half lease.
    pub fn lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    /// How long a poll waits server-side for work to appear (default 10s).
    pub fn long_poll_wait(mut self, wait: Duration) -> Self {
        self.long_poll_wait = wait;
        self
    }

    /// Restricts the worker to these queues (else it serves every queue its bound steps poll).
    pub fn queues<S: Into<String>>(mut self, queues: impl IntoIterator<Item = S>) -> Self {
        self.queues = Some(queues.into_iter().map(Into::into).collect());
        self
    }

    /// Makes `start` wait up to `wait` for a bound workflow's graph to be registered before failing
    /// -- to ride out a registration race. Zero (the default) fails fast.
    pub fn await_registration(mut self, wait: Duration) -> Self {
        self.await_registration = wait;
        self
    }
}

/// What a fork or forEach combine handler receives.
///
/// For a fork: every arm's result by arm name, in fork order. For a forEach: every item's final
/// value -- a list ordered by item index for a list input, a map keyed like the input for a map
/// input. In both cases [`base`](Combine::base) is the context from before the fan-out.
#[derive(Debug, Clone)]
pub struct Combine {
    base: Value,
    arms: Vec<(String, Value)>,
    items: Option<Value>,
}

impl Combine {
    pub(crate) fn of(task: &Task) -> Combine {
        let mut map = match &task.context {
            Value::Object(m) => m.clone(),
            _ => Map::new(),
        };
        let mut arms = Vec::with_capacity(task.arm_names.len());
        for arm in &task.arm_names {
            let value = map.remove(&format!("__arm__{arm}")).unwrap_or(Value::Null);
            arms.push((arm.clone(), value));
        }
        let items = task.collect_key.as_ref().map(|key| {
            map.remove(&format!("__steps__{key}"));
            map.remove(key).unwrap_or(Value::Null)
        });
        let base = match (&task.base_context, map.is_empty()) {
            (Some(ambient), true) => ambient.clone(),
            _ => Value::Object(map),
        };
        Combine { base, arms, items }
    }

    /// The context from before the fan-out.
    pub fn base(&self) -> &Value {
        &self.base
    }

    /// [`base`](Self::base) decoded into `T`.
    pub fn base_as<T: DeserializeOwned>(&self) -> std::result::Result<T, StepError> {
        decode(self.base.clone(), "the combine's base")
    }

    /// A fork's arms, in fork order, with each arm's result.
    pub fn arms(&self) -> &[(String, Value)] {
        &self.arms
    }

    /// One fork arm's result.
    pub fn arm(&self, name: &str) -> Option<&Value> {
        self.arms.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }

    /// One fork arm's result decoded into `T`.
    pub fn arm_as<T: DeserializeOwned>(&self, name: &str) -> std::result::Result<T, StepError> {
        let value = self
            .arm(name)
            .cloned()
            .ok_or_else(|| StepError::permanent(format!("the fork has no arm '{name}'")))?;
        decode(value, &format!("arm '{name}'"))
    }

    /// A forEach's collected item results (`None` for a fork combine).
    pub fn items(&self) -> Option<&Value> {
        self.items.as_ref()
    }

    /// [`items`](Self::items) decoded into `T` (a `Vec<_>` for a list input, a map for a map input).
    pub fn items_as<T: DeserializeOwned>(&self) -> std::result::Result<T, StepError> {
        let items = self
            .items
            .clone()
            .ok_or_else(|| StepError::permanent("this combine follows a fork, not a for_each"))?;
        decode(items, "the forEach results")
    }
}

/// What a compensator receives: the context snapshots of the step being undone, captured by the
/// engine when the step completed -- not the instance's latest context, which a later step may have
/// replaced. `result` holds the step's own products (a payment ref); `input` serves
/// restore-the-previous-value undos.
#[derive(Debug, Clone)]
pub struct Compensation {
    pub input: Value,
    pub result: Value,
}

impl Compensation {
    pub fn input_as<T: DeserializeOwned>(&self) -> std::result::Result<T, StepError> {
        decode(self.input.clone(), "the compensation input")
    }

    pub fn result_as<T: DeserializeOwned>(&self) -> std::result::Result<T, StepError> {
        decode(self.result.clone(), "the compensation result")
    }
}

pub(crate) fn decode<T: DeserializeOwned>(value: Value, what: &str) -> std::result::Result<T, StepError> {
    serde_json::from_value(value).map_err(|e| StepError::permanent(format!("cannot decode {what}: {e}")))
}

pub(crate) fn encode<O: Serialize>(out: O) -> std::result::Result<Option<Value>, StepError> {
    match serde_json::to_value(out) {
        Ok(Value::Null) => Ok(None), // nothing returned: leave the context untouched
        Ok(v) => Ok(Some(v)),
        Err(e) => Err(StepError::permanent(format!("cannot encode the step's result: {e}"))),
    }
}

fn input<I: DeserializeOwned>(task: Task) -> std::result::Result<I, StepError> {
    let what = format!("the context of step '{}'", task.step_name);
    decode(task.context, &what)
}

pub(crate) fn task_handler<I, O, F, Fut>(f: F) -> Handler
where
    I: DeserializeOwned + Send + 'static,
    O: Serialize + 'static,
    F: Fn(I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = std::result::Result<O, StepError>> + Send + 'static,
{
    let f = Arc::new(f);
    Arc::new(move |task: Task| {
        let f = f.clone();
        Box::pin(async move { Ok(Outcome::Merge(encode(f(input(task)?).await?)?)) })
    })
}

pub(crate) fn effect_handler<I, F, Fut>(f: F) -> Handler
where
    I: DeserializeOwned + Send + 'static,
    F: Fn(I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = std::result::Result<(), StepError>> + Send + 'static,
{
    let f = Arc::new(f);
    Arc::new(move |task: Task| {
        let f = f.clone();
        Box::pin(async move {
            f(input(task)?).await?;
            Ok(Outcome::Merge(None))
        })
    })
}

pub(crate) fn gate_handler<I, F, Fut>(f: F) -> Handler
where
    I: DeserializeOwned + Send + 'static,
    F: Fn(I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = std::result::Result<bool, StepError>> + Send + 'static,
{
    let f = Arc::new(f);
    Arc::new(move |task: Task| {
        let f = f.clone();
        Box::pin(async move { Ok(Outcome::Predicate(f(input(task)?).await?)) })
    })
}

pub(crate) fn compensation_handler<F, Fut>(f: F) -> Handler
where
    F: Fn(Compensation) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = std::result::Result<(), StepError>> + Send + 'static,
{
    let f = Arc::new(f);
    Arc::new(move |task: Task| {
        let f = f.clone();
        Box::pin(async move {
            let (input, result) = match task.context {
                Value::Object(mut m) => (m.remove("input").unwrap_or(Value::Null), m.remove("result").unwrap_or(Value::Null)),
                _ => (Value::Null, Value::Null),
            };
            f(Compensation { input, result }).await?;
            Ok(Outcome::Merge(None))
        })
    })
}

/// Binds handlers to the steps of registered workflows, by name, and runs them.
///
/// Handlers are async and typed: the step's context is decoded into the handler's input type with
/// serde (a decode failure fails the step permanently) and its return encoded back. A task's
/// return is the **complete** next context -- it replaces the previous one; returning a value that
/// serializes to `null` (such as `None`) leaves it untouched. Inside a forEach body the input is
/// the item's value and the return replaces the item; the frozen base is [`Step::base`].
///
/// [`Step::base`]: crate::Step::base
pub struct Worker {
    client: Client,
    id: String,
    options: WorkerOptions,
    bindings: Vec<Binding>,
    error: Option<String>,
}

impl Worker {
    pub fn new(client: Client, worker_id: impl Into<String>) -> Self {
        Worker { client, id: worker_id.into(), options: WorkerOptions::default(), bindings: Vec::new(), error: None }
    }

    pub fn options(mut self, options: WorkerOptions) -> Self {
        self.options = options;
        self
    }

    fn bind(mut self, workflow: &str, step: &str, kind: Kind, handler: Handler) -> Self {
        let binding = Binding { workflow: workflow.to_string(), step: step.to_string(), kind, handler };
        let activity = binding.activity();
        if self.bindings.iter().any(|b| b.activity() == activity) && self.error.is_none() {
            self.error = Some(format!("duplicate handler for activity '{activity}'"));
        }
        self.bindings.push(binding);
        self
    }

    /// Binds a task step: `f` receives the decoded context and returns the complete next context.
    pub fn handle<I, O, F, Fut>(self, workflow: &str, step: &str, f: F) -> Self
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<O, StepError>> + Send + 'static,
    {
        self.bind(workflow, step, Kind::Task, task_handler(f))
    }

    /// Binds a side-effect step: the context is left unchanged.
    pub fn effect<I, F, Fut>(self, workflow: &str, step: &str, f: F) -> Self
    where
        I: DeserializeOwned + Send + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), StepError>> + Send + 'static,
    {
        self.bind(workflow, step, Kind::Task, effect_handler(f))
    }

    /// Binds a predicate step: a gate, a choose guard, or a do-while condition.
    pub fn gate<I, F, Fut>(self, workflow: &str, step: &str, f: F) -> Self
    where
        I: DeserializeOwned + Send + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<bool, StepError>> + Send + 'static,
    {
        self.bind(workflow, step, Kind::Gate, gate_handler(f))
    }

    /// Binds the mandatory combine after a fork or forEach. `f` returns the **complete** post-join
    /// context: the engine replaces the context with it, so keys it omits do not survive the join
    /// (there is no implicit fold of the arms).
    pub fn combine<O, F, Fut>(self, workflow: &str, step: &str, f: F) -> Self
    where
        O: Serialize + 'static,
        F: Fn(Combine) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<O, StepError>> + Send + 'static,
    {
        let f = Arc::new(f);
        self.bind(workflow, step, Kind::Combine, Arc::new(move |task: Task| {
            let f = f.clone();
            Box::pin(async move { Ok(Outcome::Merge(encode(f(Combine::of(&task)).await?)?)) })
        }))
    }

    /// Binds the undo of a step declared [`compensate`](crate::GraphBuilder::compensate). It runs
    /// in the reverse pass after the instance fails, newest-completed first, at least once -- make
    /// it idempotent. `start` refuses a worker that serves a compensable step without its
    /// compensator: the undo task lands on the same queue, and a worker that can do but not undo
    /// would strand the reverse pass.
    pub fn compensation<F, Fut>(self, workflow: &str, step: &str, f: F) -> Self
    where
        F: Fn(Compensation) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), StepError>> + Send + 'static,
    {
        self.bind(workflow, step, Kind::Compensate, compensation_handler(f))
    }

    /// Binds every handler a typed [`Flow`](crate::Flow) was defined with -- each step, gate,
    /// combine and compensator -- under the step names the flow gave them.
    pub fn flow(mut self, flow: &crate::typed::Flow) -> Self {
        for b in flow.bindings() {
            self = self.bind(flow.name(), &b.step, b.kind, b.handler.clone());
        }
        self
    }

    /// Reconciles every binding against its registered graph -- a mistyped step name, a kind
    /// mismatch or a missing compensator fails here -- then starts polling.
    pub async fn start(self) -> Result<RunningWorker> {
        if let Some(e) = self.error {
            return Err(Error::Registration(e));
        }
        if self.bindings.is_empty() {
            return Err(Error::Registration(format!("worker {} binds no handlers", self.id)));
        }
        let mut graphs = HashMap::new();
        for workflow in self.bindings.iter().map(|b| b.workflow.clone()).collect::<BTreeSet<_>>() {
            let graph = fetch_graph(&self.client, &workflow, self.options.await_registration).await?;
            graphs.insert(workflow, graph);
        }
        let learned = reconcile(&self.bindings, &graphs).map_err(Error::Registration)?;
        let queues: Vec<String> = match &self.options.queues {
            Some(only) => only.iter().cloned().collect::<BTreeSet<_>>().into_iter().collect(),
            None => learned.into_iter().collect(),
        };
        let handlers: HashMap<String, (Kind, Handler)> =
            self.bindings.into_iter().map(|b| (b.activity(), (b.kind, b.handler))).collect();

        let inner = Arc::new(Inner { client: self.client, id: self.id, options: self.options, queues, handlers });
        let cancel = CancellationToken::new();
        log::info!(
            "wiggle: worker {} polling queues {:?} (concurrency {})",
            inner.id,
            inner.queues,
            inner.options.concurrency
        );
        let join = tokio::spawn(poll_loop(inner.clone(), cancel.clone()));
        Ok(RunningWorker { inner, cancel, join: Some(join) })
    }
}

/// A started worker. [`shutdown`](Self::shutdown) stops polling and waits for in-flight steps;
/// dropping it stops polling without waiting.
pub struct RunningWorker {
    inner: Arc<Inner>,
    cancel: CancellationToken,
    join: Option<JoinHandle<()>>,
}

impl RunningWorker {
    pub fn worker_id(&self) -> &str {
        &self.inner.id
    }

    /// The queues this worker polls.
    pub fn queues(&self) -> &[String] {
        &self.inner.queues
    }

    /// Stops polling and waits for the in-flight steps to finish and report.
    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        if let Some(join) = self.join.take() {
            let _ = join.await;
        }
    }
}

impl Drop for RunningWorker {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

struct Inner {
    client: Client,
    id: String,
    options: WorkerOptions,
    queues: Vec<String>,
    handlers: HashMap<String, (Kind, Handler)>,
}

async fn fetch_graph(client: &Client, workflow: &str, await_registration: Duration) -> Result<Value> {
    let deadline = Instant::now() + await_registration;
    loop {
        match client.get_workflow(workflow).await {
            Ok(graph) => return Ok(graph),
            Err(_) if Instant::now() < deadline => tokio::time::sleep(Duration::from_millis(250)).await,
            Err(e) => {
                return Err(Error::Registration(format!(
                    "workflow '{workflow}' is not registered; register its graph before starting a worker \
                     that binds handlers to it (or set await_registration): {e}"
                )))
            }
        }
    }
}

/// Checks every binding against its workflow's graph and returns the queues the bound steps poll.
/// Pure -- graphs in, queues out -- so every rule is testable offline against a compiled FlowSpec.
fn reconcile(bindings: &[Binding], graphs: &HashMap<String, Value>) -> std::result::Result<BTreeSet<String>, String> {
    let mut queues = BTreeSet::new();
    for (workflow, graph) in graphs {
        let nodes: BTreeMap<String, &Map<String, Value>> = graph
            .get("nodes")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_object)
            .filter(|n| matches!(str_of(n, "kind"), "TASK" | "PREDICATE"))
            .filter_map(|n| n.get("name").and_then(Value::as_str).map(|name| (name.to_string(), n)))
            .collect();
        let bound: Vec<&Binding> = bindings.iter().filter(|b| &b.workflow == workflow).collect();
        for b in &bound {
            let node = nodes.get(&b.step).ok_or_else(|| {
                format!(
                    "no step '{}' in registered workflow '{workflow}' (available: {:?})",
                    b.step,
                    nodes.keys().collect::<Vec<_>>()
                )
            })?;
            let kind = str_of(node, "kind");
            let combine = is_combine(node);
            let compensable = node.get("compensable").and_then(Value::as_bool).unwrap_or(false);
            let activity = format!("{workflow}#{}", b.step);
            let expected = match (kind, combine) {
                ("PREDICATE", _) => Kind::Gate,
                (_, true) => Kind::Combine,
                _ => Kind::Task,
            };
            match b.kind {
                Kind::Compensate if expected != Kind::Task || !compensable => {
                    return Err(format!(
                        "step '{}' of workflow '{workflow}' is not declared compensate() in the topology; \
                         compensation() binds only compensable steps",
                        b.step
                    ))
                }
                Kind::Compensate => {}
                k if k != expected => {
                    return Err(format!(
                        "activity '{activity}' is a {} in the graph but bound with {}; use {}",
                        describe(expected),
                        k.verb(),
                        expected.verb()
                    ))
                }
                _ => {}
            }
            let queue = node.get("queue").and_then(Value::as_str).filter(|q| !q.is_empty()).unwrap_or(workflow);
            queues.insert(queue.to_string());
        }
        let compensated: HashSet<&str> =
            bound.iter().filter(|b| b.kind == Kind::Compensate).map(|b| b.step.as_str()).collect();
        for b in bound.iter().filter(|b| b.kind == Kind::Task && !compensated.contains(b.step.as_str())) {
            if nodes[&b.step].get("compensable").and_then(Value::as_bool).unwrap_or(false) {
                return Err(format!(
                    "step '{}' of workflow '{workflow}' is declared compensate() but this worker binds no \
                     compensator; add .compensation(\"{workflow}\", \"{}\", ..)",
                    b.step, b.step
                ));
            }
        }
    }
    Ok(queues)
}

/// A combine is a TASK carrying a fork's arm names or a forEach's collect key (or, in graphs from
/// older clients, the legacy itemsKey).
fn is_combine(node: &Map<String, Value>) -> bool {
    str_of(node, "kind") == "TASK"
        && (node.get("armNames").and_then(Value::as_array).is_some_and(|a| !a.is_empty())
            || node.get("collectKey").is_some_and(|v| !v.is_null())
            || node.get("itemsKey").and_then(Value::as_str).is_some_and(|s| !s.is_empty()))
}

fn describe(kind: Kind) -> &'static str {
    match kind {
        Kind::Gate => "predicate",
        Kind::Combine => "combine",
        _ => "task",
    }
}

fn str_of<'a>(node: &'a Map<String, Value>, key: &str) -> &'a str {
    node.get(key).and_then(Value::as_str).unwrap_or("")
}

// ---- poll / execute ----

async fn poll_loop(w: Arc<Inner>, cancel: CancellationToken) {
    let permits = Arc::new(Semaphore::new(w.options.concurrency));
    while !cancel.is_cancelled() {
        let free = permits.available_permits();
        if free == 0 {
            tokio::select! {
                _ = cancel.cancelled() => break,
                permit = permits.clone().acquire_owned() => drop(permit),
            }
            continue;
        }
        let polled = tokio::select! {
            _ = cancel.cancelled() => break,
            r = w.client.poll(&w.id, &w.queues, free as i32, w.options.lease, w.options.long_poll_wait, &[]) => r,
        };
        let backoff = match polled {
            Err(e) => {
                log::warn!("wiggle: poll failed: {e}");
                w.options.error_backoff
            }
            Ok(res) if res.tasks.is_empty() => {
                if res.retry_after.is_zero() {
                    w.options.idle_backoff
                } else {
                    res.retry_after
                }
            }
            Ok(res) => {
                for task in res.tasks {
                    let permit = permits.clone().acquire_owned().await.expect("semaphore never closes");
                    let w = w.clone();
                    tokio::spawn(async move {
                        execute(&w, task).await;
                        drop(permit);
                    });
                }
                continue;
            }
        };
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(backoff) => {}
        }
    }
    // Wait for the in-flight steps to finish and report.
    let _ = permits.acquire_many(w.options.concurrency as u32).await;
}

async fn execute(w: &Inner, task: Task) {
    let Some((kind, handler)) = w.handlers.get(&task.activity).cloned() else {
        fail(w, &task, &format!("no handler registered for activity '{}'", task.activity), false).await;
        return;
    };
    let heartbeat = start_heartbeat(w, &task);
    let started = now_millis();
    let scope = Scope::of(&task);
    let run = {
        let scope = scope.clone();
        let task = task.clone();
        tokio::spawn(async move { scope.run(handler(task)).await })
    };
    let outcome = run.await;
    let finished = now_millis();
    heartbeat.cancel();

    let outcome = match outcome {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(e)) => return fail(w, &task, e.message(), e.is_retryable()).await,
        Err(join) => {
            let msg = if join.is_panic() { "the step handler panicked" } else { "the step handler was cancelled" };
            return fail(w, &task, msg, true).await;
        }
    };
    let mut report = StepReport {
        node_id: task.node_id.clone(),
        started_at: started,
        finished_at: finished,
        events: scope.drain(),
        ..Default::default()
    };
    match (task.kind.as_str(), outcome) {
        ("PREDICATE", Outcome::Predicate(b)) => report.predicate = Some(b),
        ("PREDICATE", Outcome::Merge(_)) | (_, Outcome::Predicate(_)) => {
            let msg = format!("activity '{}' is a {} task but is bound with {}", task.activity, task.kind, kind.verb());
            return fail(w, &task, &msg, false).await;
        }
        (_, Outcome::Merge(m)) => report.merge = m,
    }
    if let Err(e) = w.client.report_steps(&task.task_id, &task.lease_owner, vec![report], true).await {
        // Refused: the lease was lost and the step re-runs elsewhere.
        log::warn!("wiggle: report of step {} of {} failed: {e}", task.step_name, task.instance_id);
    }
}

async fn fail(w: &Inner, task: &Task, message: &str, retryable: bool) {
    if let Err(e) = w.client.fail(&task.task_id, &task.lease_owner, message, retryable).await {
        log::warn!("wiggle: failing step {} of {} failed: {e}", task.step_name, task.instance_id);
    }
}

/// Extends the lease every half lease (at least every second) while a step runs.
fn start_heartbeat(w: &Inner, task: &Task) -> CancellationToken {
    let stop = CancellationToken::new();
    let interval = (w.options.lease / 2).max(Duration::from_secs(1));
    let (client, lease) = (w.client.clone(), w.options.lease);
    let (task_id, owner) = (task.task_id.clone(), task.lease_owner.clone());
    let token = stop.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(interval) => {
                    if client.heartbeat(&task_id, &owner, lease).await.is_err() {
                        return;
                    }
                }
            }
        }
    });
    stop
}

fn now_millis() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flow::{Branch, FlowSpec};
    use serde_json::json;

    fn binding(step: &str, kind: Kind) -> Binding {
        let handler: Handler = Arc::new(|_| Box::pin(async { Ok(Outcome::Merge(None)) }));
        Binding { workflow: "order".into(), step: step.into(), kind, handler }
    }

    fn graphs() -> HashMap<String, Value> {
        let spec = FlowSpec::define("order", 1)
            .step("validate")
            .gate("in-stock")
            .step("charge")
            .queue("payments")
            .compensate()
            .fork(vec![
                Branch::of("a", |s| s.step("left")),
                Branch::of("b", |s| s.step("right")),
            ])
            .combine("merge")
            .build()
            .unwrap();
        HashMap::from([("order".to_string(), spec.definition().clone())])
    }

    #[test]
    fn learns_queues_of_bound_steps() {
        let b = vec![
            binding("validate", Kind::Task),
            binding("in-stock", Kind::Gate),
            binding("charge", Kind::Task),
            binding("charge", Kind::Compensate),
            binding("merge", Kind::Combine),
        ];
        let queues = reconcile(&b, &graphs()).unwrap();
        assert_eq!(queues.into_iter().collect::<Vec<_>>(), vec!["order", "payments"]);
    }

    #[test]
    fn rejects_unknown_step_and_wrong_kind() {
        let err = reconcile(&[binding("nope", Kind::Task)], &graphs()).unwrap_err();
        assert!(err.contains("no step 'nope'"), "{err}");
        let err = reconcile(&[binding("in-stock", Kind::Task)], &graphs()).unwrap_err();
        assert!(err.contains("use gate()"), "{err}");
        let err = reconcile(&[binding("merge", Kind::Task)], &graphs()).unwrap_err();
        assert!(err.contains("use combine()"), "{err}");
    }

    #[test]
    fn checks_compensation_pairing_both_ways() {
        let err = reconcile(&[binding("charge", Kind::Task)], &graphs()).unwrap_err();
        assert!(err.contains("binds no compensator"), "{err}");
        let err = reconcile(&[binding("validate", Kind::Compensate)], &graphs()).unwrap_err();
        assert!(err.contains("not declared compensate()"), "{err}");
    }

    #[test]
    fn combine_splits_arms_from_base() {
        let task = Task {
            context: json!({"orderId": "A-1", "__arm__a": {"paid": true}, "__arm__b": {"label": "L"}}),
            arm_names: vec!["a".into(), "b".into()],
            ..blank_task()
        };
        let c = Combine::of(&task);
        assert_eq!(c.base(), &json!({"orderId": "A-1"}));
        assert_eq!(c.arm("a"), Some(&json!({"paid": true})));
        assert_eq!(c.arms()[1].0, "b");
        assert!(c.items().is_none());

        let task = Task {
            context: json!({"n": 1, "__forEach__xs": [2, 3], "__steps____forEach__xs": ["s"]}),
            collect_key: Some("__forEach__xs".into()),
            ..blank_task()
        };
        let c = Combine::of(&task);
        assert_eq!(c.base(), &json!({"n": 1}));
        assert_eq!(c.items_as::<Vec<i32>>().unwrap(), vec![2, 3]);
    }

    fn blank_task() -> Task {
        Task {
            task_id: String::new(),
            instance_id: String::new(),
            workflow: String::new(),
            version: 1,
            node_id: String::new(),
            step_name: String::new(),
            activity: String::new(),
            kind: "TASK".into(),
            attempt: 1,
            lease_expires_at: 0,
            lease_owner: String::new(),
            context: Value::Null,
            execution_mode: "SERVER".into(),
            base_context: None,
            item_index: 0,
            item_map_key: None,
            collect_key: None,
            arm_names: Vec::new(),
            correlation_id: None,
            instance_created_at: 0,
        }
    }
}
