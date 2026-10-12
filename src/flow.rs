//! The topology DSL: a workflow is a pure graph of named steps -- how they chain, branch and
//! rejoin -- with no step logic. Handlers are bound separately, by name, on a [`Worker`].
//!
//! ```
//! use std::time::Duration;
//! use wiggle::{Branch, Case, GraphBuilder};
//!
//! let order = GraphBuilder::define("order-fulfilment", 1)
//!     .step("validate")
//!     .gate("in-stock")
//!     .fork(vec![
//!         Branch::of("payment", |s| s.step("charge").queue("payments")),
//!         Branch::of("shipping", |s| s.step("reserve").sleep(Duration::from_secs(2)).step("label")),
//!     ])
//!     .combine("settle")
//!     .choose(vec![
//!         Case::when("vip", |s| s.step("concierge")),
//!         Case::otherwise(|s| s.step("thanks")),
//!     ])
//!     .effect("notify")
//!     .build()
//!     .unwrap();
//! assert_eq!(order.version(), 1);
//! ```
//!
//! [`Worker`]: crate::Worker

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::Duration;

use serde_json::{json, Map, Value};

use crate::error::{Error, Result};
use crate::retry::RetryPolicy;
use crate::typed::{FlowBinding, WiggleFlow};
use crate::worker::Kind;

/// A compiled workflow topology (nodes, edges, kinds, queues, retry) to register with the server.
/// The graph sent to the server carries no step logic and no context type. A spec made by
/// [`FlowSpec::define`] also keeps the functions its steps were written with, for
/// [`Worker::flow`](crate::Worker::flow); one built with [`GraphBuilder`] keeps none.
#[derive(Clone)]
pub struct FlowSpec {
    name: String,
    version: i32,
    definition: Value,
    queues: Vec<String>,
    bindings: Vec<FlowBinding>,
}

impl FlowSpec {
    /// Defines the workflow `name`, published as `version`, as a chain of the functions that
    /// implement its steps, starting from a `T` -- Java's `FlowSpec.define(name, version,
    /// T.class, f -> ..)`. `body` runs once, here, and returns the handle the flow ends on. The
    /// version is declared, not derived: a published version is immutable, so a changed graph
    /// needs a new number. Steps that name no retry policy get [`RetryPolicy::forever`].
    pub fn define<T, X>(name: &str, version: i32, body: impl FnOnce(WiggleFlow<T>) -> WiggleFlow<X>) -> Result<FlowSpec> {
        crate::typed::define(name, version, None, body)
    }

    /// [`define`](Self::define) with the retry policy for every step that names none -- Java's
    /// `FlowSpec.define(name, version, defaultRetry, T.class, f -> ..)`.
    pub fn define_with_retry<T, X>(
        name: &str,
        version: i32,
        default_retry: RetryPolicy,
        body: impl FnOnce(WiggleFlow<T>) -> WiggleFlow<X>,
    ) -> Result<FlowSpec> {
        crate::typed::define(name, version, Some(default_retry), body)
    }

    pub(crate) fn with_bindings(mut self, bindings: Vec<FlowBinding>) -> Self {
        self.bindings = bindings;
        self
    }

    /// The step names this spec carries handlers for (none for a [`GraphBuilder`] spec).
    pub fn steps(&self) -> Vec<String> {
        self.bindings.iter().filter(|b| b.kind != Kind::Compensate).map(|b| b.step.clone()).collect()
    }

    pub(crate) fn bindings(&self) -> &[FlowBinding] {
        &self.bindings
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> i32 {
        self.version
    }

    /// The queues the workflow's steps are dispatched on, sorted.
    pub fn queues(&self) -> &[String] {
        &self.queues
    }

    /// The definition as the server receives it (the same JSON the Java client sends).
    pub fn definition(&self) -> &Value {
        &self.definition
    }
}

impl std::fmt::Debug for FlowSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlowSpec")
            .field("name", &self.name)
            .field("version", &self.version)
            .field("definition", &self.definition)
            .field("queues", &self.queues)
            .field("steps", &self.steps())
            .finish()
    }
}

// ---- recorded topology ----

#[derive(Debug, Clone)]
struct WorkerStep {
    name: String,
    queue: Option<String>,
    retry: Option<RetryPolicy>,
}

impl WorkerStep {
    fn named(name: impl Into<String>) -> Self {
        WorkerStep { name: name.into(), queue: None, retry: None }
    }
}

#[derive(Debug, Clone)]
enum Node {
    Task { step: WorkerStep, compensable: bool },
    Gate { step: WorkerStep },
    Sleep { name: Option<String>, millis: u64 },
    Signal { name: String, timeout_millis: u64, escalation: Option<Vec<Node>> },
    SubWorkflow { name: String, workflow: String },
    Fork { branches: Vec<(String, Vec<Node>)>, combine: WorkerStep },
    ForEach { name: String, items_key: String, body: Vec<Node>, combine: WorkerStep },
    Choose { cases: Vec<(Option<WorkerStep>, Vec<Node>)> },
    DoWhile { cond: WorkerStep, budget: i64, body: Vec<Node> },
    /// The combine of the branches the task before it creates at run time (`Step::create`).
    SpawnCombine { creator: String, combine: WorkerStep },
}

#[derive(Debug)]
struct RootConf {
    name: String,
    version: i32,
    default_queue: Option<String>,
    default_retry: RetryPolicy,
}

/// Records a workflow's steps in order, naming each as a string -- the lower-level topology
/// builder (Java's `GraphBuilder`), started with [`GraphBuilder::define`]; nested bodies (a branch,
/// a case, a loop body, an escalation) receive a fresh builder of their own.
///
/// Misuse (a modifier with no step before it, an empty branch, ...) is recorded and reported by
/// [`build`](GraphBuilder::build), so a definition reads as one uninterrupted chain.
#[derive(Debug)]
pub struct GraphBuilder {
    root: Option<RootConf>,
    nodes: Vec<Node>,
    error: Option<String>,
}

impl GraphBuilder {
    /// Starts defining the workflow `name`, published as `version` -- Java's
    /// `Workflow.define(name, version)`. The version is declared, not derived: a published version
    /// is immutable, so a changed graph needs a new number.
    pub fn define(name: impl Into<String>, version: i32) -> GraphBuilder {
        GraphBuilder::root(name.into(), version)
    }

    fn root(name: String, version: i32) -> Self {
        GraphBuilder {
            root: Some(RootConf { name, version, default_queue: None, default_retry: RetryPolicy::forever() }),
            nodes: Vec::new(),
            error: None,
        }
    }

    fn child() -> Self {
        GraphBuilder { root: None, nodes: Vec::new(), error: None }
    }

    pub(crate) fn fail(mut self, msg: impl Into<String>) -> Self {
        if self.error.is_none() {
            self.error = Some(msg.into());
        }
        self
    }

    fn push(mut self, node: Node) -> Self {
        self.nodes.push(node);
        self
    }

    /// Runs a nested body on a fresh builder; its first error becomes this builder's.
    fn nested(&mut self, body: impl FnOnce(GraphBuilder) -> GraphBuilder) -> Vec<Node> {
        let sub = body(GraphBuilder::child());
        if self.error.is_none() {
            self.error = sub.error;
        }
        sub.nodes
    }

    /// A task run on a worker; its handler's return is the complete next context.
    pub fn step(self, name: impl Into<String>) -> Self {
        self.push(Node::Task { step: WorkerStep::named(name), compensable: false })
    }

    /// Alias of [`step`](Self::step), reading better mid-chain.
    pub fn then(self, name: impl Into<String>) -> Self {
        self.step(name)
    }

    /// A step run for its side effect only; the context is left unchanged. Topologically a task --
    /// the difference is only in the bound handler ([`Worker::effect`](crate::Worker::effect)).
    pub fn effect(self, name: impl Into<String>) -> Self {
        self.step(name)
    }

    /// A predicate step: the flow continues while it holds; false ends the instance as
    /// `gated:<name>` (inside a fork branch, it short-circuits to the fork's join).
    pub fn gate(self, name: impl Into<String>) -> Self {
        self.push(Node::Gate { step: WorkerStep::named(name) })
    }

    /// A server-side timer; no worker is held while it runs.
    pub fn sleep(self, duration: Duration) -> Self {
        self.push(Node::Sleep { name: None, millis: duration.as_millis() as u64 })
    }

    /// [`sleep`](Self::sleep) with a node name, for a nicer console label.
    pub fn sleep_named(self, name: impl Into<String>, duration: Duration) -> Self {
        self.push(Node::Sleep { name: Some(name.into()), millis: duration.as_millis() as u64 })
    }

    /// Waits for the named signal ([`Client::signal`](crate::Client::signal)); its payload merges
    /// into the context. Waits indefinitely.
    pub fn await_signal(self, name: impl Into<String>) -> Self {
        self.push(Node::Signal { name: name.into(), timeout_millis: 0, escalation: None })
    }

    /// [`await_signal`](Self::await_signal) that fails the instance when `timeout` passes first.
    pub fn await_signal_timeout(self, name: impl Into<String>, timeout: Duration) -> Self {
        self.push(Node::Signal { name: name.into(), timeout_millis: timeout.as_millis() as u64, escalation: None })
    }

    /// [`await_signal`](Self::await_signal) that, when `timeout` passes first, runs `escalation`
    /// and rejoins the flow instead of failing.
    pub fn await_signal_or(
        mut self,
        name: impl Into<String>,
        timeout: Duration,
        escalation: impl FnOnce(GraphBuilder) -> GraphBuilder,
    ) -> Self {
        let name = name.into();
        if timeout.is_zero() {
            return self.fail(format!("await_signal '{name}' escalation needs a positive timeout"));
        }
        let body = self.nested(escalation);
        self.push(Node::Signal { name, timeout_millis: timeout.as_millis() as u64, escalation: Some(body) })
    }

    /// Runs another registered workflow as a child; its result merges back. `name` is this node's
    /// name, `workflow` the child's.
    pub fn sub_workflow(self, name: impl Into<String>, workflow: impl Into<String>) -> Self {
        self.push(Node::SubWorkflow { name: name.into(), workflow: workflow.into() })
    }

    /// Fans out into parallel branches (at least two), each on its own isolated copy of the
    /// context, that all join before the flow continues. There is no implicit merge: the returned
    /// stage requires a [`combine`](ForkStage::combine).
    pub fn fork(self, branches: Vec<Branch>) -> ForkStage {
        ForkStage { builder: self, branches }
    }

    /// Runtime fan-out over the list or map in the context under `items_key`: one isolated branch
    /// per element. **The element is the item's context** -- each body step receives the item's
    /// current value and its return replaces it; the frozen pre-forEach context is
    /// [`Step::base`](crate::Step::base). The node is named after `items_key`.
    pub fn for_each(self, items_key: impl Into<String>, body: impl FnOnce(GraphBuilder) -> GraphBuilder) -> ForEachStage {
        let key = items_key.into();
        self.for_each_named(key.clone(), key, body)
    }

    /// [`for_each`](Self::for_each) with an explicit node name -- needed when one workflow fans
    /// over the same key twice (node names are unique).
    pub fn for_each_named(
        mut self,
        name: impl Into<String>,
        items_key: impl Into<String>,
        body: impl FnOnce(GraphBuilder) -> GraphBuilder,
    ) -> ForEachStage {
        let body = self.nested(body);
        ForEachStage { builder: self, name: name.into(), items_key: items_key.into(), body }
    }

    /// Runs `body`, then repeats it while the predicate step `condition` holds (the body runs at
    /// least once). Budgeted by the engine default (`WIGGLE_LOOP_MAX_ITERATIONS`, 10,000).
    pub fn do_while(self, condition: impl Into<String>, body: impl FnOnce(GraphBuilder) -> GraphBuilder) -> Self {
        self.loop_with(condition.into(), -1, body)
    }

    /// [`do_while`](Self::do_while) with an explicit iteration budget: one more true evaluation
    /// than `max_iterations` fails the instance.
    pub fn do_while_max(
        self,
        condition: impl Into<String>,
        max_iterations: u32,
        body: impl FnOnce(GraphBuilder) -> GraphBuilder,
    ) -> Self {
        let condition = condition.into();
        if max_iterations == 0 {
            return self.fail(format!("do_while '{condition}': max_iterations must be positive"));
        }
        self.loop_with(condition, i64::from(max_iterations), body)
    }

    fn loop_with(mut self, condition: String, budget: i64, body: impl FnOnce(GraphBuilder) -> GraphBuilder) -> Self {
        let body = self.nested(body);
        self.push(Node::DoWhile { cond: WorkerStep::named(condition), budget, body })
    }

    /// Exclusive choice: the first case whose guard holds runs; an [`otherwise`](Case::otherwise)
    /// case, which must be last, runs when none did. With no otherwise, an unmatched choice
    /// continues after it.
    pub fn choose(mut self, cases: Vec<Case>) -> Self {
        let mut out = Vec::with_capacity(cases.len());
        for case in cases {
            let body = self.nested(case.body);
            out.push((case.guard, body));
        }
        self.push(Node::Choose { cases: out })
    }

    /// Makes the task just added a **spawning step** -- its handler may open branches at run time
    /// with [`Step::create`](crate::Step::create) -- and adds the combine they rejoin at. The
    /// combine receives the branches' results (a list in creation order, or a map by key) and the
    /// spawning step's return as [`Step::base`](crate::Step::base); it may itself create branches,
    /// starting another round. Must directly follow `step()` or `effect()`.
    pub fn spawn_combine(self, name: impl Into<String>) -> Self {
        let creator = match self.nodes.last() {
            Some(Node::Task { step, .. }) => step.name.clone(),
            _ => {
                return self.fail(
                    "spawn_combine() must directly follow the step that creates the branches it combines: \
                     a step() or effect()",
                )
            }
        };
        self.push(Node::SpawnCombine { creator, combine: WorkerStep::named(name) })
    }

    /// The queue for the step just added (task, effect, gate, a combine, or a loop's condition). Defaults to the
    /// workflow's default queue.
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        match self.last_worker_step() {
            Some(step) => step.queue = Some(queue.into()),
            None => return self.fail("queue() must directly follow step(), effect(), gate() or combine()"),
        }
        self
    }

    /// The retry policy for the step just added (task, effect, gate, or a combine). Defaults to
    /// the workflow's default policy ([`RetryPolicy::forever`] unless changed).
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        if let Err(e) = retry.validate() {
            return self.fail(e);
        }
        match self.last_worker_step() {
            Some(step) => step.retry = Some(retry),
            None => return self.fail("retry() must directly follow step(), effect(), gate() or combine()"),
        }
        self
    }

    /// Marks the step just added as compensable: if the instance later fails, the step's
    /// compensator ([`Worker::compensation`](crate::Worker::compensation)) runs in the reverse
    /// pass, fed the input/result snapshots captured when the step completed.
    pub fn compensate(mut self) -> Self {
        match self.nodes.last_mut() {
            Some(Node::Task { compensable, .. }) => *compensable = true,
            Some(Node::Fork { .. }) | Some(Node::ForEach { .. }) | Some(Node::SpawnCombine { .. }) => {
                return self.fail("compensate() applies to a step or effect, not a combine")
            }
            _ => return self.fail("compensate() must directly follow step() or effect()"),
        }
        self
    }

    /// The queue for steps that name none (defaults to the workflow name). Top level only.
    pub fn default_queue(mut self, queue: impl Into<String>) -> Self {
        match self.root.as_mut() {
            Some(root) => root.default_queue = Some(queue.into()),
            None => return self.fail("default_queue() applies to the workflow, not a nested body"),
        }
        self
    }

    /// The retry policy for steps that name none (defaults to [`RetryPolicy::forever`]). Top level only.
    pub fn default_retry(mut self, retry: RetryPolicy) -> Self {
        if let Err(e) = retry.validate() {
            return self.fail(e);
        }
        match self.root.as_mut() {
            Some(root) => root.default_retry = retry,
            None => return self.fail("default_retry() applies to the workflow, not a nested body"),
        }
        self
    }

    fn last_worker_step(&mut self) -> Option<&mut WorkerStep> {
        match self.nodes.last_mut()? {
            Node::Task { step, .. } | Node::Gate { step } => Some(step),
            Node::Fork { combine, .. } | Node::ForEach { combine, .. } => Some(combine),
            Node::DoWhile { cond, .. } => Some(cond),
            Node::SpawnCombine { combine, .. } => Some(combine),
            _ => None,
        }
    }

    /// Compiles the recorded topology into a [`FlowSpec`], or reports the first problem found.
    pub fn build(self) -> Result<FlowSpec> {
        let root = self
            .root
            .ok_or_else(|| Error::Flow("build() applies to the workflow, not a nested body".into()))?;
        if let Some(e) = self.error {
            return Err(Error::Flow(e));
        }
        compile(root, &self.nodes).map_err(Error::Flow)
    }
}

/// One arm of a [`GraphBuilder::fork`]. Its name keys the arm's result for the combine.
pub struct Branch {
    name: String,
    body: Box<dyn FnOnce(GraphBuilder) -> GraphBuilder>,
}

impl Branch {
    pub fn of(name: impl Into<String>, body: impl FnOnce(GraphBuilder) -> GraphBuilder + 'static) -> Self {
        Branch { name: name.into(), body: Box::new(body) }
    }
}

/// One arm of a [`GraphBuilder::choose`].
pub struct Case {
    guard: Option<WorkerStep>,
    body: Box<dyn FnOnce(GraphBuilder) -> GraphBuilder>,
}

impl Case {
    /// Runs `body` when the predicate step `guard` holds (and no earlier case did).
    pub fn when(guard: impl Into<String>, body: impl FnOnce(GraphBuilder) -> GraphBuilder + 'static) -> Self {
        Case { guard: Some(WorkerStep::named(guard)), body: Box::new(body) }
    }

    /// Runs `body` when no guarded case held. Must be last.
    pub fn otherwise(body: impl FnOnce(GraphBuilder) -> GraphBuilder + 'static) -> Self {
        Case { guard: None, body: Box::new(body) }
    }

    /// The queue the guard step is dispatched on.
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        if let Some(g) = self.guard.as_mut() {
            g.queue = Some(queue.into());
        }
        self
    }

    /// The guard step's retry policy.
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        if let Some(g) = self.guard.as_mut() {
            g.retry = Some(retry);
        }
        self
    }
}

/// The mandatory stage after [`GraphBuilder::fork`]: the branches must rejoin at an explicit
/// combine step, whose handler ([`Worker::combine`](crate::Worker::combine)) receives every arm's
/// result and returns the complete post-join context.
#[must_use = "a fork needs combine(..) to rejoin its branches"]
pub struct ForkStage {
    builder: GraphBuilder,
    branches: Vec<Branch>,
}

impl ForkStage {
    /// Declares the combine step `name` and resumes the flow after it. Follow it with
    /// [`queue`](GraphBuilder::queue) / [`retry`](GraphBuilder::retry) to tune the combine.
    pub fn combine(self, name: impl Into<String>) -> GraphBuilder {
        let mut b = self.builder;
        if self.branches.len() < 2 {
            return b.fail("fork needs at least two branches");
        }
        let mut arms = Vec::with_capacity(self.branches.len());
        for br in self.branches {
            let body = b.nested(br.body);
            arms.push((br.name, body));
        }
        b.push(Node::Fork { branches: arms, combine: WorkerStep::named(name) })
    }
}

/// The mandatory stage after [`GraphBuilder::for_each`]: item results rejoin at an explicit
/// combine step, whose handler receives every item's final value -- a list ordered by index for a
/// list input, a map keyed like the input for a map input.
#[must_use = "a for_each needs combine(..) to collect its items"]
pub struct ForEachStage {
    builder: GraphBuilder,
    name: String,
    items_key: String,
    body: Vec<Node>,
}

impl ForEachStage {
    /// Declares the combine step `name` and resumes the flow after it.
    pub fn combine(self, name: impl Into<String>) -> GraphBuilder {
        self.builder.push(Node::ForEach {
            name: self.name,
            items_key: self.items_key,
            body: self.body,
            combine: WorkerStep::named(name),
        })
    }
}

// ---- compiler: open-ends wiring, as the Java GraphBuilder does it ----

struct Graph {
    workflow: String,
    default_queue: String,
    default_retry: RetryPolicy,
    nodes: BTreeMap<String, Map<String, Value>>,
    queues: BTreeSet<String>,
    names: HashSet<String>,
    start: Option<String>,
    counter: u32,
}

type Compile<T> = std::result::Result<T, String>;

impl Graph {
    fn add(&mut self, prefix: &str, mut node: Map<String, Value>) -> String {
        self.counter += 1;
        let id = format!("{prefix}{}", self.counter);
        node.insert("id".into(), json!(id));
        self.nodes.insert(id.clone(), node);
        id
    }

    fn reserve(&mut self, name: &str) -> Compile<()> {
        if name.trim().is_empty() {
            return Err("step name is required".into());
        }
        if !self.names.insert(name.to_string()) {
            return Err(format!("duplicate step name '{name}' in workflow {}", self.workflow));
        }
        Ok(())
    }

    fn add_worker(&mut self, kind: &str, step: &WorkerStep) -> Compile<String> {
        self.reserve(&step.name)?;
        let queue = step.queue.clone().filter(|q| !q.trim().is_empty()).unwrap_or_else(|| self.default_queue.clone());
        self.queues.insert(queue.clone());
        let retry = step.retry.unwrap_or(self.default_retry);
        let mut node = Map::new();
        node.insert("kind".into(), json!(kind));
        node.insert("name".into(), json!(step.name));
        node.insert("activity".into(), json!(format!("{}#{}", self.workflow, step.name)));
        node.insert("queue".into(), json!(queue));
        node.insert("retry".into(), retry.to_json());
        Ok(self.add("n", node))
    }

    fn add_plain(&mut self, prefix: &str, kind: &str, name: &str) -> String {
        let mut node = Map::new();
        node.insert("kind".into(), json!(kind));
        node.insert("name".into(), json!(name));
        self.add(prefix, node)
    }

    fn add_join(&mut self, expected: usize) -> String {
        let id = self.add_plain("join", "JOIN", "join");
        if expected > 0 {
            self.set(&id, "expected", json!(expected));
        }
        id
    }

    fn add_end(&mut self, reason: Option<String>) -> String {
        let id = self.add_plain("end", "END", "end");
        self.set(&id, "success", json!(true));
        if let Some(r) = reason {
            self.set(&id, "reason", json!(r));
        }
        id
    }

    fn set(&mut self, id: &str, key: &str, value: Value) {
        self.nodes.get_mut(id).expect("known node").insert(key.into(), value);
    }

    fn wire(&mut self, end: &OpenEnd, target: &str) {
        let key = if end.alt { "altNext" } else { "next" };
        self.set(&end.node, key, json!(target));
    }
}

#[derive(Clone)]
struct OpenEnd {
    node: String,
    alt: bool,
}

impl OpenEnd {
    fn next(node: &str) -> Self {
        OpenEnd { node: node.into(), alt: false }
    }
    fn alt(node: &str) -> Self {
        OpenEnd { node: node.into(), alt: true }
    }
}

/// One stream of nodes being wired: its first node, and the edges still waiting for a successor.
struct Stream {
    enclosing_join: Option<String>,
    open: Vec<OpenEnd>,
    start: Option<String>,
}

impl Stream {
    fn new(enclosing_join: Option<String>) -> Self {
        Stream { enclosing_join, open: Vec::new(), start: None }
    }

    /// Routes the open ends into `id` (or records it as the stream's start), opening nothing.
    fn route_into(&mut self, g: &mut Graph, id: &str) {
        if self.open.is_empty() {
            if self.start.is_none() {
                self.start = Some(id.to_string());
            }
        } else {
            for end in std::mem::take(&mut self.open) {
                g.wire(&end, id);
            }
        }
    }

    fn attach(&mut self, g: &mut Graph, id: &str) {
        self.route_into(g, id);
        self.open = vec![OpenEnd::next(id)];
    }

    fn wire_open_to(&mut self, g: &mut Graph, target: &str) {
        for end in std::mem::take(&mut self.open) {
            g.wire(&end, target);
        }
    }

    fn append_all(&mut self, g: &mut Graph, nodes: &[Node]) -> Compile<()> {
        for n in nodes {
            self.append(g, n)?;
        }
        Ok(())
    }

    fn sub(&self, g: &mut Graph, enclosing_join: Option<String>, nodes: &[Node], what: &str) -> Compile<Stream> {
        let mut s = Stream::new(enclosing_join);
        s.append_all(g, nodes)?;
        if s.start.is_none() {
            return Err(format!("{what} defines no steps"));
        }
        Ok(s)
    }

    fn append(&mut self, g: &mut Graph, node: &Node) -> Compile<()> {
        match node {
            Node::Task { step, compensable } => {
                let id = g.add_worker("TASK", step)?;
                if *compensable {
                    g.set(&id, "compensable", json!(true));
                }
                self.attach(g, &id);
            }
            Node::Gate { step } => {
                let id = g.add_worker("PREDICATE", step)?;
                self.route_into(g, &id);
                let target = match &self.enclosing_join {
                    Some(join) => join.clone(),
                    None => g.add_end(Some(format!("gated:{}", step.name))),
                };
                g.wire(&OpenEnd::alt(&id), &target);
                self.open = vec![OpenEnd::next(&id)];
            }
            Node::Sleep { name, millis } => {
                let name = name.clone().unwrap_or_else(|| format!("sleep-{millis}ms"));
                let id = g.add_plain("n", "SLEEP", &name);
                if *millis > 0 {
                    g.set(&id, "sleepMillis", json!(millis));
                }
                self.attach(g, &id);
            }
            Node::Signal { name, timeout_millis, escalation } => {
                g.reserve(name)?;
                let id = g.add_plain("n", "SIGNAL", name);
                if *timeout_millis > 0 {
                    g.set(&id, "sleepMillis", json!(timeout_millis));
                }
                self.route_into(g, &id);
                self.open = vec![OpenEnd::next(&id)];
                if let Some(body) = escalation {
                    let what = format!("escalation branch of '{name}'");
                    let esc = self.sub(g, self.enclosing_join.clone(), body, &what)?;
                    g.wire(&OpenEnd::alt(&id), esc.start.as_deref().expect("checked"));
                    self.open.extend(esc.open);
                }
            }
            Node::SubWorkflow { name, workflow } => {
                if workflow.trim().is_empty() {
                    return Err(format!("sub_workflow '{name}' is missing its child workflow name"));
                }
                g.reserve(name)?;
                let id = g.add_plain("n", "SUB_WORKFLOW", name);
                g.set(&id, "activity", json!(workflow));
                self.attach(g, &id);
            }
            Node::Fork { branches, combine } => {
                let fork = g.add_plain("fork", "FORK", "fork");
                self.route_into(g, &fork);
                let join = g.add_join(branches.len());
                let mut starts = Vec::with_capacity(branches.len());
                let mut arms = Vec::with_capacity(branches.len());
                for (name, body) in branches {
                    starts.push(self.branch(g, &join, body, &format!("branch '{name}'"))?);
                    arms.push(name.clone());
                }
                g.set(&fork, "branches", json!(starts));
                let combine_id = g.add_worker("TASK", combine)?;
                g.set(&combine_id, "armNames", json!(arms));
                g.wire(&OpenEnd::next(&join), &combine_id);
                self.open = vec![OpenEnd::next(&combine_id)];
            }
            Node::ForEach { name, items_key, body, combine } => {
                if items_key.trim().is_empty() {
                    return Err("for_each needs an items key".into());
                }
                g.reserve(name)?;
                let fork = g.add_plain("dynfork", "DYN_FORK", name);
                g.set(&fork, "itemsKey", json!(items_key));
                g.set(&fork, "itemKey", json!(items_key));
                self.route_into(g, &fork);
                let join = g.add_join(0);
                let template = self.branch(g, &join, body, &format!("for_each '{name}' body"))?;
                g.set(&fork, "branches", json!([template]));
                g.wire(&OpenEnd::next(&fork), &join); // taken when the collection is empty
                let combine_id = g.add_worker("TASK", combine)?;
                g.set(&combine_id, "collectKey", json!(format!("__forEach__{name}")));
                g.wire(&OpenEnd::next(&join), &combine_id);
                self.open = vec![OpenEnd::next(&combine_id)];
            }
            Node::Choose { cases } => self.choose(g, cases)?,
            Node::SpawnCombine { creator, combine } => {
                let join = g.add_join(0);
                self.attach(g, &join);
                let combine_id = g.add_worker("TASK", combine)?;
                g.set(&combine_id, "collectKey", json!(format!("__spawn__{creator}")));
                self.attach(g, &combine_id);
            }
            Node::DoWhile { cond, budget, body } => {
                let what = format!("do_while '{}' body", cond.name);
                let mut sub = self.sub(g, self.enclosing_join.clone(), body, &what)?;
                let cond_id = g.add_worker("PREDICATE", cond)?;
                g.set(&cond_id, "loopBudget", json!(budget));
                let body_start = sub.start.clone().expect("checked");
                self.route_into(g, &body_start);
                sub.wire_open_to(g, &cond_id);
                g.wire(&OpenEnd::next(&cond_id), &body_start); // true: loop back
                self.open = vec![OpenEnd::alt(&cond_id)]; // false: continue
            }
        }
        Ok(())
    }

    fn branch(&self, g: &mut Graph, join: &str, body: &[Node], what: &str) -> Compile<String> {
        let mut sub = self.sub(g, Some(join.to_string()), body, what)?;
        sub.wire_open_to(g, join);
        Ok(sub.start.expect("checked"))
    }

    fn choose(&mut self, g: &mut Graph, cases: &[(Option<WorkerStep>, Vec<Node>)]) -> Compile<()> {
        if cases.is_empty() {
            return Err("choose needs at least one case".into());
        }
        let guarded = cases.iter().take_while(|(g, _)| g.is_some()).count();
        if guarded < cases.len() - 1 {
            return Err("the otherwise case must be last".into());
        }
        if guarded == 0 {
            return Err("choose needs at least one guarded case".into());
        }
        let mut guards = Vec::with_capacity(guarded);
        for (guard, _) in &cases[..guarded] {
            guards.push(g.add_worker("PREDICATE", guard.as_ref().expect("guarded"))?);
        }
        self.route_into(g, &guards[0]);
        for pair in guards.windows(2) {
            g.wire(&OpenEnd::alt(&pair[0]), &pair[1]);
        }
        let mut open = Vec::new();
        for (i, (guard, body)) in cases[..guarded].iter().enumerate() {
            let name = &guard.as_ref().expect("guarded").name;
            let sub = self.sub(g, self.enclosing_join.clone(), body, &format!("case '{name}'"))?;
            g.wire(&OpenEnd::next(&guards[i]), sub.start.as_deref().expect("checked"));
            open.extend(sub.open);
        }
        let last = guards.last().expect("guarded > 0");
        if guarded < cases.len() {
            let sub = self.sub(g, self.enclosing_join.clone(), &cases[guarded].1, "the otherwise case")?;
            g.wire(&OpenEnd::alt(last), sub.start.as_deref().expect("checked"));
            open.extend(sub.open);
        } else {
            open.push(OpenEnd::alt(last)); // nothing matched: continue after the choice
        }
        self.open = open;
        Ok(())
    }
}

fn compile(root: RootConf, nodes: &[Node]) -> Compile<FlowSpec> {
    if root.name.trim().is_empty() {
        return Err("workflow name is required".into());
    }
    if root.version <= 0 {
        return Err(format!("workflow '{}' needs a positive version, got {}", root.name, root.version));
    }
    if nodes.is_empty() {
        return Err(format!("workflow '{}' defines no steps", root.name));
    }
    let mut g = Graph {
        workflow: root.name.clone(),
        default_queue: root.default_queue.unwrap_or_else(|| root.name.clone()),
        default_retry: root.default_retry,
        nodes: BTreeMap::new(),
        queues: BTreeSet::new(),
        names: HashSet::new(),
        start: None,
        counter: 0,
    };
    let mut stream = Stream::new(None);
    stream.append_all(&mut g, nodes)?;
    let end = g.add_end(None);
    stream.wire_open_to(&mut g, &end);
    g.start = stream.start.or(Some(end));

    let queues: Vec<String> = g.queues.into_iter().collect();
    let definition = json!({
        "name": root.name,
        "version": root.version,
        "startNode": g.start,
        "nodes": g.nodes.into_values().map(Value::Object).collect::<Vec<_>>(),
        "queues": queues,
        "executionMode": "SERVER",
    });
    Ok(FlowSpec { name: root.name, version: root.version, definition, queues, bindings: Vec::new() })
}
