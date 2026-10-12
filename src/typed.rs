//! The typed flow API: a workflow written as a chain of the very functions that implement its
//! steps -- the Rust counterpart of the Java client's `Wiggle` / `WiggleFlow`.
//!
//! ```
//! use serde::{Deserialize, Serialize};
//! use wiggle::{StepError, Wiggle};
//!
//! #[derive(Serialize, Deserialize)] struct Order { id: String, qty: u32 }
//! #[derive(Serialize, Deserialize)] struct Payment { auth: String }
//! #[derive(Serialize, Deserialize)] struct Shipment { label: String }
//! #[derive(Serialize, Deserialize)] struct Receipt { auth: String, label: String }
//!
//! async fn validate(o: Order) -> Result<Order, StepError> { Ok(o) }
//! async fn in_stock(o: Order) -> Result<bool, StepError> { Ok(o.qty > 0) }
//! async fn charge(o: Order) -> Result<Payment, StepError> { Ok(Payment { auth: o.id }) }
//! async fn label(o: Order) -> Result<Shipment, StepError> { Ok(Shipment { label: o.id }) }
//! async fn settle(p: Payment, s: Shipment) -> Result<Receipt, StepError> {
//!     Ok(Receipt { auth: p.auth, label: s.label })
//! }
//! async fn notify(_: Receipt) -> Result<(), StepError> { Ok(()) }
//!
//! let order = Wiggle::define("order-fulfilment", 1, |f| {
//!     let validated = f.then_apply(validate).then_filter(in_stock);
//!
//!     let payment = validated.then_apply(charge);   // continuing `validated` twice
//!     let shipping = validated.then_apply(label);   // is the fan-out
//!
//!     Wiggle::all_of((payment, shipping))
//!         .combine(settle)
//!         .then_accept(notify)
//! })
//! .unwrap();
//!
//! assert_eq!(order.name(), "order-fulfilment");
//! // client.register(&order).await?;
//! // Worker::new(client, "w1").flow(&order).start().await?;
//! ```
//!
//! **What this is.** A typed front-end over [`GraphBuilder`]: the body runs once, at definition,
//! recording steps; what reaches the server is the same topology the builder produces. The chain
//! is checked by the compiler -- each step's input type is the previous step's output type, and a
//! fork's combine takes one parameter per arm, in arm order.
//!
//! **Step names.** A step is named after the function it references (`validate`, `in_stock`):
//! pass `fn` items, not closures. A closure -- or a step whose name must differ from its
//! function's, say to match an existing graph's `in-stock` -- takes its name from
//! [`named`](WiggleFlow::named).
//!
//! **Handlers travel with the flow.** Unlike Java, which finds handler methods by reflection,
//! the functions named here are kept in the [`Flow`]: [`Worker::flow`](crate::Worker::flow)
//! binds all of them at once, under the same names.
//!
//! **Handles are not futures.** Nothing runs while the flow is defined, and a handle continued
//! twice is a fan-out: pass both continuations to [`Wiggle::all_of`] and combine them, or
//! continue only one -- a split that never rejoins fails the definition.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::marker::PhantomData;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::client::Task;
use crate::error::{Error, Result, StepError};
use crate::flow::{Branch, Case, FlowSpec, GraphBuilder};
use crate::retry::RetryPolicy;
use crate::step::Scope;
use crate::worker::{
    compensation_handler, decode, effect_handler, encode, gate_handler, task_handler, Combine, Compensation, Handler,
    Kind, Outcome,
};

/// A workflow defined with [`Wiggle::define`]: its topology (it derefs to the [`FlowSpec`] to
/// register) and the handlers its steps were written with, for [`Worker::flow`](crate::Worker::flow).
#[derive(Clone)]
pub struct Flow {
    spec: FlowSpec,
    bindings: Vec<FlowBinding>,
}

#[derive(Clone)]
pub(crate) struct FlowBinding {
    pub(crate) step: String,
    pub(crate) kind: Kind,
    pub(crate) handler: Handler,
}

impl Flow {
    pub fn spec(&self) -> &FlowSpec {
        &self.spec
    }

    /// The step names this flow carries handlers for, with each one's kind.
    pub fn steps(&self) -> Vec<String> {
        self.bindings.iter().filter(|b| b.kind != Kind::Compensate).map(|b| b.step.clone()).collect()
    }

    pub(crate) fn bindings(&self) -> &[FlowBinding] {
        &self.bindings
    }
}

impl Deref for Flow {
    type Target = FlowSpec;

    fn deref(&self) -> &FlowSpec {
        &self.spec
    }
}

impl AsRef<str> for Flow {
    fn as_ref(&self) -> &str {
        self.spec.name()
    }
}

impl std::fmt::Debug for Flow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Flow").field("spec", &self.spec).field("steps", &self.steps()).finish()
    }
}

/// Entry point of the typed flow API.
pub struct Wiggle;

impl Wiggle {
    /// Defines the workflow `name`, published as `version`, whose first step takes a `T`. `body`
    /// runs once, here, and returns the handle the flow ends on.
    pub fn define<T, X>(name: &str, version: i32, body: impl FnOnce(WiggleFlow<T>) -> WiggleFlow<X>) -> Result<Flow> {
        Wiggle::flow(name, version).define(body)
    }

    /// [`define`](Self::define) with workflow-wide defaults.
    pub fn flow(name: &str, version: i32) -> FlowDefaults {
        FlowDefaults { name: name.to_string(), version, default_queue: None, default_retry: None }
    }

    /// Fans the flow out over handles that branched from a common point -- a tuple of 2 to 8 of
    /// them -- and returns the mandatory combine stage. Each arm runs on its own isolated copy of
    /// the context, so the combine is the only way an arm's result reaches the flow. An arm is
    /// named after its last step, which is how the engine keys its result.
    pub fn all_of<A: AllOf>(arms: A) -> A::Fork {
        arms.fork()
    }

    /// [`all_of`](Self::all_of) over any number of arms that end at the same type; the combine
    /// receives their results in arm order.
    pub fn all_of_n<T>(arms: Vec<WiggleFlow<T>>) -> ForkN<T> {
        let leaves: Vec<_> = arms.iter().map(|a| (a.plan.clone(), a.node)).collect();
        let (plan, fork) = fork_point(&leaves);
        ForkN { plan, fork, _t: PhantomData }
    }

    /// The exclusive twin of [`all_of`](Self::all_of): exactly one arm runs -- the first whose
    /// [`when`](WiggleFlow::when) guard holds, or the [`otherwise`](WiggleFlow::otherwise) arm.
    /// Every arm must open with one of the two, and they are evaluated in the order given. No
    /// combine: the arms are alternatives on the one context, so the flow continues from whichever
    /// ran, and they must end at the same type. With no otherwise, a choice where nothing matched
    /// continues after it.
    pub fn one_of<R>(arms: Vec<WiggleFlow<R>>) -> WiggleFlow<R> {
        let Some(first) = arms.first() else {
            panic!("Wiggle::one_of needs at least one arm");
        };
        let plan = first.plan.clone();
        if let Some(e) = same_definition(&plan, arms.iter().map(|a| &a.plan)) {
            return WiggleFlow::at(plan.clone(), plan.borrow_mut().failed(first.node, e));
        }
        let leaves: Vec<usize> = arms.iter().map(|a| a.node).collect();
        let node = plan.borrow_mut().choice(&leaves);
        WiggleFlow::at(plan, node)
    }
}

/// Workflow-wide defaults for [`Wiggle::flow`].
pub struct FlowDefaults {
    name: String,
    version: i32,
    default_queue: Option<String>,
    default_retry: Option<RetryPolicy>,
}

impl FlowDefaults {
    /// The queue for steps that name none (defaults to the workflow name).
    pub fn default_queue(mut self, queue: impl Into<String>) -> Self {
        self.default_queue = Some(queue.into());
        self
    }

    /// The retry policy for steps that name none (defaults to [`RetryPolicy::forever`]).
    pub fn default_retry(mut self, retry: RetryPolicy) -> Self {
        self.default_retry = Some(retry);
        self
    }

    /// See [`Wiggle::define`].
    pub fn define<T, X>(self, body: impl FnOnce(WiggleFlow<T>) -> WiggleFlow<X>) -> Result<Flow> {
        let plan = Rc::new(RefCell::new(Arena::default()));
        let root = plan.borrow_mut().add(None, Op::Root);
        let tail = body(WiggleFlow::at(plan.clone(), root));
        if !Rc::ptr_eq(&tail.plan, &plan) {
            return Err(Error::Flow(format!(
                "the body of workflow '{}' returned a handle from another definition",
                self.name
            )));
        }
        if let Some(e) = plan.borrow().error.clone() {
            return Err(Error::Flow(e));
        }
        let mut b = FlowSpec::define(self.name.clone(), self.version);
        if let Some(q) = self.default_queue {
            b = b.default_queue(q);
        }
        if let Some(r) = self.default_retry {
            b = b.default_retry(r);
        }
        let spec = walk(&plan, root, b, &format!("workflow '{}'", self.name)).build()?;
        let bindings = plan.borrow().bindings();
        Ok(Flow { spec, bindings })
    }
}

// ---- the plan: a tree of recorded steps, shared by every handle of one definition ----

struct StepDef {
    /// The step's name, or why it has none (a closure that was never `named`).
    name: std::result::Result<String, String>,
    queue: Option<String>,
    retry: Option<RetryPolicy>,
    kind: Kind,
    handler: Handler,
}

impl StepDef {
    fn new<F>(kind: Kind, handler: Handler) -> StepDef {
        StepDef { name: fn_name::<F>(), queue: None, retry: None, kind, handler }
    }
}

enum Op {
    Root,
    Task { step: StepDef, compensator: Option<Handler> },
    Gate { step: StepDef },
    Sleep { name: Option<String>, duration: Duration },
    Await { signal: String, timeout: Option<Duration>, escalation: Option<usize> },
    SubFlow { node: String, workflow: String },
    ForEach { name: String, items_key: String, body: usize, combine: StepDef },
    Loop { cond: StepDef, max_iterations: Option<u32>, body: usize },
    Fork { arms: Vec<Vec<usize>>, combine: Option<StepDef> },
    /// The head of a `one_of` arm; `None` is the otherwise.
    Guard { guard: Option<StepDef> },
    Choice { cases: Vec<Vec<usize>> },
    /// Stands in for a step whose recording failed; the definition reports the error.
    Failed,
}

struct PNode {
    parent: Option<usize>,
    children: Vec<usize>,
    claimed: bool,
    op: Op,
}

#[derive(Default)]
struct Arena {
    nodes: Vec<PNode>,
    error: Option<String>,
}

impl Arena {
    fn add(&mut self, parent: Option<usize>, op: Op) -> usize {
        let id = self.nodes.len();
        self.nodes.push(PNode { parent, children: Vec::new(), claimed: false, op });
        if let Some(p) = parent {
            self.nodes[p].children.push(id);
        }
        id
    }

    fn fail(&mut self, msg: String) {
        if self.error.is_none() {
            self.error = Some(msg);
        }
    }

    /// Records `msg` and returns a placeholder node under `parent`, so the chain can go on.
    fn failed(&mut self, parent: usize, msg: String) -> usize {
        self.fail(msg);
        self.add(Some(parent), Op::Failed)
    }

    fn label(&self, id: usize) -> Option<String> {
        match &self.nodes[id].op {
            Op::Task { step, .. } | Op::Gate { step } => step.name.clone().ok(),
            Op::Sleep { name, duration } => {
                Some(name.clone().unwrap_or_else(|| format!("sleep-{}ms", duration.as_millis())))
            }
            Op::Await { signal, .. } => Some(signal.clone()),
            Op::SubFlow { node, .. } => Some(node.clone()),
            Op::ForEach { combine, .. } => combine.name.clone().ok(),
            Op::Loop { cond, .. } => cond.name.clone().ok(),
            Op::Fork { combine, .. } => combine.as_ref().and_then(|c| c.name.clone().ok()),
            Op::Root | Op::Guard { .. } | Op::Choice { .. } | Op::Failed => None,
        }
    }

    fn describe(&self, id: usize) -> String {
        match self.label(id) {
            Some(l) => format!("'{l}'"),
            None if self.nodes[id].parent.is_none() => "the start of the flow".into(),
            None => "a fork".into(),
        }
    }

    fn step_def_mut(&mut self, id: usize) -> Option<&mut StepDef> {
        match &mut self.nodes[id].op {
            Op::Task { step, .. } | Op::Gate { step } => Some(step),
            Op::ForEach { combine, .. } => Some(combine),
            Op::Loop { cond, .. } => Some(cond),
            Op::Fork { combine, .. } => combine.as_mut(),
            Op::Guard { guard } => guard.as_mut(),
            _ => None,
        }
    }

    fn unclaimed(&self, id: usize) -> Vec<usize> {
        self.nodes[id].children.iter().copied().filter(|c| !self.nodes[*c].claimed).collect()
    }

    fn is_ancestor(&self, ancestor: usize, of: usize) -> bool {
        let mut s = Some(of);
        while let Some(n) = s {
            if n == ancestor {
                return true;
            }
            s = self.nodes[n].parent;
        }
        false
    }

    /// The deepest node every leaf descends from (strictly above each of them).
    fn junction(&self, leaves: &[usize]) -> std::result::Result<usize, String> {
        for (i, a) in leaves.iter().enumerate() {
            for (j, b) in leaves.iter().enumerate() {
                if i != j && self.is_ancestor(*a, *b) {
                    return Err(format!(
                        "the arms must be siblings, but the handle at {} is {} the one at {}",
                        self.describe(*a),
                        if a == b { "the same as" } else { "an ancestor of" },
                        self.describe(*b)
                    ));
                }
            }
        }
        let mut junction = self.nodes[leaves[0]].parent;
        for leaf in &leaves[1..] {
            while let Some(j) = junction {
                if self.is_ancestor(j, *leaf) {
                    break;
                }
                junction = self.nodes[j].parent;
            }
        }
        junction.ok_or_else(|| {
            "the arms were given handles from different bodies (a branch, a loop body) with no common \
             point to fan out from"
                .to_string()
        })
    }

    /// The nodes from just below `junction` down to `leaf`, claimed for one arm.
    fn claim_path(&mut self, junction: usize, leaf: usize) -> std::result::Result<Vec<usize>, String> {
        let mut path = Vec::new();
        let mut s = leaf;
        while s != junction {
            if self.nodes[s].claimed {
                return Err(format!("the branch ending at {} is already an arm of another fan-out", self.describe(s)));
            }
            path.push(s);
            s = self.nodes[s].parent.expect("the leaf descends from the junction");
        }
        path.reverse();
        for n in &path {
            self.nodes[*n].claimed = true;
        }
        Ok(path)
    }

    fn fork(&mut self, leaves: &[usize]) -> usize {
        if leaves.len() < 2 {
            return self.failed(leaves[0], "all_of needs at least two arms to fan out over".into());
        }
        let junction = match self.junction(leaves) {
            Ok(j) => j,
            Err(e) => return self.failed(leaves[0], format!("all_of: {e}")),
        };
        let mut arms = Vec::with_capacity(leaves.len());
        for leaf in leaves {
            match self.claim_path(junction, *leaf) {
                Ok(p) => arms.push(p),
                Err(e) => return self.failed(leaves[0], format!("all_of: {e}")),
            }
        }
        self.add(Some(junction), Op::Fork { arms, combine: None })
    }

    fn choice(&mut self, leaves: &[usize]) -> usize {
        let junction = if leaves.len() == 1 {
            // One arm: nothing diverged, so the junction is whatever its own guard hangs off.
            let mut s = Some(leaves[0]);
            loop {
                match s {
                    Some(n) if matches!(self.nodes[n].op, Op::Guard { .. }) => break self.nodes[n].parent,
                    Some(n) => s = self.nodes[n].parent,
                    None => break None,
                }
            }
        } else {
            self.junction(leaves).ok()
        };
        let Some(junction) = junction else {
            let e = "one_of: every arm must open with when(..) or otherwise(), from one common point";
            return self.failed(leaves[0], e.into());
        };
        let mut cases = Vec::with_capacity(leaves.len());
        for leaf in leaves {
            let path = match self.claim_path(junction, *leaf) {
                Ok(p) => p,
                Err(e) => return self.failed(leaves[0], format!("one_of: {e}")),
            };
            if !matches!(self.nodes[path[0]].op, Op::Guard { .. }) {
                let e = format!(
                    "every arm of one_of must open with when(..) or otherwise(): the arm ending at {} starts with {}",
                    self.describe(*leaf),
                    self.describe(path[0])
                );
                return self.failed(leaves[0], e);
            }
            if path.len() == 1 {
                return self.failed(leaves[0], "an arm of one_of has no steps after its guard".into());
            }
            cases.push(path);
        }
        self.add(Some(junction), Op::Choice { cases })
    }

    /// Every handler the definition was written with, under its step's name.
    fn bindings(&self) -> Vec<FlowBinding> {
        let mut out = Vec::new();
        for node in &self.nodes {
            let mut push = |def: &StepDef| {
                if let Ok(step) = &def.name {
                    out.push(FlowBinding { step: step.clone(), kind: def.kind, handler: def.handler.clone() });
                }
            };
            match &node.op {
                Op::Task { step, compensator } => {
                    push(step);
                    if let (Some(undo), Ok(name)) = (compensator, &step.name) {
                        out.push(FlowBinding { step: name.clone(), kind: Kind::Compensate, handler: undo.clone() });
                    }
                }
                Op::Gate { step } => push(step),
                Op::ForEach { combine, .. } => push(combine),
                Op::Loop { cond, .. } => push(cond),
                Op::Fork { combine: Some(c), .. } => push(c),
                Op::Guard { guard: Some(g) } => push(g),
                _ => {}
            }
        }
        out
    }
}

/// `None` when every plan is `plan`; else why not.
fn same_definition<'a>(
    plan: &Rc<RefCell<Arena>>,
    mut others: impl Iterator<Item = &'a Rc<RefCell<Arena>>>,
) -> Option<String> {
    others
        .any(|p| !Rc::ptr_eq(p, plan))
        .then(|| "the arms were given handles from different definitions".to_string())
}

fn fork_point(leaves: &[(Rc<RefCell<Arena>>, usize)]) -> (Rc<RefCell<Arena>>, usize) {
    let plan = leaves[0].0.clone();
    let fork = if let Some(e) = same_definition(&plan, leaves.iter().map(|(p, _)| p)) {
        plan.borrow_mut().failed(leaves[0].1, format!("all_of: {e}"))
    } else {
        let ids: Vec<usize> = leaves.iter().map(|(_, n)| *n).collect();
        plan.borrow_mut().fork(&ids)
    };
    (plan, fork)
}

/// The step name a function gives: the last segment of its path (`orders::in_stock` names
/// `in_stock`). A closure has none.
fn fn_name<F>() -> std::result::Result<String, String> {
    let full = std::any::type_name::<F>();
    if full.contains("{{closure}}") {
        return Err(format!(
            "a step written as a closure has no name; pass a fn, or give it one with .named(\"..\") ({full})"
        ));
    }
    let mut depth = 0i32;
    let mut cut = 0;
    let bytes = full.as_bytes();
    for i in 0..bytes.len() {
        match bytes[i] {
            b'<' => depth += 1,
            b'>' => depth -= 1,
            b':' if depth == 0 && i + 1 < bytes.len() && bytes[i + 1] == b':' => cut = i + 2,
            _ => {}
        }
    }
    let name = &full[cut..];
    let name = name.split('<').next().unwrap_or(name);
    if name.is_empty() {
        Err(format!("cannot take a step name from {full}; give it one with .named(\"..\")"))
    } else {
        Ok(name.to_string())
    }
}

// ---- handles ----

/// A point in a flow being defined, carrying the type of the context there. Continue it with the
/// next step; continue it twice to fan out (see [`Wiggle::all_of`]).
pub struct WiggleFlow<T> {
    plan: Rc<RefCell<Arena>>,
    node: usize,
    _t: PhantomData<fn() -> T>,
}

impl<T> Clone for WiggleFlow<T> {
    fn clone(&self) -> Self {
        WiggleFlow::at(self.plan.clone(), self.node)
    }
}

impl<T> WiggleFlow<T> {
    fn at(plan: Rc<RefCell<Arena>>, node: usize) -> Self {
        WiggleFlow { plan, node, _t: PhantomData }
    }

    fn record<R>(&self, op: Op) -> WiggleFlow<R> {
        let node = self.plan.borrow_mut().add(Some(self.node), op);
        WiggleFlow::at(self.plan.clone(), node)
    }

    fn tune(self, what: &str, f: impl FnOnce(&mut StepDef)) -> Self {
        {
            let mut plan = self.plan.borrow_mut();
            match plan.step_def_mut(self.node) {
                Some(def) => f(def),
                None => {
                    let e = format!("{what} must directly follow a step, gate, combine, loop or when(..)");
                    plan.fail(e);
                }
            }
        }
        self
    }

    /// Names the step just added -- needed for a closure, or to match an existing step name.
    pub fn named(self, name: impl Into<String>) -> Self {
        let name = name.into();
        self.tune("named()", |d| d.name = Ok(name))
    }

    /// The queue the step just added is dispatched on.
    pub fn queue(self, queue: impl Into<String>) -> Self {
        let queue = queue.into();
        self.tune("queue()", |d| d.queue = Some(queue))
    }

    /// The retry policy of the step just added.
    pub fn retry(self, retry: RetryPolicy) -> Self {
        self.tune("retry()", |d| d.retry = Some(retry))
    }

    /// Reinterprets the context type from here on -- after a signal merged a payload, say, or to
    /// make `one_of` arms agree.
    pub fn cast<R>(self) -> WiggleFlow<R> {
        WiggleFlow::at(self.plan, self.node)
    }

    /// A server-side timer; no worker is held.
    pub fn then_sleep(&self, duration: Duration) -> WiggleFlow<T> {
        self.record(Op::Sleep { name: None, duration })
    }

    /// [`then_sleep`](Self::then_sleep) with a node name.
    pub fn then_sleep_named(&self, name: impl Into<String>, duration: Duration) -> WiggleFlow<T> {
        self.record(Op::Sleep { name: Some(name.into()), duration })
    }

    /// Waits for the named signal; its payload merges into the context.
    pub fn then_await(&self, signal: impl Into<String>) -> WiggleFlow<T> {
        self.record(Op::Await { signal: signal.into(), timeout: None, escalation: None })
    }

    /// [`then_await`](Self::then_await) that fails the instance when `timeout` passes first.
    pub fn then_await_timeout(&self, signal: impl Into<String>, timeout: Duration) -> WiggleFlow<T> {
        self.record(Op::Await { signal: signal.into(), timeout: Some(timeout), escalation: None })
    }

    /// [`then_await`](Self::then_await) that runs `escalation` and rejoins when `timeout` passes first.
    pub fn then_await_or<X>(
        &self,
        signal: impl Into<String>,
        timeout: Duration,
        escalation: impl FnOnce(WiggleFlow<T>) -> WiggleFlow<X>,
    ) -> WiggleFlow<T> {
        let root = self.sub_body(escalation);
        self.record(Op::Await { signal: signal.into(), timeout: Some(timeout), escalation: Some(root) })
    }

    /// Runs the registered workflow `workflow` as a child (node `node`); its result merges back.
    pub fn then_sub_flow<R>(&self, node: impl Into<String>, workflow: impl Into<String>) -> WiggleFlow<R> {
        self.record(Op::SubFlow { node: node.into(), workflow: workflow.into() })
    }

    /// Runtime fan-out over the list or map in the context under `items_key`: `body` runs once per
    /// element, on the element itself (`E`); the frozen pre-forEach context is
    /// [`Step::base`](crate::Step::base). The returned stage requires a combine.
    pub fn then_for_each<E, X>(&self, items_key: &str, body: impl FnOnce(WiggleFlow<E>) -> WiggleFlow<X>) -> Items<X> {
        self.then_for_each_named(items_key, items_key, body)
    }

    /// [`then_for_each`](Self::then_for_each) with an explicit node name.
    pub fn then_for_each_named<E, X>(
        &self,
        name: &str,
        items_key: &str,
        body: impl FnOnce(WiggleFlow<E>) -> WiggleFlow<X>,
    ) -> Items<X> {
        let root = self.sub_body(body);
        Items {
            from: self.node,
            plan: self.plan.clone(),
            name: name.to_string(),
            items_key: items_key.to_string(),
            body: root,
            _t: PhantomData,
        }
    }

    /// Opens an arm of [`Wiggle::one_of`] taken when no `when` arm's guard held. Must be last.
    pub fn otherwise(&self) -> WiggleFlow<T> {
        self.record(Op::Guard { guard: None })
    }

    /// Records a nested body (a branch, loop body, escalation) on a fresh root of its own.
    fn sub_body<E, X>(&self, body: impl FnOnce(WiggleFlow<E>) -> WiggleFlow<X>) -> usize {
        let root = self.plan.borrow_mut().add(None, Op::Root);
        let tail = body(WiggleFlow::at(self.plan.clone(), root));
        if !Rc::ptr_eq(&tail.plan, &self.plan) {
            self.plan.borrow_mut().fail("a nested body returned a handle from another definition".into());
        }
        root
    }
}

impl<T: DeserializeOwned + Send + 'static> WiggleFlow<T> {
    /// A task: `step` receives the context as a `T` and returns the complete next context.
    pub fn then_apply<R, F, Fut>(&self, step: F) -> WiggleFlow<R>
    where
        R: Serialize + 'static,
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<R, StepError>> + Send + 'static,
    {
        let def = StepDef::new::<F>(Kind::Task, task_handler(step));
        self.record(Op::Task { step: def, compensator: None })
    }

    /// Alias of [`then_apply`](Self::then_apply).
    pub fn apply<R, F, Fut>(&self, step: F) -> WiggleFlow<R>
    where
        R: Serialize + 'static,
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<R, StepError>> + Send + 'static,
    {
        self.then_apply(step)
    }

    /// A side effect: `effect` runs on the context and leaves it unchanged.
    pub fn then_accept<F, Fut>(&self, effect: F) -> WiggleFlow<T>
    where
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), StepError>> + Send + 'static,
    {
        let def = StepDef::new::<F>(Kind::Task, effect_handler(effect));
        self.record(Op::Task { step: def, compensator: None })
    }

    /// A gate: the flow continues while `gate` holds; false ends the instance as `gated:<name>`
    /// (inside an `all_of` arm, it short-circuits to the join).
    pub fn then_filter<F, Fut>(&self, gate: F) -> WiggleFlow<T>
    where
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<bool, StepError>> + Send + 'static,
    {
        let def = StepDef::new::<F>(Kind::Gate, gate_handler(gate));
        self.record(Op::Gate { step: def })
    }

    /// Opens an arm of [`Wiggle::one_of`], taken when `guard` holds (and no earlier arm's did).
    pub fn when<F, Fut>(&self, guard: F) -> WiggleFlow<T>
    where
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<bool, StepError>> + Send + 'static,
    {
        let def = StepDef::new::<F>(Kind::Gate, gate_handler(guard));
        self.record(Op::Guard { guard: Some(def) })
    }

    /// Runs `body`, then again while `condition` holds (at least once). Budgeted by the engine
    /// default (10,000 iterations).
    pub fn repeat_while<F, Fut>(&self, condition: F, body: impl FnOnce(WiggleFlow<T>) -> WiggleFlow<T>) -> WiggleFlow<T>
    where
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<bool, StepError>> + Send + 'static,
    {
        self.repeat(condition, None, body)
    }

    /// [`repeat_while`](Self::repeat_while) with an explicit iteration budget.
    pub fn repeat_while_max<F, Fut>(
        &self,
        condition: F,
        max_iterations: u32,
        body: impl FnOnce(WiggleFlow<T>) -> WiggleFlow<T>,
    ) -> WiggleFlow<T>
    where
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<bool, StepError>> + Send + 'static,
    {
        self.repeat(condition, Some(max_iterations), body)
    }

    fn repeat<F, Fut>(
        &self,
        condition: F,
        max_iterations: Option<u32>,
        body: impl FnOnce(WiggleFlow<T>) -> WiggleFlow<T>,
    ) -> WiggleFlow<T>
    where
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<bool, StepError>> + Send + 'static,
    {
        let cond = StepDef::new::<F>(Kind::Gate, gate_handler(condition));
        let root = self.sub_body(body);
        self.record(Op::Loop { cond, max_iterations, body: root })
    }

    /// Declares the task just added compensable, with `undo` as its compensator: if the instance
    /// later fails, `undo` runs in the reverse pass with the step's input/result snapshots.
    pub fn compensate_with<F, Fut>(self, undo: F) -> Self
    where
        F: Fn(Compensation) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), StepError>> + Send + 'static,
    {
        {
            let mut plan = self.plan.borrow_mut();
            match &mut plan.nodes[self.node].op {
                Op::Task { compensator, .. } => *compensator = Some(compensation_handler(undo)),
                _ => plan.fail("compensate_with() must directly follow then_apply() or then_accept()".into()),
            }
        }
        self
    }
}

/// The stage after [`WiggleFlow::then_for_each`]: item results rejoin at a combine.
#[must_use = "a for_each needs combine(..) to collect its items"]
pub struct Items<X> {
    plan: Rc<RefCell<Arena>>,
    from: usize,
    name: String,
    items_key: String,
    body: usize,
    _t: PhantomData<fn() -> X>,
}

impl<X: DeserializeOwned + Send + 'static> Items<X> {
    /// The combine receives every item's final value in item order (for a list input); the
    /// pre-forEach context is [`Step::base`](crate::Step::base). Its return is the complete
    /// post-join context.
    pub fn combine<R, F, Fut>(self, combine: F) -> WiggleFlow<R>
    where
        R: Serialize + 'static,
        F: Fn(Vec<X>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<R, StepError>> + Send + 'static,
    {
        let handler = collect_handler(combine);
        self.record(StepDef::new::<F>(Kind::Combine, handler))
    }

    /// [`combine`](Self::combine) for a map input: the items' final values keyed like the input.
    pub fn combine_map<R, F, Fut>(self, combine: F) -> WiggleFlow<R>
    where
        R: Serialize + 'static,
        F: Fn(BTreeMap<String, X>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<R, StepError>> + Send + 'static,
    {
        let handler = collect_handler(combine);
        self.record(StepDef::new::<F>(Kind::Combine, handler))
    }

    fn record<R>(self, combine: StepDef) -> WiggleFlow<R> {
        let op = Op::ForEach { name: self.name, items_key: self.items_key, body: self.body, combine };
        let node = self.plan.borrow_mut().add(Some(self.from), op);
        WiggleFlow::at(self.plan, node)
    }
}

fn collect_handler<C, R, F, Fut>(f: F) -> Handler
where
    C: DeserializeOwned + Send + 'static,
    R: Serialize + 'static,
    F: Fn(C) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = std::result::Result<R, StepError>> + Send + 'static,
{
    let f = Arc::new(f);
    Arc::new(move |task: Task| {
        let f = f.clone();
        Box::pin(async move {
            let c = Combine::of(&task);
            let items: C = c.items_as()?;
            let out = on_base(c.base().clone(), f(items)).await?;
            Ok(Outcome::Merge(encode(out)?))
        })
    })
}

/// Runs a combine with the pre-fan-out context as [`Step::base`](crate::Step::base).
async fn on_base<Fut: Future>(base: Value, fut: Fut) -> Fut::Output {
    match Scope::rebased(base) {
        Some(scope) => scope.run(fut).await,
        None => fut.await,
    }
}

/// A tuple of handles [`Wiggle::all_of`] can fan out over.
pub trait AllOf {
    type Fork;
    fn fork(self) -> Self::Fork;
}

macro_rules! fork_arity {
    ($Fork:ident; $($A:ident $a:ident $i:tt),+) => {
        /// The mandatory combine stage of [`Wiggle::all_of`].
        #[must_use = "all_of needs combine(..) to rejoin its arms"]
        pub struct $Fork<$($A),+> {
            plan: Rc<RefCell<Arena>>,
            fork: usize,
            _t: PhantomData<fn() -> ($($A,)+)>,
        }

        impl<$($A),+> AllOf for ($(WiggleFlow<$A>,)+) {
            type Fork = $Fork<$($A),+>;

            fn fork(self) -> Self::Fork {
                let (plan, fork) = fork_point(&[$((self.$i.plan.clone(), self.$i.node)),+]);
                $Fork { plan, fork, _t: PhantomData }
            }
        }

        impl<$($A: DeserializeOwned + Send + 'static),+> $Fork<$($A),+> {
            /// Declares the combine: one parameter per arm, in arm order, each receiving that
            /// arm's result; the pre-fork context is [`Step::base`](crate::Step::base). Its return
            /// is the complete post-join context.
            pub fn combine<R, F, Fut>(self, combine: F) -> WiggleFlow<R>
            where
                R: Serialize + 'static,
                F: Fn($($A),+) -> Fut + Send + Sync + 'static,
                Fut: Future<Output = std::result::Result<R, StepError>> + Send + 'static,
            {
                let f = Arc::new(combine);
                let handler: Handler = Arc::new(move |task: Task| {
                    let f = f.clone();
                    Box::pin(async move {
                        let c = Combine::of(&task);
                        $( let $a: $A = arm(&c, $i)?; )+
                        let out = on_base(c.base().clone(), f($($a),+)).await?;
                        Ok(Outcome::Merge(encode(out)?))
                    })
                });
                set_combine(&self.plan, self.fork, StepDef::new::<F>(Kind::Combine, handler));
                WiggleFlow::at(self.plan, self.fork)
            }
        }
    };
}

fork_arity!(Fork2; A a 0, B b 1);
fork_arity!(Fork3; A a 0, B b 1, C c 2);
fork_arity!(Fork4; A a 0, B b 1, C c 2, D d 3);
fork_arity!(Fork5; A a 0, B b 1, C c 2, D d 3, E e 4);
fork_arity!(Fork6; A a 0, B b 1, C c 2, D d 3, E e 4, G g 5);
fork_arity!(Fork7; A a 0, B b 1, C c 2, D d 3, E e 4, G g 5, H h 6);
fork_arity!(Fork8; A a 0, B b 1, C c 2, D d 3, E e 4, G g 5, H h 6, I i 7);

/// The combine stage of [`Wiggle::all_of_n`].
#[must_use = "all_of_n needs combine(..) to rejoin its arms"]
pub struct ForkN<T> {
    plan: Rc<RefCell<Arena>>,
    fork: usize,
    _t: PhantomData<fn() -> T>,
}

impl<T: DeserializeOwned + Send + 'static> ForkN<T> {
    /// Declares the combine: it receives every arm's result, in arm order.
    pub fn combine<R, F, Fut>(self, combine: F) -> WiggleFlow<R>
    where
        R: Serialize + 'static,
        F: Fn(Vec<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<R, StepError>> + Send + 'static,
    {
        let f = Arc::new(combine);
        let handler: Handler = Arc::new(move |task: Task| {
            let f = f.clone();
            Box::pin(async move {
                let c = Combine::of(&task);
                let arms = (0..c.arms().len()).map(|i| arm(&c, i)).collect::<std::result::Result<Vec<T>, _>>()?;
                let out = on_base(c.base().clone(), f(arms)).await?;
                Ok(Outcome::Merge(encode(out)?))
            })
        });
        set_combine(&self.plan, self.fork, StepDef::new::<F>(Kind::Combine, handler));
        WiggleFlow::at(self.plan, self.fork)
    }
}

fn arm<A: DeserializeOwned>(c: &Combine, i: usize) -> std::result::Result<A, StepError> {
    let (name, value) = c
        .arms()
        .get(i)
        .ok_or_else(|| StepError::permanent(format!("the combine expected an arm #{}, but the fork has {}", i + 1, c.arms().len())))?;
    decode(value.clone(), &format!("arm '{name}'"))
}

fn set_combine(plan: &Rc<RefCell<Arena>>, fork: usize, def: StepDef) {
    let mut p = plan.borrow_mut();
    match &mut p.nodes[fork].op {
        Op::Fork { combine, .. } if combine.is_none() => *combine = Some(def),
        Op::Fork { .. } => p.fail("this fan-out already has a combine; a fan-out joins once".into()),
        _ => {} // a failed fork: its error is already recorded
    }
}

// ---- compile: replay the plan onto a GraphBuilder, as the Java Plan does ----

fn walk(plan: &Rc<RefCell<Arena>>, from: usize, mut b: GraphBuilder, what: &str) -> GraphBuilder {
    let mut step = from;
    loop {
        let next = plan.borrow().unclaimed(step);
        match next.len() {
            0 => return b,
            1 => {
                b = apply(plan, next[0], b);
                step = next[0];
            }
            _ => return b.fail(split_error(&plan.borrow(), step, what)),
        }
    }
}

fn replay(plan: &Rc<RefCell<Arena>>, path: &[usize], mut b: GraphBuilder, what: &str) -> GraphBuilder {
    for (i, node) in path.iter().enumerate() {
        b = apply(plan, *node, b);
        let stray = plan.borrow().unclaimed(*node);
        let last = i + 1 == path.len();
        if (!last && !stray.is_empty()) || stray.len() > 1 {
            return b.fail(split_error(&plan.borrow(), *node, what));
        }
    }
    // A single continuation after the arm's end is a step added to the arm afterwards.
    walk(plan, *path.last().expect("an arm has steps"), b, what)
}

fn split_error(plan: &Arena, step: usize, what: &str) -> String {
    let heads: Vec<String> = plan.unclaimed(step).iter().map(|c| plan.describe(*c)).collect();
    format!(
        "in {what}, the flow after {} splits into [{}] but is never rejoined. Continuing one handle twice \
         is a fan-out: pass both to Wiggle::all_of(..) and combine them, or continue only one of them",
        plan.describe(step),
        heads.join(", ")
    )
}

/// What `apply` needs from a node, copied out so no borrow is held while the builder runs nested
/// bodies (which read the plan again).
enum Replay {
    Task { name: std::result::Result<String, String>, queue: Option<String>, retry: Option<RetryPolicy>, compensable: bool },
    Gate { name: std::result::Result<String, String>, queue: Option<String>, retry: Option<RetryPolicy> },
    Sleep { name: Option<String>, duration: Duration },
    Await { signal: String, timeout: Option<Duration>, escalation: Option<usize> },
    SubFlow { node: String, workflow: String },
    ForEach { name: String, items_key: String, body: usize, combine: Tuning },
    Loop { cond: Tuning, max_iterations: Option<u32>, body: usize },
    Fork { arms: Vec<Vec<usize>>, combine: Option<Tuning> },
    Choice { cases: Vec<Vec<usize>> },
    Stray(&'static str),
    Nothing,
}

struct Tuning {
    name: std::result::Result<String, String>,
    queue: Option<String>,
    retry: Option<RetryPolicy>,
}

impl From<&StepDef> for Tuning {
    fn from(d: &StepDef) -> Self {
        Tuning { name: d.name.clone(), queue: d.queue.clone(), retry: d.retry }
    }
}

fn tuned(mut b: GraphBuilder, queue: Option<String>, retry: Option<RetryPolicy>) -> GraphBuilder {
    if let Some(q) = queue {
        b = b.queue(q);
    }
    if let Some(r) = retry {
        b = b.retry(r);
    }
    b
}

fn named(b: GraphBuilder, name: std::result::Result<String, String>) -> (GraphBuilder, String) {
    match name {
        Ok(n) => (b, n),
        Err(e) => (b.fail(e), "unnamed".into()),
    }
}

fn apply(plan: &Rc<RefCell<Arena>>, id: usize, b: GraphBuilder) -> GraphBuilder {
    let r = {
        let p = plan.borrow();
        match &p.nodes[id].op {
            Op::Task { step, compensator } => Replay::Task {
                name: step.name.clone(),
                queue: step.queue.clone(),
                retry: step.retry,
                compensable: compensator.is_some(),
            },
            Op::Gate { step } => Replay::Gate { name: step.name.clone(), queue: step.queue.clone(), retry: step.retry },
            Op::Sleep { name, duration } => Replay::Sleep { name: name.clone(), duration: *duration },
            Op::Await { signal, timeout, escalation } => {
                Replay::Await { signal: signal.clone(), timeout: *timeout, escalation: *escalation }
            }
            Op::SubFlow { node, workflow } => Replay::SubFlow { node: node.clone(), workflow: workflow.clone() },
            Op::ForEach { name, items_key, body, combine } => Replay::ForEach {
                name: name.clone(),
                items_key: items_key.clone(),
                body: *body,
                combine: combine.into(),
            },
            Op::Loop { cond, max_iterations, body } => {
                Replay::Loop { cond: cond.into(), max_iterations: *max_iterations, body: *body }
            }
            Op::Fork { arms, combine } => Replay::Fork { arms: arms.clone(), combine: combine.as_ref().map(Tuning::from) },
            Op::Choice { cases } => Replay::Choice { cases: cases.clone() },
            Op::Guard { .. } => Replay::Stray("when()/otherwise() opens an arm of Wiggle::one_of(..), not a step of its own"),
            Op::Root | Op::Failed => Replay::Nothing,
        }
    };
    match r {
        Replay::Task { name, queue, retry, compensable } => {
            let (b, name) = named(b, name);
            let b = tuned(b.step(name), queue, retry);
            if compensable {
                b.compensate()
            } else {
                b
            }
        }
        Replay::Gate { name, queue, retry } => {
            let (b, name) = named(b, name);
            tuned(b.gate(name), queue, retry)
        }
        Replay::Sleep { name: Some(n), duration } => b.sleep_named(n, duration),
        Replay::Sleep { name: None, duration } => b.sleep(duration),
        Replay::Await { signal, timeout: None, .. } => b.await_signal(signal),
        Replay::Await { signal, timeout: Some(t), escalation: None } => b.await_signal_timeout(signal, t),
        Replay::Await { signal, timeout: Some(t), escalation: Some(root) } => {
            let plan = plan.clone();
            let what = format!("the escalation branch of '{signal}'");
            b.await_signal_or(signal, t, move |sub| walk(&plan, root, sub, &what))
        }
        Replay::SubFlow { node, workflow } => b.sub_workflow(node, workflow),
        Replay::ForEach { name, items_key, body, combine } => {
            let (b, cname) = named(b, combine.name);
            let plan = plan.clone();
            let what = format!("the forEach body for '{name}'");
            let b = b.for_each_named(name, items_key, move |sub| walk(&plan, body, sub, &what)).combine(cname);
            tuned(b, combine.queue, combine.retry)
        }
        Replay::Loop { cond, max_iterations, body } => {
            let (b, name) = named(b, cond.name);
            let plan = plan.clone();
            let what = format!("the body of loop '{name}'");
            let body = move |sub| walk(&plan, body, sub, &what);
            let b = match max_iterations {
                Some(n) => b.do_while_max(name, n, body),
                None => b.do_while(name, body),
            };
            tuned(b, cond.queue, cond.retry)
        }
        Replay::Fork { combine: None, .. } => b.fail("an all_of fan-out has no combine; follow it with combine(..)"),
        Replay::Fork { arms, combine: Some(combine) } => {
            let names: Vec<Option<String>> = {
                let p = plan.borrow();
                arms.iter().map(|path| path.iter().rev().find_map(|n| p.label(*n))).collect()
            };
            if names.iter().any(Option::is_none) {
                return b.fail("an all_of arm has no step to take its name from; the engine keys an arm's result by it");
            }
            let names: Vec<String> = names.into_iter().flatten().collect();
            if names.iter().collect::<HashSet<_>>().len() != names.len() {
                return b.fail(format!(
                    "all_of arms must be distinguishable, but two end at steps with the same name {names:?}; \
                     give unnamed sleeps distinct names with then_sleep_named"
                ));
            }
            let branches = arms
                .into_iter()
                .zip(names)
                .map(|(path, name)| {
                    let plan = plan.clone();
                    let what = format!("arm '{name}'");
                    Branch::of(name, move |sub| replay(&plan, &path, sub, &what))
                })
                .collect();
            let (b, cname) = named(b, combine.name);
            tuned(b.fork(branches).combine(cname), combine.queue, combine.retry)
        }
        Replay::Choice { cases } => {
            let mut out = Vec::with_capacity(cases.len());
            let mut b = b;
            for path in cases {
                let guard = match &plan.borrow().nodes[path[0]].op {
                    Op::Guard { guard } => guard.as_ref().map(Tuning::from),
                    _ => unreachable!("one_of checked every arm opens with a guard"),
                };
                let body = path[1..].to_vec();
                let plan = plan.clone();
                out.push(match guard {
                    None => Case::otherwise(move |sub| replay(&plan, &body, sub, "the otherwise arm")),
                    Some(g) => {
                        let name = match g.name {
                            Ok(n) => n,
                            Err(e) => {
                                b = b.fail(e);
                                "unnamed".into()
                            }
                        };
                        let what = format!("the '{name}' arm");
                        let mut case = Case::when(name, move |sub| replay(&plan, &body, sub, &what));
                        if let Some(q) = g.queue {
                            case = case.queue(q);
                        }
                        if let Some(r) = g.retry {
                            case = case.retry(r);
                        }
                        case
                    }
                });
            }
            b.choose(out)
        }
        Replay::Stray(msg) => b.fail(msg),
        Replay::Nothing => b,
    }
}

#[cfg(test)]
mod tests {
    use super::fn_name;

    async fn in_stock(_: u32) -> Result<bool, crate::StepError> {
        Ok(true)
    }

    struct Steps;
    impl Steps {
        fn validate() {}
    }

    #[test]
    fn names_come_from_the_function_path() {
        fn of<F>(_: F) -> Result<String, String> {
            fn_name::<F>()
        }
        assert_eq!(of(in_stock).unwrap(), "in_stock");
        assert_eq!(of(Steps::validate).unwrap(), "validate");
        assert_eq!(of(Vec::<u8>::new).unwrap(), "new");
        assert!(of(|x: u32| x).unwrap_err().contains("closure"));
    }
}
