//! The running step's identity, reachable from inside a handler -- the equivalent of the Java
//! client's static `Step` API (`Step.attempt()`, `Step.emit(..)`, `Step.base()`, ...).

use std::future::Future;
use std::sync::{Arc, Mutex};

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::branch::{Branch, BranchBuf};
use crate::client::{EmittedEvent, Task};
use crate::error::StepError;
use crate::pb;

tokio::task_local! {
    static CURRENT: Arc<Scope>;
}

pub(crate) struct Scope {
    attempt: i32,
    name: String,
    instance_id: String,
    workflow: String,
    correlation_id: Option<String>,
    base: Option<Value>,
    item_index: Option<i64>,
    item_map_key: Option<String>,
    emitted: Arc<Mutex<Vec<EmittedEvent>>>,
    /// Whether the step may call `Step::create`: a spawning step or its combine.
    may_create: bool,
    created: Arc<Mutex<Vec<Arc<Mutex<BranchBuf>>>>>,
}

impl Scope {
    pub(crate) fn of(task: &Task, may_create: bool) -> Arc<Scope> {
        Arc::new(Scope {
            attempt: task.attempt,
            name: task.step_name.clone(),
            instance_id: task.instance_id.clone(),
            workflow: task.workflow.clone(),
            correlation_id: task.correlation_id.clone(),
            base: task.base_context.clone(),
            item_index: task.base_context.as_ref().map(|_| task.item_index),
            item_map_key: task.item_map_key.clone(),
            emitted: Arc::new(Mutex::new(Vec::new())),
            may_create,
            created: Arc::new(Mutex::new(Vec::new())),
        })
    }

    pub(crate) fn drain(&self) -> Vec<EmittedEvent> {
        std::mem::take(&mut *self.emitted.lock().expect("emitted lock"))
    }

    /// The branches the step created, ready for its report -- or why one is malformed.
    pub(crate) fn drain_created(&self) -> Result<Vec<pb::CreatedBranch>, String> {
        let created = std::mem::take(&mut *self.created.lock().expect("created lock"));
        created.iter().map(|b| b.lock().expect("branch lock").build()).collect()
    }

    /// The running step's scope with `base` as [`Step::base`]: a combine sees the context from
    /// before the fan-out there. Events emitted under it still ride the running step's report.
    pub(crate) fn rebased(base: Value) -> Option<Arc<Scope>> {
        CURRENT
            .try_with(|s| {
                Arc::new(Scope {
                    attempt: s.attempt,
                    name: s.name.clone(),
                    instance_id: s.instance_id.clone(),
                    workflow: s.workflow.clone(),
                    correlation_id: s.correlation_id.clone(),
                    base: Some(base),
                    item_index: s.item_index,
                    item_map_key: s.item_map_key.clone(),
                    emitted: s.emitted.clone(),
                    may_create: s.may_create,
                    created: s.created.clone(),
                })
            })
            .ok()
    }

    pub(crate) async fn run<F: Future>(self: &Arc<Self>, fut: F) -> F::Output {
        CURRENT.scope(self.clone(), fut).await
    }
}

/// Accessors for the step whose handler is running. They panic when called outside a handler
/// (the scope is task-local: work spawned onto another task does not inherit it).
pub struct Step;

impl Step {
    fn with<R>(f: impl FnOnce(&Scope) -> R) -> R {
        CURRENT
            .try_with(|s| f(s))
            .expect("wiggle::Step is only available inside a step handler")
    }

    /// Whether a step handler is running on this task.
    pub fn in_scope() -> bool {
        CURRENT.try_with(|_| ()).is_ok()
    }

    /// The engine-global attempt number: 1 on the first try, incremented on every retry.
    pub fn attempt() -> i32 {
        Self::with(|s| s.attempt)
    }

    /// The running step's name.
    pub fn name() -> String {
        Self::with(|s| s.name.clone())
    }

    /// The workflow instance the running step belongs to.
    pub fn instance_id() -> String {
        Self::with(|s| s.instance_id.clone())
    }

    /// The running step's workflow.
    pub fn workflow() -> String {
        Self::with(|s| s.workflow.clone())
    }

    /// The instance's correlation id, when it was started with one.
    pub fn correlation_id() -> Option<String> {
        Self::with(|s| s.correlation_id.clone())
    }

    /// Inside a forEach body: the frozen pre-forEach context (read-only -- only the combine's
    /// return reaches the shared context). `None` outside a forEach.
    pub fn base() -> Option<Value> {
        Self::with(|s| s.base.clone())
    }

    /// [`base`](Self::base) decoded into `T`.
    pub fn base_as<T: DeserializeOwned>() -> Result<Option<T>, StepError> {
        Self::base()
            .map(|b| serde_json::from_value(b).map_err(|e| StepError::permanent(format!("cannot decode base: {e}"))))
            .transpose()
    }

    /// Inside a forEach body: the item's position in the input collection.
    pub fn item_index() -> Option<i64> {
        Self::with(|s| s.item_index)
    }

    /// Inside a forEach body over a map: the item's source key.
    pub fn item_map_key() -> Option<String> {
        Self::with(|s| s.item_map_key.clone())
    }

    /// Opens a branch over `input` -- its whole, isolated context -- to chain steps onto:
    ///
    /// ```ignore
    /// for item in order.items {
    ///     Step::create(&item)?.then_apply(reserve).then_apply(ship);
    /// }
    /// ```
    ///
    /// Like [`emit`](Self::emit), the branch is buffered and rides the step's report: it runs once
    /// the step completes, and an attempt that fails leaves nothing behind. Its result reaches the
    /// combine after the step. Only a spawning step (one followed directly by a
    /// [`spawn_combine`](crate::GraphBuilder::spawn_combine)), or that combine, may create
    /// branches. The steps chained on are named after their functions and bound on the worker that
    /// runs them with [`Worker::branch_steps`](crate::Worker::branch_steps).
    pub fn create<I: Serialize>(input: I) -> Result<Branch<I>, StepError> {
        Self::open(None, input)
    }

    /// [`create`](Self::create) with a key: the combine then receives the results as a map by key.
    /// Either every branch of a report carries a key, or none does.
    pub fn create_keyed<I: Serialize>(key: impl Into<String>, input: I) -> Result<Branch<I>, StepError> {
        Self::open(Some(key.into()), input)
    }

    fn open<I: Serialize>(key: Option<String>, input: I) -> Result<Branch<I>, StepError> {
        let (may_create, name, created) = Self::with(|s| (s.may_create, s.name.clone(), s.created.clone()));
        if !may_create {
            return Err(StepError::permanent(format!(
                "step '{name}' cannot create branches: only a step followed directly by a spawn combine, \
                 or that combine, may call Step::create"
            )));
        }
        let input = serde_json::to_value(input)
            .map_err(|e| StepError::permanent(format!("cannot encode a branch's input: {e}")))?;
        let (branch, buf) = Branch::open(input, key);
        created.lock().expect("created lock").push(buf);
        Ok(branch)
    }

    /// Buffers a domain event onto the instance's event log. It rides the step's report and is
    /// appended in the transaction that settles the step: committed if and only if this attempt
    /// completes, so an attempt that fails leaves nothing behind and its retry emits fresh.
    /// `event_type` names the fact (the `wf.` prefix is reserved for the engine); the payload must
    /// serialize to a JSON object.
    pub fn emit(event_type: impl Into<String>, payload: impl Serialize) -> Result<(), StepError> {
        let event_type = event_type.into();
        if event_type.trim().is_empty() {
            return Err(StepError::permanent("an event needs a type"));
        }
        let payload = serde_json::to_value(payload)
            .map_err(|e| StepError::permanent(format!("event '{event_type}': cannot encode payload: {e}")))?;
        if !payload.is_object() {
            return Err(StepError::permanent(format!("event '{event_type}': the payload must be a JSON object")));
        }
        Self::with(|s| {
            s.emitted.lock().expect("emitted lock").push(EmittedEvent { event_type, payload });
        });
        Ok(())
    }
}
