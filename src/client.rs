//! The control-plane connection: register workflows, start and track instances, deliver signals,
//! manage schedules, search, the event feed -- plus the low-level worker RPCs the [`Worker`] uses.
//!
//! [`Worker`]: crate::Worker

use std::future::Future;
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

use crate::error::{Error, Result};
use crate::flow::FlowSpec;
use crate::pb;
use crate::pb::wiggle_control_plane_client::WiggleControlPlaneClient;
use crate::value::{from_struct, from_value, opt_value, to_struct, to_value};

type Stub = WiggleControlPlaneClient<Channel>;

/// Statuses that end a run. COMPENSATING is not terminal: the reverse pass is still running.
const TERMINAL: [&str; 5] = ["COMPLETED", "FAILED", "CANCELLED", "COMPENSATED", "COMPENSATION_FAILED"];

/// A connection to a Wiggle server. Cheap to clone; clones share the underlying channel.
///
/// Every call is retried on `UNAVAILABLE` -- the server is down, restarting, or failing over, so
/// the call almost certainly never ran -- with exponential backoff: `WIGGLE_RPC_MAX_ATTEMPTS`
/// (default 5; 1 disables retry) and `WIGGLE_RPC_RETRY_DELAY_MILLIS` (default 200). Other errors
/// are returned as they are.
#[derive(Clone)]
pub struct Client {
    stub: Stub,
    api_key: Option<MetadataValue<tonic::metadata::Ascii>>,
    max_attempts: u32,
    retry_delay: Duration,
}

impl Client {
    /// Connects to `target` (`host:port`, or a URL) over plaintext, presenting the API key in
    /// `WIGGLE_API_KEY` when set. The channel connects lazily, so this succeeds before the server is
    /// up; calls ride out its cold start through the `UNAVAILABLE` retry.
    pub async fn connect(target: &str) -> Result<Client> {
        let url = if target.contains("://") { target.to_string() } else { format!("http://{target}") };
        let endpoint = Endpoint::from_shared(url)?;
        Client::connect_with(endpoint, std::env::var("WIGGLE_API_KEY").ok())
    }

    /// Connects through a configured `endpoint` -- for TLS, timeouts or keep-alives -- presenting
    /// `api_key` on every call as `authorization: Bearer <key>` (`None` presents none). A key
    /// travels in the clear on a plaintext channel, so use one with TLS.
    pub fn connect_with(endpoint: Endpoint, api_key: Option<String>) -> Result<Client> {
        let api_key = match api_key.map(|k| k.trim().to_string()).filter(|k| !k.is_empty()) {
            Some(key) => Some(
                format!("Bearer {key}")
                    .parse()
                    .map_err(|_| Error::Invalid("the API key is not a valid header value".into()))?,
            ),
            None => None,
        };
        Ok(Client {
            stub: WiggleControlPlaneClient::new(endpoint.connect_lazy()),
            api_key,
            max_attempts: env_u64("WIGGLE_RPC_MAX_ATTEMPTS", 5).max(1) as u32,
            retry_delay: Duration::from_millis(env_u64("WIGGLE_RPC_RETRY_DELAY_MILLIS", 200)),
        })
    }

    fn request<Q>(&self, msg: Q) -> tonic::Request<Q> {
        let mut req = tonic::Request::new(msg);
        if let Some(key) = &self.api_key {
            req.metadata_mut().insert("authorization", key.clone());
        }
        req
    }

    async fn call<Q, R, F, Fut>(&self, method: &'static str, msg: Q, f: F) -> Result<R>
    where
        Q: Clone,
        F: Fn(Stub, tonic::Request<Q>) -> Fut,
        Fut: Future<Output = std::result::Result<tonic::Response<R>, Status>>,
    {
        let mut delay = self.retry_delay;
        let mut attempt = 1;
        loop {
            match f(self.stub.clone(), self.request(msg.clone())).await {
                Ok(res) => return Ok(res.into_inner()),
                Err(status) if status.code() == Code::Unavailable && attempt < self.max_attempts => {
                    log::debug!("wiggle: rpc {method} unavailable (attempt {attempt}), retrying in {delay:?}");
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                    attempt += 1;
                }
                Err(status) => {
                    log_failure(method, &status);
                    return Err(Error::Rpc { method, status });
                }
            }
        }
    }

    // ---- workflows ----

    /// Registers a workflow definition (idempotent for the same graph).
    pub async fn register(&self, flow: &FlowSpec) -> Result<RegisterResult> {
        self.register_def(flow, false).await
    }

    /// Registers `flow`, replacing an already-registered (name, version) whose graph differs. The
    /// server refuses unless it is configured to allow graph replacement.
    pub async fn register_force(&self, flow: &FlowSpec) -> Result<RegisterResult> {
        self.register_def(flow, true).await
    }

    async fn register_def(&self, flow: &FlowSpec, force: bool) -> Result<RegisterResult> {
        let definition = match flow.definition() {
            Value::Object(m) => to_struct(m),
            _ => return Err(Error::Invalid("a definition is a JSON object".into())),
        };
        let req = pb::WorkflowDefinition { definition: Some(definition), force };
        let res = self.call("RegisterWorkflow", req, |mut s, r| async move { s.register_workflow(r).await }).await?;
        Ok(RegisterResult { name: res.name, version: res.version, nodes: res.nodes })
    }

    /// The latest registered graph of `name`, as JSON -- the server's source of truth for its step
    /// names, kinds and queues.
    pub async fn get_workflow(&self, name: &str) -> Result<Value> {
        self.fetch_workflow(name, None).await
    }

    /// One specific registered version of a workflow's graph.
    pub async fn get_workflow_version(&self, name: &str, version: i32) -> Result<Value> {
        self.fetch_workflow(name, Some(version)).await
    }

    async fn fetch_workflow(&self, name: &str, version: Option<i32>) -> Result<Value> {
        let req = pb::GetWorkflowRequest { name: name.to_string(), version };
        let res = self.call("GetWorkflow", req, |mut s, r| async move { s.get_workflow(r).await }).await?;
        Ok(res.definition.as_ref().map(from_struct).unwrap_or(Value::Null))
    }

    /// The names of every registered workflow.
    pub async fn workflow_names(&self) -> Result<Vec<String>> {
        let res = self.call("ListWorkflows", pb::Empty {}, |mut s, r| async move { s.list_workflows(r).await }).await?;
        Ok(res.workflows)
    }

    // ---- instances ----

    /// Starts an instance of `workflow` (a name, or a [`FlowSpec`]) on its latest version and
    /// returns its id.
    pub async fn start<T: Serialize + ?Sized>(&self, workflow: impl AsRef<str>, context: &T) -> Result<String> {
        self.start_with(workflow, context, StartOptions::default()).await
    }

    /// [`start`](Self::start) with a pinned version and/or a correlation id.
    pub async fn start_with<T: Serialize + ?Sized>(
        &self,
        workflow: impl AsRef<str>,
        context: &T,
        options: StartOptions,
    ) -> Result<String> {
        let req = pb::StartInstanceRequest {
            workflow: workflow.as_ref().to_string(),
            context: Some(to_value(&serde_json::to_value(context)?)),
            version: options.version,
            correlation_id: options.correlation_id,
        };
        let res = self.call("StartInstance", req, |mut s, r| async move { s.start_instance(r).await }).await?;
        Ok(res.instance_id)
    }

    /// The current state of an instance.
    pub async fn instance(&self, id: &str) -> Result<InstanceView> {
        Ok(self.instance_detail(id).await?.instance)
    }

    /// An instance together with its tokens (one per step reached).
    pub async fn instance_detail(&self, id: &str) -> Result<InstanceWithTokens> {
        let req = pb::InstanceIdRequest { instance_id: id.to_string() };
        let res = self.call("GetInstance", req, |mut s, r| async move { s.get_instance(r).await }).await?;
        let instance = res
            .instance
            .map(InstanceView::from)
            .ok_or_else(|| Error::Invalid(format!("instance {id}: the server sent no instance")))?;
        Ok(InstanceWithTokens { instance, tokens: res.tokens.into_iter().map(TokenInfo::from).collect() })
    }

    /// Instances, optionally filtered by workflow and/or status.
    pub async fn list_instances(&self, workflow: Option<&str>, status: Option<&str>, limit: i32) -> Result<Vec<InstanceView>> {
        self.list(pb::ListInstancesRequest {
            workflow: workflow.map(str::to_string),
            status: status.map(str::to_string),
            limit,
            correlation_id: None,
        })
        .await
    }

    /// Instances started with `correlation_id` (a business key), newest first.
    pub async fn find_by_correlation(&self, correlation_id: &str, limit: i32) -> Result<Vec<InstanceView>> {
        self.list(pb::ListInstancesRequest {
            workflow: None,
            status: None,
            limit,
            correlation_id: Some(correlation_id.to_string()),
        })
        .await
    }

    async fn list(&self, req: pb::ListInstancesRequest) -> Result<Vec<InstanceView>> {
        let res = self.call("ListInstances", req, |mut s, r| async move { s.list_instances(r).await }).await?;
        Ok(res.instances.into_iter().map(InstanceView::from).collect())
    }

    /// Polls until the instance reaches a terminal state, or fails with [`Error::Timeout`].
    pub async fn await_completion(&self, id: &str, timeout: Duration) -> Result<InstanceView> {
        let deadline = Instant::now() + timeout;
        loop {
            let view = self.instance(id).await?;
            if view.is_terminal() {
                return Ok(view);
            }
            if Instant::now() >= deadline {
                return Err(Error::Timeout { id: id.to_string(), status: view.status, timeout });
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Cancels a running instance; returns what the server cancelled.
    pub async fn cancel(&self, id: &str, reason: &str) -> Result<String> {
        let req = pb::CancelInstanceRequest { instance_id: id.to_string(), reason: reason.to_string() };
        let res = self.call("CancelInstance", req, |mut s, r| async move { s.cancel_instance(r).await }).await?;
        Ok(res.cancelled)
    }

    /// Delivers the named signal to an instance; the payload merges into its context.
    pub async fn signal<T: Serialize + ?Sized>(&self, id: &str, signal: &str, payload: &T) -> Result<()> {
        let req = pb::SignalRequest {
            instance_id: id.to_string(),
            signal: signal.to_string(),
            payload: Some(to_value(&serde_json::to_value(payload)?)),
        };
        self.call("SignalInstance", req, |mut s, r| async move { s.signal_instance(r).await }).await?;
        Ok(())
    }

    /// Full-text (or semantic) search over instances. Fails with FAILED_PRECONDITION when search
    /// is not enabled on the server.
    pub async fn search(&self, query: SearchQuery) -> Result<SearchResult> {
        let req = pb::SearchRequest {
            text: query.text,
            workflow: query.workflow,
            status: query.status,
            updated_from: query.updated_from,
            updated_to: query.updated_to,
            limit: query.limit,
            partial_ok: query.partial_ok,
            semantic: query.semantic,
        };
        let res = self.call("SearchInstances", req, |mut s, r| async move { s.search_instances(r).await }).await?;
        Ok(SearchResult {
            hits: res
                .hits
                .into_iter()
                .map(|h| SearchHit {
                    instance_id: h.instance_id,
                    workflow: h.workflow,
                    version: h.version,
                    status: h.status,
                    correlation_id: h.correlation_id,
                    created_at: h.created_at,
                    updated_at: h.updated_at,
                    score: h.score,
                    purged: h.purged,
                })
                .collect(),
            partial: res.partial,
            model: res.model,
        })
    }

    // ---- schedules ----

    /// Starts `workflow` with `context` every `every`.
    pub async fn create_schedule<T: Serialize + ?Sized>(&self, workflow: &str, every: Duration, context: &T) -> Result<ScheduleInfo> {
        let cadence = pb::create_schedule_request::Cadence::EveryMillis(every.as_millis() as i64);
        self.schedule(workflow, cadence, context).await
    }

    /// Starts `workflow` with `context` on a five-field cron expression, evaluated in UTC.
    pub async fn create_cron_schedule<T: Serialize + ?Sized>(&self, workflow: &str, cron: &str, context: &T) -> Result<ScheduleInfo> {
        let cadence = pb::create_schedule_request::Cadence::Cron(cron.to_string());
        self.schedule(workflow, cadence, context).await
    }

    async fn schedule<T: Serialize + ?Sized>(
        &self,
        workflow: &str,
        cadence: pb::create_schedule_request::Cadence,
        context: &T,
    ) -> Result<ScheduleInfo> {
        let req = pb::CreateScheduleRequest {
            workflow: workflow.to_string(),
            cadence: Some(cadence),
            context: Some(to_value(&serde_json::to_value(context)?)),
        };
        let res = self.call("CreateSchedule", req, |mut s, r| async move { s.create_schedule(r).await }).await?;
        Ok(ScheduleInfo::from(res))
    }

    pub async fn schedules(&self) -> Result<Vec<ScheduleInfo>> {
        let res = self.call("ListSchedules", pb::Empty {}, |mut s, r| async move { s.list_schedules(r).await }).await?;
        Ok(res.schedules.into_iter().map(ScheduleInfo::from).collect())
    }

    pub async fn delete_schedule(&self, id: &str) -> Result<()> {
        let req = pb::ScheduleIdRequest { id: id.to_string() };
        self.call("DeleteSchedule", req, |mut s, r| async move { s.delete_schedule(r).await }).await?;
        Ok(())
    }

    // ---- operations ----

    pub async fn health(&self) -> Result<Health> {
        let res = self.call("HealthCheck", pb::Empty {}, |mut s, r| async move { s.health_check(r).await }).await?;
        Ok(Health { status: res.status, node: res.node, leader: res.leader })
    }

    pub async fn cluster(&self) -> Result<ClusterView> {
        let res = self.call("GetCluster", pb::Empty {}, |mut s, r| async move { s.get_cluster(r).await }).await?;
        Ok(ClusterView {
            this_node: res.self_,
            leader: res.leader,
            members: res
                .members
                .into_iter()
                .map(|m| ClusterMember {
                    id: m.id,
                    name: m.name,
                    first_heartbeat: m.first_heartbeat,
                    last_heartbeat: m.last_heartbeat,
                    workers: m.workers,
                    leader: m.leader,
                    alive: m.alive,
                })
                .collect(),
        })
    }

    /// Dispatchable work split by (workflow, version, queue), each flagged with whether any live
    /// worker would claim it. `max` 0 = a server default.
    pub async fn backlog_coverage(&self, max: i32) -> Result<BacklogCoverage> {
        let req = pb::BacklogCoverageRequest { max };
        let res = self.call("GetBacklogCoverage", req, |mut s, r| async move { s.get_backlog_coverage(r).await }).await?;
        Ok(BacklogCoverage {
            live_pollers: res.live_pollers,
            slices: res
                .slices
                .into_iter()
                .map(|s| BacklogSlice {
                    workflow: s.workflow,
                    version: s.version,
                    queue: s.queue,
                    ready_count: s.ready_count,
                    oldest_available_at: s.oldest_available_at,
                    covered: s.covered,
                    coverage: s.coverage,
                    claimable: s.claimable,
                    criteria: s.criteria,
                })
                .collect(),
        })
    }

    /// Per-node duration statistics over recently settled steps of one workflow version
    /// (`None` = the latest). `since` is epoch millis (0 = no bound); `sample` 0 = a server default.
    pub async fn step_stats(&self, workflow: &str, version: Option<i32>, since: i64, sample: i32) -> Result<StepStats> {
        let req = pb::StepStatsRequest { workflow: workflow.to_string(), version: version.unwrap_or(0), since, sample };
        let res = self.call("GetStepStats", req, |mut s, r| async move { s.get_step_stats(r).await }).await?;
        Ok(StepStats {
            workflow: res.workflow,
            version: res.version,
            nodes: res
                .nodes
                .into_iter()
                .map(|n| NodeStats {
                    node_id: n.node_id,
                    name: n.name,
                    count: n.count,
                    mean_millis: n.mean_millis,
                    p50_millis: n.p50_millis,
                    p95_millis: n.p95_millis,
                    max_millis: n.max_millis,
                    wait_p50_millis: n.wait_p50_millis,
                    wait_p95_millis: n.wait_p95_millis,
                })
                .collect(),
        })
    }

    // ---- event feed ----

    /// Long-polls the event log through the named cursor `consumer` (the first poll registers it,
    /// starting at `start_from`: 0 = tail, -1 = earliest retained, else a seq).
    pub async fn poll_events(&self, consumer: &str, max: i32, wait: Duration, start_from: i64) -> Result<Vec<EventView>> {
        let req = pb::PollEventsRequest {
            consumer: consumer.to_string(),
            max,
            wait_millis: wait.as_millis() as i64,
            start_from,
        };
        let res = self.call("PollEvents", req, |mut s, r| async move { s.poll_events(r).await }).await?;
        Ok(res
            .events
            .into_iter()
            .map(|e| EventView {
                seq: e.seq,
                instance_id: e.instance_id,
                workflow: e.workflow,
                version: e.version,
                correlation_id: e.correlation_id,
                event_type: e.r#type,
                created_at: e.created_at,
                payload: opt_value(e.payload.as_ref()),
                node_id: e.node_id,
                shard: e.shard,
                cursor: e.cursor,
            })
            .collect())
    }

    /// Advances `consumer` past the event whose cursor is `cursor` (cumulative).
    pub async fn ack_events(&self, consumer: &str, cursor: &str) -> Result<()> {
        self.ack(pb::AckEventsRequest { consumer: consumer.to_string(), acked_seq: 0, acked_cursor: cursor.to_string() }).await
    }

    /// Advances `consumer` past every event with seq <= `seq`. Only for a log on one shard.
    pub async fn ack_events_seq(&self, consumer: &str, seq: i64) -> Result<()> {
        self.ack(pb::AckEventsRequest { consumer: consumer.to_string(), acked_seq: seq, acked_cursor: String::new() }).await
    }

    async fn ack(&self, req: pb::AckEventsRequest) -> Result<()> {
        self.call("AckEvents", req, |mut s, r| async move { s.ack_events(r).await }).await?;
        Ok(())
    }

    // ---- worker RPCs (used by Worker) ----

    /// Leases up to `max` ready tasks from `queues`, waiting up to `wait` for one to appear. A
    /// non-empty `versions` claims only those (workflow, version) pairs' tasks.
    pub async fn poll(
        &self,
        worker_id: &str,
        queues: &[String],
        max: i32,
        lease: Duration,
        wait: Duration,
        versions: &[WorkflowVersion],
    ) -> Result<PollResult> {
        let req = pb::PollRequest {
            worker_id: worker_id.to_string(),
            queues: queues.to_vec(),
            max,
            lease_millis: lease.as_millis() as i64,
            wait_millis: wait.as_millis() as i64,
            versions: versions
                .iter()
                .map(|v| pb::WorkflowVersion { workflow: v.workflow.clone(), version: v.version })
                .collect(),
            criteria: Vec::new(),
        };
        let res = self.call("PollTasks", req, |mut s, r| async move { s.poll_tasks(r).await }).await?;
        Ok(PollResult {
            tasks: res.tasks.into_iter().map(Task::from).collect(),
            retry_after: Duration::from_millis(res.retry_after_millis.max(0) as u64),
        })
    }

    /// Reports the finished steps of one leased task. `final_handback` says this worker takes no
    /// continuation. A refused run fails with [`Error::ReportRefused`].
    pub async fn report_steps(
        &self,
        task_id: &str,
        lease_owner: &str,
        steps: Vec<StepReport>,
        final_handback: bool,
    ) -> Result<RunApplied> {
        let run = pb::ReportedRun {
            task_id: task_id.to_string(),
            lease_owner: lease_owner.to_string(),
            steps: steps.into_iter().map(StepReport::into_pb).collect(),
            r#final: final_handback,
            criteria: Vec::new(),
        };
        let req = pb::ReportStepsRequest { runs: vec![run] };
        let res = self.call("ReportSteps", req, |mut s, r| async move { s.report_steps(r).await }).await?;
        let refused = |status: i32, message: String| Error::ReportRefused { task_id: task_id.to_string(), status, message };
        let outcome = res
            .results
            .into_iter()
            .find(|r| r.task_id == task_id)
            .ok_or_else(|| refused(500, "no answer for this run".into()))?;
        match outcome.applied {
            Some(a) if outcome.error_status == 0 => Ok(RunApplied {
                instance_status: a.instance_status,
                lease_expires_at: a.lease_expires_at,
                next_task_id: a.next_task_id,
            }),
            _ => Err(refused(outcome.error_status, outcome.error)),
        }
    }

    /// Reports a failed task; `retryable = false` skips the retry policy.
    pub async fn fail(&self, task_id: &str, lease_owner: &str, message: &str, retryable: bool) -> Result<()> {
        let req = pb::TaskFailureRequest {
            task_id: task_id.to_string(),
            lease_owner: lease_owner.to_string(),
            message: message.to_string(),
            retryable,
        };
        self.call("FailTask", req, |mut s, r| async move { s.fail_task(r).await }).await?;
        Ok(())
    }

    /// Extends the lease on an in-flight task; returns the new expiry (epoch millis).
    pub async fn heartbeat(&self, task_id: &str, lease_owner: &str, extend: Duration) -> Result<i64> {
        let req = pb::HeartbeatRequest {
            task_id: task_id.to_string(),
            lease_owner: lease_owner.to_string(),
            extend_millis: extend.as_millis() as i64,
        };
        let res = self.call("HeartbeatTask", req, |mut s, r| async move { s.heartbeat_task(r).await }).await?;
        Ok(res.lease_expires_at)
    }
}

/// NOT_FOUND and UNAVAILABLE are expected (a worker waiting for a registration, a server not yet
/// up) and CANCELLED is a shutdown, so only other failures are logged above debug.
fn log_failure(method: &str, status: &Status) {
    match status.code() {
        Code::Cancelled => {}
        Code::NotFound | Code::Unavailable => {
            log::debug!("wiggle: rpc {method}: {:?}: {}", status.code(), status.message())
        }
        code => log::warn!("wiggle: rpc {method} failed: {code:?}: {}", status.message()),
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.trim().parse().ok()).unwrap_or(default)
}

fn non_empty(s: String) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

impl AsRef<str> for FlowSpec {
    fn as_ref(&self) -> &str {
        self.name()
    }
}

// ---- views ----

#[derive(Debug, Clone)]
pub struct RegisterResult {
    pub name: String,
    pub version: String,
    pub nodes: i64,
}

/// Options for [`Client::start_with`].
#[derive(Debug, Clone, Default)]
pub struct StartOptions {
    pub version: Option<i32>,
    pub correlation_id: Option<String>,
}

impl StartOptions {
    /// Pins the workflow version to start (else the latest registered).
    pub fn version(mut self, version: i32) -> Self {
        self.version = Some(version);
        self
    }

    /// Attaches a correlation id (a business key) to the instance.
    pub fn correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }
}

/// A snapshot of a workflow instance.
#[derive(Debug, Clone)]
pub struct InstanceView {
    pub id: String,
    pub workflow: String,
    pub version: i32,
    pub status: String,
    pub termination_reason: Option<String>,
    pub error: Option<String>,
    pub context: Value,
    pub created_at: i64,
    pub updated_at: i64,
}

impl InstanceView {
    /// Whether the instance reached a final state.
    pub fn is_terminal(&self) -> bool {
        TERMINAL.contains(&self.status.as_str())
    }

    /// The context decoded into `T`.
    pub fn context_as<T: DeserializeOwned>(&self) -> Result<T> {
        Ok(serde_json::from_value(self.context.clone())?)
    }
}

impl From<pb::InstanceView> for InstanceView {
    fn from(v: pb::InstanceView) -> Self {
        InstanceView {
            context: opt_value(v.context.as_ref()),
            id: v.id,
            workflow: v.workflow,
            version: v.version,
            status: v.status,
            termination_reason: v.termination_reason,
            error: v.error,
            created_at: v.created_at,
            updated_at: v.updated_at,
        }
    }
}

/// One token of an instance: a step reached, with its state and (where recorded) its input/output.
#[derive(Debug, Clone)]
pub struct TokenInfo {
    pub id: String,
    pub node_id: String,
    pub kind: String,
    pub status: String,
    pub activity: String,
    pub attempt: i32,
    pub available_at: i64,
    pub lease_owner: Option<String>,
    pub last_error: Option<String>,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub input: Option<Value>,
    pub output: Option<Value>,
    pub created_at: i64,
}

impl From<pb::Token> for TokenInfo {
    fn from(t: pb::Token) -> Self {
        TokenInfo {
            input: t.input.as_ref().map(from_value),
            output: t.output.as_ref().map(from_value),
            id: t.id,
            node_id: t.node_id,
            kind: t.kind,
            status: t.status,
            activity: t.activity,
            attempt: t.attempt,
            available_at: t.available_at,
            lease_owner: t.lease_owner,
            last_error: t.last_error,
            started_at: t.started_at,
            finished_at: t.finished_at,
            created_at: t.created_at,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InstanceWithTokens {
    pub instance: InstanceView,
    pub tokens: Vec<TokenInfo>,
}

#[derive(Debug, Clone)]
pub struct Health {
    pub status: String,
    pub node: String,
    pub leader: bool,
}

#[derive(Debug, Clone)]
pub struct ClusterMember {
    pub id: String,
    pub name: String,
    pub first_heartbeat: i64,
    pub last_heartbeat: i64,
    pub workers: i32,
    pub leader: bool,
    pub alive: bool,
}

#[derive(Debug, Clone)]
pub struct ClusterView {
    /// The id of the node that answered.
    pub this_node: String,
    pub leader: bool,
    pub members: Vec<ClusterMember>,
}

#[derive(Debug, Clone)]
pub struct ScheduleInfo {
    pub id: String,
    pub workflow: String,
    /// 0 when cron-based.
    pub every_millis: i64,
    /// Empty when interval-based.
    pub cron: String,
    pub next_fire_at: i64,
    pub created_at: i64,
}

impl From<pb::ScheduleView> for ScheduleInfo {
    fn from(s: pb::ScheduleView) -> Self {
        ScheduleInfo {
            id: s.id,
            workflow: s.workflow,
            every_millis: s.every_millis,
            cron: s.cron,
            next_fire_at: s.next_fire_at,
            created_at: s.created_at,
        }
    }
}

/// A [`Client::search`] query. Every word of `text` must occur in the instance's correlation id,
/// context, termination reason or error; empty matches every instance the filters allow.
#[derive(Debug, Clone, Default)]
pub struct SearchQuery {
    pub text: String,
    pub workflow: Option<String>,
    pub status: Option<String>,
    /// Bounds on when the instance last changed (epoch millis); 0 = open.
    pub updated_from: i64,
    pub updated_to: i64,
    pub limit: i32,
    /// Answer from the search shards that respond when one does not.
    pub partial_ok: bool,
    /// Rank by closeness in meaning (vector search) instead of by the words.
    pub semantic: bool,
}

impl SearchQuery {
    pub fn text(text: impl Into<String>) -> Self {
        SearchQuery { text: text.into(), ..Default::default() }
    }
}

#[derive(Debug, Clone)]
pub struct SearchHit {
    pub instance_id: String,
    pub workflow: String,
    pub version: i32,
    pub status: String,
    pub correlation_id: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub score: f64,
    /// The instance itself has been purged; its document outlives it until its own retention.
    pub purged: bool,
}

#[derive(Debug, Clone)]
pub struct SearchResult {
    pub hits: Vec<SearchHit>,
    pub partial: bool,
    /// The embedding model a semantic search used; empty for a text search.
    pub model: String,
}

#[derive(Debug, Clone)]
pub struct BacklogSlice {
    pub workflow: String,
    pub version: i32,
    pub queue: String,
    pub ready_count: i32,
    pub oldest_available_at: i64,
    pub covered: bool,
    /// COVERED, CONDITIONAL or UNCOVERED.
    pub coverage: String,
    pub claimable: Option<i32>,
    pub criteria: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct BacklogCoverage {
    pub slices: Vec<BacklogSlice>,
    pub live_pollers: i32,
}

#[derive(Debug, Clone)]
pub struct NodeStats {
    pub node_id: String,
    pub name: String,
    pub count: i64,
    pub mean_millis: f64,
    pub p50_millis: i64,
    pub p95_millis: i64,
    pub max_millis: i64,
    pub wait_p50_millis: i64,
    pub wait_p95_millis: i64,
}

#[derive(Debug, Clone)]
pub struct StepStats {
    pub workflow: String,
    pub version: i32,
    pub nodes: Vec<NodeStats>,
}

/// One entry of the event log: a lifecycle transition (`wf.*`) or a domain event a step emitted.
#[derive(Debug, Clone)]
pub struct EventView {
    pub seq: i64,
    pub instance_id: String,
    pub workflow: String,
    pub version: i32,
    pub correlation_id: String,
    pub event_type: String,
    pub created_at: i64,
    pub payload: Value,
    pub node_id: String,
    pub shard: i32,
    /// The consumer's place just after this entry, for [`Client::ack_events`].
    pub cursor: String,
}

// ---- worker wire types ----

/// One (workflow, version) a worker serves.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkflowVersion {
    pub workflow: String,
    pub version: i32,
}

/// A unit of work leased to a worker. For a forEach item step `base_context` is set: `context` is
/// then the item's current value (any JSON value, scalars included) and `base_context` the frozen
/// pre-forEach context.
#[derive(Debug, Clone)]
pub struct Task {
    pub task_id: String,
    pub instance_id: String,
    pub workflow: String,
    pub version: i32,
    pub node_id: String,
    pub step_name: String,
    pub activity: String,
    /// TASK or PREDICATE.
    pub kind: String,
    pub attempt: i32,
    pub lease_expires_at: i64,
    pub lease_owner: String,
    pub context: Value,
    pub execution_mode: String,
    pub base_context: Option<Value>,
    pub item_index: i64,
    pub item_map_key: Option<String>,
    /// Set only for the combine of a forEach: where its results are staged.
    pub collect_key: Option<String>,
    /// Set only for a fork's combine: the arms it merges, in fork order.
    pub arm_names: Vec<String>,
    pub correlation_id: Option<String>,
    pub instance_created_at: i64,
}

impl From<pb::TaskActivation> for Task {
    fn from(t: pb::TaskActivation) -> Self {
        Task {
            context: opt_value(t.context.as_ref()),
            base_context: t.base_context.as_ref().map(from_value),
            task_id: t.task_id,
            instance_id: t.instance_id,
            workflow: t.workflow,
            version: t.version,
            node_id: t.node_id,
            step_name: t.step_name,
            activity: t.activity,
            kind: t.kind,
            attempt: t.attempt,
            lease_expires_at: t.lease_expires_at,
            lease_owner: t.lease_owner,
            execution_mode: t.execution_mode,
            item_index: t.item_index,
            item_map_key: non_empty(t.item_map_key),
            collect_key: non_empty(t.collect_key),
            arm_names: t.arm_names,
            correlation_id: non_empty(t.correlation_id),
            instance_created_at: t.instance_created_at,
        }
    }
}

/// The outcome of a poll: leased tasks, plus a backpressure hold-off (zero when none).
#[derive(Debug, Clone)]
pub struct PollResult {
    pub tasks: Vec<Task>,
    pub retry_after: Duration,
}

/// A domain event a handler emitted while its step ran.
#[derive(Debug, Clone)]
pub struct EmittedEvent {
    pub event_type: String,
    pub payload: Value,
}

/// One finished step as a worker reports it: a task's `merge` (its complete next context; `None`
/// leaves the context unchanged) or a predicate's `predicate`, with when it ran (epoch millis; 0 =
/// not reported) and the events it emitted.
#[derive(Debug, Clone, Default)]
pub struct StepReport {
    pub node_id: String,
    pub merge: Option<Value>,
    pub predicate: Option<bool>,
    pub started_at: i64,
    pub finished_at: i64,
    pub events: Vec<EmittedEvent>,
    /// Branches the step created at run time (`Step::create`), in creation order.
    pub branches: Vec<pb::CreatedBranch>,
}

impl StepReport {
    fn into_pb(self) -> pb::StepResult {
        use pb::step_result::Outcome;
        let outcome = match (self.predicate, self.merge) {
            (Some(b), _) => Some(Outcome::PredicateValue(b)),
            (None, Some(m)) => Some(Outcome::Merge(to_value(&m))),
            (None, None) => None,
        };
        pb::StepResult {
            node_id: self.node_id,
            outcome,
            started_at: self.started_at,
            finished_at: self.finished_at,
            events: self
                .events
                .into_iter()
                .map(|e| pb::EmittedEvent { r#type: e.event_type, payload: Some(to_value(&e.payload)) })
                .collect(),
            branches: self.branches,
        }
    }
}

/// What a reported run applied to. Any status but RUNNING means stop here.
#[derive(Debug, Clone)]
pub struct RunApplied {
    pub instance_status: String,
    pub lease_expires_at: i64,
    pub next_task_id: String,
}
