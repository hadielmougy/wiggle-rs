//! A Rust client and worker for [Wiggle](https://github.com/hadielmougy/wiggle), the durable
//! workflow engine.
//!
//! It speaks the same gRPC control plane as the Java client (and the Go and Python ports), so a
//! Rust worker interoperates with them on one server: dispatch is by activity name
//! (`"<workflow>#<step>"`), not by language.
//!
//! The pieces mirror the Java client:
//!
//! * [`FlowSpec::define`] -- the typed flow API, like Java's `FlowSpec.define(.., Order.class, f -> ..)`:
//!   a workflow written as a chain of the functions that implement its steps (`then_apply`,
//!   `then_filter`, [`Wiggle::all_of`]`(..).combine(..)`, [`Wiggle::one_of`], ...), checked by the
//!   compiler, carrying its handlers for [`Worker::flow`].
//! * [`GraphBuilder::define`] -- the lower-level topology builder (Java's `GraphBuilder`: `step`,
//!   `gate`, `fork(..).combine(..)`, `for_each`, `choose`, `do_while`, ...), naming steps as
//!   strings; it carries no step logic.
//! * [`Client`] -- register workflows, start / inspect / cancel / signal instances, schedules,
//!   search, the event feed, and the low-level worker RPCs.
//! * [`Worker`] -- binds a typed flow's handlers, or handlers to topology steps **by name**,
//!   reconciles them against the registered graph, then polls, runs and reports. Handlers are typed: their input is decoded from the
//!   step's JSON context with serde and their output encoded back, like the Java client's typed
//!   handler methods.
//! * [`branch::Branch`] -- run-time branches a step opens with [`Step::create`], Java's
//!   `worker.Branch` (the topology's fork arm is [`Branch`], Java's `flow.Branch`).
//! * [`Step`] -- the running step's identity (attempt, instance, forEach base/item index) and
//!   [`Step::emit`] for domain events, the equivalent of Java's `Step.current()`.
//!
//! ```no_run
//! use std::time::Duration;
//! use serde::{Deserialize, Serialize};
//! use wiggle::{Client, FlowSpec, RetryPolicy, StepError, WiggleFlow, Worker};
//!
//! #[derive(Serialize, Deserialize)]
//! #[serde(rename_all = "camelCase")]
//! struct Order { order_id: String, quantity: u32, status: Option<String> }
//!
//! // The steps: plain async functions. The input is decoded from the step's JSON context with serde,
//! // the return encoded back -- it is the COMPLETE next context.
//! async fn validate(mut o: Order) -> Result<Order, StepError> {
//!     o.status = Some("VALIDATED".into());
//!     Ok(o)
//! }
//! async fn in_stock(o: Order) -> Result<bool, StepError> { Ok(o.quantity > 0) }
//! async fn charge(o: Order) -> Result<Order, StepError> { Ok(o) }
//! async fn notify(o: Order) -> Result<(), StepError> {
//!     println!("shipped {}", o.order_id);
//!     Ok(())
//! }
//!
//! # async fn run() -> Result<(), wiggle::Error> {
//! let client = Client::connect("localhost:8080").await?;
//!
//! // The workflow: a chain of the step functions, checked by the compiler -- Java's
//! // FlowSpec.define("order", 1, Order.class, f -> f.thenApply(h::validate).thenFilter(h::inStock)...).
//! let order = FlowSpec::define("order", 1, |f: WiggleFlow<Order>| {
//!     f.then_apply(validate)
//!         .then_filter(in_stock)
//!         .then_apply(charge).queue("payments").retry(RetryPolicy::exponential(5, Duration::from_millis(100)))
//!         .then_accept(notify)
//! })?;
//! client.register(&order).await?;
//!
//! // The flow carries its handlers: the worker binds each under its step name.
//! let worker = Worker::new(client.clone(), "worker-1").flow(&order).start().await?;
//!
//! let id = client.start(&order, &Order { order_id: "A-1".into(), quantity: 3, status: None }).await?;
//! let view = client.await_completion(&id, Duration::from_secs(30)).await?;
//! println!("{} {}", view.status, view.context); // COMPLETED {...}
//!
//! worker.close().await;
//! # Ok(()) }
//! ```

pub mod branch;
mod client;
mod error;
mod flow;
mod retry;
mod step;
mod typed;
mod value;
mod worker;

/// The generated gRPC stubs for the Wiggle control plane (`proto/wiggle.proto`).
pub mod pb {
    tonic::include_proto!("com.wiggle.proto");
}

pub use branch::{AllOfArms, BranchFork, BranchSteps, OneOfArms};
pub use client::{
    BacklogCoverage, BacklogSlice, Client, ClusterMember, ClusterView, EmittedEvent, EventView,
    Health, InstanceView, InstanceWithTokens, NodeStats, PollResult, RegisterResult, RunApplied,
    ScheduleInfo, SearchHit, SearchQuery, SearchResult, StartOptions, StepReport, StepStats, Task,
    TokenInfo, WorkflowVersion,
};
pub use error::{Error, Result, StepError};
pub use flow::{Branch, Case, FlowSpec, ForEachStage, ForkStage, GraphBuilder};
pub use retry::RetryPolicy;
pub use step::Step;
pub use typed::{
    AllOf, Fork2, Fork3, Fork4, Fork5, Fork6, Fork7, Fork8, ForkN, Items, Wiggle, WiggleFlow,
};
pub use worker::{Combine, Compensation, RunningWorker, Worker, WorkerOptions};

/// The instance context: a JSON value flowing through the steps (an object at the top level).
pub type Context = serde_json::Value;
