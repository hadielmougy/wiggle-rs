//! A Rust client and worker for [Wiggle](https://github.com/hadielmougy/wiggle), the durable
//! workflow engine.
//!
//! It speaks the same gRPC control plane as the Java client (and the Go and Python ports), so a
//! Rust worker interoperates with them on one server: dispatch is by activity name
//! (`"<workflow>#<step>"`), not by language.
//!
//! The pieces mirror the Java client:
//!
//! * [`FlowSpec::define`] -- the topology DSL (`step`, `gate`, `fork(..).combine(..)`, `for_each`,
//!   `choose`, `do_while`, ...), compiled to the definition registered with the server. It carries
//!   no step logic.
//! * [`Client`] -- register workflows, start / inspect / cancel / signal instances, schedules,
//!   search, the event feed, and the low-level worker RPCs.
//! * [`Worker`] -- binds handlers to steps **by name**, reconciles them against the registered
//!   graph, then polls, runs and reports. Handlers are typed: their input is decoded from the
//!   step's JSON context with serde and their output encoded back, like the Java client's typed
//!   handler methods.
//! * [`Wiggle::define`] -- the typed flow API: a workflow written as a chain of the functions that
//!   implement its steps (`then_apply`, `then_filter`, [`Wiggle::all_of`]`(..).combine(..)`,
//!   [`Wiggle::one_of`], ...), checked by the compiler, carrying its handlers for
//!   [`Worker::flow`].
//! * [`Step`] -- the running step's identity (attempt, instance, forEach base/item index) and
//!   [`Step::emit`] for domain events, the equivalent of Java's `Step.current()`.
//!
//! ```no_run
//! use std::time::Duration;
//! use serde::{Deserialize, Serialize};
//! use wiggle::{Client, FlowSpec, RetryPolicy, Worker};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Order { order_id: String, quantity: u32, status: Option<String> }
//!
//! # async fn run() -> Result<(), wiggle::Error> {
//! let client = Client::connect("localhost:8080").await?;
//!
//! let order = FlowSpec::define("order", 1)
//!     .step("validate")
//!     .gate("in-stock")
//!     .step("charge").queue("payments").retry(RetryPolicy::exponential(5, Duration::from_millis(100)))
//!     .effect("notify")
//!     .build()?;
//! client.register(&order).await?;
//!
//! let worker = Worker::new(client.clone(), "worker-1")
//!     .handle("order", "validate", |mut o: Order| async move {
//!         o.status = Some("VALIDATED".into());
//!         Ok(o)
//!     })
//!     .gate("order", "in-stock", |o: Order| async move { Ok(o.quantity > 0) })
//!     .handle("order", "charge", |o: Order| async move { Ok(o) })
//!     .effect("order", "notify", |o: Order| async move {
//!         println!("shipped {}", o.order_id);
//!         Ok(())
//!     })
//!     .start()
//!     .await?;
//!
//! let id = client.start(&order, &Order { order_id: "A-1".into(), quantity: 3, status: None }).await?;
//! let view = client.await_completion(&id, Duration::from_secs(30)).await?;
//! println!("{} {}", view.status, view.context);
//! worker.shutdown().await;
//! # Ok(()) }
//! ```

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

pub use client::{
    BacklogCoverage, BacklogSlice, Client, ClusterMember, ClusterView, EmittedEvent, EventView,
    Health, InstanceDetail, InstanceView, NodeStats, PollResult, RegisterResult, RunApplied,
    ScheduleView, SearchHit, SearchQuery, SearchResult, StartOptions, StepReport, StepStats, Task,
    TokenView, WorkflowVersion,
};
pub use error::{Error, Result, StepError};
pub use flow::{Branch, Case, FlowSpec, ForEachStage, ForkStage, GraphBuilder};
pub use retry::RetryPolicy;
pub use step::Step;
pub use typed::{
    AllOf, Flow, FlowDefaults, Fork2, Fork3, Fork4, Fork5, Fork6, Fork7, Fork8, ForkN, Items, Wiggle, WiggleFlow,
};
pub use worker::{Combine, Compensation, RunningWorker, Worker, WorkerOptions};

/// The instance context: a JSON value flowing through the steps (an object at the top level).
pub type Context = serde_json::Value;
