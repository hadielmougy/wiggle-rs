# Wiggle — Rust client

A Rust client and worker for [Wiggle](https://github.com/hadielmougy/wiggle), the durable workflow
engine. It speaks the same gRPC control plane as the Java client (and the Go and Python ports), so a
Rust worker **interoperates** with them on one server: dispatch is by activity name
(`"<workflow>#<step>"`), not by language.

```toml
[dependencies]
wiggle-client = { git = "https://github.com/hadielmougy/wiggle-rs" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
serde = { version = "1", features = ["derive"] }
```

The library is imported as `wiggle`. Building it needs `protoc` on the `PATH` (`brew install protobuf`):
`build.rs` generates the stubs from the vendored `proto/wiggle.proto`.

## Quick start

```rust
use std::time::Duration;
use serde::{Deserialize, Serialize};
use wiggle::{Client, FlowSpec, RetryPolicy, Worker};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Order { order_id: String, quantity: u32, status: Option<String> }

#[tokio::main]
async fn main() -> Result<(), wiggle::Error> {
    let client = Client::connect("localhost:8080").await?;

    // The topology: named steps and how they chain. No step logic.
    let order = FlowSpec::define("order", 1)
        .step("validate")
        .gate("in-stock")
        .step("charge").queue("payments").retry(RetryPolicy::exponential(5, Duration::from_millis(100)))
        .effect("notify")
        .build()?;
    client.register(&order).await?;

    // The logic: handlers bound by name. Input is decoded from the step's JSON context with serde,
    // the return encoded back -- it is the COMPLETE next context.
    let worker = Worker::new(client.clone(), "worker-1")
        .handle("order", "validate", |mut o: Order| async move {
            o.status = Some("VALIDATED".into());
            Ok(o)
        })
        .gate("order", "in-stock", |o: Order| async move { Ok(o.quantity > 0) })
        .handle("order", "charge", |o: Order| async move { Ok(o) })
        .effect("order", "notify", |o: Order| async move {
            println!("shipped {}", o.order_id);
            Ok(())
        })
        .start()
        .await?;

    let id = client.start(&order, &Order { order_id: "A-1".into(), quantity: 3, status: None }).await?;
    let view = client.await_completion(&id, Duration::from_secs(30)).await?;
    println!("{} {}", view.status, view.context); // COMPLETED {...}

    worker.shutdown().await;
    Ok(())
}
```

Run the bundled example against a server on `:8080`:

```bash
WIGGLE_URL=localhost:8080 cargo run --example order
```

## Topology

`FlowSpec::define(name, version)` returns a builder with the same vocabulary as the Java
`GraphBuilder`. The version is declared, not derived: a published version is immutable, so a changed
graph needs a new number. Misuse is reported by `build()`.

```rust
use wiggle::{Branch, Case, FlowSpec};

let wf = FlowSpec::define("order-fulfilment", 1)
    .default_queue("orders")                 // else the workflow name
    .step("validate")
    .gate("in-stock")
    .fork(vec![
        Branch::of("payment",  |s| s.step("charge").queue("payments")),
        Branch::of("shipping", |s| s.step("reserve").sleep(Duration::from_secs(2)).step("label")),
    ])
    .combine("settle")                       // mandatory: branches rejoin at an explicit merge
    .for_each("lines", |s| s.step("price"))
    .combine("total")
    .choose(vec![
        Case::when("vip", |s| s.step("concierge")),
        Case::otherwise(|s| s.step("thanks")),
    ])
    .do_while("more-pages", |s| s.step("fetch-page"))
    .await_signal_or("approved", Duration::from_secs(3600), |s| s.step("escalate"))
    .sub_workflow("invoice", "invoicing")
    .effect("notify")
    .build()?;
```

| Builder call | Meaning |
|---|---|
| `step(name)` / `then(name)` | a task run on a worker (`Worker::handle`); its return replaces the context |
| `effect(name)` | a side-effect step (`Worker::effect`); context unchanged |
| `gate(name)` | a predicate (`Worker::gate`); false ends the instance as `gated:<name>` (in a fork branch: skips to the join) |
| `.queue(q)` / `.retry(p)` / `.compensate()` | modify the step just added (`queue`/`retry` also apply to a combine) |
| `fork(branches).combine(name)` | parallel branches on isolated context copies, rejoined at the **mandatory** combine (`Worker::combine`) |
| `for_each(key, body).combine(name)` | one isolated branch per element of the list/map at `key`; **the element is the item's context** |
| `choose(cases)` | the first `Case::when` whose guard holds runs; `Case::otherwise` (last) runs when none did |
| `do_while(cond, body)` / `do_while_max(cond, n, body)` | run `body`, repeat while `cond` holds; budgeted (engine default 10,000) |
| `sleep(d)` / `sleep_named(name, d)` | a server-side timer; no worker held |
| `await_signal(name)` / `await_signal_timeout` / `await_signal_or(name, d, escalation)` | wait for `Client::signal`; on timeout fail, or run the escalation and rejoin |
| `sub_workflow(name, workflow)` | run another workflow as a child; its result merges back |
| `default_queue(q)` / `default_retry(p)` | workflow-wide defaults (`RetryPolicy::forever()` unless set) |

Retry policies: `RetryPolicy::exponential(max, initial)`, `fixed(max, backoff)`, `none()`, `forever()`.

## Worker

Handlers are `async` closures bound by `(workflow, step)`. The input type is anything
`DeserializeOwned` (use `serde_json::Value` for untyped access); the output anything `Serialize`.
A decode failure fails the step permanently.

| Binding | Handler shape | Step kind |
|---|---|---|
| `handle(wf, step, f)` | `Fn(I) -> Future<Result<O, StepError>>` | task — returns the complete next context; a `null` result (e.g. `None`) leaves it untouched |
| `effect(wf, step, f)` | `Fn(I) -> Future<Result<(), StepError>>` | task, context unchanged |
| `gate(wf, step, f)` | `Fn(I) -> Future<Result<bool, StepError>>` | gate / choose guard / do-while condition |
| `combine(wf, step, f)` | `Fn(Combine) -> Future<Result<O, StepError>>` | a fork / forEach combine |
| `compensation(wf, step, f)` | `Fn(Compensation) -> Future<Result<(), StepError>>` | the undo of a `compensate()` step |

`start()` **reconciles** every binding against the registered graph: a mistyped step name fails
fast with the available names, a handler bound with the wrong kind (a task on a gate, a plain
handler on a combine) is refused, and the worker learns which queue each step polls. Use
`WorkerOptions::await_registration(d)` to ride out a registration race.

```rust
let worker = Worker::new(client.clone(), "orders-1")
    .options(WorkerOptions::default()
        .concurrency(16)                     // steps in flight (default: CPUs)
        .lease(Duration::from_secs(30))      // heartbeats extend it every half lease
        .queues(["payments"]))               // else every queue its steps poll
    .handle("order-fulfilment", "charge", charge)
    .start()
    .await?;
// ...
worker.shutdown().await;                     // stop polling, drain in-flight steps
```

### Errors

`?` works on any `std::error::Error` inside a handler: it becomes a **retryable** failure, retried
per the step's policy. `StepError::permanent(msg)` fails the step without retrying. A panicking
handler is reported as a retryable failure.

### Combines

A fork or forEach rejoins at a combine whose handler receives a `Combine` and returns the
**complete** post-join context — the engine replaces the context with it, so keys it omits do not
survive the join.

```rust
.combine("order-fulfilment", "settle", |c: Combine| async move {
    let mut out = c.base().clone();                       // the pre-fork context
    out["paymentRef"] = c.arm("payment").unwrap()["paymentRef"].clone();
    out["tracking"] = c.arm("shipping").unwrap()["tracking"].clone();
    Ok(out)
})
.combine("order-fulfilment", "total", |c: Combine| async move {
    let prices: Vec<f64> = c.items_as()?;                 // forEach: one value per item, in order
    let mut out = c.base().clone();
    out["total"] = serde_json::json!(prices.iter().sum::<f64>());
    Ok(out)
})
```

Inside a forEach body the handler's input is the **item's value** (scalars included) and its return
replaces the item; the frozen pre-forEach context is `Step::base()`, the position `Step::item_index()`.

### Compensation (sagas)

A step declared `.compensate()` is undone when the instance later fails: the engine runs its
compensator in the reverse pass (newest-completed first) with the **input/result snapshots** captured
when the step completed. The instance settles `COMPENSATED` (or `COMPENSATION_FAILED`).

```rust
.handle("trip", "book-hotel", book_hotel)
.compensation("trip", "book-hotel", |c: Compensation| async move {
    cancel_booking(c.result["bookingRef"].as_str().unwrap()).await?;   // idempotent!
    Ok(())
})
```

The pairing is checked both ways at `start()`: a compensable step served without its compensator,
or a compensator on a step not declared compensable, is refused.

### The running step

`Step` exposes the running step's identity from inside a handler, like Java's `Step.current()`:
`Step::attempt()`, `name()`, `instance_id()`, `workflow()`, `correlation_id()`, `base()`,
`item_index()`, `item_map_key()` — and `Step::emit(type, payload)`, which buffers a domain event that
is committed to the event log **if and only if** the attempt completes.

## Typed flows

The counterpart of the Java client's `Wiggle.allOf` / `WiggleFlow`: a workflow written as a chain
of the very functions that implement its steps, checked by the compiler. Each step's input type is
the previous step's output type, and a fork's combine takes one parameter per arm, in arm order.

```rust
use wiggle::{StepError, Wiggle, Worker};

async fn validate(o: Order) -> Result<Order, StepError> { /* ... */ }
async fn in_stock(o: Order) -> Result<bool, StepError> { /* ... */ }
async fn charge(o: Order) -> Result<Payment, StepError> { /* ... */ }
async fn label(o: Order) -> Result<Shipment, StepError> { /* ... */ }
async fn settle(p: Payment, s: Shipment) -> Result<Receipt, StepError> { /* ... */ }
async fn notify(r: Receipt) -> Result<(), StepError> { /* ... */ }

let order = Wiggle::define("order-fulfilment", 1, |f| {
    let validated = f.then_apply(validate).then_filter(in_stock);

    let payment = validated.then_apply(charge);       // continuing `validated` twice
    let shipping = validated.then_apply(label);       // is the fan-out

    Wiggle::all_of((payment, shipping))
        .combine(settle)                               // fn(Payment, Shipment)
        .then_accept(notify)
})?;

client.register(&order).await?;
let worker = Worker::new(client.clone(), "orders-1").flow(&order).start().await?;
```

- **Step names** come from the functions (`validate`, `in_stock`); pass `fn` items, not closures.
  A closure, or a step that must match an existing graph's name, is named with
  `.named("in-stock")`. `.queue(..)` and `.retry(..)` tune the step just added.
- **Handlers travel with the flow**: Java finds handler methods by reflection; here the functions
  are kept in the `Flow`, and `Worker::flow(&flow)` binds all of them under the same names.
- **Handles are not futures.** Nothing runs at definition time. A handle continued twice is a
  fan-out; a split that never reaches `all_of` fails the definition, naming both dangling ends.
  So does a fan-out with no combine, arms that do not share a fork point, and two arms ending at
  steps with the same name.

| Call | Meaning |
|---|---|
| `then_apply(f)` / `apply(f)` | a task: `f(T) -> R`; the flow continues as `R` |
| `then_accept(f)` | a side effect: `f(T) -> ()`; the context is unchanged |
| `then_filter(f)` | a gate: `f(T) -> bool` |
| `.compensate_with(undo)` | declares the task just added compensable, with `undo` as its compensator |
| `Wiggle::all_of((a, b, ..)).combine(f)` | 2–8 arms; `f(A, B, ..)` returns the complete post-join context, and `Step::base()` is the pre-fork context |
| `Wiggle::all_of_n(vec![..]).combine(f)` | any number of same-typed arms; `f(Vec<T>)` |
| `Wiggle::one_of(vec![f.when(g).then_apply(..), f.otherwise().then_apply(..)])` | exactly one arm runs; the arms must end at one type (use `.cast::<R>()`) |
| `then_for_each(key, \|item\| item.then_apply(..)).combine(f)` | one branch per element; `f(Vec<X>)` (or `combine_map` for a map input) |
| `repeat_while(cond, \|b\| ..)` / `repeat_while_max(cond, n, \|b\| ..)` | a do-while loop |
| `then_sleep(d)`, `then_await(signal)`, `then_await_or(signal, d, \|esc\| ..)`, `then_sub_flow::<R>(node, wf)` | timers, signals, sub-workflows |

`Wiggle::flow(name, version).default_queue(..).default_retry(..).define(..)` sets workflow-wide
defaults. A typed flow compiles to exactly the definition the builder produces for the same shape.

## Client API

```text
Client::connect(target)                       // "host:port" or a URL; plaintext, lazy connect
Client::connect_with(endpoint, api_key)       // a configured tonic Endpoint (TLS, timeouts, ...)

register(&spec) / register_force(&spec)       -> RegisterResult
get_workflow(name) / get_workflow_version(name, v) / workflow_names()
start(workflow, &ctx) / start_with(workflow, &ctx, StartOptions::default().version(2).correlation_id("..."))
instance(id) / instance_detail(id) / list_instances(wf, status, limit) / find_by_correlation(cid, limit)
await_completion(id, timeout) / cancel(id, reason) / signal(id, name, &payload)
create_schedule(wf, every, &ctx) / create_cron_schedule(wf, cron, &ctx) / schedules() / delete_schedule(id)
search(SearchQuery) / backlog_coverage(max) / step_stats(wf, version, since, sample)
poll_events(consumer, max, wait, start_from) / ack_events(consumer, cursor) / ack_events_seq(consumer, seq)
health() / cluster()
// worker RPCs: poll / report_steps / fail / heartbeat
```

`WIGGLE_API_KEY`, when set, is presented on every call as `authorization: Bearer <key>`. Every call
is retried on `UNAVAILABLE` with exponential backoff (`WIGGLE_RPC_MAX_ATTEMPTS`, default 5;
`WIGGLE_RPC_RETRY_DELAY_MILLIS`, default 200), so a worker rides out a server restart.

## Testing

```bash
cargo test                                                    # offline: topology shapes, binding rules
WIGGLE_TEST_URL=localhost:8080 cargo test --test integration  # end-to-end against a server
```

## Notes

- **Execution mode is SERVER only**: the worker runs one step per claim and hands back
  (no local step chaining / `LOCAL_SYNC` / `LOCAL_ASYNC`).
- **Not ported from the Java client**: `@ForFlow` handler objects matched by reflection (bind with
  `handle(..)`, or let a typed flow carry its handlers), step-contract interfaces, run-time branch
  creation (`Step.create`) and its spawn combine, `Items` accessor references (a forEach names its
  key as a string), queue criteria polls, and cross-instance batching.
- **Toolchain**: `Cargo.lock` pins a few transitive dependencies so the crate builds on Rust 1.82.
