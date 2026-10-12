//! End-to-end against a running server; skipped unless WIGGLE_TEST_URL is set:
//!
//!     WIGGLE_TEST_URL=localhost:8080 cargo test --test integration

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use wiggle::{Branch, Client, Combine, FlowSpec, RetryPolicy, Step, StepError, Worker, WorkerOptions};

async fn client() -> Option<Client> {
    let url = std::env::var("WIGGLE_TEST_URL").ok()?;
    Some(Client::connect(&url).await.expect("connect"))
}

/// A fresh workflow name per run, so reruns against one server never collide.
fn unique(name: &str) -> String {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("rs-{name}-{nanos}")
}

fn opts() -> WorkerOptions {
    WorkerOptions::defaults().with_long_poll_wait(Duration::from_millis(500))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Order {
    order_id: String,
    quantity: u32,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    payment_ref: Option<String>,
}

#[tokio::test]
async fn linear_flow_with_gate_effect_and_events() {
    let Some(client) = client().await else { return };
    let wf = unique("order");
    let spec = FlowSpec::define(&wf, 1)
        .step("validate")
        .gate("in-stock")
        .step("charge")
        .queue(format!("{wf}-payments"))
        .retry(RetryPolicy::fixed(3, Duration::from_millis(50)))
        .effect("notify")
        .build()
        .unwrap();
    client.register(&spec).await.unwrap();

    let charges = Arc::new(AtomicU32::new(0));
    let c = charges.clone();
    let worker = Worker::new(client.clone(), "rs-it")
        .options(opts())
        .handle(&wf, "validate", |mut o: Order| async move {
            o.status = Some("VALIDATED".into());
            Ok(o)
        })
        .gate(&wf, "in-stock", |o: Order| async move { Ok(o.quantity > 0) })
        .handle(&wf, "charge", move |mut o: Order| {
            let c = c.clone();
            async move {
                // Fail the first attempt to exercise the retry policy.
                if c.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(StepError::retryable("card network blip"));
                }
                assert_eq!(Step::attempt(), 2);
                Step::emit("order.charged", json!({"orderId": o.order_id}))?;
                o.payment_ref = Some(format!("auth-{}", o.order_id));
                Ok(o)
            }
        })
        .effect(&wf, "notify", |_: Value| async move { Ok(()) })
        .start()
        .await
        .unwrap();
    assert_eq!(worker.queues(), [wf.clone(), format!("{wf}-payments")]);

    let ok = client.start(&spec, &json!({"orderId": "A-1", "quantity": 3})).await.unwrap();
    let gated = client.start(&spec, &json!({"orderId": "A-2", "quantity": 0})).await.unwrap();
    let ok = client.await_completion(&ok, Duration::from_secs(30)).await.unwrap();
    let gated = client.await_completion(&gated, Duration::from_secs(30)).await.unwrap();
    worker.close().await;

    assert_eq!(ok.status, "COMPLETED", "{ok:?}");
    let order: Order = ok.context_as().unwrap();
    assert_eq!(order.status.as_deref(), Some("VALIDATED"));
    assert_eq!(order.payment_ref.as_deref(), Some("auth-A-1"));
    assert_eq!(charges.load(Ordering::SeqCst), 2);

    assert_eq!(gated.status, "COMPLETED", "{gated:?}");
    assert_eq!(gated.termination_reason.as_deref(), Some("gated:in-stock"));

    // The emitted event was committed with the step that emitted it.
    // The feed may trail the instance's state briefly, so read it until the event shows up.
    let consumer = unique("feed");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut seen = Vec::new();
    let charged = loop {
        let batch = client.poll_events(&consumer, 1000, Duration::from_millis(200), -1).await.unwrap();
        if let Some(last) = batch.last() {
            client.ack_events(&consumer, &last.cursor).await.unwrap();
        }
        seen.extend(batch);
        let hits: Vec<_> = seen.iter().filter(|e| e.instance_id == ok.id && e.event_type == "order.charged").collect();
        if !hits.is_empty() || std::time::Instant::now() > deadline {
            break hits;
        }
    };
    assert_eq!(charged.len(), 1, "{seen:?}");
    assert_eq!(charged[0].payload["orderId"], "A-1");
}

#[tokio::test]
async fn fork_and_for_each_combine() {
    let Some(client) = client().await else { return };
    let wf = unique("fan");
    let spec = FlowSpec::define(&wf, 1)
        .fork(vec![
            Branch::of("left", |s| s.step("l")),
            Branch::of("right", |s| s.step("r")),
        ])
        .combine("merge")
        .for_each("xs", |s| s.step("double"))
        .combine("sum")
        .build()
        .unwrap();
    client.register(&spec).await.unwrap();

    let worker = Worker::new(client.clone(), "rs-it-fan")
        .options(opts())
        .handle(&wf, "l", |_: Value| async move { Ok(json!({"side": "L"})) })
        .handle(&wf, "r", |_: Value| async move { Ok(json!({"side": "R"})) })
        .combine(&wf, "merge", |c: Combine| async move {
            let mut out = c.base().clone();
            out["sides"] = json!([c.arm("left").unwrap()["side"], c.arm("right").unwrap()["side"]]);
            Ok(out)
        })
        .handle(&wf, "double", |x: i64| async move {
            assert!(Step::base().unwrap()["xs"].is_array());
            Ok(x * 2)
        })
        .combine(&wf, "sum", |c: Combine| async move {
            let items: Vec<i64> = c.items_as()?;
            let mut out = c.base().clone();
            out["total"] = json!(items.iter().sum::<i64>());
            Ok(out)
        })
        .start()
        .await
        .unwrap();

    let id = client.start(&spec, &json!({"xs": [1, 2, 3]})).await.unwrap();
    let view = client.await_completion(&id, Duration::from_secs(30)).await.unwrap();
    worker.close().await;

    assert_eq!(view.status, "COMPLETED", "{view:?}");
    assert_eq!(view.context["sides"], json!(["L", "R"]));
    assert_eq!(view.context["total"], 12);
}

#[tokio::test]
async fn failure_runs_compensators() {
    let Some(client) = client().await else { return };
    let wf = unique("saga");
    let spec = FlowSpec::define(&wf, 1)
        .step("reserve")
        .compensate()
        .step("charge")
        .retry(RetryPolicy::none())
        .build()
        .unwrap();
    client.register(&spec).await.unwrap();

    let released = Arc::new(AtomicU32::new(0));
    let r = released.clone();
    let worker = Worker::new(client.clone(), "rs-it-saga")
        .options(opts())
        .handle(&wf, "reserve", |mut v: Value| async move {
            v["reservation"] = json!("R-7");
            Ok(v)
        })
        .compensation(&wf, "reserve", move |c| {
            let r = r.clone();
            async move {
                assert_eq!(c.result["reservation"], "R-7");
                r.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .handle(&wf, "charge", |_: Value| async move { Err::<Value, _>(StepError::permanent("card declined")) })
        .start()
        .await
        .unwrap();

    let id = client.start(&spec, &json!({})).await.unwrap();
    let view = client.await_completion(&id, Duration::from_secs(30)).await.unwrap();
    worker.close().await;

    assert_eq!(view.status, "COMPENSATED", "{view:?}");
    assert_eq!(released.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn worker_refuses_mismatched_bindings() {
    let Some(client) = client().await else { return };
    let wf = unique("strict");
    let spec = FlowSpec::define(&wf, 1).gate("ok").build().unwrap();
    client.register(&spec).await.unwrap();

    let err = Worker::new(client.clone(), "rs-it-strict")
        .handle(&wf, "ok", |v: Value| async move { Ok(v) })
        .start()
        .await
        .err()
        .expect("a task bound to a gate must be refused");
    assert!(err.to_string().contains("use gate()"), "{err}");

    let err = Worker::new(client, "rs-it-strict")
        .handle("rs-no-such-workflow", "x", |v: Value| async move { Ok(v) })
        .start()
        .await
        .err()
        .expect("an unregistered workflow must be refused");
    assert!(err.to_string().contains("is not registered"), "{err}");
}

mod typed {
    use super::*;
    use wiggle::Wiggle;

    #[derive(Debug, Serialize, Deserialize)]
    struct Trip {
        id: String,
        vip: bool,
        nights: Vec<u32>,
    }
    #[derive(Serialize, Deserialize)]
    struct Air {
        seat: String,
    }
    #[derive(Serialize, Deserialize)]
    struct Hotel {
        room: String,
    }

    type R<T> = Result<T, StepError>;

    async fn prep(t: Trip) -> R<Trip> {
        Ok(t)
    }
    async fn book_air(t: Trip) -> R<Air> {
        Ok(Air { seat: format!("{}-1A", t.id) })
    }
    async fn book_hotel(t: Trip) -> R<Hotel> {
        Ok(Hotel { room: format!("{}-101", t.id) })
    }
    async fn merge(a: Air, h: Hotel) -> R<Value> {
        // The pre-fork context is the combine's base.
        let mut base = Step::base().expect("a combine has a base");
        base["seat"] = json!(a.seat);
        base["room"] = json!(h.room);
        Ok(base)
    }
    async fn rate(n: u32) -> R<u32> {
        Ok(n * 100)
    }
    async fn bill(rates: Vec<u32>) -> R<Value> {
        let mut base = Step::base().expect("a combine has a base");
        base["bill"] = json!(rates.iter().sum::<u32>());
        Ok(base)
    }
    async fn is_vip(t: Value) -> R<bool> {
        Ok(t["vip"] == true)
    }
    async fn concierge(mut t: Value) -> R<Value> {
        t["service"] = json!("concierge");
        Ok(t)
    }
    async fn self_service(mut t: Value) -> R<Value> {
        t["service"] = json!("self");
        Ok(t)
    }

    #[tokio::test]
    async fn typed_flow_binds_its_own_handlers() {
        let Some(client) = client().await else { return };
        let wf = unique("trip");
        let flow = Wiggle::define(&wf, 1, |f| {
            let prepped = f.then_apply(prep);
            let air = prepped.then_apply(book_air);
            let hotel = prepped.then_apply(book_hotel);
            let joined = Wiggle::all_of((air, hotel))
                .combine(merge)
                .then_for_each("nights", |night| night.then_apply(rate))
                .combine(bill);
            let vip = joined.when(is_vip).then_apply(concierge);
            let rest = joined.otherwise().then_apply(self_service);
            Wiggle::one_of(vec![vip, rest])
        })
        .unwrap();
        client.register(&flow).await.unwrap();

        let worker = Worker::new(client.clone(), "rs-it-typed").options(opts()).flow(&flow).start().await.unwrap();
        let a = client.start(&flow, &Trip { id: "T1".into(), vip: true, nights: vec![1, 2] }).await.unwrap();
        let b = client.start(&flow, &Trip { id: "T2".into(), vip: false, nights: vec![3] }).await.unwrap();
        let a = client.await_completion(&a, Duration::from_secs(30)).await.unwrap();
        let b = client.await_completion(&b, Duration::from_secs(30)).await.unwrap();
        worker.close().await;

        assert_eq!(a.status, "COMPLETED", "{a:?}");
        assert_eq!(a.context["seat"], "T1-1A");
        assert_eq!(a.context["room"], "T1-101");
        assert_eq!(a.context["bill"], 300);
        assert_eq!(a.context["service"], "concierge");
        assert_eq!(b.status, "COMPLETED", "{b:?}");
        assert_eq!(b.context["bill"], 300);
        assert_eq!(b.context["service"], "self");
    }
}
