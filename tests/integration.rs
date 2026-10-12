//! End-to-end against a running server; skipped unless WIGGLE_TEST_URL is set:
//!
//!     WIGGLE_TEST_URL=localhost:8080 cargo test --test integration

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use wiggle::{Branch, Client, Combine, FlowSpec, GraphBuilder, RetryPolicy, Step, StepError, Worker, WorkerOptions};

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
    let spec = GraphBuilder::define(&wf, 1)
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
    let spec = GraphBuilder::define(&wf, 1)
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
    let spec = GraphBuilder::define(&wf, 1)
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
    let spec = GraphBuilder::define(&wf, 1).gate("ok").build().unwrap();
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
        let flow = FlowSpec::define(&wf, 1, |f| {
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

mod branches {
    use super::*;
    use wiggle::BranchSteps;

    #[derive(Serialize, Deserialize)]
    struct Line {
        sku: String,
        qty: u32,
    }
    #[derive(Serialize, Deserialize)]
    struct Priced {
        sku: String,
        amount: u32,
    }

    type R<T> = Result<T, StepError>;

    static AUDITED: AtomicU32 = AtomicU32::new(0);

    async fn price(l: Line) -> R<Priced> {
        Ok(Priced { sku: l.sku, amount: l.qty * 10 })
    }
    async fn add_tax(mut p: Priced) -> R<Priced> {
        p.amount += 1;
        Ok(p)
    }
    async fn audit(_: Priced) -> R<()> {
        AUDITED.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    #[tokio::test]
    async fn steps_open_branches_that_rejoin_at_the_spawn_combine() {
        let Some(client) = client().await else { return };
        let wf = unique("branches");
        let spec = GraphBuilder::define(&wf, 1).step("split").spawn_combine("collect").build().unwrap();
        client.register(&spec).await.unwrap();

        let worker = Worker::new(client.clone(), "rs-it-branches")
            .options(opts())
            .handle(&wf, "split", |mut v: Value| async move {
                let lines: Vec<Line> = serde_json::from_value(v["lines"].take()).map_err(StepError::from)?;
                for line in lines {
                    Step::create(line)?.then_apply(price).then_apply(add_tax).then_accept(audit);
                }
                v["split"] = json!(true);
                Ok(v)
            })
            .combine(&wf, "collect", |c: Combine| async move {
                // The branches' results in creation order; the base is the spawning step's return.
                let priced: Vec<Priced> = c.items_as()?;
                let mut out = c.base().clone();
                out["skus"] = json!(priced.iter().map(|p| p.sku.as_str()).collect::<Vec<_>>());
                out["total"] = json!(priced.iter().map(|p| p.amount).sum::<u32>());
                Ok(out)
            })
            .branch_steps(&wf, BranchSteps::new().handle(price).handle(add_tax).effect(audit))
            .start()
            .await
            .unwrap();

        let lines = json!({"lines": [{"sku": "a", "qty": 1}, {"sku": "b", "qty": 2}, {"sku": "c", "qty": 3}]});
        let id = client.start(&spec, &lines).await.unwrap();
        let view = client.await_completion(&id, Duration::from_secs(30)).await.unwrap();
        worker.close().await;

        assert_eq!(view.status, "COMPLETED", "{view:?}");
        assert_eq!(view.context["split"], true);
        assert_eq!(view.context["skus"], json!(["a", "b", "c"]));
        assert_eq!(view.context["total"], 63);
        assert_eq!(AUDITED.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn keyed_branches_rejoin_as_a_map() {
        let Some(client) = client().await else { return };
        let wf = unique("keyed-branches");
        let spec = GraphBuilder::define(&wf, 1).step("split").spawn_combine("collect").build().unwrap();
        client.register(&spec).await.unwrap();

        let worker = Worker::new(client.clone(), "rs-it-keyed")
            .options(opts())
            .handle(&wf, "split", |v: Value| async move {
                for (sku, qty) in [("x", 4), ("y", 5)] {
                    Step::create_keyed(sku, Line { sku: sku.into(), qty })?.then_apply(price);
                }
                Ok(v)
            })
            .combine(&wf, "collect", |c: Combine| async move {
                let priced: std::collections::HashMap<String, Priced> = c.items_as()?;
                let mut out = c.base().clone();
                out["x"] = json!(priced["x"].amount);
                out["y"] = json!(priced["y"].amount);
                Ok(out)
            })
            .branch_steps(&wf, BranchSteps::new().handle(price))
            .start()
            .await
            .unwrap();

        let id = client.start(&spec, &json!({"order": "K-1"})).await.unwrap();
        let view = client.await_completion(&id, Duration::from_secs(30)).await.unwrap();
        worker.close().await;

        assert_eq!(view.status, "COMPLETED", "{view:?}");
        assert_eq!(view.context["order"], "K-1");
        assert_eq!(view.context["x"], 40);
        assert_eq!(view.context["y"], 50);
    }

    #[derive(Serialize, Deserialize)]
    struct Item {
        item: String,
        #[serde(default)]
        reserved: bool,
        #[serde(default)]
        via: Option<String>,
    }

    async fn reserve(mut i: Item) -> R<Item> {
        i.reserved = true;
        Ok(i)
    }
    async fn in_stock(i: Item) -> R<bool> {
        Ok(!i.item.contains("gone"))
    }
    async fn ship(mut i: Item) -> R<Item> {
        i.via = Some("ship".into());
        Ok(i)
    }

    #[tokio::test]
    async fn a_false_gate_ends_only_its_branch() {
        let Some(client) = client().await else { return };
        let wf = unique("gated-branches");
        let spec = GraphBuilder::define(&wf, 1).step("fulfil").spawn_combine("summarise").build().unwrap();
        client.register(&spec).await.unwrap();

        let worker = Worker::new(client.clone(), "rs-it-gated")
            .options(opts())
            .handle(&wf, "fulfil", |v: Value| async move {
                for item in ["book", "chair-gone", "lamp"] {
                    Step::create(Item { item: item.into(), reserved: false, via: None })?
                        .then_apply(reserve)
                        .then_filter(in_stock)
                        .then_apply(ship);
                }
                Ok(v)
            })
            .combine(&wf, "summarise", |c: Combine| async move {
                let items: Vec<Item> = c.items_as()?;
                let summary: Vec<String> = items
                    .iter()
                    .map(|i| format!("{}:{}:{}", i.item, i.reserved, i.via.as_deref().unwrap_or("null")))
                    .collect();
                Ok(json!({"summary": summary}))
            })
            .branch_steps(&wf, BranchSteps::new().handle(reserve).gate(in_stock).handle(ship))
            .start()
            .await
            .unwrap();

        let id = client.start(&spec, &json!({})).await.unwrap();
        let view = client.await_completion(&id, Duration::from_secs(30)).await.unwrap();
        worker.close().await;

        assert_eq!(view.status, "COMPLETED", "{view:?}");
        // The gated branch rejoins with its context as of the gate: reserved, never shipped.
        assert_eq!(view.context["summary"], json!(["book:true:ship", "chair-gone:true:null", "lamp:true:ship"]));
    }

    static UNDONE: AtomicU32 = AtomicU32::new(0);

    async fn charge(mut v: Value) -> R<Value> {
        v["charged"] = json!(true);
        Ok(v)
    }
    async fn refund(c: wiggle::Compensation) -> R<()> {
        assert_eq!(c.result_as::<Value>()?["charged"], true);
        UNDONE.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    #[tokio::test]
    async fn compensable_branch_steps_are_undone_when_the_instance_fails() {
        let Some(client) = client().await else { return };
        let wf = unique("branch-saga");
        let spec = GraphBuilder::define(&wf, 1)
            .step("fulfil")
            .spawn_combine("summarise")
            .step("boom")
            .retry(RetryPolicy::none())
            .build()
            .unwrap();
        client.register(&spec).await.unwrap();

        let worker = Worker::new(client.clone(), "rs-it-branch-saga")
            .options(opts())
            .handle(&wf, "fulfil", |v: Value| async move {
                for item in ["a", "b"] {
                    Step::create(json!({"item": item}))?.then_apply_compensable(charge);
                }
                Ok(v)
            })
            .combine(&wf, "summarise", |c: Combine| async move {
                let results: Vec<Value> = c.items_as()?;
                Ok(json!({"charged": results.len()}))
            })
            .handle(&wf, "boom", |_: Value| async move {
                Err::<Value, _>(StepError::permanent("deliberate: the charges must be undone"))
            })
            .branch_steps(&wf, BranchSteps::new().handle(charge).compensation(charge, refund))
            .start()
            .await
            .unwrap();

        let id = client.start(&spec, &json!({})).await.unwrap();
        let view = client.await_completion(&id, Duration::from_secs(30)).await.unwrap();
        worker.close().await;

        assert_eq!(view.status, "COMPENSATED", "{view:?}");
        assert_eq!(UNDONE.load(Ordering::SeqCst), 2, "each branch's charge was undone");
    }

    #[derive(Serialize, Deserialize)]
    struct Basket {
        id: String,
        lines: Vec<Line>,
    }

    async fn open_lines(b: Basket) -> R<Basket> {
        for line in &b.lines {
            Step::create(Line { sku: line.sku.clone(), qty: line.qty })?.then_apply(price).then_apply(add_tax);
        }
        Ok(b)
    }
    async fn total_lines(priced: Vec<Priced>) -> R<Value> {
        let base: Basket = Step::base_as()?.expect("a spawn combine has a base");
        Ok(json!({"id": base.id, "total": priced.iter().map(|p| p.amount).sum::<u32>()}))
    }

    #[tokio::test]
    async fn typed_combine_after_a_step_collects_its_branches() {
        let Some(client) = client().await else { return };
        let wf = unique("typed-branches");
        let flow = FlowSpec::define(&wf, 1, |f: wiggle::WiggleFlow<Basket>| f.then_apply(open_lines).combine(total_lines))
            .unwrap();
        client.register(&flow).await.unwrap();

        let worker = Worker::new(client.clone(), "rs-it-typed-branches")
            .options(opts())
            .flow(&flow)
            .branch_steps(&wf, BranchSteps::new().handle(price).handle(add_tax))
            .start()
            .await
            .unwrap();

        let basket = json!({"id": "K-9", "lines": [{"sku": "a", "qty": 1}, {"sku": "b", "qty": 4}]});
        let id = client.start(&flow, &basket).await.unwrap();
        let view = client.await_completion(&id, Duration::from_secs(30)).await.unwrap();
        worker.close().await;

        assert_eq!(view.status, "COMPLETED", "{view:?}");
        assert_eq!(view.context, json!({"id": "K-9", "total": 52}));
    }
}
