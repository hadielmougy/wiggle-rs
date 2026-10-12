//! The order-fulfilment walkthrough: define a topology, bind handlers by name, run one instance.
//!
//!     WIGGLE_URL=localhost:8080 cargo run --example order

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use wiggle::{Branch, Client, Combine, GraphBuilder, RetryPolicy, Step, Worker};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Order {
    order_id: String,
    quantity: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    status: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let url = std::env::var("WIGGLE_URL").unwrap_or_else(|_| "localhost:8080".into());
    let client = Client::connect(&url).await?;

    // The topology: pure data, no step logic.
    let order = GraphBuilder::define("rs-order-fulfilment", 1)
        .step("validate")
        .gate("in-stock")
        .fork(vec![
            Branch::of("payment", |s| s.step("charge").retry(RetryPolicy::exponential(5, Duration::from_millis(100)))),
            Branch::of("shipping", |s| s.step("reserve").step("label")),
        ])
        .combine("settle")
        .effect("notify")
        .build()?;
    client.register(&order).await?;

    // The logic: handlers bound by workflow + step name.
    let wf = order.name();
    let worker = Worker::new(client.clone(), "rs-example")
        .handle(wf, "validate", |mut o: Order| async move {
            o.status = Some("VALIDATED".into());
            Ok(o)
        })
        .gate(wf, "in-stock", |o: Order| async move { Ok(o.quantity > 0) })
        .handle(wf, "charge", |o: Order| async move {
            Step::emit("order.charged", json!({"orderId": o.order_id}))?;
            Ok(json!({"paymentRef": format!("auth-{}", o.order_id)}))
        })
        .handle(wf, "reserve", |o: Order| async move { Ok(json!({"orderId": o.order_id, "slot": "B-12"})) })
        .handle(wf, "label", |v: serde_json::Value| async move {
            Ok(json!({"tracking": format!("TRK-{}", v["slot"].as_str().unwrap_or("?"))}))
        })
        // A fork rejoins at an explicit combine that returns the complete post-join context.
        .combine(wf, "settle", |c: Combine| async move {
            let mut out = c.base().clone();
            out["paymentRef"] = c.arm("payment").cloned().unwrap_or_default()["paymentRef"].clone();
            out["tracking"] = c.arm("shipping").cloned().unwrap_or_default()["tracking"].clone();
            out["status"] = json!("SETTLED");
            Ok(out)
        })
        .effect(wf, "notify", |o: Order| async move {
            println!("shipped {} (attempt {})", o.order_id, Step::attempt());
            Ok(())
        })
        .start()
        .await?;

    let id = client.start(&order, &Order { order_id: "A-1".into(), quantity: 3, status: None }).await?;
    let view = client.await_completion(&id, Duration::from_secs(30)).await?;
    println!("{} {}", view.status, view.context);

    worker.close().await;
    Ok(())
}
