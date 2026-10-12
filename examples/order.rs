//! The order-fulfilment walkthrough: define a typed flow from the step functions, let the worker
//! bind them, run one instance.
//!
//!     WIGGLE_URL=localhost:8080 cargo run --example order

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use wiggle::{Client, FlowSpec, RetryPolicy, Step, StepError, Wiggle, WiggleFlow, Worker};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Order {
    order_id: String,
    quantity: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payment_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tracking: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Payment {
    payment_ref: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Reservation {
    order_id: String,
    slot: String,
}

#[derive(Serialize, Deserialize)]
struct Shipment {
    tracking: String,
}

// The steps: plain async functions. Each step's input is the previous step's output, decoded from
// the JSON context with serde.

async fn validate(mut o: Order) -> Result<Order, StepError> {
    o.status = Some("VALIDATED".into());
    Ok(o)
}

async fn in_stock(o: Order) -> Result<bool, StepError> {
    Ok(o.quantity > 0)
}

async fn charge(o: Order) -> Result<Payment, StepError> {
    Step::emit("order.charged", json!({"orderId": o.order_id}))?;
    Ok(Payment { payment_ref: format!("auth-{}", o.order_id) })
}

async fn reserve(o: Order) -> Result<Reservation, StepError> {
    Ok(Reservation { order_id: o.order_id, slot: "B-12".into() })
}

async fn label(r: Reservation) -> Result<Shipment, StepError> {
    Ok(Shipment { tracking: format!("TRK-{}", r.slot) })
}

/// The fork rejoins here: one parameter per arm, in arm order. The return is the complete
/// post-join context; the pre-fork order is the combine's base.
async fn settle(p: Payment, s: Shipment) -> Result<Order, StepError> {
    let mut o: Order = Step::base_as()?.ok_or_else(|| StepError::permanent("a combine has a base"))?;
    o.payment_ref = Some(p.payment_ref);
    o.tracking = Some(s.tracking);
    o.status = Some("SETTLED".into());
    Ok(o)
}

async fn notify(o: Order) -> Result<(), StepError> {
    println!("shipped {} (attempt {})", o.order_id, Step::attempt());
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let url = std::env::var("WIGGLE_URL").unwrap_or_else(|_| "localhost:8080".into());
    let client = Client::connect(&url).await?;

    // The workflow: a chain of the step functions, checked by the compiler. Continuing
    // `validated` twice is the fan-out; Wiggle::all_of rejoins it at the combine.
    let order = FlowSpec::define("rs-order-fulfilment", 2, |f: WiggleFlow<Order>| {
        let validated = f.then_apply(validate).then_filter(in_stock);

        let payment = validated.then_apply(charge).retry(RetryPolicy::exponential(5, Duration::from_millis(100)));
        let shipping = validated.then_apply(reserve).then_apply(label);

        Wiggle::all_of((payment, shipping)).combine(settle).then_accept(notify)
    })?;
    client.register(&order).await?;

    // The spec carries its step functions: the worker binds each under its step name.
    let worker = Worker::new(client.clone(), "rs-example").flow(&order).start().await?;

    let input = Order {
        order_id: "A-1".into(),
        quantity: 3,
        status: None,
        payment_ref: None,
        tracking: None,
    };
    let id = client.start(&order, &input).await?;
    let view = client.await_completion(&id, Duration::from_secs(30)).await?;
    println!("{} {}", view.status, view.context);

    worker.close().await;
    Ok(())
}
