//! Run-time branches: a step decides, while it runs, how many branches to open and which steps
//! each one chains -- Java's `Step.create(..).thenApply(..)`. The branches run in isolation and
//! rejoin at the spawn combine that follows the step.
//!
//!     WIGGLE_URL=localhost:8080 cargo run --example branches

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use wiggle::{BranchSteps, Client, Compensation, FlowSpec, Step, StepError, WiggleFlow, Worker};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Order {
    order_id: String,
    items: Vec<Item>,
}

/// One branch's context: the item it fulfils, filled in as the branch's steps run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Item {
    sku: String,
    digital: bool,
    #[serde(default)]
    reserved: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    via: Option<String>,
}

// The spawning step: one branch per item, its steps picked per item at run time.

async fn fulfil(order: Order) -> Result<Order, StepError> {
    for item in &order.items {
        if item.digital {
            Step::create(item.clone())?.then_apply(link);
        } else {
            Step::create(item.clone())?
                .then_apply_compensable(reserve) // undone by `release` if the instance later fails
                .then_filter(in_stock) // false ends only this branch; it rejoins as it is
                .then_apply(ship);
        }
    }
    Ok(order)
}

// The branch steps: named after their functions, bound with BranchSteps.

async fn link(mut i: Item) -> Result<Item, StepError> {
    i.via = Some(format!("https://dl.example/{}", i.sku));
    Ok(i)
}

async fn reserve(mut i: Item) -> Result<Item, StepError> {
    i.reserved = true;
    Ok(i)
}

async fn release(c: Compensation) -> Result<(), StepError> {
    let item: Item = c.result_as()?;
    println!("released {}", item.sku);
    Ok(())
}

async fn in_stock(i: Item) -> Result<bool, StepError> {
    Ok(!i.sku.ends_with("-gone"))
}

async fn ship(mut i: Item) -> Result<Item, StepError> {
    i.via = Some(format!("TRK-{}", i.sku));
    Ok(i)
}

/// The combine after `fulfil`: every branch's result in creation order. `Step::base` is what
/// `fulfil` returned, and the return is the complete next context.
async fn summarise(items: Vec<Item>) -> Result<serde_json::Value, StepError> {
    let order: Order = Step::base_as()?.ok_or_else(|| StepError::permanent("a combine has a base"))?;
    let lines: Vec<String> = items
        .iter()
        .map(|i| format!("{}: {}", i.sku, i.via.as_deref().unwrap_or("out of stock")))
        .collect();
    Ok(json!({"orderId": order.order_id, "lines": lines}))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let url = std::env::var("WIGGLE_URL").unwrap_or_else(|_| "localhost:8080".into());
    let client = Client::connect(&url).await?;

    // A combine directly after a step makes it a spawning step: the combine collects the branches
    // `fulfil` opens -- Java's `f.thenApply(s::fulfil).combine(s::summarise)`.
    let spec = FlowSpec::define("rs-order-branches", 1, |f: WiggleFlow<Order>| f.then_apply(fulfil).combine(summarise))?;
    client.register(&spec).await?;

    let worker = Worker::new(client.clone(), "rs-branches-example")
        .flow(&spec)
        // The branch steps are in no graph, so they are bound by function rather than reconciled.
        .branch_steps(
            spec.name(),
            BranchSteps::new()
                .handle(link)
                .handle(reserve)
                .compensation(reserve, release)
                .gate(in_stock)
                .handle(ship),
        )
        .start()
        .await?;

    let order = json!({
        "orderId": "B-1",
        "items": [
            {"sku": "book", "digital": false},
            {"sku": "song", "digital": true},
            {"sku": "chair-gone", "digital": false},
        ],
    });
    let id = client.start(&spec, &order).await?;
    let view = client.await_completion(&id, Duration::from_secs(30)).await?;
    println!("{} {}", view.status, view.context);

    worker.close().await;
    Ok(())
}
