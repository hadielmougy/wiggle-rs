//! Offline: the compiled graph's shape, node for node, as the server receives it.

use std::collections::HashMap;
use std::time::Duration;

use serde_json::{json, Value};
use wiggle::{Branch, Case, FlowSpec, GraphBuilder, RetryPolicy};

fn nodes(spec: &FlowSpec) -> HashMap<String, Value> {
    spec.definition()["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| (n["id"].as_str().unwrap().to_string(), n.clone()))
        .collect()
}

fn by_name<'a>(nodes: &'a HashMap<String, Value>, name: &str) -> &'a Value {
    nodes.values().find(|n| n["name"] == name).unwrap_or_else(|| panic!("no node {name}"))
}


#[test]
fn linear_chain_with_gate() {
    let spec = GraphBuilder::define("order", 1)
        .step("validate")
        .gate("in-stock")
        .step("charge")
        .queue("payments")
        .retry(RetryPolicy::exponential(5, Duration::from_millis(100)))
        .build()
        .unwrap();
    let def = spec.definition();
    assert_eq!(def["name"], "order");
    assert_eq!(def["version"], 1);
    assert_eq!(def["executionMode"], "SERVER");
    assert_eq!(def["queues"], json!(["order", "payments"]));

    let ns = nodes(&spec);
    let validate = by_name(&ns, "validate");
    let gate = by_name(&ns, "in-stock");
    let charge = by_name(&ns, "charge");
    assert_eq!(def["startNode"], validate["id"]);
    assert_eq!(validate["kind"], "TASK");
    assert_eq!(validate["activity"], "order#validate");
    assert_eq!(validate["retry"]["maxAttempts"], 10_000);
    assert_eq!(validate["next"], gate["id"]);
    assert_eq!(gate["kind"], "PREDICATE");
    assert_eq!(gate["next"], charge["id"]);
    let gated = &ns[gate["altNext"].as_str().unwrap()];
    assert_eq!(gated["kind"], "END");
    assert_eq!(gated["reason"], "gated:in-stock");
    assert_eq!(charge["queue"], "payments");
    assert_eq!(charge["retry"]["maxAttempts"], 5);
    assert_eq!(charge["retry"]["multiplier"], 2.0);
    assert_eq!(ns[charge["next"].as_str().unwrap()]["kind"], "END");
}

#[test]
fn fork_rejoins_at_its_combine() {
    let spec = GraphBuilder::define("order", 1)
        .step("validate")
        .fork(vec![
            Branch::of("payment", |s| s.step("charge").gate("approved")),
            Branch::of("shipping", |s| s.step("reserve").sleep(Duration::from_secs(2)).step("label")),
        ])
        .combine("settle")
        .step("notify")
        .build()
        .unwrap();
    let ns = nodes(&spec);
    let fork = &ns[by_name(&ns, "validate")["next"].as_str().unwrap()];
    assert_eq!(fork["kind"], "FORK");
    let charge = by_name(&ns, "charge");
    let reserve = by_name(&ns, "reserve");
    assert_eq!(fork["branches"], json!([charge["id"], reserve["id"]]));

    let join_id = by_name(&ns, "label")["next"].as_str().unwrap();
    let join = &ns[join_id];
    assert_eq!(join["kind"], "JOIN");
    assert_eq!(join["expected"], 2);
    // A gate inside a branch short-circuits to the fork's join.
    assert_eq!(by_name(&ns, "approved")["altNext"], join_id);
    assert_eq!(by_name(&ns, "approved")["next"], join_id);

    let settle = by_name(&ns, "settle");
    assert_eq!(join["next"], settle["id"]);
    assert_eq!(settle["kind"], "TASK");
    assert_eq!(settle["armNames"], json!(["payment", "shipping"]));
    assert_eq!(settle["next"], by_name(&ns, "notify")["id"]);
}

#[test]
fn for_each_collects_at_its_combine() {
    let spec = GraphBuilder::define("batch", 1)
        .for_each("lines", |s| s.step("price"))
        .combine("total")
        .build()
        .unwrap();
    let ns = nodes(&spec);
    let dyn_fork = by_name(&ns, "lines");
    assert_eq!(dyn_fork["kind"], "DYN_FORK");
    assert_eq!(dyn_fork["itemsKey"], "lines");
    assert_eq!(spec.definition()["startNode"], dyn_fork["id"]);
    let price = by_name(&ns, "price");
    assert_eq!(dyn_fork["branches"], json!([price["id"]]));
    let join = &ns[price["next"].as_str().unwrap()];
    assert_eq!(dyn_fork["next"], join["id"]); // the empty-collection skip
    let total = by_name(&ns, "total");
    assert_eq!(join["next"], total["id"]);
    assert_eq!(total["collectKey"], "__forEach__lines");
}

#[test]
fn choose_chains_guards_and_otherwise() {
    let spec = GraphBuilder::define("greet", 1)
        .choose(vec![
            Case::when("vip", |s| s.step("concierge")),
            Case::when("regular", |s| s.step("thanks")),
            Case::otherwise(|s| s.step("welcome")),
        ])
        .step("done")
        .build()
        .unwrap();
    let ns = nodes(&spec);
    let (vip, regular) = (by_name(&ns, "vip"), by_name(&ns, "regular"));
    assert_eq!(spec.definition()["startNode"], vip["id"]);
    assert_eq!(vip["next"], by_name(&ns, "concierge")["id"]);
    assert_eq!(vip["altNext"], regular["id"]);
    assert_eq!(regular["next"], by_name(&ns, "thanks")["id"]);
    assert_eq!(regular["altNext"], by_name(&ns, "welcome")["id"]);
    let done = by_name(&ns, "done")["id"].clone();
    for arm in ["concierge", "thanks", "welcome"] {
        assert_eq!(by_name(&ns, arm)["next"], done, "{arm}");
    }
}

#[test]
fn choose_without_otherwise_falls_through() {
    let spec = GraphBuilder::define("greet", 1)
        .choose(vec![Case::when("vip", |s| s.step("concierge"))])
        .step("done")
        .build()
        .unwrap();
    let ns = nodes(&spec);
    let done = by_name(&ns, "done")["id"].clone();
    assert_eq!(by_name(&ns, "vip")["altNext"], done);
    assert_eq!(by_name(&ns, "concierge")["next"], done);
}

#[test]
fn do_while_loops_back_through_its_condition() {
    let spec = GraphBuilder::define("poll", 1)
        .do_while_max("again", 50, |s| s.step("check").sleep(Duration::from_secs(5)))
        .step("after")
        .build()
        .unwrap();
    let ns = nodes(&spec);
    let check = by_name(&ns, "check");
    let cond = by_name(&ns, "again");
    assert_eq!(spec.definition()["startNode"], check["id"]);
    assert_eq!(cond["kind"], "PREDICATE");
    assert_eq!(cond["loopBudget"], 50);
    assert_eq!(cond["next"], check["id"]);
    assert_eq!(cond["altNext"], by_name(&ns, "after")["id"]);
    let sleep = &ns[check["next"].as_str().unwrap()];
    assert_eq!(sleep["kind"], "SLEEP");
    assert_eq!(sleep["sleepMillis"], 5000);
    assert_eq!(sleep["next"], cond["id"]);

    let default_budget = GraphBuilder::define("p", 1).do_while("c", |s| s.step("x")).build().unwrap();
    assert_eq!(by_name(&nodes(&default_budget), "c")["loopBudget"], -1);
}

#[test]
fn signal_escalation_rejoins() {
    let spec = GraphBuilder::define("approve", 1)
        .await_signal_or("approved", Duration::from_secs(60), |s| s.step("escalate"))
        .step("ship")
        .build()
        .unwrap();
    let ns = nodes(&spec);
    let signal = by_name(&ns, "approved");
    assert_eq!(signal["kind"], "SIGNAL");
    assert_eq!(signal["sleepMillis"], 60_000);
    assert_eq!(signal["altNext"], by_name(&ns, "escalate")["id"]);
    let ship = by_name(&ns, "ship")["id"].clone();
    assert_eq!(signal["next"], ship);
    assert_eq!(by_name(&ns, "escalate")["next"], ship);
}

#[test]
fn compensate_and_sub_workflow() {
    let spec = GraphBuilder::define("trip", 1)
        .step("book-hotel")
        .compensate()
        .sub_workflow("pay", "payment")
        .default_queue("travel")
        .build()
        .unwrap();
    let ns = nodes(&spec);
    let hotel = by_name(&ns, "book-hotel");
    assert_eq!(hotel["compensable"], true);
    assert_eq!(hotel["queue"], "travel");
    let sub = by_name(&ns, "pay");
    assert_eq!(sub["kind"], "SUB_WORKFLOW");
    assert_eq!(sub["activity"], "payment");
    assert_eq!(spec.queues(), ["travel"]);
}

#[test]
fn rejects_invalid_topologies() {
    let err = |r: wiggle::Result<FlowSpec>| r.unwrap_err().to_string();
    assert!(err(GraphBuilder::define("w", 0).step("a").build()).contains("positive version"));
    assert!(err(GraphBuilder::define("w", 1).build()).contains("defines no steps"));
    assert!(err(GraphBuilder::define("w", 1).step("a").step("a").build()).contains("duplicate step name 'a'"));
    assert!(err(GraphBuilder::define("w", 1).fork(vec![Branch::of("x", |s| s.step("a"))]).combine("m").build())
        .contains("at least two branches"));
    assert!(err(GraphBuilder::define("w", 1)
        .fork(vec![Branch::of("x", |s| s.step("a")), Branch::of("y", |s| s)])
        .combine("m")
        .build())
    .contains("branch 'y' defines no steps"));
    assert!(err(GraphBuilder::define("w", 1)
        .choose(vec![Case::otherwise(|s| s.step("a")), Case::when("g", |s| s.step("b"))])
        .build())
    .contains("otherwise case must be last"));
    assert!(err(GraphBuilder::define("w", 1).sleep(Duration::from_secs(1)).compensate().build())
        .contains("compensate() must directly follow"));
    assert!(err(GraphBuilder::define("w", 1).sleep(Duration::from_secs(1)).queue("q").build())
        .contains("queue() must directly follow"));
    assert!(err(GraphBuilder::define("w", 1)
        .fork(vec![Branch::of("x", |s| s.step("a").default_queue("q")), Branch::of("y", |s| s.step("b"))])
        .combine("m")
        .build())
    .contains("not a nested body"));
}
