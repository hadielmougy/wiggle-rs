//! Offline: the typed flow API compiles to the same topology as the builder, and refuses the same
//! mistakes the Java `Wiggle` API refuses.

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use wiggle::{FlowSpec, RetryPolicy, StepError, Wiggle};

#[derive(Serialize, Deserialize)]
struct Trip {
    id: String,
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
async fn book_air(_: Trip) -> R<Air> {
    Ok(Air { seat: "1A".into() })
}
async fn book_hotel(_: Trip) -> R<Hotel> {
    Ok(Hotel { room: "101".into() })
}
async fn merge(a: Air, h: Hotel) -> R<Value> {
    Ok(json!({"seat": a.seat, "room": h.room}))
}
async fn book(v: Value) -> R<Value> {
    Ok(v)
}
async fn is_vip(_: Trip) -> R<bool> {
    Ok(true)
}
async fn has_room(h: Hotel) -> R<bool> {
    Ok(!h.room.is_empty())
}
async fn vip_path(t: Trip) -> R<Trip> {
    Ok(t)
}
async fn standard_path(t: Trip) -> R<Trip> {
    Ok(t)
}
async fn notify(_: Trip) -> R<()> {
    Ok(())
}
async fn release(_: wiggle::Compensation) -> R<()> {
    Ok(())
}

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
fn all_of_compiles_to_a_fork_rejoined_at_its_combine() {
    let flow = Wiggle::define("trip", 1, |f| {
        let prepped = f.then_apply(prep);
        // continuing `prepped` twice is the fan-out
        let air = prepped.then_apply(book_air);
        let hotel = prepped.then_apply(book_hotel);
        Wiggle::all_of((air, hotel)).combine(merge).then_apply(book)
    })
    .unwrap();
    let ns = nodes(&flow);
    let fork = ns.values().find(|n| n["kind"] == "FORK").unwrap();
    let join = ns.values().find(|n| n["kind"] == "JOIN").unwrap();
    let (air, hotel) = (by_name(&ns, "book_air"), by_name(&ns, "book_hotel"));
    assert_eq!(by_name(&ns, "prep")["next"], fork["id"]);
    assert_eq!(fork["branches"], json!([air["id"], hotel["id"]]));
    assert_eq!(air["next"], join["id"]);
    assert_eq!(hotel["next"], join["id"]);
    assert_eq!(join["expected"], 2);
    let m = by_name(&ns, "merge");
    assert_eq!(join["next"], m["id"]);
    // Arms are named after their last step: the keys the engine stages each result under.
    assert_eq!(m["armNames"], json!(["book_air", "book_hotel"]));
    assert_eq!(m["next"], by_name(&ns, "book")["id"]);

    // The flow carries a handler for every step, under the step's name.
    let mut steps = flow.steps();
    steps.sort();
    assert_eq!(steps, ["book", "book_air", "book_hotel", "merge", "prep"]);
}

#[test]
fn same_definition_as_the_builder() {
    let typed = Wiggle::define("trip", 1, |f| {
        let prepped = f.then_apply(prep).queue("q1").retry(RetryPolicy::none());
        let air = prepped.then_apply(book_air);
        let hotel = prepped.then_apply(book_hotel).then_sleep(Duration::from_secs(1)).then_filter(has_room);
        Wiggle::all_of((air, hotel)).combine(merge).then_apply(book)
    })
    .unwrap();
    let built = FlowSpec::define("trip", 1)
        .step("prep")
        .queue("q1")
        .retry(RetryPolicy::none())
        .fork(vec![
            wiggle::Branch::of("book_air", |s| s.step("book_air")),
            wiggle::Branch::of("has_room", |s| s.step("book_hotel").sleep(Duration::from_secs(1)).gate("has_room")),
        ])
        .combine("merge")
        .step("book")
        .build()
        .unwrap();
    assert_eq!(typed.definition(), built.definition());
}

#[test]
fn one_of_compiles_to_a_choice() {
    let flow = Wiggle::define("route", 1, |f| {
        let vip = f.when(is_vip).then_apply(vip_path);
        let standard = f.otherwise().then_apply(standard_path);
        Wiggle::one_of(vec![vip, standard]).then_accept(notify)
    })
    .unwrap();
    let ns = nodes(&flow);
    let guard = by_name(&ns, "is_vip");
    assert_eq!(guard["kind"], "PREDICATE");
    assert_eq!(flow.definition()["startNode"], guard["id"]);
    assert_eq!(guard["next"], by_name(&ns, "vip_path")["id"]);
    assert_eq!(guard["altNext"], by_name(&ns, "standard_path")["id"]);
    let notify = by_name(&ns, "notify")["id"].clone();
    assert_eq!(by_name(&ns, "vip_path")["next"], notify);
    assert_eq!(by_name(&ns, "standard_path")["next"], notify);
}

#[test]
fn a_single_guarded_arm_is_skipped_when_it_does_not_hold() {
    let flow = Wiggle::define("route", 1, |f| {
        let vip = f.when(is_vip).then_apply(vip_path);
        Wiggle::one_of(vec![vip]).then_accept(notify)
    })
    .unwrap();
    let ns = nodes(&flow);
    assert_eq!(by_name(&ns, "is_vip")["altNext"], by_name(&ns, "notify")["id"]);
}

#[test]
fn for_each_loops_and_compensation() {
    async fn price(x: f64) -> R<f64> {
        Ok(x * 1.2)
    }
    async fn total(xs: Vec<f64>) -> R<Value> {
        Ok(json!({"total": xs.iter().sum::<f64>()}))
    }
    async fn more(_: Value) -> R<bool> {
        Ok(false)
    }
    async fn fetch(v: Value) -> R<Value> {
        Ok(v)
    }
    async fn reserve(v: Value) -> R<Value> {
        Ok(v)
    }
    let flow = Wiggle::define("basket", 1, |f: wiggle::WiggleFlow<Value>| {
        f.then_apply_compensable(reserve, release)
            .then_for_each("lines", |item| item.then_apply(price))
            .combine(total)
            .repeat_while_max(more, 5, |b| b.then_apply(fetch))
    })
    .unwrap();
    let ns = nodes(&flow);
    assert_eq!(by_name(&ns, "reserve")["compensable"], true);
    assert_eq!(by_name(&ns, "lines")["kind"], "DYN_FORK");
    assert_eq!(by_name(&ns, "total")["collectKey"], "__forEach__lines");
    assert_eq!(by_name(&ns, "more")["loopBudget"], 5);
    assert_eq!(by_name(&ns, "more")["next"], by_name(&ns, "fetch")["id"]);
    assert!(flow.steps().contains(&"price".to_string()));
}

#[test]
fn closures_take_their_name_from_named() {
    let flow = Wiggle::define("c", 1, |f| {
        f.then_filter(|t: Trip| async move { Ok(!t.id.is_empty()) }).named("in-stock")
    })
    .unwrap();
    assert_eq!(by_name(&nodes(&flow), "in-stock")["kind"], "PREDICATE");

    let err = Wiggle::define("c", 1, |f| f.then_apply(|t: Trip| async move { Ok(t) })).unwrap_err();
    assert!(err.to_string().contains("named("), "{err}");
}

#[test]
fn a_split_that_is_never_combined_is_rejected_and_names_both_ends() {
    let err = Wiggle::define("split", 1, |f| {
        let start = f.then_apply(prep);
        start.then_apply(vip_path);
        start.then_apply(standard_path)
    })
    .unwrap_err()
    .to_string();
    assert!(err.contains("never rejoined"), "{err}");
    assert!(err.contains("vip_path") && err.contains("standard_path"), "{err}");
    assert!(err.contains("all_of"), "{err}");
}

#[test]
fn a_fan_out_with_no_combine_is_rejected() {
    let err = Wiggle::define("nocombine", 1, |f| {
        let p = f.then_apply(prep);
        let _ = Wiggle::all_of((p.then_apply(book_air), p.then_apply(book_hotel)));
        p
    })
    .unwrap_err()
    .to_string();
    assert!(err.contains("combine"), "{err}");
}

#[test]
fn arms_must_be_distinguishable() {
    let err = Wiggle::define("sleepy", 1, |f| {
        let p = f.then_apply(prep);
        let a = p.then_apply(vip_path).then_sleep(Duration::from_secs(1));
        let b = p.then_apply(standard_path).then_sleep(Duration::from_secs(1));
        Wiggle::all_of((a, b)).combine(merge2)
    })
    .unwrap_err()
    .to_string();
    assert!(err.contains("distinguishable") && err.contains("then_sleep_named"), "{err}");

    async fn merge2(_: Trip, _: Trip) -> R<Value> {
        Ok(json!({}))
    }
}

#[test]
fn arms_must_be_siblings_and_from_one_definition() {
    let err = Wiggle::define("anc", 1, |f| {
        let p = f.then_apply(prep);
        let deeper = p.then_apply(vip_path);
        Wiggle::all_of((p, deeper)).combine(merge2)
    })
    .unwrap_err()
    .to_string();
    assert!(err.contains("siblings"), "{err}");

    let mut stray = None;
    let _ = Wiggle::define("one", 1, |f: wiggle::WiggleFlow<Trip>| {
        stray = Some(f.then_apply(vip_path));
        f.then_apply(prep)
    });
    let err = Wiggle::define("two", 1, |f| {
        let mine = f.then_apply(standard_path);
        Wiggle::all_of((mine, stray.take().unwrap())).combine(merge2)
    })
    .unwrap_err()
    .to_string();
    assert!(err.contains("different definitions"), "{err}");

    async fn merge2(_: Trip, _: Trip) -> R<Value> {
        Ok(json!({}))
    }
}

#[test]
fn one_of_arms_must_open_with_a_guard() {
    let err = Wiggle::define("noguard", 1, |f| {
        let a = f.when(is_vip).then_apply(vip_path);
        let b = f.then_apply(standard_path);
        Wiggle::one_of(vec![a, b])
    })
    .unwrap_err()
    .to_string();
    assert!(err.contains("must open with when"), "{err}");

    let err = Wiggle::define("late", 1, |f| {
        let a = f.otherwise().then_apply(standard_path);
        let b = f.when(is_vip).then_apply(vip_path);
        Wiggle::one_of(vec![a, b])
    })
    .unwrap_err()
    .to_string();
    assert!(err.contains("otherwise case must be last"), "{err}");
}

#[test]
fn referencing_one_handler_twice_is_a_duplicate_step() {
    let err = Wiggle::define("dup", 1, |f| f.then_apply(prep).then_apply(prep)).unwrap_err().to_string();
    assert!(err.contains("duplicate step name 'prep'"), "{err}");
}
