//! Conversions between `serde_json::Value` and the protobuf `Value`/`Struct` the wire carries.

use prost_types::{value::Kind, ListValue, Struct, Value};
use serde_json::{Map, Number, Value as Json};

/// The largest integer an f64 holds exactly; whole numbers within it come back as JSON integers.
const MAX_SAFE_INT: f64 = 9_007_199_254_740_992.0;

pub(crate) fn to_value(v: &Json) -> Value {
    let kind = match v {
        Json::Null => Kind::NullValue(0),
        Json::Bool(b) => Kind::BoolValue(*b),
        Json::Number(n) => Kind::NumberValue(n.as_f64().unwrap_or(0.0)),
        Json::String(s) => Kind::StringValue(s.clone()),
        Json::Array(a) => Kind::ListValue(ListValue { values: a.iter().map(to_value).collect() }),
        Json::Object(m) => Kind::StructValue(to_struct(m)),
    };
    Value { kind: Some(kind) }
}

pub(crate) fn to_struct(m: &Map<String, Json>) -> Struct {
    Struct { fields: m.iter().map(|(k, v)| (k.clone(), to_value(v))).collect() }
}

/// The protobuf number is a double; a whole number within the exactly-representable range comes
/// back as a JSON integer, so it decodes into integer fields (`u32`, `i64`, ...).
pub(crate) fn from_value(v: &Value) -> Json {
    match &v.kind {
        None | Some(Kind::NullValue(_)) => Json::Null,
        Some(Kind::BoolValue(b)) => Json::Bool(*b),
        Some(Kind::NumberValue(n)) => number(*n),
        Some(Kind::StringValue(s)) => Json::String(s.clone()),
        Some(Kind::ListValue(l)) => Json::Array(l.values.iter().map(from_value).collect()),
        Some(Kind::StructValue(s)) => from_struct(s),
    }
}

pub(crate) fn from_struct(s: &Struct) -> Json {
    Json::Object(s.fields.iter().map(|(k, v)| (k.clone(), from_value(v))).collect())
}

pub(crate) fn opt_value(v: Option<&Value>) -> Json {
    v.map(from_value).unwrap_or(Json::Null)
}

fn number(n: f64) -> Json {
    if n.fract() == 0.0 && n.abs() < MAX_SAFE_INT {
        Json::Number(Number::from(n as i64))
    } else {
        Number::from_f64(n).map(Json::Number).unwrap_or(Json::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trips_json() {
        let v = json!({"a": 1, "b": [true, null, "x", 2.5], "c": {"d": -3}});
        assert_eq!(from_value(&to_value(&v)), v);
    }
}
