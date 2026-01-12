#![coverage(off)]

use ordered_float::OrderedFloat;
use yachtsql_common::error::Result;
use yachtsql_common::types::Value;

pub fn fn_generate_uuid(_args: &[Value]) -> Result<Value> {
    Ok(Value::String(uuid::Uuid::new_v4().to_string()))
}

pub fn fn_rand(_args: &[Value]) -> Result<Value> {
    use rand::Rng;
    let random: f64 = rand::thread_rng().gen();
    Ok(Value::Float64(OrderedFloat(random)))
}

pub fn fn_rand_canonical(_args: &[Value]) -> Result<Value> {
    use rand::Rng;
    let random: f64 = rand::thread_rng().gen();
    Ok(Value::Float64(OrderedFloat(random)))
}
