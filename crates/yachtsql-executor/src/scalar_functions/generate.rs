#![coverage(off)]

use ordered_float::OrderedFloat;
use yachtsql_common::error::Result;
use yachtsql_common::types::Value;

pub fn fn_generate_uuid(_args: &[Value]) -> Result<Value> {
    Ok(Value::String(uuid::Uuid::new_v4().to_string()))
}

pub fn fn_rand(_args: &[Value]) -> Result<Value> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let random = (seed as f64 / u64::MAX as f64).fract();
    Ok(Value::Float64(OrderedFloat(random)))
}

pub fn fn_rand_canonical(_args: &[Value]) -> Result<Value> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let x = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    let random = (x as f64) / (u64::MAX as f64);
    Ok(Value::Float64(OrderedFloat(random)))
}
