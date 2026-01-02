#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_coalesce(args: &[Value]) -> Result<Value> {
    for arg in args {
        if !arg.is_null() {
            return Ok(arg.clone());
        }
    }
    Ok(Value::Null)
}

pub fn fn_ifnull(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery("IFNULL requires 2 arguments".into()));
    }
    if args[0].is_null() {
        Ok(args[1].clone())
    } else {
        Ok(args[0].clone())
    }
}

pub fn fn_nullif(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery("NULLIF requires 2 arguments".into()));
    }
    if values_equal(&args[0], &args[1]) {
        Ok(Value::Null)
    } else {
        Ok(args[0].clone())
    }
}

pub fn fn_if(args: &[Value]) -> Result<Value> {
    if args.len() < 3 {
        return Err(Error::InvalidQuery("IF requires 3 arguments".into()));
    }
    match &args[0] {
        Value::Bool(true) => Ok(args[1].clone()),
        Value::Bool(false) | Value::Null => Ok(args[2].clone()),
        _ => Err(Error::InvalidQuery("IF requires boolean condition".into())),
    }
}

pub fn fn_zeroifnull(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) | None => Ok(Value::Int64(0)),
        Some(v) => Ok(v.clone()),
    }
}

pub fn fn_nvl(args: &[Value]) -> Result<Value> {
    fn_ifnull(args)
}

pub fn fn_nvl2(args: &[Value]) -> Result<Value> {
    if args.len() < 3 {
        return Err(Error::InvalidQuery("NVL2 requires 3 arguments".into()));
    }
    if args[0].is_null() {
        Ok(args[2].clone())
    } else {
        Ok(args[1].clone())
    }
}

fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => false,
        (Value::Int64(a), Value::Float64(b)) => (*a as f64 - b.0).abs() < f64::EPSILON,
        (Value::Float64(a), Value::Int64(b)) => (a.0 - *b as f64).abs() < f64::EPSILON,
        _ => left == right,
    }
}
