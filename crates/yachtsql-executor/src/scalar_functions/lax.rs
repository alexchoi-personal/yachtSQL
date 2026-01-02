#![coverage(off)]

use ordered_float::OrderedFloat;
use yachtsql_common::error::Result;
use yachtsql_common::types::Value;

pub fn fn_lax_int64(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Int64(n) => Ok(Value::Int64(*n)),
        Value::Float64(f) => Ok(Value::Int64(f.0 as i64)),
        Value::Bool(b) => Ok(Value::Int64(if *b { 1 } else { 0 })),
        Value::String(s) => {
            if let Ok(n) = s.trim().parse::<i64>() {
                Ok(Value::Int64(n))
            } else {
                Ok(Value::Null)
            }
        }
        Value::Json(j) => {
            if let Some(n) = j.as_i64() {
                Ok(Value::Int64(n))
            } else if let Some(f) = j.as_f64() {
                Ok(Value::Int64(f as i64))
            } else if let Some(s) = j.as_str() {
                if let Ok(n) = s.trim().parse::<i64>() {
                    Ok(Value::Int64(n))
                } else {
                    Ok(Value::Null)
                }
            } else {
                Ok(Value::Null)
            }
        }
        _ => Ok(Value::Null),
    }
}

pub fn fn_lax_float64(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Float64(f) => Ok(Value::Float64(*f)),
        Value::Int64(n) => Ok(Value::Float64(OrderedFloat(*n as f64))),
        Value::Bool(b) => Ok(Value::Float64(OrderedFloat(if *b { 1.0 } else { 0.0 }))),
        Value::String(s) => {
            if let Ok(f) = s.trim().parse::<f64>() {
                Ok(Value::Float64(OrderedFloat(f)))
            } else {
                Ok(Value::Null)
            }
        }
        Value::Json(j) => {
            if let Some(f) = j.as_f64() {
                Ok(Value::Float64(OrderedFloat(f)))
            } else if let Some(s) = j.as_str() {
                if let Ok(f) = s.trim().parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(f)))
                } else {
                    Ok(Value::Null)
                }
            } else {
                Ok(Value::Null)
            }
        }
        _ => Ok(Value::Null),
    }
}

pub fn fn_lax_bool(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Bool(b) => Ok(Value::Bool(*b)),
        Value::Int64(n) => Ok(Value::Bool(*n != 0)),
        Value::Float64(f) => Ok(Value::Bool(f.0 != 0.0)),
        Value::String(s) => {
            let lower = s.trim().to_lowercase();
            if lower == "true" || lower == "1" {
                Ok(Value::Bool(true))
            } else if lower == "false" || lower == "0" {
                Ok(Value::Bool(false))
            } else {
                Ok(Value::Null)
            }
        }
        Value::Json(j) => {
            if let Some(b) = j.as_bool() {
                Ok(Value::Bool(b))
            } else if let Some(s) = j.as_str() {
                let lower = s.trim().to_lowercase();
                if lower == "true" || lower == "1" {
                    Ok(Value::Bool(true))
                } else if lower == "false" || lower == "0" {
                    Ok(Value::Bool(false))
                } else {
                    Ok(Value::Null)
                }
            } else if let Some(n) = j.as_i64() {
                Ok(Value::Bool(n != 0))
            } else if let Some(n) = j.as_f64() {
                Ok(Value::Bool(n != 0.0))
            } else {
                Ok(Value::Null)
            }
        }
        _ => Ok(Value::Null),
    }
}

pub fn fn_lax_string(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(s) => Ok(Value::String(s.clone())),
        Value::Int64(n) => Ok(Value::String(n.to_string())),
        Value::Float64(f) => Ok(Value::String(f.to_string())),
        Value::Bool(b) => Ok(Value::String(b.to_string())),
        Value::Json(j) => {
            if let Some(s) = j.as_str() {
                Ok(Value::String(s.to_string()))
            } else {
                Ok(Value::String(j.to_string()))
            }
        }
        _ => Ok(Value::Null),
    }
}
