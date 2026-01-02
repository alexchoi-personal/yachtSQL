#![coverage(off)]

use ordered_float::OrderedFloat;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_int64_from_json(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery("INT64 requires 1 argument".into()));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Json(json) => match json {
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Value::Int64(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(Value::Int64(f as i64))
                } else {
                    Err(Error::InvalidQuery(
                        "Cannot convert JSON number to INT64".into(),
                    ))
                }
            }
            serde_json::Value::String(s) => s
                .parse::<i64>()
                .map(Value::Int64)
                .map_err(|_| Error::InvalidQuery("Cannot parse JSON string as INT64".into())),
            serde_json::Value::Bool(b) => Ok(Value::Int64(if *b { 1 } else { 0 })),
            _ => Err(Error::InvalidQuery("Cannot convert JSON to INT64".into())),
        },
        Value::Int64(n) => Ok(Value::Int64(*n)),
        Value::Float64(f) => Ok(Value::Int64(f.0 as i64)),
        Value::String(s) => s
            .parse::<i64>()
            .map(Value::Int64)
            .map_err(|_| Error::InvalidQuery("Cannot parse string as INT64".into())),
        _ => Err(Error::InvalidQuery("INT64 requires a JSON argument".into())),
    }
}

pub fn fn_float64_from_json(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery("FLOAT64 requires 1 argument".into()));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Json(json) => match json {
            serde_json::Value::Number(n) => {
                if let Some(f) = n.as_f64() {
                    Ok(Value::Float64(OrderedFloat(f)))
                } else {
                    Err(Error::InvalidQuery(
                        "Cannot convert JSON number to FLOAT64".into(),
                    ))
                }
            }
            serde_json::Value::String(s) => s
                .parse::<f64>()
                .map(|f| Value::Float64(OrderedFloat(f)))
                .map_err(|_| Error::InvalidQuery("Cannot parse JSON string as FLOAT64".into())),
            _ => Err(Error::InvalidQuery("Cannot convert JSON to FLOAT64".into())),
        },
        Value::Float64(f) => Ok(Value::Float64(*f)),
        Value::Int64(n) => Ok(Value::Float64(OrderedFloat(*n as f64))),
        Value::String(s) => s
            .parse::<f64>()
            .map(|f| Value::Float64(OrderedFloat(f)))
            .map_err(|_| Error::InvalidQuery("Cannot parse string as FLOAT64".into())),
        _ => Err(Error::InvalidQuery(
            "FLOAT64 requires a JSON argument".into(),
        )),
    }
}

pub fn fn_bool_from_json(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery("BOOL requires 1 argument".into()));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Json(json) => match json {
            serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
            serde_json::Value::String(s) => match s.to_lowercase().as_str() {
                "true" => Ok(Value::Bool(true)),
                "false" => Ok(Value::Bool(false)),
                _ => Err(Error::InvalidQuery(
                    "Cannot parse JSON string as BOOL".into(),
                )),
            },
            _ => Err(Error::InvalidQuery("Cannot convert JSON to BOOL".into())),
        },
        Value::Bool(b) => Ok(Value::Bool(*b)),
        Value::String(s) => match s.to_lowercase().as_str() {
            "true" => Ok(Value::Bool(true)),
            "false" => Ok(Value::Bool(false)),
            _ => Err(Error::InvalidQuery("Cannot parse string as BOOL".into())),
        },
        _ => Err(Error::InvalidQuery("BOOL requires a JSON argument".into())),
    }
}

pub fn fn_string_from_json(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery("STRING requires 1 argument".into()));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Json(json) => match json {
            serde_json::Value::String(s) => Ok(Value::String(s.clone())),
            serde_json::Value::Number(n) => Ok(Value::String(n.to_string())),
            serde_json::Value::Bool(b) => Ok(Value::String(b.to_string())),
            _ => Ok(Value::String(json.to_string())),
        },
        Value::String(s) => Ok(Value::String(s.clone())),
        Value::Int64(n) => Ok(Value::String(n.to_string())),
        Value::Float64(f) => Ok(Value::String(f.to_string())),
        Value::Bool(b) => Ok(Value::String(b.to_string())),
        other => Ok(Value::String(format!("{}", other))),
    }
}
