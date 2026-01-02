#![coverage(off)]

use rust_decimal::prelude::ToPrimitive;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_greatest(args: &[Value]) -> Result<Value> {
    if args.iter().any(|v| v.is_null()) {
        return Ok(Value::Null);
    }
    let mut max: Option<Value> = None;
    for arg in args {
        max = Some(match max {
            None => arg.clone(),
            Some(m) => {
                if arg > &m {
                    arg.clone()
                } else {
                    m
                }
            }
        });
    }
    Ok(max.unwrap_or(Value::Null))
}

pub fn fn_least(args: &[Value]) -> Result<Value> {
    if args.iter().any(|v| v.is_null()) {
        return Ok(Value::Null);
    }
    let mut min: Option<Value> = None;
    for arg in args {
        min = Some(match min {
            None => arg.clone(),
            Some(m) => {
                if arg < &m {
                    arg.clone()
                } else {
                    m
                }
            }
        });
    }
    Ok(min.unwrap_or(Value::Null))
}

pub fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => false,
        (Value::Int64(a), Value::Float64(b)) => (*a as f64 - b.0).abs() < f64::EPSILON,
        (Value::Float64(a), Value::Int64(b)) => (a.0 - *b as f64).abs() < f64::EPSILON,
        _ => left == right,
    }
}

pub fn eq_values(left: &Value, right: &Value) -> Result<Value> {
    eq_values_with_collation(left, right, None)
}

pub fn eq_values_with_collation(
    left: &Value,
    right: &Value,
    collation: Option<&str>,
) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int64(a), Value::Float64(b)) => {
            Ok(Value::Bool((*a as f64 - b.0).abs() < f64::EPSILON))
        }
        (Value::Float64(a), Value::Int64(b)) => {
            Ok(Value::Bool((a.0 - *b as f64).abs() < f64::EPSILON))
        }
        (Value::String(a), Value::String(b)) if matches!(collation, Some("und:ci")) => {
            Ok(Value::Bool(a.to_lowercase() == b.to_lowercase()))
        }
        _ => Ok(Value::Bool(left == right)),
    }
}

pub fn neq_values(left: &Value, right: &Value) -> Result<Value> {
    neq_values_with_collation(left, right, None)
}

pub fn neq_values_with_collation(
    left: &Value,
    right: &Value,
    collation: Option<&str>,
) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int64(a), Value::Float64(b)) => {
            Ok(Value::Bool((*a as f64 - b.0).abs() >= f64::EPSILON))
        }
        (Value::Float64(a), Value::Int64(b)) => {
            Ok(Value::Bool((a.0 - *b as f64).abs() >= f64::EPSILON))
        }
        (Value::String(a), Value::String(b)) if matches!(collation, Some("und:ci")) => {
            Ok(Value::Bool(a.to_lowercase() != b.to_lowercase()))
        }
        _ => Ok(Value::Bool(left != right)),
    }
}

pub fn compare_values<F>(left: &Value, right: &Value, cmp: F) -> Result<Value>
where
    F: Fn(std::cmp::Ordering) -> bool,
{
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int64(a), Value::Int64(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
        (Value::Float64(a), Value::Float64(b)) => Ok(Value::Bool(cmp(a
            .partial_cmp(b)
            .unwrap_or(std::cmp::Ordering::Equal)))),
        (Value::Int64(a), Value::Float64(b)) => {
            let af = *a as f64;
            Ok(Value::Bool(cmp(af
                .partial_cmp(&b.0)
                .unwrap_or(std::cmp::Ordering::Equal))))
        }
        (Value::Float64(a), Value::Int64(b)) => {
            let bf = *b as f64;
            Ok(Value::Bool(cmp(a
                .0
                .partial_cmp(&bf)
                .unwrap_or(std::cmp::Ordering::Equal))))
        }
        (Value::String(a), Value::String(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
        (Value::Bytes(a), Value::Bytes(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
        (Value::Date(a), Value::Date(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
        (Value::Time(a), Value::Time(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
        (Value::DateTime(a), Value::DateTime(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
        (Value::Timestamp(a), Value::Timestamp(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
        (Value::Numeric(a), Value::Numeric(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
        (Value::Numeric(a), Value::Int64(b)) => {
            let b_decimal = rust_decimal::Decimal::from(*b);
            Ok(Value::Bool(cmp(a.cmp(&b_decimal))))
        }
        (Value::Int64(a), Value::Numeric(b)) => {
            let a_decimal = rust_decimal::Decimal::from(*a);
            Ok(Value::Bool(cmp(a_decimal.cmp(b))))
        }
        (Value::Numeric(a), Value::Float64(b)) => {
            if let Some(a_f64) = a.to_f64() {
                Ok(Value::Bool(cmp(a_f64
                    .partial_cmp(&b.0)
                    .unwrap_or(std::cmp::Ordering::Equal))))
            } else {
                Ok(Value::Null)
            }
        }
        (Value::Float64(a), Value::Numeric(b)) => {
            if let Some(b_f64) = b.to_f64() {
                Ok(Value::Bool(cmp(a
                    .0
                    .partial_cmp(&b_f64)
                    .unwrap_or(std::cmp::Ordering::Equal))))
            } else {
                Ok(Value::Null)
            }
        }
        (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
        (Value::Interval(a), Value::Interval(b)) => {
            let a_total_nanos = a.nanos
                + a.days as i64 * 86_400_000_000_000
                + a.months as i64 * 30 * 86_400_000_000_000;
            let b_total_nanos = b.nanos
                + b.days as i64 * 86_400_000_000_000
                + b.months as i64 * 30 * 86_400_000_000_000;
            Ok(Value::Bool(cmp(a_total_nanos.cmp(&b_total_nanos))))
        }
        _ => Err(Error::InvalidQuery(format!(
            "Cannot compare {:?} with {:?}",
            left, right
        ))),
    }
}

pub fn compare_values_with_collation<F>(
    left: &Value,
    right: &Value,
    cmp: F,
    collation: Option<&str>,
) -> Result<Value>
where
    F: Fn(std::cmp::Ordering) -> bool,
{
    match (left, right) {
        (Value::String(a), Value::String(b)) if matches!(collation, Some("und:ci")) => {
            Ok(Value::Bool(cmp(a.to_lowercase().cmp(&b.to_lowercase()))))
        }
        _ => compare_values(left, right, cmp),
    }
}

pub fn and_values(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Bool(false), _) | (_, Value::Bool(false)) => Ok(Value::Bool(false)),
        (Value::Bool(true), Value::Bool(true)) => Ok(Value::Bool(true)),
        (Value::Bool(true), Value::Null) | (Value::Null, Value::Bool(true)) => Ok(Value::Null),
        (Value::Null, Value::Null) => Ok(Value::Null),
        _ => Err(Error::InvalidQuery("AND requires boolean operands".into())),
    }
}

pub fn or_values(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
        (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
        (Value::Bool(false), Value::Null) | (Value::Null, Value::Bool(false)) => Ok(Value::Null),
        (Value::Null, Value::Null) => Ok(Value::Null),
        _ => Err(Error::InvalidQuery("OR requires boolean operands".into())),
    }
}

pub fn concat_values(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{}{}", a, b))),
        (Value::Array(a), Value::Array(b)) => {
            let mut result = a.clone();
            result.extend(b.clone());
            Ok(Value::Array(result))
        }
        _ => Err(Error::InvalidQuery(
            "Concat requires string or array operands".into(),
        )),
    }
}
