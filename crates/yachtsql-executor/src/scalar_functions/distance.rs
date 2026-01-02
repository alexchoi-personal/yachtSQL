#![coverage(off)]

use ordered_float::OrderedFloat;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_cosine_distance(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(Error::InvalidQuery(
            "COSINE_DISTANCE requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Array(a), Value::Array(b)) => {
            if a.len() != b.len() {
                return Err(Error::InvalidQuery(
                    "COSINE_DISTANCE: arrays must have same length".into(),
                ));
            }
            let mut dot_product = 0.0;
            let mut norm_a = 0.0;
            let mut norm_b = 0.0;
            for (va, vb) in a.iter().zip(b.iter()) {
                let fa = match va {
                    Value::Float64(f) => f.0,
                    Value::Int64(n) => *n as f64,
                    Value::Null => return Ok(Value::Null),
                    _ => {
                        return Err(Error::InvalidQuery(
                            "COSINE_DISTANCE: array elements must be numeric".into(),
                        ));
                    }
                };
                let fb = match vb {
                    Value::Float64(f) => f.0,
                    Value::Int64(n) => *n as f64,
                    Value::Null => return Ok(Value::Null),
                    _ => {
                        return Err(Error::InvalidQuery(
                            "COSINE_DISTANCE: array elements must be numeric".into(),
                        ));
                    }
                };
                dot_product += fa * fb;
                norm_a += fa * fa;
                norm_b += fb * fb;
            }
            let similarity = dot_product / (norm_a.sqrt() * norm_b.sqrt());
            Ok(Value::Float64(OrderedFloat(1.0 - similarity)))
        }
        _ => Err(Error::InvalidQuery(
            "COSINE_DISTANCE requires array arguments".into(),
        )),
    }
}

pub fn fn_euclidean_distance(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(Error::InvalidQuery(
            "EUCLIDEAN_DISTANCE requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Array(a), Value::Array(b)) => {
            if a.len() != b.len() {
                return Err(Error::InvalidQuery(
                    "EUCLIDEAN_DISTANCE: arrays must have same length".into(),
                ));
            }
            let mut sum_sq = 0.0;
            for (va, vb) in a.iter().zip(b.iter()) {
                let fa = match va {
                    Value::Float64(f) => f.0,
                    Value::Int64(n) => *n as f64,
                    Value::Null => return Ok(Value::Null),
                    _ => {
                        return Err(Error::InvalidQuery(
                            "EUCLIDEAN_DISTANCE: array elements must be numeric".into(),
                        ));
                    }
                };
                let fb = match vb {
                    Value::Float64(f) => f.0,
                    Value::Int64(n) => *n as f64,
                    Value::Null => return Ok(Value::Null),
                    _ => {
                        return Err(Error::InvalidQuery(
                            "EUCLIDEAN_DISTANCE: array elements must be numeric".into(),
                        ));
                    }
                };
                let diff = fa - fb;
                sum_sq += diff * diff;
            }
            Ok(Value::Float64(OrderedFloat(sum_sq.sqrt())))
        }
        _ => Err(Error::InvalidQuery(
            "EUCLIDEAN_DISTANCE requires array arguments".into(),
        )),
    }
}
