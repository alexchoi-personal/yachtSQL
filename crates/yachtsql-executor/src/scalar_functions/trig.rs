#![coverage(off)]

use ordered_float::OrderedFloat;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_sin(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).sin()))),
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.sin()))),
        _ => Err(Error::InvalidQuery("SIN requires numeric argument".into())),
    }
}

pub fn fn_cos(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).cos()))),
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.cos()))),
        _ => Err(Error::InvalidQuery("COS requires numeric argument".into())),
    }
}

pub fn fn_tan(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).tan()))),
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.tan()))),
        _ => Err(Error::InvalidQuery("TAN requires numeric argument".into())),
    }
}

pub fn fn_asin(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).asin()))),
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.asin()))),
        _ => Err(Error::InvalidQuery("ASIN requires numeric argument".into())),
    }
}

pub fn fn_acos(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).acos()))),
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.acos()))),
        _ => Err(Error::InvalidQuery("ACOS requires numeric argument".into())),
    }
}

pub fn fn_atan(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).atan()))),
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.atan()))),
        _ => Err(Error::InvalidQuery("ATAN requires numeric argument".into())),
    }
}

pub fn fn_atan2(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery("ATAN2 requires 2 arguments".into()));
    }
    let y = match &args[0] {
        Value::Null => return Ok(Value::Null),
        Value::Int64(n) => *n as f64,
        Value::Float64(f) => f.0,
        _ => {
            return Err(Error::InvalidQuery(
                "ATAN2 requires numeric arguments".into(),
            ));
        }
    };
    let x = match &args[1] {
        Value::Null => return Ok(Value::Null),
        Value::Int64(n) => *n as f64,
        Value::Float64(f) => f.0,
        _ => {
            return Err(Error::InvalidQuery(
                "ATAN2 requires numeric arguments".into(),
            ));
        }
    };
    Ok(Value::Float64(OrderedFloat(y.atan2(x))))
}

pub fn fn_sinh(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).sinh()))),
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.sinh()))),
        _ => Err(Error::InvalidQuery("SINH requires numeric argument".into())),
    }
}

pub fn fn_cosh(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).cosh()))),
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.cosh()))),
        _ => Err(Error::InvalidQuery("COSH requires numeric argument".into())),
    }
}

pub fn fn_tanh(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).tanh()))),
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.tanh()))),
        _ => Err(Error::InvalidQuery("TANH requires numeric argument".into())),
    }
}

pub fn fn_asinh(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).asinh()))),
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.asinh()))),
        _ => Err(Error::InvalidQuery(
            "ASINH requires numeric argument".into(),
        )),
    }
}

pub fn fn_acosh(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => {
            let val = *n as f64;
            if val < 1.0 {
                Err(Error::InvalidQuery("ACOSH argument must be >= 1".into()))
            } else {
                Ok(Value::Float64(OrderedFloat(val.acosh())))
            }
        }
        Some(Value::Float64(f)) => {
            if f.0 < 1.0 {
                Err(Error::InvalidQuery("ACOSH argument must be >= 1".into()))
            } else {
                Ok(Value::Float64(OrderedFloat(f.0.acosh())))
            }
        }
        _ => Err(Error::InvalidQuery(
            "ACOSH requires numeric argument".into(),
        )),
    }
}

pub fn fn_atanh(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => {
            let val = *n as f64;
            if val <= -1.0 || val >= 1.0 {
                Err(Error::InvalidQuery(
                    "ATANH argument must be in (-1, 1)".into(),
                ))
            } else {
                Ok(Value::Float64(OrderedFloat(val.atanh())))
            }
        }
        Some(Value::Float64(f)) => {
            if f.0 <= -1.0 || f.0 >= 1.0 {
                Err(Error::InvalidQuery(
                    "ATANH argument must be in (-1, 1)".into(),
                ))
            } else {
                Ok(Value::Float64(OrderedFloat(f.0.atanh())))
            }
        }
        _ => Err(Error::InvalidQuery(
            "ATANH requires numeric argument".into(),
        )),
    }
}

pub fn fn_cot(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => {
            let x = *n as f64;
            Ok(Value::Float64(OrderedFloat(x.cos() / x.sin())))
        }
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.cos() / f.0.sin()))),
        _ => Err(Error::InvalidQuery("COT requires numeric argument".into())),
    }
}

pub fn fn_csc(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => {
            let x = *n as f64;
            Ok(Value::Float64(OrderedFloat(1.0 / x.sin())))
        }
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(1.0 / f.0.sin()))),
        _ => Err(Error::InvalidQuery("CSC requires numeric argument".into())),
    }
}

pub fn fn_sec(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => {
            let x = *n as f64;
            Ok(Value::Float64(OrderedFloat(1.0 / x.cos())))
        }
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(1.0 / f.0.cos()))),
        _ => Err(Error::InvalidQuery("SEC requires numeric argument".into())),
    }
}

pub fn fn_coth(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => {
            let x = *n as f64;
            Ok(Value::Float64(OrderedFloat(1.0 / x.tanh())))
        }
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(1.0 / f.0.tanh()))),
        _ => Err(Error::InvalidQuery("COTH requires numeric argument".into())),
    }
}

pub fn fn_csch(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => {
            let x = *n as f64;
            Ok(Value::Float64(OrderedFloat(1.0 / x.sinh())))
        }
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(1.0 / f.0.sinh()))),
        _ => Err(Error::InvalidQuery("CSCH requires numeric argument".into())),
    }
}

pub fn fn_sech(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => {
            let x = *n as f64;
            Ok(Value::Float64(OrderedFloat(1.0 / x.cosh())))
        }
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(1.0 / f.0.cosh()))),
        _ => Err(Error::InvalidQuery("SECH requires numeric argument".into())),
    }
}
