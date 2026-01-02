#![coverage(off)]

use ordered_float::OrderedFloat;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_abs(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => Ok(Value::Int64(n.abs())),
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.abs()))),
        Some(Value::Numeric(d)) => Ok(Value::Numeric(d.abs())),
        _ => Err(Error::InvalidQuery("ABS requires numeric argument".into())),
    }
}

pub fn fn_floor(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => Ok(Value::Int64(*n)),
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.floor()))),
        Some(Value::Numeric(d)) => Ok(Value::Numeric(d.floor())),
        _ => Err(Error::InvalidQuery(
            "FLOOR requires numeric argument".into(),
        )),
    }
}

pub fn fn_ceil(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => Ok(Value::Int64(*n)),
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.ceil()))),
        Some(Value::Numeric(d)) => Ok(Value::Numeric(d.ceil())),
        _ => Err(Error::InvalidQuery("CEIL requires numeric argument".into())),
    }
}

pub fn fn_round(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery(
            "ROUND requires at least 1 argument".into(),
        ));
    }
    let precision = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Int64(n) => Ok(Value::Int64(*n)),
        Value::Float64(f) => {
            let mult = 10f64.powi(precision as i32);
            Ok(Value::Float64(OrderedFloat((f.0 * mult).round() / mult)))
        }
        Value::Numeric(d) => Ok(Value::Numeric(d.round_dp(precision.max(0) as u32))),
        _ => Err(Error::InvalidQuery(
            "ROUND requires numeric argument".into(),
        )),
    }
}

pub fn fn_sqrt(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).sqrt()))),
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.sqrt()))),
        _ => Err(Error::InvalidQuery("SQRT requires numeric argument".into())),
    }
}

pub fn fn_cbrt(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).cbrt()))),
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.cbrt()))),
        _ => Err(Error::InvalidQuery("CBRT requires numeric argument".into())),
    }
}

pub fn fn_power(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery("POWER requires 2 arguments".into()));
    }
    let base = match &args[0] {
        Value::Null => return Ok(Value::Null),
        Value::Int64(n) => *n as f64,
        Value::Float64(f) => f.0,
        _ => {
            return Err(Error::InvalidQuery(
                "POWER requires numeric arguments".into(),
            ));
        }
    };
    let exp = match &args[1] {
        Value::Null => return Ok(Value::Null),
        Value::Int64(n) => *n as f64,
        Value::Float64(f) => f.0,
        _ => {
            return Err(Error::InvalidQuery(
                "POWER requires numeric arguments".into(),
            ));
        }
    };
    Ok(Value::Float64(OrderedFloat(base.powf(exp))))
}

pub fn fn_pow(args: &[Value]) -> Result<Value> {
    fn_power(args)
}

pub fn fn_mod(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery("MOD requires 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int64(a), Value::Int64(b)) => {
            if *b == 0 {
                return Err(Error::InvalidQuery("Division by zero".into()));
            }
            Ok(Value::Int64(a % b))
        }
        (Value::Float64(a), Value::Float64(b)) => {
            if b.0 == 0.0 {
                return Err(Error::InvalidQuery("Division by zero".into()));
            }
            Ok(Value::Float64(OrderedFloat(a.0 % b.0)))
        }
        _ => Err(Error::InvalidQuery("MOD requires numeric arguments".into())),
    }
}

pub fn fn_sign(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => Ok(Value::Int64(n.signum())),
        Some(Value::Float64(f)) => Ok(Value::Int64(if f.0 > 0.0 {
            1
        } else if f.0 < 0.0 {
            -1
        } else {
            0
        })),
        _ => Err(Error::InvalidQuery("SIGN requires numeric argument".into())),
    }
}

pub fn fn_exp(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).exp()))),
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.exp()))),
        _ => Err(Error::InvalidQuery("EXP requires numeric argument".into())),
    }
}

pub fn fn_ln(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).ln()))),
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.ln()))),
        _ => Err(Error::InvalidQuery("LN requires numeric argument".into())),
    }
}

pub fn fn_log(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery(
            "LOG requires at least 1 argument".into(),
        ));
    }
    let val = match &args[0] {
        Value::Null => return Ok(Value::Null),
        Value::Int64(n) => *n as f64,
        Value::Float64(f) => f.0,
        _ => return Err(Error::InvalidQuery("LOG requires numeric argument".into())),
    };
    let base = args
        .get(1)
        .map(|v| match v {
            Value::Int64(n) => *n as f64,
            Value::Float64(f) => f.0,
            _ => 10.0,
        })
        .unwrap_or(10.0);
    Ok(Value::Float64(OrderedFloat(val.log(base))))
}

pub fn fn_log10(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).log10()))),
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.log10()))),
        _ => Err(Error::InvalidQuery(
            "LOG10 requires numeric argument".into(),
        )),
    }
}

pub fn fn_div(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery("DIV requires 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int64(a), Value::Int64(b)) => {
            if *b == 0 {
                return Err(Error::InvalidQuery("Division by zero".into()));
            }
            Ok(Value::Int64(a / b))
        }
        (Value::Float64(a), Value::Float64(b)) => {
            if b.0 == 0.0 {
                return Err(Error::InvalidQuery("Division by zero".into()));
            }
            Ok(Value::Int64((a.0 / b.0).trunc() as i64))
        }
        _ => Err(Error::InvalidQuery("DIV requires numeric arguments".into())),
    }
}

pub fn fn_safe_divide(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "SAFE_DIVIDE requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int64(_), Value::Int64(0)) => Ok(Value::Null),
        (Value::Int64(a), Value::Int64(b)) => {
            Ok(Value::Float64(OrderedFloat(*a as f64 / *b as f64)))
        }
        (Value::Float64(_), Value::Float64(b)) if b.0 == 0.0 => Ok(Value::Null),
        (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(a.0 / b.0))),
        (Value::Int64(a), Value::Float64(b)) if b.0 == 0.0 => Ok(Value::Null),
        (Value::Int64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(*a as f64 / b.0))),
        (Value::Float64(_), Value::Int64(0)) => Ok(Value::Null),
        (Value::Float64(a), Value::Int64(b)) => Ok(Value::Float64(OrderedFloat(a.0 / *b as f64))),
        _ => Err(Error::InvalidQuery(
            "SAFE_DIVIDE requires numeric arguments".into(),
        )),
    }
}

pub fn fn_trunc(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery(
            "TRUNC requires at least 1 argument".into(),
        ));
    }
    let precision = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Int64(n) => Ok(Value::Int64(*n)),
        Value::Float64(f) => {
            let mult = 10f64.powi(precision as i32);
            Ok(Value::Float64(OrderedFloat((f.0 * mult).trunc() / mult)))
        }
        Value::Numeric(d) => Ok(Value::Numeric(d.trunc_with_scale(precision.max(0) as u32))),
        _ => Err(Error::InvalidQuery(
            "TRUNC requires numeric argument".into(),
        )),
    }
}

pub fn fn_ieee_divide(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "IEEE_DIVIDE requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int64(a), Value::Int64(b)) => {
            let a = *a as f64;
            let b = *b as f64;
            Ok(Value::Float64(OrderedFloat(a / b)))
        }
        (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(a.0 / b.0))),
        (Value::Int64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(*a as f64 / b.0))),
        (Value::Float64(a), Value::Int64(b)) => Ok(Value::Float64(OrderedFloat(a.0 / *b as f64))),
        _ => Err(Error::InvalidQuery(
            "IEEE_DIVIDE requires numeric arguments".into(),
        )),
    }
}

pub fn fn_is_nan(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Float64(f)) => Ok(Value::Bool(f.0.is_nan())),
        Some(Value::Int64(_)) => Ok(Value::Bool(false)),
        _ => Err(Error::InvalidQuery(
            "IS_NAN requires numeric argument".into(),
        )),
    }
}

pub fn fn_is_inf(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Float64(f)) => Ok(Value::Bool(f.0.is_infinite())),
        Some(Value::Int64(_)) => Ok(Value::Bool(false)),
        _ => Err(Error::InvalidQuery(
            "IS_INF requires numeric argument".into(),
        )),
    }
}

pub fn fn_pi(_args: &[Value]) -> Result<Value> {
    Ok(Value::Float64(OrderedFloat(std::f64::consts::PI)))
}

pub fn fn_safe_multiply(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "SAFE_MULTIPLY requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int64(a), Value::Int64(b)) => {
            Ok(a.checked_mul(*b).map(Value::Int64).unwrap_or(Value::Null))
        }
        (Value::Float64(a), Value::Float64(b)) => {
            let result = a.0 * b.0;
            if result.is_finite() {
                Ok(Value::Float64(OrderedFloat(result)))
            } else {
                Ok(Value::Null)
            }
        }
        _ => Err(Error::InvalidQuery(
            "SAFE_MULTIPLY requires numeric arguments".into(),
        )),
    }
}

pub fn fn_safe_add(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery("SAFE_ADD requires 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int64(a), Value::Int64(b)) => {
            Ok(a.checked_add(*b).map(Value::Int64).unwrap_or(Value::Null))
        }
        (Value::Float64(a), Value::Float64(b)) => {
            let result = a.0 + b.0;
            if result.is_finite() {
                Ok(Value::Float64(OrderedFloat(result)))
            } else {
                Ok(Value::Null)
            }
        }
        _ => Err(Error::InvalidQuery(
            "SAFE_ADD requires numeric arguments".into(),
        )),
    }
}

pub fn fn_safe_subtract(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "SAFE_SUBTRACT requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int64(a), Value::Int64(b)) => {
            Ok(a.checked_sub(*b).map(Value::Int64).unwrap_or(Value::Null))
        }
        (Value::Float64(a), Value::Float64(b)) => {
            let result = a.0 - b.0;
            if result.is_finite() {
                Ok(Value::Float64(OrderedFloat(result)))
            } else {
                Ok(Value::Null)
            }
        }
        _ => Err(Error::InvalidQuery(
            "SAFE_SUBTRACT requires numeric arguments".into(),
        )),
    }
}

pub fn fn_safe_negate(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => Ok(n.checked_neg().map(Value::Int64).unwrap_or(Value::Null)),
        Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(-f.0))),
        _ => Err(Error::InvalidQuery(
            "SAFE_NEGATE requires numeric argument".into(),
        )),
    }
}
