#![coverage(off)]

use chrono::{Datelike, Months, NaiveDate, NaiveDateTime};
use ordered_float::OrderedFloat;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{IntervalValue, Value};

pub fn add_values(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int64(a), Value::Int64(b)) => a
            .checked_add(*b)
            .map(Value::Int64)
            .ok_or_else(|| Error::InvalidQuery("Integer overflow in addition".into())),
        (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(a.0 + b.0))),
        (Value::Int64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(*a as f64 + b.0))),
        (Value::Float64(a), Value::Int64(b)) => Ok(Value::Float64(OrderedFloat(a.0 + *b as f64))),
        (Value::Numeric(a), Value::Numeric(b)) => Ok(Value::Numeric(*a + *b)),
        (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{}{}", a, b))),
        (Value::Date(d), Value::Interval(interval)) => {
            let new_date = add_interval_to_date(d, interval)?;
            Ok(Value::Date(new_date))
        }
        (Value::Interval(interval), Value::Date(d)) => {
            let new_date = add_interval_to_date(d, interval)?;
            Ok(Value::Date(new_date))
        }
        (Value::DateTime(dt), Value::Interval(interval)) => {
            let new_dt = add_interval_to_datetime(dt, interval)?;
            Ok(Value::DateTime(new_dt))
        }
        (Value::Interval(interval), Value::DateTime(dt)) => {
            let new_dt = add_interval_to_datetime(dt, interval)?;
            Ok(Value::DateTime(new_dt))
        }
        (Value::Timestamp(ts), Value::Interval(interval)) => {
            let new_dt = add_interval_to_datetime(&ts.naive_utc(), interval)?;
            Ok(Value::Timestamp(new_dt.and_utc()))
        }
        (Value::Interval(interval), Value::Timestamp(ts)) => {
            let new_dt = add_interval_to_datetime(&ts.naive_utc(), interval)?;
            Ok(Value::Timestamp(new_dt.and_utc()))
        }
        (Value::Interval(a), Value::Interval(b)) => {
            let months = a
                .months
                .checked_add(b.months)
                .ok_or_else(|| Error::InvalidQuery("Interval overflow in months".into()))?;
            let days = a
                .days
                .checked_add(b.days)
                .ok_or_else(|| Error::InvalidQuery("Interval overflow in days".into()))?;
            let nanos = a
                .nanos
                .checked_add(b.nanos)
                .ok_or_else(|| Error::InvalidQuery("Interval overflow in nanos".into()))?;
            Ok(Value::Interval(IntervalValue {
                months,
                days,
                nanos,
            }))
        }
        _ => Err(Error::InvalidQuery(format!(
            "Cannot add {:?} and {:?}",
            left, right
        ))),
    }
}

pub fn sub_values(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int64(a), Value::Int64(b)) => a
            .checked_sub(*b)
            .map(Value::Int64)
            .ok_or_else(|| Error::InvalidQuery("Integer overflow in subtraction".into())),
        (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(a.0 - b.0))),
        (Value::Int64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(*a as f64 - b.0))),
        (Value::Float64(a), Value::Int64(b)) => Ok(Value::Float64(OrderedFloat(a.0 - *b as f64))),
        (Value::Numeric(a), Value::Numeric(b)) => Ok(Value::Numeric(*a - *b)),
        (Value::Date(d), Value::Interval(interval)) => {
            let neg_interval = negate_interval(interval)?;
            let new_date = add_interval_to_date(d, &neg_interval)?;
            Ok(Value::Date(new_date))
        }
        (Value::DateTime(dt), Value::Interval(interval)) => {
            let neg_interval = negate_interval(interval)?;
            let new_dt = add_interval_to_datetime(dt, &neg_interval)?;
            Ok(Value::DateTime(new_dt))
        }
        (Value::Timestamp(ts), Value::Interval(interval)) => {
            let neg_interval = negate_interval(interval)?;
            let new_dt = add_interval_to_datetime(&ts.naive_utc(), &neg_interval)?;
            Ok(Value::Timestamp(new_dt.and_utc()))
        }
        (Value::Interval(a), Value::Interval(b)) => {
            let months = a
                .months
                .checked_sub(b.months)
                .ok_or_else(|| Error::InvalidQuery("Interval overflow in months".into()))?;
            let days = a
                .days
                .checked_sub(b.days)
                .ok_or_else(|| Error::InvalidQuery("Interval overflow in days".into()))?;
            let nanos = a
                .nanos
                .checked_sub(b.nanos)
                .ok_or_else(|| Error::InvalidQuery("Interval overflow in nanos".into()))?;
            Ok(Value::Interval(IntervalValue {
                months,
                days,
                nanos,
            }))
        }
        (Value::Float64(a), Value::String(s)) => {
            if let Ok(b) = s.parse::<f64>() {
                Ok(Value::Float64(OrderedFloat(a.0 - b)))
            } else {
                Err(Error::InvalidQuery(format!(
                    "Cannot subtract {:?} from {:?}",
                    right, left
                )))
            }
        }
        (Value::String(s), Value::Float64(b)) => {
            if let Ok(a) = s.parse::<f64>() {
                Ok(Value::Float64(OrderedFloat(a - b.0)))
            } else {
                Err(Error::InvalidQuery(format!(
                    "Cannot subtract {:?} from {:?}",
                    right, left
                )))
            }
        }
        (Value::Int64(a), Value::String(s)) => {
            if let Ok(b) = s.parse::<i64>() {
                a.checked_sub(b)
                    .map(Value::Int64)
                    .ok_or_else(|| Error::InvalidQuery("Integer overflow in subtraction".into()))
            } else if let Ok(b) = s.parse::<f64>() {
                Ok(Value::Float64(OrderedFloat(*a as f64 - b)))
            } else {
                Err(Error::InvalidQuery(format!(
                    "Cannot subtract {:?} from {:?}",
                    right, left
                )))
            }
        }
        (Value::String(s), Value::Int64(b)) => {
            if let Ok(a) = s.parse::<i64>() {
                a.checked_sub(*b)
                    .map(Value::Int64)
                    .ok_or_else(|| Error::InvalidQuery("Integer overflow in subtraction".into()))
            } else if let Ok(a) = s.parse::<f64>() {
                Ok(Value::Float64(OrderedFloat(a - *b as f64)))
            } else {
                Err(Error::InvalidQuery(format!(
                    "Cannot subtract {:?} from {:?}",
                    right, left
                )))
            }
        }
        (Value::String(s1), Value::String(s2)) => {
            if let (Ok(a), Ok(b)) = (s1.parse::<f64>(), s2.parse::<f64>()) {
                Ok(Value::Float64(OrderedFloat(a - b)))
            } else {
                Err(Error::InvalidQuery(format!(
                    "Cannot subtract {:?} from {:?}",
                    right, left
                )))
            }
        }
        _ => Err(Error::InvalidQuery(format!(
            "Cannot subtract {:?} from {:?}",
            right, left
        ))),
    }
}

pub fn mul_values(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int64(a), Value::Int64(b)) => a
            .checked_mul(*b)
            .map(Value::Int64)
            .ok_or_else(|| Error::InvalidQuery("Integer overflow in multiplication".into())),
        (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(a.0 * b.0))),
        (Value::Int64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(*a as f64 * b.0))),
        (Value::Float64(a), Value::Int64(b)) => Ok(Value::Float64(OrderedFloat(a.0 * *b as f64))),
        (Value::Numeric(a), Value::Numeric(b)) => Ok(Value::Numeric(*a * *b)),
        (Value::Interval(iv), Value::Int64(n)) => {
            let n_i32 = i32::try_from(*n)
                .map_err(|_| Error::InvalidQuery("Multiplier too large for interval".into()))?;
            let months = iv
                .months
                .checked_mul(n_i32)
                .ok_or_else(|| Error::InvalidQuery("Interval overflow in months".into()))?;
            let days = iv
                .days
                .checked_mul(n_i32)
                .ok_or_else(|| Error::InvalidQuery("Interval overflow in days".into()))?;
            let nanos = iv
                .nanos
                .checked_mul(*n)
                .ok_or_else(|| Error::InvalidQuery("Interval overflow in nanos".into()))?;
            Ok(Value::Interval(IntervalValue {
                months,
                days,
                nanos,
            }))
        }
        (Value::Int64(n), Value::Interval(iv)) => {
            let n_i32 = i32::try_from(*n)
                .map_err(|_| Error::InvalidQuery("Multiplier too large for interval".into()))?;
            let months = iv
                .months
                .checked_mul(n_i32)
                .ok_or_else(|| Error::InvalidQuery("Interval overflow in months".into()))?;
            let days = iv
                .days
                .checked_mul(n_i32)
                .ok_or_else(|| Error::InvalidQuery("Interval overflow in days".into()))?;
            let nanos = iv
                .nanos
                .checked_mul(*n)
                .ok_or_else(|| Error::InvalidQuery("Interval overflow in nanos".into()))?;
            Ok(Value::Interval(IntervalValue {
                months,
                days,
                nanos,
            }))
        }
        (Value::String(s1), Value::String(s2)) => {
            if let (Ok(a), Ok(b)) = (s1.parse::<f64>(), s2.parse::<f64>()) {
                Ok(Value::Float64(OrderedFloat(a * b)))
            } else {
                Err(Error::InvalidQuery(format!(
                    "Cannot multiply {:?} and {:?}",
                    left, right
                )))
            }
        }
        (Value::String(s), Value::Int64(b)) => {
            if let Ok(a) = s.parse::<f64>() {
                Ok(Value::Float64(OrderedFloat(a * *b as f64)))
            } else {
                Err(Error::InvalidQuery(format!(
                    "Cannot multiply {:?} and {:?}",
                    left, right
                )))
            }
        }
        (Value::Int64(a), Value::String(s)) => {
            if let Ok(b) = s.parse::<f64>() {
                Ok(Value::Float64(OrderedFloat(*a as f64 * b)))
            } else {
                Err(Error::InvalidQuery(format!(
                    "Cannot multiply {:?} and {:?}",
                    left, right
                )))
            }
        }
        (Value::String(s), Value::Float64(b)) => {
            if let Ok(a) = s.parse::<f64>() {
                Ok(Value::Float64(OrderedFloat(a * b.0)))
            } else {
                Err(Error::InvalidQuery(format!(
                    "Cannot multiply {:?} and {:?}",
                    left, right
                )))
            }
        }
        (Value::Float64(a), Value::String(s)) => {
            if let Ok(b) = s.parse::<f64>() {
                Ok(Value::Float64(OrderedFloat(a.0 * b)))
            } else {
                Err(Error::InvalidQuery(format!(
                    "Cannot multiply {:?} and {:?}",
                    left, right
                )))
            }
        }
        _ => Err(Error::InvalidQuery(format!(
            "Cannot multiply {:?} and {:?}",
            left, right
        ))),
    }
}

pub fn div_values(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (_, Value::Int64(0)) => Err(Error::InvalidQuery("Division by zero".into())),
        (_, Value::Float64(f)) if f.0 == 0.0 => Err(Error::InvalidQuery("Division by zero".into())),
        (Value::Int64(a), Value::Int64(b)) => {
            Ok(Value::Float64(OrderedFloat(*a as f64 / *b as f64)))
        }
        (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(a.0 / b.0))),
        (Value::Int64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(*a as f64 / b.0))),
        (Value::Float64(a), Value::Int64(b)) => Ok(Value::Float64(OrderedFloat(a.0 / *b as f64))),
        (Value::Numeric(a), Value::Numeric(b)) => {
            if b.is_zero() {
                Err(Error::InvalidQuery("Division by zero".into()))
            } else {
                Ok(Value::Numeric(*a / *b))
            }
        }
        (Value::Int64(a), Value::String(s)) => {
            if let Ok(b) = s.parse::<f64>() {
                if b == 0.0 {
                    Err(Error::InvalidQuery("Division by zero".into()))
                } else {
                    Ok(Value::Float64(OrderedFloat(*a as f64 / b)))
                }
            } else {
                Err(Error::InvalidQuery(format!(
                    "Cannot divide {:?} by {:?}",
                    left, right
                )))
            }
        }
        (Value::String(s), Value::Int64(b)) => {
            if *b == 0 {
                return Err(Error::InvalidQuery("Division by zero".into()));
            }
            if let Ok(a) = s.parse::<f64>() {
                Ok(Value::Float64(OrderedFloat(a / *b as f64)))
            } else {
                Err(Error::InvalidQuery(format!(
                    "Cannot divide {:?} by {:?}",
                    left, right
                )))
            }
        }
        (Value::Float64(a), Value::String(s)) => {
            if let Ok(b) = s.parse::<f64>() {
                if b == 0.0 {
                    Err(Error::InvalidQuery("Division by zero".into()))
                } else {
                    Ok(Value::Float64(OrderedFloat(a.0 / b)))
                }
            } else {
                Err(Error::InvalidQuery(format!(
                    "Cannot divide {:?} by {:?}",
                    left, right
                )))
            }
        }
        (Value::String(s), Value::Float64(b)) => {
            if b.0 == 0.0 {
                return Err(Error::InvalidQuery("Division by zero".into()));
            }
            if let Ok(a) = s.parse::<f64>() {
                Ok(Value::Float64(OrderedFloat(a / b.0)))
            } else {
                Err(Error::InvalidQuery(format!(
                    "Cannot divide {:?} by {:?}",
                    left, right
                )))
            }
        }
        (Value::String(s1), Value::String(s2)) => {
            if let (Ok(a), Ok(b)) = (s1.parse::<f64>(), s2.parse::<f64>()) {
                if b == 0.0 {
                    Err(Error::InvalidQuery("Division by zero".into()))
                } else {
                    Ok(Value::Float64(OrderedFloat(a / b)))
                }
            } else {
                Err(Error::InvalidQuery(format!(
                    "Cannot divide {:?} by {:?}",
                    left, right
                )))
            }
        }
        _ => Err(Error::InvalidQuery(format!(
            "Cannot divide {:?} by {:?}",
            left, right
        ))),
    }
}

pub fn mod_values(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (_, Value::Int64(0)) => Err(Error::InvalidQuery("Modulo by zero".into())),
        (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a % b)),
        (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(a.0 % b.0))),
        (Value::Numeric(a), Value::Numeric(b)) => {
            if b.is_zero() {
                Err(Error::InvalidQuery("Modulo by zero".into()))
            } else {
                Ok(Value::Numeric(*a % *b))
            }
        }
        _ => Err(Error::InvalidQuery(format!(
            "Cannot compute modulo of {:?} and {:?}",
            left, right
        ))),
    }
}

pub fn bitwise_and_values(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a & b)),
        _ => Err(Error::InvalidQuery(
            "Bitwise AND requires integer operands".into(),
        )),
    }
}

pub fn bitwise_or_values(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a | b)),
        _ => Err(Error::InvalidQuery(
            "Bitwise OR requires integer operands".into(),
        )),
    }
}

pub fn bitwise_xor_values(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a ^ b)),
        _ => Err(Error::InvalidQuery(
            "Bitwise XOR requires integer operands".into(),
        )),
    }
}

pub fn shift_left_values(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a << b)),
        _ => Err(Error::InvalidQuery(
            "Shift left requires integer operands".into(),
        )),
    }
}

pub fn shift_right_values(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a >> b)),
        _ => Err(Error::InvalidQuery(
            "Shift right requires integer operands".into(),
        )),
    }
}

pub fn concat_values(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{}{}", a, b))),
        (Value::String(a), other) => Ok(Value::String(format!("{}{}", a, value_to_string(other)))),
        (other, Value::String(b)) => Ok(Value::String(format!("{}{}", value_to_string(other), b))),
        (Value::Bytes(a), Value::Bytes(b)) => {
            let mut result = a.clone();
            result.extend(b);
            Ok(Value::Bytes(result))
        }
        (Value::Array(a), Value::Array(b)) => {
            let mut result = a.clone();
            result.extend(b.clone());
            Ok(Value::Array(result))
        }
        _ => Err(Error::InvalidQuery(format!(
            "Cannot concatenate {:?} and {:?}",
            left, right
        ))),
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => f.to_string(),
        Value::Numeric(n) => n.to_string(),
        Value::BigNumeric(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Date(d) => d.to_string(),
        Value::Time(t) => t.to_string(),
        Value::DateTime(dt) => dt.to_string(),
        Value::Timestamp(ts) => ts.to_rfc3339(),
        Value::Interval(iv) => {
            format!("{} months, {} days, {} nanos", iv.months, iv.days, iv.nanos)
        }
        Value::Bytes(b) => format!("{:?}", b),
        Value::Array(arr) => format!("{:?}", arr),
        Value::Struct(fields) => format!("{:?}", fields),
        Value::Json(j) => j.to_string(),
        Value::Geography(g) => g.clone(),
        Value::Range(r) => format!("[{:?}, {:?})", r.start, r.end),
        Value::Default => "DEFAULT".to_string(),
    }
}

fn add_interval_to_date(date: &NaiveDate, interval: &IntervalValue) -> Result<NaiveDate> {
    let mut result = *date;
    if interval.months != 0 {
        result = if interval.months > 0 {
            result
                .checked_add_months(Months::new(interval.months as u32))
                .ok_or_else(|| Error::InvalidQuery("Date overflow".into()))?
        } else {
            result
                .checked_sub_months(Months::new((-interval.months) as u32))
                .ok_or_else(|| Error::InvalidQuery("Date overflow".into()))?
        };
    }
    if interval.days != 0 {
        result += chrono::Duration::days(interval.days as i64);
    }
    Ok(result)
}

fn add_interval_to_datetime(dt: &NaiveDateTime, interval: &IntervalValue) -> Result<NaiveDateTime> {
    let mut result = *dt;
    if interval.months != 0 {
        result = if interval.months > 0 {
            result
                .checked_add_months(Months::new(interval.months as u32))
                .ok_or_else(|| Error::InvalidQuery("DateTime overflow".into()))?
        } else {
            result
                .checked_sub_months(Months::new((-interval.months) as u32))
                .ok_or_else(|| Error::InvalidQuery("DateTime overflow".into()))?
        };
    }
    if interval.days != 0 {
        result += chrono::Duration::days(interval.days as i64);
    }
    if interval.nanos != 0 {
        result += chrono::Duration::nanoseconds(interval.nanos);
    }
    Ok(result)
}

fn negate_interval(interval: &IntervalValue) -> Result<IntervalValue> {
    let months = interval
        .months
        .checked_neg()
        .ok_or_else(|| Error::InvalidQuery("Interval overflow in months negation".into()))?;
    let days = interval
        .days
        .checked_neg()
        .ok_or_else(|| Error::InvalidQuery("Interval overflow in days negation".into()))?;
    let nanos = interval
        .nanos
        .checked_neg()
        .ok_or_else(|| Error::InvalidQuery("Interval overflow in nanos negation".into()))?;
    Ok(IntervalValue {
        months,
        days,
        nanos,
    })
}
