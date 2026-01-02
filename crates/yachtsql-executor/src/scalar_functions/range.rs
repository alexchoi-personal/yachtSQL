#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{RangeValue, Value};

pub fn fn_range(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(Error::InvalidQuery("RANGE requires 2 arguments".into()));
    }
    let start = if args[0].is_null() {
        None
    } else {
        Some(args[0].clone())
    };
    let end = if args[1].is_null() {
        None
    } else {
        Some(args[1].clone())
    };
    Ok(Value::range(start, end))
}

pub fn fn_range_bucket(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(Error::InvalidQuery(
            "RANGE_BUCKET requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) => Ok(Value::Null),
        (_, Value::Null) => Ok(Value::Null),
        (point, Value::Array(arr)) => {
            if arr.is_empty() {
                return Ok(Value::Int64(0));
            }
            let point_val = match point {
                Value::Int64(n) => *n as f64,
                Value::Float64(f) => f.0,
                _ => {
                    return Err(Error::InvalidQuery(
                        "RANGE_BUCKET: point must be numeric".into(),
                    ));
                }
            };
            let mut bucket = 0i64;
            for v in arr {
                let boundary = match v {
                    Value::Int64(n) => *n as f64,
                    Value::Float64(f) => f.0,
                    Value::Null => return Ok(Value::Null),
                    _ => {
                        return Err(Error::InvalidQuery(
                            "RANGE_BUCKET: array elements must be numeric".into(),
                        ));
                    }
                };
                if point_val < boundary {
                    break;
                }
                bucket += 1;
            }
            Ok(Value::Int64(bucket))
        }
        _ => Err(Error::InvalidQuery(
            "RANGE_BUCKET requires point and array arguments".into(),
        )),
    }
}

pub fn fn_range_contains(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "RANGE_CONTAINS requires range and value arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Range(r), val) => {
            let in_range = match (&r.start, &r.end) {
                (Some(start), Some(end)) => val >= start && val < end,
                (Some(start), None) => val >= start,
                (None, Some(end)) => val < end,
                (None, None) => true,
            };
            Ok(Value::Bool(in_range))
        }
        _ => Ok(Value::Bool(false)),
    }
}

pub fn fn_range_overlaps(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "RANGE_OVERLAPS requires two range arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Range(r1), Value::Range(r2)) => {
            let (start1, end1) = (r1.start.as_deref(), r1.end.as_deref());
            let (start2, end2) = (r2.start.as_deref(), r2.end.as_deref());
            let overlaps = match (start1, end1, start2, end2) {
                (Some(s1), Some(e1), Some(s2), Some(e2)) => {
                    value_less_than(s1, e2) && value_less_than(s2, e1)
                }
                (None, Some(e1), Some(s2), _) => value_less_than(s2, e1),
                (Some(s1), None, _, Some(e2)) => value_less_than(s1, e2),
                (None, _, _, None) | (_, None, None, _) => true,
                _ => false,
            };
            Ok(Value::Bool(overlaps))
        }
        _ => Err(Error::InvalidQuery(
            "RANGE_OVERLAPS expects range arguments".into(),
        )),
    }
}

pub fn fn_range_start(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Range(range) => match &range.start {
            Some(v) => Ok((**v).clone()),
            None => Ok(Value::Null),
        },
        _ => Err(Error::InvalidQuery(
            "RANGE_START expects a range argument".into(),
        )),
    }
}

pub fn fn_range_end(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Range(range) => match &range.end {
            Some(v) => Ok((**v).clone()),
            None => Ok(Value::Null),
        },
        _ => Err(Error::InvalidQuery(
            "RANGE_END expects a range argument".into(),
        )),
    }
}

pub fn fn_range_is_empty(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Range(range) => {
            let is_empty = match (&range.start, &range.end) {
                (Some(start), Some(end)) => start >= end,
                (None, None) => false,
                (Some(_), None) => false,
                (None, Some(_)) => false,
            };
            Ok(Value::Bool(is_empty))
        }
        _ => Err(Error::InvalidQuery(
            "RANGE_IS_EMPTY expects a range argument".into(),
        )),
    }
}

fn value_less_than(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int64(x), Value::Int64(y)) => x < y,
        (Value::Float64(x), Value::Float64(y)) => x < y,
        (Value::Date(x), Value::Date(y)) => x < y,
        (Value::DateTime(x), Value::DateTime(y)) => x < y,
        (Value::Timestamp(x), Value::Timestamp(y)) => x < y,
        (Value::Time(x), Value::Time(y)) => x < y,
        (Value::String(x), Value::String(y)) => x < y,
        _ => false,
    }
}

fn value_max(a: &Value, b: &Value) -> Value {
    if value_less_than(a, b) {
        b.clone()
    } else {
        a.clone()
    }
}

fn value_min(a: &Value, b: &Value) -> Value {
    if value_less_than(a, b) {
        a.clone()
    } else {
        b.clone()
    }
}

pub fn fn_range_intersect(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "RANGE_INTERSECT requires two range arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Range(r1), Value::Range(r2)) => {
            let start = match (&r1.start, &r2.start) {
                (Some(s1), Some(s2)) => Some(value_max(s1, s2)),
                (Some(s), None) | (None, Some(s)) => Some((**s).clone()),
                (None, None) => None,
            };
            let end = match (&r1.end, &r2.end) {
                (Some(e1), Some(e2)) => Some(value_min(e1, e2)),
                (Some(e), None) | (None, Some(e)) => Some((**e).clone()),
                (None, None) => None,
            };
            if let (Some(s), Some(e)) = (&start, &end)
                && !value_less_than(s, e)
            {
                return Ok(Value::Null);
            }
            Ok(Value::Range(RangeValue {
                start: start.map(Box::new),
                end: end.map(Box::new),
            }))
        }
        _ => Err(Error::InvalidQuery(
            "RANGE_INTERSECT expects range arguments".into(),
        )),
    }
}

pub fn fn_generate_range_array(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "GENERATE_RANGE_ARRAY requires range and interval arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Range(range), Value::Interval(interval)) => {
            let start = match &range.start {
                Some(s) => s,
                None => {
                    return Err(Error::InvalidQuery(
                        "GENERATE_RANGE_ARRAY: range must have a start".into(),
                    ));
                }
            };
            let end = match &range.end {
                Some(e) => e,
                None => {
                    return Err(Error::InvalidQuery(
                        "GENERATE_RANGE_ARRAY: range must have an end".into(),
                    ));
                }
            };

            let mut result = Vec::new();
            let mut current = (**start).clone();

            while value_less_than(&current, end) {
                let next_end = add_interval_to_value(&current, interval)?;
                let range_end = if value_less_than(&next_end, end) {
                    next_end.clone()
                } else {
                    (**end).clone()
                };
                result.push(Value::Range(RangeValue {
                    start: Some(Box::new(current.clone())),
                    end: Some(Box::new(range_end)),
                }));
                current = next_end;
            }
            Ok(Value::Array(result))
        }
        _ => Err(Error::InvalidQuery(
            "GENERATE_RANGE_ARRAY expects range and interval arguments".into(),
        )),
    }
}

fn add_interval_to_value(
    val: &Value,
    interval: &yachtsql_common::types::IntervalValue,
) -> Result<Value> {
    use chrono::Datelike;
    match val {
        Value::Date(d) => {
            let mut new_date = *d;
            if interval.months != 0 {
                let total_months = new_date.year() * 12 + new_date.month() as i32 + interval.months;
                let new_year = (total_months - 1) / 12;
                let new_month = ((total_months - 1) % 12 + 1) as u32;
                new_date = chrono::NaiveDate::from_ymd_opt(
                    new_year,
                    new_month,
                    new_date.day().min(days_in_month(new_year, new_month)),
                )
                .ok_or_else(|| Error::InvalidQuery("Date overflow".into()))?;
            }
            if interval.days != 0 {
                new_date += chrono::Duration::days(interval.days as i64);
            }
            Ok(Value::Date(new_date))
        }
        Value::DateTime(dt) => {
            let mut new_dt = *dt;
            if interval.months != 0 {
                let d = new_dt.date();
                let total_months = d.year() * 12 + d.month() as i32 + interval.months;
                let new_year = (total_months - 1) / 12;
                let new_month = ((total_months - 1) % 12 + 1) as u32;
                let new_date = chrono::NaiveDate::from_ymd_opt(
                    new_year,
                    new_month,
                    d.day().min(days_in_month(new_year, new_month)),
                )
                .ok_or_else(|| Error::InvalidQuery("Date overflow".into()))?;
                new_dt = new_date.and_time(new_dt.time());
            }
            if interval.days != 0 {
                new_dt += chrono::Duration::days(interval.days as i64);
            }
            if interval.nanos != 0 {
                new_dt += chrono::Duration::nanoseconds(interval.nanos);
            }
            Ok(Value::DateTime(new_dt))
        }
        Value::Timestamp(ts) => {
            let dt = ts.naive_utc();
            let new_dt = add_interval_to_value(&Value::DateTime(dt), interval)?;
            if let Value::DateTime(ndt) = new_dt {
                Ok(Value::Timestamp(chrono::TimeZone::from_utc_datetime(
                    &chrono::Utc,
                    &ndt,
                )))
            } else {
                Err(Error::InvalidQuery("Unexpected error".into()))
            }
        }
        _ => Err(Error::InvalidQuery(
            "Cannot add interval to this type".into(),
        )),
    }
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}
