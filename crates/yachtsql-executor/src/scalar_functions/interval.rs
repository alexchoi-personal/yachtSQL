#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{IntervalValue, Value};

pub fn fn_make_interval(args: &[Value]) -> Result<Value> {
    let mut years = 0i32;
    let mut months = 0i32;
    let mut days = 0i32;
    let mut hours = 0i64;
    let mut minutes = 0i64;
    let mut seconds = 0i64;

    for (i, arg) in args.iter().enumerate() {
        let val = match arg {
            Value::Null => continue,
            Value::Int64(n) => *n,
            _ => {
                return Err(Error::InvalidQuery(
                    "MAKE_INTERVAL arguments must be integers".into(),
                ));
            }
        };
        match i {
            0 => years = val as i32,
            1 => months = val as i32,
            2 => days = val as i32,
            3 => hours = val,
            4 => minutes = val,
            5 => seconds = val,
            _ => {}
        }
    }

    let total_months = years * 12 + months;
    let total_nanos = (hours * 3600 + minutes * 60 + seconds) * 1_000_000_000;
    Ok(Value::Interval(IntervalValue {
        months: total_months,
        days,
        nanos: total_nanos,
    }))
}

pub fn fn_justify_days(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery(
            "JUSTIFY_DAYS requires 1 argument".into(),
        ));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Interval(iv) => {
            let extra_months = iv.days / 30;
            let remaining_days = iv.days % 30;
            Ok(Value::Interval(IntervalValue {
                months: iv.months + extra_months,
                days: remaining_days,
                nanos: iv.nanos,
            }))
        }
        _ => Err(Error::InvalidQuery(
            "JUSTIFY_DAYS requires an interval argument".into(),
        )),
    }
}

pub fn fn_justify_hours(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery(
            "JUSTIFY_HOURS requires 1 argument".into(),
        ));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Interval(iv) => {
            const NANOS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000_000;
            let extra_days = (iv.nanos / NANOS_PER_DAY) as i32;
            let remaining_nanos = iv.nanos % NANOS_PER_DAY;
            Ok(Value::Interval(IntervalValue {
                months: iv.months,
                days: iv.days + extra_days,
                nanos: remaining_nanos,
            }))
        }
        _ => Err(Error::InvalidQuery(
            "JUSTIFY_HOURS requires an interval argument".into(),
        )),
    }
}

pub fn fn_justify_interval(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery(
            "JUSTIFY_INTERVAL requires 1 argument".into(),
        ));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Interval(iv) => {
            const NANOS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000_000;
            let extra_days_from_nanos = (iv.nanos / NANOS_PER_DAY) as i32;
            let remaining_nanos = iv.nanos % NANOS_PER_DAY;
            let total_days = iv.days + extra_days_from_nanos;
            let extra_months_from_days = total_days / 30;
            let remaining_days = total_days % 30;
            Ok(Value::Interval(IntervalValue {
                months: iv.months + extra_months_from_days,
                days: remaining_days,
                nanos: remaining_nanos,
            }))
        }
        _ => Err(Error::InvalidQuery(
            "JUSTIFY_INTERVAL requires an interval argument".into(),
        )),
    }
}
