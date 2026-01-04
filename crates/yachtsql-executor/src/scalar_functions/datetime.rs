#![coverage(off)]

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{IntervalValue, Value};

fn add_interval_to_date(date: &NaiveDate, interval: &IntervalValue) -> Result<NaiveDate> {
    use chrono::Months;
    let mut result = *date;
    if interval.months != 0 {
        result = if interval.months > 0 {
            let months_u32 = u32::try_from(interval.months)
                .map_err(|_| Error::InvalidQuery("Interval months overflow".into()))?;
            result
                .checked_add_months(Months::new(months_u32))
                .ok_or_else(|| Error::InvalidQuery("Date overflow".into()))?
        } else {
            let neg_months = interval
                .months
                .checked_neg()
                .ok_or_else(|| Error::InvalidQuery("Interval months overflow".into()))?;
            let months_u32 = u32::try_from(neg_months)
                .map_err(|_| Error::InvalidQuery("Interval months overflow".into()))?;
            result
                .checked_sub_months(Months::new(months_u32))
                .ok_or_else(|| Error::InvalidQuery("Date overflow".into()))?
        };
    }
    if interval.days != 0 {
        result += chrono::Duration::days(interval.days as i64);
    }
    Ok(result)
}

fn add_interval_to_datetime(dt: &NaiveDateTime, interval: &IntervalValue) -> Result<NaiveDateTime> {
    use chrono::Months;
    let mut result = *dt;
    if interval.months != 0 {
        result = if interval.months > 0 {
            let months_u32 = u32::try_from(interval.months)
                .map_err(|_| Error::InvalidQuery("Interval months overflow".into()))?;
            result
                .checked_add_months(Months::new(months_u32))
                .ok_or_else(|| Error::InvalidQuery("DateTime overflow".into()))?
        } else {
            let neg_months = interval
                .months
                .checked_neg()
                .ok_or_else(|| Error::InvalidQuery("Interval months overflow".into()))?;
            let months_u32 = u32::try_from(neg_months)
                .map_err(|_| Error::InvalidQuery("Interval months overflow".into()))?;
            result
                .checked_sub_months(Months::new(months_u32))
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

fn negate_interval(interval: &IntervalValue) -> IntervalValue {
    IntervalValue {
        months: -interval.months,
        days: -interval.days,
        nanos: -interval.nanos,
    }
}

fn date_diff_by_part(d1: &NaiveDate, d2: &NaiveDate, part: &str) -> Result<i64> {
    match part {
        "DAY" => Ok(d1.signed_duration_since(*d2).num_days()),
        "WEEK" => Ok(d1.signed_duration_since(*d2).num_weeks()),
        "MONTH" => {
            let months1 = d1.year() as i64 * 12 + d1.month() as i64;
            let months2 = d2.year() as i64 * 12 + d2.month() as i64;
            Ok(months1 - months2)
        }
        "QUARTER" => {
            let q1 = d1.year() as i64 * 4 + ((d1.month() - 1) / 3) as i64;
            let q2 = d2.year() as i64 * 4 + ((d2.month() - 1) / 3) as i64;
            Ok(q1 - q2)
        }
        "YEAR" => Ok((d1.year() - d2.year()) as i64),
        _ => Ok(d1.signed_duration_since(*d2).num_days()),
    }
}

fn trunc_date(date: &NaiveDate, part: &str) -> Result<NaiveDate> {
    match part {
        "YEAR" => NaiveDate::from_ymd_opt(date.year(), 1, 1)
            .ok_or_else(|| Error::InvalidQuery("Invalid date".into())),
        "QUARTER" => {
            let month = ((date.month() - 1) / 3) * 3 + 1;
            NaiveDate::from_ymd_opt(date.year(), month, 1)
                .ok_or_else(|| Error::InvalidQuery("Invalid date".into()))
        }
        "MONTH" => NaiveDate::from_ymd_opt(date.year(), date.month(), 1)
            .ok_or_else(|| Error::InvalidQuery("Invalid date".into())),
        "WEEK" => {
            let days_from_monday = date.weekday().num_days_from_monday();
            Ok(*date - chrono::Duration::days(days_from_monday as i64))
        }
        _ => Ok(*date),
    }
}

fn trunc_datetime(dt: &NaiveDateTime, part: &str) -> Result<NaiveDateTime> {
    match part {
        "YEAR" => NaiveDate::from_ymd_opt(dt.year(), 1, 1)
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into())),
        "QUARTER" => {
            let month = ((dt.month() - 1) / 3) * 3 + 1;
            NaiveDate::from_ymd_opt(dt.year(), month, 1)
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))
        }
        "MONTH" => NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into())),
        "WEEK" | "WEEK_SUNDAY" => {
            let days_from_sunday = dt.weekday().num_days_from_sunday();
            let date = dt.date() - chrono::Duration::days(days_from_sunday as i64);
            date.and_hms_opt(0, 0, 0)
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))
        }
        "WEEK_MONDAY" => {
            let days_from_monday = dt.weekday().num_days_from_monday();
            let date = dt.date() - chrono::Duration::days(days_from_monday as i64);
            date.and_hms_opt(0, 0, 0)
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))
        }
        "WEEK_TUESDAY" => {
            let days = (dt.weekday().num_days_from_sunday() + 5) % 7;
            let date = dt.date() - chrono::Duration::days(days as i64);
            date.and_hms_opt(0, 0, 0)
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))
        }
        "WEEK_WEDNESDAY" => {
            let days = (dt.weekday().num_days_from_sunday() + 4) % 7;
            let date = dt.date() - chrono::Duration::days(days as i64);
            date.and_hms_opt(0, 0, 0)
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))
        }
        "WEEK_THURSDAY" => {
            let days = (dt.weekday().num_days_from_sunday() + 3) % 7;
            let date = dt.date() - chrono::Duration::days(days as i64);
            date.and_hms_opt(0, 0, 0)
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))
        }
        "WEEK_FRIDAY" => {
            let days = (dt.weekday().num_days_from_sunday() + 2) % 7;
            let date = dt.date() - chrono::Duration::days(days as i64);
            date.and_hms_opt(0, 0, 0)
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))
        }
        "WEEK_SATURDAY" => {
            let days = (dt.weekday().num_days_from_sunday() + 1) % 7;
            let date = dt.date() - chrono::Duration::days(days as i64);
            date.and_hms_opt(0, 0, 0)
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))
        }
        "ISOWEEK" => {
            let days_from_monday = dt.weekday().num_days_from_monday();
            let date = dt.date() - chrono::Duration::days(days_from_monday as i64);
            date.and_hms_opt(0, 0, 0)
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))
        }
        "ISOYEAR" => {
            let iso_year = dt.date().iso_week().year();
            let first_day_of_iso_year =
                NaiveDate::from_isoywd_opt(iso_year, 1, chrono::Weekday::Mon)
                    .ok_or_else(|| Error::InvalidQuery("Invalid ISO year".into()))?;
            first_day_of_iso_year
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))
        }
        "DAY" => dt
            .date()
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into())),
        "HOUR" => NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
            .and_then(|d| d.and_hms_opt(dt.hour(), 0, 0))
            .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into())),
        "MINUTE" => NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
            .and_then(|d| d.and_hms_opt(dt.hour(), dt.minute(), 0))
            .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into())),
        "SECOND" => NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
            .and_then(|d| d.and_hms_opt(dt.hour(), dt.minute(), dt.second()))
            .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into())),
        _ => Ok(*dt),
    }
}

fn trunc_time(time: &NaiveTime, part: &str) -> Result<NaiveTime> {
    match part {
        "HOUR" => NaiveTime::from_hms_opt(time.hour(), 0, 0)
            .ok_or_else(|| Error::InvalidQuery("Invalid time".into())),
        "MINUTE" => NaiveTime::from_hms_opt(time.hour(), time.minute(), 0)
            .ok_or_else(|| Error::InvalidQuery("Invalid time".into())),
        _ => Ok(*time),
    }
}

fn bucket_datetime(
    dt: &NaiveDateTime,
    interval: &IntervalValue,
    origin: &NaiveDateTime,
) -> Result<NaiveDateTime> {
    if interval.months > 0 {
        let months_since =
            (dt.year() - origin.year()) * 12 + (dt.month() as i32 - origin.month() as i32);
        let bucket_count = months_since / interval.months;
        let bucket_start_month = origin.month() as i32 + (bucket_count * interval.months);
        let years_to_add = (bucket_start_month - 1) / 12;
        let month = ((bucket_start_month - 1) % 12) + 1;
        return NaiveDate::from_ymd_opt(origin.year() + years_to_add, month as u32, origin.day())
            .and_then(|d| d.and_hms_opt(origin.hour(), origin.minute(), origin.second()))
            .ok_or_else(|| Error::InvalidQuery("Invalid bucket datetime".into()));
    }
    let total_nanos_in_interval =
        interval.days as i64 * 24 * 60 * 60 * 1_000_000_000 + interval.nanos;
    if total_nanos_in_interval == 0 {
        return Ok(*dt);
    }
    let diff = *dt - *origin;
    let diff_nanos = diff.num_nanoseconds().unwrap_or(0);
    let bucket_count = if diff_nanos >= 0 {
        diff_nanos / total_nanos_in_interval
    } else {
        (diff_nanos - total_nanos_in_interval + 1) / total_nanos_in_interval
    };
    let bucket_start_nanos = bucket_count * total_nanos_in_interval;
    Ok(*origin + chrono::Duration::nanoseconds(bucket_start_nanos))
}

fn bq_format_to_chrono(bq_format: &str) -> String {
    bq_format
        .replace("%F", "%Y-%m-%d")
        .replace("%T", "%H:%M:%S")
        .replace("%R", "%H:%M")
        .replace("%D", "%m/%d/%y")
        .replace("%Q", "Q%Q")
}

fn format_date_with_pattern(date: &NaiveDate, pattern: &str) -> Result<String> {
    let chrono_pattern = bq_format_to_chrono(pattern);
    Ok(date.format(&chrono_pattern).to_string())
}

fn format_datetime_with_pattern(dt: &NaiveDateTime, pattern: &str) -> Result<String> {
    let chrono_pattern = bq_format_to_chrono(pattern);
    Ok(dt.format(&chrono_pattern).to_string())
}

fn format_time_with_pattern(time: &NaiveTime, pattern: &str) -> Result<String> {
    let chrono_pattern = bq_format_to_chrono(pattern);
    Ok(time.format(&chrono_pattern).to_string())
}

fn parse_date_with_pattern(s: &str, pattern: &str) -> Result<NaiveDate> {
    let chrono_pattern = bq_format_to_chrono(pattern);
    NaiveDate::parse_from_str(s, &chrono_pattern).map_err(|e| {
        Error::InvalidQuery(format!(
            "Failed to parse date '{}' with pattern '{}': {}",
            s, pattern, e
        ))
    })
}

fn parse_datetime_with_pattern(s: &str, pattern: &str) -> Result<NaiveDateTime> {
    let chrono_pattern = bq_format_to_chrono(pattern);
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, &chrono_pattern) {
        return Ok(dt);
    }
    let has_date = pattern.contains("%Y") || pattern.contains("%m") || pattern.contains("%d");
    let has_time = pattern.contains("%H")
        || pattern.contains("%I")
        || pattern.contains("%M")
        || pattern.contains("%S");
    if has_date
        && !has_time
        && let Ok(date) = NaiveDate::parse_from_str(s, &chrono_pattern)
    {
        return date.and_hms_opt(0, 0, 0).ok_or_else(|| {
            Error::datetime_error("parse_datetime", "failed to add midnight time to date")
        });
    }
    Err(Error::InvalidQuery(format!(
        "Failed to parse datetime '{}' with pattern '{}'",
        s, pattern
    )))
}

fn parse_time_with_pattern(s: &str, pattern: &str) -> Result<NaiveTime> {
    let chrono_pattern = bq_format_to_chrono(pattern);
    if let Ok(time) = NaiveTime::parse_from_str(s, &chrono_pattern) {
        return Ok(time);
    }
    let has_hour = pattern.contains("%H") || pattern.contains("%I");
    let has_minute = pattern.contains("%M");
    let has_second = pattern.contains("%S");
    if has_hour && !has_minute && !has_second {
        let extended_pattern = format!("{} %M %S", chrono_pattern);
        let extended_input = format!("{} 00 00", s);
        if let Ok(time) = NaiveTime::parse_from_str(&extended_input, &extended_pattern) {
            return Ok(time);
        }
    } else if has_hour && has_minute && !has_second {
        let extended_pattern = format!("{} %S", chrono_pattern);
        let extended_input = format!("{} 00", s);
        if let Ok(time) = NaiveTime::parse_from_str(&extended_input, &extended_pattern) {
            return Ok(time);
        }
    }
    Err(Error::InvalidQuery(format!(
        "Failed to parse time '{}' with pattern '{}'",
        s, pattern
    )))
}

pub fn fn_current_date(_args: &[Value]) -> Result<Value> {
    Ok(Value::Date(Utc::now().date_naive()))
}

pub fn fn_current_timestamp(_args: &[Value]) -> Result<Value> {
    Ok(Value::Timestamp(Utc::now()))
}

pub fn fn_current_time(_args: &[Value]) -> Result<Value> {
    Ok(Value::Time(Utc::now().time()))
}

pub fn fn_current_datetime(_args: &[Value]) -> Result<Value> {
    Ok(Value::DateTime(Utc::now().naive_utc()))
}

pub fn fn_date(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|e| Error::InvalidQuery(format!("Invalid date: {}", e)))?;
            Ok(Value::Date(date))
        }
        Some(Value::Timestamp(ts)) => Ok(Value::Date(ts.date_naive())),
        Some(Value::DateTime(dt)) => Ok(Value::Date(dt.date())),
        _ => Err(Error::InvalidQuery(
            "DATE requires date/string argument".into(),
        )),
    }
}

pub fn fn_time(args: &[Value]) -> Result<Value> {
    if args.len() == 3 {
        if args.iter().any(|a| a.is_null()) {
            return Ok(Value::Null);
        }
        let hour = args[0]
            .as_i64()
            .ok_or_else(|| Error::InvalidQuery("TIME hour must be int".into()))?;
        let minute = args[1]
            .as_i64()
            .ok_or_else(|| Error::InvalidQuery("TIME minute must be int".into()))?;
        let second = args[2]
            .as_i64()
            .ok_or_else(|| Error::InvalidQuery("TIME second must be int".into()))?;
        let time = NaiveTime::from_hms_opt(hour as u32, minute as u32, second as u32)
            .ok_or_else(|| Error::InvalidQuery("Invalid time components".into()))?;
        return Ok(Value::Time(time));
    }
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let time = NaiveTime::parse_from_str(s, "%H:%M:%S")
                .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.f"))
                .map_err(|e| Error::InvalidQuery(format!("Invalid time: {}", e)))?;
            Ok(Value::Time(time))
        }
        Some(Value::Timestamp(ts)) => Ok(Value::Time(ts.time())),
        Some(Value::DateTime(dt)) => Ok(Value::Time(dt.time())),
        _ => Err(Error::InvalidQuery(
            "TIME requires time/string argument".into(),
        )),
    }
}

pub fn fn_datetime(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery(
            "DATETIME requires at least 1 argument".into(),
        ));
    }
    if args.len() == 6 {
        let year = args[0]
            .as_i64()
            .ok_or_else(|| Error::InvalidQuery("DATETIME year must be int".into()))?;
        let month = args[1]
            .as_i64()
            .ok_or_else(|| Error::InvalidQuery("DATETIME month must be int".into()))?;
        let day = args[2]
            .as_i64()
            .ok_or_else(|| Error::InvalidQuery("DATETIME day must be int".into()))?;
        let hour = args[3]
            .as_i64()
            .ok_or_else(|| Error::InvalidQuery("DATETIME hour must be int".into()))?;
        let minute = args[4]
            .as_i64()
            .ok_or_else(|| Error::InvalidQuery("DATETIME minute must be int".into()))?;
        let second = args[5]
            .as_i64()
            .ok_or_else(|| Error::InvalidQuery("DATETIME second must be int".into()))?;
        let dt = chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)
            .and_then(|d| d.and_hms_opt(hour as u32, minute as u32, second as u32))
            .ok_or_else(|| Error::InvalidQuery("Invalid datetime components".into()))?;
        return Ok(Value::DateTime(dt));
    }
    if args.len() == 2
        && let (Value::Date(d), Value::Time(t)) = (&args[0], &args[1])
    {
        let dt = chrono::NaiveDateTime::new(*d, *t);
        return Ok(Value::DateTime(dt));
    }
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d"))
                .map_err(|e| Error::InvalidQuery(format!("Invalid datetime: {}", e)))?;
            Ok(Value::DateTime(dt))
        }
        Some(Value::Date(d)) => {
            let dt = d.and_hms_opt(0, 0, 0).ok_or_else(|| {
                Error::datetime_error("DATETIME", "failed to add midnight time to date")
            })?;
            Ok(Value::DateTime(dt))
        }
        Some(Value::Timestamp(ts)) => Ok(Value::DateTime(ts.naive_utc())),
        _ => Err(Error::InvalidQuery(
            "DATETIME requires date/string argument".into(),
        )),
    }
}

pub fn fn_timestamp(args: &[Value]) -> Result<Value> {
    if args.len() == 2 {
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => return Ok(Value::Null),
            (Value::String(s), Value::String(tz_name)) => {
                let ndt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                    .map_err(|e| Error::InvalidQuery(format!("Invalid timestamp: {}", e)))?;
                let tz: chrono_tz::Tz = tz_name
                    .parse()
                    .map_err(|_| Error::InvalidQuery(format!("Invalid timezone: {}", tz_name)))?;
                let local_dt = ndt
                    .and_local_timezone(tz)
                    .single()
                    .ok_or_else(|| Error::InvalidQuery("Ambiguous or invalid local time".into()))?;
                return Ok(Value::Timestamp(local_dt.with_timezone(&Utc)));
            }
            _ => {
                return Err(Error::InvalidQuery(
                    "TIMESTAMP with timezone requires (string, string) arguments".into(),
                ));
            }
        }
    }
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let normalized = if s.ends_with("+00") || s.ends_with("-00") {
                format!("{}:00", s)
            } else {
                s.to_string()
            };
            let dt = DateTime::parse_from_rfc3339(&normalized)
                .map(|d| d.with_timezone(&Utc))
                .or_else(|_| {
                    DateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S%:z")
                        .map(|d| d.with_timezone(&Utc))
                })
                .or_else(|_| {
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                        .map(|ndt| ndt.and_utc())
                })
                .map_err(|e| Error::InvalidQuery(format!("Invalid timestamp: {}", e)))?;
            Ok(Value::Timestamp(dt))
        }
        Some(Value::Date(d)) => {
            let ndt = d.and_hms_opt(0, 0, 0).ok_or_else(|| {
                Error::InvalidQuery("Failed to create timestamp from date".into())
            })?;
            Ok(Value::Timestamp(ndt.and_utc()))
        }
        Some(Value::DateTime(dt)) => Ok(Value::Timestamp(dt.and_utc())),
        _ => Err(Error::InvalidQuery(
            "TIMESTAMP requires string/date argument".into(),
        )),
    }
}

pub fn fn_date_add(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery("DATE_ADD requires 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Date(d), Value::Interval(interval)) => {
            let new_date = add_interval_to_date(d, interval)?;
            Ok(Value::Date(new_date))
        }
        (Value::DateTime(dt), Value::Interval(interval)) => {
            let new_dt = add_interval_to_datetime(dt, interval)?;
            Ok(Value::DateTime(new_dt))
        }
        (Value::Timestamp(ts), Value::Interval(interval)) => {
            let new_dt = add_interval_to_datetime(&ts.naive_utc(), interval)?;
            Ok(Value::Timestamp(new_dt.and_utc()))
        }
        _ => Err(Error::InvalidQuery(
            "DATE_ADD requires date/datetime/timestamp and interval".into(),
        )),
    }
}

pub fn fn_date_sub(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery("DATE_SUB requires 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Date(d), Value::Interval(interval)) => {
            let neg_interval = negate_interval(interval);
            let new_date = add_interval_to_date(d, &neg_interval)?;
            Ok(Value::Date(new_date))
        }
        (Value::DateTime(dt), Value::Interval(interval)) => {
            let neg_interval = negate_interval(interval);
            let new_dt = add_interval_to_datetime(dt, &neg_interval)?;
            Ok(Value::DateTime(new_dt))
        }
        (Value::Timestamp(ts), Value::Interval(interval)) => {
            let neg_interval = negate_interval(interval);
            let new_dt = add_interval_to_datetime(&ts.naive_utc(), &neg_interval)?;
            Ok(Value::Timestamp(new_dt.and_utc()))
        }
        _ => Err(Error::InvalidQuery(
            "DATE_SUB requires date/datetime/timestamp and interval".into(),
        )),
    }
}

pub fn fn_date_diff(args: &[Value]) -> Result<Value> {
    if args.len() < 3 {
        return Err(Error::InvalidQuery("DATE_DIFF requires 3 arguments".into()));
    }
    let part = args[2].as_str().unwrap_or("DAY").to_uppercase();
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Date(d1), Value::Date(d2)) => {
            let result = date_diff_by_part(d1, d2, &part)?;
            Ok(Value::Int64(result))
        }
        (Value::DateTime(dt1), Value::DateTime(dt2)) => {
            let result = datetime_diff_by_part(dt1, dt2, &part)?;
            Ok(Value::Int64(result))
        }
        (Value::Timestamp(ts1), Value::Timestamp(ts2)) => {
            let dt1 = ts1.naive_utc();
            let dt2 = ts2.naive_utc();
            let result = datetime_diff_by_part(&dt1, &dt2, &part)?;
            Ok(Value::Int64(result))
        }
        _ => Err(Error::InvalidQuery(
            "DATE_DIFF requires date/datetime/timestamp arguments".into(),
        )),
    }
}

fn datetime_diff_by_part(dt1: &NaiveDateTime, dt2: &NaiveDateTime, part: &str) -> Result<i64> {
    let duration = dt1.signed_duration_since(*dt2);
    match part {
        "SECOND" | "SECONDS" => Ok(duration.num_seconds()),
        "MINUTE" | "MINUTES" => Ok(duration.num_minutes()),
        "HOUR" | "HOURS" => Ok(duration.num_hours()),
        "DAY" | "DAYS" => Ok(duration.num_days()),
        "WEEK" | "WEEKS" => Ok(duration.num_weeks()),
        "MONTH" | "MONTHS" => {
            let months1 = dt1.year() as i64 * 12 + dt1.month() as i64;
            let months2 = dt2.year() as i64 * 12 + dt2.month() as i64;
            Ok(months1 - months2)
        }
        "QUARTER" | "QUARTERS" => {
            let q1 = dt1.year() as i64 * 4 + ((dt1.month() - 1) / 3) as i64;
            let q2 = dt2.year() as i64 * 4 + ((dt2.month() - 1) / 3) as i64;
            Ok(q1 - q2)
        }
        "YEAR" | "YEARS" => Ok((dt1.year() - dt2.year()) as i64),
        "MILLISECOND" | "MILLISECONDS" => Ok(duration.num_milliseconds()),
        "MICROSECOND" | "MICROSECONDS" => Ok(duration.num_microseconds().unwrap_or(0)),
        _ => Ok(duration.num_days()),
    }
}

pub fn fn_time_add(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery("TIME_ADD requires 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Time(t), Value::Interval(interval)) => {
            let nanos = t.num_seconds_from_midnight() as i64 * 1_000_000_000
                + t.nanosecond() as i64
                + interval.nanos
                + interval.days as i64 * 86_400_000_000_000
                + interval.months as i64 * 30 * 86_400_000_000_000;
            let day_nanos = 86_400_000_000_000i64;
            let wrapped_nanos = ((nanos % day_nanos) + day_nanos) % day_nanos;
            let secs = (wrapped_nanos / 1_000_000_000) as u32;
            let nano = (wrapped_nanos % 1_000_000_000) as u32;
            let new_time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
                .ok_or_else(|| Error::InvalidQuery("Time overflow".into()))?;
            Ok(Value::Time(new_time))
        }
        _ => Err(Error::InvalidQuery(
            "TIME_ADD requires time and interval".into(),
        )),
    }
}

pub fn fn_time_sub(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery("TIME_SUB requires 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Time(t), Value::Interval(interval)) => {
            let neg_interval = negate_interval(interval);
            let nanos = t.num_seconds_from_midnight() as i64 * 1_000_000_000
                + t.nanosecond() as i64
                + neg_interval.nanos
                + neg_interval.days as i64 * 86_400_000_000_000
                + neg_interval.months as i64 * 30 * 86_400_000_000_000;
            let day_nanos = 86_400_000_000_000i64;
            let wrapped_nanos = ((nanos % day_nanos) + day_nanos) % day_nanos;
            let secs = (wrapped_nanos / 1_000_000_000) as u32;
            let nano = (wrapped_nanos % 1_000_000_000) as u32;
            let new_time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
                .ok_or_else(|| Error::InvalidQuery("Time overflow".into()))?;
            Ok(Value::Time(new_time))
        }
        _ => Err(Error::InvalidQuery(
            "TIME_SUB requires time and interval".into(),
        )),
    }
}

pub fn fn_time_diff(args: &[Value]) -> Result<Value> {
    if args.len() < 3 {
        return Err(Error::InvalidQuery("TIME_DIFF requires 3 arguments".into()));
    }
    let part = args[2].as_str().unwrap_or("SECOND").to_uppercase();
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Time(t1), Value::Time(t2)) => {
            let secs1 = t1.num_seconds_from_midnight() as i64;
            let secs2 = t2.num_seconds_from_midnight() as i64;
            let diff_secs = secs1 - secs2;
            let result = match part.as_str() {
                "HOUR" => diff_secs / 3600,
                "MINUTE" => diff_secs / 60,
                "SECOND" => diff_secs,
                "MILLISECOND" => {
                    diff_secs * 1000 + (t1.nanosecond() as i64 - t2.nanosecond() as i64) / 1_000_000
                }
                "MICROSECOND" => {
                    diff_secs * 1_000_000
                        + (t1.nanosecond() as i64 - t2.nanosecond() as i64) / 1_000
                }
                "NANOSECOND" => {
                    diff_secs * 1_000_000_000 + (t1.nanosecond() as i64 - t2.nanosecond() as i64)
                }
                _ => return Err(Error::InvalidQuery(format!("Invalid time part: {}", part))),
            };
            Ok(Value::Int64(result))
        }
        _ => Err(Error::InvalidQuery(
            "TIME_DIFF requires time arguments".into(),
        )),
    }
}

pub fn fn_date_trunc(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "DATE_TRUNC requires 2 arguments".into(),
        ));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Date(d) => {
            let part = args[1].as_str().unwrap_or("DAY").to_uppercase();
            let truncated = trunc_date(d, &part)?;
            Ok(Value::Date(truncated))
        }
        _ => Err(Error::InvalidQuery(
            "DATE_TRUNC requires a date argument".into(),
        )),
    }
}

pub fn fn_datetime_trunc(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "DATETIME_TRUNC requires 2 arguments".into(),
        ));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::DateTime(dt) => {
            let part = args[1].as_str().unwrap_or("DAY").to_uppercase();
            let truncated = trunc_datetime(dt, &part)?;
            Ok(Value::DateTime(truncated))
        }
        _ => Err(Error::InvalidQuery(
            "DATETIME_TRUNC requires a datetime argument".into(),
        )),
    }
}

pub fn fn_timestamp_trunc(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "TIMESTAMP_TRUNC requires 2 arguments".into(),
        ));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Timestamp(ts) => {
            let part = args[1].as_str().unwrap_or("DAY").to_uppercase();
            let truncated = trunc_datetime(&ts.naive_utc(), &part)?;
            Ok(Value::Timestamp(truncated.and_utc()))
        }
        _ => Err(Error::InvalidQuery(
            "TIMESTAMP_TRUNC requires a timestamp argument".into(),
        )),
    }
}

pub fn fn_time_trunc(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "TIME_TRUNC requires 2 arguments".into(),
        ));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Time(t) => {
            let part = args[1].as_str().unwrap_or("SECOND").to_uppercase();
            let truncated = trunc_time(t, &part)?;
            Ok(Value::Time(truncated))
        }
        _ => Err(Error::InvalidQuery(
            "TIME_TRUNC requires a time argument".into(),
        )),
    }
}

pub fn fn_format_date(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "FORMAT_DATE requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(fmt), Value::Date(d)) => {
            let formatted = format_date_with_pattern(d, fmt)?;
            Ok(Value::String(formatted))
        }
        _ => Err(Error::InvalidQuery(
            "FORMAT_DATE requires format string and date".into(),
        )),
    }
}

pub fn fn_format_timestamp(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "FORMAT_TIMESTAMP requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(fmt), Value::Timestamp(ts)) => {
            let formatted = format_datetime_with_pattern(&ts.naive_utc(), fmt)?;
            Ok(Value::String(formatted))
        }
        _ => Err(Error::InvalidQuery(
            "FORMAT_TIMESTAMP requires format string and timestamp".into(),
        )),
    }
}

pub fn fn_format_datetime(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "FORMAT_DATETIME requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(fmt), Value::DateTime(dt)) => {
            let formatted = format_datetime_with_pattern(dt, fmt)?;
            Ok(Value::String(formatted))
        }
        _ => Err(Error::InvalidQuery(
            "FORMAT_DATETIME requires format string and datetime".into(),
        )),
    }
}

pub fn fn_format_time(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "FORMAT_TIME requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(fmt), Value::Time(t)) => {
            let formatted = format_time_with_pattern(t, fmt)?;
            Ok(Value::String(formatted))
        }
        _ => Err(Error::InvalidQuery(
            "FORMAT_TIME requires format string and time".into(),
        )),
    }
}

pub fn fn_parse_date(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "PARSE_DATE requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(fmt), Value::String(s)) => {
            let date = parse_date_with_pattern(s, fmt)?;
            Ok(Value::Date(date))
        }
        _ => Err(Error::InvalidQuery(
            "PARSE_DATE requires format and date strings".into(),
        )),
    }
}

pub fn fn_parse_timestamp(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "PARSE_TIMESTAMP requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(fmt), Value::String(s)) => {
            let dt = parse_datetime_with_pattern(s, fmt)?;
            Ok(Value::Timestamp(dt.and_utc()))
        }
        _ => Err(Error::InvalidQuery(
            "PARSE_TIMESTAMP requires format and timestamp strings".into(),
        )),
    }
}

pub fn fn_parse_datetime(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "PARSE_DATETIME requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(fmt), Value::String(s)) => {
            let dt = parse_datetime_with_pattern(s, fmt)?;
            Ok(Value::DateTime(dt))
        }
        _ => Err(Error::InvalidQuery(
            "PARSE_DATETIME requires format and datetime strings".into(),
        )),
    }
}

pub fn fn_parse_time(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "PARSE_TIME requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(fmt), Value::String(s)) => {
            let time = parse_time_with_pattern(s, fmt)?;
            Ok(Value::Time(time))
        }
        _ => Err(Error::InvalidQuery(
            "PARSE_TIME requires format and time strings".into(),
        )),
    }
}

pub fn fn_timestamp_micros(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(micros)) => {
            let ts = DateTime::from_timestamp_micros(*micros)
                .ok_or_else(|| Error::InvalidQuery("Invalid microseconds".into()))?;
            Ok(Value::Timestamp(ts))
        }
        _ => Err(Error::InvalidQuery(
            "TIMESTAMP_MICROS requires integer argument".into(),
        )),
    }
}

pub fn fn_timestamp_millis(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(millis)) => {
            let ts = DateTime::from_timestamp_millis(*millis)
                .ok_or_else(|| Error::InvalidQuery("Invalid milliseconds".into()))?;
            Ok(Value::Timestamp(ts))
        }
        _ => Err(Error::InvalidQuery(
            "TIMESTAMP_MILLIS requires integer argument".into(),
        )),
    }
}

pub fn fn_timestamp_seconds(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(secs)) => {
            let ts = DateTime::from_timestamp(*secs, 0)
                .ok_or_else(|| Error::InvalidQuery("Invalid seconds".into()))?;
            Ok(Value::Timestamp(ts))
        }
        _ => Err(Error::InvalidQuery(
            "TIMESTAMP_SECONDS requires integer argument".into(),
        )),
    }
}

pub fn fn_unix_date(args: &[Value]) -> Result<Value> {
    let unix_epoch = chrono::DateTime::UNIX_EPOCH.date_naive();
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Date(d)) => {
            let days = d.signed_duration_since(unix_epoch).num_days();
            Ok(Value::Int64(days))
        }
        _ => Err(Error::InvalidQuery(
            "UNIX_DATE requires date argument".into(),
        )),
    }
}

pub fn fn_unix_micros(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Timestamp(ts)) => {
            let micros = ts.timestamp_micros();
            Ok(Value::Int64(micros))
        }
        _ => Err(Error::InvalidQuery(
            "UNIX_MICROS requires timestamp argument".into(),
        )),
    }
}

pub fn fn_unix_millis(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Timestamp(ts)) => {
            let millis = ts.timestamp_millis();
            Ok(Value::Int64(millis))
        }
        _ => Err(Error::InvalidQuery(
            "UNIX_MILLIS requires timestamp argument".into(),
        )),
    }
}

pub fn fn_unix_seconds(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Timestamp(ts)) => {
            let secs = ts.timestamp();
            Ok(Value::Int64(secs))
        }
        _ => Err(Error::InvalidQuery(
            "UNIX_SECONDS requires timestamp argument".into(),
        )),
    }
}

pub fn fn_date_from_unix_date(args: &[Value]) -> Result<Value> {
    let unix_epoch = chrono::DateTime::UNIX_EPOCH.date_naive();
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(days)) => {
            let date = unix_epoch + chrono::Duration::days(*days);
            Ok(Value::Date(date))
        }
        _ => Err(Error::InvalidQuery(
            "DATE_FROM_UNIX_DATE requires integer argument".into(),
        )),
    }
}

pub fn fn_last_day(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Date(d)) => {
            let year = d.year();
            let month = d.month();
            let next_month = if month == 12 { 1 } else { month + 1 };
            let next_year = if month == 12 { year + 1 } else { year };
            let first_of_next =
                NaiveDate::from_ymd_opt(next_year, next_month, 1).ok_or_else(|| {
                    Error::datetime_error("LAST_DAY", "failed to compute first day of next month")
                })?;
            let last_day = first_of_next - chrono::Duration::days(1);
            Ok(Value::Date(last_day))
        }
        Some(Value::DateTime(dt)) => {
            let year = dt.date().year();
            let month = dt.date().month();
            let next_month = if month == 12 { 1 } else { month + 1 };
            let next_year = if month == 12 { year + 1 } else { year };
            let first_of_next =
                NaiveDate::from_ymd_opt(next_year, next_month, 1).ok_or_else(|| {
                    Error::datetime_error("LAST_DAY", "failed to compute first day of next month")
                })?;
            let last_day = first_of_next - chrono::Duration::days(1);
            Ok(Value::Date(last_day))
        }
        _ => Err(Error::InvalidQuery(
            "LAST_DAY requires date argument".into(),
        )),
    }
}

pub fn fn_date_bucket(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "DATE_BUCKET requires at least 2 arguments".into(),
        ));
    }
    let date = match &args[0] {
        Value::Null => return Ok(Value::Null),
        Value::Date(d) => *d,
        _ => {
            return Err(Error::InvalidQuery(
                "DATE_BUCKET requires a date as first argument".into(),
            ));
        }
    };
    let interval = match &args[1] {
        Value::Null => return Ok(Value::Null),
        Value::Interval(i) => i,
        _ => {
            return Err(Error::InvalidQuery(
                "DATE_BUCKET requires an interval as second argument".into(),
            ));
        }
    };
    let origin = if args.len() >= 3 {
        match &args[2] {
            Value::Null => return Ok(Value::Null),
            Value::Date(d) => *d,
            _ => {
                return Err(Error::InvalidQuery(
                    "DATE_BUCKET origin must be a date".into(),
                ));
            }
        }
    } else {
        NaiveDate::from_ymd_opt(1950, 1, 1)
            .unwrap_or_else(|| chrono::DateTime::UNIX_EPOCH.date_naive())
    };
    if interval.months != 0 {
        let date_months = date.year() * 12 + date.month() as i32 - 1;
        let origin_months = origin.year() * 12 + origin.month() as i32 - 1;
        let diff_months = date_months - origin_months;
        let bucket_months = interval.months;
        let bucket_idx = if diff_months >= 0 {
            diff_months / bucket_months
        } else {
            (diff_months - bucket_months + 1) / bucket_months
        };
        let bucket_start_months = origin_months + bucket_idx * bucket_months;
        let year = bucket_start_months / 12;
        let month = (bucket_start_months % 12) as u32 + 1;
        let bucketed = NaiveDate::from_ymd_opt(year, month, 1)
            .ok_or_else(|| Error::InvalidQuery("Invalid date in DATE_BUCKET calculation".into()))?;
        Ok(Value::Date(bucketed))
    } else if interval.days != 0 {
        let diff_days = date.signed_duration_since(origin).num_days();
        let bucket_days = interval.days as i64;
        let bucket_idx = if diff_days >= 0 {
            diff_days / bucket_days
        } else {
            (diff_days - bucket_days + 1) / bucket_days
        };
        let bucketed = origin + chrono::Duration::days(bucket_idx * bucket_days);
        Ok(Value::Date(bucketed))
    } else {
        Err(Error::InvalidQuery(
            "DATE_BUCKET interval must have days or months".into(),
        ))
    }
}

pub fn fn_datetime_bucket(args: &[Value]) -> Result<Value> {
    let default_origin = NaiveDate::from_ymd_opt(1950, 1, 1)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .unwrap_or_else(|| chrono::DateTime::UNIX_EPOCH.naive_utc());
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "DATETIME_BUCKET requires at least 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::DateTime(dt), Value::Interval(interval)) => {
            let origin = if args.len() > 2 {
                match &args[2] {
                    Value::DateTime(o) => *o,
                    _ => default_origin,
                }
            } else {
                default_origin
            };
            let bucket = bucket_datetime(dt, interval, &origin)?;
            Ok(Value::DateTime(bucket))
        }
        _ => Err(Error::InvalidQuery(
            "DATETIME_BUCKET requires datetime and interval arguments".into(),
        )),
    }
}

pub fn fn_timestamp_bucket(args: &[Value]) -> Result<Value> {
    let default_origin = NaiveDate::from_ymd_opt(1950, 1, 1)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .unwrap_or_else(|| chrono::DateTime::UNIX_EPOCH.naive_utc());
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "TIMESTAMP_BUCKET requires at least 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Timestamp(ts), Value::Interval(interval)) => {
            let origin = if args.len() > 2 {
                match &args[2] {
                    Value::Timestamp(o) => o.naive_utc(),
                    _ => default_origin,
                }
            } else {
                default_origin
            };
            let bucket = bucket_datetime(&ts.naive_utc(), interval, &origin)?;
            Ok(Value::Timestamp(bucket.and_utc()))
        }
        _ => Err(Error::InvalidQuery(
            "TIMESTAMP_BUCKET requires timestamp and interval arguments".into(),
        )),
    }
}

pub fn fn_generate_date_array(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "GENERATE_DATE_ARRAY requires at least 2 arguments".into(),
        ));
    }
    let start_date = match &args[0] {
        Value::Null => return Ok(Value::Null),
        Value::Date(d) => *d,
        _ => {
            return Err(Error::InvalidQuery(
                "GENERATE_DATE_ARRAY requires DATE arguments".into(),
            ));
        }
    };
    let end_date = match &args[1] {
        Value::Null => return Ok(Value::Null),
        Value::Date(d) => *d,
        _ => {
            return Err(Error::InvalidQuery(
                "GENERATE_DATE_ARRAY requires DATE arguments".into(),
            ));
        }
    };
    let step_days = match args.get(2) {
        Some(Value::Null) => return Ok(Value::Null),
        Some(Value::Interval(iv)) => iv.days as i64 + iv.months as i64 * 30,
        Some(Value::Int64(n)) => *n,
        None => 1,
        _ => {
            return Err(Error::InvalidQuery(
                "GENERATE_DATE_ARRAY step must be INTERVAL or INT64".into(),
            ));
        }
    };
    if step_days == 0 {
        return Err(Error::InvalidQuery(
            "GENERATE_DATE_ARRAY step cannot be zero".into(),
        ));
    }
    let mut result = Vec::new();
    let mut current = start_date;
    if step_days > 0 {
        while current <= end_date {
            result.push(Value::Date(current));
            current = current + chrono::Days::new(step_days as u64);
        }
    } else {
        let neg_step = (-step_days) as u64;
        while current >= end_date {
            result.push(Value::Date(current));
            current = current - chrono::Days::new(neg_step);
        }
    }
    Ok(Value::Array(result))
}

pub fn fn_generate_timestamp_array(args: &[Value]) -> Result<Value> {
    if args.len() < 3 {
        return Err(Error::InvalidQuery(
            "GENERATE_TIMESTAMP_ARRAY requires 3 arguments".into(),
        ));
    }
    let start_ts = match &args[0] {
        Value::Null => return Ok(Value::Null),
        Value::Timestamp(ts) => *ts,
        _ => {
            return Err(Error::InvalidQuery(
                "GENERATE_TIMESTAMP_ARRAY requires TIMESTAMP arguments".into(),
            ));
        }
    };
    let end_ts = match &args[1] {
        Value::Null => return Ok(Value::Null),
        Value::Timestamp(ts) => *ts,
        _ => {
            return Err(Error::InvalidQuery(
                "GENERATE_TIMESTAMP_ARRAY requires TIMESTAMP arguments".into(),
            ));
        }
    };
    let step = match &args[2] {
        Value::Null => return Ok(Value::Null),
        Value::Interval(iv) => iv,
        _ => {
            return Err(Error::InvalidQuery(
                "GENERATE_TIMESTAMP_ARRAY step must be INTERVAL".into(),
            ));
        }
    };
    let step_nanos = step.nanos
        + step.days as i64 * 86_400_000_000_000
        + step.months as i64 * 30 * 86_400_000_000_000;
    if step_nanos == 0 {
        return Err(Error::InvalidQuery(
            "GENERATE_TIMESTAMP_ARRAY step cannot be zero".into(),
        ));
    }
    let mut result = Vec::new();
    let mut current = start_ts;
    if step_nanos > 0 {
        while current <= end_ts {
            result.push(Value::Timestamp(current));
            current += chrono::Duration::nanoseconds(step_nanos);
        }
    } else {
        while current >= end_ts {
            result.push(Value::Timestamp(current));
            current += chrono::Duration::nanoseconds(step_nanos);
        }
    }
    Ok(Value::Array(result))
}

pub fn fn_extract(_args: &[Value]) -> Result<Value> {
    Err(Error::unsupported(
        "EXTRACT function requires special handling with field parameter - use helpers::extract_from_value instead",
    ))
}
