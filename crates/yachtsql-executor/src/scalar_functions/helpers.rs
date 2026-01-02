#![coverage(off)]

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, IntervalValue, RangeValue, Value};
use yachtsql_ir::{DateTimeField, WeekStartDay};

pub fn like_pattern_to_regex(pattern: &str, case_insensitive: bool) -> Result<regex::Regex> {
    let mut regex_str = String::from("^");
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '%' => regex_str.push_str(".*"),
            '_' => regex_str.push('.'),
            '\\' => {
                if let Some(&next) = chars.peek() {
                    regex_str.push_str(&regex::escape(&next.to_string()));
                    chars.next();
                }
            }
            _ => regex_str.push_str(&regex::escape(&c.to_string())),
        }
    }
    regex_str.push('$');

    let builder = regex::RegexBuilder::new(&regex_str);
    let mut builder = builder;
    if case_insensitive {
        builder.case_insensitive(true);
    }
    builder
        .build()
        .map_err(|e| Error::InvalidQuery(format!("Invalid LIKE pattern: {}", e)))
}

pub fn extract_datetime_field(val: &Value, field: DateTimeField) -> Result<Value> {
    match val {
        Value::Null => Ok(Value::Null),
        Value::Date(date) => extract_from_date(date, field),
        Value::Timestamp(dt) => extract_from_datetime(&dt.naive_utc(), field),
        Value::Time(time) => extract_from_time(time, field),
        Value::DateTime(dt) => extract_from_datetime(dt, field),
        Value::Interval(iv) => extract_from_interval(iv, field),
        _ => Err(Error::InvalidQuery(
            "EXTRACT requires date/time/timestamp/interval argument".into(),
        )),
    }
}

fn week_number_from_date(date: &NaiveDate, start_day: WeekStartDay) -> i64 {
    let year_start = NaiveDate::from_ymd_opt(date.year(), 1, 1).unwrap();
    let start_weekday = match start_day {
        WeekStartDay::Sunday => chrono::Weekday::Sun,
        WeekStartDay::Monday => chrono::Weekday::Mon,
        WeekStartDay::Tuesday => chrono::Weekday::Tue,
        WeekStartDay::Wednesday => chrono::Weekday::Wed,
        WeekStartDay::Thursday => chrono::Weekday::Thu,
        WeekStartDay::Friday => chrono::Weekday::Fri,
        WeekStartDay::Saturday => chrono::Weekday::Sat,
    };
    let days_until_first_start_day = (start_weekday.num_days_from_sunday() as i32
        - year_start.weekday().num_days_from_sunday() as i32
        + 7)
        % 7;
    let first_week_start = year_start + chrono::Duration::days(days_until_first_start_day as i64);
    if *date < first_week_start {
        0
    } else {
        let days_since_first_week = (*date - first_week_start).num_days();
        days_since_first_week / 7 + 1
    }
}

fn extract_from_date(date: &NaiveDate, field: DateTimeField) -> Result<Value> {
    match field {
        DateTimeField::Year => Ok(Value::Int64(date.year() as i64)),
        DateTimeField::Month => Ok(Value::Int64(date.month() as i64)),
        DateTimeField::Day => Ok(Value::Int64(date.day() as i64)),
        DateTimeField::Week(start_day) => Ok(Value::Int64(week_number_from_date(date, start_day))),
        DateTimeField::IsoWeek => Ok(Value::Int64(date.iso_week().week() as i64)),
        DateTimeField::IsoYear => Ok(Value::Int64(date.iso_week().year() as i64)),
        DateTimeField::DayOfWeek => Ok(Value::Int64(
            date.weekday().num_days_from_sunday() as i64 + 1,
        )),
        DateTimeField::DayOfYear => Ok(Value::Int64(date.ordinal() as i64)),
        DateTimeField::Quarter => Ok(Value::Int64(((date.month() - 1) / 3 + 1) as i64)),
        DateTimeField::Hour
        | DateTimeField::Minute
        | DateTimeField::Second
        | DateTimeField::Millisecond
        | DateTimeField::Microsecond
        | DateTimeField::Nanosecond
        | DateTimeField::Date
        | DateTimeField::Time
        | DateTimeField::Datetime
        | DateTimeField::Timezone
        | DateTimeField::TimezoneHour
        | DateTimeField::TimezoneMinute => Err(Error::InvalidQuery(format!(
            "Cannot extract {:?} from date",
            field
        ))),
    }
}

fn extract_from_datetime(dt: &chrono::NaiveDateTime, field: DateTimeField) -> Result<Value> {
    match field {
        DateTimeField::Year => Ok(Value::Int64(dt.year() as i64)),
        DateTimeField::Month => Ok(Value::Int64(dt.month() as i64)),
        DateTimeField::Day => Ok(Value::Int64(dt.day() as i64)),
        DateTimeField::Hour => Ok(Value::Int64(dt.hour() as i64)),
        DateTimeField::Minute => Ok(Value::Int64(dt.minute() as i64)),
        DateTimeField::Second => Ok(Value::Int64(dt.second() as i64)),
        DateTimeField::Millisecond => Ok(Value::Int64((dt.nanosecond() / 1_000_000) as i64)),
        DateTimeField::Microsecond => Ok(Value::Int64((dt.nanosecond() / 1000) as i64)),
        DateTimeField::Nanosecond => Ok(Value::Int64(dt.nanosecond() as i64)),
        DateTimeField::Week(start_day) => {
            Ok(Value::Int64(week_number_from_date(&dt.date(), start_day)))
        }
        DateTimeField::IsoWeek => Ok(Value::Int64(dt.iso_week().week() as i64)),
        DateTimeField::IsoYear => Ok(Value::Int64(dt.iso_week().year() as i64)),
        DateTimeField::DayOfWeek => {
            Ok(Value::Int64(dt.weekday().num_days_from_sunday() as i64 + 1))
        }
        DateTimeField::DayOfYear => Ok(Value::Int64(dt.ordinal() as i64)),
        DateTimeField::Quarter => Ok(Value::Int64(((dt.month() - 1) / 3 + 1) as i64)),
        DateTimeField::Date => Ok(Value::Date(dt.date())),
        DateTimeField::Time => Ok(Value::Time(dt.time())),
        DateTimeField::Datetime => Ok(Value::DateTime(*dt)),
        DateTimeField::Timezone | DateTimeField::TimezoneHour | DateTimeField::TimezoneMinute => {
            Err(Error::InvalidQuery(format!(
                "Cannot extract {:?} from datetime",
                field
            )))
        }
    }
}

fn extract_from_time(time: &NaiveTime, field: DateTimeField) -> Result<Value> {
    match field {
        DateTimeField::Hour => Ok(Value::Int64(time.hour() as i64)),
        DateTimeField::Minute => Ok(Value::Int64(time.minute() as i64)),
        DateTimeField::Second => Ok(Value::Int64(time.second() as i64)),
        DateTimeField::Millisecond => Ok(Value::Int64((time.nanosecond() / 1_000_000) as i64)),
        DateTimeField::Microsecond => Ok(Value::Int64((time.nanosecond() / 1000) as i64)),
        DateTimeField::Nanosecond => Ok(Value::Int64(time.nanosecond() as i64)),
        DateTimeField::Year
        | DateTimeField::Month
        | DateTimeField::Day
        | DateTimeField::Week(_)
        | DateTimeField::IsoWeek
        | DateTimeField::IsoYear
        | DateTimeField::DayOfWeek
        | DateTimeField::DayOfYear
        | DateTimeField::Quarter
        | DateTimeField::Date
        | DateTimeField::Time
        | DateTimeField::Datetime
        | DateTimeField::Timezone
        | DateTimeField::TimezoneHour
        | DateTimeField::TimezoneMinute => Err(Error::InvalidQuery(format!(
            "Cannot extract {:?} from time",
            field
        ))),
    }
}

fn extract_from_interval(
    iv: &yachtsql_common::types::IntervalValue,
    field: DateTimeField,
) -> Result<Value> {
    match field {
        DateTimeField::Year => Ok(Value::Int64((iv.months / 12) as i64)),
        DateTimeField::Month => Ok(Value::Int64((iv.months % 12) as i64)),
        DateTimeField::Day => Ok(Value::Int64(iv.days as i64)),
        DateTimeField::Hour => {
            const NANOS_PER_HOUR: i64 = 60 * 60 * 1_000_000_000;
            Ok(Value::Int64(iv.nanos / NANOS_PER_HOUR))
        }
        DateTimeField::Minute => {
            const NANOS_PER_MINUTE: i64 = 60 * 1_000_000_000;
            const NANOS_PER_HOUR: i64 = 60 * NANOS_PER_MINUTE;
            Ok(Value::Int64((iv.nanos % NANOS_PER_HOUR) / NANOS_PER_MINUTE))
        }
        DateTimeField::Second => {
            const NANOS_PER_SECOND: i64 = 1_000_000_000;
            const NANOS_PER_MINUTE: i64 = 60 * NANOS_PER_SECOND;
            Ok(Value::Int64(
                (iv.nanos % NANOS_PER_MINUTE) / NANOS_PER_SECOND,
            ))
        }
        DateTimeField::Millisecond => {
            const NANOS_PER_MS: i64 = 1_000_000;
            const NANOS_PER_SECOND: i64 = 1_000_000_000;
            Ok(Value::Int64((iv.nanos % NANOS_PER_SECOND) / NANOS_PER_MS))
        }
        DateTimeField::Microsecond => {
            const NANOS_PER_US: i64 = 1_000;
            const NANOS_PER_MS: i64 = 1_000_000;
            Ok(Value::Int64((iv.nanos % NANOS_PER_MS) / NANOS_PER_US))
        }
        DateTimeField::Week(_)
        | DateTimeField::IsoWeek
        | DateTimeField::IsoYear
        | DateTimeField::DayOfWeek
        | DateTimeField::DayOfYear
        | DateTimeField::Quarter
        | DateTimeField::Date
        | DateTimeField::Time
        | DateTimeField::Nanosecond
        | DateTimeField::Datetime
        | DateTimeField::Timezone
        | DateTimeField::TimezoneHour
        | DateTimeField::TimezoneMinute => Err(Error::InvalidQuery(format!(
            "Cannot extract {:?} from interval",
            field
        ))),
    }
}

pub fn parse_date_string(s: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|e| Error::InvalidQuery(format!("Invalid date string: {}", e)))
}

pub fn parse_time_string(s: &str) -> Result<NaiveTime> {
    NaiveTime::parse_from_str(s, "%H:%M:%S")
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.f"))
        .map_err(|e| Error::InvalidQuery(format!("Invalid time string: {}", e)))
}

pub fn parse_timestamp_string(s: &str) -> Result<DateTime<Utc>> {
    use chrono::{FixedOffset, TimeZone};

    let s_trimmed = s.trim();

    if let Ok(dt) = DateTime::parse_from_rfc3339(s_trimmed) {
        return Ok(dt.with_timezone(&Utc));
    }
    if let Ok(dt) = DateTime::parse_from_str(s_trimmed, "%Y-%m-%d %H:%M:%S%:z") {
        return Ok(dt.with_timezone(&Utc));
    }
    if let Ok(dt) = DateTime::parse_from_str(s_trimmed, "%Y-%m-%d %H:%M:%S%z") {
        return Ok(dt.with_timezone(&Utc));
    }
    if let Ok(dt) = DateTime::parse_from_str(s_trimmed, "%Y-%m-%d %H:%M:%S%.f%:z") {
        return Ok(dt.with_timezone(&Utc));
    }
    if let Ok(dt) = DateTime::parse_from_str(s_trimmed, "%Y-%m-%d %H:%M:%S%.f%z") {
        return Ok(dt.with_timezone(&Utc));
    }

    let re = regex::Regex::new(r"^(.+?)([+-])(\d{1,2})$").unwrap();
    if let Some(caps) = re.captures(s_trimmed) {
        let datetime_part = caps.get(1).unwrap().as_str();
        let sign = caps.get(2).unwrap().as_str();
        let hours: i32 = caps.get(3).unwrap().as_str().parse().unwrap_or(0);
        let offset_secs = hours * 3600 * if sign == "-" { -1 } else { 1 };
        if let Some(offset) = FixedOffset::east_opt(offset_secs) {
            if let Ok(ndt) = NaiveDateTime::parse_from_str(datetime_part, "%Y-%m-%d %H:%M:%S") {
                let dt = offset.from_local_datetime(&ndt).single();
                if let Some(dt) = dt {
                    return Ok(dt.with_timezone(&Utc));
                }
            }
            if let Ok(ndt) = NaiveDateTime::parse_from_str(datetime_part, "%Y-%m-%d %H:%M:%S%.f") {
                let dt = offset.from_local_datetime(&ndt).single();
                if let Some(dt) = dt {
                    return Ok(dt.with_timezone(&Utc));
                }
            }
        }
    }

    let s_no_tz = if s_trimmed.ends_with(" UTC") {
        &s_trimmed[..s_trimmed.len() - 4]
    } else if s_trimmed.ends_with("+00:00") {
        &s_trimmed[..s_trimmed.len() - 6]
    } else if s_trimmed.ends_with("+00") {
        &s_trimmed[..s_trimmed.len() - 3]
    } else if s_trimmed.ends_with("Z") {
        &s_trimmed[..s_trimmed.len() - 1]
    } else {
        s_trimmed
    };

    NaiveDateTime::parse_from_str(s_no_tz, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(s_no_tz, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(s_no_tz, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(s_no_tz, "%Y-%m-%dT%H:%M:%S%.f"))
        .or_else(|_| {
            NaiveDate::parse_from_str(s_no_tz, "%Y-%m-%d").map(|d| d.and_hms_opt(0, 0, 0).unwrap())
        })
        .map(|ndt| ndt.and_utc())
        .map_err(|e| Error::InvalidQuery(format!("Invalid timestamp string: {}", e)))
}

pub fn parse_datetime_string(s: &str) -> Result<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
        .map_err(|e| Error::InvalidQuery(format!("Invalid datetime string: {}", e)))
}

pub fn parse_range_string(s: &str, inner_type: &DataType) -> Result<RangeValue> {
    let s = s.trim();
    if s.len() < 2 {
        return Err(Error::InvalidQuery(format!(
            "Invalid range string: too short: {}",
            s
        )));
    }
    let first_char = s.chars().next().unwrap();
    let last_char = s.chars().last().unwrap();
    if first_char != '[' && first_char != '(' {
        return Err(Error::InvalidQuery(format!(
            "Invalid range string: must start with '[' or '(': {}",
            s
        )));
    }
    if last_char != ']' && last_char != ')' {
        return Err(Error::InvalidQuery(format!(
            "Invalid range string: must end with ']' or ')': {}",
            s
        )));
    }
    let inner = &s[1..s.len() - 1];
    let parts: Vec<&str> = inner.splitn(2, ',').collect();
    if parts.len() != 2 {
        return Err(Error::InvalidQuery(format!(
            "Invalid range string: must contain exactly one comma: {}",
            s
        )));
    }
    let start_str = parts[0].trim();
    let end_str = parts[1].trim();
    let start = if start_str.is_empty()
        || start_str.eq_ignore_ascii_case("NULL")
        || start_str.eq_ignore_ascii_case("UNBOUNDED")
    {
        None
    } else {
        Some(parse_range_value(start_str, inner_type)?)
    };
    let end = if end_str.is_empty()
        || end_str.eq_ignore_ascii_case("NULL")
        || end_str.eq_ignore_ascii_case("UNBOUNDED")
    {
        None
    } else {
        Some(parse_range_value(end_str, inner_type)?)
    };
    Ok(RangeValue::new(start, end))
}

fn parse_range_value(s: &str, inner_type: &DataType) -> Result<Value> {
    match inner_type {
        DataType::Date => parse_date_string(s).map(Value::Date),
        DataType::DateTime => parse_datetime_string(s).map(Value::DateTime),
        DataType::Timestamp => parse_timestamp_string(s).map(Value::Timestamp),
        _ => Err(Error::InvalidQuery(format!(
            "Unsupported range inner type: {:?}",
            inner_type
        ))),
    }
}

pub fn parse_interval_string(_s: &str) -> Result<IntervalValue> {
    Ok(IntervalValue {
        months: 0,
        days: 0,
        nanos: 0,
    })
}

pub fn interval_from_field(n: i64, field: DateTimeField) -> Result<IntervalValue> {
    match field {
        DateTimeField::Year => Ok(IntervalValue {
            months: n as i32 * 12,
            days: 0,
            nanos: 0,
        }),
        DateTimeField::Month => Ok(IntervalValue {
            months: n as i32,
            days: 0,
            nanos: 0,
        }),
        DateTimeField::Day => Ok(IntervalValue {
            months: 0,
            days: n as i32,
            nanos: 0,
        }),
        DateTimeField::Hour => Ok(IntervalValue {
            months: 0,
            days: 0,
            nanos: n * 3_600_000_000_000,
        }),
        DateTimeField::Minute => Ok(IntervalValue {
            months: 0,
            days: 0,
            nanos: n * 60_000_000_000,
        }),
        DateTimeField::Second => Ok(IntervalValue {
            months: 0,
            days: 0,
            nanos: n * 1_000_000_000,
        }),
        DateTimeField::Millisecond
        | DateTimeField::Microsecond
        | DateTimeField::Nanosecond
        | DateTimeField::Week(_)
        | DateTimeField::IsoWeek
        | DateTimeField::IsoYear
        | DateTimeField::DayOfWeek
        | DateTimeField::DayOfYear
        | DateTimeField::Quarter
        | DateTimeField::Date
        | DateTimeField::Time
        | DateTimeField::Datetime
        | DateTimeField::Timezone
        | DateTimeField::TimezoneHour
        | DateTimeField::TimezoneMinute => Err(Error::InvalidQuery(format!(
            "Invalid interval field: {:?}",
            field
        ))),
    }
}

pub fn add_interval_to_date(date: &NaiveDate, interval: &IntervalValue) -> Result<NaiveDate> {
    use chrono::Months;
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

pub fn add_interval_to_datetime(
    dt: &NaiveDateTime,
    interval: &IntervalValue,
) -> Result<NaiveDateTime> {
    use chrono::Months;
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

pub fn negate_interval(interval: &IntervalValue) -> IntervalValue {
    IntervalValue {
        months: -interval.months,
        days: -interval.days,
        nanos: -interval.nanos,
    }
}

pub fn date_diff_by_part(d1: &NaiveDate, d2: &NaiveDate, part: &str) -> Result<i64> {
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

pub fn trunc_date(date: &NaiveDate, part: &str) -> Result<NaiveDate> {
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

pub fn trunc_datetime(dt: &NaiveDateTime, part: &str) -> Result<NaiveDateTime> {
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

pub fn trunc_time(time: &NaiveTime, part: &str) -> Result<NaiveTime> {
    match part {
        "HOUR" => NaiveTime::from_hms_opt(time.hour(), 0, 0)
            .ok_or_else(|| Error::InvalidQuery("Invalid time".into())),
        "MINUTE" => NaiveTime::from_hms_opt(time.hour(), time.minute(), 0)
            .ok_or_else(|| Error::InvalidQuery("Invalid time".into())),
        _ => Ok(*time),
    }
}

pub fn bucket_date(
    date: &NaiveDate,
    interval: &IntervalValue,
    origin: &NaiveDate,
) -> Result<NaiveDate> {
    let days_since_origin = (*date - *origin).num_days();
    let bucket_days = if interval.days > 0 {
        interval.days as i64
    } else if interval.months > 0 {
        let months_since =
            (date.year() - origin.year()) * 12 + (date.month() as i32 - origin.month() as i32);
        let bucket_count = months_since / interval.months;
        let bucket_start_month = origin.month() as i32 + (bucket_count * interval.months);
        let years_to_add = (bucket_start_month - 1) / 12;
        let month = ((bucket_start_month - 1) % 12) + 1;
        return NaiveDate::from_ymd_opt(origin.year() + years_to_add, month as u32, origin.day())
            .ok_or_else(|| Error::InvalidQuery("Invalid bucket date".into()));
    } else {
        1
    };
    let bucket_count = if days_since_origin >= 0 {
        days_since_origin / bucket_days
    } else {
        (days_since_origin - bucket_days + 1) / bucket_days
    };
    let bucket_start_days = bucket_count * bucket_days;
    Ok(*origin + chrono::Duration::days(bucket_start_days))
}

pub fn bucket_datetime(
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

pub fn format_date_with_pattern(date: &NaiveDate, pattern: &str) -> Result<String> {
    let chrono_pattern = bq_format_to_chrono(pattern);
    Ok(date.format(&chrono_pattern).to_string())
}

pub fn format_datetime_with_pattern(dt: &NaiveDateTime, pattern: &str) -> Result<String> {
    let chrono_pattern = bq_format_to_chrono(pattern);
    Ok(dt.format(&chrono_pattern).to_string())
}

pub fn format_time_with_pattern(time: &NaiveTime, pattern: &str) -> Result<String> {
    let chrono_pattern = bq_format_to_chrono(pattern);
    Ok(time.format(&chrono_pattern).to_string())
}

fn bq_format_to_chrono(bq_format: &str) -> String {
    bq_format
        .replace("%F", "%Y-%m-%d")
        .replace("%T", "%H:%M:%S")
        .replace("%R", "%H:%M")
        .replace("%D", "%m/%d/%y")
        .replace("%Q", "Q%Q")
}

pub fn parse_date_with_pattern(s: &str, pattern: &str) -> Result<NaiveDate> {
    let chrono_pattern = bq_format_to_chrono(pattern);
    NaiveDate::parse_from_str(s, &chrono_pattern).map_err(|e| {
        Error::InvalidQuery(format!(
            "Failed to parse date '{}' with pattern '{}': {}",
            s, pattern, e
        ))
    })
}

pub fn parse_datetime_with_pattern(s: &str, pattern: &str) -> Result<NaiveDateTime> {
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
        return Ok(date.and_hms_opt(0, 0, 0).unwrap());
    }
    Err(Error::InvalidQuery(format!(
        "Failed to parse datetime '{}' with pattern '{}'",
        s, pattern
    )))
}

pub fn parse_time_with_pattern(s: &str, pattern: &str) -> Result<NaiveTime> {
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

pub fn value_to_json(value: &Value) -> Result<serde_json::Value> {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use rust_decimal::prelude::ToPrimitive;

    match value {
        Value::Null => Ok(serde_json::Value::Null),
        Value::Bool(b) => Ok(serde_json::Value::Bool(*b)),
        Value::Int64(n) => Ok(serde_json::Value::Number((*n).into())),
        Value::Float64(f) => {
            let n = serde_json::Number::from_f64(f.0)
                .ok_or_else(|| Error::InvalidQuery("Cannot convert float to JSON".into()))?;
            Ok(serde_json::Value::Number(n))
        }
        Value::String(s) => Ok(serde_json::Value::String(s.clone())),
        Value::Array(arr) => {
            let json_arr: Result<Vec<serde_json::Value>> = arr.iter().map(value_to_json).collect();
            Ok(serde_json::Value::Array(json_arr?))
        }
        Value::Struct(fields) => {
            let mut map = serde_json::Map::new();
            for (name, val) in fields {
                map.insert(name.clone(), value_to_json(val)?);
            }
            Ok(serde_json::Value::Object(map))
        }
        Value::Json(j) => Ok(j.clone()),
        Value::Date(d) => Ok(serde_json::Value::String(d.to_string())),
        Value::Time(t) => Ok(serde_json::Value::String(t.to_string())),
        Value::DateTime(dt) => Ok(serde_json::Value::String(dt.to_string())),
        Value::Timestamp(ts) => Ok(serde_json::Value::String(ts.to_rfc3339())),
        Value::Numeric(n) => {
            if let Some(f) = n.to_f64() {
                let num = serde_json::Number::from_f64(f)
                    .ok_or_else(|| Error::InvalidQuery("Cannot convert numeric to JSON".into()))?;
                Ok(serde_json::Value::Number(num))
            } else {
                Ok(serde_json::Value::String(n.to_string()))
            }
        }
        Value::BigNumeric(n) => {
            if let Some(f) = n.to_f64() {
                let num = serde_json::Number::from_f64(f).ok_or_else(|| {
                    Error::InvalidQuery("Cannot convert bignumeric to JSON".into())
                })?;
                Ok(serde_json::Value::Number(num))
            } else {
                Ok(serde_json::Value::String(n.to_string()))
            }
        }
        Value::Bytes(b) => Ok(serde_json::Value::String(STANDARD.encode(b))),
        Value::Interval(i) => Ok(serde_json::Value::String(format!(
            "{} months, {} days, {} nanos",
            i.months, i.days, i.nanos
        ))),
        Value::Geography(g) => Ok(serde_json::Value::String(g.clone())),
        Value::Range(r) => Ok(serde_json::Value::String(format!(
            "[{:?}, {:?})",
            r.start, r.end
        ))),
        Value::Default => Ok(serde_json::Value::Null),
    }
}

pub fn extract_json_path(
    json: &serde_json::Value,
    path: &str,
) -> Result<Option<serde_json::Value>> {
    let path = path.trim_start_matches('$');
    let mut current = json.clone();

    for segment in path.split('.').filter(|s| !s.is_empty()) {
        let (key, index) = if segment.contains('[') {
            let parts: Vec<&str> = segment.split('[').collect();
            let key = parts[0];
            let idx_str = parts[1].trim_end_matches(']');
            let idx: usize = idx_str.parse().map_err(|_| {
                Error::InvalidQuery(format!("Invalid JSON path index: {}", idx_str))
            })?;
            (key, Some(idx))
        } else {
            (segment, None)
        };

        if !key.is_empty() {
            current = match current {
                serde_json::Value::Object(map) => {
                    map.get(key).cloned().unwrap_or(serde_json::Value::Null)
                }
                _ => return Ok(None),
            };
        }

        if let Some(idx) = index {
            current = match current {
                serde_json::Value::Array(arr) => {
                    arr.get(idx).cloned().unwrap_or(serde_json::Value::Null)
                }
                _ => return Ok(None),
            };
        }
    }

    if current == serde_json::Value::Null {
        Ok(None)
    } else {
        Ok(Some(current))
    }
}
