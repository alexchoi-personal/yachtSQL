#![coverage(off)]

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, RangeValue, Value};
use yachtsql_storage::Column;

pub fn eval_typed_string(type_name: &DataType, value: &str, row_count: usize) -> Result<Column> {
    let parsed_value = match type_name {
        DataType::Date => {
            let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .map_err(|e| Error::InvalidQuery(format!("Invalid date: {}", e)))?;
            Value::Date(date)
        }
        DataType::Time => {
            let time = NaiveTime::parse_from_str(value, "%H:%M:%S")
                .or_else(|_| NaiveTime::parse_from_str(value, "%H:%M:%S%.f"))
                .map_err(|e| Error::InvalidQuery(format!("Invalid time: {}", e)))?;
            Value::Time(time)
        }
        DataType::DateTime => {
            let dt = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S"))
                .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"))
                .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f"))
                .map_err(|e| Error::InvalidQuery(format!("Invalid datetime: {}", e)))?;
            Value::DateTime(dt)
        }
        DataType::Timestamp => {
            let try_parse_with_tz = |v: &str| -> Option<chrono::DateTime<Utc>> {
                let tz_formats = [
                    "%Y-%m-%d %H:%M:%S%z",
                    "%Y-%m-%d %H:%M:%S%:z",
                    "%Y-%m-%d %H:%M:%S%.f%z",
                    "%Y-%m-%d %H:%M:%S%.f%:z",
                    "%Y-%m-%dT%H:%M:%S%z",
                    "%Y-%m-%dT%H:%M:%S%:z",
                ];
                for fmt in &tz_formats {
                    if let Ok(dt) = chrono::DateTime::parse_from_str(v, fmt) {
                        return Some(dt.with_timezone(&Utc));
                    }
                }
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(v) {
                    return Some(dt.with_timezone(&Utc));
                }
                None
            };

            let normalized = if value.ends_with(" UTC") {
                format!("{}+00:00", &value[..value.len() - 4])
            } else if value.len() > 3
                && (value.ends_with("+00")
                    || value.ends_with("-00")
                    || value.chars().rev().nth(2) == Some('+')
                    || value.chars().rev().nth(2) == Some('-'))
                && !value.contains(':')
                || (value.len() > 3 && value.chars().filter(|c| *c == ':').count() == 2)
            {
                if let Some(pos) = value.rfind('+').or_else(|| value.rfind('-')) {
                    let (base, tz) = value.split_at(pos);
                    if tz.len() == 3 {
                        format!("{}{}:00", base, tz)
                    } else {
                        value.to_string()
                    }
                } else {
                    value.to_string()
                }
            } else {
                value.to_string()
            };

            if let Some(ts) = try_parse_with_tz(&normalized) {
                Value::Timestamp(ts)
            } else if let Some(ts) = try_parse_with_tz(value) {
                Value::Timestamp(ts)
            } else if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
                let dt = date.and_hms_opt(0, 0, 0).unwrap();
                let ts = Utc.from_utc_datetime(&dt);
                Value::Timestamp(ts)
            } else {
                let dt = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                    .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S"))
                    .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"))
                    .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f"))
                    .map_err(|e| Error::InvalidQuery(format!("Invalid timestamp: {}", e)))?;
                let ts = Utc.from_utc_datetime(&dt);
                Value::Timestamp(ts)
            }
        }
        DataType::Numeric(_) => {
            let num = rust_decimal::Decimal::from_str_exact(value)
                .or_else(|_| value.parse::<rust_decimal::Decimal>())
                .map_err(|e| Error::InvalidQuery(format!("Invalid numeric: {}", e)))?;
            Value::Numeric(num)
        }
        DataType::BigNumeric => {
            let num = rust_decimal::Decimal::from_str_exact(value)
                .or_else(|_| value.parse::<rust_decimal::Decimal>())
                .map_err(|e| Error::InvalidQuery(format!("Invalid bignumeric: {}", e)))?;
            Value::BigNumeric(num)
        }
        DataType::Json => {
            let json: serde_json::Value = serde_json::from_str(value)
                .map_err(|e| Error::InvalidQuery(format!("Invalid JSON: {}", e)))?;
            Value::Json(json)
        }
        DataType::Range(inner_type) => parse_range_literal(value, Some(inner_type.as_ref()))?,
        DataType::Unknown
        | DataType::Bool
        | DataType::Int64
        | DataType::Float64
        | DataType::String
        | DataType::Bytes
        | DataType::Geography
        | DataType::Struct(_)
        | DataType::Array(_)
        | DataType::Interval => Value::String(value.to_string()),
    };

    Ok(Column::broadcast(parsed_value, row_count))
}

fn parse_range_literal(value: &str, inner_type: Option<&DataType>) -> Result<Value> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed == "NULL" {
        return Ok(Value::Null);
    }

    let (start_inclusive, content, end_inclusive) = if trimmed.starts_with('[') {
        let end_char = trimmed.chars().last().unwrap_or(')');
        let end_inclusive = end_char == ']';
        let content = &trimmed[1..trimmed.len() - 1];
        (true, content, end_inclusive)
    } else if trimmed.starts_with('(') {
        let end_char = trimmed.chars().last().unwrap_or(')');
        let end_inclusive = end_char == ']';
        let content = &trimmed[1..trimmed.len() - 1];
        (false, content, end_inclusive)
    } else {
        return Err(Error::InvalidQuery(format!(
            "Invalid range literal: {}",
            value
        )));
    };

    let parts: Vec<&str> = content.splitn(2, ',').collect();
    if parts.len() != 2 {
        return Err(Error::InvalidQuery(format!(
            "Invalid range literal: {}",
            value
        )));
    }

    let start_str = parts[0].trim();
    let end_str = parts[1].trim();

    let _ = start_inclusive;
    let _ = end_inclusive;

    let start = if start_str.is_empty() || start_str == "NULL" || start_str == "UNBOUNDED" {
        None
    } else {
        Some(Box::new(parse_range_element(start_str, inner_type)?))
    };

    let end = if end_str.is_empty() || end_str == "NULL" || end_str == "UNBOUNDED" {
        None
    } else {
        Some(Box::new(parse_range_element(end_str, inner_type)?))
    };

    Ok(Value::Range(RangeValue { start, end }))
}

fn parse_range_element(value: &str, inner_type: Option<&DataType>) -> Result<Value> {
    match inner_type {
        Some(DataType::Date) => {
            let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .map_err(|e| Error::InvalidQuery(format!("Invalid date in range: {}", e)))?;
            Ok(Value::Date(date))
        }
        Some(DataType::DateTime) => {
            let dt = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S"))
                .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"))
                .map_err(|e| Error::InvalidQuery(format!("Invalid datetime in range: {}", e)))?;
            Ok(Value::DateTime(dt))
        }
        Some(DataType::Timestamp) => {
            if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
                let dt = date.and_hms_opt(0, 0, 0).unwrap();
                let ts = Utc.from_utc_datetime(&dt);
                return Ok(Value::Timestamp(ts));
            }
            let dt = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S"))
                .map_err(|e| Error::InvalidQuery(format!("Invalid timestamp in range: {}", e)))?;
            let ts = Utc.from_utc_datetime(&dt);
            Ok(Value::Timestamp(ts))
        }
        Some(DataType::Int64) => {
            let n = value
                .parse::<i64>()
                .map_err(|e| Error::InvalidQuery(format!("Invalid int64 in range: {}", e)))?;
            Ok(Value::Int64(n))
        }
        Some(DataType::Float64) => {
            let f = value
                .parse::<f64>()
                .map_err(|e| Error::InvalidQuery(format!("Invalid float64 in range: {}", e)))?;
            Ok(Value::float64(f))
        }
        _ => Ok(Value::String(value.to_string())),
    }
}
