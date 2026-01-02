#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_string(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Json(j)) => {
            if let Some(s) = j.as_str() {
                Ok(Value::String(s.to_string()))
            } else {
                Ok(Value::String(j.to_string()))
            }
        }
        Some(v) => Ok(Value::String(format!("{}", v))),
        None => Err(Error::InvalidQuery("STRING requires an argument".into())),
    }
}

pub fn fn_safe_convert_bytes_to_string(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::InvalidQuery(
            "SAFE_CONVERT_BYTES_TO_STRING requires 1 argument".into(),
        ));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Bytes(b) => match String::from_utf8(b.clone()) {
            Ok(s) => Ok(Value::String(s)),
            Err(_) => Ok(Value::Null),
        },
        Value::Bool(_)
        | Value::Int64(_)
        | Value::Float64(_)
        | Value::Numeric(_)
        | Value::BigNumeric(_)
        | Value::String(_)
        | Value::Date(_)
        | Value::Time(_)
        | Value::DateTime(_)
        | Value::Timestamp(_)
        | Value::Json(_)
        | Value::Array(_)
        | Value::Struct(_)
        | Value::Geography(_)
        | Value::Interval(_)
        | Value::Range(_)
        | Value::Default => Err(Error::InvalidQuery(
            "SAFE_CONVERT_BYTES_TO_STRING requires BYTES argument".into(),
        )),
    }
}

pub fn fn_convert_bytes_to_string(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::InvalidQuery(
            "CONVERT_BYTES_TO_STRING requires 1 argument".into(),
        ));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Bytes(b) => {
            let s = String::from_utf8(b.clone())
                .map_err(|e| Error::InvalidQuery(format!("Invalid UTF-8 in bytes: {}", e)))?;
            Ok(Value::String(s))
        }
        Value::Bool(_)
        | Value::Int64(_)
        | Value::Float64(_)
        | Value::Numeric(_)
        | Value::BigNumeric(_)
        | Value::String(_)
        | Value::Date(_)
        | Value::Time(_)
        | Value::DateTime(_)
        | Value::Timestamp(_)
        | Value::Json(_)
        | Value::Array(_)
        | Value::Struct(_)
        | Value::Geography(_)
        | Value::Interval(_)
        | Value::Range(_)
        | Value::Default => Err(Error::InvalidQuery(
            "CONVERT_BYTES_TO_STRING requires BYTES argument".into(),
        )),
    }
}

pub fn fn_type_of(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::String("NULL".to_string())),
        Some(Value::Bool(_)) => Ok(Value::String("BOOL".to_string())),
        Some(Value::Int64(_)) => Ok(Value::String("INT64".to_string())),
        Some(Value::Float64(_)) => Ok(Value::String("FLOAT64".to_string())),
        Some(Value::String(_)) => Ok(Value::String("STRING".to_string())),
        Some(Value::Bytes(_)) => Ok(Value::String("BYTES".to_string())),
        Some(Value::Date(_)) => Ok(Value::String("DATE".to_string())),
        Some(Value::Time(_)) => Ok(Value::String("TIME".to_string())),
        Some(Value::DateTime(_)) => Ok(Value::String("DATETIME".to_string())),
        Some(Value::Timestamp(_)) => Ok(Value::String("TIMESTAMP".to_string())),
        Some(Value::Array(_)) => Ok(Value::String("ARRAY".to_string())),
        Some(Value::Struct(_)) => Ok(Value::String("STRUCT".to_string())),
        Some(Value::Json(_)) => Ok(Value::String("JSON".to_string())),
        Some(Value::Numeric(_)) => Ok(Value::String("NUMERIC".to_string())),
        Some(Value::BigNumeric(_)) => Ok(Value::String("BIGNUMERIC".to_string())),
        Some(Value::Interval(_)) => Ok(Value::String("INTERVAL".to_string())),
        Some(Value::Geography(_)) => Ok(Value::String("GEOGRAPHY".to_string())),
        Some(Value::Range(_)) => Ok(Value::String("RANGE".to_string())),
        Some(Value::Default) => Ok(Value::String("DEFAULT".to_string())),
        None => Ok(Value::Null),
    }
}

pub fn fn_bit_count(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery("BIT_COUNT requires 1 argument".into()));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Int64(n) => {
            let count = n.count_ones() as i64;
            Ok(Value::Int64(count))
        }
        Value::Bytes(bytes) => {
            let count: u32 = bytes.iter().map(|b| b.count_ones()).sum();
            Ok(Value::Int64(count as i64))
        }
        Value::Bool(_)
        | Value::Float64(_)
        | Value::Numeric(_)
        | Value::BigNumeric(_)
        | Value::String(_)
        | Value::Date(_)
        | Value::Time(_)
        | Value::DateTime(_)
        | Value::Timestamp(_)
        | Value::Json(_)
        | Value::Array(_)
        | Value::Struct(_)
        | Value::Geography(_)
        | Value::Interval(_)
        | Value::Range(_)
        | Value::Default => Err(Error::InvalidQuery(
            "BIT_COUNT requires an integer or bytes argument".into(),
        )),
    }
}

pub fn fn_safe_cast(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery("SAFE_CAST requires an argument".into()));
    }
    Ok(args[0].clone())
}

pub fn fn_cast(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery("CAST requires an argument".into()));
    }
    Ok(args[0].clone())
}

pub fn fn_struct(args: &[Value]) -> Result<Value> {
    let fields: Vec<(String, Value)> = args
        .iter()
        .enumerate()
        .map(|(i, v)| (format!("_{}", i), v.clone()))
        .collect();
    Ok(Value::Struct(fields))
}

pub fn fn_safe_convert(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery(
            "SAFE_CONVERT requires an argument".into(),
        ));
    }
    Ok(args[0].clone())
}

pub fn fn_to_base64(args: &[Value]) -> Result<Value> {
    use base64::Engine;
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Bytes(b)) => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(b);
            Ok(Value::String(encoded))
        }
        Some(Value::String(s)) => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(s.as_bytes());
            Ok(Value::String(encoded))
        }
        _ => Err(Error::InvalidQuery(
            "TO_BASE64 requires BYTES or STRING argument".into(),
        )),
    }
}

pub fn fn_from_base64(args: &[Value]) -> Result<Value> {
    use base64::Engine;
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(s)
                .map_err(|e| Error::InvalidQuery(format!("Invalid base64: {}", e)))?;
            Ok(Value::Bytes(decoded))
        }
        _ => Err(Error::InvalidQuery(
            "FROM_BASE64 requires STRING argument".into(),
        )),
    }
}

pub fn fn_to_base32(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Bytes(b)) => {
            let encoded = data_encoding::BASE32.encode(b);
            Ok(Value::String(encoded))
        }
        Some(Value::String(s)) => {
            let encoded = data_encoding::BASE32.encode(s.as_bytes());
            Ok(Value::String(encoded))
        }
        _ => Err(Error::InvalidQuery(
            "TO_BASE32 requires BYTES or STRING argument".into(),
        )),
    }
}

pub fn fn_from_base32(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let decoded = data_encoding::BASE32
                .decode(s.as_bytes())
                .map_err(|e| Error::InvalidQuery(format!("Invalid base32: {}", e)))?;
            Ok(Value::Bytes(decoded))
        }
        _ => Err(Error::InvalidQuery(
            "FROM_BASE32 requires STRING argument".into(),
        )),
    }
}

pub fn fn_to_hex(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Bytes(b)) => {
            let encoded = hex::encode(b);
            Ok(Value::String(encoded))
        }
        Some(Value::String(s)) => {
            let encoded = hex::encode(s.as_bytes());
            Ok(Value::String(encoded))
        }
        Some(Value::Int64(n)) => {
            let encoded = format!("{:x}", n);
            Ok(Value::String(encoded))
        }
        _ => Err(Error::InvalidQuery(
            "TO_HEX requires BYTES, STRING, or INT64 argument".into(),
        )),
    }
}

pub fn fn_from_hex(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let hex_str = if s.len() % 2 == 1 {
                format!("0{}", s)
            } else {
                s.clone()
            };
            let decoded = hex::decode(&hex_str)
                .map_err(|e| Error::InvalidQuery(format!("Invalid hex: {}", e)))?;
            Ok(Value::Bytes(decoded))
        }
        _ => Err(Error::InvalidQuery(
            "FROM_HEX requires STRING argument".into(),
        )),
    }
}
