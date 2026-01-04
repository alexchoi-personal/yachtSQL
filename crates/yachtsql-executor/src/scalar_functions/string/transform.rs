#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_replace(args: &[Value]) -> Result<Value> {
    if args.len() < 3 {
        return Err(Error::InvalidQuery("REPLACE requires 3 arguments".into()));
    }
    match (&args[0], &args[1], &args[2]) {
        (Value::Null, _, _) => Ok(Value::Null),
        (Value::String(s), Value::String(from), Value::String(to)) => {
            if from.is_empty() {
                Ok(Value::String(s.clone()))
            } else {
                Ok(Value::String(s.replace(from.as_str(), to.as_str())))
            }
        }
        (Value::String(_), Value::String(_), _)
        | (Value::String(_), _, Value::String(_))
        | (Value::String(_), _, _)
        | (_, Value::String(_), Value::String(_))
        | (_, Value::String(_), _)
        | (_, _, Value::String(_))
        | (_, _, _) => Err(Error::InvalidQuery(
            "REPLACE requires string arguments".into(),
        )),
    }
}

pub fn fn_reverse(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => Ok(Value::String(s.chars().rev().collect())),
        Some(Value::Bytes(b)) => Ok(Value::Bytes(b.iter().rev().cloned().collect())),
        Some(Value::Array(a)) => Ok(Value::Array(a.iter().rev().cloned().collect())),
        Some(Value::Bool(_))
        | Some(Value::Int64(_))
        | Some(Value::Float64(_))
        | Some(Value::Numeric(_))
        | Some(Value::BigNumeric(_))
        | Some(Value::Date(_))
        | Some(Value::Time(_))
        | Some(Value::DateTime(_))
        | Some(Value::Timestamp(_))
        | Some(Value::Json(_))
        | Some(Value::Struct(_))
        | Some(Value::Geography(_))
        | Some(Value::Interval(_))
        | Some(Value::Range(_))
        | Some(Value::Default)
        | None => Err(Error::InvalidQuery(
            "REVERSE requires string/bytes/array argument".into(),
        )),
    }
}

const MAX_REPEAT_OUTPUT_LEN: usize = 10 * 1024 * 1024;

pub fn fn_repeat(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery("REPEAT requires 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::Int64(n)) => {
            if *n < 0 {
                return Err(Error::InvalidQuery(
                    "REPEAT count must be non-negative".into(),
                ));
            }
            let count = *n as usize;
            let output_len = s.len().saturating_mul(count);
            if output_len > MAX_REPEAT_OUTPUT_LEN {
                return Err(Error::InvalidQuery(format!(
                    "REPEAT would produce {} bytes, exceeding maximum of {} bytes",
                    output_len, MAX_REPEAT_OUTPUT_LEN
                )));
            }
            Ok(Value::String(s.repeat(count)))
        }
        (Value::String(_), _) | (_, Value::Int64(_)) | (_, _) => Err(Error::InvalidQuery(
            "REPEAT requires string and int arguments".into(),
        )),
    }
}

pub fn fn_translate(args: &[Value]) -> Result<Value> {
    if args.len() < 3 {
        return Err(Error::InvalidQuery("TRANSLATE requires 3 arguments".into()));
    }
    match (&args[0], &args[1], &args[2]) {
        (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => Ok(Value::Null),
        (Value::String(source), Value::String(from_chars), Value::String(to_chars)) => {
            let from: Vec<char> = from_chars.chars().collect();
            let to: Vec<char> = to_chars.chars().collect();
            let result: String = source
                .chars()
                .filter_map(|c| {
                    if let Some(pos) = from.iter().position(|&fc| fc == c) {
                        to.get(pos).copied()
                    } else {
                        Some(c)
                    }
                })
                .collect();
            Ok(Value::String(result))
        }
        (Value::String(_), Value::String(_), _)
        | (Value::String(_), _, Value::String(_))
        | (Value::String(_), _, _)
        | (_, Value::String(_), Value::String(_))
        | (_, Value::String(_), _)
        | (_, _, Value::String(_))
        | (_, _, _) => Err(Error::InvalidQuery(
            "TRANSLATE requires string arguments".into(),
        )),
    }
}

pub fn fn_split(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery(
            "SPLIT requires at least 1 argument".into(),
        ));
    }
    let delimiter = args.get(1).and_then(|v| v.as_str()).unwrap_or(",");
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(s) => {
            let parts: Vec<Value> = if delimiter.is_empty() {
                s.chars().map(|c| Value::String(c.to_string())).collect()
            } else {
                s.split(delimiter)
                    .map(|p| Value::String(p.to_string()))
                    .collect()
            };
            Ok(Value::Array(parts))
        }
        Value::Bool(_)
        | Value::Int64(_)
        | Value::Float64(_)
        | Value::Numeric(_)
        | Value::BigNumeric(_)
        | Value::Bytes(_)
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
        | Value::Default => Err(Error::InvalidQuery("SPLIT requires string argument".into())),
    }
}

pub fn fn_concat(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::String(String::new()));
    }
    let first = &args[0];
    let is_bytes = matches!(first, Value::Bytes(_));

    if is_bytes {
        let mut result: Vec<u8> = Vec::new();
        for arg in args {
            match arg {
                Value::Null => return Ok(Value::Null),
                Value::Bytes(b) => result.extend(b),
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
                | Value::Default => {
                    return Err(Error::InvalidQuery(
                        "CONCAT with BYTES requires all arguments to be BYTES".into(),
                    ));
                }
            }
        }
        Ok(Value::Bytes(result))
    } else {
        let mut result = String::new();
        for arg in args {
            match arg {
                Value::Null => return Ok(Value::Null),
                Value::String(s) => result.push_str(s),
                Value::Bool(_)
                | Value::Int64(_)
                | Value::Float64(_)
                | Value::Numeric(_)
                | Value::BigNumeric(_)
                | Value::Bytes(_)
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
                | Value::Default => {
                    return Err(Error::InvalidQuery(
                        "CONCAT requires STRING arguments".into(),
                    ));
                }
            }
        }
        Ok(Value::String(result))
    }
}
