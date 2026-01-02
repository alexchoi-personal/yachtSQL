#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_substr(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery(
            "SUBSTR requires at least 1 argument".into(),
        ));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(s) => {
            let start_raw = args.get(1).and_then(|v| v.as_i64()).unwrap_or(1);
            let len = args.get(2).and_then(|v| v.as_i64()).map(|l| l as usize);
            let chars: Vec<char> = s.chars().collect();
            let char_len = chars.len();

            let start_idx = if start_raw < 0 {
                char_len.saturating_sub((-start_raw) as usize)
            } else if start_raw == 0 {
                0
            } else {
                (start_raw as usize).saturating_sub(1).min(char_len)
            };

            let end_idx = len
                .map(|l| (start_idx + l).min(char_len))
                .unwrap_or(char_len);
            Ok(Value::String(chars[start_idx..end_idx].iter().collect()))
        }
        Value::Bytes(b) => {
            let start_raw = args.get(1).and_then(|v| v.as_i64()).unwrap_or(1);
            let len = args.get(2).and_then(|v| v.as_i64()).map(|l| l as usize);
            let byte_len = b.len();

            let start_idx = if start_raw < 0 {
                byte_len.saturating_sub((-start_raw) as usize)
            } else if start_raw == 0 {
                0
            } else {
                (start_raw as usize).saturating_sub(1).min(byte_len)
            };

            let end_idx = len
                .map(|l| (start_idx + l).min(byte_len))
                .unwrap_or(byte_len);
            Ok(Value::Bytes(b[start_idx..end_idx].to_vec()))
        }
        _ => Err(Error::InvalidQuery(
            "SUBSTR requires string or bytes argument".into(),
        )),
    }
}

pub fn fn_left(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery("LEFT requires 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::Int64(n)) => {
            let chars: Vec<char> = s.chars().collect();
            let n = (*n as usize).min(chars.len());
            Ok(Value::String(chars[..n].iter().collect()))
        }
        (Value::Bytes(b), Value::Int64(n)) => {
            let n = (*n as usize).min(b.len());
            Ok(Value::Bytes(b[..n].to_vec()))
        }
        _ => Err(Error::InvalidQuery(
            "LEFT requires string/bytes and int arguments".into(),
        )),
    }
}

pub fn fn_right(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery("RIGHT requires 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::Int64(n)) => {
            let chars: Vec<char> = s.chars().collect();
            let n = (*n as usize).min(chars.len());
            let start = chars.len().saturating_sub(n);
            Ok(Value::String(chars[start..].iter().collect()))
        }
        (Value::Bytes(b), Value::Int64(n)) => {
            let n = (*n as usize).min(b.len());
            let start = b.len().saturating_sub(n);
            Ok(Value::Bytes(b[start..].to_vec()))
        }
        _ => Err(Error::InvalidQuery(
            "RIGHT requires string/bytes and int arguments".into(),
        )),
    }
}

pub fn fn_substring(args: &[Value]) -> Result<Value> {
    fn_substr(args)
}
