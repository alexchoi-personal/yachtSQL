#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_length(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => Ok(Value::Int64(s.chars().count() as i64)),
        Some(Value::Bytes(b)) => Ok(Value::Int64(b.len() as i64)),
        Some(Value::Array(a)) => Ok(Value::Int64(a.len() as i64)),
        _ => Err(Error::InvalidQuery(
            "LENGTH requires string, bytes, or array argument".into(),
        )),
    }
}

pub fn fn_byte_length(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => Ok(Value::Int64(s.len() as i64)),
        Some(Value::Bytes(b)) => Ok(Value::Int64(b.len() as i64)),
        _ => Err(Error::InvalidQuery(
            "BYTE_LENGTH requires string or bytes argument".into(),
        )),
    }
}

pub fn fn_char_length(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => Ok(Value::Int64(s.chars().count() as i64)),
        _ => Err(Error::InvalidQuery(
            "CHAR_LENGTH requires string argument".into(),
        )),
    }
}
