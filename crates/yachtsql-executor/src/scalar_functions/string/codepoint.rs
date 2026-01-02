#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_ascii(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let code = s.chars().next().map(|c| c as i64).unwrap_or(0);
            Ok(Value::Int64(code))
        }
        _ => Err(Error::InvalidQuery("ASCII requires string argument".into())),
    }
}

pub fn fn_chr(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Int64(n)) => {
            if *n == 0 {
                return Ok(Value::String(String::new()));
            }
            let c = char::from_u32(*n as u32).unwrap_or('\0');
            Ok(Value::String(c.to_string()))
        }
        _ => Err(Error::InvalidQuery("CHR requires integer argument".into())),
    }
}

pub fn fn_unicode(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let code = s.chars().next().map(|c| c as i64).unwrap_or(0);
            Ok(Value::Int64(code))
        }
        _ => Err(Error::InvalidQuery(
            "UNICODE requires string argument".into(),
        )),
    }
}

pub fn fn_to_code_points(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let code_points: Vec<Value> = s.chars().map(|c| Value::Int64(c as i64)).collect();
            Ok(Value::Array(code_points))
        }
        _ => Err(Error::InvalidQuery(
            "TO_CODE_POINTS requires string argument".into(),
        )),
    }
}

pub fn fn_code_points_to_string(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Array(arr)) => {
            let mut result = String::new();
            for v in arr {
                match v {
                    Value::Null => return Ok(Value::Null),
                    Value::Int64(n) => {
                        if let Some(c) = char::from_u32(*n as u32) {
                            result.push(c);
                        }
                    }
                    _ => {
                        return Err(Error::InvalidQuery(
                            "CODE_POINTS_TO_STRING requires array of integers".into(),
                        ));
                    }
                }
            }
            Ok(Value::String(result))
        }
        _ => Err(Error::InvalidQuery(
            "CODE_POINTS_TO_STRING requires array argument".into(),
        )),
    }
}

pub fn fn_code_points_to_bytes(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Array(arr)) => {
            let mut result = Vec::new();
            for v in arr {
                match v {
                    Value::Null => return Ok(Value::Null),
                    Value::Int64(n) => {
                        if *n < 0 || *n > 255 {
                            return Err(Error::InvalidQuery(format!(
                                "CODE_POINTS_TO_BYTES: value {} out of range 0-255",
                                n
                            )));
                        }
                        result.push(*n as u8);
                    }
                    _ => {
                        return Err(Error::InvalidQuery(
                            "CODE_POINTS_TO_BYTES requires array of integers".into(),
                        ));
                    }
                }
            }
            Ok(Value::Bytes(result))
        }
        _ => Err(Error::InvalidQuery(
            "CODE_POINTS_TO_BYTES requires array argument".into(),
        )),
    }
}
