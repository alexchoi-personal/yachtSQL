#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_lpad(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "LPAD requires at least 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::Int64(n)) => {
            if *n < 0 {
                return Ok(Value::String(String::new()));
            }
            let n = *n as usize;
            let pad_str = args.get(2).and_then(|v| v.as_str()).unwrap_or(" ");
            let s_chars: Vec<char> = s.chars().collect();
            if s_chars.len() >= n {
                Ok(Value::String(s_chars[..n].iter().collect()))
            } else {
                let pad_len = n - s_chars.len();
                let pad_chars: Vec<char> = pad_str.chars().collect();
                let padded: String = pad_chars
                    .iter()
                    .cycle()
                    .take(pad_len)
                    .chain(s_chars.iter())
                    .collect();
                Ok(Value::String(padded))
            }
        }
        _ => Err(Error::InvalidQuery(
            "LPAD requires string and int arguments".into(),
        )),
    }
}

pub fn fn_rpad(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "RPAD requires at least 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::Int64(n)) => {
            if *n < 0 {
                return Ok(Value::String(String::new()));
            }
            let n = *n as usize;
            let pad_str = args.get(2).and_then(|v| v.as_str()).unwrap_or(" ");
            let s_chars: Vec<char> = s.chars().collect();
            if s_chars.len() >= n {
                Ok(Value::String(s_chars[..n].iter().collect()))
            } else {
                let pad_len = n - s_chars.len();
                let pad_chars: Vec<char> = pad_str.chars().collect();
                let padded: String = s_chars
                    .iter()
                    .chain(pad_chars.iter().cycle().take(pad_len))
                    .collect();
                Ok(Value::String(padded))
            }
        }
        _ => Err(Error::InvalidQuery(
            "RPAD requires string and int arguments".into(),
        )),
    }
}
