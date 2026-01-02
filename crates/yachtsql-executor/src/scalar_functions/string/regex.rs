#![coverage(off)]

use regex::Regex;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_regexp_contains(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "REGEXP_CONTAINS requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::String(pattern)) => {
            let re = Regex::new(pattern)
                .map_err(|e| Error::InvalidQuery(format!("Invalid regex: {}", e)))?;
            Ok(Value::Bool(re.is_match(s)))
        }
        _ => Err(Error::InvalidQuery(
            "REGEXP_CONTAINS requires string arguments".into(),
        )),
    }
}

pub fn fn_regexp_extract(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "REGEXP_EXTRACT requires 2 arguments".into(),
        ));
    }
    let group_num = args.get(2).and_then(|v| v.as_i64()).unwrap_or(1) as usize;
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::String(pattern)) => {
            let re = Regex::new(pattern)
                .map_err(|e| Error::InvalidQuery(format!("Invalid regex: {}", e)))?;
            match re.captures(s) {
                Some(caps) => {
                    let matched = caps
                        .get(group_num)
                        .or_else(|| caps.get(0))
                        .map(|m| m.as_str().to_string());
                    Ok(matched.map(Value::String).unwrap_or(Value::Null))
                }
                None => Ok(Value::Null),
            }
        }
        _ => Err(Error::InvalidQuery(
            "REGEXP_EXTRACT requires string arguments".into(),
        )),
    }
}

pub fn fn_regexp_extract_all(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "REGEXP_EXTRACT_ALL requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::String(pattern)) => {
            let re = Regex::new(pattern)
                .map_err(|e| Error::InvalidQuery(format!("Invalid regex: {}", e)))?;
            let matches: Vec<Value> = re
                .captures_iter(s)
                .filter_map(|caps| {
                    caps.get(1)
                        .or_else(|| caps.get(0))
                        .map(|m| Value::String(m.as_str().to_string()))
                })
                .collect();
            Ok(Value::Array(matches))
        }
        _ => Err(Error::InvalidQuery(
            "REGEXP_EXTRACT_ALL requires string arguments".into(),
        )),
    }
}

pub fn fn_regexp_instr(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "REGEXP_INSTR requires source and pattern arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(source), Value::String(pattern)) => match Regex::new(pattern) {
            Ok(re) => {
                if let Some(m) = re.find(source) {
                    Ok(Value::Int64((m.start() + 1) as i64))
                } else {
                    Ok(Value::Int64(0))
                }
            }
            Err(_) => Ok(Value::Int64(0)),
        },
        _ => Err(Error::InvalidQuery(
            "REGEXP_INSTR expects string arguments".into(),
        )),
    }
}

pub fn fn_regexp_replace(args: &[Value]) -> Result<Value> {
    if args.len() < 3 {
        return Err(Error::InvalidQuery(
            "REGEXP_REPLACE requires 3 arguments".into(),
        ));
    }
    match (&args[0], &args[1], &args[2]) {
        (Value::Null, _, _) => Ok(Value::Null),
        (Value::String(s), Value::String(pattern), Value::String(replacement)) => {
            let re = Regex::new(pattern)
                .map_err(|e| Error::InvalidQuery(format!("Invalid regex: {}", e)))?;
            let rust_replacement = replacement
                .replace("\\1", "$1")
                .replace("\\2", "$2")
                .replace("\\3", "$3")
                .replace("\\4", "$4")
                .replace("\\5", "$5")
                .replace("\\6", "$6")
                .replace("\\7", "$7")
                .replace("\\8", "$8")
                .replace("\\9", "$9");
            Ok(Value::String(
                re.replace_all(s, rust_replacement.as_str()).to_string(),
            ))
        }
        _ => Err(Error::InvalidQuery(
            "REGEXP_REPLACE requires string arguments".into(),
        )),
    }
}

pub fn fn_regexp_substr(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "REGEXP_SUBSTR requires source and pattern arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(source), Value::String(pattern)) => match Regex::new(pattern) {
            Ok(re) => {
                if let Some(m) = re.find(source) {
                    Ok(Value::String(m.as_str().to_string()))
                } else {
                    Ok(Value::Null)
                }
            }
            Err(_) => Ok(Value::Null),
        },
        _ => Err(Error::InvalidQuery(
            "REGEXP_SUBSTR expects string arguments".into(),
        )),
    }
}
