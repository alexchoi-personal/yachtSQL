#![coverage(off)]

use std::collections::HashSet;

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_trim(args: &[Value]) -> Result<Value> {
    match args {
        [Value::Null, ..] => Ok(Value::Null),
        [Value::String(s)] => Ok(Value::String(s.trim().to_string())),
        [Value::String(s), Value::String(chars)] => {
            let char_set: HashSet<char> = chars.chars().collect();
            let result = s
                .trim_start_matches(|c| char_set.contains(&c))
                .trim_end_matches(|c| char_set.contains(&c))
                .to_string();
            Ok(Value::String(result))
        }
        _ => Err(Error::InvalidQuery("TRIM requires string argument".into())),
    }
}

pub fn fn_ltrim(args: &[Value]) -> Result<Value> {
    match args {
        [Value::Null, ..] => Ok(Value::Null),
        [Value::String(s)] => Ok(Value::String(s.trim_start().to_string())),
        [Value::String(s), Value::String(chars)] => {
            let char_set: HashSet<char> = chars.chars().collect();
            let result = s.trim_start_matches(|c| char_set.contains(&c)).to_string();
            Ok(Value::String(result))
        }
        _ => Err(Error::InvalidQuery("LTRIM requires string argument".into())),
    }
}

pub fn fn_rtrim(args: &[Value]) -> Result<Value> {
    match args {
        [Value::Null, ..] => Ok(Value::Null),
        [Value::String(s)] => Ok(Value::String(s.trim_end().to_string())),
        [Value::String(s), Value::String(chars)] => {
            let char_set: HashSet<char> = chars.chars().collect();
            let result = s.trim_end_matches(|c| char_set.contains(&c)).to_string();
            Ok(Value::String(result))
        }
        _ => Err(Error::InvalidQuery("RTRIM requires string argument".into())),
    }
}

pub fn fn_btrim(args: &[Value]) -> Result<Value> {
    match args {
        [Value::Null, ..] => Ok(Value::Null),
        [Value::String(s)] => Ok(Value::String(s.trim().to_string())),
        [Value::String(s), Value::String(chars)] => {
            let char_set: HashSet<char> = chars.chars().collect();
            let result = s
                .trim_start_matches(|c| char_set.contains(&c))
                .trim_end_matches(|c| char_set.contains(&c))
                .to_string();
            Ok(Value::String(result))
        }
        _ => Err(Error::InvalidQuery("BTRIM requires string argument".into())),
    }
}
