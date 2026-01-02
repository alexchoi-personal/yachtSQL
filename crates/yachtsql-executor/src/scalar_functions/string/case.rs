#![coverage(off)]

use std::collections::HashSet;

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_upper(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => Ok(Value::String(s.to_uppercase())),
        _ => Err(Error::InvalidQuery("UPPER requires string argument".into())),
    }
}

pub fn fn_lower(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => Ok(Value::String(s.to_lowercase())),
        _ => Err(Error::InvalidQuery("LOWER requires string argument".into())),
    }
}

pub fn fn_initcap(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let delimiters: HashSet<char> = args
                .get(1)
                .and_then(|v| v.as_str())
                .map(|d| d.chars().collect())
                .unwrap_or_else(|| " \t\n\r-_!@#$%^&*()+=[]{}|;:',.<>?/~`".chars().collect());

            let mut result = String::new();
            let mut capitalize_next = true;
            for c in s.chars() {
                if delimiters.contains(&c) {
                    result.push(c);
                    capitalize_next = true;
                } else if capitalize_next {
                    result.extend(c.to_uppercase());
                    capitalize_next = false;
                } else {
                    result.extend(c.to_lowercase());
                }
            }
            Ok(Value::String(result))
        }
        _ => Err(Error::InvalidQuery(
            "INITCAP requires string argument".into(),
        )),
    }
}
