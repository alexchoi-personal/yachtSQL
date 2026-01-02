#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_strpos(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery("STRPOS requires 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::String(substr)) => Ok(Value::Int64(
            s.find(substr.as_str()).map(|i| i as i64 + 1).unwrap_or(0),
        )),
        _ => Err(Error::InvalidQuery(
            "STRPOS requires string arguments".into(),
        )),
    }
}

pub fn fn_instr(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery("INSTR requires 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::String(substr)) => {
            if substr.is_empty() {
                return Ok(Value::Int64(0));
            }
            let position = args.get(2).and_then(|v| v.as_i64()).unwrap_or(1);
            let occurrence = args.get(3).and_then(|v| v.as_i64()).unwrap_or(1) as usize;

            let chars: Vec<char> = s.chars().collect();
            let substr_chars: Vec<char> = substr.chars().collect();
            let char_len = chars.len();

            if position >= 0 {
                let start_idx = if position == 0 {
                    0
                } else {
                    (position as usize).saturating_sub(1).min(char_len)
                };
                let mut found_count = 0;
                let mut idx = start_idx;
                while idx + substr_chars.len() <= char_len {
                    if chars[idx..idx + substr_chars.len()] == substr_chars[..] {
                        found_count += 1;
                        if found_count == occurrence {
                            return Ok(Value::Int64((idx + 1) as i64));
                        }
                    }
                    idx += 1;
                }
            } else {
                let start_idx = char_len.saturating_sub((-position) as usize);
                let mut idx = start_idx.min(char_len.saturating_sub(substr_chars.len()));
                let mut found_count = 0;
                loop {
                    if chars[idx..idx + substr_chars.len()] == substr_chars[..] {
                        found_count += 1;
                        if found_count == occurrence {
                            return Ok(Value::Int64((idx + 1) as i64));
                        }
                    }
                    if idx == 0 {
                        break;
                    }
                    idx -= 1;
                }
            }
            Ok(Value::Int64(0))
        }
        _ => Err(Error::InvalidQuery(
            "INSTR requires string arguments".into(),
        )),
    }
}

pub fn fn_contains(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery("CONTAINS requires 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::String(substr)) => Ok(Value::Bool(s.contains(substr.as_str()))),
        _ => Err(Error::InvalidQuery(
            "CONTAINS requires string arguments".into(),
        )),
    }
}

pub fn fn_starts_with(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "STARTS_WITH requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::String(prefix)) => {
            Ok(Value::Bool(s.starts_with(prefix.as_str())))
        }
        _ => Err(Error::InvalidQuery(
            "STARTS_WITH requires string arguments".into(),
        )),
    }
}

pub fn fn_ends_with(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery("ENDS_WITH requires 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::String(suffix)) => Ok(Value::Bool(s.ends_with(suffix.as_str()))),
        _ => Err(Error::InvalidQuery(
            "ENDS_WITH requires string arguments".into(),
        )),
    }
}

pub fn fn_contains_substr(args: &[Value]) -> Result<Value> {
    match args {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [haystack_val, Value::String(needle)] => {
            use unicode_normalization::UnicodeNormalization;
            let haystack = value_to_contains_substr_string(haystack_val);
            let normalized_haystack: String = haystack.nfkc().collect();
            let normalized_needle: String = needle.nfkc().collect();
            let result = normalized_haystack
                .to_lowercase()
                .contains(&normalized_needle.to_lowercase());
            Ok(Value::Bool(result))
        }
        _ => Err(Error::InvalidQuery(
            "CONTAINS_SUBSTR requires string second argument".into(),
        )),
    }
}

fn value_to_contains_substr_string(val: &Value) -> String {
    match val {
        Value::Null => "".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => f.to_string(),
        Value::Numeric(d) | Value::BigNumeric(d) => d.to_string(),
        Value::String(s) => s.clone(),
        Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
        Value::Date(d) => d.to_string(),
        Value::Time(t) => t.to_string(),
        Value::DateTime(dt) => dt.to_string(),
        Value::Timestamp(ts) => ts.to_string(),
        Value::Json(j) => j.to_string(),
        Value::Array(arr) => {
            let elements: Vec<String> = arr.iter().map(value_to_contains_substr_string).collect();
            format!("[{}]", elements.join(", "))
        }
        Value::Struct(fields) => {
            let elements: Vec<String> = fields
                .iter()
                .map(|(_, v)| value_to_contains_substr_string(v))
                .collect();
            format!("({})", elements.join(", "))
        }
        Value::Geography(g) => g.clone(),
        Value::Interval(i) => format!("{:?}", i),
        Value::Range(r) => format!("{:?}", r),
        Value::Default => "DEFAULT".to_string(),
    }
}
