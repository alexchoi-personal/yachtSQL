#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_soundex(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            if s.is_empty() {
                return Ok(Value::String(String::new()));
            }
            let mut result = String::new();
            let mut chars = s.chars().filter(|c| c.is_ascii_alphabetic());
            if let Some(first) = chars.next() {
                result.push(first.to_ascii_uppercase());
            }
            let get_code = |c: char| -> Option<char> {
                match c.to_ascii_lowercase() {
                    'b' | 'f' | 'p' | 'v' => Some('1'),
                    'c' | 'g' | 'j' | 'k' | 'q' | 's' | 'x' | 'z' => Some('2'),
                    'd' | 't' => Some('3'),
                    'l' => Some('4'),
                    'm' | 'n' => Some('5'),
                    'r' => Some('6'),
                    _ => None,
                }
            };
            let is_hw = |c: char| matches!(c.to_ascii_lowercase(), 'h' | 'w');
            let mut last_code: Option<char> = None;
            for c in chars {
                if result.len() >= 4 {
                    break;
                }
                if is_hw(c) {
                    continue;
                }
                if let Some(code) = get_code(c) {
                    if Some(code) != last_code {
                        result.push(code);
                        last_code = Some(code);
                    }
                } else {
                    last_code = None;
                }
            }
            while result.len() < 4 {
                result.push('0');
            }
            Ok(Value::String(result))
        }
        _ => Err(Error::InvalidQuery(
            "SOUNDEX requires string argument".into(),
        )),
    }
}

const MAX_EDIT_DISTANCE_STRING_LEN: usize = 10_000;

pub fn fn_edit_distance(args: &[Value]) -> Result<Value> {
    let (s1, s2, max_distance) = match args {
        [Value::Null, ..] | [_, Value::Null, ..] => return Ok(Value::Null),
        [Value::String(a), Value::String(b)] => (a.as_str(), b.as_str(), None),
        [Value::String(a), Value::String(b), Value::Int64(max)] => {
            (a.as_str(), b.as_str(), Some(*max as usize))
        }
        _ => {
            return Err(Error::InvalidQuery(
                "EDIT_DISTANCE requires two string arguments".into(),
            ));
        }
    };

    let len1 = s1.chars().count();
    let len2 = s2.chars().count();

    if len1 > MAX_EDIT_DISTANCE_STRING_LEN || len2 > MAX_EDIT_DISTANCE_STRING_LEN {
        return Err(Error::InvalidQuery(format!(
            "EDIT_DISTANCE string length exceeds maximum of {} characters",
            MAX_EDIT_DISTANCE_STRING_LEN
        )));
    }

    if let Some(max) = max_distance
        && len1.abs_diff(len2) > max
    {
        return Ok(Value::Int64(max as i64));
    }

    let mut prev_row: Vec<usize> = (0..=len2).collect();
    let mut curr_row = vec![0; len2 + 1];

    for (i, c1) in s1.chars().enumerate() {
        curr_row[0] = i + 1;
        for (j, c2) in s2.chars().enumerate() {
            let cost = if c1 == c2 { 0 } else { 1 };
            curr_row[j + 1] = (prev_row[j + 1] + 1)
                .min(curr_row[j] + 1)
                .min(prev_row[j] + cost);
        }
        std::mem::swap(&mut prev_row, &mut curr_row);
    }

    let distance = prev_row[len2];
    let result = match max_distance {
        Some(max) if distance > max => max,
        _ => distance,
    };
    Ok(Value::Int64(result as i64))
}
