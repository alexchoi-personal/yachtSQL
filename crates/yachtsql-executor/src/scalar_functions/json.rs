#![coverage(off)]

use ordered_float::OrderedFloat;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

fn value_to_json(value: &Value) -> Result<serde_json::Value> {
    use rust_decimal::prelude::ToPrimitive;

    match value {
        Value::Null => Ok(serde_json::Value::Null),
        Value::Bool(b) => Ok(serde_json::Value::Bool(*b)),
        Value::Int64(n) => Ok(serde_json::Value::Number((*n).into())),
        Value::Float64(f) => {
            let n = serde_json::Number::from_f64(f.0)
                .ok_or_else(|| Error::InvalidQuery("Cannot convert float to JSON".into()))?;
            Ok(serde_json::Value::Number(n))
        }
        Value::String(s) => Ok(serde_json::Value::String(s.clone())),
        Value::Array(arr) => {
            let json_arr: Result<Vec<serde_json::Value>> = arr.iter().map(value_to_json).collect();
            Ok(serde_json::Value::Array(json_arr?))
        }
        Value::Struct(fields) => {
            let mut map = serde_json::Map::new();
            for (name, val) in fields {
                map.insert(name.clone(), value_to_json(val)?);
            }
            Ok(serde_json::Value::Object(map))
        }
        Value::Json(j) => Ok(j.clone()),
        Value::Date(d) => Ok(serde_json::Value::String(d.to_string())),
        Value::Time(t) => Ok(serde_json::Value::String(t.to_string())),
        Value::DateTime(dt) => Ok(serde_json::Value::String(dt.to_string())),
        Value::Timestamp(ts) => Ok(serde_json::Value::String(ts.to_rfc3339())),
        Value::Numeric(n) => {
            if let Some(f) = n.to_f64() {
                let num = serde_json::Number::from_f64(f)
                    .ok_or_else(|| Error::InvalidQuery("Cannot convert numeric to JSON".into()))?;
                Ok(serde_json::Value::Number(num))
            } else {
                Ok(serde_json::Value::String(n.to_string()))
            }
        }
        Value::BigNumeric(n) => {
            if let Some(f) = n.to_f64() {
                let num = serde_json::Number::from_f64(f).ok_or_else(|| {
                    Error::InvalidQuery("Cannot convert bignumeric to JSON".into())
                })?;
                Ok(serde_json::Value::Number(num))
            } else {
                Ok(serde_json::Value::String(n.to_string()))
            }
        }
        Value::Bytes(b) => {
            use base64::Engine;
            use base64::engine::general_purpose::STANDARD;
            Ok(serde_json::Value::String(STANDARD.encode(b)))
        }
        Value::Interval(i) => Ok(serde_json::Value::String(format!(
            "{} months, {} days, {} nanos",
            i.months, i.days, i.nanos
        ))),
        Value::Geography(g) => Ok(serde_json::Value::String(g.clone())),
        Value::Range(r) => Ok(serde_json::Value::String(format!(
            "[{:?}, {:?})",
            r.start, r.end
        ))),
        Value::Default => Ok(serde_json::Value::Null),
    }
}

fn extract_json_path(json: &serde_json::Value, path: &str) -> Result<Option<serde_json::Value>> {
    let path = path.trim_start_matches('$');
    let mut current = json.clone();

    for segment in path.split('.').filter(|s| !s.is_empty()) {
        let (key, index) = if segment.contains('[') {
            let parts: Vec<&str> = segment.split('[').collect();
            let key = parts[0];
            let idx_str = parts[1].trim_end_matches(']');
            let idx: usize = idx_str.parse().map_err(|_| {
                Error::InvalidQuery(format!("Invalid JSON path index: {}", idx_str))
            })?;
            (key, Some(idx))
        } else {
            (segment, None)
        };

        if !key.is_empty() {
            current = match current {
                serde_json::Value::Object(map) => {
                    map.get(key).cloned().unwrap_or(serde_json::Value::Null)
                }
                _ => return Ok(None),
            };
        }

        if let Some(idx) = index {
            current = match current {
                serde_json::Value::Array(arr) => {
                    arr.get(idx).cloned().unwrap_or(serde_json::Value::Null)
                }
                _ => return Ok(None),
            };
        }
    }

    if current == serde_json::Value::Null {
        Ok(None)
    } else {
        Ok(Some(current))
    }
}

pub fn fn_to_json(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(v) => {
            let json = value_to_json(v)?;
            Ok(Value::Json(json))
        }
        None => Err(Error::InvalidQuery("TO_JSON requires an argument".into())),
    }
}

pub fn fn_to_json_string(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(v) => {
            let json = value_to_json(v)?;
            Ok(Value::String(json.to_string()))
        }
        None => Err(Error::InvalidQuery(
            "TO_JSON_STRING requires an argument".into(),
        )),
    }
}

pub fn fn_parse_json(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let json: serde_json::Value = serde_json::from_str(s)
                .map_err(|e| Error::InvalidQuery(format!("Invalid JSON: {}", e)))?;
            Ok(Value::Json(json))
        }
        _ => Err(Error::InvalidQuery(
            "PARSE_JSON requires string argument".into(),
        )),
    }
}

pub fn fn_json_value(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "JSON_VALUE requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Json(json), Value::String(path)) => {
            let value = extract_json_path(json, path)?;
            match value {
                Some(serde_json::Value::String(s)) => Ok(Value::String(s)),
                Some(serde_json::Value::Number(n)) => {
                    if let Some(i) = n.as_i64() {
                        Ok(Value::String(i.to_string()))
                    } else if let Some(f) = n.as_f64() {
                        Ok(Value::String(f.to_string()))
                    } else {
                        Ok(Value::String(n.to_string()))
                    }
                }
                Some(serde_json::Value::Bool(b)) => Ok(Value::String(b.to_string())),
                Some(serde_json::Value::Null) => Ok(Value::Null),
                Some(_) => Ok(Value::Null),
                None => Ok(Value::Null),
            }
        }
        (Value::String(s), Value::String(path)) => {
            let json: serde_json::Value = serde_json::from_str(s)
                .map_err(|e| Error::InvalidQuery(format!("Invalid JSON: {}", e)))?;
            let value = extract_json_path(&json, path)?;
            match value {
                Some(serde_json::Value::String(s)) => Ok(Value::String(s)),
                Some(serde_json::Value::Number(n)) => Ok(Value::String(n.to_string())),
                Some(serde_json::Value::Bool(b)) => Ok(Value::String(b.to_string())),
                Some(serde_json::Value::Null) => Ok(Value::Null),
                Some(_) => Ok(Value::Null),
                None => Ok(Value::Null),
            }
        }
        _ => Err(Error::InvalidQuery(
            "JSON_VALUE requires JSON and path arguments".into(),
        )),
    }
}

pub fn fn_json_query(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "JSON_QUERY requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Json(json), Value::String(path)) => {
            let value = extract_json_path(json, path)?;
            match value {
                Some(v) => Ok(Value::Json(v)),
                None => Ok(Value::Null),
            }
        }
        (Value::String(s), Value::String(path)) => {
            let json: serde_json::Value = serde_json::from_str(s)
                .map_err(|e| Error::InvalidQuery(format!("Invalid JSON: {}", e)))?;
            let value = extract_json_path(&json, path)?;
            match value {
                Some(v) => Ok(Value::Json(v)),
                None => Ok(Value::Null),
            }
        }
        _ => Err(Error::InvalidQuery(
            "JSON_QUERY requires JSON and path arguments".into(),
        )),
    }
}

pub fn fn_json_extract(args: &[Value]) -> Result<Value> {
    fn_json_value(args)
}

pub fn fn_json_extract_scalar(args: &[Value]) -> Result<Value> {
    fn_json_value(args)
}

pub fn fn_json_extract_array(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    let json_val = match &args[0] {
        Value::Null => return Ok(Value::Null),
        Value::Json(j) => j.clone(),
        Value::String(s) => match serde_json::from_str::<serde_json::Value>(s) {
            Ok(j) => j,
            Err(_) => return Ok(Value::Null),
        },
        _ => return Ok(Value::Null),
    };
    let json_to_extract = if args.len() > 1 {
        if let Value::String(path) = &args[1] {
            let path = path.trim_start_matches('$');
            let path = path.trim_start_matches('.');
            if path.is_empty() {
                json_val
            } else {
                let mut current = &json_val;
                for part in path.split('.') {
                    let part = part.trim_start_matches('[').trim_end_matches(']');
                    if let Ok(idx) = part.parse::<usize>() {
                        if let Some(arr) = current.as_array() {
                            if idx < arr.len() {
                                current = &arr[idx];
                            } else {
                                return Ok(Value::Null);
                            }
                        } else {
                            return Ok(Value::Null);
                        }
                    } else if let Some(obj) = current.as_object() {
                        if let Some(val) = obj.get(part) {
                            current = val;
                        } else {
                            return Ok(Value::Null);
                        }
                    } else {
                        return Ok(Value::Null);
                    }
                }
                current.clone()
            }
        } else {
            json_val
        }
    } else {
        json_val
    };
    if let Some(arr) = json_to_extract.as_array() {
        let result: Vec<Value> = arr.iter().map(|v| Value::Json(v.clone())).collect();
        Ok(Value::Array(result))
    } else {
        Ok(Value::Array(vec![]))
    }
}

pub fn fn_json_extract_string_array(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    let json_val = match &args[0] {
        Value::Null => return Ok(Value::Null),
        Value::Json(j) => j.clone(),
        Value::String(s) => match serde_json::from_str::<serde_json::Value>(s) {
            Ok(j) => j,
            Err(_) => return Ok(Value::Array(vec![])),
        },
        _ => return Ok(Value::Null),
    };
    let json_to_extract = if args.len() > 1 {
        if let Value::String(path) = &args[1] {
            match extract_json_path(&json_val, path)? {
                Some(v) => v,
                None => return Ok(Value::Array(vec![])),
            }
        } else {
            json_val
        }
    } else {
        json_val
    };
    if let Some(arr) = json_to_extract.as_array() {
        let values: Vec<Value> = arr
            .iter()
            .map(|v| match v {
                serde_json::Value::String(s) => Value::String(s.clone()),
                serde_json::Value::Number(n) => Value::String(n.to_string()),
                serde_json::Value::Bool(b) => Value::String(b.to_string()),
                serde_json::Value::Null => Value::Null,
                _ => Value::String(v.to_string()),
            })
            .collect();
        Ok(Value::Array(values))
    } else {
        Ok(Value::Array(vec![]))
    }
}

pub fn fn_json_query_array(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    let json_val = match &args[0] {
        Value::Null => return Ok(Value::Null),
        Value::Json(j) => j.clone(),
        Value::String(s) => match serde_json::from_str::<serde_json::Value>(s) {
            Ok(j) => j,
            Err(_) => return Ok(Value::Array(vec![])),
        },
        _ => return Ok(Value::Null),
    };
    let json_to_extract = if args.len() > 1 {
        if let Value::String(path) = &args[1] {
            match extract_json_path(&json_val, path)? {
                Some(v) => v,
                None => return Ok(Value::Array(vec![])),
            }
        } else {
            json_val
        }
    } else {
        json_val
    };
    if let Some(arr) = json_to_extract.as_array() {
        let values: Vec<Value> = arr.iter().map(|v| Value::Json(v.clone())).collect();
        Ok(Value::Array(values))
    } else {
        Ok(Value::Array(vec![]))
    }
}

pub fn fn_json_value_array(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    let json_val = match &args[0] {
        Value::Null => return Ok(Value::Null),
        Value::Json(j) => j.clone(),
        Value::String(s) => match serde_json::from_str::<serde_json::Value>(s) {
            Ok(j) => j,
            Err(_) => return Ok(Value::Array(vec![])),
        },
        _ => return Ok(Value::Null),
    };
    let json_to_extract = if args.len() > 1 {
        if let Value::String(path) = &args[1] {
            match extract_json_path(&json_val, path)? {
                Some(v) => v,
                None => return Ok(Value::Array(vec![])),
            }
        } else {
            json_val
        }
    } else {
        json_val
    };
    if let Some(arr) = json_to_extract.as_array() {
        let values: Vec<Value> = arr
            .iter()
            .map(|v| match v {
                serde_json::Value::String(s) => Value::String(s.clone()),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Value::Int64(i)
                    } else if let Some(f) = n.as_f64() {
                        Value::Float64(OrderedFloat(f))
                    } else {
                        Value::Null
                    }
                }
                serde_json::Value::Bool(b) => Value::Bool(*b),
                _ => Value::Null,
            })
            .collect();
        Ok(Value::Array(values))
    } else {
        Ok(Value::Array(vec![]))
    }
}

pub fn fn_json_type(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery("JSON_TYPE requires 1 argument".into()));
    }
    let json = match &args[0] {
        Value::Null => return Ok(Value::Null),
        Value::Json(j) => j.clone(),
        Value::String(s) => serde_json::from_str(s)
            .map_err(|e| Error::InvalidQuery(format!("Invalid JSON: {}", e)))?,
        _ => {
            return Err(Error::InvalidQuery(
                "JSON_TYPE requires a JSON argument".into(),
            ));
        }
    };
    let type_name = match json {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    };
    Ok(Value::String(type_name.to_string()))
}

pub fn fn_json_keys(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Json(j) => {
            if let Some(obj) = j.as_object() {
                let keys: Vec<Value> = obj.keys().map(|k| Value::String(k.clone())).collect();
                Ok(Value::Array(keys))
            } else {
                Ok(Value::Array(vec![]))
            }
        }
        Value::String(s) => {
            if let Ok(j) = serde_json::from_str::<serde_json::Value>(s)
                && let Some(obj) = j.as_object()
            {
                let keys: Vec<Value> = obj.keys().map(|k| Value::String(k.clone())).collect();
                return Ok(Value::Array(keys));
            }
            Ok(Value::Array(vec![]))
        }
        _ => Ok(Value::Null),
    }
}

pub fn fn_json_array_length(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery(
            "JSON_ARRAY_LENGTH requires at least 1 argument".into(),
        ));
    }
    let json_val = match &args[0] {
        Value::Null => return Ok(Value::Null),
        Value::Json(j) => j.clone(),
        Value::String(s) => match serde_json::from_str::<serde_json::Value>(s) {
            Ok(j) => j,
            Err(e) => return Err(Error::InvalidQuery(format!("Invalid JSON: {}", e))),
        },
        _ => {
            return Err(Error::InvalidQuery(
                "JSON_ARRAY_LENGTH requires JSON argument".into(),
            ));
        }
    };
    let json_to_check = if args.len() > 1 {
        if let Value::String(path) = &args[1] {
            match extract_json_path(&json_val, path)? {
                Some(v) => v,
                None => return Ok(Value::Null),
            }
        } else {
            json_val
        }
    } else {
        json_val
    };
    match json_to_check {
        serde_json::Value::Array(arr) => Ok(Value::Int64(arr.len() as i64)),
        _ => Ok(Value::Null),
    }
}

pub fn fn_json_array(args: &[Value]) -> Result<Value> {
    let mut arr = Vec::new();
    for arg in args {
        let json_val = match arg {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Int64(n) => serde_json::Value::Number((*n).into()),
            Value::Float64(f) => serde_json::Number::from_f64(f.0)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Json(j) => j.clone(),
            _ => serde_json::Value::Null,
        };
        arr.push(json_val);
    }
    Ok(Value::Json(serde_json::Value::Array(arr)))
}

pub fn fn_json_object(args: &[Value]) -> Result<Value> {
    if !args.len().is_multiple_of(2) {
        return Err(Error::InvalidQuery(
            "JSON_OBJECT requires an even number of arguments".into(),
        ));
    }
    let mut obj = serde_json::Map::new();
    for chunk in args.chunks(2) {
        let key = match &chunk[0] {
            Value::String(s) => s.clone(),
            _ => continue,
        };
        let val = match &chunk[1] {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Int64(n) => serde_json::Value::Number((*n).into()),
            Value::Float64(f) => serde_json::Number::from_f64(f.0)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Json(j) => j.clone(),
            _ => serde_json::Value::Null,
        };
        obj.insert(key, val);
    }
    Ok(Value::Json(serde_json::Value::Object(obj)))
}

pub fn fn_json_set(args: &[Value]) -> Result<Value> {
    if args.len() < 3 {
        return Err(Error::InvalidQuery(
            "JSON_SET requires at least 3 arguments".into(),
        ));
    }
    let mut json_val = match &args[0] {
        Value::Null => return Ok(Value::Null),
        Value::Json(j) => j.clone(),
        Value::String(s) => serde_json::from_str(s)
            .map_err(|e| Error::InvalidQuery(format!("Invalid JSON: {}", e)))?,
        _ => {
            return Err(Error::InvalidQuery(
                "JSON_SET requires JSON argument".into(),
            ));
        }
    };
    let path = match &args[1] {
        Value::String(s) => s.clone(),
        _ => return Err(Error::InvalidQuery("JSON_SET requires string path".into())),
    };
    let new_val = match &args[2] {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int64(n) => serde_json::Value::Number((*n).into()),
        Value::Float64(f) => serde_json::Number::from_f64(f.0)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Json(j) => j.clone(),
        _ => serde_json::Value::Null,
    };
    let path = path.trim_start_matches('$').trim_start_matches('.');
    if let Some(obj) = json_val.as_object_mut() {
        obj.insert(path.to_string(), new_val);
    }
    Ok(Value::Json(json_val))
}

pub fn fn_json_remove(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "JSON_REMOVE requires at least 2 arguments".into(),
        ));
    }
    let mut json_val = match &args[0] {
        Value::Null => return Ok(Value::Null),
        Value::Json(j) => j.clone(),
        Value::String(s) => serde_json::from_str(s)
            .map_err(|e| Error::InvalidQuery(format!("Invalid JSON: {}", e)))?,
        _ => {
            return Err(Error::InvalidQuery(
                "JSON_REMOVE requires JSON argument".into(),
            ));
        }
    };
    for arg in args.iter().skip(1) {
        if let Value::String(path) = arg {
            let path = path.trim_start_matches('$').trim_start_matches('.');
            if let Some(obj) = json_val.as_object_mut() {
                obj.remove(path);
            }
        }
    }
    Ok(Value::Json(json_val))
}

pub fn fn_json_strip_nulls(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery(
            "JSON_STRIP_NULLS requires 1 argument".into(),
        ));
    }
    let json_val = match &args[0] {
        Value::Null => return Ok(Value::Null),
        Value::Json(j) => j.clone(),
        Value::String(s) => serde_json::from_str(s)
            .map_err(|e| Error::InvalidQuery(format!("Invalid JSON: {}", e)))?,
        _ => {
            return Err(Error::InvalidQuery(
                "JSON_STRIP_NULLS requires JSON argument".into(),
            ));
        }
    };
    fn strip_nulls(val: serde_json::Value) -> serde_json::Value {
        match val {
            serde_json::Value::Object(obj) => {
                let filtered: serde_json::Map<String, serde_json::Value> = obj
                    .into_iter()
                    .filter(|(_, v)| !v.is_null())
                    .map(|(k, v)| (k, strip_nulls(v)))
                    .collect();
                serde_json::Value::Object(filtered)
            }
            serde_json::Value::Array(arr) => {
                serde_json::Value::Array(arr.into_iter().map(strip_nulls).collect())
            }
            other => other,
        }
    }
    Ok(Value::Json(strip_nulls(json_val)))
}
