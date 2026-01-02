#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_map(args: &[Value]) -> Result<Value> {
    if !args.len().is_multiple_of(2) {
        return Err(Error::InvalidQuery(
            "MAP requires an even number of arguments (alternating key, value pairs)".into(),
        ));
    }
    if args.is_empty() {
        return Ok(Value::Array(vec![]));
    }
    let map_entries: Vec<Value> = args
        .chunks(2)
        .map(|pair| {
            Value::Struct(vec![
                ("key".to_string(), pair[0].clone()),
                ("value".to_string(), pair[1].clone()),
            ])
        })
        .collect();
    Ok(Value::Array(map_entries))
}

pub fn fn_map_keys(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::InvalidQuery(
            "MAPKEYS requires exactly 1 argument".into(),
        ));
    }
    let map_array = match &args[0] {
        Value::Array(arr) => arr,
        Value::Null => return Ok(Value::Null),
        _ => {
            return Err(Error::InvalidQuery(
                "MAPKEYS argument must be a MAP (array of key-value structs)".into(),
            ));
        }
    };
    let keys: Vec<Value> = map_array
        .iter()
        .filter_map(|entry| {
            if let Value::Struct(fields) = entry {
                fields
                    .iter()
                    .find(|(name, _)| name == "key")
                    .map(|(_, v)| v.clone())
            } else {
                None
            }
        })
        .collect();
    Ok(Value::Array(keys))
}

pub fn fn_map_values(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::InvalidQuery(
            "MAPVALUES requires exactly 1 argument".into(),
        ));
    }
    let map_array = match &args[0] {
        Value::Array(arr) => arr,
        Value::Null => return Ok(Value::Null),
        _ => {
            return Err(Error::InvalidQuery(
                "MAPVALUES argument must be a MAP (array of key-value structs)".into(),
            ));
        }
    };
    let values: Vec<Value> = map_array
        .iter()
        .filter_map(|entry| {
            if let Value::Struct(fields) = entry {
                fields
                    .iter()
                    .find(|(name, _)| name == "value")
                    .map(|(_, v)| v.clone())
            } else {
                None
            }
        })
        .collect();
    Ok(Value::Array(values))
}
