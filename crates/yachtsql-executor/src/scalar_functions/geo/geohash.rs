#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_st_geohash(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    let max_len = if args.len() > 1 {
        match &args[1] {
            Value::Int64(i) => *i as usize,
            Value::Null => 12,
            _ => 12,
        }
    } else {
        12
    };
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) if wkt.starts_with("POINT(") => {
            let inner = &wkt[6..wkt.len() - 1];
            let parts: Vec<&str> = inner.split_whitespace().collect();
            if parts.len() >= 2 {
                let lon: f64 = parts[0].parse().unwrap_or(0.0);
                let lat: f64 = parts[1].parse().unwrap_or(0.0);
                let hash = geohash::encode(geohash::Coord { x: lon, y: lat }, max_len)
                    .unwrap_or_else(|_| "".to_string());
                Ok(Value::String(hash))
            } else {
                Ok(Value::Null)
            }
        }
        Value::Geography(_) => Ok(Value::Null),
        _ => Err(Error::InvalidQuery(
            "ST_GEOHASH expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_geogpointfromgeohash(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(hash) => match geohash::decode(hash) {
            Ok((coord, _, _)) => Ok(Value::Geography(format!("POINT({} {})", coord.x, coord.y))),
            Err(_) => Ok(Value::Null),
        },
        _ => Err(Error::InvalidQuery(
            "ST_GEOGPOINTFROMGEOHASH expects a string argument".into(),
        )),
    }
}
