#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_st_geogpoint(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ST_GEOGPOINT requires longitude and latitude".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Float64(lon), Value::Float64(lat)) => {
            Ok(Value::Geography(format!("POINT({} {})", lon.0, lat.0)))
        }
        (Value::Int64(lon), Value::Int64(lat)) => {
            Ok(Value::Geography(format!("POINT({} {})", lon, lat)))
        }
        (Value::Float64(lon), Value::Int64(lat)) => {
            Ok(Value::Geography(format!("POINT({} {})", lon.0, lat)))
        }
        (Value::Int64(lon), Value::Float64(lat)) => {
            Ok(Value::Geography(format!("POINT({} {})", lon, lat.0)))
        }
        _ => Err(Error::InvalidQuery(
            "ST_GEOGPOINT expects numeric arguments".into(),
        )),
    }
}

pub fn fn_st_makeline(args: &[Value]) -> Result<Value> {
    let mut points = Vec::new();

    if args.len() == 1 {
        if let Value::Array(arr) = &args[0] {
            for elem in arr {
                if let Value::Geography(wkt) = elem
                    && wkt.starts_with("POINT(")
                {
                    let inner = &wkt[6..wkt.len() - 1];
                    points.push(inner.to_string());
                }
            }
        }
    } else {
        for arg in args {
            match arg {
                Value::Null => continue,
                Value::Geography(wkt) if wkt.starts_with("POINT(") => {
                    let inner = &wkt[6..wkt.len() - 1];
                    points.push(inner.to_string());
                }
                _ => {}
            }
        }
    }

    if points.len() < 2 {
        return Err(Error::InvalidQuery(
            "ST_MAKELINE requires at least two geography points".into(),
        ));
    }
    Ok(Value::Geography(format!(
        "LINESTRING({})",
        points.join(", ")
    )))
}

pub fn fn_st_makepolygon(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) if wkt.starts_with("LINESTRING(") => {
            let inner = &wkt[11..wkt.len() - 1];
            Ok(Value::Geography(format!("POLYGON(({}))", inner)))
        }
        Value::Geography(wkt) => Ok(Value::Geography(wkt.clone())),
        _ => Err(Error::InvalidQuery(
            "ST_MAKEPOLYGON expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_geogfromtext(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(wkt) => Ok(Value::Geography(wkt.clone())),
        _ => Err(Error::InvalidQuery(
            "ST_GEOGFROMTEXT expects a string argument".into(),
        )),
    }
}

pub fn fn_st_geogfromwkb(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Bytes(wkb) => {
            if wkb.len() < 5 {
                return Err(Error::InvalidQuery("Invalid WKB: too short".into()));
            }
            let byte_order = wkb[0];
            let geom_type = if byte_order == 0 {
                u32::from_be_bytes([wkb[1], wkb[2], wkb[3], wkb[4]])
            } else {
                u32::from_le_bytes([wkb[1], wkb[2], wkb[3], wkb[4]])
            };
            match geom_type {
                1 => {
                    if wkb.len() < 21 {
                        return Err(Error::InvalidQuery("Invalid WKB Point: too short".into()));
                    }
                    let (x, y) = if byte_order == 0 {
                        let x = f64::from_be_bytes([
                            wkb[5], wkb[6], wkb[7], wkb[8], wkb[9], wkb[10], wkb[11], wkb[12],
                        ]);
                        let y = f64::from_be_bytes([
                            wkb[13], wkb[14], wkb[15], wkb[16], wkb[17], wkb[18], wkb[19], wkb[20],
                        ]);
                        (x, y)
                    } else {
                        let x = f64::from_le_bytes([
                            wkb[5], wkb[6], wkb[7], wkb[8], wkb[9], wkb[10], wkb[11], wkb[12],
                        ]);
                        let y = f64::from_le_bytes([
                            wkb[13], wkb[14], wkb[15], wkb[16], wkb[17], wkb[18], wkb[19], wkb[20],
                        ]);
                        (x, y)
                    };
                    Ok(Value::Geography(format!("POINT({} {})", x, y)))
                }
                _ => Err(Error::InvalidQuery(format!(
                    "Unsupported WKB geometry type: {}",
                    geom_type
                ))),
            }
        }
        _ => Err(Error::InvalidQuery(
            "ST_GEOGFROMWKB expects a bytes argument".into(),
        )),
    }
}

pub fn fn_st_geogfromgeojson(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(geojson) => {
            let parsed: serde_json::Value = serde_json::from_str(geojson)
                .map_err(|e| Error::InvalidQuery(format!("Invalid GeoJSON: {}", e)))?;
            let geom_type = parsed.get("type").and_then(|t| t.as_str());
            let coords = parsed.get("coordinates");
            match (geom_type, coords) {
                (Some("Point"), Some(serde_json::Value::Array(c))) if c.len() >= 2 => {
                    let x = c[0].as_f64().unwrap_or(0.0);
                    let y = c[1].as_f64().unwrap_or(0.0);
                    Ok(Value::Geography(format!("POINT({} {})", x, y)))
                }
                _ => Ok(Value::Geography(geojson.clone())),
            }
        }
        _ => Err(Error::InvalidQuery(
            "ST_GEOGFROMGEOJSON expects a string argument".into(),
        )),
    }
}
