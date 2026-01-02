#![coverage(off)]

use geo::Geometry;
use ordered_float::OrderedFloat;
use wkt::TryFromWkt;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_st_astext(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) => Ok(Value::String(wkt.clone())),
        _ => Err(Error::InvalidQuery(
            "ST_ASTEXT expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_asgeojson(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) => {
            if wkt.starts_with("POINT(") && wkt.ends_with(")") {
                let inner = &wkt[6..wkt.len() - 1];
                let parts: Vec<&str> = inner.split_whitespace().collect();
                if parts.len() >= 2 {
                    let x: f64 = parts[0].parse().unwrap_or(0.0);
                    let y: f64 = parts[1].parse().unwrap_or(0.0);
                    return Ok(Value::String(format!(
                        "{{\"type\":\"Point\",\"coordinates\":[{},{}]}}",
                        x, y
                    )));
                }
            }
            Ok(Value::String(format!("{{\"wkt\":\"{}\"}}", wkt)))
        }
        _ => Err(Error::InvalidQuery(
            "ST_ASGEOJSON expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_asbinary(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) => Ok(Value::Bytes(wkt.as_bytes().to_vec())),
        _ => Err(Error::InvalidQuery(
            "ST_ASBINARY expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_x(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) => {
            if wkt.starts_with("POINT(") && wkt.ends_with(")") {
                let inner = &wkt[6..wkt.len() - 1];
                let parts: Vec<&str> = inner.split_whitespace().collect();
                if let Some(x_str) = parts.first()
                    && let Ok(x) = x_str.parse::<f64>()
                {
                    return Ok(Value::Float64(OrderedFloat(x)));
                }
            }
            Ok(Value::Null)
        }
        _ => Err(Error::InvalidQuery(
            "ST_X expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_y(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) => {
            if wkt.starts_with("POINT(") && wkt.ends_with(")") {
                let inner = &wkt[6..wkt.len() - 1];
                let parts: Vec<&str> = inner.split_whitespace().collect();
                if parts.len() >= 2
                    && let Ok(y) = parts[1].parse::<f64>()
                {
                    return Ok(Value::Float64(OrderedFloat(y)));
                }
            }
            Ok(Value::Null)
        }
        _ => Err(Error::InvalidQuery(
            "ST_Y expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_numpoints(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) => {
            if wkt.starts_with("POINT(") {
                Ok(Value::Int64(1))
            } else if wkt.starts_with("LINESTRING(") || wkt.starts_with("POLYGON(") {
                let count = wkt.matches(',').count() + 1;
                Ok(Value::Int64(count as i64))
            } else {
                Ok(Value::Int64(0))
            }
        }
        _ => Err(Error::InvalidQuery(
            "ST_NUMPOINTS expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_geometrytype(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) => {
            let geom_type = if wkt.starts_with("POINT") {
                "Point"
            } else if wkt.starts_with("LINESTRING") {
                "LineString"
            } else if wkt.starts_with("POLYGON") {
                "Polygon"
            } else if wkt.starts_with("MULTIPOINT") {
                "MultiPoint"
            } else if wkt.starts_with("MULTILINESTRING") {
                "MultiLineString"
            } else if wkt.starts_with("MULTIPOLYGON") {
                "MultiPolygon"
            } else if wkt.starts_with("GEOMETRYCOLLECTION") {
                "GeometryCollection"
            } else {
                "Unknown"
            };
            Ok(Value::String(geom_type.to_string()))
        }
        _ => Err(Error::InvalidQuery(
            "ST_GEOMETRYTYPE expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_dimension(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) => {
            let dim = if wkt.starts_with("POINT") {
                0
            } else if wkt.starts_with("LINESTRING") || wkt.starts_with("MULTILINESTRING") {
                1
            } else if wkt.starts_with("POLYGON") || wkt.starts_with("MULTIPOLYGON") {
                2
            } else {
                -1
            };
            Ok(Value::Int64(dim))
        }
        _ => Err(Error::InvalidQuery(
            "ST_DIMENSION expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_startpoint(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) if wkt.starts_with("LINESTRING(") => {
            let inner = &wkt[11..wkt.len() - 1];
            if let Some(first_comma) = inner.find(',') {
                let first_point = inner[..first_comma].trim();
                Ok(Value::Geography(format!("POINT({})", first_point)))
            } else {
                Ok(Value::Geography(format!("POINT({})", inner.trim())))
            }
        }
        Value::Geography(_) => Ok(Value::Null),
        _ => Err(Error::InvalidQuery(
            "ST_STARTPOINT expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_endpoint(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) if wkt.starts_with("LINESTRING(") => {
            let inner = &wkt[11..wkt.len() - 1];
            if let Some(last_comma) = inner.rfind(',') {
                let last_point = inner[last_comma + 1..].trim();
                Ok(Value::Geography(format!("POINT({})", last_point)))
            } else {
                Ok(Value::Geography(format!("POINT({})", inner.trim())))
            }
        }
        Value::Geography(_) => Ok(Value::Null),
        _ => Err(Error::InvalidQuery(
            "ST_ENDPOINT expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_pointn(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ST_POINTN requires a geography and an index".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Geography(wkt), Value::Int64(n)) if wkt.starts_with("LINESTRING(") => {
            let inner = &wkt[11..wkt.len() - 1];
            let points: Vec<&str> = inner.split(',').collect();
            let idx = (*n - 1) as usize;
            if idx < points.len() {
                Ok(Value::Geography(format!("POINT({})", points[idx].trim())))
            } else {
                Ok(Value::Null)
            }
        }
        (Value::Geography(_), Value::Int64(_)) => Ok(Value::Null),
        _ => Err(Error::InvalidQuery(
            "ST_POINTN expects geography and integer arguments".into(),
        )),
    }
}

pub fn fn_st_isclosed(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) => {
            let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let is_closed = match &geom {
                Geometry::Point(_) => true,
                Geometry::LineString(ls) => {
                    if ls.0.len() < 2 {
                        false
                    } else {
                        ls.0.first() == ls.0.last()
                    }
                }
                Geometry::Polygon(_) => true,
                Geometry::Line(_) => false,
                Geometry::MultiPoint(_) => false,
                Geometry::MultiLineString(_) => false,
                Geometry::MultiPolygon(_) => true,
                Geometry::GeometryCollection(_) => false,
                Geometry::Rect(_) => true,
                Geometry::Triangle(_) => true,
            };
            Ok(Value::Bool(is_closed))
        }
        _ => Err(Error::InvalidQuery(
            "ST_ISCLOSED expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_isempty(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) => {
            let is_empty = wkt.contains("EMPTY") || wkt.is_empty();
            Ok(Value::Bool(is_empty))
        }
        _ => Err(Error::InvalidQuery(
            "ST_ISEMPTY expects a geography argument".into(),
        )),
    }
}
