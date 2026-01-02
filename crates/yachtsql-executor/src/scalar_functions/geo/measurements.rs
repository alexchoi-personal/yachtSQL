#![coverage(off)]

use geo::Geometry;
use geo::algorithm::GeodesicArea;
use geo::algorithm::geodesic_distance::GeodesicDistance;
use geo::algorithm::geodesic_length::GeodesicLength;
use ordered_float::OrderedFloat;
use wkt::TryFromWkt;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

use super::utils;

pub fn fn_st_area(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) => {
            let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let area = geom.geodesic_area_signed().abs();
            Ok(Value::Float64(OrderedFloat(area)))
        }
        _ => Err(Error::InvalidQuery(
            "ST_AREA expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_length(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) => {
            let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let length = match &geom {
                Geometry::LineString(ls) => ls.geodesic_length(),
                Geometry::MultiLineString(mls) => mls.0.iter().map(|ls| ls.geodesic_length()).sum(),
                _ => 0.0,
            };
            Ok(Value::Float64(OrderedFloat(length)))
        }
        _ => Err(Error::InvalidQuery(
            "ST_LENGTH expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_perimeter(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) => {
            let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let perimeter = match &geom {
                Geometry::Polygon(poly) => poly.exterior().geodesic_length(),
                Geometry::MultiPolygon(mp) => {
                    mp.0.iter().map(|p| p.exterior().geodesic_length()).sum()
                }
                _ => 0.0,
            };
            Ok(Value::Float64(OrderedFloat(perimeter)))
        }
        _ => Err(Error::InvalidQuery(
            "ST_PERIMETER expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_distance(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ST_DISTANCE requires two geography arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Geography(wkt1), Value::Geography(wkt2)) => {
            let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let distance = utils::geodesic_distance_between_geometries(&geom1, &geom2);
            Ok(Value::Float64(OrderedFloat(distance)))
        }
        _ => Err(Error::InvalidQuery(
            "ST_DISTANCE expects geography arguments".into(),
        )),
    }
}

pub fn fn_st_maxdistance(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ST_MAXDISTANCE requires two geography arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Geography(wkt1), Value::Geography(wkt2)) => {
            let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let points1 = utils::extract_points(&geom1);
            let points2 = utils::extract_points(&geom2);
            let mut max_distance = 0.0_f64;
            for p1 in &points1 {
                for p2 in &points2 {
                    let dist = p1.geodesic_distance(p2);
                    if dist > max_distance {
                        max_distance = dist;
                    }
                }
            }
            Ok(Value::Float64(OrderedFloat(max_distance)))
        }
        _ => Err(Error::InvalidQuery(
            "ST_MAXDISTANCE expects geography arguments".into(),
        )),
    }
}
