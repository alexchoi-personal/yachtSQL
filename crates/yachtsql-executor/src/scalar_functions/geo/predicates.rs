#![coverage(off)]

use geo::algorithm::geodesic_distance::GeodesicDistance;
use geo::algorithm::intersects::Intersects;
use geo::{Contains, Geometry, Point};
use wkt::TryFromWkt;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_st_contains(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ST_CONTAINS requires two geography arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Geography(wkt1), Value::Geography(wkt2)) => {
            let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let result = geometry_contains(&geom1, &geom2);
            Ok(Value::Bool(result))
        }
        _ => Err(Error::InvalidQuery(
            "ST_CONTAINS expects geography arguments".into(),
        )),
    }
}

pub fn fn_st_intersects(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ST_INTERSECTS requires two geography arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Geography(wkt1), Value::Geography(wkt2)) => {
            let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let result = geom1.intersects(&geom2);
            Ok(Value::Bool(result))
        }
        _ => Err(Error::InvalidQuery(
            "ST_INTERSECTS expects geography arguments".into(),
        )),
    }
}

pub fn fn_st_within(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ST_WITHIN requires two geography arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Geography(wkt1), Value::Geography(wkt2)) => {
            let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let result = geometry_contains(&geom2, &geom1);
            Ok(Value::Bool(result))
        }
        _ => Err(Error::InvalidQuery(
            "ST_WITHIN expects geography arguments".into(),
        )),
    }
}

pub fn fn_st_dwithin(args: &[Value]) -> Result<Value> {
    if args.len() < 3 {
        return Err(Error::InvalidQuery(
            "ST_DWITHIN requires two geography arguments and a distance".into(),
        ));
    }
    match (&args[0], &args[1], &args[2]) {
        (Value::Null, _, _) | (_, Value::Null, _) => Ok(Value::Null),
        (Value::Geography(wkt1), Value::Geography(wkt2), distance_val) => {
            let distance_limit = match distance_val {
                Value::Float64(f) => f.0,
                Value::Int64(i) => *i as f64,
                _ => return Ok(Value::Bool(false)),
            };
            let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let distance = geodesic_distance_between_geometries(&geom1, &geom2);
            Ok(Value::Bool(distance <= distance_limit))
        }
        _ => Err(Error::InvalidQuery(
            "ST_DWITHIN expects geography arguments".into(),
        )),
    }
}

pub fn fn_st_covers(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ST_COVERS requires two geography arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Geography(wkt1), Value::Geography(wkt2)) => {
            let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let result = geometry_contains(&geom1, &geom2);
            Ok(Value::Bool(result))
        }
        _ => Err(Error::InvalidQuery(
            "ST_COVERS expects geography arguments".into(),
        )),
    }
}

pub fn fn_st_coveredby(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ST_COVEREDBY requires two geography arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Geography(wkt1), Value::Geography(wkt2)) => {
            let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let result = geometry_contains(&geom2, &geom1);
            Ok(Value::Bool(result))
        }
        _ => Err(Error::InvalidQuery(
            "ST_COVEREDBY expects geography arguments".into(),
        )),
    }
}

pub fn fn_st_touches(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ST_TOUCHES requires two geography arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Geography(wkt1), Value::Geography(wkt2)) => {
            let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let result = geometry_touches(&geom1, &geom2);
            Ok(Value::Bool(result))
        }
        _ => Err(Error::InvalidQuery(
            "ST_TOUCHES expects geography arguments".into(),
        )),
    }
}

pub fn fn_st_disjoint(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ST_DISJOINT requires two geography arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Geography(wkt1), Value::Geography(wkt2)) => {
            let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let result = !geom1.intersects(&geom2);
            Ok(Value::Bool(result))
        }
        _ => Err(Error::InvalidQuery(
            "ST_DISJOINT expects geography arguments".into(),
        )),
    }
}

pub fn fn_st_equals(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ST_EQUALS requires two geography arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Geography(a), Value::Geography(b)) => Ok(Value::Bool(a == b)),
        _ => Err(Error::InvalidQuery(
            "ST_EQUALS expects geography arguments".into(),
        )),
    }
}

pub fn fn_st_iscollection(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) => {
            let is_collection = wkt.starts_with("MULTI") || wkt.starts_with("GEOMETRYCOLLECTION");
            Ok(Value::Bool(is_collection))
        }
        _ => Err(Error::InvalidQuery(
            "ST_ISCOLLECTION expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_isring(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) if wkt.starts_with("LINESTRING(") => {
            let inner = &wkt[11..wkt.len() - 1];
            let points: Vec<&str> = inner.split(',').collect();
            if points.len() >= 4 {
                let first = points.first().map(|s| s.trim());
                let last = points.last().map(|s| s.trim());
                Ok(Value::Bool(first == last))
            } else {
                Ok(Value::Bool(false))
            }
        }
        Value::Geography(_) => Ok(Value::Bool(false)),
        _ => Err(Error::InvalidQuery(
            "ST_ISRING expects a geography argument".into(),
        )),
    }
}

fn geometry_contains(geom1: &Geometry<f64>, geom2: &Geometry<f64>) -> bool {
    match (geom1, geom2) {
        (Geometry::Polygon(poly), Geometry::Point(p)) => poly.contains(p),
        (Geometry::Polygon(poly1), Geometry::Polygon(poly2)) => {
            poly2.exterior().points().all(|p| poly1.contains(&p))
        }
        (Geometry::Polygon(poly), Geometry::LineString(ls)) => {
            ls.points().all(|p| poly.contains(&p))
        }
        (Geometry::MultiPolygon(mp), Geometry::Point(p)) => {
            mp.0.iter().any(|poly| poly.contains(p))
        }
        (Geometry::Point(_), Geometry::Point(_)) => false,
        (Geometry::Point(_), Geometry::Line(_)) => false,
        (Geometry::Point(_), Geometry::LineString(_)) => false,
        (Geometry::Point(_), Geometry::Polygon(_)) => false,
        (Geometry::Point(_), Geometry::MultiPoint(_)) => false,
        (Geometry::Point(_), Geometry::MultiLineString(_)) => false,
        (Geometry::Point(_), Geometry::MultiPolygon(_)) => false,
        (Geometry::Point(_), Geometry::GeometryCollection(_)) => false,
        (Geometry::Point(_), Geometry::Rect(_)) => false,
        (Geometry::Point(_), Geometry::Triangle(_)) => false,
        (Geometry::Line(_), Geometry::Point(_)) => false,
        (Geometry::Line(_), Geometry::Line(_)) => false,
        (Geometry::Line(_), Geometry::LineString(_)) => false,
        (Geometry::Line(_), Geometry::Polygon(_)) => false,
        (Geometry::Line(_), Geometry::MultiPoint(_)) => false,
        (Geometry::Line(_), Geometry::MultiLineString(_)) => false,
        (Geometry::Line(_), Geometry::MultiPolygon(_)) => false,
        (Geometry::Line(_), Geometry::GeometryCollection(_)) => false,
        (Geometry::Line(_), Geometry::Rect(_)) => false,
        (Geometry::Line(_), Geometry::Triangle(_)) => false,
        (Geometry::LineString(_), Geometry::Point(_)) => false,
        (Geometry::LineString(_), Geometry::Line(_)) => false,
        (Geometry::LineString(_), Geometry::LineString(_)) => false,
        (Geometry::LineString(_), Geometry::Polygon(_)) => false,
        (Geometry::LineString(_), Geometry::MultiPoint(_)) => false,
        (Geometry::LineString(_), Geometry::MultiLineString(_)) => false,
        (Geometry::LineString(_), Geometry::MultiPolygon(_)) => false,
        (Geometry::LineString(_), Geometry::GeometryCollection(_)) => false,
        (Geometry::LineString(_), Geometry::Rect(_)) => false,
        (Geometry::LineString(_), Geometry::Triangle(_)) => false,
        (Geometry::Polygon(_), Geometry::Line(_)) => false,
        (Geometry::Polygon(_), Geometry::MultiPoint(_)) => false,
        (Geometry::Polygon(_), Geometry::MultiLineString(_)) => false,
        (Geometry::Polygon(_), Geometry::MultiPolygon(_)) => false,
        (Geometry::Polygon(_), Geometry::GeometryCollection(_)) => false,
        (Geometry::Polygon(_), Geometry::Rect(_)) => false,
        (Geometry::Polygon(_), Geometry::Triangle(_)) => false,
        (Geometry::MultiPoint(_), Geometry::Point(_)) => false,
        (Geometry::MultiPoint(_), Geometry::Line(_)) => false,
        (Geometry::MultiPoint(_), Geometry::LineString(_)) => false,
        (Geometry::MultiPoint(_), Geometry::Polygon(_)) => false,
        (Geometry::MultiPoint(_), Geometry::MultiPoint(_)) => false,
        (Geometry::MultiPoint(_), Geometry::MultiLineString(_)) => false,
        (Geometry::MultiPoint(_), Geometry::MultiPolygon(_)) => false,
        (Geometry::MultiPoint(_), Geometry::GeometryCollection(_)) => false,
        (Geometry::MultiPoint(_), Geometry::Rect(_)) => false,
        (Geometry::MultiPoint(_), Geometry::Triangle(_)) => false,
        (Geometry::MultiLineString(_), Geometry::Point(_)) => false,
        (Geometry::MultiLineString(_), Geometry::Line(_)) => false,
        (Geometry::MultiLineString(_), Geometry::LineString(_)) => false,
        (Geometry::MultiLineString(_), Geometry::Polygon(_)) => false,
        (Geometry::MultiLineString(_), Geometry::MultiPoint(_)) => false,
        (Geometry::MultiLineString(_), Geometry::MultiLineString(_)) => false,
        (Geometry::MultiLineString(_), Geometry::MultiPolygon(_)) => false,
        (Geometry::MultiLineString(_), Geometry::GeometryCollection(_)) => false,
        (Geometry::MultiLineString(_), Geometry::Rect(_)) => false,
        (Geometry::MultiLineString(_), Geometry::Triangle(_)) => false,
        (Geometry::MultiPolygon(_), Geometry::Line(_)) => false,
        (Geometry::MultiPolygon(_), Geometry::LineString(_)) => false,
        (Geometry::MultiPolygon(_), Geometry::Polygon(_)) => false,
        (Geometry::MultiPolygon(_), Geometry::MultiPoint(_)) => false,
        (Geometry::MultiPolygon(_), Geometry::MultiLineString(_)) => false,
        (Geometry::MultiPolygon(_), Geometry::MultiPolygon(_)) => false,
        (Geometry::MultiPolygon(_), Geometry::GeometryCollection(_)) => false,
        (Geometry::MultiPolygon(_), Geometry::Rect(_)) => false,
        (Geometry::MultiPolygon(_), Geometry::Triangle(_)) => false,
        (Geometry::GeometryCollection(_), Geometry::Point(_)) => false,
        (Geometry::GeometryCollection(_), Geometry::Line(_)) => false,
        (Geometry::GeometryCollection(_), Geometry::LineString(_)) => false,
        (Geometry::GeometryCollection(_), Geometry::Polygon(_)) => false,
        (Geometry::GeometryCollection(_), Geometry::MultiPoint(_)) => false,
        (Geometry::GeometryCollection(_), Geometry::MultiLineString(_)) => false,
        (Geometry::GeometryCollection(_), Geometry::MultiPolygon(_)) => false,
        (Geometry::GeometryCollection(_), Geometry::GeometryCollection(_)) => false,
        (Geometry::GeometryCollection(_), Geometry::Rect(_)) => false,
        (Geometry::GeometryCollection(_), Geometry::Triangle(_)) => false,
        (Geometry::Rect(_), Geometry::Point(_)) => false,
        (Geometry::Rect(_), Geometry::Line(_)) => false,
        (Geometry::Rect(_), Geometry::LineString(_)) => false,
        (Geometry::Rect(_), Geometry::Polygon(_)) => false,
        (Geometry::Rect(_), Geometry::MultiPoint(_)) => false,
        (Geometry::Rect(_), Geometry::MultiLineString(_)) => false,
        (Geometry::Rect(_), Geometry::MultiPolygon(_)) => false,
        (Geometry::Rect(_), Geometry::GeometryCollection(_)) => false,
        (Geometry::Rect(_), Geometry::Rect(_)) => false,
        (Geometry::Rect(_), Geometry::Triangle(_)) => false,
        (Geometry::Triangle(_), Geometry::Point(_)) => false,
        (Geometry::Triangle(_), Geometry::Line(_)) => false,
        (Geometry::Triangle(_), Geometry::LineString(_)) => false,
        (Geometry::Triangle(_), Geometry::Polygon(_)) => false,
        (Geometry::Triangle(_), Geometry::MultiPoint(_)) => false,
        (Geometry::Triangle(_), Geometry::MultiLineString(_)) => false,
        (Geometry::Triangle(_), Geometry::MultiPolygon(_)) => false,
        (Geometry::Triangle(_), Geometry::GeometryCollection(_)) => false,
        (Geometry::Triangle(_), Geometry::Rect(_)) => false,
        (Geometry::Triangle(_), Geometry::Triangle(_)) => false,
    }
}

fn geometry_touches(geom1: &Geometry<f64>, geom2: &Geometry<f64>) -> bool {
    match (geom1, geom2) {
        (Geometry::Polygon(p1), Geometry::Polygon(p2)) => {
            let points1: Vec<Point<f64>> = p1.exterior().points().collect();
            let points2: Vec<Point<f64>> = p2.exterior().points().collect();
            for p in &points1 {
                if points2
                    .iter()
                    .any(|p2| (p.x() - p2.x()).abs() < 1e-10 && (p.y() - p2.y()).abs() < 1e-10)
                {
                    return true;
                }
            }
            false
        }
        (Geometry::Point(_), Geometry::Point(_)) => false,
        (Geometry::Point(_), Geometry::Line(_)) => false,
        (Geometry::Point(_), Geometry::LineString(_)) => false,
        (Geometry::Point(_), Geometry::Polygon(_)) => false,
        (Geometry::Point(_), Geometry::MultiPoint(_)) => false,
        (Geometry::Point(_), Geometry::MultiLineString(_)) => false,
        (Geometry::Point(_), Geometry::MultiPolygon(_)) => false,
        (Geometry::Point(_), Geometry::GeometryCollection(_)) => false,
        (Geometry::Point(_), Geometry::Rect(_)) => false,
        (Geometry::Point(_), Geometry::Triangle(_)) => false,
        (Geometry::Line(_), Geometry::Point(_)) => false,
        (Geometry::Line(_), Geometry::Line(_)) => false,
        (Geometry::Line(_), Geometry::LineString(_)) => false,
        (Geometry::Line(_), Geometry::Polygon(_)) => false,
        (Geometry::Line(_), Geometry::MultiPoint(_)) => false,
        (Geometry::Line(_), Geometry::MultiLineString(_)) => false,
        (Geometry::Line(_), Geometry::MultiPolygon(_)) => false,
        (Geometry::Line(_), Geometry::GeometryCollection(_)) => false,
        (Geometry::Line(_), Geometry::Rect(_)) => false,
        (Geometry::Line(_), Geometry::Triangle(_)) => false,
        (Geometry::LineString(_), Geometry::Point(_)) => false,
        (Geometry::LineString(_), Geometry::Line(_)) => false,
        (Geometry::LineString(_), Geometry::LineString(_)) => false,
        (Geometry::LineString(_), Geometry::Polygon(_)) => false,
        (Geometry::LineString(_), Geometry::MultiPoint(_)) => false,
        (Geometry::LineString(_), Geometry::MultiLineString(_)) => false,
        (Geometry::LineString(_), Geometry::MultiPolygon(_)) => false,
        (Geometry::LineString(_), Geometry::GeometryCollection(_)) => false,
        (Geometry::LineString(_), Geometry::Rect(_)) => false,
        (Geometry::LineString(_), Geometry::Triangle(_)) => false,
        (Geometry::Polygon(_), Geometry::Point(_)) => false,
        (Geometry::Polygon(_), Geometry::Line(_)) => false,
        (Geometry::Polygon(_), Geometry::LineString(_)) => false,
        (Geometry::Polygon(_), Geometry::MultiPoint(_)) => false,
        (Geometry::Polygon(_), Geometry::MultiLineString(_)) => false,
        (Geometry::Polygon(_), Geometry::MultiPolygon(_)) => false,
        (Geometry::Polygon(_), Geometry::GeometryCollection(_)) => false,
        (Geometry::Polygon(_), Geometry::Rect(_)) => false,
        (Geometry::Polygon(_), Geometry::Triangle(_)) => false,
        (Geometry::MultiPoint(_), Geometry::Point(_)) => false,
        (Geometry::MultiPoint(_), Geometry::Line(_)) => false,
        (Geometry::MultiPoint(_), Geometry::LineString(_)) => false,
        (Geometry::MultiPoint(_), Geometry::Polygon(_)) => false,
        (Geometry::MultiPoint(_), Geometry::MultiPoint(_)) => false,
        (Geometry::MultiPoint(_), Geometry::MultiLineString(_)) => false,
        (Geometry::MultiPoint(_), Geometry::MultiPolygon(_)) => false,
        (Geometry::MultiPoint(_), Geometry::GeometryCollection(_)) => false,
        (Geometry::MultiPoint(_), Geometry::Rect(_)) => false,
        (Geometry::MultiPoint(_), Geometry::Triangle(_)) => false,
        (Geometry::MultiLineString(_), Geometry::Point(_)) => false,
        (Geometry::MultiLineString(_), Geometry::Line(_)) => false,
        (Geometry::MultiLineString(_), Geometry::LineString(_)) => false,
        (Geometry::MultiLineString(_), Geometry::Polygon(_)) => false,
        (Geometry::MultiLineString(_), Geometry::MultiPoint(_)) => false,
        (Geometry::MultiLineString(_), Geometry::MultiLineString(_)) => false,
        (Geometry::MultiLineString(_), Geometry::MultiPolygon(_)) => false,
        (Geometry::MultiLineString(_), Geometry::GeometryCollection(_)) => false,
        (Geometry::MultiLineString(_), Geometry::Rect(_)) => false,
        (Geometry::MultiLineString(_), Geometry::Triangle(_)) => false,
        (Geometry::MultiPolygon(_), Geometry::Point(_)) => false,
        (Geometry::MultiPolygon(_), Geometry::Line(_)) => false,
        (Geometry::MultiPolygon(_), Geometry::LineString(_)) => false,
        (Geometry::MultiPolygon(_), Geometry::Polygon(_)) => false,
        (Geometry::MultiPolygon(_), Geometry::MultiPoint(_)) => false,
        (Geometry::MultiPolygon(_), Geometry::MultiLineString(_)) => false,
        (Geometry::MultiPolygon(_), Geometry::MultiPolygon(_)) => false,
        (Geometry::MultiPolygon(_), Geometry::GeometryCollection(_)) => false,
        (Geometry::MultiPolygon(_), Geometry::Rect(_)) => false,
        (Geometry::MultiPolygon(_), Geometry::Triangle(_)) => false,
        (Geometry::GeometryCollection(_), Geometry::Point(_)) => false,
        (Geometry::GeometryCollection(_), Geometry::Line(_)) => false,
        (Geometry::GeometryCollection(_), Geometry::LineString(_)) => false,
        (Geometry::GeometryCollection(_), Geometry::Polygon(_)) => false,
        (Geometry::GeometryCollection(_), Geometry::MultiPoint(_)) => false,
        (Geometry::GeometryCollection(_), Geometry::MultiLineString(_)) => false,
        (Geometry::GeometryCollection(_), Geometry::MultiPolygon(_)) => false,
        (Geometry::GeometryCollection(_), Geometry::GeometryCollection(_)) => false,
        (Geometry::GeometryCollection(_), Geometry::Rect(_)) => false,
        (Geometry::GeometryCollection(_), Geometry::Triangle(_)) => false,
        (Geometry::Rect(_), Geometry::Point(_)) => false,
        (Geometry::Rect(_), Geometry::Line(_)) => false,
        (Geometry::Rect(_), Geometry::LineString(_)) => false,
        (Geometry::Rect(_), Geometry::Polygon(_)) => false,
        (Geometry::Rect(_), Geometry::MultiPoint(_)) => false,
        (Geometry::Rect(_), Geometry::MultiLineString(_)) => false,
        (Geometry::Rect(_), Geometry::MultiPolygon(_)) => false,
        (Geometry::Rect(_), Geometry::GeometryCollection(_)) => false,
        (Geometry::Rect(_), Geometry::Rect(_)) => false,
        (Geometry::Rect(_), Geometry::Triangle(_)) => false,
        (Geometry::Triangle(_), Geometry::Point(_)) => false,
        (Geometry::Triangle(_), Geometry::Line(_)) => false,
        (Geometry::Triangle(_), Geometry::LineString(_)) => false,
        (Geometry::Triangle(_), Geometry::Polygon(_)) => false,
        (Geometry::Triangle(_), Geometry::MultiPoint(_)) => false,
        (Geometry::Triangle(_), Geometry::MultiLineString(_)) => false,
        (Geometry::Triangle(_), Geometry::MultiPolygon(_)) => false,
        (Geometry::Triangle(_), Geometry::GeometryCollection(_)) => false,
        (Geometry::Triangle(_), Geometry::Rect(_)) => false,
        (Geometry::Triangle(_), Geometry::Triangle(_)) => false,
    }
}

fn geodesic_distance_between_geometries(geom1: &Geometry<f64>, geom2: &Geometry<f64>) -> f64 {
    match (geom1, geom2) {
        (Geometry::Point(p1), Geometry::Point(p2)) => p1.geodesic_distance(p2),
        (Geometry::Point(p), Geometry::LineString(l))
        | (Geometry::LineString(l), Geometry::Point(p)) => l
            .points()
            .map(|lp| p.geodesic_distance(&lp))
            .fold(f64::INFINITY, f64::min),
        (Geometry::Point(p), Geometry::Polygon(poly))
        | (Geometry::Polygon(poly), Geometry::Point(p)) => poly
            .exterior()
            .points()
            .map(|pp| p.geodesic_distance(&pp))
            .fold(f64::INFINITY, f64::min),
        (Geometry::LineString(l1), Geometry::LineString(l2)) => l1
            .points()
            .flat_map(|p1| l2.points().map(move |p2| p1.geodesic_distance(&p2)))
            .fold(f64::INFINITY, f64::min),
        (Geometry::Polygon(poly1), Geometry::Polygon(poly2)) => poly1
            .exterior()
            .points()
            .flat_map(|p1| {
                poly2
                    .exterior()
                    .points()
                    .map(move |p2| p1.geodesic_distance(&p2))
            })
            .fold(f64::INFINITY, f64::min),
        (Geometry::Point(_), Geometry::Line(_)) => 0.0,
        (Geometry::Point(_), Geometry::MultiPoint(_)) => 0.0,
        (Geometry::Point(_), Geometry::MultiLineString(_)) => 0.0,
        (Geometry::Point(_), Geometry::MultiPolygon(_)) => 0.0,
        (Geometry::Point(_), Geometry::GeometryCollection(_)) => 0.0,
        (Geometry::Point(_), Geometry::Rect(_)) => 0.0,
        (Geometry::Point(_), Geometry::Triangle(_)) => 0.0,
        (Geometry::Line(_), Geometry::Point(_)) => 0.0,
        (Geometry::Line(_), Geometry::Line(_)) => 0.0,
        (Geometry::Line(_), Geometry::LineString(_)) => 0.0,
        (Geometry::Line(_), Geometry::Polygon(_)) => 0.0,
        (Geometry::Line(_), Geometry::MultiPoint(_)) => 0.0,
        (Geometry::Line(_), Geometry::MultiLineString(_)) => 0.0,
        (Geometry::Line(_), Geometry::MultiPolygon(_)) => 0.0,
        (Geometry::Line(_), Geometry::GeometryCollection(_)) => 0.0,
        (Geometry::Line(_), Geometry::Rect(_)) => 0.0,
        (Geometry::Line(_), Geometry::Triangle(_)) => 0.0,
        (Geometry::LineString(_), Geometry::Line(_)) => 0.0,
        (Geometry::LineString(_), Geometry::Polygon(_)) => 0.0,
        (Geometry::LineString(_), Geometry::MultiPoint(_)) => 0.0,
        (Geometry::LineString(_), Geometry::MultiLineString(_)) => 0.0,
        (Geometry::LineString(_), Geometry::MultiPolygon(_)) => 0.0,
        (Geometry::LineString(_), Geometry::GeometryCollection(_)) => 0.0,
        (Geometry::LineString(_), Geometry::Rect(_)) => 0.0,
        (Geometry::LineString(_), Geometry::Triangle(_)) => 0.0,
        (Geometry::Polygon(_), Geometry::Line(_)) => 0.0,
        (Geometry::Polygon(_), Geometry::LineString(_)) => 0.0,
        (Geometry::Polygon(_), Geometry::MultiPoint(_)) => 0.0,
        (Geometry::Polygon(_), Geometry::MultiLineString(_)) => 0.0,
        (Geometry::Polygon(_), Geometry::MultiPolygon(_)) => 0.0,
        (Geometry::Polygon(_), Geometry::GeometryCollection(_)) => 0.0,
        (Geometry::Polygon(_), Geometry::Rect(_)) => 0.0,
        (Geometry::Polygon(_), Geometry::Triangle(_)) => 0.0,
        (Geometry::MultiPoint(_), Geometry::Point(_)) => 0.0,
        (Geometry::MultiPoint(_), Geometry::Line(_)) => 0.0,
        (Geometry::MultiPoint(_), Geometry::LineString(_)) => 0.0,
        (Geometry::MultiPoint(_), Geometry::Polygon(_)) => 0.0,
        (Geometry::MultiPoint(_), Geometry::MultiPoint(_)) => 0.0,
        (Geometry::MultiPoint(_), Geometry::MultiLineString(_)) => 0.0,
        (Geometry::MultiPoint(_), Geometry::MultiPolygon(_)) => 0.0,
        (Geometry::MultiPoint(_), Geometry::GeometryCollection(_)) => 0.0,
        (Geometry::MultiPoint(_), Geometry::Rect(_)) => 0.0,
        (Geometry::MultiPoint(_), Geometry::Triangle(_)) => 0.0,
        (Geometry::MultiLineString(_), Geometry::Point(_)) => 0.0,
        (Geometry::MultiLineString(_), Geometry::Line(_)) => 0.0,
        (Geometry::MultiLineString(_), Geometry::LineString(_)) => 0.0,
        (Geometry::MultiLineString(_), Geometry::Polygon(_)) => 0.0,
        (Geometry::MultiLineString(_), Geometry::MultiPoint(_)) => 0.0,
        (Geometry::MultiLineString(_), Geometry::MultiLineString(_)) => 0.0,
        (Geometry::MultiLineString(_), Geometry::MultiPolygon(_)) => 0.0,
        (Geometry::MultiLineString(_), Geometry::GeometryCollection(_)) => 0.0,
        (Geometry::MultiLineString(_), Geometry::Rect(_)) => 0.0,
        (Geometry::MultiLineString(_), Geometry::Triangle(_)) => 0.0,
        (Geometry::MultiPolygon(_), Geometry::Point(_)) => 0.0,
        (Geometry::MultiPolygon(_), Geometry::Line(_)) => 0.0,
        (Geometry::MultiPolygon(_), Geometry::LineString(_)) => 0.0,
        (Geometry::MultiPolygon(_), Geometry::Polygon(_)) => 0.0,
        (Geometry::MultiPolygon(_), Geometry::MultiPoint(_)) => 0.0,
        (Geometry::MultiPolygon(_), Geometry::MultiLineString(_)) => 0.0,
        (Geometry::MultiPolygon(_), Geometry::MultiPolygon(_)) => 0.0,
        (Geometry::MultiPolygon(_), Geometry::GeometryCollection(_)) => 0.0,
        (Geometry::MultiPolygon(_), Geometry::Rect(_)) => 0.0,
        (Geometry::MultiPolygon(_), Geometry::Triangle(_)) => 0.0,
        (Geometry::GeometryCollection(_), Geometry::Point(_)) => 0.0,
        (Geometry::GeometryCollection(_), Geometry::Line(_)) => 0.0,
        (Geometry::GeometryCollection(_), Geometry::LineString(_)) => 0.0,
        (Geometry::GeometryCollection(_), Geometry::Polygon(_)) => 0.0,
        (Geometry::GeometryCollection(_), Geometry::MultiPoint(_)) => 0.0,
        (Geometry::GeometryCollection(_), Geometry::MultiLineString(_)) => 0.0,
        (Geometry::GeometryCollection(_), Geometry::MultiPolygon(_)) => 0.0,
        (Geometry::GeometryCollection(_), Geometry::GeometryCollection(_)) => 0.0,
        (Geometry::GeometryCollection(_), Geometry::Rect(_)) => 0.0,
        (Geometry::GeometryCollection(_), Geometry::Triangle(_)) => 0.0,
        (Geometry::Rect(_), Geometry::Point(_)) => 0.0,
        (Geometry::Rect(_), Geometry::Line(_)) => 0.0,
        (Geometry::Rect(_), Geometry::LineString(_)) => 0.0,
        (Geometry::Rect(_), Geometry::Polygon(_)) => 0.0,
        (Geometry::Rect(_), Geometry::MultiPoint(_)) => 0.0,
        (Geometry::Rect(_), Geometry::MultiLineString(_)) => 0.0,
        (Geometry::Rect(_), Geometry::MultiPolygon(_)) => 0.0,
        (Geometry::Rect(_), Geometry::GeometryCollection(_)) => 0.0,
        (Geometry::Rect(_), Geometry::Rect(_)) => 0.0,
        (Geometry::Rect(_), Geometry::Triangle(_)) => 0.0,
        (Geometry::Triangle(_), Geometry::Point(_)) => 0.0,
        (Geometry::Triangle(_), Geometry::Line(_)) => 0.0,
        (Geometry::Triangle(_), Geometry::LineString(_)) => 0.0,
        (Geometry::Triangle(_), Geometry::Polygon(_)) => 0.0,
        (Geometry::Triangle(_), Geometry::MultiPoint(_)) => 0.0,
        (Geometry::Triangle(_), Geometry::MultiLineString(_)) => 0.0,
        (Geometry::Triangle(_), Geometry::MultiPolygon(_)) => 0.0,
        (Geometry::Triangle(_), Geometry::GeometryCollection(_)) => 0.0,
        (Geometry::Triangle(_), Geometry::Rect(_)) => 0.0,
        (Geometry::Triangle(_), Geometry::Triangle(_)) => 0.0,
    }
}
