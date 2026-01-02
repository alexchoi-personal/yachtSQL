#![coverage(off)]

use geo::algorithm::bounding_rect::BoundingRect;
use geo::algorithm::centroid::Centroid;
use geo::algorithm::convex_hull::ConvexHull;
use geo::algorithm::geodesic_distance::GeodesicDistance;
use geo::algorithm::simplify_vw::SimplifyVw;
use geo::{BooleanOps, Coord, Geometry, LineString, MultiPolygon, Point, Polygon};
use wkt::TryFromWkt;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

use super::utils::{extract_points, geometry_to_wkt};

pub fn fn_st_centroid(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) => {
            let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let centroid = geom.centroid();
            match centroid {
                Some(p) => Ok(Value::Geography(format!("POINT({} {})", p.x(), p.y()))),
                None => Ok(Value::Null),
            }
        }
        _ => Err(Error::InvalidQuery(
            "ST_CENTROID expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_buffer(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Ok(Value::Null);
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Geography(wkt), Value::Float64(_)) | (Value::Geography(wkt), Value::Int64(_)) => {
            let distance_meters = match &args[1] {
                Value::Float64(f) => f.0,
                Value::Int64(i) => *i as f64,
                _ => 0.0,
            };
            let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let buffered = create_buffer(&geom, distance_meters);
            Ok(Value::Geography(geometry_to_wkt(&buffered)))
        }
        _ => Err(Error::InvalidQuery(
            "ST_BUFFER expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_boundingbox(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) => {
            let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let rect = geom.bounding_rect();
            match rect {
                Some(r) => {
                    let min = r.min();
                    let max = r.max();
                    let bbox_wkt = format!(
                        "POLYGON(({} {}, {} {}, {} {}, {} {}, {} {}))",
                        min.x, min.y, min.x, max.y, max.x, max.y, max.x, min.y, min.x, min.y
                    );
                    Ok(Value::Geography(bbox_wkt))
                }
                None => Ok(Value::Null),
            }
        }
        _ => Err(Error::InvalidQuery(
            "ST_BOUNDINGBOX expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_closestpoint(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ST_CLOSESTPOINT requires two geography arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Geography(wkt1), Value::Geography(wkt2)) => {
            let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let closest = find_closest_point(&geom1, &geom2);
            Ok(Value::Geography(format!(
                "POINT({} {})",
                closest.x(),
                closest.y()
            )))
        }
        _ => Err(Error::InvalidQuery(
            "ST_CLOSESTPOINT expects geography arguments".into(),
        )),
    }
}

pub fn fn_st_union(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ST_UNION requires two geography arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Geography(wkt1), Value::Geography(wkt2)) => {
            let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let result = geometry_union(&geom1, &geom2);
            Ok(Value::Geography(geometry_to_wkt(&result)))
        }
        _ => Err(Error::InvalidQuery(
            "ST_UNION expects geography arguments".into(),
        )),
    }
}

pub fn fn_st_intersection(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ST_INTERSECTION requires two geography arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Geography(wkt1), Value::Geography(wkt2)) => {
            let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let result = geometry_intersection(&geom1, &geom2);
            Ok(Value::Geography(geometry_to_wkt(&result)))
        }
        _ => Err(Error::InvalidQuery(
            "ST_INTERSECTION expects geography arguments".into(),
        )),
    }
}

pub fn fn_st_difference(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ST_DIFFERENCE requires two geography arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Geography(wkt1), Value::Geography(wkt2)) => {
            let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let result = geometry_difference(&geom1, &geom2);
            Ok(Value::Geography(geometry_to_wkt(&result)))
        }
        _ => Err(Error::InvalidQuery(
            "ST_DIFFERENCE expects geography arguments".into(),
        )),
    }
}

pub fn fn_st_convexhull(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) => {
            let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let hull = geom.convex_hull();
            Ok(Value::Geography(geometry_to_wkt(&Geometry::Polygon(hull))))
        }
        _ => Err(Error::InvalidQuery(
            "ST_CONVEXHULL expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_simplify(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Ok(Value::Null);
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) => Ok(Value::Null),
        (Value::Geography(wkt), tolerance_val) => {
            let epsilon = match tolerance_val {
                Value::Float64(f) => f.0,
                Value::Int64(i) => *i as f64,
                _ => 0.0,
            };
            let epsilon_degrees = epsilon / 111_320.0;
            let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let simplified = match geom {
                Geometry::LineString(ls) => {
                    let simplified = ls.simplify_vw(&epsilon_degrees);
                    Geometry::LineString(simplified)
                }
                Geometry::Polygon(poly) => {
                    let simplified_exterior = poly.exterior().simplify_vw(&epsilon_degrees);
                    Geometry::Polygon(Polygon::new(simplified_exterior, vec![]))
                }
                other => other,
            };
            Ok(Value::Geography(geometry_to_wkt(&simplified)))
        }
        _ => Err(Error::InvalidQuery(
            "ST_SIMPLIFY expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_snaptogrid(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Ok(Value::Null);
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) => Ok(Value::Null),
        (Value::Geography(wkt), grid_val) => {
            let grid_size = match grid_val {
                Value::Float64(f) => f.0,
                Value::Int64(i) => *i as f64,
                _ => 1.0,
            };
            let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let snapped = snap_geometry_to_grid(&geom, grid_size);
            Ok(Value::Geography(geometry_to_wkt(&snapped)))
        }
        _ => Err(Error::InvalidQuery(
            "ST_SNAPTOGRID expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_boundary(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Geography(wkt) => {
            let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let boundary = match geom {
                Geometry::Polygon(poly) => Geometry::LineString(poly.exterior().clone()),
                Geometry::LineString(ls) => {
                    if ls.0.len() >= 2 {
                        let start = ls.0.first().unwrap();
                        let end = ls.0.last().unwrap();
                        if start == end {
                            Geometry::GeometryCollection(geo_types::GeometryCollection::new_from(
                                vec![],
                            ))
                        } else {
                            Geometry::MultiPoint(geo_types::MultiPoint::new(vec![
                                Point::new(start.x, start.y),
                                Point::new(end.x, end.y),
                            ]))
                        }
                    } else {
                        Geometry::GeometryCollection(geo_types::GeometryCollection::new_from(
                            vec![],
                        ))
                    }
                }
                _ => Geometry::GeometryCollection(geo_types::GeometryCollection::new_from(vec![])),
            };
            Ok(Value::Geography(geometry_to_wkt(&boundary)))
        }
        _ => Err(Error::InvalidQuery(
            "ST_BOUNDARY expects a geography argument".into(),
        )),
    }
}

pub fn fn_st_bufferwithtolerance(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Ok(Value::Null);
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Geography(wkt), Value::Float64(_)) | (Value::Geography(wkt), Value::Int64(_)) => {
            let distance_meters = match &args[1] {
                Value::Float64(f) => f.0,
                Value::Int64(i) => *i as f64,
                _ => 0.0,
            };
            let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
            let buffered = create_buffer(&geom, distance_meters);
            Ok(Value::Geography(geometry_to_wkt(&buffered)))
        }
        _ => Err(Error::InvalidQuery(
            "ST_BUFFERWITHTOLERANCE expects a geography argument".into(),
        )),
    }
}

fn create_buffer(geom: &Geometry<f64>, distance_meters: f64) -> Geometry<f64> {
    match geom {
        Geometry::Point(p) => {
            let num_segments = 32;
            let deg_per_meter_lat = 1.0 / 111_320.0;
            let deg_per_meter_lon = 1.0 / (111_320.0 * p.y().to_radians().cos());
            let mut coords = Vec::with_capacity(num_segments + 1);
            for i in 0..num_segments {
                let angle = 2.0 * std::f64::consts::PI * (i as f64) / (num_segments as f64);
                let dx = distance_meters * angle.cos() * deg_per_meter_lon;
                let dy = distance_meters * angle.sin() * deg_per_meter_lat;
                coords.push(Coord {
                    x: p.x() + dx,
                    y: p.y() + dy,
                });
            }
            coords.push(coords[0]);
            Geometry::Polygon(Polygon::new(LineString::new(coords), vec![]))
        }
        _ => geom.clone(),
    }
}

fn find_closest_point(geom1: &Geometry<f64>, geom2: &Geometry<f64>) -> Point<f64> {
    let target_point = match geom2 {
        Geometry::Point(p) => *p,
        _ => geom2.centroid().unwrap_or(Point::new(0.0, 0.0)),
    };

    let points = extract_points(geom1);
    if points.is_empty() {
        return Point::new(0.0, 0.0);
    }

    points
        .into_iter()
        .min_by(|a, b| {
            let da = a.geodesic_distance(&target_point);
            let db = b.geodesic_distance(&target_point);
            da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
        })
        .unwrap_or(Point::new(0.0, 0.0))
}

fn geometry_union(geom1: &Geometry<f64>, geom2: &Geometry<f64>) -> Geometry<f64> {
    match (geom1, geom2) {
        (Geometry::Polygon(p1), Geometry::Polygon(p2)) => {
            let mp1 = MultiPolygon::new(vec![p1.clone()]);
            let mp2 = MultiPolygon::new(vec![p2.clone()]);
            Geometry::MultiPolygon(mp1.union(&mp2))
        }
        (Geometry::MultiPolygon(mp1), Geometry::Polygon(p2)) => {
            let mp2 = MultiPolygon::new(vec![p2.clone()]);
            Geometry::MultiPolygon(mp1.union(&mp2))
        }
        (Geometry::Polygon(p1), Geometry::MultiPolygon(mp2)) => {
            let mp1 = MultiPolygon::new(vec![p1.clone()]);
            Geometry::MultiPolygon(mp1.union(mp2))
        }
        (Geometry::MultiPolygon(mp1), Geometry::MultiPolygon(mp2)) => {
            Geometry::MultiPolygon(mp1.union(mp2))
        }
        _ => geom1.clone(),
    }
}

fn geometry_intersection(geom1: &Geometry<f64>, geom2: &Geometry<f64>) -> Geometry<f64> {
    match (geom1, geom2) {
        (Geometry::Polygon(p1), Geometry::Polygon(p2)) => {
            let mp1 = MultiPolygon::new(vec![p1.clone()]);
            let mp2 = MultiPolygon::new(vec![p2.clone()]);
            Geometry::MultiPolygon(mp1.intersection(&mp2))
        }
        (Geometry::MultiPolygon(mp1), Geometry::Polygon(p2)) => {
            let mp2 = MultiPolygon::new(vec![p2.clone()]);
            Geometry::MultiPolygon(mp1.intersection(&mp2))
        }
        (Geometry::Polygon(p1), Geometry::MultiPolygon(mp2)) => {
            let mp1 = MultiPolygon::new(vec![p1.clone()]);
            Geometry::MultiPolygon(mp1.intersection(mp2))
        }
        (Geometry::MultiPolygon(mp1), Geometry::MultiPolygon(mp2)) => {
            Geometry::MultiPolygon(mp1.intersection(mp2))
        }
        _ => Geometry::GeometryCollection(geo_types::GeometryCollection::new_from(vec![])),
    }
}

fn geometry_difference(geom1: &Geometry<f64>, geom2: &Geometry<f64>) -> Geometry<f64> {
    match (geom1, geom2) {
        (Geometry::Polygon(p1), Geometry::Polygon(p2)) => {
            let mp1 = MultiPolygon::new(vec![p1.clone()]);
            let mp2 = MultiPolygon::new(vec![p2.clone()]);
            Geometry::MultiPolygon(mp1.difference(&mp2))
        }
        (Geometry::MultiPolygon(mp1), Geometry::Polygon(p2)) => {
            let mp2 = MultiPolygon::new(vec![p2.clone()]);
            Geometry::MultiPolygon(mp1.difference(&mp2))
        }
        (Geometry::Polygon(p1), Geometry::MultiPolygon(mp2)) => {
            let mp1 = MultiPolygon::new(vec![p1.clone()]);
            Geometry::MultiPolygon(mp1.difference(mp2))
        }
        (Geometry::MultiPolygon(mp1), Geometry::MultiPolygon(mp2)) => {
            Geometry::MultiPolygon(mp1.difference(mp2))
        }
        _ => geom1.clone(),
    }
}

fn snap_geometry_to_grid(geom: &Geometry<f64>, grid_size: f64) -> Geometry<f64> {
    let snap = |v: f64| -> f64 { (v / grid_size).round() * grid_size };
    match geom {
        Geometry::Point(p) => Geometry::Point(Point::new(snap(p.x()), snap(p.y()))),
        Geometry::LineString(ls) => {
            let coords: Vec<Coord<f64>> = ls
                .coords()
                .map(|c| Coord {
                    x: snap(c.x),
                    y: snap(c.y),
                })
                .collect();
            Geometry::LineString(LineString::new(coords))
        }
        Geometry::Polygon(poly) => {
            let exterior: Vec<Coord<f64>> = poly
                .exterior()
                .coords()
                .map(|c| Coord {
                    x: snap(c.x),
                    y: snap(c.y),
                })
                .collect();
            Geometry::Polygon(Polygon::new(LineString::new(exterior), vec![]))
        }
        other => other.clone(),
    }
}
