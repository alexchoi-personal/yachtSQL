#![coverage(off)]

use geo::algorithm::geodesic_distance::GeodesicDistance;
use geo::{Geometry, Point};

pub fn geodesic_distance_between_geometries(geom1: &Geometry<f64>, geom2: &Geometry<f64>) -> f64 {
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
        (Geometry::Point(_), Geometry::MultiPoint(_))
        | (Geometry::MultiPoint(_), Geometry::Point(_))
        | (Geometry::Point(_), Geometry::MultiLineString(_))
        | (Geometry::MultiLineString(_), Geometry::Point(_))
        | (Geometry::Point(_), Geometry::MultiPolygon(_))
        | (Geometry::MultiPolygon(_), Geometry::Point(_))
        | (Geometry::Point(_), Geometry::GeometryCollection(_))
        | (Geometry::GeometryCollection(_), Geometry::Point(_))
        | (Geometry::Point(_), Geometry::Line(_))
        | (Geometry::Line(_), Geometry::Point(_))
        | (Geometry::Point(_), Geometry::Rect(_))
        | (Geometry::Rect(_), Geometry::Point(_))
        | (Geometry::Point(_), Geometry::Triangle(_))
        | (Geometry::Triangle(_), Geometry::Point(_))
        | (Geometry::LineString(_), Geometry::Polygon(_))
        | (Geometry::Polygon(_), Geometry::LineString(_))
        | (Geometry::LineString(_), Geometry::MultiPoint(_))
        | (Geometry::MultiPoint(_), Geometry::LineString(_))
        | (Geometry::LineString(_), Geometry::MultiLineString(_))
        | (Geometry::MultiLineString(_), Geometry::LineString(_))
        | (Geometry::LineString(_), Geometry::MultiPolygon(_))
        | (Geometry::MultiPolygon(_), Geometry::LineString(_))
        | (Geometry::LineString(_), Geometry::GeometryCollection(_))
        | (Geometry::GeometryCollection(_), Geometry::LineString(_))
        | (Geometry::LineString(_), Geometry::Line(_))
        | (Geometry::Line(_), Geometry::LineString(_))
        | (Geometry::LineString(_), Geometry::Rect(_))
        | (Geometry::Rect(_), Geometry::LineString(_))
        | (Geometry::LineString(_), Geometry::Triangle(_))
        | (Geometry::Triangle(_), Geometry::LineString(_))
        | (Geometry::Polygon(_), Geometry::MultiPoint(_))
        | (Geometry::MultiPoint(_), Geometry::Polygon(_))
        | (Geometry::Polygon(_), Geometry::MultiLineString(_))
        | (Geometry::MultiLineString(_), Geometry::Polygon(_))
        | (Geometry::Polygon(_), Geometry::MultiPolygon(_))
        | (Geometry::MultiPolygon(_), Geometry::Polygon(_))
        | (Geometry::Polygon(_), Geometry::GeometryCollection(_))
        | (Geometry::GeometryCollection(_), Geometry::Polygon(_))
        | (Geometry::Polygon(_), Geometry::Line(_))
        | (Geometry::Line(_), Geometry::Polygon(_))
        | (Geometry::Polygon(_), Geometry::Rect(_))
        | (Geometry::Rect(_), Geometry::Polygon(_))
        | (Geometry::Polygon(_), Geometry::Triangle(_))
        | (Geometry::Triangle(_), Geometry::Polygon(_))
        | (Geometry::MultiPoint(_), Geometry::MultiPoint(_))
        | (Geometry::MultiPoint(_), Geometry::MultiLineString(_))
        | (Geometry::MultiLineString(_), Geometry::MultiPoint(_))
        | (Geometry::MultiPoint(_), Geometry::MultiPolygon(_))
        | (Geometry::MultiPolygon(_), Geometry::MultiPoint(_))
        | (Geometry::MultiPoint(_), Geometry::GeometryCollection(_))
        | (Geometry::GeometryCollection(_), Geometry::MultiPoint(_))
        | (Geometry::MultiPoint(_), Geometry::Line(_))
        | (Geometry::Line(_), Geometry::MultiPoint(_))
        | (Geometry::MultiPoint(_), Geometry::Rect(_))
        | (Geometry::Rect(_), Geometry::MultiPoint(_))
        | (Geometry::MultiPoint(_), Geometry::Triangle(_))
        | (Geometry::Triangle(_), Geometry::MultiPoint(_))
        | (Geometry::MultiLineString(_), Geometry::MultiLineString(_))
        | (Geometry::MultiLineString(_), Geometry::MultiPolygon(_))
        | (Geometry::MultiPolygon(_), Geometry::MultiLineString(_))
        | (Geometry::MultiLineString(_), Geometry::GeometryCollection(_))
        | (Geometry::GeometryCollection(_), Geometry::MultiLineString(_))
        | (Geometry::MultiLineString(_), Geometry::Line(_))
        | (Geometry::Line(_), Geometry::MultiLineString(_))
        | (Geometry::MultiLineString(_), Geometry::Rect(_))
        | (Geometry::Rect(_), Geometry::MultiLineString(_))
        | (Geometry::MultiLineString(_), Geometry::Triangle(_))
        | (Geometry::Triangle(_), Geometry::MultiLineString(_))
        | (Geometry::MultiPolygon(_), Geometry::MultiPolygon(_))
        | (Geometry::MultiPolygon(_), Geometry::GeometryCollection(_))
        | (Geometry::GeometryCollection(_), Geometry::MultiPolygon(_))
        | (Geometry::MultiPolygon(_), Geometry::Line(_))
        | (Geometry::Line(_), Geometry::MultiPolygon(_))
        | (Geometry::MultiPolygon(_), Geometry::Rect(_))
        | (Geometry::Rect(_), Geometry::MultiPolygon(_))
        | (Geometry::MultiPolygon(_), Geometry::Triangle(_))
        | (Geometry::Triangle(_), Geometry::MultiPolygon(_))
        | (Geometry::GeometryCollection(_), Geometry::GeometryCollection(_))
        | (Geometry::GeometryCollection(_), Geometry::Line(_))
        | (Geometry::Line(_), Geometry::GeometryCollection(_))
        | (Geometry::GeometryCollection(_), Geometry::Rect(_))
        | (Geometry::Rect(_), Geometry::GeometryCollection(_))
        | (Geometry::GeometryCollection(_), Geometry::Triangle(_))
        | (Geometry::Triangle(_), Geometry::GeometryCollection(_))
        | (Geometry::Line(_), Geometry::Line(_))
        | (Geometry::Line(_), Geometry::Rect(_))
        | (Geometry::Rect(_), Geometry::Line(_))
        | (Geometry::Line(_), Geometry::Triangle(_))
        | (Geometry::Triangle(_), Geometry::Line(_))
        | (Geometry::Rect(_), Geometry::Rect(_))
        | (Geometry::Rect(_), Geometry::Triangle(_))
        | (Geometry::Triangle(_), Geometry::Rect(_))
        | (Geometry::Triangle(_), Geometry::Triangle(_)) => 0.0,
    }
}

pub fn extract_points(geom: &Geometry<f64>) -> Vec<Point<f64>> {
    match geom {
        Geometry::Point(p) => vec![*p],
        Geometry::LineString(ls) => ls.points().collect(),
        Geometry::Polygon(poly) => poly.exterior().points().collect(),
        Geometry::MultiPoint(mp) => mp.0.clone(),
        Geometry::MultiLineString(mls) => mls.0.iter().flat_map(|ls| ls.points()).collect(),
        Geometry::MultiPolygon(mp) => {
            mp.0.iter()
                .flat_map(|poly| poly.exterior().points())
                .collect()
        }
        Geometry::Line(line) => vec![line.start_point(), line.end_point()],
        Geometry::Rect(rect) => rect.to_polygon().exterior().points().collect(),
        Geometry::Triangle(tri) => tri.to_polygon().exterior().points().collect(),
        Geometry::GeometryCollection(gc) => gc.0.iter().flat_map(extract_points).collect(),
    }
}

pub fn geometry_to_wkt(geom: &Geometry<f64>) -> String {
    use std::fmt::Write;
    match geom {
        Geometry::Point(p) => format!("POINT({} {})", p.x(), p.y()),
        Geometry::LineString(ls) => {
            let coords: Vec<String> = ls.coords().map(|c| format!("{} {}", c.x, c.y)).collect();
            format!("LINESTRING({})", coords.join(", "))
        }
        Geometry::Polygon(poly) => {
            let exterior: Vec<String> = poly
                .exterior()
                .coords()
                .map(|c| format!("{} {}", c.x, c.y))
                .collect();
            if poly.interiors().is_empty() {
                format!("POLYGON(({}))", exterior.join(", "))
            } else {
                let mut result = format!("POLYGON(({})", exterior.join(", "));
                for interior in poly.interiors() {
                    let interior_coords: Vec<String> = interior
                        .coords()
                        .map(|c| format!("{} {}", c.x, c.y))
                        .collect();
                    let _ = write!(result, ", ({})", interior_coords.join(", "));
                }
                result.push(')');
                result
            }
        }
        Geometry::MultiPoint(mp) => {
            let points: Vec<String> =
                mp.0.iter()
                    .map(|p| format!("{} {}", p.x(), p.y()))
                    .collect();
            format!("MULTIPOINT({})", points.join(", "))
        }
        Geometry::MultiLineString(mls) => {
            let lines: Vec<String> = mls
                .0
                .iter()
                .map(|ls| {
                    let coords: Vec<String> =
                        ls.coords().map(|c| format!("{} {}", c.x, c.y)).collect();
                    format!("({})", coords.join(", "))
                })
                .collect();
            format!("MULTILINESTRING({})", lines.join(", "))
        }
        Geometry::MultiPolygon(mp) => {
            let polys: Vec<String> =
                mp.0.iter()
                    .map(|poly| {
                        let exterior: Vec<String> = poly
                            .exterior()
                            .coords()
                            .map(|c| format!("{} {}", c.x, c.y))
                            .collect();
                        format!("(({}))", exterior.join(", "))
                    })
                    .collect();
            format!("MULTIPOLYGON({})", polys.join(", "))
        }
        Geometry::GeometryCollection(gc) => {
            if gc.0.is_empty() {
                "GEOMETRYCOLLECTION EMPTY".to_string()
            } else {
                let geoms: Vec<String> = gc.0.iter().map(geometry_to_wkt).collect();
                format!("GEOMETRYCOLLECTION({})", geoms.join(", "))
            }
        }
        Geometry::Line(line) => {
            format!(
                "LINESTRING({} {}, {} {})",
                line.start.x, line.start.y, line.end.x, line.end.y
            )
        }
        Geometry::Rect(rect) => {
            let poly = rect.to_polygon();
            let exterior: Vec<String> = poly
                .exterior()
                .coords()
                .map(|c| format!("{} {}", c.x, c.y))
                .collect();
            format!("POLYGON(({}))", exterior.join(", "))
        }
        Geometry::Triangle(tri) => {
            let poly = tri.to_polygon();
            let exterior: Vec<String> = poly
                .exterior()
                .coords()
                .map(|c| format!("{} {}", c.x, c.y))
                .collect();
            format!("POLYGON(({}))", exterior.join(", "))
        }
    }
}
