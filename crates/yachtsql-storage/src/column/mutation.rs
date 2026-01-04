#![coverage(off)]

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{IntervalValue, RangeValue, Value};

use super::Column;

impl Column {
    pub fn push(&mut self, value: Value) -> Result<()> {
        match (self, value) {
            (Column::Bool { data, nulls }, Value::Null) => {
                data.push(false);
                nulls.push(true);
            }
            (Column::Bool { data, nulls }, Value::Bool(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Bool { data, nulls }, Value::String(v)) => {
                let b = matches!(v.to_uppercase().as_str(), "TRUE" | "1" | "YES");
                data.push(b);
                nulls.push(false);
            }
            (Column::Bool { data, nulls }, Value::Int64(v)) => {
                data.push(v != 0);
                nulls.push(false);
            }
            (Column::Int64 { data, nulls }, Value::Null) => {
                data.push(0);
                nulls.push(true);
            }
            (Column::Int64 { data, nulls }, Value::Int64(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Int64 { data, nulls }, Value::Float64(v)) => {
                data.push(v.0 as i64);
                nulls.push(false);
            }
            (Column::Int64 { data, nulls }, Value::String(v)) => {
                let n = v
                    .parse::<i64>()
                    .map_err(|_| Error::type_mismatch("INT64", format!("STRING '{}'", v)))?;
                data.push(n);
                nulls.push(false);
            }
            (Column::Float64 { data, nulls }, Value::Null) => {
                data.push(0.0);
                nulls.push(true);
            }
            (Column::Float64 { data, nulls }, Value::Float64(v)) => {
                data.push(v.0);
                nulls.push(false);
            }
            (Column::Float64 { data, nulls }, Value::Int64(v)) => {
                data.push(v as f64);
                nulls.push(false);
            }
            (Column::Float64 { data, nulls }, Value::Numeric(v)) => {
                use rust_decimal::prelude::ToPrimitive;
                let f = v
                    .to_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", format!("NUMERIC {}", v)))?;
                data.push(f);
                nulls.push(false);
            }
            (Column::Numeric { data, nulls }, Value::Null) => {
                data.push(Decimal::ZERO);
                nulls.push(true);
            }
            (Column::Numeric { data, nulls }, Value::Numeric(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Numeric { data, nulls }, Value::Float64(v)) => {
                let d = Decimal::from_f64_retain(v.0)
                    .ok_or_else(|| Error::type_mismatch("NUMERIC", format!("FLOAT64 {}", v.0)))?;
                data.push(d);
                nulls.push(false);
            }
            (Column::Numeric { data, nulls }, Value::Int64(v)) => {
                data.push(Decimal::from(v));
                nulls.push(false);
            }
            (Column::Numeric { data, nulls }, Value::BigNumeric(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Null) => {
                data.push(String::new());
                nulls.push(true);
            }
            (Column::String { data, nulls }, Value::String(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Int64(v)) => {
                data.push(v.to_string());
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Float64(v)) => {
                data.push(v.0.to_string());
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Date(v)) => {
                data.push(v.to_string());
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::DateTime(v)) => {
                data.push(v.to_string());
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Timestamp(v)) => {
                data.push(v.to_rfc3339());
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Time(v)) => {
                data.push(v.to_string());
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Bool(v)) => {
                data.push(if v {
                    "true".to_string()
                } else {
                    "false".to_string()
                });
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Struct(fields)) => {
                let s = format!("{:?}", fields);
                data.push(s);
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Array(arr)) => {
                let s = format!("{:?}", arr);
                data.push(s);
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Numeric(v)) => {
                data.push(v.to_string());
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Bytes(v)) => {
                let s = String::from_utf8_lossy(&v).to_string();
                data.push(s);
                nulls.push(false);
            }
            (Column::Bytes { data, nulls }, Value::Null) => {
                data.push(Vec::new());
                nulls.push(true);
            }
            (Column::Bytes { data, nulls }, Value::Bytes(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Date { data, nulls }, Value::Null) => {
                data.push(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
                nulls.push(true);
            }
            (Column::Date { data, nulls }, Value::Date(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Time { data, nulls }, Value::Null) => {
                data.push(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
                nulls.push(true);
            }
            (Column::Time { data, nulls }, Value::Time(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::DateTime { data, nulls }, Value::Null) => {
                data.push(chrono::DateTime::UNIX_EPOCH.naive_utc());
                nulls.push(true);
            }
            (Column::DateTime { data, nulls }, Value::DateTime(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Timestamp { data, nulls }, Value::Null) => {
                data.push(DateTime::UNIX_EPOCH);
                nulls.push(true);
            }
            (Column::Timestamp { data, nulls }, Value::Timestamp(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Json { data, nulls }, Value::Null) => {
                data.push(serde_json::Value::Null);
                nulls.push(true);
            }
            (Column::Json { data, nulls }, Value::Json(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Json { data, nulls }, Value::String(v)) => {
                let json_val = serde_json::from_str(&v).unwrap_or(serde_json::Value::String(v));
                data.push(json_val);
                nulls.push(false);
            }
            (Column::Array { data, nulls, .. }, Value::Null) => {
                data.push(Vec::new());
                nulls.push(true);
            }
            (Column::Array { data, nulls, .. }, Value::Array(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Struct { data, nulls, .. }, Value::Null) => {
                data.push(Vec::new());
                nulls.push(true);
            }
            (
                Column::Struct {
                    data,
                    nulls,
                    fields,
                },
                Value::Struct(v),
            ) => {
                let coerced = Column::coerce_struct_value(v, fields);
                data.push(coerced);
                nulls.push(false);
            }
            (Column::Geography { data, nulls }, Value::Null) => {
                data.push(String::new());
                nulls.push(true);
            }
            (Column::Geography { data, nulls }, Value::Geography(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Interval { data, nulls }, Value::Null) => {
                data.push(IntervalValue {
                    months: 0,
                    days: 0,
                    nanos: 0,
                });
                nulls.push(true);
            }
            (Column::Interval { data, nulls }, Value::Interval(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Range { data, nulls, .. }, Value::Null) => {
                data.push(RangeValue::new(None, None));
                nulls.push(true);
            }
            (Column::Range { data, nulls, .. }, Value::Range(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Bool { data, nulls }, Value::Default) => {
                data.push(false);
                nulls.push(true);
            }
            (Column::Int64 { data, nulls }, Value::Default) => {
                data.push(0);
                nulls.push(true);
            }
            (Column::Float64 { data, nulls }, Value::Default) => {
                data.push(0.0);
                nulls.push(true);
            }
            (Column::Numeric { data, nulls }, Value::Default) => {
                data.push(Decimal::ZERO);
                nulls.push(true);
            }
            (Column::String { data, nulls }, Value::Default) => {
                data.push(String::new());
                nulls.push(true);
            }
            (Column::Bytes { data, nulls }, Value::Default) => {
                data.push(Vec::new());
                nulls.push(true);
            }
            (Column::Date { data, nulls }, Value::Default) => {
                data.push(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
                nulls.push(true);
            }
            (Column::Time { data, nulls }, Value::Default) => {
                data.push(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
                nulls.push(true);
            }
            (Column::Timestamp { data, nulls }, Value::Default) => {
                data.push(DateTime::UNIX_EPOCH);
                nulls.push(true);
            }
            (Column::DateTime { data, nulls }, Value::Default) => {
                data.push(NaiveDateTime::default());
                nulls.push(true);
            }
            (Column::Interval { data, nulls }, Value::Default) => {
                data.push(IntervalValue::new(0, 0, 0));
                nulls.push(true);
            }
            (Column::Json { data, nulls }, Value::Default) => {
                data.push(serde_json::Value::Null);
                nulls.push(true);
            }
            (Column::Array { data, nulls, .. }, Value::Default) => {
                data.push(Vec::new());
                nulls.push(true);
            }
            (Column::Struct { data, nulls, .. }, Value::Default) => {
                data.push(Vec::new());
                nulls.push(true);
            }
            (Column::Geography { data, nulls }, Value::Default) => {
                data.push(String::new());
                nulls.push(true);
            }
            (Column::Range { data, nulls, .. }, Value::Default) => {
                data.push(RangeValue::new(None, None));
                nulls.push(true);
            }
            (col, val) => {
                return Err(Error::type_mismatch(
                    format!("{:?}", col.data_type()),
                    format!("{:?}", val.data_type()),
                ));
            }
        }
        Ok(())
    }

    pub fn remove(&mut self, index: usize) {
        match self {
            Column::Bool { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Int64 { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Float64 { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Numeric { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::String { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Bytes { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Date { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Time { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::DateTime { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Timestamp { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Json { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Array { data, nulls, .. } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Struct { data, nulls, .. } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Geography { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Interval { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Range { data, nulls, .. } => {
                data.remove(index);
                nulls.remove(index);
            }
        }
    }

    pub fn clear(&mut self) {
        match self {
            Column::Bool { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Int64 { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Float64 { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Numeric { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::String { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Bytes { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Date { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Time { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::DateTime { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Timestamp { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Json { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Array { data, nulls, .. } => {
                data.clear();
                nulls.clear();
            }
            Column::Struct { data, nulls, .. } => {
                data.clear();
                nulls.clear();
            }
            Column::Geography { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Interval { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Range { data, nulls, .. } => {
                data.clear();
                nulls.clear();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use aligned_vec::AVec;
    use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
    use rust_decimal::Decimal;
    use yachtsql_common::types::{DataType, StructField};

    use super::*;
    use crate::NullBitmap;

    #[test]
    fn test_push_bool_null() {
        let mut col = Column::new(&DataType::Bool);
        col.push(Value::Null).unwrap();
        assert_eq!(col.len(), 1);
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_bool_value() {
        let mut col = Column::new(&DataType::Bool);
        col.push(Value::Bool(true)).unwrap();
        col.push(Value::Bool(false)).unwrap();
        assert_eq!(col.len(), 2);
        assert!(!col.is_null(0));
        assert!(!col.is_null(1));
        assert_eq!(col.get(0).unwrap(), Value::Bool(true));
        assert_eq!(col.get(1).unwrap(), Value::Bool(false));
    }

    #[test]
    fn test_push_bool_from_string() {
        let mut col = Column::new(&DataType::Bool);
        col.push(Value::String("TRUE".to_string())).unwrap();
        col.push(Value::String("1".to_string())).unwrap();
        col.push(Value::String("YES".to_string())).unwrap();
        col.push(Value::String("false".to_string())).unwrap();
        col.push(Value::String("no".to_string())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Bool(true));
        assert_eq!(col.get(1).unwrap(), Value::Bool(true));
        assert_eq!(col.get(2).unwrap(), Value::Bool(true));
        assert_eq!(col.get(3).unwrap(), Value::Bool(false));
        assert_eq!(col.get(4).unwrap(), Value::Bool(false));
    }

    #[test]
    fn test_push_bool_from_int64() {
        let mut col = Column::new(&DataType::Bool);
        col.push(Value::Int64(1)).unwrap();
        col.push(Value::Int64(0)).unwrap();
        col.push(Value::Int64(-1)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Bool(true));
        assert_eq!(col.get(1).unwrap(), Value::Bool(false));
        assert_eq!(col.get(2).unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_push_bool_default() {
        let mut col = Column::new(&DataType::Bool);
        col.push(Value::Default).unwrap();
        assert_eq!(col.len(), 1);
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_int64_null() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::Null).unwrap();
        assert_eq!(col.len(), 1);
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_int64_value() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::Int64(42)).unwrap();
        col.push(Value::Int64(-100)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Int64(42));
        assert_eq!(col.get(1).unwrap(), Value::Int64(-100));
    }

    #[test]
    fn test_push_int64_from_float64() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::float64(42.7)).unwrap();
        col.push(Value::float64(-10.3)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Int64(42));
        assert_eq!(col.get(1).unwrap(), Value::Int64(-10));
    }

    #[test]
    fn test_push_int64_from_string() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::String("123".to_string())).unwrap();
        col.push(Value::String("invalid".to_string())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Int64(123));
        assert_eq!(col.get(1).unwrap(), Value::Int64(0));
    }

    #[test]
    fn test_push_int64_default() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::Default).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_float64_null() {
        let mut col = Column::new(&DataType::Float64);
        col.push(Value::Null).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_float64_value() {
        let mut col = Column::new(&DataType::Float64);
        col.push(Value::float64(3.15)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::float64(3.15));
    }

    #[test]
    fn test_push_float64_from_int64() {
        let mut col = Column::new(&DataType::Float64);
        col.push(Value::Int64(42)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::float64(42.0));
    }

    #[test]
    fn test_push_float64_from_numeric() {
        let mut col = Column::new(&DataType::Float64);
        col.push(Value::Numeric(Decimal::from_str("3.15").unwrap()))
            .unwrap();
        let val = col.get(0).unwrap();
        match val {
            Value::Float64(f) => assert!((f.0 - 3.15).abs() < 0.001),
            _ => panic!("Expected Float64"),
        }
    }

    #[test]
    fn test_push_float64_default() {
        let mut col = Column::new(&DataType::Float64);
        col.push(Value::Default).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_numeric_null() {
        let mut col = Column::new(&DataType::Numeric(None));
        col.push(Value::Null).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_numeric_value() {
        let mut col = Column::new(&DataType::Numeric(None));
        col.push(Value::Numeric(Decimal::from_str("123.456").unwrap()))
            .unwrap();
        assert_eq!(
            col.get(0).unwrap(),
            Value::Numeric(Decimal::from_str("123.456").unwrap())
        );
    }

    #[test]
    fn test_push_numeric_from_float64() {
        let mut col = Column::new(&DataType::Numeric(None));
        col.push(Value::float64(3.15)).unwrap();
        let val = col.get(0).unwrap();
        match val {
            Value::Numeric(d) => assert!(
                (d - Decimal::from_str("3.15").unwrap()).abs()
                    < Decimal::from_str("0.001").unwrap()
            ),
            _ => panic!("Expected Numeric"),
        }
    }

    #[test]
    fn test_push_numeric_from_int64() {
        let mut col = Column::new(&DataType::Numeric(None));
        col.push(Value::Int64(42)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Numeric(Decimal::from(42)));
    }

    #[test]
    fn test_push_numeric_from_bignumeric() {
        let mut col = Column::new(&DataType::Numeric(None));
        col.push(Value::BigNumeric(Decimal::from_str("999.999").unwrap()))
            .unwrap();
        assert_eq!(
            col.get(0).unwrap(),
            Value::Numeric(Decimal::from_str("999.999").unwrap())
        );
    }

    #[test]
    fn test_push_numeric_default() {
        let mut col = Column::new(&DataType::Numeric(None));
        col.push(Value::Default).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_string_null() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::Null).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_string_value() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::String("hello".to_string())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::String("hello".to_string()));
    }

    #[test]
    fn test_push_string_from_int64() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::Int64(42)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::String("42".to_string()));
    }

    #[test]
    fn test_push_string_from_float64() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::float64(3.15)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::String("3.15".to_string()));
    }

    #[test]
    fn test_push_string_from_date() {
        let mut col = Column::new(&DataType::String);
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        col.push(Value::Date(date)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::String("2024-01-15".to_string()));
    }

    #[test]
    fn test_push_string_from_datetime() {
        let mut col = Column::new(&DataType::String);
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(10, 30, 0)
            .unwrap();
        col.push(Value::DateTime(dt)).unwrap();
        assert_eq!(
            col.get(0).unwrap(),
            Value::String("2024-01-15 10:30:00".to_string())
        );
    }

    #[test]
    fn test_push_string_from_timestamp() {
        let mut col = Column::new(&DataType::String);
        let ts = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        col.push(Value::Timestamp(ts)).unwrap();
        let val = col.get(0).unwrap();
        match val {
            Value::String(s) => assert!(s.contains("2024-01-15")),
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_push_string_from_time() {
        let mut col = Column::new(&DataType::String);
        let t = NaiveTime::from_hms_opt(10, 30, 45).unwrap();
        col.push(Value::Time(t)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::String("10:30:45".to_string()));
    }

    #[test]
    fn test_push_string_from_bool() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::Bool(true)).unwrap();
        col.push(Value::Bool(false)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::String("true".to_string()));
        assert_eq!(col.get(1).unwrap(), Value::String("false".to_string()));
    }

    #[test]
    fn test_push_string_from_struct() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::Struct(vec![("a".to_string(), Value::Int64(1))]))
            .unwrap();
        let val = col.get(0).unwrap();
        match val {
            Value::String(s) => assert!(s.contains("a")),
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_push_string_from_array() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::Array(vec![Value::Int64(1), Value::Int64(2)]))
            .unwrap();
        let val = col.get(0).unwrap();
        match val {
            Value::String(s) => assert!(s.contains("1") && s.contains("2")),
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_push_string_from_numeric() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::Numeric(Decimal::from_str("123.456").unwrap()))
            .unwrap();
        assert_eq!(col.get(0).unwrap(), Value::String("123.456".to_string()));
    }

    #[test]
    fn test_push_string_from_bytes() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::Bytes(vec![72, 101, 108, 108, 111]))
            .unwrap();
        assert_eq!(col.get(0).unwrap(), Value::String("Hello".to_string()));
    }

    #[test]
    fn test_push_string_default() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::Default).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_bytes_null() {
        let mut col = Column::new(&DataType::Bytes);
        col.push(Value::Null).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_bytes_value() {
        let mut col = Column::new(&DataType::Bytes);
        col.push(Value::Bytes(vec![1, 2, 3])).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Bytes(vec![1, 2, 3]));
    }

    #[test]
    fn test_push_bytes_default() {
        let mut col = Column::new(&DataType::Bytes);
        col.push(Value::Default).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_date_null() {
        let mut col = Column::new(&DataType::Date);
        col.push(Value::Null).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_date_value() {
        let mut col = Column::new(&DataType::Date);
        let date = NaiveDate::from_ymd_opt(2024, 12, 25).unwrap();
        col.push(Value::Date(date)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Date(date));
    }

    #[test]
    fn test_push_date_default() {
        let mut col = Column::new(&DataType::Date);
        col.push(Value::Default).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_time_null() {
        let mut col = Column::new(&DataType::Time);
        col.push(Value::Null).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_time_value() {
        let mut col = Column::new(&DataType::Time);
        let time = NaiveTime::from_hms_opt(14, 30, 45).unwrap();
        col.push(Value::Time(time)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Time(time));
    }

    #[test]
    fn test_push_time_default() {
        let mut col = Column::new(&DataType::Time);
        col.push(Value::Default).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_datetime_null() {
        let mut col = Column::new(&DataType::DateTime);
        col.push(Value::Null).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_datetime_value() {
        let mut col = Column::new(&DataType::DateTime);
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(10, 30, 0)
            .unwrap();
        col.push(Value::DateTime(dt)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::DateTime(dt));
    }

    #[test]
    fn test_push_datetime_default() {
        let mut col = Column::new(&DataType::DateTime);
        col.push(Value::Default).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_timestamp_null() {
        let mut col = Column::new(&DataType::Timestamp);
        col.push(Value::Null).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_timestamp_value() {
        let mut col = Column::new(&DataType::Timestamp);
        let ts = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        col.push(Value::Timestamp(ts)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Timestamp(ts));
    }

    #[test]
    fn test_push_timestamp_default() {
        let mut col = Column::new(&DataType::Timestamp);
        col.push(Value::Default).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_json_null() {
        let mut col = Column::new(&DataType::Json);
        col.push(Value::Null).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_json_value() {
        let mut col = Column::new(&DataType::Json);
        let json_val = serde_json::json!({"key": "value"});
        col.push(Value::Json(json_val.clone())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Json(json_val));
    }

    #[test]
    fn test_push_json_from_string() {
        let mut col = Column::new(&DataType::Json);
        col.push(Value::String(r#"{"key": "value"}"#.to_string()))
            .unwrap();
        let val = col.get(0).unwrap();
        match val {
            Value::Json(j) => assert_eq!(j["key"], "value"),
            _ => panic!("Expected Json"),
        }
    }

    #[test]
    fn test_push_json_from_invalid_string() {
        let mut col = Column::new(&DataType::Json);
        col.push(Value::String("not json".to_string())).unwrap();
        let val = col.get(0).unwrap();
        match val {
            Value::Json(j) => assert_eq!(j, serde_json::Value::String("not json".to_string())),
            _ => panic!("Expected Json"),
        }
    }

    #[test]
    fn test_push_json_default() {
        let mut col = Column::new(&DataType::Json);
        col.push(Value::Default).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_array_null() {
        let mut col = Column::new(&DataType::Array(Box::new(DataType::Int64)));
        col.push(Value::Null).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_array_value() {
        let mut col = Column::new(&DataType::Array(Box::new(DataType::Int64)));
        col.push(Value::Array(vec![Value::Int64(1), Value::Int64(2)]))
            .unwrap();
        assert_eq!(
            col.get(0).unwrap(),
            Value::Array(vec![Value::Int64(1), Value::Int64(2)])
        );
    }

    #[test]
    fn test_push_array_default() {
        let mut col = Column::new(&DataType::Array(Box::new(DataType::Int64)));
        col.push(Value::Default).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_struct_null() {
        let fields = vec![StructField {
            name: "a".to_string(),
            data_type: DataType::Int64,
        }];
        let mut col = Column::new(&DataType::Struct(fields));
        col.push(Value::Null).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_struct_value() {
        let fields = vec![StructField {
            name: "a".to_string(),
            data_type: DataType::Int64,
        }];
        let mut col = Column::new(&DataType::Struct(fields));
        col.push(Value::Struct(vec![("x".to_string(), Value::Int64(42))]))
            .unwrap();
        let val = col.get(0).unwrap();
        match val {
            Value::Struct(s) => {
                assert_eq!(s.len(), 1);
                assert_eq!(s[0].0, "a");
                assert_eq!(s[0].1, Value::Int64(42));
            }
            _ => panic!("Expected Struct"),
        }
    }

    #[test]
    fn test_push_struct_default() {
        let fields = vec![StructField {
            name: "a".to_string(),
            data_type: DataType::Int64,
        }];
        let mut col = Column::new(&DataType::Struct(fields));
        col.push(Value::Default).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_geography_null() {
        let mut col = Column::new(&DataType::Geography);
        col.push(Value::Null).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_geography_value() {
        let mut col = Column::new(&DataType::Geography);
        col.push(Value::Geography("POINT(0 0)".to_string()))
            .unwrap();
        assert_eq!(
            col.get(0).unwrap(),
            Value::Geography("POINT(0 0)".to_string())
        );
    }

    #[test]
    fn test_push_geography_default() {
        let mut col = Column::new(&DataType::Geography);
        col.push(Value::Default).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_interval_null() {
        let mut col = Column::new(&DataType::Interval);
        col.push(Value::Null).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_interval_value() {
        let mut col = Column::new(&DataType::Interval);
        let interval = IntervalValue {
            months: 1,
            days: 2,
            nanos: 3000,
        };
        col.push(Value::Interval(interval.clone())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Interval(interval));
    }

    #[test]
    fn test_push_interval_default() {
        let mut col = Column::new(&DataType::Interval);
        col.push(Value::Default).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_range_null() {
        let mut col = Column::new(&DataType::Range(Box::new(DataType::Int64)));
        col.push(Value::Null).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_range_value() {
        let mut col = Column::new(&DataType::Range(Box::new(DataType::Int64)));
        let range = RangeValue::new(Some(Value::Int64(1)), Some(Value::Int64(10)));
        col.push(Value::Range(range.clone())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Range(range));
    }

    #[test]
    fn test_push_range_default() {
        let mut col = Column::new(&DataType::Range(Box::new(DataType::Int64)));
        col.push(Value::Default).unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn test_push_type_mismatch() {
        let mut col = Column::new(&DataType::Bool);
        let result = col.push(Value::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()));
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_bool() {
        let mut col = Column::Bool {
            data: vec![true, false, true],
            nulls: NullBitmap::new_valid(3),
        };
        col.remove(1);
        assert_eq!(col.len(), 2);
        assert_eq!(col.get(0).unwrap(), Value::Bool(true));
        assert_eq!(col.get(1).unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_remove_int64() {
        let mut col = Column::Int64 {
            data: AVec::from_iter(64, vec![1, 2, 3, 4]),
            nulls: NullBitmap::new_valid(4),
        };
        col.remove(0);
        assert_eq!(col.len(), 3);
        assert_eq!(col.get(0).unwrap(), Value::Int64(2));
    }

    #[test]
    fn test_remove_float64() {
        let mut col = Column::Float64 {
            data: AVec::from_iter(64, vec![1.0, 2.0, 3.0]),
            nulls: NullBitmap::new_valid(3),
        };
        col.remove(2);
        assert_eq!(col.len(), 2);
        assert_eq!(col.get(1).unwrap(), Value::float64(2.0));
    }

    #[test]
    fn test_remove_numeric() {
        let mut col = Column::Numeric {
            data: vec![Decimal::from(1), Decimal::from(2)],
            nulls: NullBitmap::new_valid(2),
        };
        col.remove(0);
        assert_eq!(col.len(), 1);
        assert_eq!(col.get(0).unwrap(), Value::Numeric(Decimal::from(2)));
    }

    #[test]
    fn test_remove_string() {
        let mut col = Column::String {
            data: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            nulls: NullBitmap::new_valid(3),
        };
        col.remove(1);
        assert_eq!(col.len(), 2);
        assert_eq!(col.get(0).unwrap(), Value::String("a".to_string()));
        assert_eq!(col.get(1).unwrap(), Value::String("c".to_string()));
    }

    #[test]
    fn test_remove_bytes() {
        let mut col = Column::Bytes {
            data: vec![vec![1], vec![2]],
            nulls: NullBitmap::new_valid(2),
        };
        col.remove(0);
        assert_eq!(col.len(), 1);
        assert_eq!(col.get(0).unwrap(), Value::Bytes(vec![2]));
    }

    #[test]
    fn test_remove_date() {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let mut col = Column::Date {
            data: vec![d1, d2],
            nulls: NullBitmap::new_valid(2),
        };
        col.remove(0);
        assert_eq!(col.len(), 1);
        assert_eq!(col.get(0).unwrap(), Value::Date(d2));
    }

    #[test]
    fn test_remove_time() {
        let t1 = NaiveTime::from_hms_opt(10, 0, 0).unwrap();
        let t2 = NaiveTime::from_hms_opt(11, 0, 0).unwrap();
        let mut col = Column::Time {
            data: vec![t1, t2],
            nulls: NullBitmap::new_valid(2),
        };
        col.remove(1);
        assert_eq!(col.len(), 1);
        assert_eq!(col.get(0).unwrap(), Value::Time(t1));
    }

    #[test]
    fn test_remove_datetime() {
        let dt1 = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let dt2 = NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let mut col = Column::DateTime {
            data: vec![dt1, dt2],
            nulls: NullBitmap::new_valid(2),
        };
        col.remove(0);
        assert_eq!(col.get(0).unwrap(), Value::DateTime(dt2));
    }

    #[test]
    fn test_remove_timestamp() {
        let ts1 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();
        let mut col = Column::Timestamp {
            data: vec![ts1, ts2],
            nulls: NullBitmap::new_valid(2),
        };
        col.remove(0);
        assert_eq!(col.get(0).unwrap(), Value::Timestamp(ts2));
    }

    #[test]
    fn test_remove_json() {
        let j1 = serde_json::json!({"a": 1});
        let j2 = serde_json::json!({"b": 2});
        let mut col = Column::Json {
            data: vec![j1.clone(), j2.clone()],
            nulls: NullBitmap::new_valid(2),
        };
        col.remove(0);
        assert_eq!(col.get(0).unwrap(), Value::Json(j2));
    }

    #[test]
    fn test_remove_array() {
        let mut col = Column::Array {
            data: vec![vec![Value::Int64(1)], vec![Value::Int64(2)]],
            nulls: NullBitmap::new_valid(2),
            element_type: DataType::Int64,
        };
        col.remove(0);
        assert_eq!(col.get(0).unwrap(), Value::Array(vec![Value::Int64(2)]));
    }

    #[test]
    fn test_remove_struct() {
        let s1 = vec![("a".to_string(), Value::Int64(1))];
        let s2 = vec![("a".to_string(), Value::Int64(2))];
        let mut col = Column::Struct {
            data: vec![s1, s2.clone()],
            nulls: NullBitmap::new_valid(2),
            fields: vec![("a".to_string(), DataType::Int64)],
        };
        col.remove(0);
        assert_eq!(col.get(0).unwrap(), Value::Struct(s2));
    }

    #[test]
    fn test_remove_geography() {
        let mut col = Column::Geography {
            data: vec!["POINT(0 0)".to_string(), "POINT(1 1)".to_string()],
            nulls: NullBitmap::new_valid(2),
        };
        col.remove(0);
        assert_eq!(
            col.get(0).unwrap(),
            Value::Geography("POINT(1 1)".to_string())
        );
    }

    #[test]
    fn test_remove_interval() {
        let i1 = IntervalValue {
            months: 1,
            days: 0,
            nanos: 0,
        };
        let i2 = IntervalValue {
            months: 2,
            days: 0,
            nanos: 0,
        };
        let mut col = Column::Interval {
            data: vec![i1, i2.clone()],
            nulls: NullBitmap::new_valid(2),
        };
        col.remove(0);
        assert_eq!(col.get(0).unwrap(), Value::Interval(i2));
    }

    #[test]
    fn test_remove_range() {
        let r1 = RangeValue::new(Some(Value::Int64(1)), Some(Value::Int64(5)));
        let r2 = RangeValue::new(Some(Value::Int64(10)), Some(Value::Int64(20)));
        let mut col = Column::Range {
            data: vec![r1, r2.clone()],
            nulls: NullBitmap::new_valid(2),
            element_type: DataType::Int64,
        };
        col.remove(0);
        assert_eq!(col.get(0).unwrap(), Value::Range(r2));
    }

    #[test]
    fn test_clear_bool() {
        let mut col = Column::Bool {
            data: vec![true, false],
            nulls: NullBitmap::new_valid(2),
        };
        col.clear();
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_clear_int64() {
        let mut col = Column::Int64 {
            data: AVec::from_iter(64, vec![1, 2, 3]),
            nulls: NullBitmap::new_valid(3),
        };
        col.clear();
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_clear_float64() {
        let mut col = Column::Float64 {
            data: AVec::from_iter(64, vec![1.0, 2.0]),
            nulls: NullBitmap::new_valid(2),
        };
        col.clear();
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_clear_numeric() {
        let mut col = Column::Numeric {
            data: vec![Decimal::from(1)],
            nulls: NullBitmap::new_valid(1),
        };
        col.clear();
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_clear_string() {
        let mut col = Column::String {
            data: vec!["hello".to_string()],
            nulls: NullBitmap::new_valid(1),
        };
        col.clear();
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_clear_bytes() {
        let mut col = Column::Bytes {
            data: vec![vec![1, 2, 3]],
            nulls: NullBitmap::new_valid(1),
        };
        col.clear();
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_clear_date() {
        let mut col = Column::Date {
            data: vec![NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()],
            nulls: NullBitmap::new_valid(1),
        };
        col.clear();
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_clear_time() {
        let mut col = Column::Time {
            data: vec![NaiveTime::from_hms_opt(10, 0, 0).unwrap()],
            nulls: NullBitmap::new_valid(1),
        };
        col.clear();
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_clear_datetime() {
        let mut col = Column::DateTime {
            data: vec![
                NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ],
            nulls: NullBitmap::new_valid(1),
        };
        col.clear();
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_clear_timestamp() {
        let mut col = Column::Timestamp {
            data: vec![Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()],
            nulls: NullBitmap::new_valid(1),
        };
        col.clear();
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_clear_json() {
        let mut col = Column::Json {
            data: vec![serde_json::json!({"a": 1})],
            nulls: NullBitmap::new_valid(1),
        };
        col.clear();
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_clear_array() {
        let mut col = Column::Array {
            data: vec![vec![Value::Int64(1)]],
            nulls: NullBitmap::new_valid(1),
            element_type: DataType::Int64,
        };
        col.clear();
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_clear_struct() {
        let mut col = Column::Struct {
            data: vec![vec![("a".to_string(), Value::Int64(1))]],
            nulls: NullBitmap::new_valid(1),
            fields: vec![("a".to_string(), DataType::Int64)],
        };
        col.clear();
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_clear_geography() {
        let mut col = Column::Geography {
            data: vec!["POINT(0 0)".to_string()],
            nulls: NullBitmap::new_valid(1),
        };
        col.clear();
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_clear_interval() {
        let mut col = Column::Interval {
            data: vec![IntervalValue {
                months: 1,
                days: 0,
                nanos: 0,
            }],
            nulls: NullBitmap::new_valid(1),
        };
        col.clear();
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_clear_range() {
        let mut col = Column::Range {
            data: vec![RangeValue::new(None, None)],
            nulls: NullBitmap::new_valid(1),
            element_type: DataType::Int64,
        };
        col.clear();
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_push_then_remove_preserves_nulls() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::Int64(1)).unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Int64(3)).unwrap();

        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));

        col.remove(0);
        assert!(col.is_null(0));
        assert!(!col.is_null(1));
    }

    #[test]
    fn test_push_clear_push() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::String("first".to_string())).unwrap();
        col.push(Value::String("second".to_string())).unwrap();
        assert_eq!(col.len(), 2);

        col.clear();
        assert_eq!(col.len(), 0);

        col.push(Value::String("new".to_string())).unwrap();
        assert_eq!(col.len(), 1);
        assert_eq!(col.get(0).unwrap(), Value::String("new".to_string()));
    }

    #[test]
    fn test_multiple_push_operations() {
        let mut col = Column::new(&DataType::Int64);
        for i in 0..100 {
            col.push(Value::Int64(i)).unwrap();
        }
        assert_eq!(col.len(), 100);
        for i in 0..100 {
            assert_eq!(col.get(i).unwrap(), Value::Int64(i as i64));
        }
    }

    #[test]
    fn test_remove_from_middle() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::String("a".to_string())).unwrap();
        col.push(Value::String("b".to_string())).unwrap();
        col.push(Value::String("c".to_string())).unwrap();
        col.push(Value::String("d".to_string())).unwrap();
        col.push(Value::String("e".to_string())).unwrap();

        col.remove(2);
        assert_eq!(col.len(), 4);
        assert_eq!(col.get(0).unwrap(), Value::String("a".to_string()));
        assert_eq!(col.get(1).unwrap(), Value::String("b".to_string()));
        assert_eq!(col.get(2).unwrap(), Value::String("d".to_string()));
        assert_eq!(col.get(3).unwrap(), Value::String("e".to_string()));
    }

    #[test]
    fn test_push_nested_struct() {
        let inner_fields = vec![StructField {
            name: "x".to_string(),
            data_type: DataType::Int64,
        }];
        let outer_fields = vec![StructField {
            name: "inner".to_string(),
            data_type: DataType::Struct(inner_fields),
        }];
        let mut col = Column::new(&DataType::Struct(outer_fields));

        let inner_struct = Value::Struct(vec![("x".to_string(), Value::Int64(42))]);
        let outer_struct = Value::Struct(vec![("inner".to_string(), inner_struct)]);
        col.push(outer_struct).unwrap();

        let val = col.get(0).unwrap();
        match val {
            Value::Struct(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].0, "inner");
            }
            _ => panic!("Expected Struct"),
        }
    }

    #[test]
    fn test_push_struct_with_more_fields_than_target() {
        let fields = vec![StructField {
            name: "a".to_string(),
            data_type: DataType::Int64,
        }];
        let mut col = Column::new(&DataType::Struct(fields));

        let struct_val = Value::Struct(vec![
            ("x".to_string(), Value::Int64(1)),
            ("y".to_string(), Value::Int64(2)),
            ("z".to_string(), Value::Int64(3)),
        ]);
        col.push(struct_val).unwrap();

        let val = col.get(0).unwrap();
        match val {
            Value::Struct(fields) => {
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0].0, "a");
                assert_eq!(fields[0].1, Value::Int64(1));
                assert_eq!(fields[1].0, "_field1");
                assert_eq!(fields[2].0, "_field2");
            }
            _ => panic!("Expected Struct"),
        }
    }

    #[test]
    fn test_push_type_mismatch_int64_with_date() {
        let mut col = Column::new(&DataType::Int64);
        let result = col.push(Value::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_float64_with_string() {
        let mut col = Column::new(&DataType::Float64);
        let result = col.push(Value::String("hello".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_bytes_with_int64() {
        let mut col = Column::new(&DataType::Bytes);
        let result = col.push(Value::Int64(42));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_date_with_string() {
        let mut col = Column::new(&DataType::Date);
        let result = col.push(Value::String("2024-01-01".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_time_with_int64() {
        let mut col = Column::new(&DataType::Time);
        let result = col.push(Value::Int64(1000));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_datetime_with_string() {
        let mut col = Column::new(&DataType::DateTime);
        let result = col.push(Value::String("2024-01-01T10:00:00".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_timestamp_with_float64() {
        let mut col = Column::new(&DataType::Timestamp);
        let result = col.push(Value::float64(1234567890.0));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_json_with_int64() {
        let mut col = Column::new(&DataType::Json);
        let result = col.push(Value::Int64(42));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_array_with_int64() {
        let mut col = Column::new(&DataType::Array(Box::new(DataType::Int64)));
        let result = col.push(Value::Int64(42));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_struct_with_string() {
        let fields = vec![StructField {
            name: "a".to_string(),
            data_type: DataType::Int64,
        }];
        let mut col = Column::new(&DataType::Struct(fields));
        let result = col.push(Value::String("hello".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_geography_with_int64() {
        let mut col = Column::new(&DataType::Geography);
        let result = col.push(Value::Int64(42));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_interval_with_string() {
        let mut col = Column::new(&DataType::Interval);
        let result = col.push(Value::String("1 day".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_range_with_string() {
        let mut col = Column::new(&DataType::Range(Box::new(DataType::Int64)));
        let result = col.push(Value::String("[1, 10)".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_numeric_with_bool() {
        let mut col = Column::new(&DataType::Numeric(None));
        let result = col.push(Value::Bool(true));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_string_from_bytes_non_utf8() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::Bytes(vec![0xFF, 0xFE])).unwrap();
        let val = col.get(0).unwrap();
        match val {
            Value::String(s) => assert!(s.contains('\u{FFFD}')),
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_push_bool_from_lowercase_strings() {
        let mut col = Column::new(&DataType::Bool);
        col.push(Value::String("true".to_string())).unwrap();
        col.push(Value::String("yes".to_string())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Bool(true));
        assert_eq!(col.get(1).unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_push_bool_from_other_strings() {
        let mut col = Column::new(&DataType::Bool);
        col.push(Value::String("random".to_string())).unwrap();
        col.push(Value::String("0".to_string())).unwrap();
        col.push(Value::String("".to_string())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Bool(false));
        assert_eq!(col.get(1).unwrap(), Value::Bool(false));
        assert_eq!(col.get(2).unwrap(), Value::Bool(false));
    }

    #[test]
    fn test_push_int64_from_string_negative() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::String("-999".to_string())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Int64(-999));
    }

    #[test]
    fn test_push_float64_from_int64_large() {
        let mut col = Column::new(&DataType::Float64);
        col.push(Value::Int64(i64::MAX)).unwrap();
        let val = col.get(0).unwrap();
        match val {
            Value::Float64(f) => assert!(f.0 > 0.0),
            _ => panic!("Expected Float64"),
        }
    }

    #[test]
    fn test_push_float64_from_numeric_nan_equivalent() {
        let mut col = Column::new(&DataType::Float64);
        col.push(Value::Numeric(Decimal::ZERO)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::float64(0.0));
    }

    #[test]
    fn test_push_numeric_from_float64_edge_cases() {
        let mut col = Column::new(&DataType::Numeric(None));
        col.push(Value::float64(0.0)).unwrap();
        col.push(Value::float64(-0.0)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Numeric(Decimal::ZERO));
        assert_eq!(col.get(1).unwrap(), Value::Numeric(Decimal::ZERO));
    }

    #[test]
    fn test_push_int64_from_float64_truncation() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::float64(99.999)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Int64(99));
    }

    #[test]
    fn test_push_json_from_string_array() {
        let mut col = Column::new(&DataType::Json);
        col.push(Value::String("[1, 2, 3]".to_string())).unwrap();
        let val = col.get(0).unwrap();
        match val {
            Value::Json(j) => {
                assert!(j.is_array());
                assert_eq!(j[0], 1);
                assert_eq!(j[1], 2);
                assert_eq!(j[2], 3);
            }
            _ => panic!("Expected Json"),
        }
    }

    #[test]
    fn test_push_json_from_string_number() {
        let mut col = Column::new(&DataType::Json);
        col.push(Value::String("42".to_string())).unwrap();
        let val = col.get(0).unwrap();
        match val {
            Value::Json(j) => assert_eq!(j, serde_json::json!(42)),
            _ => panic!("Expected Json"),
        }
    }

    #[test]
    fn test_push_json_from_string_boolean() {
        let mut col = Column::new(&DataType::Json);
        col.push(Value::String("true".to_string())).unwrap();
        let val = col.get(0).unwrap();
        match val {
            Value::Json(j) => assert_eq!(j, serde_json::json!(true)),
            _ => panic!("Expected Json"),
        }
    }

    #[test]
    fn test_push_json_from_string_null() {
        let mut col = Column::new(&DataType::Json);
        col.push(Value::String("null".to_string())).unwrap();
        let val = col.get(0).unwrap();
        match val {
            Value::Json(j) => assert_eq!(j, serde_json::Value::Null),
            _ => panic!("Expected Json"),
        }
    }

    #[test]
    fn test_remove_all_elements() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::Int64(1)).unwrap();
        col.push(Value::Int64(2)).unwrap();
        col.push(Value::Int64(3)).unwrap();
        col.remove(2);
        col.remove(1);
        col.remove(0);
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_push_after_remove() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::String("a".to_string())).unwrap();
        col.push(Value::String("b".to_string())).unwrap();
        col.remove(0);
        col.push(Value::String("c".to_string())).unwrap();
        assert_eq!(col.len(), 2);
        assert_eq!(col.get(0).unwrap(), Value::String("b".to_string()));
        assert_eq!(col.get(1).unwrap(), Value::String("c".to_string()));
    }

    #[test]
    fn test_push_struct_with_nested_struct_coercion() {
        let inner_fields = vec![StructField {
            name: "x".to_string(),
            data_type: DataType::Int64,
        }];
        let outer_fields = vec![
            StructField {
                name: "a".to_string(),
                data_type: DataType::Int64,
            },
            StructField {
                name: "inner".to_string(),
                data_type: DataType::Struct(inner_fields),
            },
        ];
        let mut col = Column::new(&DataType::Struct(outer_fields));

        let inner_struct = Value::Struct(vec![("y".to_string(), Value::Int64(42))]);
        let outer_struct = Value::Struct(vec![
            ("b".to_string(), Value::Int64(1)),
            ("inner_val".to_string(), inner_struct),
        ]);
        col.push(outer_struct).unwrap();

        let val = col.get(0).unwrap();
        match val {
            Value::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].0, "a");
                assert_eq!(fields[0].1, Value::Int64(1));
                assert_eq!(fields[1].0, "inner");
                match &fields[1].1 {
                    Value::Struct(inner) => {
                        assert_eq!(inner.len(), 1);
                        assert_eq!(inner[0].0, "x");
                        assert_eq!(inner[0].1, Value::Int64(42));
                    }
                    _ => panic!("Expected inner struct"),
                }
            }
            _ => panic!("Expected Struct"),
        }
    }

    #[test]
    fn test_push_struct_with_array_field_coercion() {
        let outer_fields = vec![StructField {
            name: "a".to_string(),
            data_type: DataType::Array(Box::new(DataType::Int64)),
        }];
        let mut col = Column::new(&DataType::Struct(outer_fields));

        let array_val = Value::Array(vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)]);
        let outer_struct = Value::Struct(vec![("arr".to_string(), array_val.clone())]);
        col.push(outer_struct).unwrap();

        let val = col.get(0).unwrap();
        match val {
            Value::Struct(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].0, "a");
                assert_eq!(fields[0].1, array_val);
            }
            _ => panic!("Expected Struct"),
        }
    }

    #[test]
    fn test_push_multiple_nulls() {
        let mut col = Column::new(&DataType::Int64);
        for _ in 0..10 {
            col.push(Value::Null).unwrap();
        }
        assert_eq!(col.len(), 10);
        for i in 0..10 {
            assert!(col.is_null(i));
        }
    }

    #[test]
    fn test_push_alternating_null_and_value() {
        let mut col = Column::new(&DataType::Int64);
        for i in 0..10 {
            if i % 2 == 0 {
                col.push(Value::Int64(i as i64)).unwrap();
            } else {
                col.push(Value::Null).unwrap();
            }
        }
        assert_eq!(col.len(), 10);
        for i in 0..10 {
            if i % 2 == 0 {
                assert!(!col.is_null(i));
                assert_eq!(col.get(i).unwrap(), Value::Int64(i as i64));
            } else {
                assert!(col.is_null(i));
            }
        }
    }

    #[test]
    fn test_push_empty_string() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::String(String::new())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::String(String::new()));
        assert!(!col.is_null(0));
    }

    #[test]
    fn test_push_empty_array() {
        let mut col = Column::new(&DataType::Array(Box::new(DataType::Int64)));
        col.push(Value::Array(Vec::new())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Array(Vec::new()));
        assert!(!col.is_null(0));
    }

    #[test]
    fn test_push_empty_bytes() {
        let mut col = Column::new(&DataType::Bytes);
        col.push(Value::Bytes(Vec::new())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Bytes(Vec::new()));
        assert!(!col.is_null(0));
    }

    #[test]
    fn test_push_range_with_none_bounds() {
        let mut col = Column::new(&DataType::Range(Box::new(DataType::Int64)));
        let range = RangeValue::new(None, None);
        col.push(Value::Range(range.clone())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Range(range));
    }

    #[test]
    fn test_push_range_with_start_only() {
        let mut col = Column::new(&DataType::Range(Box::new(DataType::Int64)));
        let range = RangeValue::new(Some(Value::Int64(5)), None);
        col.push(Value::Range(range.clone())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Range(range));
    }

    #[test]
    fn test_push_range_with_end_only() {
        let mut col = Column::new(&DataType::Range(Box::new(DataType::Int64)));
        let range = RangeValue::new(None, Some(Value::Int64(10)));
        col.push(Value::Range(range.clone())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Range(range));
    }

    #[test]
    fn test_clear_then_verify_data_cleared() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::Int64(1)).unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Int64(3)).unwrap();
        assert_eq!(col.len(), 3);
        col.clear();
        assert_eq!(col.len(), 0);
        col.push(Value::Int64(100)).unwrap();
        assert_eq!(col.len(), 1);
        assert_eq!(col.get(0).unwrap(), Value::Int64(100));
    }

    #[test]
    fn test_push_string_from_empty_struct() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::Struct(vec![])).unwrap();
        let val = col.get(0).unwrap();
        match val {
            Value::String(s) => assert_eq!(s, "[]"),
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_push_string_from_empty_array() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::Array(vec![])).unwrap();
        let val = col.get(0).unwrap();
        match val {
            Value::String(s) => assert_eq!(s, "[]"),
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_push_interval_with_all_zero() {
        let mut col = Column::new(&DataType::Interval);
        let interval = IntervalValue {
            months: 0,
            days: 0,
            nanos: 0,
        };
        col.push(Value::Interval(interval.clone())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Interval(interval));
    }

    #[test]
    fn test_push_interval_with_negative_values() {
        let mut col = Column::new(&DataType::Interval);
        let interval = IntervalValue {
            months: -1,
            days: -2,
            nanos: -3000,
        };
        col.push(Value::Interval(interval.clone())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Interval(interval));
    }

    #[test]
    fn test_push_float64_from_numeric_that_fails_conversion() {
        let mut col = Column::new(&DataType::Float64);
        let large_decimal = Decimal::MAX;
        col.push(Value::Numeric(large_decimal)).unwrap();
        let val = col.get(0).unwrap();
        match val {
            Value::Float64(f) => assert!(f.0.is_finite() || f.0 == 0.0),
            _ => panic!("Expected Float64"),
        }
    }

    #[test]
    fn test_push_numeric_from_float64_special_values() {
        let mut col = Column::new(&DataType::Numeric(None));
        col.push(Value::float64(f64::INFINITY)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Numeric(Decimal::ZERO));

        let mut col2 = Column::new(&DataType::Numeric(None));
        col2.push(Value::float64(f64::NEG_INFINITY)).unwrap();
        assert_eq!(col2.get(0).unwrap(), Value::Numeric(Decimal::ZERO));

        let mut col3 = Column::new(&DataType::Numeric(None));
        col3.push(Value::float64(f64::NAN)).unwrap();
        assert_eq!(col3.get(0).unwrap(), Value::Numeric(Decimal::ZERO));
    }

    #[test]
    fn test_push_struct_with_fewer_values_than_target_fields() {
        let fields = vec![
            StructField {
                name: "a".to_string(),
                data_type: DataType::Int64,
            },
            StructField {
                name: "b".to_string(),
                data_type: DataType::String,
            },
        ];
        let mut col = Column::new(&DataType::Struct(fields));

        let struct_val = Value::Struct(vec![("x".to_string(), Value::Int64(42))]);
        col.push(struct_val).unwrap();

        let val = col.get(0).unwrap();
        match val {
            Value::Struct(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].0, "a");
                assert_eq!(fields[0].1, Value::Int64(42));
            }
            _ => panic!("Expected Struct"),
        }
    }

    #[test]
    fn test_push_struct_with_null_field_values() {
        let fields = vec![StructField {
            name: "a".to_string(),
            data_type: DataType::Int64,
        }];
        let mut col = Column::new(&DataType::Struct(fields));

        let struct_val = Value::Struct(vec![("a".to_string(), Value::Null)]);
        col.push(struct_val).unwrap();

        let val = col.get(0).unwrap();
        match val {
            Value::Struct(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].0, "a");
                assert_eq!(fields[0].1, Value::Null);
            }
            _ => panic!("Expected Struct"),
        }
    }

    #[test]
    fn test_push_type_mismatch_string_with_geography() {
        let mut col = Column::new(&DataType::String);
        let result = col.push(Value::Geography("POINT(0 0)".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_bytes_with_string() {
        let mut col = Column::new(&DataType::Bytes);
        let result = col.push(Value::String("hello".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_date_with_timestamp() {
        let mut col = Column::new(&DataType::Date);
        let ts = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let result = col.push(Value::Timestamp(ts));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_time_with_datetime() {
        let mut col = Column::new(&DataType::Time);
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(10, 30, 0)
            .unwrap();
        let result = col.push(Value::DateTime(dt));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_datetime_with_timestamp() {
        let mut col = Column::new(&DataType::DateTime);
        let ts = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let result = col.push(Value::Timestamp(ts));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_timestamp_with_date() {
        let mut col = Column::new(&DataType::Timestamp);
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let result = col.push(Value::Date(date));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_json_with_array() {
        let mut col = Column::new(&DataType::Json);
        let arr = Value::Array(vec![Value::Int64(1), Value::Int64(2)]);
        let result = col.push(arr);
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_array_with_struct() {
        let mut col = Column::new(&DataType::Array(Box::new(DataType::Int64)));
        let s = Value::Struct(vec![("a".to_string(), Value::Int64(1))]);
        let result = col.push(s);
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_struct_with_array() {
        let fields = vec![StructField {
            name: "a".to_string(),
            data_type: DataType::Int64,
        }];
        let mut col = Column::new(&DataType::Struct(fields));
        let arr = Value::Array(vec![Value::Int64(1)]);
        let result = col.push(arr);
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_geography_with_string() {
        let mut col = Column::new(&DataType::Geography);
        let result = col.push(Value::String("not geography".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_interval_with_int64() {
        let mut col = Column::new(&DataType::Interval);
        let result = col.push(Value::Int64(86400));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_range_with_array() {
        let mut col = Column::new(&DataType::Range(Box::new(DataType::Int64)));
        let arr = Value::Array(vec![Value::Int64(1), Value::Int64(10)]);
        let result = col.push(arr);
        assert!(result.is_err());
    }

    #[test]
    fn test_push_json_from_string_nested_object() {
        let mut col = Column::new(&DataType::Json);
        col.push(Value::String(r#"{"nested": {"key": "value"}}"#.to_string()))
            .unwrap();
        let val = col.get(0).unwrap();
        match val {
            Value::Json(j) => {
                assert_eq!(j["nested"]["key"], "value");
            }
            _ => panic!("Expected Json"),
        }
    }

    #[test]
    fn test_push_json_from_empty_string() {
        let mut col = Column::new(&DataType::Json);
        col.push(Value::String("".to_string())).unwrap();
        let val = col.get(0).unwrap();
        match val {
            Value::Json(j) => assert_eq!(j, serde_json::Value::String("".to_string())),
            _ => panic!("Expected Json"),
        }
    }

    #[test]
    fn test_push_int64_from_float64_negative() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::float64(-123.789)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Int64(-123));
    }

    #[test]
    fn test_push_struct_deeply_nested() {
        let inner_inner = vec![StructField {
            name: "z".to_string(),
            data_type: DataType::Int64,
        }];
        let inner = vec![StructField {
            name: "y".to_string(),
            data_type: DataType::Struct(inner_inner),
        }];
        let outer = vec![StructField {
            name: "x".to_string(),
            data_type: DataType::Struct(inner),
        }];
        let mut col = Column::new(&DataType::Struct(outer));

        let inner_inner_struct = Value::Struct(vec![("z".to_string(), Value::Int64(100))]);
        let inner_struct = Value::Struct(vec![("y".to_string(), inner_inner_struct)]);
        let outer_struct = Value::Struct(vec![("x".to_string(), inner_struct)]);
        col.push(outer_struct).unwrap();

        let val = col.get(0).unwrap();
        match val {
            Value::Struct(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].0, "x");
            }
            _ => panic!("Expected Struct"),
        }
    }

    #[test]
    fn test_push_array_of_structs() {
        let struct_type = DataType::Struct(vec![StructField {
            name: "id".to_string(),
            data_type: DataType::Int64,
        }]);
        let mut col = Column::new(&DataType::Array(Box::new(struct_type)));

        let arr = Value::Array(vec![
            Value::Struct(vec![("id".to_string(), Value::Int64(1))]),
            Value::Struct(vec![("id".to_string(), Value::Int64(2))]),
        ]);
        col.push(arr).unwrap();

        let val = col.get(0).unwrap();
        match val {
            Value::Array(elements) => {
                assert_eq!(elements.len(), 2);
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_push_struct_with_array_of_arrays() {
        let nested_array_type =
            DataType::Array(Box::new(DataType::Array(Box::new(DataType::Int64))));
        let struct_fields = vec![StructField {
            name: "matrix".to_string(),
            data_type: nested_array_type,
        }];
        let mut col = Column::new(&DataType::Struct(struct_fields));

        let matrix = Value::Array(vec![
            Value::Array(vec![Value::Int64(1), Value::Int64(2)]),
            Value::Array(vec![Value::Int64(3), Value::Int64(4)]),
        ]);
        let struct_val = Value::Struct(vec![("matrix".to_string(), matrix)]);
        col.push(struct_val).unwrap();

        assert_eq!(col.len(), 1);
    }

    #[test]
    fn test_push_bool_from_mixed_case_strings() {
        let mut col = Column::new(&DataType::Bool);
        col.push(Value::String("True".to_string())).unwrap();
        col.push(Value::String("YeS".to_string())).unwrap();
        col.push(Value::String("FALSE".to_string())).unwrap();
        col.push(Value::String("NO".to_string())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Bool(true));
        assert_eq!(col.get(1).unwrap(), Value::Bool(true));
        assert_eq!(col.get(2).unwrap(), Value::Bool(false));
        assert_eq!(col.get(3).unwrap(), Value::Bool(false));
    }

    #[test]
    fn test_push_int64_from_string_with_whitespace() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::String("  123  ".to_string())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Int64(0));
    }

    #[test]
    fn test_push_int64_from_string_overflow() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::String("99999999999999999999999".to_string()))
            .unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Int64(0));
    }

    #[test]
    fn test_push_string_from_special_unicode() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::String("Hello \u{1F600} World".to_string()))
            .unwrap();
        assert_eq!(
            col.get(0).unwrap(),
            Value::String("Hello \u{1F600} World".to_string())
        );
    }

    #[test]
    fn test_push_bytes_with_all_zeros() {
        let mut col = Column::new(&DataType::Bytes);
        col.push(Value::Bytes(vec![0, 0, 0, 0])).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Bytes(vec![0, 0, 0, 0]));
    }

    #[test]
    fn test_push_bytes_with_max_bytes() {
        let mut col = Column::new(&DataType::Bytes);
        col.push(Value::Bytes(vec![255, 255, 255])).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Bytes(vec![255, 255, 255]));
    }

    #[test]
    fn test_push_interval_with_large_values() {
        let mut col = Column::new(&DataType::Interval);
        let interval = IntervalValue {
            months: i32::MAX,
            days: i32::MAX,
            nanos: i64::MAX,
        };
        col.push(Value::Interval(interval.clone())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Interval(interval));
    }

    #[test]
    fn test_push_type_mismatch_bool_with_numeric() {
        let mut col = Column::new(&DataType::Bool);
        let result = col.push(Value::Numeric(Decimal::from(1)));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_int64_with_bool() {
        let mut col = Column::new(&DataType::Int64);
        let result = col.push(Value::Bool(true));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_float64_with_bool() {
        let mut col = Column::new(&DataType::Float64);
        let result = col.push(Value::Bool(false));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_type_mismatch_numeric_with_string() {
        let mut col = Column::new(&DataType::Numeric(None));
        let result = col.push(Value::String("123.45".to_string()));
        assert!(result.is_err());
    }
}
