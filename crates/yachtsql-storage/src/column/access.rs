#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

use super::Column;

impl Column {
    pub fn is_null(&self, index: usize) -> bool {
        match self {
            Column::Bool { nulls, .. } => nulls.is_null(index),
            Column::Int64 { nulls, .. } => nulls.is_null(index),
            Column::Float64 { nulls, .. } => nulls.is_null(index),
            Column::Numeric { nulls, .. } => nulls.is_null(index),
            Column::String { nulls, .. } => nulls.is_null(index),
            Column::Bytes { nulls, .. } => nulls.is_null(index),
            Column::Date { nulls, .. } => nulls.is_null(index),
            Column::Time { nulls, .. } => nulls.is_null(index),
            Column::DateTime { nulls, .. } => nulls.is_null(index),
            Column::Timestamp { nulls, .. } => nulls.is_null(index),
            Column::Json { nulls, .. } => nulls.is_null(index),
            Column::Array { nulls, .. } => nulls.is_null(index),
            Column::Struct { nulls, .. } => nulls.is_null(index),
            Column::Geography { nulls, .. } => nulls.is_null(index),
            Column::Interval { nulls, .. } => nulls.is_null(index),
            Column::Range { nulls, .. } => nulls.is_null(index),
        }
    }

    pub fn is_all_null(&self) -> bool {
        match self {
            Column::Bool { nulls, .. } => nulls.is_all_null(),
            Column::Int64 { nulls, .. } => nulls.is_all_null(),
            Column::Float64 { nulls, .. } => nulls.is_all_null(),
            Column::Numeric { nulls, .. } => nulls.is_all_null(),
            Column::String { nulls, .. } => nulls.is_all_null(),
            Column::Bytes { nulls, .. } => nulls.is_all_null(),
            Column::Date { nulls, .. } => nulls.is_all_null(),
            Column::Time { nulls, .. } => nulls.is_all_null(),
            Column::DateTime { nulls, .. } => nulls.is_all_null(),
            Column::Timestamp { nulls, .. } => nulls.is_all_null(),
            Column::Json { nulls, .. } => nulls.is_all_null(),
            Column::Array { nulls, .. } => nulls.is_all_null(),
            Column::Struct { nulls, .. } => nulls.is_all_null(),
            Column::Geography { nulls, .. } => nulls.is_all_null(),
            Column::Interval { nulls, .. } => nulls.is_all_null(),
            Column::Range { nulls, .. } => nulls.is_all_null(),
        }
    }

    pub fn get(&self, index: usize) -> Result<Value> {
        if index >= self.len() {
            return Err(Error::invalid_query(format!(
                "Column index {} out of bounds (len: {})",
                index,
                self.len()
            )));
        }
        if self.is_null(index) {
            return Ok(Value::Null);
        }

        Ok(match self {
            Column::Bool { data, .. } => Value::Bool(data[index]),
            Column::Int64 { data, .. } => Value::Int64(data[index]),
            Column::Float64 { data, .. } => Value::float64(data[index]),
            Column::Numeric { data, .. } => Value::Numeric(data[index]),
            Column::String { data, .. } => Value::String(data[index].clone()),
            Column::Bytes { data, .. } => Value::Bytes(data[index].clone()),
            Column::Date { data, .. } => Value::Date(data[index]),
            Column::Time { data, .. } => Value::Time(data[index]),
            Column::DateTime { data, .. } => Value::DateTime(data[index]),
            Column::Timestamp { data, .. } => Value::Timestamp(data[index]),
            Column::Json { data, .. } => Value::Json(data[index].clone()),
            Column::Array { data, .. } => Value::Array(data[index].clone()),
            Column::Struct { data, .. } => Value::Struct(data[index].clone()),
            Column::Geography { data, .. } => Value::Geography(data[index].clone()),
            Column::Interval { data, .. } => Value::Interval(data[index].clone()),
            Column::Range { data, .. } => Value::Range(data[index].clone()),
        })
    }

    pub fn get_value(&self, index: usize) -> Value {
        if index >= self.len() || self.is_null(index) {
            return Value::Null;
        }

        match self {
            Column::Bool { data, .. } => Value::Bool(data[index]),
            Column::Int64 { data, .. } => Value::Int64(data[index]),
            Column::Float64 { data, .. } => Value::float64(data[index]),
            Column::Numeric { data, .. } => Value::Numeric(data[index]),
            Column::String { data, .. } => Value::String(data[index].clone()),
            Column::Bytes { data, .. } => Value::Bytes(data[index].clone()),
            Column::Date { data, .. } => Value::Date(data[index]),
            Column::Time { data, .. } => Value::Time(data[index]),
            Column::DateTime { data, .. } => Value::DateTime(data[index]),
            Column::Timestamp { data, .. } => Value::Timestamp(data[index]),
            Column::Json { data, .. } => Value::Json(data[index].clone()),
            Column::Array { data, .. } => Value::Array(data[index].clone()),
            Column::Struct { data, .. } => Value::Struct(data[index].clone()),
            Column::Geography { data, .. } => Value::Geography(data[index].clone()),
            Column::Interval { data, .. } => Value::Interval(data[index].clone()),
            Column::Range { data, .. } => Value::Range(data[index].clone()),
        }
    }

    pub fn set(&mut self, index: usize, value: Value) -> Result<()> {
        if index >= self.len() {
            return Err(Error::invalid_query(format!(
                "Column index {} out of bounds (len: {})",
                index,
                self.len()
            )));
        }
        match (self, value) {
            (Column::Bool { data, nulls }, Value::Null) => {
                data[index] = false;
                nulls.set(index, true);
            }
            (Column::Bool { data, nulls }, Value::Bool(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Int64 { data, nulls }, Value::Null) => {
                data[index] = 0;
                nulls.set(index, true);
            }
            (Column::Int64 { data, nulls }, Value::Int64(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Float64 { data, nulls }, Value::Null) => {
                data[index] = 0.0;
                nulls.set(index, true);
            }
            (Column::Float64 { data, nulls }, Value::Float64(v)) => {
                data[index] = v.0;
                nulls.set(index, false);
            }
            (Column::Numeric { data, nulls }, Value::Null) => {
                data[index] = rust_decimal::Decimal::ZERO;
                nulls.set(index, true);
            }
            (Column::Numeric { data, nulls }, Value::Numeric(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Numeric { data, nulls }, Value::BigNumeric(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::String { data, nulls }, Value::Null) => {
                data[index] = String::new();
                nulls.set(index, true);
            }
            (Column::String { data, nulls }, Value::String(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Bytes { data, nulls }, Value::Null) => {
                data[index] = Vec::new();
                nulls.set(index, true);
            }
            (Column::Bytes { data, nulls }, Value::Bytes(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Date { data, nulls }, Value::Null) => {
                data[index] = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                nulls.set(index, true);
            }
            (Column::Date { data, nulls }, Value::Date(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Time { data, nulls }, Value::Null) => {
                data[index] = chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap();
                nulls.set(index, true);
            }
            (Column::Time { data, nulls }, Value::Time(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::DateTime { data, nulls }, Value::Null) => {
                data[index] = chrono::DateTime::UNIX_EPOCH.naive_utc();
                nulls.set(index, true);
            }
            (Column::DateTime { data, nulls }, Value::DateTime(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Timestamp { data, nulls }, Value::Null) => {
                data[index] = chrono::DateTime::UNIX_EPOCH;
                nulls.set(index, true);
            }
            (Column::Timestamp { data, nulls }, Value::Timestamp(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Json { data, nulls }, Value::Null) => {
                data[index] = serde_json::Value::Null;
                nulls.set(index, true);
            }
            (Column::Json { data, nulls }, Value::Json(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Array { data, nulls, .. }, Value::Null) => {
                data[index] = Vec::new();
                nulls.set(index, true);
            }
            (Column::Array { data, nulls, .. }, Value::Array(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Struct { data, nulls, .. }, Value::Null) => {
                data[index] = Vec::new();
                nulls.set(index, true);
            }
            (Column::Struct { data, nulls, .. }, Value::Struct(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Geography { data, nulls }, Value::Null) => {
                data[index] = String::new();
                nulls.set(index, true);
            }
            (Column::Geography { data, nulls }, Value::Geography(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Interval { data, nulls }, Value::Null) => {
                data[index] = yachtsql_common::types::IntervalValue {
                    months: 0,
                    days: 0,
                    nanos: 0,
                };
                nulls.set(index, true);
            }
            (Column::Interval { data, nulls }, Value::Interval(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Range { data, nulls, .. }, Value::Null) => {
                data[index] = yachtsql_common::types::RangeValue::new(None, None);
                nulls.set(index, true);
            }
            (Column::Range { data, nulls, .. }, Value::Range(v)) => {
                data[index] = v;
                nulls.set(index, false);
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
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
    use rust_decimal::Decimal;
    use yachtsql_common::types::{DataType, IntervalValue, RangeValue, StructField};

    use super::*;

    fn create_bool_column() -> Column {
        let mut col = Column::new(&DataType::Bool);
        col.push(Value::Bool(true)).unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Bool(false)).unwrap();
        col
    }

    fn create_int64_column() -> Column {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::Int64(42)).unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Int64(-100)).unwrap();
        col
    }

    fn create_float64_column() -> Column {
        let mut col = Column::new(&DataType::Float64);
        col.push(Value::float64(3.15)).unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::float64(-2.5)).unwrap();
        col
    }

    fn create_numeric_column() -> Column {
        let mut col = Column::new(&DataType::Numeric(None));
        col.push(Value::Numeric(Decimal::new(12345, 2))).unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Numeric(Decimal::new(-999, 1))).unwrap();
        col
    }

    fn create_string_column() -> Column {
        let mut col = Column::new(&DataType::String);
        col.push(Value::String("hello".to_string())).unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::String("world".to_string())).unwrap();
        col
    }

    fn create_bytes_column() -> Column {
        let mut col = Column::new(&DataType::Bytes);
        col.push(Value::Bytes(vec![1, 2, 3])).unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Bytes(vec![4, 5])).unwrap();
        col
    }

    fn create_date_column() -> Column {
        let mut col = Column::new(&DataType::Date);
        col.push(Value::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()))
            .unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Date(NaiveDate::from_ymd_opt(2023, 6, 30).unwrap()))
            .unwrap();
        col
    }

    fn create_time_column() -> Column {
        let mut col = Column::new(&DataType::Time);
        col.push(Value::Time(NaiveTime::from_hms_opt(10, 30, 0).unwrap()))
            .unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Time(NaiveTime::from_hms_opt(23, 59, 59).unwrap()))
            .unwrap();
        col
    }

    fn create_datetime_column() -> Column {
        let mut col = Column::new(&DataType::DateTime);
        col.push(Value::DateTime(
            NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(10, 30, 0)
                .unwrap(),
        ))
        .unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::DateTime(
            NaiveDate::from_ymd_opt(2023, 6, 30)
                .unwrap()
                .and_hms_opt(23, 59, 59)
                .unwrap(),
        ))
        .unwrap();
        col
    }

    fn create_timestamp_column() -> Column {
        let mut col = Column::new(&DataType::Timestamp);
        col.push(Value::Timestamp(
            Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap(),
        ))
        .unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Timestamp(
            Utc.with_ymd_and_hms(2023, 6, 30, 23, 59, 59).unwrap(),
        ))
        .unwrap();
        col
    }

    fn create_json_column() -> Column {
        let mut col = Column::new(&DataType::Json);
        col.push(Value::Json(serde_json::json!({"key": "value"})))
            .unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Json(serde_json::json!([1, 2, 3]))).unwrap();
        col
    }

    fn create_array_column() -> Column {
        let mut col = Column::new(&DataType::Array(Box::new(DataType::Int64)));
        col.push(Value::Array(vec![Value::Int64(1), Value::Int64(2)]))
            .unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Array(vec![Value::Int64(3)])).unwrap();
        col
    }

    fn create_struct_column() -> Column {
        let mut col = Column::new(&DataType::Struct(vec![StructField {
            name: "field1".to_string(),
            data_type: DataType::String,
        }]));
        col.push(Value::Struct(vec![(
            "field1".to_string(),
            Value::String("val1".to_string()),
        )]))
        .unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Struct(vec![(
            "field1".to_string(),
            Value::String("val2".to_string()),
        )]))
        .unwrap();
        col
    }

    fn create_geography_column() -> Column {
        let mut col = Column::new(&DataType::Geography);
        col.push(Value::Geography("POINT(0 0)".to_string()))
            .unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Geography("POINT(1 1)".to_string()))
            .unwrap();
        col
    }

    fn create_interval_column() -> Column {
        let mut col = Column::new(&DataType::Interval);
        col.push(Value::Interval(IntervalValue::new(1, 2, 3)))
            .unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Interval(IntervalValue::new(4, 5, 6)))
            .unwrap();
        col
    }

    fn create_range_column() -> Column {
        let mut col = Column::new(&DataType::Range(Box::new(DataType::Int64)));
        col.push(Value::Range(RangeValue::new(
            Some(Value::Int64(1)),
            Some(Value::Int64(10)),
        )))
        .unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Range(RangeValue::new(
            Some(Value::Int64(20)),
            Some(Value::Int64(30)),
        )))
        .unwrap();
        col
    }

    #[test]
    fn test_is_null_bool() {
        let col = create_bool_column();
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
    }

    #[test]
    fn test_is_null_int64() {
        let col = create_int64_column();
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
    }

    #[test]
    fn test_is_null_float64() {
        let col = create_float64_column();
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
    }

    #[test]
    fn test_is_null_numeric() {
        let col = create_numeric_column();
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
    }

    #[test]
    fn test_is_null_string() {
        let col = create_string_column();
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
    }

    #[test]
    fn test_is_null_bytes() {
        let col = create_bytes_column();
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
    }

    #[test]
    fn test_is_null_date() {
        let col = create_date_column();
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
    }

    #[test]
    fn test_is_null_time() {
        let col = create_time_column();
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
    }

    #[test]
    fn test_is_null_datetime() {
        let col = create_datetime_column();
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
    }

    #[test]
    fn test_is_null_timestamp() {
        let col = create_timestamp_column();
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
    }

    #[test]
    fn test_is_null_json() {
        let col = create_json_column();
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
    }

    #[test]
    fn test_is_null_array() {
        let col = create_array_column();
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
    }

    #[test]
    fn test_is_null_struct() {
        let col = create_struct_column();
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
    }

    #[test]
    fn test_is_null_geography() {
        let col = create_geography_column();
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
    }

    #[test]
    fn test_is_null_interval() {
        let col = create_interval_column();
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
    }

    #[test]
    fn test_is_null_range() {
        let col = create_range_column();
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
    }

    fn create_all_null_bool_column() -> Column {
        let mut col = Column::new(&DataType::Bool);
        col.push(Value::Null).unwrap();
        col.push(Value::Null).unwrap();
        col
    }

    fn create_all_null_int64_column() -> Column {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::Null).unwrap();
        col.push(Value::Null).unwrap();
        col
    }

    fn create_all_null_float64_column() -> Column {
        let mut col = Column::new(&DataType::Float64);
        col.push(Value::Null).unwrap();
        col.push(Value::Null).unwrap();
        col
    }

    fn create_all_null_numeric_column() -> Column {
        let mut col = Column::new(&DataType::Numeric(None));
        col.push(Value::Null).unwrap();
        col.push(Value::Null).unwrap();
        col
    }

    fn create_all_null_string_column() -> Column {
        let mut col = Column::new(&DataType::String);
        col.push(Value::Null).unwrap();
        col.push(Value::Null).unwrap();
        col
    }

    fn create_all_null_bytes_column() -> Column {
        let mut col = Column::new(&DataType::Bytes);
        col.push(Value::Null).unwrap();
        col.push(Value::Null).unwrap();
        col
    }

    fn create_all_null_date_column() -> Column {
        let mut col = Column::new(&DataType::Date);
        col.push(Value::Null).unwrap();
        col.push(Value::Null).unwrap();
        col
    }

    fn create_all_null_time_column() -> Column {
        let mut col = Column::new(&DataType::Time);
        col.push(Value::Null).unwrap();
        col.push(Value::Null).unwrap();
        col
    }

    fn create_all_null_datetime_column() -> Column {
        let mut col = Column::new(&DataType::DateTime);
        col.push(Value::Null).unwrap();
        col.push(Value::Null).unwrap();
        col
    }

    fn create_all_null_timestamp_column() -> Column {
        let mut col = Column::new(&DataType::Timestamp);
        col.push(Value::Null).unwrap();
        col.push(Value::Null).unwrap();
        col
    }

    fn create_all_null_json_column() -> Column {
        let mut col = Column::new(&DataType::Json);
        col.push(Value::Null).unwrap();
        col.push(Value::Null).unwrap();
        col
    }

    fn create_all_null_array_column() -> Column {
        let mut col = Column::new(&DataType::Array(Box::new(DataType::Int64)));
        col.push(Value::Null).unwrap();
        col.push(Value::Null).unwrap();
        col
    }

    fn create_all_null_struct_column() -> Column {
        let mut col = Column::new(&DataType::Struct(vec![StructField {
            name: "f".to_string(),
            data_type: DataType::String,
        }]));
        col.push(Value::Null).unwrap();
        col.push(Value::Null).unwrap();
        col
    }

    fn create_all_null_geography_column() -> Column {
        let mut col = Column::new(&DataType::Geography);
        col.push(Value::Null).unwrap();
        col.push(Value::Null).unwrap();
        col
    }

    fn create_all_null_interval_column() -> Column {
        let mut col = Column::new(&DataType::Interval);
        col.push(Value::Null).unwrap();
        col.push(Value::Null).unwrap();
        col
    }

    fn create_all_null_range_column() -> Column {
        let mut col = Column::new(&DataType::Range(Box::new(DataType::Int64)));
        col.push(Value::Null).unwrap();
        col.push(Value::Null).unwrap();
        col
    }

    #[test]
    fn test_is_all_null_bool() {
        let col = create_bool_column();
        assert!(!col.is_all_null());
        let col = create_all_null_bool_column();
        assert!(col.is_all_null());
    }

    #[test]
    fn test_is_all_null_int64() {
        let col = create_int64_column();
        assert!(!col.is_all_null());
        let col = create_all_null_int64_column();
        assert!(col.is_all_null());
    }

    #[test]
    fn test_is_all_null_float64() {
        let col = create_float64_column();
        assert!(!col.is_all_null());
        let col = create_all_null_float64_column();
        assert!(col.is_all_null());
    }

    #[test]
    fn test_is_all_null_numeric() {
        let col = create_numeric_column();
        assert!(!col.is_all_null());
        let col = create_all_null_numeric_column();
        assert!(col.is_all_null());
    }

    #[test]
    fn test_is_all_null_string() {
        let col = create_string_column();
        assert!(!col.is_all_null());
        let col = create_all_null_string_column();
        assert!(col.is_all_null());
    }

    #[test]
    fn test_is_all_null_bytes() {
        let col = create_bytes_column();
        assert!(!col.is_all_null());
        let col = create_all_null_bytes_column();
        assert!(col.is_all_null());
    }

    #[test]
    fn test_is_all_null_date() {
        let col = create_date_column();
        assert!(!col.is_all_null());
        let col = create_all_null_date_column();
        assert!(col.is_all_null());
    }

    #[test]
    fn test_is_all_null_time() {
        let col = create_time_column();
        assert!(!col.is_all_null());
        let col = create_all_null_time_column();
        assert!(col.is_all_null());
    }

    #[test]
    fn test_is_all_null_datetime() {
        let col = create_datetime_column();
        assert!(!col.is_all_null());
        let col = create_all_null_datetime_column();
        assert!(col.is_all_null());
    }

    #[test]
    fn test_is_all_null_timestamp() {
        let col = create_timestamp_column();
        assert!(!col.is_all_null());
        let col = create_all_null_timestamp_column();
        assert!(col.is_all_null());
    }

    #[test]
    fn test_is_all_null_json() {
        let col = create_json_column();
        assert!(!col.is_all_null());
        let col = create_all_null_json_column();
        assert!(col.is_all_null());
    }

    #[test]
    fn test_is_all_null_array() {
        let col = create_array_column();
        assert!(!col.is_all_null());
        let col = create_all_null_array_column();
        assert!(col.is_all_null());
    }

    #[test]
    fn test_is_all_null_struct() {
        let col = create_struct_column();
        assert!(!col.is_all_null());
        let col = create_all_null_struct_column();
        assert!(col.is_all_null());
    }

    #[test]
    fn test_is_all_null_geography() {
        let col = create_geography_column();
        assert!(!col.is_all_null());
        let col = create_all_null_geography_column();
        assert!(col.is_all_null());
    }

    #[test]
    fn test_is_all_null_interval() {
        let col = create_interval_column();
        assert!(!col.is_all_null());
        let col = create_all_null_interval_column();
        assert!(col.is_all_null());
    }

    #[test]
    fn test_is_all_null_range() {
        let col = create_range_column();
        assert!(!col.is_all_null());
        let col = create_all_null_range_column();
        assert!(col.is_all_null());
    }

    #[test]
    fn test_get_bool() {
        let col = create_bool_column();
        assert_eq!(col.get(0).unwrap(), Value::Bool(true));
        assert_eq!(col.get(1).unwrap(), Value::Null);
        assert_eq!(col.get(2).unwrap(), Value::Bool(false));
    }

    #[test]
    fn test_get_int64() {
        let col = create_int64_column();
        assert_eq!(col.get(0).unwrap(), Value::Int64(42));
        assert_eq!(col.get(1).unwrap(), Value::Null);
        assert_eq!(col.get(2).unwrap(), Value::Int64(-100));
    }

    #[test]
    fn test_get_float64() {
        let col = create_float64_column();
        assert_eq!(col.get(0).unwrap(), Value::float64(3.15));
        assert_eq!(col.get(1).unwrap(), Value::Null);
        assert_eq!(col.get(2).unwrap(), Value::float64(-2.5));
    }

    #[test]
    fn test_get_numeric() {
        let col = create_numeric_column();
        assert_eq!(col.get(0).unwrap(), Value::Numeric(Decimal::new(12345, 2)));
        assert_eq!(col.get(1).unwrap(), Value::Null);
        assert_eq!(col.get(2).unwrap(), Value::Numeric(Decimal::new(-999, 1)));
    }

    #[test]
    fn test_get_string() {
        let col = create_string_column();
        assert_eq!(col.get(0).unwrap(), Value::String("hello".to_string()));
        assert_eq!(col.get(1).unwrap(), Value::Null);
        assert_eq!(col.get(2).unwrap(), Value::String("world".to_string()));
    }

    #[test]
    fn test_get_bytes() {
        let col = create_bytes_column();
        assert_eq!(col.get(0).unwrap(), Value::Bytes(vec![1, 2, 3]));
        assert_eq!(col.get(1).unwrap(), Value::Null);
        assert_eq!(col.get(2).unwrap(), Value::Bytes(vec![4, 5]));
    }

    #[test]
    fn test_get_date() {
        let col = create_date_column();
        assert_eq!(
            col.get(0).unwrap(),
            Value::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap())
        );
        assert_eq!(col.get(1).unwrap(), Value::Null);
        assert_eq!(
            col.get(2).unwrap(),
            Value::Date(NaiveDate::from_ymd_opt(2023, 6, 30).unwrap())
        );
    }

    #[test]
    fn test_get_time() {
        let col = create_time_column();
        assert_eq!(
            col.get(0).unwrap(),
            Value::Time(NaiveTime::from_hms_opt(10, 30, 0).unwrap())
        );
        assert_eq!(col.get(1).unwrap(), Value::Null);
        assert_eq!(
            col.get(2).unwrap(),
            Value::Time(NaiveTime::from_hms_opt(23, 59, 59).unwrap())
        );
    }

    #[test]
    fn test_get_datetime() {
        let col = create_datetime_column();
        assert_eq!(
            col.get(0).unwrap(),
            Value::DateTime(
                NaiveDate::from_ymd_opt(2024, 1, 15)
                    .unwrap()
                    .and_hms_opt(10, 30, 0)
                    .unwrap()
            )
        );
        assert_eq!(col.get(1).unwrap(), Value::Null);
        assert_eq!(
            col.get(2).unwrap(),
            Value::DateTime(
                NaiveDate::from_ymd_opt(2023, 6, 30)
                    .unwrap()
                    .and_hms_opt(23, 59, 59)
                    .unwrap()
            )
        );
    }

    #[test]
    fn test_get_timestamp() {
        let col = create_timestamp_column();
        assert_eq!(
            col.get(0).unwrap(),
            Value::Timestamp(Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap())
        );
        assert_eq!(col.get(1).unwrap(), Value::Null);
        assert_eq!(
            col.get(2).unwrap(),
            Value::Timestamp(Utc.with_ymd_and_hms(2023, 6, 30, 23, 59, 59).unwrap())
        );
    }

    #[test]
    fn test_get_json() {
        let col = create_json_column();
        assert_eq!(
            col.get(0).unwrap(),
            Value::Json(serde_json::json!({"key": "value"}))
        );
        assert_eq!(col.get(1).unwrap(), Value::Null);
        assert_eq!(
            col.get(2).unwrap(),
            Value::Json(serde_json::json!([1, 2, 3]))
        );
    }

    #[test]
    fn test_get_array() {
        let col = create_array_column();
        assert_eq!(
            col.get(0).unwrap(),
            Value::Array(vec![Value::Int64(1), Value::Int64(2)])
        );
        assert_eq!(col.get(1).unwrap(), Value::Null);
        assert_eq!(col.get(2).unwrap(), Value::Array(vec![Value::Int64(3)]));
    }

    #[test]
    fn test_get_struct() {
        let col = create_struct_column();
        assert_eq!(
            col.get(0).unwrap(),
            Value::Struct(vec![(
                "field1".to_string(),
                Value::String("val1".to_string())
            )])
        );
        assert_eq!(col.get(1).unwrap(), Value::Null);
        assert_eq!(
            col.get(2).unwrap(),
            Value::Struct(vec![(
                "field1".to_string(),
                Value::String("val2".to_string())
            )])
        );
    }

    #[test]
    fn test_get_geography() {
        let col = create_geography_column();
        assert_eq!(
            col.get(0).unwrap(),
            Value::Geography("POINT(0 0)".to_string())
        );
        assert_eq!(col.get(1).unwrap(), Value::Null);
        assert_eq!(
            col.get(2).unwrap(),
            Value::Geography("POINT(1 1)".to_string())
        );
    }

    #[test]
    fn test_get_interval() {
        let col = create_interval_column();
        assert_eq!(
            col.get(0).unwrap(),
            Value::Interval(IntervalValue::new(1, 2, 3))
        );
        assert_eq!(col.get(1).unwrap(), Value::Null);
        assert_eq!(
            col.get(2).unwrap(),
            Value::Interval(IntervalValue::new(4, 5, 6))
        );
    }

    #[test]
    fn test_get_range() {
        let col = create_range_column();
        assert_eq!(
            col.get(0).unwrap(),
            Value::Range(RangeValue::new(
                Some(Value::Int64(1)),
                Some(Value::Int64(10))
            ))
        );
        assert_eq!(col.get(1).unwrap(), Value::Null);
        assert_eq!(
            col.get(2).unwrap(),
            Value::Range(RangeValue::new(
                Some(Value::Int64(20)),
                Some(Value::Int64(30))
            ))
        );
    }

    #[test]
    fn test_get_out_of_bounds() {
        let col = create_int64_column();
        let result = col.get(100);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_value_bool() {
        let col = create_bool_column();
        assert_eq!(col.get_value(0), Value::Bool(true));
        assert_eq!(col.get_value(1), Value::Null);
        assert_eq!(col.get_value(2), Value::Bool(false));
    }

    #[test]
    fn test_get_value_int64() {
        let col = create_int64_column();
        assert_eq!(col.get_value(0), Value::Int64(42));
        assert_eq!(col.get_value(1), Value::Null);
        assert_eq!(col.get_value(2), Value::Int64(-100));
    }

    #[test]
    fn test_get_value_float64() {
        let col = create_float64_column();
        assert_eq!(col.get_value(0), Value::float64(3.15));
        assert_eq!(col.get_value(1), Value::Null);
        assert_eq!(col.get_value(2), Value::float64(-2.5));
    }

    #[test]
    fn test_get_value_numeric() {
        let col = create_numeric_column();
        assert_eq!(col.get_value(0), Value::Numeric(Decimal::new(12345, 2)));
        assert_eq!(col.get_value(1), Value::Null);
        assert_eq!(col.get_value(2), Value::Numeric(Decimal::new(-999, 1)));
    }

    #[test]
    fn test_get_value_string() {
        let col = create_string_column();
        assert_eq!(col.get_value(0), Value::String("hello".to_string()));
        assert_eq!(col.get_value(1), Value::Null);
        assert_eq!(col.get_value(2), Value::String("world".to_string()));
    }

    #[test]
    fn test_get_value_bytes() {
        let col = create_bytes_column();
        assert_eq!(col.get_value(0), Value::Bytes(vec![1, 2, 3]));
        assert_eq!(col.get_value(1), Value::Null);
        assert_eq!(col.get_value(2), Value::Bytes(vec![4, 5]));
    }

    #[test]
    fn test_get_value_date() {
        let col = create_date_column();
        assert_eq!(
            col.get_value(0),
            Value::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap())
        );
        assert_eq!(col.get_value(1), Value::Null);
        assert_eq!(
            col.get_value(2),
            Value::Date(NaiveDate::from_ymd_opt(2023, 6, 30).unwrap())
        );
    }

    #[test]
    fn test_get_value_time() {
        let col = create_time_column();
        assert_eq!(
            col.get_value(0),
            Value::Time(NaiveTime::from_hms_opt(10, 30, 0).unwrap())
        );
        assert_eq!(col.get_value(1), Value::Null);
        assert_eq!(
            col.get_value(2),
            Value::Time(NaiveTime::from_hms_opt(23, 59, 59).unwrap())
        );
    }

    #[test]
    fn test_get_value_datetime() {
        let col = create_datetime_column();
        assert_eq!(
            col.get_value(0),
            Value::DateTime(
                NaiveDate::from_ymd_opt(2024, 1, 15)
                    .unwrap()
                    .and_hms_opt(10, 30, 0)
                    .unwrap()
            )
        );
        assert_eq!(col.get_value(1), Value::Null);
        assert_eq!(
            col.get_value(2),
            Value::DateTime(
                NaiveDate::from_ymd_opt(2023, 6, 30)
                    .unwrap()
                    .and_hms_opt(23, 59, 59)
                    .unwrap()
            )
        );
    }

    #[test]
    fn test_get_value_timestamp() {
        let col = create_timestamp_column();
        assert_eq!(
            col.get_value(0),
            Value::Timestamp(Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap())
        );
        assert_eq!(col.get_value(1), Value::Null);
        assert_eq!(
            col.get_value(2),
            Value::Timestamp(Utc.with_ymd_and_hms(2023, 6, 30, 23, 59, 59).unwrap())
        );
    }

    #[test]
    fn test_get_value_json() {
        let col = create_json_column();
        assert_eq!(
            col.get_value(0),
            Value::Json(serde_json::json!({"key": "value"}))
        );
        assert_eq!(col.get_value(1), Value::Null);
        assert_eq!(col.get_value(2), Value::Json(serde_json::json!([1, 2, 3])));
    }

    #[test]
    fn test_get_value_array() {
        let col = create_array_column();
        assert_eq!(
            col.get_value(0),
            Value::Array(vec![Value::Int64(1), Value::Int64(2)])
        );
        assert_eq!(col.get_value(1), Value::Null);
        assert_eq!(col.get_value(2), Value::Array(vec![Value::Int64(3)]));
    }

    #[test]
    fn test_get_value_struct() {
        let col = create_struct_column();
        assert_eq!(
            col.get_value(0),
            Value::Struct(vec![(
                "field1".to_string(),
                Value::String("val1".to_string())
            )])
        );
        assert_eq!(col.get_value(1), Value::Null);
        assert_eq!(
            col.get_value(2),
            Value::Struct(vec![(
                "field1".to_string(),
                Value::String("val2".to_string())
            )])
        );
    }

    #[test]
    fn test_get_value_geography() {
        let col = create_geography_column();
        assert_eq!(col.get_value(0), Value::Geography("POINT(0 0)".to_string()));
        assert_eq!(col.get_value(1), Value::Null);
        assert_eq!(col.get_value(2), Value::Geography("POINT(1 1)".to_string()));
    }

    #[test]
    fn test_get_value_interval() {
        let col = create_interval_column();
        assert_eq!(
            col.get_value(0),
            Value::Interval(IntervalValue::new(1, 2, 3))
        );
        assert_eq!(col.get_value(1), Value::Null);
        assert_eq!(
            col.get_value(2),
            Value::Interval(IntervalValue::new(4, 5, 6))
        );
    }

    #[test]
    fn test_get_value_range() {
        let col = create_range_column();
        assert_eq!(
            col.get_value(0),
            Value::Range(RangeValue::new(
                Some(Value::Int64(1)),
                Some(Value::Int64(10))
            ))
        );
        assert_eq!(col.get_value(1), Value::Null);
        assert_eq!(
            col.get_value(2),
            Value::Range(RangeValue::new(
                Some(Value::Int64(20)),
                Some(Value::Int64(30))
            ))
        );
    }

    #[test]
    fn test_get_value_out_of_bounds() {
        let col = create_int64_column();
        assert_eq!(col.get_value(100), Value::Null);
    }

    #[test]
    fn test_set_bool() {
        let mut col = create_bool_column();
        col.set(0, Value::Bool(false)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Bool(false));
        col.set(0, Value::Null).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Null);
    }

    #[test]
    fn test_set_int64() {
        let mut col = create_int64_column();
        col.set(0, Value::Int64(999)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Int64(999));
        col.set(0, Value::Null).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Null);
    }

    #[test]
    fn test_set_float64() {
        let mut col = create_float64_column();
        col.set(0, Value::float64(9.99)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::float64(9.99));
        col.set(0, Value::Null).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Null);
    }

    #[test]
    fn test_set_numeric() {
        let mut col = create_numeric_column();
        col.set(0, Value::Numeric(Decimal::new(999, 0))).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Numeric(Decimal::new(999, 0)));
        col.set(0, Value::Null).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Null);
    }

    #[test]
    fn test_set_numeric_with_bignumeric() {
        let mut col = create_numeric_column();
        col.set(0, Value::BigNumeric(Decimal::new(12345, 2)))
            .unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Numeric(Decimal::new(12345, 2)));
    }

    #[test]
    fn test_set_string() {
        let mut col = create_string_column();
        col.set(0, Value::String("updated".to_string())).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::String("updated".to_string()));
        col.set(0, Value::Null).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Null);
    }

    #[test]
    fn test_set_bytes() {
        let mut col = create_bytes_column();
        col.set(0, Value::Bytes(vec![9, 8, 7])).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Bytes(vec![9, 8, 7]));
        col.set(0, Value::Null).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Null);
    }

    #[test]
    fn test_set_date() {
        let mut col = create_date_column();
        let new_date = NaiveDate::from_ymd_opt(2025, 12, 25).unwrap();
        col.set(0, Value::Date(new_date)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Date(new_date));
        col.set(0, Value::Null).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Null);
    }

    #[test]
    fn test_set_time() {
        let mut col = create_time_column();
        let new_time = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        col.set(0, Value::Time(new_time)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Time(new_time));
        col.set(0, Value::Null).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Null);
    }

    #[test]
    fn test_set_datetime() {
        let mut col = create_datetime_column();
        let new_datetime = NaiveDate::from_ymd_opt(2025, 12, 25)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        col.set(0, Value::DateTime(new_datetime)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::DateTime(new_datetime));
        col.set(0, Value::Null).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Null);
    }

    #[test]
    fn test_set_timestamp() {
        let mut col = create_timestamp_column();
        let new_ts = Utc.with_ymd_and_hms(2025, 12, 25, 12, 0, 0).unwrap();
        col.set(0, Value::Timestamp(new_ts)).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Timestamp(new_ts));
        col.set(0, Value::Null).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Null);
    }

    #[test]
    fn test_set_json() {
        let mut col = create_json_column();
        col.set(0, Value::Json(serde_json::json!({"new": "json"})))
            .unwrap();
        assert_eq!(
            col.get(0).unwrap(),
            Value::Json(serde_json::json!({"new": "json"}))
        );
        col.set(0, Value::Null).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Null);
    }

    #[test]
    fn test_set_array() {
        let mut col = create_array_column();
        col.set(0, Value::Array(vec![Value::Int64(100)])).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Array(vec![Value::Int64(100)]));
        col.set(0, Value::Null).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Null);
    }

    #[test]
    fn test_set_struct() {
        let mut col = create_struct_column();
        col.set(
            0,
            Value::Struct(vec![(
                "field1".to_string(),
                Value::String("new".to_string()),
            )]),
        )
        .unwrap();
        assert_eq!(
            col.get(0).unwrap(),
            Value::Struct(vec![(
                "field1".to_string(),
                Value::String("new".to_string())
            )])
        );
        col.set(0, Value::Null).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Null);
    }

    #[test]
    fn test_set_geography() {
        let mut col = create_geography_column();
        col.set(0, Value::Geography("POINT(5 5)".to_string()))
            .unwrap();
        assert_eq!(
            col.get(0).unwrap(),
            Value::Geography("POINT(5 5)".to_string())
        );
        col.set(0, Value::Null).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Null);
    }

    #[test]
    fn test_set_interval() {
        let mut col = create_interval_column();
        col.set(0, Value::Interval(IntervalValue::new(10, 20, 30)))
            .unwrap();
        assert_eq!(
            col.get(0).unwrap(),
            Value::Interval(IntervalValue::new(10, 20, 30))
        );
        col.set(0, Value::Null).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Null);
    }

    #[test]
    fn test_set_range() {
        let mut col = create_range_column();
        col.set(
            0,
            Value::Range(RangeValue::new(
                Some(Value::Int64(100)),
                Some(Value::Int64(200)),
            )),
        )
        .unwrap();
        assert_eq!(
            col.get(0).unwrap(),
            Value::Range(RangeValue::new(
                Some(Value::Int64(100)),
                Some(Value::Int64(200))
            ))
        );
        col.set(0, Value::Null).unwrap();
        assert_eq!(col.get(0).unwrap(), Value::Null);
    }

    #[test]
    fn test_set_type_mismatch() {
        let mut col = create_int64_column();
        let result = col.set(0, Value::String("wrong type".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_set_restores_null_to_value() {
        let mut col = create_int64_column();
        assert!(col.is_null(1));
        col.set(1, Value::Int64(999)).unwrap();
        assert!(!col.is_null(1));
        assert_eq!(col.get(1).unwrap(), Value::Int64(999));
    }
}
