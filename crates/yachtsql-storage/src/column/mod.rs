#![coverage(off)]

#[macro_use]
mod macros;

mod access;
mod aggregation;
mod mutation;
mod ops;
mod serde;

use ::serde::{Deserialize, Serialize};
use aligned_vec::AVec;
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use rust_decimal::Decimal;
use yachtsql_common::types::{DataType, IntervalValue, RangeValue, Value};

pub use self::serde::A64;
use crate::NullBitmap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Column {
    Bool {
        data: Vec<bool>,
        nulls: NullBitmap,
    },
    Int64 {
        #[serde(
            serialize_with = "serde::serialize_avec_i64",
            deserialize_with = "serde::deserialize_avec_i64"
        )]
        data: AVec<i64, A64>,
        nulls: NullBitmap,
    },
    Float64 {
        #[serde(
            serialize_with = "serde::serialize_avec_f64",
            deserialize_with = "serde::deserialize_avec_f64"
        )]
        data: AVec<f64, A64>,
        nulls: NullBitmap,
    },
    Numeric {
        data: Vec<Decimal>,
        nulls: NullBitmap,
    },
    String {
        data: Vec<String>,
        nulls: NullBitmap,
    },
    Bytes {
        data: Vec<Vec<u8>>,
        nulls: NullBitmap,
    },
    Date {
        data: Vec<NaiveDate>,
        nulls: NullBitmap,
    },
    Time {
        data: Vec<NaiveTime>,
        nulls: NullBitmap,
    },
    DateTime {
        data: Vec<chrono::NaiveDateTime>,
        nulls: NullBitmap,
    },
    Timestamp {
        data: Vec<DateTime<Utc>>,
        nulls: NullBitmap,
    },
    Json {
        data: Vec<serde_json::Value>,
        nulls: NullBitmap,
    },
    Array {
        data: Vec<Vec<Value>>,
        nulls: NullBitmap,
        element_type: DataType,
    },
    Struct {
        data: Vec<Vec<(String, Value)>>,
        nulls: NullBitmap,
        fields: Vec<(String, DataType)>,
    },
    Geography {
        data: Vec<String>,
        nulls: NullBitmap,
    },
    Interval {
        data: Vec<IntervalValue>,
        nulls: NullBitmap,
    },
    Range {
        data: Vec<RangeValue>,
        nulls: NullBitmap,
        element_type: DataType,
    },
}

impl Column {
    pub fn new(data_type: &DataType) -> Self {
        match data_type {
            DataType::Bool => Column::Bool {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::Int64 => Column::Int64 {
                data: AVec::new(64),
                nulls: NullBitmap::new(),
            },
            DataType::Float64 => Column::Float64 {
                data: AVec::new(64),
                nulls: NullBitmap::new(),
            },
            DataType::Numeric(_) | DataType::BigNumeric => Column::Numeric {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::String | DataType::Unknown => Column::String {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::Bytes => Column::Bytes {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::Date => Column::Date {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::Time => Column::Time {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::DateTime => Column::DateTime {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::Timestamp => Column::Timestamp {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::Json => Column::Json {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::Array(elem_type) => Column::Array {
                data: Vec::new(),
                nulls: NullBitmap::new(),
                element_type: (**elem_type).clone(),
            },
            DataType::Struct(fields) => Column::Struct {
                data: Vec::new(),
                nulls: NullBitmap::new(),
                fields: fields
                    .iter()
                    .map(|f| (f.name.clone(), f.data_type.clone()))
                    .collect(),
            },
            DataType::Geography => Column::Geography {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::Interval => Column::Interval {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::Range(elem_type) => Column::Range {
                data: Vec::new(),
                nulls: NullBitmap::new(),
                element_type: (**elem_type).clone(),
            },
        }
    }

    pub(crate) fn coerce_struct_value(
        value_fields: Vec<(String, Value)>,
        target_fields: &[(String, DataType)],
    ) -> Vec<(String, Value)> {
        value_fields
            .into_iter()
            .enumerate()
            .map(|(i, (_, val))| {
                let (new_name, new_type) = if i < target_fields.len() {
                    (&target_fields[i].0, &target_fields[i].1)
                } else {
                    return (format!("_field{}", i), val);
                };
                let coerced_val = Self::coerce_value_for_type(val, new_type);
                (new_name.clone(), coerced_val)
            })
            .collect()
    }

    fn coerce_value_for_type(value: Value, target_type: &DataType) -> Value {
        match (value, target_type) {
            (Value::Struct(fields), DataType::Struct(target_fields)) => {
                let target_field_info: Vec<(String, DataType)> = target_fields
                    .iter()
                    .map(|f| (f.name.clone(), f.data_type.clone()))
                    .collect();
                Value::Struct(Self::coerce_struct_value(fields, &target_field_info))
            }
            (Value::Array(elements), DataType::Array(element_type)) => {
                let coerced: Vec<Value> = elements
                    .into_iter()
                    .map(|e| Self::coerce_value_for_type(e, element_type))
                    .collect();
                Value::Array(coerced)
            }
            (v, _) => v,
        }
    }

    pub fn len(&self) -> usize {
        for_each_variant!(self, |data| data.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Column::Bool { .. } => DataType::Bool,
            Column::Int64 { .. } => DataType::Int64,
            Column::Float64 { .. } => DataType::Float64,
            Column::Numeric { .. } => DataType::Numeric(None),
            Column::String { .. } => DataType::String,
            Column::Bytes { .. } => DataType::Bytes,
            Column::Date { .. } => DataType::Date,
            Column::Time { .. } => DataType::Time,
            Column::DateTime { .. } => DataType::DateTime,
            Column::Timestamp { .. } => DataType::Timestamp,
            Column::Json { .. } => DataType::Json,
            Column::Array { element_type, .. } => DataType::Array(Box::new(element_type.clone())),
            Column::Struct { fields, .. } => {
                let struct_fields = fields
                    .iter()
                    .map(|(name, dt)| yachtsql_common::types::StructField {
                        name: name.clone(),
                        data_type: dt.clone(),
                    })
                    .collect();
                DataType::Struct(struct_fields)
            }
            Column::Geography { .. } => DataType::Geography,
            Column::Interval { .. } => DataType::Interval,
            Column::Range { element_type, .. } => DataType::Range(Box::new(element_type.clone())),
        }
    }

    pub fn count_null(&self) -> usize {
        with_nulls!(self, |nulls| nulls.count_null())
    }

    pub fn count_valid(&self) -> usize {
        self.len() - self.count_null()
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
    use rust_decimal::Decimal;
    use yachtsql_common::types::{DataType, IntervalValue, RangeValue, StructField, Value};

    use super::*;

    #[test]
    fn test_new_bool() {
        let col = Column::new(&DataType::Bool);
        assert!(col.is_empty());
        assert_eq!(col.len(), 0);
        assert_eq!(col.data_type(), DataType::Bool);
    }

    #[test]
    fn test_new_int64() {
        let col = Column::new(&DataType::Int64);
        assert!(col.is_empty());
        assert_eq!(col.data_type(), DataType::Int64);
    }

    #[test]
    fn test_new_float64() {
        let col = Column::new(&DataType::Float64);
        assert!(col.is_empty());
        assert_eq!(col.data_type(), DataType::Float64);
    }

    #[test]
    fn test_new_numeric() {
        let col = Column::new(&DataType::Numeric(Some((10, 2))));
        assert!(col.is_empty());
        assert_eq!(col.data_type(), DataType::Numeric(None));
    }

    #[test]
    fn test_new_bignumeric() {
        let col = Column::new(&DataType::BigNumeric);
        assert!(col.is_empty());
        assert_eq!(col.data_type(), DataType::Numeric(None));
    }

    #[test]
    fn test_new_string() {
        let col = Column::new(&DataType::String);
        assert!(col.is_empty());
        assert_eq!(col.data_type(), DataType::String);
    }

    #[test]
    fn test_new_unknown() {
        let col = Column::new(&DataType::Unknown);
        assert!(col.is_empty());
        assert_eq!(col.data_type(), DataType::String);
    }

    #[test]
    fn test_new_bytes() {
        let col = Column::new(&DataType::Bytes);
        assert!(col.is_empty());
        assert_eq!(col.data_type(), DataType::Bytes);
    }

    #[test]
    fn test_new_date() {
        let col = Column::new(&DataType::Date);
        assert!(col.is_empty());
        assert_eq!(col.data_type(), DataType::Date);
    }

    #[test]
    fn test_new_time() {
        let col = Column::new(&DataType::Time);
        assert!(col.is_empty());
        assert_eq!(col.data_type(), DataType::Time);
    }

    #[test]
    fn test_new_datetime() {
        let col = Column::new(&DataType::DateTime);
        assert!(col.is_empty());
        assert_eq!(col.data_type(), DataType::DateTime);
    }

    #[test]
    fn test_new_timestamp() {
        let col = Column::new(&DataType::Timestamp);
        assert!(col.is_empty());
        assert_eq!(col.data_type(), DataType::Timestamp);
    }

    #[test]
    fn test_new_json() {
        let col = Column::new(&DataType::Json);
        assert!(col.is_empty());
        assert_eq!(col.data_type(), DataType::Json);
    }

    #[test]
    fn test_new_array() {
        let col = Column::new(&DataType::Array(Box::new(DataType::Int64)));
        assert!(col.is_empty());
        assert_eq!(col.data_type(), DataType::Array(Box::new(DataType::Int64)));
    }

    #[test]
    fn test_new_struct() {
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
        let col = Column::new(&DataType::Struct(fields.clone()));
        assert!(col.is_empty());
        assert_eq!(col.data_type(), DataType::Struct(fields));
    }

    #[test]
    fn test_new_geography() {
        let col = Column::new(&DataType::Geography);
        assert!(col.is_empty());
        assert_eq!(col.data_type(), DataType::Geography);
    }

    #[test]
    fn test_new_interval() {
        let col = Column::new(&DataType::Interval);
        assert!(col.is_empty());
        assert_eq!(col.data_type(), DataType::Interval);
    }

    #[test]
    fn test_new_range() {
        let col = Column::new(&DataType::Range(Box::new(DataType::Date)));
        assert!(col.is_empty());
        assert_eq!(col.data_type(), DataType::Range(Box::new(DataType::Date)));
    }

    #[test]
    fn test_len_bool() {
        let mut col = Column::new(&DataType::Bool);
        assert_eq!(col.len(), 0);
        col.push(Value::Bool(true)).unwrap();
        assert_eq!(col.len(), 1);
        col.push(Value::Bool(false)).unwrap();
        assert_eq!(col.len(), 2);
    }

    #[test]
    fn test_len_int64() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::Int64(1)).unwrap();
        col.push(Value::Int64(2)).unwrap();
        col.push(Value::Int64(3)).unwrap();
        assert_eq!(col.len(), 3);
    }

    #[test]
    fn test_len_float64() {
        let mut col = Column::new(&DataType::Float64);
        col.push(Value::float64(1.0)).unwrap();
        assert_eq!(col.len(), 1);
    }

    #[test]
    fn test_len_numeric() {
        let mut col = Column::new(&DataType::Numeric(None));
        col.push(Value::Numeric(Decimal::from(10))).unwrap();
        col.push(Value::Numeric(Decimal::from(20))).unwrap();
        assert_eq!(col.len(), 2);
    }

    #[test]
    fn test_len_string() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::String("a".to_string())).unwrap();
        assert_eq!(col.len(), 1);
    }

    #[test]
    fn test_len_bytes() {
        let mut col = Column::new(&DataType::Bytes);
        col.push(Value::Bytes(vec![1, 2, 3])).unwrap();
        assert_eq!(col.len(), 1);
    }

    #[test]
    fn test_len_date() {
        let mut col = Column::new(&DataType::Date);
        col.push(Value::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()))
            .unwrap();
        assert_eq!(col.len(), 1);
    }

    #[test]
    fn test_len_time() {
        let mut col = Column::new(&DataType::Time);
        col.push(Value::Time(NaiveTime::from_hms_opt(10, 0, 0).unwrap()))
            .unwrap();
        assert_eq!(col.len(), 1);
    }

    #[test]
    fn test_len_datetime() {
        let mut col = Column::new(&DataType::DateTime);
        col.push(Value::DateTime(
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ))
        .unwrap();
        assert_eq!(col.len(), 1);
    }

    #[test]
    fn test_len_timestamp() {
        let mut col = Column::new(&DataType::Timestamp);
        col.push(Value::Timestamp(
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
        ))
        .unwrap();
        assert_eq!(col.len(), 1);
    }

    #[test]
    fn test_len_json() {
        let mut col = Column::new(&DataType::Json);
        col.push(Value::Json(serde_json::json!({"a": 1}))).unwrap();
        assert_eq!(col.len(), 1);
    }

    #[test]
    fn test_len_array() {
        let mut col = Column::new(&DataType::Array(Box::new(DataType::Int64)));
        col.push(Value::Array(vec![Value::Int64(1), Value::Int64(2)]))
            .unwrap();
        assert_eq!(col.len(), 1);
    }

    #[test]
    fn test_len_struct() {
        let fields = vec![StructField {
            name: "a".to_string(),
            data_type: DataType::Int64,
        }];
        let mut col = Column::new(&DataType::Struct(fields));
        col.push(Value::Struct(vec![("a".to_string(), Value::Int64(1))]))
            .unwrap();
        assert_eq!(col.len(), 1);
    }

    #[test]
    fn test_len_geography() {
        let mut col = Column::new(&DataType::Geography);
        col.push(Value::Geography("POINT(0 0)".to_string()))
            .unwrap();
        assert_eq!(col.len(), 1);
    }

    #[test]
    fn test_len_interval() {
        let mut col = Column::new(&DataType::Interval);
        col.push(Value::Interval(IntervalValue::new(1, 2, 3)))
            .unwrap();
        assert_eq!(col.len(), 1);
    }

    #[test]
    fn test_len_range() {
        let mut col = Column::new(&DataType::Range(Box::new(DataType::Int64)));
        col.push(Value::Range(RangeValue::new(
            Some(Value::Int64(1)),
            Some(Value::Int64(10)),
        )))
        .unwrap();
        assert_eq!(col.len(), 1);
    }

    #[test]
    fn test_is_empty_true() {
        let col = Column::new(&DataType::Int64);
        assert!(col.is_empty());
    }

    #[test]
    fn test_is_empty_false() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::Int64(42)).unwrap();
        assert!(!col.is_empty());
    }

    #[test]
    fn test_data_type_bool() {
        let col = Column::Bool {
            data: vec![true, false],
            nulls: NullBitmap::new_valid(2),
        };
        assert_eq!(col.data_type(), DataType::Bool);
    }

    #[test]
    fn test_data_type_int64() {
        let col = Column::Int64 {
            data: AVec::from_iter(64, vec![1, 2, 3]),
            nulls: NullBitmap::new_valid(3),
        };
        assert_eq!(col.data_type(), DataType::Int64);
    }

    #[test]
    fn test_data_type_float64() {
        let col = Column::Float64 {
            data: AVec::from_iter(64, vec![1.0, 2.0]),
            nulls: NullBitmap::new_valid(2),
        };
        assert_eq!(col.data_type(), DataType::Float64);
    }

    #[test]
    fn test_data_type_numeric() {
        let col = Column::Numeric {
            data: vec![Decimal::from(10)],
            nulls: NullBitmap::new_valid(1),
        };
        assert_eq!(col.data_type(), DataType::Numeric(None));
    }

    #[test]
    fn test_data_type_string() {
        let col = Column::String {
            data: vec!["hello".to_string()],
            nulls: NullBitmap::new_valid(1),
        };
        assert_eq!(col.data_type(), DataType::String);
    }

    #[test]
    fn test_data_type_bytes() {
        let col = Column::Bytes {
            data: vec![vec![1, 2, 3]],
            nulls: NullBitmap::new_valid(1),
        };
        assert_eq!(col.data_type(), DataType::Bytes);
    }

    #[test]
    fn test_data_type_date() {
        let col = Column::Date {
            data: vec![NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()],
            nulls: NullBitmap::new_valid(1),
        };
        assert_eq!(col.data_type(), DataType::Date);
    }

    #[test]
    fn test_data_type_time() {
        let col = Column::Time {
            data: vec![NaiveTime::from_hms_opt(10, 0, 0).unwrap()],
            nulls: NullBitmap::new_valid(1),
        };
        assert_eq!(col.data_type(), DataType::Time);
    }

    #[test]
    fn test_data_type_datetime() {
        let col = Column::DateTime {
            data: vec![
                NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ],
            nulls: NullBitmap::new_valid(1),
        };
        assert_eq!(col.data_type(), DataType::DateTime);
    }

    #[test]
    fn test_data_type_timestamp() {
        let col = Column::Timestamp {
            data: vec![Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()],
            nulls: NullBitmap::new_valid(1),
        };
        assert_eq!(col.data_type(), DataType::Timestamp);
    }

    #[test]
    fn test_data_type_json() {
        let col = Column::Json {
            data: vec![serde_json::json!({"a": 1})],
            nulls: NullBitmap::new_valid(1),
        };
        assert_eq!(col.data_type(), DataType::Json);
    }

    #[test]
    fn test_data_type_array() {
        let col = Column::Array {
            data: vec![vec![Value::Int64(1)]],
            nulls: NullBitmap::new_valid(1),
            element_type: DataType::Int64,
        };
        assert_eq!(col.data_type(), DataType::Array(Box::new(DataType::Int64)));
    }

    #[test]
    fn test_data_type_struct() {
        let col = Column::Struct {
            data: vec![vec![("a".to_string(), Value::Int64(1))]],
            nulls: NullBitmap::new_valid(1),
            fields: vec![("a".to_string(), DataType::Int64)],
        };
        let expected = DataType::Struct(vec![StructField {
            name: "a".to_string(),
            data_type: DataType::Int64,
        }]);
        assert_eq!(col.data_type(), expected);
    }

    #[test]
    fn test_data_type_geography() {
        let col = Column::Geography {
            data: vec!["POINT(0 0)".to_string()],
            nulls: NullBitmap::new_valid(1),
        };
        assert_eq!(col.data_type(), DataType::Geography);
    }

    #[test]
    fn test_data_type_interval() {
        let col = Column::Interval {
            data: vec![IntervalValue::new(1, 2, 3)],
            nulls: NullBitmap::new_valid(1),
        };
        assert_eq!(col.data_type(), DataType::Interval);
    }

    #[test]
    fn test_data_type_range() {
        let col = Column::Range {
            data: vec![RangeValue::new(None, None)],
            nulls: NullBitmap::new_valid(1),
            element_type: DataType::Date,
        };
        assert_eq!(col.data_type(), DataType::Range(Box::new(DataType::Date)));
    }

    #[test]
    fn test_count_null_bool() {
        let mut col = Column::new(&DataType::Bool);
        col.push(Value::Bool(true)).unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Bool(false)).unwrap();
        assert_eq!(col.count_null(), 1);
    }

    #[test]
    fn test_count_null_int64() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::Null).unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Int64(42)).unwrap();
        assert_eq!(col.count_null(), 2);
    }

    #[test]
    fn test_count_null_float64() {
        let mut col = Column::new(&DataType::Float64);
        col.push(Value::Null).unwrap();
        col.push(Value::float64(1.0)).unwrap();
        assert_eq!(col.count_null(), 1);
    }

    #[test]
    fn test_count_null_numeric() {
        let mut col = Column::new(&DataType::Numeric(None));
        col.push(Value::Null).unwrap();
        col.push(Value::Numeric(Decimal::from(10))).unwrap();
        col.push(Value::Null).unwrap();
        assert_eq!(col.count_null(), 2);
    }

    #[test]
    fn test_count_null_string() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::Null).unwrap();
        assert_eq!(col.count_null(), 1);
    }

    #[test]
    fn test_count_null_bytes() {
        let mut col = Column::new(&DataType::Bytes);
        col.push(Value::Bytes(vec![1])).unwrap();
        col.push(Value::Null).unwrap();
        assert_eq!(col.count_null(), 1);
    }

    #[test]
    fn test_count_null_date() {
        let mut col = Column::new(&DataType::Date);
        col.push(Value::Null).unwrap();
        col.push(Value::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()))
            .unwrap();
        assert_eq!(col.count_null(), 1);
    }

    #[test]
    fn test_count_null_time() {
        let mut col = Column::new(&DataType::Time);
        col.push(Value::Null).unwrap();
        col.push(Value::Time(NaiveTime::from_hms_opt(10, 0, 0).unwrap()))
            .unwrap();
        assert_eq!(col.count_null(), 1);
    }

    #[test]
    fn test_count_null_datetime() {
        let mut col = Column::new(&DataType::DateTime);
        col.push(Value::Null).unwrap();
        assert_eq!(col.count_null(), 1);
    }

    #[test]
    fn test_count_null_timestamp() {
        let mut col = Column::new(&DataType::Timestamp);
        col.push(Value::Null).unwrap();
        assert_eq!(col.count_null(), 1);
    }

    #[test]
    fn test_count_null_json() {
        let mut col = Column::new(&DataType::Json);
        col.push(Value::Null).unwrap();
        col.push(Value::Json(serde_json::json!(1))).unwrap();
        assert_eq!(col.count_null(), 1);
    }

    #[test]
    fn test_count_null_array() {
        let mut col = Column::new(&DataType::Array(Box::new(DataType::Int64)));
        col.push(Value::Null).unwrap();
        col.push(Value::Array(vec![Value::Int64(1)])).unwrap();
        assert_eq!(col.count_null(), 1);
    }

    #[test]
    fn test_count_null_struct() {
        let fields = vec![StructField {
            name: "a".to_string(),
            data_type: DataType::Int64,
        }];
        let mut col = Column::new(&DataType::Struct(fields));
        col.push(Value::Null).unwrap();
        assert_eq!(col.count_null(), 1);
    }

    #[test]
    fn test_count_null_geography() {
        let mut col = Column::new(&DataType::Geography);
        col.push(Value::Null).unwrap();
        assert_eq!(col.count_null(), 1);
    }

    #[test]
    fn test_count_null_interval() {
        let mut col = Column::new(&DataType::Interval);
        col.push(Value::Null).unwrap();
        col.push(Value::Interval(IntervalValue::new(1, 0, 0)))
            .unwrap();
        assert_eq!(col.count_null(), 1);
    }

    #[test]
    fn test_count_null_range() {
        let mut col = Column::new(&DataType::Range(Box::new(DataType::Int64)));
        col.push(Value::Null).unwrap();
        assert_eq!(col.count_null(), 1);
    }

    #[test]
    fn test_count_valid_all_valid() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::Int64(1)).unwrap();
        col.push(Value::Int64(2)).unwrap();
        col.push(Value::Int64(3)).unwrap();
        assert_eq!(col.count_valid(), 3);
    }

    #[test]
    fn test_count_valid_mixed() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::Int64(1)).unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Int64(3)).unwrap();
        col.push(Value::Null).unwrap();
        assert_eq!(col.count_valid(), 2);
    }

    #[test]
    fn test_count_valid_all_null() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::Null).unwrap();
        col.push(Value::Null).unwrap();
        assert_eq!(col.count_valid(), 0);
    }

    #[test]
    fn test_count_valid_empty() {
        let col = Column::new(&DataType::Int64);
        assert_eq!(col.count_valid(), 0);
    }

    #[test]
    fn test_coerce_struct_value_exact_fields() {
        let target_fields = vec![
            ("name".to_string(), DataType::String),
            ("age".to_string(), DataType::Int64),
        ];
        let value_fields = vec![
            ("x".to_string(), Value::String("Alice".to_string())),
            ("y".to_string(), Value::Int64(30)),
        ];
        let result = Column::coerce_struct_value(value_fields, &target_fields);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, "name");
        assert_eq!(result[0].1, Value::String("Alice".to_string()));
        assert_eq!(result[1].0, "age");
        assert_eq!(result[1].1, Value::Int64(30));
    }

    #[test]
    fn test_coerce_struct_value_more_values_than_target() {
        let target_fields = vec![("a".to_string(), DataType::Int64)];
        let value_fields = vec![
            ("x".to_string(), Value::Int64(1)),
            ("y".to_string(), Value::Int64(2)),
            ("z".to_string(), Value::Int64(3)),
        ];
        let result = Column::coerce_struct_value(value_fields, &target_fields);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].0, "a");
        assert_eq!(result[0].1, Value::Int64(1));
        assert_eq!(result[1].0, "_field1");
        assert_eq!(result[1].1, Value::Int64(2));
        assert_eq!(result[2].0, "_field2");
        assert_eq!(result[2].1, Value::Int64(3));
    }

    #[test]
    fn test_coerce_struct_value_empty() {
        let target_fields: Vec<(String, DataType)> = vec![];
        let value_fields: Vec<(String, Value)> = vec![];
        let result = Column::coerce_struct_value(value_fields, &target_fields);
        assert!(result.is_empty());
    }

    #[test]
    fn test_coerce_struct_value_nested_struct() {
        let inner_fields = vec![StructField {
            name: "inner_val".to_string(),
            data_type: DataType::Int64,
        }];
        let target_fields = vec![("outer".to_string(), DataType::Struct(inner_fields))];
        let inner_value = Value::Struct(vec![("x".to_string(), Value::Int64(42))]);
        let value_fields = vec![("y".to_string(), inner_value)];
        let result = Column::coerce_struct_value(value_fields, &target_fields);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "outer");
        match &result[0].1 {
            Value::Struct(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].0, "inner_val");
                assert_eq!(fields[0].1, Value::Int64(42));
            }
            _ => panic!("Expected struct value"),
        }
    }

    #[test]
    fn test_coerce_value_for_type_struct() {
        let inner_fields = vec![StructField {
            name: "field1".to_string(),
            data_type: DataType::String,
        }];
        let target_type = DataType::Struct(inner_fields);
        let value = Value::Struct(vec![("x".to_string(), Value::String("hello".to_string()))]);
        let result = Column::coerce_value_for_type(value, &target_type);
        match result {
            Value::Struct(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].0, "field1");
            }
            _ => panic!("Expected struct"),
        }
    }

    #[test]
    fn test_coerce_value_for_type_array() {
        let inner_fields = vec![StructField {
            name: "a".to_string(),
            data_type: DataType::Int64,
        }];
        let element_type = DataType::Struct(inner_fields);
        let target_type = DataType::Array(Box::new(element_type));

        let elem1 = Value::Struct(vec![("x".to_string(), Value::Int64(1))]);
        let elem2 = Value::Struct(vec![("y".to_string(), Value::Int64(2))]);
        let array_value = Value::Array(vec![elem1, elem2]);

        let result = Column::coerce_value_for_type(array_value, &target_type);
        match result {
            Value::Array(elements) => {
                assert_eq!(elements.len(), 2);
                for elem in elements {
                    match elem {
                        Value::Struct(fields) => {
                            assert_eq!(fields[0].0, "a");
                        }
                        _ => panic!("Expected struct element"),
                    }
                }
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_coerce_value_for_type_passthrough() {
        let target_type = DataType::Int64;
        let value = Value::Int64(42);
        let result = Column::coerce_value_for_type(value.clone(), &target_type);
        assert_eq!(result, value);
    }

    #[test]
    fn test_coerce_value_for_type_string_passthrough() {
        let target_type = DataType::String;
        let value = Value::String("hello".to_string());
        let result = Column::coerce_value_for_type(value.clone(), &target_type);
        assert_eq!(result, value);
    }

    #[test]
    fn test_coerce_value_for_type_nested_array_of_arrays() {
        let inner_type = DataType::Int64;
        let target_type = DataType::Array(Box::new(inner_type));
        let value = Value::Array(vec![Value::Int64(1), Value::Int64(2)]);
        let result = Column::coerce_value_for_type(value.clone(), &target_type);
        assert_eq!(result, value);
    }

    #[test]
    fn test_column_clone() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::Int64(1)).unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Int64(3)).unwrap();

        let cloned = col.clone();
        assert_eq!(cloned.len(), 3);
        assert_eq!(cloned.get(0).unwrap(), Value::Int64(1));
        assert_eq!(cloned.get(1).unwrap(), Value::Null);
        assert_eq!(cloned.get(2).unwrap(), Value::Int64(3));
    }

    #[test]
    fn test_column_partial_eq() {
        let mut col1 = Column::new(&DataType::Int64);
        col1.push(Value::Int64(1)).unwrap();
        col1.push(Value::Int64(2)).unwrap();

        let mut col2 = Column::new(&DataType::Int64);
        col2.push(Value::Int64(1)).unwrap();
        col2.push(Value::Int64(2)).unwrap();

        assert_eq!(col1, col2);
    }

    #[test]
    fn test_column_partial_eq_different() {
        let mut col1 = Column::new(&DataType::Int64);
        col1.push(Value::Int64(1)).unwrap();

        let mut col2 = Column::new(&DataType::Int64);
        col2.push(Value::Int64(2)).unwrap();

        assert_ne!(col1, col2);
    }

    #[test]
    fn test_column_debug() {
        let col = Column::new(&DataType::Bool);
        let debug_str = format!("{:?}", col);
        assert!(debug_str.contains("Bool"));
    }

    #[test]
    fn test_multiple_struct_fields_data_type() {
        let col = Column::Struct {
            data: vec![],
            nulls: NullBitmap::new(),
            fields: vec![
                ("first".to_string(), DataType::String),
                ("second".to_string(), DataType::Int64),
                ("third".to_string(), DataType::Bool),
            ],
        };
        let dt = col.data_type();
        match dt {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0].name, "first");
                assert_eq!(fields[0].data_type, DataType::String);
                assert_eq!(fields[1].name, "second");
                assert_eq!(fields[1].data_type, DataType::Int64);
                assert_eq!(fields[2].name, "third");
                assert_eq!(fields[2].data_type, DataType::Bool);
            }
            _ => panic!("Expected Struct data type"),
        }
    }

    #[test]
    fn test_count_null_empty_columns() {
        assert_eq!(Column::new(&DataType::Bool).count_null(), 0);
        assert_eq!(Column::new(&DataType::Int64).count_null(), 0);
        assert_eq!(Column::new(&DataType::Float64).count_null(), 0);
        assert_eq!(Column::new(&DataType::Numeric(None)).count_null(), 0);
        assert_eq!(Column::new(&DataType::String).count_null(), 0);
        assert_eq!(Column::new(&DataType::Bytes).count_null(), 0);
        assert_eq!(Column::new(&DataType::Date).count_null(), 0);
        assert_eq!(Column::new(&DataType::Time).count_null(), 0);
        assert_eq!(Column::new(&DataType::DateTime).count_null(), 0);
        assert_eq!(Column::new(&DataType::Timestamp).count_null(), 0);
        assert_eq!(Column::new(&DataType::Json).count_null(), 0);
        assert_eq!(
            Column::new(&DataType::Array(Box::new(DataType::Int64))).count_null(),
            0
        );
        let fields = vec![StructField {
            name: "a".to_string(),
            data_type: DataType::Int64,
        }];
        assert_eq!(Column::new(&DataType::Struct(fields)).count_null(), 0);
        assert_eq!(Column::new(&DataType::Geography).count_null(), 0);
        assert_eq!(Column::new(&DataType::Interval).count_null(), 0);
        assert_eq!(
            Column::new(&DataType::Range(Box::new(DataType::Int64))).count_null(),
            0
        );
    }
}
