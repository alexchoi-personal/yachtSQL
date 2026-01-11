use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::arrow::record_batch::RecordBatch;

#[derive(Debug, Clone, PartialEq)]
pub enum TestValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Date(i32),
    DateTime(i64),
    Timestamp(i64),
    Time(i64),
    Interval(i64),
    Numeric(rust_decimal::Decimal),
    Array(Vec<TestValue>),
    Struct(Vec<(String, TestValue)>),
}

impl From<i64> for TestValue {
    fn from(v: i64) -> Self {
        TestValue::Int64(v)
    }
}

impl From<i32> for TestValue {
    fn from(v: i32) -> Self {
        TestValue::Int64(v as i64)
    }
}

impl From<f64> for TestValue {
    fn from(v: f64) -> Self {
        TestValue::Float64(v)
    }
}

impl From<bool> for TestValue {
    fn from(v: bool) -> Self {
        TestValue::Bool(v)
    }
}

impl From<&str> for TestValue {
    fn from(v: &str) -> Self {
        TestValue::String(v.to_string())
    }
}

impl From<String> for TestValue {
    fn from(v: String) -> Self {
        TestValue::String(v)
    }
}

impl<T: Into<TestValue>> From<Option<T>> for TestValue {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(val) => val.into(),
            None => TestValue::Null,
        }
    }
}

pub fn bytes(val: &[u8]) -> TestValue {
    TestValue::Bytes(val.to_vec())
}

pub fn date(year: i32, month: u32, day: u32) -> TestValue {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let target = chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap();
    let days = (target - epoch).num_days() as i32;
    TestValue::Date(days)
}

pub fn timestamp(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> TestValue {
    use chrono::{TimeZone, Utc};
    let dt = Utc
        .with_ymd_and_hms(year, month, day, hour, min, sec)
        .unwrap();
    TestValue::Timestamp(dt.timestamp_nanos_opt().unwrap())
}

pub fn datetime(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> TestValue {
    use chrono::NaiveDate;
    let dt = NaiveDate::from_ymd_opt(year, month, day)
        .unwrap()
        .and_hms_opt(hour, min, sec)
        .unwrap();
    TestValue::DateTime(dt.and_utc().timestamp_nanos_opt().unwrap())
}

pub fn numeric(val: &str) -> TestValue {
    use std::str::FromStr;
    TestValue::Numeric(rust_decimal::Decimal::from_str(val).unwrap())
}

pub fn array(vals: Vec<TestValue>) -> TestValue {
    TestValue::Array(vals)
}

pub fn interval() -> TestValue {
    TestValue::Interval(0)
}

pub const NULL: TestValue = TestValue::Null;

pub trait IntoTestValue {
    fn into_test_value(self) -> TestValue;
}

impl IntoTestValue for TestValue {
    fn into_test_value(self) -> TestValue {
        self
    }
}

impl IntoTestValue for &TestValue {
    fn into_test_value(self) -> TestValue {
        self.clone()
    }
}

impl IntoTestValue for i64 {
    fn into_test_value(self) -> TestValue {
        TestValue::Int64(self)
    }
}

impl IntoTestValue for i32 {
    fn into_test_value(self) -> TestValue {
        TestValue::Int64(self as i64)
    }
}

impl IntoTestValue for f64 {
    fn into_test_value(self) -> TestValue {
        TestValue::Float64(self)
    }
}

impl IntoTestValue for bool {
    fn into_test_value(self) -> TestValue {
        TestValue::Bool(self)
    }
}

impl IntoTestValue for &str {
    fn into_test_value(self) -> TestValue {
        TestValue::String(self.to_string())
    }
}

impl IntoTestValue for String {
    fn into_test_value(self) -> TestValue {
        TestValue::String(self)
    }
}

pub fn convert_to_test_value<T: IntoTestValue>(val: T) -> TestValue {
    val.into_test_value()
}

pub fn extract_value(array: &ArrayRef, row: usize) -> TestValue {
    if array.is_null(row) || matches!(array.data_type(), DataType::Null) {
        return TestValue::Null;
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            TestValue::Bool(arr.value(row))
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            TestValue::Int64(arr.value(row) as i64)
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            TestValue::Int64(arr.value(row) as i64)
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            TestValue::Int64(arr.value(row) as i64)
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            TestValue::Int64(arr.value(row))
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            TestValue::Int64(arr.value(row) as i64)
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            TestValue::Int64(arr.value(row) as i64)
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            TestValue::Int64(arr.value(row) as i64)
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            TestValue::Int64(arr.value(row) as i64)
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            TestValue::Float64(arr.value(row) as f64)
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            TestValue::Float64(arr.value(row))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            TestValue::String(arr.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            TestValue::String(arr.value(row).to_string())
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            TestValue::Bytes(arr.value(row).to_vec())
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            TestValue::Date(arr.value(row))
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            TestValue::Date((arr.value(row) / 86400000) as i32)
        }
        DataType::Time64(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .unwrap();
            TestValue::Time(arr.value(row))
        }
        DataType::Timestamp(_, tz) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            if tz.is_some() {
                TestValue::Timestamp(arr.value(row))
            } else {
                TestValue::DateTime(arr.value(row))
            }
        }
        DataType::Interval(_) => TestValue::Interval(0),
        DataType::Decimal128(_, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let v = arr.value(row);
            let d = rust_decimal::Decimal::from_i128_with_scale(v, *scale as u32);
            TestValue::Numeric(d)
        }
        DataType::List(_) => {
            let arr = array.as_any().downcast_ref::<ListArray>().unwrap();
            let inner = arr.value(row);
            let values: Vec<TestValue> =
                (0..inner.len()).map(|i| extract_value(&inner, i)).collect();
            TestValue::Array(values)
        }
        DataType::Struct(fields) => {
            let arr = array.as_any().downcast_ref::<StructArray>().unwrap();
            let entries: Vec<(String, TestValue)> = fields
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    let col = arr.column(i);
                    (f.name().clone(), extract_value(col, row))
                })
                .collect();
            TestValue::Struct(entries)
        }
        DataType::Null => TestValue::Null,
        _ => TestValue::String(format!("<unsupported: {:?}>", array.data_type())),
    }
}

pub fn batch_to_rows(batches: &[RecordBatch]) -> Vec<Vec<TestValue>> {
    let mut rows = Vec::new();
    for batch in batches {
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();
        for row_idx in 0..num_rows {
            let mut row = Vec::with_capacity(num_cols);
            for col_idx in 0..num_cols {
                let array = batch.column(col_idx);
                row.push(extract_value(array, row_idx));
            }
            rows.push(row);
        }
    }
    rows
}

pub fn compare_values(actual: &TestValue, expected: &TestValue) -> bool {
    match (actual, expected) {
        (TestValue::Null, TestValue::Null) => true,
        (TestValue::Bool(a), TestValue::Bool(e)) => a == e,
        (TestValue::Int64(a), TestValue::Int64(e)) => a == e,
        (TestValue::Float64(a), TestValue::Float64(e)) => {
            if a.is_nan() && e.is_nan() {
                return true;
            }
            (a - e).abs() < 1e-10 || (a - e).abs() / e.abs().max(1.0) < 1e-10
        }
        (TestValue::String(a), TestValue::String(e)) => a == e,
        (TestValue::Bytes(a), TestValue::Bytes(e)) => a == e,
        (TestValue::Date(a), TestValue::Date(e)) => a == e,
        (TestValue::DateTime(a), TestValue::DateTime(e)) => a == e,
        (TestValue::Timestamp(a), TestValue::Timestamp(e)) => a == e,
        (TestValue::Time(a), TestValue::Time(e)) => a == e,
        (TestValue::Interval(_), TestValue::Interval(_)) => true,
        (TestValue::Numeric(a), TestValue::Numeric(e)) => a == e,
        (TestValue::Array(a), TestValue::Array(e)) => {
            a.len() == e.len()
                && a.iter()
                    .zip(e.iter())
                    .all(|(av, ev)| compare_values(av, ev))
        }
        (TestValue::Struct(a), TestValue::Struct(e)) => {
            a.len() == e.len()
                && a.iter()
                    .zip(e.iter())
                    .all(|((ak, av), (ek, ev))| ak == ek && compare_values(av, ev))
        }
        _ => false,
    }
}

pub fn compare_rows(actual: &[Vec<TestValue>], expected: &[Vec<TestValue>]) -> bool {
    if actual.len() != expected.len() {
        return false;
    }
    for (actual_row, expected_row) in actual.iter().zip(expected.iter()) {
        if actual_row.len() != expected_row.len() {
            return false;
        }
        for (actual_val, expected_val) in actual_row.iter().zip(expected_row.iter()) {
            if !compare_values(actual_val, expected_val) {
                return false;
            }
        }
    }
    true
}

#[macro_export]
macro_rules! test_val {
    (null) => {
        $crate::TestValue::Null
    };
    ($val:expr) => {{
        let v: $crate::TestValue = $val.into();
        v
    }};
}

#[macro_export]
macro_rules! assert_batch_records_eq {
    ($batches:expr, [$( [$($val:expr),* $(,)?] ),* $(,)?]) => {{
        let actual_rows = $crate::batch_to_rows(&$batches);
        let expected_rows: Vec<Vec<$crate::TestValue>> = vec![
            $(vec![$($crate::convert_to_test_value($val)),*]),*
        ];

        if !$crate::compare_rows(&actual_rows, &expected_rows) {
            panic!(
                "RecordBatch mismatch!\n\nActual ({} rows):\n{:#?}\n\nExpected ({} rows):\n{:#?}",
                actual_rows.len(),
                actual_rows,
                expected_rows.len(),
                expected_rows
            );
        }
    }};
}

#[cfg(test)]
mod tests {
    #![allow(clippy::useless_vec)]
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_assert_batch_records_eq_simple() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap();

        assert_batch_records_eq!(vec![batch], [[1, "Alice"], [2, "Bob"], [3, "Charlie"],]);
    }

    #[test]
    fn test_assert_batch_records_eq_with_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob"), None])),
            ],
        )
        .unwrap();

        assert_batch_records_eq!(vec![batch], [[1, "Alice"], [NULL, "Bob"], [3, NULL],]);
    }
}
