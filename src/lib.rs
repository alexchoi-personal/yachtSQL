#![feature(coverage_attribute)]
#![coverage(off)]
#![allow(clippy::wildcard_enum_match_arm)]
#![allow(clippy::result_unit_err)]

//! YachtSQL - A SQL database engine (BigQuery dialect) powered by Apache DataFusion.
//!
//! YachtSQL provides an in-memory SQL database with BigQuery dialect support,
//! using Apache Arrow for columnar storage and DataFusion for query execution.
//!
//! # Architecture
//!
//! The query processing pipeline is:
//! ```text
//! SQL String → Parser → LogicalPlan → DataFusion → RecordBatch
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use yachtsql::YachtSQLSession;
//!
//! #[tokio::main]
//! async fn main() {
//!     let session = YachtSQLSession::new();
//!
//!     // Register a table from a RecordBatch
//!     let schema = Arc::new(Schema::new(vec![
//!         Field::new("id", DataType::Int64, false),
//!         Field::new("name", DataType::Utf8, false),
//!     ]));
//!     let batch = RecordBatch::try_new(schema, vec![...]).unwrap();
//!     session.register_batch("users", batch).unwrap();
//!
//!     // Query data
//!     let result = session
//!         .execute_sql("SELECT * FROM users WHERE id = 1")
//!         .await
//!         .unwrap();
//! }
//! ```

pub use datafusion::arrow;
pub use datafusion::prelude::*;
pub use yachtsql_common::error::{Error, Result};
pub use yachtsql_common::types::{DataType, Field, FieldMode, Schema, Value};
pub use yachtsql_executor::{RecordBatch, YachtSQLSession};
pub use yachtsql_ir::LogicalPlan;
pub use yachtsql_parser::{CatalogProvider, Planner, parse_and_plan, parse_sql};

mod result_ext {
    use std::sync::Arc;

    use datafusion::arrow::array::ArrayRef;

    use super::RecordBatch;
    #[derive(Debug, Clone, PartialEq)]
    pub enum ResultValue {
        Null,
        Bool(bool),
        Int64(i64),
        Float64(f64),
        String(String),
        Bytes(Vec<u8>),
        Date(i32),
        Time(i64),
        DateTime(i64),
        Timestamp(i64),
        Interval(i64),
        Numeric(rust_decimal::Decimal),
        Array(Vec<ResultValue>),
        Struct(Vec<(String, ResultValue)>),
        Json(String),
    }

    impl ResultValue {
        pub fn as_str(&self) -> Option<&str> {
            match self {
                ResultValue::String(s) => Some(s),
                _ => None,
            }
        }

        pub fn as_i64(&self) -> Option<i64> {
            match self {
                ResultValue::Int64(v) => Some(*v),
                _ => None,
            }
        }

        pub fn as_f64(&self) -> Option<f64> {
            match self {
                ResultValue::Float64(v) => Some(*v),
                _ => None,
            }
        }

        pub fn as_bool(&self) -> Option<bool> {
            match self {
                ResultValue::Bool(v) => Some(*v),
                _ => None,
            }
        }

        pub fn as_date(&self) -> Option<i32> {
            match self {
                ResultValue::Date(v) => Some(*v),
                _ => None,
            }
        }

        pub fn as_time(&self) -> Option<i64> {
            match self {
                ResultValue::Time(v) => Some(*v),
                _ => None,
            }
        }

        pub fn as_datetime(&self) -> Option<i64> {
            match self {
                ResultValue::DateTime(v) => Some(*v),
                _ => None,
            }
        }

        pub fn as_timestamp(&self) -> Option<i64> {
            match self {
                ResultValue::Timestamp(v) => Some(*v),
                _ => None,
            }
        }

        pub fn as_interval(&self) -> Option<i64> {
            match self {
                ResultValue::Interval(v) => Some(*v),
                _ => None,
            }
        }

        pub fn as_array(&self) -> Option<&Vec<ResultValue>> {
            match self {
                ResultValue::Array(v) => Some(v),
                _ => None,
            }
        }

        pub fn as_struct(&self) -> Option<&[(String, ResultValue)]> {
            match self {
                ResultValue::Struct(v) => Some(v),
                _ => None,
            }
        }

        pub fn as_bytes(&self) -> Option<&[u8]> {
            match self {
                ResultValue::Bytes(v) => Some(v),
                _ => None,
            }
        }

        pub fn as_numeric(&self) -> Option<rust_decimal::Decimal> {
            match self {
                ResultValue::Numeric(v) => Some(*v),
                _ => None,
            }
        }

        pub fn as_json(&self) -> Option<&str> {
            match self {
                ResultValue::Json(s) => Some(s),
                _ => None,
            }
        }

        pub fn is_null(&self) -> bool {
            matches!(self, ResultValue::Null)
        }
    }

    fn extract_value(array: &ArrayRef, row: usize) -> ResultValue {
        use datafusion::arrow::array::*;
        use datafusion::arrow::datatypes::DataType;

        if array.is_null(row) {
            return ResultValue::Null;
        }

        match array.data_type() {
            DataType::Boolean => {
                let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                ResultValue::Bool(arr.value(row))
            }
            DataType::Int8 => {
                let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
                ResultValue::Int64(arr.value(row) as i64)
            }
            DataType::Int16 => {
                let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
                ResultValue::Int64(arr.value(row) as i64)
            }
            DataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                ResultValue::Int64(arr.value(row) as i64)
            }
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                ResultValue::Int64(arr.value(row))
            }
            DataType::UInt64 => {
                let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                ResultValue::Int64(arr.value(row) as i64)
            }
            DataType::Float32 => {
                let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
                ResultValue::Float64(arr.value(row) as f64)
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                ResultValue::Float64(arr.value(row))
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                ResultValue::String(arr.value(row).to_string())
            }
            DataType::LargeUtf8 => {
                let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                ResultValue::String(arr.value(row).to_string())
            }
            DataType::Binary => {
                let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                ResultValue::Bytes(arr.value(row).to_vec())
            }
            DataType::Date32 => {
                let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
                ResultValue::Date(arr.value(row))
            }
            DataType::Date64 => {
                let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
                ResultValue::Date((arr.value(row) / 86400000) as i32)
            }
            DataType::Time64(_) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Time64NanosecondArray>()
                    .unwrap();
                ResultValue::Time(arr.value(row))
            }
            DataType::Timestamp(_, tz) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                if tz.is_some() {
                    ResultValue::Timestamp(arr.value(row))
                } else {
                    ResultValue::DateTime(arr.value(row))
                }
            }
            DataType::Interval(_) => ResultValue::Interval(0),
            DataType::Decimal128(_, scale) => {
                let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
                let v = arr.value(row);
                let d = rust_decimal::Decimal::from_i128_with_scale(v, *scale as u32);
                ResultValue::Numeric(d)
            }
            DataType::List(_) => {
                let arr = array.as_any().downcast_ref::<ListArray>().unwrap();
                let inner = arr.value(row);
                let values: Vec<ResultValue> =
                    (0..inner.len()).map(|i| extract_value(&inner, i)).collect();
                ResultValue::Array(values)
            }
            DataType::Struct(fields) => {
                let arr = array.as_any().downcast_ref::<StructArray>().unwrap();
                let entries: Vec<(String, ResultValue)> = fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        let col = arr.column(i);
                        (f.name().clone(), extract_value(col, row))
                    })
                    .collect();
                ResultValue::Struct(entries)
            }
            _ => ResultValue::String(format!("<{:?}>", array.data_type())),
        }
    }

    pub struct ColumnRef<'a> {
        batches: &'a [RecordBatch],
        col_idx: usize,
    }

    impl<'a> ColumnRef<'a> {
        pub fn get_value(&self, row_idx: usize) -> ResultValue {
            let mut total_row = 0;
            for batch in self.batches {
                if row_idx < total_row + batch.num_rows() {
                    let local_idx = row_idx - total_row;
                    return extract_value(batch.column(self.col_idx), local_idx);
                }
                total_row += batch.num_rows();
            }
            panic!("Row index {} out of bounds", row_idx);
        }

        pub fn get(&self, row_idx: usize) -> Option<ResultValue> {
            let mut total_row = 0;
            for batch in self.batches {
                if row_idx < total_row + batch.num_rows() {
                    let local_idx = row_idx - total_row;
                    return Some(extract_value(batch.column(self.col_idx), local_idx));
                }
                total_row += batch.num_rows();
            }
            None
        }
    }

    #[derive(Debug, Clone)]
    pub struct Row(pub Vec<ResultValue>);

    impl Row {
        pub fn values(&self) -> &[ResultValue] {
            &self.0
        }

        pub fn get(&self, idx: usize) -> Option<&ResultValue> {
            self.0.get(idx)
        }

        pub fn len(&self) -> usize {
            self.0.len()
        }

        pub fn is_empty(&self) -> bool {
            self.0.is_empty()
        }
    }

    impl std::ops::Index<usize> for Row {
        type Output = ResultValue;
        fn index(&self, idx: usize) -> &Self::Output {
            &self.0[idx]
        }
    }

    pub trait RecordBatchVecExt {
        fn num_rows(&self) -> usize;
        fn row_count(&self) -> usize;
        fn get_row(&self, row_idx: usize) -> Option<Row>;
        fn to_records(&self) -> std::result::Result<Vec<Row>, ()>;
        fn column(&self, col_idx: usize) -> Option<ColumnRef<'_>>;
        fn schema(&self) -> Option<Arc<datafusion::arrow::datatypes::Schema>>;
    }

    impl RecordBatchVecExt for Vec<RecordBatch> {
        fn num_rows(&self) -> usize {
            self.iter().map(|b| b.num_rows()).sum()
        }

        fn row_count(&self) -> usize {
            self.num_rows()
        }

        fn get_row(&self, row_idx: usize) -> Option<Row> {
            let mut total_row = 0;
            for batch in self {
                if row_idx < total_row + batch.num_rows() {
                    let local_idx = row_idx - total_row;
                    return Some(Row((0..batch.num_columns())
                        .map(|col_idx| extract_value(batch.column(col_idx), local_idx))
                        .collect()));
                }
                total_row += batch.num_rows();
            }
            None
        }

        fn to_records(&self) -> std::result::Result<Vec<Row>, ()> {
            let mut rows = Vec::new();
            for batch in self {
                for row_idx in 0..batch.num_rows() {
                    let row = Row((0..batch.num_columns())
                        .map(|col_idx| extract_value(batch.column(col_idx), row_idx))
                        .collect());
                    rows.push(row);
                }
            }
            Ok(rows)
        }

        fn column(&self, col_idx: usize) -> Option<ColumnRef<'_>> {
            if self.is_empty() {
                return None;
            }
            if col_idx >= self[0].num_columns() {
                return None;
            }
            Some(ColumnRef {
                batches: self,
                col_idx,
            })
        }

        fn schema(&self) -> Option<Arc<datafusion::arrow::datatypes::Schema>> {
            self.first().map(|b| b.schema())
        }
    }
}

pub use result_ext::{ColumnRef, RecordBatchVecExt, ResultValue, Row};
