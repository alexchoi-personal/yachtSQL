#![allow(clippy::approx_constant)]
#![allow(clippy::wildcard_enum_match_arm)]
#![allow(clippy::result_unit_err)]
#![allow(dead_code)]
#![allow(unused_imports)]

pub mod common {
    use std::sync::Arc;

    use yachtsql::RecordBatch;
    use yachtsql::arrow::array::*;
    use yachtsql::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    pub use yachtsql_arrow::{TestValue, assert_batch_records_eq};

    #[derive(Debug, Clone, PartialEq)]
    pub enum Value {
        Null,
        Bool(bool),
        Int64(i64),
        Float64(f64),
        String(String),
        Bytes(Vec<u8>),
        Numeric(rust_decimal::Decimal),
        BigNumeric(rust_decimal::Decimal),
        Date(i32),
        Time(i64),
        DateTime(i64),
        Timestamp(i64),
        Array(Vec<Value>),
        Struct(Vec<(String, Value)>),
    }

    impl Value {
        pub fn null() -> Self {
            Value::Null
        }

        pub fn int64(v: i64) -> Self {
            Value::Int64(v)
        }

        pub fn float64(v: f64) -> Self {
            Value::Float64(v)
        }

        pub fn string(v: String) -> Self {
            Value::String(v)
        }

        pub fn bool_val(v: bool) -> Self {
            Value::Bool(v)
        }

        pub fn bytes(v: Vec<u8>) -> Self {
            Value::Bytes(v)
        }

        pub fn numeric(v: rust_decimal::Decimal) -> Self {
            Value::Numeric(v)
        }

        pub fn array(v: Vec<Value>) -> Self {
            Value::Array(v)
        }

        pub fn struct_val(fields: Vec<(String, Value)>) -> Self {
            Value::Struct(fields)
        }

        pub fn date(d: chrono::NaiveDate) -> Self {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let days = (d - epoch).num_days() as i32;
            Value::Date(days)
        }

        pub fn time(t: chrono::NaiveTime) -> Self {
            use chrono::Timelike;
            let nanos =
                t.num_seconds_from_midnight() as i64 * 1_000_000_000 + t.nanosecond() as i64;
            Value::Time(nanos)
        }

        pub fn datetime(dt: chrono::NaiveDateTime) -> Self {
            Value::DateTime(dt.and_utc().timestamp_nanos_opt().unwrap_or(0))
        }

        pub fn timestamp(dt: chrono::DateTime<chrono::Utc>) -> Self {
            Value::Timestamp(dt.timestamp_nanos_opt().unwrap_or(0))
        }

        pub fn is_null(&self) -> bool {
            matches!(self, Value::Null)
        }

        pub fn data_type(&self) -> yachtsql::DataType {
            match self {
                Value::Null => yachtsql::DataType::Unknown,
                Value::Bool(_) => yachtsql::DataType::Bool,
                Value::Int64(_) => yachtsql::DataType::Int64,
                Value::Float64(_) => yachtsql::DataType::Float64,
                Value::String(_) => yachtsql::DataType::String,
                Value::Bytes(_) => yachtsql::DataType::Bytes,
                Value::Numeric(_) => yachtsql::DataType::Numeric(None),
                Value::BigNumeric(_) => yachtsql::DataType::BigNumeric,
                Value::Date(_) => yachtsql::DataType::Date,
                Value::Time(_) => yachtsql::DataType::Time,
                Value::DateTime(_) => yachtsql::DataType::DateTime,
                Value::Timestamp(_) => yachtsql::DataType::Timestamp,
                Value::Array(_) => yachtsql::DataType::Array(Box::new(yachtsql::DataType::Unknown)),
                Value::Struct(_) => yachtsql::DataType::Struct(vec![]),
            }
        }
    }

    pub trait IntoValue {
        fn into_value(self) -> Value;
    }

    impl IntoValue for Value {
        fn into_value(self) -> Value {
            self
        }
    }

    impl IntoValue for i64 {
        fn into_value(self) -> Value {
            Value::int64(self)
        }
    }

    impl IntoValue for i32 {
        fn into_value(self) -> Value {
            Value::int64(self as i64)
        }
    }

    impl IntoValue for u64 {
        fn into_value(self) -> Value {
            Value::int64(self as i64)
        }
    }

    impl IntoValue for f64 {
        fn into_value(self) -> Value {
            Value::float64(self)
        }
    }

    impl IntoValue for &str {
        fn into_value(self) -> Value {
            Value::string(self.to_string())
        }
    }

    impl IntoValue for bool {
        fn into_value(self) -> Value {
            Value::bool_val(self)
        }
    }

    impl<const N: usize> IntoValue for &[u8; N] {
        fn into_value(self) -> Value {
            Value::bytes(self.to_vec())
        }
    }

    fn extract_value_from_array(array: &ArrayRef, row: usize) -> Value {
        if array.is_null(row) {
            return Value::Null;
        }

        match array.data_type() {
            ArrowDataType::Boolean => {
                let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                Value::Bool(arr.value(row))
            }
            ArrowDataType::Int8 => {
                let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
                Value::Int64(arr.value(row) as i64)
            }
            ArrowDataType::Int16 => {
                let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
                Value::Int64(arr.value(row) as i64)
            }
            ArrowDataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                Value::Int64(arr.value(row) as i64)
            }
            ArrowDataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Value::Int64(arr.value(row))
            }
            ArrowDataType::UInt8 => {
                let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                Value::Int64(arr.value(row) as i64)
            }
            ArrowDataType::UInt16 => {
                let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                Value::Int64(arr.value(row) as i64)
            }
            ArrowDataType::UInt32 => {
                let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                Value::Int64(arr.value(row) as i64)
            }
            ArrowDataType::UInt64 => {
                let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                Value::Int64(arr.value(row) as i64)
            }
            ArrowDataType::Float32 => {
                let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
                Value::Float64(arr.value(row) as f64)
            }
            ArrowDataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Value::Float64(arr.value(row))
            }
            ArrowDataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                Value::String(arr.value(row).to_string())
            }
            ArrowDataType::LargeUtf8 => {
                let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                Value::String(arr.value(row).to_string())
            }
            ArrowDataType::Binary => {
                let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                Value::Bytes(arr.value(row).to_vec())
            }
            ArrowDataType::Date32 => {
                let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
                Value::Date(arr.value(row))
            }
            ArrowDataType::Date64 => {
                let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
                Value::Date((arr.value(row) / 86400000) as i32)
            }
            ArrowDataType::Time64(_) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Time64NanosecondArray>()
                    .unwrap();
                Value::Time(arr.value(row))
            }
            ArrowDataType::Timestamp(_, tz) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                if tz.is_some() {
                    Value::Timestamp(arr.value(row))
                } else {
                    Value::DateTime(arr.value(row))
                }
            }
            ArrowDataType::Decimal128(_, scale) => {
                let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
                let v = arr.value(row);
                let d = rust_decimal::Decimal::from_i128_with_scale(v, *scale as u32);
                Value::Numeric(d)
            }
            ArrowDataType::List(_) => {
                let arr = array.as_any().downcast_ref::<ListArray>().unwrap();
                let inner = arr.value(row);
                let values: Vec<Value> = (0..inner.len())
                    .map(|i| extract_value_from_array(&inner, i))
                    .collect();
                Value::Array(values)
            }
            ArrowDataType::Struct(fields) => {
                let arr = array.as_any().downcast_ref::<StructArray>().unwrap();
                let values: Vec<(String, Value)> = fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        let col = arr.column(i);
                        (f.name().clone(), extract_value_from_array(col, row))
                    })
                    .collect();
                Value::Struct(values)
            }
            _ => Value::String(format!("<unsupported: {:?}>", array.data_type())),
        }
    }

    pub fn values_equal(actual: &Value, expected: &Value, epsilon: f64) -> bool {
        match (actual, expected) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(e)) => a == e,
            (Value::Int64(a), Value::Int64(e)) => a == e,
            (Value::Float64(a), Value::Float64(e)) => {
                if a.is_nan() && e.is_nan() {
                    return true;
                }
                if a.is_infinite() && e.is_infinite() && a.signum() == e.signum() {
                    return true;
                }
                (a - e).abs() < epsilon || (a - e).abs() / e.abs().max(1.0) < epsilon
            }
            (Value::String(a), Value::String(e)) => a == e,
            (Value::Bytes(a), Value::Bytes(e)) => a == e,
            (Value::Numeric(a), Value::Numeric(e)) => a == e,
            (Value::BigNumeric(a), Value::BigNumeric(e)) => a == e,
            (Value::Date(a), Value::Date(e)) => a == e,
            (Value::Time(a), Value::Time(e)) => a == e,
            (Value::DateTime(a), Value::DateTime(e)) => a == e,
            (Value::Timestamp(a), Value::Timestamp(e)) => a == e,
            (Value::Array(a), Value::Array(e)) => {
                a.len() == e.len()
                    && a.iter()
                        .zip(e.iter())
                        .all(|(av, ev)| values_equal(av, ev, epsilon))
            }
            (Value::Struct(a), Value::Struct(e)) => {
                a.len() == e.len()
                    && a.iter()
                        .zip(e.iter())
                        .all(|((an, av), (en, ev))| an == en && values_equal(av, ev, epsilon))
            }
            (Value::Int64(a), Value::Float64(e)) | (Value::Float64(e), Value::Int64(a)) => {
                (*a as f64 - e).abs() < epsilon
            }
            _ => false,
        }
    }

    pub fn batches_to_values(batches: &[RecordBatch]) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let mut row = Vec::new();
                for col_idx in 0..batch.num_columns() {
                    let array = batch.column(col_idx);
                    row.push(extract_value_from_array(array, row_idx));
                }
                rows.push(row);
            }
        }
        rows
    }

    pub fn table(rows: Vec<Vec<Value>>) -> Vec<RecordBatch> {
        if rows.is_empty() {
            return vec![];
        }

        let num_cols = rows[0].len();
        let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();

        for col_idx in 0..num_cols {
            let data_type = rows
                .iter()
                .map(|row| row.get(col_idx).map(|v| v.data_type()))
                .find(|dt| {
                    dt.as_ref()
                        .map(|t| *t != yachtsql::DataType::Unknown)
                        .unwrap_or(false)
                })
                .flatten()
                .unwrap_or(yachtsql::DataType::String);

            let builder: Box<dyn ArrayBuilder> = match data_type {
                yachtsql::DataType::Bool => Box::new(BooleanBuilder::new()),
                yachtsql::DataType::Int64 => Box::new(Int64Builder::new()),
                yachtsql::DataType::Float64 => Box::new(Float64Builder::new()),
                yachtsql::DataType::String => Box::new(StringBuilder::new()),
                yachtsql::DataType::Bytes => Box::new(BinaryBuilder::new()),
                yachtsql::DataType::Numeric(_) => Box::new(
                    Decimal128Builder::new()
                        .with_precision_and_scale(38, 9)
                        .unwrap(),
                ),
                yachtsql::DataType::Date => Box::new(Date32Builder::new()),
                yachtsql::DataType::Time => Box::new(Time64NanosecondBuilder::new()),
                yachtsql::DataType::DateTime => Box::new(TimestampNanosecondBuilder::new()),
                yachtsql::DataType::Timestamp => {
                    Box::new(TimestampNanosecondBuilder::new().with_timezone("UTC"))
                }
                _ => Box::new(StringBuilder::new()),
            };
            builders.push(builder);
        }

        for row in &rows {
            for (col_idx, value) in row.iter().enumerate() {
                append_value_to_builder(&mut builders[col_idx], value);
            }
        }

        let arrays: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();

        let fields: Vec<Field> = arrays
            .iter()
            .enumerate()
            .map(|(i, arr)| Field::new(format!("col{}", i), arr.data_type().clone(), true))
            .collect();

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays).unwrap();
        vec![batch]
    }

    fn append_value_to_builder(builder: &mut Box<dyn ArrayBuilder>, value: &Value) {
        match value {
            Value::Null => {
                if let Some(b) = builder.as_any_mut().downcast_mut::<BooleanBuilder>() {
                    b.append_null();
                } else if let Some(b) = builder.as_any_mut().downcast_mut::<Int64Builder>() {
                    b.append_null();
                } else if let Some(b) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
                    b.append_null();
                } else if let Some(b) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
                    b.append_null();
                } else if let Some(b) = builder.as_any_mut().downcast_mut::<BinaryBuilder>() {
                    b.append_null();
                } else if let Some(b) = builder.as_any_mut().downcast_mut::<Decimal128Builder>() {
                    b.append_null();
                } else if let Some(b) = builder.as_any_mut().downcast_mut::<Date32Builder>() {
                    b.append_null();
                } else if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<Time64NanosecondBuilder>()
                {
                    b.append_null();
                } else if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampNanosecondBuilder>()
                {
                    b.append_null();
                }
            }
            Value::Bool(v) => {
                if let Some(b) = builder.as_any_mut().downcast_mut::<BooleanBuilder>() {
                    b.append_value(*v);
                }
            }
            Value::Int64(v) => {
                if let Some(b) = builder.as_any_mut().downcast_mut::<Int64Builder>() {
                    b.append_value(*v);
                } else if let Some(b) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
                    b.append_value(*v as f64);
                }
            }
            Value::Float64(v) => {
                if let Some(b) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
                    b.append_value(*v);
                }
            }
            Value::String(v) => {
                if let Some(b) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
                    b.append_value(v);
                }
            }
            Value::Bytes(v) => {
                if let Some(b) = builder.as_any_mut().downcast_mut::<BinaryBuilder>() {
                    b.append_value(v);
                }
            }
            Value::Numeric(v) => {
                if let Some(b) = builder.as_any_mut().downcast_mut::<Decimal128Builder>() {
                    b.append_value(v.mantissa());
                }
            }
            Value::BigNumeric(v) => {
                if let Some(b) = builder.as_any_mut().downcast_mut::<Decimal128Builder>() {
                    b.append_value(v.mantissa());
                }
            }
            Value::Date(v) => {
                if let Some(b) = builder.as_any_mut().downcast_mut::<Date32Builder>() {
                    b.append_value(*v);
                }
            }
            Value::Time(v) => {
                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<Time64NanosecondBuilder>()
                {
                    b.append_value(*v);
                }
            }
            Value::DateTime(v) => {
                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampNanosecondBuilder>()
                {
                    b.append_value(*v);
                }
            }
            Value::Timestamp(v) => {
                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampNanosecondBuilder>()
                {
                    b.append_value(*v);
                }
            }
            Value::Array(_) | Value::Struct(_) => {
                if let Some(b) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
                    b.append_value(format!("{:?}", value));
                }
            }
        }
    }

    pub fn str(val: &str) -> Value {
        Value::string(val.to_string())
    }

    pub fn bool(val: bool) -> Value {
        Value::bool_val(val)
    }

    pub fn numeric(val: &str) -> Value {
        use std::str::FromStr;
        Value::numeric(rust_decimal::Decimal::from_str(val).unwrap())
    }

    pub fn i64(val: i64) -> Value {
        Value::int64(val)
    }

    pub fn f64(val: f64) -> Value {
        Value::float64(val)
    }

    pub fn array(vals: Vec<Value>) -> Value {
        Value::array(vals)
    }

    pub fn st(fields: Vec<(&str, Value)>) -> Value {
        Value::struct_val(
            fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        )
    }

    pub fn stv(vals: Vec<Value>) -> Value {
        Value::struct_val(
            vals.into_iter()
                .enumerate()
                .map(|(i, v)| (format!("_field{}", i), v))
                .collect(),
        )
    }

    pub fn stv_numeric(vals: Vec<Value>) -> Value {
        Value::struct_val(
            vals.into_iter()
                .enumerate()
                .map(|(i, v)| {
                    let coerced = match v {
                        Value::Float64(f) => {
                            let d = rust_decimal::Decimal::from_f64_retain(f)
                                .unwrap_or(rust_decimal::Decimal::ZERO);
                            Value::numeric(d)
                        }
                        other => other,
                    };
                    (format!("_field{}", i), coerced)
                })
                .collect(),
        )
    }

    pub fn tuple(vals: Vec<Value>) -> Value {
        Value::struct_val(
            vals.into_iter()
                .enumerate()
                .map(|(i, v)| ((i + 1).to_string(), v))
                .collect(),
        )
    }

    pub fn null() -> Value {
        Value::null()
    }

    pub fn date(year: i32, month: u32, day: u32) -> Value {
        Value::date(chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap())
    }

    pub fn ip(addr: &str) -> Value {
        let octets: Vec<u8> = addr.split('.').map(|s| s.parse().unwrap()).collect();
        Value::bytes(octets)
    }

    pub fn timestamp(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> Value {
        use chrono::{TimeZone, Utc};
        let dt = Utc
            .with_ymd_and_hms(year, month, day, hour, min, sec)
            .unwrap();
        Value::timestamp(dt)
    }

    pub fn d(year: i32, month: u32, day: u32) -> Value {
        date(year, month, day)
    }

    pub fn ts(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> Value {
        timestamp(year, month, day, hour, min, sec)
    }

    pub fn ts_ms(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        min: u32,
        sec: u32,
        millis: u32,
    ) -> Value {
        use chrono::{TimeZone, Timelike, Utc};
        let dt = Utc
            .with_ymd_and_hms(year, month, day, hour, min, sec)
            .unwrap()
            .with_nanosecond(millis * 1_000_000)
            .unwrap();
        Value::timestamp(dt)
    }

    pub fn n(val: &str) -> Value {
        numeric(val)
    }

    pub fn bignumeric(val: &str) -> Value {
        use std::str::FromStr;
        Value::BigNumeric(rust_decimal::Decimal::from_str(val).unwrap())
    }

    pub fn bn(val: &str) -> Value {
        bignumeric(val)
    }

    pub fn dt(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> Value {
        datetime(year, month, day, hour, min, sec)
    }

    pub fn datetime(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> Value {
        use chrono::NaiveDate;
        let dt = NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(hour, min, sec)
            .unwrap();
        Value::datetime(dt)
    }

    pub fn time(hour: u32, min: u32, sec: u32) -> Value {
        use chrono::NaiveTime;
        let t = NaiveTime::from_hms_opt(hour, min, sec).unwrap();
        Value::time(t)
    }

    pub fn tm(hour: u32, min: u32, sec: u32) -> Value {
        time(hour, min, sec)
    }

    pub fn bytes(val: &[u8]) -> Value {
        Value::bytes(val.to_vec())
    }

    pub fn get_single_value(batches: &[RecordBatch]) -> Value {
        assert!(!batches.is_empty(), "Expected at least one batch");
        let batch = &batches[0];
        assert!(batch.num_rows() >= 1, "Expected at least one row");
        assert!(batch.num_columns() >= 1, "Expected at least one column");
        extract_value_from_array(batch.column(0), 0)
    }

    pub fn num_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    pub fn row_count(batches: &[RecordBatch]) -> usize {
        num_rows(batches)
    }

    pub fn get_row(batches: &[RecordBatch], row_idx: usize) -> Vec<Value> {
        let mut total_row = 0;
        for batch in batches {
            if row_idx < total_row + batch.num_rows() {
                let local_idx = row_idx - total_row;
                return (0..batch.num_columns())
                    .map(|col_idx| extract_value_from_array(batch.column(col_idx), local_idx))
                    .collect();
            }
            total_row += batch.num_rows();
        }
        panic!(
            "Row index {} out of bounds (total rows: {})",
            row_idx, total_row
        );
    }

    pub fn column_values(batches: &[RecordBatch], col_idx: usize) -> Vec<Value> {
        let mut values = Vec::new();
        for batch in batches {
            assert!(
                col_idx < batch.num_columns(),
                "Column index {} out of bounds",
                col_idx
            );
            let array = batch.column(col_idx);
            for row_idx in 0..batch.num_rows() {
                values.push(extract_value_from_array(array, row_idx));
            }
        }
        values
    }

    pub fn schema_field_names(batches: &[RecordBatch]) -> Vec<String> {
        if batches.is_empty() {
            return vec![];
        }
        batches[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    pub trait RecordBatchExt {
        fn num_rows(&self) -> usize;
        fn row_count(&self) -> usize;
        fn get_row(&self, row_idx: usize) -> Vec<Value>;
        fn to_records(&self) -> Result<Vec<Vec<Value>>, ()>;
        fn column(&self, col_idx: usize) -> Option<ColumnView<'_>>;
        fn schema(&self) -> Option<std::sync::Arc<Schema>>;
    }

    pub struct ColumnView<'a> {
        batches: &'a [RecordBatch],
        col_idx: usize,
    }

    impl<'a> ColumnView<'a> {
        pub fn get_value(&self, row_idx: usize) -> Value {
            let mut total_row = 0;
            for batch in self.batches {
                if row_idx < total_row + batch.num_rows() {
                    let local_idx = row_idx - total_row;
                    return extract_value_from_array(batch.column(self.col_idx), local_idx);
                }
                total_row += batch.num_rows();
            }
            panic!("Row index {} out of bounds", row_idx);
        }
    }

    impl RecordBatchExt for Vec<RecordBatch> {
        fn num_rows(&self) -> usize {
            self.iter().map(|b| b.num_rows()).sum()
        }

        fn row_count(&self) -> usize {
            self.num_rows()
        }

        fn get_row(&self, row_idx: usize) -> Vec<Value> {
            get_row(self, row_idx)
        }

        fn to_records(&self) -> Result<Vec<Vec<Value>>, ()> {
            Ok(batches_to_values(self))
        }

        fn column(&self, col_idx: usize) -> Option<ColumnView<'_>> {
            if self.is_empty() {
                return None;
            }
            if col_idx >= self[0].num_columns() {
                return None;
            }
            Some(ColumnView {
                batches: self,
                col_idx,
            })
        }

        fn schema(&self) -> Option<std::sync::Arc<Schema>> {
            self.first().map(|b| b.schema())
        }
    }
}

#[macro_export]
macro_rules! val {
    ([]) => { $crate::common::array(vec![]) };
    ([$($elem:tt)*]) => {
        $crate::common::array($crate::vals![$($elem)*])
    };
    ({}) => { $crate::common::stv(vec![]) };
    ({{ $($val:tt)* }}) => {
        $crate::common::stv_numeric($crate::vals![$($val)*])
    };
    ({ $($val:tt)* }) => {
        $crate::common::stv($crate::vals![$($val)*])
    };
    (( $($val:tt),+ $(,)? )) => {
        $crate::common::tuple($crate::vals![$($val),+])
    };
    (null) => { $crate::common::null() };
    (true) => { $crate::common::IntoValue::into_value(true) };
    (false) => { $crate::common::IntoValue::into_value(false) };
    ($($e:tt)+) => { $crate::common::IntoValue::into_value($($e)+) };
}

#[macro_export]
macro_rules! vals {
    () => { vec![] };
    ($e:tt $(,)?) => { vec![$crate::val!($e)] };
    ($id:ident $args:tt $(, $($rest:tt)*)?) => {
        {
            let mut v = vec![$crate::val!($id $args)];
            v.extend($crate::vals![$($($rest)*)?]);
            v
        }
    };
    (- $e:tt $(, $($rest:tt)*)?) => {
        {
            let mut v = vec![$crate::val!(- $e)];
            v.extend($crate::vals![$($($rest)*)?]);
            v
        }
    };
    ($e:tt $(, $($rest:tt)*)?) => {
        {
            let mut v = vec![$crate::val!($e)];
            v.extend($crate::vals![$($($rest)*)?]);
            v
        }
    };
}

#[macro_export]
macro_rules! table {
    [$([$($val:tt)*]),* $(,)?] => {
        $crate::common::table(vec![$($crate::vals![$($val)*]),*])
    };
}

#[macro_export]
macro_rules! assert_table_eq {
    ($actual:expr, []) => {{
        let batches = &$actual;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0, "Expected empty table but got {} rows", total_rows);
    }};
    ($actual:expr, [$([$($val:tt)*]),* $(,)?]) => {{
        let batches = &$actual;
        let expected_rows: Vec<Vec<$crate::common::Value>> = vec![$($crate::vals![$($val)*]),*];
        let actual_rows = $crate::common::batches_to_values(batches);

        assert_eq!(
            actual_rows.len(),
            expected_rows.len(),
            "Row count mismatch: expected {}, got {}",
            expected_rows.len(),
            actual_rows.len()
        );

        for (row_idx, (actual_row, expected_row)) in actual_rows.iter().zip(expected_rows.iter()).enumerate() {
            assert_eq!(
                actual_row.len(),
                expected_row.len(),
                "Column count mismatch at row {}: expected {}, got {}",
                row_idx,
                expected_row.len(),
                actual_row.len()
            );

            for (col_idx, (actual_val, expected_val)) in actual_row.iter().zip(expected_row.iter()).enumerate() {
                assert!(
                    $crate::common::values_equal(actual_val, expected_val, 1e-9),
                    "Value mismatch at row {} col {}: expected {:?}, got {:?}",
                    row_idx,
                    col_idx,
                    expected_val,
                    actual_val
                );
            }
        }
    }};
}

pub use common::*;
