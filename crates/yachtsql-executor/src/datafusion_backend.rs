#![cfg(feature = "datafusion")]

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::DataType;
use yachtsql_storage::{Column, Field, Schema, Table};

pub struct DataFusionBackend {
    ctx: SessionContext,
}

impl DataFusionBackend {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    pub fn with_config(config: SessionConfig) -> Self {
        Self {
            ctx: SessionContext::new_with_config(config),
        }
    }

    pub fn register_table(&self, name: &str, table: &Table) -> Result<()> {
        let batch = table_to_record_batch(table)?;
        let schema = batch.schema();
        let mem_table =
            MemTable::try_new(schema, vec![vec![batch]]).map_err(|e| Error::internal(e.to_string()))?;
        self.ctx
            .register_table(name, Arc::new(mem_table))
            .map_err(|e| Error::internal(e.to_string()))?;
        Ok(())
    }

    pub async fn execute_sql(&self, sql: &str) -> Result<Table> {
        let df = self.ctx.sql(sql).await.map_err(|e| Error::internal(e.to_string()))?;
        let arrow_schema = df.schema().inner().clone();
        let batches = df.collect().await.map_err(|e| Error::internal(e.to_string()))?;

        if batches.is_empty() {
            let schema = arrow_schema_to_yachtsql(&arrow_schema);
            return Ok(Table::new(schema));
        }

        record_batches_to_table(&batches)
    }

    pub fn session_context(&self) -> &SessionContext {
        &self.ctx
    }
}

impl Default for DataFusionBackend {
    fn default() -> Self {
        Self::new()
    }
}

fn yachtsql_type_to_arrow(dt: &DataType) -> ArrowDataType {
    match dt {
        DataType::Bool => ArrowDataType::Boolean,
        DataType::Int64 => ArrowDataType::Int64,
        DataType::Float64 => ArrowDataType::Float64,
        DataType::Numeric(_) | DataType::BigNumeric => ArrowDataType::Float64,
        DataType::String => ArrowDataType::Utf8,
        DataType::Bytes => ArrowDataType::Binary,
        DataType::Date => ArrowDataType::Date32,
        DataType::Time => ArrowDataType::Time64(TimeUnit::Microsecond),
        DataType::DateTime => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp => ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::Json => ArrowDataType::Utf8,
        DataType::Geography => ArrowDataType::Utf8,
        DataType::Interval => ArrowDataType::Utf8,
        DataType::Array(inner) => {
            ArrowDataType::List(Arc::new(ArrowField::new(
                "item",
                yachtsql_type_to_arrow(inner),
                true,
            )))
        }
        DataType::Struct(fields) => {
            let arrow_fields: Vec<ArrowField> = fields
                .iter()
                .map(|sf| ArrowField::new(&sf.name, yachtsql_type_to_arrow(&sf.data_type), true))
                .collect();
            ArrowDataType::Struct(arrow_fields.into())
        }
        DataType::Range(_) => ArrowDataType::Utf8,
        DataType::Unknown => ArrowDataType::Utf8,
    }
}

fn arrow_type_to_yachtsql(dt: &ArrowDataType) -> DataType {
    match dt {
        ArrowDataType::Boolean => DataType::Bool,
        ArrowDataType::Int8 | ArrowDataType::Int16 | ArrowDataType::Int32 | ArrowDataType::Int64 => {
            DataType::Int64
        }
        ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64 => DataType::Int64,
        ArrowDataType::Float16 | ArrowDataType::Float32 | ArrowDataType::Float64 => {
            DataType::Float64
        }
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => DataType::String,
        ArrowDataType::Binary | ArrowDataType::LargeBinary => DataType::Bytes,
        ArrowDataType::Date32 | ArrowDataType::Date64 => DataType::Date,
        ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => DataType::Time,
        ArrowDataType::Timestamp(_, None) => DataType::DateTime,
        ArrowDataType::Timestamp(_, Some(_)) => DataType::Timestamp,
        ArrowDataType::Decimal128(_, _) | ArrowDataType::Decimal256(_, _) => DataType::Numeric(None),
        ArrowDataType::List(field) => {
            DataType::Array(Box::new(arrow_type_to_yachtsql(field.data_type())))
        }
        ArrowDataType::Struct(fields) => {
            let yachtsql_fields: Vec<yachtsql_common::types::StructField> = fields
                .iter()
                .map(|f| yachtsql_common::types::StructField {
                    name: f.name().clone(),
                    data_type: arrow_type_to_yachtsql(f.data_type()),
                })
                .collect();
            DataType::Struct(yachtsql_fields)
        }
        _ => DataType::String,
    }
}

fn schema_to_arrow(schema: &Schema) -> ArrowSchema {
    let fields: Vec<ArrowField> = schema
        .fields()
        .iter()
        .map(|f| {
            ArrowField::new(
                &f.name,
                yachtsql_type_to_arrow(&f.data_type),
                f.mode == yachtsql_storage::FieldMode::Nullable,
            )
        })
        .collect();
    ArrowSchema::new(fields)
}

fn arrow_schema_to_yachtsql(schema: &ArrowSchema) -> Schema {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            let mode = if f.is_nullable() {
                yachtsql_storage::FieldMode::Nullable
            } else {
                yachtsql_storage::FieldMode::Required
            };
            Field::new(f.name(), arrow_type_to_yachtsql(f.data_type()), mode)
        })
        .collect();
    Schema::from_fields(fields)
}

fn column_to_arrow_array(column: &Column) -> Result<ArrayRef> {
    match column {
        Column::Bool { data, nulls } => {
            let values: Vec<Option<bool>> = data
                .iter()
                .enumerate()
                .map(|(i, v)| if nulls.is_null(i) { None } else { Some(*v) })
                .collect();
            Ok(Arc::new(BooleanArray::from(values)))
        }
        Column::Int64 { data, nulls } => {
            let values: Vec<Option<i64>> = data
                .iter()
                .enumerate()
                .map(|(i, v)| if nulls.is_null(i) { None } else { Some(*v) })
                .collect();
            Ok(Arc::new(Int64Array::from(values)))
        }
        Column::Float64 { data, nulls } => {
            let values: Vec<Option<f64>> = data
                .iter()
                .enumerate()
                .map(|(i, v)| if nulls.is_null(i) { None } else { Some(*v) })
                .collect();
            Ok(Arc::new(Float64Array::from(values)))
        }
        Column::String { data, nulls } => {
            let values: Vec<Option<&str>> = data
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    if nulls.is_null(i) {
                        None
                    } else {
                        Some(v.as_str())
                    }
                })
                .collect();
            Ok(Arc::new(StringArray::from(values)))
        }
        Column::Date { data, nulls } => {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let values: Vec<Option<i32>> = data
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    if nulls.is_null(i) {
                        None
                    } else {
                        Some((*v - epoch).num_days() as i32)
                    }
                })
                .collect();
            Ok(Arc::new(Date32Array::from(values)))
        }
        Column::DateTime { data, nulls } => {
            let values: Vec<Option<i64>> = data
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    if nulls.is_null(i) {
                        None
                    } else {
                        Some(v.and_utc().timestamp_micros())
                    }
                })
                .collect();
            Ok(Arc::new(TimestampMicrosecondArray::from(values)))
        }
        Column::Timestamp { data, nulls } => {
            let values: Vec<Option<i64>> = data
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    if nulls.is_null(i) {
                        None
                    } else {
                        Some(v.timestamp_micros())
                    }
                })
                .collect();
            Ok(Arc::new(
                TimestampMicrosecondArray::from(values).with_timezone("UTC"),
            ))
        }
        Column::Numeric { data, nulls } => {
            let values: Vec<Option<f64>> = data
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    if nulls.is_null(i) {
                        None
                    } else {
                        use rust_decimal::prelude::ToPrimitive;
                        v.to_f64()
                    }
                })
                .collect();
            Ok(Arc::new(Float64Array::from(values)))
        }
        Column::Json { data, nulls } => {
            let values: Vec<Option<String>> = data
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    if nulls.is_null(i) {
                        None
                    } else {
                        Some(v.to_string())
                    }
                })
                .collect();
            Ok(Arc::new(StringArray::from(values)))
        }
        _ => Err(Error::internal(format!(
            "Unsupported column type for Arrow conversion: {:?}",
            column.data_type()
        ))),
    }
}

pub fn table_to_record_batch(table: &Table) -> Result<RecordBatch> {
    let arrow_schema = Arc::new(schema_to_arrow(table.schema()));

    if table.row_count() == 0 {
        return Ok(RecordBatch::new_empty(arrow_schema));
    }

    let arrays: Vec<ArrayRef> = table
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let column = table.column_by_name(&field.name).ok_or_else(|| {
                Error::internal(format!("Column {} not found", field.name))
            })?;
            column_to_arrow_array(column)
        })
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(arrow_schema, arrays).map_err(|e| Error::internal(e.to_string()))
}

fn arrow_array_to_column(
    array: &ArrayRef,
    data_type: &DataType,
    _name: &str,
) -> Result<Column> {
    use aligned_vec::AVec;
    use datafusion::arrow::array::Array;
    use yachtsql_storage::NullBitmap;

    let len = array.len();
    let null_count = array.null_count();

    let nulls = if null_count == 0 {
        NullBitmap::new_valid(len)
    } else {
        let mut bitmap = NullBitmap::new_valid(len);
        for i in 0..len {
            if array.is_null(i) {
                bitmap.set_null(i);
            }
        }
        bitmap
    };

    match data_type {
        DataType::Bool => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| Error::internal("Expected BooleanArray"))?;
            let data: Vec<bool> = (0..len).map(|i| arr.value(i)).collect();
            Ok(Column::Bool { data, nulls })
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| Error::internal("Expected Int64Array"))?;
            let data = AVec::from_iter(64, arr.values().iter().copied());
            Ok(Column::Int64 { data, nulls })
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| Error::internal("Expected Float64Array"))?;
            let data = AVec::from_iter(64, arr.values().iter().copied());
            Ok(Column::Float64 { data, nulls })
        }
        DataType::String => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::internal("Expected StringArray"))?;
            let data: Vec<String> = (0..len).map(|i| arr.value(i).to_string()).collect();
            Ok(Column::String { data, nulls })
        }
        _ => Err(Error::internal(format!(
            "Unsupported data type for conversion: {:?}",
            data_type
        ))),
    }
}

pub fn record_batches_to_table(batches: &[RecordBatch]) -> Result<Table> {
    if batches.is_empty() {
        return Ok(Table::new(Schema::new()));
    }

    let first_batch = &batches[0];
    let arrow_schema = first_batch.schema();
    let schema = arrow_schema_to_yachtsql(&arrow_schema);

    let mut columns_map = indexmap::IndexMap::new();

    for (field_idx, field) in schema.fields().iter().enumerate() {
        let mut all_arrays: Vec<&ArrayRef> = Vec::new();
        for batch in batches {
            all_arrays.push(batch.column(field_idx));
        }

        let col = if all_arrays.len() == 1 {
            arrow_array_to_column(all_arrays[0], &field.data_type, &field.name)?
        } else {
            use datafusion::arrow::compute::concat;
            let refs: Vec<&dyn datafusion::arrow::array::Array> =
                all_arrays.iter().map(|a| a.as_ref()).collect();
            let concatenated = concat(&refs).map_err(|e| Error::internal(e.to_string()))?;
            arrow_array_to_column(&concatenated, &field.data_type, &field.name)?
        };
        columns_map.insert(field.name.clone(), col);
    }

    Ok(Table::from_columns(schema, columns_map))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aligned_vec::AVec;
    use yachtsql_storage::{FieldMode, NullBitmap};

    #[tokio::test]
    async fn test_datafusion_backend_simple_query() {
        let backend = DataFusionBackend::new();

        let schema = Schema::from_fields(vec![
            Field::new("id", DataType::Int64, FieldMode::Required),
            Field::new("name", DataType::String, FieldMode::Nullable),
            Field::new("score", DataType::Int64, FieldMode::Nullable),
        ]);

        let mut columns = indexmap::IndexMap::new();
        columns.insert(
            "id".to_string(),
            Column::Int64 {
                data: AVec::from_iter(64, [1i64, 2, 3].into_iter()),
                nulls: NullBitmap::new_valid(3),
            },
        );
        columns.insert(
            "name".to_string(),
            Column::String {
                data: vec!["Alice".to_string(), "Bob".to_string(), "Charlie".to_string()],
                nulls: NullBitmap::new_valid(3),
            },
        );
        columns.insert(
            "score".to_string(),
            Column::Int64 {
                data: AVec::from_iter(64, [85i64, 90, 78].into_iter()),
                nulls: NullBitmap::new_valid(3),
            },
        );
        let table = Table::from_columns(schema, columns);

        backend.register_table("users", &table).unwrap();

        let result = backend
            .execute_sql("SELECT name, score FROM users WHERE score > 80 ORDER BY score DESC")
            .await
            .unwrap();

        assert_eq!(result.row_count(), 2);
    }

    #[tokio::test]
    async fn test_datafusion_backend_aggregation() {
        let backend = DataFusionBackend::new();

        let schema = Schema::from_fields(vec![
            Field::new("country", DataType::String, FieldMode::Required),
            Field::new("amount", DataType::Int64, FieldMode::Required),
        ]);

        let mut columns = indexmap::IndexMap::new();
        columns.insert(
            "country".to_string(),
            Column::String {
                data: vec![
                    "US".to_string(),
                    "UK".to_string(),
                    "US".to_string(),
                    "UK".to_string(),
                ],
                nulls: NullBitmap::new_valid(4),
            },
        );
        columns.insert(
            "amount".to_string(),
            Column::Int64 {
                data: AVec::from_iter(64, [100i64, 200, 150, 250].into_iter()),
                nulls: NullBitmap::new_valid(4),
            },
        );
        let table = Table::from_columns(schema, columns);

        backend.register_table("sales", &table).unwrap();

        let result = backend
            .execute_sql("SELECT country, SUM(amount) as total FROM sales GROUP BY country ORDER BY total DESC")
            .await
            .unwrap();

        assert_eq!(result.row_count(), 2);
    }
}
