#![coverage(off)]

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;

use crate::{Column, Schema};

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Record {
    values: Vec<Value>,
}

impl Record {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
        }
    }

    pub fn from_values(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn from_slice(values: &[Value]) -> Self {
        Self {
            values: values.to_vec(),
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn into_values(self) -> Vec<Value> {
        self.values
    }

    pub fn push(&mut self, value: Value) {
        self.values.push(value);
    }

    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut Value> {
        self.values.get_mut(index)
    }

    pub fn get_by_name<'a>(&'a self, schema: &'a Schema, column: &str) -> Option<&'a Value> {
        schema
            .field_index(column)
            .and_then(|idx| self.values.get(idx))
    }

    pub fn remove(&mut self, index: usize) {
        if index < self.values.len() {
            self.values.remove(index);
        }
    }

    pub fn from_columns(columns: &[Column], row_index: usize) -> Self {
        let mut values = Vec::with_capacity(columns.len());
        for col in columns {
            values.push(col.get_value(row_index));
        }
        Self { values }
    }

    pub fn to_columns(records: &[Record], schema: &Schema) -> Result<Vec<Column>> {
        let mut columns: Vec<Column> = schema
            .fields()
            .iter()
            .map(|f| Column::new(&f.data_type))
            .collect();

        for record in records {
            for (col_idx, col) in columns.iter_mut().enumerate() {
                let value = record.get(col_idx).cloned().unwrap_or(Value::null());
                col.push(value)?;
            }
        }

        Ok(columns)
    }
}

impl std::ops::Index<usize> for Record {
    type Output = Value;

    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

impl std::ops::IndexMut<usize> for Record {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.values[index]
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveTime};
    use rust_decimal::Decimal;
    use yachtsql_common::types::DataType;

    use super::*;
    use crate::Field;

    #[test]
    fn test_new() {
        let record = Record::new();
        assert!(record.is_empty());
        assert_eq!(record.len(), 0);
    }

    #[test]
    fn test_default() {
        let record = Record::default();
        assert!(record.is_empty());
        assert_eq!(record.len(), 0);
    }

    #[test]
    fn test_with_capacity() {
        let record = Record::with_capacity(10);
        assert!(record.is_empty());
        assert_eq!(record.len(), 0);
    }

    #[test]
    fn test_from_values() {
        let values = vec![Value::Int64(1), Value::String("hello".to_string())];
        let record = Record::from_values(values.clone());
        assert_eq!(record.len(), 2);
        assert!(!record.is_empty());
        assert_eq!(record.values(), values.as_slice());
    }

    #[test]
    fn test_push() {
        let mut record = Record::new();
        record.push(Value::Int64(42));
        record.push(Value::String("test".to_string()));
        record.push(Value::Null);

        assert_eq!(record.len(), 3);
        assert_eq!(record.get(0), Some(&Value::Int64(42)));
        assert_eq!(record.get(1), Some(&Value::String("test".to_string())));
        assert_eq!(record.get(2), Some(&Value::Null));
    }

    #[test]
    fn test_get() {
        let record = Record::from_values(vec![
            Value::Int64(1),
            Value::float64(2.5),
            Value::Bool(true),
        ]);

        assert_eq!(record.get(0), Some(&Value::Int64(1)));
        assert_eq!(record.get(1), Some(&Value::float64(2.5)));
        assert_eq!(record.get(2), Some(&Value::Bool(true)));
        assert_eq!(record.get(3), None);
        assert_eq!(record.get(100), None);
    }

    #[test]
    fn test_get_mut() {
        let mut record = Record::from_values(vec![Value::Int64(1), Value::Int64(2)]);

        if let Some(val) = record.get_mut(0) {
            *val = Value::Int64(100);
        }

        assert_eq!(record.get(0), Some(&Value::Int64(100)));
        assert!(record.get_mut(10).is_none());
    }

    #[test]
    fn test_values() {
        let values = vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)];
        let record = Record::from_values(values.clone());
        assert_eq!(record.values(), values.as_slice());
    }

    #[test]
    fn test_into_values() {
        let values = vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)];
        let record = Record::from_values(values.clone());
        let result = record.into_values();
        assert_eq!(result, values);
    }

    #[test]
    fn test_remove() {
        let mut record =
            Record::from_values(vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)]);

        record.remove(1);
        assert_eq!(record.len(), 2);
        assert_eq!(record.get(0), Some(&Value::Int64(1)));
        assert_eq!(record.get(1), Some(&Value::Int64(3)));
    }

    #[test]
    fn test_remove_first() {
        let mut record =
            Record::from_values(vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)]);

        record.remove(0);
        assert_eq!(record.len(), 2);
        assert_eq!(record.get(0), Some(&Value::Int64(2)));
        assert_eq!(record.get(1), Some(&Value::Int64(3)));
    }

    #[test]
    fn test_remove_last() {
        let mut record =
            Record::from_values(vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)]);

        record.remove(2);
        assert_eq!(record.len(), 2);
        assert_eq!(record.get(0), Some(&Value::Int64(1)));
        assert_eq!(record.get(1), Some(&Value::Int64(2)));
    }

    #[test]
    fn test_remove_out_of_bounds() {
        let mut record = Record::from_values(vec![Value::Int64(1)]);
        record.remove(10);
        assert_eq!(record.len(), 1);
    }

    #[test]
    fn test_index() {
        let record = Record::from_values(vec![Value::Int64(10), Value::String("test".to_string())]);

        assert_eq!(record[0], Value::Int64(10));
        assert_eq!(record[1], Value::String("test".to_string()));
    }

    #[test]
    fn test_index_mut() {
        let mut record = Record::from_values(vec![Value::Int64(1), Value::Int64(2)]);
        record[0] = Value::Int64(100);
        record[1] = Value::String("changed".to_string());

        assert_eq!(record[0], Value::Int64(100));
        assert_eq!(record[1], Value::String("changed".to_string()));
    }

    #[test]
    #[should_panic]
    fn test_index_out_of_bounds() {
        let record = Record::from_values(vec![Value::Int64(1)]);
        let _ = record[10];
    }

    #[test]
    #[should_panic]
    fn test_index_mut_out_of_bounds() {
        let mut record = Record::from_values(vec![Value::Int64(1)]);
        record[10] = Value::Int64(100);
    }

    #[test]
    fn test_get_by_name() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
            Field::nullable("active", DataType::Bool),
        ]);

        let record = Record::from_values(vec![
            Value::Int64(42),
            Value::String("Alice".to_string()),
            Value::Bool(true),
        ]);

        assert_eq!(record.get_by_name(&schema, "id"), Some(&Value::Int64(42)));
        assert_eq!(
            record.get_by_name(&schema, "name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(
            record.get_by_name(&schema, "active"),
            Some(&Value::Bool(true))
        );
        assert_eq!(record.get_by_name(&schema, "nonexistent"), None);
    }

    #[test]
    fn test_get_by_name_empty_record() {
        let schema = Schema::from_fields(vec![Field::nullable("id", DataType::Int64)]);

        let record = Record::new();
        assert_eq!(record.get_by_name(&schema, "id"), None);
    }

    #[test]
    fn test_clone() {
        let record = Record::from_values(vec![Value::Int64(1), Value::String("test".to_string())]);
        let cloned = record.clone();
        assert_eq!(record, cloned);
    }

    #[test]
    fn test_eq() {
        let record1 = Record::from_values(vec![Value::Int64(1), Value::Int64(2)]);
        let record2 = Record::from_values(vec![Value::Int64(1), Value::Int64(2)]);
        let record3 = Record::from_values(vec![Value::Int64(1), Value::Int64(3)]);

        assert_eq!(record1, record2);
        assert_ne!(record1, record3);
    }

    #[test]
    fn test_debug() {
        let record = Record::from_values(vec![Value::Int64(1)]);
        let debug_str = format!("{:?}", record);
        assert!(debug_str.contains("Record"));
    }

    #[test]
    fn test_from_columns() {
        let mut col1 = Column::new(&DataType::Int64);
        col1.push(Value::Int64(1)).unwrap();
        col1.push(Value::Int64(2)).unwrap();
        col1.push(Value::Int64(3)).unwrap();

        let mut col2 = Column::new(&DataType::String);
        col2.push(Value::String("a".to_string())).unwrap();
        col2.push(Value::String("b".to_string())).unwrap();
        col2.push(Value::String("c".to_string())).unwrap();

        let columns = vec![col1, col2];

        let record0 = Record::from_columns(&columns, 0);
        assert_eq!(record0.len(), 2);
        assert_eq!(record0[0], Value::Int64(1));
        assert_eq!(record0[1], Value::String("a".to_string()));

        let record1 = Record::from_columns(&columns, 1);
        assert_eq!(record1[0], Value::Int64(2));
        assert_eq!(record1[1], Value::String("b".to_string()));

        let record2 = Record::from_columns(&columns, 2);
        assert_eq!(record2[0], Value::Int64(3));
        assert_eq!(record2[1], Value::String("c".to_string()));
    }

    #[test]
    fn test_from_columns_with_nulls() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::Int64(1)).unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Int64(3)).unwrap();

        let columns = vec![col];

        let record0 = Record::from_columns(&columns, 0);
        assert_eq!(record0[0], Value::Int64(1));

        let record1 = Record::from_columns(&columns, 1);
        assert_eq!(record1[0], Value::Null);

        let record2 = Record::from_columns(&columns, 2);
        assert_eq!(record2[0], Value::Int64(3));
    }

    #[test]
    fn test_to_columns() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ]);

        let records = vec![
            Record::from_values(vec![Value::Int64(1), Value::String("Alice".to_string())]),
            Record::from_values(vec![Value::Int64(2), Value::String("Bob".to_string())]),
            Record::from_values(vec![Value::Int64(3), Value::String("Charlie".to_string())]),
        ];

        let columns = Record::to_columns(&records, &schema).unwrap();

        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].len(), 3);
        assert_eq!(columns[1].len(), 3);

        assert_eq!(columns[0].get_value(0), Value::Int64(1));
        assert_eq!(columns[0].get_value(1), Value::Int64(2));
        assert_eq!(columns[0].get_value(2), Value::Int64(3));

        assert_eq!(columns[1].get_value(0), Value::String("Alice".to_string()));
        assert_eq!(columns[1].get_value(1), Value::String("Bob".to_string()));
        assert_eq!(
            columns[1].get_value(2),
            Value::String("Charlie".to_string())
        );
    }

    #[test]
    fn test_to_columns_with_nulls() {
        let schema = Schema::from_fields(vec![Field::nullable("value", DataType::Int64)]);

        let records = vec![
            Record::from_values(vec![Value::Int64(1)]),
            Record::from_values(vec![Value::Null]),
            Record::from_values(vec![Value::Int64(3)]),
        ];

        let columns = Record::to_columns(&records, &schema).unwrap();

        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].get_value(0), Value::Int64(1));
        assert_eq!(columns[0].get_value(1), Value::Null);
        assert_eq!(columns[0].get_value(2), Value::Int64(3));
    }

    #[test]
    fn test_to_columns_empty_records() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ]);

        let records: Vec<Record> = vec![];
        let columns = Record::to_columns(&records, &schema).unwrap();

        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].len(), 0);
        assert_eq!(columns[1].len(), 0);
    }

    #[test]
    fn test_to_columns_missing_values() {
        let schema = Schema::from_fields(vec![
            Field::nullable("a", DataType::Int64),
            Field::nullable("b", DataType::Int64),
            Field::nullable("c", DataType::Int64),
        ]);

        let records = vec![Record::from_values(vec![Value::Int64(1)])];

        let columns = Record::to_columns(&records, &schema).unwrap();

        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0].get_value(0), Value::Int64(1));
        assert_eq!(columns[1].get_value(0), Value::Null);
        assert_eq!(columns[2].get_value(0), Value::Null);
    }

    #[test]
    fn test_roundtrip_columns_to_records() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
            Field::nullable("score", DataType::Float64),
        ]);

        let original_records = vec![
            Record::from_values(vec![
                Value::Int64(1),
                Value::String("Alice".to_string()),
                Value::float64(95.5),
            ]),
            Record::from_values(vec![
                Value::Int64(2),
                Value::String("Bob".to_string()),
                Value::Null,
            ]),
        ];

        let columns = Record::to_columns(&original_records, &schema).unwrap();

        let reconstructed0 = Record::from_columns(&columns, 0);
        let reconstructed1 = Record::from_columns(&columns, 1);

        assert_eq!(original_records[0], reconstructed0);
        assert_eq!(original_records[1], reconstructed1);
    }

    #[test]
    fn test_various_data_types() {
        let record = Record::from_values(vec![
            Value::Bool(true),
            Value::Int64(42),
            Value::float64(3.15),
            Value::String("hello".to_string()),
            Value::Null,
            Value::Numeric(Decimal::new(123, 2)),
            Value::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()),
            Value::Array(vec![Value::Int64(1), Value::Int64(2)]),
            Value::Struct(vec![("x".to_string(), Value::Int64(10))]),
        ]);

        assert_eq!(record.len(), 9);
        assert_eq!(record[0], Value::Bool(true));
        assert_eq!(record[1], Value::Int64(42));
        assert_eq!(record[4], Value::Null);
        assert_eq!(
            record[7],
            Value::Array(vec![Value::Int64(1), Value::Int64(2)])
        );
    }

    #[test]
    fn test_is_empty_vs_len() {
        let empty = Record::new();
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);

        let mut record = Record::new();
        record.push(Value::Int64(1));
        assert!(!record.is_empty());
        assert_eq!(record.len(), 1);

        record.remove(0);
        assert!(record.is_empty());
        assert_eq!(record.len(), 0);
    }

    #[test]
    fn test_from_columns_empty() {
        let columns: Vec<Column> = vec![];
        let record = Record::from_columns(&columns, 0);
        assert!(record.is_empty());
    }

    #[test]
    fn test_to_columns_all_types() {
        let schema = Schema::from_fields(vec![
            Field::nullable("bool_col", DataType::Bool),
            Field::nullable("int_col", DataType::Int64),
            Field::nullable("float_col", DataType::Float64),
            Field::nullable("string_col", DataType::String),
            Field::nullable("numeric_col", DataType::Numeric(None)),
            Field::nullable("date_col", DataType::Date),
            Field::nullable("time_col", DataType::Time),
        ]);

        let records = vec![Record::from_values(vec![
            Value::Bool(true),
            Value::Int64(100),
            Value::float64(1.5),
            Value::String("test".to_string()),
            Value::Numeric(Decimal::new(500, 2)),
            Value::Date(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()),
            Value::Time(NaiveTime::from_hms_opt(12, 30, 45).unwrap()),
        ])];

        let columns = Record::to_columns(&records, &schema).unwrap();
        assert_eq!(columns.len(), 7);

        assert_eq!(columns[0].get_value(0), Value::Bool(true));
        assert_eq!(columns[1].get_value(0), Value::Int64(100));
        assert_eq!(columns[3].get_value(0), Value::String("test".to_string()));
    }

    #[test]
    fn test_from_columns_out_of_bounds_row() {
        let mut col = Column::new(&DataType::Int64);
        col.push(Value::Int64(1)).unwrap();
        col.push(Value::Int64(2)).unwrap();

        let columns = vec![col];

        let record = Record::from_columns(&columns, 100);
        assert_eq!(record.len(), 1);
        assert_eq!(record[0], Value::Null);
    }

    #[test]
    fn test_multiple_push_and_get() {
        let mut record = Record::new();
        for i in 0..100i64 {
            record.push(Value::Int64(i));
        }
        assert_eq!(record.len(), 100);
        for i in 0..100 {
            assert_eq!(record.get(i), Some(&Value::Int64(i as i64)));
        }
        assert_eq!(record.get(100), None);
    }

    #[test]
    fn test_remove_all_elements() {
        let mut record =
            Record::from_values(vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)]);

        record.remove(0);
        record.remove(0);
        record.remove(0);
        assert!(record.is_empty());
    }

    #[test]
    fn test_get_by_name_with_schema_column_mismatch() {
        let schema = Schema::from_fields(vec![
            Field::nullable("a", DataType::Int64),
            Field::nullable("b", DataType::Int64),
            Field::nullable("c", DataType::Int64),
        ]);

        let record = Record::from_values(vec![Value::Int64(1)]);

        assert_eq!(record.get_by_name(&schema, "a"), Some(&Value::Int64(1)));
        assert_eq!(record.get_by_name(&schema, "b"), None);
        assert_eq!(record.get_by_name(&schema, "c"), None);
    }

    #[test]
    fn test_with_capacity_then_push() {
        let mut record = Record::with_capacity(5);
        assert!(record.is_empty());

        record.push(Value::Int64(1));
        record.push(Value::Int64(2));
        record.push(Value::Int64(3));

        assert_eq!(record.len(), 3);
        assert_eq!(record[0], Value::Int64(1));
        assert_eq!(record[1], Value::Int64(2));
        assert_eq!(record[2], Value::Int64(3));
    }

    #[test]
    fn test_nested_array_values() {
        let nested_array = Value::Array(vec![
            Value::Array(vec![Value::Int64(1), Value::Int64(2)]),
            Value::Array(vec![Value::Int64(3), Value::Int64(4)]),
        ]);
        let record = Record::from_values(vec![nested_array.clone()]);

        assert_eq!(record.len(), 1);
        assert_eq!(record[0], nested_array);
    }

    #[test]
    fn test_nested_struct_values() {
        let nested_struct = Value::Struct(vec![(
            "outer".to_string(),
            Value::Struct(vec![("inner".to_string(), Value::Int64(42))]),
        )]);
        let record = Record::from_values(vec![nested_struct.clone()]);

        assert_eq!(record.len(), 1);
        assert_eq!(record[0], nested_struct);
    }

    #[test]
    fn test_single_column_single_record_roundtrip() {
        let schema = Schema::from_fields(vec![Field::nullable("x", DataType::Int64)]);
        let records = vec![Record::from_values(vec![Value::Int64(999)])];

        let columns = Record::to_columns(&records, &schema).unwrap();
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].len(), 1);

        let reconstructed = Record::from_columns(&columns, 0);
        assert_eq!(records[0], reconstructed);
    }

    #[test]
    fn test_all_null_record() {
        let record = Record::from_values(vec![Value::Null, Value::Null, Value::Null]);

        assert_eq!(record.len(), 3);
        assert_eq!(record[0], Value::Null);
        assert_eq!(record[1], Value::Null);
        assert_eq!(record[2], Value::Null);
    }

    #[test]
    fn test_to_columns_all_null_records() {
        let schema = Schema::from_fields(vec![
            Field::nullable("a", DataType::Int64),
            Field::nullable("b", DataType::String),
        ]);

        let records = vec![
            Record::from_values(vec![Value::Null, Value::Null]),
            Record::from_values(vec![Value::Null, Value::Null]),
        ];

        let columns = Record::to_columns(&records, &schema).unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].get_value(0), Value::Null);
        assert_eq!(columns[0].get_value(1), Value::Null);
        assert_eq!(columns[1].get_value(0), Value::Null);
        assert_eq!(columns[1].get_value(1), Value::Null);
    }

    #[test]
    fn test_get_mut_modify_multiple() {
        let mut record =
            Record::from_values(vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)]);

        if let Some(v) = record.get_mut(0) {
            *v = Value::Int64(10);
        }
        if let Some(v) = record.get_mut(1) {
            *v = Value::Int64(20);
        }
        if let Some(v) = record.get_mut(2) {
            *v = Value::Int64(30);
        }

        assert_eq!(record[0], Value::Int64(10));
        assert_eq!(record[1], Value::Int64(20));
        assert_eq!(record[2], Value::Int64(30));
    }

    #[test]
    fn test_index_mut_all_positions() {
        let mut record =
            Record::from_values(vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)]);

        record[0] = Value::String("first".to_string());
        record[1] = Value::String("second".to_string());
        record[2] = Value::String("third".to_string());

        assert_eq!(record[0], Value::String("first".to_string()));
        assert_eq!(record[1], Value::String("second".to_string()));
        assert_eq!(record[2], Value::String("third".to_string()));
    }

    #[test]
    fn test_from_columns_single_column() {
        let mut col = Column::new(&DataType::String);
        col.push(Value::String("only".to_string())).unwrap();

        let record = Record::from_columns(&[col], 0);
        assert_eq!(record.len(), 1);
        assert_eq!(record[0], Value::String("only".to_string()));
    }

    #[test]
    fn test_to_columns_single_record_single_field() {
        let schema = Schema::from_fields(vec![Field::nullable("single", DataType::Bool)]);
        let records = vec![Record::from_values(vec![Value::Bool(false)])];

        let columns = Record::to_columns(&records, &schema).unwrap();
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].len(), 1);
        assert_eq!(columns[0].get_value(0), Value::Bool(false));
    }
}
