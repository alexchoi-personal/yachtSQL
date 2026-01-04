#![coverage(off)]

use std::sync::Arc;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

use crate::{Column, Record, Schema};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Table {
    schema: Schema,
    columns: IndexMap<String, Arc<Column>>,
    row_count: usize,
}

impl Table {
    pub fn new(schema: Schema) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|f| (f.name.clone(), Arc::new(Column::new(&f.data_type))))
            .collect();
        Self {
            schema,
            columns,
            row_count: 0,
        }
    }

    pub fn from_columns(schema: Schema, columns: IndexMap<String, Column>) -> Self {
        let row_count = columns.values().next().map(|c| c.len()).unwrap_or(0);
        let arc_columns = columns.into_iter().map(|(k, v)| (k, Arc::new(v))).collect();
        Self {
            schema,
            columns: arc_columns,
            row_count,
        }
    }

    pub fn from_arc_columns(schema: Schema, columns: IndexMap<String, Arc<Column>>) -> Self {
        let row_count = columns.values().next().map(|c| c.len()).unwrap_or(0);
        Self {
            schema,
            columns,
            row_count,
        }
    }

    pub fn empty(schema: Schema) -> Self {
        Self::new(schema)
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn num_rows(&self) -> usize {
        self.row_count
    }

    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    pub fn column(&self, idx: usize) -> Option<&Column> {
        self.columns.values().nth(idx).map(|arc| arc.as_ref())
    }

    pub fn column_by_name(&self, name: &str) -> Option<&Column> {
        self.columns.get(name).map(|arc| arc.as_ref())
    }

    pub fn get_column(&self, name: &str) -> Option<Arc<Column>> {
        self.columns.get(name).map(Arc::clone)
    }

    pub fn get_column_arc(&self, idx: usize) -> Option<Arc<Column>> {
        self.columns.values().nth(idx).map(Arc::clone)
    }

    pub fn columns(&self) -> &IndexMap<String, Arc<Column>> {
        &self.columns
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn columns_mut(&mut self) -> &mut IndexMap<String, Arc<Column>> {
        &mut self.columns
    }

    pub fn get_column_mut(&mut self, name: &str) -> Option<&mut Column> {
        self.columns.get_mut(name).map(Arc::make_mut)
    }

    pub fn push_row(&mut self, values: Vec<Value>) -> Result<()> {
        for (col, value) in self.columns.values_mut().zip(values.into_iter()) {
            Arc::make_mut(col).push(value)?;
        }
        self.row_count += 1;
        Ok(())
    }

    pub fn push_rows(&mut self, rows: Vec<Vec<Value>>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let num_rows = rows.len();
        let num_cols = self.columns.len();

        let mut columns_data: Vec<Vec<Value>> = vec![Vec::with_capacity(num_rows); num_cols];

        for row in rows {
            for (col_idx, value) in row.into_iter().enumerate() {
                if col_idx < num_cols {
                    columns_data[col_idx].push(value);
                }
            }
        }

        for (col, values) in self.columns.values_mut().zip(columns_data.into_iter()) {
            let col_mut = Arc::make_mut(col);
            for value in values {
                col_mut.push(value)?;
            }
        }

        self.row_count += num_rows;
        Ok(())
    }

    pub fn get_row(&self, index: usize) -> Result<Record> {
        if index >= self.row_count {
            return Err(yachtsql_common::error::Error::invalid_query(format!(
                "Row index {} out of bounds (count: {})",
                index, self.row_count
            )));
        }
        let values: Vec<Value> = self.columns.values().map(|c| c.get_value(index)).collect();
        Ok(Record::from_values(values))
    }

    pub fn to_records(&self) -> Result<Vec<Record>> {
        let mut records = Vec::with_capacity(self.row_count);
        for i in 0..self.row_count {
            records.push(self.get_row(i)?);
        }
        Ok(records)
    }

    pub fn rows(&self) -> Result<Vec<Record>> {
        self.to_records()
    }

    pub fn from_records(schema: Schema, records: Vec<Record>) -> Result<Self> {
        let mut table = Self::new(schema);
        for record in records {
            table.push_row(record.into_values())?;
        }
        Ok(table)
    }

    pub fn from_values(schema: Schema, values: Vec<Vec<Value>>) -> Result<Self> {
        let mut table = Self::new(schema);
        table.push_rows(values)?;
        Ok(table)
    }

    pub fn clear(&mut self) {
        for col in self.columns.values_mut() {
            Arc::make_mut(col).clear();
        }
        self.row_count = 0;
    }

    pub fn remove_row(&mut self, index: usize) {
        if index >= self.row_count {
            return;
        }
        for col in self.columns.values_mut() {
            Arc::make_mut(col).remove(index);
        }
        self.row_count -= 1;
    }

    pub fn update_row(&mut self, index: usize, values: Vec<Value>) -> Result<()> {
        for (col, value) in self.columns.values_mut().zip(values.into_iter()) {
            Arc::make_mut(col).set(index, value)?;
        }
        Ok(())
    }

    pub fn drop_column(&mut self, name: &str) -> Result<()> {
        let upper = name.to_uppercase();
        let found = self
            .columns
            .keys()
            .find(|k| k.to_uppercase() == upper)
            .cloned();
        if let Some(key) = found {
            self.columns.shift_remove(&key);
            let fields: Vec<_> = self
                .schema
                .fields()
                .iter()
                .filter(|f| f.name.to_uppercase() != upper)
                .cloned()
                .collect();
            self.schema = Schema::from_fields(fields);
            Ok(())
        } else {
            Err(yachtsql_common::error::Error::ColumnNotFound(
                name.to_string(),
            ))
        }
    }

    pub fn rename_column(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        let upper = old_name.to_uppercase();
        let found_idx = self.columns.keys().position(|k| k.to_uppercase() == upper);
        if let Some(idx) = found_idx {
            let key = self.columns.keys().nth(idx).cloned().unwrap();
            if let Some(arc_col) = self.columns.shift_remove(&key) {
                self.columns
                    .shift_insert(idx, new_name.to_string(), arc_col);
            }
            let fields: Vec<_> = self
                .schema
                .fields()
                .iter()
                .map(|f| {
                    if f.name.to_uppercase() == upper {
                        crate::Field::new(new_name.to_string(), f.data_type.clone(), f.mode)
                    } else {
                        f.clone()
                    }
                })
                .collect();
            self.schema = Schema::from_fields(fields);
            Ok(())
        } else {
            Err(yachtsql_common::error::Error::ColumnNotFound(
                old_name.to_string(),
            ))
        }
    }

    pub fn set_column_not_null(&mut self, col_name: &str) -> Result<()> {
        let upper = col_name.to_uppercase();
        let found = self
            .schema
            .fields()
            .iter()
            .any(|f| f.name.to_uppercase() == upper);
        if !found {
            return Err(yachtsql_common::error::Error::ColumnNotFound(
                col_name.to_string(),
            ));
        }
        let fields: Vec<_> = self
            .schema
            .fields()
            .iter()
            .map(|f| {
                if f.name.to_uppercase() == upper {
                    crate::Field::new(
                        f.name.clone(),
                        f.data_type.clone(),
                        crate::FieldMode::Required,
                    )
                } else {
                    f.clone()
                }
            })
            .collect();
        self.schema = Schema::from_fields(fields);
        Ok(())
    }

    pub fn set_column_nullable(&mut self, col_name: &str) -> Result<()> {
        let upper = col_name.to_uppercase();
        let found = self
            .schema
            .fields()
            .iter()
            .any(|f| f.name.to_uppercase() == upper);
        if !found {
            return Err(yachtsql_common::error::Error::ColumnNotFound(
                col_name.to_string(),
            ));
        }
        let fields: Vec<_> = self
            .schema
            .fields()
            .iter()
            .map(|f| {
                if f.name.to_uppercase() == upper {
                    crate::Field::new(
                        f.name.clone(),
                        f.data_type.clone(),
                        crate::FieldMode::Nullable,
                    )
                } else {
                    f.clone()
                }
            })
            .collect();
        self.schema = Schema::from_fields(fields);
        Ok(())
    }

    pub fn set_column_default(&mut self, col_name: &str, default: Value) -> Result<()> {
        let upper = col_name.to_uppercase();
        let found = self
            .schema
            .fields()
            .iter()
            .any(|f| f.name.to_uppercase() == upper);
        if !found {
            return Err(yachtsql_common::error::Error::ColumnNotFound(
                col_name.to_string(),
            ));
        }
        let fields: Vec<_> = self
            .schema
            .fields()
            .iter()
            .map(|f| {
                if f.name.to_uppercase() == upper {
                    let mut new_field = f.clone();
                    new_field.default_value = Some(default.clone());
                    new_field
                } else {
                    f.clone()
                }
            })
            .collect();
        self.schema = Schema::from_fields(fields);
        Ok(())
    }

    pub fn drop_column_default(&mut self, col_name: &str) -> Result<()> {
        let upper = col_name.to_uppercase();
        let found = self
            .schema
            .fields()
            .iter()
            .any(|f| f.name.to_uppercase() == upper);
        if !found {
            return Err(yachtsql_common::error::Error::ColumnNotFound(
                col_name.to_string(),
            ));
        }
        let fields: Vec<_> = self
            .schema
            .fields()
            .iter()
            .map(|f| {
                if f.name.to_uppercase() == upper {
                    let mut new_field = f.clone();
                    new_field.default_value = None;
                    new_field
                } else {
                    f.clone()
                }
            })
            .collect();
        self.schema = Schema::from_fields(fields);
        Ok(())
    }

    pub fn set_column_data_type(
        &mut self,
        col_name: &str,
        new_data_type: yachtsql_common::types::DataType,
    ) -> Result<()> {
        let upper = col_name.to_uppercase();
        let field_idx = self
            .schema
            .fields()
            .iter()
            .position(|f| f.name.to_uppercase() == upper);
        let field_idx = match field_idx {
            Some(idx) => idx,
            None => {
                return Err(yachtsql_common::error::Error::ColumnNotFound(
                    col_name.to_string(),
                ));
            }
        };

        let old_field = &self.schema.fields()[field_idx];
        let old_type = &old_field.data_type;

        let needs_conversion = !Self::types_compatible(old_type, &new_data_type);

        if needs_conversion {
            let col_name_key = self
                .columns
                .keys()
                .find(|k| k.to_uppercase() == upper)
                .cloned();
            if let Some(key) = col_name_key
                && let Some(old_col) = self.columns.get(&key)
            {
                let new_col = Self::convert_column(old_col.as_ref(), old_type, &new_data_type)?;
                self.columns.insert(key, Arc::new(new_col));
            }
        }

        let fields: Vec<_> = self
            .schema
            .fields()
            .iter()
            .map(|f| {
                if f.name.to_uppercase() == upper {
                    crate::Field::new(f.name.clone(), new_data_type.clone(), f.mode)
                } else {
                    f.clone()
                }
            })
            .collect();
        self.schema = Schema::from_fields(fields);
        Ok(())
    }

    pub fn set_column_collation(&mut self, col_name: &str, collation: String) -> Result<()> {
        let upper = col_name.to_uppercase();
        let field_idx = self
            .schema
            .fields()
            .iter()
            .position(|f| f.name.to_uppercase() == upper);
        let field_idx = match field_idx {
            Some(idx) => idx,
            None => {
                return Err(yachtsql_common::error::Error::ColumnNotFound(
                    col_name.to_string(),
                ));
            }
        };

        let fields: Vec<_> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if i == field_idx {
                    let mut new_field = f.clone();
                    new_field.collation = Some(collation.clone());
                    new_field
                } else {
                    f.clone()
                }
            })
            .collect();
        self.schema = Schema::from_fields(fields);
        Ok(())
    }

    fn types_compatible(
        old_type: &yachtsql_common::types::DataType,
        new_type: &yachtsql_common::types::DataType,
    ) -> bool {
        use yachtsql_common::types::DataType;
        matches!(
            (old_type, new_type),
            (DataType::String, DataType::String)
                | (DataType::Int64, DataType::Int64)
                | (DataType::Float64, DataType::Float64)
                | (DataType::Numeric(_), DataType::Numeric(_))
        )
    }

    fn convert_column(
        old_col: &Column,
        _old_type: &yachtsql_common::types::DataType,
        new_type: &yachtsql_common::types::DataType,
    ) -> Result<Column> {
        use rust_decimal::Decimal;
        use yachtsql_common::types::DataType;

        match new_type {
            DataType::Numeric(_) => {
                let len = old_col.len();
                let mut new_data = Vec::with_capacity(len);
                let mut new_nulls = crate::NullBitmap::new_valid(len);
                for i in 0..len {
                    let val = old_col.get_value(i);
                    match val {
                        Value::Null => {
                            new_data.push(Decimal::ZERO);
                            new_nulls.set_null(i);
                        }
                        Value::Int64(v) => {
                            new_data.push(Decimal::from(v));
                        }
                        Value::Float64(v) => {
                            let f = v.into_inner();
                            new_data.push(
                                Decimal::try_from(f).unwrap_or_else(|_| Decimal::from(f as i64)),
                            );
                        }
                        Value::Numeric(d) => {
                            new_data.push(d);
                        }
                        _ => {
                            return Err(yachtsql_common::error::Error::invalid_query(format!(
                                "Cannot convert {:?} to NUMERIC",
                                val
                            )));
                        }
                    }
                }
                Ok(Column::Numeric {
                    data: new_data,
                    nulls: new_nulls,
                })
            }
            _ => Err(yachtsql_common::error::Error::UnsupportedFeature(format!(
                "Data type conversion to {:?} not yet implemented",
                new_type
            ))),
        }
    }

    pub fn with_schema(&self, new_schema: Schema) -> Table {
        let mut new_columns = IndexMap::new();
        for (old_col, new_field) in self.columns.values().zip(new_schema.fields().iter()) {
            new_columns.insert(new_field.name.clone(), Arc::clone(old_col));
        }
        Table {
            schema: new_schema,
            columns: new_columns,
            row_count: self.row_count,
        }
    }

    pub fn with_reordered_schema(&self, new_schema: Schema, column_indices: &[usize]) -> Table {
        let source_columns: Vec<_> = self.columns.values().collect();
        let mut new_columns = IndexMap::new();
        for (new_field, &idx) in new_schema.fields().iter().zip(column_indices.iter()) {
            if idx < source_columns.len() {
                new_columns.insert(new_field.name.clone(), Arc::clone(source_columns[idx]));
            }
        }
        Table {
            schema: new_schema,
            columns: new_columns,
            row_count: self.row_count,
        }
    }

    pub fn to_query_result(&self) -> Result<yachtsql_common::QueryResult> {
        use yachtsql_common::{ColumnInfo, QueryResult, Row};

        let schema: Vec<ColumnInfo> = self
            .schema
            .fields()
            .iter()
            .map(|f| ColumnInfo::new(&f.name, f.data_type.to_bq_type()))
            .collect();

        let records = self.to_records()?;
        let rows: Vec<Row> = records
            .into_iter()
            .map(|record| Row::new(record.into_values()))
            .collect();

        Ok(QueryResult::new(schema, rows))
    }

    pub fn filter_by_mask(&self, mask: &Column) -> Result<Self> {
        let Column::Bool {
            data: mask_data,
            nulls: mask_nulls,
        } = mask
        else {
            return Err(Error::internal(
                "filter_by_mask requires a Bool column as mask",
            ));
        };

        let mut indices = Vec::new();
        for (i, &val) in mask_data.iter().enumerate() {
            if val && !mask_nulls.is_null(i) {
                indices.push(i);
            }
        }
        Ok(self.gather_rows(&indices))
    }

    pub fn gather_rows(&self, indices: &[usize]) -> Self {
        let new_columns: IndexMap<String, Arc<Column>> = self
            .columns
            .iter()
            .map(|(name, col)| (name.clone(), Arc::new(col.gather(indices))))
            .collect();
        Self {
            schema: self.schema.clone(),
            columns: new_columns,
            row_count: indices.len(),
        }
    }

    pub fn reorder_by_indices(&self, indices: &[usize]) -> Self {
        self.gather_rows(indices)
    }

    pub fn concat(&self, other: &Table) -> Self {
        let mut new_columns: IndexMap<String, Arc<Column>> = IndexMap::new();
        for (name, col) in &self.columns {
            let mut new_col = col.as_ref().clone();
            if let Some(other_col) = other.columns.get(name) {
                let _ = new_col.extend(other_col.as_ref());
            }
            new_columns.insert(name.clone(), Arc::new(new_col));
        }
        Self {
            schema: self.schema.clone(),
            columns: new_columns,
            row_count: self.row_count + other.row_count,
        }
    }

    pub fn concat_tables(schema: Schema, tables: &[&Table]) -> Self {
        if tables.is_empty() {
            return Self::empty(schema);
        }

        let mut result = tables[0].clone();
        for table in &tables[1..] {
            result = result.concat(table);
        }
        result
    }
}

pub trait TableSchemaOps {
    fn add_column(&mut self, field: crate::Field, default: Option<Value>) -> Result<()>;
}

impl TableSchemaOps for Table {
    fn add_column(&mut self, field: crate::Field, default: Option<Value>) -> Result<()> {
        let default_val = default.unwrap_or(Value::Null);
        let mut col = Column::new(&field.data_type);
        for _ in 0..self.row_count {
            col.push(default_val.clone())?;
        }
        self.columns.insert(field.name.clone(), Arc::new(col));
        let mut fields: Vec<_> = self.schema.fields().to_vec();
        fields.push(field);
        self.schema = Schema::from_fields(fields);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use yachtsql_common::types::{DataType, Value};

    use super::*;
    use crate::{Field, FieldMode, NullBitmap};

    fn create_test_schema() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
            Field::nullable("score", DataType::Float64),
        ])
    }

    fn create_test_table() -> Table {
        let schema = create_test_schema();
        let mut table = Table::new(schema);
        table
            .push_row(vec![
                Value::Int64(1),
                Value::String("Alice".to_string()),
                Value::float64(95.5),
            ])
            .unwrap();
        table
            .push_row(vec![
                Value::Int64(2),
                Value::String("Bob".to_string()),
                Value::float64(87.0),
            ])
            .unwrap();
        table
            .push_row(vec![
                Value::Int64(3),
                Value::String("Charlie".to_string()),
                Value::float64(92.3),
            ])
            .unwrap();
        table
    }

    #[test]
    fn test_table_new() {
        let schema = create_test_schema();
        let table = Table::new(schema.clone());
        assert_eq!(table.row_count(), 0);
        assert_eq!(table.num_rows(), 0);
        assert!(table.is_empty());
        assert_eq!(table.num_columns(), 3);
        assert_eq!(table.schema(), &schema);
    }

    #[test]
    fn test_table_empty() {
        let schema = create_test_schema();
        let table = Table::empty(schema.clone());
        assert!(table.is_empty());
        assert_eq!(table.num_columns(), 3);
    }

    #[test]
    fn test_from_columns() {
        let mut columns = IndexMap::new();
        let mut int_col = Column::new(&DataType::Int64);
        int_col.push(Value::Int64(1)).unwrap();
        int_col.push(Value::Int64(2)).unwrap();
        columns.insert("id".to_string(), int_col);

        let mut str_col = Column::new(&DataType::String);
        str_col.push(Value::String("a".to_string())).unwrap();
        str_col.push(Value::String("b".to_string())).unwrap();
        columns.insert("name".to_string(), str_col);

        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ]);

        let table = Table::from_columns(schema, columns);
        assert_eq!(table.row_count(), 2);
        assert_eq!(table.num_columns(), 2);
    }

    #[test]
    fn test_from_columns_empty() {
        let columns = IndexMap::new();
        let schema = Schema::new();
        let table = Table::from_columns(schema, columns);
        assert_eq!(table.row_count(), 0);
    }

    #[test]
    fn test_push_row() {
        let table = create_test_table();
        assert_eq!(table.row_count(), 3);
        assert!(!table.is_empty());
    }

    #[test]
    fn test_column_by_index() {
        let table = create_test_table();
        let col = table.column(0);
        assert!(col.is_some());
        assert_eq!(col.unwrap().len(), 3);

        let col = table.column(1);
        assert!(col.is_some());

        let col = table.column(10);
        assert!(col.is_none());
    }

    #[test]
    fn test_column_by_name() {
        let table = create_test_table();
        let col = table.column_by_name("id");
        assert!(col.is_some());
        assert_eq!(col.unwrap().len(), 3);

        let col = table.column_by_name("name");
        assert!(col.is_some());

        let col = table.column_by_name("nonexistent");
        assert!(col.is_none());
    }

    #[test]
    fn test_columns_accessor() {
        let table = create_test_table();
        let cols = table.columns();
        assert_eq!(cols.len(), 3);
    }

    #[test]
    fn test_columns_mut() {
        let mut table = create_test_table();
        let cols = table.columns_mut();
        assert_eq!(cols.len(), 3);
    }

    #[test]
    fn test_get_row() {
        let table = create_test_table();
        let row = table.get_row(0).unwrap();
        assert_eq!(row.len(), 3);
        assert_eq!(row.values()[0], Value::Int64(1));
        assert_eq!(row.values()[1], Value::String("Alice".to_string()));

        let row = table.get_row(2).unwrap();
        assert_eq!(row.values()[0], Value::Int64(3));
    }

    #[test]
    fn test_get_row_out_of_bounds() {
        let table = create_test_table();
        let result = table.get_row(10);
        assert!(result.is_err());
    }

    #[test]
    fn test_to_records() {
        let table = create_test_table();
        let records = table.to_records().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].values()[0], Value::Int64(1));
        assert_eq!(records[1].values()[0], Value::Int64(2));
        assert_eq!(records[2].values()[0], Value::Int64(3));
    }

    #[test]
    fn test_rows() {
        let table = create_test_table();
        let rows = table.rows().unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_from_records() {
        let schema = create_test_schema();
        let records = vec![
            Record::from_values(vec![
                Value::Int64(1),
                Value::String("A".to_string()),
                Value::float64(1.0),
            ]),
            Record::from_values(vec![
                Value::Int64(2),
                Value::String("B".to_string()),
                Value::float64(2.0),
            ]),
        ];
        let table = Table::from_records(schema, records).unwrap();
        assert_eq!(table.row_count(), 2);
    }

    #[test]
    fn test_from_values() {
        let schema = create_test_schema();
        let values = vec![
            vec![
                Value::Int64(1),
                Value::String("X".to_string()),
                Value::float64(10.0),
            ],
            vec![
                Value::Int64(2),
                Value::String("Y".to_string()),
                Value::float64(20.0),
            ],
        ];
        let table = Table::from_values(schema, values).unwrap();
        assert_eq!(table.row_count(), 2);
    }

    #[test]
    fn test_clear() {
        let mut table = create_test_table();
        assert_eq!(table.row_count(), 3);
        table.clear();
        assert_eq!(table.row_count(), 0);
        assert!(table.is_empty());
    }

    #[test]
    fn test_remove_row() {
        let mut table = create_test_table();
        assert_eq!(table.row_count(), 3);
        table.remove_row(1);
        assert_eq!(table.row_count(), 2);
        let row0 = table.get_row(0).unwrap();
        assert_eq!(row0.values()[0], Value::Int64(1));
        let row1 = table.get_row(1).unwrap();
        assert_eq!(row1.values()[0], Value::Int64(3));
    }

    #[test]
    fn test_remove_row_empty_table() {
        let schema = create_test_schema();
        let mut table = Table::new(schema);
        table.remove_row(0);
        assert_eq!(table.row_count(), 0);
    }

    #[test]
    fn test_update_row() {
        let mut table = create_test_table();
        table
            .update_row(
                1,
                vec![
                    Value::Int64(100),
                    Value::String("Updated".to_string()),
                    Value::float64(99.9),
                ],
            )
            .unwrap();
        let row = table.get_row(1).unwrap();
        assert_eq!(row.values()[0], Value::Int64(100));
        assert_eq!(row.values()[1], Value::String("Updated".to_string()));
    }

    #[test]
    fn test_drop_column() {
        let mut table = create_test_table();
        assert_eq!(table.num_columns(), 3);
        table.drop_column("name").unwrap();
        assert_eq!(table.num_columns(), 2);
        assert!(table.column_by_name("name").is_none());
        assert!(table.column_by_name("id").is_some());
        assert!(table.column_by_name("score").is_some());
    }

    #[test]
    fn test_drop_column_case_insensitive() {
        let mut table = create_test_table();
        table.drop_column("NAME").unwrap();
        assert_eq!(table.num_columns(), 2);
    }

    #[test]
    fn test_drop_column_not_found() {
        let mut table = create_test_table();
        let result = table.drop_column("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_rename_column() {
        let mut table = create_test_table();
        table.rename_column("name", "full_name").unwrap();
        assert!(table.column_by_name("name").is_none());
        assert!(table.column_by_name("full_name").is_some());
    }

    #[test]
    fn test_rename_column_case_insensitive() {
        let mut table = create_test_table();
        table.rename_column("NAME", "full_name").unwrap();
        assert!(table.column_by_name("full_name").is_some());
    }

    #[test]
    fn test_rename_column_not_found() {
        let mut table = create_test_table();
        let result = table.rename_column("nonexistent", "new_name");
        assert!(result.is_err());
    }

    #[test]
    fn test_set_column_not_null() {
        let mut table = create_test_table();
        table.set_column_not_null("name").unwrap();
        let field = table
            .schema()
            .fields()
            .iter()
            .find(|f| f.name == "name")
            .unwrap();
        assert_eq!(field.mode, FieldMode::Required);
    }

    #[test]
    fn test_set_column_not_null_case_insensitive() {
        let mut table = create_test_table();
        table.set_column_not_null("NAME").unwrap();
    }

    #[test]
    fn test_set_column_not_null_not_found() {
        let mut table = create_test_table();
        let result = table.set_column_not_null("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_set_column_nullable() {
        let schema = Schema::from_fields(vec![Field::required("id", DataType::Int64)]);
        let mut table = Table::new(schema);
        table.push_row(vec![Value::Int64(1)]).unwrap();
        table.set_column_nullable("id").unwrap();
        let field = table
            .schema()
            .fields()
            .iter()
            .find(|f| f.name == "id")
            .unwrap();
        assert_eq!(field.mode, FieldMode::Nullable);
    }

    #[test]
    fn test_set_column_nullable_not_found() {
        let mut table = create_test_table();
        let result = table.set_column_nullable("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_set_column_default() {
        let mut table = create_test_table();
        table
            .set_column_default("name", Value::String("default".to_string()))
            .unwrap();
        let field = table
            .schema()
            .fields()
            .iter()
            .find(|f| f.name == "name")
            .unwrap();
        assert_eq!(
            field.default_value,
            Some(Value::String("default".to_string()))
        );
    }

    #[test]
    fn test_set_column_default_not_found() {
        let mut table = create_test_table();
        let result = table.set_column_default("nonexistent", Value::Int64(0));
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_column_default() {
        let mut table = create_test_table();
        table
            .set_column_default("name", Value::String("default".to_string()))
            .unwrap();
        table.drop_column_default("name").unwrap();
        let field = table
            .schema()
            .fields()
            .iter()
            .find(|f| f.name == "name")
            .unwrap();
        assert_eq!(field.default_value, None);
    }

    #[test]
    fn test_drop_column_default_not_found() {
        let mut table = create_test_table();
        let result = table.drop_column_default("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_set_column_data_type_compatible() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Numeric(None))]);
        let mut table = Table::new(schema);
        table
            .push_row(vec![Value::Numeric(Decimal::from(10))])
            .unwrap();
        table
            .set_column_data_type("val", DataType::Numeric(Some((10, 2))))
            .unwrap();
        let field = table
            .schema()
            .fields()
            .iter()
            .find(|f| f.name == "val")
            .unwrap();
        assert_eq!(field.data_type, DataType::Numeric(Some((10, 2))));
    }

    #[test]
    fn test_set_column_data_type_int64_to_numeric() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let mut table = Table::new(schema);
        table.push_row(vec![Value::Int64(42)]).unwrap();
        table.push_row(vec![Value::Null]).unwrap();
        table
            .set_column_data_type("val", DataType::Numeric(None))
            .unwrap();
        let row0 = table.get_row(0).unwrap();
        assert_eq!(row0.values()[0], Value::Numeric(Decimal::from(42)));
        let row1 = table.get_row(1).unwrap();
        assert_eq!(row1.values()[0], Value::Null);
    }

    #[test]
    fn test_set_column_data_type_float64_to_numeric() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let mut table = Table::new(schema);
        table.push_row(vec![Value::float64(3.15)]).unwrap();
        table
            .set_column_data_type("val", DataType::Numeric(None))
            .unwrap();
        let row = table.get_row(0).unwrap();
        match &row.values()[0] {
            Value::Numeric(d) => {
                assert!(*d > Decimal::from(3) && *d < Decimal::from(4));
            }
            _ => panic!("Expected Numeric"),
        }
    }

    #[test]
    fn test_set_column_data_type_numeric_to_numeric() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Numeric(None))]);
        let mut table = Table::new(schema);
        table
            .push_row(vec![Value::Numeric(Decimal::from(100))])
            .unwrap();
        table
            .set_column_data_type("val", DataType::Numeric(Some((10, 2))))
            .unwrap();
    }

    #[test]
    fn test_set_column_data_type_not_found() {
        let mut table = create_test_table();
        let result = table.set_column_data_type("nonexistent", DataType::String);
        assert!(result.is_err());
    }

    #[test]
    fn test_set_column_data_type_unsupported_conversion() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let mut table = Table::new(schema);
        table
            .push_row(vec![Value::String("hello".to_string())])
            .unwrap();
        let result = table.set_column_data_type("val", DataType::Bool);
        assert!(result.is_err());
    }

    #[test]
    fn test_set_column_collation() {
        let mut table = create_test_table();
        table
            .set_column_collation("name", "unicode:ci".to_string())
            .unwrap();
        let field = table
            .schema()
            .fields()
            .iter()
            .find(|f| f.name == "name")
            .unwrap();
        assert_eq!(field.collation, Some("unicode:ci".to_string()));
    }

    #[test]
    fn test_set_column_collation_not_found() {
        let mut table = create_test_table();
        let result = table.set_column_collation("nonexistent", "unicode:ci".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_with_schema() {
        let table = create_test_table();
        let new_schema = Schema::from_fields(vec![
            Field::nullable("new_id", DataType::Int64),
            Field::nullable("new_name", DataType::String),
            Field::nullable("new_score", DataType::Float64),
        ]);
        let new_table = table.with_schema(new_schema.clone());
        assert_eq!(new_table.schema(), &new_schema);
        assert_eq!(new_table.row_count(), 3);
        assert!(new_table.column_by_name("new_id").is_some());
    }

    #[test]
    fn test_with_reordered_schema() {
        let table = create_test_table();
        let new_schema = Schema::from_fields(vec![
            Field::nullable("score", DataType::Float64),
            Field::nullable("id", DataType::Int64),
        ]);
        let new_table = table.with_reordered_schema(new_schema, &[2, 0]);
        assert_eq!(new_table.num_columns(), 2);
        assert_eq!(new_table.row_count(), 3);
        let row = new_table.get_row(0).unwrap();
        assert_eq!(row.values()[0], Value::float64(95.5));
        assert_eq!(row.values()[1], Value::Int64(1));
    }

    #[test]
    fn test_with_reordered_schema_out_of_bounds_index() {
        let table = create_test_table();
        let new_schema = Schema::from_fields(vec![Field::nullable("id", DataType::Int64)]);
        let new_table = table.with_reordered_schema(new_schema, &[100]);
        assert_eq!(new_table.num_columns(), 0);
    }

    #[test]
    fn test_to_query_result() {
        let table = create_test_table();
        let result = table.to_query_result().unwrap();
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.schema.len(), 3);
    }

    #[test]
    fn test_filter_by_mask() {
        let table = create_test_table();
        let mask = Column::Bool {
            data: vec![true, false, true],
            nulls: NullBitmap::new_valid(3),
        };
        let filtered = table.filter_by_mask(&mask).unwrap();
        assert_eq!(filtered.row_count(), 2);
        let row0 = filtered.get_row(0).unwrap();
        assert_eq!(row0.values()[0], Value::Int64(1));
        let row1 = filtered.get_row(1).unwrap();
        assert_eq!(row1.values()[0], Value::Int64(3));
    }

    #[test]
    fn test_filter_by_mask_with_nulls() {
        let table = create_test_table();
        let mut nulls = NullBitmap::new();
        nulls.push(false);
        nulls.push(true);
        nulls.push(false);
        let mask = Column::Bool {
            data: vec![true, true, true],
            nulls,
        };
        let filtered = table.filter_by_mask(&mask).unwrap();
        assert_eq!(filtered.row_count(), 2);
    }

    #[test]
    fn test_filter_by_mask_non_bool() {
        let table = create_test_table();
        let mask = Column::new(&DataType::Int64);
        let result = table.filter_by_mask(&mask);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("filter_by_mask requires a Bool column as mask")
        );
    }

    #[test]
    fn test_gather_rows() {
        let table = create_test_table();
        let gathered = table.gather_rows(&[2, 0]);
        assert_eq!(gathered.row_count(), 2);
        let row0 = gathered.get_row(0).unwrap();
        assert_eq!(row0.values()[0], Value::Int64(3));
        let row1 = gathered.get_row(1).unwrap();
        assert_eq!(row1.values()[0], Value::Int64(1));
    }

    #[test]
    fn test_reorder_by_indices() {
        let table = create_test_table();
        let reordered = table.reorder_by_indices(&[1, 2, 0]);
        assert_eq!(reordered.row_count(), 3);
        let row0 = reordered.get_row(0).unwrap();
        assert_eq!(row0.values()[0], Value::Int64(2));
    }

    #[test]
    fn test_concat() {
        let table1 = create_test_table();
        let table2 = create_test_table();
        let concatenated = table1.concat(&table2);
        assert_eq!(concatenated.row_count(), 6);
    }

    #[test]
    fn test_concat_tables_empty() {
        let schema = create_test_schema();
        let result = Table::concat_tables(schema.clone(), &[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_concat_tables_single() {
        let table = create_test_table();
        let schema = table.schema().clone();
        let result = Table::concat_tables(schema, &[&table]);
        assert_eq!(result.row_count(), 3);
    }

    #[test]
    fn test_concat_tables_multiple() {
        let table1 = create_test_table();
        let table2 = create_test_table();
        let schema = table1.schema().clone();
        let result = Table::concat_tables(schema, &[&table1, &table2]);
        assert_eq!(result.row_count(), 6);
    }

    #[test]
    fn test_add_column_trait() {
        let mut table = create_test_table();
        let field = Field::nullable("new_col", DataType::Int64);
        table.add_column(field, Some(Value::Int64(42))).unwrap();
        assert_eq!(table.num_columns(), 4);
        let col = table.column_by_name("new_col").unwrap();
        assert_eq!(col.len(), 3);
        assert_eq!(col.get_value(0), Value::Int64(42));
    }

    #[test]
    fn test_add_column_with_null_default() {
        let mut table = create_test_table();
        let field = Field::nullable("new_col", DataType::String);
        table.add_column(field, None).unwrap();
        let col = table.column_by_name("new_col").unwrap();
        assert_eq!(col.get_value(0), Value::Null);
    }

    #[test]
    fn test_table_with_nulls() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ]);
        let mut table = Table::new(schema);
        table.push_row(vec![Value::Int64(1), Value::Null]).unwrap();
        table
            .push_row(vec![Value::Null, Value::String("Bob".to_string())])
            .unwrap();
        assert_eq!(table.row_count(), 2);
        let row0 = table.get_row(0).unwrap();
        assert_eq!(row0.values()[1], Value::Null);
        let row1 = table.get_row(1).unwrap();
        assert_eq!(row1.values()[0], Value::Null);
    }

    #[test]
    fn test_set_column_data_type_string_to_numeric_error() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let mut table = Table::new(schema);
        table
            .push_row(vec![Value::String("not_a_number".to_string())])
            .unwrap();
        let result = table.set_column_data_type("val", DataType::Numeric(None));
        assert!(result.is_err());
    }

    #[test]
    fn test_concat_with_missing_column() {
        let schema1 = Schema::from_fields(vec![
            Field::nullable("a", DataType::Int64),
            Field::nullable("b", DataType::String),
        ]);
        let schema2 = Schema::from_fields(vec![Field::nullable("a", DataType::Int64)]);

        let mut table1 = Table::new(schema1);
        table1
            .push_row(vec![Value::Int64(1), Value::String("x".to_string())])
            .unwrap();

        let mut table2 = Table::new(schema2);
        table2.push_row(vec![Value::Int64(2)]).unwrap();

        let concatenated = table1.concat(&table2);
        assert_eq!(concatenated.row_count(), 2);
        assert_eq!(concatenated.num_columns(), 2);
    }

    #[test]
    fn test_set_column_nullable_case_insensitive() {
        let schema = Schema::from_fields(vec![Field::required("ID", DataType::Int64)]);
        let mut table = Table::new(schema);
        table.push_row(vec![Value::Int64(1)]).unwrap();
        table.set_column_nullable("id").unwrap();
    }

    #[test]
    fn test_set_column_default_case_insensitive() {
        let mut table = create_test_table();
        table
            .set_column_default("NAME", Value::String("default".to_string()))
            .unwrap();
    }

    #[test]
    fn test_drop_column_default_case_insensitive() {
        let mut table = create_test_table();
        table
            .set_column_default("name", Value::String("default".to_string()))
            .unwrap();
        table.drop_column_default("NAME").unwrap();
    }

    #[test]
    fn test_set_column_collation_case_insensitive() {
        let mut table = create_test_table();
        table
            .set_column_collation("NAME", "unicode:ci".to_string())
            .unwrap();
    }

    #[test]
    fn test_set_column_data_type_case_insensitive() {
        let schema = Schema::from_fields(vec![Field::nullable("VAL", DataType::Int64)]);
        let mut table = Table::new(schema);
        table.push_row(vec![Value::Int64(42)]).unwrap();
        table
            .set_column_data_type("val", DataType::Numeric(None))
            .unwrap();
    }

    #[test]
    fn test_remove_row_out_of_bounds() {
        let mut table = create_test_table();
        assert_eq!(table.row_count(), 3);
        table.remove_row(100);
        assert_eq!(table.row_count(), 3);
    }

    #[test]
    fn test_remove_row_first() {
        let mut table = create_test_table();
        table.remove_row(0);
        assert_eq!(table.row_count(), 2);
        let row0 = table.get_row(0).unwrap();
        assert_eq!(row0.values()[0], Value::Int64(2));
    }

    #[test]
    fn test_remove_row_last() {
        let mut table = create_test_table();
        table.remove_row(2);
        assert_eq!(table.row_count(), 2);
        let row1 = table.get_row(1).unwrap();
        assert_eq!(row1.values()[0], Value::Int64(2));
    }

    #[test]
    fn test_gather_rows_empty() {
        let table = create_test_table();
        let gathered = table.gather_rows(&[]);
        assert_eq!(gathered.row_count(), 0);
        assert_eq!(gathered.num_columns(), 3);
    }

    #[test]
    fn test_gather_rows_duplicate_indices() {
        let table = create_test_table();
        let gathered = table.gather_rows(&[0, 0, 1, 1]);
        assert_eq!(gathered.row_count(), 4);
        let row0 = gathered.get_row(0).unwrap();
        let row1 = gathered.get_row(1).unwrap();
        assert_eq!(row0.values()[0], Value::Int64(1));
        assert_eq!(row1.values()[0], Value::Int64(1));
    }

    #[test]
    fn test_filter_by_mask_all_false() {
        let table = create_test_table();
        let mask = Column::Bool {
            data: vec![false, false, false],
            nulls: NullBitmap::new_valid(3),
        };
        let filtered = table.filter_by_mask(&mask).unwrap();
        assert_eq!(filtered.row_count(), 0);
    }

    #[test]
    fn test_filter_by_mask_all_true() {
        let table = create_test_table();
        let mask = Column::Bool {
            data: vec![true, true, true],
            nulls: NullBitmap::new_valid(3),
        };
        let filtered = table.filter_by_mask(&mask).unwrap();
        assert_eq!(filtered.row_count(), 3);
    }

    #[test]
    fn test_filter_by_mask_all_null() {
        let table = create_test_table();
        let mask = Column::Bool {
            data: vec![true, true, true],
            nulls: NullBitmap::new_null(3),
        };
        let filtered = table.filter_by_mask(&mask).unwrap();
        assert_eq!(filtered.row_count(), 0);
    }

    #[test]
    fn test_types_compatible_string() {
        assert!(Table::types_compatible(
            &DataType::String,
            &DataType::String
        ));
    }

    #[test]
    fn test_types_compatible_int64() {
        assert!(Table::types_compatible(&DataType::Int64, &DataType::Int64));
    }

    #[test]
    fn test_types_compatible_float64() {
        assert!(Table::types_compatible(
            &DataType::Float64,
            &DataType::Float64
        ));
    }

    #[test]
    fn test_types_compatible_numeric() {
        assert!(Table::types_compatible(
            &DataType::Numeric(None),
            &DataType::Numeric(Some((10, 2)))
        ));
    }

    #[test]
    fn test_types_not_compatible() {
        assert!(!Table::types_compatible(
            &DataType::String,
            &DataType::Int64
        ));
        assert!(!Table::types_compatible(
            &DataType::Int64,
            &DataType::Float64
        ));
        assert!(!Table::types_compatible(&DataType::Bool, &DataType::String));
    }

    #[test]
    fn test_serde_roundtrip() {
        let table = create_test_table();
        let serialized = serde_json::to_string(&table).unwrap();
        let deserialized: Table = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.row_count(), 3);
        assert_eq!(deserialized.num_columns(), 3);
        let row0 = deserialized.get_row(0).unwrap();
        assert_eq!(row0.values()[0], Value::Int64(1));
    }

    #[test]
    fn test_clone() {
        let table = create_test_table();
        let cloned = table.clone();
        assert_eq!(cloned.row_count(), table.row_count());
        assert_eq!(cloned.num_columns(), table.num_columns());
    }

    #[test]
    fn test_partial_eq() {
        let table1 = create_test_table();
        let table2 = create_test_table();
        assert_eq!(table1, table2);

        let schema = create_test_schema();
        let table3 = Table::new(schema);
        assert_ne!(table1, table3);
    }

    #[test]
    fn test_with_schema_empty_table() {
        let schema = create_test_schema();
        let table = Table::new(schema);
        let new_schema = Schema::from_fields(vec![
            Field::nullable("new_id", DataType::Int64),
            Field::nullable("new_name", DataType::String),
            Field::nullable("new_score", DataType::Float64),
        ]);
        let new_table = table.with_schema(new_schema.clone());
        assert_eq!(new_table.schema(), &new_schema);
        assert!(new_table.is_empty());
    }

    #[test]
    fn test_add_column_to_empty_table() {
        let schema = Schema::from_fields(vec![Field::nullable("id", DataType::Int64)]);
        let mut table = Table::new(schema);
        let field = Field::nullable("new_col", DataType::String);
        table
            .add_column(field, Some(Value::String("default".to_string())))
            .unwrap();
        assert_eq!(table.num_columns(), 2);
        assert!(table.is_empty());
    }

    #[test]
    fn test_concat_empty_tables() {
        let schema = create_test_schema();
        let table1 = Table::new(schema.clone());
        let table2 = Table::new(schema);
        let concatenated = table1.concat(&table2);
        assert!(concatenated.is_empty());
    }

    #[test]
    fn test_concat_one_empty() {
        let table1 = create_test_table();
        let schema = create_test_schema();
        let table2 = Table::new(schema);
        let concatenated = table1.concat(&table2);
        assert_eq!(concatenated.row_count(), 3);
    }

    #[test]
    fn test_set_column_data_type_string_string_compatible() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let mut table = Table::new(schema);
        table
            .push_row(vec![Value::String("hello".to_string())])
            .unwrap();
        table.set_column_data_type("val", DataType::String).unwrap();
        let row = table.get_row(0).unwrap();
        assert_eq!(row.values()[0], Value::String("hello".to_string()));
    }

    #[test]
    fn test_set_column_data_type_int64_int64_compatible() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let mut table = Table::new(schema);
        table.push_row(vec![Value::Int64(42)]).unwrap();
        table.set_column_data_type("val", DataType::Int64).unwrap();
        let row = table.get_row(0).unwrap();
        assert_eq!(row.values()[0], Value::Int64(42));
    }

    #[test]
    fn test_set_column_data_type_float64_float64_compatible() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let mut table = Table::new(schema);
        table.push_row(vec![Value::float64(3.15)]).unwrap();
        table
            .set_column_data_type("val", DataType::Float64)
            .unwrap();
        let row = table.get_row(0).unwrap();
        assert_eq!(row.values()[0], Value::float64(3.15));
    }

    #[test]
    fn test_convert_column_float64_large_value() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let mut table = Table::new(schema);
        table.push_row(vec![Value::float64(1e20)]).unwrap();
        table
            .set_column_data_type("val", DataType::Numeric(None))
            .unwrap();
    }

    #[test]
    fn test_to_records_empty_table() {
        let schema = create_test_schema();
        let table = Table::new(schema);
        let records = table.to_records().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_rows_empty_table() {
        let schema = create_test_schema();
        let table = Table::new(schema);
        let rows = table.rows().unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_from_records_empty() {
        let schema = create_test_schema();
        let records: Vec<Record> = vec![];
        let table = Table::from_records(schema, records).unwrap();
        assert!(table.is_empty());
    }

    #[test]
    fn test_from_values_empty() {
        let schema = create_test_schema();
        let values: Vec<Vec<Value>> = vec![];
        let table = Table::from_values(schema, values).unwrap();
        assert!(table.is_empty());
    }

    #[test]
    fn test_reorder_by_indices_empty() {
        let table = create_test_table();
        let reordered = table.reorder_by_indices(&[]);
        assert!(reordered.is_empty());
    }

    #[test]
    fn test_to_query_result_empty() {
        let schema = create_test_schema();
        let table = Table::new(schema);
        let result = table.to_query_result().unwrap();
        assert!(result.rows.is_empty());
        assert_eq!(result.schema.len(), 3);
    }

    #[test]
    fn test_debug_format() {
        let table = create_test_table();
        let debug_str = format!("{:?}", table);
        assert!(debug_str.contains("Table"));
    }

    #[test]
    fn test_with_reordered_schema_partial() {
        let table = create_test_table();
        let new_schema = Schema::from_fields(vec![Field::nullable("score", DataType::Float64)]);
        let new_table = table.with_reordered_schema(new_schema, &[2]);
        assert_eq!(new_table.num_columns(), 1);
        assert_eq!(new_table.row_count(), 3);
    }

    #[test]
    fn test_set_column_not_null_preserves_data() {
        let mut table = create_test_table();
        table.set_column_not_null("name").unwrap();
        let row = table.get_row(0).unwrap();
        assert_eq!(row.values()[1], Value::String("Alice".to_string()));
    }

    #[test]
    fn test_set_column_nullable_preserves_data() {
        let schema = Schema::from_fields(vec![Field::required("id", DataType::Int64)]);
        let mut table = Table::new(schema);
        table.push_row(vec![Value::Int64(42)]).unwrap();
        table.set_column_nullable("id").unwrap();
        let row = table.get_row(0).unwrap();
        assert_eq!(row.values()[0], Value::Int64(42));
    }

    #[test]
    fn test_rename_column_preserves_data() {
        let mut table = create_test_table();
        table.rename_column("name", "full_name").unwrap();
        let row = table.get_row(0).unwrap();
        assert_eq!(row.values()[1], Value::String("Alice".to_string()));
    }

    #[test]
    fn test_drop_column_preserves_other_data() {
        let mut table = create_test_table();
        table.drop_column("name").unwrap();
        let row = table.get_row(0).unwrap();
        assert_eq!(row.values()[0], Value::Int64(1));
        assert_eq!(row.values()[1], Value::float64(95.5));
    }

    #[test]
    fn test_concat_tables_two_tables() {
        let table1 = create_test_table();
        let table2 = create_test_table();
        let schema = table1.schema().clone();
        let result = Table::concat_tables(schema, &[&table1, &table2]);
        assert_eq!(result.row_count(), 6);
    }

    #[test]
    fn test_concat_tables_three_tables() {
        let table1 = create_test_table();
        let table2 = create_test_table();
        let table3 = create_test_table();
        let schema = table1.schema().clone();
        let result = Table::concat_tables(schema, &[&table1, &table2, &table3]);
        assert_eq!(result.row_count(), 9);
    }

    #[test]
    fn test_clear_already_empty() {
        let schema = create_test_schema();
        let mut table = Table::new(schema);
        table.clear();
        assert!(table.is_empty());
    }

    #[test]
    fn test_set_column_nullable_preserves_other_fields() {
        let schema = Schema::from_fields(vec![
            Field::required("a", DataType::Int64),
            Field::required("b", DataType::String),
            Field::required("c", DataType::Float64),
        ]);
        let mut table = Table::new(schema);
        table
            .push_row(vec![
                Value::Int64(1),
                Value::String("test".to_string()),
                Value::float64(1.5),
            ])
            .unwrap();
        table.set_column_nullable("b").unwrap();
        let field_a = table
            .schema()
            .fields()
            .iter()
            .find(|f| f.name == "a")
            .unwrap();
        let field_b = table
            .schema()
            .fields()
            .iter()
            .find(|f| f.name == "b")
            .unwrap();
        let field_c = table
            .schema()
            .fields()
            .iter()
            .find(|f| f.name == "c")
            .unwrap();
        assert_eq!(field_a.mode, FieldMode::Required);
        assert_eq!(field_b.mode, FieldMode::Nullable);
        assert_eq!(field_c.mode, FieldMode::Required);
    }

    #[test]
    fn test_set_column_data_type_preserves_other_fields() {
        let schema = Schema::from_fields(vec![
            Field::nullable("a", DataType::Int64),
            Field::nullable("b", DataType::Int64),
            Field::nullable("c", DataType::String),
        ]);
        let mut table = Table::new(schema);
        table
            .push_row(vec![
                Value::Int64(1),
                Value::Int64(2),
                Value::String("test".to_string()),
            ])
            .unwrap();
        table
            .set_column_data_type("b", DataType::Numeric(None))
            .unwrap();
        let field_a = table
            .schema()
            .fields()
            .iter()
            .find(|f| f.name == "a")
            .unwrap();
        let field_b = table
            .schema()
            .fields()
            .iter()
            .find(|f| f.name == "b")
            .unwrap();
        let field_c = table
            .schema()
            .fields()
            .iter()
            .find(|f| f.name == "c")
            .unwrap();
        assert_eq!(field_a.data_type, DataType::Int64);
        assert_eq!(field_b.data_type, DataType::Numeric(None));
        assert_eq!(field_c.data_type, DataType::String);
    }

    #[test]
    fn test_convert_numeric_value_to_numeric() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let mut table = Table::new(schema);
        table.push_row(vec![Value::Int64(42)]).unwrap();
        table
            .set_column_data_type("val", DataType::Numeric(None))
            .unwrap();
        table
            .set_column_data_type("val", DataType::Numeric(Some((10, 2))))
            .unwrap();
        let row = table.get_row(0).unwrap();
        assert_eq!(row.values()[0], Value::Numeric(Decimal::from(42)));
    }

    #[test]
    fn test_convert_column_with_multiple_values() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let mut table = Table::new(schema);
        table.push_row(vec![Value::Int64(10)]).unwrap();
        table.push_row(vec![Value::Null]).unwrap();
        table.push_row(vec![Value::Int64(30)]).unwrap();
        table
            .set_column_data_type("val", DataType::Numeric(None))
            .unwrap();
        let row0 = table.get_row(0).unwrap();
        let row1 = table.get_row(1).unwrap();
        let row2 = table.get_row(2).unwrap();
        assert_eq!(row0.values()[0], Value::Numeric(Decimal::from(10)));
        assert_eq!(row1.values()[0], Value::Null);
        assert_eq!(row2.values()[0], Value::Numeric(Decimal::from(30)));
    }

    #[test]
    fn test_set_column_data_type_with_multiple_columns() {
        let schema = Schema::from_fields(vec![
            Field::nullable("a", DataType::Int64),
            Field::nullable("b", DataType::Int64),
        ]);
        let mut table = Table::new(schema);
        table
            .push_row(vec![Value::Int64(1), Value::Int64(100)])
            .unwrap();
        table
            .push_row(vec![Value::Int64(2), Value::Int64(200)])
            .unwrap();
        table
            .set_column_data_type("a", DataType::Numeric(None))
            .unwrap();
        let row0 = table.get_row(0).unwrap();
        let row1 = table.get_row(1).unwrap();
        assert_eq!(row0.values()[0], Value::Numeric(Decimal::from(1)));
        assert_eq!(row0.values()[1], Value::Int64(100));
        assert_eq!(row1.values()[0], Value::Numeric(Decimal::from(2)));
        assert_eq!(row1.values()[1], Value::Int64(200));
    }
}
