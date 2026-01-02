#![coverage(off)]

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::types::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

impl ColumnInfo {
    pub fn new(name: impl Into<String>, data_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Row {
    values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn into_values(self) -> Vec<Value> {
        self.values
    }

    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Value> {
        self.values.iter()
    }

    pub fn to_json(&self) -> Vec<JsonValue> {
        self.values.iter().map(|v| v.to_json()).collect()
    }
}

impl From<Vec<Value>> for Row {
    fn from(values: Vec<Value>) -> Self {
        Self::new(values)
    }
}

impl<'a> IntoIterator for &'a Row {
    type Item = &'a Value;
    type IntoIter = std::slice::Iter<'a, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}

impl IntoIterator for Row {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueryResult {
    pub schema: Vec<ColumnInfo>,
    pub rows: Vec<Row>,
}

impl QueryResult {
    pub fn new(schema: Vec<ColumnInfo>, rows: Vec<Row>) -> Self {
        Self { schema, rows }
    }

    pub fn from_values(schema: Vec<ColumnInfo>, rows: Vec<Vec<Value>>) -> Self {
        Self {
            schema,
            rows: rows.into_iter().map(Row::new).collect(),
        }
    }

    pub fn empty() -> Self {
        Self::default()
    }

    pub fn with_schema(schema: Vec<ColumnInfo>) -> Self {
        Self {
            schema,
            rows: Vec::new(),
        }
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn column_count(&self) -> usize {
        self.schema.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.schema.iter().map(|c| c.name.as_str()).collect()
    }

    pub fn get(&self, row: usize, col: usize) -> Option<&Value> {
        self.rows.get(row).and_then(|r| r.get(col))
    }

    pub fn get_by_name(&self, row: usize, col_name: &str) -> Option<&Value> {
        let col_idx = self.schema.iter().position(|c| c.name == col_name)?;
        self.get(row, col_idx)
    }

    pub fn first_row(&self) -> Option<&Row> {
        self.rows.first()
    }

    pub fn first_value(&self) -> Option<&Value> {
        self.rows.first().and_then(|r| r.get(0))
    }

    pub fn to_json_rows(&self) -> Vec<Vec<JsonValue>> {
        self.rows.iter().map(|row| row.to_json()).collect()
    }

    pub fn to_bq_response(&self) -> JsonValue {
        let schema_fields: Vec<JsonValue> = self
            .schema
            .iter()
            .map(|col| serde_json::json!({ "name": col.name, "type": col.data_type }))
            .collect();

        let rows: Vec<JsonValue> = self
            .rows
            .iter()
            .map(|row| {
                let fields: Vec<JsonValue> = row
                    .iter()
                    .map(|v| serde_json::json!({ "v": v.to_json() }))
                    .collect();
                serde_json::json!({ "f": fields })
            })
            .collect();

        serde_json::json!({
            "kind": "bigquery#queryResponse",
            "schema": { "fields": schema_fields },
            "rows": rows,
            "totalRows": self.rows.len().to_string(),
            "jobComplete": true
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_info_new() {
        let col = ColumnInfo::new("name", "STRING");
        assert_eq!(col.name, "name");
        assert_eq!(col.data_type, "STRING");
    }

    #[test]
    fn test_row_new() {
        let row = Row::new(vec![Value::Int64(1), Value::String("hello".to_string())]);
        assert_eq!(row.len(), 2);
        assert!(!row.is_empty());
    }

    #[test]
    fn test_row_empty() {
        let row = Row::default();
        assert_eq!(row.len(), 0);
        assert!(row.is_empty());
    }

    #[test]
    fn test_row_values() {
        let values = vec![Value::Int64(1), Value::Int64(2)];
        let row = Row::new(values.clone());
        assert_eq!(row.values(), &[Value::Int64(1), Value::Int64(2)]);
    }

    #[test]
    fn test_row_into_values() {
        let values = vec![Value::Int64(1), Value::Int64(2)];
        let row = Row::new(values.clone());
        assert_eq!(row.into_values(), values);
    }

    #[test]
    fn test_row_get() {
        let row = Row::new(vec![Value::Int64(1), Value::Int64(2)]);
        assert_eq!(row.get(0), Some(&Value::Int64(1)));
        assert_eq!(row.get(1), Some(&Value::Int64(2)));
        assert_eq!(row.get(2), None);
    }

    #[test]
    fn test_row_iter() {
        let row = Row::new(vec![Value::Int64(1), Value::Int64(2)]);
        let collected: Vec<_> = row.iter().collect();
        assert_eq!(collected, vec![&Value::Int64(1), &Value::Int64(2)]);
    }

    #[test]
    fn test_row_to_json() {
        let row = Row::new(vec![Value::Int64(1), Value::String("hello".to_string())]);
        let json = row.to_json();
        assert_eq!(json, vec![serde_json::json!(1), serde_json::json!("hello")]);
    }

    #[test]
    fn test_row_from_vec() {
        let values = vec![Value::Int64(1), Value::Int64(2)];
        let row: Row = values.into();
        assert_eq!(row.len(), 2);
    }

    #[test]
    fn test_row_into_iterator() {
        let row = Row::new(vec![Value::Int64(1), Value::Int64(2)]);
        let collected: Vec<_> = row.into_iter().collect();
        assert_eq!(collected, vec![Value::Int64(1), Value::Int64(2)]);
    }

    #[test]
    fn test_row_ref_into_iterator() {
        let row = Row::new(vec![Value::Int64(1), Value::Int64(2)]);
        let collected: Vec<_> = (&row).into_iter().collect();
        assert_eq!(collected, vec![&Value::Int64(1), &Value::Int64(2)]);
    }

    #[test]
    fn test_query_result_new() {
        let schema = vec![ColumnInfo::new("a", "INT64")];
        let rows = vec![Row::new(vec![Value::Int64(1)])];
        let result = QueryResult::new(schema, rows);
        assert_eq!(result.row_count(), 1);
        assert_eq!(result.column_count(), 1);
    }

    #[test]
    fn test_query_result_from_values() {
        let schema = vec![ColumnInfo::new("a", "INT64")];
        let rows = vec![vec![Value::Int64(1)], vec![Value::Int64(2)]];
        let result = QueryResult::from_values(schema, rows);
        assert_eq!(result.row_count(), 2);
    }

    #[test]
    fn test_query_result_empty() {
        let result = QueryResult::empty();
        assert!(result.is_empty());
        assert_eq!(result.row_count(), 0);
        assert_eq!(result.column_count(), 0);
    }

    #[test]
    fn test_query_result_with_schema() {
        let schema = vec![
            ColumnInfo::new("a", "INT64"),
            ColumnInfo::new("b", "STRING"),
        ];
        let result = QueryResult::with_schema(schema);
        assert!(result.is_empty());
        assert_eq!(result.column_count(), 2);
    }

    #[test]
    fn test_query_result_column_names() {
        let schema = vec![
            ColumnInfo::new("a", "INT64"),
            ColumnInfo::new("b", "STRING"),
        ];
        let result = QueryResult::with_schema(schema);
        assert_eq!(result.column_names(), vec!["a", "b"]);
    }

    #[test]
    fn test_query_result_get() {
        let schema = vec![
            ColumnInfo::new("a", "INT64"),
            ColumnInfo::new("b", "STRING"),
        ];
        let rows = vec![
            Row::new(vec![Value::Int64(1), Value::String("hello".to_string())]),
            Row::new(vec![Value::Int64(2), Value::String("world".to_string())]),
        ];
        let result = QueryResult::new(schema, rows);

        assert_eq!(result.get(0, 0), Some(&Value::Int64(1)));
        assert_eq!(result.get(0, 1), Some(&Value::String("hello".to_string())));
        assert_eq!(result.get(1, 0), Some(&Value::Int64(2)));
        assert_eq!(result.get(1, 1), Some(&Value::String("world".to_string())));
        assert_eq!(result.get(2, 0), None);
        assert_eq!(result.get(0, 2), None);
    }

    #[test]
    fn test_query_result_get_by_name() {
        let schema = vec![
            ColumnInfo::new("a", "INT64"),
            ColumnInfo::new("b", "STRING"),
        ];
        let rows = vec![Row::new(vec![
            Value::Int64(1),
            Value::String("hello".to_string()),
        ])];
        let result = QueryResult::new(schema, rows);

        assert_eq!(result.get_by_name(0, "a"), Some(&Value::Int64(1)));
        assert_eq!(
            result.get_by_name(0, "b"),
            Some(&Value::String("hello".to_string()))
        );
        assert_eq!(result.get_by_name(0, "c"), None);
        assert_eq!(result.get_by_name(1, "a"), None);
    }

    #[test]
    fn test_query_result_first_row() {
        let schema = vec![ColumnInfo::new("a", "INT64")];
        let rows = vec![
            Row::new(vec![Value::Int64(1)]),
            Row::new(vec![Value::Int64(2)]),
        ];
        let result = QueryResult::new(schema, rows);

        let first = result.first_row().unwrap();
        assert_eq!(first.get(0), Some(&Value::Int64(1)));

        let empty = QueryResult::empty();
        assert!(empty.first_row().is_none());
    }

    #[test]
    fn test_query_result_first_value() {
        let schema = vec![ColumnInfo::new("a", "INT64")];
        let rows = vec![Row::new(vec![Value::Int64(42)])];
        let result = QueryResult::new(schema, rows);

        assert_eq!(result.first_value(), Some(&Value::Int64(42)));

        let empty = QueryResult::empty();
        assert!(empty.first_value().is_none());
    }

    #[test]
    fn test_query_result_to_json_rows() {
        let schema = vec![
            ColumnInfo::new("a", "INT64"),
            ColumnInfo::new("b", "STRING"),
        ];
        let rows = vec![
            Row::new(vec![Value::Int64(1), Value::String("hello".to_string())]),
            Row::new(vec![Value::Int64(2), Value::String("world".to_string())]),
        ];
        let result = QueryResult::new(schema, rows);

        let json = result.to_json_rows();
        assert_eq!(json.len(), 2);
        assert_eq!(
            json[0],
            vec![serde_json::json!(1), serde_json::json!("hello")]
        );
        assert_eq!(
            json[1],
            vec![serde_json::json!(2), serde_json::json!("world")]
        );
    }

    #[test]
    fn test_query_result_to_bq_response() {
        let schema = vec![ColumnInfo::new("a", "INT64")];
        let rows = vec![Row::new(vec![Value::Int64(1)])];
        let result = QueryResult::new(schema, rows);

        let response = result.to_bq_response();
        assert_eq!(response["kind"], "bigquery#queryResponse");
        assert_eq!(response["totalRows"], "1");
        assert_eq!(response["jobComplete"], true);
        assert!(response["schema"]["fields"].is_array());
        assert!(response["rows"].is_array());
    }

    #[test]
    fn test_query_result_default() {
        let result: QueryResult = Default::default();
        assert!(result.is_empty());
        assert_eq!(result.column_count(), 0);
    }

    #[test]
    fn test_column_info_serde() {
        let col = ColumnInfo::new("name", "STRING");
        let json = serde_json::to_string(&col).unwrap();
        let deserialized: ColumnInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, "name");
        assert_eq!(deserialized.data_type, "STRING");
    }

    #[test]
    fn test_column_info_debug() {
        let col = ColumnInfo::new("name", "STRING");
        let debug = format!("{:?}", col);
        assert!(debug.contains("name"));
        assert!(debug.contains("STRING"));
    }

    #[test]
    fn test_row_debug() {
        let row = Row::new(vec![Value::Int64(1)]);
        let debug = format!("{:?}", row);
        assert!(debug.contains("Row"));
    }

    #[test]
    fn test_query_result_debug() {
        let result = QueryResult::empty();
        let debug = format!("{:?}", result);
        assert!(debug.contains("QueryResult"));
    }
}
