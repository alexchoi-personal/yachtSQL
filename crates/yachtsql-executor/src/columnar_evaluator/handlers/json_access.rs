#![coverage(off)]

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::JsonPathElement;
use yachtsql_storage::{Column, Table};

use crate::columnar_evaluator::ColumnarEvaluator;

pub fn eval_json_access(
    evaluator: &ColumnarEvaluator,
    expr: &yachtsql_ir::Expr,
    path: &[JsonPathElement],
    table: &Table,
) -> Result<Column> {
    let json_col = evaluator.evaluate(expr, table)?;
    let n = table.row_count();
    let mut results = Vec::with_capacity(n);

    for i in 0..n {
        let mut current = json_col.get_value(i);

        for element in path {
            current = navigate_json(&current, element);
        }

        results.push(current);
    }

    Ok(Column::from_values(&results))
}

fn navigate_json(json: &Value, element: &JsonPathElement) -> Value {
    match (json, element) {
        (Value::Null, _) => Value::Null,
        (Value::Json(j), JsonPathElement::Key(k)) => j
            .get(k)
            .map(|v| Value::Json(v.clone()))
            .unwrap_or(Value::Null),
        (Value::Json(j), JsonPathElement::Index(idx)) => {
            let index = if *idx >= 0 {
                *idx as usize
            } else {
                return Value::Null;
            };
            j.get(index)
                .map(|v| Value::Json(v.clone()))
                .unwrap_or(Value::Null)
        }
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::DataType;
    use yachtsql_ir::{Expr, Literal};
    use yachtsql_storage::{Field, FieldMode, Schema};

    use super::*;

    fn make_json_table() -> Table {
        let schema = Schema::from_fields(vec![Field::new(
            "data".to_string(),
            DataType::Json,
            FieldMode::Nullable,
        )]);
        let mut table = Table::new(schema);
        table
            .push_row(vec![Value::Json(
                serde_json::json!({"name": "Alice", "age": 30}),
            )])
            .unwrap();
        table
            .push_row(vec![Value::Json(
                serde_json::json!({"name": "Bob", "items": [1, 2, 3]}),
            )])
            .unwrap();
        table.push_row(vec![Value::Null]).unwrap();
        table
    }

    #[test]
    fn test_json_access_key() {
        let table = make_json_table();
        let evaluator = ColumnarEvaluator::new(table.schema());

        let result = eval_json_access(
            &evaluator,
            &Expr::Column {
                table: None,
                name: "data".to_string(),
                index: Some(0),
            },
            &[JsonPathElement::Key("name".to_string())],
            &table,
        )
        .unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.get_value(0), Value::Json(serde_json::json!("Alice")));
        assert_eq!(result.get_value(1), Value::Json(serde_json::json!("Bob")));
        assert_eq!(result.get_value(2), Value::Null);
    }

    #[test]
    fn test_json_access_nested() {
        let table = make_json_table();
        let evaluator = ColumnarEvaluator::new(table.schema());

        let result = eval_json_access(
            &evaluator,
            &Expr::Column {
                table: None,
                name: "data".to_string(),
                index: Some(0),
            },
            &[
                JsonPathElement::Key("items".to_string()),
                JsonPathElement::Index(1),
            ],
            &table,
        )
        .unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Json(serde_json::json!(2)));
        assert_eq!(result.get_value(2), Value::Null);
    }

    #[test]
    fn test_json_access_missing_key() {
        let table = make_json_table();
        let evaluator = ColumnarEvaluator::new(table.schema());

        let result = eval_json_access(
            &evaluator,
            &Expr::Column {
                table: None,
                name: "data".to_string(),
                index: Some(0),
            },
            &[JsonPathElement::Key("nonexistent".to_string())],
            &table,
        )
        .unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Null);
        assert_eq!(result.get_value(2), Value::Null);
    }
}
