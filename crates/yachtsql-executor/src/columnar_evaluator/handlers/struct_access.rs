#![coverage(off)]

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::Expr;
use yachtsql_storage::{Column, Table};

use crate::columnar_evaluator::ColumnarEvaluator;

pub fn eval_struct_access(
    evaluator: &ColumnarEvaluator,
    expr: &Expr,
    field_name: &str,
    table: &Table,
) -> Result<Column> {
    let struct_col = evaluator.evaluate(expr, table)?;

    let n = table.row_count();
    let mut results = Vec::with_capacity(n);

    for i in 0..n {
        let s = struct_col.get_value(i);

        match s {
            Value::Null => results.push(Value::Null),
            Value::Struct(fields) => {
                let value = fields
                    .iter()
                    .find(|(name, _)| name == field_name)
                    .map(|(_, v)| v.clone())
                    .unwrap_or(Value::Null);
                results.push(value);
            }
            _ => results.push(Value::Null),
        }
    }
    Ok(Column::from_values(&results))
}
