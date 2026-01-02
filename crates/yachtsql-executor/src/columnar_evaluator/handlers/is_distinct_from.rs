#![coverage(off)]

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::Expr;
use yachtsql_storage::{Column, Table};

use crate::columnar_evaluator::ColumnarEvaluator;

pub fn eval_is_distinct_from(
    evaluator: &ColumnarEvaluator,
    left: &Expr,
    right: &Expr,
    negated: bool,
    table: &Table,
) -> Result<Column> {
    let left_col = evaluator.evaluate(left, table)?;
    let right_col = evaluator.evaluate(right, table)?;

    let n = table.row_count();
    let mut results = Vec::with_capacity(n);
    for i in 0..n {
        let l = left_col.get_value(i);
        let r = right_col.get_value(i);
        let is_distinct = match (&l, &r) {
            (Value::Null, Value::Null) => false,
            (Value::Null, _) | (_, Value::Null) => true,
            _ => l != r,
        };
        results.push(Value::Bool(if negated {
            !is_distinct
        } else {
            is_distinct
        }));
    }
    Ok(Column::from_values(&results))
}
