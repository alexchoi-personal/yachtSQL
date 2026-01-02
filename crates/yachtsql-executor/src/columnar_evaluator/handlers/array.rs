#![coverage(off)]

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::Expr;
use yachtsql_storage::{Column, Table};

use crate::columnar_evaluator::ColumnarEvaluator;

pub fn eval_array(
    evaluator: &ColumnarEvaluator,
    elements: &[Expr],
    table: &Table,
) -> Result<Column> {
    let elem_cols: Vec<Column> = elements
        .iter()
        .map(|e| evaluator.evaluate(e, table))
        .collect::<Result<_>>()?;

    let n = table.row_count();
    let mut results = Vec::with_capacity(n);

    for i in 0..n {
        let arr: Vec<Value> = elem_cols.iter().map(|col| col.get_value(i)).collect();
        results.push(Value::Array(arr));
    }
    Ok(Column::from_values(&results))
}
