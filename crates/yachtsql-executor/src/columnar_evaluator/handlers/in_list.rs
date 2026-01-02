#![coverage(off)]

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::Expr;
use yachtsql_storage::{Column, Table};

use crate::columnar_evaluator::ColumnarEvaluator;
use crate::scalar_functions::comparison::values_equal;

pub fn eval_in_list(
    evaluator: &ColumnarEvaluator,
    expr: &Expr,
    list: &[Expr],
    negated: bool,
    table: &Table,
) -> Result<Column> {
    let val_col = evaluator.evaluate(expr, table)?;

    let list_cols: Vec<Column> = list
        .iter()
        .map(|e| evaluator.evaluate(e, table))
        .collect::<Result<_>>()?;

    let n = table.row_count();
    let mut results = Vec::with_capacity(n);

    for i in 0..n {
        let v = val_col.get_value(i);
        if v.is_null() {
            results.push(Value::Null);
            continue;
        }

        let mut found = false;
        let mut has_null = false;
        for list_col in &list_cols {
            let list_val = list_col.get_value(i);
            if list_val.is_null() {
                has_null = true;
            } else if values_equal(&v, &list_val) {
                found = true;
                break;
            }
        }

        let result = if found {
            Value::Bool(!negated)
        } else if has_null {
            Value::Null
        } else {
            Value::Bool(negated)
        };
        results.push(result);
    }
    Ok(Column::from_values(&results))
}
