#![coverage(off)]

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::Expr;
use yachtsql_storage::{Column, Table};

use crate::columnar_evaluator::ColumnarEvaluator;

pub fn eval_in_unnest(
    evaluator: &ColumnarEvaluator,
    expr: &Expr,
    array_expr: &Expr,
    negated: bool,
    table: &Table,
) -> Result<Column> {
    let val_col = evaluator.evaluate(expr, table)?;
    let array_col = evaluator.evaluate(array_expr, table)?;

    let n = table.row_count();
    if n == 0 {
        return Ok(Column::Bool {
            data: Vec::new(),
            nulls: yachtsql_storage::NullBitmap::new(),
        });
    }
    let mut results = Vec::with_capacity(n);

    for i in 0..n {
        let v = val_col.get_value(i);
        let arr = array_col.get_value(i);

        if v.is_null() {
            results.push(Value::Null);
            continue;
        }

        match arr {
            Value::Null => results.push(Value::Null),
            Value::Array(elements) => {
                let mut found = false;
                let mut has_null = false;
                for elem in &elements {
                    if elem.is_null() {
                        has_null = true;
                    } else if v == *elem {
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
            _ => results.push(Value::Bool(negated)),
        }
    }
    Ok(Column::from_values(&results))
}
