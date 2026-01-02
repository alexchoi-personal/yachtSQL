#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::Expr;
use yachtsql_storage::{Column, Table};

use crate::columnar_evaluator::ColumnarEvaluator;

pub fn eval_position(
    evaluator: &ColumnarEvaluator,
    substring: &Expr,
    string: &Expr,
    table: &Table,
) -> Result<Column> {
    let substr_col = evaluator.evaluate(substring, table)?;
    let str_col = evaluator.evaluate(string, table)?;

    let n = table.row_count();
    let mut results = Vec::with_capacity(n);

    for i in 0..n {
        let substr = substr_col.get_value(i);
        let s = str_col.get_value(i);

        match (&substr, &s) {
            (Value::Null, _) | (_, Value::Null) => results.push(Value::Null),
            (Value::String(substr), Value::String(s)) => {
                let pos = s.find(substr.as_str()).map(|p| p + 1).unwrap_or(0) as i64;
                results.push(Value::Int64(pos));
            }
            _ => {
                return Err(Error::InvalidQuery(
                    "POSITION requires string arguments".into(),
                ));
            }
        }
    }
    Ok(Column::from_values(&results))
}
