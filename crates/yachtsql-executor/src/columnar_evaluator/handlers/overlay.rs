#![coverage(off)]

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::Expr;
use yachtsql_storage::{Column, Table};

use crate::columnar_evaluator::ColumnarEvaluator;

pub fn eval_overlay(
    evaluator: &ColumnarEvaluator,
    string: &Expr,
    placing: &Expr,
    from: &Expr,
    for_length: Option<&Expr>,
    table: &Table,
) -> Result<Column> {
    let str_col = evaluator.evaluate(string, table)?;
    let placing_col = evaluator.evaluate(placing, table)?;
    let from_col = evaluator.evaluate(from, table)?;
    let len_col = for_length
        .map(|e| evaluator.evaluate(e, table))
        .transpose()?;

    let n = table.row_count();
    let mut results = Vec::with_capacity(n);

    for i in 0..n {
        let s = str_col.get_value(i);
        let replacement = placing_col.get_value(i);
        let start = from_col.get_value(i);
        let length = len_col.as_ref().map(|c| c.get_value(i));

        match (&s, &replacement, &start) {
            (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => {
                results.push(Value::Null);
            }
            (Value::String(s), Value::String(r), Value::Int64(start)) => {
                let start_idx = (*start as usize).saturating_sub(1);
                let len = match length {
                    Some(Value::Int64(l)) => l as usize,
                    Some(Value::Null) => {
                        results.push(Value::Null);
                        continue;
                    }
                    None => r.chars().count(),
                    Some(_) => r.chars().count(),
                };

                let chars: Vec<char> = s.chars().collect();
                let mut result = String::new();
                result.extend(chars.iter().take(start_idx));
                result.push_str(r);
                result.extend(chars.iter().skip(start_idx + len));
                results.push(Value::String(result));
            }
            _ => results.push(Value::Null),
        }
    }
    Ok(Column::from_values(&results))
}
