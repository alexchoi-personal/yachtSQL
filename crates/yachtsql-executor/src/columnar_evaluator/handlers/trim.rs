#![coverage(off)]

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, TrimWhere};
use yachtsql_storage::{Column, Table};

use crate::columnar_evaluator::ColumnarEvaluator;

pub fn eval_trim(
    evaluator: &ColumnarEvaluator,
    expr: &Expr,
    trim_what: Option<&Expr>,
    trim_where: TrimWhere,
    table: &Table,
) -> Result<Column> {
    let str_col = evaluator.evaluate(expr, table)?;
    let chars_col = trim_what
        .map(|e| evaluator.evaluate(e, table))
        .transpose()?;

    let n = table.row_count();
    let mut results = Vec::with_capacity(n);

    for i in 0..n {
        let s = str_col.get_value(i);
        let chars = chars_col.as_ref().map(|c| c.get_value(i));

        match &s {
            Value::Null => results.push(Value::Null),
            Value::String(s) => {
                let trim_chars: Vec<char> = match chars {
                    Some(Value::String(c)) => c.chars().collect(),
                    Some(Value::Null) => {
                        results.push(Value::Null);
                        continue;
                    }
                    None => vec![' '],
                    _ => vec![' '],
                };

                let trimmed = match trim_where {
                    TrimWhere::Both => s.trim_matches(|c| trim_chars.contains(&c)).to_string(),
                    TrimWhere::Leading => s
                        .trim_start_matches(|c| trim_chars.contains(&c))
                        .to_string(),
                    TrimWhere::Trailing => {
                        s.trim_end_matches(|c| trim_chars.contains(&c)).to_string()
                    }
                };
                results.push(Value::String(trimmed));
            }
            _ => results.push(Value::Null),
        }
    }
    Ok(Column::from_values(&results))
}
