#![coverage(off)]

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::Expr;
use yachtsql_storage::{Column, Table};

use crate::columnar_evaluator::ColumnarEvaluator;

pub fn eval_substring(
    evaluator: &ColumnarEvaluator,
    expr: &Expr,
    start: Option<&Expr>,
    length: Option<&Expr>,
    table: &Table,
) -> Result<Column> {
    let str_col = evaluator.evaluate(expr, table)?;
    let start_col = start.map(|s| evaluator.evaluate(s, table)).transpose()?;
    let len_col = length.map(|l| evaluator.evaluate(l, table)).transpose()?;

    let n = table.row_count();
    let mut results = Vec::with_capacity(n);

    for i in 0..n {
        let s = str_col.get_value(i);
        let start_val = start_col.as_ref().map(|c| c.get_value(i));
        let len_val = len_col.as_ref().map(|c| c.get_value(i));

        match s {
            Value::Null => results.push(Value::Null),
            Value::String(s) => {
                let chars: Vec<char> = s.chars().collect();
                let char_len = chars.len();
                let start_raw = start_val
                    .and_then(|v| match v {
                        Value::Null => None,
                        Value::Int64(i) => Some(i),
                        _ => None,
                    })
                    .unwrap_or(1);

                let start_idx = if start_raw < 0 {
                    char_len.saturating_sub((-start_raw) as usize)
                } else if start_raw == 0 {
                    0
                } else {
                    (start_raw as usize).saturating_sub(1).min(char_len)
                };

                let len = match len_val {
                    Some(Value::Null) => {
                        results.push(Value::Null);
                        continue;
                    }
                    Some(Value::Int64(l)) => l.max(0) as usize,
                    _ => char_len.saturating_sub(start_idx),
                };

                let substr: String = chars.into_iter().skip(start_idx).take(len).collect();
                results.push(Value::String(substr));
            }
            Value::Bytes(b) => {
                let byte_len = b.len();
                let start_raw = start_val
                    .and_then(|v| match v {
                        Value::Null => None,
                        Value::Int64(i) => Some(i),
                        _ => None,
                    })
                    .unwrap_or(1);

                let start_idx = if start_raw < 0 {
                    byte_len.saturating_sub((-start_raw) as usize)
                } else if start_raw == 0 {
                    0
                } else {
                    (start_raw as usize).saturating_sub(1).min(byte_len)
                };

                let len = match len_val {
                    Some(Value::Null) => {
                        results.push(Value::Null);
                        continue;
                    }
                    Some(Value::Int64(l)) => l.max(0) as usize,
                    _ => byte_len.saturating_sub(start_idx),
                };

                let end_idx = (start_idx + len).min(byte_len);
                let result: Vec<u8> = b[start_idx..end_idx].to_vec();
                results.push(Value::Bytes(result));
            }
            _ => results.push(Value::Null),
        }
    }
    Ok(Column::from_values(&results))
}
