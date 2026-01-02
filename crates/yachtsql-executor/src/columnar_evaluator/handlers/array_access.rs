#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, ScalarFunction};
use yachtsql_storage::{Column, Table};

use crate::columnar_evaluator::ColumnarEvaluator;

pub fn eval_array_access(
    evaluator: &ColumnarEvaluator,
    array: &Expr,
    index: &Expr,
    _zero_indexed: bool,
    table: &Table,
) -> Result<Column> {
    let arr_col = evaluator.evaluate(array, table)?;

    let (idx_col, access_mode) = match index {
        Expr::ScalarFunction { name, args } if args.len() == 1 => {
            let idx_col = evaluator.evaluate(&args[0], table)?;
            let mode = match name {
                ScalarFunction::ArrayOffset => AccessMode::Offset,
                ScalarFunction::ArrayOrdinal => AccessMode::Ordinal,
                ScalarFunction::SafeOffset => AccessMode::SafeOffset,
                ScalarFunction::SafeOrdinal => AccessMode::SafeOrdinal,
                _ => {
                    let idx_col = evaluator.evaluate(index, table)?;
                    return eval_access(&arr_col, &idx_col, AccessMode::Default, table.row_count());
                }
            };
            (idx_col, mode)
        }
        _ => {
            let idx_col = evaluator.evaluate(index, table)?;
            (idx_col, AccessMode::Default)
        }
    };

    eval_access(&arr_col, &idx_col, access_mode, table.row_count())
}

#[derive(Copy, Clone)]
enum AccessMode {
    Default,
    Offset,
    Ordinal,
    SafeOffset,
    SafeOrdinal,
}

fn eval_access(arr_col: &Column, idx_col: &Column, mode: AccessMode, n: usize) -> Result<Column> {
    let mut results = Vec::with_capacity(n);

    for i in 0..n {
        let arr = arr_col.get_value(i);
        let idx = idx_col.get_value(i);

        match (&arr, &idx) {
            (Value::Null, _) | (_, Value::Null) => results.push(Value::Null),
            (Value::Array(elements), Value::Int64(idx)) => {
                let (actual_idx, safe) = match mode {
                    AccessMode::Default => ((*idx as usize).saturating_sub(1), true),
                    AccessMode::Offset => (*idx as usize, false),
                    AccessMode::SafeOffset => (*idx as usize, true),
                    AccessMode::Ordinal => ((*idx as usize).saturating_sub(1), false),
                    AccessMode::SafeOrdinal => ((*idx as usize).saturating_sub(1), true),
                };

                if *idx < 0 || actual_idx >= elements.len() {
                    if safe {
                        results.push(Value::Null);
                    } else {
                        return Err(Error::InvalidQuery(format!(
                            "Array index {} out of bounds for array of length {}",
                            idx,
                            elements.len()
                        )));
                    }
                } else {
                    results.push(elements[actual_idx].clone());
                }
            }
            (Value::Json(json), Value::Int64(idx)) => {
                if let Some(arr) = json.as_array() {
                    let actual_idx = *idx as usize;
                    if *idx < 0 || actual_idx >= arr.len() {
                        results.push(Value::Null);
                    } else {
                        results.push(Value::Json(arr[actual_idx].clone()));
                    }
                } else {
                    results.push(Value::Null);
                }
            }
            (Value::Json(json), Value::String(key)) => {
                if let Some(obj) = json.as_object() {
                    results.push(
                        obj.get(key)
                            .map(|v| Value::Json(v.clone()))
                            .unwrap_or(Value::Null),
                    );
                } else {
                    results.push(Value::Null);
                }
            }
            _ => results.push(Value::Null),
        }
    }
    Ok(Column::from_values(&results))
}
