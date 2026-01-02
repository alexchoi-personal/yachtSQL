use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{AggregateFunction, Expr, PlanSchema};
use yachtsql_storage::Table;

use super::plan_schema_to_schema;

#[allow(clippy::wildcard_enum_match_arm)]
fn get_simple_column_index(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Column { index, .. } => *index,
        Expr::Alias { expr, .. } => get_simple_column_index(expr),
        Expr::Aggregate { args, .. } if args.len() == 1 => get_simple_column_index(&args[0]),
        _ => None,
    }
}

#[allow(clippy::wildcard_enum_match_arm)]
pub(super) fn can_use_columnar_aggregate(
    aggregates: &[Expr],
    group_by: &[Expr],
    grouping_sets: Option<&Vec<Vec<usize>>>,
) -> bool {
    if !group_by.is_empty() || grouping_sets.is_some() {
        return false;
    }

    aggregates.iter().all(|expr| {
        let (func, distinct, filter, args) = match expr {
            Expr::Aggregate {
                func,
                args,
                distinct,
                filter,
                ..
            } => (func, *distinct, filter.is_some(), args),
            Expr::Alias { expr, .. } => match expr.as_ref() {
                Expr::Aggregate {
                    func,
                    args,
                    distinct,
                    filter,
                    ..
                } => (func, *distinct, filter.is_some(), args),
                _ => return false,
            },
            _ => return false,
        };

        if distinct || filter {
            return false;
        }

        let is_simple_column = args.len() == 1 && get_simple_column_index(&args[0]).is_some();
        let is_count_star = args.is_empty() || matches!(args.first(), Some(Expr::Wildcard { .. }));

        match func {
            AggregateFunction::Count => is_count_star || is_simple_column,
            AggregateFunction::Sum
            | AggregateFunction::Avg
            | AggregateFunction::Min
            | AggregateFunction::Max => is_simple_column,
            _ => false,
        }
    })
}

pub(super) fn execute_columnar_aggregate(
    input_table: &Table,
    aggregates: &[Expr],
    schema: &PlanSchema,
) -> Result<Table> {
    let result_schema = plan_schema_to_schema(schema);
    let mut result = Table::empty(result_schema);
    let mut row: Vec<Value> = Vec::with_capacity(aggregates.len());

    for expr in aggregates {
        let (func, args) = match expr {
            Expr::Aggregate { func, args, .. } => (func, args),
            Expr::Alias { expr, .. } => match expr.as_ref() {
                Expr::Aggregate { func, args, .. } => (func, args),
                _ => {
                    return Err(Error::internal(
                        "Expected Aggregate inside Alias in columnar aggregate",
                    ))
                }
            },
            _ => {
                return Err(Error::internal(
                    "Expected Aggregate expression in columnar aggregate",
                ))
            }
        };

        let value = match func {
            AggregateFunction::Count => {
                if args.is_empty() || matches!(args.first(), Some(Expr::Wildcard { .. })) {
                    Value::Int64(input_table.row_count() as i64)
                } else {
                    let col_idx = get_simple_column_index(&args[0])
                        .ok_or_else(|| Error::internal("Missing column index for COUNT"))?;
                    let column = input_table
                        .column(col_idx)
                        .ok_or_else(|| Error::internal("Column not found for COUNT"))?;
                    Value::Int64(column.count_valid() as i64)
                }
            }
            AggregateFunction::Sum => {
                let col_idx = get_simple_column_index(&args[0])
                    .ok_or_else(|| Error::internal("Missing column index for SUM"))?;
                let column = input_table
                    .column(col_idx)
                    .ok_or_else(|| Error::internal("Column not found for SUM"))?;
                column.sum().map(Value::float64).unwrap_or(Value::Null)
            }
            AggregateFunction::Avg => {
                let col_idx = get_simple_column_index(&args[0])
                    .ok_or_else(|| Error::internal("Missing column index for AVG"))?;
                let column = input_table
                    .column(col_idx)
                    .ok_or_else(|| Error::internal("Column not found for AVG"))?;
                let count = column.count_valid();
                if count == 0 {
                    Value::Null
                } else {
                    column
                        .sum()
                        .map(|s| Value::float64(s / count as f64))
                        .unwrap_or(Value::Null)
                }
            }
            AggregateFunction::Min => {
                let col_idx = get_simple_column_index(&args[0])
                    .ok_or_else(|| Error::internal("Missing column index for MIN"))?;
                let column = input_table
                    .column(col_idx)
                    .ok_or_else(|| Error::internal("Column not found for MIN"))?;
                column.min().unwrap_or(Value::Null)
            }
            AggregateFunction::Max => {
                let col_idx = get_simple_column_index(&args[0])
                    .ok_or_else(|| Error::internal("Missing column index for MAX"))?;
                let column = input_table
                    .column(col_idx)
                    .ok_or_else(|| Error::internal("Column not found for MAX"))?;
                column.max().unwrap_or(Value::Null)
            }
            _ => {
                return Err(Error::internal(format!(
                    "Unsupported aggregate function in columnar aggregate: {:?}",
                    func
                )))
            }
        };
        row.push(value);
    }

    result.push_row(row)?;
    Ok(result)
}
