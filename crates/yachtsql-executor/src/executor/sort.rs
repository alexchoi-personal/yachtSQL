#![coverage(off)]

use std::cmp::Ordering;

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, SortExpr};
use yachtsql_storage::{Column, Table};

use super::PlanExecutor;
use crate::columnar_evaluator::ColumnarEvaluator;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub(crate) fn execute_sort(
        &mut self,
        input: &PhysicalPlan,
        sort_exprs: &[SortExpr],
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let schema = input_table.schema().clone();
        let n = input_table.row_count();

        if n == 0 {
            return Ok(input_table);
        }

        let evaluator = ColumnarEvaluator::new(&schema)
            .with_variables(&self.variables)
            .with_system_variables(self.session.system_variables())
            .with_user_functions(&self.user_function_defs);

        let sort_columns: Vec<Column> = sort_exprs
            .iter()
            .map(|se| evaluator.evaluate(&se.expr, &input_table))
            .collect::<Result<_>>()?;

        let asc: Vec<bool> = sort_exprs.iter().map(|e| e.asc).collect();
        let nulls_first: Vec<bool> = sort_exprs.iter().map(|e| e.nulls_first).collect();

        let mut indices: Vec<usize> = (0..n).collect();
        indices.sort_by(|&a, &b| compare_rows_by_columns(&sort_columns, a, b, &asc, &nulls_first));

        Ok(input_table.gather_rows(&indices))
    }
}

fn compare_rows_by_columns(
    columns: &[Column],
    a: usize,
    b: usize,
    asc: &[bool],
    nulls_first: &[bool],
) -> Ordering {
    for (i, col) in columns.iter().enumerate() {
        let a_val = col.get_value(a);
        let b_val = col.get_value(b);

        let a_null = a_val.is_null();
        let b_null = b_val.is_null();

        match (a_null, b_null) {
            (true, true) => continue,
            (true, false) => {
                return if nulls_first[i] {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }
            (false, true) => {
                return if nulls_first[i] {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
            }
            (false, false) => {}
        }

        let ordering = compare_values(&a_val, &b_val);
        let ordering = if asc[i] { ordering } else { ordering.reverse() };

        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
        (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Value::Int64(a), Value::Float64(b)) => {
            (*a as f64).partial_cmp(&b.0).unwrap_or(Ordering::Equal)
        }
        (Value::Float64(a), Value::Int64(b)) => {
            a.0.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
        }
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Date(a), Value::Date(b)) => a.cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
        (Value::DateTime(a), Value::DateTime(b)) => a.cmp(b),
        (Value::Time(a), Value::Time(b)) => a.cmp(b),
        (Value::Numeric(a), Value::Numeric(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}
