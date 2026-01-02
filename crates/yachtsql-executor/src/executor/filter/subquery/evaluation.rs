#![coverage(off)]

use std::collections::HashSet;

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{BinaryOp, Expr, LogicalPlan};
use yachtsql_optimizer::optimize;
use yachtsql_storage::{Column, Record, Schema, Table};

use super::super::PlanExecutor;
use crate::plan::PhysicalPlan;
use crate::value_evaluator::ValueEvaluator;

impl<'a> PlanExecutor<'a> {
    pub(crate) fn eval_expr_with_subquery(
        &mut self,
        expr: &Expr,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Value> {
        match expr {
            Expr::Exists { subquery, negated } => {
                let has_rows = self.eval_exists(subquery, outer_schema, outer_record)?;
                Ok(Value::Bool(if *negated { !has_rows } else { has_rows }))
            }
            Expr::InSubquery {
                expr: value_expr,
                subquery,
                negated,
            } => {
                let value = self.eval_expr_with_subquery(value_expr, outer_schema, outer_record)?;
                let in_list = self.eval_in_subquery(subquery, outer_schema, outer_record)?;
                let is_in = in_list.contains(&value);
                Ok(Value::Bool(if *negated { !is_in } else { is_in }))
            }
            Expr::BinaryOp { left, op, right } => {
                self.eval_binary_op_with_subquery(left, *op, right, outer_schema, outer_record)
            }
            Expr::UnaryOp {
                op: yachtsql_ir::UnaryOp::Not,
                expr: inner,
            } => {
                let val = self.eval_expr_with_subquery(inner, outer_schema, outer_record)?;
                Ok(Value::Bool(!val.as_bool().unwrap_or(false)))
            }
            Expr::Subquery(subquery) | Expr::ScalarSubquery(subquery) => {
                if Self::plan_contains_outer_refs(subquery, outer_schema) {
                    self.evaluate_scalar_subquery_with_outer(subquery, outer_schema, outer_record)
                } else {
                    self.evaluate_scalar_subquery(subquery)
                }
            }
            Expr::ArraySubquery(subquery) => {
                self.evaluate_array_subquery(subquery, outer_schema, outer_record)
            }
            Expr::ScalarFunction { name, args } => {
                let arg_vals: Vec<Value> = args
                    .iter()
                    .map(|a| self.eval_expr_with_subquery(a, outer_schema, outer_record))
                    .collect::<Result<_>>()?;
                let evaluator = ValueEvaluator::new(outer_schema)
                    .with_variables(&self.variables)
                    .with_system_variables(self.session.system_variables())
                    .with_user_functions(&self.user_function_defs);
                evaluator.eval_scalar_function_with_values(name, &arg_vals)
            }
            _ => {
                let evaluator = ValueEvaluator::new(outer_schema)
                    .with_variables(&self.variables)
                    .with_system_variables(self.session.system_variables())
                    .with_user_functions(&self.user_function_defs);
                evaluator.evaluate(expr, outer_record)
            }
        }
    }

    fn eval_binary_op_with_subquery(
        &mut self,
        left: &Expr,
        op: BinaryOp,
        right: &Expr,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Value> {
        match op {
            BinaryOp::And => {
                let left_val = self.eval_expr_with_subquery(left, outer_schema, outer_record)?;
                if !left_val.as_bool().unwrap_or(false) {
                    return Ok(Value::Bool(false));
                }
                let right_val = self.eval_expr_with_subquery(right, outer_schema, outer_record)?;
                Ok(Value::Bool(right_val.as_bool().unwrap_or(false)))
            }
            BinaryOp::Or => {
                let left_val = self.eval_expr_with_subquery(left, outer_schema, outer_record)?;
                if left_val.as_bool().unwrap_or(false) {
                    return Ok(Value::Bool(true));
                }
                let right_val = self.eval_expr_with_subquery(right, outer_schema, outer_record)?;
                Ok(Value::Bool(right_val.as_bool().unwrap_or(false)))
            }
            _ => {
                let left_val = self.eval_expr_with_subquery(left, outer_schema, outer_record)?;
                let right_val = self.eval_expr_with_subquery(right, outer_schema, outer_record)?;
                self.eval_subquery_binary_op_values(
                    &left_val,
                    op,
                    &right_val,
                    outer_schema,
                    outer_record,
                )
            }
        }
    }

    fn eval_subquery_binary_op_values(
        &self,
        left_val: &Value,
        op: BinaryOp,
        right_val: &Value,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Value> {
        match op {
            BinaryOp::Eq => Ok(Value::Bool(Self::values_equal(left_val, right_val))),
            BinaryOp::NotEq => Ok(Value::Bool(!Self::values_equal(left_val, right_val))),
            BinaryOp::Lt => Ok(Value::Bool(
                Self::compare_values(left_val, right_val) == std::cmp::Ordering::Less,
            )),
            BinaryOp::LtEq => Ok(Value::Bool(matches!(
                Self::compare_values(left_val, right_val),
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal
            ))),
            BinaryOp::Gt => Ok(Value::Bool(
                Self::compare_values(left_val, right_val) == std::cmp::Ordering::Greater,
            )),
            BinaryOp::GtEq => Ok(Value::Bool(matches!(
                Self::compare_values(left_val, right_val),
                std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
            ))),
            BinaryOp::Add => Self::arithmetic_op(left_val, right_val, |a, b| a + b),
            BinaryOp::Sub => Self::arithmetic_op(left_val, right_val, |a, b| a - b),
            BinaryOp::Mul => Self::arithmetic_op(left_val, right_val, |a, b| a * b),
            BinaryOp::Div => Self::arithmetic_op(left_val, right_val, |a, b| a / b),
            _ => {
                let new_left = Self::value_to_literal(left_val.clone());
                let new_right = Self::value_to_literal(right_val.clone());
                let simplified_expr = Expr::BinaryOp {
                    left: Box::new(Expr::Literal(new_left)),
                    op,
                    right: Box::new(Expr::Literal(new_right)),
                };
                let evaluator = ValueEvaluator::new(outer_schema)
                    .with_variables(&self.variables)
                    .with_system_variables(self.session.system_variables())
                    .with_user_functions(&self.user_function_defs);
                evaluator.evaluate(&simplified_expr, outer_record)
            }
        }
    }

    pub(in super::super) fn eval_exists(
        &mut self,
        subquery: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<bool> {
        let mut inner_tables = HashSet::new();
        Self::collect_plan_tables(subquery, &mut inner_tables);
        let substituted = self.substitute_outer_refs_in_plan_with_inner_tables(
            subquery,
            outer_schema,
            outer_record,
            &inner_tables,
        )?;
        let physical = optimize(&substituted)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;
        Ok(!result_table.is_empty())
    }

    pub(in super::super) fn eval_in_subquery(
        &mut self,
        subquery: &LogicalPlan,
        _outer_schema: &Schema,
        _outer_record: &Record,
    ) -> Result<Vec<Value>> {
        let physical = optimize(subquery)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;

        if result_table.num_columns() == 0 {
            return Ok(Vec::new());
        }
        let first_col = result_table.column(0).unwrap();
        let n = result_table.row_count();
        let values: Vec<Value> = (0..n).map(|i| first_col.get_value(i)).collect();
        Ok(values)
    }

    pub(in super::super) fn evaluate_scalar_subquery(
        &mut self,
        subquery: &LogicalPlan,
    ) -> Result<Value> {
        let physical = optimize(subquery)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;

        if result_table.row_count() == 0 || result_table.num_columns() == 0 {
            return Ok(Value::Null);
        }
        let first_col = result_table.column(0).unwrap();
        Ok(first_col.get_value(0))
    }

    pub(in super::super) fn evaluate_scalar_subquery_with_outer(
        &mut self,
        subquery: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Value> {
        let mut inner_tables = HashSet::new();
        Self::collect_plan_tables(subquery, &mut inner_tables);
        let substituted = self.substitute_outer_refs_in_plan_with_inner_tables(
            subquery,
            outer_schema,
            outer_record,
            &inner_tables,
        )?;
        let physical = optimize(&substituted)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;

        if result_table.row_count() == 0 || result_table.num_columns() == 0 {
            return Ok(Value::Null);
        }
        let first_col = result_table.column(0).unwrap();
        Ok(first_col.get_value(0))
    }

    pub(in super::super) fn evaluate_array_subquery(
        &mut self,
        subquery: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Value> {
        let mut inner_tables = HashSet::new();
        Self::collect_plan_tables(subquery, &mut inner_tables);
        let substituted = self.substitute_outer_refs_in_plan_with_inner_tables(
            subquery,
            outer_schema,
            outer_record,
            &inner_tables,
        )?;
        let physical = optimize(&substituted)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;

        let result_schema = result_table.schema();
        let num_fields = result_schema.field_count();
        let n = result_table.row_count();
        let columns: Vec<&Column> = result_table.columns().iter().map(|(_, c)| c).collect();

        let mut array_values = Vec::with_capacity(n);
        for row_idx in 0..n {
            if num_fields == 1 {
                array_values.push(columns[0].get_value(row_idx));
            } else {
                let fields: Vec<(String, Value)> = result_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(i, f)| (f.name.clone(), columns[i].get_value(row_idx)))
                    .collect();
                array_values.push(Value::Struct(fields));
            }
        }

        Ok(Value::Array(array_values))
    }

    pub(in super::super) fn execute_filter_subquery_inner(
        &mut self,
        input: &Table,
        predicate: &Expr,
    ) -> Result<Table> {
        use std::collections::HashMap;

        let schema = input.schema().clone();
        let n = input.row_count();
        let columns: Vec<&Column> = input.columns().iter().map(|(_, c)| c).collect();

        let outer_col_indices = Self::collect_outer_column_indices_from_expr(predicate, &schema);
        let mut subquery_cache: HashMap<Vec<Value>, Value> = HashMap::new();
        let mut matching_indices = Vec::new();

        for row_idx in 0..n {
            let row_values: Vec<Value> = columns.iter().map(|c| c.get_value(row_idx)).collect();
            let record = Record::from_values(row_values);

            let cache_key: Vec<Value> = outer_col_indices
                .iter()
                .map(|&idx| record.values().get(idx).cloned().unwrap_or(Value::Null))
                .collect();

            let val = if let Some(cached) = subquery_cache.get(&cache_key) {
                cached.clone()
            } else {
                let computed = self.eval_expr_with_subquery(predicate, &schema, &record)?;
                subquery_cache.insert(cache_key, computed.clone());
                computed
            };

            if val.as_bool().unwrap_or(false) {
                matching_indices.push(row_idx);
            }
        }

        Ok(input.gather_rows(&matching_indices))
    }
}
