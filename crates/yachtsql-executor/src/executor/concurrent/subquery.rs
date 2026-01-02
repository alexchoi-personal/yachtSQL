#![coverage(off)]

use async_recursion::async_recursion;
use chrono::{Datelike, Timelike};
use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{BinaryOp, Expr, LogicalPlan, SortExpr, UnnestColumn};
use yachtsql_optimizer::optimize;
use yachtsql_storage::{Column, Record, Schema, Table};

use super::ConcurrentPlanExecutor;
use crate::plan::PhysicalPlan;
use crate::value_evaluator::{ValueEvaluator, cast_value};

impl ConcurrentPlanExecutor {
    #[async_recursion]
    pub(crate) async fn eval_expr_with_subqueries(
        &self,
        expr: &Expr,
        schema: &Schema,
        record: &Record,
    ) -> Result<Value> {
        match expr {
            Expr::Subquery(plan) | Expr::ScalarSubquery(plan) => {
                self.eval_scalar_subquery(plan, schema, record).await
            }
            Expr::Exists { subquery, negated } => {
                let has_rows = self.eval_exists_subquery(subquery, schema, record).await?;
                Ok(Value::Bool(if *negated { !has_rows } else { has_rows }))
            }
            Expr::ArraySubquery(plan) => self.eval_array_subquery(plan, schema, record).await,
            Expr::InSubquery {
                expr: inner_expr,
                subquery,
                negated,
            } => {
                let val = self
                    .eval_expr_with_subqueries(inner_expr, schema, record)
                    .await?;
                let in_result = self
                    .eval_value_in_subquery(&val, subquery, schema, record)
                    .await?;
                Ok(Value::Bool(if *negated { !in_result } else { in_result }))
            }
            Expr::InUnnest {
                expr: inner_expr,
                array_expr,
                negated,
            } => {
                let val = self
                    .eval_expr_with_subqueries(inner_expr, schema, record)
                    .await?;
                let array_val = self
                    .eval_expr_with_subqueries(array_expr, schema, record)
                    .await?;
                let in_result = if let Value::Array(arr) = array_val {
                    arr.contains(&val)
                } else {
                    false
                };
                Ok(Value::Bool(if *negated { !in_result } else { in_result }))
            }
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.eval_expr_with_subqueries(left, schema, record).await?;
                let right_val = self
                    .eval_expr_with_subqueries(right, schema, record)
                    .await?;
                self.eval_binary_op_values(left_val, *op, right_val)
            }
            Expr::UnaryOp { op, expr: inner } => {
                let val = self
                    .eval_expr_with_subqueries(inner, schema, record)
                    .await?;
                self.eval_unary_op_value(*op, val)
            }
            Expr::ScalarFunction { name, args } => {
                let mut arg_vals: Vec<Value> = Vec::with_capacity(args.len());
                for a in args {
                    arg_vals.push(self.eval_expr_with_subqueries(a, schema, record).await?);
                }
                let vars = self.get_variables();
                let sys_vars = self.get_system_variables();
                let udf = self.get_user_functions();
                let evaluator = ValueEvaluator::new(schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);
                evaluator.eval_scalar_function_with_values(name, &arg_vals)
            }
            Expr::Cast {
                expr: inner,
                data_type,
                safe,
            } => {
                let val = self
                    .eval_expr_with_subqueries(inner, schema, record)
                    .await?;
                cast_value(val, data_type, *safe)
            }
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                let operand_val = match operand.as_ref() {
                    Some(e) => Some(self.eval_expr_with_subqueries(e, schema, record).await?),
                    None => None,
                };

                for clause in when_clauses {
                    let condition_val = if let Some(op_val) = &operand_val {
                        let cond_val = self
                            .eval_expr_with_subqueries(&clause.condition, schema, record)
                            .await?;
                        Value::Bool(op_val == &cond_val)
                    } else {
                        self.eval_expr_with_subqueries(&clause.condition, schema, record)
                            .await?
                    };

                    if matches!(condition_val, Value::Bool(true)) {
                        return self
                            .eval_expr_with_subqueries(&clause.result, schema, record)
                            .await;
                    }
                }

                if let Some(else_expr) = else_result {
                    self.eval_expr_with_subqueries(else_expr, schema, record)
                        .await
                } else {
                    Ok(Value::Null)
                }
            }
            Expr::Alias { expr: inner, .. } => {
                self.eval_expr_with_subqueries(inner, schema, record).await
            }
            _ => {
                let vars = self.get_variables();
                let sys_vars = self.get_system_variables();
                let udf = self.get_user_functions();
                let evaluator = ValueEvaluator::new(schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);
                evaluator.evaluate(expr, record)
            }
        }
    }

    pub(crate) async fn eval_scalar_subquery(
        &self,
        plan: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Value> {
        let substituted = self.substitute_outer_refs_in_plan(plan, outer_schema, outer_record)?;
        let physical = optimize(&substituted)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan).await?;

        if result_table.row_count() == 0 || result_table.num_columns() == 0 {
            return Ok(Value::Null);
        }

        let first_col = result_table.column(0).unwrap();
        Ok(first_col.get_value(0))
    }

    pub(crate) async fn eval_scalar_subquery_as_row(&self, plan: &LogicalPlan) -> Result<Value> {
        let physical = optimize(plan)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan).await?;

        if result_table.row_count() == 0 {
            return Ok(Value::Struct(vec![]));
        }

        let schema = result_table.schema();
        let fields = schema.fields();
        let columns: Vec<&Column> = result_table.columns().iter().map(|(_, c)| c).collect();

        let result: Vec<(String, Value)> = fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name.clone(), columns[i].get_value(0)))
            .collect();

        Ok(Value::Struct(result))
    }

    pub(crate) async fn eval_exists_subquery(
        &self,
        plan: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<bool> {
        let substituted = self.substitute_outer_refs_in_plan(plan, outer_schema, outer_record)?;
        let physical = optimize(&substituted)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan).await?;
        Ok(!result_table.is_empty())
    }

    pub(crate) async fn eval_array_subquery(
        &self,
        plan: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Value> {
        let substituted = self.substitute_outer_refs_in_plan(plan, outer_schema, outer_record)?;
        let physical = optimize(&substituted)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan).await?;

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

    pub(crate) async fn eval_value_in_subquery(
        &self,
        value: &Value,
        plan: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<bool> {
        if matches!(value, Value::Null) {
            return Ok(false);
        }

        let substituted = self.substitute_outer_refs_in_plan(plan, outer_schema, outer_record)?;
        let physical = optimize(&substituted)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan).await?;

        if result_table.num_columns() == 0 {
            return Ok(false);
        }
        let first_col = result_table.column(0).unwrap();
        for row_idx in 0..result_table.row_count() {
            if &first_col.get_value(row_idx) == value {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) fn substitute_outer_refs_in_plan(
        &self,
        plan: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Scan {
                table_name,
                schema,
                projection,
            } => Ok(LogicalPlan::Scan {
                table_name: table_name.clone(),
                schema: schema.clone(),
                projection: projection.clone(),
            }),
            LogicalPlan::Filter { input, predicate } => {
                let new_input =
                    self.substitute_outer_refs_in_plan(input, outer_schema, outer_record)?;
                let new_predicate =
                    self.substitute_outer_refs_in_expr(predicate, outer_schema, outer_record)?;
                Ok(LogicalPlan::Filter {
                    input: Box::new(new_input),
                    predicate: new_predicate,
                })
            }
            LogicalPlan::Project {
                input,
                expressions,
                schema,
            } => {
                let new_input =
                    self.substitute_outer_refs_in_plan(input, outer_schema, outer_record)?;
                let new_expressions = expressions
                    .iter()
                    .map(|e| self.substitute_outer_refs_in_expr(e, outer_schema, outer_record))
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::Project {
                    input: Box::new(new_input),
                    expressions: new_expressions,
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let new_input =
                    self.substitute_outer_refs_in_plan(input, outer_schema, outer_record)?;
                Ok(LogicalPlan::Limit {
                    input: Box::new(new_input),
                    limit: *limit,
                    offset: *offset,
                })
            }
            LogicalPlan::Sort { input, sort_exprs } => {
                let new_input =
                    self.substitute_outer_refs_in_plan(input, outer_schema, outer_record)?;
                let new_sort_exprs = sort_exprs
                    .iter()
                    .map(|se| {
                        let new_expr = self.substitute_outer_refs_in_expr(
                            &se.expr,
                            outer_schema,
                            outer_record,
                        )?;
                        Ok(SortExpr {
                            expr: new_expr,
                            asc: se.asc,
                            nulls_first: se.nulls_first,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::Sort {
                    input: Box::new(new_input),
                    sort_exprs: new_sort_exprs,
                })
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                schema,
                grouping_sets,
            } => {
                let new_input =
                    self.substitute_outer_refs_in_plan(input, outer_schema, outer_record)?;
                let new_group_by = group_by
                    .iter()
                    .map(|e| self.substitute_outer_refs_in_expr(e, outer_schema, outer_record))
                    .collect::<Result<Vec<_>>>()?;
                let new_aggregates = aggregates
                    .iter()
                    .map(|e| self.substitute_outer_refs_in_expr(e, outer_schema, outer_record))
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::Aggregate {
                    input: Box::new(new_input),
                    group_by: new_group_by,
                    aggregates: new_aggregates,
                    schema: schema.clone(),
                    grouping_sets: grouping_sets.clone(),
                })
            }
            LogicalPlan::Unnest {
                input,
                columns,
                schema,
            } => {
                let new_input =
                    self.substitute_outer_refs_in_plan(input, outer_schema, outer_record)?;
                let new_columns = columns
                    .iter()
                    .map(|uc| {
                        let new_expr = self.substitute_outer_refs_in_unnest_expr(
                            &uc.expr,
                            outer_schema,
                            outer_record,
                        )?;
                        Ok(UnnestColumn {
                            expr: new_expr,
                            alias: uc.alias.clone(),
                            with_offset: uc.with_offset,
                            offset_alias: uc.offset_alias.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::Unnest {
                    input: Box::new(new_input),
                    columns: new_columns,
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
                schema,
            } => {
                let new_left =
                    self.substitute_outer_refs_in_plan(left, outer_schema, outer_record)?;
                let new_right =
                    self.substitute_outer_refs_in_plan(right, outer_schema, outer_record)?;
                let new_condition = condition
                    .as_ref()
                    .map(|c| self.substitute_outer_refs_in_expr(c, outer_schema, outer_record))
                    .transpose()?;
                Ok(LogicalPlan::Join {
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    join_type: *join_type,
                    condition: new_condition,
                    schema: schema.clone(),
                })
            }
            LogicalPlan::SetOperation {
                left,
                right,
                op,
                all,
                schema,
            } => {
                let new_left =
                    self.substitute_outer_refs_in_plan(left, outer_schema, outer_record)?;
                let new_right =
                    self.substitute_outer_refs_in_plan(right, outer_schema, outer_record)?;
                Ok(LogicalPlan::SetOperation {
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    op: *op,
                    all: *all,
                    schema: schema.clone(),
                })
            }
            other => Ok(other.clone()),
        }
    }

    pub(crate) fn substitute_outer_refs_in_expr(
        &self,
        expr: &Expr,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Expr> {
        match expr {
            Expr::Column { table, name, index } => {
                if index.is_some() {
                    return Ok(Expr::Column {
                        table: table.clone(),
                        name: name.clone(),
                        index: *index,
                    });
                }
                let idx = if let Some(tbl) = table {
                    outer_schema.fields().iter().position(|f| {
                        (f.source_table
                            .as_ref()
                            .is_some_and(|src| src.eq_ignore_ascii_case(tbl))
                            || f.source_table.is_none())
                            && f.name.eq_ignore_ascii_case(name)
                    })
                } else {
                    outer_schema
                        .fields()
                        .iter()
                        .position(|f| f.name.eq_ignore_ascii_case(name))
                };

                if let Some(idx) = idx {
                    let value = outer_record
                        .values()
                        .get(idx)
                        .cloned()
                        .unwrap_or(Value::Null);
                    return Ok(Expr::Literal(Self::value_to_literal(value)));
                }
                Ok(Expr::Column {
                    table: table.clone(),
                    name: name.clone(),
                    index: *index,
                })
            }
            Expr::BinaryOp { left, op, right } => {
                let new_left =
                    self.substitute_outer_refs_in_expr(left, outer_schema, outer_record)?;
                let new_right =
                    self.substitute_outer_refs_in_expr(right, outer_schema, outer_record)?;
                Ok(Expr::BinaryOp {
                    left: Box::new(new_left),
                    op: *op,
                    right: Box::new(new_right),
                })
            }
            Expr::IsNull {
                expr: inner,
                negated,
            } => {
                let new_inner =
                    self.substitute_outer_refs_in_expr(inner, outer_schema, outer_record)?;
                Ok(Expr::IsNull {
                    expr: Box::new(new_inner),
                    negated: *negated,
                })
            }
            Expr::ScalarFunction { name, args } => {
                let new_args = args
                    .iter()
                    .map(|a| self.substitute_outer_refs_in_expr(a, outer_schema, outer_record))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::ScalarFunction {
                    name: name.clone(),
                    args: new_args,
                })
            }
            Expr::Cast {
                expr: inner,
                data_type,
                safe,
            } => {
                let new_inner =
                    self.substitute_outer_refs_in_expr(inner, outer_schema, outer_record)?;
                Ok(Expr::Cast {
                    expr: Box::new(new_inner),
                    data_type: data_type.clone(),
                    safe: *safe,
                })
            }
            _ => Ok(expr.clone()),
        }
    }

    fn substitute_outer_refs_in_unnest_expr(
        &self,
        expr: &Expr,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Expr> {
        match expr {
            Expr::Column { table, name, index } => {
                let should_substitute = if let Some(tbl) = table {
                    outer_schema.fields().iter().any(|f| {
                        f.source_table
                            .as_ref()
                            .is_some_and(|src| src.eq_ignore_ascii_case(tbl))
                            && f.name.eq_ignore_ascii_case(name)
                    })
                } else {
                    outer_schema.field_index(name).is_some()
                };

                if should_substitute
                    && let Some(idx) = outer_schema.field_index_qualified(name, table.as_deref())
                {
                    let value = outer_record
                        .values()
                        .get(idx)
                        .cloned()
                        .unwrap_or(Value::Null);
                    return Ok(Expr::Literal(Self::value_to_literal(value)));
                }
                Ok(Expr::Column {
                    table: table.clone(),
                    name: name.clone(),
                    index: *index,
                })
            }
            Expr::ScalarFunction { name, args } => {
                let new_args = args
                    .iter()
                    .map(|a| {
                        self.substitute_outer_refs_in_unnest_expr(a, outer_schema, outer_record)
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::ScalarFunction {
                    name: name.clone(),
                    args: new_args,
                })
            }
            Expr::StructAccess { expr: base, field } => {
                let new_base =
                    self.substitute_outer_refs_in_unnest_expr(base, outer_schema, outer_record)?;
                Ok(Expr::StructAccess {
                    expr: Box::new(new_base),
                    field: field.clone(),
                })
            }
            Expr::ArrayAccess { array, index } => {
                let new_array =
                    self.substitute_outer_refs_in_unnest_expr(array, outer_schema, outer_record)?;
                let new_index =
                    self.substitute_outer_refs_in_unnest_expr(index, outer_schema, outer_record)?;
                Ok(Expr::ArrayAccess {
                    array: Box::new(new_array),
                    index: Box::new(new_index),
                })
            }
            _ => self.substitute_outer_refs_in_expr(expr, outer_schema, outer_record),
        }
    }

    pub(crate) fn value_to_literal(value: Value) -> yachtsql_ir::Literal {
        use yachtsql_ir::Literal;
        match value {
            Value::Null => Literal::Null,
            Value::Bool(b) => Literal::Bool(b),
            Value::Int64(n) => Literal::Int64(n),
            Value::Float64(f) => Literal::Float64(f),
            Value::String(s) => Literal::String(s),
            Value::Date(d) => Literal::Date(d.num_days_from_ce() - 719163),
            Value::Time(t) => Literal::Time(
                (t.hour() as i64 * 3600 + t.minute() as i64 * 60 + t.second() as i64)
                    * 1_000_000_000
                    + t.nanosecond() as i64,
            ),
            Value::DateTime(dt) => Literal::Datetime(dt.and_utc().timestamp_micros()),
            Value::Timestamp(ts) => Literal::Timestamp(ts.timestamp_micros()),
            Value::Numeric(n) => Literal::Numeric(n),
            Value::Bytes(b) => Literal::Bytes(b),
            Value::Interval(i) => Literal::Interval {
                months: i.months,
                days: i.days,
                nanos: i.nanos,
            },
            Value::Array(arr) => {
                let literal_elements: Vec<Literal> =
                    arr.into_iter().map(Self::value_to_literal).collect();
                Literal::Array(literal_elements)
            }
            Value::Struct(fields) => {
                let literal_fields: Vec<(String, Literal)> = fields
                    .into_iter()
                    .map(|(name, val)| (name, Self::value_to_literal(val)))
                    .collect();
                Literal::Struct(literal_fields)
            }
            _ => Literal::Null,
        }
    }

    pub(crate) fn eval_binary_op_values(
        &self,
        left: Value,
        op: BinaryOp,
        right: Value,
    ) -> Result<Value> {
        match op {
            BinaryOp::Add => match (&left, &right) {
                (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l + r)),
                (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(*l + *r)),
                (Value::Int64(l), Value::Float64(r)) => {
                    Ok(Value::Float64(ordered_float::OrderedFloat(*l as f64) + *r))
                }
                (Value::Float64(l), Value::Int64(r)) => {
                    Ok(Value::Float64(*l + ordered_float::OrderedFloat(*r as f64)))
                }
                _ => Ok(Value::Null),
            },
            BinaryOp::Sub => match (&left, &right) {
                (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l - r)),
                (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(*l - *r)),
                (Value::Int64(l), Value::Float64(r)) => {
                    Ok(Value::Float64(ordered_float::OrderedFloat(*l as f64) - *r))
                }
                (Value::Float64(l), Value::Int64(r)) => {
                    Ok(Value::Float64(*l - ordered_float::OrderedFloat(*r as f64)))
                }
                _ => Ok(Value::Null),
            },
            BinaryOp::Mul => match (&left, &right) {
                (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l * r)),
                (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(*l * *r)),
                (Value::Int64(l), Value::Float64(r)) => {
                    Ok(Value::Float64(ordered_float::OrderedFloat(*l as f64) * *r))
                }
                (Value::Float64(l), Value::Int64(r)) => {
                    Ok(Value::Float64(*l * ordered_float::OrderedFloat(*r as f64)))
                }
                _ => Ok(Value::Null),
            },
            BinaryOp::Div => match (&left, &right) {
                (Value::Int64(l), Value::Int64(r)) if *r != 0 => Ok(Value::Float64(
                    ordered_float::OrderedFloat(*l as f64 / *r as f64),
                )),
                (Value::Float64(l), Value::Float64(r)) if r.0 != 0.0 => Ok(Value::Float64(*l / *r)),
                (Value::Int64(l), Value::Float64(r)) if r.0 != 0.0 => {
                    Ok(Value::Float64(ordered_float::OrderedFloat(*l as f64) / *r))
                }
                (Value::Float64(l), Value::Int64(r)) if *r != 0 => {
                    Ok(Value::Float64(*l / ordered_float::OrderedFloat(*r as f64)))
                }
                _ => Ok(Value::Null),
            },
            BinaryOp::And => {
                let l = left.as_bool().unwrap_or(false);
                let r = right.as_bool().unwrap_or(false);
                Ok(Value::Bool(l && r))
            }
            BinaryOp::Or => {
                let l = left.as_bool().unwrap_or(false);
                let r = right.as_bool().unwrap_or(false);
                Ok(Value::Bool(l || r))
            }
            BinaryOp::Eq => Ok(Value::Bool(left == right)),
            BinaryOp::NotEq => Ok(Value::Bool(left != right)),
            BinaryOp::Lt => Ok(Value::Bool(left < right)),
            BinaryOp::LtEq => Ok(Value::Bool(left <= right)),
            BinaryOp::Gt => Ok(Value::Bool(left > right)),
            BinaryOp::GtEq => Ok(Value::Bool(left >= right)),
            _ => Ok(Value::Null),
        }
    }

    pub(crate) fn eval_unary_op_value(
        &self,
        op: yachtsql_ir::UnaryOp,
        val: Value,
    ) -> Result<Value> {
        match op {
            yachtsql_ir::UnaryOp::Not => {
                let b = val.as_bool().unwrap_or(false);
                Ok(Value::Bool(!b))
            }
            yachtsql_ir::UnaryOp::Minus => match val {
                Value::Int64(n) => Ok(Value::Int64(-n)),
                Value::Float64(f) => Ok(Value::Float64(-f)),
                _ => Ok(Value::Null),
            },
            yachtsql_ir::UnaryOp::Plus => Ok(val),
            yachtsql_ir::UnaryOp::BitwiseNot => match val {
                Value::Int64(n) => Ok(Value::Int64(!n)),
                _ => Ok(Value::Null),
            },
        }
    }

    #[async_recursion]
    pub(crate) async fn resolve_subqueries_in_expr(&self, expr: &Expr) -> Result<Expr> {
        match expr {
            Expr::InSubquery {
                expr: inner_expr,
                subquery,
                negated,
            } => {
                let resolved_inner = self.resolve_subqueries_in_expr(inner_expr).await?;
                let empty_schema = Schema::new();
                let empty_record = Record::new();
                let substituted =
                    self.substitute_outer_refs_in_plan(subquery, &empty_schema, &empty_record)?;
                let physical = optimize(&substituted)?;
                let executor_plan = PhysicalPlan::from_physical(&physical);
                let result_table = self.execute_plan(&executor_plan).await?;

                let mut list_exprs = Vec::new();
                if result_table.num_columns() > 0 {
                    let first_col = result_table.column(0).unwrap();
                    for row_idx in 0..result_table.row_count() {
                        let literal = Self::value_to_literal(first_col.get_value(row_idx));
                        list_exprs.push(Expr::Literal(literal));
                    }
                }

                Ok(Expr::InList {
                    expr: Box::new(resolved_inner),
                    list: list_exprs,
                    negated: *negated,
                })
            }
            Expr::Exists { subquery, negated } => {
                let empty_schema = Schema::new();
                let empty_record = Record::new();
                let substituted =
                    self.substitute_outer_refs_in_plan(subquery, &empty_schema, &empty_record)?;
                let physical = optimize(&substituted)?;
                let executor_plan = PhysicalPlan::from_physical(&physical);
                let result_table = self.execute_plan(&executor_plan).await?;
                let has_rows = !result_table.is_empty();
                let result = if *negated { !has_rows } else { has_rows };
                Ok(Expr::Literal(yachtsql_ir::Literal::Bool(result)))
            }
            Expr::Subquery(plan) | Expr::ScalarSubquery(plan) => {
                let empty_schema = Schema::new();
                let empty_record = Record::new();
                let substituted =
                    self.substitute_outer_refs_in_plan(plan, &empty_schema, &empty_record)?;
                let physical = optimize(&substituted)?;
                let executor_plan = PhysicalPlan::from_physical(&physical);
                let result_table = self.execute_plan(&executor_plan).await?;

                if result_table.row_count() == 0 || result_table.num_columns() == 0 {
                    return Ok(Expr::Literal(yachtsql_ir::Literal::Null));
                }

                let first_col = result_table.column(0).unwrap();
                let literal = Self::value_to_literal(first_col.get_value(0));
                Ok(Expr::Literal(literal))
            }
            Expr::ArraySubquery(plan) => {
                let empty_schema = Schema::new();
                let empty_record = Record::new();
                let substituted =
                    self.substitute_outer_refs_in_plan(plan, &empty_schema, &empty_record)?;
                let physical = optimize(&substituted)?;
                let executor_plan = PhysicalPlan::from_physical(&physical);
                let result_table = self.execute_plan(&executor_plan).await?;

                let mut array_elements = Vec::new();
                if result_table.num_columns() > 0 {
                    let first_col = result_table.column(0).unwrap();
                    for row_idx in 0..result_table.row_count() {
                        array_elements.push(Self::value_to_literal(first_col.get_value(row_idx)));
                    }
                }
                Ok(Expr::Literal(yachtsql_ir::Literal::Array(array_elements)))
            }
            Expr::BinaryOp { left, op, right } => {
                let resolved_left = self.resolve_subqueries_in_expr(left).await?;
                let resolved_right = self.resolve_subqueries_in_expr(right).await?;
                Ok(Expr::BinaryOp {
                    left: Box::new(resolved_left),
                    op: *op,
                    right: Box::new(resolved_right),
                })
            }
            Expr::UnaryOp { op, expr: inner } => {
                let resolved_inner = self.resolve_subqueries_in_expr(inner).await?;
                Ok(Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(resolved_inner),
                })
            }
            Expr::Cast {
                expr: inner,
                data_type,
                safe,
            } => {
                let resolved_inner = self.resolve_subqueries_in_expr(inner).await?;
                Ok(Expr::Cast {
                    expr: Box::new(resolved_inner),
                    data_type: data_type.clone(),
                    safe: *safe,
                })
            }
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                let resolved_operand = match operand.as_ref() {
                    Some(e) => Some(Box::new(self.resolve_subqueries_in_expr(e).await?)),
                    None => None,
                };

                let mut resolved_clauses = Vec::new();
                for clause in when_clauses {
                    resolved_clauses.push(yachtsql_ir::WhenClause {
                        condition: self.resolve_subqueries_in_expr(&clause.condition).await?,
                        result: self.resolve_subqueries_in_expr(&clause.result).await?,
                    });
                }

                let resolved_else = match else_result.as_ref() {
                    Some(e) => Some(Box::new(self.resolve_subqueries_in_expr(e).await?)),
                    None => None,
                };

                Ok(Expr::Case {
                    operand: resolved_operand,
                    when_clauses: resolved_clauses,
                    else_result: resolved_else,
                })
            }
            Expr::ScalarFunction { name, args } => {
                let mut resolved_args = Vec::with_capacity(args.len());
                for a in args {
                    resolved_args.push(self.resolve_subqueries_in_expr(a).await?);
                }
                Ok(Expr::ScalarFunction {
                    name: name.clone(),
                    args: resolved_args,
                })
            }
            Expr::Alias { expr: inner, name } => {
                let resolved_inner = self.resolve_subqueries_in_expr(inner).await?;
                Ok(Expr::Alias {
                    expr: Box::new(resolved_inner),
                    name: name.clone(),
                })
            }
            Expr::IsNull {
                expr: inner,
                negated,
            } => {
                let resolved_inner = self.resolve_subqueries_in_expr(inner).await?;
                Ok(Expr::IsNull {
                    expr: Box::new(resolved_inner),
                    negated: *negated,
                })
            }
            Expr::InList {
                expr: inner,
                list,
                negated,
            } => {
                let resolved_inner = self.resolve_subqueries_in_expr(inner).await?;
                let mut resolved_list = Vec::with_capacity(list.len());
                for e in list {
                    resolved_list.push(self.resolve_subqueries_in_expr(e).await?);
                }
                Ok(Expr::InList {
                    expr: Box::new(resolved_inner),
                    list: resolved_list,
                    negated: *negated,
                })
            }
            Expr::Between {
                expr: inner,
                low,
                high,
                negated,
            } => {
                let resolved_inner = self.resolve_subqueries_in_expr(inner).await?;
                let resolved_low = self.resolve_subqueries_in_expr(low).await?;
                let resolved_high = self.resolve_subqueries_in_expr(high).await?;
                Ok(Expr::Between {
                    expr: Box::new(resolved_inner),
                    low: Box::new(resolved_low),
                    high: Box::new(resolved_high),
                    negated: *negated,
                })
            }
            _ => Ok(expr.clone()),
        }
    }

    pub(crate) fn expr_contains_subquery(expr: &Expr) -> bool {
        match expr {
            Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::Subquery(_)
            | Expr::ScalarSubquery(_)
            | Expr::ArraySubquery(_) => true,
            Expr::InUnnest {
                expr: inner_expr,
                array_expr,
                ..
            } => {
                Self::expr_contains_subquery(inner_expr) || Self::expr_contains_subquery(array_expr)
            }
            Expr::InList { expr, list, .. } => {
                Self::expr_contains_subquery(expr) || list.iter().any(Self::expr_contains_subquery)
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_contains_subquery(left) || Self::expr_contains_subquery(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_contains_subquery(expr),
            Expr::ScalarFunction { args, .. } => args.iter().any(Self::expr_contains_subquery),
            Expr::Cast { expr, .. } => Self::expr_contains_subquery(expr),
            Expr::Alias { expr, .. } => Self::expr_contains_subquery(expr),
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|e| Self::expr_contains_subquery(e))
                    || when_clauses.iter().any(|wc| {
                        Self::expr_contains_subquery(&wc.condition)
                            || Self::expr_contains_subquery(&wc.result)
                    })
                    || else_result
                        .as_ref()
                        .is_some_and(|e| Self::expr_contains_subquery(e))
            }
            _ => false,
        }
    }
}
