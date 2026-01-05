#![coverage(off)]

use rand::Rng;
use rand::seq::SliceRandom;
use rustc_hash::FxHashSet;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, PlanSchema, SortExpr};
use yachtsql_optimizer::SampleType;
use yachtsql_storage::{Column, Field, FieldMode, Record, Schema, Table};

use super::{ConcurrentPlanExecutor, compare_values_for_sort};
use crate::columnar_evaluator::ColumnarEvaluator;
use crate::executor::plan_schema_to_schema;
use crate::plan::PhysicalPlan;
use crate::value_evaluator::ValueEvaluator;

impl ConcurrentPlanExecutor {
    pub(crate) fn execute_scan(
        &self,
        table_name: &str,
        planned_schema: &PlanSchema,
    ) -> Result<Table> {
        if let Some(cte_table) = self
            .cte_results
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(table_name)
        {
            return Ok(self.apply_planned_schema(cte_table, planned_schema));
        }
        let table_name_upper = table_name.to_uppercase();
        if let Some(cte_table) = self
            .cte_results
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&table_name_upper)
        {
            return Ok(self.apply_planned_schema(cte_table, planned_schema));
        }
        let table_name_lower = table_name.to_lowercase();
        if let Some(cte_table) = self
            .cte_results
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&table_name_lower)
        {
            return Ok(self.apply_planned_schema(cte_table, planned_schema));
        }

        if let Some(table) = self.tables.get_table(table_name) {
            return Ok(self.apply_planned_schema(&table, planned_schema));
        }

        if let Some(handle) = self.catalog.get_table_handle(table_name) {
            let guard = handle.read();
            return Ok(self.apply_planned_schema(&guard, planned_schema));
        }

        Err(Error::TableNotFound(table_name.to_string()))
    }

    pub(crate) fn apply_planned_schema(
        &self,
        source_table: &Table,
        planned_schema: &PlanSchema,
    ) -> Table {
        if planned_schema.fields.is_empty() {
            return source_table.clone();
        }

        let mut new_schema = Schema::new();
        let mut column_indices = Vec::new();
        for plan_field in &planned_schema.fields {
            let mode = if plan_field.nullable {
                FieldMode::Nullable
            } else {
                FieldMode::Required
            };
            let mut field = Field::new(&plan_field.name, plan_field.data_type.clone(), mode);
            if let Some(ref table) = plan_field.table {
                field = field.with_source_table(table.clone());
            }
            let source_field_idx = source_table
                .schema()
                .fields()
                .iter()
                .position(|f| f.name.eq_ignore_ascii_case(&plan_field.name));
            if let Some(idx) = source_field_idx {
                if let Some(ref collation) = source_table.schema().fields()[idx].collation {
                    field.collation = Some(collation.clone());
                }
                column_indices.push(idx);
            }
            new_schema.add_field(field);
        }
        source_table.with_reordered_schema(new_schema, &column_indices)
    }

    pub(crate) fn execute_filter(&self, input: &PhysicalPlan, predicate: &Expr) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let schema = input_table.schema().clone();

        let has_collation = schema.fields().iter().any(|f| f.collation.is_some());

        if Self::expr_contains_subquery(predicate) {
            let mut result = Table::empty(schema.clone());
            let n = input_table.row_count();
            let columns: Vec<&Column> = input_table
                .columns()
                .iter()
                .map(|(_, c)| c.as_ref())
                .collect();

            let mut record = Record::with_capacity(columns.len());
            for row_idx in 0..n {
                record.set_from_columns(&columns, row_idx);
                let val = self.eval_expr_with_subqueries(predicate, &schema, &record)?;
                if val.as_bool().unwrap_or(false) {
                    result.push_row(record.values().to_vec())?;
                }
            }
            Ok(result)
        } else if has_collation {
            let vars = self.get_variables();
            let sys_vars = self.get_system_variables();
            let udf = self.get_user_functions();
            let evaluator = ValueEvaluator::new(&schema)
                .with_variables(&vars)
                .with_system_variables(&sys_vars)
                .with_user_functions(&udf);

            let mut result = Table::empty(schema.clone());
            let n = input_table.row_count();
            let columns: Vec<&Column> = input_table
                .columns()
                .iter()
                .map(|(_, c)| c.as_ref())
                .collect();

            let mut record = Record::with_capacity(columns.len());
            for row_idx in 0..n {
                record.set_from_columns(&columns, row_idx);
                let val = evaluator.evaluate(predicate, &record)?;
                if val.as_bool().unwrap_or(false) {
                    result.push_row(record.values().to_vec())?;
                }
            }
            Ok(result)
        } else {
            let vars = self.get_variables();
            let sys_vars = self.get_system_variables();
            let udf = self.get_user_functions();
            let evaluator = ColumnarEvaluator::new(&schema)
                .with_variables(&vars)
                .with_system_variables(&sys_vars)
                .with_user_functions(&udf);

            let mask = evaluator.evaluate(predicate, &input_table)?;
            input_table.filter_by_mask(&mask)
        }
    }

    pub(crate) fn execute_project(
        &self,
        input: &PhysicalPlan,
        expressions: &[Expr],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let input_schema = input_table.schema().clone();
        let result_schema = plan_schema_to_schema(schema);

        if expressions.iter().any(Self::expr_contains_subquery) {
            let mut result = Table::empty(result_schema);
            let n = input_table.row_count();
            let columns: Vec<&Column> = input_table
                .columns()
                .iter()
                .map(|(_, c)| c.as_ref())
                .collect();

            for row_idx in 0..n {
                let values: Vec<Value> = columns.iter().map(|c| c.get_value(row_idx)).collect();
                let record = Record::from_values(values);
                let mut new_row = Vec::with_capacity(expressions.len());
                for expr in expressions {
                    let val = self.eval_expr_with_subqueries(expr, &input_schema, &record)?;
                    new_row.push(val);
                }
                result.push_row(new_row)?;
            }
            Ok(result)
        } else {
            let vars = self.get_variables();
            let sys_vars = self.get_system_variables();
            let udf = self.get_user_functions();
            let evaluator = ValueEvaluator::new(&input_schema)
                .with_variables(&vars)
                .with_system_variables(&sys_vars)
                .with_user_functions(&udf);

            let n = input_table.row_count();
            let columns: Vec<&Column> = input_table
                .columns()
                .iter()
                .map(|(_, c)| c.as_ref())
                .collect();

            let mut result = Table::empty(result_schema);
            let mut record = Record::with_capacity(columns.len());
            for row_idx in 0..n {
                record.set_from_columns(&columns, row_idx);
                let mut new_row = Vec::with_capacity(expressions.len());
                for expr in expressions {
                    let val = evaluator.evaluate(expr, &record)?;
                    new_row.push(val);
                }
                result.push_row(new_row)?;
            }
            Ok(result)
        }
    }

    pub(crate) fn execute_sample(
        &self,
        input: &PhysicalPlan,
        sample_type: &SampleType,
        sample_value: i64,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let n = input_table.row_count();
        let columns: Vec<&Column> = input_table
            .columns()
            .iter()
            .map(|(_, c)| c.as_ref())
            .collect();

        let indices: Vec<usize> = match sample_type {
            SampleType::Rows => {
                let limit = (sample_value.max(0) as usize).min(n);
                let mut all_indices: Vec<usize> = (0..n).collect();
                let mut rng = rand::thread_rng();
                all_indices.shuffle(&mut rng);
                all_indices.truncate(limit);
                all_indices
            }
            SampleType::Percent => {
                let pct = sample_value as f64 / 100.0;
                let mut rng = rand::thread_rng();
                (0..n).filter(|_| rng.r#gen::<f64>() < pct).collect()
            }
        };

        input_table.gather_rows(&indices)
    }

    pub(crate) fn execute_sort(
        &self,
        input: &PhysicalPlan,
        sort_exprs: &[SortExpr],
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let schema = input_table.schema().clone();
        let vars = self.get_variables();
        let sys_vars = self.get_system_variables();
        let udf = self.get_user_functions();
        let evaluator = ValueEvaluator::new(&schema)
            .with_variables(&vars)
            .with_system_variables(&sys_vars)
            .with_user_functions(&udf);

        let n = input_table.row_count();
        let columns: Vec<&Column> = input_table
            .columns()
            .iter()
            .map(|(_, c)| c.as_ref())
            .collect();

        let sort_keys: Vec<Vec<Value>> = {
            let mut row_values: Vec<Value> = Vec::with_capacity(columns.len());
            (0..n)
                .map(|idx| {
                    row_values.clear();
                    row_values.extend(columns.iter().map(|c| c.get_value(idx)));
                    let record = Record::from_slice(&row_values);
                    sort_exprs
                        .iter()
                        .map(|se| evaluator.evaluate(&se.expr, &record).unwrap_or(Value::Null))
                        .collect()
                })
                .collect()
        };

        let mut indices: Vec<usize> = (0..n).collect();
        indices.sort_by(|&a, &b| {
            let keys_a = &sort_keys[a];
            let keys_b = &sort_keys[b];
            for (i, sort_expr) in sort_exprs.iter().enumerate() {
                let val_a = &keys_a[i];
                let val_b = &keys_b[i];

                let ordering = compare_values_for_sort(val_a, val_b);
                let ordering = if !sort_expr.asc {
                    ordering.reverse()
                } else {
                    ordering
                };

                match (val_a.is_null(), val_b.is_null()) {
                    (true, true) => {}
                    (true, false) => {
                        return if sort_expr.nulls_first {
                            std::cmp::Ordering::Less
                        } else {
                            std::cmp::Ordering::Greater
                        };
                    }
                    (false, true) => {
                        return if sort_expr.nulls_first {
                            std::cmp::Ordering::Greater
                        } else {
                            std::cmp::Ordering::Less
                        };
                    }
                    (false, false) => {}
                }

                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        });

        input_table.gather_rows(&indices)
    }

    pub(crate) fn execute_limit(
        &self,
        input: &PhysicalPlan,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let n = input_table.row_count();
        let offset = offset.unwrap_or(0);
        let limit = limit.unwrap_or(usize::MAX);

        let start = offset.min(n);
        let end = (offset + limit).min(n);
        let indices: Vec<usize> = (start..end).collect();

        input_table.gather_rows(&indices)
    }

    pub(crate) fn execute_topn(
        &self,
        input: &PhysicalPlan,
        sort_exprs: &[SortExpr],
        limit: usize,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let n = input_table.row_count();

        if limit == 0 {
            return Ok(Table::empty(input_table.schema().clone()));
        }

        let schema = input_table.schema().clone();
        let vars = self.get_variables();
        let sys_vars = self.get_system_variables();
        let udf = self.get_user_functions();
        let evaluator = ValueEvaluator::new(&schema)
            .with_variables(&vars)
            .with_system_variables(&sys_vars)
            .with_user_functions(&udf);

        let columns: Vec<&Column> = input_table
            .columns()
            .iter()
            .map(|(_, c)| c.as_ref())
            .collect();

        let sort_keys: Vec<Vec<Value>> = {
            let mut row_values: Vec<Value> = Vec::with_capacity(columns.len());
            (0..n)
                .map(|idx| {
                    row_values.clear();
                    row_values.extend(columns.iter().map(|c| c.get_value(idx)));
                    let record = Record::from_slice(&row_values);
                    sort_exprs
                        .iter()
                        .map(|se| evaluator.evaluate(&se.expr, &record).unwrap_or(Value::Null))
                        .collect()
                })
                .collect()
        };

        let compare = |a: &usize, b: &usize| -> std::cmp::Ordering {
            let keys_a = &sort_keys[*a];
            let keys_b = &sort_keys[*b];
            for (i, sort_expr) in sort_exprs.iter().enumerate() {
                let val_a = &keys_a[i];
                let val_b = &keys_b[i];

                let ordering = compare_values_for_sort(val_a, val_b);
                let ordering = if !sort_expr.asc {
                    ordering.reverse()
                } else {
                    ordering
                };

                match (val_a.is_null(), val_b.is_null()) {
                    (true, true) => {}
                    (true, false) => {
                        return if sort_expr.nulls_first {
                            std::cmp::Ordering::Less
                        } else {
                            std::cmp::Ordering::Greater
                        };
                    }
                    (false, true) => {
                        return if sort_expr.nulls_first {
                            std::cmp::Ordering::Greater
                        } else {
                            std::cmp::Ordering::Less
                        };
                    }
                    (false, false) => {}
                }

                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        };

        let actual_limit = limit.min(n);
        let mut indices: Vec<usize> = (0..n).collect();

        if actual_limit < n / 4 {
            indices.select_nth_unstable_by(actual_limit, compare);
            indices.truncate(actual_limit);
            indices.sort_by(compare);
        } else {
            indices.sort_by(compare);
            indices.truncate(actual_limit);
        }

        input_table.gather_rows(&indices)
    }

    pub(crate) fn execute_distinct(&self, input: &PhysicalPlan) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let n = input_table.row_count();
        let columns: Vec<&Column> = input_table
            .columns()
            .iter()
            .map(|(_, c)| c.as_ref())
            .collect();

        let mut seen: FxHashSet<Vec<Value>> = FxHashSet::default();
        let mut unique_indices: Vec<usize> = Vec::with_capacity(n);

        for row_idx in 0..n {
            let values: Vec<Value> = columns.iter().map(|c| c.get_value(row_idx)).collect();
            if seen.insert(values) {
                unique_indices.push(row_idx);
            }
        }

        input_table.gather_rows(&unique_indices)
    }

    pub(crate) fn execute_aggregate(
        &self,
        input: &PhysicalPlan,
        group_by: &[Expr],
        aggregates: &[Expr],
        schema: &PlanSchema,
        grouping_sets: Option<&Vec<Vec<usize>>>,
        parallel: bool,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let vars = self.get_variables();
        let udf = self.get_user_functions();
        crate::executor::aggregate::compute_aggregate(
            &input_table,
            group_by,
            aggregates,
            schema,
            grouping_sets,
            &vars,
            &udf,
            parallel,
        )
    }

    pub(crate) fn execute_window(
        &self,
        input: &PhysicalPlan,
        window_exprs: &[Expr],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let vars = self.get_variables();
        let udf = self.get_user_functions();
        crate::executor::window::compute_window(&input_table, window_exprs, schema, &vars, &udf)
    }

    pub(crate) fn execute_values(
        &self,
        values: &[Vec<Expr>],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let result_schema = plan_schema_to_schema(schema);
        let empty_schema = Schema::new();
        let vars = self.get_variables();
        let sys_vars = self.get_system_variables();
        let udf = self.get_user_functions();
        let evaluator = ValueEvaluator::new(&empty_schema)
            .with_variables(&vars)
            .with_system_variables(&sys_vars)
            .with_user_functions(&udf);
        let empty_record = Record::new();
        let mut result = Table::empty(result_schema);

        for row_exprs in values {
            let mut row = Vec::new();
            for expr in row_exprs {
                let val = evaluator.evaluate(expr, &empty_record)?;
                row.push(val);
            }
            result.push_row(row)?;
        }

        Ok(result)
    }
}
