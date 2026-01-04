#![coverage(off)]

use std::collections::{HashMap, HashSet};

use tracing::instrument;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, JoinType, PlanSchema};
use yachtsql_storage::{Column, Record, Schema, Table};

use super::{ConcurrentPlanExecutor, plan_schema_to_schema};
use crate::plan::PhysicalPlan;
use crate::value_evaluator::ValueEvaluator;

fn extract_column_indices(keys: &[Expr], schema: &Schema) -> Option<Vec<usize>> {
    keys.iter()
        .map(|expr| match expr {
            Expr::Column { name, index, .. } => {
                if let Some(idx) = index {
                    Some(*idx)
                } else {
                    schema.field_index(name)
                }
            }
            _ => None,
        })
        .collect()
}

fn extract_key_values_direct(
    cols: &[&Column],
    row_idx: usize,
    col_indices: &[usize],
) -> Vec<Value> {
    col_indices
        .iter()
        .map(|&idx| cols[idx].get_value(row_idx))
        .collect()
}

impl ConcurrentPlanExecutor {
    #[instrument(skip(self, left, right, condition), fields(join_type = ?join_type))]
    pub(crate) async fn execute_nested_loop_join(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        join_type: &JoinType,
        condition: Option<&Expr>,
        schema: &PlanSchema,
        parallel: bool,
    ) -> Result<Table> {
        let (left_table, right_table) = if parallel {
            let executor_l = self.clone();
            let executor_r = self.clone();
            let left_plan = left.clone();
            let right_plan = right.clone();
            let (l, r) = tokio::join!(
                tokio::spawn(async move { executor_l.execute_plan(&left_plan).await }),
                tokio::spawn(async move { executor_r.execute_plan(&right_plan).await })
            );
            (
                l.map_err(|e| Error::Internal(e.to_string()))??,
                r.map_err(|e| Error::Internal(e.to_string()))??,
            )
        } else {
            (
                self.execute_plan(left).await?,
                self.execute_plan(right).await?,
            )
        };
        let left_schema = left_table.schema().clone();
        let right_schema = right_table.schema().clone();
        let result_schema = plan_schema_to_schema(schema);

        let mut combined_schema = Schema::new();
        for field in left_schema.fields() {
            combined_schema.add_field(field.clone());
        }
        for field in right_schema.fields() {
            combined_schema.add_field(field.clone());
        }

        let vars = self.get_variables();
        let sys_vars = self.get_system_variables();
        let udf = self.get_user_functions();
        let evaluator = ValueEvaluator::new(&combined_schema)
            .with_variables(&vars)
            .with_system_variables(&sys_vars)
            .with_user_functions(&udf);

        let mut result = Table::empty(result_schema.clone());
        let left_n = left_table.row_count();
        let right_n = right_table.row_count();
        let left_columns: Vec<&Column> = left_table
            .columns()
            .iter()
            .map(|(_, c)| c.as_ref())
            .collect();
        let right_columns: Vec<&Column> = right_table
            .columns()
            .iter()
            .map(|(_, c)| c.as_ref())
            .collect();
        let left_width = left_schema.field_count();
        let right_width = right_schema.field_count();

        match join_type {
            JoinType::Inner => {
                let mut combined_values: Vec<Value> = Vec::with_capacity(left_width + right_width);

                for left_idx in 0..left_n {
                    for right_idx in 0..right_n {
                        combined_values.clear();
                        combined_values.extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                        combined_values
                            .extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                        let matches = match condition {
                            Some(c) => {
                                let combined_record = Record::from_slice(&combined_values);
                                evaluator
                                    .evaluate(c, &combined_record)?
                                    .as_bool()
                                    .unwrap_or(false)
                            }
                            None => true,
                        };

                        if matches {
                            result.push_row(std::mem::take(&mut combined_values))?;
                            combined_values = Vec::with_capacity(left_width + right_width);
                        }
                    }
                }
            }
            JoinType::Left => {
                let mut combined_values: Vec<Value> = Vec::with_capacity(left_width + right_width);

                for left_idx in 0..left_n {
                    let mut found_match = false;
                    for right_idx in 0..right_n {
                        combined_values.clear();
                        combined_values.extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                        combined_values
                            .extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                        let matches = match condition {
                            Some(c) => {
                                let combined_record = Record::from_slice(&combined_values);
                                evaluator
                                    .evaluate(c, &combined_record)?
                                    .as_bool()
                                    .unwrap_or(false)
                            }
                            None => true,
                        };

                        if matches {
                            found_match = true;
                            result.push_row(std::mem::take(&mut combined_values))?;
                            combined_values = Vec::with_capacity(left_width + right_width);
                        }
                    }
                    if !found_match {
                        combined_values.clear();
                        combined_values.extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                        combined_values.extend(std::iter::repeat_n(Value::Null, right_width));
                        result.push_row(std::mem::take(&mut combined_values))?;
                        combined_values = Vec::with_capacity(left_width + right_width);
                    }
                }
            }
            JoinType::Right => {
                let mut combined_values: Vec<Value> = Vec::with_capacity(left_width + right_width);

                for right_idx in 0..right_n {
                    let mut found_match = false;
                    for left_idx in 0..left_n {
                        combined_values.clear();
                        combined_values.extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                        combined_values
                            .extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                        let matches = match condition {
                            Some(c) => {
                                let combined_record = Record::from_slice(&combined_values);
                                evaluator
                                    .evaluate(c, &combined_record)?
                                    .as_bool()
                                    .unwrap_or(false)
                            }
                            None => true,
                        };

                        if matches {
                            found_match = true;
                            result.push_row(std::mem::take(&mut combined_values))?;
                            combined_values = Vec::with_capacity(left_width + right_width);
                        }
                    }
                    if !found_match {
                        combined_values.clear();
                        combined_values.extend(std::iter::repeat_n(Value::Null, left_width));
                        combined_values
                            .extend(right_columns.iter().map(|c| c.get_value(right_idx)));
                        result.push_row(std::mem::take(&mut combined_values))?;
                        combined_values = Vec::with_capacity(left_width + right_width);
                    }
                }
            }
            JoinType::Full => {
                let mut matched_right: HashSet<usize> = HashSet::with_capacity(right_n);
                let mut combined_values: Vec<Value> = Vec::with_capacity(left_width + right_width);

                for left_idx in 0..left_n {
                    let mut found_match = false;
                    for right_idx in 0..right_n {
                        combined_values.clear();
                        combined_values.extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                        combined_values
                            .extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                        let matches = match condition {
                            Some(c) => {
                                let combined_record = Record::from_slice(&combined_values);
                                evaluator
                                    .evaluate(c, &combined_record)?
                                    .as_bool()
                                    .unwrap_or(false)
                            }
                            None => true,
                        };

                        if matches {
                            found_match = true;
                            matched_right.insert(right_idx);
                            result.push_row(std::mem::take(&mut combined_values))?;
                            combined_values = Vec::with_capacity(left_width + right_width);
                        }
                    }
                    if !found_match {
                        combined_values.clear();
                        combined_values.extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                        combined_values.extend(std::iter::repeat_n(Value::Null, right_width));
                        result.push_row(std::mem::take(&mut combined_values))?;
                        combined_values = Vec::with_capacity(left_width + right_width);
                    }
                }
                for right_idx in 0..right_n {
                    if !matched_right.contains(&right_idx) {
                        combined_values.clear();
                        combined_values.extend(std::iter::repeat_n(Value::Null, left_width));
                        combined_values
                            .extend(right_columns.iter().map(|c| c.get_value(right_idx)));
                        result.push_row(std::mem::take(&mut combined_values))?;
                        combined_values = Vec::with_capacity(left_width + right_width);
                    }
                }
            }
            JoinType::Cross => {
                let mut combined_values: Vec<Value> = Vec::with_capacity(left_width + right_width);

                for left_idx in 0..left_n {
                    for right_idx in 0..right_n {
                        combined_values.clear();
                        combined_values.extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                        combined_values
                            .extend(right_columns.iter().map(|c| c.get_value(right_idx)));
                        result.push_row(std::mem::take(&mut combined_values))?;
                        combined_values = Vec::with_capacity(left_width + right_width);
                    }
                }
            }
        }

        Ok(result)
    }

    pub(crate) async fn execute_cross_join(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        schema: &PlanSchema,
        parallel: bool,
    ) -> Result<Table> {
        self.execute_nested_loop_join(left, right, &JoinType::Cross, None, schema, parallel)
            .await
    }

    #[instrument(skip(self, left, right, left_keys, right_keys))]
    pub(crate) async fn execute_hash_join(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        join_type: &JoinType,
        left_keys: &[Expr],
        right_keys: &[Expr],
        schema: &PlanSchema,
        parallel: bool,
    ) -> Result<Table> {
        let (left_table, right_table) = if parallel {
            let executor_l = self.clone();
            let executor_r = self.clone();
            let left_plan = left.clone();
            let right_plan = right.clone();
            let (l, r) = tokio::join!(
                tokio::spawn(async move { executor_l.execute_plan(&left_plan).await }),
                tokio::spawn(async move { executor_r.execute_plan(&right_plan).await })
            );
            (
                l.map_err(|e| Error::Internal(e.to_string()))??,
                r.map_err(|e| Error::Internal(e.to_string()))??,
            )
        } else {
            (
                self.execute_plan(left).await?,
                self.execute_plan(right).await?,
            )
        };
        let left_schema = left_table.schema().clone();
        let right_schema = right_table.schema().clone();
        let result_schema = plan_schema_to_schema(schema);

        let left_n = left_table.row_count();
        let left_cols: Vec<&Column> = left_table
            .columns()
            .iter()
            .map(|(_, c)| c.as_ref())
            .collect();

        let right_n = right_table.row_count();
        let right_cols: Vec<&Column> = right_table
            .columns()
            .iter()
            .map(|(_, c)| c.as_ref())
            .collect();

        let left_width = left_schema.field_count();
        let right_width = right_schema.field_count();

        let vars = self.get_variables();
        let sys_vars = self.get_system_variables();
        let udf = self.get_user_functions();

        match join_type {
            JoinType::Inner => {
                let build_on_right = right_n <= left_n;

                let (
                    build_n,
                    probe_n,
                    build_cols,
                    probe_cols,
                    build_schema,
                    probe_schema,
                    build_keys,
                    probe_keys,
                ) = if build_on_right {
                    (
                        right_n,
                        left_n,
                        &right_cols,
                        &left_cols,
                        &right_schema,
                        &left_schema,
                        right_keys,
                        left_keys,
                    )
                } else {
                    (
                        left_n,
                        right_n,
                        &left_cols,
                        &right_cols,
                        &left_schema,
                        &right_schema,
                        left_keys,
                        right_keys,
                    )
                };

                let build_key_indices = extract_column_indices(build_keys, build_schema);
                let probe_key_indices = extract_column_indices(probe_keys, probe_schema);

                let mut hash_table: HashMap<Vec<Value>, Vec<usize>> =
                    HashMap::with_capacity(build_n);

                if let Some(ref indices) = build_key_indices {
                    for build_idx in 0..build_n {
                        let key_values = extract_key_values_direct(build_cols, build_idx, indices);
                        if key_values.iter().any(|v| matches!(v, Value::Null)) {
                            continue;
                        }
                        hash_table.entry(key_values).or_default().push(build_idx);
                    }
                } else {
                    let build_evaluator = ValueEvaluator::new(build_schema)
                        .with_variables(&vars)
                        .with_system_variables(&sys_vars)
                        .with_user_functions(&udf);
                    for build_idx in 0..build_n {
                        let build_record = Record::from_values(
                            build_cols.iter().map(|c| c.get_value(build_idx)).collect(),
                        );
                        let key_values: Vec<Value> = build_keys
                            .iter()
                            .map(|expr| build_evaluator.evaluate(expr, &build_record))
                            .collect::<Result<Vec<_>>>()?;
                        if key_values.iter().any(|v| matches!(v, Value::Null)) {
                            continue;
                        }
                        hash_table.entry(key_values).or_default().push(build_idx);
                    }
                }

                if parallel && probe_n >= 2000 {
                    let num_threads = std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(4);
                    let chunk_size = probe_n.div_ceil(num_threads);
                    let probe_indices: Vec<usize> = (0..probe_n).collect();

                    let chunk_results: Vec<Result<Vec<Vec<Value>>>> = std::thread::scope(|s| {
                        let handles: Vec<_> = probe_indices
                            .chunks(chunk_size)
                            .map(|chunk| {
                                let hash_table = &hash_table;
                                let vars = &vars;
                                let sys_vars = &sys_vars;
                                let udf = &udf;
                                let build_cols = &build_cols;
                                let probe_cols = &probe_cols;
                                let probe_key_indices = &probe_key_indices;
                                s.spawn(move || {
                                    let mut rows = Vec::new();
                                    if let Some(indices) = probe_key_indices {
                                        for &probe_idx in chunk {
                                            let key_values = extract_key_values_direct(
                                                probe_cols, probe_idx, indices,
                                            );
                                            if key_values.iter().any(|v| matches!(v, Value::Null)) {
                                                continue;
                                            }
                                            if let Some(matching_indices) =
                                                hash_table.get(&key_values)
                                            {
                                                for &build_idx in matching_indices {
                                                    let mut combined: Vec<Value> =
                                                        Vec::with_capacity(
                                                            left_width + right_width,
                                                        );
                                                    if build_on_right {
                                                        combined.extend(
                                                            probe_cols
                                                                .iter()
                                                                .map(|c| c.get_value(probe_idx)),
                                                        );
                                                        combined.extend(
                                                            build_cols
                                                                .iter()
                                                                .map(|c| c.get_value(build_idx)),
                                                        );
                                                    } else {
                                                        combined.extend(
                                                            build_cols
                                                                .iter()
                                                                .map(|c| c.get_value(build_idx)),
                                                        );
                                                        combined.extend(
                                                            probe_cols
                                                                .iter()
                                                                .map(|c| c.get_value(probe_idx)),
                                                        );
                                                    }
                                                    rows.push(combined);
                                                }
                                            }
                                        }
                                    } else {
                                        let probe_evaluator = ValueEvaluator::new(probe_schema)
                                            .with_variables(vars)
                                            .with_system_variables(sys_vars)
                                            .with_user_functions(udf);
                                        for &probe_idx in chunk {
                                            let probe_record = Record::from_values(
                                                probe_cols
                                                    .iter()
                                                    .map(|c| c.get_value(probe_idx))
                                                    .collect(),
                                            );
                                            let key_values: Vec<Value> = probe_keys
                                                .iter()
                                                .map(|expr| {
                                                    probe_evaluator.evaluate(expr, &probe_record)
                                                })
                                                .collect::<Result<Vec<_>>>()?;
                                            if key_values.iter().any(|v| matches!(v, Value::Null)) {
                                                continue;
                                            }
                                            if let Some(matching_indices) =
                                                hash_table.get(&key_values)
                                            {
                                                for &build_idx in matching_indices {
                                                    let mut combined: Vec<Value> =
                                                        Vec::with_capacity(
                                                            left_width + right_width,
                                                        );
                                                    if build_on_right {
                                                        combined.extend(
                                                            probe_cols
                                                                .iter()
                                                                .map(|c| c.get_value(probe_idx)),
                                                        );
                                                        combined.extend(
                                                            build_cols
                                                                .iter()
                                                                .map(|c| c.get_value(build_idx)),
                                                        );
                                                    } else {
                                                        combined.extend(
                                                            build_cols
                                                                .iter()
                                                                .map(|c| c.get_value(build_idx)),
                                                        );
                                                        combined.extend(
                                                            probe_cols
                                                                .iter()
                                                                .map(|c| c.get_value(probe_idx)),
                                                        );
                                                    }
                                                    rows.push(combined);
                                                }
                                            }
                                        }
                                    }
                                    Ok(rows)
                                })
                            })
                            .collect();
                        handles
                            .into_iter()
                            .map(|h| {
                                h.join()
                                    .unwrap_or_else(|_| Err(Error::internal("Thread join failed")))
                            })
                            .collect()
                    });

                    let mut result = Table::empty(result_schema);
                    for chunk_result in chunk_results {
                        for row in chunk_result? {
                            result.push_row(row)?;
                        }
                    }
                    Ok(result)
                } else {
                    let mut result = Table::empty(result_schema);
                    if let Some(ref indices) = probe_key_indices {
                        for probe_idx in 0..probe_n {
                            let key_values =
                                extract_key_values_direct(probe_cols, probe_idx, indices);
                            if key_values.iter().any(|v| matches!(v, Value::Null)) {
                                continue;
                            }
                            if let Some(matching_indices) = hash_table.get(&key_values) {
                                for &build_idx in matching_indices {
                                    let mut combined: Vec<Value> =
                                        Vec::with_capacity(left_width + right_width);
                                    if build_on_right {
                                        combined.extend(
                                            probe_cols.iter().map(|c| c.get_value(probe_idx)),
                                        );
                                        combined.extend(
                                            build_cols.iter().map(|c| c.get_value(build_idx)),
                                        );
                                    } else {
                                        combined.extend(
                                            build_cols.iter().map(|c| c.get_value(build_idx)),
                                        );
                                        combined.extend(
                                            probe_cols.iter().map(|c| c.get_value(probe_idx)),
                                        );
                                    }
                                    result.push_row(combined)?;
                                }
                            }
                        }
                        return Ok(result);
                    }
                    let probe_evaluator = ValueEvaluator::new(probe_schema)
                        .with_variables(&vars)
                        .with_system_variables(&sys_vars)
                        .with_user_functions(&udf);
                    for probe_idx in 0..probe_n {
                        let probe_record = Record::from_values(
                            probe_cols.iter().map(|c| c.get_value(probe_idx)).collect(),
                        );
                        let key_values: Vec<Value> = probe_keys
                            .iter()
                            .map(|expr| probe_evaluator.evaluate(expr, &probe_record))
                            .collect::<Result<Vec<_>>>()?;

                        if key_values.iter().any(|v| matches!(v, Value::Null)) {
                            continue;
                        }

                        if let Some(matching_indices) = hash_table.get(&key_values) {
                            for &build_idx in matching_indices {
                                let mut combined: Vec<Value> =
                                    Vec::with_capacity(left_width + right_width);
                                if build_on_right {
                                    combined
                                        .extend(probe_cols.iter().map(|c| c.get_value(probe_idx)));
                                    combined
                                        .extend(build_cols.iter().map(|c| c.get_value(build_idx)));
                                } else {
                                    combined
                                        .extend(build_cols.iter().map(|c| c.get_value(build_idx)));
                                    combined
                                        .extend(probe_cols.iter().map(|c| c.get_value(probe_idx)));
                                }
                                result.push_row(combined)?;
                            }
                        }
                    }
                    Ok(result)
                }
            }
            JoinType::Left => {
                let right_evaluator = ValueEvaluator::new(&right_schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);

                let mut hash_table: HashMap<Vec<Value>, Vec<usize>> =
                    HashMap::with_capacity(right_n);
                for right_idx in 0..right_n {
                    let right_record = Record::from_values(
                        right_cols.iter().map(|c| c.get_value(right_idx)).collect(),
                    );
                    let key_values: Vec<Value> = right_keys
                        .iter()
                        .map(|expr| right_evaluator.evaluate(expr, &right_record))
                        .collect::<Result<Vec<_>>>()?;

                    if key_values.iter().any(|v| matches!(v, Value::Null)) {
                        continue;
                    }

                    hash_table.entry(key_values).or_default().push(right_idx);
                }

                let left_evaluator = ValueEvaluator::new(&left_schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);

                let mut result = Table::empty(result_schema);
                let mut combined: Vec<Value> = Vec::with_capacity(left_width + right_width);

                for left_idx in 0..left_n {
                    let left_record = Record::from_values(
                        left_cols.iter().map(|c| c.get_value(left_idx)).collect(),
                    );
                    let key_values: Vec<Value> = left_keys
                        .iter()
                        .map(|expr| left_evaluator.evaluate(expr, &left_record))
                        .collect::<Result<Vec<_>>>()?;

                    let has_null_key = key_values.iter().any(|v| matches!(v, Value::Null));
                    let matching = if has_null_key {
                        None
                    } else {
                        hash_table.get(&key_values)
                    };

                    match matching {
                        Some(matches) => {
                            for &right_idx in matches {
                                combined.clear();
                                combined.extend(left_cols.iter().map(|c| c.get_value(left_idx)));
                                combined.extend(right_cols.iter().map(|c| c.get_value(right_idx)));
                                result.push_row(combined.clone())?;
                            }
                        }
                        None => {
                            combined.clear();
                            combined.extend(left_cols.iter().map(|c| c.get_value(left_idx)));
                            combined.extend(std::iter::repeat_n(Value::Null, right_width));
                            result.push_row(combined.clone())?;
                        }
                    }
                }
                Ok(result)
            }
            JoinType::Right => {
                let left_evaluator = ValueEvaluator::new(&left_schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);

                let mut hash_table: HashMap<Vec<Value>, Vec<usize>> =
                    HashMap::with_capacity(left_n);
                for left_idx in 0..left_n {
                    let left_record = Record::from_values(
                        left_cols.iter().map(|c| c.get_value(left_idx)).collect(),
                    );
                    let key_values: Vec<Value> = left_keys
                        .iter()
                        .map(|expr| left_evaluator.evaluate(expr, &left_record))
                        .collect::<Result<Vec<_>>>()?;

                    if key_values.iter().any(|v| matches!(v, Value::Null)) {
                        continue;
                    }

                    hash_table.entry(key_values).or_default().push(left_idx);
                }

                let right_evaluator = ValueEvaluator::new(&right_schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);

                let mut result = Table::empty(result_schema);
                let mut combined: Vec<Value> = Vec::with_capacity(left_width + right_width);

                for right_idx in 0..right_n {
                    let right_record = Record::from_values(
                        right_cols.iter().map(|c| c.get_value(right_idx)).collect(),
                    );
                    let key_values: Vec<Value> = right_keys
                        .iter()
                        .map(|expr| right_evaluator.evaluate(expr, &right_record))
                        .collect::<Result<Vec<_>>>()?;

                    let has_null_key = key_values.iter().any(|v| matches!(v, Value::Null));
                    let matching = if has_null_key {
                        None
                    } else {
                        hash_table.get(&key_values)
                    };

                    match matching {
                        Some(matches) => {
                            for &left_idx in matches {
                                combined.clear();
                                combined.extend(left_cols.iter().map(|c| c.get_value(left_idx)));
                                combined.extend(right_cols.iter().map(|c| c.get_value(right_idx)));
                                result.push_row(combined.clone())?;
                            }
                        }
                        None => {
                            combined.clear();
                            combined.extend(std::iter::repeat_n(Value::Null, left_width));
                            combined.extend(right_cols.iter().map(|c| c.get_value(right_idx)));
                            result.push_row(combined.clone())?;
                        }
                    }
                }
                Ok(result)
            }
            JoinType::Full => {
                let right_evaluator = ValueEvaluator::new(&right_schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);

                let mut hash_table: HashMap<Vec<Value>, Vec<usize>> =
                    HashMap::with_capacity(right_n);
                for right_idx in 0..right_n {
                    let right_record = Record::from_values(
                        right_cols.iter().map(|c| c.get_value(right_idx)).collect(),
                    );
                    let key_values: Vec<Value> = right_keys
                        .iter()
                        .map(|expr| right_evaluator.evaluate(expr, &right_record))
                        .collect::<Result<Vec<_>>>()?;

                    if key_values.iter().any(|v| matches!(v, Value::Null)) {
                        continue;
                    }

                    hash_table.entry(key_values).or_default().push(right_idx);
                }

                let left_evaluator = ValueEvaluator::new(&left_schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);

                let mut matched_right: HashSet<usize> = HashSet::with_capacity(right_n);
                let mut result = Table::empty(result_schema);
                let mut combined: Vec<Value> = Vec::with_capacity(left_width + right_width);

                for left_idx in 0..left_n {
                    let left_record = Record::from_values(
                        left_cols.iter().map(|c| c.get_value(left_idx)).collect(),
                    );
                    let key_values: Vec<Value> = left_keys
                        .iter()
                        .map(|expr| left_evaluator.evaluate(expr, &left_record))
                        .collect::<Result<Vec<_>>>()?;

                    let has_null_key = key_values.iter().any(|v| matches!(v, Value::Null));
                    let matching = if has_null_key {
                        None
                    } else {
                        hash_table.get(&key_values)
                    };

                    match matching {
                        Some(matches) => {
                            for &right_idx in matches {
                                matched_right.insert(right_idx);
                                combined.clear();
                                combined.extend(left_cols.iter().map(|c| c.get_value(left_idx)));
                                combined.extend(right_cols.iter().map(|c| c.get_value(right_idx)));
                                result.push_row(combined.clone())?;
                            }
                        }
                        None => {
                            combined.clear();
                            combined.extend(left_cols.iter().map(|c| c.get_value(left_idx)));
                            combined.extend(std::iter::repeat_n(Value::Null, right_width));
                            result.push_row(combined.clone())?;
                        }
                    }
                }

                for right_idx in 0..right_n {
                    if !matched_right.contains(&right_idx) {
                        combined.clear();
                        combined.extend(std::iter::repeat_n(Value::Null, left_width));
                        combined.extend(right_cols.iter().map(|c| c.get_value(right_idx)));
                        result.push_row(combined.clone())?;
                    }
                }

                Ok(result)
            }
            JoinType::Cross => {
                return Err(Error::internal(
                    "Cross join should not be handled by HashJoin",
                ));
            }
        }
    }
}
