#![coverage(off)]

use std::collections::{HashMap, HashSet};

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, JoinType, PlanSchema};
use yachtsql_storage::{Column, Record, Schema, Table};

use super::{ConcurrentPlanExecutor, plan_schema_to_schema};
use crate::plan::PhysicalPlan;
use crate::value_evaluator::ValueEvaluator;

impl ConcurrentPlanExecutor {
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

        let get_left_values =
            |idx: usize| -> Vec<Value> { left_columns.iter().map(|c| c.get_value(idx)).collect() };
        let get_right_values =
            |idx: usize| -> Vec<Value> { right_columns.iter().map(|c| c.get_value(idx)).collect() };

        match join_type {
            JoinType::Inner => {
                for left_idx in 0..left_n {
                    let left_values = get_left_values(left_idx);
                    for right_idx in 0..right_n {
                        let right_values = get_right_values(right_idx);
                        let mut combined = left_values.clone();
                        combined.extend(right_values);
                        let combined_record = Record::from_values(combined.clone());

                        let matches = condition
                            .map(|c| evaluator.evaluate(c, &combined_record))
                            .transpose()?
                            .map(|v| v.as_bool().unwrap_or(false))
                            .unwrap_or(true);

                        if matches {
                            result.push_row(combined)?;
                        }
                    }
                }
            }
            JoinType::Left => {
                for left_idx in 0..left_n {
                    let left_values = get_left_values(left_idx);
                    let mut found_match = false;
                    for right_idx in 0..right_n {
                        let right_values = get_right_values(right_idx);
                        let mut combined = left_values.clone();
                        combined.extend(right_values);
                        let combined_record = Record::from_values(combined.clone());

                        let matches = condition
                            .map(|c| evaluator.evaluate(c, &combined_record))
                            .transpose()?
                            .map(|v| v.as_bool().unwrap_or(false))
                            .unwrap_or(true);

                        if matches {
                            found_match = true;
                            result.push_row(combined)?;
                        }
                    }
                    if !found_match {
                        let mut combined = left_values;
                        combined.extend(vec![Value::Null; right_width]);
                        result.push_row(combined)?;
                    }
                }
            }
            JoinType::Right => {
                for right_idx in 0..right_n {
                    let right_values = get_right_values(right_idx);
                    let mut found_match = false;
                    for left_idx in 0..left_n {
                        let left_values = get_left_values(left_idx);
                        let mut combined = left_values;
                        combined.extend(right_values.clone());
                        let combined_record = Record::from_values(combined.clone());

                        let matches = condition
                            .map(|c| evaluator.evaluate(c, &combined_record))
                            .transpose()?
                            .map(|v| v.as_bool().unwrap_or(false))
                            .unwrap_or(true);

                        if matches {
                            found_match = true;
                            result.push_row(combined)?;
                        }
                    }
                    if !found_match {
                        let mut combined = vec![Value::Null; left_width];
                        combined.extend(right_values);
                        result.push_row(combined)?;
                    }
                }
            }
            JoinType::Full => {
                let mut matched_right: HashSet<usize> = HashSet::new();
                for left_idx in 0..left_n {
                    let left_values = get_left_values(left_idx);
                    let mut found_match = false;
                    for right_idx in 0..right_n {
                        let right_values = get_right_values(right_idx);
                        let mut combined = left_values.clone();
                        combined.extend(right_values);
                        let combined_record = Record::from_values(combined.clone());

                        let matches = condition
                            .map(|c| evaluator.evaluate(c, &combined_record))
                            .transpose()?
                            .map(|v| v.as_bool().unwrap_or(false))
                            .unwrap_or(true);

                        if matches {
                            found_match = true;
                            matched_right.insert(right_idx);
                            result.push_row(combined)?;
                        }
                    }
                    if !found_match {
                        let mut combined = left_values;
                        combined.extend(vec![Value::Null; right_width]);
                        result.push_row(combined)?;
                    }
                }
                for right_idx in 0..right_n {
                    if !matched_right.contains(&right_idx) {
                        let mut combined = vec![Value::Null; left_width];
                        combined.extend(get_right_values(right_idx));
                        result.push_row(combined)?;
                    }
                }
            }
            JoinType::Cross => {
                for left_idx in 0..left_n {
                    let left_values = get_left_values(left_idx);
                    for right_idx in 0..right_n {
                        let mut combined = left_values.clone();
                        combined.extend(get_right_values(right_idx));
                        result.push_row(combined)?;
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

        match join_type {
            JoinType::Inner => {
                let left_n = left_table.row_count();
                let left_cols: Vec<&Column> = left_table
                    .columns()
                    .iter()
                    .map(|(_, c)| c.as_ref())
                    .collect();
                let left_rows: Vec<Record> = (0..left_n)
                    .map(|i| {
                        Record::from_values(left_cols.iter().map(|c| c.get_value(i)).collect())
                    })
                    .collect();

                let right_n = right_table.row_count();
                let right_cols: Vec<&Column> = right_table
                    .columns()
                    .iter()
                    .map(|(_, c)| c.as_ref())
                    .collect();
                let right_rows: Vec<Record> = (0..right_n)
                    .map(|i| {
                        Record::from_values(right_cols.iter().map(|c| c.get_value(i)).collect())
                    })
                    .collect();

                let build_on_right = right_rows.len() <= left_rows.len();

                let (build_rows, probe_rows, build_schema, probe_schema, build_keys, probe_keys) =
                    if build_on_right {
                        (
                            right_rows,
                            left_rows,
                            &right_schema,
                            &left_schema,
                            right_keys,
                            left_keys,
                        )
                    } else {
                        (
                            left_rows,
                            right_rows,
                            &left_schema,
                            &right_schema,
                            left_keys,
                            right_keys,
                        )
                    };

                let vars = self.get_variables();
                let sys_vars = self.get_system_variables();
                let udf = self.get_user_functions();
                let build_evaluator = ValueEvaluator::new(build_schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);

                let mut hash_table: HashMap<Vec<Value>, Vec<Record>> = HashMap::new();
                for build_record in &build_rows {
                    let key_values: Vec<Value> = build_keys
                        .iter()
                        .map(|expr| build_evaluator.evaluate(expr, build_record))
                        .collect::<Result<Vec<_>>>()?;

                    if key_values.iter().any(|v| matches!(v, Value::Null)) {
                        continue;
                    }

                    hash_table
                        .entry(key_values)
                        .or_default()
                        .push(build_record.clone());
                }

                let combine_row = |probe_rec: &Record, build_rec: &Record| -> Vec<Value> {
                    if build_on_right {
                        let mut combined = probe_rec.values().to_vec();
                        combined.extend(build_rec.values().to_vec());
                        combined
                    } else {
                        let mut combined = build_rec.values().to_vec();
                        combined.extend(probe_rec.values().to_vec());
                        combined
                    }
                };

                if parallel && probe_rows.len() >= 10000 {
                    let num_threads = std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(4);
                    let chunk_size = probe_rows.len().div_ceil(num_threads);

                    let chunk_results: Vec<Result<Vec<Vec<Value>>>> = std::thread::scope(|s| {
                        let handles: Vec<_> = probe_rows
                            .chunks(chunk_size)
                            .map(|chunk| {
                                let hash_table = &hash_table;
                                let vars = &vars;
                                let sys_vars = &sys_vars;
                                let udf = &udf;
                                let combine_row = &combine_row;
                                s.spawn(move || {
                                    let probe_evaluator = ValueEvaluator::new(probe_schema)
                                        .with_variables(vars)
                                        .with_system_variables(sys_vars)
                                        .with_user_functions(udf);
                                    let mut rows = Vec::new();
                                    for probe_record in chunk {
                                        let key_values: Vec<Value> = probe_keys
                                            .iter()
                                            .map(|expr| {
                                                probe_evaluator.evaluate(expr, probe_record)
                                            })
                                            .collect::<Result<Vec<_>>>()?;

                                        if key_values.iter().any(|v| matches!(v, Value::Null)) {
                                            continue;
                                        }

                                        if let Some(matching_rows) = hash_table.get(&key_values) {
                                            for build_record in matching_rows {
                                                rows.push(combine_row(probe_record, build_record));
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
                    let probe_evaluator = ValueEvaluator::new(probe_schema)
                        .with_variables(&vars)
                        .with_system_variables(&sys_vars)
                        .with_user_functions(&udf);
                    let mut result = Table::empty(result_schema);
                    for probe_record in &probe_rows {
                        let key_values: Vec<Value> = probe_keys
                            .iter()
                            .map(|expr| probe_evaluator.evaluate(expr, probe_record))
                            .collect::<Result<Vec<_>>>()?;

                        if key_values.iter().any(|v| matches!(v, Value::Null)) {
                            continue;
                        }

                        if let Some(matching_rows) = hash_table.get(&key_values) {
                            for build_record in matching_rows {
                                result.push_row(combine_row(probe_record, build_record))?;
                            }
                        }
                    }
                    Ok(result)
                }
            }
            _ => Err(Error::unsupported(
                "HashJoin only supports Inner join type currently",
            )),
        }
    }
}
