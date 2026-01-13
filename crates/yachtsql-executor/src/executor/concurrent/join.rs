#![coverage(off)]

use std::hash::{Hash, Hasher};

use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
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

fn hash_json_value<H: Hasher>(json: &serde_json::Value, hasher: &mut H) {
    match json {
        serde_json::Value::Null => 0u8.hash(hasher),
        serde_json::Value::Bool(b) => {
            1u8.hash(hasher);
            b.hash(hasher);
        }
        serde_json::Value::Number(n) => {
            2u8.hash(hasher);
            if let Some(i) = n.as_i64() {
                i.hash(hasher);
            } else if let Some(u) = n.as_u64() {
                u.hash(hasher);
            } else if let Some(f) = n.as_f64() {
                f.to_bits().hash(hasher);
            }
        }
        serde_json::Value::String(s) => {
            3u8.hash(hasher);
            s.hash(hasher);
        }
        serde_json::Value::Array(arr) => {
            4u8.hash(hasher);
            arr.len().hash(hasher);
            for v in arr {
                hash_json_value(v, hasher);
            }
        }
        serde_json::Value::Object(obj) => {
            5u8.hash(hasher);
            obj.len().hash(hasher);
            for (k, v) in obj {
                k.hash(hasher);
                hash_json_value(v, hasher);
            }
        }
    }
}

fn hash_key_values(key_values: &[Value]) -> u64 {
    let mut hasher = rustc_hash::FxHasher::default();
    for value in key_values {
        match value {
            Value::Null => 0u8.hash(&mut hasher),
            Value::Bool(b) => b.hash(&mut hasher),
            Value::Int64(i) => i.hash(&mut hasher),
            Value::Float64(f) => f.to_bits().hash(&mut hasher),
            Value::Numeric(n) => n.hash(&mut hasher),
            Value::BigNumeric(n) => n.hash(&mut hasher),
            Value::String(s) => s.hash(&mut hasher),
            Value::Bytes(b) => b.hash(&mut hasher),
            Value::Date(d) => d.hash(&mut hasher),
            Value::Time(t) => t.hash(&mut hasher),
            Value::DateTime(dt) => dt.hash(&mut hasher),
            Value::Timestamp(ts) => ts.hash(&mut hasher),
            Value::Json(j) => hash_json_value(j, &mut hasher),
            Value::Array(a) => {
                for v in a {
                    hash_key_values(std::slice::from_ref(v)).hash(&mut hasher);
                }
            }
            Value::Struct(s) => {
                for (k, v) in s {
                    k.hash(&mut hasher);
                    hash_key_values(std::slice::from_ref(v)).hash(&mut hasher);
                }
            }
            Value::Geography(g) => g.hash(&mut hasher),
            Value::Interval(i) => {
                i.months.hash(&mut hasher);
                i.days.hash(&mut hasher);
                i.nanos.hash(&mut hasher);
            }
            Value::Range(r) => {
                r.start
                    .as_ref()
                    .map(|v| hash_key_values(std::slice::from_ref(v.as_ref())))
                    .hash(&mut hasher);
                r.end
                    .as_ref()
                    .map(|v| hash_key_values(std::slice::from_ref(v.as_ref())))
                    .hash(&mut hasher);
            }
            Value::Default => 1u8.hash(&mut hasher),
        }
    }
    hasher.finish()
}

fn build_hash_table_direct(
    cols: &[&Column],
    n: usize,
    col_indices: &[usize],
) -> FxHashMap<Vec<Value>, Vec<usize>> {
    let mut hash_table: FxHashMap<Vec<Value>, Vec<usize>> =
        FxHashMap::with_capacity_and_hasher(n, Default::default());
    for idx in 0..n {
        let key_values = extract_key_values_direct(cols, idx, col_indices);
        if key_values.iter().any(|v| matches!(v, Value::Null)) {
            continue;
        }
        hash_table.entry(key_values).or_default().push(idx);
    }
    hash_table
}

impl ConcurrentPlanExecutor {
    #[instrument(skip(self, left, right, condition), fields(join_type = ?join_type))]
    pub(crate) fn execute_nested_loop_join(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        join_type: &JoinType,
        condition: Option<&Expr>,
        schema: &PlanSchema,
        parallel: bool,
    ) -> Result<Table> {
        let (left_table, right_table) = if parallel {
            let (l, r) = rayon::join(|| self.execute_plan(left), || self.execute_plan(right));
            (l?, r?)
        } else {
            (self.execute_plan(left)?, self.execute_plan(right)?)
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

        let threshold = self.get_parallel_threshold();
        let total_work = left_n.saturating_mul(right_n);
        let use_parallel = parallel && total_work >= threshold;

        match join_type {
            JoinType::Inner => {
                if use_parallel {
                    let row_batches: Vec<Vec<Vec<Value>>> = (0..left_n)
                        .into_par_iter()
                        .map(|left_idx| {
                            let mut matches = Vec::new();
                            for right_idx in 0..right_n {
                                let mut combined_values: Vec<Value> =
                                    Vec::with_capacity(left_width + right_width);
                                combined_values
                                    .extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                                combined_values
                                    .extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                                let is_match = match condition {
                                    Some(c) => {
                                        let eval_record = Record::from_slice(&combined_values);
                                        evaluator
                                            .evaluate(c, &eval_record)
                                            .ok()
                                            .and_then(|v| v.as_bool())
                                            .unwrap_or(false)
                                    }
                                    None => true,
                                };

                                if is_match {
                                    matches.push(combined_values);
                                }
                            }
                            matches
                        })
                        .collect();

                    for batch in row_batches {
                        for row in batch {
                            result.push_row(row)?;
                        }
                    }
                } else {
                    let mut combined_values: Vec<Value> =
                        Vec::with_capacity(left_width + right_width);
                    let mut eval_record = Record::with_capacity(left_width + right_width);
                    for left_idx in 0..left_n {
                        for right_idx in 0..right_n {
                            combined_values.clear();
                            combined_values
                                .extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                            combined_values
                                .extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                            let matches = match condition {
                                Some(c) => {
                                    eval_record.set_from_slice(&combined_values);
                                    evaluator
                                        .evaluate(c, &eval_record)?
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
            }
            JoinType::Left => {
                if use_parallel {
                    let row_batches: Vec<Vec<Vec<Value>>> = (0..left_n)
                        .into_par_iter()
                        .map(|left_idx| {
                            let mut output_rows = Vec::new();
                            let mut found_match = false;
                            for right_idx in 0..right_n {
                                let mut combined_values: Vec<Value> =
                                    Vec::with_capacity(left_width + right_width);
                                combined_values
                                    .extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                                combined_values
                                    .extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                                let is_match = match condition {
                                    Some(c) => {
                                        let eval_record = Record::from_slice(&combined_values);
                                        evaluator
                                            .evaluate(c, &eval_record)
                                            .ok()
                                            .and_then(|v| v.as_bool())
                                            .unwrap_or(false)
                                    }
                                    None => true,
                                };

                                if is_match {
                                    found_match = true;
                                    output_rows.push(combined_values);
                                }
                            }
                            if !found_match {
                                let mut null_row: Vec<Value> =
                                    Vec::with_capacity(left_width + right_width);
                                null_row.extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                                null_row.extend(std::iter::repeat_n(Value::Null, right_width));
                                output_rows.push(null_row);
                            }
                            output_rows
                        })
                        .collect();

                    for batch in row_batches {
                        for row in batch {
                            result.push_row(row)?;
                        }
                    }
                } else {
                    let mut combined_values: Vec<Value> =
                        Vec::with_capacity(left_width + right_width);
                    let mut eval_record = Record::with_capacity(left_width + right_width);
                    for left_idx in 0..left_n {
                        let mut found_match = false;
                        for right_idx in 0..right_n {
                            combined_values.clear();
                            combined_values
                                .extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                            combined_values
                                .extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                            let matches = match condition {
                                Some(c) => {
                                    eval_record.set_from_slice(&combined_values);
                                    evaluator
                                        .evaluate(c, &eval_record)?
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
                            combined_values
                                .extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                            combined_values.extend(std::iter::repeat_n(Value::Null, right_width));
                            result.push_row(std::mem::take(&mut combined_values))?;
                            combined_values = Vec::with_capacity(left_width + right_width);
                        }
                    }
                }
            }
            JoinType::Right => {
                if use_parallel {
                    let row_batches: Vec<Vec<Vec<Value>>> = (0..right_n)
                        .into_par_iter()
                        .map(|right_idx| {
                            let mut output_rows = Vec::new();
                            let mut found_match = false;
                            for left_idx in 0..left_n {
                                let mut combined_values: Vec<Value> =
                                    Vec::with_capacity(left_width + right_width);
                                combined_values
                                    .extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                                combined_values
                                    .extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                                let is_match = match condition {
                                    Some(c) => {
                                        let eval_record = Record::from_slice(&combined_values);
                                        evaluator
                                            .evaluate(c, &eval_record)
                                            .ok()
                                            .and_then(|v| v.as_bool())
                                            .unwrap_or(false)
                                    }
                                    None => true,
                                };

                                if is_match {
                                    found_match = true;
                                    output_rows.push(combined_values);
                                }
                            }
                            if !found_match {
                                let mut null_row: Vec<Value> =
                                    Vec::with_capacity(left_width + right_width);
                                null_row.extend(std::iter::repeat_n(Value::Null, left_width));
                                null_row
                                    .extend(right_columns.iter().map(|c| c.get_value(right_idx)));
                                output_rows.push(null_row);
                            }
                            output_rows
                        })
                        .collect();

                    for batch in row_batches {
                        for row in batch {
                            result.push_row(row)?;
                        }
                    }
                } else {
                    let mut combined_values: Vec<Value> =
                        Vec::with_capacity(left_width + right_width);
                    let mut eval_record = Record::with_capacity(left_width + right_width);
                    for right_idx in 0..right_n {
                        let mut found_match = false;
                        for left_idx in 0..left_n {
                            combined_values.clear();
                            combined_values
                                .extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                            combined_values
                                .extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                            let matches = match condition {
                                Some(c) => {
                                    eval_record.set_from_slice(&combined_values);
                                    evaluator
                                        .evaluate(c, &eval_record)?
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
            }
            JoinType::Full => {
                let mut combined_values: Vec<Value> = Vec::with_capacity(left_width + right_width);
                let mut eval_record = Record::with_capacity(left_width + right_width);
                let mut matched_right: FxHashSet<usize> =
                    FxHashSet::with_capacity_and_hasher(right_n, Default::default());

                for left_idx in 0..left_n {
                    let mut found_match = false;
                    for right_idx in 0..right_n {
                        combined_values.clear();
                        combined_values.extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                        combined_values
                            .extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                        let matches = match condition {
                            Some(c) => {
                                eval_record.set_from_slice(&combined_values);
                                evaluator
                                    .evaluate(c, &eval_record)?
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
                if use_parallel {
                    let row_batches: Vec<Vec<Vec<Value>>> = (0..left_n)
                        .into_par_iter()
                        .map(|left_idx| {
                            let mut rows = Vec::with_capacity(right_n);
                            for right_idx in 0..right_n {
                                let mut combined_values: Vec<Value> =
                                    Vec::with_capacity(left_width + right_width);
                                combined_values
                                    .extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                                combined_values
                                    .extend(right_columns.iter().map(|c| c.get_value(right_idx)));
                                rows.push(combined_values);
                            }
                            rows
                        })
                        .collect();

                    for batch in row_batches {
                        for row in batch {
                            result.push_row(row)?;
                        }
                    }
                } else {
                    let mut combined_values: Vec<Value> =
                        Vec::with_capacity(left_width + right_width);
                    for left_idx in 0..left_n {
                        for right_idx in 0..right_n {
                            combined_values.clear();
                            combined_values
                                .extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                            combined_values
                                .extend(right_columns.iter().map(|c| c.get_value(right_idx)));
                            result.push_row(std::mem::take(&mut combined_values))?;
                            combined_values = Vec::with_capacity(left_width + right_width);
                        }
                    }
                }
            }
            JoinType::LeftSemi => {
                if use_parallel {
                    let row_batches: Vec<Option<Vec<Value>>> = (0..left_n)
                        .into_par_iter()
                        .map(|left_idx| {
                            for right_idx in 0..right_n {
                                let mut combined_values: Vec<Value> =
                                    Vec::with_capacity(left_width + right_width);
                                combined_values
                                    .extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                                combined_values
                                    .extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                                let is_match = match condition {
                                    Some(c) => {
                                        let eval_record = Record::from_slice(&combined_values);
                                        evaluator
                                            .evaluate(c, &eval_record)
                                            .ok()
                                            .and_then(|v| v.as_bool())
                                            .unwrap_or(false)
                                    }
                                    None => true,
                                };

                                if is_match {
                                    let left_row: Vec<Value> = left_columns
                                        .iter()
                                        .map(|c| c.get_value(left_idx))
                                        .collect();
                                    return Some(left_row);
                                }
                            }
                            None
                        })
                        .collect();

                    for row in row_batches.into_iter().flatten() {
                        result.push_row(row)?;
                    }
                } else {
                    let mut combined_values: Vec<Value> =
                        Vec::with_capacity(left_width + right_width);
                    let mut eval_record = Record::with_capacity(left_width + right_width);
                    for left_idx in 0..left_n {
                        let mut found_match = false;
                        for right_idx in 0..right_n {
                            combined_values.clear();
                            combined_values
                                .extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                            combined_values
                                .extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                            let matches = match condition {
                                Some(c) => {
                                    eval_record.set_from_slice(&combined_values);
                                    evaluator
                                        .evaluate(c, &eval_record)?
                                        .as_bool()
                                        .unwrap_or(false)
                                }
                                None => true,
                            };

                            if matches {
                                found_match = true;
                                break;
                            }
                        }
                        if found_match {
                            let left_row: Vec<Value> =
                                left_columns.iter().map(|c| c.get_value(left_idx)).collect();
                            result.push_row(left_row)?;
                        }
                    }
                }
            }
            JoinType::LeftAnti => {
                if use_parallel {
                    let row_batches: Vec<Option<Vec<Value>>> = (0..left_n)
                        .into_par_iter()
                        .map(|left_idx| {
                            for right_idx in 0..right_n {
                                let mut combined_values: Vec<Value> =
                                    Vec::with_capacity(left_width + right_width);
                                combined_values
                                    .extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                                combined_values
                                    .extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                                let is_match = match condition {
                                    Some(c) => {
                                        let eval_record = Record::from_slice(&combined_values);
                                        evaluator
                                            .evaluate(c, &eval_record)
                                            .ok()
                                            .and_then(|v| v.as_bool())
                                            .unwrap_or(false)
                                    }
                                    None => true,
                                };

                                if is_match {
                                    return None;
                                }
                            }
                            let left_row: Vec<Value> =
                                left_columns.iter().map(|c| c.get_value(left_idx)).collect();
                            Some(left_row)
                        })
                        .collect();

                    for row in row_batches.into_iter().flatten() {
                        result.push_row(row)?;
                    }
                } else {
                    let mut combined_values: Vec<Value> =
                        Vec::with_capacity(left_width + right_width);
                    let mut eval_record = Record::with_capacity(left_width + right_width);
                    for left_idx in 0..left_n {
                        let mut found_match = false;
                        for right_idx in 0..right_n {
                            combined_values.clear();
                            combined_values
                                .extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                            combined_values
                                .extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                            let matches = match condition {
                                Some(c) => {
                                    eval_record.set_from_slice(&combined_values);
                                    evaluator
                                        .evaluate(c, &eval_record)?
                                        .as_bool()
                                        .unwrap_or(false)
                                }
                                None => true,
                            };

                            if matches {
                                found_match = true;
                                break;
                            }
                        }
                        if !found_match {
                            let left_row: Vec<Value> =
                                left_columns.iter().map(|c| c.get_value(left_idx)).collect();
                            result.push_row(left_row)?;
                        }
                    }
                }
            }
            JoinType::RightSemi => {
                if use_parallel {
                    let row_batches: Vec<Option<Vec<Value>>> = (0..right_n)
                        .into_par_iter()
                        .map(|right_idx| {
                            for left_idx in 0..left_n {
                                let mut combined_values: Vec<Value> =
                                    Vec::with_capacity(left_width + right_width);
                                combined_values
                                    .extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                                combined_values
                                    .extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                                let is_match = match condition {
                                    Some(c) => {
                                        let eval_record = Record::from_slice(&combined_values);
                                        evaluator
                                            .evaluate(c, &eval_record)
                                            .ok()
                                            .and_then(|v| v.as_bool())
                                            .unwrap_or(false)
                                    }
                                    None => true,
                                };

                                if is_match {
                                    let right_row: Vec<Value> = right_columns
                                        .iter()
                                        .map(|c| c.get_value(right_idx))
                                        .collect();
                                    return Some(right_row);
                                }
                            }
                            None
                        })
                        .collect();

                    for row in row_batches.into_iter().flatten() {
                        result.push_row(row)?;
                    }
                } else {
                    let mut combined_values: Vec<Value> =
                        Vec::with_capacity(left_width + right_width);
                    let mut eval_record = Record::with_capacity(left_width + right_width);
                    for right_idx in 0..right_n {
                        let mut found_match = false;
                        for left_idx in 0..left_n {
                            combined_values.clear();
                            combined_values
                                .extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                            combined_values
                                .extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                            let matches = match condition {
                                Some(c) => {
                                    eval_record.set_from_slice(&combined_values);
                                    evaluator
                                        .evaluate(c, &eval_record)?
                                        .as_bool()
                                        .unwrap_or(false)
                                }
                                None => true,
                            };

                            if matches {
                                found_match = true;
                                break;
                            }
                        }
                        if found_match {
                            let right_row: Vec<Value> = right_columns
                                .iter()
                                .map(|c| c.get_value(right_idx))
                                .collect();
                            result.push_row(right_row)?;
                        }
                    }
                }
            }
            JoinType::RightAnti => {
                if use_parallel {
                    let row_batches: Vec<Option<Vec<Value>>> = (0..right_n)
                        .into_par_iter()
                        .map(|right_idx| {
                            for left_idx in 0..left_n {
                                let mut combined_values: Vec<Value> =
                                    Vec::with_capacity(left_width + right_width);
                                combined_values
                                    .extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                                combined_values
                                    .extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                                let is_match = match condition {
                                    Some(c) => {
                                        let eval_record = Record::from_slice(&combined_values);
                                        evaluator
                                            .evaluate(c, &eval_record)
                                            .ok()
                                            .and_then(|v| v.as_bool())
                                            .unwrap_or(false)
                                    }
                                    None => true,
                                };

                                if is_match {
                                    return None;
                                }
                            }
                            let right_row: Vec<Value> = right_columns
                                .iter()
                                .map(|c| c.get_value(right_idx))
                                .collect();
                            Some(right_row)
                        })
                        .collect();

                    for row in row_batches.into_iter().flatten() {
                        result.push_row(row)?;
                    }
                } else {
                    let mut combined_values: Vec<Value> =
                        Vec::with_capacity(left_width + right_width);
                    let mut eval_record = Record::with_capacity(left_width + right_width);
                    for right_idx in 0..right_n {
                        let mut found_match = false;
                        for left_idx in 0..left_n {
                            combined_values.clear();
                            combined_values
                                .extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                            combined_values
                                .extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                            let matches = match condition {
                                Some(c) => {
                                    eval_record.set_from_slice(&combined_values);
                                    evaluator
                                        .evaluate(c, &eval_record)?
                                        .as_bool()
                                        .unwrap_or(false)
                                }
                                None => true,
                            };

                            if matches {
                                found_match = true;
                                break;
                            }
                        }
                        if !found_match {
                            let right_row: Vec<Value> = right_columns
                                .iter()
                                .map(|c| c.get_value(right_idx))
                                .collect();
                            result.push_row(right_row)?;
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    pub(crate) fn execute_cross_join(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        schema: &PlanSchema,
        parallel: bool,
    ) -> Result<Table> {
        self.execute_nested_loop_join(left, right, &JoinType::Cross, None, schema, parallel)
    }

    #[instrument(skip(self, left, right, left_keys, right_keys))]
    pub(crate) fn execute_hash_join(
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
            let (l, r) = rayon::join(|| self.execute_plan(left), || self.execute_plan(right));
            (l?, r?)
        } else {
            (self.execute_plan(left)?, self.execute_plan(right)?)
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
        let threshold = self.get_parallel_threshold();

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

                let hash_table = if let Some(ref indices) = build_key_indices {
                    build_hash_table_direct(build_cols, build_n, indices)
                } else {
                    let mut hash_table: FxHashMap<Vec<Value>, Vec<usize>> =
                        FxHashMap::with_capacity_and_hasher(build_n, Default::default());
                    let build_evaluator = ValueEvaluator::new(build_schema)
                        .with_variables(&vars)
                        .with_system_variables(&sys_vars)
                        .with_user_functions(&udf);
                    let mut build_record = Record::with_capacity(build_cols.len());
                    let mut build_values: Vec<Value> = Vec::with_capacity(build_cols.len());
                    for build_idx in 0..build_n {
                        build_values.clear();
                        build_values.extend(build_cols.iter().map(|c| c.get_value(build_idx)));
                        build_record.set_from_slice(&build_values);
                        let key_values: Vec<Value> = build_keys
                            .iter()
                            .map(|expr| build_evaluator.evaluate(expr, &build_record))
                            .collect::<Result<Vec<_>>>()?;
                        if key_values.iter().any(|v| matches!(v, Value::Null)) {
                            continue;
                        }
                        hash_table.entry(key_values).or_default().push(build_idx);
                    }
                    hash_table
                };

                if parallel && probe_n >= threshold {
                    let row_batches: Vec<Vec<Vec<Value>>> = if let Some(ref indices) =
                        probe_key_indices
                    {
                        (0..probe_n)
                            .into_par_iter()
                            .map(|probe_idx| {
                                let key_values =
                                    extract_key_values_direct(probe_cols, probe_idx, indices);
                                if key_values.iter().any(|v| matches!(v, Value::Null)) {
                                    return Vec::new();
                                }
                                let Some(matching_indices) = hash_table.get(&key_values) else {
                                    return Vec::new();
                                };
                                matching_indices
                                    .iter()
                                    .map(|&build_idx| {
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
                                        combined
                                    })
                                    .collect()
                            })
                            .collect()
                    } else {
                        (0..probe_n)
                            .into_par_iter()
                            .map(|probe_idx| {
                                let probe_evaluator = ValueEvaluator::new(probe_schema)
                                    .with_variables(&vars)
                                    .with_system_variables(&sys_vars)
                                    .with_user_functions(&udf);
                                let probe_values: Vec<Value> =
                                    probe_cols.iter().map(|c| c.get_value(probe_idx)).collect();
                                let probe_record = Record::from_slice(&probe_values);
                                let key_values: Vec<Value> = probe_keys
                                    .iter()
                                    .filter_map(|expr| {
                                        probe_evaluator.evaluate(expr, &probe_record).ok()
                                    })
                                    .collect();
                                if key_values.len() != probe_keys.len()
                                    || key_values.iter().any(|v| matches!(v, Value::Null))
                                {
                                    return Vec::new();
                                }
                                let Some(matching_indices) = hash_table.get(&key_values) else {
                                    return Vec::new();
                                };
                                matching_indices
                                    .iter()
                                    .map(|&build_idx| {
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
                                        combined
                                    })
                                    .collect()
                            })
                            .collect()
                    };

                    let mut result = Table::empty(result_schema);
                    for batch in row_batches {
                        for row in batch {
                            result.push_row(row)?;
                        }
                    }
                    Ok(result)
                } else {
                    let mut result = Table::empty(result_schema);
                    let mut combined: Vec<Value> = Vec::with_capacity(left_width + right_width);
                    if let Some(ref indices) = probe_key_indices {
                        for probe_idx in 0..probe_n {
                            let key_values =
                                extract_key_values_direct(probe_cols, probe_idx, indices);
                            if key_values.iter().any(|v| matches!(v, Value::Null)) {
                                continue;
                            }
                            if let Some(matching_indices) = hash_table.get(&key_values) {
                                for &build_idx in matching_indices {
                                    combined.clear();
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
                                    result.push_row(std::mem::take(&mut combined))?;
                                    combined = Vec::with_capacity(left_width + right_width);
                                }
                            }
                        }
                        return Ok(result);
                    }
                    let probe_evaluator = ValueEvaluator::new(probe_schema)
                        .with_variables(&vars)
                        .with_system_variables(&sys_vars)
                        .with_user_functions(&udf);
                    let mut probe_record = Record::with_capacity(probe_cols.len());
                    let mut probe_values: Vec<Value> = Vec::with_capacity(probe_cols.len());
                    for probe_idx in 0..probe_n {
                        probe_values.clear();
                        probe_values.extend(probe_cols.iter().map(|c| c.get_value(probe_idx)));
                        probe_record.set_from_slice(&probe_values);
                        let key_values: Vec<Value> = probe_keys
                            .iter()
                            .map(|expr| probe_evaluator.evaluate(expr, &probe_record))
                            .collect::<Result<Vec<_>>>()?;

                        if key_values.iter().any(|v| matches!(v, Value::Null)) {
                            continue;
                        }

                        if let Some(matching_indices) = hash_table.get(&key_values) {
                            for &build_idx in matching_indices {
                                combined.clear();
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
                                result.push_row(std::mem::take(&mut combined))?;
                                combined = Vec::with_capacity(left_width + right_width);
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

                let mut hash_table: FxHashMap<Vec<Value>, Vec<usize>> =
                    FxHashMap::with_capacity_and_hasher(right_n, Default::default());
                let mut right_record = Record::with_capacity(right_cols.len());
                let mut right_values: Vec<Value> = Vec::with_capacity(right_cols.len());
                for right_idx in 0..right_n {
                    right_values.clear();
                    right_values.extend(right_cols.iter().map(|c| c.get_value(right_idx)));
                    right_record.set_from_slice(&right_values);
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
                let mut left_record = Record::with_capacity(left_cols.len());
                let mut left_values: Vec<Value> = Vec::with_capacity(left_cols.len());

                for left_idx in 0..left_n {
                    left_values.clear();
                    left_values.extend(left_cols.iter().map(|c| c.get_value(left_idx)));
                    left_record.set_from_slice(&left_values);
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
                                result.push_row(std::mem::take(&mut combined))?;
                            }
                        }
                        None => {
                            combined.clear();
                            combined.extend(left_cols.iter().map(|c| c.get_value(left_idx)));
                            combined.extend(std::iter::repeat_n(Value::Null, right_width));
                            result.push_row(std::mem::take(&mut combined))?;
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

                let mut hash_table: FxHashMap<Vec<Value>, Vec<usize>> =
                    FxHashMap::with_capacity_and_hasher(left_n, Default::default());
                let mut left_record = Record::with_capacity(left_cols.len());
                let mut left_values: Vec<Value> = Vec::with_capacity(left_cols.len());
                for left_idx in 0..left_n {
                    left_values.clear();
                    left_values.extend(left_cols.iter().map(|c| c.get_value(left_idx)));
                    left_record.set_from_slice(&left_values);
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
                let mut right_record = Record::with_capacity(right_cols.len());
                let mut right_values: Vec<Value> = Vec::with_capacity(right_cols.len());

                for right_idx in 0..right_n {
                    right_values.clear();
                    right_values.extend(right_cols.iter().map(|c| c.get_value(right_idx)));
                    right_record.set_from_slice(&right_values);
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
                                result.push_row(std::mem::take(&mut combined))?;
                            }
                        }
                        None => {
                            combined.clear();
                            combined.extend(std::iter::repeat_n(Value::Null, left_width));
                            combined.extend(right_cols.iter().map(|c| c.get_value(right_idx)));
                            result.push_row(std::mem::take(&mut combined))?;
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

                let mut hash_table: FxHashMap<Vec<Value>, Vec<usize>> =
                    FxHashMap::with_capacity_and_hasher(right_n, Default::default());
                let mut right_record = Record::with_capacity(right_cols.len());
                let mut right_values: Vec<Value> = Vec::with_capacity(right_cols.len());
                for right_idx in 0..right_n {
                    right_values.clear();
                    right_values.extend(right_cols.iter().map(|c| c.get_value(right_idx)));
                    right_record.set_from_slice(&right_values);
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

                let mut matched_right: FxHashSet<usize> =
                    FxHashSet::with_capacity_and_hasher(right_n, Default::default());
                let mut result = Table::empty(result_schema);
                let mut combined: Vec<Value> = Vec::with_capacity(left_width + right_width);
                let mut left_record = Record::with_capacity(left_cols.len());
                let mut left_values: Vec<Value> = Vec::with_capacity(left_cols.len());

                for left_idx in 0..left_n {
                    left_values.clear();
                    left_values.extend(left_cols.iter().map(|c| c.get_value(left_idx)));
                    left_record.set_from_slice(&left_values);
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
                                result.push_row(std::mem::take(&mut combined))?;
                            }
                        }
                        None => {
                            combined.clear();
                            combined.extend(left_cols.iter().map(|c| c.get_value(left_idx)));
                            combined.extend(std::iter::repeat_n(Value::Null, right_width));
                            result.push_row(std::mem::take(&mut combined))?;
                        }
                    }
                }

                for right_idx in 0..right_n {
                    if !matched_right.contains(&right_idx) {
                        combined.clear();
                        combined.extend(std::iter::repeat_n(Value::Null, left_width));
                        combined.extend(right_cols.iter().map(|c| c.get_value(right_idx)));
                        result.push_row(std::mem::take(&mut combined))?;
                    }
                }

                Ok(result)
            }
            JoinType::Cross => Err(Error::internal(
                "Cross join should not be handled by HashJoin",
            )),
            JoinType::LeftSemi => {
                let right_evaluator = ValueEvaluator::new(&right_schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);

                let mut hash_table: FxHashMap<Vec<Value>, Vec<usize>> =
                    FxHashMap::with_capacity_and_hasher(right_n, Default::default());
                let mut right_record = Record::with_capacity(right_cols.len());
                let mut right_values: Vec<Value> = Vec::with_capacity(right_cols.len());
                for right_idx in 0..right_n {
                    right_values.clear();
                    right_values.extend(right_cols.iter().map(|c| c.get_value(right_idx)));
                    right_record.set_from_slice(&right_values);
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
                let mut left_record = Record::with_capacity(left_cols.len());
                let mut left_values: Vec<Value> = Vec::with_capacity(left_cols.len());

                for left_idx in 0..left_n {
                    left_values.clear();
                    left_values.extend(left_cols.iter().map(|c| c.get_value(left_idx)));
                    left_record.set_from_slice(&left_values);
                    let key_values: Vec<Value> = left_keys
                        .iter()
                        .map(|expr| left_evaluator.evaluate(expr, &left_record))
                        .collect::<Result<Vec<_>>>()?;

                    let has_null_key = key_values.iter().any(|v| matches!(v, Value::Null));
                    if has_null_key {
                        continue;
                    }

                    if hash_table.contains_key(&key_values) {
                        let left_row: Vec<Value> =
                            left_cols.iter().map(|c| c.get_value(left_idx)).collect();
                        result.push_row(left_row)?;
                    }
                }
                Ok(result)
            }
            JoinType::LeftAnti => {
                let right_evaluator = ValueEvaluator::new(&right_schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);

                let mut hash_table: FxHashMap<Vec<Value>, Vec<usize>> =
                    FxHashMap::with_capacity_and_hasher(right_n, Default::default());
                let mut right_record = Record::with_capacity(right_cols.len());
                let mut right_values: Vec<Value> = Vec::with_capacity(right_cols.len());
                for right_idx in 0..right_n {
                    right_values.clear();
                    right_values.extend(right_cols.iter().map(|c| c.get_value(right_idx)));
                    right_record.set_from_slice(&right_values);
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
                let mut left_record = Record::with_capacity(left_cols.len());
                let mut left_values: Vec<Value> = Vec::with_capacity(left_cols.len());

                for left_idx in 0..left_n {
                    left_values.clear();
                    left_values.extend(left_cols.iter().map(|c| c.get_value(left_idx)));
                    left_record.set_from_slice(&left_values);
                    let key_values: Vec<Value> = left_keys
                        .iter()
                        .map(|expr| left_evaluator.evaluate(expr, &left_record))
                        .collect::<Result<Vec<_>>>()?;

                    let has_null_key = key_values.iter().any(|v| matches!(v, Value::Null));
                    let has_match = !has_null_key && hash_table.contains_key(&key_values);

                    if !has_match {
                        let left_row: Vec<Value> =
                            left_cols.iter().map(|c| c.get_value(left_idx)).collect();
                        result.push_row(left_row)?;
                    }
                }
                Ok(result)
            }
            JoinType::RightSemi => {
                let left_evaluator = ValueEvaluator::new(&left_schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);

                let mut hash_table: FxHashMap<Vec<Value>, Vec<usize>> =
                    FxHashMap::with_capacity_and_hasher(left_n, Default::default());
                let mut left_record = Record::with_capacity(left_cols.len());
                let mut left_values: Vec<Value> = Vec::with_capacity(left_cols.len());
                for left_idx in 0..left_n {
                    left_values.clear();
                    left_values.extend(left_cols.iter().map(|c| c.get_value(left_idx)));
                    left_record.set_from_slice(&left_values);
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
                let mut right_record = Record::with_capacity(right_cols.len());
                let mut right_values: Vec<Value> = Vec::with_capacity(right_cols.len());

                for right_idx in 0..right_n {
                    right_values.clear();
                    right_values.extend(right_cols.iter().map(|c| c.get_value(right_idx)));
                    right_record.set_from_slice(&right_values);
                    let key_values: Vec<Value> = right_keys
                        .iter()
                        .map(|expr| right_evaluator.evaluate(expr, &right_record))
                        .collect::<Result<Vec<_>>>()?;

                    let has_null_key = key_values.iter().any(|v| matches!(v, Value::Null));
                    if has_null_key {
                        continue;
                    }

                    if hash_table.contains_key(&key_values) {
                        let right_row: Vec<Value> =
                            right_cols.iter().map(|c| c.get_value(right_idx)).collect();
                        result.push_row(right_row)?;
                    }
                }
                Ok(result)
            }
            JoinType::RightAnti => {
                let left_evaluator = ValueEvaluator::new(&left_schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);

                let mut hash_table: FxHashMap<Vec<Value>, Vec<usize>> =
                    FxHashMap::with_capacity_and_hasher(left_n, Default::default());
                let mut left_record = Record::with_capacity(left_cols.len());
                let mut left_values: Vec<Value> = Vec::with_capacity(left_cols.len());
                for left_idx in 0..left_n {
                    left_values.clear();
                    left_values.extend(left_cols.iter().map(|c| c.get_value(left_idx)));
                    left_record.set_from_slice(&left_values);
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
                let mut right_record = Record::with_capacity(right_cols.len());
                let mut right_values: Vec<Value> = Vec::with_capacity(right_cols.len());

                for right_idx in 0..right_n {
                    right_values.clear();
                    right_values.extend(right_cols.iter().map(|c| c.get_value(right_idx)));
                    right_record.set_from_slice(&right_values);
                    let key_values: Vec<Value> = right_keys
                        .iter()
                        .map(|expr| right_evaluator.evaluate(expr, &right_record))
                        .collect::<Result<Vec<_>>>()?;

                    let has_null_key = key_values.iter().any(|v| matches!(v, Value::Null));
                    let has_match = !has_null_key && hash_table.contains_key(&key_values);

                    if !has_match {
                        let right_row: Vec<Value> =
                            right_cols.iter().map(|c| c.get_value(right_idx)).collect();
                        result.push_row(right_row)?;
                    }
                }
                Ok(result)
            }
        }
    }
}
