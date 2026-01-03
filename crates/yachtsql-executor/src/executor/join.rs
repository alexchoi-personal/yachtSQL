#![coverage(off)]

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, JoinType, PlanSchema};
use yachtsql_storage::{Column, Record, Schema, Table};

use super::{PlanExecutor, plan_schema_to_schema};
use crate::columnar_evaluator::ColumnarEvaluator;
use crate::plan::PhysicalPlan;
use crate::value_evaluator::ValueEvaluator;

#[derive(Clone, PartialEq)]
struct HashKey(Vec<Value>);

impl Eq for HashKey {}

impl Hash for HashKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for val in &self.0 {
            match val {
                Value::Null => 0u8.hash(state),
                Value::Bool(b) => {
                    1u8.hash(state);
                    b.hash(state);
                }
                Value::Int64(i) => {
                    2u8.hash(state);
                    i.hash(state);
                }
                Value::Float64(f) => {
                    3u8.hash(state);
                    f.to_bits().hash(state);
                }
                Value::String(s) => {
                    4u8.hash(state);
                    s.hash(state);
                }
                Value::Bytes(b) => {
                    5u8.hash(state);
                    b.hash(state);
                }
                Value::Date(d) => {
                    6u8.hash(state);
                    d.hash(state);
                }
                Value::DateTime(dt) => {
                    7u8.hash(state);
                    dt.hash(state);
                }
                Value::Time(t) => {
                    8u8.hash(state);
                    t.hash(state);
                }
                Value::Timestamp(ts) => {
                    9u8.hash(state);
                    ts.hash(state);
                }
                Value::Interval(interval) => {
                    10u8.hash(state);
                    interval.months.hash(state);
                    interval.days.hash(state);
                    interval.nanos.hash(state);
                }
                Value::Numeric(n) => {
                    11u8.hash(state);
                    n.mantissa().hash(state);
                    n.scale().hash(state);
                }
                Value::Array(arr) => {
                    12u8.hash(state);
                    arr.len().hash(state);
                }
                Value::Struct(fields) => {
                    13u8.hash(state);
                    fields.len().hash(state);
                }
                Value::Json(j) => {
                    14u8.hash(state);
                    hash_json_value(j, state);
                }
                Value::Geography(_) => {
                    15u8.hash(state);
                }
                Value::Range { .. } => {
                    16u8.hash(state);
                }
                Value::BigNumeric(n) => {
                    17u8.hash(state);
                    n.mantissa().hash(state);
                    n.scale().hash(state);
                }
                Value::Default => {
                    18u8.hash(state);
                }
            }
        }
    }
}

fn hash_json_value<H: Hasher>(json: &serde_json::Value, state: &mut H) {
    match json {
        serde_json::Value::Null => 0u8.hash(state),
        serde_json::Value::Bool(b) => {
            1u8.hash(state);
            b.hash(state);
        }
        serde_json::Value::Number(n) => {
            2u8.hash(state);
            if let Some(i) = n.as_i64() {
                i.hash(state);
            } else if let Some(u) = n.as_u64() {
                u.hash(state);
            } else if let Some(f) = n.as_f64() {
                f.to_bits().hash(state);
            }
        }
        serde_json::Value::String(s) => {
            3u8.hash(state);
            s.hash(state);
        }
        serde_json::Value::Array(arr) => {
            4u8.hash(state);
            arr.len().hash(state);
            for item in arr {
                hash_json_value(item, state);
            }
        }
        serde_json::Value::Object(obj) => {
            5u8.hash(state);
            obj.len().hash(state);
            for (k, v) in obj {
                k.hash(state);
                hash_json_value(v, state);
            }
        }
    }
}

impl<'a> PlanExecutor<'a> {
    pub(crate) fn execute_nested_loop_join(
        &mut self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        join_type: &JoinType,
        condition: Option<&Expr>,
        schema: &PlanSchema,
    ) -> Result<Table> {
        let left_table = self.execute_plan(left)?;
        let right_table = self.execute_plan(right)?;

        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema.clone());

        let combined_schema = combine_schemas(left_table.schema(), right_table.schema());

        match join_type {
            JoinType::Inner => {
                self.inner_join(
                    &left_table,
                    &right_table,
                    condition,
                    &combined_schema,
                    &mut result,
                )?;
            }
            JoinType::Left => {
                self.left_join(
                    &left_table,
                    &right_table,
                    condition,
                    &combined_schema,
                    &mut result,
                )?;
            }
            JoinType::Right => {
                self.right_join(
                    &left_table,
                    &right_table,
                    condition,
                    &combined_schema,
                    &mut result,
                )?;
            }
            JoinType::Full => {
                self.full_join(
                    &left_table,
                    &right_table,
                    condition,
                    &combined_schema,
                    &mut result,
                )?;
            }
            JoinType::Cross => {
                self.cross_join_inner(&left_table, &right_table, &mut result)?;
            }
        }

        Ok(result)
    }

    pub(crate) fn execute_cross_join(
        &mut self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        schema: &PlanSchema,
    ) -> Result<Table> {
        let left_table = self.execute_plan(left)?;
        let right_table = self.execute_plan(right)?;

        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);

        self.cross_join_inner(&left_table, &right_table, &mut result)?;

        Ok(result)
    }

    pub(crate) fn execute_hash_join(
        &mut self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        join_type: &JoinType,
        left_keys: &[Expr],
        right_keys: &[Expr],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let left_table = self.execute_plan(left)?;
        let right_table = self.execute_plan(right)?;

        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);

        match join_type {
            JoinType::Inner => {
                self.hash_inner_join(
                    &left_table,
                    &right_table,
                    left_keys,
                    right_keys,
                    &mut result,
                )?;
            }
            _ => {
                return Err(Error::unsupported("Only INNER JOIN supported in HashJoin"));
            }
        }

        Ok(result)
    }

    fn hash_inner_join(
        &self,
        left: &Table,
        right: &Table,
        left_keys: &[Expr],
        right_keys: &[Expr],
        result: &mut Table,
    ) -> Result<()> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        let left_evaluator = ColumnarEvaluator::new(left_schema)
            .with_variables(&self.variables)
            .with_system_variables(self.session.system_variables())
            .with_user_functions(&self.user_function_defs);
        let right_evaluator = ColumnarEvaluator::new(right_schema)
            .with_variables(&self.variables)
            .with_system_variables(self.session.system_variables())
            .with_user_functions(&self.user_function_defs);

        let right_n = right.row_count();
        let right_columns: Vec<&Column> = right.columns().iter().map(|(_, c)| c.as_ref()).collect();
        let right_key_cols: Vec<Column> = right_keys
            .iter()
            .map(|expr| right_evaluator.evaluate(expr, right))
            .collect::<Result<_>>()?;

        let mut hash_table: HashMap<HashKey, Vec<usize>> = HashMap::with_capacity(right_n);
        for i in 0..right_n {
            let key_values: Vec<Value> = right_key_cols.iter().map(|c| c.get_value(i)).collect();
            let has_null = key_values.iter().any(|v| matches!(v, Value::Null));
            if has_null {
                continue;
            }
            let key = HashKey(key_values);
            hash_table.entry(key).or_default().push(i);
        }

        let left_n = left.row_count();
        let left_columns: Vec<&Column> = left.columns().iter().map(|(_, c)| c.as_ref()).collect();
        let left_key_cols: Vec<Column> = left_keys
            .iter()
            .map(|expr| left_evaluator.evaluate(expr, left))
            .collect::<Result<_>>()?;

        let left_width = left_columns.len();
        let right_width = right_columns.len();
        let mut combined_values: Vec<Value> = Vec::with_capacity(left_width + right_width);

        for left_idx in 0..left_n {
            let key_values: Vec<Value> = left_key_cols
                .iter()
                .map(|c| c.get_value(left_idx))
                .collect();
            let has_null = key_values.iter().any(|v| matches!(v, Value::Null));
            if has_null {
                continue;
            }
            let key = HashKey(key_values);

            if let Some(matching_indices) = hash_table.get(&key) {
                for &right_idx in matching_indices {
                    combined_values.clear();
                    combined_values.extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                    combined_values.extend(right_columns.iter().map(|c| c.get_value(right_idx)));
                    result.push_row(combined_values.clone())?;
                }
            }
        }

        Ok(())
    }

    fn inner_join(
        &self,
        left: &Table,
        right: &Table,
        condition: Option<&Expr>,
        combined_schema: &Schema,
        result: &mut Table,
    ) -> Result<()> {
        let evaluator = ValueEvaluator::new(combined_schema);
        let left_n = left.row_count();
        let right_n = right.row_count();
        let left_columns: Vec<&Column> = left.columns().iter().map(|(_, c)| c.as_ref()).collect();
        let right_columns: Vec<&Column> = right.columns().iter().map(|(_, c)| c.as_ref()).collect();
        let left_width = left_columns.len();
        let right_width = right_columns.len();

        let mut combined_values: Vec<Value> = Vec::with_capacity(left_width + right_width);

        for left_idx in 0..left_n {
            for right_idx in 0..right_n {
                combined_values.clear();
                combined_values.extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                combined_values.extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                let combined_record = Record::from_values(combined_values.clone());

                let matches = match condition {
                    Some(expr) => evaluator
                        .evaluate(expr, &combined_record)?
                        .as_bool()
                        .unwrap_or(false),
                    None => true,
                };

                if matches {
                    result.push_row(combined_values.clone())?;
                }
            }
        }

        Ok(())
    }

    fn left_join(
        &self,
        left: &Table,
        right: &Table,
        condition: Option<&Expr>,
        combined_schema: &Schema,
        result: &mut Table,
    ) -> Result<()> {
        let evaluator = ValueEvaluator::new(combined_schema);
        let left_n = left.row_count();
        let right_n = right.row_count();
        let left_columns: Vec<&Column> = left.columns().iter().map(|(_, c)| c.as_ref()).collect();
        let right_columns: Vec<&Column> = right.columns().iter().map(|(_, c)| c.as_ref()).collect();
        let left_width = left_columns.len();
        let right_width = right_columns.len();
        let right_null_row: Vec<Value> = (0..right.schema().field_count())
            .map(|_| Value::Null)
            .collect();

        let mut combined_values: Vec<Value> = Vec::with_capacity(left_width + right_width);

        for left_idx in 0..left_n {
            let mut had_match = false;

            for right_idx in 0..right_n {
                combined_values.clear();
                combined_values.extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                combined_values.extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                let combined_record = Record::from_values(combined_values.clone());

                let matches = match condition {
                    Some(expr) => evaluator
                        .evaluate(expr, &combined_record)?
                        .as_bool()
                        .unwrap_or(false),
                    None => true,
                };

                if matches {
                    had_match = true;
                    result.push_row(combined_values.clone())?;
                }
            }

            if !had_match {
                combined_values.clear();
                combined_values.extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                combined_values.extend(right_null_row.iter().cloned());
                result.push_row(combined_values.clone())?;
            }
        }

        Ok(())
    }

    fn right_join(
        &self,
        left: &Table,
        right: &Table,
        condition: Option<&Expr>,
        combined_schema: &Schema,
        result: &mut Table,
    ) -> Result<()> {
        let evaluator = ValueEvaluator::new(combined_schema);
        let left_n = left.row_count();
        let right_n = right.row_count();
        let left_columns: Vec<&Column> = left.columns().iter().map(|(_, c)| c.as_ref()).collect();
        let right_columns: Vec<&Column> = right.columns().iter().map(|(_, c)| c.as_ref()).collect();
        let left_width = left_columns.len();
        let right_width = right_columns.len();
        let left_null_row: Vec<Value> = (0..left.schema().field_count())
            .map(|_| Value::Null)
            .collect();

        let mut combined_values: Vec<Value> = Vec::with_capacity(left_width + right_width);

        for right_idx in 0..right_n {
            let mut had_match = false;

            for left_idx in 0..left_n {
                combined_values.clear();
                combined_values.extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                combined_values.extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                let combined_record = Record::from_values(combined_values.clone());

                let matches = match condition {
                    Some(expr) => evaluator
                        .evaluate(expr, &combined_record)?
                        .as_bool()
                        .unwrap_or(false),
                    None => true,
                };

                if matches {
                    had_match = true;
                    result.push_row(combined_values.clone())?;
                }
            }

            if !had_match {
                combined_values.clear();
                combined_values.extend(left_null_row.iter().cloned());
                combined_values.extend(right_columns.iter().map(|c| c.get_value(right_idx)));
                result.push_row(combined_values.clone())?;
            }
        }

        Ok(())
    }

    fn full_join(
        &self,
        left: &Table,
        right: &Table,
        condition: Option<&Expr>,
        combined_schema: &Schema,
        result: &mut Table,
    ) -> Result<()> {
        let evaluator = ValueEvaluator::new(combined_schema);
        let left_n = left.row_count();
        let right_n = right.row_count();
        let left_columns: Vec<&Column> = left.columns().iter().map(|(_, c)| c.as_ref()).collect();
        let right_columns: Vec<&Column> = right.columns().iter().map(|(_, c)| c.as_ref()).collect();
        let left_width = left_columns.len();
        let right_width = right_columns.len();
        let left_null_row: Vec<Value> = (0..left.schema().field_count())
            .map(|_| Value::Null)
            .collect();
        let right_null_row: Vec<Value> = (0..right.schema().field_count())
            .map(|_| Value::Null)
            .collect();

        let mut right_matched: Vec<bool> = vec![false; right_n];
        let mut combined_values: Vec<Value> = Vec::with_capacity(left_width + right_width);

        for left_idx in 0..left_n {
            let mut had_match = false;

            for (right_idx, matched) in right_matched.iter_mut().enumerate() {
                combined_values.clear();
                combined_values.extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                combined_values.extend(right_columns.iter().map(|c| c.get_value(right_idx)));

                let combined_record = Record::from_values(combined_values.clone());

                let match_result = match condition {
                    Some(expr) => evaluator
                        .evaluate(expr, &combined_record)?
                        .as_bool()
                        .unwrap_or(false),
                    None => true,
                };

                if match_result {
                    had_match = true;
                    *matched = true;
                    result.push_row(combined_values.clone())?;
                }
            }

            if !had_match {
                combined_values.clear();
                combined_values.extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                combined_values.extend(right_null_row.iter().cloned());
                result.push_row(combined_values.clone())?;
            }
        }

        for (right_idx, &matched) in right_matched.iter().enumerate() {
            if !matched {
                combined_values.clear();
                combined_values.extend(left_null_row.iter().cloned());
                combined_values.extend(right_columns.iter().map(|c| c.get_value(right_idx)));
                result.push_row(combined_values.clone())?;
            }
        }

        Ok(())
    }

    fn cross_join_inner(&self, left: &Table, right: &Table, result: &mut Table) -> Result<()> {
        let left_n = left.row_count();
        let right_n = right.row_count();
        let left_columns: Vec<&Column> = left.columns().iter().map(|(_, c)| c.as_ref()).collect();
        let right_columns: Vec<&Column> = right.columns().iter().map(|(_, c)| c.as_ref()).collect();
        let left_width = left_columns.len();
        let right_width = right_columns.len();

        let mut combined_values: Vec<Value> = Vec::with_capacity(left_width + right_width);

        for left_idx in 0..left_n {
            for right_idx in 0..right_n {
                combined_values.clear();
                combined_values.extend(left_columns.iter().map(|c| c.get_value(left_idx)));
                combined_values.extend(right_columns.iter().map(|c| c.get_value(right_idx)));
                result.push_row(combined_values.clone())?;
            }
        }
        Ok(())
    }
}

fn combine_schemas(left: &Schema, right: &Schema) -> Schema {
    let mut schema = Schema::new();
    for field in left.fields() {
        schema.add_field(field.clone());
    }
    for field in right.fields() {
        schema.add_field(field.clone());
    }
    schema
}
