#![coverage(off)]

use rustc_hash::FxHashSet;
use yachtsql_ir::Expr;

use super::predicate::collect_column_indices_into;
use crate::optimized_logical_plan::OptimizedLogicalPlan;

#[derive(Clone, Default)]
pub struct RequiredColumns {
    indices: FxHashSet<usize>,
}

impl RequiredColumns {
    pub fn new() -> Self {
        Self {
            indices: FxHashSet::default(),
        }
    }

    pub fn all(schema_len: usize) -> Self {
        Self {
            indices: (0..schema_len).collect(),
        }
    }

    pub fn add(&mut self, index: usize) {
        self.indices.insert(index);
    }

    pub fn contains(&self, idx: usize) -> bool {
        self.indices.contains(&idx)
    }

    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.indices.iter().copied()
    }
}

pub struct ProjectionPushdown;

impl ProjectionPushdown {
    pub fn optimize(plan: OptimizedLogicalPlan) -> OptimizedLogicalPlan {
        let schema_len = plan.schema().fields.len();
        let required = RequiredColumns::all(schema_len);
        Self::push_required(plan, required)
    }

    fn extract_required_columns(expr: &Expr, required: &mut RequiredColumns) {
        collect_column_indices_into(expr, &mut required.indices);
    }

    fn push_required(
        plan: OptimizedLogicalPlan,
        required: RequiredColumns,
    ) -> OptimizedLogicalPlan {
        match plan {
            OptimizedLogicalPlan::TableScan {
                table_name,
                schema,
                projection,
            } => {
                let new_projection = if required.indices.len() < schema.fields.len() {
                    let mut cols: Vec<_> = required.iter().collect();
                    cols.sort_unstable();
                    Some(cols)
                } else {
                    projection
                };
                OptimizedLogicalPlan::TableScan {
                    table_name,
                    schema,
                    projection: new_projection,
                }
            }

            OptimizedLogicalPlan::Filter { input, predicate } => {
                let mut input_required = required;
                Self::extract_required_columns(&predicate, &mut input_required);
                let optimized_input = Self::push_required(*input, input_required);
                OptimizedLogicalPlan::Filter {
                    input: Box::new(optimized_input),
                    predicate,
                }
            }

            OptimizedLogicalPlan::Project {
                input,
                expressions,
                schema,
            } => {
                let mut input_required = RequiredColumns::new();
                for (idx, expr) in expressions.iter().enumerate() {
                    if required.contains(idx) {
                        Self::extract_required_columns(expr, &mut input_required);
                    }
                }
                let optimized_input = Self::push_required(*input, input_required);
                OptimizedLogicalPlan::Project {
                    input: Box::new(optimized_input),
                    expressions,
                    schema,
                }
            }

            OptimizedLogicalPlan::HashJoin {
                left,
                right,
                join_type,
                left_keys,
                right_keys,
                schema,
            } => {
                let left_schema_len = left.schema().fields.len();

                let mut left_required = RequiredColumns::new();
                let mut right_required = RequiredColumns::new();

                for idx in required.iter() {
                    if idx < left_schema_len {
                        left_required.add(idx);
                    } else {
                        right_required.add(idx - left_schema_len);
                    }
                }

                for key in &left_keys {
                    Self::extract_required_columns(key, &mut left_required);
                }
                for key in &right_keys {
                    Self::extract_required_columns(key, &mut right_required);
                }

                let optimized_left = Self::push_required(*left, left_required);
                let optimized_right = Self::push_required(*right, right_required);

                OptimizedLogicalPlan::HashJoin {
                    left: Box::new(optimized_left),
                    right: Box::new(optimized_right),
                    join_type,
                    left_keys,
                    right_keys,
                    schema,
                }
            }

            OptimizedLogicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
                schema,
            } => {
                let left_schema_len = left.schema().fields.len();

                let mut left_required = RequiredColumns::new();
                let mut right_required = RequiredColumns::new();

                for idx in required.iter() {
                    if idx < left_schema_len {
                        left_required.add(idx);
                    } else {
                        right_required.add(idx - left_schema_len);
                    }
                }

                if let Some(cond) = &condition {
                    let mut cond_required = RequiredColumns::new();
                    Self::extract_required_columns(cond, &mut cond_required);
                    for idx in cond_required.iter() {
                        if idx < left_schema_len {
                            left_required.add(idx);
                        } else {
                            right_required.add(idx - left_schema_len);
                        }
                    }
                }

                let optimized_left = Self::push_required(*left, left_required);
                let optimized_right = Self::push_required(*right, right_required);

                OptimizedLogicalPlan::NestedLoopJoin {
                    left: Box::new(optimized_left),
                    right: Box::new(optimized_right),
                    join_type,
                    condition,
                    schema,
                }
            }

            OptimizedLogicalPlan::CrossJoin {
                left,
                right,
                schema,
            } => {
                let left_schema_len = left.schema().fields.len();

                let mut left_required = RequiredColumns::new();
                let mut right_required = RequiredColumns::new();

                for idx in required.iter() {
                    if idx < left_schema_len {
                        left_required.add(idx);
                    } else {
                        right_required.add(idx - left_schema_len);
                    }
                }

                let optimized_left = Self::push_required(*left, left_required);
                let optimized_right = Self::push_required(*right, right_required);

                OptimizedLogicalPlan::CrossJoin {
                    left: Box::new(optimized_left),
                    right: Box::new(optimized_right),
                    schema,
                }
            }

            OptimizedLogicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
                schema,
                grouping_sets,
            } => {
                let mut input_required = RequiredColumns::new();
                for expr in &group_by {
                    Self::extract_required_columns(expr, &mut input_required);
                }
                for expr in &aggregates {
                    Self::extract_required_columns(expr, &mut input_required);
                }
                let optimized_input = Self::push_required(*input, input_required);
                OptimizedLogicalPlan::HashAggregate {
                    input: Box::new(optimized_input),
                    group_by,
                    aggregates,
                    schema,
                    grouping_sets,
                }
            }

            OptimizedLogicalPlan::Sort { input, sort_exprs } => {
                let mut input_required = required;
                for sort_expr in &sort_exprs {
                    Self::extract_required_columns(&sort_expr.expr, &mut input_required);
                }
                let optimized_input = Self::push_required(*input, input_required);
                OptimizedLogicalPlan::Sort {
                    input: Box::new(optimized_input),
                    sort_exprs,
                }
            }

            OptimizedLogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let optimized_input = Self::push_required(*input, required);
                OptimizedLogicalPlan::Limit {
                    input: Box::new(optimized_input),
                    limit,
                    offset,
                }
            }

            OptimizedLogicalPlan::TopN {
                input,
                sort_exprs,
                limit,
            } => {
                let mut input_required = required;
                for sort_expr in &sort_exprs {
                    Self::extract_required_columns(&sort_expr.expr, &mut input_required);
                }
                let optimized_input = Self::push_required(*input, input_required);
                OptimizedLogicalPlan::TopN {
                    input: Box::new(optimized_input),
                    sort_exprs,
                    limit,
                }
            }

            OptimizedLogicalPlan::Distinct { input } => {
                let optimized_input = Self::push_required(*input, required);
                OptimizedLogicalPlan::Distinct {
                    input: Box::new(optimized_input),
                }
            }

            OptimizedLogicalPlan::Union {
                inputs,
                all,
                schema,
            } => {
                let optimized_inputs = inputs
                    .into_iter()
                    .map(|inp| Self::push_required(inp, required.clone()))
                    .collect();
                OptimizedLogicalPlan::Union {
                    inputs: optimized_inputs,
                    all,
                    schema,
                }
            }

            OptimizedLogicalPlan::Intersect {
                left,
                right,
                all,
                schema,
            } => {
                let optimized_left = Self::push_required(*left, required.clone());
                let optimized_right = Self::push_required(*right, required);
                OptimizedLogicalPlan::Intersect {
                    left: Box::new(optimized_left),
                    right: Box::new(optimized_right),
                    all,
                    schema,
                }
            }

            OptimizedLogicalPlan::Except {
                left,
                right,
                all,
                schema,
            } => {
                let optimized_left = Self::push_required(*left, required.clone());
                let optimized_right = Self::push_required(*right, required);
                OptimizedLogicalPlan::Except {
                    left: Box::new(optimized_left),
                    right: Box::new(optimized_right),
                    all,
                    schema,
                }
            }

            OptimizedLogicalPlan::Window {
                input,
                window_exprs,
                schema,
            } => {
                let input_schema_len = input.schema().fields.len();
                let mut input_required = RequiredColumns::new();

                for idx in required.iter() {
                    if idx < input_schema_len {
                        input_required.add(idx);
                    }
                }

                for expr in &window_exprs {
                    Self::extract_required_columns(expr, &mut input_required);
                }

                let optimized_input = Self::push_required(*input, input_required);
                OptimizedLogicalPlan::Window {
                    input: Box::new(optimized_input),
                    window_exprs,
                    schema,
                }
            }

            OptimizedLogicalPlan::Unnest {
                input,
                columns,
                schema,
            } => {
                let mut input_required = required;
                for col in &columns {
                    Self::extract_required_columns(&col.expr, &mut input_required);
                }
                let optimized_input = Self::push_required(*input, input_required);
                OptimizedLogicalPlan::Unnest {
                    input: Box::new(optimized_input),
                    columns,
                    schema,
                }
            }

            OptimizedLogicalPlan::Qualify { input, predicate } => {
                let mut input_required = required;
                Self::extract_required_columns(&predicate, &mut input_required);
                let optimized_input = Self::push_required(*input, input_required);
                OptimizedLogicalPlan::Qualify {
                    input: Box::new(optimized_input),
                    predicate,
                }
            }

            OptimizedLogicalPlan::Sample {
                input,
                sample_type,
                sample_value,
            } => {
                let optimized_input = Self::push_required(*input, required);
                OptimizedLogicalPlan::Sample {
                    input: Box::new(optimized_input),
                    sample_type,
                    sample_value,
                }
            }

            OptimizedLogicalPlan::GapFill {
                input,
                ts_column,
                bucket_width,
                value_columns,
                partitioning_columns,
                origin,
                input_schema,
                schema,
            } => {
                let input_schema_len = input.schema().fields.len();
                let input_required = RequiredColumns::all(input_schema_len);
                let optimized_input = Self::push_required(*input, input_required);
                OptimizedLogicalPlan::GapFill {
                    input: Box::new(optimized_input),
                    ts_column,
                    bucket_width,
                    value_columns,
                    partitioning_columns,
                    origin,
                    input_schema,
                    schema,
                }
            }

            OptimizedLogicalPlan::WithCte { ctes, body } => {
                let optimized_body = Self::push_required(*body, required);
                OptimizedLogicalPlan::WithCte {
                    ctes,
                    body: Box::new(optimized_body),
                }
            }

            OptimizedLogicalPlan::Explain {
                input,
                analyze,
                logical_plan_text,
            } => {
                let input_schema_len = input.schema().fields.len();
                let input_required = RequiredColumns::all(input_schema_len);
                let optimized_input = Self::push_required(*input, input_required);
                OptimizedLogicalPlan::Explain {
                    input: Box::new(optimized_input),
                    analyze,
                    logical_plan_text,
                }
            }

            other => other,
        }
    }
}
