#![coverage(off)]

use rustc_hash::FxHashSet;
use yachtsql_ir::Expr;

use crate::PhysicalPlan;
use crate::planner::predicate::collect_column_indices_into;

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
    pub fn optimize(plan: PhysicalPlan) -> PhysicalPlan {
        let schema_len = plan.schema().fields.len();
        let required = RequiredColumns::all(schema_len);
        Self::push_required(plan, required)
    }

    fn extract_required_columns(expr: &Expr, required: &mut RequiredColumns) {
        collect_column_indices_into(expr, &mut required.indices);
    }

    fn push_required(plan: PhysicalPlan, required: RequiredColumns) -> PhysicalPlan {
        match plan {
            PhysicalPlan::TableScan {
                table_name,
                schema,
                projection,
                row_count,
            } => {
                let new_projection = if required.indices.len() < schema.fields.len() {
                    let mut cols: Vec<_> = required.iter().collect();
                    cols.sort_unstable();
                    Some(cols)
                } else {
                    projection
                };
                PhysicalPlan::TableScan {
                    table_name,
                    schema,
                    projection: new_projection,
                    row_count,
                }
            }

            PhysicalPlan::Filter { input, predicate } => {
                let mut input_required = required;
                Self::extract_required_columns(&predicate, &mut input_required);
                let optimized_input = Self::push_required(*input, input_required);
                PhysicalPlan::Filter {
                    input: Box::new(optimized_input),
                    predicate,
                }
            }

            PhysicalPlan::Project {
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
                PhysicalPlan::Project {
                    input: Box::new(optimized_input),
                    expressions,
                    schema,
                }
            }

            PhysicalPlan::HashJoin {
                left,
                right,
                join_type,
                left_keys,
                right_keys,
                schema,
                parallel,
                hints,
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

                PhysicalPlan::HashJoin {
                    left: Box::new(optimized_left),
                    right: Box::new(optimized_right),
                    join_type,
                    left_keys,
                    right_keys,
                    schema,
                    parallel,
                    hints,
                }
            }

            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
                schema,
                parallel,
                hints,
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

                PhysicalPlan::NestedLoopJoin {
                    left: Box::new(optimized_left),
                    right: Box::new(optimized_right),
                    join_type,
                    condition,
                    schema,
                    parallel,
                    hints,
                }
            }

            PhysicalPlan::CrossJoin {
                left,
                right,
                schema,
                parallel,
                hints,
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

                PhysicalPlan::CrossJoin {
                    left: Box::new(optimized_left),
                    right: Box::new(optimized_right),
                    schema,
                    parallel,
                    hints,
                }
            }

            PhysicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
                schema,
                grouping_sets,
                hints,
            } => {
                let mut input_required = RequiredColumns::new();
                for expr in &group_by {
                    Self::extract_required_columns(expr, &mut input_required);
                }
                for expr in &aggregates {
                    Self::extract_required_columns(expr, &mut input_required);
                }
                let optimized_input = Self::push_required(*input, input_required);
                PhysicalPlan::HashAggregate {
                    input: Box::new(optimized_input),
                    group_by,
                    aggregates,
                    schema,
                    grouping_sets,
                    hints,
                }
            }

            PhysicalPlan::Sort {
                input,
                sort_exprs,
                hints,
            } => {
                let mut input_required = required;
                for sort_expr in &sort_exprs {
                    Self::extract_required_columns(&sort_expr.expr, &mut input_required);
                }
                let optimized_input = Self::push_required(*input, input_required);
                PhysicalPlan::Sort {
                    input: Box::new(optimized_input),
                    sort_exprs,
                    hints,
                }
            }

            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let optimized_input = Self::push_required(*input, required);
                PhysicalPlan::Limit {
                    input: Box::new(optimized_input),
                    limit,
                    offset,
                }
            }

            PhysicalPlan::TopN {
                input,
                sort_exprs,
                limit,
            } => {
                let mut input_required = required;
                for sort_expr in &sort_exprs {
                    Self::extract_required_columns(&sort_expr.expr, &mut input_required);
                }
                let optimized_input = Self::push_required(*input, input_required);
                PhysicalPlan::TopN {
                    input: Box::new(optimized_input),
                    sort_exprs,
                    limit,
                }
            }

            PhysicalPlan::Distinct { input } => {
                let optimized_input = Self::push_required(*input, required);
                PhysicalPlan::Distinct {
                    input: Box::new(optimized_input),
                }
            }

            PhysicalPlan::Union {
                inputs,
                all,
                schema,
                parallel,
                hints,
            } => {
                let optimized_inputs = inputs
                    .into_iter()
                    .map(|inp| Self::push_required(inp, required.clone()))
                    .collect();
                PhysicalPlan::Union {
                    inputs: optimized_inputs,
                    all,
                    schema,
                    parallel,
                    hints,
                }
            }

            PhysicalPlan::Intersect {
                left,
                right,
                all,
                schema,
                parallel,
                hints,
            } => {
                let optimized_left = Self::push_required(*left, required.clone());
                let optimized_right = Self::push_required(*right, required);
                PhysicalPlan::Intersect {
                    left: Box::new(optimized_left),
                    right: Box::new(optimized_right),
                    all,
                    schema,
                    parallel,
                    hints,
                }
            }

            PhysicalPlan::Except {
                left,
                right,
                all,
                schema,
                parallel,
                hints,
            } => {
                let optimized_left = Self::push_required(*left, required.clone());
                let optimized_right = Self::push_required(*right, required);
                PhysicalPlan::Except {
                    left: Box::new(optimized_left),
                    right: Box::new(optimized_right),
                    all,
                    schema,
                    parallel,
                    hints,
                }
            }

            PhysicalPlan::Window {
                input,
                window_exprs,
                schema,
                hints,
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
                PhysicalPlan::Window {
                    input: Box::new(optimized_input),
                    window_exprs,
                    schema,
                    hints,
                }
            }

            PhysicalPlan::Unnest {
                input,
                columns,
                schema,
            } => {
                let mut input_required = required;
                for col in &columns {
                    Self::extract_required_columns(&col.expr, &mut input_required);
                }
                let optimized_input = Self::push_required(*input, input_required);
                PhysicalPlan::Unnest {
                    input: Box::new(optimized_input),
                    columns,
                    schema,
                }
            }

            PhysicalPlan::Qualify { input, predicate } => {
                let mut input_required = required;
                Self::extract_required_columns(&predicate, &mut input_required);
                let optimized_input = Self::push_required(*input, input_required);
                PhysicalPlan::Qualify {
                    input: Box::new(optimized_input),
                    predicate,
                }
            }

            PhysicalPlan::Sample {
                input,
                sample_type,
                sample_value,
            } => {
                let optimized_input = Self::push_required(*input, required);
                PhysicalPlan::Sample {
                    input: Box::new(optimized_input),
                    sample_type,
                    sample_value,
                }
            }

            PhysicalPlan::GapFill {
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
                PhysicalPlan::GapFill {
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

            PhysicalPlan::WithCte {
                ctes,
                body,
                parallel_ctes,
                hints,
            } => {
                let optimized_body = Self::push_required(*body, required);
                PhysicalPlan::WithCte {
                    ctes,
                    body: Box::new(optimized_body),
                    parallel_ctes,
                    hints,
                }
            }

            PhysicalPlan::Explain {
                input,
                analyze,
                logical_plan_text,
                physical_plan_text,
            } => {
                let input_schema_len = input.schema().fields.len();
                let input_required = RequiredColumns::all(input_schema_len);
                let optimized_input = Self::push_required(*input, input_required);
                PhysicalPlan::Explain {
                    input: Box::new(optimized_input),
                    analyze,
                    logical_plan_text,
                    physical_plan_text,
                }
            }

            other => other,
        }
    }
}
