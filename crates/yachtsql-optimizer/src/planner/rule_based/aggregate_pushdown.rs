use rustc_hash::FxHashSet;
use yachtsql_ir::{AggregateFunction, Expr, JoinType, PlanField, PlanSchema, WhenClause};

use crate::PhysicalPlan;
use crate::planner::predicate::collect_column_indices;

fn is_decomposable_aggregate(func: AggregateFunction) -> bool {
    matches!(
        func,
        AggregateFunction::Sum
            | AggregateFunction::Count
            | AggregateFunction::Min
            | AggregateFunction::Max
    )
}

fn collect_column_indices_set(expr: &Expr) -> FxHashSet<usize> {
    collect_column_indices(expr).into_iter().collect()
}

fn all_columns_in_range(indices: &FxHashSet<usize>, start: usize, end: usize) -> bool {
    indices.iter().all(|&idx| idx >= start && idx < end)
}

fn any_column_in_range(indices: &FxHashSet<usize>, start: usize, end: usize) -> bool {
    indices.iter().any(|&idx| idx >= start && idx < end)
}

fn shift_column_indices(expr: &Expr, offset: isize) -> Expr {
    match expr {
        Expr::Column { table, name, index } => Expr::Column {
            table: table.clone(),
            name: name.clone(),
            index: index.map(|i| (i as isize + offset) as usize),
        },
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(shift_column_indices(left, offset)),
            op: *op,
            right: Box::new(shift_column_indices(right, offset)),
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(shift_column_indices(expr, offset)),
        },
        Expr::ScalarFunction { name, args } => Expr::ScalarFunction {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| shift_column_indices(a, offset))
                .collect(),
        },
        Expr::Aggregate {
            func,
            args,
            distinct,
            filter,
            order_by,
            limit,
            ignore_nulls,
        } => Expr::Aggregate {
            func: *func,
            args: args
                .iter()
                .map(|a| shift_column_indices(a, offset))
                .collect(),
            distinct: *distinct,
            filter: filter
                .as_ref()
                .map(|f| Box::new(shift_column_indices(f, offset))),
            order_by: order_by.clone(),
            limit: *limit,
            ignore_nulls: *ignore_nulls,
        },
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => Expr::Case {
            operand: operand
                .as_ref()
                .map(|o| Box::new(shift_column_indices(o, offset))),
            when_clauses: when_clauses
                .iter()
                .map(|wc| WhenClause {
                    condition: shift_column_indices(&wc.condition, offset),
                    result: shift_column_indices(&wc.result, offset),
                })
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|e| Box::new(shift_column_indices(e, offset))),
        },
        Expr::Cast {
            expr,
            data_type,
            safe,
        } => Expr::Cast {
            expr: Box::new(shift_column_indices(expr, offset)),
            data_type: data_type.clone(),
            safe: *safe,
        },
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(shift_column_indices(expr, offset)),
            negated: *negated,
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(shift_column_indices(expr, offset)),
            low: Box::new(shift_column_indices(low, offset)),
            high: Box::new(shift_column_indices(high, offset)),
            negated: *negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(shift_column_indices(expr, offset)),
            list: list
                .iter()
                .map(|e| shift_column_indices(e, offset))
                .collect(),
            negated: *negated,
        },
        Expr::Alias { expr, name } => Expr::Alias {
            expr: Box::new(shift_column_indices(expr, offset)),
            name: name.clone(),
        },
        other => other.clone(),
    }
}

struct AggregatePushdownCandidate {
    left_schema_len: usize,
    right_input: Box<PhysicalPlan>,
    right_group_by: Vec<Expr>,
    right_aggregates: Vec<Expr>,
    right_agg_schema: PlanSchema,
    new_join_right_keys: Vec<Expr>,
    outer_group_by: Vec<Expr>,
}

#[allow(clippy::too_many_arguments)]
fn try_pushdown_aggregate_through_join(
    group_by: &[Expr],
    aggregates: &[Expr],
    agg_schema: &PlanSchema,
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    join_type: JoinType,
    left_keys: &[Expr],
    right_keys: &[Expr],
) -> Option<AggregatePushdownCandidate> {
    if join_type != JoinType::Inner {
        return None;
    }

    let left_schema_len = left.schema().fields.len();
    let right_schema_len = right.schema().fields.len();

    let mut agg_column_indices = FxHashSet::default();
    for agg in aggregates {
        agg_column_indices.extend(collect_column_indices(agg));
    }

    let aggs_use_left = any_column_in_range(&agg_column_indices, 0, left_schema_len);
    let aggs_use_right = any_column_in_range(
        &agg_column_indices,
        left_schema_len,
        left_schema_len + right_schema_len,
    );

    if aggs_use_left && aggs_use_right {
        return None;
    }
    if !aggs_use_right {
        return None;
    }

    for agg in aggregates {
        if let Expr::Aggregate { func, .. } = agg
            && !is_decomposable_aggregate(*func)
        {
            return None;
        }
    }

    let mut group_by_left_indices = FxHashSet::default();
    let mut group_by_right_indices = FxHashSet::default();

    for gb_expr in group_by {
        let indices = collect_column_indices_set(gb_expr);
        for idx in indices {
            if idx < left_schema_len {
                group_by_left_indices.insert(idx);
            } else {
                group_by_right_indices.insert(idx - left_schema_len);
            }
        }
    }

    if group_by_left_indices.is_empty() {
        return None;
    }

    let mut matching_right_key_indices = Vec::new();
    for (i, left_key) in left_keys.iter().enumerate() {
        let left_key_indices = collect_column_indices_set(left_key);
        if left_key_indices.len() == 1 {
            let left_idx = *left_key_indices.iter().next().unwrap();
            if group_by_left_indices.contains(&left_idx) {
                matching_right_key_indices.push(i);
            }
        }
    }

    if matching_right_key_indices.is_empty() {
        return None;
    }

    let mut right_group_by = Vec::new();
    let mut right_group_by_indices = Vec::new();

    for &key_idx in &matching_right_key_indices {
        let right_key = &right_keys[key_idx];
        let shifted = shift_column_indices(right_key, -(left_schema_len as isize));
        right_group_by.push(shifted);

        let indices = collect_column_indices_set(right_key);
        for idx in indices {
            if idx >= left_schema_len {
                right_group_by_indices.push(idx - left_schema_len);
            }
        }
    }

    for idx in group_by_right_indices {
        if !right_group_by_indices.contains(&idx) {
            right_group_by.push(Expr::Column {
                table: None,
                name: right.schema().fields[idx].name.clone(),
                index: Some(idx),
            });
            right_group_by_indices.push(idx);
        }
    }

    let right_aggregates: Vec<Expr> = aggregates
        .iter()
        .map(|agg| shift_column_indices(agg, -(left_schema_len as isize)))
        .collect();

    let mut right_agg_schema_fields = Vec::new();

    for (i, &idx) in right_group_by_indices.iter().enumerate() {
        let orig_field = &right.schema().fields[idx];
        right_agg_schema_fields.push(PlanField {
            name: format!("__group_{}", i),
            data_type: orig_field.data_type.clone(),
            nullable: orig_field.nullable,
            table: None,
        });
    }

    for (i, _agg) in aggregates.iter().enumerate() {
        let orig_field = &agg_schema.fields[group_by.len() + i];
        right_agg_schema_fields.push(PlanField {
            name: format!("__agg_{}", i),
            data_type: orig_field.data_type.clone(),
            nullable: true,
            table: None,
        });
    }

    let right_agg_schema = PlanSchema {
        fields: right_agg_schema_fields,
    };

    let new_join_right_keys: Vec<Expr> = (0..matching_right_key_indices.len())
        .map(|i| Expr::Column {
            table: None,
            name: format!("__group_{}", i),
            index: Some(i),
        })
        .collect();

    let mut outer_group_by = Vec::new();
    for (i, gb) in group_by.iter().enumerate() {
        let indices = collect_column_indices_set(gb);
        if all_columns_in_range(&indices, 0, left_schema_len) {
            outer_group_by.push(gb.clone());
        } else {
            let right_idx_in_agg = right_group_by_indices
                .iter()
                .position(|&ri| {
                    let gb_indices = collect_column_indices_set(gb);
                    gb_indices.len() == 1 && gb_indices.contains(&(ri + left_schema_len))
                })
                .unwrap_or(i);
            outer_group_by.push(Expr::Column {
                table: None,
                name: format!("__group_{}", right_idx_in_agg),
                index: Some(left_schema_len + right_idx_in_agg),
            });
        }
    }

    Some(AggregatePushdownCandidate {
        left_schema_len,
        right_input: Box::new(right.clone()),
        right_group_by,
        right_aggregates,
        right_agg_schema,
        new_join_right_keys,
        outer_group_by,
    })
}

pub fn apply_aggregate_pushdown(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::HashAggregate {
            input,
            group_by,
            aggregates,
            schema,
            grouping_sets,
            hints,
        } => {
            let optimized_input = apply_aggregate_pushdown(*input);

            if grouping_sets.is_some() {
                return PhysicalPlan::HashAggregate {
                    input: Box::new(optimized_input),
                    group_by,
                    aggregates,
                    schema,
                    grouping_sets,
                    hints,
                };
            }

            if let PhysicalPlan::HashJoin {
                left,
                right,
                join_type,
                left_keys,
                right_keys,
                schema: _join_schema,
                parallel,
                hints: join_hints,
            } = &optimized_input
                && let Some(candidate) = try_pushdown_aggregate_through_join(
                    &group_by,
                    &aggregates,
                    &schema,
                    left,
                    right,
                    *join_type,
                    left_keys,
                    right_keys,
                )
            {
                let pre_aggregate = PhysicalPlan::HashAggregate {
                    input: candidate.right_input,
                    group_by: candidate.right_group_by,
                    aggregates: candidate.right_aggregates,
                    schema: candidate.right_agg_schema.clone(),
                    grouping_sets: None,
                    hints,
                };

                let mut new_join_schema_fields = left.schema().fields.clone();
                new_join_schema_fields.extend(candidate.right_agg_schema.fields.clone());
                let new_join_schema = PlanSchema {
                    fields: new_join_schema_fields,
                };

                let new_join = PhysicalPlan::HashJoin {
                    left: left.clone(),
                    right: Box::new(pre_aggregate),
                    join_type: *join_type,
                    left_keys: left_keys.clone(),
                    right_keys: candidate.new_join_right_keys,
                    schema: new_join_schema,
                    parallel: *parallel,
                    hints: *join_hints,
                };

                let _num_group_cols = candidate.outer_group_by.len();
                let outer_aggregates: Vec<Expr> = aggregates
                    .iter()
                    .enumerate()
                    .map(|(i, agg)| {
                        let agg_col_idx = candidate.left_schema_len
                            + candidate.right_agg_schema.fields.len()
                            - aggregates.len()
                            + i;
                        match agg {
                            Expr::Aggregate { func, .. } => match func {
                                AggregateFunction::Count => Expr::Aggregate {
                                    func: AggregateFunction::Sum,
                                    args: vec![Expr::Column {
                                        table: None,
                                        name: format!("__agg_{}", i),
                                        index: Some(agg_col_idx),
                                    }],
                                    distinct: false,
                                    filter: None,
                                    order_by: vec![],
                                    limit: None,
                                    ignore_nulls: false,
                                },
                                _ => Expr::Aggregate {
                                    func: *func,
                                    args: vec![Expr::Column {
                                        table: None,
                                        name: format!("__agg_{}", i),
                                        index: Some(agg_col_idx),
                                    }],
                                    distinct: false,
                                    filter: None,
                                    order_by: vec![],
                                    limit: None,
                                    ignore_nulls: false,
                                },
                            },
                            _ => agg.clone(),
                        }
                    })
                    .collect();

                return PhysicalPlan::HashAggregate {
                    input: Box::new(new_join),
                    group_by: candidate.outer_group_by,
                    aggregates: outer_aggregates,
                    schema,
                    grouping_sets: None,
                    hints,
                };
            }

            PhysicalPlan::HashAggregate {
                input: Box::new(optimized_input),
                group_by,
                aggregates,
                schema,
                grouping_sets,
                hints,
            }
        }

        PhysicalPlan::Project {
            input,
            expressions,
            schema,
        } => PhysicalPlan::Project {
            input: Box::new(apply_aggregate_pushdown(*input)),
            expressions,
            schema,
        },

        PhysicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(apply_aggregate_pushdown(*input)),
            predicate,
        },

        PhysicalPlan::HashJoin {
            left,
            right,
            join_type,
            left_keys,
            right_keys,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::HashJoin {
            left: Box::new(apply_aggregate_pushdown(*left)),
            right: Box::new(apply_aggregate_pushdown(*right)),
            join_type,
            left_keys,
            right_keys,
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            join_type,
            condition,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::NestedLoopJoin {
            left: Box::new(apply_aggregate_pushdown(*left)),
            right: Box::new(apply_aggregate_pushdown(*right)),
            join_type,
            condition,
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::CrossJoin {
            left,
            right,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::CrossJoin {
            left: Box::new(apply_aggregate_pushdown(*left)),
            right: Box::new(apply_aggregate_pushdown(*right)),
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::Sort {
            input,
            sort_exprs,
            hints,
        } => PhysicalPlan::Sort {
            input: Box::new(apply_aggregate_pushdown(*input)),
            sort_exprs,
            hints,
        },

        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => PhysicalPlan::Limit {
            input: Box::new(apply_aggregate_pushdown(*input)),
            limit,
            offset,
        },

        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => PhysicalPlan::TopN {
            input: Box::new(apply_aggregate_pushdown(*input)),
            sort_exprs,
            limit,
        },

        PhysicalPlan::Distinct { input } => PhysicalPlan::Distinct {
            input: Box::new(apply_aggregate_pushdown(*input)),
        },

        PhysicalPlan::Union {
            inputs,
            all,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::Union {
            inputs: inputs.into_iter().map(apply_aggregate_pushdown).collect(),
            all,
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::Intersect {
            left,
            right,
            all,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::Intersect {
            left: Box::new(apply_aggregate_pushdown(*left)),
            right: Box::new(apply_aggregate_pushdown(*right)),
            all,
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::Except {
            left,
            right,
            all,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::Except {
            left: Box::new(apply_aggregate_pushdown(*left)),
            right: Box::new(apply_aggregate_pushdown(*right)),
            all,
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::Window {
            input,
            window_exprs,
            schema,
            hints,
        } => PhysicalPlan::Window {
            input: Box::new(apply_aggregate_pushdown(*input)),
            window_exprs,
            schema,
            hints,
        },

        PhysicalPlan::WithCte {
            ctes,
            body,
            parallel_ctes,
            hints,
        } => PhysicalPlan::WithCte {
            ctes,
            body: Box::new(apply_aggregate_pushdown(*body)),
            parallel_ctes,
            hints,
        },

        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => PhysicalPlan::Unnest {
            input: Box::new(apply_aggregate_pushdown(*input)),
            columns,
            schema,
        },

        PhysicalPlan::Qualify { input, predicate } => PhysicalPlan::Qualify {
            input: Box::new(apply_aggregate_pushdown(*input)),
            predicate,
        },

        PhysicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => PhysicalPlan::Sample {
            input: Box::new(apply_aggregate_pushdown(*input)),
            sample_type,
            sample_value,
        },

        PhysicalPlan::GapFill {
            input,
            ts_column,
            bucket_width,
            value_columns,
            partitioning_columns,
            origin,
            input_schema,
            schema,
        } => PhysicalPlan::GapFill {
            input: Box::new(apply_aggregate_pushdown(*input)),
            ts_column,
            bucket_width,
            value_columns,
            partitioning_columns,
            origin,
            input_schema,
            schema,
        },

        PhysicalPlan::Explain {
            input,
            analyze,
            logical_plan_text,
            physical_plan_text,
        } => PhysicalPlan::Explain {
            input: Box::new(apply_aggregate_pushdown(*input)),
            analyze,
            logical_plan_text,
            physical_plan_text,
        },

        other => other,
    }
}
