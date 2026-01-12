use rustc_hash::FxHashSet;
use yachtsql_ir::{Expr, JoinType, SortExpr};

use crate::PhysicalPlan;
use crate::planner::predicate::collect_column_indices;

fn collect_used_columns_from_exprs(exprs: &[Expr], used: &mut FxHashSet<usize>) {
    for expr in exprs {
        collect_column_indices_into_set(expr, used);
    }
}

fn collect_column_indices_into_set(expr: &Expr, indices: &mut FxHashSet<usize>) {
    let collected = collect_column_indices(expr);
    indices.extend(collected);
}

fn collect_used_columns_from_sort_exprs(sort_exprs: &[SortExpr], used: &mut FxHashSet<usize>) {
    for se in sort_exprs {
        collect_column_indices_into_set(&se.expr, used);
    }
}

fn uses_columns_from_range(used: &FxHashSet<usize>, start: usize, end: usize) -> bool {
    used.iter().any(|&idx| idx >= start && idx < end)
}

enum EliminationResult {
    EliminateRight,
    EliminateLeft { left_len: usize },
    NoElimination,
}

fn try_eliminate_join(
    join_type: JoinType,
    left_schema_len: usize,
    right_schema_len: usize,
    used_above: &FxHashSet<usize>,
) -> EliminationResult {
    if used_above.is_empty() {
        return EliminationResult::NoElimination;
    }

    let uses_left = uses_columns_from_range(used_above, 0, left_schema_len);
    let uses_right = uses_columns_from_range(
        used_above,
        left_schema_len,
        left_schema_len + right_schema_len,
    );

    match join_type {
        JoinType::Left => {
            if !uses_right {
                EliminationResult::EliminateRight
            } else {
                EliminationResult::NoElimination
            }
        }
        JoinType::Right => {
            if !uses_left {
                EliminationResult::EliminateLeft {
                    left_len: left_schema_len,
                }
            } else {
                EliminationResult::NoElimination
            }
        }
        JoinType::Inner | JoinType::Full | JoinType::Cross => EliminationResult::NoElimination,
    }
}

pub fn apply_join_elimination(plan: PhysicalPlan) -> PhysicalPlan {
    apply_join_elimination_with_context(plan, &FxHashSet::default())
}

fn apply_join_elimination_with_context(
    plan: PhysicalPlan,
    used_above: &FxHashSet<usize>,
) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Project {
            input,
            expressions,
            schema,
        } => {
            let mut new_used = used_above.clone();
            collect_used_columns_from_exprs(&expressions, &mut new_used);
            let optimized_input = apply_join_elimination_with_context(*input, &new_used);
            PhysicalPlan::Project {
                input: Box::new(optimized_input),
                expressions,
                schema,
            }
        }

        PhysicalPlan::Filter { input, predicate } => {
            let mut new_used = used_above.clone();
            collect_column_indices_into_set(&predicate, &mut new_used);
            let optimized_input = apply_join_elimination_with_context(*input, &new_used);
            PhysicalPlan::Filter {
                input: Box::new(optimized_input),
                predicate,
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
            let left_len = left.schema().fields.len();
            let right_len = right.schema().fields.len();

            match try_eliminate_join(join_type, left_len, right_len, used_above) {
                EliminationResult::EliminateRight => {
                    apply_join_elimination_with_context(*left, used_above)
                }
                EliminationResult::EliminateLeft { left_len } => {
                    let adjusted_used: FxHashSet<usize> = used_above
                        .iter()
                        .filter_map(|&idx| {
                            if idx >= left_len {
                                Some(idx - left_len)
                            } else {
                                None
                            }
                        })
                        .collect();
                    apply_join_elimination_with_context(*right, &adjusted_used)
                }
                EliminationResult::NoElimination => {
                    let mut left_used = used_above.clone();
                    collect_used_columns_from_exprs(&left_keys, &mut left_used);

                    let mut right_key_indices = FxHashSet::default();
                    collect_used_columns_from_exprs(&right_keys, &mut right_key_indices);

                    let right_offset_used: FxHashSet<usize> = used_above
                        .iter()
                        .copied()
                        .chain(right_key_indices.iter().copied())
                        .filter_map(|idx| {
                            if idx >= left_len {
                                Some(idx - left_len)
                            } else {
                                None
                            }
                        })
                        .collect();

                    let optimized_left = apply_join_elimination_with_context(*left, &left_used);
                    let optimized_right =
                        apply_join_elimination_with_context(*right, &right_offset_used);

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
            let left_len = left.schema().fields.len();
            let right_len = right.schema().fields.len();

            match try_eliminate_join(join_type, left_len, right_len, used_above) {
                EliminationResult::EliminateRight => {
                    apply_join_elimination_with_context(*left, used_above)
                }
                EliminationResult::EliminateLeft { left_len } => {
                    let adjusted_used: FxHashSet<usize> = used_above
                        .iter()
                        .filter_map(|&idx| {
                            if idx >= left_len {
                                Some(idx - left_len)
                            } else {
                                None
                            }
                        })
                        .collect();
                    apply_join_elimination_with_context(*right, &adjusted_used)
                }
                EliminationResult::NoElimination => {
                    let mut left_used = used_above.clone();
                    if let Some(ref cond) = condition {
                        collect_column_indices_into_set(cond, &mut left_used);
                    }

                    let mut condition_indices = FxHashSet::default();
                    if let Some(ref cond) = condition {
                        collect_column_indices_into_set(cond, &mut condition_indices);
                    }

                    let right_offset_used: FxHashSet<usize> = used_above
                        .iter()
                        .copied()
                        .chain(condition_indices.iter().copied())
                        .filter_map(|idx| {
                            if idx >= left_len {
                                Some(idx - left_len)
                            } else {
                                None
                            }
                        })
                        .collect();

                    let optimized_left = apply_join_elimination_with_context(*left, &left_used);
                    let optimized_right =
                        apply_join_elimination_with_context(*right, &right_offset_used);

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
            }
        }

        PhysicalPlan::CrossJoin {
            left,
            right,
            schema,
            parallel,
            hints,
        } => {
            let left_len = left.schema().fields.len();

            let right_offset_used: FxHashSet<usize> = used_above
                .iter()
                .filter_map(|&idx| {
                    if idx >= left_len {
                        Some(idx - left_len)
                    } else {
                        None
                    }
                })
                .collect();

            let optimized_left = apply_join_elimination_with_context(*left, used_above);
            let optimized_right = apply_join_elimination_with_context(*right, &right_offset_used);

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
            let mut new_used = FxHashSet::default();
            collect_used_columns_from_exprs(&group_by, &mut new_used);
            collect_used_columns_from_exprs(&aggregates, &mut new_used);
            let optimized_input = apply_join_elimination_with_context(*input, &new_used);
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
            let mut new_used = used_above.clone();
            collect_used_columns_from_sort_exprs(&sort_exprs, &mut new_used);
            let optimized_input = apply_join_elimination_with_context(*input, &new_used);
            PhysicalPlan::Sort {
                input: Box::new(optimized_input),
                sort_exprs,
                hints,
            }
        }

        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => {
            let mut new_used = used_above.clone();
            collect_used_columns_from_sort_exprs(&sort_exprs, &mut new_used);
            let optimized_input = apply_join_elimination_with_context(*input, &new_used);
            PhysicalPlan::TopN {
                input: Box::new(optimized_input),
                sort_exprs,
                limit,
            }
        }

        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let optimized_input = apply_join_elimination_with_context(*input, used_above);
            PhysicalPlan::Limit {
                input: Box::new(optimized_input),
                limit,
                offset,
            }
        }

        PhysicalPlan::Distinct { input } => {
            let optimized_input = apply_join_elimination_with_context(*input, used_above);
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
        } => PhysicalPlan::Union {
            inputs: inputs
                .into_iter()
                .map(|p| apply_join_elimination_with_context(p, used_above))
                .collect(),
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
            left: Box::new(apply_join_elimination_with_context(*left, used_above)),
            right: Box::new(apply_join_elimination_with_context(*right, used_above)),
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
            left: Box::new(apply_join_elimination_with_context(*left, used_above)),
            right: Box::new(apply_join_elimination_with_context(*right, used_above)),
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
        } => {
            let mut new_used = used_above.clone();
            collect_used_columns_from_exprs(&window_exprs, &mut new_used);
            let optimized_input = apply_join_elimination_with_context(*input, &new_used);
            PhysicalPlan::Window {
                input: Box::new(optimized_input),
                window_exprs,
                schema,
                hints,
            }
        }

        PhysicalPlan::WithCte {
            ctes,
            body,
            parallel_ctes,
            hints,
        } => PhysicalPlan::WithCte {
            ctes,
            body: Box::new(apply_join_elimination_with_context(*body, used_above)),
            parallel_ctes,
            hints,
        },

        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => {
            let optimized_input = apply_join_elimination_with_context(*input, used_above);
            PhysicalPlan::Unnest {
                input: Box::new(optimized_input),
                columns,
                schema,
            }
        }

        PhysicalPlan::Qualify { input, predicate } => {
            let mut new_used = used_above.clone();
            collect_column_indices_into_set(&predicate, &mut new_used);
            let optimized_input = apply_join_elimination_with_context(*input, &new_used);
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
            let optimized_input = apply_join_elimination_with_context(*input, used_above);
            PhysicalPlan::Sample {
                input: Box::new(optimized_input),
                sample_type,
                sample_value,
            }
        }

        PhysicalPlan::Insert {
            table_name,
            columns,
            source,
        } => PhysicalPlan::Insert {
            table_name,
            columns,
            source: Box::new(apply_join_elimination(*source)),
        },

        PhysicalPlan::CreateTable {
            table_name,
            columns,
            if_not_exists,
            or_replace,
            query,
        } => PhysicalPlan::CreateTable {
            table_name,
            columns,
            if_not_exists,
            or_replace,
            query: query.map(|q| Box::new(apply_join_elimination(*q))),
        },

        PhysicalPlan::CreateView {
            name,
            query,
            query_sql,
            column_aliases,
            or_replace,
            if_not_exists,
        } => PhysicalPlan::CreateView {
            name,
            query: Box::new(apply_join_elimination(*query)),
            query_sql,
            column_aliases,
            or_replace,
            if_not_exists,
        },

        PhysicalPlan::Merge {
            target_table,
            source,
            on,
            clauses,
        } => PhysicalPlan::Merge {
            target_table,
            source: Box::new(apply_join_elimination(*source)),
            on,
            clauses,
        },

        PhysicalPlan::Update {
            table_name,
            alias,
            assignments,
            from,
            filter,
        } => PhysicalPlan::Update {
            table_name,
            alias,
            assignments,
            from: from.map(|f| Box::new(apply_join_elimination(*f))),
            filter,
        },

        PhysicalPlan::ExportData { options, query } => PhysicalPlan::ExportData {
            options,
            query: Box::new(apply_join_elimination(*query)),
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
            input: Box::new(apply_join_elimination(*input)),
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
            input: Box::new(apply_join_elimination(*input)),
            analyze,
            logical_plan_text,
            physical_plan_text,
        },

        PhysicalPlan::For {
            variable,
            query,
            body,
        } => PhysicalPlan::For {
            variable,
            query: Box::new(apply_join_elimination(*query)),
            body: body.into_iter().map(apply_join_elimination).collect(),
        },

        PhysicalPlan::If {
            condition,
            then_branch,
            else_branch,
        } => PhysicalPlan::If {
            condition,
            then_branch: then_branch
                .into_iter()
                .map(apply_join_elimination)
                .collect(),
            else_branch: else_branch.map(|b| b.into_iter().map(apply_join_elimination).collect()),
        },

        PhysicalPlan::While {
            condition,
            body,
            label,
        } => PhysicalPlan::While {
            condition,
            body: body.into_iter().map(apply_join_elimination).collect(),
            label,
        },

        PhysicalPlan::Loop { body, label } => PhysicalPlan::Loop {
            body: body.into_iter().map(apply_join_elimination).collect(),
            label,
        },

        PhysicalPlan::Block { body, label } => PhysicalPlan::Block {
            body: body.into_iter().map(apply_join_elimination).collect(),
            label,
        },

        PhysicalPlan::Repeat {
            body,
            until_condition,
        } => PhysicalPlan::Repeat {
            body: body.into_iter().map(apply_join_elimination).collect(),
            until_condition,
        },

        PhysicalPlan::CreateProcedure {
            name,
            args,
            body,
            or_replace,
            if_not_exists,
        } => PhysicalPlan::CreateProcedure {
            name,
            args,
            body: body.into_iter().map(apply_join_elimination).collect(),
            or_replace,
            if_not_exists,
        },

        PhysicalPlan::TryCatch {
            try_block,
            catch_block,
        } => PhysicalPlan::TryCatch {
            try_block: try_block
                .into_iter()
                .map(|(p, sql)| (apply_join_elimination(p), sql))
                .collect(),
            catch_block: catch_block
                .into_iter()
                .map(apply_join_elimination)
                .collect(),
        },

        other => other,
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::DataType;
    use yachtsql_ir::{BinaryOp, Literal, PlanField, PlanSchema};

    use super::*;
    use crate::ExecutionHints;

    fn make_schema(table_name: &str, num_columns: usize) -> PlanSchema {
        let fields = (0..num_columns)
            .map(|i| PlanField::new(format!("col{}", i), DataType::Int64).with_table(table_name))
            .collect();
        PlanSchema::from_fields(fields)
    }

    fn make_scan(table_name: &str, num_columns: usize) -> PhysicalPlan {
        PhysicalPlan::TableScan {
            table_name: table_name.to_string(),
            schema: make_schema(table_name, num_columns),
            projection: None,
            row_count: None,
        }
    }

    fn col(table: &str, name: &str, index: usize) -> Expr {
        Expr::Column {
            table: Some(table.to_string()),
            name: name.to_string(),
            index: Some(index),
        }
    }

    fn lit_int(val: i64) -> Expr {
        Expr::Literal(Literal::Int64(val))
    }

    fn make_left_hash_join(left: PhysicalPlan, right: PhysicalPlan) -> PhysicalPlan {
        let left_len = left.schema().fields.len();
        let mut fields = left.schema().fields.clone();
        fields.extend(right.schema().fields.clone());
        let schema = PlanSchema::from_fields(fields);

        PhysicalPlan::HashJoin {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Left,
            left_keys: vec![col("a", "col0", 0)],
            right_keys: vec![col("b", "col0", left_len)],
            schema,
            parallel: false,
            hints: ExecutionHints::default(),
        }
    }

    fn make_right_hash_join(left: PhysicalPlan, right: PhysicalPlan) -> PhysicalPlan {
        let left_len = left.schema().fields.len();
        let mut fields = left.schema().fields.clone();
        fields.extend(right.schema().fields.clone());
        let schema = PlanSchema::from_fields(fields);

        PhysicalPlan::HashJoin {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Right,
            left_keys: vec![col("a", "col0", 0)],
            right_keys: vec![col("b", "col0", left_len)],
            schema,
            parallel: false,
            hints: ExecutionHints::default(),
        }
    }

    fn make_inner_hash_join(left: PhysicalPlan, right: PhysicalPlan) -> PhysicalPlan {
        let left_len = left.schema().fields.len();
        let mut fields = left.schema().fields.clone();
        fields.extend(right.schema().fields.clone());
        let schema = PlanSchema::from_fields(fields);

        PhysicalPlan::HashJoin {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            left_keys: vec![col("a", "col0", 0)],
            right_keys: vec![col("b", "col0", left_len)],
            schema,
            parallel: false,
            hints: ExecutionHints::default(),
        }
    }

    fn make_cross_join(left: PhysicalPlan, right: PhysicalPlan) -> PhysicalPlan {
        let mut fields = left.schema().fields.clone();
        fields.extend(right.schema().fields.clone());
        let schema = PlanSchema::from_fields(fields);

        PhysicalPlan::CrossJoin {
            left: Box::new(left),
            right: Box::new(right),
            schema,
            parallel: false,
            hints: ExecutionHints::default(),
        }
    }

    #[test]
    fn eliminates_left_join_when_right_unused() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_left_hash_join(left, right);

        let project = PhysicalPlan::Project {
            input: Box::new(join),
            expressions: vec![col("a", "col0", 0), col("a", "col1", 1)],
            schema: make_schema("result", 2),
        };

        let result = apply_join_elimination(project);

        match result {
            PhysicalPlan::Project { input, .. } => match *input {
                PhysicalPlan::TableScan { table_name, .. } => {
                    assert_eq!(table_name, "a");
                }
                _ => panic!("Expected TableScan, got {:?}", input),
            },
            _ => panic!("Expected Project"),
        }
    }

    #[test]
    fn preserves_left_join_when_right_used() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_left_hash_join(left, right);

        let project = PhysicalPlan::Project {
            input: Box::new(join),
            expressions: vec![col("a", "col0", 0), col("b", "col0", 2)],
            schema: make_schema("result", 2),
        };

        let result = apply_join_elimination(project);

        match result {
            PhysicalPlan::Project { input, .. } => {
                assert!(matches!(*input, PhysicalPlan::HashJoin { .. }));
            }
            _ => panic!("Expected Project with HashJoin"),
        }
    }

    #[test]
    fn eliminates_right_join_when_left_unused() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_right_hash_join(left, right);

        let project = PhysicalPlan::Project {
            input: Box::new(join),
            expressions: vec![col("b", "col0", 2), col("b", "col1", 3)],
            schema: make_schema("result", 2),
        };

        let result = apply_join_elimination(project);

        match result {
            PhysicalPlan::Project { input, .. } => match *input {
                PhysicalPlan::TableScan { table_name, .. } => {
                    assert_eq!(table_name, "b");
                }
                _ => panic!("Expected TableScan for table b"),
            },
            _ => panic!("Expected Project"),
        }
    }

    #[test]
    fn preserves_inner_join() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_inner_hash_join(left, right);

        let project = PhysicalPlan::Project {
            input: Box::new(join),
            expressions: vec![col("a", "col0", 0)],
            schema: make_schema("result", 1),
        };

        let result = apply_join_elimination(project);

        match result {
            PhysicalPlan::Project { input, .. } => {
                assert!(matches!(*input, PhysicalPlan::HashJoin { .. }));
            }
            _ => panic!("Expected Project with HashJoin preserved"),
        }
    }

    #[test]
    fn preserves_cross_join_even_when_right_unused() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_cross_join(left, right);

        let project = PhysicalPlan::Project {
            input: Box::new(join),
            expressions: vec![col("a", "col0", 0)],
            schema: make_schema("result", 1),
        };

        let result = apply_join_elimination(project);

        match result {
            PhysicalPlan::Project { input, .. } => {
                assert!(matches!(*input, PhysicalPlan::CrossJoin { .. }));
            }
            _ => panic!("Expected Project with CrossJoin"),
        }
    }

    #[test]
    fn preserves_cross_join_even_when_left_unused() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_cross_join(left, right);

        let project = PhysicalPlan::Project {
            input: Box::new(join),
            expressions: vec![col("b", "col0", 2)],
            schema: make_schema("result", 1),
        };

        let result = apply_join_elimination(project);

        match result {
            PhysicalPlan::Project { input, .. } => {
                assert!(matches!(*input, PhysicalPlan::CrossJoin { .. }));
            }
            _ => panic!("Expected Project with CrossJoin"),
        }
    }

    #[test]
    fn preserves_left_join_when_right_used_in_filter() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_left_hash_join(left, right);

        let filter = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate: Expr::BinaryOp {
                left: Box::new(col("b", "col0", 2)),
                op: BinaryOp::Eq,
                right: Box::new(lit_int(5)),
            },
        };

        let project = PhysicalPlan::Project {
            input: Box::new(filter),
            expressions: vec![col("a", "col0", 0)],
            schema: make_schema("result", 1),
        };

        let result = apply_join_elimination(project);

        match result {
            PhysicalPlan::Project { input, .. } => match *input {
                PhysicalPlan::Filter { input, .. } => {
                    assert!(matches!(*input, PhysicalPlan::HashJoin { .. }));
                }
                _ => panic!("Expected Filter"),
            },
            _ => panic!("Expected Project"),
        }
    }

    #[test]
    fn handles_nested_left_joins_elimination() {
        let a = make_scan("a", 2);
        let b = make_scan("b", 2);
        let c = make_scan("c", 2);

        let join_ab = make_left_hash_join(a, b);
        let mut fields = join_ab.schema().fields.clone();
        fields.extend(c.schema().fields.clone());
        let schema_abc = PlanSchema::from_fields(fields);

        let join_abc = PhysicalPlan::HashJoin {
            left: Box::new(join_ab),
            right: Box::new(c),
            join_type: JoinType::Left,
            left_keys: vec![col("a", "col0", 0)],
            right_keys: vec![col("c", "col0", 4)],
            schema: schema_abc,
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let project = PhysicalPlan::Project {
            input: Box::new(join_abc),
            expressions: vec![col("a", "col0", 0)],
            schema: make_schema("result", 1),
        };

        let result = apply_join_elimination(project);

        match result {
            PhysicalPlan::Project { input, .. } => match *input {
                PhysicalPlan::TableScan { table_name, .. } => {
                    assert_eq!(table_name, "a");
                }
                _ => panic!("Expected TableScan a after eliminating both joins"),
            },
            _ => panic!("Expected Project"),
        }
    }

    #[test]
    fn preserves_left_join_when_right_used_in_sort() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_left_hash_join(left, right);

        let sort = PhysicalPlan::Sort {
            input: Box::new(join),
            sort_exprs: vec![SortExpr {
                expr: col("b", "col0", 2),
                asc: true,
                nulls_first: false,
            }],
            hints: ExecutionHints::default(),
        };

        let project = PhysicalPlan::Project {
            input: Box::new(sort),
            expressions: vec![col("a", "col0", 0)],
            schema: make_schema("result", 1),
        };

        let result = apply_join_elimination(project);

        match result {
            PhysicalPlan::Project { input, .. } => match *input {
                PhysicalPlan::Sort { input, .. } => {
                    assert!(matches!(*input, PhysicalPlan::HashJoin { .. }));
                }
                _ => panic!("Expected Sort"),
            },
            _ => panic!("Expected Project"),
        }
    }

    #[test]
    fn handles_join_without_projection_above() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_left_hash_join(left, right);

        let result = apply_join_elimination(join);

        assert!(matches!(result, PhysicalPlan::HashJoin { .. }));
    }

    #[test]
    fn test_uses_columns_from_range() {
        let mut used = FxHashSet::default();
        used.insert(0);
        used.insert(1);
        used.insert(4);

        assert!(uses_columns_from_range(&used, 0, 2));
        assert!(!uses_columns_from_range(&used, 2, 4));
        assert!(uses_columns_from_range(&used, 4, 6));
    }

    #[test]
    fn eliminates_nested_loop_left_join() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);

        let mut fields = left.schema().fields.clone();
        fields.extend(right.schema().fields.clone());
        let schema = PlanSchema::from_fields(fields);

        let join = PhysicalPlan::NestedLoopJoin {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Left,
            condition: Some(Expr::BinaryOp {
                left: Box::new(col("a", "col0", 0)),
                op: BinaryOp::Eq,
                right: Box::new(col("b", "col0", 2)),
            }),
            schema,
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let project = PhysicalPlan::Project {
            input: Box::new(join),
            expressions: vec![col("a", "col0", 0)],
            schema: make_schema("result", 1),
        };

        let result = apply_join_elimination(project);

        match result {
            PhysicalPlan::Project { input, .. } => match *input {
                PhysicalPlan::TableScan { table_name, .. } => {
                    assert_eq!(table_name, "a");
                }
                _ => panic!("Expected TableScan a"),
            },
            _ => panic!("Expected Project"),
        }
    }

    #[test]
    fn preserves_join_when_used_in_aggregate() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_left_hash_join(left, right);

        let aggregate = PhysicalPlan::HashAggregate {
            input: Box::new(join),
            group_by: vec![col("a", "col0", 0)],
            aggregates: vec![Expr::Aggregate {
                func: yachtsql_ir::AggregateFunction::Sum,
                args: vec![col("b", "col1", 3)],
                distinct: false,
                filter: None,
                order_by: vec![],
                limit: None,
                ignore_nulls: false,
            }],
            schema: make_schema("result", 2),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        };

        let result = apply_join_elimination(aggregate);

        match result {
            PhysicalPlan::HashAggregate { input, .. } => {
                assert!(matches!(*input, PhysicalPlan::HashJoin { .. }));
            }
            _ => panic!("Expected HashAggregate with HashJoin preserved"),
        }
    }
}
