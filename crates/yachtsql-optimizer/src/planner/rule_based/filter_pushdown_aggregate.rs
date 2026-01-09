#![coverage(off)]

use yachtsql_ir::Expr;

use crate::PhysicalPlan;
use crate::planner::predicate::{
    build_aggregate_output_to_input_map, collect_column_indices, combine_predicates,
    remap_predicate_indices, split_and_predicates,
};

fn expr_only_references_columns(expr: &Expr, max_index: usize) -> bool {
    let indices = collect_column_indices(expr);
    indices.iter().all(|&idx| idx < max_index)
}

fn partition_predicates_for_aggregate(
    predicates: Vec<Expr>,
    group_by_len: usize,
) -> (Vec<Expr>, Vec<Expr>) {
    let mut pushable = Vec::new();
    let mut non_pushable = Vec::new();

    for pred in predicates {
        if expr_only_references_columns(&pred, group_by_len) {
            pushable.push(pred);
        } else {
            non_pushable.push(pred);
        }
    }

    (pushable, non_pushable)
}

pub fn apply_filter_pushdown_aggregate(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Filter { input, predicate } => {
            let optimized_input = apply_filter_pushdown_aggregate(*input);

            match optimized_input {
                PhysicalPlan::HashAggregate {
                    input: agg_input,
                    group_by,
                    aggregates,
                    schema,
                    grouping_sets,
                    hints,
                } => {
                    let predicates = split_and_predicates(&predicate);
                    let (pushable, non_pushable) =
                        partition_predicates_for_aggregate(predicates, group_by.len());

                    if pushable.is_empty() {
                        return PhysicalPlan::Filter {
                            input: Box::new(PhysicalPlan::HashAggregate {
                                input: agg_input,
                                group_by,
                                aggregates,
                                schema,
                                grouping_sets,
                                hints,
                            }),
                            predicate,
                        };
                    }

                    let output_to_input = build_aggregate_output_to_input_map(&group_by);

                    let remapped_pushable: Vec<Expr> = pushable
                        .iter()
                        .filter_map(|p| remap_predicate_indices(p, &output_to_input))
                        .collect();

                    if remapped_pushable.len() != pushable.len() {
                        return PhysicalPlan::Filter {
                            input: Box::new(PhysicalPlan::HashAggregate {
                                input: agg_input,
                                group_by,
                                aggregates,
                                schema,
                                grouping_sets,
                                hints,
                            }),
                            predicate,
                        };
                    }

                    let new_agg_input = match combine_predicates(remapped_pushable) {
                        Some(pushed_predicate) => Box::new(PhysicalPlan::Filter {
                            input: agg_input,
                            predicate: pushed_predicate,
                        }),
                        None => agg_input,
                    };

                    let new_aggregate = PhysicalPlan::HashAggregate {
                        input: new_agg_input,
                        group_by,
                        aggregates,
                        schema,
                        grouping_sets,
                        hints,
                    };

                    match combine_predicates(non_pushable) {
                        Some(remaining_predicate) => PhysicalPlan::Filter {
                            input: Box::new(new_aggregate),
                            predicate: remaining_predicate,
                        },
                        None => new_aggregate,
                    }
                }
                other => PhysicalPlan::Filter {
                    input: Box::new(other),
                    predicate,
                },
            }
        }

        PhysicalPlan::Project {
            input,
            expressions,
            schema,
        } => PhysicalPlan::Project {
            input: Box::new(apply_filter_pushdown_aggregate(*input)),
            expressions,
            schema,
        },

        PhysicalPlan::HashAggregate {
            input,
            group_by,
            aggregates,
            schema,
            grouping_sets,
            hints,
        } => PhysicalPlan::HashAggregate {
            input: Box::new(apply_filter_pushdown_aggregate(*input)),
            group_by,
            aggregates,
            schema,
            grouping_sets,
            hints,
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
            left: Box::new(apply_filter_pushdown_aggregate(*left)),
            right: Box::new(apply_filter_pushdown_aggregate(*right)),
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
            left: Box::new(apply_filter_pushdown_aggregate(*left)),
            right: Box::new(apply_filter_pushdown_aggregate(*right)),
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
            left: Box::new(apply_filter_pushdown_aggregate(*left)),
            right: Box::new(apply_filter_pushdown_aggregate(*right)),
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::Sort {
            input,
            sort_exprs,
            hints,
        } => PhysicalPlan::Sort {
            input: Box::new(apply_filter_pushdown_aggregate(*input)),
            sort_exprs,
            hints,
        },

        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => PhysicalPlan::TopN {
            input: Box::new(apply_filter_pushdown_aggregate(*input)),
            sort_exprs,
            limit,
        },

        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => PhysicalPlan::Limit {
            input: Box::new(apply_filter_pushdown_aggregate(*input)),
            limit,
            offset,
        },

        PhysicalPlan::Distinct { input } => PhysicalPlan::Distinct {
            input: Box::new(apply_filter_pushdown_aggregate(*input)),
        },

        PhysicalPlan::Union {
            inputs,
            all,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::Union {
            inputs: inputs
                .into_iter()
                .map(apply_filter_pushdown_aggregate)
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
            left: Box::new(apply_filter_pushdown_aggregate(*left)),
            right: Box::new(apply_filter_pushdown_aggregate(*right)),
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
            left: Box::new(apply_filter_pushdown_aggregate(*left)),
            right: Box::new(apply_filter_pushdown_aggregate(*right)),
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
            input: Box::new(apply_filter_pushdown_aggregate(*input)),
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
            body: Box::new(apply_filter_pushdown_aggregate(*body)),
            parallel_ctes,
            hints,
        },

        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => PhysicalPlan::Unnest {
            input: Box::new(apply_filter_pushdown_aggregate(*input)),
            columns,
            schema,
        },

        PhysicalPlan::Qualify { input, predicate } => PhysicalPlan::Qualify {
            input: Box::new(apply_filter_pushdown_aggregate(*input)),
            predicate,
        },

        PhysicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => PhysicalPlan::Sample {
            input: Box::new(apply_filter_pushdown_aggregate(*input)),
            sample_type,
            sample_value,
        },

        PhysicalPlan::Insert {
            table_name,
            columns,
            source,
        } => PhysicalPlan::Insert {
            table_name,
            columns,
            source: Box::new(apply_filter_pushdown_aggregate(*source)),
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
            query: query.map(|q| Box::new(apply_filter_pushdown_aggregate(*q))),
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
            query: Box::new(apply_filter_pushdown_aggregate(*query)),
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
            source: Box::new(apply_filter_pushdown_aggregate(*source)),
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
            from: from.map(|f| Box::new(apply_filter_pushdown_aggregate(*f))),
            filter,
        },

        PhysicalPlan::ExportData { options, query } => PhysicalPlan::ExportData {
            options,
            query: Box::new(apply_filter_pushdown_aggregate(*query)),
        },

        PhysicalPlan::For {
            variable,
            query,
            body,
        } => PhysicalPlan::For {
            variable,
            query: Box::new(apply_filter_pushdown_aggregate(*query)),
            body: body
                .into_iter()
                .map(apply_filter_pushdown_aggregate)
                .collect(),
        },

        PhysicalPlan::If {
            condition,
            then_branch,
            else_branch,
        } => PhysicalPlan::If {
            condition,
            then_branch: then_branch
                .into_iter()
                .map(apply_filter_pushdown_aggregate)
                .collect(),
            else_branch: else_branch
                .map(|b| b.into_iter().map(apply_filter_pushdown_aggregate).collect()),
        },

        PhysicalPlan::While {
            condition,
            body,
            label,
        } => PhysicalPlan::While {
            condition,
            body: body
                .into_iter()
                .map(apply_filter_pushdown_aggregate)
                .collect(),
            label,
        },

        PhysicalPlan::Loop { body, label } => PhysicalPlan::Loop {
            body: body
                .into_iter()
                .map(apply_filter_pushdown_aggregate)
                .collect(),
            label,
        },

        PhysicalPlan::Block { body, label } => PhysicalPlan::Block {
            body: body
                .into_iter()
                .map(apply_filter_pushdown_aggregate)
                .collect(),
            label,
        },

        PhysicalPlan::Repeat {
            body,
            until_condition,
        } => PhysicalPlan::Repeat {
            body: body
                .into_iter()
                .map(apply_filter_pushdown_aggregate)
                .collect(),
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
            body: body
                .into_iter()
                .map(apply_filter_pushdown_aggregate)
                .collect(),
            or_replace,
            if_not_exists,
        },

        PhysicalPlan::TryCatch {
            try_block,
            catch_block,
        } => PhysicalPlan::TryCatch {
            try_block: try_block
                .into_iter()
                .map(|(p, sql)| (apply_filter_pushdown_aggregate(p), sql))
                .collect(),
            catch_block: catch_block
                .into_iter()
                .map(apply_filter_pushdown_aggregate)
                .collect(),
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
            input: Box::new(apply_filter_pushdown_aggregate(*input)),
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
            input: Box::new(apply_filter_pushdown_aggregate(*input)),
            analyze,
            logical_plan_text,
            physical_plan_text,
        },

        other => other,
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::DataType;
    use yachtsql_ir::{AggregateFunction, BinaryOp, Literal, PlanField, PlanSchema};

    use super::*;
    use crate::optimized_logical_plan::ExecutionHints;

    fn make_table_schema(table_name: &str, num_columns: usize) -> PlanSchema {
        let fields = (0..num_columns)
            .map(|i| PlanField::new(format!("col{}", i), DataType::Int64).with_table(table_name))
            .collect();
        PlanSchema::from_fields(fields)
    }

    fn make_scan(table_name: &str, num_columns: usize) -> PhysicalPlan {
        PhysicalPlan::TableScan {
            table_name: table_name.to_string(),
            schema: make_table_schema(table_name, num_columns),
            projection: None,
            row_count: None,
        }
    }

    fn make_aggregate_schema(group_by_count: usize, agg_count: usize) -> PlanSchema {
        let mut fields: Vec<PlanField> = (0..group_by_count)
            .map(|i| PlanField::new(format!("group{}", i), DataType::Int64))
            .collect();
        for i in 0..agg_count {
            fields.push(PlanField::new(format!("agg{}", i), DataType::Int64));
        }
        PlanSchema::from_fields(fields)
    }

    fn make_group_by_exprs(count: usize) -> Vec<Expr> {
        (0..count)
            .map(|i| Expr::Column {
                table: Some("t".to_string()),
                name: format!("col{}", i),
                index: Some(i),
            })
            .collect()
    }

    fn make_count_aggregate() -> Expr {
        Expr::Aggregate {
            func: AggregateFunction::Count,
            args: vec![Expr::Literal(Literal::Int64(1))],
            distinct: false,
            filter: None,
            order_by: vec![],
            limit: None,
            ignore_nulls: false,
        }
    }

    fn make_predicate_on_column(col_index: usize, value: i64) -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: None,
                name: format!("col{}", col_index),
                index: Some(col_index),
            }),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int64(value))),
        }
    }

    #[test]
    fn pushes_filter_on_group_by_column_below_aggregate() {
        let scan = make_scan("t", 3);
        let aggregate = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: make_group_by_exprs(2),
            aggregates: vec![make_count_aggregate()],
            schema: make_aggregate_schema(2, 1),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        };

        let predicate = make_predicate_on_column(0, 10);

        let plan = PhysicalPlan::Filter {
            input: Box::new(aggregate),
            predicate,
        };

        let result = apply_filter_pushdown_aggregate(plan);

        match result {
            PhysicalPlan::HashAggregate { input, .. } => match *input {
                PhysicalPlan::Filter {
                    input: inner_input,
                    predicate: pushed_pred,
                } => {
                    assert!(matches!(*inner_input, PhysicalPlan::TableScan { .. }));
                    match pushed_pred {
                        Expr::BinaryOp {
                            left,
                            op: BinaryOp::Gt,
                            right,
                        } => {
                            if let Expr::Column { index, .. } = *left {
                                assert_eq!(index, Some(0));
                            } else {
                                panic!("Expected Column expr");
                            }
                            assert_eq!(*right, Expr::Literal(Literal::Int64(10)));
                        }
                        _ => panic!("Expected BinaryOp"),
                    }
                }
                _ => panic!("Expected Filter below HashAggregate"),
            },
            _ => panic!("Expected HashAggregate at top"),
        }
    }

    #[test]
    fn keeps_filter_on_aggregate_result_above_aggregate() {
        let scan = make_scan("t", 3);
        let aggregate = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: make_group_by_exprs(2),
            aggregates: vec![make_count_aggregate()],
            schema: make_aggregate_schema(2, 1),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        };

        let predicate = make_predicate_on_column(2, 100);

        let plan = PhysicalPlan::Filter {
            input: Box::new(aggregate),
            predicate: predicate.clone(),
        };

        let result = apply_filter_pushdown_aggregate(plan);

        match result {
            PhysicalPlan::Filter {
                input,
                predicate: p,
                ..
            } => {
                assert_eq!(p, predicate);
                match *input {
                    PhysicalPlan::HashAggregate {
                        input: agg_input, ..
                    } => {
                        assert!(matches!(*agg_input, PhysicalPlan::TableScan { .. }));
                    }
                    _ => panic!("Expected HashAggregate below Filter"),
                }
            }
            _ => panic!("Expected Filter at top"),
        }
    }

    #[test]
    fn splits_mixed_predicates_pushing_group_by_predicates_only() {
        let scan = make_scan("t", 3);
        let aggregate = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: make_group_by_exprs(2),
            aggregates: vec![make_count_aggregate()],
            schema: make_aggregate_schema(2, 1),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        };

        let group_by_pred = make_predicate_on_column(0, 10);
        let agg_pred = make_predicate_on_column(2, 100);

        let combined_predicate = Expr::BinaryOp {
            left: Box::new(group_by_pred.clone()),
            op: BinaryOp::And,
            right: Box::new(agg_pred.clone()),
        };

        let plan = PhysicalPlan::Filter {
            input: Box::new(aggregate),
            predicate: combined_predicate,
        };

        let result = apply_filter_pushdown_aggregate(plan);

        match result {
            PhysicalPlan::Filter {
                input,
                predicate: remaining,
            } => {
                assert_eq!(remaining, agg_pred);

                match *input {
                    PhysicalPlan::HashAggregate {
                        input: agg_input, ..
                    } => match *agg_input {
                        PhysicalPlan::Filter {
                            input: scan_input,
                            predicate: pushed,
                        } => {
                            assert!(matches!(*scan_input, PhysicalPlan::TableScan { .. }));
                            match pushed {
                                Expr::BinaryOp {
                                    left,
                                    op: BinaryOp::Gt,
                                    ..
                                } => {
                                    if let Expr::Column { index, .. } = *left {
                                        assert_eq!(index, Some(0));
                                    } else {
                                        panic!("Expected Column");
                                    }
                                }
                                _ => panic!("Expected BinaryOp"),
                            }
                        }
                        _ => panic!("Expected Filter below HashAggregate"),
                    },
                    _ => panic!("Expected HashAggregate"),
                }
            }
            _ => panic!("Expected Filter at top"),
        }
    }

    #[test]
    fn handles_multiple_group_by_predicates() {
        let scan = make_scan("t", 4);
        let aggregate = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: make_group_by_exprs(3),
            aggregates: vec![make_count_aggregate()],
            schema: make_aggregate_schema(3, 1),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        };

        let pred0 = make_predicate_on_column(0, 10);
        let pred1 = make_predicate_on_column(1, 20);
        let pred2 = make_predicate_on_column(2, 30);

        let combined = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(pred0),
                op: BinaryOp::And,
                right: Box::new(pred1),
            }),
            op: BinaryOp::And,
            right: Box::new(pred2),
        };

        let plan = PhysicalPlan::Filter {
            input: Box::new(aggregate),
            predicate: combined,
        };

        let result = apply_filter_pushdown_aggregate(plan);

        match result {
            PhysicalPlan::HashAggregate { input, .. } => match *input {
                PhysicalPlan::Filter { input, predicate } => {
                    assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
                    let indices = collect_column_indices(&predicate);
                    assert!(indices.contains(&0) || indices.contains(&1) || indices.contains(&2));
                }
                _ => panic!("Expected Filter below HashAggregate"),
            },
            _ => panic!("Expected HashAggregate at top"),
        }
    }

    #[test]
    fn handles_filter_not_on_aggregate() {
        let scan = make_scan("t", 3);
        let predicate = make_predicate_on_column(0, 10);

        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: predicate.clone(),
        };

        let result = apply_filter_pushdown_aggregate(plan);

        match result {
            PhysicalPlan::Filter {
                input,
                predicate: p,
            } => {
                assert_eq!(p, predicate);
                assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
            }
            _ => panic!("Expected Filter at top"),
        }
    }

    #[test]
    fn handles_aggregate_with_no_group_by() {
        let scan = make_scan("t", 3);
        let aggregate = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: vec![],
            aggregates: vec![make_count_aggregate()],
            schema: make_aggregate_schema(0, 1),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        };

        let predicate = make_predicate_on_column(0, 100);

        let plan = PhysicalPlan::Filter {
            input: Box::new(aggregate),
            predicate: predicate.clone(),
        };

        let result = apply_filter_pushdown_aggregate(plan);

        match result {
            PhysicalPlan::Filter {
                input,
                predicate: p,
            } => {
                assert_eq!(p, predicate);
                match *input {
                    PhysicalPlan::HashAggregate { group_by, .. } => {
                        assert!(group_by.is_empty());
                    }
                    _ => panic!("Expected HashAggregate"),
                }
            }
            _ => panic!("Expected Filter at top"),
        }
    }

    #[test]
    fn pushes_second_group_by_column_predicate() {
        let scan = make_scan("t", 3);
        let aggregate = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: make_group_by_exprs(2),
            aggregates: vec![make_count_aggregate()],
            schema: make_aggregate_schema(2, 1),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        };

        let predicate = make_predicate_on_column(1, 50);

        let plan = PhysicalPlan::Filter {
            input: Box::new(aggregate),
            predicate,
        };

        let result = apply_filter_pushdown_aggregate(plan);

        match result {
            PhysicalPlan::HashAggregate { input, .. } => match *input {
                PhysicalPlan::Filter { predicate, .. } => match predicate {
                    Expr::BinaryOp { left, .. } => {
                        if let Expr::Column { index, .. } = *left {
                            assert_eq!(index, Some(1));
                        } else {
                            panic!("Expected Column");
                        }
                    }
                    _ => panic!("Expected BinaryOp"),
                },
                _ => panic!("Expected Filter below HashAggregate"),
            },
            _ => panic!("Expected HashAggregate at top"),
        }
    }

    #[test]
    fn recursively_optimizes_nested_plans() {
        let scan = make_scan("t", 3);
        let aggregate = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: make_group_by_exprs(2),
            aggregates: vec![make_count_aggregate()],
            schema: make_aggregate_schema(2, 1),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        };

        let filter = PhysicalPlan::Filter {
            input: Box::new(aggregate),
            predicate: make_predicate_on_column(0, 10),
        };

        let project = PhysicalPlan::Project {
            input: Box::new(filter),
            expressions: vec![Expr::Column {
                table: None,
                name: "group0".to_string(),
                index: Some(0),
            }],
            schema: make_aggregate_schema(1, 0),
        };

        let result = apply_filter_pushdown_aggregate(project);

        match result {
            PhysicalPlan::Project { input, .. } => match *input {
                PhysicalPlan::HashAggregate { input, .. } => match *input {
                    PhysicalPlan::Filter { input, .. } => {
                        assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
                    }
                    _ => panic!("Expected Filter below HashAggregate"),
                },
                _ => panic!("Expected HashAggregate"),
            },
            _ => panic!("Expected Project at top"),
        }
    }

    #[test]
    fn handles_or_predicate_mixing_group_and_agg_columns() {
        let scan = make_scan("t", 3);
        let aggregate = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: make_group_by_exprs(2),
            aggregates: vec![make_count_aggregate()],
            schema: make_aggregate_schema(2, 1),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        };

        let group_by_pred = make_predicate_on_column(0, 10);
        let agg_pred = make_predicate_on_column(2, 100);

        let or_predicate = Expr::BinaryOp {
            left: Box::new(group_by_pred),
            op: BinaryOp::Or,
            right: Box::new(agg_pred),
        };

        let plan = PhysicalPlan::Filter {
            input: Box::new(aggregate),
            predicate: or_predicate.clone(),
        };

        let result = apply_filter_pushdown_aggregate(plan);

        match result {
            PhysicalPlan::Filter { input, predicate } => {
                assert_eq!(predicate, or_predicate);
                match *input {
                    PhysicalPlan::HashAggregate {
                        input: agg_input, ..
                    } => {
                        assert!(matches!(*agg_input, PhysicalPlan::TableScan { .. }));
                    }
                    _ => panic!("Expected HashAggregate"),
                }
            }
            _ => panic!("Expected Filter at top"),
        }
    }

    #[test]
    fn handles_is_null_predicate_on_group_by_column() {
        let scan = make_scan("t", 3);
        let aggregate = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: make_group_by_exprs(2),
            aggregates: vec![make_count_aggregate()],
            schema: make_aggregate_schema(2, 1),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        };

        let predicate = Expr::IsNull {
            expr: Box::new(Expr::Column {
                table: None,
                name: "col0".to_string(),
                index: Some(0),
            }),
            negated: false,
        };

        let plan = PhysicalPlan::Filter {
            input: Box::new(aggregate),
            predicate,
        };

        let result = apply_filter_pushdown_aggregate(plan);

        match result {
            PhysicalPlan::HashAggregate { input, .. } => match *input {
                PhysicalPlan::Filter { predicate, .. } => {
                    assert!(matches!(predicate, Expr::IsNull { .. }));
                }
                _ => panic!("Expected Filter below HashAggregate"),
            },
            _ => panic!("Expected HashAggregate at top"),
        }
    }

    #[test]
    fn handles_in_list_predicate_on_group_by_column() {
        let scan = make_scan("t", 3);
        let aggregate = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: make_group_by_exprs(2),
            aggregates: vec![make_count_aggregate()],
            schema: make_aggregate_schema(2, 1),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        };

        let predicate = Expr::InList {
            expr: Box::new(Expr::Column {
                table: None,
                name: "col0".to_string(),
                index: Some(0),
            }),
            list: vec![
                Expr::Literal(Literal::Int64(1)),
                Expr::Literal(Literal::Int64(2)),
                Expr::Literal(Literal::Int64(3)),
            ],
            negated: false,
        };

        let plan = PhysicalPlan::Filter {
            input: Box::new(aggregate),
            predicate,
        };

        let result = apply_filter_pushdown_aggregate(plan);

        match result {
            PhysicalPlan::HashAggregate { input, .. } => match *input {
                PhysicalPlan::Filter { predicate, .. } => {
                    assert!(matches!(predicate, Expr::InList { .. }));
                }
                _ => panic!("Expected Filter below HashAggregate"),
            },
            _ => panic!("Expected HashAggregate at top"),
        }
    }

    #[test]
    fn handles_between_predicate_on_group_by_column() {
        let scan = make_scan("t", 3);
        let aggregate = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: make_group_by_exprs(2),
            aggregates: vec![make_count_aggregate()],
            schema: make_aggregate_schema(2, 1),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        };

        let predicate = Expr::Between {
            expr: Box::new(Expr::Column {
                table: None,
                name: "col0".to_string(),
                index: Some(0),
            }),
            low: Box::new(Expr::Literal(Literal::Int64(1))),
            high: Box::new(Expr::Literal(Literal::Int64(100))),
            negated: false,
        };

        let plan = PhysicalPlan::Filter {
            input: Box::new(aggregate),
            predicate,
        };

        let result = apply_filter_pushdown_aggregate(plan);

        match result {
            PhysicalPlan::HashAggregate { input, .. } => match *input {
                PhysicalPlan::Filter { predicate, .. } => {
                    assert!(matches!(predicate, Expr::Between { .. }));
                }
                _ => panic!("Expected Filter below HashAggregate"),
            },
            _ => panic!("Expected HashAggregate at top"),
        }
    }

    #[test]
    fn handles_like_predicate_on_group_by_column() {
        let scan = make_scan("t", 3);
        let aggregate = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: make_group_by_exprs(2),
            aggregates: vec![make_count_aggregate()],
            schema: make_aggregate_schema(2, 1),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        };

        let predicate = Expr::Like {
            expr: Box::new(Expr::Column {
                table: None,
                name: "col0".to_string(),
                index: Some(0),
            }),
            pattern: Box::new(Expr::Literal(Literal::String("test%".to_string()))),
            negated: false,
            case_insensitive: false,
        };

        let plan = PhysicalPlan::Filter {
            input: Box::new(aggregate),
            predicate,
        };

        let result = apply_filter_pushdown_aggregate(plan);

        match result {
            PhysicalPlan::HashAggregate { input, .. } => match *input {
                PhysicalPlan::Filter { predicate, .. } => {
                    assert!(matches!(predicate, Expr::Like { .. }));
                }
                _ => panic!("Expected Filter below HashAggregate"),
            },
            _ => panic!("Expected HashAggregate at top"),
        }
    }

    #[test]
    fn handles_cast_on_group_by_column() {
        let scan = make_scan("t", 3);
        let aggregate = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: make_group_by_exprs(2),
            aggregates: vec![make_count_aggregate()],
            schema: make_aggregate_schema(2, 1),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        };

        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::Cast {
                expr: Box::new(Expr::Column {
                    table: None,
                    name: "col0".to_string(),
                    index: Some(0),
                }),
                data_type: DataType::String,
                safe: false,
            }),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::String("10".to_string()))),
        };

        let plan = PhysicalPlan::Filter {
            input: Box::new(aggregate),
            predicate,
        };

        let result = apply_filter_pushdown_aggregate(plan);

        match result {
            PhysicalPlan::HashAggregate { input, .. } => match *input {
                PhysicalPlan::Filter { predicate, .. } => match predicate {
                    Expr::BinaryOp { left, .. } => {
                        assert!(matches!(*left, Expr::Cast { .. }));
                    }
                    _ => panic!("Expected BinaryOp"),
                },
                _ => panic!("Expected Filter below HashAggregate"),
            },
            _ => panic!("Expected HashAggregate at top"),
        }
    }

    #[test]
    fn handles_scalar_function_on_group_by_column() {
        let scan = make_scan("t", 3);
        let aggregate = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: make_group_by_exprs(2),
            aggregates: vec![make_count_aggregate()],
            schema: make_aggregate_schema(2, 1),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        };

        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::ScalarFunction {
                name: yachtsql_ir::ScalarFunction::Abs,
                args: vec![Expr::Column {
                    table: None,
                    name: "col0".to_string(),
                    index: Some(0),
                }],
            }),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int64(5))),
        };

        let plan = PhysicalPlan::Filter {
            input: Box::new(aggregate),
            predicate,
        };

        let result = apply_filter_pushdown_aggregate(plan);

        match result {
            PhysicalPlan::HashAggregate { input, .. } => match *input {
                PhysicalPlan::Filter { predicate, .. } => match predicate {
                    Expr::BinaryOp { left, .. } => {
                        assert!(matches!(*left, Expr::ScalarFunction { .. }));
                    }
                    _ => panic!("Expected BinaryOp"),
                },
                _ => panic!("Expected Filter below HashAggregate"),
            },
            _ => panic!("Expected HashAggregate at top"),
        }
    }

    #[test]
    fn test_expr_only_references_columns() {
        let expr_on_col_0 = make_predicate_on_column(0, 10);
        assert!(expr_only_references_columns(&expr_on_col_0, 2));
        assert!(expr_only_references_columns(&expr_on_col_0, 1));
        assert!(!expr_only_references_columns(&expr_on_col_0, 0));

        let expr_on_col_1 = make_predicate_on_column(1, 10);
        assert!(expr_only_references_columns(&expr_on_col_1, 2));
        assert!(!expr_only_references_columns(&expr_on_col_1, 1));

        let combined = Expr::BinaryOp {
            left: Box::new(expr_on_col_0.clone()),
            op: BinaryOp::And,
            right: Box::new(expr_on_col_1),
        };
        assert!(expr_only_references_columns(&combined, 2));
        assert!(!expr_only_references_columns(&combined, 1));

        let literal_only = Expr::Literal(Literal::Bool(true));
        assert!(expr_only_references_columns(&literal_only, 0));
    }

    #[test]
    fn test_partition_predicates_for_aggregate() {
        let pred0 = make_predicate_on_column(0, 10);
        let pred1 = make_predicate_on_column(1, 20);
        let pred2 = make_predicate_on_column(2, 30);

        let predicates = vec![pred0.clone(), pred1.clone(), pred2.clone()];
        let (pushable, non_pushable) = partition_predicates_for_aggregate(predicates, 2);

        assert_eq!(pushable.len(), 2);
        assert_eq!(non_pushable.len(), 1);
        assert!(pushable.contains(&pred0));
        assert!(pushable.contains(&pred1));
        assert!(non_pushable.contains(&pred2));
    }

    #[test]
    fn test_partition_predicates_empty() {
        let (pushable, non_pushable) = partition_predicates_for_aggregate(vec![], 2);
        assert!(pushable.is_empty());
        assert!(non_pushable.is_empty());
    }

    #[test]
    fn test_partition_predicates_all_pushable() {
        let pred0 = make_predicate_on_column(0, 10);
        let pred1 = make_predicate_on_column(1, 20);

        let predicates = vec![pred0.clone(), pred1.clone()];
        let (pushable, non_pushable) = partition_predicates_for_aggregate(predicates, 3);

        assert_eq!(pushable.len(), 2);
        assert!(non_pushable.is_empty());
    }

    #[test]
    fn test_partition_predicates_none_pushable() {
        let pred2 = make_predicate_on_column(2, 30);
        let pred3 = make_predicate_on_column(3, 40);

        let predicates = vec![pred2.clone(), pred3.clone()];
        let (pushable, non_pushable) = partition_predicates_for_aggregate(predicates, 2);

        assert!(pushable.is_empty());
        assert_eq!(non_pushable.len(), 2);
    }

    #[test]
    fn handles_grouping_sets() {
        let scan = make_scan("t", 3);
        let aggregate = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: make_group_by_exprs(2),
            aggregates: vec![make_count_aggregate()],
            schema: make_aggregate_schema(2, 1),
            grouping_sets: Some(vec![vec![0], vec![1], vec![0, 1]]),
            hints: ExecutionHints::default(),
        };

        let predicate = make_predicate_on_column(0, 10);

        let plan = PhysicalPlan::Filter {
            input: Box::new(aggregate),
            predicate,
        };

        let result = apply_filter_pushdown_aggregate(plan);

        match result {
            PhysicalPlan::HashAggregate {
                input,
                grouping_sets,
                ..
            } => {
                assert!(grouping_sets.is_some());
                match *input {
                    PhysicalPlan::Filter { .. } => {}
                    _ => panic!("Expected Filter below HashAggregate"),
                }
            }
            _ => panic!("Expected HashAggregate at top"),
        }
    }

    #[test]
    fn handles_deeply_nested_plan() {
        let scan = make_scan("t", 3);
        let aggregate1 = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: make_group_by_exprs(2),
            aggregates: vec![make_count_aggregate()],
            schema: make_aggregate_schema(2, 1),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        };

        let filter1 = PhysicalPlan::Filter {
            input: Box::new(aggregate1),
            predicate: make_predicate_on_column(0, 10),
        };

        let project = PhysicalPlan::Project {
            input: Box::new(filter1),
            expressions: vec![
                Expr::Column {
                    table: None,
                    name: "group0".to_string(),
                    index: Some(0),
                },
                Expr::Column {
                    table: None,
                    name: "agg0".to_string(),
                    index: Some(2),
                },
            ],
            schema: make_aggregate_schema(1, 1),
        };

        let aggregate2 = PhysicalPlan::HashAggregate {
            input: Box::new(project),
            group_by: vec![Expr::Column {
                table: None,
                name: "group0".to_string(),
                index: Some(0),
            }],
            aggregates: vec![make_count_aggregate()],
            schema: make_aggregate_schema(1, 1),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        };

        let filter2 = PhysicalPlan::Filter {
            input: Box::new(aggregate2),
            predicate: make_predicate_on_column(0, 5),
        };

        let result = apply_filter_pushdown_aggregate(filter2);

        fn count_filters(plan: &PhysicalPlan) -> usize {
            match plan {
                PhysicalPlan::Filter { input, .. } => 1 + count_filters(input),
                PhysicalPlan::HashAggregate { input, .. } => count_filters(input),
                PhysicalPlan::Project { input, .. } => count_filters(input),
                _ => 0,
            }
        }

        let filter_count = count_filters(&result);
        assert_eq!(filter_count, 2);
    }

    #[test]
    fn handles_unary_op_on_group_by_column() {
        let scan = make_scan("t", 3);
        let aggregate = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: make_group_by_exprs(2),
            aggregates: vec![make_count_aggregate()],
            schema: make_aggregate_schema(2, 1),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        };

        let predicate = Expr::UnaryOp {
            op: yachtsql_ir::UnaryOp::Not,
            expr: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column {
                    table: None,
                    name: "col0".to_string(),
                    index: Some(0),
                }),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Literal(Literal::Int64(0))),
            }),
        };

        let plan = PhysicalPlan::Filter {
            input: Box::new(aggregate),
            predicate,
        };

        let result = apply_filter_pushdown_aggregate(plan);

        match result {
            PhysicalPlan::HashAggregate { input, .. } => match *input {
                PhysicalPlan::Filter { predicate, .. } => {
                    assert!(matches!(predicate, Expr::UnaryOp { .. }));
                }
                _ => panic!("Expected Filter below HashAggregate"),
            },
            _ => panic!("Expected HashAggregate at top"),
        }
    }

    #[test]
    fn handles_all_predicates_on_agg_columns() {
        let scan = make_scan("t", 3);
        let aggregate = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: make_group_by_exprs(1),
            aggregates: vec![make_count_aggregate()],
            schema: make_aggregate_schema(1, 1),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        };

        let pred1 = make_predicate_on_column(1, 10);
        let pred2 = make_predicate_on_column(1, 20);
        let combined = Expr::BinaryOp {
            left: Box::new(pred1),
            op: BinaryOp::And,
            right: Box::new(pred2),
        };

        let plan = PhysicalPlan::Filter {
            input: Box::new(aggregate),
            predicate: combined.clone(),
        };

        let result = apply_filter_pushdown_aggregate(plan);

        match result {
            PhysicalPlan::Filter { input, predicate } => {
                assert_eq!(predicate, combined);
                match *input {
                    PhysicalPlan::HashAggregate {
                        input: agg_input, ..
                    } => {
                        assert!(matches!(*agg_input, PhysicalPlan::TableScan { .. }));
                    }
                    _ => panic!("Expected HashAggregate"),
                }
            }
            _ => panic!("Expected Filter at top"),
        }
    }
}
