#![coverage(off)]

use crate::PhysicalPlan;

pub fn apply_limit_pushdown(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let optimized_input = apply_limit_pushdown(*input);
            try_push_limit(optimized_input, limit, offset)
        }

        PhysicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(apply_limit_pushdown(*input)),
            predicate,
        },

        PhysicalPlan::Project {
            input,
            expressions,
            schema,
        } => PhysicalPlan::Project {
            input: Box::new(apply_limit_pushdown(*input)),
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
            input: Box::new(apply_limit_pushdown(*input)),
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
            left: Box::new(apply_limit_pushdown(*left)),
            right: Box::new(apply_limit_pushdown(*right)),
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
            left: Box::new(apply_limit_pushdown(*left)),
            right: Box::new(apply_limit_pushdown(*right)),
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
            left: Box::new(apply_limit_pushdown(*left)),
            right: Box::new(apply_limit_pushdown(*right)),
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::Sort {
            input,
            sort_exprs,
            hints,
        } => PhysicalPlan::Sort {
            input: Box::new(apply_limit_pushdown(*input)),
            sort_exprs,
            hints,
        },

        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => PhysicalPlan::TopN {
            input: Box::new(apply_limit_pushdown(*input)),
            sort_exprs,
            limit,
        },

        PhysicalPlan::Distinct { input } => PhysicalPlan::Distinct {
            input: Box::new(apply_limit_pushdown(*input)),
        },

        PhysicalPlan::Union {
            inputs,
            all,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::Union {
            inputs: inputs.into_iter().map(apply_limit_pushdown).collect(),
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
            left: Box::new(apply_limit_pushdown(*left)),
            right: Box::new(apply_limit_pushdown(*right)),
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
            left: Box::new(apply_limit_pushdown(*left)),
            right: Box::new(apply_limit_pushdown(*right)),
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
            input: Box::new(apply_limit_pushdown(*input)),
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
            body: Box::new(apply_limit_pushdown(*body)),
            parallel_ctes,
            hints,
        },

        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => PhysicalPlan::Unnest {
            input: Box::new(apply_limit_pushdown(*input)),
            columns,
            schema,
        },

        PhysicalPlan::Qualify { input, predicate } => PhysicalPlan::Qualify {
            input: Box::new(apply_limit_pushdown(*input)),
            predicate,
        },

        PhysicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => PhysicalPlan::Sample {
            input: Box::new(apply_limit_pushdown(*input)),
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
            source: Box::new(apply_limit_pushdown(*source)),
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
            query: query.map(|q| Box::new(apply_limit_pushdown(*q))),
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
            query: Box::new(apply_limit_pushdown(*query)),
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
            source: Box::new(apply_limit_pushdown(*source)),
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
            from: from.map(|f| Box::new(apply_limit_pushdown(*f))),
            filter,
        },

        PhysicalPlan::ExportData { options, query } => PhysicalPlan::ExportData {
            options,
            query: Box::new(apply_limit_pushdown(*query)),
        },

        PhysicalPlan::For {
            variable,
            query,
            body,
        } => PhysicalPlan::For {
            variable,
            query: Box::new(apply_limit_pushdown(*query)),
            body: body.into_iter().map(apply_limit_pushdown).collect(),
        },

        PhysicalPlan::If {
            condition,
            then_branch,
            else_branch,
        } => PhysicalPlan::If {
            condition,
            then_branch: then_branch.into_iter().map(apply_limit_pushdown).collect(),
            else_branch: else_branch.map(|b| b.into_iter().map(apply_limit_pushdown).collect()),
        },

        PhysicalPlan::While {
            condition,
            body,
            label,
        } => PhysicalPlan::While {
            condition,
            body: body.into_iter().map(apply_limit_pushdown).collect(),
            label,
        },

        PhysicalPlan::Loop { body, label } => PhysicalPlan::Loop {
            body: body.into_iter().map(apply_limit_pushdown).collect(),
            label,
        },

        PhysicalPlan::Block { body, label } => PhysicalPlan::Block {
            body: body.into_iter().map(apply_limit_pushdown).collect(),
            label,
        },

        PhysicalPlan::Repeat {
            body,
            until_condition,
        } => PhysicalPlan::Repeat {
            body: body.into_iter().map(apply_limit_pushdown).collect(),
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
            body: body.into_iter().map(apply_limit_pushdown).collect(),
            or_replace,
            if_not_exists,
        },

        PhysicalPlan::TryCatch {
            try_block,
            catch_block,
        } => PhysicalPlan::TryCatch {
            try_block: try_block
                .into_iter()
                .map(|(p, sql)| (apply_limit_pushdown(p), sql))
                .collect(),
            catch_block: catch_block.into_iter().map(apply_limit_pushdown).collect(),
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
            input: Box::new(apply_limit_pushdown(*input)),
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
            input: Box::new(apply_limit_pushdown(*input)),
            analyze,
            logical_plan_text,
            physical_plan_text,
        },

        other => other,
    }
}

fn try_push_limit(
    input: PhysicalPlan,
    limit: Option<usize>,
    offset: Option<usize>,
) -> PhysicalPlan {
    match input {
        PhysicalPlan::Project {
            input: proj_input,
            expressions,
            schema,
        } => PhysicalPlan::Project {
            input: Box::new(PhysicalPlan::Limit {
                input: proj_input,
                limit,
                offset,
            }),
            expressions,
            schema,
        },

        PhysicalPlan::Union {
            inputs,
            all: true,
            schema,
            parallel,
            hints,
        } => {
            let pushed_limit = compute_pushed_limit(limit, offset);
            let limited_inputs: Vec<PhysicalPlan> = inputs
                .into_iter()
                .map(|branch| PhysicalPlan::Limit {
                    input: Box::new(branch),
                    limit: pushed_limit,
                    offset: None,
                })
                .collect();

            PhysicalPlan::Limit {
                input: Box::new(PhysicalPlan::Union {
                    inputs: limited_inputs,
                    all: true,
                    schema,
                    parallel,
                    hints,
                }),
                limit,
                offset,
            }
        }

        _ => PhysicalPlan::Limit {
            input: Box::new(input),
            limit,
            offset,
        },
    }
}

fn compute_pushed_limit(limit: Option<usize>, offset: Option<usize>) -> Option<usize> {
    match (limit, offset) {
        (Some(l), Some(o)) => Some(l.saturating_add(o)),
        (Some(l), None) => Some(l),
        (None, Some(_)) => None,
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::DataType;
    use yachtsql_ir::{Expr, PlanField, PlanSchema, SortExpr};

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

    #[test]
    fn pushes_limit_through_union_all() {
        let scan_a = make_scan("a", 2);
        let scan_b = make_scan("b", 2);
        let scan_c = make_scan("c", 2);

        let union = PhysicalPlan::Union {
            inputs: vec![scan_a, scan_b, scan_c],
            all: true,
            schema: make_table_schema("union", 2),
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let plan = PhysicalPlan::Limit {
            input: Box::new(union),
            limit: Some(10),
            offset: None,
        };

        let result = apply_limit_pushdown(plan);

        match result {
            PhysicalPlan::Limit {
                input,
                limit: outer_limit,
                offset: outer_offset,
            } => {
                assert_eq!(outer_limit, Some(10));
                assert_eq!(outer_offset, None);

                match *input {
                    PhysicalPlan::Union { inputs, all, .. } => {
                        assert!(all);
                        assert_eq!(inputs.len(), 3);

                        for branch in inputs {
                            match branch {
                                PhysicalPlan::Limit {
                                    limit: branch_limit,
                                    offset: branch_offset,
                                    ..
                                } => {
                                    assert_eq!(branch_limit, Some(10));
                                    assert_eq!(branch_offset, None);
                                }
                                _ => panic!("Expected Limit on each union branch"),
                            }
                        }
                    }
                    _ => panic!("Expected Union inside outer Limit"),
                }
            }
            _ => panic!("Expected outer Limit"),
        }
    }

    #[test]
    fn pushes_limit_through_project() {
        let scan = make_scan("t", 3);
        let project = PhysicalPlan::Project {
            input: Box::new(scan),
            expressions: vec![Expr::Column {
                table: Some("t".to_string()),
                name: "col0".to_string(),
                index: Some(0),
            }],
            schema: make_table_schema("t", 1),
        };

        let plan = PhysicalPlan::Limit {
            input: Box::new(project),
            limit: Some(5),
            offset: None,
        };

        let result = apply_limit_pushdown(plan);

        match result {
            PhysicalPlan::Project { input, .. } => match *input {
                PhysicalPlan::Limit {
                    limit,
                    offset,
                    input: inner_input,
                } => {
                    assert_eq!(limit, Some(5));
                    assert_eq!(offset, None);
                    assert!(matches!(*inner_input, PhysicalPlan::TableScan { .. }));
                }
                _ => panic!("Expected Limit inside Project"),
            },
            _ => panic!("Expected Project at top"),
        }
    }

    #[test]
    fn handles_limit_with_offset_correctly() {
        let scan_a = make_scan("a", 2);
        let scan_b = make_scan("b", 2);

        let union = PhysicalPlan::Union {
            inputs: vec![scan_a, scan_b],
            all: true,
            schema: make_table_schema("union", 2),
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let plan = PhysicalPlan::Limit {
            input: Box::new(union),
            limit: Some(10),
            offset: Some(5),
        };

        let result = apply_limit_pushdown(plan);

        match result {
            PhysicalPlan::Limit {
                input,
                limit: outer_limit,
                offset: outer_offset,
            } => {
                assert_eq!(outer_limit, Some(10));
                assert_eq!(outer_offset, Some(5));

                match *input {
                    PhysicalPlan::Union { inputs, all, .. } => {
                        assert!(all);

                        for branch in inputs {
                            match branch {
                                PhysicalPlan::Limit {
                                    limit: branch_limit,
                                    offset: branch_offset,
                                    ..
                                } => {
                                    assert_eq!(branch_limit, Some(15));
                                    assert_eq!(branch_offset, None);
                                }
                                _ => panic!("Expected Limit on each union branch"),
                            }
                        }
                    }
                    _ => panic!("Expected Union inside outer Limit"),
                }
            }
            _ => panic!("Expected outer Limit"),
        }
    }

    #[test]
    fn does_not_push_through_sort() {
        let scan = make_scan("t", 2);
        let sort = PhysicalPlan::Sort {
            input: Box::new(scan),
            sort_exprs: vec![SortExpr {
                expr: Expr::Column {
                    table: Some("t".to_string()),
                    name: "col0".to_string(),
                    index: Some(0),
                },
                asc: true,
                nulls_first: false,
            }],
            hints: ExecutionHints::default(),
        };

        let plan = PhysicalPlan::Limit {
            input: Box::new(sort),
            limit: Some(10),
            offset: None,
        };

        let result = apply_limit_pushdown(plan);

        match result {
            PhysicalPlan::Limit { input, limit, .. } => {
                assert_eq!(limit, Some(10));
                assert!(matches!(*input, PhysicalPlan::Sort { .. }));
            }
            _ => panic!("Expected Limit at top (no pushdown through Sort)"),
        }
    }

    #[test]
    fn does_not_push_through_aggregate() {
        let scan = make_scan("t", 2);
        let aggregate = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: vec![Expr::Column {
                table: Some("t".to_string()),
                name: "col0".to_string(),
                index: Some(0),
            }],
            aggregates: vec![],
            schema: make_table_schema("agg", 1),
            grouping_sets: None,
            hints: ExecutionHints::default(),
        };

        let plan = PhysicalPlan::Limit {
            input: Box::new(aggregate),
            limit: Some(10),
            offset: None,
        };

        let result = apply_limit_pushdown(plan);

        match result {
            PhysicalPlan::Limit { input, limit, .. } => {
                assert_eq!(limit, Some(10));
                assert!(matches!(*input, PhysicalPlan::HashAggregate { .. }));
            }
            _ => panic!("Expected Limit at top (no pushdown through HashAggregate)"),
        }
    }

    #[test]
    fn does_not_push_through_distinct() {
        let scan = make_scan("t", 2);
        let distinct = PhysicalPlan::Distinct {
            input: Box::new(scan),
        };

        let plan = PhysicalPlan::Limit {
            input: Box::new(distinct),
            limit: Some(10),
            offset: None,
        };

        let result = apply_limit_pushdown(plan);

        match result {
            PhysicalPlan::Limit { input, limit, .. } => {
                assert_eq!(limit, Some(10));
                assert!(matches!(*input, PhysicalPlan::Distinct { .. }));
            }
            _ => panic!("Expected Limit at top (no pushdown through Distinct)"),
        }
    }

    #[test]
    fn does_not_push_through_union_distinct() {
        let scan_a = make_scan("a", 2);
        let scan_b = make_scan("b", 2);

        let union = PhysicalPlan::Union {
            inputs: vec![scan_a, scan_b],
            all: false,
            schema: make_table_schema("union", 2),
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let plan = PhysicalPlan::Limit {
            input: Box::new(union),
            limit: Some(10),
            offset: None,
        };

        let result = apply_limit_pushdown(plan);

        match result {
            PhysicalPlan::Limit { input, limit, .. } => {
                assert_eq!(limit, Some(10));
                match *input {
                    PhysicalPlan::Union { all, inputs, .. } => {
                        assert!(!all);
                        for branch in inputs {
                            assert!(
                                !matches!(branch, PhysicalPlan::Limit { .. }),
                                "Should not push limit into union distinct branches"
                            );
                        }
                    }
                    _ => panic!("Expected Union inside Limit"),
                }
            }
            _ => panic!("Expected Limit at top (no pushdown through Union DISTINCT)"),
        }
    }

    #[test]
    fn does_not_push_through_window() {
        let scan = make_scan("t", 2);
        let window = PhysicalPlan::Window {
            input: Box::new(scan),
            window_exprs: vec![],
            schema: make_table_schema("window", 2),
            hints: ExecutionHints::default(),
        };

        let plan = PhysicalPlan::Limit {
            input: Box::new(window),
            limit: Some(10),
            offset: None,
        };

        let result = apply_limit_pushdown(plan);

        match result {
            PhysicalPlan::Limit { input, limit, .. } => {
                assert_eq!(limit, Some(10));
                assert!(matches!(*input, PhysicalPlan::Window { .. }));
            }
            _ => panic!("Expected Limit at top (no pushdown through Window)"),
        }
    }

    #[test]
    fn does_not_push_through_joins() {
        let scan_a = make_scan("a", 2);
        let scan_b = make_scan("b", 2);

        let hash_join = PhysicalPlan::HashJoin {
            left: Box::new(scan_a),
            right: Box::new(scan_b),
            join_type: yachtsql_ir::JoinType::Inner,
            left_keys: vec![],
            right_keys: vec![],
            schema: make_table_schema("joined", 4),
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let plan = PhysicalPlan::Limit {
            input: Box::new(hash_join),
            limit: Some(10),
            offset: None,
        };

        let result = apply_limit_pushdown(plan);

        match result {
            PhysicalPlan::Limit { input, limit, .. } => {
                assert_eq!(limit, Some(10));
                assert!(matches!(*input, PhysicalPlan::HashJoin { .. }));
            }
            _ => panic!("Expected Limit at top (no pushdown through HashJoin)"),
        }
    }

    #[test]
    fn nested_limit_through_project_and_union_all() {
        let scan_a = make_scan("a", 2);
        let scan_b = make_scan("b", 2);

        let union = PhysicalPlan::Union {
            inputs: vec![scan_a, scan_b],
            all: true,
            schema: make_table_schema("union", 2),
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let project = PhysicalPlan::Project {
            input: Box::new(union),
            expressions: vec![Expr::Column {
                table: None,
                name: "col0".to_string(),
                index: Some(0),
            }],
            schema: make_table_schema("proj", 1),
        };

        let plan = PhysicalPlan::Limit {
            input: Box::new(project),
            limit: Some(10),
            offset: None,
        };

        let result = apply_limit_pushdown(plan);

        match result {
            PhysicalPlan::Project { input, .. } => match *input {
                PhysicalPlan::Limit {
                    input: inner,
                    limit,
                    ..
                } => {
                    assert_eq!(limit, Some(10));
                    assert!(matches!(*inner, PhysicalPlan::Union { .. }));
                }
                _ => panic!("Expected Limit inside Project"),
            },
            _ => panic!("Expected Project at top"),
        }
    }

    #[test]
    fn limit_with_none_limit_value() {
        let scan = make_scan("t", 2);
        let project = PhysicalPlan::Project {
            input: Box::new(scan),
            expressions: vec![],
            schema: make_table_schema("t", 0),
        };

        let plan = PhysicalPlan::Limit {
            input: Box::new(project),
            limit: None,
            offset: Some(5),
        };

        let result = apply_limit_pushdown(plan);

        match result {
            PhysicalPlan::Project { input, .. } => match *input {
                PhysicalPlan::Limit { limit, offset, .. } => {
                    assert_eq!(limit, None);
                    assert_eq!(offset, Some(5));
                }
                _ => panic!("Expected Limit inside Project"),
            },
            _ => panic!("Expected Project at top"),
        }
    }
}
