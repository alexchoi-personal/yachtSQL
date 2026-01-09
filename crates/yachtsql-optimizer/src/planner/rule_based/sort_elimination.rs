use yachtsql_ir::SortExpr;

use crate::PhysicalPlan;

fn sort_exprs_match(a: &[SortExpr], b: &[SortExpr]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|(x, y)| x.expr == y.expr && x.asc == y.asc && x.nulls_first == y.nulls_first)
}

pub fn apply_sort_elimination(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Sort {
            input,
            sort_exprs,
            hints,
        } => {
            let optimized_input = apply_sort_elimination(*input);

            match &optimized_input {
                PhysicalPlan::Sort {
                    sort_exprs: inner_sort_exprs,
                    ..
                } => {
                    if sort_exprs_match(&sort_exprs, inner_sort_exprs) {
                        optimized_input
                    } else {
                        PhysicalPlan::Sort {
                            input: Box::new(optimized_input),
                            sort_exprs,
                            hints,
                        }
                    }
                }
                PhysicalPlan::TopN {
                    sort_exprs: topn_sort_exprs,
                    ..
                } => {
                    if sort_exprs_match(&sort_exprs, topn_sort_exprs) {
                        optimized_input
                    } else {
                        PhysicalPlan::Sort {
                            input: Box::new(optimized_input),
                            sort_exprs,
                            hints,
                        }
                    }
                }
                _ => PhysicalPlan::Sort {
                    input: Box::new(optimized_input),
                    sort_exprs,
                    hints,
                },
            }
        }

        PhysicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(apply_sort_elimination(*input)),
            predicate,
        },

        PhysicalPlan::Project {
            input,
            expressions,
            schema,
        } => PhysicalPlan::Project {
            input: Box::new(apply_sort_elimination(*input)),
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
            input: Box::new(apply_sort_elimination(*input)),
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
            left: Box::new(apply_sort_elimination(*left)),
            right: Box::new(apply_sort_elimination(*right)),
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
            left: Box::new(apply_sort_elimination(*left)),
            right: Box::new(apply_sort_elimination(*right)),
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
            left: Box::new(apply_sort_elimination(*left)),
            right: Box::new(apply_sort_elimination(*right)),
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => PhysicalPlan::TopN {
            input: Box::new(apply_sort_elimination(*input)),
            sort_exprs,
            limit,
        },

        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => PhysicalPlan::Limit {
            input: Box::new(apply_sort_elimination(*input)),
            limit,
            offset,
        },

        PhysicalPlan::Distinct { input } => PhysicalPlan::Distinct {
            input: Box::new(apply_sort_elimination(*input)),
        },

        PhysicalPlan::Union {
            inputs,
            all,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::Union {
            inputs: inputs.into_iter().map(apply_sort_elimination).collect(),
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
            left: Box::new(apply_sort_elimination(*left)),
            right: Box::new(apply_sort_elimination(*right)),
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
            left: Box::new(apply_sort_elimination(*left)),
            right: Box::new(apply_sort_elimination(*right)),
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
            input: Box::new(apply_sort_elimination(*input)),
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
            body: Box::new(apply_sort_elimination(*body)),
            parallel_ctes,
            hints,
        },

        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => PhysicalPlan::Unnest {
            input: Box::new(apply_sort_elimination(*input)),
            columns,
            schema,
        },

        PhysicalPlan::Qualify { input, predicate } => PhysicalPlan::Qualify {
            input: Box::new(apply_sort_elimination(*input)),
            predicate,
        },

        PhysicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => PhysicalPlan::Sample {
            input: Box::new(apply_sort_elimination(*input)),
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
            source: Box::new(apply_sort_elimination(*source)),
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
            query: query.map(|q| Box::new(apply_sort_elimination(*q))),
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
            query: Box::new(apply_sort_elimination(*query)),
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
            source: Box::new(apply_sort_elimination(*source)),
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
            from: from.map(|f| Box::new(apply_sort_elimination(*f))),
            filter,
        },

        PhysicalPlan::ExportData { options, query } => PhysicalPlan::ExportData {
            options,
            query: Box::new(apply_sort_elimination(*query)),
        },

        PhysicalPlan::For {
            variable,
            query,
            body,
        } => PhysicalPlan::For {
            variable,
            query: Box::new(apply_sort_elimination(*query)),
            body: body.into_iter().map(apply_sort_elimination).collect(),
        },

        PhysicalPlan::If {
            condition,
            then_branch,
            else_branch,
        } => PhysicalPlan::If {
            condition,
            then_branch: then_branch
                .into_iter()
                .map(apply_sort_elimination)
                .collect(),
            else_branch: else_branch.map(|b| b.into_iter().map(apply_sort_elimination).collect()),
        },

        PhysicalPlan::While {
            condition,
            body,
            label,
        } => PhysicalPlan::While {
            condition,
            body: body.into_iter().map(apply_sort_elimination).collect(),
            label,
        },

        PhysicalPlan::Loop { body, label } => PhysicalPlan::Loop {
            body: body.into_iter().map(apply_sort_elimination).collect(),
            label,
        },

        PhysicalPlan::Block { body, label } => PhysicalPlan::Block {
            body: body.into_iter().map(apply_sort_elimination).collect(),
            label,
        },

        PhysicalPlan::Repeat {
            body,
            until_condition,
        } => PhysicalPlan::Repeat {
            body: body.into_iter().map(apply_sort_elimination).collect(),
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
            body: body.into_iter().map(apply_sort_elimination).collect(),
            or_replace,
            if_not_exists,
        },

        PhysicalPlan::TryCatch {
            try_block,
            catch_block,
        } => PhysicalPlan::TryCatch {
            try_block: try_block
                .into_iter()
                .map(|(p, sql)| (apply_sort_elimination(p), sql))
                .collect(),
            catch_block: catch_block
                .into_iter()
                .map(apply_sort_elimination)
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
            input: Box::new(apply_sort_elimination(*input)),
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
            input: Box::new(apply_sort_elimination(*input)),
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
    use yachtsql_ir::{Expr, PlanField, PlanSchema};

    use super::*;
    use crate::ExecutionHints;

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

    fn make_sort_expr(
        col_name: &str,
        table: &str,
        index: usize,
        asc: bool,
        nulls_first: bool,
    ) -> SortExpr {
        SortExpr {
            expr: Expr::Column {
                table: Some(table.to_string()),
                name: col_name.to_string(),
                index: Some(index),
            },
            asc,
            nulls_first,
        }
    }

    #[test]
    fn eliminates_redundant_sort_on_sort_same_order() {
        let scan = make_scan("t", 3);
        let sort_exprs = vec![make_sort_expr("col0", "t", 0, true, false)];

        let inner_sort = PhysicalPlan::Sort {
            input: Box::new(scan),
            sort_exprs: sort_exprs.clone(),
            hints: ExecutionHints::default(),
        };

        let outer_sort = PhysicalPlan::Sort {
            input: Box::new(inner_sort),
            sort_exprs: sort_exprs.clone(),
            hints: ExecutionHints::default(),
        };

        let result = apply_sort_elimination(outer_sort);

        match result {
            PhysicalPlan::Sort {
                input,
                sort_exprs: result_exprs,
                ..
            } => {
                assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
                assert_eq!(result_exprs.len(), 1);
            }
            _ => panic!("Expected Sort plan"),
        }
    }

    #[test]
    fn eliminates_sort_on_topn_same_order() {
        let scan = make_scan("t", 3);
        let sort_exprs = vec![make_sort_expr("col0", "t", 0, true, false)];

        let topn = PhysicalPlan::TopN {
            input: Box::new(scan),
            sort_exprs: sort_exprs.clone(),
            limit: 10,
        };

        let outer_sort = PhysicalPlan::Sort {
            input: Box::new(topn),
            sort_exprs: sort_exprs.clone(),
            hints: ExecutionHints::default(),
        };

        let result = apply_sort_elimination(outer_sort);

        match result {
            PhysicalPlan::TopN { input, limit, .. } => {
                assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
                assert_eq!(limit, 10);
            }
            _ => panic!("Expected TopN plan"),
        }
    }

    #[test]
    fn preserves_sort_on_sort_different_order() {
        let scan = make_scan("t", 3);
        let inner_sort_exprs = vec![make_sort_expr("col0", "t", 0, true, false)];
        let outer_sort_exprs = vec![make_sort_expr("col0", "t", 0, false, false)];

        let inner_sort = PhysicalPlan::Sort {
            input: Box::new(scan),
            sort_exprs: inner_sort_exprs,
            hints: ExecutionHints::default(),
        };

        let outer_sort = PhysicalPlan::Sort {
            input: Box::new(inner_sort),
            sort_exprs: outer_sort_exprs.clone(),
            hints: ExecutionHints::default(),
        };

        let result = apply_sort_elimination(outer_sort);

        match result {
            PhysicalPlan::Sort {
                input,
                sort_exprs: result_exprs,
                ..
            } => {
                assert!(matches!(*input, PhysicalPlan::Sort { .. }));
                assert!(!result_exprs[0].asc);
            }
            _ => panic!("Expected Sort plan"),
        }
    }

    #[test]
    fn preserves_sort_on_topn_different_order() {
        let scan = make_scan("t", 3);
        let topn_sort_exprs = vec![make_sort_expr("col0", "t", 0, true, false)];
        let outer_sort_exprs = vec![make_sort_expr("col1", "t", 1, true, false)];

        let topn = PhysicalPlan::TopN {
            input: Box::new(scan),
            sort_exprs: topn_sort_exprs,
            limit: 10,
        };

        let outer_sort = PhysicalPlan::Sort {
            input: Box::new(topn),
            sort_exprs: outer_sort_exprs.clone(),
            hints: ExecutionHints::default(),
        };

        let result = apply_sort_elimination(outer_sort);

        match result {
            PhysicalPlan::Sort {
                input,
                sort_exprs: result_exprs,
                ..
            } => {
                assert!(matches!(*input, PhysicalPlan::TopN { .. }));
                assert_eq!(result_exprs[0].expr, outer_sort_exprs[0].expr);
            }
            _ => panic!("Expected Sort plan"),
        }
    }

    #[test]
    fn handles_nested_sort_chains() {
        let scan = make_scan("t", 3);
        let sort_exprs_a = vec![make_sort_expr("col0", "t", 0, true, false)];
        let sort_exprs_b = vec![make_sort_expr("col1", "t", 1, true, false)];

        let sort1 = PhysicalPlan::Sort {
            input: Box::new(scan),
            sort_exprs: sort_exprs_a.clone(),
            hints: ExecutionHints::default(),
        };

        let sort2 = PhysicalPlan::Sort {
            input: Box::new(sort1),
            sort_exprs: sort_exprs_a.clone(),
            hints: ExecutionHints::default(),
        };

        let sort3 = PhysicalPlan::Sort {
            input: Box::new(sort2),
            sort_exprs: sort_exprs_b.clone(),
            hints: ExecutionHints::default(),
        };

        let sort4 = PhysicalPlan::Sort {
            input: Box::new(sort3),
            sort_exprs: sort_exprs_b.clone(),
            hints: ExecutionHints::default(),
        };

        let result = apply_sort_elimination(sort4);

        match &result {
            PhysicalPlan::Sort {
                input, sort_exprs, ..
            } => {
                assert_eq!(sort_exprs[0].expr, sort_exprs_b[0].expr);
                match input.as_ref() {
                    PhysicalPlan::Sort {
                        input: inner,
                        sort_exprs: inner_exprs,
                        ..
                    } => {
                        assert_eq!(inner_exprs[0].expr, sort_exprs_a[0].expr);
                        assert!(matches!(inner.as_ref(), PhysicalPlan::TableScan { .. }));
                    }
                    _ => panic!("Expected inner Sort plan"),
                }
            }
            _ => panic!("Expected outer Sort plan"),
        }
    }

    #[test]
    fn preserves_sort_on_sort_different_nulls_first() {
        let scan = make_scan("t", 3);
        let inner_sort_exprs = vec![make_sort_expr("col0", "t", 0, true, false)];
        let outer_sort_exprs = vec![make_sort_expr("col0", "t", 0, true, true)];

        let inner_sort = PhysicalPlan::Sort {
            input: Box::new(scan),
            sort_exprs: inner_sort_exprs,
            hints: ExecutionHints::default(),
        };

        let outer_sort = PhysicalPlan::Sort {
            input: Box::new(inner_sort),
            sort_exprs: outer_sort_exprs.clone(),
            hints: ExecutionHints::default(),
        };

        let result = apply_sort_elimination(outer_sort);

        match result {
            PhysicalPlan::Sort {
                input,
                sort_exprs: result_exprs,
                ..
            } => {
                assert!(matches!(*input, PhysicalPlan::Sort { .. }));
                assert!(result_exprs[0].nulls_first);
            }
            _ => panic!("Expected Sort plan"),
        }
    }

    #[test]
    fn preserves_sort_on_sort_different_length() {
        let scan = make_scan("t", 3);
        let inner_sort_exprs = vec![make_sort_expr("col0", "t", 0, true, false)];
        let outer_sort_exprs = vec![
            make_sort_expr("col0", "t", 0, true, false),
            make_sort_expr("col1", "t", 1, true, false),
        ];

        let inner_sort = PhysicalPlan::Sort {
            input: Box::new(scan),
            sort_exprs: inner_sort_exprs,
            hints: ExecutionHints::default(),
        };

        let outer_sort = PhysicalPlan::Sort {
            input: Box::new(inner_sort),
            sort_exprs: outer_sort_exprs.clone(),
            hints: ExecutionHints::default(),
        };

        let result = apply_sort_elimination(outer_sort);

        match result {
            PhysicalPlan::Sort {
                input,
                sort_exprs: result_exprs,
                ..
            } => {
                assert!(matches!(*input, PhysicalPlan::Sort { .. }));
                assert_eq!(result_exprs.len(), 2);
            }
            _ => panic!("Expected Sort plan"),
        }
    }

    #[test]
    fn traverses_through_project() {
        let scan = make_scan("t", 3);
        let sort_exprs = vec![make_sort_expr("col0", "t", 0, true, false)];

        let inner_sort = PhysicalPlan::Sort {
            input: Box::new(scan.clone()),
            sort_exprs: sort_exprs.clone(),
            hints: ExecutionHints::default(),
        };

        let outer_sort = PhysicalPlan::Sort {
            input: Box::new(inner_sort),
            sort_exprs: sort_exprs.clone(),
            hints: ExecutionHints::default(),
        };

        let project = PhysicalPlan::Project {
            input: Box::new(outer_sort),
            expressions: vec![Expr::Column {
                table: Some("t".to_string()),
                name: "col0".to_string(),
                index: Some(0),
            }],
            schema: make_table_schema("t", 1),
        };

        let result = apply_sort_elimination(project);

        match result {
            PhysicalPlan::Project { input, .. } => match *input {
                PhysicalPlan::Sort {
                    input: sort_input, ..
                } => {
                    assert!(matches!(*sort_input, PhysicalPlan::TableScan { .. }));
                }
                _ => panic!("Expected Sort inside Project"),
            },
            _ => panic!("Expected Project plan"),
        }
    }

    #[test]
    fn sort_exprs_match_empty() {
        assert!(sort_exprs_match(&[], &[]));
    }

    #[test]
    fn sort_exprs_match_single_identical() {
        let exprs = vec![make_sort_expr("col0", "t", 0, true, false)];
        assert!(sort_exprs_match(&exprs, &exprs));
    }

    #[test]
    fn sort_exprs_match_different_expr() {
        let a = vec![make_sort_expr("col0", "t", 0, true, false)];
        let b = vec![make_sort_expr("col1", "t", 1, true, false)];
        assert!(!sort_exprs_match(&a, &b));
    }

    #[test]
    fn sort_exprs_match_different_asc() {
        let a = vec![make_sort_expr("col0", "t", 0, true, false)];
        let b = vec![make_sort_expr("col0", "t", 0, false, false)];
        assert!(!sort_exprs_match(&a, &b));
    }

    #[test]
    fn sort_exprs_match_different_nulls_first() {
        let a = vec![make_sort_expr("col0", "t", 0, true, false)];
        let b = vec![make_sort_expr("col0", "t", 0, true, true)];
        assert!(!sort_exprs_match(&a, &b));
    }

    #[test]
    fn sort_exprs_match_different_length() {
        let a = vec![make_sort_expr("col0", "t", 0, true, false)];
        let b = vec![
            make_sort_expr("col0", "t", 0, true, false),
            make_sort_expr("col1", "t", 1, true, false),
        ];
        assert!(!sort_exprs_match(&a, &b));
    }
}
