#![coverage(off)]

use crate::PhysicalPlan;

pub fn apply_topn_pushdown(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => {
            let optimized_input = apply_topn_pushdown(*input);
            try_push_topn(optimized_input, sort_exprs, limit)
        }

        PhysicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(apply_topn_pushdown(*input)),
            predicate,
        },

        PhysicalPlan::Project {
            input,
            expressions,
            schema,
        } => PhysicalPlan::Project {
            input: Box::new(apply_topn_pushdown(*input)),
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
            input: Box::new(apply_topn_pushdown(*input)),
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
            left: Box::new(apply_topn_pushdown(*left)),
            right: Box::new(apply_topn_pushdown(*right)),
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
            left: Box::new(apply_topn_pushdown(*left)),
            right: Box::new(apply_topn_pushdown(*right)),
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
            left: Box::new(apply_topn_pushdown(*left)),
            right: Box::new(apply_topn_pushdown(*right)),
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::Sort {
            input,
            sort_exprs,
            hints,
        } => PhysicalPlan::Sort {
            input: Box::new(apply_topn_pushdown(*input)),
            sort_exprs,
            hints,
        },

        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => PhysicalPlan::Limit {
            input: Box::new(apply_topn_pushdown(*input)),
            limit,
            offset,
        },

        PhysicalPlan::Distinct { input } => PhysicalPlan::Distinct {
            input: Box::new(apply_topn_pushdown(*input)),
        },

        PhysicalPlan::Union {
            inputs,
            all,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::Union {
            inputs: inputs.into_iter().map(apply_topn_pushdown).collect(),
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
            left: Box::new(apply_topn_pushdown(*left)),
            right: Box::new(apply_topn_pushdown(*right)),
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
            left: Box::new(apply_topn_pushdown(*left)),
            right: Box::new(apply_topn_pushdown(*right)),
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
            input: Box::new(apply_topn_pushdown(*input)),
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
            body: Box::new(apply_topn_pushdown(*body)),
            parallel_ctes,
            hints,
        },

        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => PhysicalPlan::Unnest {
            input: Box::new(apply_topn_pushdown(*input)),
            columns,
            schema,
        },

        PhysicalPlan::Qualify { input, predicate } => PhysicalPlan::Qualify {
            input: Box::new(apply_topn_pushdown(*input)),
            predicate,
        },

        PhysicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => PhysicalPlan::Sample {
            input: Box::new(apply_topn_pushdown(*input)),
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
            source: Box::new(apply_topn_pushdown(*source)),
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
            query: query.map(|q| Box::new(apply_topn_pushdown(*q))),
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
            query: Box::new(apply_topn_pushdown(*query)),
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
            source: Box::new(apply_topn_pushdown(*source)),
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
            from: from.map(|f| Box::new(apply_topn_pushdown(*f))),
            filter,
        },

        PhysicalPlan::ExportData { options, query } => PhysicalPlan::ExportData {
            options,
            query: Box::new(apply_topn_pushdown(*query)),
        },

        PhysicalPlan::For {
            variable,
            query,
            body,
        } => PhysicalPlan::For {
            variable,
            query: Box::new(apply_topn_pushdown(*query)),
            body: body.into_iter().map(apply_topn_pushdown).collect(),
        },

        PhysicalPlan::If {
            condition,
            then_branch,
            else_branch,
        } => PhysicalPlan::If {
            condition,
            then_branch: then_branch.into_iter().map(apply_topn_pushdown).collect(),
            else_branch: else_branch.map(|b| b.into_iter().map(apply_topn_pushdown).collect()),
        },

        PhysicalPlan::While {
            condition,
            body,
            label,
        } => PhysicalPlan::While {
            condition,
            body: body.into_iter().map(apply_topn_pushdown).collect(),
            label,
        },

        PhysicalPlan::Loop { body, label } => PhysicalPlan::Loop {
            body: body.into_iter().map(apply_topn_pushdown).collect(),
            label,
        },

        PhysicalPlan::Block { body, label } => PhysicalPlan::Block {
            body: body.into_iter().map(apply_topn_pushdown).collect(),
            label,
        },

        PhysicalPlan::Repeat {
            body,
            until_condition,
        } => PhysicalPlan::Repeat {
            body: body.into_iter().map(apply_topn_pushdown).collect(),
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
            body: body.into_iter().map(apply_topn_pushdown).collect(),
            or_replace,
            if_not_exists,
        },

        PhysicalPlan::TryCatch {
            try_block,
            catch_block,
        } => PhysicalPlan::TryCatch {
            try_block: try_block
                .into_iter()
                .map(|(p, sql)| (apply_topn_pushdown(p), sql))
                .collect(),
            catch_block: catch_block.into_iter().map(apply_topn_pushdown).collect(),
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
            input: Box::new(apply_topn_pushdown(*input)),
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
            input: Box::new(apply_topn_pushdown(*input)),
            analyze,
            logical_plan_text,
            physical_plan_text,
        },

        PhysicalPlan::TableScan {
            table_name,
            schema,
            projection,
            row_count,
        } => PhysicalPlan::TableScan {
            table_name,
            schema,
            projection,
            row_count,
        },

        PhysicalPlan::Values { values, schema } => PhysicalPlan::Values { values, schema },

        PhysicalPlan::Empty { schema } => PhysicalPlan::Empty { schema },

        PhysicalPlan::Delete {
            table_name,
            alias,
            filter,
        } => PhysicalPlan::Delete {
            table_name,
            alias,
            filter,
        },

        PhysicalPlan::DropTable {
            table_names,
            if_exists,
        } => PhysicalPlan::DropTable {
            table_names,
            if_exists,
        },

        PhysicalPlan::AlterTable {
            table_name,
            operation,
            if_exists,
        } => PhysicalPlan::AlterTable {
            table_name,
            operation,
            if_exists,
        },

        PhysicalPlan::Truncate { table_name } => PhysicalPlan::Truncate { table_name },

        PhysicalPlan::DropView { name, if_exists } => PhysicalPlan::DropView { name, if_exists },

        PhysicalPlan::CreateSchema {
            name,
            if_not_exists,
            or_replace,
        } => PhysicalPlan::CreateSchema {
            name,
            if_not_exists,
            or_replace,
        },

        PhysicalPlan::DropSchema {
            name,
            if_exists,
            cascade,
        } => PhysicalPlan::DropSchema {
            name,
            if_exists,
            cascade,
        },

        PhysicalPlan::UndropSchema {
            name,
            if_not_exists,
        } => PhysicalPlan::UndropSchema {
            name,
            if_not_exists,
        },

        PhysicalPlan::AlterSchema { name, options } => PhysicalPlan::AlterSchema { name, options },

        PhysicalPlan::CreateFunction {
            name,
            args,
            return_type,
            body,
            or_replace,
            if_not_exists,
            is_temp,
            is_aggregate,
        } => PhysicalPlan::CreateFunction {
            name,
            args,
            return_type,
            body,
            or_replace,
            if_not_exists,
            is_temp,
            is_aggregate,
        },

        PhysicalPlan::DropFunction { name, if_exists } => {
            PhysicalPlan::DropFunction { name, if_exists }
        }

        PhysicalPlan::DropProcedure { name, if_exists } => {
            PhysicalPlan::DropProcedure { name, if_exists }
        }

        PhysicalPlan::Call {
            procedure_name,
            args,
        } => PhysicalPlan::Call {
            procedure_name,
            args,
        },

        PhysicalPlan::LoadData {
            table_name,
            options,
            temp_table,
            temp_schema,
        } => PhysicalPlan::LoadData {
            table_name,
            options,
            temp_table,
            temp_schema,
        },

        PhysicalPlan::Declare {
            name,
            data_type,
            default,
        } => PhysicalPlan::Declare {
            name,
            data_type,
            default,
        },

        PhysicalPlan::SetVariable { name, value } => PhysicalPlan::SetVariable { name, value },

        PhysicalPlan::SetMultipleVariables { names, value } => {
            PhysicalPlan::SetMultipleVariables { names, value }
        }

        PhysicalPlan::Return { value } => PhysicalPlan::Return { value },

        PhysicalPlan::Raise { message, level } => PhysicalPlan::Raise { message, level },

        PhysicalPlan::ExecuteImmediate {
            sql_expr,
            into_variables,
            using_params,
        } => PhysicalPlan::ExecuteImmediate {
            sql_expr,
            into_variables,
            using_params,
        },

        PhysicalPlan::Break { label } => PhysicalPlan::Break { label },

        PhysicalPlan::Continue { label } => PhysicalPlan::Continue { label },

        PhysicalPlan::CreateSnapshot {
            snapshot_name,
            source_name,
            if_not_exists,
        } => PhysicalPlan::CreateSnapshot {
            snapshot_name,
            source_name,
            if_not_exists,
        },

        PhysicalPlan::DropSnapshot {
            snapshot_name,
            if_exists,
        } => PhysicalPlan::DropSnapshot {
            snapshot_name,
            if_exists,
        },

        PhysicalPlan::Assert { condition, message } => PhysicalPlan::Assert { condition, message },

        PhysicalPlan::Grant {
            roles,
            resource_type,
            resource_name,
            grantees,
        } => PhysicalPlan::Grant {
            roles,
            resource_type,
            resource_name,
            grantees,
        },

        PhysicalPlan::Revoke {
            roles,
            resource_type,
            resource_name,
            grantees,
        } => PhysicalPlan::Revoke {
            roles,
            resource_type,
            resource_name,
            grantees,
        },

        PhysicalPlan::BeginTransaction => PhysicalPlan::BeginTransaction,

        PhysicalPlan::Commit => PhysicalPlan::Commit,

        PhysicalPlan::Rollback => PhysicalPlan::Rollback,
    }
}

use yachtsql_ir::SortExpr;

fn try_push_topn(input: PhysicalPlan, sort_exprs: Vec<SortExpr>, limit: usize) -> PhysicalPlan {
    match input {
        PhysicalPlan::Union {
            inputs,
            all: true,
            schema,
            parallel,
            hints,
        } => {
            let topn_inputs: Vec<PhysicalPlan> = inputs
                .into_iter()
                .map(|branch| PhysicalPlan::TopN {
                    input: Box::new(branch),
                    sort_exprs: sort_exprs.clone(),
                    limit,
                })
                .collect();

            PhysicalPlan::TopN {
                input: Box::new(PhysicalPlan::Union {
                    inputs: topn_inputs,
                    all: true,
                    schema,
                    parallel,
                    hints,
                }),
                sort_exprs,
                limit,
            }
        }

        _ => PhysicalPlan::TopN {
            input: Box::new(input),
            sort_exprs,
            limit,
        },
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::DataType;
    use yachtsql_ir::{Expr, Literal, PlanField, PlanSchema, SortExpr};

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

    fn make_sort_exprs() -> Vec<SortExpr> {
        vec![SortExpr {
            expr: Expr::Column {
                table: Some("t".to_string()),
                name: "col0".to_string(),
                index: Some(0),
            },
            asc: true,
            nulls_first: false,
        }]
    }

    #[test]
    fn pushes_topn_through_union_all() {
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

        let plan = PhysicalPlan::TopN {
            input: Box::new(union),
            sort_exprs: make_sort_exprs(),
            limit: 10,
        };

        let result = apply_topn_pushdown(plan);

        match result {
            PhysicalPlan::TopN {
                input,
                limit: outer_limit,
                sort_exprs: outer_sort,
            } => {
                assert_eq!(outer_limit, 10);
                assert_eq!(outer_sort.len(), 1);

                match *input {
                    PhysicalPlan::Union { inputs, all, .. } => {
                        assert!(all);
                        assert_eq!(inputs.len(), 3);

                        for branch in inputs {
                            match branch {
                                PhysicalPlan::TopN {
                                    limit: branch_limit,
                                    sort_exprs: branch_sort,
                                    ..
                                } => {
                                    assert_eq!(branch_limit, 10);
                                    assert_eq!(branch_sort.len(), 1);
                                }
                                _ => panic!("Expected TopN on each union branch"),
                            }
                        }
                    }
                    _ => panic!("Expected Union inside outer TopN"),
                }
            }
            _ => panic!("Expected outer TopN"),
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

        let plan = PhysicalPlan::TopN {
            input: Box::new(union),
            sort_exprs: make_sort_exprs(),
            limit: 10,
        };

        let result = apply_topn_pushdown(plan);

        match result {
            PhysicalPlan::TopN { input, limit, .. } => {
                assert_eq!(limit, 10);
                match *input {
                    PhysicalPlan::Union { all, inputs, .. } => {
                        assert!(!all);
                        for branch in inputs {
                            assert!(
                                !matches!(branch, PhysicalPlan::TopN { .. }),
                                "Should not push TopN into union distinct branches"
                            );
                        }
                    }
                    _ => panic!("Expected Union inside TopN"),
                }
            }
            _ => panic!("Expected TopN at top (no pushdown through Union DISTINCT)"),
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

        let plan = PhysicalPlan::TopN {
            input: Box::new(aggregate),
            sort_exprs: make_sort_exprs(),
            limit: 10,
        };

        let result = apply_topn_pushdown(plan);

        match result {
            PhysicalPlan::TopN { input, limit, .. } => {
                assert_eq!(limit, 10);
                assert!(matches!(*input, PhysicalPlan::HashAggregate { .. }));
            }
            _ => panic!("Expected TopN at top (no pushdown through HashAggregate)"),
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

        let plan = PhysicalPlan::TopN {
            input: Box::new(window),
            sort_exprs: make_sort_exprs(),
            limit: 10,
        };

        let result = apply_topn_pushdown(plan);

        match result {
            PhysicalPlan::TopN { input, limit, .. } => {
                assert_eq!(limit, 10);
                assert!(matches!(*input, PhysicalPlan::Window { .. }));
            }
            _ => panic!("Expected TopN at top (no pushdown through Window)"),
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

        let plan = PhysicalPlan::TopN {
            input: Box::new(hash_join),
            sort_exprs: make_sort_exprs(),
            limit: 10,
        };

        let result = apply_topn_pushdown(plan);

        match result {
            PhysicalPlan::TopN { input, limit, .. } => {
                assert_eq!(limit, 10);
                assert!(matches!(*input, PhysicalPlan::HashJoin { .. }));
            }
            _ => panic!("Expected TopN at top (no pushdown through HashJoin)"),
        }
    }

    #[test]
    fn does_not_push_through_distinct() {
        let scan = make_scan("t", 2);
        let distinct = PhysicalPlan::Distinct {
            input: Box::new(scan),
        };

        let plan = PhysicalPlan::TopN {
            input: Box::new(distinct),
            sort_exprs: make_sort_exprs(),
            limit: 10,
        };

        let result = apply_topn_pushdown(plan);

        match result {
            PhysicalPlan::TopN { input, limit, .. } => {
                assert_eq!(limit, 10);
                assert!(matches!(*input, PhysicalPlan::Distinct { .. }));
            }
            _ => panic!("Expected TopN at top (no pushdown through Distinct)"),
        }
    }

    #[test]
    fn handles_nested_unions() {
        let scan_a = make_scan("a", 2);
        let scan_b = make_scan("b", 2);
        let scan_c = make_scan("c", 2);
        let scan_d = make_scan("d", 2);

        let inner_union = PhysicalPlan::Union {
            inputs: vec![scan_c, scan_d],
            all: true,
            schema: make_table_schema("inner_union", 2),
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let outer_union = PhysicalPlan::Union {
            inputs: vec![scan_a, scan_b, inner_union],
            all: true,
            schema: make_table_schema("outer_union", 2),
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let plan = PhysicalPlan::TopN {
            input: Box::new(outer_union),
            sort_exprs: make_sort_exprs(),
            limit: 5,
        };

        let result = apply_topn_pushdown(plan);

        match result {
            PhysicalPlan::TopN {
                input,
                limit: outer_limit,
                ..
            } => {
                assert_eq!(outer_limit, 5);

                match *input {
                    PhysicalPlan::Union { inputs, all, .. } => {
                        assert!(all);
                        assert_eq!(inputs.len(), 3);

                        for branch in &inputs {
                            assert!(matches!(branch, PhysicalPlan::TopN { limit: 5, .. }));
                        }
                    }
                    _ => panic!("Expected Union inside outer TopN"),
                }
            }
            _ => panic!("Expected outer TopN"),
        }
    }

    #[test]
    fn preserves_sort_expressions() {
        let scan_a = make_scan("a", 2);
        let scan_b = make_scan("b", 2);

        let union = PhysicalPlan::Union {
            inputs: vec![scan_a, scan_b],
            all: true,
            schema: make_table_schema("union", 2),
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let sort_exprs = vec![
            SortExpr {
                expr: Expr::Column {
                    table: Some("t".to_string()),
                    name: "col0".to_string(),
                    index: Some(0),
                },
                asc: false,
                nulls_first: true,
            },
            SortExpr {
                expr: Expr::Column {
                    table: Some("t".to_string()),
                    name: "col1".to_string(),
                    index: Some(1),
                },
                asc: true,
                nulls_first: false,
            },
        ];

        let plan = PhysicalPlan::TopN {
            input: Box::new(union),
            sort_exprs: sort_exprs.clone(),
            limit: 10,
        };

        let result = apply_topn_pushdown(plan);

        match result {
            PhysicalPlan::TopN {
                input,
                sort_exprs: outer_sort,
                ..
            } => {
                assert_eq!(outer_sort.len(), 2);
                assert!(!outer_sort[0].asc);
                assert!(outer_sort[0].nulls_first);
                assert!(outer_sort[1].asc);
                assert!(!outer_sort[1].nulls_first);

                match *input {
                    PhysicalPlan::Union { inputs, .. } => {
                        for branch in inputs {
                            match branch {
                                PhysicalPlan::TopN {
                                    sort_exprs: branch_sort,
                                    ..
                                } => {
                                    assert_eq!(branch_sort.len(), 2);
                                    assert!(!branch_sort[0].asc);
                                    assert!(branch_sort[0].nulls_first);
                                    assert!(branch_sort[1].asc);
                                    assert!(!branch_sort[1].nulls_first);
                                }
                                _ => panic!("Expected TopN on each branch"),
                            }
                        }
                    }
                    _ => panic!("Expected Union"),
                }
            }
            _ => panic!("Expected TopN"),
        }
    }

    #[test]
    fn passthrough_for_simple_scan() {
        let scan = make_scan("t", 2);
        let plan = PhysicalPlan::TopN {
            input: Box::new(scan),
            sort_exprs: make_sort_exprs(),
            limit: 10,
        };

        let result = apply_topn_pushdown(plan);

        match result {
            PhysicalPlan::TopN { input, limit, .. } => {
                assert_eq!(limit, 10);
                assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
            }
            _ => panic!("Expected TopN with TableScan input"),
        }
    }

    #[test]
    fn recursively_applies_to_subplans() {
        let scan_a = make_scan("a", 2);
        let scan_b = make_scan("b", 2);

        let inner_union = PhysicalPlan::Union {
            inputs: vec![scan_a, scan_b],
            all: true,
            schema: make_table_schema("union", 2),
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let inner_topn = PhysicalPlan::TopN {
            input: Box::new(inner_union),
            sort_exprs: make_sort_exprs(),
            limit: 5,
        };

        let filter = PhysicalPlan::Filter {
            input: Box::new(inner_topn),
            predicate: Expr::Literal(Literal::Bool(true)),
        };

        let result = apply_topn_pushdown(filter);

        match result {
            PhysicalPlan::Filter { input, .. } => match *input {
                PhysicalPlan::TopN {
                    input: topn_input, ..
                } => match *topn_input {
                    PhysicalPlan::Union { inputs, .. } => {
                        for branch in inputs {
                            assert!(matches!(branch, PhysicalPlan::TopN { .. }));
                        }
                    }
                    _ => panic!("Expected Union inside inner TopN"),
                },
                _ => panic!("Expected TopN inside Filter"),
            },
            _ => panic!("Expected Filter at top"),
        }
    }

    #[test]
    fn handles_single_branch_union() {
        let scan = make_scan("a", 2);

        let union = PhysicalPlan::Union {
            inputs: vec![scan],
            all: true,
            schema: make_table_schema("union", 2),
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let plan = PhysicalPlan::TopN {
            input: Box::new(union),
            sort_exprs: make_sort_exprs(),
            limit: 10,
        };

        let result = apply_topn_pushdown(plan);

        match result {
            PhysicalPlan::TopN { input, limit, .. } => {
                assert_eq!(limit, 10);
                match *input {
                    PhysicalPlan::Union { inputs, .. } => {
                        assert_eq!(inputs.len(), 1);
                        assert!(matches!(&inputs[0], PhysicalPlan::TopN { .. }));
                    }
                    _ => panic!("Expected Union"),
                }
            }
            _ => panic!("Expected TopN"),
        }
    }

    #[test]
    fn handles_empty_union() {
        let union = PhysicalPlan::Union {
            inputs: vec![],
            all: true,
            schema: make_table_schema("union", 2),
            parallel: false,
            hints: ExecutionHints::default(),
        };

        let plan = PhysicalPlan::TopN {
            input: Box::new(union),
            sort_exprs: make_sort_exprs(),
            limit: 10,
        };

        let result = apply_topn_pushdown(plan);

        match result {
            PhysicalPlan::TopN { input, limit, .. } => {
                assert_eq!(limit, 10);
                match *input {
                    PhysicalPlan::Union { inputs, .. } => {
                        assert!(inputs.is_empty());
                    }
                    _ => panic!("Expected Union"),
                }
            }
            _ => panic!("Expected TopN"),
        }
    }
}
