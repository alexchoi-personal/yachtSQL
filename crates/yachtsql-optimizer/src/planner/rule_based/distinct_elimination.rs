#![coverage(off)]

use crate::PhysicalPlan;

fn input_is_unique(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::Limit { limit: Some(1), .. } => true,
        PhysicalPlan::Empty { .. } => true,
        PhysicalPlan::Values { values, .. } => values.len() <= 1,
        PhysicalPlan::TopN { limit: 1, .. } => true,
        PhysicalPlan::Distinct { .. } => true,
        PhysicalPlan::Project { input, .. } => input_is_unique(input),
        PhysicalPlan::Filter { input, .. } => input_is_unique(input),
        PhysicalPlan::TableScan { .. }
        | PhysicalPlan::HashAggregate { .. }
        | PhysicalPlan::Sample { .. }
        | PhysicalPlan::NestedLoopJoin { .. }
        | PhysicalPlan::CrossJoin { .. }
        | PhysicalPlan::HashJoin { .. }
        | PhysicalPlan::Sort { .. }
        | PhysicalPlan::Limit { .. }
        | PhysicalPlan::TopN { .. }
        | PhysicalPlan::Union { .. }
        | PhysicalPlan::Intersect { .. }
        | PhysicalPlan::Except { .. }
        | PhysicalPlan::Window { .. }
        | PhysicalPlan::Unnest { .. }
        | PhysicalPlan::Qualify { .. }
        | PhysicalPlan::WithCte { .. }
        | PhysicalPlan::Insert { .. }
        | PhysicalPlan::Update { .. }
        | PhysicalPlan::Delete { .. }
        | PhysicalPlan::Merge { .. }
        | PhysicalPlan::CreateTable { .. }
        | PhysicalPlan::DropTable { .. }
        | PhysicalPlan::AlterTable { .. }
        | PhysicalPlan::Truncate { .. }
        | PhysicalPlan::CreateView { .. }
        | PhysicalPlan::DropView { .. }
        | PhysicalPlan::CreateSchema { .. }
        | PhysicalPlan::DropSchema { .. }
        | PhysicalPlan::UndropSchema { .. }
        | PhysicalPlan::AlterSchema { .. }
        | PhysicalPlan::CreateFunction { .. }
        | PhysicalPlan::DropFunction { .. }
        | PhysicalPlan::CreateProcedure { .. }
        | PhysicalPlan::DropProcedure { .. }
        | PhysicalPlan::Call { .. }
        | PhysicalPlan::ExportData { .. }
        | PhysicalPlan::LoadData { .. }
        | PhysicalPlan::Declare { .. }
        | PhysicalPlan::SetVariable { .. }
        | PhysicalPlan::SetMultipleVariables { .. }
        | PhysicalPlan::If { .. }
        | PhysicalPlan::While { .. }
        | PhysicalPlan::Loop { .. }
        | PhysicalPlan::Block { .. }
        | PhysicalPlan::Repeat { .. }
        | PhysicalPlan::For { .. }
        | PhysicalPlan::Return { .. }
        | PhysicalPlan::Raise { .. }
        | PhysicalPlan::ExecuteImmediate { .. }
        | PhysicalPlan::Break { .. }
        | PhysicalPlan::Continue { .. }
        | PhysicalPlan::CreateSnapshot { .. }
        | PhysicalPlan::DropSnapshot { .. }
        | PhysicalPlan::Assert { .. }
        | PhysicalPlan::Grant { .. }
        | PhysicalPlan::Revoke { .. }
        | PhysicalPlan::BeginTransaction
        | PhysicalPlan::Commit
        | PhysicalPlan::Rollback
        | PhysicalPlan::TryCatch { .. }
        | PhysicalPlan::GapFill { .. }
        | PhysicalPlan::Explain { .. } => false,
    }
}

pub fn apply_distinct_elimination(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Distinct { input } => {
            let optimized_input = apply_distinct_elimination(*input);

            if input_is_unique(&optimized_input) {
                optimized_input
            } else {
                PhysicalPlan::Distinct {
                    input: Box::new(optimized_input),
                }
            }
        }

        PhysicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(apply_distinct_elimination(*input)),
            predicate,
        },

        PhysicalPlan::Project {
            input,
            expressions,
            schema,
        } => PhysicalPlan::Project {
            input: Box::new(apply_distinct_elimination(*input)),
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
            input: Box::new(apply_distinct_elimination(*input)),
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
            left: Box::new(apply_distinct_elimination(*left)),
            right: Box::new(apply_distinct_elimination(*right)),
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
            left: Box::new(apply_distinct_elimination(*left)),
            right: Box::new(apply_distinct_elimination(*right)),
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
            left: Box::new(apply_distinct_elimination(*left)),
            right: Box::new(apply_distinct_elimination(*right)),
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::Sort {
            input,
            sort_exprs,
            hints,
        } => PhysicalPlan::Sort {
            input: Box::new(apply_distinct_elimination(*input)),
            sort_exprs,
            hints,
        },

        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => PhysicalPlan::TopN {
            input: Box::new(apply_distinct_elimination(*input)),
            sort_exprs,
            limit,
        },

        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => PhysicalPlan::Limit {
            input: Box::new(apply_distinct_elimination(*input)),
            limit,
            offset,
        },

        PhysicalPlan::Union {
            inputs,
            all,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::Union {
            inputs: inputs.into_iter().map(apply_distinct_elimination).collect(),
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
            left: Box::new(apply_distinct_elimination(*left)),
            right: Box::new(apply_distinct_elimination(*right)),
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
            left: Box::new(apply_distinct_elimination(*left)),
            right: Box::new(apply_distinct_elimination(*right)),
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
            input: Box::new(apply_distinct_elimination(*input)),
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
            body: Box::new(apply_distinct_elimination(*body)),
            parallel_ctes,
            hints,
        },

        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => PhysicalPlan::Unnest {
            input: Box::new(apply_distinct_elimination(*input)),
            columns,
            schema,
        },

        PhysicalPlan::Qualify { input, predicate } => PhysicalPlan::Qualify {
            input: Box::new(apply_distinct_elimination(*input)),
            predicate,
        },

        PhysicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => PhysicalPlan::Sample {
            input: Box::new(apply_distinct_elimination(*input)),
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
            source: Box::new(apply_distinct_elimination(*source)),
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
            query: query.map(|q| Box::new(apply_distinct_elimination(*q))),
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
            query: Box::new(apply_distinct_elimination(*query)),
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
            source: Box::new(apply_distinct_elimination(*source)),
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
            from: from.map(|f| Box::new(apply_distinct_elimination(*f))),
            filter,
        },

        PhysicalPlan::ExportData { options, query } => PhysicalPlan::ExportData {
            options,
            query: Box::new(apply_distinct_elimination(*query)),
        },

        PhysicalPlan::For {
            variable,
            query,
            body,
        } => PhysicalPlan::For {
            variable,
            query: Box::new(apply_distinct_elimination(*query)),
            body: body.into_iter().map(apply_distinct_elimination).collect(),
        },

        PhysicalPlan::If {
            condition,
            then_branch,
            else_branch,
        } => PhysicalPlan::If {
            condition,
            then_branch: then_branch
                .into_iter()
                .map(apply_distinct_elimination)
                .collect(),
            else_branch: else_branch
                .map(|b| b.into_iter().map(apply_distinct_elimination).collect()),
        },

        PhysicalPlan::While {
            condition,
            body,
            label,
        } => PhysicalPlan::While {
            condition,
            body: body.into_iter().map(apply_distinct_elimination).collect(),
            label,
        },

        PhysicalPlan::Loop { body, label } => PhysicalPlan::Loop {
            body: body.into_iter().map(apply_distinct_elimination).collect(),
            label,
        },

        PhysicalPlan::Block { body, label } => PhysicalPlan::Block {
            body: body.into_iter().map(apply_distinct_elimination).collect(),
            label,
        },

        PhysicalPlan::Repeat {
            body,
            until_condition,
        } => PhysicalPlan::Repeat {
            body: body.into_iter().map(apply_distinct_elimination).collect(),
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
            body: body.into_iter().map(apply_distinct_elimination).collect(),
            or_replace,
            if_not_exists,
        },

        PhysicalPlan::TryCatch {
            try_block,
            catch_block,
        } => PhysicalPlan::TryCatch {
            try_block: try_block
                .into_iter()
                .map(|(p, sql)| (apply_distinct_elimination(p), sql))
                .collect(),
            catch_block: catch_block
                .into_iter()
                .map(apply_distinct_elimination)
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
            input: Box::new(apply_distinct_elimination(*input)),
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
            input: Box::new(apply_distinct_elimination(*input)),
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
    use yachtsql_ir::{Expr, Literal, PlanField, PlanSchema, SortExpr};

    use super::*;

    fn make_schema(num_columns: usize) -> PlanSchema {
        let fields = (0..num_columns)
            .map(|i| PlanField::new(format!("col{}", i), DataType::Int64))
            .collect();
        PlanSchema::from_fields(fields)
    }

    fn make_scan(table_name: &str, num_columns: usize) -> PhysicalPlan {
        PhysicalPlan::TableScan {
            table_name: table_name.to_string(),
            schema: make_schema(num_columns),
            projection: None,
            row_count: None,
        }
    }

    #[test]
    fn preserves_distinct_over_hash_aggregate_with_group_by() {
        let scan = make_scan("t", 3);
        let agg = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: vec![Expr::Column {
                table: None,
                name: "col0".to_string(),
                index: Some(0),
            }],
            aggregates: vec![],
            schema: make_schema(1),
            grouping_sets: None,
            hints: Default::default(),
        };
        let distinct = PhysicalPlan::Distinct {
            input: Box::new(agg),
        };

        let result = apply_distinct_elimination(distinct);

        assert!(matches!(result, PhysicalPlan::Distinct { .. }));
    }

    #[test]
    fn preserves_distinct_over_hash_aggregate_without_group_by() {
        let scan = make_scan("t", 3);
        let agg = PhysicalPlan::HashAggregate {
            input: Box::new(scan),
            group_by: vec![],
            aggregates: vec![Expr::Aggregate {
                func: yachtsql_ir::AggregateFunction::Count,
                args: vec![Expr::Literal(Literal::Int64(1))],
                distinct: false,
                filter: None,
                order_by: vec![],
                limit: None,
                ignore_nulls: false,
            }],
            schema: make_schema(1),
            grouping_sets: None,
            hints: Default::default(),
        };
        let distinct = PhysicalPlan::Distinct {
            input: Box::new(agg),
        };

        let result = apply_distinct_elimination(distinct);

        assert!(matches!(result, PhysicalPlan::Distinct { .. }));
    }

    #[test]
    fn eliminates_distinct_over_limit_1() {
        let scan = make_scan("t", 3);
        let limit = PhysicalPlan::Limit {
            input: Box::new(scan),
            limit: Some(1),
            offset: None,
        };
        let distinct = PhysicalPlan::Distinct {
            input: Box::new(limit),
        };

        let result = apply_distinct_elimination(distinct);

        assert!(matches!(result, PhysicalPlan::Limit { limit: Some(1), .. }));
    }

    #[test]
    fn preserves_distinct_over_limit_greater_than_1() {
        let scan = make_scan("t", 3);
        let limit = PhysicalPlan::Limit {
            input: Box::new(scan),
            limit: Some(10),
            offset: None,
        };
        let distinct = PhysicalPlan::Distinct {
            input: Box::new(limit),
        };

        let result = apply_distinct_elimination(distinct);

        assert!(matches!(result, PhysicalPlan::Distinct { .. }));
    }

    #[test]
    fn eliminates_distinct_over_empty() {
        let empty = PhysicalPlan::Empty {
            schema: make_schema(3),
        };
        let distinct = PhysicalPlan::Distinct {
            input: Box::new(empty),
        };

        let result = apply_distinct_elimination(distinct);

        assert!(matches!(result, PhysicalPlan::Empty { .. }));
    }

    #[test]
    fn eliminates_distinct_over_values_single_row() {
        let values = PhysicalPlan::Values {
            values: vec![vec![Expr::Literal(Literal::Int64(1))]],
            schema: make_schema(1),
        };
        let distinct = PhysicalPlan::Distinct {
            input: Box::new(values),
        };

        let result = apply_distinct_elimination(distinct);

        assert!(matches!(result, PhysicalPlan::Values { .. }));
    }

    #[test]
    fn eliminates_distinct_over_values_empty() {
        let values = PhysicalPlan::Values {
            values: vec![],
            schema: make_schema(1),
        };
        let distinct = PhysicalPlan::Distinct {
            input: Box::new(values),
        };

        let result = apply_distinct_elimination(distinct);

        assert!(matches!(result, PhysicalPlan::Values { .. }));
    }

    #[test]
    fn preserves_distinct_over_values_multiple_rows() {
        let values = PhysicalPlan::Values {
            values: vec![
                vec![Expr::Literal(Literal::Int64(1))],
                vec![Expr::Literal(Literal::Int64(2))],
            ],
            schema: make_schema(1),
        };
        let distinct = PhysicalPlan::Distinct {
            input: Box::new(values),
        };

        let result = apply_distinct_elimination(distinct);

        assert!(matches!(result, PhysicalPlan::Distinct { .. }));
    }

    #[test]
    fn eliminates_distinct_over_topn_1() {
        let scan = make_scan("t", 3);
        let topn = PhysicalPlan::TopN {
            input: Box::new(scan),
            sort_exprs: vec![SortExpr {
                expr: Expr::Column {
                    table: None,
                    name: "col0".to_string(),
                    index: Some(0),
                },
                asc: true,
                nulls_first: false,
            }],
            limit: 1,
        };
        let distinct = PhysicalPlan::Distinct {
            input: Box::new(topn),
        };

        let result = apply_distinct_elimination(distinct);

        assert!(matches!(result, PhysicalPlan::TopN { limit: 1, .. }));
    }

    #[test]
    fn preserves_distinct_over_topn_greater_than_1() {
        let scan = make_scan("t", 3);
        let topn = PhysicalPlan::TopN {
            input: Box::new(scan),
            sort_exprs: vec![SortExpr {
                expr: Expr::Column {
                    table: None,
                    name: "col0".to_string(),
                    index: Some(0),
                },
                asc: true,
                nulls_first: false,
            }],
            limit: 10,
        };
        let distinct = PhysicalPlan::Distinct {
            input: Box::new(topn),
        };

        let result = apply_distinct_elimination(distinct);

        assert!(matches!(result, PhysicalPlan::Distinct { .. }));
    }

    #[test]
    fn eliminates_nested_distinct() {
        let scan = make_scan("t", 3);
        let inner_distinct = PhysicalPlan::Distinct {
            input: Box::new(PhysicalPlan::Limit {
                input: Box::new(scan),
                limit: Some(1),
                offset: None,
            }),
        };
        let outer_distinct = PhysicalPlan::Distinct {
            input: Box::new(inner_distinct),
        };

        let result = apply_distinct_elimination(outer_distinct);

        assert!(matches!(result, PhysicalPlan::Limit { limit: Some(1), .. }));
    }

    #[test]
    fn preserves_distinct_over_table_scan() {
        let scan = make_scan("t", 3);
        let distinct = PhysicalPlan::Distinct {
            input: Box::new(scan),
        };

        let result = apply_distinct_elimination(distinct);

        assert!(matches!(result, PhysicalPlan::Distinct { .. }));
    }

    #[test]
    fn eliminates_outer_distinct_over_inner_distinct() {
        let scan = make_scan("t", 3);
        let inner_distinct = PhysicalPlan::Distinct {
            input: Box::new(scan),
        };
        let outer_distinct = PhysicalPlan::Distinct {
            input: Box::new(inner_distinct),
        };

        let result = apply_distinct_elimination(outer_distinct);

        match result {
            PhysicalPlan::Distinct { input } => {
                assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
            }
            _ => panic!("Expected single Distinct over TableScan"),
        }
    }

    #[test]
    fn eliminates_multiple_nested_distincts() {
        let scan = make_scan("t", 3);
        let d1 = PhysicalPlan::Distinct {
            input: Box::new(scan),
        };
        let d2 = PhysicalPlan::Distinct {
            input: Box::new(d1),
        };
        let d3 = PhysicalPlan::Distinct {
            input: Box::new(d2),
        };

        let result = apply_distinct_elimination(d3);

        match result {
            PhysicalPlan::Distinct { input } => {
                assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
            }
            _ => panic!("Expected single Distinct over TableScan"),
        }
    }

    #[test]
    fn recurses_through_filter() {
        let scan = make_scan("t", 3);
        let limit = PhysicalPlan::Limit {
            input: Box::new(scan),
            limit: Some(1),
            offset: None,
        };
        let distinct = PhysicalPlan::Distinct {
            input: Box::new(limit),
        };
        let filter = PhysicalPlan::Filter {
            input: Box::new(distinct),
            predicate: Expr::Literal(Literal::Bool(true)),
        };

        let result = apply_distinct_elimination(filter);

        match result {
            PhysicalPlan::Filter { input, .. } => {
                assert!(matches!(*input, PhysicalPlan::Limit { limit: Some(1), .. }));
            }
            _ => panic!("Expected Filter plan"),
        }
    }
}
