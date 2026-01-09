#![coverage(off)]

use yachtsql_ir::{BinaryOp, Expr};

use crate::PhysicalPlan;

fn merge_predicates(outer: Expr, inner: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(outer),
        op: BinaryOp::And,
        right: Box::new(inner),
    }
}

pub fn apply_filter_merging(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Filter {
            input,
            predicate: outer_pred,
        } => {
            let optimized_input = apply_filter_merging(*input);

            match optimized_input {
                PhysicalPlan::Filter {
                    input: inner_input,
                    predicate: inner_pred,
                } => {
                    let merged_predicate = merge_predicates(outer_pred, inner_pred);
                    PhysicalPlan::Filter {
                        input: inner_input,
                        predicate: merged_predicate,
                    }
                }
                other => PhysicalPlan::Filter {
                    input: Box::new(other),
                    predicate: outer_pred,
                },
            }
        }

        PhysicalPlan::Project {
            input,
            expressions,
            schema,
        } => PhysicalPlan::Project {
            input: Box::new(apply_filter_merging(*input)),
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
            input: Box::new(apply_filter_merging(*input)),
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
            left: Box::new(apply_filter_merging(*left)),
            right: Box::new(apply_filter_merging(*right)),
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
            left: Box::new(apply_filter_merging(*left)),
            right: Box::new(apply_filter_merging(*right)),
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
            left: Box::new(apply_filter_merging(*left)),
            right: Box::new(apply_filter_merging(*right)),
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::Sort {
            input,
            sort_exprs,
            hints,
        } => PhysicalPlan::Sort {
            input: Box::new(apply_filter_merging(*input)),
            sort_exprs,
            hints,
        },

        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => PhysicalPlan::TopN {
            input: Box::new(apply_filter_merging(*input)),
            sort_exprs,
            limit,
        },

        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => PhysicalPlan::Limit {
            input: Box::new(apply_filter_merging(*input)),
            limit,
            offset,
        },

        PhysicalPlan::Distinct { input } => PhysicalPlan::Distinct {
            input: Box::new(apply_filter_merging(*input)),
        },

        PhysicalPlan::Union {
            inputs,
            all,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::Union {
            inputs: inputs.into_iter().map(apply_filter_merging).collect(),
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
            left: Box::new(apply_filter_merging(*left)),
            right: Box::new(apply_filter_merging(*right)),
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
            left: Box::new(apply_filter_merging(*left)),
            right: Box::new(apply_filter_merging(*right)),
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
            input: Box::new(apply_filter_merging(*input)),
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
            body: Box::new(apply_filter_merging(*body)),
            parallel_ctes,
            hints,
        },

        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => PhysicalPlan::Unnest {
            input: Box::new(apply_filter_merging(*input)),
            columns,
            schema,
        },

        PhysicalPlan::Qualify { input, predicate } => PhysicalPlan::Qualify {
            input: Box::new(apply_filter_merging(*input)),
            predicate,
        },

        PhysicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => PhysicalPlan::Sample {
            input: Box::new(apply_filter_merging(*input)),
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
            source: Box::new(apply_filter_merging(*source)),
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
            query: query.map(|q| Box::new(apply_filter_merging(*q))),
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
            query: Box::new(apply_filter_merging(*query)),
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
            source: Box::new(apply_filter_merging(*source)),
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
            from: from.map(|f| Box::new(apply_filter_merging(*f))),
            filter,
        },

        PhysicalPlan::ExportData { options, query } => PhysicalPlan::ExportData {
            options,
            query: Box::new(apply_filter_merging(*query)),
        },

        PhysicalPlan::For {
            variable,
            query,
            body,
        } => PhysicalPlan::For {
            variable,
            query: Box::new(apply_filter_merging(*query)),
            body: body.into_iter().map(apply_filter_merging).collect(),
        },

        PhysicalPlan::If {
            condition,
            then_branch,
            else_branch,
        } => PhysicalPlan::If {
            condition,
            then_branch: then_branch.into_iter().map(apply_filter_merging).collect(),
            else_branch: else_branch.map(|b| b.into_iter().map(apply_filter_merging).collect()),
        },

        PhysicalPlan::While {
            condition,
            body,
            label,
        } => PhysicalPlan::While {
            condition,
            body: body.into_iter().map(apply_filter_merging).collect(),
            label,
        },

        PhysicalPlan::Loop { body, label } => PhysicalPlan::Loop {
            body: body.into_iter().map(apply_filter_merging).collect(),
            label,
        },

        PhysicalPlan::Block { body, label } => PhysicalPlan::Block {
            body: body.into_iter().map(apply_filter_merging).collect(),
            label,
        },

        PhysicalPlan::Repeat {
            body,
            until_condition,
        } => PhysicalPlan::Repeat {
            body: body.into_iter().map(apply_filter_merging).collect(),
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
            body: body.into_iter().map(apply_filter_merging).collect(),
            or_replace,
            if_not_exists,
        },

        PhysicalPlan::TryCatch {
            try_block,
            catch_block,
        } => PhysicalPlan::TryCatch {
            try_block: try_block
                .into_iter()
                .map(|(p, sql)| (apply_filter_merging(p), sql))
                .collect(),
            catch_block: catch_block.into_iter().map(apply_filter_merging).collect(),
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
            input: Box::new(apply_filter_merging(*input)),
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
            input: Box::new(apply_filter_merging(*input)),
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
    use yachtsql_ir::{Literal, PlanField, PlanSchema};

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

    fn make_predicate(col_name: &str, value: i64) -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: None,
                name: col_name.to_string(),
                index: None,
            }),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int64(value))),
        }
    }

    #[test]
    fn merges_two_adjacent_filters() {
        let scan = make_scan("t", 3);
        let pred1 = make_predicate("col0", 10);
        let pred2 = make_predicate("col1", 20);

        let inner_filter = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: pred1.clone(),
        };
        let outer_filter = PhysicalPlan::Filter {
            input: Box::new(inner_filter),
            predicate: pred2.clone(),
        };

        let result = apply_filter_merging(outer_filter);

        match result {
            PhysicalPlan::Filter { input, predicate } => {
                assert!(matches!(*input, PhysicalPlan::TableScan { .. }));

                match predicate {
                    Expr::BinaryOp {
                        left,
                        op: BinaryOp::And,
                        right,
                    } => {
                        assert_eq!(*left, pred2);
                        assert_eq!(*right, pred1);
                    }
                    _ => panic!("Expected AND expression"),
                }
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn merges_three_adjacent_filters() {
        let scan = make_scan("t", 3);
        let pred1 = make_predicate("col0", 10);
        let pred2 = make_predicate("col1", 20);
        let pred3 = make_predicate("col2", 30);

        let filter1 = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: pred1.clone(),
        };
        let filter2 = PhysicalPlan::Filter {
            input: Box::new(filter1),
            predicate: pred2.clone(),
        };
        let filter3 = PhysicalPlan::Filter {
            input: Box::new(filter2),
            predicate: pred3.clone(),
        };

        let result = apply_filter_merging(filter3);

        match result {
            PhysicalPlan::Filter { input, predicate } => {
                assert!(matches!(*input, PhysicalPlan::TableScan { .. }));

                match &predicate {
                    Expr::BinaryOp {
                        left,
                        op: BinaryOp::And,
                        right,
                    } => {
                        assert_eq!(**left, pred3);

                        match right.as_ref() {
                            Expr::BinaryOp {
                                left: inner_left,
                                op: BinaryOp::And,
                                right: inner_right,
                            } => {
                                assert_eq!(**inner_left, pred2);
                                assert_eq!(**inner_right, pred1);
                            }
                            _ => panic!("Expected nested AND expression"),
                        }
                    }
                    _ => panic!("Expected AND expression"),
                }
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn preserves_filter_when_no_adjacent_filters() {
        let scan = make_scan("t", 3);
        let pred = make_predicate("col0", 10);

        let filter = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: pred.clone(),
        };

        let result = apply_filter_merging(filter);

        match result {
            PhysicalPlan::Filter { input, predicate } => {
                assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
                assert_eq!(predicate, pred);
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn merges_filters_with_project_in_between() {
        let scan = make_scan("t", 3);
        let pred1 = make_predicate("col0", 10);
        let pred2 = make_predicate("col1", 20);
        let schema = make_schema(2);

        let inner_filter = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: pred1.clone(),
        };
        let project = PhysicalPlan::Project {
            input: Box::new(inner_filter),
            expressions: vec![],
            schema,
        };
        let outer_filter = PhysicalPlan::Filter {
            input: Box::new(project),
            predicate: pred2.clone(),
        };

        let result = apply_filter_merging(outer_filter);

        match result {
            PhysicalPlan::Filter { input, predicate } => {
                assert_eq!(predicate, pred2);
                match *input {
                    PhysicalPlan::Project {
                        input: proj_input, ..
                    } => match *proj_input {
                        PhysicalPlan::Filter {
                            predicate: inner_pred,
                            ..
                        } => {
                            assert_eq!(inner_pred, pred1);
                        }
                        _ => panic!("Expected inner Filter"),
                    },
                    _ => panic!("Expected Project"),
                }
            }
            _ => panic!("Expected Filter plan"),
        }
    }
}
