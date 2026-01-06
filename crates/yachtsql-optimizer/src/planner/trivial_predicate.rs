#![coverage(off)]

use yachtsql_ir::{Expr, Literal};

use crate::PhysicalPlan;

pub enum TrivialPredicateResult {
    AlwaysTrue,
    AlwaysFalse,
    NonTrivial(Expr),
}

pub fn classify_predicate(predicate: &Expr) -> TrivialPredicateResult {
    match predicate {
        Expr::Literal(Literal::Bool(true)) => TrivialPredicateResult::AlwaysTrue,
        Expr::Literal(Literal::Bool(false)) => TrivialPredicateResult::AlwaysFalse,
        _ => TrivialPredicateResult::NonTrivial(predicate.clone()),
    }
}

pub fn apply_trivial_predicate_removal(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Filter { input, predicate } => {
            let optimized_input = apply_trivial_predicate_removal(*input);

            match classify_predicate(&predicate) {
                TrivialPredicateResult::AlwaysTrue => optimized_input,
                TrivialPredicateResult::AlwaysFalse => {
                    if optimized_input.schema().fields.is_empty() {
                        PhysicalPlan::Filter {
                            input: Box::new(optimized_input),
                            predicate,
                        }
                    } else {
                        PhysicalPlan::Empty {
                            schema: optimized_input.schema().clone(),
                        }
                    }
                }
                TrivialPredicateResult::NonTrivial(pred) => PhysicalPlan::Filter {
                    input: Box::new(optimized_input),
                    predicate: pred,
                },
            }
        }

        PhysicalPlan::Project {
            input,
            expressions,
            schema,
        } => PhysicalPlan::Project {
            input: Box::new(apply_trivial_predicate_removal(*input)),
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
            input: Box::new(apply_trivial_predicate_removal(*input)),
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
            left: Box::new(apply_trivial_predicate_removal(*left)),
            right: Box::new(apply_trivial_predicate_removal(*right)),
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
            left: Box::new(apply_trivial_predicate_removal(*left)),
            right: Box::new(apply_trivial_predicate_removal(*right)),
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
            left: Box::new(apply_trivial_predicate_removal(*left)),
            right: Box::new(apply_trivial_predicate_removal(*right)),
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::Sort {
            input,
            sort_exprs,
            hints,
        } => PhysicalPlan::Sort {
            input: Box::new(apply_trivial_predicate_removal(*input)),
            sort_exprs,
            hints,
        },

        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => PhysicalPlan::TopN {
            input: Box::new(apply_trivial_predicate_removal(*input)),
            sort_exprs,
            limit,
        },

        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => PhysicalPlan::Limit {
            input: Box::new(apply_trivial_predicate_removal(*input)),
            limit,
            offset,
        },

        PhysicalPlan::Distinct { input } => PhysicalPlan::Distinct {
            input: Box::new(apply_trivial_predicate_removal(*input)),
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
                .map(apply_trivial_predicate_removal)
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
            left: Box::new(apply_trivial_predicate_removal(*left)),
            right: Box::new(apply_trivial_predicate_removal(*right)),
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
            left: Box::new(apply_trivial_predicate_removal(*left)),
            right: Box::new(apply_trivial_predicate_removal(*right)),
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
            input: Box::new(apply_trivial_predicate_removal(*input)),
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
            body: Box::new(apply_trivial_predicate_removal(*body)),
            parallel_ctes,
            hints,
        },

        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => PhysicalPlan::Unnest {
            input: Box::new(apply_trivial_predicate_removal(*input)),
            columns,
            schema,
        },

        PhysicalPlan::Qualify { input, predicate } => {
            let optimized_input = apply_trivial_predicate_removal(*input);

            match classify_predicate(&predicate) {
                TrivialPredicateResult::AlwaysTrue => optimized_input,
                TrivialPredicateResult::AlwaysFalse => {
                    if optimized_input.schema().fields.is_empty() {
                        PhysicalPlan::Qualify {
                            input: Box::new(optimized_input),
                            predicate,
                        }
                    } else {
                        PhysicalPlan::Empty {
                            schema: optimized_input.schema().clone(),
                        }
                    }
                }
                TrivialPredicateResult::NonTrivial(pred) => PhysicalPlan::Qualify {
                    input: Box::new(optimized_input),
                    predicate: pred,
                },
            }
        }

        PhysicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => PhysicalPlan::Sample {
            input: Box::new(apply_trivial_predicate_removal(*input)),
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
            source: Box::new(apply_trivial_predicate_removal(*source)),
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
            query: query.map(|q| Box::new(apply_trivial_predicate_removal(*q))),
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
            query: Box::new(apply_trivial_predicate_removal(*query)),
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
            source: Box::new(apply_trivial_predicate_removal(*source)),
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
            from: from.map(|f| Box::new(apply_trivial_predicate_removal(*f))),
            filter,
        },

        PhysicalPlan::ExportData { options, query } => PhysicalPlan::ExportData {
            options,
            query: Box::new(apply_trivial_predicate_removal(*query)),
        },

        PhysicalPlan::For {
            variable,
            query,
            body,
        } => PhysicalPlan::For {
            variable,
            query: Box::new(apply_trivial_predicate_removal(*query)),
            body: body
                .into_iter()
                .map(apply_trivial_predicate_removal)
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
                .map(apply_trivial_predicate_removal)
                .collect(),
            else_branch: else_branch
                .map(|b| b.into_iter().map(apply_trivial_predicate_removal).collect()),
        },

        PhysicalPlan::While {
            condition,
            body,
            label,
        } => PhysicalPlan::While {
            condition,
            body: body
                .into_iter()
                .map(apply_trivial_predicate_removal)
                .collect(),
            label,
        },

        PhysicalPlan::Loop { body, label } => PhysicalPlan::Loop {
            body: body
                .into_iter()
                .map(apply_trivial_predicate_removal)
                .collect(),
            label,
        },

        PhysicalPlan::Block { body, label } => PhysicalPlan::Block {
            body: body
                .into_iter()
                .map(apply_trivial_predicate_removal)
                .collect(),
            label,
        },

        PhysicalPlan::Repeat {
            body,
            until_condition,
        } => PhysicalPlan::Repeat {
            body: body
                .into_iter()
                .map(apply_trivial_predicate_removal)
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
                .map(apply_trivial_predicate_removal)
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
                .map(|(p, sql)| (apply_trivial_predicate_removal(p), sql))
                .collect(),
            catch_block: catch_block
                .into_iter()
                .map(apply_trivial_predicate_removal)
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
            input: Box::new(apply_trivial_predicate_removal(*input)),
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
            input: Box::new(apply_trivial_predicate_removal(*input)),
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
    use yachtsql_ir::{BinaryOp, PlanField, PlanSchema};

    use super::*;

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
    fn removes_filter_with_true_predicate() {
        let scan = make_scan("t", 3);
        let plan = PhysicalPlan::Filter {
            input: Box::new(scan.clone()),
            predicate: Expr::Literal(Literal::Bool(true)),
        };

        let result = apply_trivial_predicate_removal(plan);

        assert!(matches!(result, PhysicalPlan::TableScan { .. }));
    }

    #[test]
    fn converts_filter_with_false_to_empty() {
        let scan = make_scan("t", 3);
        let original_field_count = scan.schema().fields.len();
        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: Expr::Literal(Literal::Bool(false)),
        };

        let result = apply_trivial_predicate_removal(plan);

        match result {
            PhysicalPlan::Empty { schema } => {
                assert_eq!(schema.fields.len(), original_field_count);
            }
            _ => panic!("Expected Empty plan"),
        }
    }

    #[test]
    fn preserves_filter_with_non_trivial_predicate() {
        let scan = make_scan("t", 3);
        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: Some("t".to_string()),
                name: "col0".to_string(),
                index: Some(0),
            }),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int64(100))),
        };
        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: predicate.clone(),
        };

        let result = apply_trivial_predicate_removal(plan);

        match result {
            PhysicalPlan::Filter {
                predicate: result_pred,
                ..
            } => {
                assert_eq!(result_pred, predicate);
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn removes_nested_trivial_filters() {
        let scan = make_scan("t", 3);
        let inner_filter = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: Expr::Literal(Literal::Bool(true)),
        };
        let outer_filter = PhysicalPlan::Filter {
            input: Box::new(inner_filter),
            predicate: Expr::Literal(Literal::Bool(true)),
        };

        let result = apply_trivial_predicate_removal(outer_filter);

        assert!(matches!(result, PhysicalPlan::TableScan { .. }));
    }

    #[test]
    fn removes_qualify_with_true_predicate() {
        let scan = make_scan("t", 3);
        let plan = PhysicalPlan::Qualify {
            input: Box::new(scan.clone()),
            predicate: Expr::Literal(Literal::Bool(true)),
        };

        let result = apply_trivial_predicate_removal(plan);

        assert!(matches!(result, PhysicalPlan::TableScan { .. }));
    }

    #[test]
    fn converts_qualify_with_false_to_empty() {
        let scan = make_scan("t", 3);
        let original_field_count = scan.schema().fields.len();
        let plan = PhysicalPlan::Qualify {
            input: Box::new(scan),
            predicate: Expr::Literal(Literal::Bool(false)),
        };

        let result = apply_trivial_predicate_removal(plan);

        match result {
            PhysicalPlan::Empty { schema } => {
                assert_eq!(schema.fields.len(), original_field_count);
            }
            _ => panic!("Expected Empty plan"),
        }
    }
}
