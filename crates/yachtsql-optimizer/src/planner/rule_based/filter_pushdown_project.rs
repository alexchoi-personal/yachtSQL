#![coverage(off)]

use yachtsql_ir::Expr;

use super::project_merging::substitute_column_refs;
use crate::PhysicalPlan;

fn is_simple_column_ref(expr: &Expr) -> bool {
    match expr {
        Expr::Column { .. } => true,
        Expr::Alias { expr: inner, .. } => is_simple_column_ref(inner),
        _ => false,
    }
}

fn all_simple_refs(expressions: &[Expr]) -> bool {
    expressions.iter().all(is_simple_column_ref)
}

fn try_push_filter_through_project(input: PhysicalPlan, predicate: Expr) -> PhysicalPlan {
    match input {
        PhysicalPlan::Project {
            input: proj_input,
            expressions,
            schema,
        } => {
            if all_simple_refs(&expressions) {
                let remapped = substitute_column_refs(&predicate, &expressions);
                PhysicalPlan::Project {
                    input: Box::new(PhysicalPlan::Filter {
                        input: proj_input,
                        predicate: remapped,
                    }),
                    expressions,
                    schema,
                }
            } else {
                PhysicalPlan::Filter {
                    input: Box::new(PhysicalPlan::Project {
                        input: proj_input,
                        expressions,
                        schema,
                    }),
                    predicate,
                }
            }
        }
        other => PhysicalPlan::Filter {
            input: Box::new(other),
            predicate,
        },
    }
}

pub fn apply_filter_pushdown_project(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Filter { input, predicate } => {
            let optimized_input = apply_filter_pushdown_project(*input);
            try_push_filter_through_project(optimized_input, predicate)
        }

        PhysicalPlan::Project {
            input,
            expressions,
            schema,
        } => PhysicalPlan::Project {
            input: Box::new(apply_filter_pushdown_project(*input)),
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
            input: Box::new(apply_filter_pushdown_project(*input)),
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
            left: Box::new(apply_filter_pushdown_project(*left)),
            right: Box::new(apply_filter_pushdown_project(*right)),
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
            left: Box::new(apply_filter_pushdown_project(*left)),
            right: Box::new(apply_filter_pushdown_project(*right)),
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
            left: Box::new(apply_filter_pushdown_project(*left)),
            right: Box::new(apply_filter_pushdown_project(*right)),
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::Sort {
            input,
            sort_exprs,
            hints,
        } => PhysicalPlan::Sort {
            input: Box::new(apply_filter_pushdown_project(*input)),
            sort_exprs,
            hints,
        },

        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => PhysicalPlan::TopN {
            input: Box::new(apply_filter_pushdown_project(*input)),
            sort_exprs,
            limit,
        },

        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => PhysicalPlan::Limit {
            input: Box::new(apply_filter_pushdown_project(*input)),
            limit,
            offset,
        },

        PhysicalPlan::Distinct { input } => PhysicalPlan::Distinct {
            input: Box::new(apply_filter_pushdown_project(*input)),
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
                .map(apply_filter_pushdown_project)
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
            left: Box::new(apply_filter_pushdown_project(*left)),
            right: Box::new(apply_filter_pushdown_project(*right)),
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
            left: Box::new(apply_filter_pushdown_project(*left)),
            right: Box::new(apply_filter_pushdown_project(*right)),
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
            input: Box::new(apply_filter_pushdown_project(*input)),
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
            body: Box::new(apply_filter_pushdown_project(*body)),
            parallel_ctes,
            hints,
        },

        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => PhysicalPlan::Unnest {
            input: Box::new(apply_filter_pushdown_project(*input)),
            columns,
            schema,
        },

        PhysicalPlan::Qualify { input, predicate } => PhysicalPlan::Qualify {
            input: Box::new(apply_filter_pushdown_project(*input)),
            predicate,
        },

        PhysicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => PhysicalPlan::Sample {
            input: Box::new(apply_filter_pushdown_project(*input)),
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
            source: Box::new(apply_filter_pushdown_project(*source)),
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
            query: query.map(|q| Box::new(apply_filter_pushdown_project(*q))),
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
            query: Box::new(apply_filter_pushdown_project(*query)),
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
            source: Box::new(apply_filter_pushdown_project(*source)),
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
            from: from.map(|f| Box::new(apply_filter_pushdown_project(*f))),
            filter,
        },

        PhysicalPlan::ExportData { options, query } => PhysicalPlan::ExportData {
            options,
            query: Box::new(apply_filter_pushdown_project(*query)),
        },

        PhysicalPlan::For {
            variable,
            query,
            body,
        } => PhysicalPlan::For {
            variable,
            query: Box::new(apply_filter_pushdown_project(*query)),
            body: body
                .into_iter()
                .map(apply_filter_pushdown_project)
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
                .map(apply_filter_pushdown_project)
                .collect(),
            else_branch: else_branch
                .map(|b| b.into_iter().map(apply_filter_pushdown_project).collect()),
        },

        PhysicalPlan::While {
            condition,
            body,
            label,
        } => PhysicalPlan::While {
            condition,
            body: body
                .into_iter()
                .map(apply_filter_pushdown_project)
                .collect(),
            label,
        },

        PhysicalPlan::Loop { body, label } => PhysicalPlan::Loop {
            body: body
                .into_iter()
                .map(apply_filter_pushdown_project)
                .collect(),
            label,
        },

        PhysicalPlan::Block { body, label } => PhysicalPlan::Block {
            body: body
                .into_iter()
                .map(apply_filter_pushdown_project)
                .collect(),
            label,
        },

        PhysicalPlan::Repeat {
            body,
            until_condition,
        } => PhysicalPlan::Repeat {
            body: body
                .into_iter()
                .map(apply_filter_pushdown_project)
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
                .map(apply_filter_pushdown_project)
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
                .map(|(p, sql)| (apply_filter_pushdown_project(p), sql))
                .collect(),
            catch_block: catch_block
                .into_iter()
                .map(apply_filter_pushdown_project)
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
            input: Box::new(apply_filter_pushdown_project(*input)),
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
            input: Box::new(apply_filter_pushdown_project(*input)),
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
    use yachtsql_ir::{BinaryOp, Literal, PlanField, PlanSchema};

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

    fn col_expr(idx: usize, name: &str) -> Expr {
        Expr::Column {
            table: None,
            name: name.to_string(),
            index: Some(idx),
        }
    }

    #[test]
    fn pushes_filter_through_simple_project() {
        let scan = make_scan("t", 3);

        let project = PhysicalPlan::Project {
            input: Box::new(scan),
            expressions: vec![col_expr(0, "a"), col_expr(1, "b")],
            schema: make_schema(2),
        };

        let filter = PhysicalPlan::Filter {
            input: Box::new(project),
            predicate: Expr::BinaryOp {
                left: Box::new(col_expr(0, "a")),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Literal(Literal::Int64(42))),
            },
        };

        let result = apply_filter_pushdown_project(filter);

        match result {
            PhysicalPlan::Project { input, .. } => match *input {
                PhysicalPlan::Filter { input: inner, .. } => {
                    assert!(matches!(*inner, PhysicalPlan::TableScan { .. }));
                }
                _ => panic!("Expected Filter inside Project"),
            },
            _ => panic!("Expected Project"),
        }
    }

    #[test]
    fn does_not_push_through_computed_project() {
        let scan = make_scan("t", 3);

        let project = PhysicalPlan::Project {
            input: Box::new(scan),
            expressions: vec![Expr::BinaryOp {
                left: Box::new(col_expr(0, "a")),
                op: BinaryOp::Add,
                right: Box::new(Expr::Literal(Literal::Int64(1))),
            }],
            schema: make_schema(1),
        };

        let filter = PhysicalPlan::Filter {
            input: Box::new(project),
            predicate: Expr::BinaryOp {
                left: Box::new(col_expr(0, "computed")),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(Literal::Int64(10))),
            },
        };

        let result = apply_filter_pushdown_project(filter);

        match result {
            PhysicalPlan::Filter { input, .. } => {
                assert!(matches!(*input, PhysicalPlan::Project { .. }));
            }
            _ => panic!("Expected Filter on top"),
        }
    }

    #[test]
    fn pushes_filter_through_alias_project() {
        let scan = make_scan("t", 2);

        let project = PhysicalPlan::Project {
            input: Box::new(scan),
            expressions: vec![Expr::Alias {
                expr: Box::new(col_expr(0, "col0")),
                name: "renamed".to_string(),
            }],
            schema: make_schema(1),
        };

        let filter = PhysicalPlan::Filter {
            input: Box::new(project),
            predicate: Expr::BinaryOp {
                left: Box::new(col_expr(0, "renamed")),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Literal(Literal::String("test".to_string()))),
            },
        };

        let result = apply_filter_pushdown_project(filter);

        match result {
            PhysicalPlan::Project { input, .. } => {
                assert!(matches!(*input, PhysicalPlan::Filter { .. }));
            }
            _ => panic!("Expected Project"),
        }
    }
}
