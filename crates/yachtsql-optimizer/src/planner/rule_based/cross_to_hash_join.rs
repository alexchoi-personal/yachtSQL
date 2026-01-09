use yachtsql_ir::{BinaryOp, Expr, JoinType};

use crate::PhysicalPlan;
use crate::planner::equi_join::adjust_right_expr;
use crate::planner::predicate::{combine_predicates, split_and_predicates};

pub fn apply_cross_to_hash_join(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Filter { input, predicate } => {
            let optimized_input = apply_cross_to_hash_join(*input);

            match optimized_input {
                PhysicalPlan::CrossJoin {
                    left,
                    right,
                    schema,
                    parallel,
                    hints,
                } => {
                    let left_schema_len = left.schema().fields.len();
                    let predicates = split_and_predicates(&predicate);

                    let mut left_keys = Vec::new();
                    let mut right_keys = Vec::new();
                    let mut residual_predicates = Vec::new();

                    for pred in predicates {
                        match try_extract_single_equi_key(&pred, left_schema_len) {
                            Some((left_key, right_key)) => {
                                left_keys.push(left_key);
                                right_keys.push(right_key);
                            }
                            None => {
                                residual_predicates.push(pred);
                            }
                        }
                    }

                    if left_keys.is_empty() {
                        return PhysicalPlan::Filter {
                            input: Box::new(PhysicalPlan::CrossJoin {
                                left,
                                right,
                                schema,
                                parallel,
                                hints,
                            }),
                            predicate,
                        };
                    }

                    let hash_join = PhysicalPlan::HashJoin {
                        left,
                        right,
                        join_type: JoinType::Inner,
                        left_keys,
                        right_keys,
                        schema,
                        parallel,
                        hints,
                    };

                    match combine_predicates(residual_predicates) {
                        Some(residual) => PhysicalPlan::Filter {
                            input: Box::new(hash_join),
                            predicate: residual,
                        },
                        None => hash_join,
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
            input: Box::new(apply_cross_to_hash_join(*input)),
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
            input: Box::new(apply_cross_to_hash_join(*input)),
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
            left: Box::new(apply_cross_to_hash_join(*left)),
            right: Box::new(apply_cross_to_hash_join(*right)),
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
            left: Box::new(apply_cross_to_hash_join(*left)),
            right: Box::new(apply_cross_to_hash_join(*right)),
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
            left: Box::new(apply_cross_to_hash_join(*left)),
            right: Box::new(apply_cross_to_hash_join(*right)),
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::Sort {
            input,
            sort_exprs,
            hints,
        } => PhysicalPlan::Sort {
            input: Box::new(apply_cross_to_hash_join(*input)),
            sort_exprs,
            hints,
        },

        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => PhysicalPlan::TopN {
            input: Box::new(apply_cross_to_hash_join(*input)),
            sort_exprs,
            limit,
        },

        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => PhysicalPlan::Limit {
            input: Box::new(apply_cross_to_hash_join(*input)),
            limit,
            offset,
        },

        PhysicalPlan::Distinct { input } => PhysicalPlan::Distinct {
            input: Box::new(apply_cross_to_hash_join(*input)),
        },

        PhysicalPlan::Union {
            inputs,
            all,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::Union {
            inputs: inputs.into_iter().map(apply_cross_to_hash_join).collect(),
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
            left: Box::new(apply_cross_to_hash_join(*left)),
            right: Box::new(apply_cross_to_hash_join(*right)),
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
            left: Box::new(apply_cross_to_hash_join(*left)),
            right: Box::new(apply_cross_to_hash_join(*right)),
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
            input: Box::new(apply_cross_to_hash_join(*input)),
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
            body: Box::new(apply_cross_to_hash_join(*body)),
            parallel_ctes,
            hints,
        },

        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => PhysicalPlan::Unnest {
            input: Box::new(apply_cross_to_hash_join(*input)),
            columns,
            schema,
        },

        PhysicalPlan::Qualify { input, predicate } => PhysicalPlan::Qualify {
            input: Box::new(apply_cross_to_hash_join(*input)),
            predicate,
        },

        PhysicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => PhysicalPlan::Sample {
            input: Box::new(apply_cross_to_hash_join(*input)),
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
            source: Box::new(apply_cross_to_hash_join(*source)),
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
            query: query.map(|q| Box::new(apply_cross_to_hash_join(*q))),
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
            query: Box::new(apply_cross_to_hash_join(*query)),
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
            source: Box::new(apply_cross_to_hash_join(*source)),
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
            from: from.map(|f| Box::new(apply_cross_to_hash_join(*f))),
            filter,
        },

        PhysicalPlan::ExportData { options, query } => PhysicalPlan::ExportData {
            options,
            query: Box::new(apply_cross_to_hash_join(*query)),
        },

        PhysicalPlan::For {
            variable,
            query,
            body,
        } => PhysicalPlan::For {
            variable,
            query: Box::new(apply_cross_to_hash_join(*query)),
            body: body.into_iter().map(apply_cross_to_hash_join).collect(),
        },

        PhysicalPlan::If {
            condition,
            then_branch,
            else_branch,
        } => PhysicalPlan::If {
            condition,
            then_branch: then_branch
                .into_iter()
                .map(apply_cross_to_hash_join)
                .collect(),
            else_branch: else_branch.map(|b| b.into_iter().map(apply_cross_to_hash_join).collect()),
        },

        PhysicalPlan::While {
            condition,
            body,
            label,
        } => PhysicalPlan::While {
            condition,
            body: body.into_iter().map(apply_cross_to_hash_join).collect(),
            label,
        },

        PhysicalPlan::Loop { body, label } => PhysicalPlan::Loop {
            body: body.into_iter().map(apply_cross_to_hash_join).collect(),
            label,
        },

        PhysicalPlan::Block { body, label } => PhysicalPlan::Block {
            body: body.into_iter().map(apply_cross_to_hash_join).collect(),
            label,
        },

        PhysicalPlan::Repeat {
            body,
            until_condition,
        } => PhysicalPlan::Repeat {
            body: body.into_iter().map(apply_cross_to_hash_join).collect(),
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
            body: body.into_iter().map(apply_cross_to_hash_join).collect(),
            or_replace,
            if_not_exists,
        },

        PhysicalPlan::TryCatch {
            try_block,
            catch_block,
        } => PhysicalPlan::TryCatch {
            try_block: try_block
                .into_iter()
                .map(|(p, sql)| (apply_cross_to_hash_join(p), sql))
                .collect(),
            catch_block: catch_block
                .into_iter()
                .map(apply_cross_to_hash_join)
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
            input: Box::new(apply_cross_to_hash_join(*input)),
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
            input: Box::new(apply_cross_to_hash_join(*input)),
            analyze,
            logical_plan_text,
            physical_plan_text,
        },

        other => other,
    }
}

#[derive(PartialEq)]
enum ExprSide {
    Left,
    Right,
}

fn classify_expr_side(expr: &Expr, left_schema_len: usize) -> Option<ExprSide> {
    match expr {
        Expr::Column {
            index: Some(idx), ..
        } => {
            if *idx < left_schema_len {
                Some(ExprSide::Left)
            } else {
                Some(ExprSide::Right)
            }
        }
        Expr::Column { index: None, .. } => None,
        _ => None,
    }
}

fn try_extract_single_equi_key(expr: &Expr, left_schema_len: usize) -> Option<(Expr, Expr)> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        } => {
            let left_side = classify_expr_side(left, left_schema_len);
            let right_side = classify_expr_side(right, left_schema_len);

            match (left_side, right_side) {
                (Some(ExprSide::Left), Some(ExprSide::Right)) => {
                    Some(((**left).clone(), adjust_right_expr(right, left_schema_len)))
                }
                (Some(ExprSide::Right), Some(ExprSide::Left)) => {
                    Some(((**right).clone(), adjust_right_expr(left, left_schema_len)))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::DataType;
    use yachtsql_ir::{Literal, PlanField, PlanSchema};

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

    fn col(table: &str, name: &str, index: usize) -> Expr {
        Expr::Column {
            table: Some(table.to_string()),
            name: name.to_string(),
            index: Some(index),
        }
    }

    fn eq(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Eq,
            right: Box::new(right),
        }
    }

    fn gt(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Gt,
            right: Box::new(right),
        }
    }

    fn and(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
        }
    }

    fn lit_int(val: i64) -> Expr {
        Expr::Literal(Literal::Int64(val))
    }

    #[test]
    fn converts_filter_cross_join_with_equality_to_hash_join() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let cross = make_cross_join(left, right);

        let predicate = eq(col("a", "col0", 0), col("b", "col0", 2));

        let plan = PhysicalPlan::Filter {
            input: Box::new(cross),
            predicate,
        };

        let result = apply_cross_to_hash_join(plan);

        match result {
            PhysicalPlan::HashJoin {
                join_type,
                left_keys,
                right_keys,
                ..
            } => {
                assert_eq!(join_type, JoinType::Inner);
                assert_eq!(left_keys.len(), 1);
                assert_eq!(right_keys.len(), 1);
                assert_eq!(left_keys[0], col("a", "col0", 0));
                assert_eq!(right_keys[0], col("b", "col0", 0));
            }
            _ => panic!("Expected HashJoin, got {:?}", result),
        }
    }

    #[test]
    fn preserves_residual_filter_predicates() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let cross = make_cross_join(left, right);

        let eq_pred = eq(col("a", "col0", 0), col("b", "col0", 2));
        let residual = gt(col("a", "col1", 1), lit_int(10));
        let predicate = and(eq_pred, residual.clone());

        let plan = PhysicalPlan::Filter {
            input: Box::new(cross),
            predicate,
        };

        let result = apply_cross_to_hash_join(plan);

        match result {
            PhysicalPlan::Filter {
                input,
                predicate: result_pred,
            } => {
                assert_eq!(result_pred, residual);
                match *input {
                    PhysicalPlan::HashJoin {
                        join_type,
                        left_keys,
                        right_keys,
                        ..
                    } => {
                        assert_eq!(join_type, JoinType::Inner);
                        assert_eq!(left_keys.len(), 1);
                        assert_eq!(right_keys.len(), 1);
                    }
                    _ => panic!("Expected HashJoin inside Filter"),
                }
            }
            _ => panic!("Expected Filter, got {:?}", result),
        }
    }

    #[test]
    fn handles_multiple_equi_join_keys() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let cross = make_cross_join(left, right);

        let eq1 = eq(col("a", "col0", 0), col("b", "col0", 2));
        let eq2 = eq(col("a", "col1", 1), col("b", "col1", 3));
        let predicate = and(eq1, eq2);

        let plan = PhysicalPlan::Filter {
            input: Box::new(cross),
            predicate,
        };

        let result = apply_cross_to_hash_join(plan);

        match result {
            PhysicalPlan::HashJoin {
                join_type,
                left_keys,
                right_keys,
                ..
            } => {
                assert_eq!(join_type, JoinType::Inner);
                assert_eq!(left_keys.len(), 2);
                assert_eq!(right_keys.len(), 2);
                assert_eq!(left_keys[0], col("a", "col0", 0));
                assert_eq!(right_keys[0], col("b", "col0", 0));
                assert_eq!(left_keys[1], col("a", "col1", 1));
                assert_eq!(right_keys[1], col("b", "col1", 1));
            }
            _ => panic!("Expected HashJoin, got {:?}", result),
        }
    }

    #[test]
    fn preserves_cross_join_without_equi_condition() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let cross = make_cross_join(left, right);

        let predicate = gt(col("a", "col0", 0), col("b", "col0", 2));

        let plan = PhysicalPlan::Filter {
            input: Box::new(cross),
            predicate: predicate.clone(),
        };

        let result = apply_cross_to_hash_join(plan);

        match result {
            PhysicalPlan::Filter {
                input,
                predicate: result_pred,
            } => {
                assert_eq!(result_pred, predicate);
                assert!(matches!(*input, PhysicalPlan::CrossJoin { .. }));
            }
            _ => panic!("Expected Filter over CrossJoin, got {:?}", result),
        }
    }

    #[test]
    fn handles_cross_join_without_filter_no_change() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let cross = make_cross_join(left, right);

        let result = apply_cross_to_hash_join(cross);

        assert!(matches!(result, PhysicalPlan::CrossJoin { .. }));
    }

    #[test]
    fn recursively_transforms_nested_plans() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let cross = make_cross_join(left, right);

        let eq_pred = eq(col("a", "col0", 0), col("b", "col0", 2));

        let inner_filter = PhysicalPlan::Filter {
            input: Box::new(cross),
            predicate: eq_pred,
        };

        let plan = PhysicalPlan::Project {
            input: Box::new(inner_filter),
            expressions: vec![col("a", "col0", 0)],
            schema: make_table_schema("result", 1),
        };

        let result = apply_cross_to_hash_join(plan);

        match result {
            PhysicalPlan::Project { input, .. } => {
                assert!(matches!(*input, PhysicalPlan::HashJoin { .. }));
            }
            _ => panic!("Expected Project, got {:?}", result),
        }
    }

    #[test]
    fn handles_reversed_equi_join_columns() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let cross = make_cross_join(left, right);

        let predicate = eq(col("b", "col0", 2), col("a", "col0", 0));

        let plan = PhysicalPlan::Filter {
            input: Box::new(cross),
            predicate,
        };

        let result = apply_cross_to_hash_join(plan);

        match result {
            PhysicalPlan::HashJoin {
                join_type,
                left_keys,
                right_keys,
                ..
            } => {
                assert_eq!(join_type, JoinType::Inner);
                assert_eq!(left_keys.len(), 1);
                assert_eq!(right_keys.len(), 1);
                assert_eq!(left_keys[0], col("a", "col0", 0));
                assert_eq!(right_keys[0], col("b", "col0", 0));
            }
            _ => panic!("Expected HashJoin, got {:?}", result),
        }
    }

    #[test]
    fn handles_multiple_keys_with_residual() {
        let left = make_scan("a", 3);
        let right = make_scan("b", 3);
        let cross = make_cross_join(left, right);

        let eq1 = eq(col("a", "col0", 0), col("b", "col0", 3));
        let eq2 = eq(col("a", "col1", 1), col("b", "col1", 4));
        let residual = gt(col("a", "col2", 2), lit_int(10));

        let predicate = and(and(eq1, eq2), residual.clone());

        let plan = PhysicalPlan::Filter {
            input: Box::new(cross),
            predicate,
        };

        let result = apply_cross_to_hash_join(plan);

        match result {
            PhysicalPlan::Filter {
                input,
                predicate: result_pred,
            } => {
                assert_eq!(result_pred, residual);
                match *input {
                    PhysicalPlan::HashJoin {
                        left_keys,
                        right_keys,
                        ..
                    } => {
                        assert_eq!(left_keys.len(), 2);
                        assert_eq!(right_keys.len(), 2);
                    }
                    _ => panic!("Expected HashJoin inside Filter"),
                }
            }
            _ => panic!("Expected Filter, got {:?}", result),
        }
    }

    #[test]
    fn preserves_equi_join_on_same_side_columns() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let cross = make_cross_join(left, right);

        let predicate = eq(col("a", "col0", 0), col("a", "col1", 1));

        let plan = PhysicalPlan::Filter {
            input: Box::new(cross),
            predicate: predicate.clone(),
        };

        let result = apply_cross_to_hash_join(plan);

        match result {
            PhysicalPlan::Filter {
                input,
                predicate: result_pred,
            } => {
                assert_eq!(result_pred, predicate);
                assert!(matches!(*input, PhysicalPlan::CrossJoin { .. }));
            }
            _ => panic!("Expected Filter over CrossJoin, got {:?}", result),
        }
    }
}
