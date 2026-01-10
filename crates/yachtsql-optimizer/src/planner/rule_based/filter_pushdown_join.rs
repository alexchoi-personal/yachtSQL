#![coverage(off)]

use rustc_hash::FxHashMap;
use yachtsql_ir::{Expr, JoinType};

use super::super::predicate::{
    PredicateSide, classify_predicate_side, combine_predicates, remap_predicate_indices,
    split_and_predicates,
};
use crate::PhysicalPlan;

pub fn apply_filter_pushdown_join(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Filter { input, predicate } => {
            let optimized_input = apply_filter_pushdown_join(*input);
            try_push_filter_through_join(optimized_input, predicate)
        }
        PhysicalPlan::Project {
            input,
            expressions,
            schema,
        } => PhysicalPlan::Project {
            input: Box::new(apply_filter_pushdown_join(*input)),
            expressions,
            schema,
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
            left: Box::new(apply_filter_pushdown_join(*left)),
            right: Box::new(apply_filter_pushdown_join(*right)),
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
            left: Box::new(apply_filter_pushdown_join(*left)),
            right: Box::new(apply_filter_pushdown_join(*right)),
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
            left: Box::new(apply_filter_pushdown_join(*left)),
            right: Box::new(apply_filter_pushdown_join(*right)),
            schema,
            parallel,
            hints,
        },
        PhysicalPlan::HashAggregate {
            input,
            group_by,
            aggregates,
            schema,
            grouping_sets,
            hints,
        } => PhysicalPlan::HashAggregate {
            input: Box::new(apply_filter_pushdown_join(*input)),
            group_by,
            aggregates,
            schema,
            grouping_sets,
            hints,
        },
        PhysicalPlan::Sort {
            input,
            sort_exprs,
            hints,
        } => PhysicalPlan::Sort {
            input: Box::new(apply_filter_pushdown_join(*input)),
            sort_exprs,
            hints,
        },
        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => PhysicalPlan::Limit {
            input: Box::new(apply_filter_pushdown_join(*input)),
            limit,
            offset,
        },
        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => PhysicalPlan::TopN {
            input: Box::new(apply_filter_pushdown_join(*input)),
            sort_exprs,
            limit,
        },
        PhysicalPlan::Distinct { input } => PhysicalPlan::Distinct {
            input: Box::new(apply_filter_pushdown_join(*input)),
        },
        PhysicalPlan::Union {
            inputs,
            all,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::Union {
            inputs: inputs.into_iter().map(apply_filter_pushdown_join).collect(),
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
            left: Box::new(apply_filter_pushdown_join(*left)),
            right: Box::new(apply_filter_pushdown_join(*right)),
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
            left: Box::new(apply_filter_pushdown_join(*left)),
            right: Box::new(apply_filter_pushdown_join(*right)),
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
            input: Box::new(apply_filter_pushdown_join(*input)),
            window_exprs,
            schema,
            hints,
        },
        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => PhysicalPlan::Unnest {
            input: Box::new(apply_filter_pushdown_join(*input)),
            columns,
            schema,
        },
        PhysicalPlan::Qualify { input, predicate } => PhysicalPlan::Qualify {
            input: Box::new(apply_filter_pushdown_join(*input)),
            predicate,
        },
        PhysicalPlan::WithCte {
            ctes,
            body,
            parallel_ctes,
            hints,
        } => PhysicalPlan::WithCte {
            ctes,
            body: Box::new(apply_filter_pushdown_join(*body)),
            parallel_ctes,
            hints,
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
            input: Box::new(apply_filter_pushdown_join(*input)),
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
            input: Box::new(apply_filter_pushdown_join(*input)),
            analyze,
            logical_plan_text,
            physical_plan_text,
        },
        other => other,
    }
}

fn try_push_filter_through_join(input: PhysicalPlan, predicate: Expr) -> PhysicalPlan {
    match input {
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
            let left_schema_len = left.schema().fields.len();
            let (left_preds, right_preds, remaining) =
                classify_and_remap_predicates(&predicate, left_schema_len, join_type);

            let new_left = wrap_with_filter(*left, left_preds);
            let new_right = wrap_with_filter(*right, right_preds);

            let join = PhysicalPlan::HashJoin {
                left: Box::new(new_left),
                right: Box::new(new_right),
                join_type,
                left_keys,
                right_keys,
                schema,
                parallel,
                hints,
            };

            wrap_with_filter(join, remaining)
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
            let left_schema_len = left.schema().fields.len();
            let (left_preds, right_preds, remaining) =
                classify_and_remap_predicates(&predicate, left_schema_len, join_type);

            let new_left = wrap_with_filter(*left, left_preds);
            let new_right = wrap_with_filter(*right, right_preds);

            let join = PhysicalPlan::NestedLoopJoin {
                left: Box::new(new_left),
                right: Box::new(new_right),
                join_type,
                condition,
                schema,
                parallel,
                hints,
            };

            wrap_with_filter(join, remaining)
        }
        PhysicalPlan::CrossJoin {
            left,
            right,
            schema,
            parallel,
            hints,
        } => {
            let left_schema_len = left.schema().fields.len();
            let (left_preds, right_preds, remaining) =
                classify_and_remap_predicates(&predicate, left_schema_len, JoinType::Inner);

            let new_left = wrap_with_filter(*left, left_preds);
            let new_right = wrap_with_filter(*right, right_preds);

            let join = PhysicalPlan::CrossJoin {
                left: Box::new(new_left),
                right: Box::new(new_right),
                schema,
                parallel,
                hints,
            };

            wrap_with_filter(join, remaining)
        }
        other => PhysicalPlan::Filter {
            input: Box::new(other),
            predicate,
        },
    }
}

fn classify_and_remap_predicates(
    predicate: &Expr,
    left_schema_len: usize,
    join_type: JoinType,
) -> (Vec<Expr>, Vec<Expr>, Vec<Expr>) {
    let conjuncts = split_and_predicates(predicate);

    let mut left_preds = Vec::new();
    let mut right_preds = Vec::new();
    let mut remaining = Vec::new();

    let (can_push_left, can_push_right) = match join_type {
        JoinType::Inner | JoinType::Cross => (true, true),
        JoinType::Left => (true, false),
        JoinType::Right => (false, true),
        JoinType::Full => (false, false),
    };

    for conjunct in conjuncts {
        match classify_predicate_side(&conjunct, left_schema_len) {
            Some(PredicateSide::Left) if can_push_left => {
                left_preds.push(conjunct);
            }
            Some(PredicateSide::Right) if can_push_right => {
                if let Some(remapped) = remap_right_predicate(&conjunct, left_schema_len) {
                    right_preds.push(remapped);
                } else {
                    remaining.push(conjunct);
                }
            }
            _ => {
                remaining.push(conjunct);
            }
        }
    }

    (left_preds, right_preds, remaining)
}

fn remap_right_predicate(predicate: &Expr, left_schema_len: usize) -> Option<Expr> {
    let mut mapping = FxHashMap::default();
    collect_column_indices(predicate, &mut mapping, left_schema_len);

    if mapping.is_empty() {
        return Some(predicate.clone());
    }

    remap_predicate_indices(predicate, &mapping)
}

fn collect_column_indices(
    expr: &Expr,
    mapping: &mut FxHashMap<usize, usize>,
    left_schema_len: usize,
) {
    match expr {
        Expr::Column {
            index: Some(idx), ..
        } => {
            if *idx >= left_schema_len {
                mapping.insert(*idx, *idx - left_schema_len);
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_column_indices(left, mapping, left_schema_len);
            collect_column_indices(right, mapping, left_schema_len);
        }
        Expr::UnaryOp { expr, .. } => {
            collect_column_indices(expr, mapping, left_schema_len);
        }
        Expr::IsNull { expr, .. } => {
            collect_column_indices(expr, mapping, left_schema_len);
        }
        Expr::IsDistinctFrom { left, right, .. } => {
            collect_column_indices(left, mapping, left_schema_len);
            collect_column_indices(right, mapping, left_schema_len);
        }
        Expr::Cast { expr, .. } => {
            collect_column_indices(expr, mapping, left_schema_len);
        }
        Expr::Like { expr, pattern, .. } => {
            collect_column_indices(expr, mapping, left_schema_len);
            collect_column_indices(pattern, mapping, left_schema_len);
        }
        Expr::InList { expr, list, .. } => {
            collect_column_indices(expr, mapping, left_schema_len);
            for item in list {
                collect_column_indices(item, mapping, left_schema_len);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_column_indices(expr, mapping, left_schema_len);
            collect_column_indices(low, mapping, left_schema_len);
            collect_column_indices(high, mapping, left_schema_len);
        }
        Expr::ScalarFunction { args, .. } => {
            for arg in args {
                collect_column_indices(arg, mapping, left_schema_len);
            }
        }
        Expr::Case {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_column_indices(op, mapping, left_schema_len);
            }
            for clause in when_clauses {
                collect_column_indices(&clause.condition, mapping, left_schema_len);
                collect_column_indices(&clause.result, mapping, left_schema_len);
            }
            if let Some(else_expr) = else_result {
                collect_column_indices(else_expr, mapping, left_schema_len);
            }
        }
        Expr::Subquery { .. } | Expr::ScalarSubquery { .. } | Expr::InSubquery { .. } => {}
        _ => {}
    }
}

fn wrap_with_filter(plan: PhysicalPlan, predicates: Vec<Expr>) -> PhysicalPlan {
    match combine_predicates(predicates) {
        Some(combined) => PhysicalPlan::Filter {
            input: Box::new(plan),
            predicate: combined,
        },
        None => plan,
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_ir::{BinaryOp, Literal, PlanField, PlanSchema};

    use super::*;

    fn make_scan(name: &str, columns: &[&str]) -> PhysicalPlan {
        PhysicalPlan::TableScan {
            table_name: name.to_string(),
            schema: PlanSchema {
                fields: columns
                    .iter()
                    .map(|c| PlanField {
                        name: c.to_string(),
                        data_type: yachtsql_common::types::DataType::Int64,
                        nullable: true,
                        table: Some(name.to_string()),
                    })
                    .collect(),
            },
            projection: None,
            row_count: None,
        }
    }

    fn make_col(index: usize) -> Expr {
        Expr::Column {
            table: None,
            name: format!("col{}", index),
            index: Some(index),
        }
    }

    fn make_eq_literal(col_idx: usize, val: i64) -> Expr {
        Expr::BinaryOp {
            left: Box::new(make_col(col_idx)),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Int64(val))),
        }
    }

    fn make_hash_join(
        left: PhysicalPlan,
        right: PhysicalPlan,
        join_type: JoinType,
    ) -> PhysicalPlan {
        let left_schema = left.schema();
        let right_schema = right.schema();
        let mut fields = left_schema.fields.clone();
        for col in right_schema.fields.iter() {
            fields.push(col.clone());
        }

        PhysicalPlan::HashJoin {
            left: Box::new(left),
            right: Box::new(right),
            join_type,
            left_keys: vec![make_col(0)],
            right_keys: vec![make_col(0)],
            schema: PlanSchema { fields },
            parallel: false,
            hints: Default::default(),
        }
    }

    #[test]
    fn filter_pushed_to_left_side_of_inner_join() {
        let left = make_scan("orders", &["id", "customer_id", "amount"]);
        let right = make_scan("customers", &["id", "name", "country"]);
        let join = make_hash_join(left, right, JoinType::Inner);

        let filter = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate: make_eq_literal(2, 100),
        };

        let optimized = apply_filter_pushdown_join(filter);

        match optimized {
            PhysicalPlan::HashJoin { left, .. } => match *left {
                PhysicalPlan::Filter { predicate, .. } => {
                    assert_eq!(predicate, make_eq_literal(2, 100));
                }
                other => panic!("Expected Filter on left, got {:?}", other),
            },
            other => panic!("Expected HashJoin, got {:?}", other),
        }
    }

    #[test]
    fn filter_pushed_to_right_side_of_inner_join() {
        let left = make_scan("orders", &["id", "customer_id", "amount"]);
        let right = make_scan("customers", &["id", "name", "country"]);
        let join = make_hash_join(left, right, JoinType::Inner);

        let filter = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate: make_eq_literal(5, 42),
        };

        let optimized = apply_filter_pushdown_join(filter);

        match optimized {
            PhysicalPlan::HashJoin { right, .. } => match *right {
                PhysicalPlan::Filter { predicate, .. } => match &predicate {
                    Expr::BinaryOp { left, op, right } => {
                        assert_eq!(*op, BinaryOp::Eq);
                        match left.as_ref() {
                            Expr::Column {
                                index: Some(idx), ..
                            } => {
                                assert_eq!(*idx, 2);
                            }
                            other => panic!("Expected Column, got {:?}", other),
                        }
                        assert_eq!(**right, Expr::Literal(Literal::Int64(42)));
                    }
                    other => panic!("Expected BinaryOp, got {:?}", other),
                },
                other => panic!("Expected Filter on right, got {:?}", other),
            },
            other => panic!("Expected HashJoin, got {:?}", other),
        }
    }

    #[test]
    fn filter_not_pushed_to_right_side_of_left_join() {
        let left = make_scan("orders", &["id", "customer_id", "amount"]);
        let right = make_scan("customers", &["id", "name", "country"]);
        let join = make_hash_join(left, right, JoinType::Left);

        let filter = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate: make_eq_literal(5, 42),
        };

        let optimized = apply_filter_pushdown_join(filter);

        match optimized {
            PhysicalPlan::Filter {
                input,
                predicate: outer_pred,
            } => {
                assert_eq!(outer_pred, make_eq_literal(5, 42));
                match *input {
                    PhysicalPlan::HashJoin { right, .. } => match *right {
                        PhysicalPlan::TableScan { .. } => {}
                        other => panic!("Expected TableScan on right, got {:?}", other),
                    },
                    other => panic!("Expected HashJoin, got {:?}", other),
                }
            }
            other => panic!("Expected Filter, got {:?}", other),
        }
    }

    #[test]
    fn filter_pushed_to_left_side_of_left_join() {
        let left = make_scan("orders", &["id", "customer_id", "amount"]);
        let right = make_scan("customers", &["id", "name", "country"]);
        let join = make_hash_join(left, right, JoinType::Left);

        let filter = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate: make_eq_literal(2, 100),
        };

        let optimized = apply_filter_pushdown_join(filter);

        match optimized {
            PhysicalPlan::HashJoin { left, .. } => match *left {
                PhysicalPlan::Filter { predicate, .. } => {
                    assert_eq!(predicate, make_eq_literal(2, 100));
                }
                other => panic!("Expected Filter on left, got {:?}", other),
            },
            other => panic!("Expected HashJoin, got {:?}", other),
        }
    }

    #[test]
    fn mixed_predicates_split_correctly() {
        let left = make_scan("orders", &["id", "customer_id", "amount"]);
        let right = make_scan("customers", &["id", "name", "country"]);
        let join = make_hash_join(left, right, JoinType::Inner);

        let left_pred = make_eq_literal(2, 100);
        let right_pred = make_eq_literal(5, 42);
        let combined = Expr::BinaryOp {
            left: Box::new(left_pred.clone()),
            op: BinaryOp::And,
            right: Box::new(right_pred),
        };

        let filter = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate: combined,
        };

        let optimized = apply_filter_pushdown_join(filter);

        match optimized {
            PhysicalPlan::HashJoin { left, right, .. } => {
                match *left {
                    PhysicalPlan::Filter { predicate, .. } => {
                        assert_eq!(predicate, make_eq_literal(2, 100));
                    }
                    other => panic!("Expected Filter on left, got {:?}", other),
                }
                match *right {
                    PhysicalPlan::Filter { predicate, .. } => match &predicate {
                        Expr::BinaryOp { left, op, right } => {
                            assert_eq!(*op, BinaryOp::Eq);
                            match left.as_ref() {
                                Expr::Column {
                                    index: Some(idx), ..
                                } => {
                                    assert_eq!(*idx, 2);
                                }
                                other => panic!("Expected Column, got {:?}", other),
                            }
                            assert_eq!(**right, Expr::Literal(Literal::Int64(42)));
                        }
                        other => panic!("Expected BinaryOp, got {:?}", other),
                    },
                    other => panic!("Expected Filter on right, got {:?}", other),
                }
            }
            other => panic!("Expected HashJoin, got {:?}", other),
        }
    }

    #[test]
    fn both_side_predicate_stays_on_top() {
        let left = make_scan("orders", &["id", "customer_id", "amount"]);
        let right = make_scan("customers", &["id", "name", "country"]);
        let join = make_hash_join(left, right, JoinType::Inner);

        let both_pred = Expr::BinaryOp {
            left: Box::new(make_col(2)),
            op: BinaryOp::Eq,
            right: Box::new(make_col(5)),
        };

        let filter = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate: both_pred.clone(),
        };

        let optimized = apply_filter_pushdown_join(filter);

        match optimized {
            PhysicalPlan::Filter { predicate, .. } => {
                assert_eq!(predicate, both_pred);
            }
            other => panic!("Expected Filter on top, got {:?}", other),
        }
    }

    #[test]
    fn full_outer_join_no_pushdown() {
        let left = make_scan("orders", &["id", "customer_id", "amount"]);
        let right = make_scan("customers", &["id", "name", "country"]);
        let join = make_hash_join(left, right, JoinType::Full);

        let filter = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate: make_eq_literal(2, 100),
        };

        let optimized = apply_filter_pushdown_join(filter);

        match optimized {
            PhysicalPlan::Filter { input, .. } => match *input {
                PhysicalPlan::HashJoin { left, .. } => match *left {
                    PhysicalPlan::TableScan { .. } => {}
                    other => panic!("Expected TableScan on left (no pushdown), got {:?}", other),
                },
                other => panic!("Expected HashJoin, got {:?}", other),
            },
            other => panic!("Expected Filter on top, got {:?}", other),
        }
    }
}
