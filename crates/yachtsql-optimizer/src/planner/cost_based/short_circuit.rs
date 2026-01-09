use yachtsql_ir::{BinaryOp, Expr};

use crate::PhysicalPlan;
use crate::join_order::CostModel;
use crate::planner::predicate::{estimate_predicate_selectivity, split_and_predicates};

pub fn apply_short_circuit_ordering(plan: PhysicalPlan) -> PhysicalPlan {
    let cost_model = CostModel::new();
    apply_short_circuit_ordering_with_cost_model(plan, &cost_model)
}

fn apply_short_circuit_ordering_with_cost_model(
    plan: PhysicalPlan,
    cost_model: &CostModel,
) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(apply_short_circuit_ordering_with_cost_model(
                *input, cost_model,
            )),
            predicate: reorder_predicate(predicate, cost_model),
        },

        PhysicalPlan::Qualify { input, predicate } => PhysicalPlan::Qualify {
            input: Box::new(apply_short_circuit_ordering_with_cost_model(
                *input, cost_model,
            )),
            predicate: reorder_predicate(predicate, cost_model),
        },

        PhysicalPlan::Project {
            input,
            expressions,
            schema,
        } => PhysicalPlan::Project {
            input: Box::new(apply_short_circuit_ordering_with_cost_model(
                *input, cost_model,
            )),
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
            input: Box::new(apply_short_circuit_ordering_with_cost_model(
                *input, cost_model,
            )),
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
            left: Box::new(apply_short_circuit_ordering_with_cost_model(
                *left, cost_model,
            )),
            right: Box::new(apply_short_circuit_ordering_with_cost_model(
                *right, cost_model,
            )),
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
            left: Box::new(apply_short_circuit_ordering_with_cost_model(
                *left, cost_model,
            )),
            right: Box::new(apply_short_circuit_ordering_with_cost_model(
                *right, cost_model,
            )),
            join_type,
            condition: condition.map(|c| reorder_predicate(c, cost_model)),
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
            left: Box::new(apply_short_circuit_ordering_with_cost_model(
                *left, cost_model,
            )),
            right: Box::new(apply_short_circuit_ordering_with_cost_model(
                *right, cost_model,
            )),
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::Sort {
            input,
            sort_exprs,
            hints,
        } => PhysicalPlan::Sort {
            input: Box::new(apply_short_circuit_ordering_with_cost_model(
                *input, cost_model,
            )),
            sort_exprs,
            hints,
        },

        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => PhysicalPlan::TopN {
            input: Box::new(apply_short_circuit_ordering_with_cost_model(
                *input, cost_model,
            )),
            sort_exprs,
            limit,
        },

        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => PhysicalPlan::Limit {
            input: Box::new(apply_short_circuit_ordering_with_cost_model(
                *input, cost_model,
            )),
            limit,
            offset,
        },

        PhysicalPlan::Distinct { input } => PhysicalPlan::Distinct {
            input: Box::new(apply_short_circuit_ordering_with_cost_model(
                *input, cost_model,
            )),
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
                .map(|p| apply_short_circuit_ordering_with_cost_model(p, cost_model))
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
            left: Box::new(apply_short_circuit_ordering_with_cost_model(
                *left, cost_model,
            )),
            right: Box::new(apply_short_circuit_ordering_with_cost_model(
                *right, cost_model,
            )),
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
            left: Box::new(apply_short_circuit_ordering_with_cost_model(
                *left, cost_model,
            )),
            right: Box::new(apply_short_circuit_ordering_with_cost_model(
                *right, cost_model,
            )),
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
            input: Box::new(apply_short_circuit_ordering_with_cost_model(
                *input, cost_model,
            )),
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
            body: Box::new(apply_short_circuit_ordering_with_cost_model(
                *body, cost_model,
            )),
            parallel_ctes,
            hints,
        },

        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => PhysicalPlan::Unnest {
            input: Box::new(apply_short_circuit_ordering_with_cost_model(
                *input, cost_model,
            )),
            columns,
            schema,
        },

        PhysicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => PhysicalPlan::Sample {
            input: Box::new(apply_short_circuit_ordering_with_cost_model(
                *input, cost_model,
            )),
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
            source: Box::new(apply_short_circuit_ordering_with_cost_model(
                *source, cost_model,
            )),
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
            query: query
                .map(|q| Box::new(apply_short_circuit_ordering_with_cost_model(*q, cost_model))),
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
            query: Box::new(apply_short_circuit_ordering_with_cost_model(
                *query, cost_model,
            )),
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
            source: Box::new(apply_short_circuit_ordering_with_cost_model(
                *source, cost_model,
            )),
            on: reorder_predicate(on, cost_model),
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
            from: from
                .map(|f| Box::new(apply_short_circuit_ordering_with_cost_model(*f, cost_model))),
            filter: filter.map(|f| reorder_predicate(f, cost_model)),
        },

        PhysicalPlan::ExportData { options, query } => PhysicalPlan::ExportData {
            options,
            query: Box::new(apply_short_circuit_ordering_with_cost_model(
                *query, cost_model,
            )),
        },

        PhysicalPlan::For {
            variable,
            query,
            body,
        } => PhysicalPlan::For {
            variable,
            query: Box::new(apply_short_circuit_ordering_with_cost_model(
                *query, cost_model,
            )),
            body: body
                .into_iter()
                .map(|p| apply_short_circuit_ordering_with_cost_model(p, cost_model))
                .collect(),
        },

        PhysicalPlan::If {
            condition,
            then_branch,
            else_branch,
        } => PhysicalPlan::If {
            condition: reorder_predicate(condition, cost_model),
            then_branch: then_branch
                .into_iter()
                .map(|p| apply_short_circuit_ordering_with_cost_model(p, cost_model))
                .collect(),
            else_branch: else_branch.map(|b| {
                b.into_iter()
                    .map(|p| apply_short_circuit_ordering_with_cost_model(p, cost_model))
                    .collect()
            }),
        },

        PhysicalPlan::While {
            condition,
            body,
            label,
        } => PhysicalPlan::While {
            condition: reorder_predicate(condition, cost_model),
            body: body
                .into_iter()
                .map(|p| apply_short_circuit_ordering_with_cost_model(p, cost_model))
                .collect(),
            label,
        },

        PhysicalPlan::Loop { body, label } => PhysicalPlan::Loop {
            body: body
                .into_iter()
                .map(|p| apply_short_circuit_ordering_with_cost_model(p, cost_model))
                .collect(),
            label,
        },

        PhysicalPlan::Block { body, label } => PhysicalPlan::Block {
            body: body
                .into_iter()
                .map(|p| apply_short_circuit_ordering_with_cost_model(p, cost_model))
                .collect(),
            label,
        },

        PhysicalPlan::Repeat {
            body,
            until_condition,
        } => PhysicalPlan::Repeat {
            body: body
                .into_iter()
                .map(|p| apply_short_circuit_ordering_with_cost_model(p, cost_model))
                .collect(),
            until_condition: reorder_predicate(until_condition, cost_model),
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
                .map(|p| apply_short_circuit_ordering_with_cost_model(p, cost_model))
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
                .map(|(p, sql)| {
                    (
                        apply_short_circuit_ordering_with_cost_model(p, cost_model),
                        sql,
                    )
                })
                .collect(),
            catch_block: catch_block
                .into_iter()
                .map(|p| apply_short_circuit_ordering_with_cost_model(p, cost_model))
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
            input: Box::new(apply_short_circuit_ordering_with_cost_model(
                *input, cost_model,
            )),
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
            input: Box::new(apply_short_circuit_ordering_with_cost_model(
                *input, cost_model,
            )),
            analyze,
            logical_plan_text,
            physical_plan_text,
        },

        PhysicalPlan::Assert { condition, message } => PhysicalPlan::Assert {
            condition: reorder_predicate(condition, cost_model),
            message,
        },

        PhysicalPlan::Delete {
            table_name,
            alias,
            filter,
        } => PhysicalPlan::Delete {
            table_name,
            alias,
            filter: filter.map(|f| reorder_predicate(f, cost_model)),
        },

        other => other,
    }
}

fn reorder_predicate(expr: Expr, cost_model: &CostModel) -> Expr {
    match &expr {
        Expr::BinaryOp {
            op: BinaryOp::And, ..
        } => {
            let predicates = split_and_predicates(&expr);
            if predicates.len() <= 1 {
                return reorder_subexpressions(expr, cost_model);
            }
            let reordered_predicates: Vec<Expr> = predicates
                .into_iter()
                .map(|p| reorder_predicate(p, cost_model))
                .collect();
            combine_predicates_by_selectivity(reordered_predicates, BinaryOp::And, cost_model)
        }
        Expr::BinaryOp {
            op: BinaryOp::Or, ..
        } => {
            let predicates = split_or_predicates(&expr);
            if predicates.len() <= 1 {
                return reorder_subexpressions(expr, cost_model);
            }
            let reordered_predicates: Vec<Expr> = predicates
                .into_iter()
                .map(|p| reorder_predicate(p, cost_model))
                .collect();
            combine_predicates_by_selectivity(reordered_predicates, BinaryOp::Or, cost_model)
        }
        _ => reorder_subexpressions(expr, cost_model),
    }
}

fn split_or_predicates(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOp::Or,
            right,
        } => {
            let mut result = split_or_predicates(left);
            result.extend(split_or_predicates(right));
            result
        }
        other => vec![other.clone()],
    }
}

fn combine_predicates_by_selectivity(
    mut predicates: Vec<Expr>,
    op: BinaryOp,
    cost_model: &CostModel,
) -> Expr {
    match op {
        BinaryOp::And => {
            predicates.sort_by(|a, b| {
                let sel_a = estimate_predicate_selectivity(a, cost_model);
                let sel_b = estimate_predicate_selectivity(b, cost_model);
                sel_a
                    .partial_cmp(&sel_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }
        BinaryOp::Or => {
            predicates.sort_by(|a, b| {
                let sel_a = estimate_predicate_selectivity(a, cost_model);
                let sel_b = estimate_predicate_selectivity(b, cost_model);
                sel_b
                    .partial_cmp(&sel_a)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }
        _ => panic!("combine_predicates_by_selectivity called with non-logical operator"),
    }

    let mut iter = predicates.into_iter();
    let first = iter.next().expect("predicates should not be empty");
    iter.fold(first, |acc, pred| Expr::BinaryOp {
        left: Box::new(acc),
        op,
        right: Box::new(pred),
    })
}

fn reorder_subexpressions(expr: Expr, cost_model: &CostModel) -> Expr {
    match expr {
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(reorder_predicate(*left, cost_model)),
            op,
            right: Box::new(reorder_predicate(*right, cost_model)),
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op,
            expr: Box::new(reorder_predicate(*expr, cost_model)),
        },
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(reorder_predicate(*expr, cost_model)),
            negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(reorder_predicate(*expr, cost_model)),
            list: list
                .into_iter()
                .map(|e| reorder_predicate(e, cost_model))
                .collect(),
            negated,
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(reorder_predicate(*expr, cost_model)),
            low: Box::new(reorder_predicate(*low, cost_model)),
            high: Box::new(reorder_predicate(*high, cost_model)),
            negated,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => Expr::Like {
            expr: Box::new(reorder_predicate(*expr, cost_model)),
            pattern: Box::new(reorder_predicate(*pattern, cost_model)),
            negated,
            case_insensitive,
        },
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => Expr::Case {
            operand: operand.map(|e| Box::new(reorder_predicate(*e, cost_model))),
            when_clauses: when_clauses
                .into_iter()
                .map(|wc| yachtsql_ir::WhenClause {
                    condition: reorder_predicate(wc.condition, cost_model),
                    result: reorder_predicate(wc.result, cost_model),
                })
                .collect(),
            else_result: else_result.map(|e| Box::new(reorder_predicate(*e, cost_model))),
        },
        Expr::Cast {
            expr,
            data_type,
            safe,
        } => Expr::Cast {
            expr: Box::new(reorder_predicate(*expr, cost_model)),
            data_type,
            safe,
        },
        Expr::ScalarFunction { name, args } => Expr::ScalarFunction {
            name,
            args: args
                .into_iter()
                .map(|a| reorder_predicate(a, cost_model))
                .collect(),
        },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::DataType;
    use yachtsql_ir::{Literal, PlanField, PlanSchema};

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

    fn make_eq_predicate(col_name: &str, col_index: usize, value: i64) -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: None,
                name: col_name.to_string(),
                index: Some(col_index),
            }),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Int64(value))),
        }
    }

    fn make_lt_predicate(col_name: &str, col_index: usize, value: i64) -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: None,
                name: col_name.to_string(),
                index: Some(col_index),
            }),
            op: BinaryOp::Lt,
            right: Box::new(Expr::Literal(Literal::Int64(value))),
        }
    }

    fn make_is_null_predicate(col_name: &str, col_index: usize) -> Expr {
        Expr::IsNull {
            expr: Box::new(Expr::Column {
                table: None,
                name: col_name.to_string(),
                index: Some(col_index),
            }),
            negated: false,
        }
    }

    fn make_is_not_null_predicate(col_name: &str, col_index: usize) -> Expr {
        Expr::IsNull {
            expr: Box::new(Expr::Column {
                table: None,
                name: col_name.to_string(),
                index: Some(col_index),
            }),
            negated: true,
        }
    }

    fn make_like_predicate(col_name: &str, col_index: usize, pattern: &str) -> Expr {
        Expr::Like {
            expr: Box::new(Expr::Column {
                table: None,
                name: col_name.to_string(),
                index: Some(col_index),
            }),
            pattern: Box::new(Expr::Literal(Literal::String(pattern.to_string()))),
            negated: false,
            case_insensitive: false,
        }
    }

    fn combine_and(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
        }
    }

    fn combine_or(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Or,
            right: Box::new(right),
        }
    }

    fn collect_and_predicates(expr: &Expr) -> Vec<Expr> {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                let mut result = collect_and_predicates(left);
                result.push(right.as_ref().clone());
                result
            }
            other => vec![other.clone()],
        }
    }

    fn collect_or_predicates(expr: &Expr) -> Vec<Expr> {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOp::Or,
                right,
            } => {
                let mut result = collect_or_predicates(left);
                result.push(right.as_ref().clone());
                result
            }
            other => vec![other.clone()],
        }
    }

    #[test]
    fn test_reorders_and_predicates_by_selectivity() {
        let scan = make_scan("t", 3);

        let is_not_null = make_is_not_null_predicate("a", 0);
        let lt_pred = make_lt_predicate("b", 1, 100);
        let eq_pred = make_eq_predicate("c", 2, 42);
        let is_null = make_is_null_predicate("d", 0);

        let combined = combine_and(
            combine_and(
                combine_and(is_not_null.clone(), lt_pred.clone()),
                eq_pred.clone(),
            ),
            is_null.clone(),
        );

        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: combined,
        };

        let result = apply_short_circuit_ordering(plan);

        match result {
            PhysicalPlan::Filter { predicate, .. } => {
                let collected = collect_and_predicates(&predicate);
                assert_eq!(collected.len(), 4);
                assert_eq!(collected[0], is_null);
                assert_eq!(collected[1], eq_pred);
                assert_eq!(collected[2], lt_pred);
                assert_eq!(collected[3], is_not_null);
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn test_reorders_or_predicates_by_selectivity() {
        let scan = make_scan("t", 3);

        let is_null = make_is_null_predicate("a", 0);
        let eq_pred = make_eq_predicate("b", 1, 42);
        let lt_pred = make_lt_predicate("c", 2, 100);
        let is_not_null = make_is_not_null_predicate("d", 0);

        let combined = combine_or(
            combine_or(
                combine_or(is_null.clone(), eq_pred.clone()),
                lt_pred.clone(),
            ),
            is_not_null.clone(),
        );

        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: combined,
        };

        let result = apply_short_circuit_ordering(plan);

        match result {
            PhysicalPlan::Filter { predicate, .. } => {
                let collected = collect_or_predicates(&predicate);
                assert_eq!(collected.len(), 4);
                assert_eq!(collected[0], is_not_null);
                assert_eq!(collected[1], lt_pred);
                assert_eq!(collected[2], eq_pred);
                assert_eq!(collected[3], is_null);
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn test_handles_flat_and_chain() {
        let scan = make_scan("t", 3);

        let eq1 = make_eq_predicate("a", 0, 1);
        let eq2 = make_eq_predicate("b", 1, 2);
        let eq3 = make_eq_predicate("c", 2, 3);

        let combined = combine_and(combine_and(eq1.clone(), eq2.clone()), eq3.clone());

        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: combined,
        };

        let result = apply_short_circuit_ordering(plan);

        match result {
            PhysicalPlan::Filter { predicate, .. } => {
                let collected = collect_and_predicates(&predicate);
                assert_eq!(collected.len(), 3);
                for p in &collected {
                    match p {
                        Expr::BinaryOp {
                            op: BinaryOp::Eq, ..
                        } => {}
                        _ => panic!("Expected equality predicate"),
                    }
                }
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn test_handles_flat_or_chain() {
        let scan = make_scan("t", 3);

        let lt1 = make_lt_predicate("a", 0, 10);
        let lt2 = make_lt_predicate("b", 1, 20);
        let lt3 = make_lt_predicate("c", 2, 30);

        let combined = combine_or(combine_or(lt1.clone(), lt2.clone()), lt3.clone());

        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: combined,
        };

        let result = apply_short_circuit_ordering(plan);

        match result {
            PhysicalPlan::Filter { predicate, .. } => {
                let collected = collect_or_predicates(&predicate);
                assert_eq!(collected.len(), 3);
                for p in &collected {
                    match p {
                        Expr::BinaryOp {
                            op: BinaryOp::Lt, ..
                        } => {}
                        _ => panic!("Expected less-than predicate"),
                    }
                }
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn test_handles_nested_and_chains() {
        let scan = make_scan("t", 4);

        let is_null = make_is_null_predicate("a", 0);
        let eq = make_eq_predicate("b", 1, 42);
        let lt = make_lt_predicate("c", 2, 100);
        let is_not_null = make_is_not_null_predicate("d", 3);

        let left_and = combine_and(is_not_null.clone(), lt.clone());
        let right_and = combine_and(eq.clone(), is_null.clone());
        let combined = combine_and(left_and, right_and);

        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: combined,
        };

        let result = apply_short_circuit_ordering(plan);

        match result {
            PhysicalPlan::Filter { predicate, .. } => {
                let collected = collect_and_predicates(&predicate);
                assert_eq!(collected.len(), 4);
                assert_eq!(collected[0], is_null);
                assert_eq!(collected[1], eq);
                assert_eq!(collected[2], lt);
                assert_eq!(collected[3], is_not_null);
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn test_preserves_single_predicates() {
        let scan = make_scan("t", 1);
        let eq = make_eq_predicate("a", 0, 42);

        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: eq.clone(),
        };

        let result = apply_short_circuit_ordering(plan);

        match result {
            PhysicalPlan::Filter { predicate, .. } => {
                assert_eq!(predicate, eq);
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn test_preserves_non_logical_binary_ops() {
        let scan = make_scan("t", 1);
        let lt = make_lt_predicate("a", 0, 100);

        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: lt.clone(),
        };

        let result = apply_short_circuit_ordering(plan);

        match result {
            PhysicalPlan::Filter { predicate, .. } => {
                assert_eq!(predicate, lt);
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn test_handles_mixed_and_or_predicates() {
        let scan = make_scan("t", 3);

        let is_null = make_is_null_predicate("a", 0);
        let eq = make_eq_predicate("b", 1, 42);
        let lt = make_lt_predicate("c", 2, 100);
        let is_not_null = make_is_not_null_predicate("d", 0);

        let or_part = combine_or(lt.clone(), is_not_null.clone());
        let combined = combine_and(combine_and(is_null.clone(), eq.clone()), or_part);

        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: combined,
        };

        let result = apply_short_circuit_ordering(plan);

        match result {
            PhysicalPlan::Filter { predicate, .. } => {
                let collected = collect_and_predicates(&predicate);
                assert_eq!(collected.len(), 3);
                assert_eq!(collected[0], is_null);
                assert_eq!(collected[1], eq);

                match &collected[2] {
                    Expr::BinaryOp {
                        op: BinaryOp::Or, ..
                    } => {
                        let or_preds = collect_or_predicates(&collected[2]);
                        assert_eq!(or_preds.len(), 2);
                        assert_eq!(or_preds[0], is_not_null);
                        assert_eq!(or_preds[1], lt);
                    }
                    _ => panic!("Expected OR predicate"),
                }
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn test_traverses_nested_plans() {
        let scan = make_scan("t", 2);

        let inner_pred = combine_and(
            make_is_not_null_predicate("a", 0),
            make_is_null_predicate("b", 1),
        );
        let inner_filter = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: inner_pred,
        };

        let outer_pred = combine_and(
            make_lt_predicate("c", 0, 100),
            make_eq_predicate("d", 1, 42),
        );
        let outer_filter = PhysicalPlan::Filter {
            input: Box::new(inner_filter),
            predicate: outer_pred,
        };

        let result = apply_short_circuit_ordering(outer_filter);

        match result {
            PhysicalPlan::Filter {
                input, predicate, ..
            } => {
                let outer_collected = collect_and_predicates(&predicate);
                assert_eq!(outer_collected.len(), 2);
                match &outer_collected[0] {
                    Expr::BinaryOp {
                        op: BinaryOp::Eq, ..
                    } => {}
                    _ => panic!("Expected equality predicate first in outer"),
                }
                match &outer_collected[1] {
                    Expr::BinaryOp {
                        op: BinaryOp::Lt, ..
                    } => {}
                    _ => panic!("Expected less-than predicate second in outer"),
                }

                match *input {
                    PhysicalPlan::Filter { predicate, .. } => {
                        let inner_collected = collect_and_predicates(&predicate);
                        assert_eq!(inner_collected.len(), 2);
                        match &inner_collected[0] {
                            Expr::IsNull { negated: false, .. } => {}
                            _ => panic!("Expected IS NULL predicate first in inner"),
                        }
                        match &inner_collected[1] {
                            Expr::IsNull { negated: true, .. } => {}
                            _ => panic!("Expected IS NOT NULL predicate second in inner"),
                        }
                    }
                    _ => panic!("Expected inner Filter plan"),
                }
            }
            _ => panic!("Expected outer Filter plan"),
        }
    }

    #[test]
    fn test_reorders_qualify_predicates() {
        let scan = make_scan("t", 2);

        let pred = combine_and(
            make_is_not_null_predicate("a", 0),
            make_eq_predicate("b", 1, 42),
        );

        let plan = PhysicalPlan::Qualify {
            input: Box::new(scan),
            predicate: pred,
        };

        let result = apply_short_circuit_ordering(plan);

        match result {
            PhysicalPlan::Qualify { predicate, .. } => {
                let collected = collect_and_predicates(&predicate);
                assert_eq!(collected.len(), 2);
                match &collected[0] {
                    Expr::BinaryOp {
                        op: BinaryOp::Eq, ..
                    } => {}
                    _ => panic!("Expected equality predicate first"),
                }
                match &collected[1] {
                    Expr::IsNull { negated: true, .. } => {}
                    _ => panic!("Expected IS NOT NULL predicate second"),
                }
            }
            _ => panic!("Expected Qualify plan"),
        }
    }

    #[test]
    fn test_handles_deeply_nested_and() {
        let scan = make_scan("t", 5);

        let p1 = make_is_not_null_predicate("a", 0);
        let p2 = make_like_predicate("b", 1, "%test%");
        let p3 = make_lt_predicate("c", 2, 100);
        let p4 = make_eq_predicate("d", 3, 42);
        let p5 = make_is_null_predicate("e", 4);

        let combined = combine_and(
            combine_and(
                combine_and(combine_and(p1.clone(), p2.clone()), p3.clone()),
                p4.clone(),
            ),
            p5.clone(),
        );

        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: combined,
        };

        let result = apply_short_circuit_ordering(plan);

        match result {
            PhysicalPlan::Filter { predicate, .. } => {
                let collected = collect_and_predicates(&predicate);
                assert_eq!(collected.len(), 5);
                assert_eq!(collected[0], p5);
                assert_eq!(collected[1], p4);
                assert_eq!(collected[2], p3);
                assert_eq!(collected[3], p2);
                assert_eq!(collected[4], p1);
            }
            _ => panic!("Expected Filter plan"),
        }
    }
}
