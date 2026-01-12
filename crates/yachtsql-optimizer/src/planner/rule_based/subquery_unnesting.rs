use rustc_hash::FxHashSet;
use yachtsql_ir::{BinaryOp, Expr, JoinType, LogicalPlan, PlanSchema};

use crate::PhysicalPlan;

pub fn apply_subquery_unnesting(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Filter { input, predicate } => {
            let optimized_input = apply_subquery_unnesting(*input);
            try_unnest_exists_in_filter(optimized_input, predicate)
        }
        PhysicalPlan::Project {
            input,
            expressions,
            schema,
        } => PhysicalPlan::Project {
            input: Box::new(apply_subquery_unnesting(*input)),
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
            left: Box::new(apply_subquery_unnesting(*left)),
            right: Box::new(apply_subquery_unnesting(*right)),
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
            left: Box::new(apply_subquery_unnesting(*left)),
            right: Box::new(apply_subquery_unnesting(*right)),
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
            left: Box::new(apply_subquery_unnesting(*left)),
            right: Box::new(apply_subquery_unnesting(*right)),
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
            input: Box::new(apply_subquery_unnesting(*input)),
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
            input: Box::new(apply_subquery_unnesting(*input)),
            sort_exprs,
            hints,
        },
        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => PhysicalPlan::TopN {
            input: Box::new(apply_subquery_unnesting(*input)),
            sort_exprs,
            limit,
        },
        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => PhysicalPlan::Limit {
            input: Box::new(apply_subquery_unnesting(*input)),
            limit,
            offset,
        },
        PhysicalPlan::Distinct { input } => PhysicalPlan::Distinct {
            input: Box::new(apply_subquery_unnesting(*input)),
        },
        PhysicalPlan::Union {
            inputs,
            all,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::Union {
            inputs: inputs.into_iter().map(apply_subquery_unnesting).collect(),
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
            left: Box::new(apply_subquery_unnesting(*left)),
            right: Box::new(apply_subquery_unnesting(*right)),
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
            left: Box::new(apply_subquery_unnesting(*left)),
            right: Box::new(apply_subquery_unnesting(*right)),
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
            input: Box::new(apply_subquery_unnesting(*input)),
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
            body: Box::new(apply_subquery_unnesting(*body)),
            parallel_ctes,
            hints,
        },
        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => PhysicalPlan::Unnest {
            input: Box::new(apply_subquery_unnesting(*input)),
            columns,
            schema,
        },
        PhysicalPlan::Qualify { input, predicate } => PhysicalPlan::Qualify {
            input: Box::new(apply_subquery_unnesting(*input)),
            predicate,
        },
        PhysicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => PhysicalPlan::Sample {
            input: Box::new(apply_subquery_unnesting(*input)),
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
            source: Box::new(apply_subquery_unnesting(*source)),
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
            query: query.map(|q| Box::new(apply_subquery_unnesting(*q))),
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
            query: Box::new(apply_subquery_unnesting(*query)),
            query_sql,
            column_aliases,
            or_replace,
            if_not_exists,
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
            from: from.map(|f| Box::new(apply_subquery_unnesting(*f))),
            filter,
        },
        PhysicalPlan::Merge {
            target_table,
            source,
            on,
            clauses,
        } => PhysicalPlan::Merge {
            target_table,
            source: Box::new(apply_subquery_unnesting(*source)),
            on,
            clauses,
        },
        PhysicalPlan::ExportData { options, query } => PhysicalPlan::ExportData {
            options,
            query: Box::new(apply_subquery_unnesting(*query)),
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
            input: Box::new(apply_subquery_unnesting(*input)),
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
            input: Box::new(apply_subquery_unnesting(*input)),
            analyze,
            logical_plan_text,
            physical_plan_text,
        },
        PhysicalPlan::If {
            condition,
            then_branch,
            else_branch,
        } => PhysicalPlan::If {
            condition,
            then_branch: then_branch
                .into_iter()
                .map(apply_subquery_unnesting)
                .collect(),
            else_branch: else_branch.map(|b| b.into_iter().map(apply_subquery_unnesting).collect()),
        },
        PhysicalPlan::While {
            condition,
            body,
            label,
        } => PhysicalPlan::While {
            condition,
            body: body.into_iter().map(apply_subquery_unnesting).collect(),
            label,
        },
        PhysicalPlan::Loop { body, label } => PhysicalPlan::Loop {
            body: body.into_iter().map(apply_subquery_unnesting).collect(),
            label,
        },
        PhysicalPlan::Block { body, label } => PhysicalPlan::Block {
            body: body.into_iter().map(apply_subquery_unnesting).collect(),
            label,
        },
        PhysicalPlan::Repeat {
            body,
            until_condition,
        } => PhysicalPlan::Repeat {
            body: body.into_iter().map(apply_subquery_unnesting).collect(),
            until_condition,
        },
        PhysicalPlan::For {
            variable,
            query,
            body,
        } => PhysicalPlan::For {
            variable,
            query: Box::new(apply_subquery_unnesting(*query)),
            body: body.into_iter().map(apply_subquery_unnesting).collect(),
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
            body: body.into_iter().map(apply_subquery_unnesting).collect(),
            or_replace,
            if_not_exists,
        },
        PhysicalPlan::TryCatch {
            try_block,
            catch_block,
        } => PhysicalPlan::TryCatch {
            try_block: try_block
                .into_iter()
                .map(|(p, sql)| (apply_subquery_unnesting(p), sql))
                .collect(),
            catch_block: catch_block
                .into_iter()
                .map(apply_subquery_unnesting)
                .collect(),
        },
        other => other,
    }
}

fn try_unnest_exists_in_filter(input: PhysicalPlan, predicate: Expr) -> PhysicalPlan {
    let outer_schema = input.schema().clone();

    if let Some((outer_expr, subquery, negated, remaining_predicate)) =
        try_extract_in_subquery(&predicate)
        && let Some(unnested) =
            try_unnest_in_subquery(&input, &outer_schema, &outer_expr, &subquery, negated)
    {
        return if let Some(remaining) = remaining_predicate {
            PhysicalPlan::Filter {
                input: Box::new(unnested),
                predicate: remaining,
            }
        } else {
            unnested
        };
    }

    PhysicalPlan::Filter {
        input: Box::new(input),
        predicate,
    }
}

#[allow(dead_code)]
fn try_extract_exists_subquery(predicate: &Expr) -> Option<(LogicalPlan, bool, Option<Expr>)> {
    match predicate {
        Expr::Exists { subquery, negated } => Some((*subquery.clone(), *negated, None)),
        Expr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
        } => {
            if let Some((subquery, negated, _)) = try_extract_exists_subquery(left) {
                return Some((subquery, negated, Some(*right.clone())));
            }
            if let Some((subquery, negated, _)) = try_extract_exists_subquery(right) {
                return Some((subquery, negated, Some(*left.clone())));
            }
            None
        }
        _ => None,
    }
}

fn try_extract_in_subquery(predicate: &Expr) -> Option<(Expr, LogicalPlan, bool, Option<Expr>)> {
    match predicate {
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Some((*expr.clone(), *subquery.clone(), *negated, None)),
        Expr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
        } => {
            if let Some((outer_expr, subquery, negated, _)) = try_extract_in_subquery(left) {
                return Some((outer_expr, subquery, negated, Some(*right.clone())));
            }
            if let Some((outer_expr, subquery, negated, _)) = try_extract_in_subquery(right) {
                return Some((outer_expr, subquery, negated, Some(*left.clone())));
            }
            None
        }
        _ => None,
    }
}

fn try_unnest_in_subquery(
    outer_input: &PhysicalPlan,
    outer_schema: &PlanSchema,
    outer_expr: &Expr,
    subquery: &LogicalPlan,
    negated: bool,
) -> Option<PhysicalPlan> {
    let (inner_plan, inner_expr) = extract_single_column_projection(subquery)?;

    let join_type = if negated {
        JoinType::LeftAnti
    } else {
        JoinType::LeftSemi
    };

    let inner_physical = logical_to_physical(&inner_plan)?;

    let result_schema = outer_schema.clone();

    let adjusted_inner_expr = adjust_inner_column_indices(&inner_expr, outer_schema.fields.len());

    Some(PhysicalPlan::HashJoin {
        left: Box::new(outer_input.clone()),
        right: Box::new(inner_physical),
        join_type,
        left_keys: vec![outer_expr.clone()],
        right_keys: vec![adjusted_inner_expr],
        schema: result_schema,
        parallel: false,
        hints: crate::ExecutionHints::default(),
    })
}

fn extract_single_column_projection(plan: &LogicalPlan) -> Option<(LogicalPlan, Expr)> {
    match plan {
        LogicalPlan::Project {
            input, expressions, ..
        } if expressions.len() == 1 => {
            let expr = expressions.first()?.clone();
            let unwrapped_expr = unwrap_alias(&expr);
            Some((*input.clone(), unwrapped_expr))
        }
        LogicalPlan::Filter { input, predicate } => {
            let (inner_plan, expr) = extract_single_column_projection(input)?;
            let new_plan = LogicalPlan::Filter {
                input: Box::new(inner_plan),
                predicate: predicate.clone(),
            };
            Some((new_plan, expr))
        }
        LogicalPlan::Distinct { input } => extract_single_column_projection(input),
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let (inner_plan, expr) = extract_single_column_projection(input)?;
            let new_plan = LogicalPlan::Limit {
                input: Box::new(inner_plan),
                limit: *limit,
                offset: *offset,
            };
            Some((new_plan, expr))
        }
        LogicalPlan::Sort { input, sort_exprs } => {
            let (inner_plan, expr) = extract_single_column_projection(input)?;
            let new_plan = LogicalPlan::Sort {
                input: Box::new(inner_plan),
                sort_exprs: sort_exprs.clone(),
            };
            Some((new_plan, expr))
        }
        _ => None,
    }
}

fn unwrap_alias(expr: &Expr) -> Expr {
    match expr {
        Expr::Alias { expr: inner, .. } => unwrap_alias(inner),
        other => other.clone(),
    }
}

#[allow(dead_code)]
fn try_unnest_exists(
    outer_input: &PhysicalPlan,
    outer_schema: &PlanSchema,
    subquery: &LogicalPlan,
    negated: bool,
) -> Option<PhysicalPlan> {
    let (inner_plan, correlation_predicate) = extract_correlated_filter(subquery, outer_schema)?;

    let join_type = if negated {
        JoinType::LeftAnti
    } else {
        JoinType::LeftSemi
    };

    let inner_physical = logical_to_physical(&inner_plan)?;

    let result_schema = outer_schema.clone();

    let adjusted_condition =
        adjust_inner_column_indices(&correlation_predicate, outer_schema.fields.len());

    Some(PhysicalPlan::NestedLoopJoin {
        left: Box::new(outer_input.clone()),
        right: Box::new(inner_physical),
        join_type,
        condition: Some(adjusted_condition),
        schema: result_schema,
        parallel: false,
        hints: crate::ExecutionHints::default(),
    })
}

#[allow(dead_code)]
fn extract_correlated_filter(
    plan: &LogicalPlan,
    outer_schema: &PlanSchema,
) -> Option<(LogicalPlan, Expr)> {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let outer_table_names: FxHashSet<_> = outer_schema
                .fields
                .iter()
                .filter_map(|f| f.table.as_ref())
                .map(|t| t.to_uppercase())
                .collect();

            if outer_table_names.is_empty() {
                return None;
            }

            let (correlated, uncorrelated) =
                split_correlation_predicates_by_table(predicate, &outer_table_names);

            if correlated.is_empty() {
                return None;
            }

            let correlation_expr = combine_predicates_and(&correlated)?;

            let inner_plan = if uncorrelated.is_empty() {
                *input.clone()
            } else {
                let uncorr_expr = combine_predicates_and(&uncorrelated)?;
                LogicalPlan::Filter {
                    input: input.clone(),
                    predicate: uncorr_expr,
                }
            };

            Some((inner_plan, correlation_expr))
        }
        LogicalPlan::Project { input, .. } => extract_correlated_filter(input, outer_schema),
        LogicalPlan::Distinct { input } => extract_correlated_filter(input, outer_schema),
        LogicalPlan::Limit { input, .. } => extract_correlated_filter(input, outer_schema),
        _ => None,
    }
}

#[allow(dead_code)]
fn split_correlation_predicates(
    predicate: &Expr,
    outer_col_names: &FxHashSet<String>,
) -> (Vec<Expr>, Vec<Expr>) {
    let conjuncts = split_and_predicates(predicate);
    let mut correlated = Vec::new();
    let mut uncorrelated = Vec::new();

    for conjunct in conjuncts {
        if references_outer_columns(&conjunct, outer_col_names) {
            correlated.push(conjunct);
        } else {
            uncorrelated.push(conjunct);
        }
    }

    (correlated, uncorrelated)
}

fn split_correlation_predicates_by_table(
    predicate: &Expr,
    outer_table_names: &FxHashSet<String>,
) -> (Vec<Expr>, Vec<Expr>) {
    let conjuncts = split_and_predicates(predicate);
    let mut correlated = Vec::new();
    let mut uncorrelated = Vec::new();

    for conjunct in conjuncts {
        if references_outer_table(&conjunct, outer_table_names) {
            correlated.push(conjunct);
        } else {
            uncorrelated.push(conjunct);
        }
    }

    (correlated, uncorrelated)
}

#[allow(dead_code)]
fn references_outer_table(expr: &Expr, outer_table_names: &FxHashSet<String>) -> bool {
    match expr {
        Expr::Column { table: Some(t), .. } => outer_table_names.contains(&t.to_uppercase()),
        Expr::Column { table: None, .. } => false,
        Expr::BinaryOp { left, right, .. } => {
            references_outer_table(left, outer_table_names)
                || references_outer_table(right, outer_table_names)
        }
        Expr::UnaryOp { expr, .. } => references_outer_table(expr, outer_table_names),
        Expr::ScalarFunction { args, .. } => args
            .iter()
            .any(|a| references_outer_table(a, outer_table_names)),
        Expr::Cast { expr, .. } => references_outer_table(expr, outer_table_names),
        Expr::IsNull { expr, .. } => references_outer_table(expr, outer_table_names),
        Expr::InList { expr, list, .. } => {
            references_outer_table(expr, outer_table_names)
                || list
                    .iter()
                    .any(|e| references_outer_table(e, outer_table_names))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            references_outer_table(expr, outer_table_names)
                || references_outer_table(low, outer_table_names)
                || references_outer_table(high, outer_table_names)
        }
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|o| references_outer_table(o, outer_table_names))
                || when_clauses.iter().any(|wc| {
                    references_outer_table(&wc.condition, outer_table_names)
                        || references_outer_table(&wc.result, outer_table_names)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|e| references_outer_table(e, outer_table_names))
        }
        Expr::Alias { expr, .. } => references_outer_table(expr, outer_table_names),
        _ => false,
    }
}

#[allow(dead_code)]
fn split_and_predicates(predicate: &Expr) -> Vec<Expr> {
    match predicate {
        Expr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
        } => {
            let mut result = split_and_predicates(left);
            result.extend(split_and_predicates(right));
            result
        }
        other => vec![other.clone()],
    }
}

#[allow(dead_code)]
fn references_outer_columns(expr: &Expr, outer_col_names: &FxHashSet<String>) -> bool {
    match expr {
        Expr::Column { table, name, .. } => {
            if table.is_some() {
                let upper_name = name.to_uppercase();
                outer_col_names.contains(&upper_name)
            } else {
                false
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            references_outer_columns(left, outer_col_names)
                || references_outer_columns(right, outer_col_names)
        }
        Expr::UnaryOp { expr, .. } => references_outer_columns(expr, outer_col_names),
        Expr::ScalarFunction { args, .. } => args
            .iter()
            .any(|a| references_outer_columns(a, outer_col_names)),
        Expr::Cast { expr, .. } => references_outer_columns(expr, outer_col_names),
        Expr::IsNull { expr, .. } => references_outer_columns(expr, outer_col_names),
        Expr::InList { expr, list, .. } => {
            references_outer_columns(expr, outer_col_names)
                || list
                    .iter()
                    .any(|e| references_outer_columns(e, outer_col_names))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            references_outer_columns(expr, outer_col_names)
                || references_outer_columns(low, outer_col_names)
                || references_outer_columns(high, outer_col_names)
        }
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|o| references_outer_columns(o, outer_col_names))
                || when_clauses.iter().any(|wc| {
                    references_outer_columns(&wc.condition, outer_col_names)
                        || references_outer_columns(&wc.result, outer_col_names)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|e| references_outer_columns(e, outer_col_names))
        }
        Expr::Alias { expr, .. } => references_outer_columns(expr, outer_col_names),
        _ => false,
    }
}

#[allow(dead_code)]
fn combine_predicates_and(predicates: &[Expr]) -> Option<Expr> {
    if predicates.is_empty() {
        return None;
    }

    let mut iter = predicates.iter().cloned();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, pred| Expr::BinaryOp {
        left: Box::new(acc),
        op: BinaryOp::And,
        right: Box::new(pred),
    }))
}

fn adjust_inner_column_indices(expr: &Expr, outer_len: usize) -> Expr {
    match expr {
        Expr::Column { table, name, index } => Expr::Column {
            table: table.clone(),
            name: name.clone(),
            index: index.map(|i| i + outer_len),
        },
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(adjust_inner_column_indices(left, outer_len)),
            op: *op,
            right: Box::new(adjust_inner_column_indices(right, outer_len)),
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(adjust_inner_column_indices(expr, outer_len)),
        },
        Expr::ScalarFunction { name, args } => Expr::ScalarFunction {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| adjust_inner_column_indices(a, outer_len))
                .collect(),
        },
        Expr::Cast {
            expr,
            data_type,
            safe,
        } => Expr::Cast {
            expr: Box::new(adjust_inner_column_indices(expr, outer_len)),
            data_type: data_type.clone(),
            safe: *safe,
        },
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(adjust_inner_column_indices(expr, outer_len)),
            negated: *negated,
        },
        Expr::Alias { expr, name } => Expr::Alias {
            expr: Box::new(adjust_inner_column_indices(expr, outer_len)),
            name: name.clone(),
        },
        other => other.clone(),
    }
}

fn logical_to_physical(plan: &LogicalPlan) -> Option<PhysicalPlan> {
    use crate::PhysicalPlanner;
    PhysicalPlanner::new().plan(plan).ok()
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::DataType;
    use yachtsql_ir::{Literal, PlanField};

    use super::*;

    fn make_schema(name: &str, num_cols: usize) -> PlanSchema {
        let fields = (0..num_cols)
            .map(|i| PlanField::new(format!("col{}", i), DataType::Int64).with_table(name))
            .collect();
        PlanSchema::from_fields(fields)
    }

    fn make_scan(name: &str, num_cols: usize) -> PhysicalPlan {
        PhysicalPlan::TableScan {
            table_name: name.to_string(),
            schema: make_schema(name, num_cols),
            projection: None,
            row_count: None,
        }
    }

    fn col(table: Option<&str>, name: &str, index: Option<usize>) -> Expr {
        Expr::Column {
            table: table.map(String::from),
            name: name.to_string(),
            index,
        }
    }

    fn eq(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Eq,
            right: Box::new(right),
        }
    }

    fn lit_int(val: i64) -> Expr {
        Expr::Literal(Literal::Int64(val))
    }

    #[test]
    fn test_split_and_predicates_single() {
        let pred = eq(col(None, "a", Some(0)), lit_int(1));
        let result = split_and_predicates(&pred);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_split_and_predicates_multiple() {
        let pred1 = eq(col(None, "a", Some(0)), lit_int(1));
        let pred2 = eq(col(None, "b", Some(1)), lit_int(2));
        let combined = Expr::BinaryOp {
            left: Box::new(pred1),
            op: BinaryOp::And,
            right: Box::new(pred2),
        };
        let result = split_and_predicates(&combined);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_references_outer_columns() {
        let mut outer_cols = FxHashSet::default();
        outer_cols.insert("COL0".to_string());

        let inner_ref = col(None, "inner_col", Some(0));
        assert!(!references_outer_columns(&inner_ref, &outer_cols));

        let outer_ref = col(Some("outer"), "col0", Some(0));
        assert!(references_outer_columns(&outer_ref, &outer_cols));
    }

    #[test]
    fn test_combine_predicates_and_empty() {
        let result = combine_predicates_and(&[]);
        assert!(result.is_none());
    }

    #[test]
    fn test_combine_predicates_and_single() {
        let pred = eq(col(None, "a", Some(0)), lit_int(1));
        let result = combine_predicates_and(std::slice::from_ref(&pred));
        assert_eq!(result, Some(pred));
    }

    #[test]
    fn test_combine_predicates_and_multiple() {
        let pred1 = eq(col(None, "a", Some(0)), lit_int(1));
        let pred2 = eq(col(None, "b", Some(1)), lit_int(2));
        let result = combine_predicates_and(&[pred1.clone(), pred2.clone()]);
        assert!(result.is_some());
        match result.unwrap() {
            Expr::BinaryOp {
                op: BinaryOp::And, ..
            } => {}
            _ => panic!("Expected AND expression"),
        }
    }

    #[test]
    fn test_adjust_inner_column_indices() {
        let expr = col(None, "inner_col", Some(0));
        let adjusted = adjust_inner_column_indices(&expr, 3);
        match adjusted {
            Expr::Column {
                index: Some(idx), ..
            } => assert_eq!(idx, 3),
            _ => panic!("Expected column with adjusted index"),
        }
    }

    #[test]
    fn test_adjust_inner_column_indices_with_table() {
        let expr = col(Some("inner"), "col", Some(0));
        let adjusted = adjust_inner_column_indices(&expr, 3);
        match adjusted {
            Expr::Column {
                index: Some(idx), ..
            } => assert_eq!(idx, 3),
            _ => panic!("Expected column with adjusted index"),
        }
    }

    #[test]
    fn test_try_extract_exists_subquery_simple() {
        let subquery = LogicalPlan::Empty {
            schema: PlanSchema::new(),
        };
        let exists = Expr::Exists {
            subquery: Box::new(subquery.clone()),
            negated: false,
        };

        let result = try_extract_exists_subquery(&exists);
        assert!(result.is_some());
        let (_, negated, remaining) = result.unwrap();
        assert!(!negated);
        assert!(remaining.is_none());
    }

    #[test]
    fn test_try_extract_exists_subquery_negated() {
        let subquery = LogicalPlan::Empty {
            schema: PlanSchema::new(),
        };
        let not_exists = Expr::Exists {
            subquery: Box::new(subquery),
            negated: true,
        };

        let result = try_extract_exists_subquery(&not_exists);
        assert!(result.is_some());
        let (_, negated, _) = result.unwrap();
        assert!(negated);
    }

    #[test]
    fn test_try_extract_exists_with_and() {
        let subquery = LogicalPlan::Empty {
            schema: PlanSchema::new(),
        };
        let exists = Expr::Exists {
            subquery: Box::new(subquery),
            negated: false,
        };
        let other_pred = eq(col(None, "a", Some(0)), lit_int(1));
        let combined = Expr::BinaryOp {
            left: Box::new(exists),
            op: BinaryOp::And,
            right: Box::new(other_pred.clone()),
        };

        let result = try_extract_exists_subquery(&combined);
        assert!(result.is_some());
        let (_, _, remaining) = result.unwrap();
        assert!(remaining.is_some());
    }

    #[test]
    fn test_apply_subquery_unnesting_no_exists() {
        let scan = make_scan("t", 3);
        let filter = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: eq(col(None, "col0", Some(0)), lit_int(1)),
        };

        let result = apply_subquery_unnesting(filter.clone());
        match result {
            PhysicalPlan::Filter { .. } => {}
            _ => panic!("Expected Filter to be preserved"),
        }
    }

    #[test]
    fn test_try_extract_in_subquery_simple() {
        let subquery = LogicalPlan::Empty {
            schema: PlanSchema::new(),
        };
        let in_subq = Expr::InSubquery {
            expr: Box::new(col(None, "col0", Some(0))),
            subquery: Box::new(subquery),
            negated: false,
        };

        let result = try_extract_in_subquery(&in_subq);
        assert!(result.is_some());
        let (_, _, negated, remaining) = result.unwrap();
        assert!(!negated);
        assert!(remaining.is_none());
    }

    #[test]
    fn test_try_extract_in_subquery_negated() {
        let subquery = LogicalPlan::Empty {
            schema: PlanSchema::new(),
        };
        let not_in_subq = Expr::InSubquery {
            expr: Box::new(col(None, "col0", Some(0))),
            subquery: Box::new(subquery),
            negated: true,
        };

        let result = try_extract_in_subquery(&not_in_subq);
        assert!(result.is_some());
        let (_, _, negated, _) = result.unwrap();
        assert!(negated);
    }

    #[test]
    fn test_try_extract_in_subquery_with_and() {
        let subquery = LogicalPlan::Empty {
            schema: PlanSchema::new(),
        };
        let in_subq = Expr::InSubquery {
            expr: Box::new(col(None, "col0", Some(0))),
            subquery: Box::new(subquery),
            negated: false,
        };
        let other_pred = eq(col(None, "a", Some(0)), lit_int(1));
        let combined = Expr::BinaryOp {
            left: Box::new(in_subq),
            op: BinaryOp::And,
            right: Box::new(other_pred),
        };

        let result = try_extract_in_subquery(&combined);
        assert!(result.is_some());
        let (_, _, _, remaining) = result.unwrap();
        assert!(remaining.is_some());
    }

    #[test]
    fn test_extract_single_column_projection() {
        let inner_scan = LogicalPlan::Scan {
            table_name: "inner".to_string(),
            schema: make_schema("inner", 2),
            projection: None,
        };
        let project = LogicalPlan::Project {
            input: Box::new(inner_scan),
            expressions: vec![col(None, "col0", Some(0))],
            schema: make_schema("result", 1),
        };

        let result = extract_single_column_projection(&project);
        assert!(result.is_some());
    }

    #[test]
    fn test_extract_single_column_projection_with_alias() {
        let inner_scan = LogicalPlan::Scan {
            table_name: "inner".to_string(),
            schema: make_schema("inner", 2),
            projection: None,
        };
        let aliased = Expr::Alias {
            expr: Box::new(col(None, "col0", Some(0))),
            name: "alias".to_string(),
        };
        let project = LogicalPlan::Project {
            input: Box::new(inner_scan),
            expressions: vec![aliased],
            schema: make_schema("result", 1),
        };

        let result = extract_single_column_projection(&project);
        assert!(result.is_some());
        let (_, expr) = result.unwrap();
        match expr {
            Expr::Column { .. } => {}
            _ => panic!("Expected unwrapped column expression"),
        }
    }

    #[test]
    fn test_unwrap_alias_nested() {
        let inner = col(None, "col0", Some(0));
        let alias1 = Expr::Alias {
            expr: Box::new(inner.clone()),
            name: "alias1".to_string(),
        };
        let alias2 = Expr::Alias {
            expr: Box::new(alias1),
            name: "alias2".to_string(),
        };

        let result = unwrap_alias(&alias2);
        assert_eq!(result, inner);
    }

    #[test]
    fn test_apply_subquery_unnesting_preserves_project() {
        let scan = make_scan("t", 3);
        let project = PhysicalPlan::Project {
            input: Box::new(scan),
            expressions: vec![col(None, "col0", Some(0))],
            schema: make_schema("result", 1),
        };

        let result = apply_subquery_unnesting(project);
        match result {
            PhysicalPlan::Project { .. } => {}
            _ => panic!("Expected Project to be preserved"),
        }
    }

    #[test]
    fn test_apply_subquery_unnesting_preserves_hash_join() {
        let left = make_scan("left", 2);
        let right = make_scan("right", 2);

        let mut fields = left.schema().fields.clone();
        fields.extend(right.schema().fields.clone());
        let schema = PlanSchema::from_fields(fields);

        let join = PhysicalPlan::HashJoin {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            left_keys: vec![col(None, "col0", Some(0))],
            right_keys: vec![col(None, "col0", Some(0))],
            schema,
            parallel: false,
            hints: crate::ExecutionHints::default(),
        };

        let result = apply_subquery_unnesting(join);
        match result {
            PhysicalPlan::HashJoin { .. } => {}
            _ => panic!("Expected HashJoin to be preserved"),
        }
    }
}
