use rustc_hash::FxHashSet;
use yachtsql_ir::{
    AggregateFunction, BinaryOp, Expr, JoinType, LogicalPlan, PlanField, PlanSchema,
};

use crate::{ExecutionHints, PhysicalPlan};

pub fn apply_decorrelation(plan: PhysicalPlan) -> PhysicalPlan {
    decorrelate_plan(plan)
}

fn decorrelate_plan(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Project {
            input,
            expressions,
            schema,
        } => {
            let optimized_input = decorrelate_plan(*input);
            try_decorrelate_project(optimized_input, expressions, schema)
        }
        PhysicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(decorrelate_plan(*input)),
            predicate,
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
            left: Box::new(decorrelate_plan(*left)),
            right: Box::new(decorrelate_plan(*right)),
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
            left: Box::new(decorrelate_plan(*left)),
            right: Box::new(decorrelate_plan(*right)),
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
            left: Box::new(decorrelate_plan(*left)),
            right: Box::new(decorrelate_plan(*right)),
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
            input: Box::new(decorrelate_plan(*input)),
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
            input: Box::new(decorrelate_plan(*input)),
            sort_exprs,
            hints,
        },
        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => PhysicalPlan::TopN {
            input: Box::new(decorrelate_plan(*input)),
            sort_exprs,
            limit,
        },
        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => PhysicalPlan::Limit {
            input: Box::new(decorrelate_plan(*input)),
            limit,
            offset,
        },
        PhysicalPlan::Distinct { input } => PhysicalPlan::Distinct {
            input: Box::new(decorrelate_plan(*input)),
        },
        PhysicalPlan::Union {
            inputs,
            all,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::Union {
            inputs: inputs.into_iter().map(decorrelate_plan).collect(),
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
            left: Box::new(decorrelate_plan(*left)),
            right: Box::new(decorrelate_plan(*right)),
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
            left: Box::new(decorrelate_plan(*left)),
            right: Box::new(decorrelate_plan(*right)),
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
            input: Box::new(decorrelate_plan(*input)),
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
            body: Box::new(decorrelate_plan(*body)),
            parallel_ctes,
            hints,
        },
        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => PhysicalPlan::Unnest {
            input: Box::new(decorrelate_plan(*input)),
            columns,
            schema,
        },
        PhysicalPlan::Qualify { input, predicate } => PhysicalPlan::Qualify {
            input: Box::new(decorrelate_plan(*input)),
            predicate,
        },
        PhysicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => PhysicalPlan::Sample {
            input: Box::new(decorrelate_plan(*input)),
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
            source: Box::new(decorrelate_plan(*source)),
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
            query: query.map(|q| Box::new(decorrelate_plan(*q))),
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
            query: Box::new(decorrelate_plan(*query)),
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
            from: from.map(|f| Box::new(decorrelate_plan(*f))),
            filter,
        },
        PhysicalPlan::Merge {
            target_table,
            source,
            on,
            clauses,
        } => PhysicalPlan::Merge {
            target_table,
            source: Box::new(decorrelate_plan(*source)),
            on,
            clauses,
        },
        PhysicalPlan::ExportData { options, query } => PhysicalPlan::ExportData {
            options,
            query: Box::new(decorrelate_plan(*query)),
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
            input: Box::new(decorrelate_plan(*input)),
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
            input: Box::new(decorrelate_plan(*input)),
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
            then_branch: then_branch.into_iter().map(decorrelate_plan).collect(),
            else_branch: else_branch.map(|b| b.into_iter().map(decorrelate_plan).collect()),
        },
        PhysicalPlan::While {
            condition,
            body,
            label,
        } => PhysicalPlan::While {
            condition,
            body: body.into_iter().map(decorrelate_plan).collect(),
            label,
        },
        PhysicalPlan::Loop { body, label } => PhysicalPlan::Loop {
            body: body.into_iter().map(decorrelate_plan).collect(),
            label,
        },
        PhysicalPlan::Block { body, label } => PhysicalPlan::Block {
            body: body.into_iter().map(decorrelate_plan).collect(),
            label,
        },
        PhysicalPlan::Repeat {
            body,
            until_condition,
        } => PhysicalPlan::Repeat {
            body: body.into_iter().map(decorrelate_plan).collect(),
            until_condition,
        },
        PhysicalPlan::For {
            variable,
            query,
            body,
        } => PhysicalPlan::For {
            variable,
            query: Box::new(decorrelate_plan(*query)),
            body: body.into_iter().map(decorrelate_plan).collect(),
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
            body: body.into_iter().map(decorrelate_plan).collect(),
            or_replace,
            if_not_exists,
        },
        PhysicalPlan::TryCatch {
            try_block,
            catch_block,
        } => PhysicalPlan::TryCatch {
            try_block: try_block
                .into_iter()
                .map(|(p, sql)| (decorrelate_plan(p), sql))
                .collect(),
            catch_block: catch_block.into_iter().map(decorrelate_plan).collect(),
        },
        other => other,
    }
}

fn try_decorrelate_project(
    input: PhysicalPlan,
    expressions: Vec<Expr>,
    schema: PlanSchema,
) -> PhysicalPlan {
    let outer_schema = input.schema().clone();
    let outer_table_names = collect_table_names(&outer_schema);

    if outer_table_names.is_empty() {
        return PhysicalPlan::Project {
            input: Box::new(input),
            expressions,
            schema,
        };
    }

    let mut decorrelated_input = input;
    let mut new_expressions = Vec::with_capacity(expressions.len());
    let mut new_schema_fields = Vec::with_capacity(schema.fields.len());
    let mut subquery_idx = 0;

    for (expr, field) in expressions.into_iter().zip(schema.fields.into_iter()) {
        match try_decorrelate_scalar_subquery(
            &expr,
            &outer_schema,
            &outer_table_names,
            subquery_idx,
        ) {
            Some(DecorrelationResult {
                inner_plan,
                outer_key,
                inner_key,
                agg_expr,
                agg_alias,
            }) => {
                let inner_physical = match logical_to_physical(&inner_plan) {
                    Some(p) => p,
                    None => {
                        new_expressions.push(expr);
                        new_schema_fields.push(field);
                        continue;
                    }
                };

                let inner_schema = inner_physical.schema().clone();
                let agg_result_expr = Expr::Aggregate {
                    func: agg_expr.func,
                    args: agg_expr.args.clone(),
                    distinct: agg_expr.distinct,
                    filter: agg_expr.filter.clone(),
                    order_by: agg_expr.order_by.clone(),
                    limit: agg_expr.limit,
                    ignore_nulls: agg_expr.ignore_nulls,
                };

                let inner_key_type = get_expr_data_type(&inner_key, &inner_schema)
                    .unwrap_or(yachtsql_common::types::DataType::Int64);
                let agg_schema = PlanSchema::from_fields(vec![
                    PlanField::new(format!("__decorr_key_{}", subquery_idx), inner_key_type),
                    PlanField::new(agg_alias.clone(), field.data_type.clone()),
                ]);

                let aggregate_plan = PhysicalPlan::HashAggregate {
                    input: Box::new(inner_physical),
                    group_by: vec![inner_key.clone()],
                    aggregates: vec![agg_result_expr],
                    schema: agg_schema.clone(),
                    grouping_sets: None,
                    hints: ExecutionHints::default(),
                };

                let outer_len = decorrelated_input.schema().fields.len();
                let mut joined_fields = decorrelated_input.schema().fields.clone();
                joined_fields.extend(agg_schema.fields.clone());
                let joined_schema = PlanSchema::from_fields(joined_fields);

                let right_key = Expr::Column {
                    table: None,
                    name: format!("__decorr_key_{}", subquery_idx),
                    index: Some(outer_len),
                };

                decorrelated_input = PhysicalPlan::HashJoin {
                    left: Box::new(decorrelated_input),
                    right: Box::new(aggregate_plan),
                    join_type: JoinType::Left,
                    left_keys: vec![outer_key],
                    right_keys: vec![right_key],
                    schema: joined_schema,
                    parallel: false,
                    hints: ExecutionHints::default(),
                };

                let new_outer_len = decorrelated_input.schema().fields.len();
                let agg_col_ref = Expr::Column {
                    table: None,
                    name: agg_alias,
                    index: Some(new_outer_len - 1),
                };

                let result_expr = if agg_expr.func == AggregateFunction::Count {
                    Expr::ScalarFunction {
                        name: yachtsql_ir::ScalarFunction::Coalesce,
                        args: vec![agg_col_ref, Expr::Literal(yachtsql_ir::Literal::Int64(0))],
                    }
                } else {
                    agg_col_ref
                };

                let aliased_expr = if let Expr::Alias { name, .. } = &expr {
                    Expr::Alias {
                        expr: Box::new(result_expr),
                        name: name.clone(),
                    }
                } else {
                    result_expr
                };

                new_expressions.push(aliased_expr);
                new_schema_fields.push(field);
                subquery_idx += 1;
            }
            None => {
                new_expressions.push(expr);
                new_schema_fields.push(field);
            }
        }
    }

    PhysicalPlan::Project {
        input: Box::new(decorrelated_input),
        expressions: new_expressions,
        schema: PlanSchema::from_fields(new_schema_fields),
    }
}

struct AggregateInfo {
    func: AggregateFunction,
    args: Vec<Expr>,
    distinct: bool,
    filter: Option<Box<Expr>>,
    order_by: Vec<yachtsql_ir::SortExpr>,
    limit: Option<usize>,
    ignore_nulls: bool,
}

struct DecorrelationResult {
    inner_plan: LogicalPlan,
    outer_key: Expr,
    inner_key: Expr,
    agg_expr: AggregateInfo,
    agg_alias: String,
}

fn try_decorrelate_scalar_subquery(
    expr: &Expr,
    _outer_schema: &PlanSchema,
    outer_table_names: &FxHashSet<String>,
    subquery_idx: usize,
) -> Option<DecorrelationResult> {
    let (subquery_plan, alias_name) = match expr {
        Expr::ScalarSubquery(plan) | Expr::Subquery(plan) => (plan.as_ref(), None),
        Expr::Alias { expr: inner, name } => match inner.as_ref() {
            Expr::ScalarSubquery(plan) | Expr::Subquery(plan) => {
                (plan.as_ref(), Some(name.clone()))
            }
            _ => return None,
        },
        _ => return None,
    };

    let (projection_exprs, inner_plan) = extract_projection(subquery_plan)?;
    if projection_exprs.len() != 1 {
        return None;
    }

    let proj_expr = &projection_exprs[0];
    let agg_info = extract_aggregate(proj_expr)?;

    let (filter_plan, correlation_predicate) =
        extract_correlated_filter(&inner_plan, outer_table_names)?;

    let (outer_key, inner_key) = extract_equi_join_keys(&correlation_predicate, outer_table_names)?;

    let agg_alias = alias_name.unwrap_or_else(|| format!("__decorr_agg_{}", subquery_idx));

    Some(DecorrelationResult {
        inner_plan: filter_plan,
        outer_key,
        inner_key,
        agg_expr: agg_info,
        agg_alias,
    })
}

fn extract_projection(plan: &LogicalPlan) -> Option<(Vec<Expr>, LogicalPlan)> {
    match plan {
        LogicalPlan::Project {
            expressions, input, ..
        } => {
            if let LogicalPlan::Aggregate {
                aggregates,
                group_by,
                input: agg_input,
                ..
            } = input.as_ref()
            {
                let is_single_aggregate_reference = group_by.is_empty()
                    && aggregates.len() == 1
                    && expressions.len() == 1
                    && matches!(&expressions[0], Expr::Column { index: Some(0), .. });

                if is_single_aggregate_reference {
                    return Some((aggregates.clone(), *agg_input.clone()));
                }
            }
            Some((expressions.clone(), *input.clone()))
        }
        LogicalPlan::Aggregate {
            aggregates,
            group_by,
            input,
            schema,
            ..
        } => {
            if !group_by.is_empty() {
                return None;
            }
            Some((
                aggregates.clone(),
                LogicalPlan::Aggregate {
                    input: input.clone(),
                    group_by: vec![],
                    aggregates: vec![],
                    schema: schema.clone(),
                    grouping_sets: None,
                },
            ))
        }
        _ => None,
    }
}

fn extract_aggregate(expr: &Expr) -> Option<AggregateInfo> {
    match expr {
        Expr::Aggregate {
            func,
            args,
            distinct,
            filter,
            order_by,
            limit,
            ignore_nulls,
        } => {
            if !is_decomposable_aggregate(func) {
                return None;
            }
            Some(AggregateInfo {
                func: *func,
                args: args.clone(),
                distinct: *distinct,
                filter: filter.clone(),
                order_by: order_by.clone(),
                limit: *limit,
                ignore_nulls: *ignore_nulls,
            })
        }
        Expr::Alias { expr: inner, .. } => extract_aggregate(inner),
        _ => None,
    }
}

fn is_decomposable_aggregate(func: &AggregateFunction) -> bool {
    matches!(
        func,
        AggregateFunction::Sum
            | AggregateFunction::Count
            | AggregateFunction::Avg
            | AggregateFunction::Min
            | AggregateFunction::Max
    )
}

fn extract_correlated_filter(
    plan: &LogicalPlan,
    outer_table_names: &FxHashSet<String>,
) -> Option<(LogicalPlan, Expr)> {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let (correlated, uncorrelated) =
                split_correlation_predicates(predicate, outer_table_names);

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
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
            grouping_sets,
        } => {
            let (inner_plan, corr) = extract_correlated_filter(input, outer_table_names)?;
            Some((
                LogicalPlan::Aggregate {
                    input: Box::new(inner_plan),
                    group_by: group_by.clone(),
                    aggregates: aggregates.clone(),
                    schema: schema.clone(),
                    grouping_sets: grouping_sets.clone(),
                },
                corr,
            ))
        }
        LogicalPlan::Project {
            input,
            expressions,
            schema,
        } => {
            let (inner_plan, corr) = extract_correlated_filter(input, outer_table_names)?;
            Some((
                LogicalPlan::Project {
                    input: Box::new(inner_plan),
                    expressions: expressions.clone(),
                    schema: schema.clone(),
                },
                corr,
            ))
        }
        _ => None,
    }
}

fn split_correlation_predicates(
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
        Expr::Alias { expr, .. } => references_outer_table(expr, outer_table_names),
        _ => false,
    }
}

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

fn extract_equi_join_keys(
    predicate: &Expr,
    outer_table_names: &FxHashSet<String>,
) -> Option<(Expr, Expr)> {
    match predicate {
        Expr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        } => {
            let left_is_outer = references_outer_table(left, outer_table_names);
            let right_is_outer = references_outer_table(right, outer_table_names);

            match (left_is_outer, right_is_outer) {
                (true, false) => Some((*left.clone(), *right.clone())),
                (false, true) => Some((*right.clone(), *left.clone())),
                _ => None,
            }
        }
        _ => None,
    }
}

fn collect_table_names(schema: &PlanSchema) -> FxHashSet<String> {
    schema
        .fields
        .iter()
        .filter_map(|f| f.table.as_ref())
        .map(|t| t.to_uppercase())
        .collect()
}

fn get_expr_data_type(
    expr: &Expr,
    schema: &PlanSchema,
) -> Option<yachtsql_common::types::DataType> {
    match expr {
        Expr::Column { name, index, .. } => {
            if let Some(idx) = index {
                schema.fields.get(*idx).map(|f| f.data_type.clone())
            } else {
                schema
                    .fields
                    .iter()
                    .find(|f| f.name.eq_ignore_ascii_case(name))
                    .map(|f| f.data_type.clone())
            }
        }
        Expr::Literal(lit) => Some(lit.data_type()),
        _ => None,
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

    fn make_schema(name: &str, cols: &[(&str, DataType)]) -> PlanSchema {
        let fields = cols
            .iter()
            .map(|(col_name, dt)| PlanField::new(col_name.to_string(), dt.clone()).with_table(name))
            .collect();
        PlanSchema::from_fields(fields)
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

    #[test]
    fn test_collect_table_names() {
        let schema = make_schema(
            "customers",
            &[("id", DataType::Int64), ("name", DataType::String)],
        );
        let names = collect_table_names(&schema);
        assert!(names.contains("CUSTOMERS"));
    }

    #[test]
    fn test_references_outer_table_with_match() {
        let mut outer_names = FxHashSet::default();
        outer_names.insert("CUSTOMERS".to_string());

        let expr = col(Some("customers"), "id", Some(0));
        assert!(references_outer_table(&expr, &outer_names));
    }

    #[test]
    fn test_references_outer_table_without_match() {
        let mut outer_names = FxHashSet::default();
        outer_names.insert("CUSTOMERS".to_string());

        let expr = col(Some("orders"), "id", Some(0));
        assert!(!references_outer_table(&expr, &outer_names));
    }

    #[test]
    fn test_extract_equi_join_keys() {
        let mut outer_names = FxHashSet::default();
        outer_names.insert("C".to_string());

        let pred = eq(
            col(Some("o"), "customer_id", Some(0)),
            col(Some("c"), "id", Some(0)),
        );

        let result = extract_equi_join_keys(&pred, &outer_names);
        assert!(result.is_some());
        let (outer_key, inner_key) = result.unwrap();

        match outer_key {
            Expr::Column { table: Some(t), .. } => assert_eq!(t.to_uppercase(), "C"),
            _ => panic!("Expected column with table"),
        }
        match inner_key {
            Expr::Column { table: Some(t), .. } => assert_eq!(t.to_uppercase(), "O"),
            _ => panic!("Expected column with table"),
        }
    }

    #[test]
    fn test_is_decomposable_aggregate() {
        assert!(is_decomposable_aggregate(&AggregateFunction::Sum));
        assert!(is_decomposable_aggregate(&AggregateFunction::Count));
        assert!(is_decomposable_aggregate(&AggregateFunction::Avg));
        assert!(is_decomposable_aggregate(&AggregateFunction::Min));
        assert!(is_decomposable_aggregate(&AggregateFunction::Max));
        assert!(!is_decomposable_aggregate(&AggregateFunction::ArrayAgg));
    }

    #[test]
    fn test_split_correlation_predicates() {
        let mut outer_names = FxHashSet::default();
        outer_names.insert("C".to_string());

        let correlated = eq(
            col(Some("o"), "customer_id", Some(0)),
            col(Some("c"), "id", Some(0)),
        );
        let uncorrelated = eq(
            col(Some("o"), "amount", Some(1)),
            Expr::Literal(Literal::Int64(100)),
        );
        let combined = Expr::BinaryOp {
            left: Box::new(correlated),
            op: BinaryOp::And,
            right: Box::new(uncorrelated),
        };

        let (corr, uncorr) = split_correlation_predicates(&combined, &outer_names);
        assert_eq!(corr.len(), 1);
        assert_eq!(uncorr.len(), 1);
    }
}
