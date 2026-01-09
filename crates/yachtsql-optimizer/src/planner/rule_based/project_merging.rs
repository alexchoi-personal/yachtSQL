#![coverage(off)]

use rustc_hash::FxHashMap;
use yachtsql_ir::{Expr, ScalarFunction};

use crate::PhysicalPlan;

fn is_volatile_function(name: &ScalarFunction) -> bool {
    matches!(
        name,
        ScalarFunction::GenerateUuid
            | ScalarFunction::Rand
            | ScalarFunction::RandCanonical
            | ScalarFunction::CurrentTimestamp
            | ScalarFunction::CurrentDate
            | ScalarFunction::CurrentTime
            | ScalarFunction::CurrentDatetime
    )
}

fn contains_volatile_expr(expr: &Expr) -> bool {
    match expr {
        Expr::ScalarFunction { name, args } => {
            is_volatile_function(name) || args.iter().any(contains_volatile_expr)
        }
        Expr::BinaryOp { left, right, .. } => {
            contains_volatile_expr(left) || contains_volatile_expr(right)
        }
        Expr::UnaryOp { expr: inner, .. } => contains_volatile_expr(inner),
        Expr::Cast { expr: inner, .. } => contains_volatile_expr(inner),
        Expr::Alias { expr: inner, .. } => contains_volatile_expr(inner),
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            operand.as_ref().is_some_and(|o| contains_volatile_expr(o))
                || when_clauses.iter().any(|w| {
                    contains_volatile_expr(&w.condition) || contains_volatile_expr(&w.result)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|e| contains_volatile_expr(e))
        }
        Expr::IsNull { expr: inner, .. } => contains_volatile_expr(inner),
        Expr::InList {
            expr: inner, list, ..
        } => contains_volatile_expr(inner) || list.iter().any(contains_volatile_expr),
        Expr::Between {
            expr, low, high, ..
        } => {
            contains_volatile_expr(expr)
                || contains_volatile_expr(low)
                || contains_volatile_expr(high)
        }
        _ => false,
    }
}

fn count_column_references(expr: &Expr, counts: &mut FxHashMap<usize, usize>) {
    match expr {
        Expr::Column { index: Some(i), .. } => {
            *counts.entry(*i).or_insert(0) += 1;
        }
        Expr::BinaryOp { left, right, .. } => {
            count_column_references(left, counts);
            count_column_references(right, counts);
        }
        Expr::UnaryOp { expr: inner, .. } => count_column_references(inner, counts),
        Expr::ScalarFunction { args, .. } => {
            for arg in args {
                count_column_references(arg, counts);
            }
        }
        Expr::Cast { expr: inner, .. } => count_column_references(inner, counts),
        Expr::Alias { expr: inner, .. } => count_column_references(inner, counts),
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            if let Some(o) = operand {
                count_column_references(o, counts);
            }
            for w in when_clauses {
                count_column_references(&w.condition, counts);
                count_column_references(&w.result, counts);
            }
            if let Some(e) = else_result {
                count_column_references(e, counts);
            }
        }
        Expr::IsNull { expr: inner, .. } => count_column_references(inner, counts),
        Expr::InList {
            expr: inner, list, ..
        } => {
            count_column_references(inner, counts);
            for l in list {
                count_column_references(l, counts);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            count_column_references(expr, counts);
            count_column_references(low, counts);
            count_column_references(high, counts);
        }
        _ => {}
    }
}

fn can_merge_projects(outer_exprs: &[Expr], inner_exprs: &[Expr]) -> bool {
    let mut ref_counts: FxHashMap<usize, usize> = FxHashMap::default();
    for expr in outer_exprs {
        count_column_references(expr, &mut ref_counts);
    }

    for (idx, count) in ref_counts {
        if count > 1 && inner_exprs.get(idx).is_some_and(contains_volatile_expr) {
            return false;
        }
    }
    true
}

pub(crate) fn substitute_column_refs(expr: &Expr, inner_exprs: &[Expr]) -> Expr {
    match expr {
        Expr::Column { index: Some(i), .. } => {
            if *i < inner_exprs.len() {
                inner_exprs[*i].clone()
            } else {
                expr.clone()
            }
        }

        Expr::Column { index: None, .. } => expr.clone(),

        Expr::Literal(_) => expr.clone(),

        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(substitute_column_refs(left, inner_exprs)),
            op: *op,
            right: Box::new(substitute_column_refs(right, inner_exprs)),
        },

        Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(substitute_column_refs(inner, inner_exprs)),
        },

        Expr::ScalarFunction { name, args } => Expr::ScalarFunction {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| substitute_column_refs(a, inner_exprs))
                .collect(),
        },

        Expr::Aggregate {
            func,
            args,
            distinct,
            filter,
            order_by,
            limit,
            ignore_nulls,
        } => Expr::Aggregate {
            func: *func,
            args: args
                .iter()
                .map(|a| substitute_column_refs(a, inner_exprs))
                .collect(),
            distinct: *distinct,
            filter: filter
                .as_ref()
                .map(|f| Box::new(substitute_column_refs(f, inner_exprs))),
            order_by: order_by
                .iter()
                .map(|s| yachtsql_ir::SortExpr {
                    expr: substitute_column_refs(&s.expr, inner_exprs),
                    asc: s.asc,
                    nulls_first: s.nulls_first,
                })
                .collect(),
            limit: *limit,
            ignore_nulls: *ignore_nulls,
        },

        Expr::UserDefinedAggregate {
            name,
            args,
            distinct,
            filter,
        } => Expr::UserDefinedAggregate {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| substitute_column_refs(a, inner_exprs))
                .collect(),
            distinct: *distinct,
            filter: filter
                .as_ref()
                .map(|f| Box::new(substitute_column_refs(f, inner_exprs))),
        },

        Expr::Window {
            func,
            args,
            partition_by,
            order_by,
            frame,
        } => Expr::Window {
            func: *func,
            args: args
                .iter()
                .map(|a| substitute_column_refs(a, inner_exprs))
                .collect(),
            partition_by: partition_by
                .iter()
                .map(|p| substitute_column_refs(p, inner_exprs))
                .collect(),
            order_by: order_by
                .iter()
                .map(|s| yachtsql_ir::SortExpr {
                    expr: substitute_column_refs(&s.expr, inner_exprs),
                    asc: s.asc,
                    nulls_first: s.nulls_first,
                })
                .collect(),
            frame: frame.clone(),
        },

        Expr::AggregateWindow {
            func,
            args,
            distinct,
            partition_by,
            order_by,
            frame,
        } => Expr::AggregateWindow {
            func: *func,
            args: args
                .iter()
                .map(|a| substitute_column_refs(a, inner_exprs))
                .collect(),
            distinct: *distinct,
            partition_by: partition_by
                .iter()
                .map(|p| substitute_column_refs(p, inner_exprs))
                .collect(),
            order_by: order_by
                .iter()
                .map(|s| yachtsql_ir::SortExpr {
                    expr: substitute_column_refs(&s.expr, inner_exprs),
                    asc: s.asc,
                    nulls_first: s.nulls_first,
                })
                .collect(),
            frame: frame.clone(),
        },

        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => Expr::Case {
            operand: operand
                .as_ref()
                .map(|o| Box::new(substitute_column_refs(o, inner_exprs))),
            when_clauses: when_clauses
                .iter()
                .map(|wc| yachtsql_ir::WhenClause {
                    condition: substitute_column_refs(&wc.condition, inner_exprs),
                    result: substitute_column_refs(&wc.result, inner_exprs),
                })
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|e| Box::new(substitute_column_refs(e, inner_exprs))),
        },

        Expr::Cast {
            expr: inner,
            data_type,
            safe,
        } => Expr::Cast {
            expr: Box::new(substitute_column_refs(inner, inner_exprs)),
            data_type: data_type.clone(),
            safe: *safe,
        },

        Expr::IsNull {
            expr: inner,
            negated,
        } => Expr::IsNull {
            expr: Box::new(substitute_column_refs(inner, inner_exprs)),
            negated: *negated,
        },

        Expr::IsDistinctFrom {
            left,
            right,
            negated,
        } => Expr::IsDistinctFrom {
            left: Box::new(substitute_column_refs(left, inner_exprs)),
            right: Box::new(substitute_column_refs(right, inner_exprs)),
            negated: *negated,
        },

        Expr::InList {
            expr: inner,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(substitute_column_refs(inner, inner_exprs)),
            list: list
                .iter()
                .map(|l| substitute_column_refs(l, inner_exprs))
                .collect(),
            negated: *negated,
        },

        Expr::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => Expr::InSubquery {
            expr: Box::new(substitute_column_refs(inner, inner_exprs)),
            subquery: subquery.clone(),
            negated: *negated,
        },

        Expr::InUnnest {
            expr: inner,
            array_expr,
            negated,
        } => Expr::InUnnest {
            expr: Box::new(substitute_column_refs(inner, inner_exprs)),
            array_expr: Box::new(substitute_column_refs(array_expr, inner_exprs)),
            negated: *negated,
        },

        Expr::Exists { subquery, negated } => Expr::Exists {
            subquery: subquery.clone(),
            negated: *negated,
        },

        Expr::Between {
            expr: inner,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(substitute_column_refs(inner, inner_exprs)),
            low: Box::new(substitute_column_refs(low, inner_exprs)),
            high: Box::new(substitute_column_refs(high, inner_exprs)),
            negated: *negated,
        },

        Expr::Like {
            expr: inner,
            pattern,
            negated,
            case_insensitive,
        } => Expr::Like {
            expr: Box::new(substitute_column_refs(inner, inner_exprs)),
            pattern: Box::new(substitute_column_refs(pattern, inner_exprs)),
            negated: *negated,
            case_insensitive: *case_insensitive,
        },

        Expr::Extract { field, expr: inner } => Expr::Extract {
            field: *field,
            expr: Box::new(substitute_column_refs(inner, inner_exprs)),
        },

        Expr::Substring {
            expr: inner,
            start,
            length,
        } => Expr::Substring {
            expr: Box::new(substitute_column_refs(inner, inner_exprs)),
            start: start
                .as_ref()
                .map(|s| Box::new(substitute_column_refs(s, inner_exprs))),
            length: length
                .as_ref()
                .map(|l| Box::new(substitute_column_refs(l, inner_exprs))),
        },

        Expr::Trim {
            expr: inner,
            trim_what,
            trim_where,
        } => Expr::Trim {
            expr: Box::new(substitute_column_refs(inner, inner_exprs)),
            trim_what: trim_what
                .as_ref()
                .map(|t| Box::new(substitute_column_refs(t, inner_exprs))),
            trim_where: *trim_where,
        },

        Expr::Position { substr, string } => Expr::Position {
            substr: Box::new(substitute_column_refs(substr, inner_exprs)),
            string: Box::new(substitute_column_refs(string, inner_exprs)),
        },

        Expr::Overlay {
            expr: inner,
            overlay_what,
            overlay_from,
            overlay_for,
        } => Expr::Overlay {
            expr: Box::new(substitute_column_refs(inner, inner_exprs)),
            overlay_what: Box::new(substitute_column_refs(overlay_what, inner_exprs)),
            overlay_from: Box::new(substitute_column_refs(overlay_from, inner_exprs)),
            overlay_for: overlay_for
                .as_ref()
                .map(|o| Box::new(substitute_column_refs(o, inner_exprs))),
        },

        Expr::Array {
            elements,
            element_type,
        } => Expr::Array {
            elements: elements
                .iter()
                .map(|e| substitute_column_refs(e, inner_exprs))
                .collect(),
            element_type: element_type.clone(),
        },

        Expr::ArrayAccess { array, index } => Expr::ArrayAccess {
            array: Box::new(substitute_column_refs(array, inner_exprs)),
            index: Box::new(substitute_column_refs(index, inner_exprs)),
        },

        Expr::Struct { fields } => Expr::Struct {
            fields: fields
                .iter()
                .map(|(name, e)| (name.clone(), substitute_column_refs(e, inner_exprs)))
                .collect(),
        },

        Expr::StructAccess { expr: inner, field } => Expr::StructAccess {
            expr: Box::new(substitute_column_refs(inner, inner_exprs)),
            field: field.clone(),
        },

        Expr::TypedString { data_type, value } => Expr::TypedString {
            data_type: data_type.clone(),
            value: value.clone(),
        },

        Expr::Interval {
            value,
            leading_field,
        } => Expr::Interval {
            value: Box::new(substitute_column_refs(value, inner_exprs)),
            leading_field: *leading_field,
        },

        Expr::Alias { expr: inner, name } => Expr::Alias {
            expr: Box::new(substitute_column_refs(inner, inner_exprs)),
            name: name.clone(),
        },

        Expr::Wildcard { table } => Expr::Wildcard {
            table: table.clone(),
        },

        Expr::Subquery(subquery) => Expr::Subquery(subquery.clone()),

        Expr::ScalarSubquery(subquery) => Expr::ScalarSubquery(subquery.clone()),

        Expr::ArraySubquery(subquery) => Expr::ArraySubquery(subquery.clone()),

        Expr::Parameter { name } => Expr::Parameter { name: name.clone() },

        Expr::Variable { name } => Expr::Variable { name: name.clone() },

        Expr::Placeholder { id } => Expr::Placeholder { id: id.clone() },

        Expr::Lambda { params, body } => Expr::Lambda {
            params: params.clone(),
            body: Box::new(substitute_column_refs(body, inner_exprs)),
        },

        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => Expr::AtTimeZone {
            timestamp: Box::new(substitute_column_refs(timestamp, inner_exprs)),
            time_zone: Box::new(substitute_column_refs(time_zone, inner_exprs)),
        },

        Expr::JsonAccess { expr: inner, path } => Expr::JsonAccess {
            expr: Box::new(substitute_column_refs(inner, inner_exprs)),
            path: path.clone(),
        },

        Expr::Default => Expr::Default,
    }
}

pub fn apply_project_merging(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Project {
            input,
            expressions: outer_exprs,
            schema: outer_schema,
        } => {
            let optimized_input = apply_project_merging(*input);

            match optimized_input {
                PhysicalPlan::Project {
                    input: inner_input,
                    expressions: inner_exprs,
                    schema: inner_schema,
                } => {
                    if can_merge_projects(&outer_exprs, &inner_exprs) {
                        let merged_exprs: Vec<Expr> = outer_exprs
                            .iter()
                            .map(|e| substitute_column_refs(e, &inner_exprs))
                            .collect();

                        PhysicalPlan::Project {
                            input: inner_input,
                            expressions: merged_exprs,
                            schema: outer_schema,
                        }
                    } else {
                        PhysicalPlan::Project {
                            input: Box::new(PhysicalPlan::Project {
                                input: inner_input,
                                expressions: inner_exprs,
                                schema: inner_schema,
                            }),
                            expressions: outer_exprs,
                            schema: outer_schema,
                        }
                    }
                }
                other => PhysicalPlan::Project {
                    input: Box::new(other),
                    expressions: outer_exprs,
                    schema: outer_schema,
                },
            }
        }

        PhysicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(apply_project_merging(*input)),
            predicate,
        },

        PhysicalPlan::HashAggregate {
            input,
            group_by,
            aggregates,
            schema,
            grouping_sets,
            hints,
        } => PhysicalPlan::HashAggregate {
            input: Box::new(apply_project_merging(*input)),
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
            left: Box::new(apply_project_merging(*left)),
            right: Box::new(apply_project_merging(*right)),
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
            left: Box::new(apply_project_merging(*left)),
            right: Box::new(apply_project_merging(*right)),
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
            left: Box::new(apply_project_merging(*left)),
            right: Box::new(apply_project_merging(*right)),
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::Sort {
            input,
            sort_exprs,
            hints,
        } => PhysicalPlan::Sort {
            input: Box::new(apply_project_merging(*input)),
            sort_exprs,
            hints,
        },

        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => PhysicalPlan::TopN {
            input: Box::new(apply_project_merging(*input)),
            sort_exprs,
            limit,
        },

        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => PhysicalPlan::Limit {
            input: Box::new(apply_project_merging(*input)),
            limit,
            offset,
        },

        PhysicalPlan::Distinct { input } => PhysicalPlan::Distinct {
            input: Box::new(apply_project_merging(*input)),
        },

        PhysicalPlan::Union {
            inputs,
            all,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::Union {
            inputs: inputs.into_iter().map(apply_project_merging).collect(),
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
            left: Box::new(apply_project_merging(*left)),
            right: Box::new(apply_project_merging(*right)),
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
            left: Box::new(apply_project_merging(*left)),
            right: Box::new(apply_project_merging(*right)),
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
            input: Box::new(apply_project_merging(*input)),
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
            body: Box::new(apply_project_merging(*body)),
            parallel_ctes,
            hints,
        },

        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => PhysicalPlan::Unnest {
            input: Box::new(apply_project_merging(*input)),
            columns,
            schema,
        },

        PhysicalPlan::Qualify { input, predicate } => PhysicalPlan::Qualify {
            input: Box::new(apply_project_merging(*input)),
            predicate,
        },

        PhysicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => PhysicalPlan::Sample {
            input: Box::new(apply_project_merging(*input)),
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
            source: Box::new(apply_project_merging(*source)),
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
            query: query.map(|q| Box::new(apply_project_merging(*q))),
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
            query: Box::new(apply_project_merging(*query)),
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
            source: Box::new(apply_project_merging(*source)),
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
            from: from.map(|f| Box::new(apply_project_merging(*f))),
            filter,
        },

        PhysicalPlan::ExportData { options, query } => PhysicalPlan::ExportData {
            options,
            query: Box::new(apply_project_merging(*query)),
        },

        PhysicalPlan::For {
            variable,
            query,
            body,
        } => PhysicalPlan::For {
            variable,
            query: Box::new(apply_project_merging(*query)),
            body: body.into_iter().map(apply_project_merging).collect(),
        },

        PhysicalPlan::If {
            condition,
            then_branch,
            else_branch,
        } => PhysicalPlan::If {
            condition,
            then_branch: then_branch.into_iter().map(apply_project_merging).collect(),
            else_branch: else_branch.map(|b| b.into_iter().map(apply_project_merging).collect()),
        },

        PhysicalPlan::While {
            condition,
            body,
            label,
        } => PhysicalPlan::While {
            condition,
            body: body.into_iter().map(apply_project_merging).collect(),
            label,
        },

        PhysicalPlan::Loop { body, label } => PhysicalPlan::Loop {
            body: body.into_iter().map(apply_project_merging).collect(),
            label,
        },

        PhysicalPlan::Block { body, label } => PhysicalPlan::Block {
            body: body.into_iter().map(apply_project_merging).collect(),
            label,
        },

        PhysicalPlan::Repeat {
            body,
            until_condition,
        } => PhysicalPlan::Repeat {
            body: body.into_iter().map(apply_project_merging).collect(),
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
            body: body.into_iter().map(apply_project_merging).collect(),
            or_replace,
            if_not_exists,
        },

        PhysicalPlan::TryCatch {
            try_block,
            catch_block,
        } => PhysicalPlan::TryCatch {
            try_block: try_block
                .into_iter()
                .map(|(p, sql)| (apply_project_merging(p), sql))
                .collect(),
            catch_block: catch_block.into_iter().map(apply_project_merging).collect(),
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
            input: Box::new(apply_project_merging(*input)),
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
            input: Box::new(apply_project_merging(*input)),
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
    use yachtsql_ir::{BinaryOp, Literal, PlanField, PlanSchema, ScalarFunction};

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
    fn merges_two_adjacent_projects() {
        let scan = make_scan("t", 3);

        let inner_exprs = vec![
            col_expr(0, "col0"),
            col_expr(1, "col1"),
            col_expr(2, "col2"),
        ];
        let inner_project = PhysicalPlan::Project {
            input: Box::new(scan),
            expressions: inner_exprs,
            schema: make_schema(3),
        };

        let outer_exprs = vec![col_expr(0, "col0"), col_expr(1, "col1")];
        let outer_project = PhysicalPlan::Project {
            input: Box::new(inner_project),
            expressions: outer_exprs,
            schema: make_schema(2),
        };

        let result = apply_project_merging(outer_project);

        match result {
            PhysicalPlan::Project {
                input,
                expressions,
                schema,
            } => {
                assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
                assert_eq!(expressions.len(), 2);
                assert_eq!(schema.fields.len(), 2);
            }
            _ => panic!("Expected Project plan"),
        }
    }

    #[test]
    fn substitutes_column_refs_in_expressions() {
        let scan = make_scan("t", 3);

        let inner_exprs = vec![
            Expr::BinaryOp {
                left: Box::new(col_expr(0, "col0")),
                op: BinaryOp::Add,
                right: Box::new(Expr::Literal(Literal::Int64(1))),
            },
            col_expr(1, "col1"),
        ];
        let inner_project = PhysicalPlan::Project {
            input: Box::new(scan),
            expressions: inner_exprs,
            schema: make_schema(2),
        };

        let outer_exprs = vec![Expr::BinaryOp {
            left: Box::new(col_expr(0, "x")),
            op: BinaryOp::Mul,
            right: Box::new(Expr::Literal(Literal::Int64(2))),
        }];
        let outer_project = PhysicalPlan::Project {
            input: Box::new(inner_project),
            expressions: outer_exprs,
            schema: make_schema(1),
        };

        let result = apply_project_merging(outer_project);

        match result {
            PhysicalPlan::Project {
                input, expressions, ..
            } => {
                assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
                assert_eq!(expressions.len(), 1);

                match &expressions[0] {
                    Expr::BinaryOp {
                        left,
                        op: BinaryOp::Mul,
                        right,
                    } => {
                        match left.as_ref() {
                            Expr::BinaryOp {
                                op: BinaryOp::Add, ..
                            } => {}
                            _ => panic!("Expected inner expression to be (col0 + 1)"),
                        }
                        assert_eq!(**right, Expr::Literal(Literal::Int64(2)));
                    }
                    _ => panic!("Expected multiplication expression"),
                }
            }
            _ => panic!("Expected Project plan"),
        }
    }

    #[test]
    fn merges_three_adjacent_projects() {
        let scan = make_scan("t", 3);

        let proj1 = PhysicalPlan::Project {
            input: Box::new(scan),
            expressions: vec![
                col_expr(0, "col0"),
                col_expr(1, "col1"),
                col_expr(2, "col2"),
            ],
            schema: make_schema(3),
        };

        let proj2 = PhysicalPlan::Project {
            input: Box::new(proj1),
            expressions: vec![col_expr(0, "col0"), col_expr(1, "col1")],
            schema: make_schema(2),
        };

        let proj3 = PhysicalPlan::Project {
            input: Box::new(proj2),
            expressions: vec![col_expr(0, "col0")],
            schema: make_schema(1),
        };

        let result = apply_project_merging(proj3);

        match result {
            PhysicalPlan::Project {
                input, expressions, ..
            } => {
                assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
                assert_eq!(expressions.len(), 1);
            }
            _ => panic!("Expected Project plan"),
        }
    }

    #[test]
    fn preserves_single_project() {
        let scan = make_scan("t", 3);

        let project = PhysicalPlan::Project {
            input: Box::new(scan),
            expressions: vec![col_expr(0, "col0"), col_expr(1, "col1")],
            schema: make_schema(2),
        };

        let result = apply_project_merging(project);

        match result {
            PhysicalPlan::Project {
                input, expressions, ..
            } => {
                assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
                assert_eq!(expressions.len(), 2);
            }
            _ => panic!("Expected Project plan"),
        }
    }

    #[test]
    fn preserves_project_with_non_project_input() {
        let scan = make_scan("t", 3);

        let filter = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: Expr::Literal(Literal::Bool(true)),
        };

        let project = PhysicalPlan::Project {
            input: Box::new(filter),
            expressions: vec![col_expr(0, "col0")],
            schema: make_schema(1),
        };

        let result = apply_project_merging(project);

        match result {
            PhysicalPlan::Project { input, .. } => {
                assert!(matches!(*input, PhysicalPlan::Filter { .. }));
            }
            _ => panic!("Expected Project plan"),
        }
    }

    #[test]
    fn handles_literal_expressions() {
        let scan = make_scan("t", 3);

        let inner_exprs = vec![
            Expr::Literal(Literal::Int64(100)),
            Expr::Literal(Literal::String("test".to_string())),
        ];
        let inner_project = PhysicalPlan::Project {
            input: Box::new(scan),
            expressions: inner_exprs,
            schema: make_schema(2),
        };

        let outer_exprs = vec![col_expr(0, "lit1"), col_expr(1, "lit2")];
        let outer_project = PhysicalPlan::Project {
            input: Box::new(inner_project),
            expressions: outer_exprs,
            schema: make_schema(2),
        };

        let result = apply_project_merging(outer_project);

        match result {
            PhysicalPlan::Project {
                input, expressions, ..
            } => {
                assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
                assert_eq!(expressions.len(), 2);
                assert_eq!(expressions[0], Expr::Literal(Literal::Int64(100)));
                assert_eq!(
                    expressions[1],
                    Expr::Literal(Literal::String("test".to_string()))
                );
            }
            _ => panic!("Expected Project plan"),
        }
    }

    #[test]
    fn handles_function_expressions() {
        let scan = make_scan("t", 3);

        let inner_exprs = vec![
            Expr::ScalarFunction {
                name: ScalarFunction::Upper,
                args: vec![col_expr(0, "col0")],
            },
            col_expr(1, "col1"),
        ];
        let inner_project = PhysicalPlan::Project {
            input: Box::new(scan),
            expressions: inner_exprs,
            schema: make_schema(2),
        };

        let outer_exprs = vec![Expr::ScalarFunction {
            name: ScalarFunction::Lower,
            args: vec![col_expr(0, "upper_col")],
        }];
        let outer_project = PhysicalPlan::Project {
            input: Box::new(inner_project),
            expressions: outer_exprs,
            schema: make_schema(1),
        };

        let result = apply_project_merging(outer_project);

        match result {
            PhysicalPlan::Project {
                input, expressions, ..
            } => {
                assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
                assert_eq!(expressions.len(), 1);

                match &expressions[0] {
                    Expr::ScalarFunction {
                        name: ScalarFunction::Lower,
                        args,
                    } => match &args[0] {
                        Expr::ScalarFunction {
                            name: ScalarFunction::Upper,
                            ..
                        } => {}
                        _ => panic!("Expected UPPER function"),
                    },
                    _ => panic!("Expected LOWER function"),
                }
            }
            _ => panic!("Expected Project plan"),
        }
    }

    #[test]
    fn handles_case_expressions() {
        let scan = make_scan("t", 3);

        let inner_exprs = vec![col_expr(0, "col0"), Expr::Literal(Literal::Int64(10))];
        let inner_project = PhysicalPlan::Project {
            input: Box::new(scan),
            expressions: inner_exprs,
            schema: make_schema(2),
        };

        let outer_exprs = vec![Expr::Case {
            operand: None,
            when_clauses: vec![yachtsql_ir::WhenClause {
                condition: Expr::BinaryOp {
                    left: Box::new(col_expr(0, "col0")),
                    op: BinaryOp::Gt,
                    right: Box::new(col_expr(1, "threshold")),
                },
                result: Expr::Literal(Literal::String("big".to_string())),
            }],
            else_result: Some(Box::new(Expr::Literal(Literal::String(
                "small".to_string(),
            )))),
        }];
        let outer_project = PhysicalPlan::Project {
            input: Box::new(inner_project),
            expressions: outer_exprs,
            schema: make_schema(1),
        };

        let result = apply_project_merging(outer_project);

        match result {
            PhysicalPlan::Project {
                input, expressions, ..
            } => {
                assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
                assert_eq!(expressions.len(), 1);
                assert!(matches!(&expressions[0], Expr::Case { .. }));
            }
            _ => panic!("Expected Project plan"),
        }
    }

    #[test]
    fn recurses_through_filter() {
        let scan = make_scan("t", 3);

        let proj1 = PhysicalPlan::Project {
            input: Box::new(scan),
            expressions: vec![col_expr(0, "col0"), col_expr(1, "col1")],
            schema: make_schema(2),
        };

        let proj2 = PhysicalPlan::Project {
            input: Box::new(proj1),
            expressions: vec![col_expr(0, "col0")],
            schema: make_schema(1),
        };

        let filter = PhysicalPlan::Filter {
            input: Box::new(proj2),
            predicate: Expr::Literal(Literal::Bool(true)),
        };

        let result = apply_project_merging(filter);

        match result {
            PhysicalPlan::Filter { input, .. } => match *input {
                PhysicalPlan::Project { input: inner, .. } => {
                    assert!(matches!(*inner, PhysicalPlan::TableScan { .. }));
                }
                _ => panic!("Expected inner Project"),
            },
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn handles_alias_expressions() {
        let scan = make_scan("t", 2);

        let inner_exprs = vec![Expr::Alias {
            expr: Box::new(col_expr(0, "col0")),
            name: "aliased".to_string(),
        }];
        let inner_project = PhysicalPlan::Project {
            input: Box::new(scan),
            expressions: inner_exprs,
            schema: make_schema(1),
        };

        let outer_exprs = vec![col_expr(0, "aliased")];
        let outer_project = PhysicalPlan::Project {
            input: Box::new(inner_project),
            expressions: outer_exprs,
            schema: make_schema(1),
        };

        let result = apply_project_merging(outer_project);

        match result {
            PhysicalPlan::Project {
                input, expressions, ..
            } => {
                assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
                assert_eq!(expressions.len(), 1);
                match &expressions[0] {
                    Expr::Alias { name, .. } => {
                        assert_eq!(name, "aliased");
                    }
                    _ => panic!("Expected Alias expression"),
                }
            }
            _ => panic!("Expected Project plan"),
        }
    }

    #[test]
    fn handles_cast_expressions() {
        let scan = make_scan("t", 2);

        let inner_exprs = vec![Expr::Cast {
            expr: Box::new(col_expr(0, "col0")),
            data_type: DataType::Float64,
            safe: false,
        }];
        let inner_project = PhysicalPlan::Project {
            input: Box::new(scan),
            expressions: inner_exprs,
            schema: make_schema(1),
        };

        let outer_exprs = vec![Expr::Cast {
            expr: Box::new(col_expr(0, "float_col")),
            data_type: DataType::String,
            safe: true,
        }];
        let outer_project = PhysicalPlan::Project {
            input: Box::new(inner_project),
            expressions: outer_exprs,
            schema: make_schema(1),
        };

        let result = apply_project_merging(outer_project);

        match result {
            PhysicalPlan::Project {
                input, expressions, ..
            } => {
                assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
                assert_eq!(expressions.len(), 1);
                match &expressions[0] {
                    Expr::Cast {
                        expr,
                        data_type: DataType::String,
                        safe: true,
                    } => match expr.as_ref() {
                        Expr::Cast {
                            data_type: DataType::Float64,
                            safe: false,
                            ..
                        } => {}
                        _ => panic!("Expected inner CAST to Float64"),
                    },
                    _ => panic!("Expected CAST to String"),
                }
            }
            _ => panic!("Expected Project plan"),
        }
    }
}
