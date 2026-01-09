#![coverage(off)]

use yachtsql_ir::{BinaryOp, Expr, SortExpr, UnaryOp, WhenClause};

use crate::PhysicalPlan;

pub fn apply_predicate_simplification(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Filter { input, predicate } => {
            let optimized_input = apply_predicate_simplification(*input);
            let simplified_predicate = simplify_expr(predicate);
            PhysicalPlan::Filter {
                input: Box::new(optimized_input),
                predicate: simplified_predicate,
            }
        }

        PhysicalPlan::Qualify { input, predicate } => {
            let optimized_input = apply_predicate_simplification(*input);
            let simplified_predicate = simplify_expr(predicate);
            PhysicalPlan::Qualify {
                input: Box::new(optimized_input),
                predicate: simplified_predicate,
            }
        }

        PhysicalPlan::Project {
            input,
            expressions,
            schema,
        } => PhysicalPlan::Project {
            input: Box::new(apply_predicate_simplification(*input)),
            expressions: expressions.into_iter().map(simplify_expr).collect(),
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
            input: Box::new(apply_predicate_simplification(*input)),
            group_by: group_by.into_iter().map(simplify_expr).collect(),
            aggregates: aggregates.into_iter().map(simplify_expr).collect(),
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
            left: Box::new(apply_predicate_simplification(*left)),
            right: Box::new(apply_predicate_simplification(*right)),
            join_type,
            left_keys: left_keys.into_iter().map(simplify_expr).collect(),
            right_keys: right_keys.into_iter().map(simplify_expr).collect(),
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
            left: Box::new(apply_predicate_simplification(*left)),
            right: Box::new(apply_predicate_simplification(*right)),
            join_type,
            condition: condition.map(simplify_expr),
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
            left: Box::new(apply_predicate_simplification(*left)),
            right: Box::new(apply_predicate_simplification(*right)),
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::Sort {
            input,
            sort_exprs,
            hints,
        } => PhysicalPlan::Sort {
            input: Box::new(apply_predicate_simplification(*input)),
            sort_exprs: sort_exprs.into_iter().map(simplify_sort_expr).collect(),
            hints,
        },

        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => PhysicalPlan::TopN {
            input: Box::new(apply_predicate_simplification(*input)),
            sort_exprs: sort_exprs.into_iter().map(simplify_sort_expr).collect(),
            limit,
        },

        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => PhysicalPlan::Limit {
            input: Box::new(apply_predicate_simplification(*input)),
            limit,
            offset,
        },

        PhysicalPlan::Distinct { input } => PhysicalPlan::Distinct {
            input: Box::new(apply_predicate_simplification(*input)),
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
                .map(apply_predicate_simplification)
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
            left: Box::new(apply_predicate_simplification(*left)),
            right: Box::new(apply_predicate_simplification(*right)),
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
            left: Box::new(apply_predicate_simplification(*left)),
            right: Box::new(apply_predicate_simplification(*right)),
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
            input: Box::new(apply_predicate_simplification(*input)),
            window_exprs: window_exprs.into_iter().map(simplify_expr).collect(),
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
            body: Box::new(apply_predicate_simplification(*body)),
            parallel_ctes,
            hints,
        },

        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => PhysicalPlan::Unnest {
            input: Box::new(apply_predicate_simplification(*input)),
            columns,
            schema,
        },

        PhysicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => PhysicalPlan::Sample {
            input: Box::new(apply_predicate_simplification(*input)),
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
            source: Box::new(apply_predicate_simplification(*source)),
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
            query: query.map(|q| Box::new(apply_predicate_simplification(*q))),
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
            query: Box::new(apply_predicate_simplification(*query)),
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
            source: Box::new(apply_predicate_simplification(*source)),
            on: simplify_expr(on),
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
            from: from.map(|f| Box::new(apply_predicate_simplification(*f))),
            filter: filter.map(simplify_expr),
        },

        PhysicalPlan::Delete {
            table_name,
            alias,
            filter,
        } => PhysicalPlan::Delete {
            table_name,
            alias,
            filter: filter.map(simplify_expr),
        },

        PhysicalPlan::ExportData { options, query } => PhysicalPlan::ExportData {
            options,
            query: Box::new(apply_predicate_simplification(*query)),
        },

        PhysicalPlan::For {
            variable,
            query,
            body,
        } => PhysicalPlan::For {
            variable,
            query: Box::new(apply_predicate_simplification(*query)),
            body: body
                .into_iter()
                .map(apply_predicate_simplification)
                .collect(),
        },

        PhysicalPlan::If {
            condition,
            then_branch,
            else_branch,
        } => PhysicalPlan::If {
            condition: simplify_expr(condition),
            then_branch: then_branch
                .into_iter()
                .map(apply_predicate_simplification)
                .collect(),
            else_branch: else_branch
                .map(|b| b.into_iter().map(apply_predicate_simplification).collect()),
        },

        PhysicalPlan::While {
            condition,
            body,
            label,
        } => PhysicalPlan::While {
            condition: simplify_expr(condition),
            body: body
                .into_iter()
                .map(apply_predicate_simplification)
                .collect(),
            label,
        },

        PhysicalPlan::Loop { body, label } => PhysicalPlan::Loop {
            body: body
                .into_iter()
                .map(apply_predicate_simplification)
                .collect(),
            label,
        },

        PhysicalPlan::Block { body, label } => PhysicalPlan::Block {
            body: body
                .into_iter()
                .map(apply_predicate_simplification)
                .collect(),
            label,
        },

        PhysicalPlan::Repeat {
            body,
            until_condition,
        } => PhysicalPlan::Repeat {
            body: body
                .into_iter()
                .map(apply_predicate_simplification)
                .collect(),
            until_condition: simplify_expr(until_condition),
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
                .map(apply_predicate_simplification)
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
                .map(|(p, sql)| (apply_predicate_simplification(p), sql))
                .collect(),
            catch_block: catch_block
                .into_iter()
                .map(apply_predicate_simplification)
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
            input: Box::new(apply_predicate_simplification(*input)),
            ts_column,
            bucket_width: simplify_expr(bucket_width),
            value_columns,
            partitioning_columns,
            origin: origin.map(simplify_expr),
            input_schema,
            schema,
        },

        PhysicalPlan::Explain {
            input,
            analyze,
            logical_plan_text,
            physical_plan_text,
        } => PhysicalPlan::Explain {
            input: Box::new(apply_predicate_simplification(*input)),
            analyze,
            logical_plan_text,
            physical_plan_text,
        },

        PhysicalPlan::Assert { condition, message } => PhysicalPlan::Assert {
            condition: simplify_expr(condition),
            message: message.map(simplify_expr),
        },

        PhysicalPlan::Values { values, schema } => PhysicalPlan::Values {
            values: values
                .into_iter()
                .map(|row| row.into_iter().map(simplify_expr).collect())
                .collect(),
            schema,
        },

        other => other,
    }
}

fn simplify_sort_expr(sort_expr: SortExpr) -> SortExpr {
    SortExpr {
        expr: simplify_expr(sort_expr.expr),
        asc: sort_expr.asc,
        nulls_first: sort_expr.nulls_first,
    }
}

fn simplify_expr(expr: Expr) -> Expr {
    match expr {
        Expr::BinaryOp { left, op, right } => simplify_binary_op(*left, op, *right),

        Expr::UnaryOp { op, expr: inner } => simplify_unary_op(op, *inner),

        Expr::InList {
            expr: inner,
            list,
            negated,
        } => simplify_in_list(*inner, list, negated),

        Expr::Between {
            expr: inner,
            low,
            high,
            negated,
        } => simplify_between(*inner, *low, *high, negated),

        Expr::ScalarFunction { name, args } => Expr::ScalarFunction {
            name,
            args: args.into_iter().map(simplify_expr).collect(),
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
            func,
            args: args.into_iter().map(simplify_expr).collect(),
            distinct,
            filter: filter.map(|f| Box::new(simplify_expr(*f))),
            order_by: order_by.into_iter().map(simplify_sort_expr).collect(),
            limit,
            ignore_nulls,
        },

        Expr::UserDefinedAggregate {
            name,
            args,
            distinct,
            filter,
        } => Expr::UserDefinedAggregate {
            name,
            args: args.into_iter().map(simplify_expr).collect(),
            distinct,
            filter: filter.map(|f| Box::new(simplify_expr(*f))),
        },

        Expr::Window {
            func,
            args,
            partition_by,
            order_by,
            frame,
        } => Expr::Window {
            func,
            args: args.into_iter().map(simplify_expr).collect(),
            partition_by: partition_by.into_iter().map(simplify_expr).collect(),
            order_by: order_by.into_iter().map(simplify_sort_expr).collect(),
            frame,
        },

        Expr::AggregateWindow {
            func,
            args,
            distinct,
            partition_by,
            order_by,
            frame,
        } => Expr::AggregateWindow {
            func,
            args: args.into_iter().map(simplify_expr).collect(),
            distinct,
            partition_by: partition_by.into_iter().map(simplify_expr).collect(),
            order_by: order_by.into_iter().map(simplify_sort_expr).collect(),
            frame,
        },

        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => Expr::Case {
            operand: operand.map(|o| Box::new(simplify_expr(*o))),
            when_clauses: when_clauses
                .into_iter()
                .map(|wc| WhenClause {
                    condition: simplify_expr(wc.condition),
                    result: simplify_expr(wc.result),
                })
                .collect(),
            else_result: else_result.map(|e| Box::new(simplify_expr(*e))),
        },

        Expr::Cast {
            expr: inner,
            data_type,
            safe,
        } => Expr::Cast {
            expr: Box::new(simplify_expr(*inner)),
            data_type,
            safe,
        },

        Expr::IsNull {
            expr: inner,
            negated,
        } => Expr::IsNull {
            expr: Box::new(simplify_expr(*inner)),
            negated,
        },

        Expr::IsDistinctFrom {
            left,
            right,
            negated,
        } => Expr::IsDistinctFrom {
            left: Box::new(simplify_expr(*left)),
            right: Box::new(simplify_expr(*right)),
            negated,
        },

        Expr::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => Expr::InSubquery {
            expr: Box::new(simplify_expr(*inner)),
            subquery,
            negated,
        },

        Expr::InUnnest {
            expr: inner,
            array_expr,
            negated,
        } => Expr::InUnnest {
            expr: Box::new(simplify_expr(*inner)),
            array_expr: Box::new(simplify_expr(*array_expr)),
            negated,
        },

        Expr::Like {
            expr: inner,
            pattern,
            negated,
            case_insensitive,
        } => Expr::Like {
            expr: Box::new(simplify_expr(*inner)),
            pattern: Box::new(simplify_expr(*pattern)),
            negated,
            case_insensitive,
        },

        Expr::Extract { field, expr: inner } => Expr::Extract {
            field,
            expr: Box::new(simplify_expr(*inner)),
        },

        Expr::Substring {
            expr: inner,
            start,
            length,
        } => Expr::Substring {
            expr: Box::new(simplify_expr(*inner)),
            start: start.map(|s| Box::new(simplify_expr(*s))),
            length: length.map(|l| Box::new(simplify_expr(*l))),
        },

        Expr::Trim {
            expr: inner,
            trim_what,
            trim_where,
        } => Expr::Trim {
            expr: Box::new(simplify_expr(*inner)),
            trim_what: trim_what.map(|t| Box::new(simplify_expr(*t))),
            trim_where,
        },

        Expr::Position { substr, string } => Expr::Position {
            substr: Box::new(simplify_expr(*substr)),
            string: Box::new(simplify_expr(*string)),
        },

        Expr::Overlay {
            expr: inner,
            overlay_what,
            overlay_from,
            overlay_for,
        } => Expr::Overlay {
            expr: Box::new(simplify_expr(*inner)),
            overlay_what: Box::new(simplify_expr(*overlay_what)),
            overlay_from: Box::new(simplify_expr(*overlay_from)),
            overlay_for: overlay_for.map(|o| Box::new(simplify_expr(*o))),
        },

        Expr::Array {
            elements,
            element_type,
        } => Expr::Array {
            elements: elements.into_iter().map(simplify_expr).collect(),
            element_type,
        },

        Expr::ArrayAccess { array, index } => Expr::ArrayAccess {
            array: Box::new(simplify_expr(*array)),
            index: Box::new(simplify_expr(*index)),
        },

        Expr::Struct { fields } => Expr::Struct {
            fields: fields
                .into_iter()
                .map(|(name, e)| (name, simplify_expr(e)))
                .collect(),
        },

        Expr::StructAccess { expr: inner, field } => Expr::StructAccess {
            expr: Box::new(simplify_expr(*inner)),
            field,
        },

        Expr::Interval {
            value,
            leading_field,
        } => Expr::Interval {
            value: Box::new(simplify_expr(*value)),
            leading_field,
        },

        Expr::Alias { expr: inner, name } => Expr::Alias {
            expr: Box::new(simplify_expr(*inner)),
            name,
        },

        Expr::Lambda { params, body } => Expr::Lambda {
            params,
            body: Box::new(simplify_expr(*body)),
        },

        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => Expr::AtTimeZone {
            timestamp: Box::new(simplify_expr(*timestamp)),
            time_zone: Box::new(simplify_expr(*time_zone)),
        },

        Expr::JsonAccess { expr: inner, path } => Expr::JsonAccess {
            expr: Box::new(simplify_expr(*inner)),
            path,
        },

        other => other,
    }
}

fn simplify_binary_op(left: Expr, op: BinaryOp, right: Expr) -> Expr {
    let left_simplified = simplify_expr(left);
    let right_simplified = simplify_expr(right);

    match op {
        BinaryOp::And => {
            if left_simplified == right_simplified {
                return left_simplified;
            }
        }
        BinaryOp::Or => {
            if left_simplified == right_simplified {
                return left_simplified;
            }
        }
        _ => {}
    }

    Expr::BinaryOp {
        left: Box::new(left_simplified),
        op,
        right: Box::new(right_simplified),
    }
}

fn simplify_unary_op(op: UnaryOp, inner: Expr) -> Expr {
    let inner_simplified = simplify_expr(inner);

    match op {
        UnaryOp::Not => match inner_simplified {
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: double_negated,
            } => *double_negated,
            _ => Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(inner_simplified),
            },
        },
        _ => Expr::UnaryOp {
            op,
            expr: Box::new(inner_simplified),
        },
    }
}

fn simplify_in_list(inner: Expr, list: Vec<Expr>, negated: bool) -> Expr {
    let inner_simplified = simplify_expr(inner);
    let list_simplified: Vec<Expr> = list.into_iter().map(simplify_expr).collect();

    if list_simplified.len() == 1 {
        let single_value = list_simplified.into_iter().next().unwrap();
        let comparison_op = if negated {
            BinaryOp::NotEq
        } else {
            BinaryOp::Eq
        };
        return Expr::BinaryOp {
            left: Box::new(inner_simplified),
            op: comparison_op,
            right: Box::new(single_value),
        };
    }

    Expr::InList {
        expr: Box::new(inner_simplified),
        list: list_simplified,
        negated,
    }
}

fn simplify_between(inner: Expr, low: Expr, high: Expr, negated: bool) -> Expr {
    let inner_simplified = simplify_expr(inner);
    let low_simplified = simplify_expr(low);
    let high_simplified = simplify_expr(high);

    if !negated && low_simplified == high_simplified {
        return Expr::BinaryOp {
            left: Box::new(inner_simplified),
            op: BinaryOp::Eq,
            right: Box::new(low_simplified),
        };
    }

    Expr::Between {
        expr: Box::new(inner_simplified),
        low: Box::new(low_simplified),
        high: Box::new(high_simplified),
        negated,
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::DataType;
    use yachtsql_ir::{Literal, PlanField, PlanSchema};

    use super::*;

    fn col(name: &str) -> Expr {
        Expr::Column {
            table: None,
            name: name.to_string(),
            index: None,
        }
    }

    fn lit_i64(v: i64) -> Expr {
        Expr::Literal(Literal::Int64(v))
    }

    fn lit_bool(v: bool) -> Expr {
        Expr::Literal(Literal::Bool(v))
    }

    fn binary(left: Expr, op: BinaryOp, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    fn unary(op: UnaryOp, expr: Expr) -> Expr {
        Expr::UnaryOp {
            op,
            expr: Box::new(expr),
        }
    }

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
    fn simplify_x_and_x_to_x() {
        let expr = binary(col("x"), BinaryOp::And, col("x"));
        let result = simplify_expr(expr);
        assert_eq!(result, col("x"));
    }

    #[test]
    fn simplify_x_or_x_to_x() {
        let expr = binary(col("x"), BinaryOp::Or, col("x"));
        let result = simplify_expr(expr);
        assert_eq!(result, col("x"));
    }

    #[test]
    fn simplify_not_not_x_to_x() {
        let expr = unary(UnaryOp::Not, unary(UnaryOp::Not, col("x")));
        let result = simplify_expr(expr);
        assert_eq!(result, col("x"));
    }

    #[test]
    fn simplify_in_single_element_to_eq() {
        let expr = Expr::InList {
            expr: Box::new(col("x")),
            list: vec![lit_i64(42)],
            negated: false,
        };
        let result = simplify_expr(expr);
        assert_eq!(result, binary(col("x"), BinaryOp::Eq, lit_i64(42)));
    }

    #[test]
    fn simplify_not_in_single_element_to_neq() {
        let expr = Expr::InList {
            expr: Box::new(col("x")),
            list: vec![lit_i64(42)],
            negated: true,
        };
        let result = simplify_expr(expr);
        assert_eq!(result, binary(col("x"), BinaryOp::NotEq, lit_i64(42)));
    }

    #[test]
    fn simplify_between_equal_bounds_to_eq() {
        let expr = Expr::Between {
            expr: Box::new(col("x")),
            low: Box::new(lit_i64(10)),
            high: Box::new(lit_i64(10)),
            negated: false,
        };
        let result = simplify_expr(expr);
        assert_eq!(result, binary(col("x"), BinaryOp::Eq, lit_i64(10)));
    }

    #[test]
    fn preserve_between_different_bounds() {
        let expr = Expr::Between {
            expr: Box::new(col("x")),
            low: Box::new(lit_i64(1)),
            high: Box::new(lit_i64(10)),
            negated: false,
        };
        let result = simplify_expr(expr.clone());
        assert_eq!(result, expr);
    }

    #[test]
    fn preserve_negated_between_equal_bounds() {
        let expr = Expr::Between {
            expr: Box::new(col("x")),
            low: Box::new(lit_i64(10)),
            high: Box::new(lit_i64(10)),
            negated: true,
        };
        let result = simplify_expr(expr.clone());
        assert_eq!(result, expr);
    }

    #[test]
    fn preserve_in_list_multiple_elements() {
        let expr = Expr::InList {
            expr: Box::new(col("x")),
            list: vec![lit_i64(1), lit_i64(2), lit_i64(3)],
            negated: false,
        };
        let result = simplify_expr(expr.clone());
        assert_eq!(result, expr);
    }

    #[test]
    fn preserve_different_and_operands() {
        let expr = binary(col("x"), BinaryOp::And, col("y"));
        let result = simplify_expr(expr.clone());
        assert_eq!(result, expr);
    }

    #[test]
    fn preserve_different_or_operands() {
        let expr = binary(col("x"), BinaryOp::Or, col("y"));
        let result = simplify_expr(expr.clone());
        assert_eq!(result, expr);
    }

    #[test]
    fn preserve_single_not() {
        let expr = unary(UnaryOp::Not, col("x"));
        let result = simplify_expr(expr.clone());
        assert_eq!(result, expr);
    }

    #[test]
    fn simplify_nested_x_and_x() {
        let inner = binary(col("a"), BinaryOp::And, col("a"));
        let outer = binary(inner, BinaryOp::Or, col("b"));
        let result = simplify_expr(outer);
        assert_eq!(result, binary(col("a"), BinaryOp::Or, col("b")));
    }

    #[test]
    fn simplify_deeply_nested_not_not_not_not() {
        let expr = unary(
            UnaryOp::Not,
            unary(
                UnaryOp::Not,
                unary(UnaryOp::Not, unary(UnaryOp::Not, col("x"))),
            ),
        );
        let result = simplify_expr(expr);
        assert_eq!(result, col("x"));
    }

    #[test]
    fn simplify_triple_not_to_not() {
        let expr = unary(
            UnaryOp::Not,
            unary(UnaryOp::Not, unary(UnaryOp::Not, col("x"))),
        );
        let result = simplify_expr(expr);
        assert_eq!(result, unary(UnaryOp::Not, col("x")));
    }

    #[test]
    fn simplify_complex_nested_expression() {
        let nested_and = binary(col("x"), BinaryOp::And, col("x"));
        let in_list = Expr::InList {
            expr: Box::new(col("y")),
            list: vec![lit_i64(5)],
            negated: false,
        };
        let double_not = unary(UnaryOp::Not, unary(UnaryOp::Not, col("z")));
        let outer = binary(
            binary(nested_and, BinaryOp::And, in_list),
            BinaryOp::And,
            double_not,
        );

        let result = simplify_expr(outer);

        let expected = binary(
            binary(
                col("x"),
                BinaryOp::And,
                binary(col("y"), BinaryOp::Eq, lit_i64(5)),
            ),
            BinaryOp::And,
            col("z"),
        );
        assert_eq!(result, expected);
    }

    #[test]
    fn simplify_expression_in_case_when() {
        let expr = Expr::Case {
            operand: None,
            when_clauses: vec![yachtsql_ir::WhenClause {
                condition: binary(col("x"), BinaryOp::And, col("x")),
                result: lit_i64(1),
            }],
            else_result: Some(Box::new(lit_i64(0))),
        };
        let result = simplify_expr(expr);

        match result {
            Expr::Case { when_clauses, .. } => {
                assert_eq!(when_clauses[0].condition, col("x"));
            }
            _ => panic!("Expected Case expression"),
        }
    }

    #[test]
    fn simplify_expression_in_alias() {
        let expr = Expr::Alias {
            expr: Box::new(binary(col("x"), BinaryOp::Or, col("x"))),
            name: "simplified".to_string(),
        };
        let result = simplify_expr(expr);

        match result {
            Expr::Alias { expr: inner, name } => {
                assert_eq!(*inner, col("x"));
                assert_eq!(name, "simplified");
            }
            _ => panic!("Expected Alias expression"),
        }
    }

    #[test]
    fn apply_simplification_to_filter() {
        let scan = make_scan("t", 3);
        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: binary(col("x"), BinaryOp::And, col("x")),
        };

        let result = apply_predicate_simplification(plan);

        match result {
            PhysicalPlan::Filter { predicate, .. } => {
                assert_eq!(predicate, col("x"));
            }
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn apply_simplification_to_qualify() {
        let scan = make_scan("t", 3);
        let plan = PhysicalPlan::Qualify {
            input: Box::new(scan),
            predicate: unary(UnaryOp::Not, unary(UnaryOp::Not, col("x"))),
        };

        let result = apply_predicate_simplification(plan);

        match result {
            PhysicalPlan::Qualify { predicate, .. } => {
                assert_eq!(predicate, col("x"));
            }
            _ => panic!("Expected Qualify plan"),
        }
    }

    #[test]
    fn apply_simplification_to_project_expressions() {
        let scan = make_scan("t", 3);
        let schema = make_table_schema("t", 1);
        let plan = PhysicalPlan::Project {
            input: Box::new(scan),
            expressions: vec![binary(col("x"), BinaryOp::Or, col("x"))],
            schema,
        };

        let result = apply_predicate_simplification(plan);

        match result {
            PhysicalPlan::Project { expressions, .. } => {
                assert_eq!(expressions.len(), 1);
                assert_eq!(expressions[0], col("x"));
            }
            _ => panic!("Expected Project plan"),
        }
    }

    #[test]
    fn apply_simplification_nested_plans() {
        let scan = make_scan("t", 3);
        let filter1 = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: binary(col("a"), BinaryOp::And, col("a")),
        };
        let filter2 = PhysicalPlan::Filter {
            input: Box::new(filter1),
            predicate: binary(col("b"), BinaryOp::Or, col("b")),
        };

        let result = apply_predicate_simplification(filter2);

        match result {
            PhysicalPlan::Filter {
                predicate: outer_pred,
                input,
            } => {
                assert_eq!(outer_pred, col("b"));
                match *input {
                    PhysicalPlan::Filter {
                        predicate: inner_pred,
                        ..
                    } => {
                        assert_eq!(inner_pred, col("a"));
                    }
                    _ => panic!("Expected inner Filter"),
                }
            }
            _ => panic!("Expected outer Filter"),
        }
    }

    #[test]
    fn simplify_complex_and_chain() {
        let expr = binary(
            col("x"),
            BinaryOp::And,
            binary(col("x"), BinaryOp::And, col("x")),
        );
        let result = simplify_expr(expr);
        assert_eq!(result, col("x"));
    }

    #[test]
    fn preserve_literal_expression() {
        let expr = lit_bool(true);
        let result = simplify_expr(expr.clone());
        assert_eq!(result, expr);
    }

    #[test]
    fn preserve_column_expression() {
        let expr = col("x");
        let result = simplify_expr(expr.clone());
        assert_eq!(result, expr);
    }

    #[test]
    fn simplify_in_list_with_complex_value() {
        let complex_value = binary(lit_i64(1), BinaryOp::Add, lit_i64(2));
        let expr = Expr::InList {
            expr: Box::new(col("x")),
            list: vec![complex_value.clone()],
            negated: false,
        };
        let result = simplify_expr(expr);
        assert_eq!(result, binary(col("x"), BinaryOp::Eq, complex_value));
    }

    #[test]
    fn simplify_between_with_column_bounds() {
        let expr = Expr::Between {
            expr: Box::new(col("x")),
            low: Box::new(col("a")),
            high: Box::new(col("a")),
            negated: false,
        };
        let result = simplify_expr(expr);
        assert_eq!(result, binary(col("x"), BinaryOp::Eq, col("a")));
    }

    #[test]
    fn preserves_arithmetic_operations() {
        let expr = binary(col("x"), BinaryOp::Add, col("x"));
        let result = simplify_expr(expr.clone());
        assert_eq!(result, expr);
    }

    #[test]
    fn preserves_comparison_with_same_operands() {
        let expr = binary(col("x"), BinaryOp::Eq, col("x"));
        let result = simplify_expr(expr.clone());
        assert_eq!(result, expr);
    }
}
