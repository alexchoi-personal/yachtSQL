#![coverage(off)]

use yachtsql_ir::{CteDefinition, Expr, LogicalPlan};

fn count_cte_references_in_expr(expr: &Expr, cte_name: &str) -> usize {
    match expr {
        Expr::InSubquery { expr, subquery, .. } => {
            count_cte_references_in_expr(expr, cte_name) + count_cte_references(subquery, cte_name)
        }
        Expr::Exists { subquery, .. } => count_cte_references(subquery, cte_name),
        Expr::Subquery(plan) | Expr::ScalarSubquery(plan) | Expr::ArraySubquery(plan) => {
            count_cte_references(plan, cte_name)
        }
        Expr::BinaryOp { left, right, .. } => {
            count_cte_references_in_expr(left, cte_name)
                + count_cte_references_in_expr(right, cte_name)
        }
        Expr::UnaryOp { expr, .. } => count_cte_references_in_expr(expr, cte_name),
        Expr::ScalarFunction { args, .. } => args
            .iter()
            .map(|a| count_cte_references_in_expr(a, cte_name))
            .sum(),
        Expr::Aggregate { args, filter, .. } => {
            let args_count: usize = args
                .iter()
                .map(|a| count_cte_references_in_expr(a, cte_name))
                .sum();
            let filter_count = filter
                .as_ref()
                .map_or(0, |f| count_cte_references_in_expr(f, cte_name));
            args_count + filter_count
        }
        Expr::Window {
            args,
            partition_by,
            order_by,
            ..
        } => {
            let args_count: usize = args
                .iter()
                .map(|a| count_cte_references_in_expr(a, cte_name))
                .sum();
            let partition_count: usize = partition_by
                .iter()
                .map(|a| count_cte_references_in_expr(a, cte_name))
                .sum();
            let order_count: usize = order_by
                .iter()
                .map(|o| count_cte_references_in_expr(&o.expr, cte_name))
                .sum();
            args_count + partition_count + order_count
        }
        Expr::AggregateWindow {
            args,
            partition_by,
            order_by,
            ..
        } => {
            let args_count: usize = args
                .iter()
                .map(|a| count_cte_references_in_expr(a, cte_name))
                .sum();
            let partition_count: usize = partition_by
                .iter()
                .map(|a| count_cte_references_in_expr(a, cte_name))
                .sum();
            let order_count: usize = order_by
                .iter()
                .map(|o| count_cte_references_in_expr(&o.expr, cte_name))
                .sum();
            args_count + partition_count + order_count
        }
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            let op_count = operand
                .as_ref()
                .map_or(0, |o| count_cte_references_in_expr(o, cte_name));
            let when_count: usize = when_clauses
                .iter()
                .map(|w| {
                    count_cte_references_in_expr(&w.condition, cte_name)
                        + count_cte_references_in_expr(&w.result, cte_name)
                })
                .sum();
            let else_count = else_result
                .as_ref()
                .map_or(0, |e| count_cte_references_in_expr(e, cte_name));
            op_count + when_count + else_count
        }
        Expr::Cast { expr, .. } | Expr::IsNull { expr, .. } | Expr::Alias { expr, .. } => {
            count_cte_references_in_expr(expr, cte_name)
        }
        Expr::IsDistinctFrom { left, right, .. } => {
            count_cte_references_in_expr(left, cte_name)
                + count_cte_references_in_expr(right, cte_name)
        }
        Expr::InList { expr, list, .. } => {
            count_cte_references_in_expr(expr, cte_name)
                + list
                    .iter()
                    .map(|e| count_cte_references_in_expr(e, cte_name))
                    .sum::<usize>()
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            count_cte_references_in_expr(expr, cte_name)
                + count_cte_references_in_expr(array_expr, cte_name)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            count_cte_references_in_expr(expr, cte_name)
                + count_cte_references_in_expr(low, cte_name)
                + count_cte_references_in_expr(high, cte_name)
        }
        Expr::Like { expr, pattern, .. } => {
            count_cte_references_in_expr(expr, cte_name)
                + count_cte_references_in_expr(pattern, cte_name)
        }
        Expr::Extract { expr, .. }
        | Expr::StructAccess { expr, .. }
        | Expr::Interval { value: expr, .. } => count_cte_references_in_expr(expr, cte_name),
        Expr::Substring {
            expr,
            start,
            length,
        } => {
            count_cte_references_in_expr(expr, cte_name)
                + start
                    .as_ref()
                    .map_or(0, |s| count_cte_references_in_expr(s, cte_name))
                + length
                    .as_ref()
                    .map_or(0, |l| count_cte_references_in_expr(l, cte_name))
        }
        Expr::Trim {
            expr, trim_what, ..
        } => {
            count_cte_references_in_expr(expr, cte_name)
                + trim_what
                    .as_ref()
                    .map_or(0, |t| count_cte_references_in_expr(t, cte_name))
        }
        Expr::Position { substr, string } => {
            count_cte_references_in_expr(substr, cte_name)
                + count_cte_references_in_expr(string, cte_name)
        }
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            count_cte_references_in_expr(expr, cte_name)
                + count_cte_references_in_expr(overlay_what, cte_name)
                + count_cte_references_in_expr(overlay_from, cte_name)
                + overlay_for
                    .as_ref()
                    .map_or(0, |o| count_cte_references_in_expr(o, cte_name))
        }
        Expr::Array { elements, .. } => elements
            .iter()
            .map(|e| count_cte_references_in_expr(e, cte_name))
            .sum(),
        Expr::ArrayAccess { array, index } => {
            count_cte_references_in_expr(array, cte_name)
                + count_cte_references_in_expr(index, cte_name)
        }
        Expr::Struct { fields } => fields
            .iter()
            .map(|(_, e)| count_cte_references_in_expr(e, cte_name))
            .sum(),
        Expr::Lambda { body, .. } => count_cte_references_in_expr(body, cte_name),
        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => {
            count_cte_references_in_expr(timestamp, cte_name)
                + count_cte_references_in_expr(time_zone, cte_name)
        }
        Expr::JsonAccess { expr, .. } => count_cte_references_in_expr(expr, cte_name),
        Expr::UserDefinedAggregate { args, filter, .. } => {
            let args_count: usize = args
                .iter()
                .map(|a| count_cte_references_in_expr(a, cte_name))
                .sum();
            let filter_count = filter
                .as_ref()
                .map_or(0, |f| count_cte_references_in_expr(f, cte_name));
            args_count + filter_count
        }
        Expr::Literal(_)
        | Expr::Column { .. }
        | Expr::TypedString { .. }
        | Expr::Wildcard { .. }
        | Expr::Parameter { .. }
        | Expr::Variable { .. }
        | Expr::Placeholder { .. }
        | Expr::Default => 0,
    }
}

fn count_cte_references_in_opt_expr(expr: &Option<Expr>, cte_name: &str) -> usize {
    expr.as_ref()
        .map_or(0, |e| count_cte_references_in_expr(e, cte_name))
}

pub(crate) fn count_cte_references(plan: &LogicalPlan, cte_name: &str) -> usize {
    match plan {
        LogicalPlan::Scan {
            table_name: name, ..
        } => {
            if name.eq_ignore_ascii_case(cte_name) {
                1
            } else {
                0
            }
        }
        LogicalPlan::Filter { input, predicate } => {
            count_cte_references(input, cte_name)
                + count_cte_references_in_expr(predicate, cte_name)
        }
        LogicalPlan::Project {
            input, expressions, ..
        } => {
            count_cte_references(input, cte_name)
                + expressions
                    .iter()
                    .map(|e| count_cte_references_in_expr(e, cte_name))
                    .sum::<usize>()
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } => {
            count_cte_references(input, cte_name)
                + group_by
                    .iter()
                    .map(|e| count_cte_references_in_expr(e, cte_name))
                    .sum::<usize>()
                + aggregates
                    .iter()
                    .map(|e| count_cte_references_in_expr(e, cte_name))
                    .sum::<usize>()
        }
        LogicalPlan::Join {
            left,
            right,
            condition,
            ..
        } => {
            count_cte_references(left, cte_name)
                + count_cte_references(right, cte_name)
                + count_cte_references_in_opt_expr(condition, cte_name)
        }
        LogicalPlan::Sort { input, sort_exprs } => {
            count_cte_references(input, cte_name)
                + sort_exprs
                    .iter()
                    .map(|s| count_cte_references_in_expr(&s.expr, cte_name))
                    .sum::<usize>()
        }
        LogicalPlan::Limit { input, .. } => count_cte_references(input, cte_name),
        LogicalPlan::Distinct { input, .. } => count_cte_references(input, cte_name),
        LogicalPlan::SetOperation { left, right, .. } => {
            count_cte_references(left, cte_name) + count_cte_references(right, cte_name)
        }
        LogicalPlan::Window {
            input,
            window_exprs,
            ..
        } => {
            count_cte_references(input, cte_name)
                + window_exprs
                    .iter()
                    .map(|e| count_cte_references_in_expr(e, cte_name))
                    .sum::<usize>()
        }
        LogicalPlan::WithCte { body, ctes, .. } => {
            let mut count = count_cte_references(body, cte_name);
            for cte in ctes {
                count += count_cte_references(&cte.query, cte_name);
            }
            count
        }
        LogicalPlan::Unnest { input, .. } => count_cte_references(input, cte_name),
        LogicalPlan::Qualify { input, predicate } => {
            count_cte_references(input, cte_name)
                + count_cte_references_in_expr(predicate, cte_name)
        }
        LogicalPlan::Sample { input, .. } => count_cte_references(input, cte_name),
        LogicalPlan::GapFill { input, .. } => count_cte_references(input, cte_name),
        LogicalPlan::Insert { source, .. } => count_cte_references(source, cte_name),
        LogicalPlan::CreateTable { query, .. } => query
            .as_ref()
            .map_or(0, |q| count_cte_references(q, cte_name)),
        LogicalPlan::CreateView { query, .. } => count_cte_references(query, cte_name),
        LogicalPlan::Merge { source, on, .. } => {
            count_cte_references(source, cte_name) + count_cte_references_in_expr(on, cte_name)
        }
        LogicalPlan::Update { from, filter, .. } => {
            from.as_ref()
                .map_or(0, |f| count_cte_references(f, cte_name))
                + count_cte_references_in_opt_expr(filter, cte_name)
        }
        LogicalPlan::ExportData { query, .. } => count_cte_references(query, cte_name),
        LogicalPlan::For { query, body, .. } => {
            count_cte_references(query, cte_name)
                + body
                    .iter()
                    .map(|p| count_cte_references(p, cte_name))
                    .sum::<usize>()
        }
        LogicalPlan::If {
            then_branch,
            else_branch,
            ..
        } => {
            let then_count: usize = then_branch
                .iter()
                .map(|p| count_cte_references(p, cte_name))
                .sum();
            let else_count: usize = else_branch.as_ref().map_or(0, |b| {
                b.iter().map(|p| count_cte_references(p, cte_name)).sum()
            });
            then_count + else_count
        }
        LogicalPlan::While { body, .. } => {
            body.iter().map(|p| count_cte_references(p, cte_name)).sum()
        }
        LogicalPlan::Loop { body, .. } => {
            body.iter().map(|p| count_cte_references(p, cte_name)).sum()
        }
        LogicalPlan::Block { body, .. } => {
            body.iter().map(|p| count_cte_references(p, cte_name)).sum()
        }
        LogicalPlan::Repeat { body, .. } => {
            body.iter().map(|p| count_cte_references(p, cte_name)).sum()
        }
        LogicalPlan::CreateProcedure { body, .. } => {
            body.iter().map(|p| count_cte_references(p, cte_name)).sum()
        }
        LogicalPlan::TryCatch {
            try_block,
            catch_block,
            ..
        } => {
            let try_count: usize = try_block
                .iter()
                .map(|(p, _)| count_cte_references(p, cte_name))
                .sum();
            let catch_count: usize = catch_block
                .iter()
                .map(|p| count_cte_references(p, cte_name))
                .sum();
            try_count + catch_count
        }
        LogicalPlan::Explain { input, .. } => count_cte_references(input, cte_name),
        LogicalPlan::Delete { filter, .. } => count_cte_references_in_opt_expr(filter, cte_name),
        LogicalPlan::Assert { condition, message } => {
            count_cte_references_in_expr(condition, cte_name)
                + message
                    .as_ref()
                    .map_or(0, |m| count_cte_references_in_expr(m, cte_name))
        }
        LogicalPlan::Values { .. }
        | LogicalPlan::Empty { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::Truncate { .. }
        | LogicalPlan::DropView { .. }
        | LogicalPlan::CreateSchema { .. }
        | LogicalPlan::DropSchema { .. }
        | LogicalPlan::UndropSchema { .. }
        | LogicalPlan::AlterSchema { .. }
        | LogicalPlan::CreateFunction { .. }
        | LogicalPlan::DropFunction { .. }
        | LogicalPlan::DropProcedure { .. }
        | LogicalPlan::Call { .. }
        | LogicalPlan::LoadData { .. }
        | LogicalPlan::Declare { .. }
        | LogicalPlan::SetVariable { .. }
        | LogicalPlan::SetMultipleVariables { .. }
        | LogicalPlan::Return { .. }
        | LogicalPlan::Raise { .. }
        | LogicalPlan::ExecuteImmediate { .. }
        | LogicalPlan::Break { .. }
        | LogicalPlan::Continue { .. }
        | LogicalPlan::CreateSnapshot { .. }
        | LogicalPlan::DropSnapshot { .. }
        | LogicalPlan::Grant { .. }
        | LogicalPlan::Revoke { .. }
        | LogicalPlan::BeginTransaction
        | LogicalPlan::Commit
        | LogicalPlan::Rollback => 0,
    }
}

pub(crate) fn is_recursive_cte(cte_query: &LogicalPlan, cte_name: &str) -> bool {
    count_cte_references(cte_query, cte_name) > 0
}

fn count_cte_scan_references(plan: &LogicalPlan, cte_name: &str) -> usize {
    match plan {
        LogicalPlan::Scan { table_name, .. } => {
            if table_name.eq_ignore_ascii_case(cte_name) {
                1
            } else {
                0
            }
        }
        LogicalPlan::Filter { input, .. } => count_cte_scan_references(input, cte_name),
        LogicalPlan::Project { input, .. } => count_cte_scan_references(input, cte_name),
        LogicalPlan::Aggregate { input, .. } => count_cte_scan_references(input, cte_name),
        LogicalPlan::Join { left, right, .. } => {
            count_cte_scan_references(left, cte_name) + count_cte_scan_references(right, cte_name)
        }
        LogicalPlan::Sort { input, .. } => count_cte_scan_references(input, cte_name),
        LogicalPlan::Limit { input, .. } => count_cte_scan_references(input, cte_name),
        LogicalPlan::Distinct { input, .. } => count_cte_scan_references(input, cte_name),
        LogicalPlan::SetOperation { left, right, .. } => {
            count_cte_scan_references(left, cte_name) + count_cte_scan_references(right, cte_name)
        }
        LogicalPlan::Window { input, .. } => count_cte_scan_references(input, cte_name),
        LogicalPlan::WithCte { body, ctes, .. } => {
            let mut count = count_cte_scan_references(body, cte_name);
            for cte in ctes {
                count += count_cte_scan_references(&cte.query, cte_name);
            }
            count
        }
        LogicalPlan::Unnest { input, .. } => count_cte_scan_references(input, cte_name),
        LogicalPlan::Qualify { input, .. } => count_cte_scan_references(input, cte_name),
        LogicalPlan::Sample { input, .. } => count_cte_scan_references(input, cte_name),
        LogicalPlan::GapFill { input, .. } => count_cte_scan_references(input, cte_name),
        LogicalPlan::Insert { source, .. } => count_cte_scan_references(source, cte_name),
        LogicalPlan::CreateTable { query, .. } => query
            .as_ref()
            .map_or(0, |q| count_cte_scan_references(q, cte_name)),
        LogicalPlan::CreateView { query, .. } => count_cte_scan_references(query, cte_name),
        LogicalPlan::Merge { source, .. } => count_cte_scan_references(source, cte_name),
        LogicalPlan::Update { from, .. } => from
            .as_ref()
            .map_or(0, |f| count_cte_scan_references(f, cte_name)),
        LogicalPlan::ExportData { query, .. } => count_cte_scan_references(query, cte_name),
        LogicalPlan::For { query, body, .. } => {
            count_cte_scan_references(query, cte_name)
                + body
                    .iter()
                    .map(|p| count_cte_scan_references(p, cte_name))
                    .sum::<usize>()
        }
        LogicalPlan::If {
            then_branch,
            else_branch,
            ..
        } => {
            let then_count: usize = then_branch
                .iter()
                .map(|p| count_cte_scan_references(p, cte_name))
                .sum();
            let else_count: usize = else_branch.as_ref().map_or(0, |b| {
                b.iter()
                    .map(|p| count_cte_scan_references(p, cte_name))
                    .sum()
            });
            then_count + else_count
        }
        LogicalPlan::While { body, .. }
        | LogicalPlan::Loop { body, .. }
        | LogicalPlan::Block { body, .. }
        | LogicalPlan::Repeat { body, .. } => body
            .iter()
            .map(|p| count_cte_scan_references(p, cte_name))
            .sum(),
        LogicalPlan::CreateProcedure { body, .. } => body
            .iter()
            .map(|p| count_cte_scan_references(p, cte_name))
            .sum(),
        LogicalPlan::TryCatch {
            try_block,
            catch_block,
            ..
        } => {
            let try_count: usize = try_block
                .iter()
                .map(|(p, _)| count_cte_scan_references(p, cte_name))
                .sum();
            let catch_count: usize = catch_block
                .iter()
                .map(|p| count_cte_scan_references(p, cte_name))
                .sum();
            try_count + catch_count
        }
        LogicalPlan::Explain { input, .. } => count_cte_scan_references(input, cte_name),
        _ => 0,
    }
}

fn has_subqueries_in_expressions(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Project {
            input, expressions, ..
        } => {
            expressions.iter().any(expression_has_subquery) || has_subqueries_in_expressions(input)
        }
        LogicalPlan::Filter { input, predicate } => {
            expression_has_subquery(predicate) || has_subqueries_in_expressions(input)
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } => {
            group_by.iter().any(expression_has_subquery)
                || aggregates.iter().any(expression_has_subquery)
                || has_subqueries_in_expressions(input)
        }
        LogicalPlan::Join {
            left,
            right,
            condition,
            ..
        } => {
            condition.as_ref().is_some_and(expression_has_subquery)
                || has_subqueries_in_expressions(left)
                || has_subqueries_in_expressions(right)
        }
        LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Unnest { input, .. }
        | LogicalPlan::Qualify { input, .. }
        | LogicalPlan::Sample { input, .. }
        | LogicalPlan::GapFill { input, .. } => has_subqueries_in_expressions(input),
        LogicalPlan::SetOperation { left, right, .. } => {
            has_subqueries_in_expressions(left) || has_subqueries_in_expressions(right)
        }
        LogicalPlan::WithCte { body, ctes, .. } => {
            has_subqueries_in_expressions(body)
                || ctes.iter().any(|c| has_subqueries_in_expressions(&c.query))
        }
        _ => false,
    }
}

fn expression_has_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::Subquery(_) | Expr::ScalarSubquery(_) | Expr::ArraySubquery(_) => true,
        Expr::InSubquery { .. } | Expr::Exists { .. } => true,
        Expr::BinaryOp { left, right, .. } => {
            expression_has_subquery(left) || expression_has_subquery(right)
        }
        Expr::UnaryOp { expr, .. } => expression_has_subquery(expr),
        Expr::ScalarFunction { args, .. } => args.iter().any(expression_has_subquery),
        Expr::Aggregate { args, filter, .. } => {
            args.iter().any(expression_has_subquery)
                || filter.as_deref().is_some_and(expression_has_subquery)
        }
        Expr::Window {
            args,
            partition_by,
            order_by,
            ..
        }
        | Expr::AggregateWindow {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter().any(expression_has_subquery)
                || partition_by.iter().any(expression_has_subquery)
                || order_by.iter().any(|s| expression_has_subquery(&s.expr))
        }
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            operand.as_deref().is_some_and(expression_has_subquery)
                || when_clauses.iter().any(|w| {
                    expression_has_subquery(&w.condition) || expression_has_subquery(&w.result)
                })
                || else_result.as_deref().is_some_and(expression_has_subquery)
        }
        Expr::Cast { expr, .. } | Expr::IsNull { expr, .. } | Expr::Alias { expr, .. } => {
            expression_has_subquery(expr)
        }
        Expr::IsDistinctFrom { left, right, .. } => {
            expression_has_subquery(left) || expression_has_subquery(right)
        }
        Expr::InList { expr, list, .. } => {
            expression_has_subquery(expr) || list.iter().any(expression_has_subquery)
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => expression_has_subquery(expr) || expression_has_subquery(array_expr),
        Expr::Between {
            expr, low, high, ..
        } => {
            expression_has_subquery(expr)
                || expression_has_subquery(low)
                || expression_has_subquery(high)
        }
        Expr::Like { expr, pattern, .. } => {
            expression_has_subquery(expr) || expression_has_subquery(pattern)
        }
        Expr::Extract { expr, .. }
        | Expr::StructAccess { expr, .. }
        | Expr::Interval { value: expr, .. } => expression_has_subquery(expr),
        Expr::Substring {
            expr,
            start,
            length,
        } => {
            expression_has_subquery(expr)
                || start.as_deref().is_some_and(expression_has_subquery)
                || length.as_deref().is_some_and(expression_has_subquery)
        }
        Expr::Trim {
            expr, trim_what, ..
        } => {
            expression_has_subquery(expr)
                || trim_what.as_deref().is_some_and(expression_has_subquery)
        }
        Expr::Position { substr, string } => {
            expression_has_subquery(substr) || expression_has_subquery(string)
        }
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            expression_has_subquery(expr)
                || expression_has_subquery(overlay_what)
                || expression_has_subquery(overlay_from)
                || overlay_for.as_deref().is_some_and(expression_has_subquery)
        }
        Expr::Array { elements, .. } => elements.iter().any(expression_has_subquery),
        Expr::ArrayAccess { array, index } => {
            expression_has_subquery(array) || expression_has_subquery(index)
        }
        Expr::Struct { fields } => fields.iter().any(|(_, e)| expression_has_subquery(e)),
        Expr::Lambda { body, .. } => expression_has_subquery(body),
        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => expression_has_subquery(timestamp) || expression_has_subquery(time_zone),
        Expr::JsonAccess { expr, .. } => expression_has_subquery(expr),
        Expr::UserDefinedAggregate { args, filter, .. } => {
            args.iter().any(expression_has_subquery)
                || filter.as_deref().is_some_and(expression_has_subquery)
        }
        _ => false,
    }
}

pub(crate) fn should_inline_cte(
    cte: &CteDefinition,
    total_usage_count: usize,
    scan_usage_count: usize,
) -> bool {
    if cte.recursive {
        return false;
    }
    if cte.materialized == Some(true) {
        return false;
    }
    if is_recursive_cte(&cte.query, &cte.name) {
        return false;
    }
    if total_usage_count != scan_usage_count {
        return false;
    }
    if has_subqueries_in_expressions(&cte.query) {
        return false;
    }
    total_usage_count <= 1
}

pub(crate) fn inline_cte(
    body: LogicalPlan,
    cte_name: &str,
    cte_query: &LogicalPlan,
) -> LogicalPlan {
    match body {
        LogicalPlan::Scan {
            table_name,
            schema,
            projection,
        } => {
            if table_name.eq_ignore_ascii_case(cte_name) {
                apply_projection_to_plan(cte_query.clone(), &projection)
            } else {
                LogicalPlan::Scan {
                    table_name,
                    schema,
                    projection,
                }
            }
        }
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(inline_cte(*input, cte_name, cte_query)),
            predicate,
        },
        LogicalPlan::Project {
            input,
            expressions,
            schema,
        } => LogicalPlan::Project {
            input: Box::new(inline_cte(*input, cte_name, cte_query)),
            expressions,
            schema,
        },
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
            grouping_sets,
        } => LogicalPlan::Aggregate {
            input: Box::new(inline_cte(*input, cte_name, cte_query)),
            group_by,
            aggregates,
            schema,
            grouping_sets,
        },
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            schema,
        } => LogicalPlan::Join {
            left: Box::new(inline_cte(*left, cte_name, cte_query)),
            right: Box::new(inline_cte(*right, cte_name, cte_query)),
            join_type,
            condition,
            schema,
        },
        LogicalPlan::Sort { input, sort_exprs } => LogicalPlan::Sort {
            input: Box::new(inline_cte(*input, cte_name, cte_query)),
            sort_exprs,
        },
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => LogicalPlan::Limit {
            input: Box::new(inline_cte(*input, cte_name, cte_query)),
            limit,
            offset,
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(inline_cte(*input, cte_name, cte_query)),
        },
        LogicalPlan::SetOperation {
            left,
            right,
            op,
            all,
            schema,
        } => LogicalPlan::SetOperation {
            left: Box::new(inline_cte(*left, cte_name, cte_query)),
            right: Box::new(inline_cte(*right, cte_name, cte_query)),
            op,
            all,
            schema,
        },
        LogicalPlan::Window {
            input,
            window_exprs,
            schema,
        } => LogicalPlan::Window {
            input: Box::new(inline_cte(*input, cte_name, cte_query)),
            window_exprs,
            schema,
        },
        LogicalPlan::WithCte { ctes, body } => {
            let inlined_ctes: Vec<CteDefinition> = ctes
                .into_iter()
                .map(|mut cte_def| {
                    cte_def.query = Box::new(inline_cte(*cte_def.query, cte_name, cte_query));
                    cte_def
                })
                .collect();
            LogicalPlan::WithCte {
                ctes: inlined_ctes,
                body: Box::new(inline_cte(*body, cte_name, cte_query)),
            }
        }
        LogicalPlan::Unnest {
            input,
            columns,
            schema,
        } => LogicalPlan::Unnest {
            input: Box::new(inline_cte(*input, cte_name, cte_query)),
            columns,
            schema,
        },
        LogicalPlan::Qualify { input, predicate } => LogicalPlan::Qualify {
            input: Box::new(inline_cte(*input, cte_name, cte_query)),
            predicate,
        },
        LogicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => LogicalPlan::Sample {
            input: Box::new(inline_cte(*input, cte_name, cte_query)),
            sample_type,
            sample_value,
        },
        LogicalPlan::GapFill {
            input,
            ts_column,
            bucket_width,
            value_columns,
            partitioning_columns,
            origin,
            input_schema,
            schema,
        } => LogicalPlan::GapFill {
            input: Box::new(inline_cte(*input, cte_name, cte_query)),
            ts_column,
            bucket_width,
            value_columns,
            partitioning_columns,
            origin,
            input_schema,
            schema,
        },
        LogicalPlan::Insert {
            table_name,
            columns,
            source,
        } => LogicalPlan::Insert {
            table_name,
            columns,
            source: Box::new(inline_cte(*source, cte_name, cte_query)),
        },
        LogicalPlan::Merge {
            target_table,
            source,
            on,
            clauses,
        } => LogicalPlan::Merge {
            target_table,
            source: Box::new(inline_cte(*source, cte_name, cte_query)),
            on,
            clauses,
        },
        LogicalPlan::Update {
            table_name,
            alias,
            assignments,
            from,
            filter,
        } => LogicalPlan::Update {
            table_name,
            alias,
            assignments,
            from: from.map(|f| Box::new(inline_cte(*f, cte_name, cte_query))),
            filter,
        },
        LogicalPlan::CreateTable {
            table_name,
            columns,
            if_not_exists,
            or_replace,
            query,
        } => LogicalPlan::CreateTable {
            table_name,
            columns,
            if_not_exists,
            or_replace,
            query: query.map(|q| Box::new(inline_cte(*q, cte_name, cte_query))),
        },
        LogicalPlan::CreateView {
            name,
            query,
            query_sql,
            column_aliases,
            or_replace,
            if_not_exists,
        } => LogicalPlan::CreateView {
            name,
            query: Box::new(inline_cte(*query, cte_name, cte_query)),
            query_sql,
            column_aliases,
            or_replace,
            if_not_exists,
        },
        LogicalPlan::ExportData { options, query } => LogicalPlan::ExportData {
            options,
            query: Box::new(inline_cte(*query, cte_name, cte_query)),
        },
        LogicalPlan::For {
            variable,
            query,
            body,
        } => LogicalPlan::For {
            variable,
            query: Box::new(inline_cte(*query, cte_name, cte_query)),
            body: body
                .into_iter()
                .map(|p| inline_cte(p, cte_name, cte_query))
                .collect(),
        },
        LogicalPlan::If {
            condition,
            then_branch,
            else_branch,
        } => LogicalPlan::If {
            condition,
            then_branch: then_branch
                .into_iter()
                .map(|p| inline_cte(p, cte_name, cte_query))
                .collect(),
            else_branch: else_branch.map(|b| {
                b.into_iter()
                    .map(|p| inline_cte(p, cte_name, cte_query))
                    .collect()
            }),
        },
        LogicalPlan::While {
            condition,
            body,
            label,
        } => LogicalPlan::While {
            condition,
            body: body
                .into_iter()
                .map(|p| inline_cte(p, cte_name, cte_query))
                .collect(),
            label,
        },
        LogicalPlan::Loop { body, label } => LogicalPlan::Loop {
            body: body
                .into_iter()
                .map(|p| inline_cte(p, cte_name, cte_query))
                .collect(),
            label,
        },
        LogicalPlan::Block { body, label } => LogicalPlan::Block {
            body: body
                .into_iter()
                .map(|p| inline_cte(p, cte_name, cte_query))
                .collect(),
            label,
        },
        LogicalPlan::Repeat {
            body,
            until_condition,
        } => LogicalPlan::Repeat {
            body: body
                .into_iter()
                .map(|p| inline_cte(p, cte_name, cte_query))
                .collect(),
            until_condition,
        },
        LogicalPlan::CreateProcedure {
            name,
            args,
            body,
            or_replace,
            if_not_exists,
        } => LogicalPlan::CreateProcedure {
            name,
            args,
            body: body
                .into_iter()
                .map(|p| inline_cte(p, cte_name, cte_query))
                .collect(),
            or_replace,
            if_not_exists,
        },
        LogicalPlan::TryCatch {
            try_block,
            catch_block,
        } => LogicalPlan::TryCatch {
            try_block: try_block
                .into_iter()
                .map(|(p, sql)| (inline_cte(p, cte_name, cte_query), sql))
                .collect(),
            catch_block: catch_block
                .into_iter()
                .map(|p| inline_cte(p, cte_name, cte_query))
                .collect(),
        },
        LogicalPlan::Explain { input, analyze } => LogicalPlan::Explain {
            input: Box::new(inline_cte(*input, cte_name, cte_query)),
            analyze,
        },
        LogicalPlan::Values { values, schema } => LogicalPlan::Values { values, schema },
        LogicalPlan::Empty { schema } => LogicalPlan::Empty { schema },
        LogicalPlan::Delete {
            table_name,
            alias,
            filter,
        } => LogicalPlan::Delete {
            table_name,
            alias,
            filter,
        },
        LogicalPlan::DropTable {
            table_names,
            if_exists,
        } => LogicalPlan::DropTable {
            table_names,
            if_exists,
        },
        LogicalPlan::AlterTable {
            table_name,
            operation,
            if_exists,
        } => LogicalPlan::AlterTable {
            table_name,
            operation,
            if_exists,
        },
        LogicalPlan::Truncate { table_name } => LogicalPlan::Truncate { table_name },
        LogicalPlan::DropView { name, if_exists } => LogicalPlan::DropView { name, if_exists },
        LogicalPlan::CreateSchema {
            name,
            if_not_exists,
            or_replace,
        } => LogicalPlan::CreateSchema {
            name,
            if_not_exists,
            or_replace,
        },
        LogicalPlan::DropSchema {
            name,
            if_exists,
            cascade,
        } => LogicalPlan::DropSchema {
            name,
            if_exists,
            cascade,
        },
        LogicalPlan::UndropSchema {
            name,
            if_not_exists,
        } => LogicalPlan::UndropSchema {
            name,
            if_not_exists,
        },
        LogicalPlan::AlterSchema { name, options } => LogicalPlan::AlterSchema { name, options },
        LogicalPlan::CreateFunction {
            name,
            args,
            return_type,
            body,
            or_replace,
            if_not_exists,
            is_temp,
            is_aggregate,
        } => LogicalPlan::CreateFunction {
            name,
            args,
            return_type,
            body,
            or_replace,
            if_not_exists,
            is_temp,
            is_aggregate,
        },
        LogicalPlan::DropFunction { name, if_exists } => {
            LogicalPlan::DropFunction { name, if_exists }
        }
        LogicalPlan::DropProcedure { name, if_exists } => {
            LogicalPlan::DropProcedure { name, if_exists }
        }
        LogicalPlan::Call {
            procedure_name,
            args,
        } => LogicalPlan::Call {
            procedure_name,
            args,
        },
        LogicalPlan::LoadData {
            table_name,
            options,
            temp_table,
            temp_schema,
        } => LogicalPlan::LoadData {
            table_name,
            options,
            temp_table,
            temp_schema,
        },
        LogicalPlan::Declare {
            name,
            data_type,
            default,
        } => LogicalPlan::Declare {
            name,
            data_type,
            default,
        },
        LogicalPlan::SetVariable { name, value } => LogicalPlan::SetVariable { name, value },
        LogicalPlan::SetMultipleVariables { names, value } => {
            LogicalPlan::SetMultipleVariables { names, value }
        }
        LogicalPlan::Return { value } => LogicalPlan::Return { value },
        LogicalPlan::Raise { message, level } => LogicalPlan::Raise { message, level },
        LogicalPlan::ExecuteImmediate {
            sql_expr,
            into_variables,
            using_params,
        } => LogicalPlan::ExecuteImmediate {
            sql_expr,
            into_variables,
            using_params,
        },
        LogicalPlan::Break { label } => LogicalPlan::Break { label },
        LogicalPlan::Continue { label } => LogicalPlan::Continue { label },
        LogicalPlan::CreateSnapshot {
            snapshot_name,
            source_name,
            if_not_exists,
        } => LogicalPlan::CreateSnapshot {
            snapshot_name,
            source_name,
            if_not_exists,
        },
        LogicalPlan::DropSnapshot {
            snapshot_name,
            if_exists,
        } => LogicalPlan::DropSnapshot {
            snapshot_name,
            if_exists,
        },
        LogicalPlan::Assert { condition, message } => LogicalPlan::Assert { condition, message },
        LogicalPlan::Grant {
            roles,
            resource_type,
            resource_name,
            grantees,
        } => LogicalPlan::Grant {
            roles,
            resource_type,
            resource_name,
            grantees,
        },
        LogicalPlan::Revoke {
            roles,
            resource_type,
            resource_name,
            grantees,
        } => LogicalPlan::Revoke {
            roles,
            resource_type,
            resource_name,
            grantees,
        },
        LogicalPlan::BeginTransaction => LogicalPlan::BeginTransaction,
        LogicalPlan::Commit => LogicalPlan::Commit,
        LogicalPlan::Rollback => LogicalPlan::Rollback,
    }
}

fn apply_projection_to_plan(plan: LogicalPlan, projection: &Option<Vec<usize>>) -> LogicalPlan {
    match projection {
        Some(indices) if !indices.is_empty() => {
            let schema = plan.schema();
            let projected_schema = yachtsql_ir::PlanSchema::from_fields(
                indices
                    .iter()
                    .filter_map(|&i| schema.fields.get(i).cloned())
                    .collect(),
            );
            let expressions: Vec<yachtsql_ir::Expr> = indices
                .iter()
                .filter_map(|&i| {
                    schema.fields.get(i).map(|f| yachtsql_ir::Expr::Column {
                        table: None,
                        name: f.name.clone(),
                        index: Some(i),
                    })
                })
                .collect();
            LogicalPlan::Project {
                input: Box::new(plan),
                expressions,
                schema: projected_schema,
            }
        }
        _ => plan,
    }
}

pub(crate) fn optimize_ctes(
    ctes: Vec<CteDefinition>,
    body: LogicalPlan,
) -> (Vec<CteDefinition>, LogicalPlan) {
    let mut remaining_ctes: Vec<CteDefinition> = Vec::new();
    let mut current_body = body;
    let mut pending_ctes = ctes;

    while !pending_ctes.is_empty() {
        let cte = pending_ctes.remove(0);

        let mut total_usage_count = count_cte_references(&current_body, &cte.name);
        let mut scan_usage_count = count_cte_scan_references(&current_body, &cte.name);
        for other_cte in &pending_ctes {
            total_usage_count += count_cte_references(&other_cte.query, &cte.name);
            scan_usage_count += count_cte_scan_references(&other_cte.query, &cte.name);
        }
        for other_cte in &remaining_ctes {
            total_usage_count += count_cte_references(&other_cte.query, &cte.name);
            scan_usage_count += count_cte_scan_references(&other_cte.query, &cte.name);
        }

        let any_pending_has_subqueries = pending_ctes
            .iter()
            .any(|c| has_subqueries_in_expressions(&c.query));
        let should_inline = should_inline_cte(&cte, total_usage_count, scan_usage_count)
            && !any_pending_has_subqueries;
        if should_inline {
            current_body = inline_cte(current_body, &cte.name, &cte.query);
            for pending in &mut pending_ctes {
                pending.query = Box::new(inline_cte(
                    pending.query.as_ref().clone(),
                    &cte.name,
                    &cte.query,
                ));
            }
        } else {
            remaining_ctes.push(cte);
        }
    }

    (remaining_ctes, current_body)
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::DataType;
    use yachtsql_ir::{Expr, JoinType, PlanField, PlanSchema};

    use super::*;

    fn make_schema(fields: &[(&str, DataType)]) -> PlanSchema {
        let plan_fields: Vec<PlanField> = fields
            .iter()
            .map(|(name, dt)| PlanField::new(name.to_string(), dt.clone()))
            .collect();
        PlanSchema::from_fields(plan_fields)
    }

    fn make_scan(name: &str) -> LogicalPlan {
        LogicalPlan::Scan {
            table_name: name.to_string(),
            schema: make_schema(&[("id", DataType::Int64), ("value", DataType::String)]),
            projection: None,
        }
    }

    fn make_cte(name: &str, query: LogicalPlan, recursive: bool) -> CteDefinition {
        CteDefinition {
            name: name.to_string(),
            columns: None,
            query: Box::new(query),
            recursive,
            materialized: None,
        }
    }

    #[test]
    fn count_cte_references_zero_when_not_present() {
        let scan = make_scan("orders");
        assert_eq!(count_cte_references(&scan, "my_cte"), 0);
    }

    #[test]
    fn count_cte_references_finds_single_scan() {
        let scan = make_scan("my_cte");
        assert_eq!(count_cte_references(&scan, "my_cte"), 1);
    }

    #[test]
    fn count_cte_references_case_insensitive() {
        let scan = make_scan("MY_CTE");
        assert_eq!(count_cte_references(&scan, "my_cte"), 1);
    }

    #[test]
    fn count_cte_references_in_filter() {
        let scan = make_scan("my_cte");
        let filter = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: Expr::literal_bool(true),
        };
        assert_eq!(count_cte_references(&filter, "my_cte"), 1);
    }

    #[test]
    fn count_cte_references_in_project() {
        let scan = make_scan("my_cte");
        let project = LogicalPlan::Project {
            input: Box::new(scan),
            expressions: vec![],
            schema: make_schema(&[]),
        };
        assert_eq!(count_cte_references(&project, "my_cte"), 1);
    }

    #[test]
    fn count_cte_references_in_join_both_sides() {
        let left = make_scan("my_cte");
        let right = make_scan("my_cte");
        let join = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            condition: None,
            schema: make_schema(&[]),
        };
        assert_eq!(count_cte_references(&join, "my_cte"), 2);
    }

    #[test]
    fn count_cte_references_in_join_left_only() {
        let left = make_scan("my_cte");
        let right = make_scan("other_table");
        let join = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            condition: None,
            schema: make_schema(&[]),
        };
        assert_eq!(count_cte_references(&join, "my_cte"), 1);
    }

    #[test]
    fn count_cte_references_in_union() {
        let left = make_scan("my_cte");
        let right = make_scan("my_cte");
        let union = LogicalPlan::SetOperation {
            left: Box::new(left),
            right: Box::new(right),
            op: yachtsql_ir::SetOperationType::Union,
            all: true,
            schema: make_schema(&[]),
        };
        assert_eq!(count_cte_references(&union, "my_cte"), 2);
    }

    #[test]
    fn is_recursive_cte_false_for_non_self_referencing() {
        let scan = make_scan("other_table");
        assert!(!is_recursive_cte(&scan, "my_cte"));
    }

    #[test]
    fn is_recursive_cte_true_for_self_referencing() {
        let scan = make_scan("my_cte");
        assert!(is_recursive_cte(&scan, "my_cte"));
    }

    #[test]
    fn should_inline_cte_true_for_single_use_non_recursive() {
        let scan = make_scan("other_table");
        let cte = make_cte("my_cte", scan, false);
        assert!(should_inline_cte(&cte, 1, 1));
    }

    #[test]
    fn should_inline_cte_false_for_multi_use() {
        let scan = make_scan("other_table");
        let cte = make_cte("my_cte", scan, false);
        assert!(!should_inline_cte(&cte, 2, 2));
    }

    #[test]
    fn should_inline_cte_true_for_zero_use() {
        let scan = make_scan("other_table");
        let cte = make_cte("my_cte", scan, false);
        assert!(should_inline_cte(&cte, 0, 0));
    }

    #[test]
    fn should_inline_cte_false_for_recursive_flag() {
        let scan = make_scan("other_table");
        let cte = make_cte("my_cte", scan, true);
        assert!(!should_inline_cte(&cte, 1, 1));
    }

    #[test]
    fn should_inline_cte_false_for_self_referencing() {
        let scan = make_scan("my_cte");
        let cte = make_cte("my_cte", scan, false);
        assert!(!should_inline_cte(&cte, 1, 1));
    }

    #[test]
    fn should_inline_cte_false_for_materialized() {
        let scan = make_scan("other_table");
        let mut cte = make_cte("my_cte", scan, false);
        cte.materialized = Some(true);
        assert!(!should_inline_cte(&cte, 1, 1));
    }

    #[test]
    fn should_inline_cte_true_for_not_materialized() {
        let scan = make_scan("other_table");
        let mut cte = make_cte("my_cte", scan, false);
        cte.materialized = Some(false);
        assert!(should_inline_cte(&cte, 1, 1));
    }

    #[test]
    fn should_inline_cte_false_for_subquery_reference() {
        let scan = make_scan("other_table");
        let cte = make_cte("my_cte", scan, false);
        assert!(!should_inline_cte(&cte, 1, 0));
    }

    #[test]
    fn inline_cte_replaces_scan() {
        let body = make_scan("my_cte");
        let cte_query = make_scan("real_table");

        let result = inline_cte(body, "my_cte", &cte_query);

        match result {
            LogicalPlan::Scan { table_name, .. } => {
                assert_eq!(table_name, "real_table");
            }
            other => panic!("Expected Scan, got {:?}", other),
        }
    }

    #[test]
    fn inline_cte_preserves_other_scans() {
        let body = make_scan("other_table");
        let cte_query = make_scan("real_table");

        let result = inline_cte(body, "my_cte", &cte_query);

        match result {
            LogicalPlan::Scan { table_name, .. } => {
                assert_eq!(table_name, "other_table");
            }
            other => panic!("Expected Scan, got {:?}", other),
        }
    }

    #[test]
    fn inline_cte_replaces_in_filter() {
        let scan = make_scan("my_cte");
        let body = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: Expr::literal_bool(true),
        };
        let cte_query = make_scan("real_table");

        let result = inline_cte(body, "my_cte", &cte_query);

        match result {
            LogicalPlan::Filter { input, .. } => match *input {
                LogicalPlan::Scan { table_name, .. } => {
                    assert_eq!(table_name, "real_table");
                }
                other => panic!("Expected Scan inside Filter, got {:?}", other),
            },
            other => panic!("Expected Filter, got {:?}", other),
        }
    }

    #[test]
    fn inline_cte_replaces_in_join_left() {
        let left = make_scan("my_cte");
        let right = make_scan("other");
        let body = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            condition: None,
            schema: make_schema(&[]),
        };
        let cte_query = make_scan("real_table");

        let result = inline_cte(body, "my_cte", &cte_query);

        match result {
            LogicalPlan::Join { left, right, .. } => {
                match *left {
                    LogicalPlan::Scan { table_name, .. } => {
                        assert_eq!(table_name, "real_table");
                    }
                    other => panic!("Expected Scan on left, got {:?}", other),
                }
                match *right {
                    LogicalPlan::Scan { table_name, .. } => {
                        assert_eq!(table_name, "other");
                    }
                    other => panic!("Expected Scan on right, got {:?}", other),
                }
            }
            other => panic!("Expected Join, got {:?}", other),
        }
    }

    #[test]
    fn inline_cte_case_insensitive() {
        let body = make_scan("MY_CTE");
        let cte_query = make_scan("real_table");

        let result = inline_cte(body, "my_cte", &cte_query);

        match result {
            LogicalPlan::Scan { table_name, .. } => {
                assert_eq!(table_name, "real_table");
            }
            other => panic!("Expected Scan, got {:?}", other),
        }
    }

    #[test]
    fn optimize_ctes_inlines_single_use() {
        let cte_query = make_scan("real_table");
        let body = make_scan("my_cte");
        let ctes = vec![make_cte("my_cte", cte_query, false)];

        let (remaining, result_body) = optimize_ctes(ctes, body);

        assert!(remaining.is_empty());
        match result_body {
            LogicalPlan::Scan { table_name, .. } => {
                assert_eq!(table_name, "real_table");
            }
            other => panic!("Expected Scan, got {:?}", other),
        }
    }

    #[test]
    fn optimize_ctes_keeps_multi_use() {
        let left = make_scan("my_cte");
        let right = make_scan("my_cte");
        let body = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            condition: None,
            schema: make_schema(&[]),
        };
        let cte_query = make_scan("real_table");
        let ctes = vec![make_cte("my_cte", cte_query, false)];

        let (remaining, _) = optimize_ctes(ctes, body);

        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].name, "my_cte");
    }

    #[test]
    fn optimize_ctes_keeps_recursive() {
        let body = make_scan("my_cte");
        let cte_query = make_scan("real_table");
        let ctes = vec![make_cte("my_cte", cte_query, true)];

        let (remaining, _) = optimize_ctes(ctes, body);

        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn optimize_ctes_removes_unused() {
        let body = make_scan("other_table");
        let cte_query = make_scan("real_table");
        let ctes = vec![make_cte("unused_cte", cte_query, false)];

        let (remaining, _) = optimize_ctes(ctes, body);

        assert!(remaining.is_empty());
    }

    #[test]
    fn optimize_ctes_handles_multiple_ctes() {
        let cte1_query = make_scan("real_table1");
        let cte2_query = make_scan("real_table2");

        let left = make_scan("cte1");
        let right = make_scan("cte2");
        let body = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(LogicalPlan::Join {
                left: Box::new(right.clone()),
                right: Box::new(make_scan("cte2")),
                join_type: JoinType::Inner,
                condition: None,
                schema: make_schema(&[]),
            }),
            join_type: JoinType::Inner,
            condition: None,
            schema: make_schema(&[]),
        };

        let ctes = vec![
            make_cte("cte1", cte1_query, false),
            make_cte("cte2", cte2_query, false),
        ];

        let (remaining, result_body) = optimize_ctes(ctes, body);

        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].name, "cte2");

        match result_body {
            LogicalPlan::Join { left, .. } => match *left {
                LogicalPlan::Scan { table_name, .. } => {
                    assert_eq!(table_name, "real_table1");
                }
                other => panic!("Expected Scan, got {:?}", other),
            },
            other => panic!("Expected Join, got {:?}", other),
        }
    }
}
