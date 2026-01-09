#![coverage(off)]

use rustc_hash::FxHashSet;
use yachtsql_ir::{BinaryOp, Expr, JoinType, ScalarFunction};

use crate::PhysicalPlan;
use crate::planner::predicate::{collect_column_indices, split_and_predicates};

fn get_nullable_column_indices(
    schema_len: usize,
    join_type: JoinType,
    left_len: usize,
) -> FxHashSet<usize> {
    let mut nullable = FxHashSet::default();
    match join_type {
        JoinType::Left => {
            for i in left_len..schema_len {
                nullable.insert(i);
            }
        }
        JoinType::Right => {
            for i in 0..left_len {
                nullable.insert(i);
            }
        }
        JoinType::Full => {
            for i in 0..schema_len {
                nullable.insert(i);
            }
        }
        JoinType::Inner | JoinType::Cross => {}
    }
    nullable
}

fn has_exposed_nullable_column(expr: &Expr, nullable_columns: &FxHashSet<usize>) -> bool {
    match expr {
        Expr::Column {
            index: Some(idx), ..
        } => nullable_columns.contains(idx),

        Expr::Column { index: None, .. } => false,

        Expr::ScalarFunction {
            name: ScalarFunction::Coalesce,
            ..
        } => false,

        Expr::ScalarFunction { args, .. } => args
            .iter()
            .any(|arg| has_exposed_nullable_column(arg, nullable_columns)),

        Expr::BinaryOp { left, right, .. } => {
            has_exposed_nullable_column(left, nullable_columns)
                || has_exposed_nullable_column(right, nullable_columns)
        }

        Expr::UnaryOp { expr, .. } => has_exposed_nullable_column(expr, nullable_columns),

        Expr::Cast { expr, .. } => has_exposed_nullable_column(expr, nullable_columns),

        Expr::IsNull { expr, .. } => has_exposed_nullable_column(expr, nullable_columns),

        Expr::InList { expr, list, .. } => {
            has_exposed_nullable_column(expr, nullable_columns)
                || list
                    .iter()
                    .any(|e| has_exposed_nullable_column(e, nullable_columns))
        }

        Expr::Between {
            expr, low, high, ..
        } => {
            has_exposed_nullable_column(expr, nullable_columns)
                || has_exposed_nullable_column(low, nullable_columns)
                || has_exposed_nullable_column(high, nullable_columns)
        }

        Expr::Like { expr, pattern, .. } => {
            has_exposed_nullable_column(expr, nullable_columns)
                || has_exposed_nullable_column(pattern, nullable_columns)
        }

        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            if operand
                .as_ref()
                .is_some_and(|op| has_exposed_nullable_column(op, nullable_columns))
            {
                return true;
            }
            for wc in when_clauses {
                if has_exposed_nullable_column(&wc.condition, nullable_columns)
                    || has_exposed_nullable_column(&wc.result, nullable_columns)
                {
                    return true;
                }
            }
            else_result
                .as_ref()
                .is_some_and(|e| has_exposed_nullable_column(e, nullable_columns))
        }

        Expr::Alias { expr, .. } => has_exposed_nullable_column(expr, nullable_columns),

        Expr::Literal(_) => false,

        _ => {
            let col_indices = collect_column_indices(expr);
            col_indices.iter().any(|idx| nullable_columns.contains(idx))
        }
    }
}

fn is_null_rejecting(predicate: &Expr, nullable_columns: &FxHashSet<usize>) -> bool {
    match predicate {
        Expr::IsNull {
            expr,
            negated: true,
        } => has_exposed_nullable_column(expr, nullable_columns),

        Expr::IsNull { negated: false, .. } => false,

        Expr::BinaryOp {
            op: BinaryOp::And,
            left,
            right,
        } => {
            is_null_rejecting(left, nullable_columns) || is_null_rejecting(right, nullable_columns)
        }

        Expr::BinaryOp {
            op: BinaryOp::Or,
            left,
            right,
        } => {
            is_null_rejecting(left, nullable_columns) && is_null_rejecting(right, nullable_columns)
        }

        Expr::BinaryOp {
            op:
                BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq,
            left,
            right,
        } => {
            has_exposed_nullable_column(left, nullable_columns)
                || has_exposed_nullable_column(right, nullable_columns)
        }

        Expr::BinaryOp {
            op:
                BinaryOp::Add
                | BinaryOp::Sub
                | BinaryOp::Mul
                | BinaryOp::Div
                | BinaryOp::Mod
                | BinaryOp::Concat
                | BinaryOp::BitwiseAnd
                | BinaryOp::BitwiseOr
                | BinaryOp::BitwiseXor
                | BinaryOp::ShiftLeft
                | BinaryOp::ShiftRight,
            ..
        } => false,

        Expr::InList { expr, .. } => has_exposed_nullable_column(expr, nullable_columns),

        Expr::Between { expr, .. } => has_exposed_nullable_column(expr, nullable_columns),

        Expr::Like { expr, .. } => has_exposed_nullable_column(expr, nullable_columns),

        Expr::ScalarFunction {
            name: ScalarFunction::Coalesce,
            ..
        } => false,

        Expr::ScalarFunction { args, .. } => args
            .iter()
            .any(|arg| has_exposed_nullable_column(arg, nullable_columns)),

        Expr::Cast { expr, .. } => is_null_rejecting(expr, nullable_columns),

        Expr::UnaryOp {
            op: yachtsql_ir::UnaryOp::Not,
            expr,
        } => is_null_rejecting(expr, nullable_columns),

        Expr::UnaryOp { expr, .. } => has_exposed_nullable_column(expr, nullable_columns),

        _ => false,
    }
}

fn get_left_nullable_columns(_schema_len: usize, left_len: usize) -> FxHashSet<usize> {
    let mut nullable = FxHashSet::default();
    for i in 0..left_len {
        nullable.insert(i);
    }
    nullable
}

fn get_right_nullable_columns(schema_len: usize, left_len: usize) -> FxHashSet<usize> {
    let mut nullable = FxHashSet::default();
    for i in left_len..schema_len {
        nullable.insert(i);
    }
    nullable
}

fn try_convert_join(
    join_type: JoinType,
    predicate: &Expr,
    schema_len: usize,
    left_len: usize,
) -> Option<JoinType> {
    let nullable_columns = get_nullable_column_indices(schema_len, join_type, left_len);

    match join_type {
        JoinType::Left => {
            let predicates = split_and_predicates(predicate);
            for pred in &predicates {
                if is_null_rejecting(pred, &nullable_columns) {
                    return Some(JoinType::Inner);
                }
            }
            None
        }
        JoinType::Right => {
            let predicates = split_and_predicates(predicate);
            for pred in &predicates {
                if is_null_rejecting(pred, &nullable_columns) {
                    return Some(JoinType::Inner);
                }
            }
            None
        }
        JoinType::Full => {
            let predicates = split_and_predicates(predicate);
            let left_nullable = get_left_nullable_columns(schema_len, left_len);
            let right_nullable = get_right_nullable_columns(schema_len, left_len);

            let mut rejects_left_nulls = false;
            let mut rejects_right_nulls = false;

            for pred in &predicates {
                if is_null_rejecting(pred, &left_nullable) {
                    rejects_left_nulls = true;
                }
                if is_null_rejecting(pred, &right_nullable) {
                    rejects_right_nulls = true;
                }
            }

            match (rejects_left_nulls, rejects_right_nulls) {
                (true, true) => Some(JoinType::Inner),
                (true, false) => Some(JoinType::Right),
                (false, true) => Some(JoinType::Left),
                (false, false) => None,
            }
        }
        JoinType::Inner | JoinType::Cross => None,
    }
}

pub fn apply_outer_to_inner_join(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Filter { input, predicate } => {
            let optimized_input = apply_outer_to_inner_join(*input);

            match optimized_input {
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
                    let schema_len = schema.fields.len();
                    let left_len = left.schema().fields.len();

                    match try_convert_join(join_type, &predicate, schema_len, left_len) {
                        Some(new_join_type) => PhysicalPlan::Filter {
                            input: Box::new(PhysicalPlan::HashJoin {
                                left,
                                right,
                                join_type: new_join_type,
                                left_keys,
                                right_keys,
                                schema,
                                parallel,
                                hints,
                            }),
                            predicate,
                        },
                        None => PhysicalPlan::Filter {
                            input: Box::new(PhysicalPlan::HashJoin {
                                left,
                                right,
                                join_type,
                                left_keys,
                                right_keys,
                                schema,
                                parallel,
                                hints,
                            }),
                            predicate,
                        },
                    }
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
                    let schema_len = schema.fields.len();
                    let left_len = left.schema().fields.len();

                    match try_convert_join(join_type, &predicate, schema_len, left_len) {
                        Some(new_join_type) => PhysicalPlan::Filter {
                            input: Box::new(PhysicalPlan::NestedLoopJoin {
                                left,
                                right,
                                join_type: new_join_type,
                                condition,
                                schema,
                                parallel,
                                hints,
                            }),
                            predicate,
                        },
                        None => PhysicalPlan::Filter {
                            input: Box::new(PhysicalPlan::NestedLoopJoin {
                                left,
                                right,
                                join_type,
                                condition,
                                schema,
                                parallel,
                                hints,
                            }),
                            predicate,
                        },
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
            input: Box::new(apply_outer_to_inner_join(*input)),
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
            input: Box::new(apply_outer_to_inner_join(*input)),
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
            left: Box::new(apply_outer_to_inner_join(*left)),
            right: Box::new(apply_outer_to_inner_join(*right)),
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
            left: Box::new(apply_outer_to_inner_join(*left)),
            right: Box::new(apply_outer_to_inner_join(*right)),
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
            left: Box::new(apply_outer_to_inner_join(*left)),
            right: Box::new(apply_outer_to_inner_join(*right)),
            schema,
            parallel,
            hints,
        },

        PhysicalPlan::Sort {
            input,
            sort_exprs,
            hints,
        } => PhysicalPlan::Sort {
            input: Box::new(apply_outer_to_inner_join(*input)),
            sort_exprs,
            hints,
        },

        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => PhysicalPlan::TopN {
            input: Box::new(apply_outer_to_inner_join(*input)),
            sort_exprs,
            limit,
        },

        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => PhysicalPlan::Limit {
            input: Box::new(apply_outer_to_inner_join(*input)),
            limit,
            offset,
        },

        PhysicalPlan::Distinct { input } => PhysicalPlan::Distinct {
            input: Box::new(apply_outer_to_inner_join(*input)),
        },

        PhysicalPlan::Union {
            inputs,
            all,
            schema,
            parallel,
            hints,
        } => PhysicalPlan::Union {
            inputs: inputs.into_iter().map(apply_outer_to_inner_join).collect(),
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
            left: Box::new(apply_outer_to_inner_join(*left)),
            right: Box::new(apply_outer_to_inner_join(*right)),
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
            left: Box::new(apply_outer_to_inner_join(*left)),
            right: Box::new(apply_outer_to_inner_join(*right)),
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
            input: Box::new(apply_outer_to_inner_join(*input)),
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
            body: Box::new(apply_outer_to_inner_join(*body)),
            parallel_ctes,
            hints,
        },

        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => PhysicalPlan::Unnest {
            input: Box::new(apply_outer_to_inner_join(*input)),
            columns,
            schema,
        },

        PhysicalPlan::Qualify { input, predicate } => PhysicalPlan::Qualify {
            input: Box::new(apply_outer_to_inner_join(*input)),
            predicate,
        },

        PhysicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => PhysicalPlan::Sample {
            input: Box::new(apply_outer_to_inner_join(*input)),
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
            source: Box::new(apply_outer_to_inner_join(*source)),
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
            query: query.map(|q| Box::new(apply_outer_to_inner_join(*q))),
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
            query: Box::new(apply_outer_to_inner_join(*query)),
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
            source: Box::new(apply_outer_to_inner_join(*source)),
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
            from: from.map(|f| Box::new(apply_outer_to_inner_join(*f))),
            filter,
        },

        PhysicalPlan::ExportData { options, query } => PhysicalPlan::ExportData {
            options,
            query: Box::new(apply_outer_to_inner_join(*query)),
        },

        PhysicalPlan::For {
            variable,
            query,
            body,
        } => PhysicalPlan::For {
            variable,
            query: Box::new(apply_outer_to_inner_join(*query)),
            body: body.into_iter().map(apply_outer_to_inner_join).collect(),
        },

        PhysicalPlan::If {
            condition,
            then_branch,
            else_branch,
        } => PhysicalPlan::If {
            condition,
            then_branch: then_branch
                .into_iter()
                .map(apply_outer_to_inner_join)
                .collect(),
            else_branch: else_branch
                .map(|b| b.into_iter().map(apply_outer_to_inner_join).collect()),
        },

        PhysicalPlan::While {
            condition,
            body,
            label,
        } => PhysicalPlan::While {
            condition,
            body: body.into_iter().map(apply_outer_to_inner_join).collect(),
            label,
        },

        PhysicalPlan::Loop { body, label } => PhysicalPlan::Loop {
            body: body.into_iter().map(apply_outer_to_inner_join).collect(),
            label,
        },

        PhysicalPlan::Block { body, label } => PhysicalPlan::Block {
            body: body.into_iter().map(apply_outer_to_inner_join).collect(),
            label,
        },

        PhysicalPlan::Repeat {
            body,
            until_condition,
        } => PhysicalPlan::Repeat {
            body: body.into_iter().map(apply_outer_to_inner_join).collect(),
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
            body: body.into_iter().map(apply_outer_to_inner_join).collect(),
            or_replace,
            if_not_exists,
        },

        PhysicalPlan::TryCatch {
            try_block,
            catch_block,
        } => PhysicalPlan::TryCatch {
            try_block: try_block
                .into_iter()
                .map(|(p, sql)| (apply_outer_to_inner_join(p), sql))
                .collect(),
            catch_block: catch_block
                .into_iter()
                .map(apply_outer_to_inner_join)
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
            input: Box::new(apply_outer_to_inner_join(*input)),
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
            input: Box::new(apply_outer_to_inner_join(*input)),
            analyze,
            logical_plan_text,
            physical_plan_text,
        },

        PhysicalPlan::TableScan {
            table_name,
            schema,
            projection,
            row_count,
        } => PhysicalPlan::TableScan {
            table_name,
            schema,
            projection,
            row_count,
        },

        PhysicalPlan::Values { values, schema } => PhysicalPlan::Values { values, schema },

        PhysicalPlan::Empty { schema } => PhysicalPlan::Empty { schema },

        PhysicalPlan::Delete {
            table_name,
            alias,
            filter,
        } => PhysicalPlan::Delete {
            table_name,
            alias,
            filter,
        },

        PhysicalPlan::DropTable {
            table_names,
            if_exists,
        } => PhysicalPlan::DropTable {
            table_names,
            if_exists,
        },

        PhysicalPlan::AlterTable {
            table_name,
            operation,
            if_exists,
        } => PhysicalPlan::AlterTable {
            table_name,
            operation,
            if_exists,
        },

        PhysicalPlan::Truncate { table_name } => PhysicalPlan::Truncate { table_name },

        PhysicalPlan::DropView { name, if_exists } => PhysicalPlan::DropView { name, if_exists },

        PhysicalPlan::CreateSchema {
            name,
            if_not_exists,
            or_replace,
        } => PhysicalPlan::CreateSchema {
            name,
            if_not_exists,
            or_replace,
        },

        PhysicalPlan::DropSchema {
            name,
            if_exists,
            cascade,
        } => PhysicalPlan::DropSchema {
            name,
            if_exists,
            cascade,
        },

        PhysicalPlan::UndropSchema {
            name,
            if_not_exists,
        } => PhysicalPlan::UndropSchema {
            name,
            if_not_exists,
        },

        PhysicalPlan::AlterSchema { name, options } => PhysicalPlan::AlterSchema { name, options },

        PhysicalPlan::CreateFunction {
            name,
            args,
            return_type,
            body,
            or_replace,
            if_not_exists,
            is_temp,
            is_aggregate,
        } => PhysicalPlan::CreateFunction {
            name,
            args,
            return_type,
            body,
            or_replace,
            if_not_exists,
            is_temp,
            is_aggregate,
        },

        PhysicalPlan::DropFunction { name, if_exists } => {
            PhysicalPlan::DropFunction { name, if_exists }
        }

        PhysicalPlan::DropProcedure { name, if_exists } => {
            PhysicalPlan::DropProcedure { name, if_exists }
        }

        PhysicalPlan::Call {
            procedure_name,
            args,
        } => PhysicalPlan::Call {
            procedure_name,
            args,
        },

        PhysicalPlan::LoadData {
            table_name,
            options,
            temp_table,
            temp_schema,
        } => PhysicalPlan::LoadData {
            table_name,
            options,
            temp_table,
            temp_schema,
        },

        PhysicalPlan::Declare {
            name,
            data_type,
            default,
        } => PhysicalPlan::Declare {
            name,
            data_type,
            default,
        },

        PhysicalPlan::SetVariable { name, value } => PhysicalPlan::SetVariable { name, value },

        PhysicalPlan::SetMultipleVariables { names, value } => {
            PhysicalPlan::SetMultipleVariables { names, value }
        }

        PhysicalPlan::Return { value } => PhysicalPlan::Return { value },

        PhysicalPlan::Raise { message, level } => PhysicalPlan::Raise { message, level },

        PhysicalPlan::ExecuteImmediate {
            sql_expr,
            into_variables,
            using_params,
        } => PhysicalPlan::ExecuteImmediate {
            sql_expr,
            into_variables,
            using_params,
        },

        PhysicalPlan::Break { label } => PhysicalPlan::Break { label },

        PhysicalPlan::Continue { label } => PhysicalPlan::Continue { label },

        PhysicalPlan::CreateSnapshot {
            snapshot_name,
            source_name,
            if_not_exists,
        } => PhysicalPlan::CreateSnapshot {
            snapshot_name,
            source_name,
            if_not_exists,
        },

        PhysicalPlan::DropSnapshot {
            snapshot_name,
            if_exists,
        } => PhysicalPlan::DropSnapshot {
            snapshot_name,
            if_exists,
        },

        PhysicalPlan::Assert { condition, message } => PhysicalPlan::Assert { condition, message },

        PhysicalPlan::Grant {
            roles,
            resource_type,
            resource_name,
            grantees,
        } => PhysicalPlan::Grant {
            roles,
            resource_type,
            resource_name,
            grantees,
        },

        PhysicalPlan::Revoke {
            roles,
            resource_type,
            resource_name,
            grantees,
        } => PhysicalPlan::Revoke {
            roles,
            resource_type,
            resource_name,
            grantees,
        },

        PhysicalPlan::BeginTransaction => PhysicalPlan::BeginTransaction,

        PhysicalPlan::Commit => PhysicalPlan::Commit,

        PhysicalPlan::Rollback => PhysicalPlan::Rollback,
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

    fn make_hash_join(
        left: PhysicalPlan,
        right: PhysicalPlan,
        join_type: JoinType,
    ) -> PhysicalPlan {
        let mut fields = left.schema().fields.clone();
        fields.extend(right.schema().fields.clone());
        let schema = PlanSchema::from_fields(fields);
        PhysicalPlan::HashJoin {
            left: Box::new(left),
            right: Box::new(right),
            join_type,
            left_keys: vec![col("a", "col0", 0)],
            right_keys: vec![col("b", "col0", 0)],
            schema,
            parallel: false,
            hints: ExecutionHints::default(),
        }
    }

    fn make_nested_loop_join(
        left: PhysicalPlan,
        right: PhysicalPlan,
        join_type: JoinType,
    ) -> PhysicalPlan {
        let mut fields = left.schema().fields.clone();
        fields.extend(right.schema().fields.clone());
        let schema = PlanSchema::from_fields(fields);
        PhysicalPlan::NestedLoopJoin {
            left: Box::new(left),
            right: Box::new(right),
            join_type,
            condition: Some(eq(col("a", "col0", 0), col("b", "col0", 2))),
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

    fn or(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Or,
            right: Box::new(right),
        }
    }

    fn lit_int(val: i64) -> Expr {
        Expr::Literal(Literal::Int64(val))
    }

    fn is_not_null(expr: Expr) -> Expr {
        Expr::IsNull {
            expr: Box::new(expr),
            negated: true,
        }
    }

    fn is_null(expr: Expr) -> Expr {
        Expr::IsNull {
            expr: Box::new(expr),
            negated: false,
        }
    }

    fn in_list(expr: Expr, list: Vec<Expr>) -> Expr {
        Expr::InList {
            expr: Box::new(expr),
            list,
            negated: false,
        }
    }

    fn between(expr: Expr, low: Expr, high: Expr) -> Expr {
        Expr::Between {
            expr: Box::new(expr),
            low: Box::new(low),
            high: Box::new(high),
            negated: false,
        }
    }

    fn coalesce(args: Vec<Expr>) -> Expr {
        Expr::ScalarFunction {
            name: ScalarFunction::Coalesce,
            args,
        }
    }

    #[test]
    fn converts_left_join_to_inner_with_is_not_null_on_right() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_hash_join(left, right, JoinType::Left);

        let predicate = is_not_null(col("b", "col0", 2));

        let plan = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate,
        };

        let result = apply_outer_to_inner_join(plan);

        match result {
            PhysicalPlan::Filter { input, .. } => match *input {
                PhysicalPlan::HashJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Inner);
                }
                _ => panic!("Expected HashJoin"),
            },
            _ => panic!("Expected Filter"),
        }
    }

    #[test]
    fn converts_left_join_to_inner_with_equality_on_right() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_hash_join(left, right, JoinType::Left);

        let predicate = eq(col("b", "col0", 2), lit_int(5));

        let plan = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate,
        };

        let result = apply_outer_to_inner_join(plan);

        match result {
            PhysicalPlan::Filter { input, .. } => match *input {
                PhysicalPlan::HashJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Inner);
                }
                _ => panic!("Expected HashJoin"),
            },
            _ => panic!("Expected Filter"),
        }
    }

    #[test]
    fn converts_right_join_to_inner_with_comparison_on_left() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_hash_join(left, right, JoinType::Right);

        let predicate = gt(col("a", "col0", 0), lit_int(10));

        let plan = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate,
        };

        let result = apply_outer_to_inner_join(plan);

        match result {
            PhysicalPlan::Filter { input, .. } => match *input {
                PhysicalPlan::HashJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Inner);
                }
                _ => panic!("Expected HashJoin"),
            },
            _ => panic!("Expected Filter"),
        }
    }

    #[test]
    fn preserves_left_join_with_is_null_on_right() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_hash_join(left, right, JoinType::Left);

        let predicate = is_null(col("b", "col0", 2));

        let plan = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate,
        };

        let result = apply_outer_to_inner_join(plan);

        match result {
            PhysicalPlan::Filter { input, .. } => match *input {
                PhysicalPlan::HashJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Left);
                }
                _ => panic!("Expected HashJoin"),
            },
            _ => panic!("Expected Filter"),
        }
    }

    #[test]
    fn preserves_left_join_with_predicate_on_left_side_only() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_hash_join(left, right, JoinType::Left);

        let predicate = eq(col("a", "col0", 0), lit_int(5));

        let plan = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate,
        };

        let result = apply_outer_to_inner_join(plan);

        match result {
            PhysicalPlan::Filter { input, .. } => match *input {
                PhysicalPlan::HashJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Left);
                }
                _ => panic!("Expected HashJoin"),
            },
            _ => panic!("Expected Filter"),
        }
    }

    #[test]
    fn converts_full_join_to_inner_when_both_sides_rejected() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_hash_join(left, right, JoinType::Full);

        let left_pred = is_not_null(col("a", "col0", 0));
        let right_pred = is_not_null(col("b", "col0", 2));
        let predicate = and(left_pred, right_pred);

        let plan = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate,
        };

        let result = apply_outer_to_inner_join(plan);

        match result {
            PhysicalPlan::Filter { input, .. } => match *input {
                PhysicalPlan::HashJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Inner);
                }
                _ => panic!("Expected HashJoin"),
            },
            _ => panic!("Expected Filter"),
        }
    }

    #[test]
    fn converts_full_join_to_left_when_right_nulls_rejected() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_hash_join(left, right, JoinType::Full);

        let predicate = is_not_null(col("b", "col0", 2));

        let plan = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate,
        };

        let result = apply_outer_to_inner_join(plan);

        match result {
            PhysicalPlan::Filter { input, .. } => match *input {
                PhysicalPlan::HashJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Left);
                }
                _ => panic!("Expected HashJoin"),
            },
            _ => panic!("Expected Filter"),
        }
    }

    #[test]
    fn converts_full_join_to_right_when_left_nulls_rejected() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_hash_join(left, right, JoinType::Full);

        let predicate = is_not_null(col("a", "col0", 0));

        let plan = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate,
        };

        let result = apply_outer_to_inner_join(plan);

        match result {
            PhysicalPlan::Filter { input, .. } => match *input {
                PhysicalPlan::HashJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Right);
                }
                _ => panic!("Expected HashJoin"),
            },
            _ => panic!("Expected Filter"),
        }
    }

    #[test]
    fn handles_nested_loop_join_conversion() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_nested_loop_join(left, right, JoinType::Left);

        let predicate = eq(col("b", "col0", 2), lit_int(5));

        let plan = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate,
        };

        let result = apply_outer_to_inner_join(plan);

        match result {
            PhysicalPlan::Filter { input, .. } => match *input {
                PhysicalPlan::NestedLoopJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Inner);
                }
                _ => panic!("Expected NestedLoopJoin"),
            },
            _ => panic!("Expected Filter"),
        }
    }

    #[test]
    fn converts_with_in_list_predicate() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_hash_join(left, right, JoinType::Left);

        let predicate = in_list(
            col("b", "col0", 2),
            vec![lit_int(1), lit_int(2), lit_int(3)],
        );

        let plan = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate,
        };

        let result = apply_outer_to_inner_join(plan);

        match result {
            PhysicalPlan::Filter { input, .. } => match *input {
                PhysicalPlan::HashJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Inner);
                }
                _ => panic!("Expected HashJoin"),
            },
            _ => panic!("Expected Filter"),
        }
    }

    #[test]
    fn converts_with_between_predicate() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_hash_join(left, right, JoinType::Left);

        let predicate = between(col("b", "col0", 2), lit_int(1), lit_int(10));

        let plan = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate,
        };

        let result = apply_outer_to_inner_join(plan);

        match result {
            PhysicalPlan::Filter { input, .. } => match *input {
                PhysicalPlan::HashJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Inner);
                }
                _ => panic!("Expected HashJoin"),
            },
            _ => panic!("Expected Filter"),
        }
    }

    #[test]
    fn preserves_with_coalesce_predicate() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_hash_join(left, right, JoinType::Left);

        let coalesce_expr = coalesce(vec![col("b", "col0", 2), lit_int(0)]);
        let predicate = eq(coalesce_expr, lit_int(5));

        let plan = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate,
        };

        let result = apply_outer_to_inner_join(plan);

        match result {
            PhysicalPlan::Filter { input, .. } => match *input {
                PhysicalPlan::HashJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Left);
                }
                _ => panic!("Expected HashJoin"),
            },
            _ => panic!("Expected Filter"),
        }
    }

    #[test]
    fn converts_with_and_predicate_where_one_rejects() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_hash_join(left, right, JoinType::Left);

        let left_pred = eq(col("a", "col0", 0), lit_int(5));
        let right_pred = eq(col("b", "col0", 2), lit_int(10));
        let predicate = and(left_pred, right_pred);

        let plan = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate,
        };

        let result = apply_outer_to_inner_join(plan);

        match result {
            PhysicalPlan::Filter { input, .. } => match *input {
                PhysicalPlan::HashJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Inner);
                }
                _ => panic!("Expected HashJoin"),
            },
            _ => panic!("Expected Filter"),
        }
    }

    #[test]
    fn preserves_with_or_predicate_where_only_one_rejects() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_hash_join(left, right, JoinType::Left);

        let left_pred = eq(col("a", "col0", 0), lit_int(5));
        let right_pred = eq(col("b", "col0", 2), lit_int(10));
        let predicate = or(left_pred, right_pred);

        let plan = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate,
        };

        let result = apply_outer_to_inner_join(plan);

        match result {
            PhysicalPlan::Filter { input, .. } => match *input {
                PhysicalPlan::HashJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Left);
                }
                _ => panic!("Expected HashJoin"),
            },
            _ => panic!("Expected Filter"),
        }
    }

    #[test]
    fn converts_with_or_predicate_where_both_reject() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_hash_join(left, right, JoinType::Left);

        let pred1 = eq(col("b", "col0", 2), lit_int(5));
        let pred2 = gt(col("b", "col1", 3), lit_int(10));
        let predicate = or(pred1, pred2);

        let plan = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate,
        };

        let result = apply_outer_to_inner_join(plan);

        match result {
            PhysicalPlan::Filter { input, .. } => match *input {
                PhysicalPlan::HashJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Inner);
                }
                _ => panic!("Expected HashJoin"),
            },
            _ => panic!("Expected Filter"),
        }
    }

    #[test]
    fn preserves_inner_join_unchanged() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_hash_join(left, right, JoinType::Inner);

        let predicate = is_not_null(col("b", "col0", 2));

        let plan = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate,
        };

        let result = apply_outer_to_inner_join(plan);

        match result {
            PhysicalPlan::Filter { input, .. } => match *input {
                PhysicalPlan::HashJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Inner);
                }
                _ => panic!("Expected HashJoin"),
            },
            _ => panic!("Expected Filter"),
        }
    }

    #[test]
    fn recursively_transforms_nested_plans() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_hash_join(left, right, JoinType::Left);

        let predicate = is_not_null(col("b", "col0", 2));

        let inner_filter = PhysicalPlan::Filter {
            input: Box::new(join),
            predicate,
        };

        let plan = PhysicalPlan::Project {
            input: Box::new(inner_filter),
            expressions: vec![col("a", "col0", 0)],
            schema: make_table_schema("result", 1),
        };

        let result = apply_outer_to_inner_join(plan);

        match result {
            PhysicalPlan::Project { input, .. } => match *input {
                PhysicalPlan::Filter { input, .. } => match *input {
                    PhysicalPlan::HashJoin { join_type, .. } => {
                        assert_eq!(join_type, JoinType::Inner);
                    }
                    _ => panic!("Expected HashJoin"),
                },
                _ => panic!("Expected Filter"),
            },
            _ => panic!("Expected Project"),
        }
    }

    #[test]
    fn handles_join_without_filter_no_change() {
        let left = make_scan("a", 2);
        let right = make_scan("b", 2);
        let join = make_hash_join(left, right, JoinType::Left);

        let result = apply_outer_to_inner_join(join);

        match result {
            PhysicalPlan::HashJoin { join_type, .. } => {
                assert_eq!(join_type, JoinType::Left);
            }
            _ => panic!("Expected HashJoin"),
        }
    }

    #[test]
    fn test_get_nullable_column_indices_left_join() {
        let nullable = get_nullable_column_indices(4, JoinType::Left, 2);
        assert!(!nullable.contains(&0));
        assert!(!nullable.contains(&1));
        assert!(nullable.contains(&2));
        assert!(nullable.contains(&3));
    }

    #[test]
    fn test_get_nullable_column_indices_right_join() {
        let nullable = get_nullable_column_indices(4, JoinType::Right, 2);
        assert!(nullable.contains(&0));
        assert!(nullable.contains(&1));
        assert!(!nullable.contains(&2));
        assert!(!nullable.contains(&3));
    }

    #[test]
    fn test_get_nullable_column_indices_full_join() {
        let nullable = get_nullable_column_indices(4, JoinType::Full, 2);
        assert!(nullable.contains(&0));
        assert!(nullable.contains(&1));
        assert!(nullable.contains(&2));
        assert!(nullable.contains(&3));
    }

    #[test]
    fn test_get_nullable_column_indices_inner_join() {
        let nullable = get_nullable_column_indices(4, JoinType::Inner, 2);
        assert!(nullable.is_empty());
    }

    #[test]
    fn test_is_null_rejecting_comparison_operators() {
        let mut nullable = FxHashSet::default();
        nullable.insert(0);

        let col_expr = col("t", "c", 0);

        assert!(is_null_rejecting(
            &eq(col_expr.clone(), lit_int(5)),
            &nullable
        ));
        assert!(is_null_rejecting(
            &gt(col_expr.clone(), lit_int(5)),
            &nullable
        ));

        let lt_expr = Expr::BinaryOp {
            left: Box::new(col_expr.clone()),
            op: BinaryOp::Lt,
            right: Box::new(lit_int(5)),
        };
        assert!(is_null_rejecting(&lt_expr, &nullable));

        let lteq_expr = Expr::BinaryOp {
            left: Box::new(col_expr.clone()),
            op: BinaryOp::LtEq,
            right: Box::new(lit_int(5)),
        };
        assert!(is_null_rejecting(&lteq_expr, &nullable));

        let gteq_expr = Expr::BinaryOp {
            left: Box::new(col_expr.clone()),
            op: BinaryOp::GtEq,
            right: Box::new(lit_int(5)),
        };
        assert!(is_null_rejecting(&gteq_expr, &nullable));

        let neq_expr = Expr::BinaryOp {
            left: Box::new(col_expr.clone()),
            op: BinaryOp::NotEq,
            right: Box::new(lit_int(5)),
        };
        assert!(is_null_rejecting(&neq_expr, &nullable));
    }

    #[test]
    fn test_is_null_rejecting_is_null_variants() {
        let mut nullable = FxHashSet::default();
        nullable.insert(0);

        let col_expr = col("t", "c", 0);

        assert!(is_null_rejecting(&is_not_null(col_expr.clone()), &nullable));
        assert!(!is_null_rejecting(&is_null(col_expr.clone()), &nullable));
    }

    #[test]
    fn test_is_null_rejecting_logical_operators() {
        let mut nullable = FxHashSet::default();
        nullable.insert(0);
        nullable.insert(1);

        let col0 = col("t", "c0", 0);
        let col1 = col("t", "c1", 1);

        let and_pred = and(eq(col0.clone(), lit_int(5)), is_null(col1.clone()));
        assert!(is_null_rejecting(&and_pred, &nullable));

        let or_pred_both_reject = or(eq(col0.clone(), lit_int(5)), eq(col1.clone(), lit_int(10)));
        assert!(is_null_rejecting(&or_pred_both_reject, &nullable));

        let or_pred_one_rejects = or(eq(col0.clone(), lit_int(5)), is_null(col1.clone()));
        assert!(!is_null_rejecting(&or_pred_one_rejects, &nullable));
    }

    #[test]
    fn test_is_null_rejecting_no_nullable_reference() {
        let mut nullable = FxHashSet::default();
        nullable.insert(2);

        let col_expr = col("t", "c", 0);
        assert!(!is_null_rejecting(
            &eq(col_expr.clone(), lit_int(5)),
            &nullable
        ));
    }
}
