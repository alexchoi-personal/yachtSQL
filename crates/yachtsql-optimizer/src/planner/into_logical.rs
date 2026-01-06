#![coverage(off)]

use yachtsql_ir::{BinaryOp, Expr, JoinType, LogicalPlan, SetOperationType};

use crate::PhysicalPlan;

impl PhysicalPlan {
    pub fn into_logical(self) -> LogicalPlan {
        match self {
            PhysicalPlan::TableScan {
                table_name,
                schema,
                projection,
                ..
            } => LogicalPlan::Scan {
                table_name,
                schema,
                projection,
            },
            PhysicalPlan::Sample {
                input,
                sample_type,
                sample_value,
            } => LogicalPlan::Sample {
                input: Box::new(input.into_logical()),
                sample_type,
                sample_value,
            },
            PhysicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
                input: Box::new(input.into_logical()),
                predicate,
            },
            PhysicalPlan::Project {
                input,
                expressions,
                schema,
            } => LogicalPlan::Project {
                input: Box::new(input.into_logical()),
                expressions,
                schema,
            },
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
                schema,
                ..
            } => LogicalPlan::Join {
                left: Box::new(left.into_logical()),
                right: Box::new(right.into_logical()),
                join_type,
                condition,
                schema,
            },
            PhysicalPlan::CrossJoin {
                left,
                right,
                schema,
                ..
            } => LogicalPlan::Join {
                left: Box::new(left.into_logical()),
                right: Box::new(right.into_logical()),
                join_type: JoinType::Cross,
                condition: None,
                schema,
            },
            PhysicalPlan::HashJoin {
                left,
                right,
                join_type,
                left_keys,
                right_keys,
                schema,
                ..
            } => {
                let left_schema_len = left.schema().fields.len();
                let restore_right_index = |expr: Expr| -> Expr {
                    match expr {
                        Expr::Column { table, name, index } => Expr::Column {
                            table,
                            name,
                            index: index.map(|i| i + left_schema_len),
                        },
                        other => other,
                    }
                };
                let condition = if left_keys.len() == 1 {
                    let mut left_iter = left_keys.into_iter();
                    let mut right_iter = right_keys.into_iter();
                    let left_key = left_iter.next().unwrap_or_else(|| {
                        panic!("invariant violation: left_keys.len() == 1 but iterator empty")
                    });
                    let right_key = right_iter.next().unwrap_or_else(|| {
                        panic!("invariant violation: right_keys must match left_keys length")
                    });
                    Some(Expr::BinaryOp {
                        left: Box::new(left_key),
                        op: BinaryOp::Eq,
                        right: Box::new(restore_right_index(right_key)),
                    })
                } else {
                    let equalities: Vec<Expr> = left_keys
                        .into_iter()
                        .zip(right_keys)
                        .map(|(l, r)| Expr::BinaryOp {
                            left: Box::new(l),
                            op: BinaryOp::Eq,
                            right: Box::new(restore_right_index(r)),
                        })
                        .collect();
                    equalities.into_iter().reduce(|acc, e| Expr::BinaryOp {
                        left: Box::new(acc),
                        op: BinaryOp::And,
                        right: Box::new(e),
                    })
                };
                LogicalPlan::Join {
                    left: Box::new(left.into_logical()),
                    right: Box::new(right.into_logical()),
                    join_type,
                    condition,
                    schema,
                }
            }
            PhysicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
                schema,
                grouping_sets,
                ..
            } => LogicalPlan::Aggregate {
                input: Box::new(input.into_logical()),
                group_by,
                aggregates,
                schema,
                grouping_sets,
            },
            PhysicalPlan::Sort {
                input, sort_exprs, ..
            } => LogicalPlan::Sort {
                input: Box::new(input.into_logical()),
                sort_exprs,
            },
            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(input.into_logical()),
                limit,
                offset,
            },
            PhysicalPlan::TopN {
                input,
                sort_exprs,
                limit,
            } => LogicalPlan::Limit {
                input: Box::new(LogicalPlan::Sort {
                    input: Box::new(input.into_logical()),
                    sort_exprs,
                }),
                limit: Some(limit),
                offset: None,
            },
            PhysicalPlan::Distinct { input } => LogicalPlan::Distinct {
                input: Box::new(input.into_logical()),
            },
            PhysicalPlan::Union {
                inputs,
                all,
                schema,
                ..
            } => {
                let mut iter = inputs.into_iter();
                let first = iter.next().unwrap_or_else(|| {
                    panic!("invariant violation: UNION must have at least one input")
                });
                let first = first.into_logical();
                iter.fold(first, |acc, plan| LogicalPlan::SetOperation {
                    left: Box::new(acc),
                    right: Box::new(plan.into_logical()),
                    op: SetOperationType::Union,
                    all,
                    schema: schema.clone(),
                })
            }
            PhysicalPlan::Intersect {
                left,
                right,
                all,
                schema,
                ..
            } => LogicalPlan::SetOperation {
                left: Box::new(left.into_logical()),
                right: Box::new(right.into_logical()),
                op: SetOperationType::Intersect,
                all,
                schema,
            },
            PhysicalPlan::Except {
                left,
                right,
                all,
                schema,
                ..
            } => LogicalPlan::SetOperation {
                left: Box::new(left.into_logical()),
                right: Box::new(right.into_logical()),
                op: SetOperationType::Except,
                all,
                schema,
            },
            PhysicalPlan::Window {
                input,
                window_exprs,
                schema,
                ..
            } => LogicalPlan::Window {
                input: Box::new(input.into_logical()),
                window_exprs,
                schema,
            },
            PhysicalPlan::Unnest {
                input,
                columns,
                schema,
            } => LogicalPlan::Unnest {
                input: Box::new(input.into_logical()),
                columns,
                schema,
            },
            PhysicalPlan::Qualify { input, predicate } => LogicalPlan::Qualify {
                input: Box::new(input.into_logical()),
                predicate,
            },
            PhysicalPlan::WithCte { ctes, body, .. } => LogicalPlan::WithCte {
                ctes,
                body: Box::new(body.into_logical()),
            },
            PhysicalPlan::Values { values, schema } => LogicalPlan::Values { values, schema },
            PhysicalPlan::Empty { schema } => LogicalPlan::Empty { schema },
            PhysicalPlan::Insert {
                table_name,
                columns,
                source,
            } => LogicalPlan::Insert {
                table_name,
                columns,
                source: Box::new(source.into_logical()),
            },
            PhysicalPlan::Update {
                table_name,
                alias,
                assignments,
                from,
                filter,
            } => LogicalPlan::Update {
                table_name,
                alias,
                assignments,
                from: from.map(|p| Box::new(p.into_logical())),
                filter,
            },
            PhysicalPlan::Delete {
                table_name,
                alias,
                filter,
            } => LogicalPlan::Delete {
                table_name,
                alias,
                filter,
            },
            PhysicalPlan::Merge {
                target_table,
                source,
                on,
                clauses,
            } => LogicalPlan::Merge {
                target_table,
                source: Box::new(source.into_logical()),
                on,
                clauses,
            },
            PhysicalPlan::CreateTable {
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
                query: query.map(|q| Box::new(q.into_logical())),
            },
            PhysicalPlan::DropTable {
                table_names,
                if_exists,
            } => LogicalPlan::DropTable {
                table_names,
                if_exists,
            },
            PhysicalPlan::AlterTable {
                table_name,
                operation,
                if_exists,
            } => LogicalPlan::AlterTable {
                table_name,
                operation,
                if_exists,
            },
            PhysicalPlan::Truncate { table_name } => LogicalPlan::Truncate { table_name },
            PhysicalPlan::CreateView {
                name,
                query,
                query_sql,
                column_aliases,
                or_replace,
                if_not_exists,
            } => LogicalPlan::CreateView {
                name,
                query: Box::new(query.into_logical()),
                query_sql,
                column_aliases,
                or_replace,
                if_not_exists,
            },
            PhysicalPlan::DropView { name, if_exists } => LogicalPlan::DropView { name, if_exists },
            PhysicalPlan::CreateSchema {
                name,
                if_not_exists,
                or_replace,
            } => LogicalPlan::CreateSchema {
                name,
                if_not_exists,
                or_replace,
            },
            PhysicalPlan::DropSchema {
                name,
                if_exists,
                cascade,
            } => LogicalPlan::DropSchema {
                name,
                if_exists,
                cascade,
            },
            PhysicalPlan::UndropSchema {
                name,
                if_not_exists,
            } => LogicalPlan::UndropSchema {
                name,
                if_not_exists,
            },
            PhysicalPlan::AlterSchema { name, options } => {
                LogicalPlan::AlterSchema { name, options }
            }
            PhysicalPlan::CreateFunction {
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
            PhysicalPlan::DropFunction { name, if_exists } => {
                LogicalPlan::DropFunction { name, if_exists }
            }
            PhysicalPlan::CreateProcedure {
                name,
                args,
                body,
                or_replace,
                if_not_exists,
            } => LogicalPlan::CreateProcedure {
                name,
                args,
                body: body.into_iter().map(|p| p.into_logical()).collect(),
                or_replace,
                if_not_exists,
            },
            PhysicalPlan::DropProcedure { name, if_exists } => {
                LogicalPlan::DropProcedure { name, if_exists }
            }
            PhysicalPlan::Call {
                procedure_name,
                args,
            } => LogicalPlan::Call {
                procedure_name,
                args,
            },
            PhysicalPlan::ExportData { options, query } => LogicalPlan::ExportData {
                options,
                query: Box::new(query.into_logical()),
            },
            PhysicalPlan::LoadData {
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
            PhysicalPlan::Declare {
                name,
                data_type,
                default,
            } => LogicalPlan::Declare {
                name,
                data_type,
                default,
            },
            PhysicalPlan::SetVariable { name, value } => LogicalPlan::SetVariable { name, value },
            PhysicalPlan::SetMultipleVariables { names, value } => {
                LogicalPlan::SetMultipleVariables { names, value }
            }
            PhysicalPlan::If {
                condition,
                then_branch,
                else_branch,
            } => LogicalPlan::If {
                condition,
                then_branch: then_branch.into_iter().map(|p| p.into_logical()).collect(),
                else_branch: else_branch.map(|b| b.into_iter().map(|p| p.into_logical()).collect()),
            },
            PhysicalPlan::While {
                condition,
                body,
                label,
            } => LogicalPlan::While {
                condition,
                body: body.into_iter().map(|p| p.into_logical()).collect(),
                label,
            },
            PhysicalPlan::Loop { body, label } => LogicalPlan::Loop {
                body: body.into_iter().map(|p| p.into_logical()).collect(),
                label,
            },
            PhysicalPlan::Block { body, label } => LogicalPlan::Block {
                body: body.into_iter().map(|p| p.into_logical()).collect(),
                label,
            },
            PhysicalPlan::Repeat {
                body,
                until_condition,
            } => LogicalPlan::Repeat {
                body: body.into_iter().map(|p| p.into_logical()).collect(),
                until_condition,
            },
            PhysicalPlan::For {
                variable,
                query,
                body,
            } => LogicalPlan::For {
                variable,
                query: Box::new(query.into_logical()),
                body: body.into_iter().map(|p| p.into_logical()).collect(),
            },
            PhysicalPlan::Return { value } => LogicalPlan::Return { value },
            PhysicalPlan::Raise { message, level } => LogicalPlan::Raise { message, level },
            PhysicalPlan::ExecuteImmediate {
                sql_expr,
                into_variables,
                using_params,
            } => LogicalPlan::ExecuteImmediate {
                sql_expr,
                into_variables,
                using_params,
            },
            PhysicalPlan::Break { label } => LogicalPlan::Break { label },
            PhysicalPlan::Continue { label } => LogicalPlan::Continue { label },
            PhysicalPlan::CreateSnapshot {
                snapshot_name,
                source_name,
                if_not_exists,
            } => LogicalPlan::CreateSnapshot {
                snapshot_name,
                source_name,
                if_not_exists,
            },
            PhysicalPlan::DropSnapshot {
                snapshot_name,
                if_exists,
            } => LogicalPlan::DropSnapshot {
                snapshot_name,
                if_exists,
            },
            PhysicalPlan::Assert { condition, message } => {
                LogicalPlan::Assert { condition, message }
            }
            PhysicalPlan::Grant {
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
            PhysicalPlan::Revoke {
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
            PhysicalPlan::BeginTransaction => LogicalPlan::BeginTransaction,
            PhysicalPlan::Commit => LogicalPlan::Commit,
            PhysicalPlan::Rollback => LogicalPlan::Rollback,
            PhysicalPlan::TryCatch {
                try_block,
                catch_block,
            } => LogicalPlan::TryCatch {
                try_block: try_block
                    .into_iter()
                    .map(|(p, sql)| (p.into_logical(), sql))
                    .collect(),
                catch_block: catch_block.into_iter().map(|p| p.into_logical()).collect(),
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
            } => LogicalPlan::GapFill {
                input: Box::new(input.into_logical()),
                ts_column,
                bucket_width,
                value_columns,
                partitioning_columns,
                origin,
                input_schema,
                schema,
            },
            PhysicalPlan::Explain { input, analyze, .. } => LogicalPlan::Explain {
                input: Box::new(input.into_logical()),
                analyze,
            },
        }
    }
}
