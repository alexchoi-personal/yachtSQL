#![coverage(off)]

use yachtsql_ir::{BinaryOp, Expr, JoinType, LogicalPlan, SetOperationType};

use crate::optimized_logical_plan::{OptimizedLogicalPlan, SampleType};

impl OptimizedLogicalPlan {
    pub fn into_logical(self) -> LogicalPlan {
        match self {
            OptimizedLogicalPlan::TableScan {
                table_name,
                schema,
                projection,
            } => LogicalPlan::Scan {
                table_name,
                schema,
                projection,
            },
            OptimizedLogicalPlan::Sample {
                input,
                sample_type,
                sample_value,
            } => {
                let logical_sample_type = match sample_type {
                    SampleType::Rows => yachtsql_ir::SampleType::Rows,
                    SampleType::Percent => yachtsql_ir::SampleType::Percent,
                };
                LogicalPlan::Sample {
                    input: Box::new(input.into_logical()),
                    sample_type: logical_sample_type,
                    sample_value,
                }
            }
            OptimizedLogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
                input: Box::new(input.into_logical()),
                predicate,
            },
            OptimizedLogicalPlan::Project {
                input,
                expressions,
                schema,
            } => LogicalPlan::Project {
                input: Box::new(input.into_logical()),
                expressions,
                schema,
            },
            OptimizedLogicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
                schema,
            } => LogicalPlan::Join {
                left: Box::new(left.into_logical()),
                right: Box::new(right.into_logical()),
                join_type,
                condition,
                schema,
            },
            OptimizedLogicalPlan::CrossJoin {
                left,
                right,
                schema,
            } => LogicalPlan::Join {
                left: Box::new(left.into_logical()),
                right: Box::new(right.into_logical()),
                join_type: JoinType::Cross,
                condition: None,
                schema,
            },
            OptimizedLogicalPlan::HashJoin {
                left,
                right,
                join_type,
                left_keys,
                right_keys,
                schema,
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
                    Some(Expr::BinaryOp {
                        left: Box::new(left_keys.into_iter().next().unwrap()),
                        op: BinaryOp::Eq,
                        right: Box::new(restore_right_index(
                            right_keys.into_iter().next().unwrap(),
                        )),
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
            OptimizedLogicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
                schema,
                grouping_sets,
            } => LogicalPlan::Aggregate {
                input: Box::new(input.into_logical()),
                group_by,
                aggregates,
                schema,
                grouping_sets,
            },
            OptimizedLogicalPlan::Sort { input, sort_exprs } => LogicalPlan::Sort {
                input: Box::new(input.into_logical()),
                sort_exprs,
            },
            OptimizedLogicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(input.into_logical()),
                limit,
                offset,
            },
            OptimizedLogicalPlan::TopN {
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
            OptimizedLogicalPlan::Distinct { input } => LogicalPlan::Distinct {
                input: Box::new(input.into_logical()),
            },
            OptimizedLogicalPlan::Union {
                inputs,
                all,
                schema,
            } => {
                let mut iter = inputs.into_iter();
                let first = iter.next().unwrap().into_logical();
                iter.fold(first, |acc, plan| LogicalPlan::SetOperation {
                    left: Box::new(acc),
                    right: Box::new(plan.into_logical()),
                    op: SetOperationType::Union,
                    all,
                    schema: schema.clone(),
                })
            }
            OptimizedLogicalPlan::Intersect {
                left,
                right,
                all,
                schema,
            } => LogicalPlan::SetOperation {
                left: Box::new(left.into_logical()),
                right: Box::new(right.into_logical()),
                op: SetOperationType::Intersect,
                all,
                schema,
            },
            OptimizedLogicalPlan::Except {
                left,
                right,
                all,
                schema,
            } => LogicalPlan::SetOperation {
                left: Box::new(left.into_logical()),
                right: Box::new(right.into_logical()),
                op: SetOperationType::Except,
                all,
                schema,
            },
            OptimizedLogicalPlan::Window {
                input,
                window_exprs,
                schema,
            } => LogicalPlan::Window {
                input: Box::new(input.into_logical()),
                window_exprs,
                schema,
            },
            OptimizedLogicalPlan::Unnest {
                input,
                columns,
                schema,
            } => LogicalPlan::Unnest {
                input: Box::new(input.into_logical()),
                columns,
                schema,
            },
            OptimizedLogicalPlan::Qualify { input, predicate } => LogicalPlan::Qualify {
                input: Box::new(input.into_logical()),
                predicate,
            },
            OptimizedLogicalPlan::WithCte { ctes, body } => LogicalPlan::WithCte {
                ctes,
                body: Box::new(body.into_logical()),
            },
            OptimizedLogicalPlan::Values { values, schema } => {
                LogicalPlan::Values { values, schema }
            }
            OptimizedLogicalPlan::Empty { schema } => LogicalPlan::Empty { schema },
            OptimizedLogicalPlan::Insert {
                table_name,
                columns,
                source,
            } => LogicalPlan::Insert {
                table_name,
                columns,
                source: Box::new(source.into_logical()),
            },
            OptimizedLogicalPlan::Update {
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
            OptimizedLogicalPlan::Delete {
                table_name,
                alias,
                filter,
            } => LogicalPlan::Delete {
                table_name,
                alias,
                filter,
            },
            OptimizedLogicalPlan::Merge {
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
            OptimizedLogicalPlan::CreateTable {
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
            OptimizedLogicalPlan::DropTable {
                table_names,
                if_exists,
            } => LogicalPlan::DropTable {
                table_names,
                if_exists,
            },
            OptimizedLogicalPlan::AlterTable {
                table_name,
                operation,
                if_exists,
            } => LogicalPlan::AlterTable {
                table_name,
                operation,
                if_exists,
            },
            OptimizedLogicalPlan::Truncate { table_name } => LogicalPlan::Truncate { table_name },
            OptimizedLogicalPlan::CreateView {
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
            OptimizedLogicalPlan::DropView { name, if_exists } => {
                LogicalPlan::DropView { name, if_exists }
            }
            OptimizedLogicalPlan::CreateSchema {
                name,
                if_not_exists,
                or_replace,
            } => LogicalPlan::CreateSchema {
                name,
                if_not_exists,
                or_replace,
            },
            OptimizedLogicalPlan::DropSchema {
                name,
                if_exists,
                cascade,
            } => LogicalPlan::DropSchema {
                name,
                if_exists,
                cascade,
            },
            OptimizedLogicalPlan::UndropSchema {
                name,
                if_not_exists,
            } => LogicalPlan::UndropSchema {
                name,
                if_not_exists,
            },
            OptimizedLogicalPlan::AlterSchema { name, options } => {
                LogicalPlan::AlterSchema { name, options }
            }
            OptimizedLogicalPlan::CreateFunction {
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
            OptimizedLogicalPlan::DropFunction { name, if_exists } => {
                LogicalPlan::DropFunction { name, if_exists }
            }
            OptimizedLogicalPlan::CreateProcedure {
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
            OptimizedLogicalPlan::DropProcedure { name, if_exists } => {
                LogicalPlan::DropProcedure { name, if_exists }
            }
            OptimizedLogicalPlan::Call {
                procedure_name,
                args,
            } => LogicalPlan::Call {
                procedure_name,
                args,
            },
            OptimizedLogicalPlan::ExportData { options, query } => LogicalPlan::ExportData {
                options,
                query: Box::new(query.into_logical()),
            },
            OptimizedLogicalPlan::LoadData {
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
            OptimizedLogicalPlan::Declare {
                name,
                data_type,
                default,
            } => LogicalPlan::Declare {
                name,
                data_type,
                default,
            },
            OptimizedLogicalPlan::SetVariable { name, value } => {
                LogicalPlan::SetVariable { name, value }
            }
            OptimizedLogicalPlan::SetMultipleVariables { names, value } => {
                LogicalPlan::SetMultipleVariables { names, value }
            }
            OptimizedLogicalPlan::If {
                condition,
                then_branch,
                else_branch,
            } => LogicalPlan::If {
                condition,
                then_branch: then_branch.into_iter().map(|p| p.into_logical()).collect(),
                else_branch: else_branch.map(|b| b.into_iter().map(|p| p.into_logical()).collect()),
            },
            OptimizedLogicalPlan::While {
                condition,
                body,
                label,
            } => LogicalPlan::While {
                condition,
                body: body.into_iter().map(|p| p.into_logical()).collect(),
                label,
            },
            OptimizedLogicalPlan::Loop { body, label } => LogicalPlan::Loop {
                body: body.into_iter().map(|p| p.into_logical()).collect(),
                label,
            },
            OptimizedLogicalPlan::Block { body, label } => LogicalPlan::Block {
                body: body.into_iter().map(|p| p.into_logical()).collect(),
                label,
            },
            OptimizedLogicalPlan::Repeat {
                body,
                until_condition,
            } => LogicalPlan::Repeat {
                body: body.into_iter().map(|p| p.into_logical()).collect(),
                until_condition,
            },
            OptimizedLogicalPlan::For {
                variable,
                query,
                body,
            } => LogicalPlan::For {
                variable,
                query: Box::new(query.into_logical()),
                body: body.into_iter().map(|p| p.into_logical()).collect(),
            },
            OptimizedLogicalPlan::Return { value } => LogicalPlan::Return { value },
            OptimizedLogicalPlan::Raise { message, level } => LogicalPlan::Raise { message, level },
            OptimizedLogicalPlan::ExecuteImmediate {
                sql_expr,
                into_variables,
                using_params,
            } => LogicalPlan::ExecuteImmediate {
                sql_expr,
                into_variables,
                using_params,
            },
            OptimizedLogicalPlan::Break { label } => LogicalPlan::Break { label },
            OptimizedLogicalPlan::Continue { label } => LogicalPlan::Continue { label },
            OptimizedLogicalPlan::CreateSnapshot {
                snapshot_name,
                source_name,
                if_not_exists,
            } => LogicalPlan::CreateSnapshot {
                snapshot_name,
                source_name,
                if_not_exists,
            },
            OptimizedLogicalPlan::DropSnapshot {
                snapshot_name,
                if_exists,
            } => LogicalPlan::DropSnapshot {
                snapshot_name,
                if_exists,
            },
            OptimizedLogicalPlan::Assert { condition, message } => {
                LogicalPlan::Assert { condition, message }
            }
            OptimizedLogicalPlan::Grant {
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
            OptimizedLogicalPlan::Revoke {
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
            OptimizedLogicalPlan::BeginTransaction => LogicalPlan::BeginTransaction,
            OptimizedLogicalPlan::Commit => LogicalPlan::Commit,
            OptimizedLogicalPlan::Rollback => LogicalPlan::Rollback,
            OptimizedLogicalPlan::TryCatch {
                try_block,
                catch_block,
            } => LogicalPlan::TryCatch {
                try_block: try_block
                    .into_iter()
                    .map(|(p, sql)| (p.into_logical(), sql))
                    .collect(),
                catch_block: catch_block.into_iter().map(|p| p.into_logical()).collect(),
            },
            OptimizedLogicalPlan::GapFill {
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
        }
    }
}
