#![coverage(off)]

use yachtsql_optimizer::OptimizedLogicalPlan;

use super::{ExecutionHints, PARALLEL_ROW_THRESHOLD, PhysicalPlan};

impl PhysicalPlan {
    pub fn from_physical(plan: &OptimizedLogicalPlan) -> Self {
        match plan {
            OptimizedLogicalPlan::TableScan {
                table_name,
                schema,
                projection,
            } => PhysicalPlan::TableScan {
                table_name: table_name.clone(),
                schema: schema.clone(),
                projection: projection.clone(),
                row_count: None,
            },

            OptimizedLogicalPlan::Sample {
                input,
                sample_type,
                sample_value,
            } => PhysicalPlan::Sample {
                input: Box::new(Self::from_physical(input)),
                sample_type: *sample_type,
                sample_value: *sample_value,
            },

            OptimizedLogicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
                input: Box::new(Self::from_physical(input)),
                predicate: predicate.clone(),
            },

            OptimizedLogicalPlan::Project {
                input,
                expressions,
                schema,
            } => PhysicalPlan::Project {
                input: Box::new(Self::from_physical(input)),
                expressions: expressions.clone(),
                schema: schema.clone(),
            },

            OptimizedLogicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
                schema,
            } => {
                let left_plan = Box::new(Self::from_physical(left));
                let right_plan = Box::new(Self::from_physical(right));
                let parallel = Self::should_parallelize(&left_plan, &right_plan);
                PhysicalPlan::NestedLoopJoin {
                    left: left_plan,
                    right: right_plan,
                    join_type: *join_type,
                    condition: condition.clone(),
                    schema: schema.clone(),
                    parallel,
                    hints: ExecutionHints::default(),
                }
            }

            OptimizedLogicalPlan::CrossJoin {
                left,
                right,
                schema,
            } => {
                let left_plan = Box::new(Self::from_physical(left));
                let right_plan = Box::new(Self::from_physical(right));
                let parallel = Self::should_parallelize(&left_plan, &right_plan);
                PhysicalPlan::CrossJoin {
                    left: left_plan,
                    right: right_plan,
                    schema: schema.clone(),
                    parallel,
                    hints: ExecutionHints::default(),
                }
            }

            OptimizedLogicalPlan::HashJoin {
                left,
                right,
                join_type,
                left_keys,
                right_keys,
                schema,
            } => {
                let left_plan = Box::new(Self::from_physical(left));
                let right_plan = Box::new(Self::from_physical(right));
                let parallel = Self::should_parallelize(&left_plan, &right_plan);
                PhysicalPlan::HashJoin {
                    left: left_plan,
                    right: right_plan,
                    join_type: *join_type,
                    left_keys: left_keys.clone(),
                    right_keys: right_keys.clone(),
                    schema: schema.clone(),
                    parallel,
                    hints: ExecutionHints::default(),
                }
            }

            OptimizedLogicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
                schema,
                grouping_sets,
            } => PhysicalPlan::HashAggregate {
                input: Box::new(Self::from_physical(input)),
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
                schema: schema.clone(),
                grouping_sets: grouping_sets.clone(),
                hints: ExecutionHints::default(),
            },

            OptimizedLogicalPlan::Sort { input, sort_exprs } => PhysicalPlan::Sort {
                input: Box::new(Self::from_physical(input)),
                sort_exprs: sort_exprs.clone(),
                hints: ExecutionHints::default(),
            },

            OptimizedLogicalPlan::Limit {
                input,
                limit,
                offset,
            } => PhysicalPlan::Limit {
                input: Box::new(Self::from_physical(input)),
                limit: *limit,
                offset: *offset,
            },

            OptimizedLogicalPlan::TopN {
                input,
                sort_exprs,
                limit,
            } => PhysicalPlan::TopN {
                input: Box::new(Self::from_physical(input)),
                sort_exprs: sort_exprs.clone(),
                limit: *limit,
            },

            OptimizedLogicalPlan::Distinct { input } => PhysicalPlan::Distinct {
                input: Box::new(Self::from_physical(input)),
            },

            OptimizedLogicalPlan::Union {
                inputs,
                all,
                schema,
            } => {
                let input_plans: Vec<_> = inputs.iter().map(Self::from_physical).collect();
                let parallel = Self::should_parallelize_union(&input_plans);
                PhysicalPlan::Union {
                    inputs: input_plans,
                    all: *all,
                    schema: schema.clone(),
                    parallel,
                    hints: ExecutionHints::default(),
                }
            }

            OptimizedLogicalPlan::Intersect {
                left,
                right,
                all,
                schema,
            } => {
                let left_plan = Box::new(Self::from_physical(left));
                let right_plan = Box::new(Self::from_physical(right));
                let parallel = Self::should_parallelize(&left_plan, &right_plan);
                PhysicalPlan::Intersect {
                    left: left_plan,
                    right: right_plan,
                    all: *all,
                    schema: schema.clone(),
                    parallel,
                    hints: ExecutionHints::default(),
                }
            }

            OptimizedLogicalPlan::Except {
                left,
                right,
                all,
                schema,
            } => {
                let left_plan = Box::new(Self::from_physical(left));
                let right_plan = Box::new(Self::from_physical(right));
                let parallel = Self::should_parallelize(&left_plan, &right_plan);
                PhysicalPlan::Except {
                    left: left_plan,
                    right: right_plan,
                    all: *all,
                    schema: schema.clone(),
                    parallel,
                    hints: ExecutionHints::default(),
                }
            }

            OptimizedLogicalPlan::Window {
                input,
                window_exprs,
                schema,
            } => PhysicalPlan::Window {
                input: Box::new(Self::from_physical(input)),
                window_exprs: window_exprs.clone(),
                schema: schema.clone(),
                hints: ExecutionHints::default(),
            },

            OptimizedLogicalPlan::Unnest {
                input,
                columns,
                schema,
            } => PhysicalPlan::Unnest {
                input: Box::new(Self::from_physical(input)),
                columns: columns.clone(),
                schema: schema.clone(),
            },

            OptimizedLogicalPlan::Qualify { input, predicate } => PhysicalPlan::Qualify {
                input: Box::new(Self::from_physical(input)),
                predicate: predicate.clone(),
            },

            OptimizedLogicalPlan::WithCte { ctes, body } => {
                let parallel_ctes: Vec<usize> = ctes
                    .iter()
                    .enumerate()
                    .filter(|(_, cte)| !cte.recursive)
                    .filter(|(_, cte)| {
                        if let Ok(optimized) = yachtsql_optimizer::optimize(&cte.query) {
                            let plan = PhysicalPlan::from_physical(&optimized);
                            plan.estimate_rows() >= PARALLEL_ROW_THRESHOLD
                        } else {
                            false
                        }
                    })
                    .map(|(i, _)| i)
                    .collect();
                PhysicalPlan::WithCte {
                    ctes: ctes.clone(),
                    body: Box::new(Self::from_physical(body)),
                    parallel_ctes,
                    hints: ExecutionHints::default(),
                }
            }

            OptimizedLogicalPlan::Values { values, schema } => PhysicalPlan::Values {
                values: values.clone(),
                schema: schema.clone(),
            },

            OptimizedLogicalPlan::Empty { schema } => PhysicalPlan::Empty {
                schema: schema.clone(),
            },

            OptimizedLogicalPlan::Insert {
                table_name,
                columns,
                source,
            } => PhysicalPlan::Insert {
                table_name: table_name.clone(),
                columns: columns.clone(),
                source: Box::new(Self::from_physical(source)),
            },

            OptimizedLogicalPlan::Update {
                table_name,
                alias,
                assignments,
                from,
                filter,
            } => PhysicalPlan::Update {
                table_name: table_name.clone(),
                alias: alias.clone(),
                assignments: assignments.clone(),
                from: from.as_ref().map(|p| Box::new(Self::from_physical(p))),
                filter: filter.clone(),
            },

            OptimizedLogicalPlan::Delete {
                table_name,
                alias,
                filter,
            } => PhysicalPlan::Delete {
                table_name: table_name.clone(),
                alias: alias.clone(),
                filter: filter.clone(),
            },

            OptimizedLogicalPlan::Merge {
                target_table,
                source,
                on,
                clauses,
            } => PhysicalPlan::Merge {
                target_table: target_table.clone(),
                source: Box::new(Self::from_physical(source)),
                on: on.clone(),
                clauses: clauses.clone(),
            },

            OptimizedLogicalPlan::CreateTable {
                table_name,
                columns,
                if_not_exists,
                or_replace,
                query,
            } => PhysicalPlan::CreateTable {
                table_name: table_name.clone(),
                columns: columns.clone(),
                if_not_exists: *if_not_exists,
                or_replace: *or_replace,
                query: query.as_ref().map(|q| Box::new(Self::from_physical(q))),
            },

            OptimizedLogicalPlan::DropTable {
                table_names,
                if_exists,
            } => PhysicalPlan::DropTable {
                table_names: table_names.clone(),
                if_exists: *if_exists,
            },

            OptimizedLogicalPlan::AlterTable {
                table_name,
                operation,
                if_exists,
            } => PhysicalPlan::AlterTable {
                table_name: table_name.clone(),
                operation: operation.clone(),
                if_exists: *if_exists,
            },

            OptimizedLogicalPlan::Truncate { table_name } => PhysicalPlan::Truncate {
                table_name: table_name.clone(),
            },

            OptimizedLogicalPlan::CreateView {
                name,
                query,
                query_sql,
                column_aliases,
                or_replace,
                if_not_exists,
            } => PhysicalPlan::CreateView {
                name: name.clone(),
                query: Box::new(Self::from_physical(query)),
                query_sql: query_sql.clone(),
                column_aliases: column_aliases.clone(),
                or_replace: *or_replace,
                if_not_exists: *if_not_exists,
            },

            OptimizedLogicalPlan::DropView { name, if_exists } => PhysicalPlan::DropView {
                name: name.clone(),
                if_exists: *if_exists,
            },

            OptimizedLogicalPlan::CreateSchema {
                name,
                if_not_exists,
                or_replace,
            } => PhysicalPlan::CreateSchema {
                name: name.clone(),
                if_not_exists: *if_not_exists,
                or_replace: *or_replace,
            },

            OptimizedLogicalPlan::DropSchema {
                name,
                if_exists,
                cascade,
            } => PhysicalPlan::DropSchema {
                name: name.clone(),
                if_exists: *if_exists,
                cascade: *cascade,
            },

            OptimizedLogicalPlan::UndropSchema {
                name,
                if_not_exists,
            } => PhysicalPlan::UndropSchema {
                name: name.clone(),
                if_not_exists: *if_not_exists,
            },

            OptimizedLogicalPlan::AlterSchema { name, options } => PhysicalPlan::AlterSchema {
                name: name.clone(),
                options: options.clone(),
            },

            OptimizedLogicalPlan::CreateFunction {
                name,
                args,
                return_type,
                body,
                or_replace,
                if_not_exists,
                is_temp,
                is_aggregate,
            } => PhysicalPlan::CreateFunction {
                name: name.clone(),
                args: args.clone(),
                return_type: return_type.clone(),
                body: body.clone(),
                or_replace: *or_replace,
                if_not_exists: *if_not_exists,
                is_temp: *is_temp,
                is_aggregate: *is_aggregate,
            },

            OptimizedLogicalPlan::DropFunction { name, if_exists } => PhysicalPlan::DropFunction {
                name: name.clone(),
                if_exists: *if_exists,
            },

            OptimizedLogicalPlan::CreateProcedure {
                name,
                args,
                body,
                or_replace,
                if_not_exists,
            } => PhysicalPlan::CreateProcedure {
                name: name.clone(),
                args: args.clone(),
                body: body.iter().map(Self::from_physical).collect(),
                or_replace: *or_replace,
                if_not_exists: *if_not_exists,
            },

            OptimizedLogicalPlan::DropProcedure { name, if_exists } => {
                PhysicalPlan::DropProcedure {
                    name: name.clone(),
                    if_exists: *if_exists,
                }
            }

            OptimizedLogicalPlan::Call {
                procedure_name,
                args,
            } => PhysicalPlan::Call {
                procedure_name: procedure_name.clone(),
                args: args.clone(),
            },

            OptimizedLogicalPlan::ExportData { options, query } => PhysicalPlan::ExportData {
                options: options.clone(),
                query: Box::new(Self::from_physical(query)),
            },

            OptimizedLogicalPlan::LoadData {
                table_name,
                options,
                temp_table,
                temp_schema,
            } => PhysicalPlan::LoadData {
                table_name: table_name.clone(),
                options: options.clone(),
                temp_table: *temp_table,
                temp_schema: temp_schema.clone(),
            },

            OptimizedLogicalPlan::Declare {
                name,
                data_type,
                default,
            } => PhysicalPlan::Declare {
                name: name.clone(),
                data_type: data_type.clone(),
                default: default.clone(),
            },

            OptimizedLogicalPlan::SetVariable { name, value } => PhysicalPlan::SetVariable {
                name: name.clone(),
                value: value.clone(),
            },

            OptimizedLogicalPlan::SetMultipleVariables { names, value } => {
                PhysicalPlan::SetMultipleVariables {
                    names: names.clone(),
                    value: value.clone(),
                }
            }

            OptimizedLogicalPlan::If {
                condition,
                then_branch,
                else_branch,
            } => PhysicalPlan::If {
                condition: condition.clone(),
                then_branch: then_branch.iter().map(Self::from_physical).collect(),
                else_branch: else_branch
                    .as_ref()
                    .map(|b| b.iter().map(Self::from_physical).collect()),
            },

            OptimizedLogicalPlan::While {
                condition,
                body,
                label,
            } => PhysicalPlan::While {
                condition: condition.clone(),
                body: body.iter().map(Self::from_physical).collect(),
                label: label.clone(),
            },

            OptimizedLogicalPlan::Loop { body, label } => PhysicalPlan::Loop {
                body: body.iter().map(Self::from_physical).collect(),
                label: label.clone(),
            },

            OptimizedLogicalPlan::Block { body, label } => PhysicalPlan::Block {
                body: body.iter().map(Self::from_physical).collect(),
                label: label.clone(),
            },

            OptimizedLogicalPlan::Repeat {
                body,
                until_condition,
            } => PhysicalPlan::Repeat {
                body: body.iter().map(Self::from_physical).collect(),
                until_condition: until_condition.clone(),
            },

            OptimizedLogicalPlan::For {
                variable,
                query,
                body,
            } => PhysicalPlan::For {
                variable: variable.clone(),
                query: Box::new(Self::from_physical(query)),
                body: body.iter().map(Self::from_physical).collect(),
            },

            OptimizedLogicalPlan::Return { value } => PhysicalPlan::Return {
                value: value.clone(),
            },

            OptimizedLogicalPlan::Raise { message, level } => PhysicalPlan::Raise {
                message: message.clone(),
                level: *level,
            },

            OptimizedLogicalPlan::ExecuteImmediate {
                sql_expr,
                into_variables,
                using_params,
            } => PhysicalPlan::ExecuteImmediate {
                sql_expr: sql_expr.clone(),
                into_variables: into_variables.clone(),
                using_params: using_params.clone(),
            },

            OptimizedLogicalPlan::Break { label } => PhysicalPlan::Break {
                label: label.clone(),
            },

            OptimizedLogicalPlan::Continue { label } => PhysicalPlan::Continue {
                label: label.clone(),
            },

            OptimizedLogicalPlan::CreateSnapshot {
                snapshot_name,
                source_name,
                if_not_exists,
            } => PhysicalPlan::CreateSnapshot {
                snapshot_name: snapshot_name.clone(),
                source_name: source_name.clone(),
                if_not_exists: *if_not_exists,
            },

            OptimizedLogicalPlan::DropSnapshot {
                snapshot_name,
                if_exists,
            } => PhysicalPlan::DropSnapshot {
                snapshot_name: snapshot_name.clone(),
                if_exists: *if_exists,
            },

            OptimizedLogicalPlan::Assert { condition, message } => PhysicalPlan::Assert {
                condition: condition.clone(),
                message: message.clone(),
            },

            OptimizedLogicalPlan::Grant {
                roles,
                resource_type,
                resource_name,
                grantees,
            } => PhysicalPlan::Grant {
                roles: roles.clone(),
                resource_type: resource_type.clone(),
                resource_name: resource_name.clone(),
                grantees: grantees.clone(),
            },

            OptimizedLogicalPlan::Revoke {
                roles,
                resource_type,
                resource_name,
                grantees,
            } => PhysicalPlan::Revoke {
                roles: roles.clone(),
                resource_type: resource_type.clone(),
                resource_name: resource_name.clone(),
                grantees: grantees.clone(),
            },

            OptimizedLogicalPlan::BeginTransaction => PhysicalPlan::BeginTransaction,
            OptimizedLogicalPlan::Commit => PhysicalPlan::Commit,
            OptimizedLogicalPlan::Rollback => PhysicalPlan::Rollback,

            OptimizedLogicalPlan::TryCatch {
                try_block,
                catch_block,
            } => PhysicalPlan::TryCatch {
                try_block: try_block
                    .iter()
                    .map(|(p, sql)| (PhysicalPlan::from_physical(p), sql.clone()))
                    .collect(),
                catch_block: catch_block
                    .iter()
                    .map(PhysicalPlan::from_physical)
                    .collect(),
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
            } => PhysicalPlan::GapFill {
                input: Box::new(PhysicalPlan::from_physical(input)),
                ts_column: ts_column.clone(),
                bucket_width: bucket_width.clone(),
                value_columns: value_columns.clone(),
                partitioning_columns: partitioning_columns.clone(),
                origin: origin.clone(),
                input_schema: input_schema.clone(),
                schema: schema.clone(),
            },

            OptimizedLogicalPlan::Explain {
                input,
                analyze,
                logical_plan_text,
            } => {
                let physical_input = Self::from_physical(input);
                let physical_plan_text = format!("{:#?}", physical_input);
                PhysicalPlan::Explain {
                    input: Box::new(physical_input),
                    analyze: *analyze,
                    logical_plan_text: logical_plan_text.clone(),
                    physical_plan_text,
                }
            }
        }
    }
}
