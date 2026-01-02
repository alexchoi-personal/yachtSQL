#![coverage(off)]

use yachtsql_common::error::Result;
use yachtsql_ir::{JoinType, LogicalPlan, SetOperationType};

use super::equi_join::extract_equi_join_keys;
use super::predicate::{
    PredicateSide, adjust_predicate_indices, classify_predicate_side, combine_predicates,
    split_and_predicates,
};
use crate::optimized_logical_plan::{OptimizedLogicalPlan, SampleType};

pub struct PhysicalPlanner;

impl PhysicalPlanner {
    pub fn new() -> Self {
        Self
    }

    #[allow(clippy::only_used_in_recursion)]
    pub fn plan(&self, logical: &LogicalPlan) -> Result<OptimizedLogicalPlan> {
        match logical {
            LogicalPlan::Scan {
                table_name,
                schema,
                projection,
            } => Ok(OptimizedLogicalPlan::TableScan {
                table_name: table_name.clone(),
                schema: schema.clone(),
                projection: projection.clone(),
            }),

            LogicalPlan::Sample {
                input,
                sample_type,
                sample_value,
            } => {
                let input = self.plan(input)?;
                let phys_sample_type = match sample_type {
                    yachtsql_ir::SampleType::Rows => SampleType::Rows,
                    yachtsql_ir::SampleType::Percent => SampleType::Percent,
                };
                Ok(OptimizedLogicalPlan::Sample {
                    input: Box::new(input),
                    sample_type: phys_sample_type,
                    sample_value: *sample_value,
                })
            }

            LogicalPlan::Filter { input, predicate } => match input.as_ref() {
                LogicalPlan::Join {
                    left,
                    right,
                    join_type,
                    condition,
                    schema,
                } if *join_type == JoinType::Inner => {
                    let left_schema_len = left.schema().fields.len();
                    let predicates = split_and_predicates(predicate);

                    let mut left_preds = Vec::new();
                    let mut right_preds = Vec::new();
                    let mut post_join_preds = Vec::new();

                    for pred in predicates {
                        match classify_predicate_side(&pred, left_schema_len) {
                            Some(PredicateSide::Left) => left_preds.push(pred),
                            Some(PredicateSide::Right) => {
                                right_preds.push(adjust_predicate_indices(&pred, left_schema_len))
                            }
                            Some(PredicateSide::Both) | None => post_join_preds.push(pred),
                        }
                    }

                    let optimized_left = if let Some(left_filter) = combine_predicates(left_preds) {
                        let base_left = self.plan(left)?;
                        OptimizedLogicalPlan::Filter {
                            input: Box::new(base_left),
                            predicate: left_filter,
                        }
                    } else {
                        self.plan(left)?
                    };

                    let optimized_right =
                        if let Some(right_filter) = combine_predicates(right_preds) {
                            let base_right = self.plan(right)?;
                            OptimizedLogicalPlan::Filter {
                                input: Box::new(base_right),
                                predicate: right_filter,
                            }
                        } else {
                            self.plan(right)?
                        };

                    let join_plan = if let Some(cond) = condition
                        && let Some((left_keys, right_keys)) =
                            extract_equi_join_keys(cond, left_schema_len)
                    {
                        OptimizedLogicalPlan::HashJoin {
                            left: Box::new(optimized_left),
                            right: Box::new(optimized_right),
                            join_type: *join_type,
                            left_keys,
                            right_keys,
                            schema: schema.clone(),
                        }
                    } else {
                        OptimizedLogicalPlan::NestedLoopJoin {
                            left: Box::new(optimized_left),
                            right: Box::new(optimized_right),
                            join_type: *join_type,
                            condition: condition.clone(),
                            schema: schema.clone(),
                        }
                    };

                    if let Some(post_filter) = combine_predicates(post_join_preds) {
                        Ok(OptimizedLogicalPlan::Filter {
                            input: Box::new(join_plan),
                            predicate: post_filter,
                        })
                    } else {
                        Ok(join_plan)
                    }
                }
                _ => {
                    let optimized_input = self.plan(input)?;
                    Ok(OptimizedLogicalPlan::Filter {
                        input: Box::new(optimized_input),
                        predicate: predicate.clone(),
                    })
                }
            },

            LogicalPlan::Project {
                input,
                expressions,
                schema,
            } => {
                let input = self.plan(input)?;
                Ok(OptimizedLogicalPlan::Project {
                    input: Box::new(input),
                    expressions: expressions.clone(),
                    schema: schema.clone(),
                })
            }

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                schema,
                grouping_sets,
            } => {
                let input = self.plan(input)?;
                Ok(OptimizedLogicalPlan::HashAggregate {
                    input: Box::new(input),
                    group_by: group_by.clone(),
                    aggregates: aggregates.clone(),
                    schema: schema.clone(),
                    grouping_sets: grouping_sets.clone(),
                })
            }

            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
                schema,
            } => {
                let left_schema_len = left.schema().fields.len();
                let optimized_left = self.plan(left)?;
                let optimized_right = self.plan(right)?;
                match join_type {
                    JoinType::Cross => Ok(OptimizedLogicalPlan::CrossJoin {
                        left: Box::new(optimized_left),
                        right: Box::new(optimized_right),
                        schema: schema.clone(),
                    }),
                    JoinType::Inner => {
                        if let Some(cond) = condition
                            && let Some((left_keys, right_keys)) =
                                extract_equi_join_keys(cond, left_schema_len)
                        {
                            return Ok(OptimizedLogicalPlan::HashJoin {
                                left: Box::new(optimized_left),
                                right: Box::new(optimized_right),
                                join_type: *join_type,
                                left_keys,
                                right_keys,
                                schema: schema.clone(),
                            });
                        }
                        Ok(OptimizedLogicalPlan::NestedLoopJoin {
                            left: Box::new(optimized_left),
                            right: Box::new(optimized_right),
                            join_type: *join_type,
                            condition: condition.clone(),
                            schema: schema.clone(),
                        })
                    }
                    JoinType::Left | JoinType::Right | JoinType::Full => {
                        Ok(OptimizedLogicalPlan::NestedLoopJoin {
                            left: Box::new(optimized_left),
                            right: Box::new(optimized_right),
                            join_type: *join_type,
                            condition: condition.clone(),
                            schema: schema.clone(),
                        })
                    }
                }
            }

            LogicalPlan::Sort { input, sort_exprs } => {
                let input = self.plan(input)?;
                Ok(OptimizedLogicalPlan::Sort {
                    input: Box::new(input),
                    sort_exprs: sort_exprs.clone(),
                })
            }

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => self.plan_limit(input, *limit, *offset),

            LogicalPlan::Distinct { input } => {
                let input = self.plan(input)?;
                Ok(OptimizedLogicalPlan::Distinct {
                    input: Box::new(input),
                })
            }

            LogicalPlan::Values { values, schema } => Ok(OptimizedLogicalPlan::Values {
                values: values.clone(),
                schema: schema.clone(),
            }),

            LogicalPlan::Empty { schema } => Ok(OptimizedLogicalPlan::Empty {
                schema: schema.clone(),
            }),

            LogicalPlan::SetOperation {
                left,
                right,
                op,
                all,
                schema,
            } => {
                let left = self.plan(left)?;
                let right = self.plan(right)?;
                match op {
                    SetOperationType::Union => Ok(OptimizedLogicalPlan::Union {
                        inputs: vec![left, right],
                        all: *all,
                        schema: schema.clone(),
                    }),
                    SetOperationType::Intersect => Ok(OptimizedLogicalPlan::Intersect {
                        left: Box::new(left),
                        right: Box::new(right),
                        all: *all,
                        schema: schema.clone(),
                    }),
                    SetOperationType::Except => Ok(OptimizedLogicalPlan::Except {
                        left: Box::new(left),
                        right: Box::new(right),
                        all: *all,
                        schema: schema.clone(),
                    }),
                }
            }

            LogicalPlan::Window {
                input,
                window_exprs,
                schema,
            } => {
                let input = self.plan(input)?;
                Ok(OptimizedLogicalPlan::Window {
                    input: Box::new(input),
                    window_exprs: window_exprs.clone(),
                    schema: schema.clone(),
                })
            }

            LogicalPlan::WithCte { ctes, body } => {
                let body = self.plan(body)?;
                let ctes = ctes
                    .iter()
                    .map(|cte| {
                        let query = self.plan(&cte.query)?;
                        Ok(yachtsql_ir::CteDefinition {
                            name: cte.name.clone(),
                            columns: cte.columns.clone(),
                            query: Box::new(query.into_logical()),
                            recursive: cte.recursive,
                            materialized: cte.materialized,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(OptimizedLogicalPlan::WithCte {
                    ctes,
                    body: Box::new(body),
                })
            }

            LogicalPlan::Unnest {
                input,
                columns,
                schema,
            } => {
                let input = self.plan(input)?;
                Ok(OptimizedLogicalPlan::Unnest {
                    input: Box::new(input),
                    columns: columns.clone(),
                    schema: schema.clone(),
                })
            }

            LogicalPlan::Qualify { input, predicate } => {
                let input = self.plan(input)?;
                Ok(OptimizedLogicalPlan::Qualify {
                    input: Box::new(input),
                    predicate: predicate.clone(),
                })
            }

            LogicalPlan::Insert {
                table_name,
                columns,
                source,
            } => {
                let source = self.plan(source)?;
                Ok(OptimizedLogicalPlan::Insert {
                    table_name: table_name.clone(),
                    columns: columns.clone(),
                    source: Box::new(source),
                })
            }

            LogicalPlan::Update {
                table_name,
                alias,
                assignments,
                from,
                filter,
            } => {
                let from_plan = match from {
                    Some(plan) => Some(Box::new(self.plan(plan)?)),
                    None => None,
                };
                Ok(OptimizedLogicalPlan::Update {
                    table_name: table_name.clone(),
                    alias: alias.clone(),
                    assignments: assignments.clone(),
                    from: from_plan,
                    filter: filter.clone(),
                })
            }

            LogicalPlan::Delete {
                table_name,
                alias,
                filter,
            } => Ok(OptimizedLogicalPlan::Delete {
                table_name: table_name.clone(),
                alias: alias.clone(),
                filter: filter.clone(),
            }),

            LogicalPlan::Merge {
                target_table,
                source,
                on,
                clauses,
            } => {
                let source = self.plan(source)?;
                Ok(OptimizedLogicalPlan::Merge {
                    target_table: target_table.clone(),
                    source: Box::new(source),
                    on: on.clone(),
                    clauses: clauses.clone(),
                })
            }

            LogicalPlan::CreateTable {
                table_name,
                columns,
                if_not_exists,
                or_replace,
                query,
            } => {
                let optimized_query = if let Some(q) = query {
                    Some(Box::new(self.plan(q)?))
                } else {
                    None
                };
                Ok(OptimizedLogicalPlan::CreateTable {
                    table_name: table_name.clone(),
                    columns: columns.clone(),
                    if_not_exists: *if_not_exists,
                    or_replace: *or_replace,
                    query: optimized_query,
                })
            }

            LogicalPlan::DropTable {
                table_names,
                if_exists,
            } => Ok(OptimizedLogicalPlan::DropTable {
                table_names: table_names.clone(),
                if_exists: *if_exists,
            }),

            LogicalPlan::AlterTable {
                table_name,
                operation,
                if_exists,
            } => Ok(OptimizedLogicalPlan::AlterTable {
                table_name: table_name.clone(),
                operation: operation.clone(),
                if_exists: *if_exists,
            }),

            LogicalPlan::Truncate { table_name } => Ok(OptimizedLogicalPlan::Truncate {
                table_name: table_name.clone(),
            }),

            LogicalPlan::CreateView {
                name,
                query,
                query_sql,
                column_aliases,
                or_replace,
                if_not_exists,
            } => {
                let query = self.plan(query)?;
                Ok(OptimizedLogicalPlan::CreateView {
                    name: name.clone(),
                    query: Box::new(query),
                    query_sql: query_sql.clone(),
                    column_aliases: column_aliases.clone(),
                    or_replace: *or_replace,
                    if_not_exists: *if_not_exists,
                })
            }

            LogicalPlan::DropView { name, if_exists } => Ok(OptimizedLogicalPlan::DropView {
                name: name.clone(),
                if_exists: *if_exists,
            }),

            LogicalPlan::CreateSchema {
                name,
                if_not_exists,
                or_replace,
            } => Ok(OptimizedLogicalPlan::CreateSchema {
                name: name.clone(),
                if_not_exists: *if_not_exists,
                or_replace: *or_replace,
            }),

            LogicalPlan::DropSchema {
                name,
                if_exists,
                cascade,
            } => Ok(OptimizedLogicalPlan::DropSchema {
                name: name.clone(),
                if_exists: *if_exists,
                cascade: *cascade,
            }),

            LogicalPlan::UndropSchema {
                name,
                if_not_exists,
            } => Ok(OptimizedLogicalPlan::UndropSchema {
                name: name.clone(),
                if_not_exists: *if_not_exists,
            }),

            LogicalPlan::AlterSchema { name, options } => Ok(OptimizedLogicalPlan::AlterSchema {
                name: name.clone(),
                options: options.clone(),
            }),

            LogicalPlan::CreateFunction {
                name,
                args,
                return_type,
                body,
                or_replace,
                if_not_exists,
                is_temp,
                is_aggregate,
            } => Ok(OptimizedLogicalPlan::CreateFunction {
                name: name.clone(),
                args: args.clone(),
                return_type: return_type.clone(),
                body: body.clone(),
                or_replace: *or_replace,
                if_not_exists: *if_not_exists,
                is_temp: *is_temp,
                is_aggregate: *is_aggregate,
            }),

            LogicalPlan::DropFunction { name, if_exists } => {
                Ok(OptimizedLogicalPlan::DropFunction {
                    name: name.clone(),
                    if_exists: *if_exists,
                })
            }

            LogicalPlan::CreateProcedure {
                name,
                args,
                body,
                or_replace,
                if_not_exists,
            } => {
                let body = body
                    .iter()
                    .map(|stmt| self.plan(stmt))
                    .collect::<Result<Vec<_>>>()?;
                Ok(OptimizedLogicalPlan::CreateProcedure {
                    name: name.clone(),
                    args: args.clone(),
                    body,
                    or_replace: *or_replace,
                    if_not_exists: *if_not_exists,
                })
            }

            LogicalPlan::DropProcedure { name, if_exists } => {
                Ok(OptimizedLogicalPlan::DropProcedure {
                    name: name.clone(),
                    if_exists: *if_exists,
                })
            }

            LogicalPlan::Call {
                procedure_name,
                args,
            } => Ok(OptimizedLogicalPlan::Call {
                procedure_name: procedure_name.clone(),
                args: args.clone(),
            }),

            LogicalPlan::ExportData { options, query } => {
                let query = self.plan(query)?;
                Ok(OptimizedLogicalPlan::ExportData {
                    options: options.clone(),
                    query: Box::new(query),
                })
            }

            LogicalPlan::LoadData {
                table_name,
                options,
                temp_table,
                temp_schema,
            } => Ok(OptimizedLogicalPlan::LoadData {
                table_name: table_name.clone(),
                options: options.clone(),
                temp_table: *temp_table,
                temp_schema: temp_schema.clone(),
            }),

            LogicalPlan::Declare {
                name,
                data_type,
                default,
            } => Ok(OptimizedLogicalPlan::Declare {
                name: name.clone(),
                data_type: data_type.clone(),
                default: default.clone(),
            }),

            LogicalPlan::SetVariable { name, value } => Ok(OptimizedLogicalPlan::SetVariable {
                name: name.clone(),
                value: value.clone(),
            }),

            LogicalPlan::SetMultipleVariables { names, value } => {
                Ok(OptimizedLogicalPlan::SetMultipleVariables {
                    names: names.clone(),
                    value: value.clone(),
                })
            }

            LogicalPlan::If {
                condition,
                then_branch,
                else_branch,
            } => {
                let then_branch = then_branch
                    .iter()
                    .map(|p| self.plan(p))
                    .collect::<Result<Vec<_>>>()?;
                let else_branch = else_branch
                    .as_ref()
                    .map(|branch| {
                        branch
                            .iter()
                            .map(|p| self.plan(p))
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()?;
                Ok(OptimizedLogicalPlan::If {
                    condition: condition.clone(),
                    then_branch,
                    else_branch,
                })
            }

            LogicalPlan::While {
                condition,
                body,
                label,
            } => {
                let body = body
                    .iter()
                    .map(|p| self.plan(p))
                    .collect::<Result<Vec<_>>>()?;
                Ok(OptimizedLogicalPlan::While {
                    condition: condition.clone(),
                    body,
                    label: label.clone(),
                })
            }

            LogicalPlan::Loop { body, label } => {
                let body = body
                    .iter()
                    .map(|p| self.plan(p))
                    .collect::<Result<Vec<_>>>()?;
                Ok(OptimizedLogicalPlan::Loop {
                    body,
                    label: label.clone(),
                })
            }

            LogicalPlan::Block { body, label } => {
                let body = body
                    .iter()
                    .map(|p| self.plan(p))
                    .collect::<Result<Vec<_>>>()?;
                Ok(OptimizedLogicalPlan::Block {
                    body,
                    label: label.clone(),
                })
            }

            LogicalPlan::Repeat {
                body,
                until_condition,
            } => {
                let body = body
                    .iter()
                    .map(|p| self.plan(p))
                    .collect::<Result<Vec<_>>>()?;
                Ok(OptimizedLogicalPlan::Repeat {
                    body,
                    until_condition: until_condition.clone(),
                })
            }

            LogicalPlan::For {
                variable,
                query,
                body,
            } => {
                let query = self.plan(query)?;
                let body = body
                    .iter()
                    .map(|p| self.plan(p))
                    .collect::<Result<Vec<_>>>()?;
                Ok(OptimizedLogicalPlan::For {
                    variable: variable.clone(),
                    query: Box::new(query),
                    body,
                })
            }

            LogicalPlan::Return { value } => Ok(OptimizedLogicalPlan::Return {
                value: value.clone(),
            }),

            LogicalPlan::Raise { message, level } => Ok(OptimizedLogicalPlan::Raise {
                message: message.clone(),
                level: *level,
            }),

            LogicalPlan::ExecuteImmediate {
                sql_expr,
                into_variables,
                using_params,
            } => Ok(OptimizedLogicalPlan::ExecuteImmediate {
                sql_expr: sql_expr.clone(),
                into_variables: into_variables.clone(),
                using_params: using_params.clone(),
            }),

            LogicalPlan::Break { label } => Ok(OptimizedLogicalPlan::Break {
                label: label.clone(),
            }),

            LogicalPlan::Continue { label } => Ok(OptimizedLogicalPlan::Continue {
                label: label.clone(),
            }),

            LogicalPlan::CreateSnapshot {
                snapshot_name,
                source_name,
                if_not_exists,
            } => Ok(OptimizedLogicalPlan::CreateSnapshot {
                snapshot_name: snapshot_name.clone(),
                source_name: source_name.clone(),
                if_not_exists: *if_not_exists,
            }),

            LogicalPlan::DropSnapshot {
                snapshot_name,
                if_exists,
            } => Ok(OptimizedLogicalPlan::DropSnapshot {
                snapshot_name: snapshot_name.clone(),
                if_exists: *if_exists,
            }),

            LogicalPlan::Assert { condition, message } => Ok(OptimizedLogicalPlan::Assert {
                condition: condition.clone(),
                message: message.clone(),
            }),

            LogicalPlan::Grant {
                roles,
                resource_type,
                resource_name,
                grantees,
            } => Ok(OptimizedLogicalPlan::Grant {
                roles: roles.clone(),
                resource_type: resource_type.clone(),
                resource_name: resource_name.clone(),
                grantees: grantees.clone(),
            }),

            LogicalPlan::Revoke {
                roles,
                resource_type,
                resource_name,
                grantees,
            } => Ok(OptimizedLogicalPlan::Revoke {
                roles: roles.clone(),
                resource_type: resource_type.clone(),
                resource_name: resource_name.clone(),
                grantees: grantees.clone(),
            }),

            LogicalPlan::BeginTransaction => Ok(OptimizedLogicalPlan::BeginTransaction),
            LogicalPlan::Commit => Ok(OptimizedLogicalPlan::Commit),
            LogicalPlan::Rollback => Ok(OptimizedLogicalPlan::Rollback),

            LogicalPlan::TryCatch {
                try_block,
                catch_block,
            } => {
                let try_block = try_block
                    .iter()
                    .map(|(p, sql)| Ok((self.plan(p)?, sql.clone())))
                    .collect::<Result<Vec<_>>>()?;
                let catch_block = catch_block
                    .iter()
                    .map(|p| self.plan(p))
                    .collect::<Result<Vec<_>>>()?;
                Ok(OptimizedLogicalPlan::TryCatch {
                    try_block,
                    catch_block,
                })
            }

            LogicalPlan::GapFill {
                input,
                ts_column,
                bucket_width,
                value_columns,
                partitioning_columns,
                origin,
                input_schema,
                schema,
            } => Ok(OptimizedLogicalPlan::GapFill {
                input: Box::new(self.plan(input)?),
                ts_column: ts_column.clone(),
                bucket_width: bucket_width.clone(),
                value_columns: value_columns.clone(),
                partitioning_columns: partitioning_columns.clone(),
                origin: origin.clone(),
                input_schema: input_schema.clone(),
                schema: schema.clone(),
            }),
        }
    }

    fn plan_limit(
        &self,
        input: &LogicalPlan,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<OptimizedLogicalPlan> {
        match (input, limit, offset) {
            (
                LogicalPlan::Sort {
                    input: sort_input,
                    sort_exprs,
                },
                Some(limit_val),
                None,
            ) => {
                let optimized_input = self.plan(sort_input)?;
                Ok(OptimizedLogicalPlan::TopN {
                    input: Box::new(optimized_input),
                    sort_exprs: sort_exprs.clone(),
                    limit: limit_val,
                })
            }
            _ => {
                let optimized_input = self.plan(input)?;
                Ok(OptimizedLogicalPlan::Limit {
                    input: Box::new(optimized_input),
                    limit,
                    offset,
                })
            }
        }
    }
}

impl Default for PhysicalPlanner {
    fn default() -> Self {
        Self::new()
    }
}
