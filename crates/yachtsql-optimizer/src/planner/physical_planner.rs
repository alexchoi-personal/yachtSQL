#![coverage(off)]

use yachtsql_common::error::Result;
use yachtsql_ir::{Expr, JoinType, LogicalPlan, SetOperationType, SortExpr};

use super::equi_join::{extract_equi_join_keys, extract_equi_join_keys_partial};
use super::predicate::{
    build_aggregate_output_to_input_map, can_push_through_aggregate, can_push_through_window,
    classify_join_condition_predicates, classify_predicates_for_join, combine_predicates,
    remap_predicate_indices, split_and_predicates,
};
use super::rule_based::project_merging::substitute_column_refs;
use crate::{ExecutionHints, PhysicalPlan};

fn is_simple_column_ref(expr: &Expr) -> bool {
    match expr {
        Expr::Column { .. } => true,
        Expr::Alias { expr: inner, .. } => is_simple_column_ref(inner),
        _ => false,
    }
}

fn all_simple_refs(expressions: &[Expr]) -> bool {
    expressions.iter().all(is_simple_column_ref)
}

fn remap_sort_exprs(sort_exprs: &[SortExpr], proj_exprs: &[Expr]) -> Vec<SortExpr> {
    sort_exprs
        .iter()
        .map(|se| SortExpr {
            expr: substitute_column_refs(&se.expr, proj_exprs),
            asc: se.asc,
            nulls_first: se.nulls_first,
        })
        .collect()
}

pub struct PhysicalPlanner {
    filter_pushdown_enabled: bool,
}

impl PhysicalPlanner {
    pub fn new() -> Self {
        Self {
            filter_pushdown_enabled: true,
        }
    }

    pub fn with_settings(filter_pushdown_enabled: bool) -> Self {
        Self {
            filter_pushdown_enabled,
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    pub fn plan(&self, logical: &LogicalPlan) -> Result<PhysicalPlan> {
        match logical {
            LogicalPlan::Scan {
                table_name,
                schema,
                projection,
            } => Ok(PhysicalPlan::TableScan {
                table_name: table_name.clone(),
                schema: schema.clone(),
                projection: projection.clone(),
                row_count: None,
            }),

            LogicalPlan::Sample {
                input,
                sample_type,
                sample_value,
            } => {
                let input = self.plan(input)?;
                Ok(PhysicalPlan::Sample {
                    input: Box::new(input),
                    sample_type: *sample_type,
                    sample_value: *sample_value,
                })
            }

            LogicalPlan::Filter { input, predicate } => {
                if !self.filter_pushdown_enabled {
                    let optimized_input = self.plan(input)?;
                    return Ok(PhysicalPlan::Filter {
                        input: Box::new(optimized_input),
                        predicate: predicate.clone(),
                    });
                }

                match input.as_ref() {
                    LogicalPlan::Join {
                        left,
                        right,
                        join_type,
                        condition,
                        schema,
                    } if matches!(
                        join_type,
                        JoinType::Inner | JoinType::Left | JoinType::Right
                    ) =>
                    {
                        let left_schema_len = left.schema().fields.len();
                        let predicates = split_and_predicates(predicate);

                        let (left_preds, right_preds, post_join_preds) =
                            classify_predicates_for_join(*join_type, &predicates, left_schema_len);

                        let optimized_left =
                            if let Some(left_filter) = combine_predicates(left_preds) {
                                let base_left = self.plan(left)?;
                                PhysicalPlan::Filter {
                                    input: Box::new(base_left),
                                    predicate: left_filter,
                                }
                            } else {
                                self.plan(left)?
                            };

                        let optimized_right =
                            if let Some(right_filter) = combine_predicates(right_preds) {
                                let base_right = self.plan(right)?;
                                PhysicalPlan::Filter {
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
                            PhysicalPlan::HashJoin {
                                left: Box::new(optimized_left),
                                right: Box::new(optimized_right),
                                join_type: *join_type,
                                left_keys,
                                right_keys,
                                schema: schema.clone(),
                                parallel: false,
                                hints: ExecutionHints::default(),
                            }
                        } else {
                            PhysicalPlan::NestedLoopJoin {
                                left: Box::new(optimized_left),
                                right: Box::new(optimized_right),
                                join_type: *join_type,
                                condition: condition.clone(),
                                schema: schema.clone(),
                                parallel: false,
                                hints: ExecutionHints::default(),
                            }
                        };

                        if let Some(post_filter) = combine_predicates(post_join_preds) {
                            Ok(PhysicalPlan::Filter {
                                input: Box::new(join_plan),
                                predicate: post_filter,
                            })
                        } else {
                            Ok(join_plan)
                        }
                    }

                    LogicalPlan::Distinct {
                        input: distinct_input,
                    } => {
                        let optimized_input = self.plan(distinct_input)?;
                        let filtered = PhysicalPlan::Filter {
                            input: Box::new(optimized_input),
                            predicate: predicate.clone(),
                        };
                        Ok(PhysicalPlan::Distinct {
                            input: Box::new(filtered),
                        })
                    }

                    LogicalPlan::Aggregate {
                        input: agg_input,
                        group_by,
                        aggregates,
                        schema,
                        grouping_sets,
                    } => {
                        let predicates = split_and_predicates(predicate);
                        let num_group_by_cols = group_by.len();
                        let output_to_input = build_aggregate_output_to_input_map(group_by);

                        let (pushable, post_agg): (Vec<_>, Vec<_>) = predicates
                            .into_iter()
                            .partition(|p| can_push_through_aggregate(p, num_group_by_cols));

                        let remapped_pushable: Vec<_> = pushable
                            .iter()
                            .filter_map(|p| remap_predicate_indices(p, &output_to_input))
                            .collect();

                        let optimized_input =
                            if let Some(push_filter) = combine_predicates(remapped_pushable) {
                                let base_input = self.plan(agg_input)?;
                                PhysicalPlan::Filter {
                                    input: Box::new(base_input),
                                    predicate: push_filter,
                                }
                            } else {
                                self.plan(agg_input)?
                            };

                        let agg_plan = PhysicalPlan::HashAggregate {
                            input: Box::new(optimized_input),
                            group_by: group_by.clone(),
                            aggregates: aggregates.clone(),
                            schema: schema.clone(),
                            grouping_sets: grouping_sets.clone(),
                            hints: ExecutionHints::default(),
                        };

                        if let Some(post_filter) = combine_predicates(post_agg) {
                            Ok(PhysicalPlan::Filter {
                                input: Box::new(agg_plan),
                                predicate: post_filter,
                            })
                        } else {
                            Ok(agg_plan)
                        }
                    }

                    LogicalPlan::Window {
                        input: window_input,
                        window_exprs,
                        schema,
                    } => {
                        let input_schema_len = window_input.schema().fields.len();
                        let predicates = split_and_predicates(predicate);

                        let (pushable, post_window): (Vec<_>, Vec<_>) = predicates
                            .into_iter()
                            .partition(|p| can_push_through_window(p, input_schema_len));

                        let optimized_input =
                            if let Some(push_filter) = combine_predicates(pushable) {
                                let base_input = self.plan(window_input)?;
                                PhysicalPlan::Filter {
                                    input: Box::new(base_input),
                                    predicate: push_filter,
                                }
                            } else {
                                self.plan(window_input)?
                            };

                        let window_plan = PhysicalPlan::Window {
                            input: Box::new(optimized_input),
                            window_exprs: window_exprs.clone(),
                            schema: schema.clone(),
                            hints: ExecutionHints::default(),
                        };

                        if let Some(post_filter) = combine_predicates(post_window) {
                            Ok(PhysicalPlan::Filter {
                                input: Box::new(window_plan),
                                predicate: post_filter,
                            })
                        } else {
                            Ok(window_plan)
                        }
                    }

                    _ => {
                        let optimized_input = self.plan(input)?;
                        Ok(PhysicalPlan::Filter {
                            input: Box::new(optimized_input),
                            predicate: predicate.clone(),
                        })
                    }
                }
            }

            LogicalPlan::Project {
                input,
                expressions,
                schema,
            } => {
                let input = self.plan(input)?;
                Ok(PhysicalPlan::Project {
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
                Ok(PhysicalPlan::HashAggregate {
                    input: Box::new(input),
                    group_by: group_by.clone(),
                    aggregates: aggregates.clone(),
                    schema: schema.clone(),
                    grouping_sets: grouping_sets.clone(),
                    hints: ExecutionHints::default(),
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

                match join_type {
                    JoinType::Cross => {
                        let optimized_left = self.plan(left)?;
                        let optimized_right = self.plan(right)?;
                        Ok(PhysicalPlan::CrossJoin {
                            left: Box::new(optimized_left),
                            right: Box::new(optimized_right),
                            schema: schema.clone(),
                            parallel: false,
                            hints: ExecutionHints::default(),
                        })
                    }
                    JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => self
                        .plan_equi_or_nested_join(
                            left,
                            right,
                            *join_type,
                            condition.as_ref(),
                            schema,
                            left_schema_len,
                        ),
                }
            }

            LogicalPlan::Sort { input, sort_exprs } => {
                let input = self.plan(input)?;
                Ok(PhysicalPlan::Sort {
                    input: Box::new(input),
                    sort_exprs: sort_exprs.clone(),
                    hints: ExecutionHints::default(),
                })
            }

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => self.plan_limit(input, *limit, *offset),

            LogicalPlan::Distinct { input } => {
                let input = self.plan(input)?;
                Ok(PhysicalPlan::Distinct {
                    input: Box::new(input),
                })
            }

            LogicalPlan::Values { values, schema } => Ok(PhysicalPlan::Values {
                values: values.clone(),
                schema: schema.clone(),
            }),

            LogicalPlan::Empty { schema } => Ok(PhysicalPlan::Empty {
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
                    SetOperationType::Union => Ok(PhysicalPlan::Union {
                        inputs: vec![left, right],
                        all: *all,
                        schema: schema.clone(),
                        parallel: false,
                        hints: ExecutionHints::default(),
                    }),
                    SetOperationType::Intersect => Ok(PhysicalPlan::Intersect {
                        left: Box::new(left),
                        right: Box::new(right),
                        all: *all,
                        schema: schema.clone(),
                        parallel: false,
                        hints: ExecutionHints::default(),
                    }),
                    SetOperationType::Except => Ok(PhysicalPlan::Except {
                        left: Box::new(left),
                        right: Box::new(right),
                        all: *all,
                        schema: schema.clone(),
                        parallel: false,
                        hints: ExecutionHints::default(),
                    }),
                }
            }

            LogicalPlan::Window {
                input,
                window_exprs,
                schema,
            } => {
                let input = self.plan(input)?;
                Ok(PhysicalPlan::Window {
                    input: Box::new(input),
                    window_exprs: window_exprs.clone(),
                    schema: schema.clone(),
                    hints: ExecutionHints::default(),
                })
            }

            LogicalPlan::WithCte { ctes, body } => {
                use super::cte_optimization::optimize_ctes;

                let (remaining_ctes, optimized_body) =
                    optimize_ctes(ctes.clone(), body.as_ref().clone());

                if remaining_ctes.is_empty() {
                    return self.plan(&optimized_body);
                }

                let optimized_body = self.plan(&optimized_body)?;
                let optimized_ctes = remaining_ctes
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
                Ok(PhysicalPlan::WithCte {
                    ctes: optimized_ctes,
                    body: Box::new(optimized_body),
                    parallel_ctes: Vec::new(),
                    hints: ExecutionHints::default(),
                })
            }

            LogicalPlan::Unnest {
                input,
                columns,
                schema,
            } => {
                let input = self.plan(input)?;
                Ok(PhysicalPlan::Unnest {
                    input: Box::new(input),
                    columns: columns.clone(),
                    schema: schema.clone(),
                })
            }

            LogicalPlan::Qualify { input, predicate } => {
                let input = self.plan(input)?;
                Ok(PhysicalPlan::Qualify {
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
                Ok(PhysicalPlan::Insert {
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
                Ok(PhysicalPlan::Update {
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
            } => Ok(PhysicalPlan::Delete {
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
                Ok(PhysicalPlan::Merge {
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
                Ok(PhysicalPlan::CreateTable {
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
            } => Ok(PhysicalPlan::DropTable {
                table_names: table_names.clone(),
                if_exists: *if_exists,
            }),

            LogicalPlan::AlterTable {
                table_name,
                operation,
                if_exists,
            } => Ok(PhysicalPlan::AlterTable {
                table_name: table_name.clone(),
                operation: operation.clone(),
                if_exists: *if_exists,
            }),

            LogicalPlan::Truncate { table_name } => Ok(PhysicalPlan::Truncate {
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
                Ok(PhysicalPlan::CreateView {
                    name: name.clone(),
                    query: Box::new(query),
                    query_sql: query_sql.clone(),
                    column_aliases: column_aliases.clone(),
                    or_replace: *or_replace,
                    if_not_exists: *if_not_exists,
                })
            }

            LogicalPlan::DropView { name, if_exists } => Ok(PhysicalPlan::DropView {
                name: name.clone(),
                if_exists: *if_exists,
            }),

            LogicalPlan::CreateSchema {
                name,
                if_not_exists,
                or_replace,
            } => Ok(PhysicalPlan::CreateSchema {
                name: name.clone(),
                if_not_exists: *if_not_exists,
                or_replace: *or_replace,
            }),

            LogicalPlan::DropSchema {
                name,
                if_exists,
                cascade,
            } => Ok(PhysicalPlan::DropSchema {
                name: name.clone(),
                if_exists: *if_exists,
                cascade: *cascade,
            }),

            LogicalPlan::UndropSchema {
                name,
                if_not_exists,
            } => Ok(PhysicalPlan::UndropSchema {
                name: name.clone(),
                if_not_exists: *if_not_exists,
            }),

            LogicalPlan::AlterSchema { name, options } => Ok(PhysicalPlan::AlterSchema {
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
            } => Ok(PhysicalPlan::CreateFunction {
                name: name.clone(),
                args: args.clone(),
                return_type: return_type.clone(),
                body: body.clone(),
                or_replace: *or_replace,
                if_not_exists: *if_not_exists,
                is_temp: *is_temp,
                is_aggregate: *is_aggregate,
            }),

            LogicalPlan::DropFunction { name, if_exists } => Ok(PhysicalPlan::DropFunction {
                name: name.clone(),
                if_exists: *if_exists,
            }),

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
                Ok(PhysicalPlan::CreateProcedure {
                    name: name.clone(),
                    args: args.clone(),
                    body,
                    or_replace: *or_replace,
                    if_not_exists: *if_not_exists,
                })
            }

            LogicalPlan::DropProcedure { name, if_exists } => Ok(PhysicalPlan::DropProcedure {
                name: name.clone(),
                if_exists: *if_exists,
            }),

            LogicalPlan::Call {
                procedure_name,
                args,
            } => Ok(PhysicalPlan::Call {
                procedure_name: procedure_name.clone(),
                args: args.clone(),
            }),

            LogicalPlan::ExportData { options, query } => {
                let query = self.plan(query)?;
                Ok(PhysicalPlan::ExportData {
                    options: options.clone(),
                    query: Box::new(query),
                })
            }

            LogicalPlan::LoadData {
                table_name,
                options,
                temp_table,
                temp_schema,
            } => Ok(PhysicalPlan::LoadData {
                table_name: table_name.clone(),
                options: options.clone(),
                temp_table: *temp_table,
                temp_schema: temp_schema.clone(),
            }),

            LogicalPlan::Declare {
                name,
                data_type,
                default,
            } => Ok(PhysicalPlan::Declare {
                name: name.clone(),
                data_type: data_type.clone(),
                default: default.clone(),
            }),

            LogicalPlan::SetVariable { name, value } => Ok(PhysicalPlan::SetVariable {
                name: name.clone(),
                value: value.clone(),
            }),

            LogicalPlan::SetMultipleVariables { names, value } => {
                Ok(PhysicalPlan::SetMultipleVariables {
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
                Ok(PhysicalPlan::If {
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
                Ok(PhysicalPlan::While {
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
                Ok(PhysicalPlan::Loop {
                    body,
                    label: label.clone(),
                })
            }

            LogicalPlan::Block { body, label } => {
                let body = body
                    .iter()
                    .map(|p| self.plan(p))
                    .collect::<Result<Vec<_>>>()?;
                Ok(PhysicalPlan::Block {
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
                Ok(PhysicalPlan::Repeat {
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
                Ok(PhysicalPlan::For {
                    variable: variable.clone(),
                    query: Box::new(query),
                    body,
                })
            }

            LogicalPlan::Return { value } => Ok(PhysicalPlan::Return {
                value: value.clone(),
            }),

            LogicalPlan::Raise { message, level } => Ok(PhysicalPlan::Raise {
                message: message.clone(),
                level: *level,
            }),

            LogicalPlan::ExecuteImmediate {
                sql_expr,
                into_variables,
                using_params,
            } => Ok(PhysicalPlan::ExecuteImmediate {
                sql_expr: sql_expr.clone(),
                into_variables: into_variables.clone(),
                using_params: using_params.clone(),
            }),

            LogicalPlan::Break { label } => Ok(PhysicalPlan::Break {
                label: label.clone(),
            }),

            LogicalPlan::Continue { label } => Ok(PhysicalPlan::Continue {
                label: label.clone(),
            }),

            LogicalPlan::CreateSnapshot {
                snapshot_name,
                source_name,
                if_not_exists,
            } => Ok(PhysicalPlan::CreateSnapshot {
                snapshot_name: snapshot_name.clone(),
                source_name: source_name.clone(),
                if_not_exists: *if_not_exists,
            }),

            LogicalPlan::DropSnapshot {
                snapshot_name,
                if_exists,
            } => Ok(PhysicalPlan::DropSnapshot {
                snapshot_name: snapshot_name.clone(),
                if_exists: *if_exists,
            }),

            LogicalPlan::Assert { condition, message } => Ok(PhysicalPlan::Assert {
                condition: condition.clone(),
                message: message.clone(),
            }),

            LogicalPlan::Grant {
                roles,
                resource_type,
                resource_name,
                grantees,
            } => Ok(PhysicalPlan::Grant {
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
            } => Ok(PhysicalPlan::Revoke {
                roles: roles.clone(),
                resource_type: resource_type.clone(),
                resource_name: resource_name.clone(),
                grantees: grantees.clone(),
            }),

            LogicalPlan::BeginTransaction => Ok(PhysicalPlan::BeginTransaction),
            LogicalPlan::Commit => Ok(PhysicalPlan::Commit),
            LogicalPlan::Rollback => Ok(PhysicalPlan::Rollback),

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
                Ok(PhysicalPlan::TryCatch {
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
            } => Ok(PhysicalPlan::GapFill {
                input: Box::new(self.plan(input)?),
                ts_column: ts_column.clone(),
                bucket_width: bucket_width.clone(),
                value_columns: value_columns.clone(),
                partitioning_columns: partitioning_columns.clone(),
                origin: origin.clone(),
                input_schema: input_schema.clone(),
                schema: schema.clone(),
            }),

            LogicalPlan::Explain { input, analyze } => {
                let logical_text = format!("{:#?}", input);
                let optimized_input = self.plan(input)?;
                let physical_text = format!("{:#?}", optimized_input);
                Ok(PhysicalPlan::Explain {
                    input: Box::new(optimized_input),
                    analyze: *analyze,
                    logical_plan_text: logical_text,
                    physical_plan_text: physical_text,
                })
            }
        }
    }

    fn plan_limit(
        &self,
        input: &LogicalPlan,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<PhysicalPlan> {
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
                Ok(PhysicalPlan::TopN {
                    input: Box::new(optimized_input),
                    sort_exprs: sort_exprs.clone(),
                    limit: limit_val,
                })
            }
            (
                LogicalPlan::Project {
                    input: proj_input,
                    expressions,
                    schema,
                },
                Some(limit_val),
                None,
            ) => {
                if let LogicalPlan::Sort {
                    input: sort_input,
                    sort_exprs,
                } = proj_input.as_ref()
                    && all_simple_refs(expressions)
                {
                    let remapped = remap_sort_exprs(sort_exprs, expressions);
                    let optimized_input = self.plan(sort_input)?;
                    return Ok(PhysicalPlan::Project {
                        input: Box::new(PhysicalPlan::TopN {
                            input: Box::new(optimized_input),
                            sort_exprs: remapped,
                            limit: limit_val,
                        }),
                        expressions: expressions.clone(),
                        schema: schema.clone(),
                    });
                }
                let optimized_input = self.plan(input)?;
                Ok(PhysicalPlan::Limit {
                    input: Box::new(optimized_input),
                    limit,
                    offset,
                })
            }
            _ => {
                let optimized_input = self.plan(input)?;
                Ok(PhysicalPlan::Limit {
                    input: Box::new(optimized_input),
                    limit,
                    offset,
                })
            }
        }
    }

    fn plan_equi_or_nested_join(
        &self,
        left: &LogicalPlan,
        right: &LogicalPlan,
        join_type: JoinType,
        condition: Option<&yachtsql_ir::Expr>,
        schema: &yachtsql_ir::PlanSchema,
        left_schema_len: usize,
    ) -> Result<PhysicalPlan> {
        let Some(cond) = condition else {
            let optimized_left = self.plan(left)?;
            let optimized_right = self.plan(right)?;
            return Ok(PhysicalPlan::NestedLoopJoin {
                left: Box::new(optimized_left),
                right: Box::new(optimized_right),
                join_type,
                condition: None,
                schema: schema.clone(),
                parallel: false,
                hints: ExecutionHints::default(),
            });
        };

        if let Some((left_keys, right_keys, remaining)) =
            extract_equi_join_keys_partial(cond, left_schema_len)
        {
            let (left_preds, right_preds, post_preds) =
                classify_join_condition_predicates(join_type, &remaining, left_schema_len);

            let optimized_left = if let Some(filter) = combine_predicates(left_preds) {
                let base_left = self.plan(left)?;
                PhysicalPlan::Filter {
                    input: Box::new(base_left),
                    predicate: filter,
                }
            } else {
                self.plan(left)?
            };

            let optimized_right = if let Some(filter) = combine_predicates(right_preds) {
                let base_right = self.plan(right)?;
                PhysicalPlan::Filter {
                    input: Box::new(base_right),
                    predicate: filter,
                }
            } else {
                self.plan(right)?
            };

            let hash_join = PhysicalPlan::HashJoin {
                left: Box::new(optimized_left),
                right: Box::new(optimized_right),
                join_type,
                left_keys,
                right_keys,
                schema: schema.clone(),
                parallel: false,
                hints: ExecutionHints::default(),
            };

            if let Some(post_filter) = combine_predicates(post_preds) {
                Ok(PhysicalPlan::Filter {
                    input: Box::new(hash_join),
                    predicate: post_filter,
                })
            } else {
                Ok(hash_join)
            }
        } else {
            let optimized_left = self.plan(left)?;
            let optimized_right = self.plan(right)?;
            Ok(PhysicalPlan::NestedLoopJoin {
                left: Box::new(optimized_left),
                right: Box::new(optimized_right),
                join_type,
                condition: Some(cond.clone()),
                schema: schema.clone(),
                parallel: false,
                hints: ExecutionHints::default(),
            })
        }
    }
}

impl Default for PhysicalPlanner {
    fn default() -> Self {
        Self::new()
    }
}
