#![coverage(off)]

mod cte;
mod ddl;
mod dml;
mod dql;
mod gap_fill;
mod io;
mod join;
mod scripting;
mod set_ops;
mod subquery;
mod unnest;
mod utils;

use std::sync::{Arc, RwLock};

use rustc_hash::FxHashMap;
use tracing::instrument;
pub(crate) use utils::{coerce_value, compare_values_for_sort, default_value_for_type};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, PlanSchema};
use yachtsql_storage::{Record, Schema, Table};

use crate::concurrent_catalog::{ConcurrentCatalog, TableLockSet};
use crate::concurrent_session::ConcurrentSession;
pub(crate) use crate::executor::plan_schema_to_schema;
use crate::plan::PhysicalPlan;
use crate::value_evaluator::{UserFunctionDef, ValueEvaluator};

#[derive(Clone)]
pub struct ConcurrentPlanExecutor {
    pub(crate) catalog: Arc<ConcurrentCatalog>,
    pub(crate) session: Arc<ConcurrentSession>,
    pub(crate) tables: Arc<TableLockSet>,
    pub(crate) variables: Arc<RwLock<FxHashMap<String, Value>>>,
    pub(crate) system_variables: Arc<RwLock<FxHashMap<String, Value>>>,
    pub(crate) cte_results: Arc<RwLock<FxHashMap<String, Table>>>,
    pub(crate) user_function_defs: Arc<RwLock<FxHashMap<String, UserFunctionDef>>>,
}

impl ConcurrentPlanExecutor {
    pub fn new(
        catalog: Arc<ConcurrentCatalog>,
        session: Arc<ConcurrentSession>,
        tables: TableLockSet,
    ) -> Self {
        let user_function_defs = catalog
            .get_functions()
            .iter()
            .map(|(name, func)| {
                (
                    name.clone(),
                    UserFunctionDef {
                        parameters: func.parameters.iter().map(|p| p.name.clone()).collect(),
                        body: func.body.clone(),
                    },
                )
            })
            .collect();

        let variables: FxHashMap<String, Value> = session
            .variables()
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect();

        let system_variables = session.system_variables().clone();

        Self {
            catalog,
            session,
            tables: Arc::new(tables),
            variables: Arc::new(RwLock::new(variables)),
            system_variables: Arc::new(RwLock::new(system_variables)),
            cte_results: Arc::new(RwLock::new(FxHashMap::default())),
            user_function_defs: Arc::new(RwLock::new(user_function_defs)),
        }
    }

    pub(crate) fn get_system_variables(
        &self,
    ) -> std::sync::RwLockReadGuard<'_, FxHashMap<String, Value>> {
        self.system_variables
            .read()
            .unwrap_or_else(|e| e.into_inner())
    }

    fn refresh_user_functions(&self) {
        let new_defs: FxHashMap<String, UserFunctionDef> = self
            .catalog
            .get_functions()
            .iter()
            .map(|(name, func)| {
                (
                    name.clone(),
                    UserFunctionDef {
                        parameters: func.parameters.iter().map(|p| p.name.clone()).collect(),
                        body: func.body.clone(),
                    },
                )
            })
            .collect();
        *self
            .user_function_defs
            .write()
            .unwrap_or_else(|e| e.into_inner()) = new_defs;
    }

    pub(crate) fn get_variables(&self) -> std::sync::RwLockReadGuard<'_, FxHashMap<String, Value>> {
        self.variables.read().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn is_parallel_enabled(&self) -> bool {
        if let Some(val) = self
            .variables
            .read()
            .ok()
            .and_then(|v| v.get("PARALLEL_EXECUTION").cloned())
        {
            return val.as_bool().unwrap_or(true);
        }
        if let Some(val) = self
            .system_variables
            .read()
            .ok()
            .and_then(|v| v.get("PARALLEL_EXECUTION").cloned())
        {
            return val.as_bool().unwrap_or(true);
        }
        true
    }

    pub(crate) fn get_user_functions(
        &self,
    ) -> std::sync::RwLockReadGuard<'_, FxHashMap<String, UserFunctionDef>> {
        self.user_function_defs
            .read()
            .unwrap_or_else(|e| e.into_inner())
    }

    pub fn execute(&self, plan: &PhysicalPlan) -> Result<Table> {
        self.execute_plan(plan)
    }

    #[instrument(skip(self))]
    pub fn execute_plan(&self, plan: &PhysicalPlan) -> Result<Table> {
        match plan {
            PhysicalPlan::TableScan {
                table_name, schema, ..
            } => self.execute_scan(table_name, schema),
            PhysicalPlan::Sample {
                input,
                sample_type,
                sample_value,
            } => self.execute_sample(input, sample_type, *sample_value),
            PhysicalPlan::Filter { input, predicate } => self.execute_filter(input, predicate),
            PhysicalPlan::Project {
                input,
                expressions,
                schema,
            } => self.execute_project(input, expressions, schema),
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
                schema,
                hints,
                ..
            } => self.execute_nested_loop_join(
                left,
                right,
                join_type,
                condition.as_ref(),
                schema,
                hints.parallel,
            ),
            PhysicalPlan::CrossJoin {
                left,
                right,
                schema,
                hints,
                ..
            } => self.execute_cross_join(left, right, schema, hints.parallel),
            PhysicalPlan::HashJoin {
                left,
                right,
                join_type,
                left_keys,
                right_keys,
                schema,
                hints,
                ..
            } => self.execute_hash_join(
                left,
                right,
                join_type,
                left_keys,
                right_keys,
                schema,
                hints.parallel,
            ),
            PhysicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
                schema,
                grouping_sets,
                hints,
            } => self.execute_aggregate(
                input,
                group_by,
                aggregates,
                schema,
                grouping_sets.as_ref(),
                hints.parallel,
            ),
            PhysicalPlan::Sort {
                input, sort_exprs, ..
            } => self.execute_sort(input, sort_exprs),
            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => self.execute_limit(input, *limit, *offset),
            PhysicalPlan::TopN {
                input,
                sort_exprs,
                limit,
            } => self.execute_topn(input, sort_exprs, *limit),
            PhysicalPlan::Distinct { input } => self.execute_distinct(input),
            PhysicalPlan::Union {
                inputs,
                all,
                schema,
                hints,
                ..
            } => self.execute_union(inputs, *all, schema, hints.parallel),
            PhysicalPlan::Intersect {
                left,
                right,
                all,
                schema,
                hints,
                ..
            } => self.execute_intersect(left, right, *all, schema, hints.parallel),
            PhysicalPlan::Except {
                left,
                right,
                all,
                schema,
                hints,
                ..
            } => self.execute_except(left, right, *all, schema, hints.parallel),
            PhysicalPlan::Window {
                input,
                window_exprs,
                schema,
                ..
            } => self.execute_window(input, window_exprs, schema),
            PhysicalPlan::WithCte {
                ctes,
                body,
                parallel_ctes,
                ..
            } => self.execute_cte(ctes, body, parallel_ctes),
            PhysicalPlan::Unnest {
                input,
                columns,
                schema,
            } => self.execute_unnest(input, columns, schema),
            PhysicalPlan::Qualify { input, predicate } => self.execute_qualify(input, predicate),
            PhysicalPlan::Values { values, schema } => self.execute_values(values, schema),
            PhysicalPlan::Empty { schema } => {
                let result_schema = plan_schema_to_schema(schema);
                let mut table = Table::empty(result_schema.clone());
                if result_schema.field_count() == 0 {
                    table.push_row(vec![])?;
                }
                Ok(table)
            }
            PhysicalPlan::Insert {
                table_name,
                columns,
                source,
            } => self.execute_insert(table_name, columns, source),
            PhysicalPlan::Update {
                table_name,
                alias,
                assignments,
                from,
                filter,
            } => self.execute_update(
                table_name,
                alias.as_deref(),
                assignments,
                from.as_deref(),
                filter.as_ref(),
            ),
            PhysicalPlan::Delete {
                table_name,
                alias,
                filter,
            } => self.execute_delete(table_name, alias.as_deref(), filter.as_ref()),
            PhysicalPlan::Merge {
                target_table,
                source,
                on,
                clauses,
            } => self.execute_merge(target_table, source, on, clauses),
            PhysicalPlan::CreateTable {
                table_name,
                columns,
                if_not_exists,
                or_replace,
                query,
            } => self.execute_create_table(
                table_name,
                columns,
                *if_not_exists,
                *or_replace,
                query.as_deref(),
            ),
            PhysicalPlan::DropTable {
                table_names,
                if_exists,
            } => self.execute_drop_tables(table_names, *if_exists),
            PhysicalPlan::AlterTable {
                table_name,
                operation,
                if_exists,
            } => self.execute_alter_table(table_name, operation, *if_exists),
            PhysicalPlan::Truncate { table_name } => self.execute_truncate(table_name),
            PhysicalPlan::CreateView {
                name,
                query: _,
                query_sql,
                column_aliases,
                or_replace,
                if_not_exists,
            } => self.execute_create_view(
                name,
                query_sql,
                column_aliases,
                *or_replace,
                *if_not_exists,
            ),
            PhysicalPlan::DropView { name, if_exists } => self.execute_drop_view(name, *if_exists),
            PhysicalPlan::CreateSchema {
                name,
                if_not_exists,
                or_replace,
            } => self.execute_create_schema(name, *if_not_exists, *or_replace),
            PhysicalPlan::DropSchema {
                name,
                if_exists,
                cascade,
            } => self.execute_drop_schema(name, *if_exists, *cascade),
            PhysicalPlan::UndropSchema {
                name,
                if_not_exists,
            } => self.execute_undrop_schema(name, *if_not_exists),
            PhysicalPlan::AlterSchema { name, options } => self.execute_alter_schema(name, options),
            PhysicalPlan::CreateFunction {
                name,
                args,
                return_type,
                body,
                or_replace,
                if_not_exists,
                is_temp,
                is_aggregate,
            } => self.execute_create_function(
                name,
                args,
                return_type,
                body,
                *or_replace,
                *if_not_exists,
                *is_temp,
                *is_aggregate,
            ),
            PhysicalPlan::DropFunction { name, if_exists } => {
                self.execute_drop_function(name, *if_exists)
            }
            PhysicalPlan::CreateProcedure {
                name,
                args,
                body,
                or_replace,
                if_not_exists,
            } => self.execute_create_procedure(name, args, body, *or_replace, *if_not_exists),
            PhysicalPlan::DropProcedure { name, if_exists } => {
                self.execute_drop_procedure(name, *if_exists)
            }
            PhysicalPlan::Call {
                procedure_name,
                args,
            } => self.execute_call(procedure_name, args),
            PhysicalPlan::ExportData { options, query } => self.execute_export(options, query),
            PhysicalPlan::LoadData {
                table_name,
                options,
                temp_table,
                temp_schema,
            } => self.execute_load(table_name, options, *temp_table, temp_schema.as_ref()),
            PhysicalPlan::Declare {
                name,
                data_type,
                default,
            } => self.execute_declare(name, data_type, default.as_ref()),
            PhysicalPlan::SetVariable { name, value } => self.execute_set_variable(name, value),
            PhysicalPlan::SetMultipleVariables { names, value } => {
                self.execute_set_multiple_variables(names, value)
            }
            PhysicalPlan::If {
                condition,
                then_branch,
                else_branch,
            } => self.execute_if(condition, then_branch, else_branch.as_deref()),
            PhysicalPlan::While {
                condition,
                body,
                label,
            } => self.execute_while(condition, body, label.as_deref()),
            PhysicalPlan::Loop { body, label } => self.execute_loop(body, label.as_deref()),
            PhysicalPlan::Block { body, label } => self.execute_block(body, label.as_deref()),
            PhysicalPlan::Repeat {
                body,
                until_condition,
            } => self.execute_repeat(body, until_condition),
            PhysicalPlan::For {
                variable,
                query,
                body,
            } => self.execute_for(variable, query, body),
            PhysicalPlan::Return { value: _ } => {
                Err(Error::InvalidQuery("RETURN outside of function".into()))
            }
            PhysicalPlan::Raise { message, level } => self.execute_raise(message.as_ref(), *level),
            PhysicalPlan::Break { label } => {
                let msg = match label {
                    Some(lbl) => format!("BREAK:{}", lbl),
                    None => "BREAK outside of loop".to_string(),
                };
                Err(Error::InvalidQuery(msg))
            }
            PhysicalPlan::Continue { label } => {
                let msg = match label {
                    Some(lbl) => format!("CONTINUE:{}", lbl),
                    None => "CONTINUE outside of loop".to_string(),
                };
                Err(Error::InvalidQuery(msg))
            }
            PhysicalPlan::CreateSnapshot {
                snapshot_name,
                source_name,
                if_not_exists,
            } => self.execute_create_snapshot(snapshot_name, source_name, *if_not_exists),
            PhysicalPlan::DropSnapshot {
                snapshot_name,
                if_exists,
            } => self.execute_drop_snapshot(snapshot_name, *if_exists),
            PhysicalPlan::Assert { condition, message } => {
                self.execute_assert(condition, message.as_ref())
            }
            PhysicalPlan::ExecuteImmediate {
                sql_expr,
                into_variables,
                using_params,
            } => self.execute_execute_immediate(sql_expr, into_variables, using_params),
            PhysicalPlan::Grant { .. } => Ok(Table::empty(Schema::new())),
            PhysicalPlan::Revoke { .. } => Ok(Table::empty(Schema::new())),
            PhysicalPlan::BeginTransaction => {
                self.catalog.begin_transaction()?;
                let locked_snapshots = self.tables.snapshot_write_locked_tables();
                for (name, table_data) in locked_snapshots {
                    self.catalog.snapshot_table(&name, table_data);
                }
                Ok(Table::empty(Schema::new()))
            }
            PhysicalPlan::Commit => {
                self.catalog.commit();
                Ok(Table::empty(Schema::new()))
            }
            PhysicalPlan::Rollback => {
                self.rollback_transaction();
                Ok(Table::empty(Schema::new()))
            }
            PhysicalPlan::TryCatch {
                try_block,
                catch_block,
            } => self.execute_try_catch(try_block, catch_block),
            PhysicalPlan::GapFill {
                input,
                ts_column,
                bucket_width,
                value_columns,
                partitioning_columns,
                origin,
                input_schema,
                schema,
            } => self.execute_gap_fill(
                input,
                ts_column,
                bucket_width,
                value_columns,
                partitioning_columns,
                origin.as_ref(),
                input_schema,
                schema,
            ),
            PhysicalPlan::Explain {
                logical_plan_text,
                physical_plan_text,
                analyze,
                input,
            } => self.execute_explain(logical_plan_text, physical_plan_text, *analyze, input),
        }
    }

    fn execute_explain(
        &self,
        logical_plan_text: &str,
        physical_plan_text: &str,
        analyze: bool,
        input: &PhysicalPlan,
    ) -> Result<Table> {
        use yachtsql_common::types::DataType;
        use yachtsql_storage::Field;

        let schema = Schema::from_fields(vec![
            Field::nullable("plan_type", DataType::String),
            Field::nullable("plan", DataType::String),
        ]);

        if analyze {
            let start = std::time::Instant::now();
            let result = self.execute_plan(input)?;
            let elapsed = start.elapsed();

            let records = vec![
                Record::from_values(vec![
                    Value::String("logical".to_string()),
                    Value::String(logical_plan_text.to_string()),
                ]),
                Record::from_values(vec![
                    Value::String("physical".to_string()),
                    Value::String(physical_plan_text.to_string()),
                ]),
                Record::from_values(vec![
                    Value::String("execution_time".to_string()),
                    Value::String(format!("{:?}", elapsed)),
                ]),
                Record::from_values(vec![
                    Value::String("rows_returned".to_string()),
                    Value::String(result.row_count().to_string()),
                ]),
            ];

            Table::from_records(schema, records)
        } else {
            let records = vec![
                Record::from_values(vec![
                    Value::String("logical".to_string()),
                    Value::String(logical_plan_text.to_string()),
                ]),
                Record::from_values(vec![
                    Value::String("physical".to_string()),
                    Value::String(physical_plan_text.to_string()),
                ]),
            ];

            Table::from_records(schema, records)
        }
    }

    fn execute_assert(&self, condition: &Expr, message: Option<&Expr>) -> Result<Table> {
        let empty_schema = Schema::new();
        let empty_record = Record::new();

        let result = if Self::expr_contains_subquery(condition) {
            self.eval_expr_with_subqueries(condition, &empty_schema, &empty_record)?
        } else {
            let vars = self.get_variables();
            let sys_vars = self.get_system_variables();
            let udf = self.get_user_functions();
            let evaluator = ValueEvaluator::new(&empty_schema)
                .with_variables(&vars)
                .with_system_variables(&sys_vars)
                .with_user_functions(&udf);
            evaluator.evaluate(condition, &empty_record)?
        };

        match result {
            Value::Bool(true) => Ok(Table::empty(Schema::new())),
            Value::Bool(false) => {
                let msg = if let Some(msg_expr) = message {
                    let vars = self.get_variables();
                    let sys_vars = self.get_system_variables();
                    let udf = self.get_user_functions();
                    let evaluator = ValueEvaluator::new(&empty_schema)
                        .with_variables(&vars)
                        .with_system_variables(&sys_vars)
                        .with_user_functions(&udf);
                    let msg_val = evaluator.evaluate(msg_expr, &empty_record)?;
                    match msg_val {
                        Value::String(s) => s,
                        _ => format!("{:?}", msg_val),
                    }
                } else {
                    "Assertion failed".to_string()
                };
                Err(Error::InvalidQuery(format!("ASSERT failed: {}", msg)))
            }
            _ => Err(Error::InvalidQuery(
                "ASSERT condition must evaluate to a boolean".into(),
            )),
        }
    }
}
