use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, IntervalUnit, Schema as ArrowSchema, TimeUnit,
};
use datafusion::catalog_common::MemorySchemaProvider;
use datafusion::common::{TableReference, ToDFSchema};
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::logical_expr::{
    EmptyRelation, Expr as DFExpr, JoinType as DFJoinType, LogicalPlan as DFLogicalPlan,
    LogicalPlanBuilder, SortExpr as DFSortExpr,
};
use datafusion::prelude::*;
use futures::FutureExt;
use parking_lot::RwLock;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Field, FieldMode, Schema};
use yachtsql_ir::plan::{AlterTableOp, FunctionBody};
use yachtsql_ir::{JoinType, LogicalPlan, PlanSchema, SetOperationType, SortExpr};
use yachtsql_parser::{CatalogProvider, FunctionDefinition, ViewDefinition};

pub struct YachtSQLSession {
    ctx: SessionContext,
    views: RwLock<HashMap<String, ViewDefinition>>,
    functions: RwLock<HashMap<String, FunctionDefinition>>,
    schemas: RwLock<HashSet<String>>,
}

struct SessionCatalog<'a> {
    session: &'a YachtSQLSession,
}

impl<'a> CatalogProvider for SessionCatalog<'a> {
    fn get_table_schema(&self, name: &str) -> Option<Schema> {
        let (schema_name, table) = YachtSQLSession::parse_table_name(name);

        if let Some(ref schema) = schema_name {
            let catalog = self.session.ctx.catalog("datafusion")?;
            let schema_provider = catalog.schema(schema)?;
            let table_provider = schema_provider.table(&table).now_or_never()?.ok()??;
            let arrow_schema = table_provider.schema();
            return Some(arrow_schema_to_yachtsql(&arrow_schema));
        }

        let table_ref = YachtSQLSession::table_reference(schema_name.as_deref(), &table);
        let provider = self
            .session
            .ctx
            .table_provider(table_ref)
            .now_or_never()?
            .ok()?;
        let arrow_schema = provider.schema();
        Some(arrow_schema_to_yachtsql(&arrow_schema))
    }

    fn get_view(&self, name: &str) -> Option<ViewDefinition> {
        self.session.views.read().get(&name.to_lowercase()).cloned()
    }

    fn get_function(&self, name: &str) -> Option<FunctionDefinition> {
        self.session
            .functions
            .read()
            .get(&name.to_lowercase())
            .cloned()
    }
}

fn arrow_schema_to_yachtsql(schema: &ArrowSchema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|f| {
            let mode = if f.is_nullable() {
                FieldMode::Nullable
            } else {
                FieldMode::Required
            };
            Field::new(
                f.name().clone(),
                arrow_type_to_yachtsql(f.data_type()),
                mode,
            )
        })
        .collect();
    Schema::from_fields(fields)
}

impl YachtSQLSession {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
            views: RwLock::new(HashMap::new()),
            functions: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashSet::new()),
        }
    }

    pub fn with_config(config: SessionConfig) -> Self {
        Self {
            ctx: SessionContext::new_with_config(config),
            views: RwLock::new(HashMap::new()),
            functions: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashSet::new()),
        }
    }

    fn parse_table_name(name: &str) -> (Option<String>, String) {
        let lower = name.to_lowercase();
        if let Some(dot_idx) = lower.find('.') {
            let (schema, table) = lower.split_at(dot_idx);
            (Some(schema.to_string()), table[1..].to_string())
        } else {
            (None, lower)
        }
    }

    fn table_reference(schema: Option<&str>, table: &str) -> TableReference {
        match schema {
            Some(s) => TableReference::partial(s.to_owned(), table.to_owned()),
            None => TableReference::bare(table.to_owned()),
        }
    }

    pub async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let catalog = SessionCatalog { session: self };
        let plan = yachtsql_parser::parse_and_plan(sql, &catalog)?;
        self.execute_plan(&plan).await
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    async fn execute_plan(&self, plan: &LogicalPlan) -> Result<Vec<RecordBatch>> {
        match plan {
            LogicalPlan::Scan { .. }
            | LogicalPlan::Filter { .. }
            | LogicalPlan::Project { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Distinct { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::Empty { .. }
            | LogicalPlan::SetOperation { .. }
            | LogicalPlan::Window { .. }
            | LogicalPlan::Qualify { .. }
            | LogicalPlan::WithCte { .. }
            | LogicalPlan::Sample { .. } => self.execute_query(plan).await,

            LogicalPlan::CreateTable {
                table_name,
                columns,
                if_not_exists,
                or_replace,
                query,
            } => {
                self.execute_create_table(
                    table_name,
                    columns,
                    *if_not_exists,
                    *or_replace,
                    query.as_deref(),
                )
                .await
            }

            LogicalPlan::DropTable {
                table_names,
                if_exists,
            } => self.execute_drop_table(table_names, *if_exists).await,

            LogicalPlan::Insert {
                table_name,
                columns,
                source,
            } => self.execute_insert(table_name, columns, source).await,

            LogicalPlan::Update {
                table_name,
                alias,
                assignments,
                from,
                filter,
            } => {
                self.execute_update(
                    table_name,
                    alias.as_deref(),
                    assignments,
                    from.as_deref(),
                    filter.as_ref(),
                )
                .await
            }

            LogicalPlan::Delete {
                table_name,
                alias,
                filter,
            } => {
                self.execute_delete(table_name, alias.as_deref(), filter.as_ref())
                    .await
            }

            LogicalPlan::Truncate { table_name } => self.execute_truncate(table_name).await,

            LogicalPlan::AlterTable {
                table_name,
                operation,
                if_exists,
            } => {
                self.execute_alter_table(table_name, operation, *if_exists)
                    .await
            }

            LogicalPlan::CreateView {
                name,
                query_sql,
                column_aliases,
                or_replace,
                if_not_exists,
                ..
            } => {
                self.execute_create_view(
                    name,
                    query_sql,
                    column_aliases,
                    *or_replace,
                    *if_not_exists,
                )
                .await
            }

            LogicalPlan::DropView { name, if_exists } => {
                self.execute_drop_view(name, *if_exists).await
            }

            LogicalPlan::CreateSchema {
                name,
                if_not_exists,
                ..
            } => self.execute_create_schema(name, *if_not_exists).await,

            LogicalPlan::DropSchema {
                name,
                if_exists,
                cascade,
            } => self.execute_drop_schema(name, *if_exists, *cascade).await,

            LogicalPlan::CreateFunction {
                name,
                args,
                return_type,
                body,
                or_replace,
                if_not_exists,
                is_aggregate,
                ..
            } => {
                self.execute_create_function(
                    name,
                    args,
                    return_type,
                    body,
                    *or_replace,
                    *if_not_exists,
                    *is_aggregate,
                )
                .await
            }

            LogicalPlan::DropFunction { name, if_exists } => {
                self.execute_drop_function(name, *if_exists).await
            }

            LogicalPlan::Explain { input, analyze } => self.execute_explain(input, *analyze).await,

            LogicalPlan::BeginTransaction | LogicalPlan::Commit | LogicalPlan::Rollback => {
                Ok(vec![])
            }

            _ => Err(Error::internal(format!(
                "Plan execution not implemented: {:?}",
                std::mem::discriminant(plan)
            ))),
        }
    }

    async fn execute_query(&self, plan: &LogicalPlan) -> Result<Vec<RecordBatch>> {
        if let LogicalPlan::WithCte { ctes, body } = plan {
            for cte in ctes {
                let cte_batches = Box::pin(self.execute_query(&cte.query)).await?;
                if !cte_batches.is_empty() {
                    let schema = cte_batches[0].schema();
                    let mem_table = MemTable::try_new(schema, vec![cte_batches])
                        .map_err(|e| Error::internal(e.to_string()))?;
                    let _ = self
                        .ctx
                        .register_table(cte.name.to_lowercase(), Arc::new(mem_table));
                }
            }
            return Box::pin(self.execute_query(body)).await;
        }

        let df_plan = self.convert_plan(plan)?;
        let df = DataFrame::new(self.ctx.state(), df_plan);
        df.collect()
            .await
            .map_err(|e| Error::internal(e.to_string()))
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    fn convert_plan(&self, plan: &LogicalPlan) -> Result<DFLogicalPlan> {
        match plan {
            LogicalPlan::Scan {
                table_name,
                schema,
                projection,
            } => {
                let (schema_name, table) = Self::parse_table_name(table_name);
                let table_ref = Self::table_reference(schema_name.as_deref(), &table);
                let provider = self
                    .ctx
                    .table_provider(table_ref)
                    .now_or_never()
                    .ok_or_else(|| Error::internal(format!("Table not found: {}", table_name)))?
                    .map_err(|e| Error::internal(e.to_string()))?;

                let source = provider_as_source(provider);
                let proj_cols: Option<Vec<usize>> = projection.clone();

                let scan_alias = schema
                    .fields
                    .first()
                    .and_then(|f| f.table.as_ref())
                    .map(|t| t.to_lowercase())
                    .unwrap_or(table);

                LogicalPlanBuilder::scan(scan_alias, source, proj_cols)
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::Sample {
                input,
                sample_value,
                ..
            } => {
                let input_plan = self.convert_plan(input)?;
                LogicalPlanBuilder::from(input_plan)
                    .limit(0, Some(*sample_value as usize))
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::Filter { input, predicate } => {
                let input_plan = self.convert_plan(input)?;
                let predicate_expr = self.convert_expr(predicate)?;
                LogicalPlanBuilder::from(input_plan)
                    .filter(predicate_expr)
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::Project {
                input, expressions, ..
            } => {
                let input_plan = self.convert_plan(input)?;
                let project_exprs: Vec<DFExpr> = expressions
                    .iter()
                    .map(|e| self.convert_expr(e))
                    .collect::<Result<_>>()?;
                LogicalPlanBuilder::from(input_plan)
                    .project(project_exprs)
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                ..
            } => {
                let input_plan = self.convert_plan(input)?;
                let group_exprs: Vec<DFExpr> = group_by
                    .iter()
                    .map(|e| self.convert_expr(e))
                    .collect::<Result<_>>()?;
                let agg_exprs: Vec<DFExpr> = aggregates
                    .iter()
                    .map(|e| self.convert_expr(e))
                    .collect::<Result<_>>()?;
                LogicalPlanBuilder::from(input_plan)
                    .aggregate(group_exprs, agg_exprs)
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
                ..
            } => {
                let left_plan = self.convert_plan(left)?;
                let right_plan = self.convert_plan(right)?;
                let df_join_type = convert_join_type(join_type);

                let mut builder = LogicalPlanBuilder::from(left_plan);
                match condition {
                    Some(cond) => {
                        let cond_expr = self.convert_expr(cond)?;
                        builder = builder
                            .join_on(right_plan, df_join_type, vec![cond_expr])
                            .map_err(|e| Error::internal(e.to_string()))?;
                    }
                    None => {
                        builder = builder
                            .cross_join(right_plan)
                            .map_err(|e| Error::internal(e.to_string()))?;
                    }
                }
                builder.build().map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::Sort { input, sort_exprs } => {
                let input_plan = self.convert_plan(input)?;
                let df_sort_exprs: Vec<DFSortExpr> = sort_exprs
                    .iter()
                    .map(|se| self.convert_sort_expr(se))
                    .collect::<Result<_>>()?;
                LogicalPlanBuilder::from(input_plan)
                    .sort(df_sort_exprs)
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let input_plan = self.convert_plan(input)?;
                let skip = offset.unwrap_or(0);
                let fetch = *limit;
                LogicalPlanBuilder::from(input_plan)
                    .limit(skip, fetch)
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::Distinct { input } => {
                let input_plan = self.convert_plan(input)?;
                LogicalPlanBuilder::from(input_plan)
                    .distinct()
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::Values { values, .. } => {
                let df_values: Vec<Vec<DFExpr>> = values
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|e| self.convert_expr(e))
                            .collect::<Result<_>>()
                    })
                    .collect::<Result<_>>()?;
                LogicalPlanBuilder::values(df_values)
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::Empty { schema } => {
                let arrow_schema = convert_plan_schema(schema);
                Ok(DFLogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: arrow_schema
                        .to_dfschema_ref()
                        .map_err(|e| Error::internal(e.to_string()))?,
                }))
            }

            LogicalPlan::SetOperation {
                left,
                right,
                op,
                all,
                ..
            } => {
                let left_plan = self.convert_plan(left)?;
                let right_plan = self.convert_plan(right)?;

                match op {
                    SetOperationType::Union => {
                        if *all {
                            LogicalPlanBuilder::from(left_plan)
                                .union(right_plan)
                                .map_err(|e| Error::internal(e.to_string()))?
                                .build()
                                .map_err(|e| Error::internal(e.to_string()))
                        } else {
                            LogicalPlanBuilder::from(left_plan)
                                .union_distinct(right_plan)
                                .map_err(|e| Error::internal(e.to_string()))?
                                .build()
                                .map_err(|e| Error::internal(e.to_string()))
                        }
                    }
                    SetOperationType::Intersect => {
                        LogicalPlanBuilder::intersect(left_plan, right_plan, *all)
                            .map_err(|e| Error::internal(e.to_string()))
                    }
                    SetOperationType::Except => {
                        LogicalPlanBuilder::except(left_plan, right_plan, *all)
                            .map_err(|e| Error::internal(e.to_string()))
                    }
                }
            }

            LogicalPlan::Window {
                input,
                window_exprs,
                ..
            } => {
                let input_plan = self.convert_plan(input)?;
                let df_window_exprs: Vec<DFExpr> = window_exprs
                    .iter()
                    .map(|e| self.convert_expr(e))
                    .collect::<Result<_>>()?;
                LogicalPlanBuilder::from(input_plan)
                    .window(df_window_exprs)
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::Qualify { input, predicate } => {
                let input_plan = self.convert_plan(input)?;
                let predicate_expr = self.convert_expr(predicate)?;
                LogicalPlanBuilder::from(input_plan)
                    .filter(predicate_expr)
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::WithCte { body, .. } => self.convert_plan(body),

            _ => Err(Error::internal(format!(
                "Query conversion not implemented: {:?}",
                std::mem::discriminant(plan)
            ))),
        }
    }

    fn convert_expr(&self, expr: &yachtsql_ir::Expr) -> Result<DFExpr> {
        yachtsql_parser::DataFusionConverter::convert_expr(expr)
            .map_err(|e| Error::internal(e.to_string()))
    }

    fn convert_sort_expr(&self, se: &SortExpr) -> Result<DFSortExpr> {
        let expr = self.convert_expr(&se.expr)?;
        Ok(DFSortExpr {
            expr,
            asc: se.asc,
            nulls_first: se.nulls_first,
        })
    }

    async fn execute_create_table(
        &self,
        table_name: &str,
        columns: &[yachtsql_ir::ColumnDef],
        if_not_exists: bool,
        or_replace: bool,
        query: Option<&LogicalPlan>,
    ) -> Result<Vec<RecordBatch>> {
        let (schema_name, table) = Self::parse_table_name(table_name);
        let table_ref = Self::table_reference(schema_name.as_deref(), &table);

        let existing = self.ctx.table_provider(table_ref.clone()).now_or_never();
        if existing.is_some() && existing.unwrap().is_ok() {
            if if_not_exists {
                return Ok(vec![]);
            }
            if !or_replace {
                return Err(Error::internal(format!(
                    "Table {} already exists",
                    table_name
                )));
            }
            if let Some(ref schema) = schema_name {
                let catalog = self
                    .ctx
                    .catalog("datafusion")
                    .ok_or_else(|| Error::internal("Default catalog 'datafusion' not found"))?;
                if let Some(schema_provider) = catalog.schema(schema) {
                    let _ = schema_provider.deregister_table(&table);
                }
            } else {
                let _ = self.ctx.deregister_table(&table);
            }
        }

        let (arrow_schema, batches) = match query {
            Some(q) => {
                let result = self.execute_query(q).await?;
                let arrow_schema = if result.is_empty() {
                    let fields: Vec<ArrowField> = columns
                        .iter()
                        .map(|c| {
                            ArrowField::new(
                                &c.name,
                                yachtsql_type_to_arrow(&c.data_type),
                                c.nullable,
                            )
                        })
                        .collect();
                    Arc::new(ArrowSchema::new(fields))
                } else {
                    result[0].schema()
                };
                (arrow_schema, result)
            }
            None => {
                let fields: Vec<ArrowField> = columns
                    .iter()
                    .map(|c| {
                        ArrowField::new(&c.name, yachtsql_type_to_arrow(&c.data_type), c.nullable)
                    })
                    .collect();
                let arrow_schema = Arc::new(ArrowSchema::new(fields));
                (arrow_schema, vec![])
            }
        };

        let partitions = if batches.is_empty() {
            vec![]
        } else {
            vec![batches]
        };

        let mem_table = MemTable::try_new(arrow_schema, partitions)
            .map_err(|e| Error::internal(e.to_string()))?;

        if let Some(ref schema) = schema_name {
            let catalog = self
                .ctx
                .catalog("datafusion")
                .ok_or_else(|| Error::internal("Default catalog 'datafusion' not found"))?;
            let schema_provider = catalog
                .schema(schema)
                .ok_or_else(|| Error::internal(format!("Schema not found: {}", schema)))?;
            schema_provider
                .register_table(table.clone(), Arc::new(mem_table))
                .map_err(|e| Error::internal(e.to_string()))?;
        } else {
            self.ctx
                .register_table(&table, Arc::new(mem_table))
                .map_err(|e| Error::internal(e.to_string()))?;
        }

        Ok(vec![])
    }

    async fn execute_drop_table(
        &self,
        table_names: &[String],
        if_exists: bool,
    ) -> Result<Vec<RecordBatch>> {
        for table_name in table_names {
            let (schema_name, table) = Self::parse_table_name(table_name);

            let dropped = if let Some(ref schema) = schema_name {
                let catalog = self
                    .ctx
                    .catalog("datafusion")
                    .ok_or_else(|| Error::internal("Default catalog 'datafusion' not found"))?;
                match catalog.schema(schema) {
                    Some(schema_provider) => schema_provider
                        .deregister_table(&table)
                        .map_err(|e| Error::internal(e.to_string()))?
                        .is_some(),
                    None => false,
                }
            } else {
                self.ctx
                    .deregister_table(&table)
                    .map_err(|e| Error::internal(e.to_string()))?
                    .is_some()
            };

            if !dropped && !if_exists {
                return Err(Error::internal(format!(
                    "Table {} does not exist",
                    table_name
                )));
            }
        }
        Ok(vec![])
    }

    async fn execute_insert(
        &self,
        table_name: &str,
        _columns: &[String],
        source: &LogicalPlan,
    ) -> Result<Vec<RecordBatch>> {
        let (schema_name, table) = Self::parse_table_name(table_name);
        let new_batches = self.execute_query(source).await?;

        let (_provider, table_schema) = if let Some(ref schema) = schema_name {
            let catalog = self
                .ctx
                .catalog("datafusion")
                .ok_or_else(|| Error::internal("Default catalog 'datafusion' not found"))?;
            let schema_provider = catalog
                .schema(schema)
                .ok_or_else(|| Error::internal(format!("Schema not found: {}", schema)))?;
            let provider = schema_provider
                .table(&table)
                .now_or_never()
                .ok_or_else(|| Error::internal(format!("Table not found: {}", table_name)))?
                .map_err(|e| Error::internal(e.to_string()))?
                .ok_or_else(|| Error::internal(format!("Table not found: {}", table_name)))?;
            let table_schema = provider.schema();
            (provider, table_schema)
        } else {
            let table_ref = Self::table_reference(schema_name.as_deref(), &table);
            let provider = self
                .ctx
                .table_provider(table_ref)
                .now_or_never()
                .ok_or_else(|| Error::internal(format!("Table not found: {}", table_name)))?
                .map_err(|e| Error::internal(e.to_string()))?;
            let table_schema = provider.schema();
            (provider, table_schema)
        };

        let existing_batches = if let Some(ref schema) = schema_name {
            let catalog = self.ctx.catalog("datafusion").unwrap();
            let schema_provider = catalog.schema(schema).unwrap();
            let table_provider = schema_provider
                .table(&table)
                .now_or_never()
                .unwrap()
                .unwrap()
                .unwrap();
            let scan = table_provider
                .scan(&self.ctx.state(), None, &[], None)
                .now_or_never()
                .ok_or_else(|| Error::internal("Scan failed"))?
                .map_err(|e| Error::internal(e.to_string()))?;
            datafusion::physical_plan::collect(scan, self.ctx.task_ctx())
                .now_or_never()
                .ok_or_else(|| Error::internal("Collection failed"))?
                .map_err(|e| Error::internal(e.to_string()))?
        } else {
            let existing_df = self
                .ctx
                .table(TableReference::bare(table.clone()))
                .now_or_never()
                .ok_or_else(|| Error::internal("Table read failed"))?
                .map_err(|e| Error::internal(e.to_string()))?;
            existing_df
                .collect()
                .now_or_never()
                .ok_or_else(|| Error::internal("Collection failed"))?
                .map_err(|e| Error::internal(e.to_string()))?
        };

        let casted_batches: Vec<RecordBatch> = new_batches
            .into_iter()
            .map(|batch| cast_batch_to_schema(&batch, &table_schema))
            .collect::<Result<_>>()?;

        let mut all_batches = existing_batches;
        all_batches.extend(casted_batches);

        if let Some(ref schema) = schema_name {
            let catalog = self.ctx.catalog("datafusion").unwrap();
            let schema_provider = catalog.schema(schema).unwrap();
            let _ = schema_provider.deregister_table(&table);

            let partitions = if all_batches.is_empty() {
                vec![]
            } else {
                vec![all_batches]
            };

            let mem_table = MemTable::try_new(table_schema, partitions)
                .map_err(|e| Error::internal(e.to_string()))?;
            schema_provider
                .register_table(table, Arc::new(mem_table))
                .map_err(|e| Error::internal(e.to_string()))?;
        } else {
            let _ = self.ctx.deregister_table(&table);

            let partitions = if all_batches.is_empty() {
                vec![]
            } else {
                vec![all_batches]
            };

            let mem_table = MemTable::try_new(table_schema, partitions)
                .map_err(|e| Error::internal(e.to_string()))?;
            self.ctx
                .register_table(&table, Arc::new(mem_table))
                .map_err(|e| Error::internal(e.to_string()))?;
        }

        Ok(vec![])
    }

    async fn execute_update(
        &self,
        table_name: &str,
        _alias: Option<&str>,
        assignments: &[yachtsql_ir::Assignment],
        _from: Option<&LogicalPlan>,
        filter: Option<&yachtsql_ir::Expr>,
    ) -> Result<Vec<RecordBatch>> {
        let lower = table_name.to_lowercase();
        let provider = self
            .ctx
            .table_provider(TableReference::bare(lower.clone()))
            .now_or_never()
            .ok_or_else(|| Error::internal(format!("Table not found: {}", table_name)))?
            .map_err(|e| Error::internal(e.to_string()))?;

        let schema = provider.schema();
        let source = provider_as_source(provider);

        let scan = LogicalPlanBuilder::scan(&lower, source, None)
            .map_err(|e| Error::internal(e.to_string()))?
            .build()
            .map_err(|e| Error::internal(e.to_string()))?;

        let df = DataFrame::new(self.ctx.state(), scan);
        let all_rows = df
            .collect()
            .await
            .map_err(|e| Error::internal(e.to_string()))?;

        if all_rows.is_empty() {
            return Ok(vec![]);
        }

        let mut updated_batches = Vec::new();

        for batch in &all_rows {
            let num_rows = batch.num_rows();
            let mut mask = vec![false; num_rows];

            if let Some(filter_expr) = filter {
                let filter_sql = self.expr_to_sql(filter_expr)?;
                let filter_batch = self.evaluate_filter(batch, &filter_sql).await?;
                for (i, m) in mask.iter_mut().enumerate() {
                    if filter_batch.get(i).copied().unwrap_or(false) {
                        *m = true;
                    }
                }
            } else {
                mask.fill(true);
            }

            let mut new_columns: Vec<ArrayRef> = batch.columns().to_vec();

            for assignment in assignments {
                let col_name = assignment.column.to_lowercase();
                let col_idx = schema
                    .fields()
                    .iter()
                    .position(|f| f.name().to_lowercase() == col_name)
                    .ok_or_else(|| {
                        Error::internal(format!("Column not found: {}", assignment.column))
                    })?;

                let value_sql = self.expr_to_sql(&assignment.value)?;
                let new_values = self
                    .evaluate_expr(batch, &value_sql, schema.field(col_idx).data_type())
                    .await?;

                let mut builder = datafusion::arrow::array::make_builder(
                    schema.field(col_idx).data_type(),
                    num_rows,
                );

                for (i, m) in mask.iter().enumerate() {
                    if *m {
                        self.append_value(&mut builder, &new_values, i)?;
                    } else {
                        self.append_value(&mut builder, batch.column(col_idx), i)?;
                    }
                }

                new_columns[col_idx] = builder.finish();
            }

            let updated_batch = RecordBatch::try_new(schema.clone(), new_columns)
                .map_err(|e| Error::internal(e.to_string()))?;
            updated_batches.push(updated_batch);
        }

        let _ = self.ctx.deregister_table(&lower);
        let mem_table = MemTable::try_new(schema, vec![updated_batches])
            .map_err(|e| Error::internal(e.to_string()))?;
        self.ctx
            .register_table(&lower, Arc::new(mem_table))
            .map_err(|e| Error::internal(e.to_string()))?;

        Ok(vec![])
    }

    async fn execute_delete(
        &self,
        table_name: &str,
        _alias: Option<&str>,
        filter: Option<&yachtsql_ir::Expr>,
    ) -> Result<Vec<RecordBatch>> {
        let lower = table_name.to_lowercase();
        let provider = self
            .ctx
            .table_provider(TableReference::bare(lower.clone()))
            .now_or_never()
            .ok_or_else(|| Error::internal(format!("Table not found: {}", table_name)))?
            .map_err(|e| Error::internal(e.to_string()))?;

        let schema = provider.schema();
        let source = provider_as_source(provider);

        let scan = LogicalPlanBuilder::scan(&lower, source, None)
            .map_err(|e| Error::internal(e.to_string()))?
            .build()
            .map_err(|e| Error::internal(e.to_string()))?;

        let df = DataFrame::new(self.ctx.state(), scan);

        let result_df = match filter {
            Some(filter_expr) => {
                let df_filter = self.convert_expr(filter_expr)?;
                df.filter(df_filter.not())
                    .map_err(|e| Error::internal(e.to_string()))?
            }
            None => {
                let _ = self.ctx.deregister_table(&lower);
                let mem_table = MemTable::try_new(schema, vec![])
                    .map_err(|e| Error::internal(e.to_string()))?;
                self.ctx
                    .register_table(&lower, Arc::new(mem_table))
                    .map_err(|e| Error::internal(e.to_string()))?;
                return Ok(vec![]);
            }
        };

        let remaining_rows = result_df
            .collect()
            .await
            .map_err(|e| Error::internal(e.to_string()))?;

        let _ = self.ctx.deregister_table(&lower);
        let mem_table = MemTable::try_new(schema, vec![remaining_rows])
            .map_err(|e| Error::internal(e.to_string()))?;
        self.ctx
            .register_table(&lower, Arc::new(mem_table))
            .map_err(|e| Error::internal(e.to_string()))?;

        Ok(vec![])
    }

    #[allow(clippy::only_used_in_recursion, clippy::wildcard_enum_match_arm)]
    fn expr_to_sql(&self, expr: &yachtsql_ir::Expr) -> Result<String> {
        use yachtsql_ir::{BinaryOp, Expr, Literal};
        match expr {
            Expr::Literal(lit) => match lit {
                Literal::Null => Ok("NULL".to_string()),
                Literal::Bool(b) => Ok(if *b { "TRUE" } else { "FALSE" }.to_string()),
                Literal::Int64(i) => Ok(i.to_string()),
                Literal::Float64(f) => Ok(f.to_string()),
                Literal::String(s) => Ok(format!("'{}'", s.replace('\'', "''"))),
                Literal::Numeric(d) => Ok(d.to_string()),
                Literal::BigNumeric(d) => Ok(d.to_string()),
                Literal::Date(d) => Ok(format!(
                    "DATE '{}'",
                    chrono::NaiveDate::from_num_days_from_ce_opt(*d)
                        .map(|d| d.format("%Y-%m-%d").to_string())
                        .unwrap_or_default()
                )),
                Literal::Timestamp(ts) => Ok(format!(
                    "TIMESTAMP '{}'",
                    chrono::DateTime::from_timestamp_micros(*ts)
                        .map(|d| d.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
                        .unwrap_or_default()
                )),
                _ => Err(Error::internal(format!(
                    "Unsupported literal type: {:?}",
                    std::mem::discriminant(lit)
                ))),
            },
            Expr::Column { name, table, .. } => match table {
                Some(t) => Ok(format!("{}.{}", t, name)),
                None => Ok(name.clone()),
            },
            Expr::BinaryOp { left, op, right } => {
                let l = self.expr_to_sql(left)?;
                let r = self.expr_to_sql(right)?;
                let op_str = match op {
                    BinaryOp::Eq => "=",
                    BinaryOp::NotEq => "<>",
                    BinaryOp::Lt => "<",
                    BinaryOp::LtEq => "<=",
                    BinaryOp::Gt => ">",
                    BinaryOp::GtEq => ">=",
                    BinaryOp::And => "AND",
                    BinaryOp::Or => "OR",
                    BinaryOp::Add => "+",
                    BinaryOp::Sub => "-",
                    BinaryOp::Mul => "*",
                    BinaryOp::Div => "/",
                    BinaryOp::Mod => "%",
                    BinaryOp::BitwiseAnd => "&",
                    BinaryOp::BitwiseOr => "|",
                    BinaryOp::BitwiseXor => "^",
                    BinaryOp::Concat => "||",
                    BinaryOp::ShiftLeft => "<<",
                    BinaryOp::ShiftRight => ">>",
                };
                Ok(format!("({} {} {})", l, op_str, r))
            }
            Expr::IsNull { expr, negated } => {
                let inner = self.expr_to_sql(expr)?;
                let op = if *negated { "IS NOT NULL" } else { "IS NULL" };
                Ok(format!("({} {})", inner, op))
            }
            Expr::Like {
                expr,
                pattern,
                negated,
                ..
            } => {
                let e = self.expr_to_sql(expr)?;
                let p = self.expr_to_sql(pattern)?;
                let neg = if *negated { "NOT " } else { "" };
                Ok(format!("({} {}LIKE {})", e, neg, p))
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let e = self.expr_to_sql(expr)?;
                let values: Vec<String> = list
                    .iter()
                    .map(|v| self.expr_to_sql(v))
                    .collect::<Result<_>>()?;
                let neg = if *negated { "NOT " } else { "" };
                Ok(format!("({} {}IN ({}))", e, neg, values.join(", ")))
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let e = self.expr_to_sql(expr)?;
                let l = self.expr_to_sql(low)?;
                let h = self.expr_to_sql(high)?;
                let neg = if *negated { "NOT " } else { "" };
                Ok(format!("({} {}BETWEEN {} AND {})", e, neg, l, h))
            }
            _ => Err(Error::internal(format!(
                "Expression not supported in UPDATE/DELETE: {:?}",
                std::mem::discriminant(expr)
            ))),
        }
    }

    async fn evaluate_filter(&self, batch: &RecordBatch, filter_sql: &str) -> Result<Vec<bool>> {
        let schema = batch.schema();
        let mem_table = MemTable::try_new(schema.clone(), vec![vec![batch.clone()]])
            .map_err(|e| Error::internal(e.to_string()))?;

        let tmp_name = format!(
            "__tmp_filter_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        self.ctx
            .register_table(&tmp_name, Arc::new(mem_table))
            .map_err(|e| Error::internal(e.to_string()))?;

        let query = format!("SELECT {} FROM {}", filter_sql, tmp_name);
        let result = self
            .ctx
            .sql(&query)
            .await
            .map_err(|e| Error::internal(e.to_string()))?;
        let batches = result
            .collect()
            .await
            .map_err(|e| Error::internal(e.to_string()))?;
        let _ = self.ctx.deregister_table(&tmp_name);

        let mut values = Vec::new();
        for b in batches {
            if b.num_columns() > 0 {
                let col = b.column(0);
                if let Some(bool_arr) = col
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                {
                    for i in 0..bool_arr.len() {
                        values.push(bool_arr.value(i));
                    }
                }
            }
        }
        Ok(values)
    }

    async fn evaluate_expr(
        &self,
        batch: &RecordBatch,
        expr_sql: &str,
        _target_type: &ArrowDataType,
    ) -> Result<ArrayRef> {
        let schema = batch.schema();
        let mem_table = MemTable::try_new(schema.clone(), vec![vec![batch.clone()]])
            .map_err(|e| Error::internal(e.to_string()))?;

        let tmp_name = format!(
            "__tmp_expr_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        self.ctx
            .register_table(&tmp_name, Arc::new(mem_table))
            .map_err(|e| Error::internal(e.to_string()))?;

        let query = format!("SELECT {} FROM {}", expr_sql, tmp_name);
        let result = self
            .ctx
            .sql(&query)
            .await
            .map_err(|e| Error::internal(e.to_string()))?;
        let batches = result
            .collect()
            .await
            .map_err(|e| Error::internal(e.to_string()))?;
        let _ = self.ctx.deregister_table(&tmp_name);

        if batches.is_empty() || batches[0].num_columns() == 0 {
            return Err(Error::internal("Expression evaluation returned no data"));
        }

        Ok(batches[0].column(0).clone())
    }

    #[allow(clippy::only_used_in_recursion, clippy::wildcard_enum_match_arm)]
    fn append_value(
        &self,
        builder: &mut Box<dyn datafusion::arrow::array::ArrayBuilder>,
        source: &ArrayRef,
        idx: usize,
    ) -> Result<()> {
        use datafusion::arrow::array::*;

        let is_null = source.is_null(idx);

        match source.data_type() {
            ArrowDataType::Int64 => {
                let b = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
                if is_null {
                    b.append_null();
                } else {
                    let arr = source.as_any().downcast_ref::<Int64Array>().unwrap();
                    b.append_value(arr.value(idx));
                }
            }
            ArrowDataType::Float64 => {
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Float64Builder>()
                    .unwrap();
                if is_null {
                    b.append_null();
                } else {
                    let arr = source.as_any().downcast_ref::<Float64Array>().unwrap();
                    b.append_value(arr.value(idx));
                }
            }
            ArrowDataType::Utf8 => {
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .unwrap();
                if is_null {
                    b.append_null();
                } else {
                    let arr = source.as_any().downcast_ref::<StringArray>().unwrap();
                    b.append_value(arr.value(idx));
                }
            }
            ArrowDataType::Boolean => {
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<BooleanBuilder>()
                    .unwrap();
                if is_null {
                    b.append_null();
                } else {
                    let arr = source.as_any().downcast_ref::<BooleanArray>().unwrap();
                    b.append_value(arr.value(idx));
                }
            }
            ArrowDataType::Date32 => {
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Date32Builder>()
                    .unwrap();
                if is_null {
                    b.append_null();
                } else {
                    let arr = source.as_any().downcast_ref::<Date32Array>().unwrap();
                    b.append_value(arr.value(idx));
                }
            }
            _ => {
                return Err(Error::internal(format!(
                    "Unsupported data type for UPDATE: {:?}",
                    source.data_type()
                )));
            }
        }
        Ok(())
    }

    async fn execute_truncate(&self, table_name: &str) -> Result<Vec<RecordBatch>> {
        let lower = table_name.to_lowercase();
        let provider = self
            .ctx
            .table_provider(TableReference::bare(lower.clone()))
            .now_or_never()
            .ok_or_else(|| Error::internal(format!("Table not found: {}", table_name)))?
            .map_err(|e| Error::internal(e.to_string()))?;

        let schema = provider.schema();
        let _ = self.ctx.deregister_table(&lower);
        let mem_table =
            MemTable::try_new(schema, vec![]).map_err(|e| Error::internal(e.to_string()))?;
        self.ctx
            .register_table(&lower, Arc::new(mem_table))
            .map_err(|e| Error::internal(e.to_string()))?;

        Ok(vec![])
    }

    async fn execute_alter_table(
        &self,
        _table_name: &str,
        _operation: &AlterTableOp,
        _if_exists: bool,
    ) -> Result<Vec<RecordBatch>> {
        Err(Error::internal("ALTER TABLE not yet implemented"))
    }

    async fn execute_create_view(
        &self,
        name: &str,
        query_sql: &str,
        column_aliases: &[String],
        or_replace: bool,
        if_not_exists: bool,
    ) -> Result<Vec<RecordBatch>> {
        let lower = name.to_lowercase();

        {
            let views = self.views.read();
            if views.contains_key(&lower) {
                if if_not_exists {
                    return Ok(vec![]);
                }
                if !or_replace {
                    return Err(Error::internal(format!("View {} already exists", name)));
                }
            }
        }

        self.views.write().insert(
            lower,
            ViewDefinition {
                query: query_sql.to_string(),
                column_aliases: column_aliases.to_vec(),
            },
        );

        Ok(vec![])
    }

    async fn execute_drop_view(&self, name: &str, if_exists: bool) -> Result<Vec<RecordBatch>> {
        let lower = name.to_lowercase();

        match self.views.write().remove(&lower) {
            Some(_) => Ok(vec![]),
            None => {
                if if_exists {
                    Ok(vec![])
                } else {
                    Err(Error::internal(format!("View {} not found", name)))
                }
            }
        }
    }

    async fn execute_create_schema(
        &self,
        name: &str,
        if_not_exists: bool,
    ) -> Result<Vec<RecordBatch>> {
        let lower = name.to_lowercase();

        {
            let schemas = self.schemas.read();
            if schemas.contains(&lower) {
                if if_not_exists {
                    return Ok(vec![]);
                }
                return Err(Error::internal(format!("Schema already exists: {}", name)));
            }
        }

        let catalog = self
            .ctx
            .catalog("datafusion")
            .ok_or_else(|| Error::internal("Default catalog 'datafusion' not found"))?;

        let schema_provider = Arc::new(MemorySchemaProvider::new());
        catalog
            .register_schema(&lower, schema_provider)
            .map_err(|e| Error::internal(e.to_string()))?;

        self.schemas.write().insert(lower);

        Ok(vec![])
    }

    async fn execute_drop_schema(
        &self,
        name: &str,
        if_exists: bool,
        cascade: bool,
    ) -> Result<Vec<RecordBatch>> {
        let lower = name.to_lowercase();

        {
            let schemas = self.schemas.read();
            if !schemas.contains(&lower) {
                if if_exists {
                    return Ok(vec![]);
                }
                return Err(Error::internal(format!("Schema not found: {}", name)));
            }
        }

        let catalog = self
            .ctx
            .catalog("datafusion")
            .ok_or_else(|| Error::internal("Default catalog 'datafusion' not found"))?;

        if let Some(schema) = catalog.schema(&lower) {
            let tables = schema.table_names();
            if !cascade && !tables.is_empty() {
                return Err(Error::internal(format!(
                    "Schema {} is not empty. Use CASCADE to drop.",
                    name
                )));
            }
            for table_name in tables {
                schema
                    .deregister_table(&table_name)
                    .map_err(|e| Error::internal(e.to_string()))?;
            }
        }

        catalog
            .deregister_schema(&lower, cascade)
            .map_err(|e| Error::internal(e.to_string()))?;

        self.schemas.write().remove(&lower);

        Ok(vec![])
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_create_function(
        &self,
        name: &str,
        args: &[yachtsql_ir::FunctionArg],
        return_type: &DataType,
        body: &FunctionBody,
        or_replace: bool,
        if_not_exists: bool,
        is_aggregate: bool,
    ) -> Result<Vec<RecordBatch>> {
        let lower = name.to_lowercase();

        {
            let functions = self.functions.read();
            if functions.contains_key(&lower) {
                if if_not_exists {
                    return Ok(vec![]);
                }
                if !or_replace {
                    return Err(Error::internal(format!("Function {} already exists", name)));
                }
            }
        }

        self.functions.write().insert(
            lower,
            FunctionDefinition {
                name: name.to_string(),
                parameters: args.to_vec(),
                return_type: return_type.clone(),
                body: body.clone(),
                is_aggregate,
            },
        );

        Ok(vec![])
    }

    async fn execute_drop_function(&self, name: &str, if_exists: bool) -> Result<Vec<RecordBatch>> {
        let lower = name.to_lowercase();

        match self.functions.write().remove(&lower) {
            Some(_) => Ok(vec![]),
            None => {
                if if_exists {
                    Ok(vec![])
                } else {
                    Err(Error::internal(format!("Function {} not found", name)))
                }
            }
        }
    }

    async fn execute_explain(
        &self,
        input: &LogicalPlan,
        _analyze: bool,
    ) -> Result<Vec<RecordBatch>> {
        let df_plan = self.convert_plan(input)?;
        let plan_string = format!("{:?}", df_plan);

        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "plan",
            ArrowDataType::Utf8,
            false,
        )]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![plan_string]))])
                .map_err(|e| Error::internal(e.to_string()))?;

        Ok(vec![batch])
    }

    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        let catalog = SessionCatalog { session: self };
        let plan = yachtsql_parser::parse_and_plan(sql, &catalog)?;
        let df_plan = self.convert_plan(&plan)?;
        Ok(DataFrame::new(self.ctx.state(), df_plan))
    }

    pub fn register_batch(&self, name: &str, batch: RecordBatch) -> Result<()> {
        let lower = name.to_lowercase();
        let schema = batch.schema();
        let mem_table = MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| Error::internal(e.to_string()))?;
        self.ctx
            .register_table(&lower, Arc::new(mem_table))
            .map_err(|e| Error::internal(e.to_string()))?;
        Ok(())
    }

    pub fn register_batches(&self, name: &str, batches: Vec<RecordBatch>) -> Result<()> {
        if batches.is_empty() {
            return Err(Error::internal(
                "Cannot register empty batch list".to_string(),
            ));
        }
        let lower = name.to_lowercase();
        let schema = batches[0].schema();
        let mem_table =
            MemTable::try_new(schema, vec![batches]).map_err(|e| Error::internal(e.to_string()))?;
        self.ctx
            .register_table(&lower, Arc::new(mem_table))
            .map_err(|e| Error::internal(e.to_string()))?;
        Ok(())
    }

    pub fn session_context(&self) -> &SessionContext {
        &self.ctx
    }
}

impl Default for YachtSQLSession {
    fn default() -> Self {
        Self::new()
    }
}

fn convert_plan_schema(schema: &PlanSchema) -> Arc<ArrowSchema> {
    let fields: Vec<ArrowField> = schema
        .fields
        .iter()
        .map(|f| {
            ArrowField::new(
                f.name.clone(),
                yachtsql_type_to_arrow(&f.data_type),
                f.nullable,
            )
        })
        .collect();
    Arc::new(ArrowSchema::new(fields))
}

fn convert_join_type(jt: &JoinType) -> DFJoinType {
    match jt {
        JoinType::Inner => DFJoinType::Inner,
        JoinType::Left => DFJoinType::Left,
        JoinType::Right => DFJoinType::Right,
        JoinType::Full => DFJoinType::Full,
        JoinType::Cross => DFJoinType::Inner,
    }
}

pub(crate) fn yachtsql_type_to_arrow(dt: &DataType) -> ArrowDataType {
    match dt {
        DataType::Bool => ArrowDataType::Boolean,
        DataType::Int64 => ArrowDataType::Int64,
        DataType::Float64 => ArrowDataType::Float64,
        DataType::Numeric(_) | DataType::BigNumeric => ArrowDataType::Decimal128(38, 9),
        DataType::String => ArrowDataType::Utf8,
        DataType::Bytes => ArrowDataType::Binary,
        DataType::Date => ArrowDataType::Date32,
        DataType::Time => ArrowDataType::Time64(TimeUnit::Nanosecond),
        DataType::DateTime => ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
        DataType::Timestamp => ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        DataType::Json => ArrowDataType::Utf8,
        DataType::Geography => ArrowDataType::Utf8,
        DataType::Interval => ArrowDataType::Interval(IntervalUnit::MonthDayNano),
        DataType::Array(inner) => ArrowDataType::List(Arc::new(ArrowField::new(
            "item",
            yachtsql_type_to_arrow(inner),
            true,
        ))),
        DataType::Struct(fields) => {
            let arrow_fields: Vec<ArrowField> = fields
                .iter()
                .map(|sf| ArrowField::new(&sf.name, yachtsql_type_to_arrow(&sf.data_type), true))
                .collect();
            ArrowDataType::Struct(arrow_fields.into())
        }
        DataType::Range(_) => ArrowDataType::Utf8,
        DataType::Unknown => ArrowDataType::Utf8,
    }
}

#[allow(clippy::wildcard_enum_match_arm)]
pub(crate) fn arrow_type_to_yachtsql(dt: &ArrowDataType) -> DataType {
    match dt {
        ArrowDataType::Boolean => DataType::Bool,
        ArrowDataType::Int8
        | ArrowDataType::Int16
        | ArrowDataType::Int32
        | ArrowDataType::Int64
        | ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64 => DataType::Int64,
        ArrowDataType::Float16 | ArrowDataType::Float32 | ArrowDataType::Float64 => {
            DataType::Float64
        }
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => DataType::String,
        ArrowDataType::Binary | ArrowDataType::LargeBinary => DataType::Bytes,
        ArrowDataType::Date32 | ArrowDataType::Date64 => DataType::Date,
        ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => DataType::Time,
        ArrowDataType::Timestamp(_, None) => DataType::DateTime,
        ArrowDataType::Timestamp(_, Some(_)) => DataType::Timestamp,
        ArrowDataType::Decimal128(_, _) | ArrowDataType::Decimal256(_, _) => {
            DataType::Numeric(None)
        }
        ArrowDataType::List(field) => {
            DataType::Array(Box::new(arrow_type_to_yachtsql(field.data_type())))
        }
        ArrowDataType::Struct(fields) => {
            let yachtsql_fields: Vec<yachtsql_common::types::StructField> = fields
                .iter()
                .map(|f| yachtsql_common::types::StructField {
                    name: f.name().clone(),
                    data_type: arrow_type_to_yachtsql(f.data_type()),
                })
                .collect();
            DataType::Struct(yachtsql_fields)
        }
        _ => DataType::String,
    }
}

fn cast_batch_to_schema(
    batch: &RecordBatch,
    target_schema: &Arc<ArrowSchema>,
) -> Result<RecordBatch> {
    if batch.schema() == *target_schema {
        return Ok(batch.clone());
    }

    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .zip(target_schema.fields().iter())
        .map(|(col, target_field)| {
            if col.data_type() == target_field.data_type() {
                Ok(col.clone())
            } else {
                cast(col, target_field.data_type()).map_err(|e| Error::internal(e.to_string()))
            }
        })
        .collect::<Result<_>>()?;

    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| Error::internal(e.to_string()))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int64Array, StringArray};

    use super::*;

    #[tokio::test]
    async fn test_simple_query() {
        let session = YachtSQLSession::new();

        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("name", ArrowDataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap();

        session.register_batch("users", batch).unwrap();

        let result = session
            .execute_sql("SELECT name FROM users WHERE id > 1 ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_aggregation() {
        let session = YachtSQLSession::new();

        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("country", ArrowDataType::Utf8, false),
            ArrowField::new("amount", ArrowDataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["US", "UK", "US", "UK"])),
                Arc::new(Int64Array::from(vec![100, 200, 150, 250])),
            ],
        )
        .unwrap();

        session.register_batch("sales", batch).unwrap();

        let result = session
            .execute_sql("SELECT country FROM sales GROUP BY country")
            .await
            .unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_create_table_and_insert() {
        let session = YachtSQLSession::new();

        session
            .execute_sql("CREATE TABLE test_table (id INT64, name STRING)")
            .await
            .unwrap();

        session
            .execute_sql("INSERT INTO test_table VALUES (1, 'Alice')")
            .await
            .unwrap();

        let result = session
            .execute_sql("SELECT * FROM test_table")
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1);
    }
}
