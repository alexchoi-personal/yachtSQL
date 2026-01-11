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
use datafusion::error::Result as DFResult;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    ColumnarValue, EmptyRelation, Expr as DFExpr, JoinType as DFJoinType,
    LogicalPlan as DFLogicalPlan, LogicalPlanBuilder, ScalarUDF, ScalarUDFImpl, Signature,
    SortExpr as DFSortExpr, Subquery, Volatility,
};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use futures::FutureExt;
use parking_lot::RwLock;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Field, FieldMode, Schema};
use yachtsql_datafusion_functions::BigQueryFunctionRegistry;
use yachtsql_ir::plan::{AlterColumnAction, AlterTableOp, FunctionBody, MergeClause};
use yachtsql_ir::{JoinType, LogicalPlan, PlanSchema, SetOperationType, SortExpr};
use yachtsql_parser::{CatalogProvider, FunctionDefinition, ViewDefinition};

#[allow(dead_code)]
pub struct ProcedureDefinition {
    pub name: String,
    pub args: Vec<yachtsql_ir::ProcedureArg>,
    pub body: Vec<LogicalPlan>,
}

enum ControlFlow {
    Normal(Vec<RecordBatch>),
    Break(Option<String>),
    Continue(Option<String>),
    Return,
}

#[derive(Debug)]
struct UserDefinedScalarFunction {
    name: String,
    signature: Signature,
    return_type: ArrowDataType,
    body_sql: String,
    param_names: Vec<String>,
}

impl UserDefinedScalarFunction {
    fn new(
        name: String,
        params: &[yachtsql_ir::FunctionArg],
        return_type: ArrowDataType,
        body_sql: String,
    ) -> Self {
        let param_types: Vec<ArrowDataType> = params
            .iter()
            .map(|p| yachtsql_type_to_arrow(&p.data_type))
            .collect();
        let param_names: Vec<String> = params.iter().map(|p| p.name.to_lowercase()).collect();

        Self {
            name,
            signature: Signature::exact(param_types, Volatility::Immutable),
            return_type,
            body_sql,
            param_names,
        }
    }
}

impl ScalarUDFImpl for UserDefinedScalarFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
        use datafusion::arrow::array::*;

        if args.is_empty() {
            let ctx = SessionContext::new();
            let query = format!("SELECT {}", self.body_sql);
            let batches =
                futures::executor::block_on(async { ctx.sql(&query).await?.collect().await })?;

            if batches.is_empty() || batches[0].num_rows() == 0 {
                return Ok(ColumnarValue::Scalar(ScalarValue::Null));
            }

            let result = batches[0].column(0).clone();
            if result.len() == 1 && num_rows > 1 {
                let scalar = ScalarValue::try_from_array(&result, 0)?;
                return Ok(ColumnarValue::Scalar(scalar));
            }
            return Ok(ColumnarValue::Array(result));
        }

        let actual_num_rows = args
            .iter()
            .find_map(|a| match a {
                ColumnarValue::Array(arr) => Some(arr.len()),
                ColumnarValue::Scalar(_) => None,
            })
            .unwrap_or(num_rows);

        let arrays: Vec<ArrayRef> = args
            .iter()
            .map(|a| match a {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(actual_num_rows).unwrap(),
            })
            .collect();

        let ctx = SessionContext::new();
        let schema = ArrowSchema::new(
            self.param_names
                .iter()
                .zip(arrays.iter())
                .map(|(name, arr)| ArrowField::new(name, arr.data_type().clone(), true))
                .collect::<Vec<_>>(),
        );
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        let mem_table = MemTable::try_new(Arc::new(schema), vec![vec![batch]])?;

        futures::executor::block_on(async {
            ctx.register_table("__udf_args", Arc::new(mem_table))?;
            let query = format!("SELECT {} FROM __udf_args", self.body_sql);
            let batches = ctx.sql(&query).await?.collect().await?;

            if batches.is_empty() || batches[0].num_rows() == 0 {
                let null_arr: StringArray = (0..actual_num_rows).map(|_| None::<&str>).collect();
                return Ok(ColumnarValue::Array(Arc::new(null_arr)));
            }

            let result = batches[0].column(0).clone();
            Ok(ColumnarValue::Array(result))
        })
    }
}

#[derive(Debug)]
struct UserDefinedAggregateFunction {
    name: String,
    signature: Signature,
    return_type: ArrowDataType,
    body_sql: String,
    param_names: Vec<String>,
}

impl UserDefinedAggregateFunction {
    fn new(
        name: String,
        params: &[yachtsql_ir::FunctionArg],
        return_type: ArrowDataType,
        body_sql: String,
    ) -> Self {
        let param_types: Vec<ArrowDataType> = params
            .iter()
            .map(|p| yachtsql_type_to_arrow(&p.data_type))
            .collect();
        let param_names: Vec<String> = params.iter().map(|p| p.name.to_lowercase()).collect();

        Self {
            name,
            signature: Signature::exact(param_types, Volatility::Immutable),
            return_type,
            body_sql,
            param_names,
        }
    }

    fn into_udaf(self) -> datafusion::logical_expr::AggregateUDF {
        use datafusion::logical_expr::function::StateFieldsArgs;
        use datafusion::logical_expr::{Accumulator, AggregateUDF, AggregateUDFImpl};

        #[derive(Debug)]
        struct UdafImpl {
            name: String,
            signature: Signature,
            return_type: ArrowDataType,
            body_sql: String,
            param_names: Vec<String>,
        }

        impl AggregateUDFImpl for UdafImpl {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn name(&self) -> &str {
                &self.name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
                Ok(self.return_type.clone())
            }

            fn accumulator(
                &self,
                _acc_args: datafusion::logical_expr::function::AccumulatorArgs,
            ) -> DFResult<Box<dyn Accumulator>> {
                Ok(Box::new(SqlAggAccumulator {
                    body_sql: self.body_sql.clone(),
                    param_names: self.param_names.clone(),
                    values: Vec::new(),
                }))
            }

            fn state_fields(&self, _args: StateFieldsArgs) -> DFResult<Vec<ArrowField>> {
                Ok(vec![ArrowField::new("state", ArrowDataType::Utf8, true)])
            }
        }

        #[derive(Debug)]
        struct SqlAggAccumulator {
            body_sql: String,
            param_names: Vec<String>,
            values: Vec<Vec<ScalarValue>>,
        }

        impl Accumulator for SqlAggAccumulator {
            fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
                if values.is_empty() {
                    return Ok(());
                }
                let num_rows = values[0].len();
                for row in 0..num_rows {
                    let row_values: Vec<ScalarValue> = values
                        .iter()
                        .map(|arr| ScalarValue::try_from_array(arr, row))
                        .collect::<DFResult<_>>()?;
                    self.values.push(row_values);
                }
                Ok(())
            }

            fn evaluate(&mut self) -> DFResult<ScalarValue> {
                if self.values.is_empty() {
                    return Ok(ScalarValue::Null);
                }

                let ctx = SessionContext::new();

                let arrays: Vec<ArrayRef> = (0..self.param_names.len())
                    .map(|col_idx| {
                        let col_values: Vec<ScalarValue> = self
                            .values
                            .iter()
                            .map(|row| row.get(col_idx).cloned().unwrap_or(ScalarValue::Null))
                            .collect();
                        ScalarValue::iter_to_array(col_values)
                    })
                    .collect::<DFResult<_>>()?;

                let schema = ArrowSchema::new(
                    self.param_names
                        .iter()
                        .zip(arrays.iter())
                        .map(|(name, arr)| ArrowField::new(name, arr.data_type().clone(), true))
                        .collect::<Vec<_>>(),
                );
                let batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                let mem_table = MemTable::try_new(Arc::new(schema), vec![vec![batch]])?;

                futures::executor::block_on(async {
                    ctx.register_table("__udaf_args", Arc::new(mem_table))?;
                    let query = format!("SELECT {} FROM __udaf_args", self.body_sql);
                    let batches = ctx.sql(&query).await?.collect().await?;

                    if batches.is_empty() || batches[0].num_rows() == 0 {
                        return Ok(ScalarValue::Null);
                    }

                    ScalarValue::try_from_array(batches[0].column(0), 0)
                })
            }

            fn size(&self) -> usize {
                std::mem::size_of_val(self)
            }

            fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
                Ok(vec![ScalarValue::Utf8(Some(String::new()))])
            }

            fn merge_batch(&mut self, _states: &[ArrayRef]) -> DFResult<()> {
                Ok(())
            }
        }

        AggregateUDF::new_from_impl(UdafImpl {
            name: self.name,
            signature: self.signature,
            return_type: self.return_type,
            body_sql: self.body_sql,
            param_names: self.param_names,
        })
    }
}

pub struct YachtSQLSession {
    ctx: SessionContext,
    views: RwLock<HashMap<String, ViewDefinition>>,
    functions: RwLock<HashMap<String, FunctionDefinition>>,
    procedures: RwLock<HashMap<String, ProcedureDefinition>>,
    schemas: RwLock<HashSet<String>>,
    search_path: RwLock<Vec<String>>,
    column_defaults: RwLock<HashMap<String, HashMap<String, yachtsql_ir::Expr>>>,
    variables: RwLock<HashMap<String, ScalarValue>>,
    outer_aliases: RwLock<HashMap<String, String>>,
}

struct SessionCatalog<'a> {
    session: &'a YachtSQLSession,
}

impl<'a> CatalogProvider for SessionCatalog<'a> {
    fn get_table_schema(&self, name: &str) -> Option<Schema> {
        let (schema_name, table) = self.session.resolve_table_name(name);

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
        let ctx = SessionContext::new();
        BigQueryFunctionRegistry::register_all(&ctx);
        Self {
            ctx,
            views: RwLock::new(HashMap::new()),
            functions: RwLock::new(HashMap::new()),
            procedures: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashSet::new()),
            search_path: RwLock::new(Vec::new()),
            column_defaults: RwLock::new(HashMap::new()),
            variables: RwLock::new(HashMap::new()),
            outer_aliases: RwLock::new(HashMap::new()),
        }
    }

    pub fn with_config(config: SessionConfig) -> Self {
        let ctx = SessionContext::new_with_config(config);
        BigQueryFunctionRegistry::register_all(&ctx);
        Self {
            ctx,
            views: RwLock::new(HashMap::new()),
            functions: RwLock::new(HashMap::new()),
            procedures: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashSet::new()),
            search_path: RwLock::new(Vec::new()),
            column_defaults: RwLock::new(HashMap::new()),
            variables: RwLock::new(HashMap::new()),
            outer_aliases: RwLock::new(HashMap::new()),
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
        match self.execute_plan(&plan).await {
            Ok(result) => Ok(result),
            Err(e) if e.to_string().contains("RETURN_SIGNAL") => Ok(vec![]),
            Err(e) => Err(e),
        }
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

            LogicalPlan::GapFill {
                input,
                ts_column,
                bucket_width,
                value_columns,
                partitioning_columns,
                origin,
                input_schema,
                schema,
            } => {
                self.execute_gap_fill(
                    input,
                    ts_column,
                    bucket_width,
                    value_columns,
                    partitioning_columns,
                    origin.as_ref(),
                    input_schema,
                    schema,
                )
                .await
            }

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

            LogicalPlan::Merge {
                target_table,
                source,
                on,
                clauses,
            } => self.execute_merge(target_table, source, on, clauses).await,

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

            LogicalPlan::CreateProcedure {
                name,
                args,
                body,
                or_replace,
                if_not_exists,
            } => {
                self.execute_create_procedure(name, args, body, *or_replace, *if_not_exists)
                    .await
            }

            LogicalPlan::DropProcedure { name, if_exists } => {
                self.execute_drop_procedure(name, *if_exists).await
            }

            LogicalPlan::Call {
                procedure_name,
                args,
            } => self.execute_call(procedure_name, args).await,

            LogicalPlan::SetVariable { name, value } => {
                self.execute_set_variable(name, value).await
            }

            LogicalPlan::Explain { input, analyze } => self.execute_explain(input, *analyze).await,

            LogicalPlan::BeginTransaction | LogicalPlan::Commit | LogicalPlan::Rollback => {
                Ok(vec![])
            }

            LogicalPlan::AlterSchema { name, .. } => {
                let schemas = self.schemas.read();
                if !schemas.contains(&name.to_lowercase()) {
                    return Err(Error::invalid_query(format!(
                        "Schema '{}' does not exist",
                        name
                    )));
                }
                Ok(vec![])
            }

            LogicalPlan::UndropSchema { .. } => Ok(vec![]),

            LogicalPlan::Declare {
                name,
                data_type: _,
                default,
            } => {
                let value = match default {
                    Some(expr) => self.eval_const_expr(expr),
                    None => ScalarValue::Null,
                };
                self.variables.write().insert(name.to_lowercase(), value);
                Ok(vec![])
            }

            LogicalPlan::SetMultipleVariables { names, value } => {
                let scalar = self.eval_const_expr(value);
                for name in names {
                    self.variables
                        .write()
                        .insert(name.to_lowercase(), scalar.clone());
                }
                Ok(vec![])
            }

            LogicalPlan::If {
                condition,
                then_branch,
                else_branch,
            } => {
                let cond_value = self.eval_bool_expr(condition);
                let branch = if cond_value {
                    then_branch
                } else {
                    match else_branch {
                        Some(b) => b,
                        None => return Ok(vec![]),
                    }
                };
                self.execute_statements(branch).await
            }

            LogicalPlan::While {
                condition,
                body,
                label,
            } => {
                let my_label = label.as_ref().map(|l| l.to_lowercase());
                loop {
                    let cond_value = self.eval_bool_expr(condition);
                    if !cond_value {
                        break;
                    }
                    match self.execute_statements_with_control(body).await? {
                        ControlFlow::Normal(result) => {
                            if !result.is_empty() {
                                return Ok(result);
                            }
                        }
                        ControlFlow::Break(None) => break,
                        ControlFlow::Break(Some(ref target))
                            if my_label.as_ref() == Some(target) =>
                        {
                            break;
                        }
                        ControlFlow::Break(Some(lbl)) => {
                            return Err(Error::internal(format!("BREAK_SIGNAL:{}", lbl)));
                        }
                        ControlFlow::Continue(None) => continue,
                        ControlFlow::Continue(Some(ref target))
                            if my_label.as_ref() == Some(target) =>
                        {
                            continue;
                        }
                        ControlFlow::Continue(Some(lbl)) => {
                            return Err(Error::internal(format!("CONTINUE_SIGNAL:{}", lbl)));
                        }
                        ControlFlow::Return => return Err(Error::internal("RETURN_SIGNAL")),
                    }
                }
                Ok(vec![])
            }

            LogicalPlan::Loop { body, label } => {
                let my_label = label.as_ref().map(|l| l.to_lowercase());
                loop {
                    match self.execute_statements_with_control(body).await? {
                        ControlFlow::Normal(result) => {
                            if !result.is_empty() {
                                return Ok(result);
                            }
                        }
                        ControlFlow::Break(None) => break,
                        ControlFlow::Break(Some(ref target))
                            if my_label.as_ref() == Some(target) =>
                        {
                            break;
                        }
                        ControlFlow::Break(Some(lbl)) => {
                            return Err(Error::internal(format!("BREAK_SIGNAL:{}", lbl)));
                        }
                        ControlFlow::Continue(None) => continue,
                        ControlFlow::Continue(Some(ref target))
                            if my_label.as_ref() == Some(target) =>
                        {
                            continue;
                        }
                        ControlFlow::Continue(Some(lbl)) => {
                            return Err(Error::internal(format!("CONTINUE_SIGNAL:{}", lbl)));
                        }
                        ControlFlow::Return => return Err(Error::internal("RETURN_SIGNAL")),
                    }
                }
                Ok(vec![])
            }

            LogicalPlan::Block { body, label: _ } => self.execute_statements(body).await,

            LogicalPlan::Repeat {
                body,
                until_condition,
            } => {
                loop {
                    match self.execute_statements_with_control(body).await? {
                        ControlFlow::Normal(result) => {
                            if !result.is_empty() {
                                return Ok(result);
                            }
                        }
                        ControlFlow::Break(None) => break,
                        ControlFlow::Break(Some(lbl)) => {
                            return Err(Error::internal(format!("BREAK_SIGNAL:{}", lbl)));
                        }
                        ControlFlow::Continue(_) => {}
                        ControlFlow::Return => return Err(Error::internal("RETURN_SIGNAL")),
                    }
                    let cond_value = self.eval_bool_expr(until_condition);
                    if cond_value {
                        break;
                    }
                }
                Ok(vec![])
            }

            LogicalPlan::For {
                variable,
                query,
                body,
            } => {
                let batches = Box::pin(self.execute_query(query)).await?;
                'outer: for batch in &batches {
                    let schema = batch.schema();
                    for row_idx in 0..batch.num_rows() {
                        use datafusion::arrow::array::StructArray;
                        let arrays: Vec<Arc<dyn datafusion::arrow::array::Array>> = (0..batch
                            .num_columns())
                            .map(|i| {
                                let val = ScalarValue::try_from_array(batch.column(i), row_idx)
                                    .unwrap_or(ScalarValue::Null);
                                val.to_array().map_err(|e| Error::internal(e.to_string()))
                            })
                            .collect::<Result<Vec<_>>>()?;
                        let struct_array = StructArray::new(schema.fields().clone(), arrays, None);
                        let struct_val = ScalarValue::Struct(Arc::new(struct_array));
                        self.variables
                            .write()
                            .insert(variable.to_lowercase(), struct_val);
                        match self.execute_statements_with_control(body).await? {
                            ControlFlow::Normal(result) => {
                                if !result.is_empty() {
                                    return Ok(result);
                                }
                            }
                            ControlFlow::Break(None) => break 'outer,
                            ControlFlow::Break(Some(lbl)) => {
                                return Err(Error::internal(format!("BREAK_SIGNAL:{}", lbl)));
                            }
                            ControlFlow::Continue(None) => continue,
                            ControlFlow::Continue(Some(lbl)) => {
                                return Err(Error::internal(format!("CONTINUE_SIGNAL:{}", lbl)));
                            }
                            ControlFlow::Return => return Err(Error::internal("RETURN_SIGNAL")),
                        }
                    }
                }
                Ok(vec![])
            }

            LogicalPlan::Break { label } => {
                let lbl = label.as_ref().map(|l| l.to_lowercase());
                Err(Error::internal(format!(
                    "BREAK_SIGNAL:{}",
                    lbl.unwrap_or_default()
                )))
            }

            LogicalPlan::Continue { label } => {
                let lbl = label.as_ref().map(|l| l.to_lowercase());
                Err(Error::internal(format!(
                    "CONTINUE_SIGNAL:{}",
                    lbl.unwrap_or_default()
                )))
            }

            LogicalPlan::Return { value: _ } => Err(Error::internal("RETURN_SIGNAL")),

            LogicalPlan::Assert { condition, message } => {
                let cond_value = self.eval_bool_expr(condition);
                if !cond_value {
                    let msg = message
                        .as_ref()
                        .map(|e| {
                            let val = self.eval_const_expr(e);
                            match val {
                                ScalarValue::Utf8(Some(s)) => s,
                                _ => "Assertion failed".to_string(),
                            }
                        })
                        .unwrap_or_else(|| "Assertion failed".to_string());
                    return Err(Error::internal(msg));
                }
                Ok(vec![])
            }

            LogicalPlan::ExportData { .. }
            | LogicalPlan::LoadData { .. }
            | LogicalPlan::Raise { .. }
            | LogicalPlan::Grant { .. }
            | LogicalPlan::Revoke { .. }
            | LogicalPlan::CreateSnapshot { .. }
            | LogicalPlan::DropSnapshot { .. }
            | LogicalPlan::ExecuteImmediate { .. }
            | LogicalPlan::TryCatch { .. }
            | LogicalPlan::Unnest { .. } => Ok(vec![]),
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

        let processed_plan = Box::pin(self.preprocess_gap_fill(plan)).await?;
        let df_plan = self.convert_plan(&processed_plan)?;
        let df = DataFrame::new(self.ctx.state(), df_plan);
        df.collect()
            .await
            .map_err(|e| Error::internal(e.to_string()))
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    async fn preprocess_gap_fill(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::GapFill {
                input,
                ts_column,
                bucket_width,
                value_columns,
                partitioning_columns,
                origin,
                input_schema,
                schema,
            } => {
                let batches = self
                    .execute_gap_fill(
                        input,
                        ts_column,
                        bucket_width,
                        value_columns,
                        partitioning_columns,
                        origin.as_ref(),
                        input_schema,
                        schema,
                    )
                    .await?;

                let temp_name = format!(
                    "__gap_fill_{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos()
                );

                let arrow_schema = if !batches.is_empty() {
                    batches[0].schema()
                } else {
                    convert_plan_schema(schema)
                };

                let mem_table = MemTable::try_new(arrow_schema, vec![batches])
                    .map_err(|e| Error::internal(e.to_string()))?;
                self.ctx
                    .register_table(&temp_name, Arc::new(mem_table))
                    .map_err(|e| Error::internal(e.to_string()))?;

                Ok(LogicalPlan::Scan {
                    table_name: temp_name,
                    schema: schema.clone(),
                    projection: None,
                })
            }

            LogicalPlan::Filter { input, predicate } => {
                let processed_input = Box::pin(self.preprocess_gap_fill(input)).await?;
                Ok(LogicalPlan::Filter {
                    input: Box::new(processed_input),
                    predicate: predicate.clone(),
                })
            }

            LogicalPlan::Project {
                input,
                expressions,
                schema,
            } => {
                let processed_input = Box::pin(self.preprocess_gap_fill(input)).await?;
                Ok(LogicalPlan::Project {
                    input: Box::new(processed_input),
                    expressions: expressions.clone(),
                    schema: schema.clone(),
                })
            }

            LogicalPlan::Sort { input, sort_exprs } => {
                let processed_input = Box::pin(self.preprocess_gap_fill(input)).await?;
                Ok(LogicalPlan::Sort {
                    input: Box::new(processed_input),
                    sort_exprs: sort_exprs.clone(),
                })
            }

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let processed_input = Box::pin(self.preprocess_gap_fill(input)).await?;
                Ok(LogicalPlan::Limit {
                    input: Box::new(processed_input),
                    limit: *limit,
                    offset: *offset,
                })
            }

            LogicalPlan::Distinct { input } => {
                let processed_input = Box::pin(self.preprocess_gap_fill(input)).await?;
                Ok(LogicalPlan::Distinct {
                    input: Box::new(processed_input),
                })
            }

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                schema,
                grouping_sets,
            } => {
                let processed_input = Box::pin(self.preprocess_gap_fill(input)).await?;
                Ok(LogicalPlan::Aggregate {
                    input: Box::new(processed_input),
                    group_by: group_by.clone(),
                    aggregates: aggregates.clone(),
                    schema: schema.clone(),
                    grouping_sets: grouping_sets.clone(),
                })
            }

            LogicalPlan::Window {
                input,
                window_exprs,
                schema,
            } => {
                let processed_input = Box::pin(self.preprocess_gap_fill(input)).await?;
                Ok(LogicalPlan::Window {
                    input: Box::new(processed_input),
                    window_exprs: window_exprs.clone(),
                    schema: schema.clone(),
                })
            }

            LogicalPlan::Qualify { input, predicate } => {
                let processed_input = Box::pin(self.preprocess_gap_fill(input)).await?;
                Ok(LogicalPlan::Qualify {
                    input: Box::new(processed_input),
                    predicate: predicate.clone(),
                })
            }

            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
                schema,
            } => {
                let processed_left = Box::pin(self.preprocess_gap_fill(left)).await?;
                let processed_right = Box::pin(self.preprocess_gap_fill(right)).await?;
                Ok(LogicalPlan::Join {
                    left: Box::new(processed_left),
                    right: Box::new(processed_right),
                    join_type: *join_type,
                    condition: condition.clone(),
                    schema: schema.clone(),
                })
            }

            LogicalPlan::SetOperation {
                left,
                right,
                op,
                all,
                schema,
            } => {
                let processed_left = Box::pin(self.preprocess_gap_fill(left)).await?;
                let processed_right = Box::pin(self.preprocess_gap_fill(right)).await?;
                Ok(LogicalPlan::SetOperation {
                    left: Box::new(processed_left),
                    right: Box::new(processed_right),
                    op: *op,
                    all: *all,
                    schema: schema.clone(),
                })
            }

            LogicalPlan::Sample {
                input,
                sample_type,
                sample_value,
            } => {
                let processed_input = Box::pin(self.preprocess_gap_fill(input)).await?;
                Ok(LogicalPlan::Sample {
                    input: Box::new(processed_input),
                    sample_type: *sample_type,
                    sample_value: *sample_value,
                })
            }

            _ => Ok(plan.clone()),
        }
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    fn eval_bool_expr(&self, expr: &yachtsql_ir::Expr) -> bool {
        let val = self.eval_const_expr(expr);
        match val {
            ScalarValue::Boolean(Some(b)) => b,
            ScalarValue::Int64(Some(i)) => i != 0,
            ScalarValue::UInt64(Some(u)) => u != 0,
            _ => false,
        }
    }

    async fn execute_statements(&self, stmts: &[LogicalPlan]) -> Result<Vec<RecordBatch>> {
        for stmt in stmts {
            let result = Box::pin(self.execute_plan(stmt)).await?;
            if !result.is_empty() {
                return Ok(result);
            }
        }
        Ok(vec![])
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    async fn execute_statements_with_control(&self, stmts: &[LogicalPlan]) -> Result<ControlFlow> {
        for stmt in stmts {
            match stmt {
                LogicalPlan::Break { label } => {
                    return Ok(ControlFlow::Break(label.as_ref().map(|l| l.to_lowercase())));
                }
                LogicalPlan::Continue { label } => {
                    return Ok(ControlFlow::Continue(
                        label.as_ref().map(|l| l.to_lowercase()),
                    ));
                }
                LogicalPlan::Return { .. } => {
                    return Ok(ControlFlow::Return);
                }
                _ => {}
            }
            let result = Box::pin(self.execute_plan(stmt)).await;
            match result {
                Ok(batches) => {
                    if !batches.is_empty() {
                        return Ok(ControlFlow::Normal(batches));
                    }
                }
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("RETURN_SIGNAL") {
                        return Ok(ControlFlow::Return);
                    }
                    if let Some(idx) = msg.find("BREAK_SIGNAL:") {
                        let after = &msg[idx + "BREAK_SIGNAL:".len()..];
                        let lbl = if after.is_empty() {
                            None
                        } else {
                            Some(after.to_string())
                        };
                        return Ok(ControlFlow::Break(lbl));
                    }
                    if msg.contains("BREAK_SIGNAL") {
                        return Ok(ControlFlow::Break(None));
                    }
                    if let Some(idx) = msg.find("CONTINUE_SIGNAL:") {
                        let after = &msg[idx + "CONTINUE_SIGNAL:".len()..];
                        let lbl = if after.is_empty() {
                            None
                        } else {
                            Some(after.to_string())
                        };
                        return Ok(ControlFlow::Continue(lbl));
                    }
                    if msg.contains("CONTINUE_SIGNAL") {
                        return Ok(ControlFlow::Continue(None));
                    }
                    return Err(e);
                }
            }
        }
        Ok(ControlFlow::Normal(vec![]))
    }

    fn set_outer_aliases(&self, plan: &LogicalPlan) {
        let mut aliases = self.outer_aliases.write();
        extract_alias_mapping(plan, &mut aliases);
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    fn convert_plan(&self, plan: &LogicalPlan) -> Result<DFLogicalPlan> {
        self.outer_aliases.write().clear();
        self.convert_plan_inner(plan)
    }

    fn convert_plan_inner(&self, plan: &LogicalPlan) -> Result<DFLogicalPlan> {
        match plan {
            LogicalPlan::Scan {
                table_name,
                schema,
                projection,
            } => {
                let (schema_name, table) = self.resolve_table_name(table_name);
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
                let input_plan = self.convert_plan_inner(input)?;
                LogicalPlanBuilder::from(input_plan)
                    .limit(0, Some(*sample_value as usize))
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::Filter { input, predicate } => {
                let input_plan = self.convert_plan_inner(input)?;
                self.set_outer_aliases(input);
                let predicate_expr = self.convert_expr(predicate)?;
                LogicalPlanBuilder::from(input_plan)
                    .filter(predicate_expr)
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::Project {
                input,
                expressions,
                schema,
            } => {
                let input_plan = self.convert_plan_inner(input)?;
                self.set_outer_aliases(input);

                let is_subquery_alias = {
                    let all_columns = expressions
                        .iter()
                        .all(|e| matches!(e, yachtsql_ir::Expr::Column { .. }));
                    let first_table = schema.fields.first().and_then(|f| f.table.as_ref());
                    let same_table = first_table.is_some()
                        && schema
                            .fields
                            .iter()
                            .all(|f| f.table.as_ref() == first_table);
                    all_columns && same_table
                };

                if is_subquery_alias {
                    let alias = schema.fields.first().unwrap().table.as_ref().unwrap();
                    LogicalPlanBuilder::from(input_plan)
                        .alias(alias.as_str())
                        .map_err(|e| Error::internal(e.to_string()))?
                        .build()
                        .map_err(|e| Error::internal(e.to_string()))
                } else {
                    let mut name_occurrences: std::collections::HashMap<String, usize> =
                        std::collections::HashMap::new();

                    let project_exprs: Vec<DFExpr> = expressions
                        .iter()
                        .zip(schema.fields.iter())
                        .map(|(e, field)| {
                            let df_expr = self.convert_expr(e)?;
                            let base_name = field.name.clone();
                            let occurrence = name_occurrences.entry(base_name.clone()).or_insert(0);
                            let alias_name = if *occurrence == 0 {
                                base_name
                            } else {
                                format!("{}_{}", field.name, occurrence)
                            };
                            *occurrence += 1;
                            let expr_name = df_expr.schema_name().to_string();
                            if expr_name != alias_name {
                                Ok(df_expr.alias(&alias_name))
                            } else {
                                Ok(df_expr)
                            }
                        })
                        .collect::<Result<_>>()?;
                    LogicalPlanBuilder::from(input_plan)
                        .project(project_exprs)
                        .map_err(|e| Error::internal(e.to_string()))?
                        .build()
                        .map_err(|e| Error::internal(e.to_string()))
                }
            }

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                schema,
                ..
            } => {
                let input_plan = self.convert_plan_inner(input)?;
                self.set_outer_aliases(input);
                let group_exprs: Vec<DFExpr> = group_by
                    .iter()
                    .zip(schema.fields.iter())
                    .map(|(e, field)| {
                        let df_expr = self.convert_expr(e)?;
                        Ok(df_expr.alias(field.name.clone()))
                    })
                    .collect::<Result<_>>()?;
                let agg_exprs: Vec<DFExpr> = aggregates
                    .iter()
                    .zip(schema.fields.iter().skip(group_by.len()))
                    .map(|(e, field)| {
                        let df_expr = self.convert_expr(e)?;
                        Ok(df_expr.alias(field.name.clone()))
                    })
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
                let left_plan = self.convert_plan_inner(left)?;
                let right_plan = self.convert_plan_inner(right)?;
                let df_join_type = convert_join_type(join_type);
                self.set_outer_aliases(left);
                self.set_outer_aliases(right);

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
                let input_plan = self.convert_plan_inner(input)?;
                self.set_outer_aliases(input);
                let input_schema = input_plan.schema();
                let df_sort_exprs: Vec<DFSortExpr> = sort_exprs
                    .iter()
                    .map(|se| self.convert_sort_expr_with_schema(se, input_schema))
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
                let input_plan = self.convert_plan_inner(input)?;
                let skip = offset.unwrap_or(0);
                let fetch = *limit;
                LogicalPlanBuilder::from(input_plan)
                    .limit(skip, fetch)
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::Distinct { input } => {
                let input_plan = self.convert_plan_inner(input)?;
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
                    produce_one_row: true,
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
                let left_plan = self.convert_plan_inner(left)?;
                let right_plan = self.convert_plan_inner(right)?;

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
                schema,
                ..
            } => {
                let input_plan = self.convert_plan_inner(input)?;
                self.set_outer_aliases(input);
                let input_schema = input.schema();
                let df_window_exprs: Vec<DFExpr> = window_exprs
                    .iter()
                    .enumerate()
                    .map(|(i, e)| {
                        let expr = self.convert_expr(e)?;
                        let field_idx = input_schema.fields.len() + i;
                        let alias_name = schema
                            .fields
                            .get(field_idx)
                            .map(|f| f.name.clone())
                            .unwrap_or_else(|| format!("__window_{}", i));
                        Ok(expr.alias(alias_name))
                    })
                    .collect::<Result<_>>()?;
                LogicalPlanBuilder::from(input_plan)
                    .window(df_window_exprs)
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::Qualify { input, predicate } => {
                let input_plan = self.convert_plan_inner(input)?;
                self.set_outer_aliases(input);
                let predicate_expr = self.convert_expr(predicate)?;
                LogicalPlanBuilder::from(input_plan)
                    .filter(predicate_expr)
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::WithCte { body, .. } => self.convert_plan_inner(body),

            LogicalPlan::Unnest {
                input,
                columns,
                schema: _,
            } => {
                let input_plan = self.convert_plan_inner(input)?;
                let mut builder = LogicalPlanBuilder::from(input_plan);

                struct UnnestInfo {
                    internal_name: String,
                    output_alias: Option<String>,
                    with_offset: bool,
                    offset_alias: Option<String>,
                }

                let mut expr_projections: Vec<(DFExpr, String)> = Vec::new();
                let mut unnest_infos: Vec<UnnestInfo> = Vec::new();

                for (idx, col) in columns.iter().enumerate() {
                    let internal_name = match &col.expr {
                        yachtsql_ir::Expr::Column { name, .. } => name.clone(),
                        yachtsql_ir::Expr::Alias { name, .. } => name.clone(),
                        yachtsql_ir::Expr::Literal(_)
                        | yachtsql_ir::Expr::BinaryOp { .. }
                        | yachtsql_ir::Expr::UnaryOp { .. }
                        | yachtsql_ir::Expr::ScalarFunction { .. }
                        | yachtsql_ir::Expr::Aggregate { .. }
                        | yachtsql_ir::Expr::UserDefinedAggregate { .. }
                        | yachtsql_ir::Expr::Window { .. }
                        | yachtsql_ir::Expr::AggregateWindow { .. }
                        | yachtsql_ir::Expr::Case { .. }
                        | yachtsql_ir::Expr::Cast { .. }
                        | yachtsql_ir::Expr::IsNull { .. }
                        | yachtsql_ir::Expr::IsDistinctFrom { .. }
                        | yachtsql_ir::Expr::InList { .. }
                        | yachtsql_ir::Expr::InSubquery { .. }
                        | yachtsql_ir::Expr::InUnnest { .. }
                        | yachtsql_ir::Expr::Exists { .. }
                        | yachtsql_ir::Expr::Between { .. }
                        | yachtsql_ir::Expr::Like { .. }
                        | yachtsql_ir::Expr::Extract { .. }
                        | yachtsql_ir::Expr::Substring { .. }
                        | yachtsql_ir::Expr::Trim { .. }
                        | yachtsql_ir::Expr::Position { .. }
                        | yachtsql_ir::Expr::Overlay { .. }
                        | yachtsql_ir::Expr::Array { .. }
                        | yachtsql_ir::Expr::ArrayAccess { .. }
                        | yachtsql_ir::Expr::Struct { .. }
                        | yachtsql_ir::Expr::StructAccess { .. }
                        | yachtsql_ir::Expr::TypedString { .. }
                        | yachtsql_ir::Expr::Interval { .. }
                        | yachtsql_ir::Expr::Wildcard { .. }
                        | yachtsql_ir::Expr::Subquery(_)
                        | yachtsql_ir::Expr::ScalarSubquery(_)
                        | yachtsql_ir::Expr::ArraySubquery(_)
                        | yachtsql_ir::Expr::Parameter { .. }
                        | yachtsql_ir::Expr::Variable { .. }
                        | yachtsql_ir::Expr::Placeholder { .. }
                        | yachtsql_ir::Expr::Lambda { .. }
                        | yachtsql_ir::Expr::AtTimeZone { .. }
                        | yachtsql_ir::Expr::JsonAccess { .. }
                        | yachtsql_ir::Expr::Default => {
                            let gen_name = format!("__unnest_expr_{}", idx);
                            let df_expr = self.convert_expr(&col.expr)?;
                            expr_projections.push((df_expr, gen_name.clone()));
                            gen_name
                        }
                    };
                    unnest_infos.push(UnnestInfo {
                        internal_name,
                        output_alias: col.alias.clone(),
                        with_offset: col.with_offset,
                        offset_alias: col.offset_alias.clone(),
                    });
                }

                if !expr_projections.is_empty() {
                    let current_schema = builder.schema();
                    let mut proj_exprs: Vec<DFExpr> = current_schema
                        .columns()
                        .iter()
                        .map(|c| DFExpr::Column(c.clone()))
                        .collect();
                    for (expr, name) in expr_projections {
                        proj_exprs.push(expr.alias(&name));
                    }
                    builder = builder
                        .project(proj_exprs)
                        .map_err(|e| Error::internal(e.to_string()))?;
                }

                for info in &unnest_infos {
                    let col_expr = datafusion::common::Column::new_unqualified(&info.internal_name);
                    builder = builder
                        .unnest_column(col_expr)
                        .map_err(|e| Error::internal(e.to_string()))?;
                }

                let has_offset = unnest_infos.iter().any(|info| info.with_offset);
                let has_alias = unnest_infos.iter().any(|info| info.output_alias.is_some());

                let schema = builder.schema();
                let all_columns: Vec<_> = schema.columns().into_iter().collect();

                if unnest_infos.len() == 1 {
                    let info = &unnest_infos[0];
                    if let Some(unnest_col) =
                        all_columns.iter().find(|c| c.name == info.internal_name)
                        && let Ok(field) = schema.field_from_column(unnest_col)
                        && let ArrowDataType::Struct(struct_fields) = field.data_type()
                    {
                        let other_cols: Vec<_> = all_columns
                            .iter()
                            .filter(|c| c.name != info.internal_name)
                            .collect();

                        let mut proj_exprs: Vec<DFExpr> = other_cols
                            .iter()
                            .map(|c| DFExpr::Column((*c).clone()))
                            .collect();

                        let table_alias = info.output_alias.as_ref();

                        for struct_field in struct_fields.iter() {
                            let field_name = struct_field.name();
                            let field_expr = datafusion::functions::core::expr_fn::get_field(
                                DFExpr::Column(unnest_col.clone()),
                                field_name.as_str(),
                            );

                            if let Some(alias) = table_alias {
                                let table_ref: Option<TableReference> =
                                    Some(TableReference::bare(alias.clone()));
                                proj_exprs.push(field_expr.alias_qualified(table_ref, field_name));
                            } else {
                                proj_exprs.push(field_expr.alias(field_name));
                            }
                        }

                        if info.with_offset {
                            let row_num_expr = datafusion::functions_window::expr_fn::row_number();
                            let offset_col_name = "__unnest_offset__";
                            builder = builder
                                .window(vec![row_num_expr.alias(offset_col_name)])
                                .map_err(|e| Error::internal(e.to_string()))?;

                            let offset_name = info.offset_alias.as_deref().unwrap_or("offset");
                            let offset_col = DFExpr::Column(
                                datafusion::common::Column::new_unqualified(offset_col_name),
                            );
                            proj_exprs.push(
                                (offset_col - datafusion::prelude::lit(1i64)).alias(offset_name),
                            );
                        }

                        builder = builder
                            .project(proj_exprs)
                            .map_err(|e| Error::internal(e.to_string()))?;

                        return builder.build().map_err(|e| Error::internal(e.to_string()));
                    }
                }

                if has_offset {
                    let row_num_expr = datafusion::functions_window::expr_fn::row_number();
                    let offset_col_name = "__unnest_offset__";
                    let window_expr = row_num_expr.alias(offset_col_name);
                    builder = builder
                        .window(vec![window_expr])
                        .map_err(|e| Error::internal(e.to_string()))?;

                    let schema = builder.schema();
                    let mut proj_exprs: Vec<DFExpr> = Vec::new();

                    for col in schema.columns().iter() {
                        if col.name == offset_col_name {
                            continue;
                        }
                        let expr = DFExpr::Column(col.clone());
                        if let Some(info) =
                            unnest_infos.iter().find(|i| i.internal_name == col.name)
                        {
                            if let Some(alias) = &info.output_alias {
                                proj_exprs.push(expr.alias(alias));
                            } else {
                                proj_exprs.push(expr);
                            }

                            if info.with_offset {
                                let offset_name = info.offset_alias.as_deref().unwrap_or("offset");
                                let offset_col = DFExpr::Column(
                                    datafusion::common::Column::new_unqualified(offset_col_name),
                                );
                                let offset_expr = (offset_col - datafusion::prelude::lit(1i64))
                                    .alias(offset_name);
                                proj_exprs.push(offset_expr);
                            }
                        } else {
                            proj_exprs.push(expr);
                        }
                    }
                    builder = builder
                        .project(proj_exprs)
                        .map_err(|e| Error::internal(e.to_string()))?;
                } else if has_alias {
                    let schema = builder.schema();
                    let mut proj_exprs: Vec<DFExpr> = Vec::new();

                    for col in schema.columns().iter() {
                        let expr = DFExpr::Column(col.clone());
                        if let Some(info) =
                            unnest_infos.iter().find(|i| i.internal_name == col.name)
                        {
                            if let Some(alias) = &info.output_alias {
                                proj_exprs.push(expr.alias(alias));
                            } else {
                                proj_exprs.push(expr);
                            }
                        } else {
                            proj_exprs.push(expr);
                        }
                    }
                    builder = builder
                        .project(proj_exprs)
                        .map_err(|e| Error::internal(e.to_string()))?;
                }

                builder.build().map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::Insert { .. }
            | LogicalPlan::Update { .. }
            | LogicalPlan::Delete { .. }
            | LogicalPlan::Merge { .. }
            | LogicalPlan::CreateTable { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::AlterTable { .. }
            | LogicalPlan::Truncate { .. }
            | LogicalPlan::CreateView { .. }
            | LogicalPlan::DropView { .. }
            | LogicalPlan::CreateSchema { .. }
            | LogicalPlan::DropSchema { .. }
            | LogicalPlan::UndropSchema { .. }
            | LogicalPlan::AlterSchema { .. }
            | LogicalPlan::CreateFunction { .. }
            | LogicalPlan::DropFunction { .. }
            | LogicalPlan::CreateProcedure { .. }
            | LogicalPlan::DropProcedure { .. }
            | LogicalPlan::Call { .. }
            | LogicalPlan::ExportData { .. }
            | LogicalPlan::LoadData { .. }
            | LogicalPlan::Declare { .. }
            | LogicalPlan::SetVariable { .. }
            | LogicalPlan::SetMultipleVariables { .. }
            | LogicalPlan::If { .. }
            | LogicalPlan::While { .. }
            | LogicalPlan::Loop { .. }
            | LogicalPlan::Block { .. }
            | LogicalPlan::Repeat { .. }
            | LogicalPlan::For { .. }
            | LogicalPlan::Return { .. }
            | LogicalPlan::Raise { .. }
            | LogicalPlan::ExecuteImmediate { .. }
            | LogicalPlan::Break { .. }
            | LogicalPlan::Continue { .. }
            | LogicalPlan::CreateSnapshot { .. }
            | LogicalPlan::DropSnapshot { .. }
            | LogicalPlan::Assert { .. }
            | LogicalPlan::Grant { .. }
            | LogicalPlan::Revoke { .. }
            | LogicalPlan::BeginTransaction
            | LogicalPlan::Commit
            | LogicalPlan::Rollback
            | LogicalPlan::TryCatch { .. }
            | LogicalPlan::GapFill { .. }
            | LogicalPlan::Explain { .. } => Err(Error::internal(format!(
                "Query conversion not implemented: {:?}",
                std::mem::discriminant(plan)
            ))),
        }
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    fn expr_needs_float_cast(expr: &yachtsql_ir::Expr) -> bool {
        match expr {
            yachtsql_ir::Expr::Literal(yachtsql_ir::Literal::Int64(_)) => true,
            yachtsql_ir::Expr::BinaryOp { left, right, .. } => {
                Self::expr_needs_float_cast(left) && Self::expr_needs_float_cast(right)
            }
            yachtsql_ir::Expr::UnaryOp { expr: inner, .. } => Self::expr_needs_float_cast(inner),
            _ => false,
        }
    }

    fn convert_expr(&self, expr: &yachtsql_ir::Expr) -> Result<DFExpr> {
        match expr {
            yachtsql_ir::Expr::UserDefinedAggregate {
                name,
                args,
                distinct,
                filter,
            } => {
                let df_args: Vec<DFExpr> = args
                    .iter()
                    .map(|a| self.convert_expr(a))
                    .collect::<Result<_>>()?;
                let df_filter = filter
                    .as_ref()
                    .map(|f| self.convert_expr(f))
                    .transpose()?
                    .map(Box::new);

                let lower_name = name.to_lowercase();
                let udaf = self
                    .ctx
                    .udaf(&lower_name)
                    .map_err(|e| Error::internal(e.to_string()))?;

                Ok(DFExpr::AggregateFunction(
                    datafusion::logical_expr::expr::AggregateFunction::new_udf(
                        udaf, df_args, *distinct, df_filter, None, None,
                    ),
                ))
            }

            yachtsql_ir::Expr::BinaryOp { left, op, right } => {
                let left_expr = self.convert_expr(left)?;
                let right_expr = self.convert_expr(right)?;

                let is_arithmetic = matches!(
                    op,
                    yachtsql_ir::BinaryOp::Add
                        | yachtsql_ir::BinaryOp::Sub
                        | yachtsql_ir::BinaryOp::Mul
                        | yachtsql_ir::BinaryOp::Div
                        | yachtsql_ir::BinaryOp::Mod
                );
                let left_is_null = matches!(&left_expr, DFExpr::Literal(ScalarValue::Null));
                let right_is_null = matches!(&right_expr, DFExpr::Literal(ScalarValue::Null));

                let (left_expr, right_expr) = if is_arithmetic && left_is_null && right_is_null {
                    (
                        DFExpr::Literal(ScalarValue::Int64(None)),
                        DFExpr::Literal(ScalarValue::Int64(None)),
                    )
                } else if is_arithmetic && left_is_null {
                    (DFExpr::Literal(ScalarValue::Int64(None)), right_expr)
                } else if is_arithmetic && right_is_null {
                    (left_expr, DFExpr::Literal(ScalarValue::Int64(None)))
                } else {
                    (left_expr, right_expr)
                };

                if *op == yachtsql_ir::BinaryOp::Div {
                    let left_needs_cast = Self::expr_needs_float_cast(left);
                    let right_needs_cast = Self::expr_needs_float_cast(right);
                    if left_needs_cast || right_needs_cast {
                        let left_cast = DFExpr::Cast(datafusion::logical_expr::Cast::new(
                            Box::new(left_expr),
                            datafusion::arrow::datatypes::DataType::Float64,
                        ));
                        let right_cast = DFExpr::Cast(datafusion::logical_expr::Cast::new(
                            Box::new(right_expr),
                            datafusion::arrow::datatypes::DataType::Float64,
                        ));
                        return Ok(DFExpr::BinaryExpr(
                            datafusion::logical_expr::BinaryExpr::new(
                                Box::new(left_cast),
                                datafusion::logical_expr::Operator::Divide,
                                Box::new(right_cast),
                            ),
                        ));
                    }
                }

                let operator = match op {
                    yachtsql_ir::BinaryOp::Add => datafusion::logical_expr::Operator::Plus,
                    yachtsql_ir::BinaryOp::Sub => datafusion::logical_expr::Operator::Minus,
                    yachtsql_ir::BinaryOp::Mul => datafusion::logical_expr::Operator::Multiply,
                    yachtsql_ir::BinaryOp::Div => datafusion::logical_expr::Operator::Divide,
                    yachtsql_ir::BinaryOp::Mod => datafusion::logical_expr::Operator::Modulo,
                    yachtsql_ir::BinaryOp::Eq => datafusion::logical_expr::Operator::Eq,
                    yachtsql_ir::BinaryOp::NotEq => datafusion::logical_expr::Operator::NotEq,
                    yachtsql_ir::BinaryOp::Lt => datafusion::logical_expr::Operator::Lt,
                    yachtsql_ir::BinaryOp::LtEq => datafusion::logical_expr::Operator::LtEq,
                    yachtsql_ir::BinaryOp::Gt => datafusion::logical_expr::Operator::Gt,
                    yachtsql_ir::BinaryOp::GtEq => datafusion::logical_expr::Operator::GtEq,
                    yachtsql_ir::BinaryOp::And => datafusion::logical_expr::Operator::And,
                    yachtsql_ir::BinaryOp::Or => datafusion::logical_expr::Operator::Or,
                    yachtsql_ir::BinaryOp::BitwiseAnd => {
                        datafusion::logical_expr::Operator::BitwiseAnd
                    }
                    yachtsql_ir::BinaryOp::BitwiseOr => {
                        datafusion::logical_expr::Operator::BitwiseOr
                    }
                    yachtsql_ir::BinaryOp::BitwiseXor => {
                        datafusion::logical_expr::Operator::BitwiseXor
                    }
                    yachtsql_ir::BinaryOp::ShiftLeft => {
                        datafusion::logical_expr::Operator::BitwiseShiftLeft
                    }
                    yachtsql_ir::BinaryOp::ShiftRight => {
                        datafusion::logical_expr::Operator::BitwiseShiftRight
                    }
                    yachtsql_ir::BinaryOp::Concat => {
                        datafusion::logical_expr::Operator::StringConcat
                    }
                };
                Ok(DFExpr::BinaryExpr(
                    datafusion::logical_expr::BinaryExpr::new(
                        Box::new(left_expr),
                        operator,
                        Box::new(right_expr),
                    ),
                ))
            }

            yachtsql_ir::Expr::UnaryOp { op, expr: inner } => {
                let inner_expr = self.convert_expr(inner)?;
                match op {
                    yachtsql_ir::UnaryOp::Not => Ok(DFExpr::Not(Box::new(inner_expr))),
                    yachtsql_ir::UnaryOp::Minus => Ok(DFExpr::Negative(Box::new(inner_expr))),
                    yachtsql_ir::UnaryOp::Plus => Ok(inner_expr),
                    yachtsql_ir::UnaryOp::BitwiseNot => Ok(DFExpr::BinaryExpr(
                        datafusion::logical_expr::BinaryExpr::new(
                            Box::new(DFExpr::Literal(datafusion::scalar::ScalarValue::Int64(
                                Some(-1),
                            ))),
                            datafusion::logical_expr::Operator::BitwiseXor,
                            Box::new(inner_expr),
                        ),
                    )),
                }
            }

            yachtsql_ir::Expr::ScalarSubquery(subquery) | yachtsql_ir::Expr::Subquery(subquery) => {
                let subquery_tables = extract_tables_from_plan(subquery);
                let df_plan = self.convert_subquery_plan(subquery, &subquery_tables)?;
                let outer_refs = df_plan.all_out_ref_exprs();
                let subq = Subquery {
                    subquery: Arc::new(df_plan),
                    outer_ref_columns: outer_refs,
                };
                Ok(DFExpr::ScalarSubquery(subq))
            }

            yachtsql_ir::Expr::ArraySubquery(subquery) => {
                let subquery_tables = extract_tables_from_plan(subquery);
                let df_plan = self.convert_subquery_plan(subquery, &subquery_tables)?;
                let outer_refs = df_plan.all_out_ref_exprs();
                let (inner_plan, order_by) = extract_sort_from_plan(&df_plan);
                let schema = inner_plan.schema();
                if schema.fields().len() != 1 {
                    return Err(Error::internal(format!(
                        "ARRAY subquery must return exactly one column, got {}",
                        schema.fields().len()
                    )));
                }
                let col_name = schema.fields()[0].name().clone();
                let agg_expr = datafusion::functions_aggregate::array_agg::array_agg(
                    DFExpr::Column(datafusion::common::Column::new_unqualified(col_name)),
                );
                let agg_expr_with_order = if let Some(order_exprs) = order_by {
                    use datafusion::logical_expr::ExprFunctionExt;
                    agg_expr
                        .order_by(order_exprs)
                        .build()
                        .map_err(|e| Error::internal(e.to_string()))?
                } else {
                    agg_expr
                };
                let agg_plan = LogicalPlanBuilder::from(inner_plan)
                    .aggregate(Vec::<DFExpr>::new(), vec![agg_expr_with_order])
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))?;
                let subq = Subquery {
                    subquery: Arc::new(agg_plan),
                    outer_ref_columns: outer_refs,
                };
                Ok(DFExpr::ScalarSubquery(subq))
            }

            yachtsql_ir::Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let left_expr = self.convert_expr(expr)?;
                let subquery_tables = extract_tables_from_plan(subquery);
                let df_plan = self.convert_subquery_plan(subquery, &subquery_tables)?;
                let outer_refs = df_plan.all_out_ref_exprs();
                let subq = Subquery {
                    subquery: Arc::new(df_plan),
                    outer_ref_columns: outer_refs,
                };
                Ok(DFExpr::InSubquery(
                    datafusion::logical_expr::expr::InSubquery::new(
                        Box::new(left_expr),
                        subq,
                        *negated,
                    ),
                ))
            }

            yachtsql_ir::Expr::Exists { subquery, negated } => {
                let subquery_tables = extract_tables_from_plan(subquery);
                let df_plan = self.convert_subquery_plan(subquery, &subquery_tables)?;
                let outer_refs = df_plan.all_out_ref_exprs();
                let subq = Subquery {
                    subquery: Arc::new(df_plan),
                    outer_ref_columns: outer_refs,
                };
                Ok(DFExpr::Exists(datafusion::logical_expr::expr::Exists::new(
                    subq, *negated,
                )))
            }

            yachtsql_ir::Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                let operand_expr = operand.as_ref().map(|o| self.convert_expr(o)).transpose()?;
                let when_then: Vec<(Box<DFExpr>, Box<DFExpr>)> = when_clauses
                    .iter()
                    .map(|wc| {
                        let when = self.convert_expr(&wc.condition)?;
                        let then = self.convert_expr(&wc.result)?;
                        Ok((Box::new(when), Box::new(then)))
                    })
                    .collect::<Result<_>>()?;
                let else_expr = else_result
                    .as_ref()
                    .map(|e| self.convert_expr(e))
                    .transpose()?
                    .map(Box::new);
                Ok(DFExpr::Case(datafusion::logical_expr::Case::new(
                    operand_expr.map(Box::new),
                    when_then,
                    else_expr,
                )))
            }

            yachtsql_ir::Expr::Column {
                table: Some(table_name),
                name,
                index: None,
                ..
            } => {
                let var_name = table_name.to_lowercase();
                let field_name = name.to_lowercase();
                let variables = self.variables.read();
                if let Some(ScalarValue::Struct(struct_arr)) = variables.get(&var_name) {
                    let schema = struct_arr.fields();
                    for (idx, field) in schema.iter().enumerate() {
                        if field.name().to_lowercase() == field_name {
                            let col = struct_arr.column(idx);
                            if !col.is_empty() {
                                let val = ScalarValue::try_from_array(col, 0)
                                    .map_err(|e| Error::internal(e.to_string()))?;
                                return Ok(DFExpr::Literal(val));
                            }
                        }
                    }
                }
                yachtsql_parser::DataFusionConverter::convert_expr(expr)
                    .map_err(|e| Error::internal(e.to_string()))
            }

            yachtsql_ir::Expr::Column {
                table: None,
                name,
                index: None,
                ..
            } => {
                let var_name = name.to_lowercase();
                let variables = self.variables.read();
                if let Some(value) = variables.get(&var_name) {
                    Ok(DFExpr::Literal(value.clone()))
                } else {
                    yachtsql_parser::DataFusionConverter::convert_expr(expr)
                        .map_err(|e| Error::internal(e.to_string()))
                }
            }

            yachtsql_ir::Expr::Literal(_)
            | yachtsql_ir::Expr::Column { .. }
            | yachtsql_ir::Expr::ScalarFunction { .. }
            | yachtsql_ir::Expr::Aggregate { .. }
            | yachtsql_ir::Expr::Window { .. }
            | yachtsql_ir::Expr::AggregateWindow { .. }
            | yachtsql_ir::Expr::Cast { .. }
            | yachtsql_ir::Expr::IsNull { .. }
            | yachtsql_ir::Expr::IsDistinctFrom { .. }
            | yachtsql_ir::Expr::InList { .. }
            | yachtsql_ir::Expr::InUnnest { .. }
            | yachtsql_ir::Expr::Between { .. }
            | yachtsql_ir::Expr::Like { .. }
            | yachtsql_ir::Expr::Extract { .. }
            | yachtsql_ir::Expr::Substring { .. }
            | yachtsql_ir::Expr::Trim { .. }
            | yachtsql_ir::Expr::Position { .. }
            | yachtsql_ir::Expr::Overlay { .. }
            | yachtsql_ir::Expr::Array { .. }
            | yachtsql_ir::Expr::ArrayAccess { .. }
            | yachtsql_ir::Expr::Struct { .. }
            | yachtsql_ir::Expr::StructAccess { .. }
            | yachtsql_ir::Expr::TypedString { .. }
            | yachtsql_ir::Expr::Interval { .. }
            | yachtsql_ir::Expr::Alias { .. }
            | yachtsql_ir::Expr::Wildcard { .. }
            | yachtsql_ir::Expr::Parameter { .. }
            | yachtsql_ir::Expr::Placeholder { .. }
            | yachtsql_ir::Expr::Lambda { .. }
            | yachtsql_ir::Expr::AtTimeZone { .. }
            | yachtsql_ir::Expr::JsonAccess { .. }
            | yachtsql_ir::Expr::Default => {
                yachtsql_parser::DataFusionConverter::convert_expr(expr)
                    .map_err(|e| Error::internal(e.to_string()))
            }

            yachtsql_ir::Expr::Variable { name } => {
                let var_name = name.trim_start_matches('@').to_lowercase();
                let variables = self.variables.read();
                let value = variables
                    .get(&var_name)
                    .cloned()
                    .unwrap_or(ScalarValue::Null);
                Ok(DFExpr::Literal(value))
            }
        }
    }

    fn convert_sort_expr_with_schema(
        &self,
        se: &SortExpr,
        input_schema: &datafusion::common::DFSchemaRef,
    ) -> Result<DFSortExpr> {
        let expr = self.convert_expr_with_schema(&se.expr, input_schema)?;
        Ok(DFSortExpr {
            expr,
            asc: se.asc,
            nulls_first: se.nulls_first,
        })
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    fn convert_expr_with_schema(
        &self,
        expr: &yachtsql_ir::Expr,
        input_schema: &datafusion::common::DFSchemaRef,
    ) -> Result<DFExpr> {
        match expr {
            yachtsql_ir::Expr::Column { table, name, .. } => {
                if let Some(t) = table {
                    let qualified = datafusion::common::Column::new(Some(t.clone()), name.clone());
                    if input_schema.has_column(&qualified) {
                        return Ok(DFExpr::Column(qualified));
                    }
                    let unqualified = datafusion::common::Column::new_unqualified(name.clone());
                    if input_schema.has_column(&unqualified) {
                        return Ok(DFExpr::Column(unqualified));
                    }
                }
                self.convert_expr(expr)
            }
            _ => self.convert_expr(expr),
        }
    }

    fn convert_subquery_plan(
        &self,
        plan: &LogicalPlan,
        subquery_tables: &HashSet<String>,
    ) -> Result<DFLogicalPlan> {
        match plan {
            LogicalPlan::Scan {
                table_name,
                schema,
                projection,
            } => {
                let (schema_name, table) = self.resolve_table_name(table_name);
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

            LogicalPlan::Filter { input, predicate } => {
                let input_plan = self.convert_subquery_plan(input, subquery_tables)?;
                let predicate_expr = self.convert_subquery_expr(predicate, subquery_tables)?;
                LogicalPlanBuilder::from(input_plan)
                    .filter(predicate_expr)
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::Project {
                input,
                expressions,
                schema,
            } => {
                let input_plan = self.convert_subquery_plan(input, subquery_tables)?;
                let project_exprs: Vec<DFExpr> = expressions
                    .iter()
                    .zip(schema.fields.iter())
                    .map(|(e, field)| {
                        let df_expr = self.convert_subquery_expr(e, subquery_tables)?;
                        let expr_name = df_expr.schema_name().to_string();
                        if expr_name != field.name {
                            Ok(df_expr.alias(&field.name))
                        } else {
                            Ok(df_expr)
                        }
                    })
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
                schema,
                ..
            } => {
                let input_plan = self.convert_subquery_plan(input, subquery_tables)?;
                let group_exprs: Vec<DFExpr> = group_by
                    .iter()
                    .map(|e| self.convert_subquery_expr(e, subquery_tables))
                    .collect::<Result<_>>()?;
                let agg_exprs: Vec<DFExpr> = aggregates
                    .iter()
                    .zip(schema.fields.iter().skip(group_by.len()))
                    .map(|(e, field)| {
                        let df_expr = self.convert_subquery_expr(e, subquery_tables)?;
                        Ok(df_expr.alias(field.name.clone()))
                    })
                    .collect::<Result<_>>()?;
                LogicalPlanBuilder::from(input_plan)
                    .aggregate(group_exprs, agg_exprs)
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::Sort { input, sort_exprs } => {
                let input_plan = self.convert_subquery_plan(input, subquery_tables)?;
                let df_sort_exprs: Vec<DFSortExpr> = sort_exprs
                    .iter()
                    .map(|se| {
                        let expr = self.convert_subquery_expr(&se.expr, subquery_tables)?;
                        Ok(DFSortExpr {
                            expr,
                            asc: se.asc,
                            nulls_first: se.nulls_first,
                        })
                    })
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
                let input_plan = self.convert_subquery_plan(input, subquery_tables)?;
                let skip = offset.unwrap_or(0);
                let fetch = *limit;
                LogicalPlanBuilder::from(input_plan)
                    .limit(skip, fetch)
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::Distinct { input } => {
                let input_plan = self.convert_subquery_plan(input, subquery_tables)?;
                LogicalPlanBuilder::from(input_plan)
                    .distinct()
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))
            }

            LogicalPlan::Sample { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::Empty { .. }
            | LogicalPlan::SetOperation { .. }
            | LogicalPlan::Window { .. }
            | LogicalPlan::WithCte { .. }
            | LogicalPlan::Unnest { .. }
            | LogicalPlan::Qualify { .. }
            | LogicalPlan::Insert { .. }
            | LogicalPlan::Update { .. }
            | LogicalPlan::Delete { .. }
            | LogicalPlan::Merge { .. }
            | LogicalPlan::CreateTable { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::AlterTable { .. }
            | LogicalPlan::Truncate { .. }
            | LogicalPlan::CreateView { .. }
            | LogicalPlan::DropView { .. }
            | LogicalPlan::CreateSchema { .. }
            | LogicalPlan::DropSchema { .. }
            | LogicalPlan::UndropSchema { .. }
            | LogicalPlan::AlterSchema { .. }
            | LogicalPlan::CreateFunction { .. }
            | LogicalPlan::DropFunction { .. }
            | LogicalPlan::CreateProcedure { .. }
            | LogicalPlan::DropProcedure { .. }
            | LogicalPlan::Call { .. }
            | LogicalPlan::ExportData { .. }
            | LogicalPlan::LoadData { .. }
            | LogicalPlan::Declare { .. }
            | LogicalPlan::SetVariable { .. }
            | LogicalPlan::SetMultipleVariables { .. }
            | LogicalPlan::If { .. }
            | LogicalPlan::While { .. }
            | LogicalPlan::Loop { .. }
            | LogicalPlan::Block { .. }
            | LogicalPlan::Repeat { .. }
            | LogicalPlan::For { .. }
            | LogicalPlan::Return { .. }
            | LogicalPlan::Raise { .. }
            | LogicalPlan::ExecuteImmediate { .. }
            | LogicalPlan::Break { .. }
            | LogicalPlan::Continue { .. }
            | LogicalPlan::CreateSnapshot { .. }
            | LogicalPlan::DropSnapshot { .. }
            | LogicalPlan::Assert { .. }
            | LogicalPlan::Grant { .. }
            | LogicalPlan::Revoke { .. }
            | LogicalPlan::BeginTransaction
            | LogicalPlan::Commit
            | LogicalPlan::Rollback
            | LogicalPlan::TryCatch { .. }
            | LogicalPlan::GapFill { .. }
            | LogicalPlan::Explain { .. } => self.convert_plan_inner(plan),
        }
    }

    fn convert_subquery_expr(
        &self,
        expr: &yachtsql_ir::Expr,
        subquery_tables: &HashSet<String>,
    ) -> Result<DFExpr> {
        match expr {
            yachtsql_ir::Expr::Column { table, name, .. } => {
                if let Some(t) = table {
                    let t_lower = t.to_lowercase();
                    if !subquery_tables.contains(&t_lower) {
                        let data_type = self.lookup_column_type(t, name);
                        return Ok(DFExpr::OuterReferenceColumn(
                            data_type,
                            datafusion::common::Column::new(Some(t.clone()), name.clone()),
                        ));
                    }
                }
                let col = match table {
                    Some(t) => datafusion::common::Column::new(Some(t.clone()), name.clone()),
                    None => datafusion::common::Column::new_unqualified(name.clone()),
                };
                Ok(DFExpr::Column(col))
            }

            yachtsql_ir::Expr::BinaryOp { left, op, right } => {
                let left_expr = self.convert_subquery_expr(left, subquery_tables)?;
                let right_expr = self.convert_subquery_expr(right, subquery_tables)?;

                if *op == yachtsql_ir::BinaryOp::Div {
                    let left_needs_cast = Self::expr_needs_float_cast(left);
                    let right_needs_cast = Self::expr_needs_float_cast(right);
                    if left_needs_cast || right_needs_cast {
                        let left_cast = DFExpr::Cast(datafusion::logical_expr::Cast::new(
                            Box::new(left_expr),
                            datafusion::arrow::datatypes::DataType::Float64,
                        ));
                        let right_cast = DFExpr::Cast(datafusion::logical_expr::Cast::new(
                            Box::new(right_expr),
                            datafusion::arrow::datatypes::DataType::Float64,
                        ));
                        return Ok(DFExpr::BinaryExpr(
                            datafusion::logical_expr::BinaryExpr::new(
                                Box::new(left_cast),
                                datafusion::logical_expr::Operator::Divide,
                                Box::new(right_cast),
                            ),
                        ));
                    }
                }

                let operator = match op {
                    yachtsql_ir::BinaryOp::Add => datafusion::logical_expr::Operator::Plus,
                    yachtsql_ir::BinaryOp::Sub => datafusion::logical_expr::Operator::Minus,
                    yachtsql_ir::BinaryOp::Mul => datafusion::logical_expr::Operator::Multiply,
                    yachtsql_ir::BinaryOp::Div => datafusion::logical_expr::Operator::Divide,
                    yachtsql_ir::BinaryOp::Mod => datafusion::logical_expr::Operator::Modulo,
                    yachtsql_ir::BinaryOp::Eq => datafusion::logical_expr::Operator::Eq,
                    yachtsql_ir::BinaryOp::NotEq => datafusion::logical_expr::Operator::NotEq,
                    yachtsql_ir::BinaryOp::Lt => datafusion::logical_expr::Operator::Lt,
                    yachtsql_ir::BinaryOp::LtEq => datafusion::logical_expr::Operator::LtEq,
                    yachtsql_ir::BinaryOp::Gt => datafusion::logical_expr::Operator::Gt,
                    yachtsql_ir::BinaryOp::GtEq => datafusion::logical_expr::Operator::GtEq,
                    yachtsql_ir::BinaryOp::And => datafusion::logical_expr::Operator::And,
                    yachtsql_ir::BinaryOp::Or => datafusion::logical_expr::Operator::Or,
                    yachtsql_ir::BinaryOp::BitwiseAnd => {
                        datafusion::logical_expr::Operator::BitwiseAnd
                    }
                    yachtsql_ir::BinaryOp::BitwiseOr => {
                        datafusion::logical_expr::Operator::BitwiseOr
                    }
                    yachtsql_ir::BinaryOp::BitwiseXor => {
                        datafusion::logical_expr::Operator::BitwiseXor
                    }
                    yachtsql_ir::BinaryOp::ShiftLeft => {
                        datafusion::logical_expr::Operator::BitwiseShiftLeft
                    }
                    yachtsql_ir::BinaryOp::ShiftRight => {
                        datafusion::logical_expr::Operator::BitwiseShiftRight
                    }
                    yachtsql_ir::BinaryOp::Concat => {
                        datafusion::logical_expr::Operator::StringConcat
                    }
                };
                Ok(DFExpr::BinaryExpr(
                    datafusion::logical_expr::BinaryExpr::new(
                        Box::new(left_expr),
                        operator,
                        Box::new(right_expr),
                    ),
                ))
            }

            yachtsql_ir::Expr::Literal(_)
            | yachtsql_ir::Expr::UnaryOp { .. }
            | yachtsql_ir::Expr::ScalarFunction { .. }
            | yachtsql_ir::Expr::Aggregate { .. }
            | yachtsql_ir::Expr::UserDefinedAggregate { .. }
            | yachtsql_ir::Expr::Window { .. }
            | yachtsql_ir::Expr::AggregateWindow { .. }
            | yachtsql_ir::Expr::Case { .. }
            | yachtsql_ir::Expr::Cast { .. }
            | yachtsql_ir::Expr::IsNull { .. }
            | yachtsql_ir::Expr::IsDistinctFrom { .. }
            | yachtsql_ir::Expr::InList { .. }
            | yachtsql_ir::Expr::InSubquery { .. }
            | yachtsql_ir::Expr::InUnnest { .. }
            | yachtsql_ir::Expr::Exists { .. }
            | yachtsql_ir::Expr::Between { .. }
            | yachtsql_ir::Expr::Like { .. }
            | yachtsql_ir::Expr::Extract { .. }
            | yachtsql_ir::Expr::Substring { .. }
            | yachtsql_ir::Expr::Trim { .. }
            | yachtsql_ir::Expr::Position { .. }
            | yachtsql_ir::Expr::Overlay { .. }
            | yachtsql_ir::Expr::Array { .. }
            | yachtsql_ir::Expr::ArrayAccess { .. }
            | yachtsql_ir::Expr::Struct { .. }
            | yachtsql_ir::Expr::StructAccess { .. }
            | yachtsql_ir::Expr::TypedString { .. }
            | yachtsql_ir::Expr::Interval { .. }
            | yachtsql_ir::Expr::Alias { .. }
            | yachtsql_ir::Expr::Wildcard { .. }
            | yachtsql_ir::Expr::Subquery(_)
            | yachtsql_ir::Expr::ScalarSubquery(_)
            | yachtsql_ir::Expr::ArraySubquery(_)
            | yachtsql_ir::Expr::Parameter { .. }
            | yachtsql_ir::Expr::Placeholder { .. }
            | yachtsql_ir::Expr::Lambda { .. }
            | yachtsql_ir::Expr::AtTimeZone { .. }
            | yachtsql_ir::Expr::JsonAccess { .. }
            | yachtsql_ir::Expr::Default => {
                yachtsql_parser::DataFusionConverter::convert_expr(expr)
                    .map_err(|e| Error::internal(e.to_string()))
            }

            yachtsql_ir::Expr::Variable { name } => {
                let var_name = name.trim_start_matches('@').to_lowercase();
                let variables = self.variables.read();
                let value = variables
                    .get(&var_name)
                    .cloned()
                    .unwrap_or(ScalarValue::Null);
                Ok(DFExpr::Literal(value))
            }
        }
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
            vec![vec![]]
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

        let mut defaults: HashMap<String, yachtsql_ir::Expr> = HashMap::new();
        for col in columns {
            if let Some(ref default_expr) = col.default_value {
                defaults.insert(col.name.to_lowercase(), default_expr.clone());
            }
        }
        if !defaults.is_empty() {
            let full_name = match schema_name {
                Some(s) => format!("{}.{}", s, table),
                None => table.clone(),
            };
            self.column_defaults.write().insert(full_name, defaults);
        }

        Ok(vec![])
    }

    async fn execute_drop_table(
        &self,
        table_names: &[String],
        if_exists: bool,
    ) -> Result<Vec<RecordBatch>> {
        for table_name in table_names {
            let (schema_name, table) = self.resolve_table_name(table_name);

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
        columns: &[String],
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

        let full_table_name = match schema_name {
            Some(ref s) => format!("{}.{}", s, table),
            None => table.clone(),
        };

        let defaults = self.column_defaults.read().get(&full_table_name).cloned();
        let new_batches = if let Some(ref defaults) = defaults {
            if !columns.is_empty() {
                let columns_lower: Vec<String> = columns.iter().map(|c| c.to_lowercase()).collect();
                self.apply_column_defaults(&new_batches, &table_schema, &columns_lower, defaults)
                    .await?
            } else {
                new_batches
            }
        } else {
            new_batches
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
                vec![vec![]]
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
                vec![vec![]]
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
                let mem_table = MemTable::try_new(schema, vec![vec![]])
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

    async fn execute_merge(
        &self,
        target_table: &str,
        source: &LogicalPlan,
        on: &yachtsql_ir::Expr,
        clauses: &[MergeClause],
    ) -> Result<Vec<RecordBatch>> {
        let lower = target_table.to_lowercase();
        let provider = self
            .ctx
            .table_provider(TableReference::bare(lower.clone()))
            .now_or_never()
            .ok_or_else(|| Error::internal(format!("Table not found: {}", target_table)))?
            .map_err(|e| Error::internal(e.to_string()))?;

        let target_schema = provider.schema();
        let target_source = provider_as_source(provider);

        let target_scan = LogicalPlanBuilder::scan(&lower, target_source, None)
            .map_err(|e| Error::internal(e.to_string()))?
            .build()
            .map_err(|e| Error::internal(e.to_string()))?;

        let target_df = DataFrame::new(self.ctx.state(), target_scan);
        let target_batches = target_df
            .collect()
            .await
            .map_err(|e| Error::internal(e.to_string()))?;

        let source_batches = self.execute_query(source).await?;

        if target_batches.is_empty() && source_batches.is_empty() {
            return Ok(vec![]);
        }

        let source_schema = if source_batches.is_empty() {
            target_schema.clone()
        } else {
            source_batches[0].schema()
        };

        let on_sql = self.expr_to_sql(on)?;

        let mut target_matched: HashMap<usize, Vec<usize>> = HashMap::new();
        let mut source_matched: HashSet<usize> = HashSet::new();

        let mut global_target_idx = 0usize;
        for target_batch in &target_batches {
            for target_row in 0..target_batch.num_rows() {
                let mut global_source_idx = 0usize;
                for source_batch in &source_batches {
                    for source_row in 0..source_batch.num_rows() {
                        let matches = self
                            .evaluate_merge_condition(
                                target_batch,
                                target_row,
                                source_batch,
                                source_row,
                                &on_sql,
                            )
                            .await?;

                        if matches {
                            target_matched
                                .entry(global_target_idx)
                                .or_default()
                                .push(global_source_idx);
                            source_matched.insert(global_source_idx);
                        }
                        global_source_idx += 1;
                    }
                }
                global_target_idx += 1;
            }
        }

        for (target_idx, source_indices) in &target_matched {
            if source_indices.len() > 1 {
                return Err(Error::internal(format!(
                    "MERGE statement matched multiple source rows for target row {}",
                    target_idx
                )));
            }
        }

        let mut result_rows: Vec<Vec<ScalarValue>> = Vec::new();
        let mut deleted_targets: HashSet<usize> = HashSet::new();

        let mut global_target_idx = 0usize;
        for target_batch in &target_batches {
            for target_row in 0..target_batch.num_rows() {
                let matched_source = target_matched.get(&global_target_idx);

                match matched_source {
                    Some(source_indices) => {
                        let source_idx = source_indices[0];
                        let (source_batch, source_row) =
                            self.get_source_row(&source_batches, source_idx);

                        let mut handled = false;
                        for clause in clauses {
                            match clause {
                                MergeClause::MatchedUpdate {
                                    condition,
                                    assignments,
                                } => {
                                    if handled {
                                        continue;
                                    }
                                    let cond_met = self
                                        .evaluate_merge_clause_condition(
                                            condition.as_ref(),
                                            target_batch,
                                            target_row,
                                            Some((source_batch, source_row)),
                                        )
                                        .await?;
                                    if cond_met {
                                        let row = self
                                            .apply_merge_update(
                                                target_batch,
                                                target_row,
                                                source_batch,
                                                source_row,
                                                assignments,
                                                &target_schema,
                                            )
                                            .await?;
                                        result_rows.push(row);
                                        handled = true;
                                    }
                                }
                                MergeClause::MatchedDelete { condition } => {
                                    if handled {
                                        continue;
                                    }
                                    let cond_met = self
                                        .evaluate_merge_clause_condition(
                                            condition.as_ref(),
                                            target_batch,
                                            target_row,
                                            Some((source_batch, source_row)),
                                        )
                                        .await?;
                                    if cond_met {
                                        deleted_targets.insert(global_target_idx);
                                        handled = true;
                                    }
                                }
                                MergeClause::NotMatched { .. }
                                | MergeClause::NotMatchedBySource { .. }
                                | MergeClause::NotMatchedBySourceDelete { .. } => {}
                            }
                        }
                        if !handled && !deleted_targets.contains(&global_target_idx) {
                            let row = self.extract_target_row(target_batch, target_row)?;
                            result_rows.push(row);
                        }
                    }
                    None => {
                        let mut handled = false;
                        for clause in clauses {
                            match clause {
                                MergeClause::NotMatchedBySource {
                                    condition,
                                    assignments,
                                } => {
                                    if handled {
                                        continue;
                                    }
                                    let cond_met = self
                                        .evaluate_merge_clause_condition(
                                            condition.as_ref(),
                                            target_batch,
                                            target_row,
                                            None,
                                        )
                                        .await?;
                                    if cond_met {
                                        let row = self
                                            .apply_merge_update_target_only(
                                                target_batch,
                                                target_row,
                                                assignments,
                                                &target_schema,
                                            )
                                            .await?;
                                        result_rows.push(row);
                                        handled = true;
                                    }
                                }
                                MergeClause::NotMatchedBySourceDelete { condition } => {
                                    if handled {
                                        continue;
                                    }
                                    let cond_met = self
                                        .evaluate_merge_clause_condition(
                                            condition.as_ref(),
                                            target_batch,
                                            target_row,
                                            None,
                                        )
                                        .await?;
                                    if cond_met {
                                        deleted_targets.insert(global_target_idx);
                                        handled = true;
                                    }
                                }
                                MergeClause::MatchedUpdate { .. }
                                | MergeClause::MatchedDelete { .. }
                                | MergeClause::NotMatched { .. } => {}
                            }
                        }
                        if !handled && !deleted_targets.contains(&global_target_idx) {
                            let row = self.extract_target_row(target_batch, target_row)?;
                            result_rows.push(row);
                        }
                    }
                }
                global_target_idx += 1;
            }
        }

        let total_source_rows: usize = source_batches.iter().map(|b| b.num_rows()).sum();
        for source_idx in 0..total_source_rows {
            if source_matched.contains(&source_idx) {
                continue;
            }

            let (source_batch, source_row) = self.get_source_row(&source_batches, source_idx);

            for clause in clauses {
                if let MergeClause::NotMatched {
                    condition,
                    columns,
                    values,
                } = clause
                {
                    let cond_met = self
                        .evaluate_not_matched_condition(
                            condition.as_ref(),
                            source_batch,
                            source_row,
                        )
                        .await?;
                    if cond_met {
                        let row = self
                            .build_insert_row(
                                source_batch,
                                source_row,
                                columns,
                                values,
                                &target_schema,
                                &source_schema,
                            )
                            .await?;
                        result_rows.push(row);
                        break;
                    }
                }
            }
        }

        let result_batches = self.build_result_batches(&result_rows, &target_schema)?;

        let _ = self.ctx.deregister_table(&lower);
        let partitions = if result_batches.is_empty() {
            vec![vec![]]
        } else {
            vec![result_batches]
        };
        let mem_table = MemTable::try_new(target_schema, partitions)
            .map_err(|e| Error::internal(e.to_string()))?;
        self.ctx
            .register_table(&lower, Arc::new(mem_table))
            .map_err(|e| Error::internal(e.to_string()))?;

        Ok(vec![])
    }

    async fn evaluate_merge_condition(
        &self,
        target_batch: &RecordBatch,
        target_row: usize,
        source_batch: &RecordBatch,
        source_row: usize,
        on_sql: &str,
    ) -> Result<bool> {
        let combined_schema = self.create_combined_schema(target_batch, source_batch)?;
        let combined_batch =
            self.create_combined_row(target_batch, target_row, source_batch, source_row)?;

        let mem_table = MemTable::try_new(combined_schema.clone(), vec![vec![combined_batch]])
            .map_err(|e| Error::internal(e.to_string()))?;

        let tmp_name = format!(
            "__tmp_merge_cond_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        self.ctx
            .register_table(&tmp_name, Arc::new(mem_table))
            .map_err(|e| Error::internal(e.to_string()))?;

        let query = format!("SELECT {} FROM {}", on_sql, tmp_name);
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

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(false);
        }

        let col = batches[0].column(0);
        if let Some(bool_arr) = col
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
        {
            return Ok(bool_arr.value(0));
        }

        Ok(false)
    }

    fn create_combined_schema(
        &self,
        target_batch: &RecordBatch,
        source_batch: &RecordBatch,
    ) -> Result<Arc<ArrowSchema>> {
        let mut fields: Vec<ArrowField> = Vec::new();

        for field in target_batch.schema().fields() {
            fields.push(ArrowField::new(
                field.name(),
                field.data_type().clone(),
                true,
            ));
        }

        for field in source_batch.schema().fields() {
            let name = if fields.iter().any(|f| f.name() == field.name()) {
                format!("__source_{}", field.name())
            } else {
                field.name().clone()
            };
            fields.push(ArrowField::new(name, field.data_type().clone(), true));
        }

        Ok(Arc::new(ArrowSchema::new(fields)))
    }

    fn create_combined_row(
        &self,
        target_batch: &RecordBatch,
        target_row: usize,
        source_batch: &RecordBatch,
        source_row: usize,
    ) -> Result<RecordBatch> {
        let mut columns: Vec<ArrayRef> = Vec::new();

        for col_idx in 0..target_batch.num_columns() {
            let col = target_batch.column(col_idx);
            let single = self.extract_single_value(col, target_row)?;
            columns.push(single);
        }

        for col_idx in 0..source_batch.num_columns() {
            let col = source_batch.column(col_idx);
            let single = self.extract_single_value(col, source_row)?;
            columns.push(single);
        }

        let schema = self.create_combined_schema(target_batch, source_batch)?;
        RecordBatch::try_new(schema, columns).map_err(|e| Error::internal(e.to_string()))
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    fn extract_single_value(&self, arr: &ArrayRef, idx: usize) -> Result<ArrayRef> {
        use datafusion::arrow::array::*;

        if arr.is_null(idx) {
            return Ok(new_null_array(arr.data_type(), 1));
        }

        match arr.data_type() {
            ArrowDataType::Int64 => {
                let val = arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(idx);
                Ok(Arc::new(Int64Array::from(vec![val])))
            }
            ArrowDataType::Float64 => {
                let val = arr
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(idx);
                Ok(Arc::new(Float64Array::from(vec![val])))
            }
            ArrowDataType::Utf8 => {
                let val = arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(idx);
                Ok(Arc::new(StringArray::from(vec![val])))
            }
            ArrowDataType::Boolean => {
                let val = arr
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(idx);
                Ok(Arc::new(BooleanArray::from(vec![val])))
            }
            ArrowDataType::Date32 => {
                let val = arr
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .unwrap()
                    .value(idx);
                Ok(Arc::new(Date32Array::from(vec![val])))
            }
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                let val = arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .value(idx);
                Ok(Arc::new(
                    TimestampNanosecondArray::from(vec![val]).with_timezone_opt(tz.clone()),
                ))
            }
            ArrowDataType::Decimal128(p, s) => {
                let val = arr
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .value(idx);
                Ok(Arc::new(
                    Decimal128Array::from(vec![val])
                        .with_precision_and_scale(*p, *s)
                        .map_err(|e| Error::internal(e.to_string()))?,
                ))
            }
            _ => Ok(new_null_array(arr.data_type(), 1)),
        }
    }

    fn get_source_row<'a>(
        &self,
        source_batches: &'a [RecordBatch],
        global_idx: usize,
    ) -> (&'a RecordBatch, usize) {
        let mut remaining = global_idx;
        for batch in source_batches {
            if remaining < batch.num_rows() {
                return (batch, remaining);
            }
            remaining -= batch.num_rows();
        }
        panic!("Source row index out of bounds");
    }

    async fn evaluate_merge_clause_condition(
        &self,
        condition: Option<&yachtsql_ir::Expr>,
        target_batch: &RecordBatch,
        target_row: usize,
        source: Option<(&RecordBatch, usize)>,
    ) -> Result<bool> {
        let condition = match condition {
            Some(c) => c,
            None => return Ok(true),
        };

        let cond_sql = self.expr_to_sql(condition)?;

        let (schema, batch) = match source {
            Some((source_batch, source_row)) => {
                let schema = self.create_combined_schema(target_batch, source_batch)?;
                let batch =
                    self.create_combined_row(target_batch, target_row, source_batch, source_row)?;
                (schema, batch)
            }
            None => {
                let single_row = self.extract_single_row(target_batch, target_row)?;
                (target_batch.schema(), single_row)
            }
        };

        let mem_table = MemTable::try_new(schema.clone(), vec![vec![batch]])
            .map_err(|e| Error::internal(e.to_string()))?;

        let tmp_name = format!(
            "__tmp_merge_clause_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        self.ctx
            .register_table(&tmp_name, Arc::new(mem_table))
            .map_err(|e| Error::internal(e.to_string()))?;

        let query = format!("SELECT {} FROM {}", cond_sql, tmp_name);
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

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(false);
        }

        let col = batches[0].column(0);
        if let Some(bool_arr) = col
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
        {
            return Ok(bool_arr.value(0));
        }

        Ok(false)
    }

    fn extract_single_row(&self, batch: &RecordBatch, row: usize) -> Result<RecordBatch> {
        let columns: Vec<ArrayRef> = (0..batch.num_columns())
            .map(|col_idx| self.extract_single_value(batch.column(col_idx), row))
            .collect::<Result<_>>()?;

        RecordBatch::try_new(batch.schema(), columns).map_err(|e| Error::internal(e.to_string()))
    }

    fn extract_target_row(&self, batch: &RecordBatch, row: usize) -> Result<Vec<ScalarValue>> {
        let mut values = Vec::new();
        for col_idx in 0..batch.num_columns() {
            let col = batch.column(col_idx);
            let scalar = ScalarValue::try_from_array(col, row)
                .map_err(|e| Error::internal(e.to_string()))?;
            values.push(scalar);
        }
        Ok(values)
    }

    async fn apply_merge_update(
        &self,
        target_batch: &RecordBatch,
        target_row: usize,
        source_batch: &RecordBatch,
        source_row: usize,
        assignments: &[yachtsql_ir::Assignment],
        target_schema: &Arc<ArrowSchema>,
    ) -> Result<Vec<ScalarValue>> {
        let combined_schema = self.create_combined_schema(target_batch, source_batch)?;
        let combined_batch =
            self.create_combined_row(target_batch, target_row, source_batch, source_row)?;

        let mem_table = MemTable::try_new(combined_schema.clone(), vec![vec![combined_batch]])
            .map_err(|e| Error::internal(e.to_string()))?;

        let tmp_name = format!(
            "__tmp_merge_update_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        self.ctx
            .register_table(&tmp_name, Arc::new(mem_table))
            .map_err(|e| Error::internal(e.to_string()))?;

        let mut row = self.extract_target_row(target_batch, target_row)?;

        for assignment in assignments {
            let col_name = assignment.column.to_lowercase();
            let col_idx = target_schema
                .fields()
                .iter()
                .position(|f| f.name().to_lowercase() == col_name)
                .ok_or_else(|| {
                    Error::internal(format!("Column not found: {}", assignment.column))
                })?;

            let value_sql = self.expr_to_sql(&assignment.value)?;
            let query = format!("SELECT {} FROM {}", value_sql, tmp_name);
            let result = self
                .ctx
                .sql(&query)
                .await
                .map_err(|e| Error::internal(e.to_string()))?;
            let batches = result
                .collect()
                .await
                .map_err(|e| Error::internal(e.to_string()))?;

            if !batches.is_empty() && batches[0].num_rows() > 0 {
                let value = ScalarValue::try_from_array(batches[0].column(0), 0)
                    .map_err(|e| Error::internal(e.to_string()))?;
                row[col_idx] = value;
            }
        }

        let _ = self.ctx.deregister_table(&tmp_name);

        Ok(row)
    }

    async fn apply_merge_update_target_only(
        &self,
        target_batch: &RecordBatch,
        target_row: usize,
        assignments: &[yachtsql_ir::Assignment],
        target_schema: &Arc<ArrowSchema>,
    ) -> Result<Vec<ScalarValue>> {
        let single_row = self.extract_single_row(target_batch, target_row)?;

        let mem_table = MemTable::try_new(target_batch.schema(), vec![vec![single_row]])
            .map_err(|e| Error::internal(e.to_string()))?;

        let tmp_name = format!(
            "__tmp_merge_update_target_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        self.ctx
            .register_table(&tmp_name, Arc::new(mem_table))
            .map_err(|e| Error::internal(e.to_string()))?;

        let mut row = self.extract_target_row(target_batch, target_row)?;

        for assignment in assignments {
            let col_name = assignment.column.to_lowercase();
            let col_idx = target_schema
                .fields()
                .iter()
                .position(|f| f.name().to_lowercase() == col_name)
                .ok_or_else(|| {
                    Error::internal(format!("Column not found: {}", assignment.column))
                })?;

            let value_sql = self.expr_to_sql(&assignment.value)?;
            let query = format!("SELECT {} FROM {}", value_sql, tmp_name);
            let result = self
                .ctx
                .sql(&query)
                .await
                .map_err(|e| Error::internal(e.to_string()))?;
            let batches = result
                .collect()
                .await
                .map_err(|e| Error::internal(e.to_string()))?;

            if !batches.is_empty() && batches[0].num_rows() > 0 {
                let value = ScalarValue::try_from_array(batches[0].column(0), 0)
                    .map_err(|e| Error::internal(e.to_string()))?;
                row[col_idx] = value;
            }
        }

        let _ = self.ctx.deregister_table(&tmp_name);

        Ok(row)
    }

    async fn evaluate_not_matched_condition(
        &self,
        condition: Option<&yachtsql_ir::Expr>,
        source_batch: &RecordBatch,
        source_row: usize,
    ) -> Result<bool> {
        let condition = match condition {
            Some(c) => c,
            None => return Ok(true),
        };

        let cond_sql = self.expr_to_sql(condition)?;
        let single_row = self.extract_single_row(source_batch, source_row)?;

        let mem_table = MemTable::try_new(source_batch.schema(), vec![vec![single_row]])
            .map_err(|e| Error::internal(e.to_string()))?;

        let tmp_name = format!(
            "__tmp_merge_not_matched_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        self.ctx
            .register_table(&tmp_name, Arc::new(mem_table))
            .map_err(|e| Error::internal(e.to_string()))?;

        let query = format!("SELECT {} FROM {}", cond_sql, tmp_name);
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

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(false);
        }

        let col = batches[0].column(0);
        if let Some(bool_arr) = col
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
        {
            return Ok(bool_arr.value(0));
        }

        Ok(false)
    }

    async fn build_insert_row(
        &self,
        source_batch: &RecordBatch,
        source_row: usize,
        columns: &[String],
        values: &[yachtsql_ir::Expr],
        target_schema: &Arc<ArrowSchema>,
        _source_schema: &Arc<ArrowSchema>,
    ) -> Result<Vec<ScalarValue>> {
        let single_row = self.extract_single_row(source_batch, source_row)?;

        let mem_table = MemTable::try_new(source_batch.schema(), vec![vec![single_row]])
            .map_err(|e| Error::internal(e.to_string()))?;

        let tmp_name = format!(
            "__tmp_merge_insert_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        self.ctx
            .register_table(&tmp_name, Arc::new(mem_table))
            .map_err(|e| Error::internal(e.to_string()))?;

        let mut row: Vec<ScalarValue> = target_schema
            .fields()
            .iter()
            .map(|_| ScalarValue::Null)
            .collect();

        let columns_lower: Vec<String> = columns.iter().map(|c| c.to_lowercase()).collect();

        for (i, value_expr) in values.iter().enumerate() {
            let col_name = if i < columns_lower.len() {
                &columns_lower[i]
            } else {
                continue;
            };

            let col_idx = target_schema
                .fields()
                .iter()
                .position(|f| f.name().to_lowercase() == *col_name);

            let col_idx = match col_idx {
                Some(idx) => idx,
                None => continue,
            };

            let value_sql = self.expr_to_sql(value_expr)?;
            let query = format!("SELECT {} FROM {}", value_sql, tmp_name);
            let result = self
                .ctx
                .sql(&query)
                .await
                .map_err(|e| Error::internal(e.to_string()))?;
            let batches = result
                .collect()
                .await
                .map_err(|e| Error::internal(e.to_string()))?;

            if !batches.is_empty() && batches[0].num_rows() > 0 {
                let value = ScalarValue::try_from_array(batches[0].column(0), 0)
                    .map_err(|e| Error::internal(e.to_string()))?;
                row[col_idx] = value;
            }
        }

        let _ = self.ctx.deregister_table(&tmp_name);

        Ok(row)
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    fn build_result_batches(
        &self,
        rows: &[Vec<ScalarValue>],
        schema: &Arc<ArrowSchema>,
    ) -> Result<Vec<RecordBatch>> {
        if rows.is_empty() {
            return Ok(vec![]);
        }

        let num_cols = schema.fields().len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);

        for col_idx in 0..num_cols {
            let col_values: Vec<ScalarValue> = rows.iter().map(|r| r[col_idx].clone()).collect();
            let arr = ScalarValue::iter_to_array(col_values)
                .map_err(|e| Error::internal(e.to_string()))?;
            columns.push(arr);
        }

        let batch = RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| Error::internal(e.to_string()))?;

        Ok(vec![batch])
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    fn scalar_function_to_sql(&self, func: &yachtsql_ir::ScalarFunction) -> String {
        use yachtsql_ir::ScalarFunction as SF;
        match func {
            SF::CurrentTimestamp => "CURRENT_TIMESTAMP".to_string(),
            SF::CurrentDate => "CURRENT_DATE".to_string(),
            SF::CurrentTime => "CURRENT_TIME".to_string(),
            SF::CurrentDatetime => "CURRENT_DATETIME".to_string(),
            _ => format!("{:?}", func).to_uppercase(),
        }
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    fn aggregate_function_to_sql(&self, func: &yachtsql_ir::AggregateFunction) -> String {
        use yachtsql_ir::AggregateFunction as AF;
        match func {
            AF::Count => "COUNT".to_string(),
            AF::Sum => "SUM".to_string(),
            AF::Avg => "AVG".to_string(),
            AF::Min => "MIN".to_string(),
            AF::Max => "MAX".to_string(),
            AF::ArrayAgg => "ARRAY_AGG".to_string(),
            AF::StringAgg => "STRING_AGG".to_string(),
            AF::CountIf => "COUNT_IF".to_string(),
            AF::LogicalAnd => "BOOL_AND".to_string(),
            AF::LogicalOr => "BOOL_OR".to_string(),
            AF::BitAnd => "BIT_AND".to_string(),
            AF::BitOr => "BIT_OR".to_string(),
            AF::BitXor => "BIT_XOR".to_string(),
            AF::AnyValue => "ANY_VALUE".to_string(),
            _ => format!("{:?}", func).to_uppercase(),
        }
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    fn eval_const_expr(&self, expr: &yachtsql_ir::Expr) -> ScalarValue {
        use yachtsql_ir::Literal;
        match expr {
            yachtsql_ir::Expr::Literal(lit) => match lit {
                Literal::Null => ScalarValue::Null,
                Literal::Bool(b) => ScalarValue::Boolean(Some(*b)),
                Literal::Int64(i) => ScalarValue::Int64(Some(*i)),
                Literal::Float64(f) => ScalarValue::Float64(Some(**f)),
                Literal::String(s) => ScalarValue::Utf8(Some(s.clone())),
                Literal::Date(d) => ScalarValue::Date32(Some(*d)),
                Literal::Timestamp(ts) => ScalarValue::TimestampMicrosecond(Some(*ts), None),
                _ => ScalarValue::Null,
            },
            yachtsql_ir::Expr::Variable { name } => {
                let var_name = name.trim_start_matches('@').to_lowercase();
                let variables = self.variables.read();
                variables
                    .get(&var_name)
                    .cloned()
                    .unwrap_or(ScalarValue::Null)
            }
            yachtsql_ir::Expr::Column {
                table: Some(table_name),
                name,
                index: None,
                ..
            } => {
                let var_name = table_name.to_lowercase();
                let field_name = name.to_lowercase();
                let variables = self.variables.read();
                if let Some(ScalarValue::Struct(struct_arr)) = variables.get(&var_name) {
                    let schema = struct_arr.fields();
                    for (idx, field) in schema.iter().enumerate() {
                        if field.name().to_lowercase() == field_name {
                            let col = struct_arr.column(idx);
                            if !col.is_empty() {
                                return ScalarValue::try_from_array(col, 0)
                                    .unwrap_or(ScalarValue::Null);
                            }
                        }
                    }
                }
                ScalarValue::Null
            }
            yachtsql_ir::Expr::Column {
                table: None,
                name,
                index: None,
                ..
            } => {
                let var_name = name.to_lowercase();
                let variables = self.variables.read();
                variables
                    .get(&var_name)
                    .cloned()
                    .unwrap_or(ScalarValue::Null)
            }
            yachtsql_ir::Expr::BinaryOp { left, op, right } => {
                let left_val = self.eval_const_expr(left);
                let right_val = self.eval_const_expr(right);
                self.eval_binary_op(&left_val, op, &right_val)
            }
            yachtsql_ir::Expr::Array { elements, .. } => {
                let values: Vec<ScalarValue> =
                    elements.iter().map(|e| self.eval_const_expr(e)).collect();
                if values.is_empty() {
                    ScalarValue::List(ScalarValue::new_list_nullable(&[], &ArrowDataType::Null))
                } else {
                    let data_type = values[0].data_type();
                    ScalarValue::List(ScalarValue::new_list_nullable(&values, &data_type))
                }
            }
            yachtsql_ir::Expr::Cast {
                expr, data_type, ..
            } => {
                let val = self.eval_const_expr(expr);
                self.cast_scalar_value(val, data_type)
            }
            yachtsql_ir::Expr::ScalarFunction {
                name: yachtsql_ir::ScalarFunction::Date,
                args,
                ..
            } => {
                if let Some(arg) = args.first() {
                    let val = self.eval_const_expr(arg);
                    match val {
                        ScalarValue::Utf8(Some(s)) => {
                            if let Ok(date) = chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d") {
                                use chrono::Datelike;
                                let days = date.num_days_from_ce();
                                ScalarValue::Date32(Some(days))
                            } else {
                                ScalarValue::Null
                            }
                        }
                        _ => ScalarValue::Null,
                    }
                } else {
                    ScalarValue::Null
                }
            }
            yachtsql_ir::Expr::ScalarFunction { .. } => ScalarValue::Null,
            _ => ScalarValue::Null,
        }
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    fn eval_binary_op(
        &self,
        left: &ScalarValue,
        op: &yachtsql_ir::BinaryOp,
        right: &ScalarValue,
    ) -> ScalarValue {
        use yachtsql_ir::BinaryOp;
        match (left, right) {
            (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => match op {
                BinaryOp::Add => ScalarValue::Int64(Some(l + r)),
                BinaryOp::Sub => ScalarValue::Int64(Some(l - r)),
                BinaryOp::Mul => ScalarValue::Int64(Some(l * r)),
                BinaryOp::Div => ScalarValue::Int64(Some(l / r)),
                BinaryOp::Mod => ScalarValue::Int64(Some(l % r)),
                BinaryOp::Lt => ScalarValue::Boolean(Some(l < r)),
                BinaryOp::LtEq => ScalarValue::Boolean(Some(l <= r)),
                BinaryOp::Gt => ScalarValue::Boolean(Some(l > r)),
                BinaryOp::GtEq => ScalarValue::Boolean(Some(l >= r)),
                BinaryOp::Eq => ScalarValue::Boolean(Some(l == r)),
                BinaryOp::NotEq => ScalarValue::Boolean(Some(l != r)),
                _ => ScalarValue::Null,
            },
            (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => match op {
                BinaryOp::Add => ScalarValue::Float64(Some(l + r)),
                BinaryOp::Sub => ScalarValue::Float64(Some(l - r)),
                BinaryOp::Mul => ScalarValue::Float64(Some(l * r)),
                BinaryOp::Div => ScalarValue::Float64(Some(l / r)),
                BinaryOp::Lt => ScalarValue::Boolean(Some(l < r)),
                BinaryOp::LtEq => ScalarValue::Boolean(Some(l <= r)),
                BinaryOp::Gt => ScalarValue::Boolean(Some(l > r)),
                BinaryOp::GtEq => ScalarValue::Boolean(Some(l >= r)),
                BinaryOp::Eq => ScalarValue::Boolean(Some(l == r)),
                BinaryOp::NotEq => ScalarValue::Boolean(Some(l != r)),
                _ => ScalarValue::Null,
            },
            (ScalarValue::Utf8(Some(l)), ScalarValue::Utf8(Some(r))) => match op {
                BinaryOp::Concat => ScalarValue::Utf8(Some(format!("{}{}", l, r))),
                BinaryOp::Eq => ScalarValue::Boolean(Some(l == r)),
                BinaryOp::NotEq => ScalarValue::Boolean(Some(l != r)),
                BinaryOp::Lt => ScalarValue::Boolean(Some(l < r)),
                BinaryOp::LtEq => ScalarValue::Boolean(Some(l <= r)),
                BinaryOp::Gt => ScalarValue::Boolean(Some(l > r)),
                BinaryOp::GtEq => ScalarValue::Boolean(Some(l >= r)),
                _ => ScalarValue::Null,
            },
            (ScalarValue::Boolean(Some(l)), ScalarValue::Boolean(Some(r))) => match op {
                BinaryOp::And => ScalarValue::Boolean(Some(*l && *r)),
                BinaryOp::Or => ScalarValue::Boolean(Some(*l || *r)),
                BinaryOp::Eq => ScalarValue::Boolean(Some(l == r)),
                BinaryOp::NotEq => ScalarValue::Boolean(Some(l != r)),
                _ => ScalarValue::Null,
            },
            _ => ScalarValue::Null,
        }
    }

    fn cast_scalar_value(
        &self,
        val: ScalarValue,
        target: &yachtsql_common::types::DataType,
    ) -> ScalarValue {
        use chrono::Datelike;
        use yachtsql_common::types::DataType;
        match (val, target) {
            (ScalarValue::Utf8(Some(s)), DataType::Date) => {
                if let Ok(date) = chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d") {
                    let days = date.num_days_from_ce();
                    ScalarValue::Date32(Some(days))
                } else {
                    ScalarValue::Null
                }
            }
            (v, _) => v,
        }
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
            Expr::ScalarFunction { name, args } => {
                let arg_strs: Vec<String> = args
                    .iter()
                    .map(|a| self.expr_to_sql(a))
                    .collect::<Result<_>>()?;
                let func_name = self.scalar_function_to_sql(name);
                Ok(format!("{}({})", func_name, arg_strs.join(", ")))
            }
            Expr::Aggregate {
                func,
                args,
                distinct,
                ..
            } => {
                let arg_strs: Vec<String> = args
                    .iter()
                    .map(|a| self.expr_to_sql(a))
                    .collect::<Result<_>>()?;
                let func_name = self.aggregate_function_to_sql(func);
                let distinct_str = if *distinct { "DISTINCT " } else { "" };
                Ok(format!(
                    "{}({}{})",
                    func_name,
                    distinct_str,
                    arg_strs.join(", ")
                ))
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
            MemTable::try_new(schema, vec![vec![]]).map_err(|e| Error::internal(e.to_string()))?;
        self.ctx
            .register_table(&lower, Arc::new(mem_table))
            .map_err(|e| Error::internal(e.to_string()))?;

        Ok(vec![])
    }

    async fn execute_alter_table(
        &self,
        table_name: &str,
        operation: &AlterTableOp,
        if_exists: bool,
    ) -> Result<Vec<RecordBatch>> {
        match operation {
            AlterTableOp::RenameTable { new_name } => {
                let (schema_name, old_table) = self.resolve_table_name(table_name);
                let old_table_ref = Self::table_reference(schema_name.as_deref(), &old_table);

                let table_provider = match self.ctx.table_provider(old_table_ref.clone()).await {
                    Ok(provider) => provider,
                    Err(_) => {
                        if if_exists {
                            return Ok(vec![]);
                        }
                        return Err(Error::internal(format!("Table {} not found", table_name)));
                    }
                };

                let (new_schema_name, new_table) = Self::parse_table_name(new_name);
                let new_table_ref = Self::table_reference(new_schema_name.as_deref(), &new_table);

                let existing = self.ctx.table_provider(new_table_ref.clone()).await;
                if existing.is_ok() {
                    return Err(Error::internal(format!(
                        "Table {} already exists",
                        new_name
                    )));
                }

                if let Some(ref schema) = schema_name {
                    let catalog = self
                        .ctx
                        .catalog("datafusion")
                        .ok_or_else(|| Error::internal("Default catalog 'datafusion' not found"))?;
                    if let Some(schema_provider) = catalog.schema(schema) {
                        schema_provider
                            .deregister_table(&old_table)
                            .map_err(|e| Error::internal(e.to_string()))?;
                    }
                } else {
                    self.ctx
                        .deregister_table(&old_table)
                        .map_err(|e| Error::internal(e.to_string()))?;
                }

                if let Some(ref schema) = new_schema_name {
                    let catalog = self
                        .ctx
                        .catalog("datafusion")
                        .ok_or_else(|| Error::internal("Default catalog 'datafusion' not found"))?;
                    let schema_provider = catalog
                        .schema(schema)
                        .ok_or_else(|| Error::internal(format!("Schema not found: {}", schema)))?;
                    schema_provider
                        .register_table(new_table.clone(), table_provider)
                        .map_err(|e| Error::internal(e.to_string()))?;
                } else {
                    self.ctx
                        .register_table(&new_table, table_provider)
                        .map_err(|e| Error::internal(e.to_string()))?;
                }

                let old_full_name = match schema_name {
                    Some(s) => format!("{}.{}", s, old_table),
                    None => old_table.clone(),
                };
                let new_full_name = match new_schema_name {
                    Some(s) => format!("{}.{}", s, new_table),
                    None => new_table.clone(),
                };
                if let Some(defaults) = self.column_defaults.write().remove(&old_full_name) {
                    self.column_defaults.write().insert(new_full_name, defaults);
                }

                Ok(vec![])
            }
            AlterTableOp::AddColumn {
                column,
                if_not_exists,
            } => {
                let (schema_name, table) = self.resolve_table_name(table_name);
                let table_ref = Self::table_reference(schema_name.as_deref(), &table);

                let table_provider = match self.ctx.table_provider(table_ref.clone()).await {
                    Ok(provider) => provider,
                    Err(_) => {
                        if if_exists {
                            return Ok(vec![]);
                        }
                        return Err(Error::internal(format!("Table {} not found", table_name)));
                    }
                };

                let existing_schema = table_provider.schema();
                let col_name_lower = column.name.to_lowercase();

                if existing_schema
                    .fields()
                    .iter()
                    .any(|f| f.name().to_lowercase() == col_name_lower)
                {
                    if *if_not_exists {
                        return Ok(vec![]);
                    }
                    return Err(Error::internal(format!(
                        "Column {} already exists in table {}",
                        column.name, table_name
                    )));
                }

                let new_field = ArrowField::new(
                    &col_name_lower,
                    yachtsql_type_to_arrow(&column.data_type),
                    column.nullable,
                );
                let mut new_fields: Vec<ArrowField> = existing_schema
                    .fields()
                    .iter()
                    .map(|f| f.as_ref().clone())
                    .collect();
                new_fields.push(new_field);
                let new_schema = Arc::new(ArrowSchema::new(new_fields));

                let source = provider_as_source(table_provider);
                let scan = LogicalPlanBuilder::scan(&table, source, None)
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))?;
                let df = DataFrame::new(self.ctx.state(), scan);
                let existing_batches = df
                    .collect()
                    .await
                    .map_err(|e| Error::internal(e.to_string()))?;

                let new_batches: Vec<RecordBatch> = existing_batches
                    .iter()
                    .map(|batch| {
                        let num_rows = batch.num_rows();
                        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
                        let new_column = self
                            .create_null_array(
                                new_schema.field(new_schema.fields().len() - 1).data_type(),
                                num_rows,
                            )
                            .unwrap();
                        columns.push(new_column);
                        RecordBatch::try_new(new_schema.clone(), columns)
                            .map_err(|e| Error::internal(e.to_string()))
                    })
                    .collect::<Result<_>>()?;

                if let Some(ref schema) = schema_name {
                    let catalog = self.ctx.catalog("datafusion").unwrap();
                    let schema_provider = catalog.schema(schema).unwrap();
                    let _ = schema_provider.deregister_table(&table);
                    let partitions = if new_batches.is_empty() {
                        vec![vec![]]
                    } else {
                        vec![new_batches]
                    };
                    let mem_table = MemTable::try_new(new_schema, partitions)
                        .map_err(|e| Error::internal(e.to_string()))?;
                    schema_provider
                        .register_table(table.clone(), Arc::new(mem_table))
                        .map_err(|e| Error::internal(e.to_string()))?;
                } else {
                    let _ = self.ctx.deregister_table(&table);
                    let partitions = if new_batches.is_empty() {
                        vec![vec![]]
                    } else {
                        vec![new_batches]
                    };
                    let mem_table = MemTable::try_new(new_schema, partitions)
                        .map_err(|e| Error::internal(e.to_string()))?;
                    self.ctx
                        .register_table(&table, Arc::new(mem_table))
                        .map_err(|e| Error::internal(e.to_string()))?;
                }

                let full_name = match schema_name {
                    Some(s) => format!("{}.{}", s, table),
                    None => table.clone(),
                };
                if let Some(ref default_expr) = column.default_value {
                    self.column_defaults
                        .write()
                        .entry(full_name)
                        .or_default()
                        .insert(col_name_lower, default_expr.clone());
                }

                Ok(vec![])
            }

            AlterTableOp::DropColumn {
                name,
                if_exists: col_if_exists,
            } => {
                let (schema_name, table) = self.resolve_table_name(table_name);
                let table_ref = Self::table_reference(schema_name.as_deref(), &table);

                let table_provider = match self.ctx.table_provider(table_ref.clone()).await {
                    Ok(provider) => provider,
                    Err(_) => {
                        if if_exists {
                            return Ok(vec![]);
                        }
                        return Err(Error::internal(format!("Table {} not found", table_name)));
                    }
                };

                let existing_schema = table_provider.schema();
                let col_name_lower = name.to_lowercase();

                let col_idx = existing_schema
                    .fields()
                    .iter()
                    .position(|f| f.name().to_lowercase() == col_name_lower);

                let col_idx = match col_idx {
                    Some(idx) => idx,
                    None => {
                        if *col_if_exists {
                            return Ok(vec![]);
                        }
                        return Err(Error::internal(format!(
                            "Column {} not found in table {}",
                            name, table_name
                        )));
                    }
                };

                let new_fields: Vec<ArrowField> = existing_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != col_idx)
                    .map(|(_, f)| f.as_ref().clone())
                    .collect();
                let new_schema = Arc::new(ArrowSchema::new(new_fields));

                let source = provider_as_source(table_provider);
                let scan = LogicalPlanBuilder::scan(&table, source, None)
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))?;
                let df = DataFrame::new(self.ctx.state(), scan);
                let existing_batches = df
                    .collect()
                    .await
                    .map_err(|e| Error::internal(e.to_string()))?;

                let new_batches: Vec<RecordBatch> = existing_batches
                    .iter()
                    .map(|batch| {
                        let columns: Vec<ArrayRef> = batch
                            .columns()
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| *i != col_idx)
                            .map(|(_, c)| c.clone())
                            .collect();
                        RecordBatch::try_new(new_schema.clone(), columns)
                            .map_err(|e| Error::internal(e.to_string()))
                    })
                    .collect::<Result<_>>()?;

                if let Some(ref schema) = schema_name {
                    let catalog = self.ctx.catalog("datafusion").unwrap();
                    let schema_provider = catalog.schema(schema).unwrap();
                    let _ = schema_provider.deregister_table(&table);
                    let partitions = if new_batches.is_empty() {
                        vec![vec![]]
                    } else {
                        vec![new_batches]
                    };
                    let mem_table = MemTable::try_new(new_schema, partitions)
                        .map_err(|e| Error::internal(e.to_string()))?;
                    schema_provider
                        .register_table(table.clone(), Arc::new(mem_table))
                        .map_err(|e| Error::internal(e.to_string()))?;
                } else {
                    let _ = self.ctx.deregister_table(&table);
                    let partitions = if new_batches.is_empty() {
                        vec![vec![]]
                    } else {
                        vec![new_batches]
                    };
                    let mem_table = MemTable::try_new(new_schema, partitions)
                        .map_err(|e| Error::internal(e.to_string()))?;
                    self.ctx
                        .register_table(&table, Arc::new(mem_table))
                        .map_err(|e| Error::internal(e.to_string()))?;
                }

                let full_name = match schema_name {
                    Some(s) => format!("{}.{}", s, table),
                    None => table.clone(),
                };
                if let Some(defaults) = self.column_defaults.write().get_mut(&full_name) {
                    defaults.remove(&col_name_lower);
                }

                Ok(vec![])
            }

            AlterTableOp::RenameColumn { old_name, new_name } => {
                let (schema_name, table) = self.resolve_table_name(table_name);
                let table_ref = Self::table_reference(schema_name.as_deref(), &table);

                let table_provider = match self.ctx.table_provider(table_ref.clone()).await {
                    Ok(provider) => provider,
                    Err(_) => {
                        if if_exists {
                            return Ok(vec![]);
                        }
                        return Err(Error::internal(format!("Table {} not found", table_name)));
                    }
                };

                let existing_schema = table_provider.schema();
                let old_name_lower = old_name.to_lowercase();
                let new_name_lower = new_name.to_lowercase();

                let col_idx = existing_schema
                    .fields()
                    .iter()
                    .position(|f| f.name().to_lowercase() == old_name_lower);

                let col_idx = match col_idx {
                    Some(idx) => idx,
                    None => {
                        return Err(Error::internal(format!(
                            "Column {} not found in table {}",
                            old_name, table_name
                        )));
                    }
                };

                if existing_schema
                    .fields()
                    .iter()
                    .any(|f| f.name().to_lowercase() == new_name_lower)
                {
                    return Err(Error::internal(format!(
                        "Column {} already exists in table {}",
                        new_name, table_name
                    )));
                }

                let new_fields: Vec<ArrowField> = existing_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        if i == col_idx {
                            ArrowField::new(&new_name_lower, f.data_type().clone(), f.is_nullable())
                        } else {
                            f.as_ref().clone()
                        }
                    })
                    .collect();
                let new_schema = Arc::new(ArrowSchema::new(new_fields));

                let source = provider_as_source(table_provider);
                let scan = LogicalPlanBuilder::scan(&table, source, None)
                    .map_err(|e| Error::internal(e.to_string()))?
                    .build()
                    .map_err(|e| Error::internal(e.to_string()))?;
                let df = DataFrame::new(self.ctx.state(), scan);
                let existing_batches = df
                    .collect()
                    .await
                    .map_err(|e| Error::internal(e.to_string()))?;

                let new_batches: Vec<RecordBatch> = existing_batches
                    .iter()
                    .map(|batch| {
                        RecordBatch::try_new(new_schema.clone(), batch.columns().to_vec())
                            .map_err(|e| Error::internal(e.to_string()))
                    })
                    .collect::<Result<_>>()?;

                if let Some(ref schema) = schema_name {
                    let catalog = self.ctx.catalog("datafusion").unwrap();
                    let schema_provider = catalog.schema(schema).unwrap();
                    let _ = schema_provider.deregister_table(&table);
                    let partitions = if new_batches.is_empty() {
                        vec![vec![]]
                    } else {
                        vec![new_batches]
                    };
                    let mem_table = MemTable::try_new(new_schema, partitions)
                        .map_err(|e| Error::internal(e.to_string()))?;
                    schema_provider
                        .register_table(table.clone(), Arc::new(mem_table))
                        .map_err(|e| Error::internal(e.to_string()))?;
                } else {
                    let _ = self.ctx.deregister_table(&table);
                    let partitions = if new_batches.is_empty() {
                        vec![vec![]]
                    } else {
                        vec![new_batches]
                    };
                    let mem_table = MemTable::try_new(new_schema, partitions)
                        .map_err(|e| Error::internal(e.to_string()))?;
                    self.ctx
                        .register_table(&table, Arc::new(mem_table))
                        .map_err(|e| Error::internal(e.to_string()))?;
                }

                let full_name = match schema_name {
                    Some(s) => format!("{}.{}", s, table),
                    None => table.clone(),
                };
                if let Some(defaults) = self.column_defaults.write().get_mut(&full_name)
                    && let Some(default_expr) = defaults.remove(&old_name_lower)
                {
                    defaults.insert(new_name_lower, default_expr);
                }

                Ok(vec![])
            }

            AlterTableOp::AlterColumn { name, action } => {
                let (schema_name, table) = self.resolve_table_name(table_name);
                let table_ref = Self::table_reference(schema_name.as_deref(), &table);

                let table_provider = match self.ctx.table_provider(table_ref.clone()).await {
                    Ok(provider) => provider,
                    Err(_) => {
                        if if_exists {
                            return Ok(vec![]);
                        }
                        return Err(Error::internal(format!("Table {} not found", table_name)));
                    }
                };

                let existing_schema = table_provider.schema();
                let col_name_lower = name.to_lowercase();

                let col_idx = existing_schema
                    .fields()
                    .iter()
                    .position(|f| f.name().to_lowercase() == col_name_lower);

                let col_idx = match col_idx {
                    Some(idx) => idx,
                    None => {
                        return Err(Error::internal(format!(
                            "Column {} not found in table {}",
                            name, table_name
                        )));
                    }
                };

                let full_name = match &schema_name {
                    Some(s) => format!("{}.{}", s, table),
                    None => table.clone(),
                };

                match action {
                    AlterColumnAction::SetNotNull => {
                        let old_field = existing_schema.field(col_idx);
                        let new_fields: Vec<ArrowField> = existing_schema
                            .fields()
                            .iter()
                            .enumerate()
                            .map(|(i, f)| {
                                if i == col_idx {
                                    ArrowField::new(f.name(), f.data_type().clone(), false)
                                } else {
                                    f.as_ref().clone()
                                }
                            })
                            .collect();
                        let new_schema = Arc::new(ArrowSchema::new(new_fields));

                        let source = provider_as_source(table_provider);
                        let scan = LogicalPlanBuilder::scan(&table, source, None)
                            .map_err(|e| Error::internal(e.to_string()))?
                            .build()
                            .map_err(|e| Error::internal(e.to_string()))?;
                        let df = DataFrame::new(self.ctx.state(), scan);
                        let existing_batches = df
                            .collect()
                            .await
                            .map_err(|e| Error::internal(e.to_string()))?;

                        for batch in &existing_batches {
                            let col = batch.column(col_idx);
                            if col.null_count() > 0 {
                                return Err(Error::internal(format!(
                                    "Column {} contains NULL values, cannot set NOT NULL",
                                    old_field.name()
                                )));
                            }
                        }

                        let new_batches: Vec<RecordBatch> = existing_batches
                            .iter()
                            .map(|batch| {
                                RecordBatch::try_new(new_schema.clone(), batch.columns().to_vec())
                                    .map_err(|e| Error::internal(e.to_string()))
                            })
                            .collect::<Result<_>>()?;

                        if let Some(ref schema) = schema_name {
                            let catalog = self.ctx.catalog("datafusion").unwrap();
                            let schema_provider = catalog.schema(schema).unwrap();
                            let _ = schema_provider.deregister_table(&table);
                            let partitions = if new_batches.is_empty() {
                                vec![vec![]]
                            } else {
                                vec![new_batches]
                            };
                            let mem_table = MemTable::try_new(new_schema, partitions)
                                .map_err(|e| Error::internal(e.to_string()))?;
                            schema_provider
                                .register_table(table, Arc::new(mem_table))
                                .map_err(|e| Error::internal(e.to_string()))?;
                        } else {
                            let _ = self.ctx.deregister_table(&table);
                            let partitions = if new_batches.is_empty() {
                                vec![vec![]]
                            } else {
                                vec![new_batches]
                            };
                            let mem_table = MemTable::try_new(new_schema, partitions)
                                .map_err(|e| Error::internal(e.to_string()))?;
                            self.ctx
                                .register_table(&table, Arc::new(mem_table))
                                .map_err(|e| Error::internal(e.to_string()))?;
                        }
                    }

                    AlterColumnAction::DropNotNull => {
                        let new_fields: Vec<ArrowField> = existing_schema
                            .fields()
                            .iter()
                            .enumerate()
                            .map(|(i, f)| {
                                if i == col_idx {
                                    ArrowField::new(f.name(), f.data_type().clone(), true)
                                } else {
                                    f.as_ref().clone()
                                }
                            })
                            .collect();
                        let new_schema = Arc::new(ArrowSchema::new(new_fields));

                        let source = provider_as_source(table_provider);
                        let scan = LogicalPlanBuilder::scan(&table, source, None)
                            .map_err(|e| Error::internal(e.to_string()))?
                            .build()
                            .map_err(|e| Error::internal(e.to_string()))?;
                        let df = DataFrame::new(self.ctx.state(), scan);
                        let existing_batches = df
                            .collect()
                            .await
                            .map_err(|e| Error::internal(e.to_string()))?;

                        let new_batches: Vec<RecordBatch> = existing_batches
                            .iter()
                            .map(|batch| {
                                RecordBatch::try_new(new_schema.clone(), batch.columns().to_vec())
                                    .map_err(|e| Error::internal(e.to_string()))
                            })
                            .collect::<Result<_>>()?;

                        if let Some(ref schema) = schema_name {
                            let catalog = self.ctx.catalog("datafusion").unwrap();
                            let schema_provider = catalog.schema(schema).unwrap();
                            let _ = schema_provider.deregister_table(&table);
                            let partitions = if new_batches.is_empty() {
                                vec![vec![]]
                            } else {
                                vec![new_batches]
                            };
                            let mem_table = MemTable::try_new(new_schema, partitions)
                                .map_err(|e| Error::internal(e.to_string()))?;
                            schema_provider
                                .register_table(table, Arc::new(mem_table))
                                .map_err(|e| Error::internal(e.to_string()))?;
                        } else {
                            let _ = self.ctx.deregister_table(&table);
                            let partitions = if new_batches.is_empty() {
                                vec![vec![]]
                            } else {
                                vec![new_batches]
                            };
                            let mem_table = MemTable::try_new(new_schema, partitions)
                                .map_err(|e| Error::internal(e.to_string()))?;
                            self.ctx
                                .register_table(&table, Arc::new(mem_table))
                                .map_err(|e| Error::internal(e.to_string()))?;
                        }
                    }

                    AlterColumnAction::SetDefault { default } => {
                        self.column_defaults
                            .write()
                            .entry(full_name)
                            .or_default()
                            .insert(col_name_lower, default.clone());
                    }

                    AlterColumnAction::DropDefault => {
                        if let Some(defaults) = self.column_defaults.write().get_mut(&full_name) {
                            defaults.remove(&col_name_lower);
                        }
                    }

                    AlterColumnAction::SetDataType { data_type } => {
                        let new_arrow_type = yachtsql_type_to_arrow(data_type);
                        let new_fields: Vec<ArrowField> = existing_schema
                            .fields()
                            .iter()
                            .enumerate()
                            .map(|(i, f)| {
                                if i == col_idx {
                                    ArrowField::new(
                                        f.name(),
                                        new_arrow_type.clone(),
                                        f.is_nullable(),
                                    )
                                } else {
                                    f.as_ref().clone()
                                }
                            })
                            .collect();
                        let new_schema = Arc::new(ArrowSchema::new(new_fields));

                        let source = provider_as_source(table_provider);
                        let scan = LogicalPlanBuilder::scan(&table, source, None)
                            .map_err(|e| Error::internal(e.to_string()))?
                            .build()
                            .map_err(|e| Error::internal(e.to_string()))?;
                        let df = DataFrame::new(self.ctx.state(), scan);
                        let existing_batches = df
                            .collect()
                            .await
                            .map_err(|e| Error::internal(e.to_string()))?;

                        let new_batches: Vec<RecordBatch> = existing_batches
                            .iter()
                            .map(|batch| {
                                let columns: Vec<ArrayRef> = batch
                                    .columns()
                                    .iter()
                                    .enumerate()
                                    .map(|(i, col)| {
                                        if i == col_idx {
                                            cast(col, &new_arrow_type)
                                                .map_err(|e| Error::internal(e.to_string()))
                                        } else {
                                            Ok(col.clone())
                                        }
                                    })
                                    .collect::<Result<_>>()?;
                                RecordBatch::try_new(new_schema.clone(), columns)
                                    .map_err(|e| Error::internal(e.to_string()))
                            })
                            .collect::<Result<_>>()?;

                        if let Some(ref schema) = schema_name {
                            let catalog = self.ctx.catalog("datafusion").unwrap();
                            let schema_provider = catalog.schema(schema).unwrap();
                            let _ = schema_provider.deregister_table(&table);
                            let partitions = if new_batches.is_empty() {
                                vec![vec![]]
                            } else {
                                vec![new_batches]
                            };
                            let mem_table = MemTable::try_new(new_schema, partitions)
                                .map_err(|e| Error::internal(e.to_string()))?;
                            schema_provider
                                .register_table(table, Arc::new(mem_table))
                                .map_err(|e| Error::internal(e.to_string()))?;
                        } else {
                            let _ = self.ctx.deregister_table(&table);
                            let partitions = if new_batches.is_empty() {
                                vec![vec![]]
                            } else {
                                vec![new_batches]
                            };
                            let mem_table = MemTable::try_new(new_schema, partitions)
                                .map_err(|e| Error::internal(e.to_string()))?;
                            self.ctx
                                .register_table(&table, Arc::new(mem_table))
                                .map_err(|e| Error::internal(e.to_string()))?;
                        }
                    }

                    AlterColumnAction::SetOptions { .. } => {}
                }

                Ok(vec![])
            }

            AlterTableOp::SetOptions { .. }
            | AlterTableOp::AddConstraint { .. }
            | AlterTableOp::DropConstraint { .. }
            | AlterTableOp::DropPrimaryKey => Ok(vec![]),
        }
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

        if !is_aggregate {
            if let Some(body_sql) = self.function_body_to_sql(body, args)? {
                let arrow_return_type = yachtsql_type_to_arrow(return_type);
                let udf = UserDefinedScalarFunction::new(
                    lower.clone(),
                    args,
                    arrow_return_type,
                    body_sql,
                );
                self.ctx.register_udf(ScalarUDF::new_from_impl(udf));
            }
        } else if let Some(body_sql) = self.function_body_to_sql(body, args)? {
            let arrow_return_type = yachtsql_type_to_arrow(return_type);
            let udaf =
                UserDefinedAggregateFunction::new(lower.clone(), args, arrow_return_type, body_sql);
            self.ctx.register_udaf(udaf.into_udaf());
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

    fn function_body_to_sql(
        &self,
        body: &FunctionBody,
        _args: &[yachtsql_ir::FunctionArg],
    ) -> Result<Option<String>> {
        match body {
            FunctionBody::Sql(expr) => {
                let sql = self.expr_to_sql(expr)?;
                Ok(Some(sql))
            }
            FunctionBody::SqlQuery(query) => Ok(Some(query.clone())),
            FunctionBody::JavaScript(_) | FunctionBody::Language { .. } => Ok(None),
        }
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

    async fn execute_create_procedure(
        &self,
        name: &str,
        args: &[yachtsql_ir::ProcedureArg],
        body: &[LogicalPlan],
        or_replace: bool,
        if_not_exists: bool,
    ) -> Result<Vec<RecordBatch>> {
        let lower = name.to_lowercase();

        {
            let procedures = self.procedures.read();
            if procedures.contains_key(&lower) {
                if if_not_exists {
                    return Ok(vec![]);
                }
                if !or_replace {
                    return Err(Error::internal(format!(
                        "Procedure {} already exists",
                        name
                    )));
                }
            }
        }

        self.procedures.write().insert(
            lower,
            ProcedureDefinition {
                name: name.to_string(),
                args: args.to_vec(),
                body: body.to_vec(),
            },
        );

        Ok(vec![])
    }

    async fn execute_drop_procedure(
        &self,
        name: &str,
        if_exists: bool,
    ) -> Result<Vec<RecordBatch>> {
        let lower = name.to_lowercase();

        match self.procedures.write().remove(&lower) {
            Some(_) => Ok(vec![]),
            None => {
                if if_exists {
                    Ok(vec![])
                } else {
                    Err(Error::internal(format!("Procedure {} not found", name)))
                }
            }
        }
    }

    async fn execute_call(
        &self,
        procedure_name: &str,
        _args: &[yachtsql_ir::Expr],
    ) -> Result<Vec<RecordBatch>> {
        let lower = procedure_name.to_lowercase();

        let body = {
            let procedures = self.procedures.read();
            let proc = procedures.get(&lower).ok_or_else(|| {
                Error::internal(format!("Procedure {} not found", procedure_name))
            })?;
            proc.body.clone()
        };

        let mut last_result = Vec::new();
        for stmt in &body {
            last_result = Box::pin(self.execute_plan(stmt)).await?;
        }

        Ok(last_result)
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    async fn execute_set_variable(
        &self,
        name: &str,
        value: &yachtsql_ir::Expr,
    ) -> Result<Vec<RecordBatch>> {
        let lower_name = name.to_lowercase();

        if lower_name == "search_path" {
            let schema_name = match value {
                yachtsql_ir::Expr::Column { name, .. } => name.to_lowercase(),
                yachtsql_ir::Expr::Literal(yachtsql_ir::Literal::String(s)) => s.to_lowercase(),
                _ => {
                    return Err(Error::internal(format!(
                        "Invalid value for search_path: {:?}",
                        value
                    )));
                }
            };

            *self.search_path.write() = vec![schema_name];
            return Ok(vec![]);
        }

        let scalar = self.eval_const_expr(value);
        self.variables.write().insert(lower_name, scalar);
        Ok(vec![])
    }

    fn resolve_table_name(&self, name: &str) -> (Option<String>, String) {
        let (schema, table) = Self::parse_table_name(name);
        if schema.is_some() {
            return (schema, table);
        }

        let search_path = self.search_path.read();
        if !search_path.is_empty() {
            return (Some(search_path[0].clone()), table);
        }

        (None, table)
    }

    fn lookup_column_type(&self, table_name: &str, column_name: &str) -> ArrowDataType {
        let actual_table = {
            let aliases = self.outer_aliases.read();
            aliases
                .get(&table_name.to_lowercase())
                .cloned()
                .unwrap_or_else(|| table_name.to_string())
        };
        let (schema_name, table) = self.resolve_table_name(&actual_table);
        let table_ref = Self::table_reference(schema_name.as_deref(), &table);
        if let Some(Ok(provider)) = self.ctx.table_provider(table_ref).now_or_never() {
            let schema = provider.schema();
            if let Ok(field) = schema.field_with_name(column_name) {
                return field.data_type().clone();
            }
            let col_lower = column_name.to_lowercase();
            for field in schema.fields() {
                if field.name().to_lowercase() == col_lower {
                    return field.data_type().clone();
                }
            }
        }
        ArrowDataType::Utf8
    }

    async fn apply_column_defaults(
        &self,
        batches: &[RecordBatch],
        table_schema: &Arc<ArrowSchema>,
        insert_columns: &[String],
        defaults: &HashMap<String, yachtsql_ir::Expr>,
    ) -> Result<Vec<RecordBatch>> {
        let mut result = Vec::new();
        for batch in batches {
            let num_rows = batch.num_rows();
            let mut new_columns: Vec<ArrayRef> = Vec::new();

            for field in table_schema.fields().iter() {
                let col_name = field.name().to_lowercase();

                let insert_col_idx = insert_columns.iter().position(|c| *c == col_name);

                let column = match insert_col_idx {
                    Some(idx) if idx < batch.num_columns() => batch.column(idx).clone(),
                    _ => {
                        if let Some(default_expr) = defaults.get(&col_name) {
                            let default_sql = self.expr_to_sql(default_expr)?;
                            self.evaluate_default_value(&default_sql, field.data_type(), num_rows)
                                .await?
                        } else {
                            self.create_null_array(field.data_type(), num_rows)?
                        }
                    }
                };

                let casted = if column.data_type() != field.data_type() {
                    cast(&column, field.data_type()).map_err(|e| Error::internal(e.to_string()))?
                } else {
                    column
                };
                new_columns.push(casted);
            }

            let new_batch = RecordBatch::try_new(table_schema.clone(), new_columns)
                .map_err(|e| Error::internal(e.to_string()))?;
            result.push(new_batch);
        }

        Ok(result)
    }

    async fn evaluate_default_value(
        &self,
        expr_sql: &str,
        target_type: &ArrowDataType,
        num_rows: usize,
    ) -> Result<ArrayRef> {
        let query = format!("SELECT {}", expr_sql);
        let batches = self
            .ctx
            .sql(&query)
            .await
            .map_err(|e| Error::internal(e.to_string()))?
            .collect()
            .await
            .map_err(|e| Error::internal(e.to_string()))?;

        if batches.is_empty() || batches[0].num_columns() == 0 {
            return self.create_null_array(target_type, num_rows);
        }

        let single_value = batches[0].column(0);
        if single_value.len() != 1 {
            return Err(Error::internal(
                "Default expression must return single value",
            ));
        }

        self.repeat_array_value(single_value, num_rows, target_type)
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    fn repeat_array_value(
        &self,
        arr: &ArrayRef,
        num_rows: usize,
        target_type: &ArrowDataType,
    ) -> Result<ArrayRef> {
        use datafusion::arrow::array::*;

        if arr.is_null(0) {
            return self.create_null_array(target_type, num_rows);
        }

        match arr.data_type() {
            ArrowDataType::Int64 => {
                let val = arr.as_any().downcast_ref::<Int64Array>().unwrap().value(0);
                Ok(Arc::new(Int64Array::from(vec![val; num_rows])))
            }
            ArrowDataType::Float64 => {
                let val = arr
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0);
                Ok(Arc::new(Float64Array::from(vec![val; num_rows])))
            }
            ArrowDataType::Utf8 => {
                let val = arr.as_any().downcast_ref::<StringArray>().unwrap().value(0);
                Ok(Arc::new(StringArray::from(vec![val; num_rows])))
            }
            ArrowDataType::Boolean => {
                let val = arr
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(0);
                Ok(Arc::new(BooleanArray::from(vec![val; num_rows])))
            }
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                let val = arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .value(0);
                Ok(Arc::new(
                    TimestampNanosecondArray::from(vec![val; num_rows])
                        .with_timezone_opt(tz.clone()),
                ))
            }
            _ => self.create_null_array(target_type, num_rows),
        }
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    fn create_null_array(&self, data_type: &ArrowDataType, num_rows: usize) -> Result<ArrayRef> {
        use datafusion::arrow::array::*;

        match data_type {
            ArrowDataType::Int64 => {
                let arr: Int64Array = (0..num_rows).map(|_| None::<i64>).collect();
                Ok(Arc::new(arr))
            }
            ArrowDataType::Float64 => {
                let arr: Float64Array = (0..num_rows).map(|_| None::<f64>).collect();
                Ok(Arc::new(arr))
            }
            ArrowDataType::Utf8 => {
                let arr: StringArray = (0..num_rows).map(|_| None::<&str>).collect();
                Ok(Arc::new(arr))
            }
            ArrowDataType::Boolean => {
                let arr: BooleanArray = (0..num_rows).map(|_| None::<bool>).collect();
                Ok(Arc::new(arr))
            }
            ArrowDataType::Date32 => {
                let arr: Date32Array = (0..num_rows).map(|_| None::<i32>).collect();
                Ok(Arc::new(arr))
            }
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                let arr: TimestampNanosecondArray = (0..num_rows).map(|_| None::<i64>).collect();
                Ok(Arc::new(arr.with_timezone_opt(tz.clone())))
            }
            _ => {
                let arr: StringArray = (0..num_rows).map(|_| None::<&str>).collect();
                Ok(Arc::new(arr))
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

    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::wildcard_enum_match_arm)]
    async fn execute_gap_fill(
        &self,
        input: &LogicalPlan,
        ts_column: &str,
        bucket_width: &yachtsql_ir::Expr,
        value_columns: &[yachtsql_ir::GapFillColumn],
        partitioning_columns: &[String],
        origin: Option<&yachtsql_ir::Expr>,
        input_schema: &yachtsql_ir::PlanSchema,
        output_schema: &yachtsql_ir::PlanSchema,
    ) -> Result<Vec<RecordBatch>> {
        use std::collections::BTreeMap;

        let input_batches = self.execute_query(input).await?;
        if input_batches.is_empty() {
            return Ok(vec![]);
        }

        let ts_idx = input_schema
            .fields
            .iter()
            .position(|f| f.name.to_uppercase() == ts_column.to_uppercase())
            .ok_or_else(|| Error::invalid_query(format!("Column not found: {}", ts_column)))?;

        let partition_indices: Vec<usize> = partitioning_columns
            .iter()
            .filter_map(|p| {
                input_schema
                    .fields
                    .iter()
                    .position(|f| f.name.to_uppercase() == p.to_uppercase())
            })
            .collect();

        let value_col_indices: Vec<(usize, yachtsql_ir::GapFillStrategy)> = value_columns
            .iter()
            .filter_map(|vc| {
                input_schema
                    .fields
                    .iter()
                    .position(|f| f.name.to_uppercase() == vc.column_name.to_uppercase())
                    .map(|idx| (idx, vc.strategy))
            })
            .collect();

        let bucket_millis = self.evaluate_interval_expr(bucket_width)?;
        let origin_offset = if let Some(origin_expr) = origin {
            Self::evaluate_origin_expr(origin_expr, bucket_millis)?
        } else {
            0
        };

        #[derive(Clone)]
        struct PartitionEntry {
            partition_key: Vec<ScalarValue>,
            entries: Vec<(i64, Vec<ScalarValue>)>,
        }

        let mut ts_timezone: Option<Arc<str>> = None;

        let mut partitions: BTreeMap<String, PartitionEntry> = BTreeMap::new();

        for batch in &input_batches {
            for row_idx in 0..batch.num_rows() {
                let ts_val = ScalarValue::try_from_array(batch.column(ts_idx), row_idx)
                    .map_err(|e| Error::internal(e.to_string()))?;

                let ts_millis = match &ts_val {
                    ScalarValue::TimestampNanosecond(Some(ns), tz) => {
                        if ts_timezone.is_none() {
                            ts_timezone = tz.clone();
                        }
                        *ns / 1_000_000
                    }
                    ScalarValue::TimestampMicrosecond(Some(us), tz) => {
                        if ts_timezone.is_none() {
                            ts_timezone = tz.clone();
                        }
                        *us / 1_000
                    }
                    ScalarValue::TimestampMillisecond(Some(ms), tz) => {
                        if ts_timezone.is_none() {
                            ts_timezone = tz.clone();
                        }
                        *ms
                    }
                    ScalarValue::TimestampSecond(Some(s), tz) => {
                        if ts_timezone.is_none() {
                            ts_timezone = tz.clone();
                        }
                        *s * 1_000
                    }
                    ScalarValue::Date32(Some(d)) => *d as i64 * 24 * 60 * 60 * 1_000,
                    ScalarValue::Date64(Some(ms)) => *ms,
                    _ => continue,
                };

                let partition_key: Vec<ScalarValue> = partition_indices
                    .iter()
                    .map(|&idx| {
                        ScalarValue::try_from_array(batch.column(idx), row_idx)
                            .unwrap_or(ScalarValue::Null)
                    })
                    .collect();

                let partition_key_str = partition_key
                    .iter()
                    .map(|v| format!("{:?}", v))
                    .collect::<Vec<_>>()
                    .join("|");

                let values_for_row: Vec<ScalarValue> = value_col_indices
                    .iter()
                    .map(|(idx, _)| {
                        ScalarValue::try_from_array(batch.column(*idx), row_idx)
                            .unwrap_or(ScalarValue::Null)
                    })
                    .collect();

                partitions
                    .entry(partition_key_str)
                    .or_insert_with(|| PartitionEntry {
                        partition_key: partition_key.clone(),
                        entries: Vec::new(),
                    })
                    .entries
                    .push((ts_millis, values_for_row));
            }
        }

        let output_arrow_schema = convert_plan_schema(output_schema);
        let mut result_rows: Vec<Vec<ScalarValue>> = Vec::new();

        for (_key, mut partition) in partitions {
            partition.entries.sort_by_key(|(ts, _)| *ts);

            if partition.entries.is_empty() {
                continue;
            }

            let min_original_ts = partition.entries.first().map(|(ts, _)| *ts).unwrap();
            let max_original_ts = partition.entries.last().map(|(ts, _)| *ts).unwrap();

            let min_bucket = {
                let floored = ((min_original_ts - origin_offset) / bucket_millis) * bucket_millis
                    + origin_offset;
                if floored < min_original_ts {
                    floored + bucket_millis
                } else {
                    floored
                }
            };
            let max_bucket =
                ((max_original_ts - origin_offset) / bucket_millis) * bucket_millis + origin_offset;

            let mut exact_match_map: BTreeMap<i64, Vec<ScalarValue>> = BTreeMap::new();
            for (ts, values) in &partition.entries {
                let bucket_ts =
                    ((*ts - origin_offset) / bucket_millis) * bucket_millis + origin_offset;
                if *ts == bucket_ts {
                    exact_match_map.insert(bucket_ts, values.clone());
                }
            }

            let mut last_values: Vec<Option<ScalarValue>> = vec![None; value_col_indices.len()];

            let mut bucket = min_bucket;
            while bucket <= max_bucket {
                for (ts, values) in &partition.entries {
                    if *ts <= bucket {
                        for (i, val) in values.iter().enumerate() {
                            if !val.is_null() {
                                last_values[i] = Some(val.clone());
                            }
                        }
                    }
                }

                let value_field_offset = 1 + partition_indices.len();
                let row_values = if let Some(existing) = exact_match_map.get(&bucket) {
                    existing.clone()
                } else {
                    value_col_indices
                        .iter()
                        .enumerate()
                        .map(|(i, (_, strategy))| {
                            let typed_null = output_schema
                                .fields
                                .get(value_field_offset + i)
                                .map(|f| typed_null_for_data_type(&f.data_type))
                                .unwrap_or(ScalarValue::Null);
                            match strategy {
                                yachtsql_ir::GapFillStrategy::Null => typed_null,
                                yachtsql_ir::GapFillStrategy::Locf => {
                                    last_values[i].clone().unwrap_or(typed_null)
                                }
                                yachtsql_ir::GapFillStrategy::Linear => {
                                    let prev_entry = partition
                                        .entries
                                        .iter()
                                        .filter(|(ts, _)| *ts < bucket)
                                        .next_back();
                                    let next_entry =
                                        partition.entries.iter().find(|(ts, _)| *ts > bucket);

                                    match (prev_entry, next_entry) {
                                        (
                                            Some((prev_ts, prev_vals)),
                                            Some((next_ts, next_vals)),
                                        ) => interpolate_scalar_value(
                                            &prev_vals[i],
                                            &next_vals[i],
                                            *prev_ts,
                                            *next_ts,
                                            bucket,
                                        ),
                                        _ => typed_null,
                                    }
                                }
                            }
                        })
                        .collect()
                };

                let ts_value =
                    ScalarValue::TimestampNanosecond(Some(bucket * 1_000_000), ts_timezone.clone());

                let mut record_values = vec![ts_value];
                record_values.extend(partition.partition_key.clone());
                record_values.extend(row_values);

                result_rows.push(record_values);

                bucket += bucket_millis;
            }
        }

        if result_rows.is_empty() {
            return Ok(vec![]);
        }

        let num_cols = result_rows[0].len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);

        for col_idx in 0..num_cols {
            let col_values: Vec<ScalarValue> = result_rows
                .iter()
                .map(|row| row.get(col_idx).cloned().unwrap_or(ScalarValue::Null))
                .collect();
            let array = ScalarValue::iter_to_array(col_values)
                .map_err(|e| Error::internal(e.to_string()))?;
            columns.push(array);
        }

        let result_batch = RecordBatch::try_new(output_arrow_schema, columns)
            .map_err(|e| Error::internal(e.to_string()))?;

        Ok(vec![result_batch])
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    fn evaluate_interval_expr(&self, expr: &yachtsql_ir::Expr) -> Result<i64> {
        match expr {
            yachtsql_ir::Expr::Literal(yachtsql_ir::Literal::Interval {
                months,
                days,
                nanos,
            }) => {
                let millis = (*months as i64 * 30 * 24 * 60 * 60 * 1000)
                    + (*days as i64 * 24 * 60 * 60 * 1000)
                    + (*nanos / 1_000_000);
                Ok(millis)
            }
            _ => Err(Error::invalid_query("bucket_width must be an interval")),
        }
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    fn evaluate_origin_expr(expr: &yachtsql_ir::Expr, bucket_millis: i64) -> Result<i64> {
        match expr {
            yachtsql_ir::Expr::Literal(yachtsql_ir::Literal::Datetime(dt_nanos)) => {
                let dt_millis = *dt_nanos / 1_000_000;
                Ok(dt_millis % bucket_millis)
            }
            yachtsql_ir::Expr::Literal(yachtsql_ir::Literal::Timestamp(ts_nanos)) => {
                let ts_millis = *ts_nanos / 1_000_000;
                Ok(ts_millis % bucket_millis)
            }
            yachtsql_ir::Expr::TypedString { value, .. } => {
                if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
                    let dt_millis = dt.and_utc().timestamp_millis();
                    Ok(dt_millis % bucket_millis)
                } else if let Ok(dt) =
                    chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S")
                {
                    let dt_millis = dt.and_utc().timestamp_millis();
                    Ok(dt_millis % bucket_millis)
                } else {
                    Ok(0)
                }
            }
            yachtsql_ir::Expr::Cast { expr: inner, .. } => {
                Self::evaluate_origin_expr(inner, bucket_millis)
            }
            _ => Ok(0),
        }
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

#[allow(clippy::wildcard_enum_match_arm)]
fn interpolate_scalar_value(
    prev: &ScalarValue,
    next: &ScalarValue,
    prev_ts: i64,
    next_ts: i64,
    current_ts: i64,
) -> ScalarValue {
    if next_ts == prev_ts {
        return prev.clone();
    }

    let ratio = (current_ts - prev_ts) as f64 / (next_ts - prev_ts) as f64;

    match (prev, next) {
        (ScalarValue::Int8(Some(p)), ScalarValue::Int8(Some(n))) => {
            let interpolated = *p as f64 + (*n as f64 - *p as f64) * ratio;
            ScalarValue::Int8(Some(interpolated.round() as i8))
        }
        (ScalarValue::Int16(Some(p)), ScalarValue::Int16(Some(n))) => {
            let interpolated = *p as f64 + (*n as f64 - *p as f64) * ratio;
            ScalarValue::Int16(Some(interpolated.round() as i16))
        }
        (ScalarValue::Int32(Some(p)), ScalarValue::Int32(Some(n))) => {
            let interpolated = *p as f64 + (*n as f64 - *p as f64) * ratio;
            ScalarValue::Int32(Some(interpolated.round() as i32))
        }
        (ScalarValue::Int64(Some(p)), ScalarValue::Int64(Some(n))) => {
            let interpolated = *p as f64 + (*n as f64 - *p as f64) * ratio;
            ScalarValue::Int64(Some(interpolated.round() as i64))
        }
        (ScalarValue::Float32(Some(p)), ScalarValue::Float32(Some(n))) => {
            ScalarValue::Float32(Some(*p + (*n - *p) * ratio as f32))
        }
        (ScalarValue::Float64(Some(p)), ScalarValue::Float64(Some(n))) => {
            ScalarValue::Float64(Some(*p + (*n - *p) * ratio))
        }
        (ScalarValue::Int64(Some(p)), ScalarValue::Float64(Some(n))) => {
            ScalarValue::Float64(Some(*p as f64 + (*n - *p as f64) * ratio))
        }
        (ScalarValue::Float64(Some(p)), ScalarValue::Int64(Some(n))) => {
            ScalarValue::Float64(Some(*p + (*n as f64 - *p) * ratio))
        }
        _ => match prev {
            ScalarValue::Utf8(_) => ScalarValue::Utf8(None),
            ScalarValue::LargeUtf8(_) => ScalarValue::LargeUtf8(None),
            ScalarValue::Binary(_) => ScalarValue::Binary(None),
            ScalarValue::Boolean(_) => ScalarValue::Boolean(None),
            ScalarValue::Int8(_) => ScalarValue::Int8(None),
            ScalarValue::Int16(_) => ScalarValue::Int16(None),
            ScalarValue::Int32(_) => ScalarValue::Int32(None),
            ScalarValue::Int64(_) => ScalarValue::Int64(None),
            ScalarValue::Float32(_) => ScalarValue::Float32(None),
            ScalarValue::Float64(_) => ScalarValue::Float64(None),
            ScalarValue::Decimal128(_, p, s) => ScalarValue::Decimal128(None, *p, *s),
            _ => ScalarValue::Null,
        },
    }
}

#[allow(clippy::wildcard_enum_match_arm)]
fn typed_null_for_data_type(data_type: &yachtsql_common::types::DataType) -> ScalarValue {
    use yachtsql_common::types::DataType;
    match data_type {
        DataType::Bool => ScalarValue::Boolean(None),
        DataType::Int64 => ScalarValue::Int64(None),
        DataType::Float64 => ScalarValue::Float64(None),
        DataType::String => ScalarValue::Utf8(None),
        DataType::Bytes => ScalarValue::Binary(None),
        DataType::Date => ScalarValue::Date32(None),
        DataType::DateTime => ScalarValue::TimestampNanosecond(None, None),
        DataType::Timestamp => ScalarValue::TimestampNanosecond(None, None),
        DataType::Time => ScalarValue::Time64Nanosecond(None),
        DataType::Numeric { .. } => ScalarValue::Decimal128(None, 38, 9),
        DataType::Interval => ScalarValue::IntervalMonthDayNano(None),
        _ => ScalarValue::Null,
    }
}

fn extract_tables_from_plan(plan: &LogicalPlan) -> HashSet<String> {
    match plan {
        LogicalPlan::Scan { schema, .. } => {
            let mut tables = HashSet::new();
            if let Some(t) = schema.fields.first().and_then(|f| f.table.as_ref()) {
                tables.insert(t.to_lowercase());
            }
            tables
        }
        LogicalPlan::Filter { input, .. } => extract_tables_from_plan(input),
        LogicalPlan::Project { input, .. } => extract_tables_from_plan(input),
        LogicalPlan::Aggregate { input, .. } => extract_tables_from_plan(input),
        LogicalPlan::Join { left, right, .. } => {
            let mut tables = extract_tables_from_plan(left);
            tables.extend(extract_tables_from_plan(right));
            tables
        }
        LogicalPlan::Sort { input, .. } => extract_tables_from_plan(input),
        LogicalPlan::Limit { input, .. } => extract_tables_from_plan(input),
        LogicalPlan::Distinct { input } => extract_tables_from_plan(input),
        LogicalPlan::SetOperation { left, right, .. } => {
            let mut tables = extract_tables_from_plan(left);
            tables.extend(extract_tables_from_plan(right));
            tables
        }
        LogicalPlan::WithCte { ctes, body, .. } => {
            let mut tables = HashSet::new();
            for cte in ctes {
                tables.insert(cte.name.to_lowercase());
            }
            tables.extend(extract_tables_from_plan(body));
            tables
        }
        LogicalPlan::Window { input, .. } => extract_tables_from_plan(input),
        LogicalPlan::Qualify { input, .. } => extract_tables_from_plan(input),
        LogicalPlan::Sample { .. }
        | LogicalPlan::Values { .. }
        | LogicalPlan::Empty { .. }
        | LogicalPlan::Unnest { .. }
        | LogicalPlan::Insert { .. }
        | LogicalPlan::Update { .. }
        | LogicalPlan::Delete { .. }
        | LogicalPlan::Merge { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::Truncate { .. }
        | LogicalPlan::CreateView { .. }
        | LogicalPlan::DropView { .. }
        | LogicalPlan::CreateSchema { .. }
        | LogicalPlan::DropSchema { .. }
        | LogicalPlan::UndropSchema { .. }
        | LogicalPlan::AlterSchema { .. }
        | LogicalPlan::CreateFunction { .. }
        | LogicalPlan::DropFunction { .. }
        | LogicalPlan::CreateProcedure { .. }
        | LogicalPlan::DropProcedure { .. }
        | LogicalPlan::Call { .. }
        | LogicalPlan::ExportData { .. }
        | LogicalPlan::LoadData { .. }
        | LogicalPlan::Declare { .. }
        | LogicalPlan::SetVariable { .. }
        | LogicalPlan::SetMultipleVariables { .. }
        | LogicalPlan::If { .. }
        | LogicalPlan::While { .. }
        | LogicalPlan::Loop { .. }
        | LogicalPlan::Block { .. }
        | LogicalPlan::Repeat { .. }
        | LogicalPlan::For { .. }
        | LogicalPlan::Return { .. }
        | LogicalPlan::Raise { .. }
        | LogicalPlan::ExecuteImmediate { .. }
        | LogicalPlan::Break { .. }
        | LogicalPlan::Continue { .. }
        | LogicalPlan::CreateSnapshot { .. }
        | LogicalPlan::DropSnapshot { .. }
        | LogicalPlan::Assert { .. }
        | LogicalPlan::Grant { .. }
        | LogicalPlan::Revoke { .. }
        | LogicalPlan::BeginTransaction
        | LogicalPlan::Commit
        | LogicalPlan::Rollback
        | LogicalPlan::TryCatch { .. }
        | LogicalPlan::GapFill { .. }
        | LogicalPlan::Explain { .. } => HashSet::new(),
    }
}

fn extract_alias_mapping(plan: &LogicalPlan, mapping: &mut HashMap<String, String>) {
    match plan {
        LogicalPlan::Scan {
            table_name, schema, ..
        } => {
            if let Some(alias) = schema.fields.first().and_then(|f| f.table.as_ref()) {
                mapping.insert(alias.to_lowercase(), table_name.clone());
            }
        }
        LogicalPlan::Filter { input, .. } => extract_alias_mapping(input, mapping),
        LogicalPlan::Project { input, .. } => extract_alias_mapping(input, mapping),
        LogicalPlan::Aggregate { input, .. } => extract_alias_mapping(input, mapping),
        LogicalPlan::Join { left, right, .. } => {
            extract_alias_mapping(left, mapping);
            extract_alias_mapping(right, mapping);
        }
        LogicalPlan::Sort { input, .. } => extract_alias_mapping(input, mapping),
        LogicalPlan::Limit { input, .. } => extract_alias_mapping(input, mapping),
        LogicalPlan::Distinct { input } => extract_alias_mapping(input, mapping),
        LogicalPlan::SetOperation { left, right, .. } => {
            extract_alias_mapping(left, mapping);
            extract_alias_mapping(right, mapping);
        }
        LogicalPlan::WithCte { body, .. } => extract_alias_mapping(body, mapping),
        LogicalPlan::Window { input, .. } => extract_alias_mapping(input, mapping),
        LogicalPlan::Qualify { input, .. } => extract_alias_mapping(input, mapping),
        LogicalPlan::Sample { input, .. } => extract_alias_mapping(input, mapping),
        LogicalPlan::Values { .. }
        | LogicalPlan::Empty { .. }
        | LogicalPlan::Unnest { .. }
        | LogicalPlan::Insert { .. }
        | LogicalPlan::Update { .. }
        | LogicalPlan::Delete { .. }
        | LogicalPlan::Merge { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::Truncate { .. }
        | LogicalPlan::CreateView { .. }
        | LogicalPlan::DropView { .. }
        | LogicalPlan::CreateSchema { .. }
        | LogicalPlan::DropSchema { .. }
        | LogicalPlan::UndropSchema { .. }
        | LogicalPlan::AlterSchema { .. }
        | LogicalPlan::CreateFunction { .. }
        | LogicalPlan::DropFunction { .. }
        | LogicalPlan::CreateProcedure { .. }
        | LogicalPlan::DropProcedure { .. }
        | LogicalPlan::Call { .. }
        | LogicalPlan::ExportData { .. }
        | LogicalPlan::LoadData { .. }
        | LogicalPlan::Declare { .. }
        | LogicalPlan::SetVariable { .. }
        | LogicalPlan::SetMultipleVariables { .. }
        | LogicalPlan::If { .. }
        | LogicalPlan::While { .. }
        | LogicalPlan::Loop { .. }
        | LogicalPlan::Block { .. }
        | LogicalPlan::Repeat { .. }
        | LogicalPlan::For { .. }
        | LogicalPlan::Return { .. }
        | LogicalPlan::Raise { .. }
        | LogicalPlan::ExecuteImmediate { .. }
        | LogicalPlan::Break { .. }
        | LogicalPlan::Continue { .. }
        | LogicalPlan::CreateSnapshot { .. }
        | LogicalPlan::DropSnapshot { .. }
        | LogicalPlan::Assert { .. }
        | LogicalPlan::Grant { .. }
        | LogicalPlan::Revoke { .. }
        | LogicalPlan::BeginTransaction
        | LogicalPlan::Commit
        | LogicalPlan::Rollback
        | LogicalPlan::TryCatch { .. }
        | LogicalPlan::GapFill { .. }
        | LogicalPlan::Explain { .. } => {}
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

fn extract_sort_from_plan(
    plan: &DFLogicalPlan,
) -> (
    DFLogicalPlan,
    Option<Vec<datafusion::logical_expr::SortExpr>>,
) {
    fn sort_exprs_valid_for_schema(
        exprs: &[datafusion::logical_expr::SortExpr],
        schema: &datafusion::common::DFSchema,
    ) -> bool {
        exprs.iter().all(|sort_expr| {
            if let DFExpr::Column(col) = &sort_expr.expr {
                schema
                    .field_with_name(col.relation.as_ref(), &col.name)
                    .is_ok()
            } else {
                false
            }
        })
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    match plan {
        DFLogicalPlan::Sort(datafusion::logical_expr::Sort {
            expr,
            input,
            fetch: _,
        }) => (input.as_ref().clone(), Some(expr.clone())),
        DFLogicalPlan::Projection(proj) => {
            let (inner_input, order_by) = extract_sort_from_plan(&proj.input);
            if let Some(ref order_exprs) = order_by
                && sort_exprs_valid_for_schema(order_exprs, &proj.schema)
            {
                let new_proj = datafusion::logical_expr::Projection::try_new(
                    proj.expr.clone(),
                    Arc::new(inner_input),
                )
                .ok()
                .map(DFLogicalPlan::Projection);
                if let Some(p) = new_proj {
                    return (p, order_by);
                }
            }
            (plan.clone(), None)
        }
        DFLogicalPlan::Limit(limit) => {
            let (inner_input, order_by) = extract_sort_from_plan(&limit.input);
            if order_by.is_some() {
                let new_limit = DFLogicalPlan::Limit(datafusion::logical_expr::Limit {
                    skip: limit.skip.clone(),
                    fetch: limit.fetch.clone(),
                    input: Arc::new(inner_input),
                });
                (new_limit, order_by)
            } else {
                (plan.clone(), None)
            }
        }
        _ => (plan.clone(), None),
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
