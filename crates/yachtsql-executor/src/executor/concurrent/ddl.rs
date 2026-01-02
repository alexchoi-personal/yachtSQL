#![coverage(off)]

use std::collections::HashMap;

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::DataType;
use yachtsql_ir::{AlterTableOp, ColumnDef, FunctionArg, FunctionBody, ProcedureArg};
use yachtsql_storage::{Field, FieldMode, Record, Schema, Table};

use super::ConcurrentPlanExecutor;
use crate::catalog::{ColumnDefault, UserFunction, UserProcedure};
use crate::plan::PhysicalPlan;
use crate::value_evaluator::ValueEvaluator;

impl ConcurrentPlanExecutor {
    pub(crate) fn execute_truncate(&self, table_name: &str) -> Result<Table> {
        self.tables
            .with_table_mut(table_name, |table| {
                table.clear();
            })
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) async fn execute_create_table(
        &self,
        table_name: &str,
        columns: &[ColumnDef],
        if_not_exists: bool,
        or_replace: bool,
        query: Option<&PhysicalPlan>,
    ) -> Result<Table> {
        if let Some(dot_idx) = table_name.find('.') {
            let schema_name = &table_name[..dot_idx];
            if self.catalog.is_schema_dropped(schema_name) {
                return Err(Error::invalid_query(format!(
                    "Schema not found: {}",
                    schema_name
                )));
            }
        }

        if self.catalog.table_exists(table_name) {
            if if_not_exists {
                return Ok(Table::empty(Schema::new()));
            }
            if !or_replace {
                return Err(Error::invalid_query(format!(
                    "Table already exists: {}",
                    table_name
                )));
            }
        }

        if let Some(query_plan) = query {
            let result = self.execute_plan(query_plan).await?;
            let schema = result.schema().clone();
            if or_replace && self.catalog.table_exists(table_name) {
                self.catalog.create_or_replace_table(table_name, result);
            } else {
                self.catalog.insert_table(table_name, result)?;
            }
            return Ok(Table::empty(schema));
        }

        let schema_default_collation = if let Some(dot_idx) = table_name.find('.') {
            let schema_name = &table_name[..dot_idx];
            self.catalog.get_schema_default_collation(schema_name)
        } else {
            None
        };

        let mut schema = Schema::new();
        let mut defaults = Vec::new();
        for col in columns {
            let mode = if col.nullable {
                FieldMode::Nullable
            } else {
                FieldMode::Required
            };
            let mut field = Field::new(&col.name, col.data_type.clone(), mode);
            if let Some(ref collation) = col.collation {
                field = field.with_collation(collation);
            } else if let Some(ref default_coll) = schema_default_collation
                && col.data_type == DataType::String
            {
                field = field.with_collation(default_coll);
            }
            schema.add_field(field);
            if let Some(ref default_expr) = col.default_value {
                defaults.push(ColumnDefault {
                    column_name: col.name.clone(),
                    default_expr: default_expr.clone(),
                });
            }
        }

        if or_replace && self.catalog.table_exists(table_name) {
            self.catalog
                .create_or_replace_table(table_name, Table::new(schema));
        } else {
            self.catalog.create_table(table_name, schema)?;
        }

        if !defaults.is_empty() {
            self.catalog.set_table_defaults(table_name, defaults);
        }

        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_drop_tables(
        &self,
        table_names: &[String],
        if_exists: bool,
    ) -> Result<Table> {
        for name in table_names {
            if self.catalog.table_exists(name) {
                self.catalog.drop_table(name)?;
            } else if !if_exists {
                return Err(Error::TableNotFound(name.clone()));
            }
        }
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_alter_table(
        &self,
        table_name: &str,
        operation: &AlterTableOp,
        if_exists: bool,
    ) -> Result<Table> {
        let table_exists = self.tables.get_table(table_name).is_some();
        if !table_exists {
            if if_exists {
                return Ok(Table::empty(Schema::new()));
            }
            return Err(Error::TableNotFound(table_name.to_string()));
        }

        match operation {
            AlterTableOp::AddColumn {
                column,
                if_not_exists,
            } => {
                {
                    let table = self
                        .tables
                        .get_table(table_name)
                        .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
                    if *if_not_exists && table.schema().field(&column.name).is_some() {
                        return Ok(Table::empty(Schema::new()));
                    }
                }

                let field = if column.nullable {
                    Field::nullable(column.name.clone(), column.data_type.clone())
                } else {
                    Field::required(column.name.clone(), column.data_type.clone())
                };

                let default_value = match column.default_value.as_ref() {
                    Some(expr) => {
                        let empty_schema = Schema::new();
                        let vars = self.get_variables();
                        let udf = self.get_user_functions();
                        let evaluator = ValueEvaluator::new(&empty_schema)
                            .with_variables(&vars)
                            .with_user_functions(&udf);
                        evaluator.evaluate(expr, &Record::new()).ok()
                    }
                    None => None,
                };

                use yachtsql_storage::TableSchemaOps;
                self.tables
                    .with_table_mut(table_name, |table| table.add_column(field, default_value))
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))??;
            }
            AlterTableOp::DropColumn {
                name,
                if_exists: column_if_exists,
            } => {
                self.tables
                    .with_table_mut(table_name, |table| {
                        if *column_if_exists && table.schema().field(name).is_none() {
                            return Ok(());
                        }
                        table.drop_column(name)
                    })
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))??;
            }
            AlterTableOp::RenameColumn { old_name, new_name } => {
                self.tables
                    .with_table_mut(table_name, |table| table.rename_column(old_name, new_name))
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))??;
            }
            AlterTableOp::RenameTable { new_name } => {
                self.catalog.rename_table(table_name, new_name)?;
            }
            AlterTableOp::AlterColumn { name, action } => {
                let evaluated_default = match action {
                    yachtsql_ir::AlterColumnAction::SetDefault { default } => {
                        let empty_schema = Schema::new();
                        let vars = self.get_variables();
                        let udf = self.get_user_functions();
                        let evaluator = ValueEvaluator::new(&empty_schema)
                            .with_variables(&vars)
                            .with_user_functions(&udf);
                        Some((
                            evaluator.evaluate(default, &Record::new())?,
                            default.clone(),
                        ))
                    }
                    _ => None,
                };

                let catalog_update: Option<(String, Option<yachtsql_ir::Expr>)> = self
                    .tables
                    .with_table_mut(table_name, |table| match action {
                        yachtsql_ir::AlterColumnAction::SetNotNull => {
                            table.set_column_not_null(name)?;
                            Ok(None)
                        }
                        yachtsql_ir::AlterColumnAction::DropNotNull => {
                            table.set_column_nullable(name)?;
                            Ok(None)
                        }
                        yachtsql_ir::AlterColumnAction::SetDefault { default: _ } => {
                            let (value, default_expr) = evaluated_default.ok_or_else(|| {
                                Error::internal(
                                    "Expected evaluated default value for SetDefault action",
                                )
                            })?;
                            table.set_column_default(name, value)?;
                            Ok(Some((name.clone(), Some(default_expr))))
                        }
                        yachtsql_ir::AlterColumnAction::DropDefault => {
                            table.drop_column_default(name)?;
                            Ok(Some((name.clone(), None)))
                        }
                        yachtsql_ir::AlterColumnAction::SetDataType { data_type } => {
                            table.set_column_data_type(name, data_type.clone())?;
                            Ok(None)
                        }
                        yachtsql_ir::AlterColumnAction::SetOptions { collation } => {
                            if let Some(coll) = collation {
                                table.set_column_collation(name, coll.clone())?;
                            }
                            Ok(None)
                        }
                    })
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))??;

                if let Some((col_name, maybe_expr)) = catalog_update {
                    let mut defaults = self
                        .catalog
                        .get_table_defaults(table_name)
                        .unwrap_or_default();
                    defaults.retain(|d| d.column_name.to_uppercase() != col_name.to_uppercase());
                    if let Some(default_expr) = maybe_expr {
                        defaults.push(ColumnDefault {
                            column_name: col_name,
                            default_expr,
                        });
                    }
                    self.catalog.set_table_defaults(table_name, defaults);
                }
            }
            AlterTableOp::SetOptions { options: _ } => {}
            AlterTableOp::AddConstraint { constraint: _ } => {}
            AlterTableOp::DropConstraint { name: _ } => {}
            AlterTableOp::DropPrimaryKey => {}
        }
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_create_view(
        &self,
        name: &str,
        query_sql: &str,
        column_aliases: &[String],
        or_replace: bool,
        if_not_exists: bool,
    ) -> Result<Table> {
        self.catalog.create_view(
            name,
            query_sql.to_string(),
            column_aliases.to_vec(),
            or_replace,
            if_not_exists,
        )?;
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_drop_view(&self, name: &str, if_exists: bool) -> Result<Table> {
        self.catalog.drop_view(name, if_exists)?;
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_create_schema(
        &self,
        name: &str,
        if_not_exists: bool,
        or_replace: bool,
    ) -> Result<Table> {
        if or_replace && self.catalog.schema_exists(name) {
            self.catalog.drop_schema(name, true, true)?;
        }
        self.catalog.create_schema(name, if_not_exists)?;
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_drop_schema(
        &self,
        name: &str,
        if_exists: bool,
        cascade: bool,
    ) -> Result<Table> {
        self.catalog.drop_schema(name, if_exists, cascade)?;
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_undrop_schema(&self, name: &str, if_not_exists: bool) -> Result<Table> {
        self.catalog.undrop_schema(name, if_not_exists)?;
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_alter_schema(
        &self,
        name: &str,
        options: &[(String, String)],
    ) -> Result<Table> {
        let opts: HashMap<String, String> = options.iter().cloned().collect();
        self.catalog.alter_schema_options(name, opts)?;
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_create_function(
        &self,
        name: &str,
        args: &[FunctionArg],
        return_type: &DataType,
        body: &FunctionBody,
        or_replace: bool,
        if_not_exists: bool,
        is_temp: bool,
        is_aggregate: bool,
    ) -> Result<Table> {
        if self.catalog.function_exists(name) && !or_replace {
            if if_not_exists {
                return Ok(Table::empty(Schema::new()));
            }
            return Err(Error::invalid_query(format!(
                "Function already exists: {}",
                name
            )));
        }

        let func = UserFunction {
            name: name.to_string(),
            parameters: args.to_vec(),
            return_type: return_type.clone(),
            body: body.clone(),
            is_temporary: is_temp,
            is_aggregate,
        };
        self.catalog.create_function(func, or_replace)?;
        self.refresh_user_functions();
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_drop_function(&self, name: &str, if_exists: bool) -> Result<Table> {
        if !self.catalog.function_exists(name) && !if_exists {
            return Err(Error::invalid_query(format!(
                "Function not found: {}",
                name
            )));
        }
        if self.catalog.function_exists(name) {
            self.catalog.drop_function(name)?;
            self.refresh_user_functions();
        }
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_create_procedure(
        &self,
        name: &str,
        args: &[ProcedureArg],
        body: &[PhysicalPlan],
        or_replace: bool,
        if_not_exists: bool,
    ) -> Result<Table> {
        if if_not_exists && self.catalog.procedure_exists(name) {
            return Ok(Table::empty(Schema::new()));
        }
        let proc = UserProcedure {
            name: name.to_string(),
            parameters: args.to_vec(),
            body: Vec::new(),
        };
        self.catalog
            .create_procedure(proc, or_replace, if_not_exists)?;
        self.catalog.set_procedure_body(name, body.to_vec());
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_drop_procedure(&self, name: &str, if_exists: bool) -> Result<Table> {
        if !self.catalog.procedure_exists(name) && !if_exists {
            return Err(Error::invalid_query(format!(
                "Procedure not found: {}",
                name
            )));
        }
        if self.catalog.procedure_exists(name) {
            self.catalog.drop_procedure(name)?;
        }
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_create_snapshot(
        &self,
        snapshot_name: &str,
        source_name: &str,
        if_not_exists: bool,
    ) -> Result<Table> {
        if self.catalog.table_exists(snapshot_name) {
            if if_not_exists {
                return Ok(Table::empty(Schema::new()));
            }
            return Err(Error::invalid_query(format!(
                "Snapshot already exists: {}",
                snapshot_name
            )));
        }

        let source = self
            .tables
            .get_table(source_name)
            .ok_or_else(|| Error::TableNotFound(source_name.to_string()))?
            .clone();

        self.catalog.insert_table(snapshot_name, source)?;
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_drop_snapshot(
        &self,
        snapshot_name: &str,
        if_exists: bool,
    ) -> Result<Table> {
        if !self.catalog.table_exists(snapshot_name) {
            if if_exists {
                return Ok(Table::empty(Schema::new()));
            }
            return Err(Error::TableNotFound(snapshot_name.to_string()));
        }
        self.catalog.drop_table(snapshot_name)?;
        Ok(Table::empty(Schema::new()))
    }
}
