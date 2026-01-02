#![coverage(off)]

use sqlparser::ast::{self, ObjectName};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::DataType;
use yachtsql_ir::{
    AlterColumnAction, AlterTableOp, ColumnDef, Expr, FunctionArg, FunctionBody, Literal,
    LogicalPlan, PlanField, PlanSchema,
};

use super::{Planner, object_name_to_raw_string};
use crate::CatalogProvider;
use crate::expr_planner::ExprPlanner;

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub(super) fn plan_create_table(&self, create: &ast::CreateTable) -> Result<LogicalPlan> {
        let table_name = object_name_to_raw_string(&create.name);
        let empty_schema = PlanSchema::new();

        let default_collation = match &create.table_options {
            ast::CreateTableOptions::Plain(opts) => opts.iter().find_map(|opt| {
                if let ast::SqlOption::KeyValue {
                    key,
                    value:
                        ast::Expr::Value(ast::ValueWithSpan {
                            value: ast::Value::SingleQuotedString(v),
                            ..
                        }),
                } = opt
                    && key.value.eq_ignore_ascii_case("DEFAULT COLLATE")
                {
                    return Some(v.clone());
                }
                None
            }),
            _ => None,
        };

        let columns: Vec<ColumnDef> = create
            .columns
            .iter()
            .map(|col| {
                let data_type = self.sql_type_to_data_type(&col.data_type);
                let nullable = !col
                    .options
                    .iter()
                    .any(|o| matches!(o.option, ast::ColumnOption::NotNull));
                let default_value = col.options.iter().find_map(|o| match &o.option {
                    ast::ColumnOption::Default(expr) => {
                        ExprPlanner::plan_expr(expr, &empty_schema).ok()
                    }
                    _ => None,
                });
                let col_collation = col.options.iter().find_map(|o| match &o.option {
                    ast::ColumnOption::Collation(obj_name) => {
                        let s = obj_name.to_string();
                        let trimmed = s.trim_matches(|c| c == '\'' || c == '"' || c == '`');
                        Some(trimmed.to_string())
                    }
                    ast::ColumnOption::Options(opts) => opts.iter().find_map(|opt| {
                        if let ast::SqlOption::KeyValue {
                            key,
                            value:
                                ast::Expr::Value(ast::ValueWithSpan {
                                    value: ast::Value::SingleQuotedString(v),
                                    ..
                                }),
                        } = opt
                            && key.value.eq_ignore_ascii_case("COLLATE")
                        {
                            return Some(v.clone());
                        }
                        None
                    }),
                    _ => None,
                });
                let collation = col_collation.or_else(|| {
                    if matches!(data_type, DataType::String) {
                        default_collation.clone()
                    } else {
                        None
                    }
                });
                ColumnDef {
                    name: col.name.value.clone(),
                    data_type,
                    nullable,
                    default_value,
                    collation,
                }
            })
            .collect();

        let query = if let Some(query_box) = &create.query {
            Some(Box::new(self.plan_query(query_box)?))
        } else if let Some(clone_source) = &create.clone {
            let source_name = object_name_to_raw_string(clone_source);
            Some(Box::new(LogicalPlan::Scan {
                table_name: source_name,
                schema: PlanSchema::new(),
                projection: None,
            }))
        } else if let Some(copy_source) = &create.copy {
            let source_name = object_name_to_raw_string(copy_source);
            Some(Box::new(LogicalPlan::Scan {
                table_name: source_name,
                schema: PlanSchema::new(),
                projection: None,
            }))
        } else {
            None
        };

        Ok(LogicalPlan::CreateTable {
            table_name,
            columns,
            if_not_exists: create.if_not_exists,
            or_replace: create.or_replace,
            query,
        })
    }

    pub(super) fn plan_drop(
        &self,
        object_type: &ast::ObjectType,
        names: &[ast::ObjectName],
        if_exists: bool,
        cascade: bool,
    ) -> Result<LogicalPlan> {
        match object_type {
            ast::ObjectType::Table => {
                if names.is_empty() {
                    return Err(Error::parse_error("DROP TABLE requires a table name"));
                }
                let table_names: Vec<String> =
                    names.iter().map(object_name_to_raw_string).collect();

                Ok(LogicalPlan::DropTable {
                    table_names,
                    if_exists,
                })
            }
            ast::ObjectType::Schema => {
                let name = names
                    .first()
                    .map(object_name_to_raw_string)
                    .ok_or_else(|| Error::parse_error("DROP SCHEMA requires a schema name"))?;

                Ok(LogicalPlan::DropSchema {
                    name,
                    if_exists,
                    cascade,
                })
            }
            ast::ObjectType::View => {
                let name = names
                    .first()
                    .map(object_name_to_raw_string)
                    .ok_or_else(|| Error::parse_error("DROP VIEW requires a view name"))?;

                Ok(LogicalPlan::DropView { name, if_exists })
            }
            ast::ObjectType::MaterializedView => {
                let name = names
                    .first()
                    .map(object_name_to_raw_string)
                    .ok_or_else(|| {
                        Error::parse_error("DROP MATERIALIZED VIEW requires a view name")
                    })?;

                Ok(LogicalPlan::DropView { name, if_exists })
            }
            _ => Err(Error::unsupported(format!(
                "Unsupported DROP object type: {:?}",
                object_type
            ))),
        }
    }

    pub(super) fn plan_truncate(
        &self,
        table_names: &[ast::TruncateTableTarget],
    ) -> Result<LogicalPlan> {
        let table_name = table_names
            .first()
            .map(|t| object_name_to_raw_string(&t.name))
            .ok_or_else(|| Error::parse_error("TRUNCATE requires a table name"))?;

        Ok(LogicalPlan::Truncate { table_name })
    }

    pub(super) fn plan_create_schema(
        &self,
        schema_name: &ast::SchemaName,
        if_not_exists: bool,
        or_replace: bool,
    ) -> Result<LogicalPlan> {
        let name = match schema_name {
            ast::SchemaName::Simple(name) => object_name_to_raw_string(name),
            ast::SchemaName::UnnamedAuthorization(auth) => auth.value.clone(),
            ast::SchemaName::NamedAuthorization(name, _) => object_name_to_raw_string(name),
        };
        Ok(LogicalPlan::CreateSchema {
            name,
            if_not_exists,
            or_replace,
        })
    }

    pub(super) fn plan_alter_schema(&self, alter_schema: &ast::AlterSchema) -> Result<LogicalPlan> {
        let name = object_name_to_raw_string(&alter_schema.name);
        let mut options = Vec::new();

        for operation in &alter_schema.operations {
            match operation {
                ast::AlterSchemaOperation::SetOptionsParens { options: opts } => {
                    for opt in opts {
                        if let ast::SqlOption::KeyValue { key, value } = opt {
                            let key_str = key.value.clone();
                            let value_str = self.extract_sql_option_value(value);
                            options.push((key_str, value_str));
                        }
                    }
                }
                ast::AlterSchemaOperation::SetDefaultCollate { collate } => {
                    let collate_str = self.extract_sql_option_value(collate);
                    options.push(("default_collate".to_string(), collate_str));
                }
                ast::AlterSchemaOperation::Rename { name: new_name } => {
                    options.push(("rename_to".to_string(), new_name.to_string()));
                }
                ast::AlterSchemaOperation::OwnerTo { owner } => {
                    options.push(("owner".to_string(), owner.to_string()));
                }
                ast::AlterSchemaOperation::AddReplica { .. }
                | ast::AlterSchemaOperation::DropReplica { .. } => {
                    return Err(Error::unsupported(format!(
                        "ALTER SCHEMA operation not supported: {:?}",
                        operation
                    )));
                }
            }
        }

        Ok(LogicalPlan::AlterSchema { name, options })
    }

    pub(super) fn extract_sql_option_value(&self, expr: &ast::Expr) -> String {
        match expr {
            ast::Expr::Value(v) => match &v.value {
                ast::Value::SingleQuotedString(s)
                | ast::Value::DoubleQuotedString(s)
                | ast::Value::DollarQuotedString(ast::DollarQuotedString { value: s, .. }) => {
                    s.clone()
                }
                ast::Value::Number(n, _) => n.clone(),
                _ => format!("{}", expr),
            },
            _ => format!("{}", expr),
        }
    }

    pub(super) fn plan_create_function(&self, create: &ast::CreateFunction) -> Result<LogicalPlan> {
        let name = object_name_to_raw_string(&create.name).to_uppercase();

        let language = create
            .language
            .as_ref()
            .map(|l| l.value.to_uppercase())
            .unwrap_or_else(|| "SQL".to_string());

        let args: Vec<FunctionArg> = create
            .args
            .as_ref()
            .map(|args| {
                args.iter()
                    .filter_map(|arg| {
                        let param_name = arg.name.as_ref()?.value.clone();
                        let data_type = Self::convert_sql_type(&arg.data_type);
                        let default = arg
                            .default_expr
                            .as_ref()
                            .and_then(|e| ExprPlanner::plan_expr(e, &PlanSchema::new()).ok());
                        Some(FunctionArg {
                            name: param_name,
                            data_type,
                            default,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let arg_schema = PlanSchema::from_fields(
            args.iter()
                .map(|a| PlanField::new(a.name.clone(), a.data_type.clone()))
                .collect(),
        );

        let body = if create.remote_connection.is_some() {
            FunctionBody::Language {
                name: "REMOTE".to_string(),
                code: String::new(),
            }
        } else {
            match language.as_str() {
                "JAVASCRIPT" | "JS" => {
                    let js_code = match &create.function_body {
                        Some(ast::CreateFunctionBody::AsBeforeOptions(expr)) => {
                            self.extract_string_from_expr(expr)?
                        }
                        Some(ast::CreateFunctionBody::AsAfterOptions(expr)) => {
                            self.extract_string_from_expr(expr)?
                        }
                        _ => {
                            return Err(Error::InvalidQuery(
                                "JavaScript UDF requires AS 'code' body".to_string(),
                            ));
                        }
                    };
                    FunctionBody::JavaScript(js_code)
                }
                "PYTHON" => {
                    let py_code = match &create.function_body {
                        Some(ast::CreateFunctionBody::AsBeforeOptions(expr)) => {
                            self.extract_string_from_expr(expr)?
                        }
                        Some(ast::CreateFunctionBody::AsAfterOptions(expr)) => {
                            self.extract_string_from_expr(expr)?
                        }
                        _ => {
                            return Err(Error::InvalidQuery(
                                "Python UDF requires AS '''code''' body".to_string(),
                            ));
                        }
                    };
                    FunctionBody::Language {
                        name: "PYTHON".to_string(),
                        code: py_code,
                    }
                }
                "SQL" | "" => {
                    let body_expr = match &create.function_body {
                        Some(ast::CreateFunctionBody::AsBeforeOptions(expr)) => expr,
                        Some(ast::CreateFunctionBody::AsAfterOptions(expr)) => expr,
                        _ => {
                            return Err(Error::UnsupportedFeature(
                                "SQL UDF requires AS (expr) body".to_string(),
                            ));
                        }
                    };

                    let is_table_function = create.table_function
                        || matches!(&create.return_type, Some(ast::DataType::Table(_)));
                    if is_table_function {
                        let query_str = match body_expr {
                            ast::Expr::Subquery(query) => query.to_string(),
                            ast::Expr::Nested(inner) => match inner.as_ref() {
                                ast::Expr::Subquery(query) => query.to_string(),
                                _ => {
                                    return Err(Error::InvalidQuery(
                                        "TABLE FUNCTION body must be a subquery".to_string(),
                                    ));
                                }
                            },
                            _ => {
                                return Err(Error::InvalidQuery(
                                    "TABLE FUNCTION body must be a subquery".to_string(),
                                ));
                            }
                        };
                        FunctionBody::SqlQuery(query_str)
                    } else {
                        let expr = ExprPlanner::plan_expr(body_expr, &arg_schema)?;
                        FunctionBody::Sql(Box::new(expr))
                    }
                }
                _ => {
                    return Err(Error::UnsupportedFeature(format!(
                        "Unsupported function language: {}. Supported: SQL, JAVASCRIPT, PYTHON",
                        language
                    )));
                }
            }
        };

        let return_type = match &create.return_type {
            Some(dt) => self.sql_type_to_data_type(dt),
            None => match &body {
                FunctionBody::Sql(expr) => Self::infer_expr_type_static(expr, &arg_schema),
                FunctionBody::SqlQuery(_) => DataType::Unknown,
                FunctionBody::JavaScript(_) | FunctionBody::Language { .. } => {
                    return Err(Error::InvalidQuery(
                        "RETURNS clause is required for non-SQL functions".to_string(),
                    ));
                }
            },
        };

        Ok(LogicalPlan::CreateFunction {
            name,
            args,
            return_type,
            body,
            or_replace: create.or_replace,
            if_not_exists: create.if_not_exists,
            is_temp: create.temporary,
            is_aggregate: create.aggregate,
        })
    }

    pub(super) fn extract_string_from_expr(&self, expr: &ast::Expr) -> Result<String> {
        match expr {
            ast::Expr::Value(val_with_span) => match &val_with_span.value {
                ast::Value::SingleQuotedString(s)
                | ast::Value::DoubleQuotedString(s)
                | ast::Value::TripleSingleQuotedString(s)
                | ast::Value::TripleDoubleQuotedString(s)
                | ast::Value::TripleSingleQuotedRawStringLiteral(s)
                | ast::Value::TripleDoubleQuotedRawStringLiteral(s)
                | ast::Value::SingleQuotedRawStringLiteral(s)
                | ast::Value::DoubleQuotedRawStringLiteral(s) => Ok(s.clone()),
                _ => Err(Error::InvalidQuery(format!(
                    "Expected string literal for function body, got: {:?}",
                    expr
                ))),
            },
            _ => Err(Error::InvalidQuery(format!(
                "Expected string literal for function body, got: {:?}",
                expr
            ))),
        }
    }

    pub(super) fn plan_alter_table(
        &self,
        name: &ObjectName,
        operations: &[ast::AlterTableOperation],
        if_exists: bool,
    ) -> Result<LogicalPlan> {
        let table_name = object_name_to_raw_string(name);

        if operations.len() > 1 {
            let mut plans: Vec<LogicalPlan> = Vec::new();
            for operation in operations {
                let op = self.plan_single_alter_op(operation)?;
                plans.push(LogicalPlan::AlterTable {
                    table_name: table_name.clone(),
                    operation: op,
                    if_exists,
                });
            }
            return Ok(LogicalPlan::If {
                condition: Expr::Literal(Literal::Bool(true)),
                then_branch: plans,
                else_branch: None,
            });
        }

        let operation = operations
            .first()
            .ok_or_else(|| Error::parse_error("ALTER TABLE requires an operation"))?;

        let op = self.plan_single_alter_op(operation)?;

        Ok(LogicalPlan::AlterTable {
            table_name,
            operation: op,
            if_exists,
        })
    }

    pub(super) fn plan_single_alter_op(
        &self,
        operation: &ast::AlterTableOperation,
    ) -> Result<AlterTableOp> {
        match operation {
            ast::AlterTableOperation::AddColumn {
                column_def,
                if_not_exists,
                ..
            } => {
                let data_type = self.sql_type_to_data_type(&column_def.data_type);
                let nullable = !column_def
                    .options
                    .iter()
                    .any(|o| matches!(o.option, ast::ColumnOption::NotNull));
                let empty_schema = PlanSchema::new();
                let default_value = column_def.options.iter().find_map(|o| match &o.option {
                    ast::ColumnOption::Default(expr) => {
                        ExprPlanner::plan_expr(expr, &empty_schema).ok()
                    }
                    _ => None,
                });
                Ok(AlterTableOp::AddColumn {
                    column: ColumnDef {
                        name: column_def.name.value.clone(),
                        data_type,
                        nullable,
                        default_value,
                        collation: None,
                    },
                    if_not_exists: *if_not_exists,
                })
            }
            ast::AlterTableOperation::DropColumn {
                column_names,
                if_exists,
                ..
            } => {
                let name = column_names
                    .first()
                    .map(|c| c.value.clone())
                    .unwrap_or_default();
                Ok(AlterTableOp::DropColumn {
                    name,
                    if_exists: *if_exists,
                })
            }
            ast::AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => Ok(AlterTableOp::RenameColumn {
                old_name: old_column_name.value.clone(),
                new_name: new_column_name.value.clone(),
            }),
            ast::AlterTableOperation::RenameTable {
                table_name: new_name,
            } => {
                let new_name_str = match new_name {
                    ast::RenameTableNameKind::To(name) | ast::RenameTableNameKind::As(name) => {
                        object_name_to_raw_string(name)
                    }
                };
                Ok(AlterTableOp::RenameTable {
                    new_name: new_name_str,
                })
            }
            ast::AlterTableOperation::AlterColumn { column_name, op } => {
                let action = match op {
                    ast::AlterColumnOperation::SetNotNull => AlterColumnAction::SetNotNull,
                    ast::AlterColumnOperation::DropNotNull => AlterColumnAction::DropNotNull,
                    ast::AlterColumnOperation::DropDefault => AlterColumnAction::DropDefault,
                    ast::AlterColumnOperation::SetDefault { value } => {
                        let empty_schema = PlanSchema::new();
                        let expr = ExprPlanner::plan_expr(value, &empty_schema)?;
                        AlterColumnAction::SetDefault { default: expr }
                    }
                    ast::AlterColumnOperation::SetDataType { data_type, .. } => {
                        let dt = self.sql_type_to_data_type(data_type);
                        AlterColumnAction::SetDataType { data_type: dt }
                    }
                    ast::AlterColumnOperation::SetOptions { options } => {
                        let collation = options.iter().find_map(|opt| match opt {
                            ast::SqlOption::KeyValue { key, value }
                                if key.value.to_lowercase() == "collate" =>
                            {
                                Some(value.to_string().trim_matches('\'').to_string())
                            }
                            _ => None,
                        });
                        AlterColumnAction::SetOptions { collation }
                    }
                    _ => {
                        return Err(Error::unsupported(format!(
                            "Unsupported ALTER COLUMN operation: {:?}",
                            op
                        )));
                    }
                };
                Ok(AlterTableOp::AlterColumn {
                    name: column_name.value.clone(),
                    action,
                })
            }
            ast::AlterTableOperation::AddConstraint { constraint, .. } => {
                let table_constraint = self.plan_table_constraint(constraint)?;
                Ok(AlterTableOp::AddConstraint {
                    constraint: table_constraint,
                })
            }
            ast::AlterTableOperation::DropConstraint { name, .. } => {
                Ok(AlterTableOp::DropConstraint {
                    name: name.to_string(),
                })
            }
            ast::AlterTableOperation::DropPrimaryKey { .. } => Ok(AlterTableOp::DropPrimaryKey),
            ast::AlterTableOperation::SetTblProperties { table_properties } => {
                let options: Vec<(String, String)> = table_properties
                    .iter()
                    .filter_map(|opt| match opt {
                        ast::SqlOption::KeyValue { key, value } => {
                            Some((key.to_string(), value.to_string()))
                        }
                        _ => None,
                    })
                    .collect();
                Ok(AlterTableOp::SetOptions { options })
            }
            ast::AlterTableOperation::SetDefaultCollate { .. } => {
                Ok(AlterTableOp::SetOptions { options: vec![] })
            }
            _ => Err(Error::unsupported(format!(
                "Unsupported ALTER TABLE operation: {:?}",
                operation
            ))),
        }
    }

    pub(super) fn plan_create_view(
        &self,
        name: &ObjectName,
        columns: &[ast::ViewColumnDef],
        query: &ast::Query,
        or_replace: bool,
        if_not_exists: bool,
    ) -> Result<LogicalPlan> {
        let view_name = object_name_to_raw_string(name);
        let query_sql = query.to_string();
        let query_plan = self.plan_query(query)?;
        let column_aliases: Vec<String> = columns.iter().map(|c| c.name.value.clone()).collect();

        Ok(LogicalPlan::CreateView {
            name: view_name,
            query: Box::new(query_plan),
            query_sql,
            column_aliases,
            or_replace,
            if_not_exists,
        })
    }
}
