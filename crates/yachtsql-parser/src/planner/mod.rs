#![coverage(off)]

use std::cell::RefCell;
use std::collections::HashMap;

use sqlparser::ast::{self, ObjectName, ObjectNamePart, Statement, TableObject};
use yachtsql_common::error::{Error, Result};
use yachtsql_ir::{LogicalPlan, PlanSchema};

use crate::CatalogProvider;
use crate::expr_planner::ExprPlanner;

mod dcl;
mod ddl;
mod dml;
mod query;
mod scripting;

pub(super) fn object_name_to_raw_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .filter_map(|part| match part {
            ObjectNamePart::Identifier(ident) => Some(ident.value.as_str()),
            ObjectNamePart::Function(_) => None,
        })
        .collect::<Vec<_>>()
        .join(".")
}

pub(super) fn table_object_to_raw_string(table: &TableObject) -> String {
    match table {
        TableObject::TableName(name) => object_name_to_raw_string(name),
        TableObject::TableFunction(func) => object_name_to_raw_string(&func.name),
    }
}

pub struct Planner<'a, C: CatalogProvider> {
    catalog: &'a C,
    cte_schemas: RefCell<HashMap<String, PlanSchema>>,
    outer_schema: RefCell<Option<PlanSchema>>,
}

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub fn new(catalog: &'a C) -> Self {
        Self {
            catalog,
            cte_schemas: RefCell::new(HashMap::new()),
            outer_schema: RefCell::new(None),
        }
    }

    fn with_outer_schema(&self, schema: &PlanSchema) {
        *self.outer_schema.borrow_mut() = Some(schema.clone());
    }

    fn clear_outer_schema(&self) {
        *self.outer_schema.borrow_mut() = None;
    }

    pub fn plan_statement(&self, stmt: &Statement) -> Result<LogicalPlan> {
        match stmt {
            Statement::Query(query) => self.plan_query(query),
            Statement::Insert(insert) => self.plan_insert(insert),
            Statement::Update {
                table,
                assignments,
                from,
                selection,
                ..
            } => self.plan_update(table, assignments, from.as_ref(), selection.as_ref()),
            Statement::Delete(delete) => self.plan_delete(delete),
            Statement::CreateTable(create) => self.plan_create_table(create),
            Statement::Drop {
                object_type,
                names,
                if_exists,
                cascade,
                ..
            } => self.plan_drop(object_type, names, *if_exists, *cascade),
            Statement::Truncate { table_names, .. } => self.plan_truncate(table_names),
            Statement::AlterTable {
                name,
                operations,
                if_exists,
                ..
            } => self.plan_alter_table(name, operations, *if_exists),
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
                or_replace,
                ..
            } => self.plan_create_schema(schema_name, *if_not_exists, *or_replace),
            Statement::AlterSchema(alter_schema) => self.plan_alter_schema(alter_schema),
            Statement::UndropSchema {
                if_not_exists,
                schema_name,
            } => {
                let name = object_name_to_raw_string(schema_name);
                Ok(LogicalPlan::UndropSchema {
                    name,
                    if_not_exists: *if_not_exists,
                })
            }
            Statement::AlterMaterializedView { .. } => Ok(LogicalPlan::Empty {
                schema: PlanSchema::new(),
            }),
            Statement::AlterViewWithOperations { .. } => Ok(LogicalPlan::Empty {
                schema: PlanSchema::new(),
            }),
            Statement::AlterFunction { .. } => Ok(LogicalPlan::Empty {
                schema: PlanSchema::new(),
            }),
            Statement::AlterProcedure { .. } => Ok(LogicalPlan::Empty {
                schema: PlanSchema::new(),
            }),
            Statement::CreateSearchIndex { .. } => Ok(LogicalPlan::Empty {
                schema: PlanSchema::new(),
            }),
            Statement::CreateVectorIndex { .. } => Ok(LogicalPlan::Empty {
                schema: PlanSchema::new(),
            }),
            Statement::CreateRowAccessPolicy { .. } => Ok(LogicalPlan::Empty {
                schema: PlanSchema::new(),
            }),
            Statement::DropSearchIndex { .. } => Ok(LogicalPlan::Empty {
                schema: PlanSchema::new(),
            }),
            Statement::DropVectorIndex { .. } => Ok(LogicalPlan::Empty {
                schema: PlanSchema::new(),
            }),
            Statement::DropRowAccessPolicy { .. } => Ok(LogicalPlan::Empty {
                schema: PlanSchema::new(),
            }),
            Statement::DropAllRowAccessPolicies { .. } => Ok(LogicalPlan::Empty {
                schema: PlanSchema::new(),
            }),
            Statement::CreateMaterializedViewReplica { .. } => Ok(LogicalPlan::Empty {
                schema: PlanSchema::new(),
            }),
            Statement::CreateView {
                name,
                columns,
                query,
                or_replace,
                if_not_exists,
                ..
            } => self.plan_create_view(name, columns, query, *or_replace, *if_not_exists),
            Statement::CreateFunction(create) => self.plan_create_function(create),
            Statement::DropFunction {
                func_desc,
                if_exists,
                ..
            } => {
                let name = func_desc
                    .first()
                    .map(|desc| desc.name.to_string())
                    .ok_or_else(|| Error::parse_error("DROP FUNCTION requires a function name"))?;

                Ok(LogicalPlan::DropFunction {
                    name,
                    if_exists: *if_exists,
                })
            }
            Statement::Set(set_stmt) => self.plan_set(set_stmt),
            Statement::ExportData(export_data) => self.plan_export_data(export_data),
            Statement::Merge {
                table,
                source,
                on,
                clauses,
                ..
            } => self.plan_merge(table, source, on, clauses),
            Statement::StartTransaction {
                statements,
                exception,
                label,
                ..
            } => {
                if !statements.is_empty() || exception.is_some() {
                    let block_label = label.as_ref().map(|l| l.value.clone());

                    if let Some(exception_whens) = exception {
                        let try_block: Vec<(LogicalPlan, Option<String>)> = statements
                            .iter()
                            .map(|stmt| {
                                let sql_text = format!("{}", stmt).trim().to_string();
                                let plan = self.plan_statement(stmt)?;
                                Ok((plan, Some(sql_text)))
                            })
                            .collect::<Result<Vec<_>>>()?;

                        let catch_block: Vec<LogicalPlan> = exception_whens
                            .iter()
                            .flat_map(|ew| &ew.statements)
                            .map(|stmt| self.plan_statement(stmt))
                            .collect::<Result<Vec<_>>>()?;

                        if block_label.is_some() {
                            Ok(LogicalPlan::Block {
                                body: vec![LogicalPlan::TryCatch {
                                    try_block,
                                    catch_block,
                                }],
                                label: block_label,
                            })
                        } else {
                            Ok(LogicalPlan::TryCatch {
                                try_block,
                                catch_block,
                            })
                        }
                    } else {
                        let body: Vec<LogicalPlan> = statements
                            .iter()
                            .map(|stmt| self.plan_statement(stmt))
                            .collect::<Result<Vec<_>>>()?;

                        if block_label.is_some() {
                            Ok(LogicalPlan::Block {
                                body,
                                label: block_label,
                            })
                        } else {
                            Ok(LogicalPlan::Block { body, label: None })
                        }
                    }
                } else {
                    Ok(LogicalPlan::BeginTransaction)
                }
            }
            Statement::Commit { .. } => Ok(LogicalPlan::Commit),
            Statement::Rollback { .. } => Ok(LogicalPlan::Rollback),
            Statement::CreateProcedure {
                or_alter,
                if_not_exists,
                name,
                params,
                body,
                ..
            } => self.plan_create_procedure(name, params, body, *or_alter, *if_not_exists),
            Statement::DropProcedure {
                if_exists,
                proc_desc,
                ..
            } => {
                let name = proc_desc
                    .first()
                    .map(|desc| desc.name.to_string())
                    .ok_or_else(|| {
                        Error::parse_error("DROP PROCEDURE requires a procedure name")
                    })?;

                Ok(LogicalPlan::DropProcedure {
                    name,
                    if_exists: *if_exists,
                })
            }
            Statement::Call(func) => self.plan_call(func),
            Statement::Declare { stmts } => self.plan_declare(stmts),
            Statement::Assert { condition, message } => {
                self.plan_assert(condition, message.as_ref())
            }
            Statement::If(if_stmt) => self.plan_if(if_stmt),
            Statement::While(while_stmt) => self.plan_while(while_stmt),
            Statement::Loop(loop_stmt) => self.plan_loop(loop_stmt),
            Statement::For(for_stmt) => self.plan_for(for_stmt),
            Statement::Case(case_stmt) => self.plan_case(case_stmt),
            Statement::Repeat(repeat_stmt) => self.plan_repeat(repeat_stmt),
            Statement::Leave { label } => Ok(LogicalPlan::Break {
                label: label.as_ref().map(|i| i.value.clone()),
            }),
            Statement::Break { label } => Ok(LogicalPlan::Break {
                label: label.as_ref().map(|i| i.value.clone()),
            }),
            Statement::Iterate { label } => Ok(LogicalPlan::Continue {
                label: label.as_ref().map(|i| i.value.clone()),
            }),
            Statement::Continue { label } => Ok(LogicalPlan::Continue {
                label: label.as_ref().map(|i| i.value.clone()),
            }),
            Statement::Grant {
                privileges,
                objects,
                grantees,
                ..
            } => self.plan_grant(privileges, objects.as_ref(), grantees),
            Statement::Revoke {
                privileges,
                objects,
                grantees,
                ..
            } => self.plan_revoke(privileges, objects.as_ref(), grantees),
            Statement::Raise(raise_stmt) => self.plan_raise(raise_stmt),
            Statement::Execute {
                parameters,
                immediate,
                into,
                using,
                ..
            } => {
                if !*immediate {
                    return Err(Error::unsupported("Only EXECUTE IMMEDIATE is supported"));
                }
                self.plan_execute_immediate(parameters, into, using)
            }
            Statement::Return(return_stmt) => {
                let value = match &return_stmt.value {
                    Some(ast::ReturnStatementValue::Expr(expr)) => {
                        Some(ExprPlanner::plan_expr(expr, &PlanSchema::new())?)
                    }
                    None => None,
                };
                Ok(LogicalPlan::Return { value })
            }
            _ => Err(Error::unsupported(format!(
                "Unsupported statement: {:?}",
                stmt
            ))),
        }
    }
}
