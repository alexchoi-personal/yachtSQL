#![coverage(off)]

use sqlparser::ast::{self, ObjectName};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::DataType;
use yachtsql_ir::{
    Expr, Literal, LogicalPlan, PlanSchema, ProcedureArg, ProcedureArgMode, RaiseLevel,
};

use super::{Planner, object_name_to_raw_string};
use crate::CatalogProvider;
use crate::expr_planner::ExprPlanner;

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub(super) fn plan_loop(&self, loop_stmt: &ast::LoopStatement) -> Result<LogicalPlan> {
        let body = loop_stmt
            .body
            .iter()
            .map(|s| self.plan_statement(s))
            .collect::<Result<Vec<_>>>()?;
        let label = loop_stmt.label.as_ref().map(|l| l.value.clone());
        Ok(LogicalPlan::Loop { body, label })
    }

    pub(super) fn plan_for(&self, for_stmt: &ast::ForStatement) -> Result<LogicalPlan> {
        let variable = for_stmt.variable.value.clone();
        let query = self.plan_query(&for_stmt.query)?;
        let body = for_stmt
            .body
            .iter()
            .map(|s| self.plan_statement(s))
            .collect::<Result<Vec<_>>>()?;
        Ok(LogicalPlan::For {
            variable,
            query: Box::new(query),
            body,
        })
    }

    pub(super) fn plan_assert(
        &self,
        condition: &ast::Expr,
        message: Option<&ast::Expr>,
    ) -> Result<LogicalPlan> {
        let empty_schema = PlanSchema::new();
        let subquery_planner = |query: &ast::Query| self.plan_query(query);
        let cond_expr = ExprPlanner::plan_expr_with_subquery(
            condition,
            &empty_schema,
            Some(&subquery_planner),
        )?;
        let msg_expr = message
            .map(|m| {
                ExprPlanner::plan_expr_with_subquery(m, &empty_schema, Some(&subquery_planner))
            })
            .transpose()?;
        Ok(LogicalPlan::Assert {
            condition: cond_expr,
            message: msg_expr,
        })
    }

    pub(super) fn plan_raise(&self, raise_stmt: &ast::RaiseStatement) -> Result<LogicalPlan> {
        let empty_schema = PlanSchema::new();
        let message = match &raise_stmt.value {
            Some(ast::RaiseStatementValue::UsingMessage(expr))
            | Some(ast::RaiseStatementValue::Expr(expr)) => {
                Some(ExprPlanner::plan_expr(expr, &empty_schema)?)
            }
            None => None,
        };
        Ok(LogicalPlan::Raise {
            message,
            level: RaiseLevel::Exception,
        })
    }

    pub(super) fn plan_execute_immediate(
        &self,
        parameters: &[ast::Expr],
        into_vars: &[ast::Ident],
        using_params: &[ast::ExprWithAlias],
    ) -> Result<LogicalPlan> {
        let empty_schema = PlanSchema::new();

        let sql_expr = if let Some(first_param) = parameters.first() {
            ExprPlanner::plan_expr(first_param, &empty_schema)?
        } else {
            return Err(Error::parse_error(
                "EXECUTE IMMEDIATE requires a SQL string",
            ));
        };

        let into_variables: Vec<String> = into_vars.iter().map(|i| i.value.clone()).collect();

        let using_params_ir: Vec<(Expr, Option<String>)> = using_params
            .iter()
            .map(|p| {
                let expr = ExprPlanner::plan_expr(&p.expr, &empty_schema)?;
                let alias = p.alias.as_ref().map(|a| a.value.clone());
                Ok((expr, alias))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(LogicalPlan::ExecuteImmediate {
            sql_expr,
            into_variables,
            using_params: using_params_ir,
        })
    }

    pub(super) fn plan_if(&self, if_stmt: &ast::IfStatement) -> Result<LogicalPlan> {
        let empty_schema = PlanSchema::new();
        let subquery_planner = |query: &ast::Query| self.plan_query(query);
        let condition = if_stmt
            .if_block
            .condition
            .as_ref()
            .ok_or_else(|| Error::parse_error("IF statement missing condition"))?;
        let cond_expr = ExprPlanner::plan_expr_with_subquery(
            condition,
            &empty_schema,
            Some(&subquery_planner),
        )?;

        let then_branch = self.plan_statement_sequence(&if_stmt.if_block.conditional_statements)?;

        let mut else_branch = if let Some(else_block) = &if_stmt.else_block {
            Some(self.plan_statement_sequence(&else_block.conditional_statements)?)
        } else {
            None
        };

        for elseif_block in if_stmt.elseif_blocks.iter().rev() {
            if let Some(elseif_cond) = &elseif_block.condition {
                let elseif_cond_expr = ExprPlanner::plan_expr_with_subquery(
                    elseif_cond,
                    &empty_schema,
                    Some(&subquery_planner),
                )?;
                let elseif_then =
                    self.plan_statement_sequence(&elseif_block.conditional_statements)?;
                let nested_if = LogicalPlan::If {
                    condition: elseif_cond_expr,
                    then_branch: elseif_then,
                    else_branch,
                };
                else_branch = Some(vec![nested_if]);
            }
        }

        Ok(LogicalPlan::If {
            condition: cond_expr,
            then_branch,
            else_branch,
        })
    }

    pub(super) fn plan_while(&self, while_stmt: &ast::WhileStatement) -> Result<LogicalPlan> {
        let empty_schema = PlanSchema::new();
        let condition = while_stmt
            .while_block
            .condition
            .as_ref()
            .ok_or_else(|| Error::parse_error("WHILE statement missing condition"))?;
        let cond_expr = ExprPlanner::plan_expr(condition, &empty_schema)?;

        let body = self.plan_statement_sequence(&while_stmt.while_block.conditional_statements)?;
        let label = while_stmt.label.as_ref().map(|i| i.value.clone());

        Ok(LogicalPlan::While {
            condition: cond_expr,
            body,
            label,
        })
    }

    pub(super) fn plan_repeat(&self, repeat_stmt: &ast::RepeatStatement) -> Result<LogicalPlan> {
        let empty_schema = PlanSchema::new();
        let body: Vec<LogicalPlan> = repeat_stmt
            .body
            .iter()
            .map(|stmt| self.plan_statement(stmt))
            .collect::<Result<Vec<_>>>()?;
        let until_condition = ExprPlanner::plan_expr(&repeat_stmt.until_condition, &empty_schema)?;

        Ok(LogicalPlan::Repeat {
            body,
            until_condition,
        })
    }

    pub(super) fn plan_case(&self, case_stmt: &ast::CaseStatement) -> Result<LogicalPlan> {
        let empty_schema = PlanSchema::new();

        let else_branch = if let Some(else_block) = &case_stmt.else_block {
            Some(self.plan_statement_sequence(&else_block.conditional_statements)?)
        } else {
            None
        };

        let mut result_plan = else_branch;

        for when_block in case_stmt.when_blocks.iter().rev() {
            let when_condition = when_block
                .condition
                .as_ref()
                .ok_or_else(|| Error::parse_error("CASE WHEN block missing condition"))?;

            let condition = if let Some(match_expr) = &case_stmt.match_expr {
                let eq_expr = ast::Expr::BinaryOp {
                    left: Box::new(match_expr.clone()),
                    op: ast::BinaryOperator::Eq,
                    right: Box::new(when_condition.clone()),
                };
                ExprPlanner::plan_expr(&eq_expr, &empty_schema)?
            } else {
                ExprPlanner::plan_expr(when_condition, &empty_schema)?
            };

            let then_branch = self.plan_statement_sequence(&when_block.conditional_statements)?;

            result_plan = Some(vec![LogicalPlan::If {
                condition,
                then_branch,
                else_branch: result_plan,
            }]);
        }

        result_plan
            .and_then(|plans| plans.into_iter().next())
            .ok_or_else(|| Error::parse_error("CASE statement has no WHEN blocks"))
    }

    pub(super) fn plan_statement_sequence(
        &self,
        seq: &ast::ConditionalStatements,
    ) -> Result<Vec<LogicalPlan>> {
        match seq {
            ast::ConditionalStatements::Sequence { statements } => statements
                .iter()
                .map(|stmt| self.plan_statement(stmt))
                .collect(),
            ast::ConditionalStatements::BeginEnd(begin_end) => begin_end
                .statements
                .iter()
                .map(|stmt| self.plan_statement(stmt))
                .collect(),
        }
    }

    pub(super) fn plan_declare(&self, stmts: &[ast::Declare]) -> Result<LogicalPlan> {
        if stmts.is_empty() {
            return Err(Error::parse_error("DECLARE requires at least one variable"));
        }

        let mut all_plans: Vec<LogicalPlan> = Vec::new();

        for decl in stmts {
            let data_type = match &decl.data_type {
                Some(dt) => self.sql_type_to_data_type(dt),
                None => DataType::Unknown,
            };

            let empty_schema = PlanSchema::new();
            let default = match &decl.assignment {
                Some(ast::DeclareAssignment::Default(expr))
                | Some(ast::DeclareAssignment::Expr(expr))
                | Some(ast::DeclareAssignment::For(expr))
                | Some(ast::DeclareAssignment::DuckAssignment(expr))
                | Some(ast::DeclareAssignment::MsSqlAssignment(expr)) => {
                    Some(ExprPlanner::plan_expr(expr, &empty_schema)?)
                }
                None => None,
            };

            for name_ident in &decl.names {
                all_plans.push(LogicalPlan::Declare {
                    name: name_ident.value.clone(),
                    data_type: data_type.clone(),
                    default: default.clone(),
                });
            }
        }

        if all_plans.len() == 1 {
            return Ok(all_plans.remove(0));
        }

        Ok(LogicalPlan::If {
            condition: Expr::Literal(Literal::Bool(true)),
            then_branch: all_plans,
            else_branch: None,
        })
    }

    pub(super) fn plan_set(&self, set_stmt: &ast::Set) -> Result<LogicalPlan> {
        match set_stmt {
            ast::Set::SingleAssignment {
                variable, values, ..
            } => {
                let var_name = variable.to_string();
                let value = values
                    .first()
                    .map(|v| self.plan_set_value(v))
                    .transpose()?
                    .unwrap_or(Expr::Literal(Literal::Null));
                Ok(LogicalPlan::SetVariable {
                    name: var_name,
                    value,
                })
            }
            ast::Set::ParenthesizedAssignments { variables, values } => {
                if values.len() == 1 && variables.len() > 1 {
                    let empty_schema = PlanSchema::new();
                    let subquery_planner = |subquery: &ast::Query| self.plan_query(subquery);
                    let value = ExprPlanner::plan_expr_with_subquery(
                        &values[0],
                        &empty_schema,
                        Some(&subquery_planner),
                    )?;
                    let names: Vec<String> = variables.iter().map(|v| v.to_string()).collect();
                    return Ok(LogicalPlan::SetMultipleVariables { names, value });
                }

                if variables.len() != values.len() {
                    return Err(Error::parse_error(
                        "SET: number of variables must match number of values",
                    ));
                }

                let mut plans: Vec<LogicalPlan> = Vec::new();
                for (var, val) in variables.iter().zip(values.iter()) {
                    let var_name = var.to_string();
                    let value = self.plan_set_value(val)?;
                    plans.push(LogicalPlan::SetVariable {
                        name: var_name,
                        value,
                    });
                }

                if plans.len() == 1 {
                    return Ok(plans.remove(0));
                }

                Ok(LogicalPlan::If {
                    condition: Expr::Literal(Literal::Bool(true)),
                    then_branch: plans,
                    else_branch: None,
                })
            }
            _ => Err(Error::unsupported(format!(
                "Unsupported SET statement: {:?}",
                set_stmt
            ))),
        }
    }

    pub(super) fn plan_set_value(&self, expr: &ast::Expr) -> Result<Expr> {
        match expr {
            ast::Expr::Identifier(ident) => {
                if ident.value.starts_with('@') {
                    Ok(Expr::Variable {
                        name: ident.value.clone(),
                    })
                } else {
                    Ok(Expr::Literal(Literal::String(ident.value.clone())))
                }
            }
            ast::Expr::CompoundIdentifier(idents) if !idents.is_empty() => {
                if idents[0].value.starts_with('@') {
                    let empty_schema = PlanSchema::new();
                    ExprPlanner::resolve_compound_identifier(idents, &empty_schema)
                } else if idents.len() >= 2 {
                    let mut expr = Expr::Variable {
                        name: idents[0].value.clone(),
                    };
                    for part in &idents[1..] {
                        expr = Expr::StructAccess {
                            expr: Box::new(expr),
                            field: part.value.clone(),
                        };
                    }
                    Ok(expr)
                } else {
                    let name = idents
                        .iter()
                        .map(|i| i.value.as_str())
                        .collect::<Vec<_>>()
                        .join(".");
                    Ok(Expr::Literal(Literal::String(name)))
                }
            }
            _ => {
                let empty_schema = PlanSchema::new();
                let subquery_planner = |subquery: &ast::Query| self.plan_query(subquery);
                ExprPlanner::plan_expr_with_subquery(expr, &empty_schema, Some(&subquery_planner))
            }
        }
    }

    pub(super) fn plan_create_procedure(
        &self,
        name: &ObjectName,
        params: &Option<Vec<ast::ProcedureParam>>,
        body: &ast::ConditionalStatements,
        or_replace: bool,
        if_not_exists: bool,
    ) -> Result<LogicalPlan> {
        let raw_name = object_name_to_raw_string(name);
        let (proc_name, or_replace) = if let Some(stripped) = raw_name.strip_prefix("__orp__") {
            (stripped.to_string(), true)
        } else {
            (raw_name, or_replace)
        };
        let args = match params {
            Some(params) => params
                .iter()
                .map(|p| {
                    let mode = match p.mode {
                        Some(ast::ArgMode::In) => ProcedureArgMode::In,
                        Some(ast::ArgMode::Out) => ProcedureArgMode::Out,
                        Some(ast::ArgMode::InOut) => ProcedureArgMode::InOut,
                        None => ProcedureArgMode::In,
                    };
                    ProcedureArg {
                        name: p.name.value.clone(),
                        data_type: Self::convert_sql_type(&p.data_type),
                        mode,
                    }
                })
                .collect(),
            None => Vec::new(),
        };

        let body_plans = self.plan_conditional_statements(body)?;

        Ok(LogicalPlan::CreateProcedure {
            name: proc_name,
            args,
            body: body_plans,
            or_replace,
            if_not_exists,
        })
    }

    pub(super) fn plan_conditional_statements(
        &self,
        stmts: &ast::ConditionalStatements,
    ) -> Result<Vec<LogicalPlan>> {
        stmts
            .statements()
            .iter()
            .map(|s| self.plan_statement(s))
            .collect()
    }

    pub(super) fn plan_call(&self, func: &ast::Function) -> Result<LogicalPlan> {
        let procedure_name = object_name_to_raw_string(&func.name);
        let args = match &func.args {
            ast::FunctionArguments::List(args) => args
                .args
                .iter()
                .filter_map(|arg| match arg {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                        ExprPlanner::plan_expr(e, &PlanSchema::new()).ok()
                    }
                    _ => None,
                })
                .collect(),
            ast::FunctionArguments::None => Vec::new(),
            ast::FunctionArguments::Subquery(_) => {
                return Err(Error::unsupported("Subquery in CALL not supported"));
            }
        };

        Ok(LogicalPlan::Call {
            procedure_name,
            args,
        })
    }
}
