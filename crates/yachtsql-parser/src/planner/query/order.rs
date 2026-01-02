#![coverage(off)]

use sqlparser::ast;
use yachtsql_common::error::{Error, Result};
use yachtsql_ir::{Expr, LogicalPlan, PlanSchema, SortExpr, WhenClause};

use super::Planner;
use crate::CatalogProvider;
use crate::expr_planner::ExprPlanner;

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub(super) fn plan_order_by(
        &self,
        input: LogicalPlan,
        order_by: &ast::OrderBy,
    ) -> Result<LogicalPlan> {
        let mut sort_exprs = Vec::new();

        let exprs = match &order_by.kind {
            ast::OrderByKind::All(_) => return Err(Error::unsupported("ORDER BY ALL")),
            ast::OrderByKind::Expressions(exprs) => exprs,
        };

        for order_expr in exprs {
            let expr = ExprPlanner::plan_expr(&order_expr.expr, input.schema())?;
            let asc = order_expr.options.asc.unwrap_or(true);
            let nulls_first = order_expr.options.nulls_first.unwrap_or(!asc);
            sort_exprs.push(SortExpr {
                expr,
                asc,
                nulls_first,
            });
        }

        Ok(LogicalPlan::Sort {
            input: Box::new(input),
            sort_exprs,
        })
    }

    pub(super) fn plan_order_by_with_aliases(
        &self,
        input: LogicalPlan,
        order_by: &ast::OrderBy,
        projection_exprs: &[Expr],
        projection_schema: &PlanSchema,
    ) -> Result<LogicalPlan> {
        let mut sort_exprs = Vec::new();

        let exprs = match &order_by.kind {
            ast::OrderByKind::All(_) => return Err(Error::unsupported("ORDER BY ALL")),
            ast::OrderByKind::Expressions(exprs) => exprs,
        };

        for order_expr in exprs {
            let expr = if let ast::Expr::Identifier(ident) = &order_expr.expr {
                let name = ident.value.to_uppercase();
                let mut found_alias = None;
                for (i, proj_expr) in projection_exprs.iter().enumerate() {
                    if let Expr::Alias {
                        name: alias_name,
                        expr: inner,
                    } = proj_expr
                        && alias_name.to_uppercase() == name
                    {
                        found_alias = Some(inner.as_ref().clone());
                        break;
                    }
                    if i < projection_schema.fields.len()
                        && projection_schema.fields[i].name.to_uppercase() == name
                    {
                        found_alias = Some(proj_expr.clone());
                        break;
                    }
                }
                match found_alias {
                    Some(e) => e,
                    None => ExprPlanner::plan_expr(&order_expr.expr, input.schema())?,
                }
            } else if let ast::Expr::CompoundIdentifier(parts) = &order_expr.expr {
                let last_name = parts
                    .last()
                    .map(|p| p.value.to_uppercase())
                    .unwrap_or_default();
                let mut found_alias = None;
                for (i, proj_expr) in projection_exprs.iter().enumerate() {
                    if let Expr::Alias {
                        name: alias_name,
                        expr: inner,
                    } = proj_expr
                        && alias_name.to_uppercase() == last_name
                    {
                        found_alias = Some(inner.as_ref().clone());
                        break;
                    }
                    if i < projection_schema.fields.len()
                        && projection_schema.fields[i].name.to_uppercase() == last_name
                    {
                        found_alias = Some(proj_expr.clone());
                        break;
                    }
                }
                match found_alias {
                    Some(e) => e,
                    None => ExprPlanner::plan_expr(&order_expr.expr, input.schema())?,
                }
            } else {
                let planned = ExprPlanner::plan_expr(&order_expr.expr, input.schema())
                    .or_else(|_| ExprPlanner::plan_expr(&order_expr.expr, projection_schema))?;
                Self::resolve_order_by_with_aggregates(planned, input.schema(), projection_schema)
            };

            let asc = order_expr.options.asc.unwrap_or(true);
            let nulls_first = order_expr.options.nulls_first.unwrap_or(!asc);
            sort_exprs.push(SortExpr {
                expr,
                asc,
                nulls_first,
            });
        }

        Ok(LogicalPlan::Sort {
            input: Box::new(input),
            sort_exprs,
        })
    }

    fn resolve_order_by_with_aggregates(
        expr: Expr,
        input_schema: &PlanSchema,
        projection_schema: &PlanSchema,
    ) -> Expr {
        match &expr {
            Expr::Aggregate { .. } => {
                let canonical = Self::canonical_planned_agg_name(&expr);
                for (idx, field) in input_schema.fields.iter().enumerate() {
                    if field.name.to_uppercase() == canonical {
                        return Expr::Column {
                            table: None,
                            name: field.name.clone(),
                            index: Some(idx),
                        };
                    }
                }
                Self::resolve_order_by_aliases_in_ir(expr, projection_schema)
            }
            _ => Self::resolve_order_by_aliases_in_ir(expr, projection_schema),
        }
    }

    pub(super) fn resolve_order_by_aliases_in_ir(
        expr: Expr,
        projection_schema: &PlanSchema,
    ) -> Expr {
        match expr {
            Expr::Column { name, table, index } if index.is_none() => {
                let upper_name = name.to_uppercase();
                if let Some(idx) = projection_schema
                    .fields
                    .iter()
                    .position(|f| f.name.to_uppercase() == upper_name)
                {
                    Expr::Column {
                        name,
                        table,
                        index: Some(idx),
                    }
                } else {
                    Expr::Column { name, table, index }
                }
            }
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => Expr::Case {
                operand: operand
                    .map(|e| Box::new(Self::resolve_order_by_aliases_in_ir(*e, projection_schema))),
                when_clauses: when_clauses
                    .into_iter()
                    .map(|w| WhenClause {
                        condition: Self::resolve_order_by_aliases_in_ir(
                            w.condition,
                            projection_schema,
                        ),
                        result: Self::resolve_order_by_aliases_in_ir(w.result, projection_schema),
                    })
                    .collect(),
                else_result: else_result
                    .map(|e| Box::new(Self::resolve_order_by_aliases_in_ir(*e, projection_schema))),
            },
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(Self::resolve_order_by_aliases_in_ir(
                    *left,
                    projection_schema,
                )),
                op,
                right: Box::new(Self::resolve_order_by_aliases_in_ir(
                    *right,
                    projection_schema,
                )),
            },
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op,
                expr: Box::new(Self::resolve_order_by_aliases_in_ir(
                    *expr,
                    projection_schema,
                )),
            },
            Expr::ScalarFunction { name, args } => Expr::ScalarFunction {
                name,
                args: args
                    .into_iter()
                    .map(|a| Self::resolve_order_by_aliases_in_ir(a, projection_schema))
                    .collect(),
            },
            Expr::Cast {
                expr,
                data_type,
                safe,
            } => Expr::Cast {
                expr: Box::new(Self::resolve_order_by_aliases_in_ir(
                    *expr,
                    projection_schema,
                )),
                data_type,
                safe,
            },
            other => other,
        }
    }
}
