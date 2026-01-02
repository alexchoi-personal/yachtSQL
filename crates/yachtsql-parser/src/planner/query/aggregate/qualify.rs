#![coverage(off)]

use sqlparser::ast;
use yachtsql_common::error::Result;
use yachtsql_ir::{Expr, PlanSchema};

use super::super::Planner;
use crate::CatalogProvider;
use crate::expr_planner::ExprPlanner;

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub(in crate::planner::query::aggregate) fn plan_qualify_expr_with_agg_schema(
        &self,
        expr: &ast::Expr,
        agg_schema: &PlanSchema,
    ) -> Result<Expr> {
        match expr {
            ast::Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                if func.over.is_some() {
                    let planned = ExprPlanner::plan_expr(expr, agg_schema)?;
                    self.replace_aggs_in_window_with_columns(&planned, agg_schema)
                } else if Self::is_aggregate_function_name(&name) {
                    let canonical = Self::canonical_agg_name(expr);
                    if let Some(idx) = agg_schema.field_index(&canonical) {
                        Ok(Expr::Column {
                            table: None,
                            name: canonical,
                            index: Some(idx),
                        })
                    } else {
                        for (idx, field) in agg_schema.fields.iter().enumerate() {
                            if Self::canonical_agg_name_matches(&field.name, &canonical) {
                                return Ok(Expr::Column {
                                    table: None,
                                    name: field.name.clone(),
                                    index: Some(idx),
                                });
                            }
                        }
                        ExprPlanner::plan_expr(expr, agg_schema)
                    }
                } else {
                    ExprPlanner::plan_expr(expr, agg_schema)
                }
            }
            ast::Expr::BinaryOp { left, op, right } => {
                let left_expr = self.plan_qualify_expr_with_agg_schema(left, agg_schema)?;
                let right_expr = self.plan_qualify_expr_with_agg_schema(right, agg_schema)?;
                Ok(Expr::BinaryOp {
                    left: Box::new(left_expr),
                    op: ExprPlanner::plan_binary_op(op)?,
                    right: Box::new(right_expr),
                })
            }
            ast::Expr::UnaryOp { op, expr: inner } => {
                let inner_expr = self.plan_qualify_expr_with_agg_schema(inner, agg_schema)?;
                Ok(Expr::UnaryOp {
                    op: ExprPlanner::plan_unary_op(op)?,
                    expr: Box::new(inner_expr),
                })
            }
            ast::Expr::Nested(inner) => self.plan_qualify_expr_with_agg_schema(inner, agg_schema),
            _ => ExprPlanner::plan_expr(expr, agg_schema),
        }
    }

    fn replace_aggs_in_window_with_columns(
        &self,
        expr: &Expr,
        schema: &PlanSchema,
    ) -> Result<Expr> {
        match expr {
            Expr::Window {
                func,
                args,
                partition_by,
                order_by,
                frame,
            } => {
                let new_args: Vec<Expr> = args
                    .iter()
                    .map(|a| Self::replace_agg_with_column(a, schema))
                    .collect::<Result<Vec<_>>>()?;
                let new_partition_by: Vec<Expr> = partition_by
                    .iter()
                    .map(|e| Self::replace_agg_with_column(e, schema))
                    .collect::<Result<Vec<_>>>()?;
                let new_order_by: Vec<yachtsql_ir::SortExpr> = order_by
                    .iter()
                    .map(|se| {
                        Ok(yachtsql_ir::SortExpr {
                            expr: Self::replace_agg_with_column(&se.expr, schema)?,
                            asc: se.asc,
                            nulls_first: se.nulls_first,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::Window {
                    func: *func,
                    args: new_args,
                    partition_by: new_partition_by,
                    order_by: new_order_by,
                    frame: frame.clone(),
                })
            }
            Expr::AggregateWindow {
                func,
                args,
                distinct,
                partition_by,
                order_by,
                frame,
            } => {
                let new_args: Vec<Expr> = args
                    .iter()
                    .map(|a| Self::replace_agg_with_column(a, schema))
                    .collect::<Result<Vec<_>>>()?;
                let new_partition_by: Vec<Expr> = partition_by
                    .iter()
                    .map(|e| Self::replace_agg_with_column(e, schema))
                    .collect::<Result<Vec<_>>>()?;
                let new_order_by: Vec<yachtsql_ir::SortExpr> = order_by
                    .iter()
                    .map(|se| {
                        Ok(yachtsql_ir::SortExpr {
                            expr: Self::replace_agg_with_column(&se.expr, schema)?,
                            asc: se.asc,
                            nulls_first: se.nulls_first,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::AggregateWindow {
                    func: *func,
                    args: new_args,
                    distinct: *distinct,
                    partition_by: new_partition_by,
                    order_by: new_order_by,
                    frame: frame.clone(),
                })
            }
            _ => Ok(expr.clone()),
        }
    }

    fn replace_agg_with_column(expr: &Expr, schema: &PlanSchema) -> Result<Expr> {
        match expr {
            Expr::Aggregate { .. } => {
                let canonical = Self::canonical_planned_agg_name(expr);
                for (idx, field) in schema.fields.iter().enumerate() {
                    if Self::canonical_agg_name_matches(&field.name, &canonical) {
                        return Ok(Expr::Column {
                            table: None,
                            name: field.name.clone(),
                            index: Some(idx),
                        });
                    }
                }
                Ok(expr.clone())
            }
            Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
                left: Box::new(Self::replace_agg_with_column(left, schema)?),
                op: *op,
                right: Box::new(Self::replace_agg_with_column(right, schema)?),
            }),
            Expr::UnaryOp { op, expr: inner } => Ok(Expr::UnaryOp {
                op: *op,
                expr: Box::new(Self::replace_agg_with_column(inner, schema)?),
            }),
            _ => Ok(expr.clone()),
        }
    }
}
