#![coverage(off)]

use sqlparser::ast;
use yachtsql_common::error::Result;
use yachtsql_ir::{Expr, PlanField, PlanSchema};

use super::super::Planner;
use crate::CatalogProvider;
use crate::expr_planner::ExprPlanner;

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub(in crate::planner::query::aggregate) fn collect_having_aggregates(
        expr: &ast::Expr,
        input_schema: &PlanSchema,
        agg_names: &mut Vec<String>,
        agg_exprs: &mut Vec<Expr>,
        fields: &mut Vec<PlanField>,
    ) -> Result<()> {
        match expr {
            ast::Expr::Function(func)
                if Self::is_aggregate_function_name(&func.name.to_string()) =>
            {
                let canonical = Self::canonical_agg_name(expr);
                if !agg_names.contains(&canonical) {
                    let planned = ExprPlanner::plan_expr(expr, input_schema)?;
                    let data_type = Self::compute_expr_type(&planned, input_schema);
                    fields.push(PlanField::new(canonical.clone(), data_type));
                    agg_exprs.push(planned);
                    agg_names.push(canonical);
                }
            }
            ast::Expr::BinaryOp { left, right, .. } => {
                Self::collect_having_aggregates(left, input_schema, agg_names, agg_exprs, fields)?;
                Self::collect_having_aggregates(right, input_schema, agg_names, agg_exprs, fields)?;
            }
            ast::Expr::UnaryOp { expr: inner, .. } => {
                Self::collect_having_aggregates(inner, input_schema, agg_names, agg_exprs, fields)?;
            }
            ast::Expr::Nested(inner) => {
                Self::collect_having_aggregates(inner, input_schema, agg_names, agg_exprs, fields)?;
            }
            _ => {}
        }
        Ok(())
    }

    pub(in crate::planner::query::aggregate) fn collect_order_by_aggregates(
        order_by: &ast::OrderBy,
        input_schema: &PlanSchema,
        agg_names: &mut Vec<String>,
        agg_exprs: &mut Vec<Expr>,
        fields: &mut Vec<PlanField>,
    ) -> Result<()> {
        match &order_by.kind {
            ast::OrderByKind::Expressions(exprs) => {
                for order_expr in exprs {
                    Self::collect_expr_aggregates(
                        &order_expr.expr,
                        input_schema,
                        agg_names,
                        agg_exprs,
                        fields,
                    )?;
                }
            }
            ast::OrderByKind::All(_) => {}
        }
        Ok(())
    }

    fn collect_expr_aggregates(
        expr: &ast::Expr,
        input_schema: &PlanSchema,
        agg_names: &mut Vec<String>,
        agg_exprs: &mut Vec<Expr>,
        fields: &mut Vec<PlanField>,
    ) -> Result<()> {
        match expr {
            ast::Expr::Function(func)
                if Self::is_aggregate_function_name(&func.name.to_string()) =>
            {
                let planned = ExprPlanner::plan_expr(expr, input_schema)?;
                let canonical = Self::canonical_planned_agg_name(&planned);
                if !agg_names.contains(&canonical) {
                    let data_type = Self::compute_expr_type(&planned, input_schema);
                    fields.push(PlanField::new(canonical.clone(), data_type));
                    agg_exprs.push(planned);
                    agg_names.push(canonical);
                }
            }
            ast::Expr::BinaryOp { left, right, .. } => {
                Self::collect_expr_aggregates(left, input_schema, agg_names, agg_exprs, fields)?;
                Self::collect_expr_aggregates(right, input_schema, agg_names, agg_exprs, fields)?;
            }
            ast::Expr::UnaryOp { expr: inner, .. } => {
                Self::collect_expr_aggregates(inner, input_schema, agg_names, agg_exprs, fields)?;
            }
            ast::Expr::Nested(inner) => {
                Self::collect_expr_aggregates(inner, input_schema, agg_names, agg_exprs, fields)?;
            }
            ast::Expr::Cast { expr: inner, .. } => {
                Self::collect_expr_aggregates(inner, input_schema, agg_names, agg_exprs, fields)?;
            }
            _ => {}
        }
        Ok(())
    }

    pub(in crate::planner::query::aggregate) fn is_aggregate_function_name(name: &str) -> bool {
        let name_upper = name.to_uppercase();
        matches!(
            name_upper.as_str(),
            "COUNT"
                | "SUM"
                | "AVG"
                | "MIN"
                | "MAX"
                | "ARRAY_AGG"
                | "STRING_AGG"
                | "LISTAGG"
                | "XMLAGG"
                | "ANY_VALUE"
                | "COUNTIF"
                | "COUNT_IF"
                | "SUMIF"
                | "SUM_IF"
                | "AVGIF"
                | "AVG_IF"
                | "MINIF"
                | "MIN_IF"
                | "MAXIF"
                | "MAX_IF"
                | "BIT_AND"
                | "BIT_OR"
                | "BIT_XOR"
                | "LOGICAL_AND"
                | "LOGICAL_OR"
                | "STDDEV"
                | "STDDEV_POP"
                | "STDDEV_SAMP"
                | "VARIANCE"
                | "VAR_POP"
                | "VAR_SAMP"
                | "CORR"
                | "COVAR_POP"
                | "COVAR_SAMP"
                | "APPROX_COUNT_DISTINCT"
                | "APPROX_QUANTILES"
                | "APPROX_TOP_COUNT"
                | "APPROX_TOP_SUM"
                | "GROUPING"
                | "GROUPING_ID"
        )
    }

    #[allow(clippy::only_used_in_recursion)]
    pub(in crate::planner::query::aggregate) fn plan_having_expr(
        &self,
        expr: &ast::Expr,
        agg_schema: &PlanSchema,
    ) -> Result<Expr> {
        match expr {
            ast::Expr::Function(func)
                if Self::is_aggregate_function_name(&func.name.to_string()) =>
            {
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
            }
            ast::Expr::BinaryOp { left, op, right } => {
                let left_expr = self.plan_having_expr(left, agg_schema)?;
                let right_expr = self.plan_having_expr(right, agg_schema)?;
                Ok(Expr::BinaryOp {
                    left: Box::new(left_expr),
                    op: ExprPlanner::plan_binary_op(op)?,
                    right: Box::new(right_expr),
                })
            }
            ast::Expr::UnaryOp { op, expr: inner } => {
                let inner_expr = self.plan_having_expr(inner, agg_schema)?;
                Ok(Expr::UnaryOp {
                    op: ExprPlanner::plan_unary_op(op)?,
                    expr: Box::new(inner_expr),
                })
            }
            ast::Expr::Nested(inner) => self.plan_having_expr(inner, agg_schema),
            _ => ExprPlanner::plan_expr(expr, agg_schema),
        }
    }
}
