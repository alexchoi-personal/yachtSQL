#![coverage(off)]

use sqlparser::ast;
use yachtsql_common::error::Result;
use yachtsql_ir::{Expr, PlanSchema};

use super::{ExprPlanner, SubqueryPlannerFn, UdfResolverFn};

pub fn plan_struct(
    values: &[ast::Expr],
    schema: &PlanSchema,
    subquery_planner: Option<SubqueryPlannerFn>,
    named_windows: &[ast::NamedWindowDefinition],
    udf_resolver: Option<UdfResolverFn>,
) -> Result<Expr> {
    let mut fields = Vec::new();
    for value in values {
        match value {
            ast::Expr::Named { expr, name } => {
                let ir_expr = ExprPlanner::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                fields.push((Some(name.value.clone()), ir_expr));
            }
            other => {
                let ir_expr = ExprPlanner::plan_expr_full(
                    other,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                fields.push((None, ir_expr));
            }
        }
    }
    Ok(Expr::Struct { fields })
}

pub fn plan_tuple(
    exprs: &[ast::Expr],
    schema: &PlanSchema,
    subquery_planner: Option<SubqueryPlannerFn>,
    named_windows: &[ast::NamedWindowDefinition],
    udf_resolver: Option<UdfResolverFn>,
) -> Result<Expr> {
    let fields: Vec<(Option<String>, Expr)> = exprs
        .iter()
        .map(|e| {
            let expr = ExprPlanner::plan_expr_full(
                e,
                schema,
                subquery_planner,
                named_windows,
                udf_resolver,
            )?;
            Ok((None, expr))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Expr::Struct { fields })
}
