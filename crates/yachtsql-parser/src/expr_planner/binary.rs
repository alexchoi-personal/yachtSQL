#![coverage(off)]

use sqlparser::ast;
use yachtsql_common::error::Result;
use yachtsql_ir::{Expr, PlanSchema};

use super::literals::plan_binary_op;
use super::{ExprPlanner, SubqueryPlannerFn, UdfResolverFn};

pub fn plan_binary_expr(
    left: &ast::Expr,
    op: &ast::BinaryOperator,
    right: &ast::Expr,
    schema: &PlanSchema,
    subquery_planner: Option<SubqueryPlannerFn>,
    named_windows: &[ast::NamedWindowDefinition],
    udf_resolver: Option<UdfResolverFn>,
) -> Result<Expr> {
    let left =
        ExprPlanner::plan_expr_full(left, schema, subquery_planner, named_windows, udf_resolver)?;
    let right =
        ExprPlanner::plan_expr_full(right, schema, subquery_planner, named_windows, udf_resolver)?;
    let op = plan_binary_op(op)?;
    Ok(Expr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}
