#![coverage(off)]

use sqlparser::ast;
use yachtsql_common::error::Result;
use yachtsql_ir::{Expr, PlanSchema, WhenClause};

use super::{ExprPlanner, SubqueryPlannerFn, UdfResolverFn};

pub fn plan_case_expr(
    operand: Option<&ast::Expr>,
    conditions: &[ast::CaseWhen],
    else_result: Option<&ast::Expr>,
    schema: &PlanSchema,
    subquery_planner: Option<SubqueryPlannerFn>,
    named_windows: &[ast::NamedWindowDefinition],
    udf_resolver: Option<UdfResolverFn>,
) -> Result<Expr> {
    let operand = operand
        .map(|e| {
            ExprPlanner::plan_expr_full(e, schema, subquery_planner, named_windows, udf_resolver)
        })
        .transpose()?
        .map(Box::new);

    let mut when_clauses = Vec::new();
    for cw in conditions {
        let condition = ExprPlanner::plan_expr_full(
            &cw.condition,
            schema,
            subquery_planner,
            named_windows,
            udf_resolver,
        )?;
        let result = ExprPlanner::plan_expr_full(
            &cw.result,
            schema,
            subquery_planner,
            named_windows,
            udf_resolver,
        )?;
        when_clauses.push(WhenClause { condition, result });
    }

    let else_result = else_result
        .map(|e| {
            ExprPlanner::plan_expr_full(e, schema, subquery_planner, named_windows, udf_resolver)
        })
        .transpose()?
        .map(Box::new);

    Ok(Expr::Case {
        operand,
        when_clauses,
        else_result,
    })
}
