#![coverage(off)]

use sqlparser::ast;
use yachtsql_common::error::{Error, Result};
use yachtsql_ir::{Expr, PlanSchema};

use super::{ExprPlanner, SubqueryPlannerFn, UdfResolverFn};

pub fn plan_exists(
    subquery: &ast::Query,
    negated: bool,
    subquery_planner: Option<SubqueryPlannerFn>,
) -> Result<Expr> {
    let planner = subquery_planner
        .ok_or_else(|| Error::unsupported("EXISTS subquery requires subquery planner context"))?;
    let plan = planner(subquery)?;
    Ok(Expr::Exists {
        subquery: Box::new(plan),
        negated,
    })
}

pub fn plan_in_subquery(
    expr: &ast::Expr,
    subquery: &ast::Query,
    negated: bool,
    schema: &PlanSchema,
    subquery_planner: Option<SubqueryPlannerFn>,
    named_windows: &[ast::NamedWindowDefinition],
    udf_resolver: Option<UdfResolverFn>,
) -> Result<Expr> {
    let planned_expr =
        ExprPlanner::plan_expr_full(expr, schema, subquery_planner, named_windows, udf_resolver)?;
    let planner = subquery_planner
        .ok_or_else(|| Error::unsupported("IN subquery requires subquery planner context"))?;
    let plan = planner(subquery)?;
    Ok(Expr::InSubquery {
        expr: Box::new(planned_expr),
        subquery: Box::new(plan),
        negated,
    })
}

pub fn plan_scalar_subquery(
    query: &ast::Query,
    subquery_planner: Option<SubqueryPlannerFn>,
) -> Result<Expr> {
    let planner = subquery_planner
        .ok_or_else(|| Error::unsupported("Scalar subquery requires subquery planner context"))?;
    let plan = planner(query)?;
    Ok(Expr::Subquery(Box::new(plan)))
}

pub fn plan_array_subquery(
    subquery: &ast::Query,
    subquery_planner: Option<SubqueryPlannerFn>,
) -> Result<Expr> {
    let planner = subquery_planner
        .ok_or_else(|| Error::unsupported("ARRAY subquery requires subquery planner context"))?;
    let plan = planner(subquery)?;
    Ok(Expr::ArraySubquery(Box::new(plan)))
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::helpers::attached_token::AttachedToken;
    use sqlparser::ast::{self, SetExpr};
    use yachtsql_common::types::DataType;
    use yachtsql_ir::{Expr, LogicalPlan, PlanField, PlanSchema};

    use super::*;

    fn make_simple_query() -> ast::Query {
        ast::Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(ast::Select {
                select_token: AttachedToken::empty(),
                distinct: None,
                top: None,
                top_before_distinct: false,
                projection: vec![ast::SelectItem::Wildcard(
                    ast::WildcardAdditionalOptions::default(),
                )],
                exclude: None,
                into: None,
                from: vec![],
                lateral_views: vec![],
                prewhere: None,
                selection: None,
                group_by: ast::GroupByExpr::Expressions(vec![], vec![]),
                cluster_by: vec![],
                distribute_by: vec![],
                sort_by: vec![],
                having: None,
                named_window: vec![],
                qualify: None,
                window_before_qualify: false,
                value_table_mode: None,
                connect_by: None,
                flavor: ast::SelectFlavor::Standard,
            }))),
            order_by: None,
            limit_clause: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: vec![],
        }
    }

    fn mock_subquery_planner(_query: &ast::Query) -> yachtsql_common::error::Result<LogicalPlan> {
        Ok(LogicalPlan::Empty {
            schema: PlanSchema::new(),
        })
    }

    #[test]
    fn test_plan_exists_with_planner() {
        let query = make_simple_query();
        let result = plan_exists(&query, false, Some(&mock_subquery_planner));
        assert!(result.is_ok());
        match result.unwrap() {
            Expr::Exists { negated, .. } => {
                assert!(!negated);
            }
            _ => panic!("Expected Expr::Exists"),
        }
    }

    #[test]
    fn test_plan_exists_negated_with_planner() {
        let query = make_simple_query();
        let result = plan_exists(&query, true, Some(&mock_subquery_planner));
        assert!(result.is_ok());
        match result.unwrap() {
            Expr::Exists { negated, .. } => {
                assert!(negated);
            }
            _ => panic!("Expected Expr::Exists"),
        }
    }

    #[test]
    fn test_plan_exists_without_planner() {
        let query = make_simple_query();
        let result = plan_exists(&query, false, None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("EXISTS subquery requires subquery planner context")
        );
    }

    #[test]
    fn test_plan_in_subquery_with_planner() {
        let query = make_simple_query();
        let schema = PlanSchema::from_fields(vec![PlanField::new("x", DataType::Int64)]);
        let expr = ast::Expr::Identifier(ast::Ident::new("x"));
        let result = plan_in_subquery(
            &expr,
            &query,
            false,
            &schema,
            Some(&mock_subquery_planner),
            &[],
            None,
        );
        assert!(result.is_ok());
        match result.unwrap() {
            Expr::InSubquery { negated, .. } => {
                assert!(!negated);
            }
            _ => panic!("Expected Expr::InSubquery"),
        }
    }

    #[test]
    fn test_plan_in_subquery_negated_with_planner() {
        let query = make_simple_query();
        let schema = PlanSchema::from_fields(vec![PlanField::new("x", DataType::Int64)]);
        let expr = ast::Expr::Identifier(ast::Ident::new("x"));
        let result = plan_in_subquery(
            &expr,
            &query,
            true,
            &schema,
            Some(&mock_subquery_planner),
            &[],
            None,
        );
        assert!(result.is_ok());
        match result.unwrap() {
            Expr::InSubquery { negated, .. } => {
                assert!(negated);
            }
            _ => panic!("Expected Expr::InSubquery"),
        }
    }

    #[test]
    fn test_plan_in_subquery_without_planner() {
        let query = make_simple_query();
        let schema = PlanSchema::from_fields(vec![PlanField::new("x", DataType::Int64)]);
        let expr = ast::Expr::Identifier(ast::Ident::new("x"));
        let result = plan_in_subquery(&expr, &query, false, &schema, None, &[], None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("IN subquery requires subquery planner context")
        );
    }

    #[test]
    fn test_plan_scalar_subquery_with_planner() {
        let query = make_simple_query();
        let result = plan_scalar_subquery(&query, Some(&mock_subquery_planner));
        assert!(result.is_ok());
        match result.unwrap() {
            Expr::Subquery(_) => {}
            _ => panic!("Expected Expr::Subquery"),
        }
    }

    #[test]
    fn test_plan_scalar_subquery_without_planner() {
        let query = make_simple_query();
        let result = plan_scalar_subquery(&query, None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("Scalar subquery requires subquery planner context")
        );
    }

    #[test]
    fn test_plan_array_subquery_with_planner() {
        let query = make_simple_query();
        let result = plan_array_subquery(&query, Some(&mock_subquery_planner));
        assert!(result.is_ok());
        match result.unwrap() {
            Expr::ArraySubquery(_) => {}
            _ => panic!("Expected Expr::ArraySubquery"),
        }
    }

    #[test]
    fn test_plan_array_subquery_without_planner() {
        let query = make_simple_query();
        let result = plan_array_subquery(&query, None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("ARRAY subquery requires subquery planner context")
        );
    }

    fn failing_subquery_planner(
        _query: &ast::Query,
    ) -> yachtsql_common::error::Result<LogicalPlan> {
        Err(yachtsql_common::error::Error::unsupported(
            "Subquery planning failed",
        ))
    }

    #[test]
    fn test_plan_exists_planner_error() {
        let query = make_simple_query();
        let result = plan_exists(&query, false, Some(&failing_subquery_planner));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Subquery planning failed"));
    }

    #[test]
    fn test_plan_in_subquery_planner_error() {
        let query = make_simple_query();
        let schema = PlanSchema::from_fields(vec![PlanField::new("x", DataType::Int64)]);
        let expr = ast::Expr::Identifier(ast::Ident::new("x"));
        let result = plan_in_subquery(
            &expr,
            &query,
            false,
            &schema,
            Some(&failing_subquery_planner),
            &[],
            None,
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Subquery planning failed"));
    }

    #[test]
    fn test_plan_scalar_subquery_planner_error() {
        let query = make_simple_query();
        let result = plan_scalar_subquery(&query, Some(&failing_subquery_planner));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Subquery planning failed"));
    }

    #[test]
    fn test_plan_array_subquery_planner_error() {
        let query = make_simple_query();
        let result = plan_array_subquery(&query, Some(&failing_subquery_planner));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Subquery planning failed"));
    }

    #[test]
    fn test_plan_in_subquery_expr_planning_error() {
        let query = make_simple_query();
        let schema = PlanSchema::from_fields(vec![PlanField::new("x", DataType::Int64)]);
        let unsupported_expr = ast::Expr::Collate {
            expr: Box::new(ast::Expr::Identifier(ast::Ident::new("x"))),
            collation: ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                "utf8",
            ))]),
        };
        let result = plan_in_subquery(
            &unsupported_expr,
            &query,
            false,
            &schema,
            Some(&mock_subquery_planner),
            &[],
            None,
        );
        assert!(result.is_err());
    }
}
