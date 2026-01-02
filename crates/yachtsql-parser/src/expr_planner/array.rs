#![coverage(off)]

use sqlparser::ast;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::DataType;
use yachtsql_ir::{Expr, Literal, PlanSchema};

use super::types::plan_data_type;
use super::{ExprPlanner, SubqueryPlannerFn, UdfResolverFn};

pub fn plan_array(
    arr: &ast::Array,
    schema: &PlanSchema,
    subquery_planner: Option<SubqueryPlannerFn>,
    named_windows: &[ast::NamedWindowDefinition],
    udf_resolver: Option<UdfResolverFn>,
) -> Result<Expr> {
    let element_type = arr
        .element_type
        .as_ref()
        .map(|dt| plan_data_type(dt))
        .transpose()?;

    let struct_field_names: Option<Vec<String>> = element_type.as_ref().and_then(|t| {
        if let DataType::Struct(fields) = t {
            Some(fields.iter().map(|f| f.name.clone()).collect())
        } else {
            None
        }
    });

    let elements = arr
        .elem
        .iter()
        .map(|e| {
            let mut expr = ExprPlanner::plan_expr_full(
                e,
                schema,
                subquery_planner,
                named_windows,
                udf_resolver,
            )?;
            if let (Some(names), Expr::Struct { fields }) = (&struct_field_names, &expr) {
                let new_fields: Vec<(Option<String>, Expr)> = fields
                    .iter()
                    .enumerate()
                    .map(|(i, (old_name, field_expr))| {
                        let name = if old_name.is_some() {
                            old_name.clone()
                        } else if i < names.len() {
                            Some(names[i].clone())
                        } else {
                            None
                        };
                        (name, field_expr.clone())
                    })
                    .collect();
                expr = Expr::Struct { fields: new_fields };
            }
            Ok(expr)
        })
        .collect::<Result<Vec<_>>>()?;

    let all_literals = elements.iter().all(|e| matches!(e, Expr::Literal(_)));
    if all_literals {
        let literals: Vec<Literal> = elements
            .into_iter()
            .filter_map(|e| match e {
                Expr::Literal(lit) => Some(lit),
                Expr::Struct { fields } => {
                    let struct_fields: Vec<(String, Literal)> = fields
                        .into_iter()
                        .enumerate()
                        .filter_map(|(i, (name, expr))| {
                            if let Expr::Literal(lit) = expr {
                                let field_name = name.unwrap_or_else(|| format!("_field{}", i));
                                Some((field_name, lit))
                            } else {
                                None
                            }
                        })
                        .collect();
                    Some(Literal::Struct(struct_fields))
                }
                _ => None,
            })
            .collect();
        Ok(Expr::Literal(Literal::Array(literals)))
    } else {
        Ok(Expr::Array {
            elements,
            element_type,
        })
    }
}

pub fn plan_in_unnest(
    expr: &ast::Expr,
    array_expr: &ast::Expr,
    negated: bool,
    schema: &PlanSchema,
    subquery_planner: Option<SubqueryPlannerFn>,
    named_windows: &[ast::NamedWindowDefinition],
    udf_resolver: Option<UdfResolverFn>,
) -> Result<Expr> {
    let expr =
        ExprPlanner::plan_expr_full(expr, schema, subquery_planner, named_windows, udf_resolver)?;
    let array_expr = ExprPlanner::plan_expr_full(
        array_expr,
        schema,
        subquery_planner,
        named_windows,
        udf_resolver,
    )?;
    Ok(Expr::InUnnest {
        expr: Box::new(expr),
        array_expr: Box::new(array_expr),
        negated,
    })
}

pub fn plan_compound_field_access(
    root: &ast::Expr,
    access_chain: &[ast::AccessExpr],
    schema: &PlanSchema,
    subquery_planner: Option<SubqueryPlannerFn>,
    named_windows: &[ast::NamedWindowDefinition],
    udf_resolver: Option<UdfResolverFn>,
) -> Result<Expr> {
    let mut result =
        ExprPlanner::plan_expr_full(root, schema, subquery_planner, named_windows, udf_resolver)?;
    for accessor in access_chain {
        match accessor {
            ast::AccessExpr::Subscript(sub) => match sub {
                ast::Subscript::Index { index } => {
                    let index_expr = ExprPlanner::plan_expr_full(
                        index,
                        schema,
                        subquery_planner,
                        named_windows,
                        udf_resolver,
                    )?;
                    result = Expr::ArrayAccess {
                        array: Box::new(result),
                        index: Box::new(index_expr),
                    };
                }
                ast::Subscript::Slice { .. } => {
                    return Err(Error::unsupported("Array slice not yet supported"));
                }
            },
            ast::AccessExpr::Dot(ident) => {
                let field_name = match ident {
                    ast::Expr::Identifier(id) => id.value.clone(),
                    _ => {
                        return Err(Error::unsupported(format!(
                            "Unsupported field accessor: {:?}",
                            ident
                        )));
                    }
                };
                result = Expr::StructAccess {
                    expr: Box::new(result),
                    field: field_name,
                };
            }
        }
    }
    Ok(result)
}
