#![coverage(off)]

use sqlparser::ast;
use yachtsql_common::error::{Error, Result};
use yachtsql_ir::plan::FunctionBody;
use yachtsql_ir::{Expr, Literal, PlanSchema, ScalarFunction, SortExpr};

use super::aggregates::try_aggregate_function;
use super::scalars::try_scalar_function;
use super::subquery::plan_array_subquery;
use super::substitution::{apply_struct_field_names, substitute_parameters};
use super::window::{plan_window_spec, try_window_function};
use super::{ExprPlanner, SubqueryPlannerFn, UdfResolverFn};

pub fn plan_function(
    func: &ast::Function,
    schema: &PlanSchema,
    subquery_planner: Option<SubqueryPlannerFn>,
    named_windows: &[ast::NamedWindowDefinition],
    udf_resolver: Option<UdfResolverFn>,
) -> Result<Expr> {
    let name = func.name.to_string().to_uppercase();

    if name == "ARRAY"
        && let ast::FunctionArguments::Subquery(subquery) = &func.args
    {
        return plan_array_subquery(subquery, subquery_planner);
    }

    if let Some(over) = &func.over {
        if let Some(window_func) = try_window_function(&name) {
            let args = extract_function_args(func, schema)?;
            let (partition_by, order_by, frame) = plan_window_spec(over, schema, named_windows)?;
            return Ok(Expr::Window {
                func: window_func,
                args,
                partition_by,
                order_by,
                frame,
            });
        }
        if let Some(agg_func) = try_aggregate_function(&name) {
            let distinct = matches!(
                &func.args,
                ast::FunctionArguments::List(list) if list.duplicate_treatment == Some(ast::DuplicateTreatment::Distinct)
            );
            let args = extract_function_args(func, schema)?;
            let (partition_by, order_by, frame) = plan_window_spec(over, schema, named_windows)?;
            return Ok(Expr::AggregateWindow {
                func: agg_func,
                args,
                distinct,
                partition_by,
                order_by,
                frame,
            });
        }
    }

    if let Some(agg_func) = try_aggregate_function(&name) {
        let (distinct, order_by, limit, ignore_nulls) = match &func.args {
            ast::FunctionArguments::List(list) => {
                let distinct = list.duplicate_treatment == Some(ast::DuplicateTreatment::Distinct);
                let mut order_by_exprs = Vec::new();
                let mut limit = None;
                let mut ignore_nulls = false;
                for clause in &list.clauses {
                    match clause {
                        ast::FunctionArgumentClause::OrderBy(order_list) => {
                            for ob in order_list {
                                let expr = ExprPlanner::plan_expr(&ob.expr, schema)?;
                                let asc = ob.options.asc.is_none_or(|asc| asc);
                                order_by_exprs.push(SortExpr {
                                    expr,
                                    asc,
                                    nulls_first: ob.options.nulls_first.unwrap_or(!asc),
                                });
                            }
                        }
                        ast::FunctionArgumentClause::Limit(ast::Expr::Value(
                            ast::ValueWithSpan {
                                value: ast::Value::Number(n, _),
                                ..
                            },
                        )) => {
                            limit = n.parse::<usize>().ok();
                        }
                        ast::FunctionArgumentClause::Limit(_) => {}
                        ast::FunctionArgumentClause::IgnoreOrRespectNulls(treatment) => {
                            ignore_nulls = matches!(treatment, ast::NullTreatment::IgnoreNulls);
                        }
                        _ => {}
                    }
                }
                (distinct, order_by_exprs, limit, ignore_nulls)
            }
            _ => (false, Vec::new(), None, false),
        };
        let args = extract_function_args(func, schema)?;
        return Ok(Expr::Aggregate {
            func: agg_func,
            args,
            distinct,
            filter: None,
            order_by,
            limit,
            ignore_nulls,
        });
    }

    if name == "MAKE_INTERVAL" {
        let args = extract_make_interval_args(func, schema)?;
        return Ok(Expr::ScalarFunction {
            name: ScalarFunction::MakeInterval,
            args,
        });
    }

    if matches!(
        name.as_str(),
        "TIMESTAMP_TRUNC" | "DATETIME_TRUNC" | "DATE_TRUNC" | "TIME_TRUNC"
    ) {
        let args = extract_trunc_args(func, schema)?;
        let scalar_func = try_scalar_function(&name)?;
        return Ok(Expr::ScalarFunction {
            name: scalar_func,
            args,
        });
    }

    if name == "NORMALIZE" {
        let args = extract_normalize_args(func, schema)?;
        return Ok(Expr::ScalarFunction {
            name: ScalarFunction::Normalize,
            args,
        });
    }

    if let Some(resolver) = udf_resolver
        && let Some(udf) = resolver(&name)
    {
        if udf.is_aggregate {
            let distinct = matches!(
                &func.args,
                ast::FunctionArguments::List(list) if list.duplicate_treatment == Some(ast::DuplicateTreatment::Distinct)
            );
            let args = extract_function_args_full(
                func,
                schema,
                subquery_planner,
                named_windows,
                udf_resolver,
            )?;
            return Ok(Expr::UserDefinedAggregate {
                name: name.to_lowercase(),
                args,
                distinct,
                filter: None,
            });
        }
        if matches!(&udf.body, FunctionBody::Sql(_)) {
            let call_args = extract_function_args_full(
                func,
                schema,
                subquery_planner,
                named_windows,
                udf_resolver,
            )?;
            if let FunctionBody::Sql(body_expr) = &udf.body {
                let substituted = substitute_parameters(body_expr, &udf.parameters, &call_args);
                let result = apply_struct_field_names(substituted, &udf.return_type);
                return Ok(result);
            }
        }
    }

    let args = extract_function_args(func, schema)?;
    let scalar_func = try_scalar_function(&name)?;
    Ok(Expr::ScalarFunction {
        name: scalar_func,
        args,
    })
}

pub fn extract_function_args_full(
    func: &ast::Function,
    schema: &PlanSchema,
    subquery_planner: Option<SubqueryPlannerFn>,
    named_windows: &[ast::NamedWindowDefinition],
    udf_resolver: Option<UdfResolverFn>,
) -> Result<Vec<Expr>> {
    match &func.args {
        ast::FunctionArguments::None => Ok(vec![]),
        ast::FunctionArguments::Subquery(_) => {
            Err(Error::unsupported("Subquery function arguments"))
        }
        ast::FunctionArguments::List(list) => {
            let mut args = Vec::new();
            for arg in &list.args {
                match arg {
                    ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                        ast::FunctionArgExpr::Expr(e) => {
                            args.push(ExprPlanner::plan_expr_full(
                                e,
                                schema,
                                subquery_planner,
                                named_windows,
                                udf_resolver,
                            )?);
                        }
                        ast::FunctionArgExpr::Wildcard => {
                            args.push(Expr::Wildcard { table: None });
                        }
                        ast::FunctionArgExpr::QualifiedWildcard(name) => {
                            args.push(Expr::Wildcard {
                                table: Some(name.to_string()),
                            });
                        }
                        ast::FunctionArgExpr::TableRef(_) => {
                            return Err(Error::unsupported("TABLE argument in scalar function"));
                        }
                    },
                    ast::FunctionArg::Named { arg, .. } => {
                        if let ast::FunctionArgExpr::Expr(e) = arg {
                            args.push(ExprPlanner::plan_expr_full(
                                e,
                                schema,
                                subquery_planner,
                                named_windows,
                                udf_resolver,
                            )?);
                        }
                    }
                    ast::FunctionArg::ExprNamed { arg, .. } => {
                        if let ast::FunctionArgExpr::Expr(e) = arg {
                            args.push(ExprPlanner::plan_expr_full(
                                e,
                                schema,
                                subquery_planner,
                                named_windows,
                                udf_resolver,
                            )?);
                        }
                    }
                }
            }
            Ok(args)
        }
    }
}

pub fn extract_function_args(func: &ast::Function, schema: &PlanSchema) -> Result<Vec<Expr>> {
    match &func.args {
        ast::FunctionArguments::None => Ok(vec![]),
        ast::FunctionArguments::Subquery(_) => {
            Err(Error::unsupported("Subquery function arguments"))
        }
        ast::FunctionArguments::List(list) => {
            let mut args = Vec::new();
            for arg in &list.args {
                match arg {
                    ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                        ast::FunctionArgExpr::Expr(e) => {
                            args.push(ExprPlanner::plan_expr(e, schema)?);
                        }
                        ast::FunctionArgExpr::Wildcard => {
                            args.push(Expr::Wildcard { table: None });
                        }
                        ast::FunctionArgExpr::QualifiedWildcard(name) => {
                            args.push(Expr::Wildcard {
                                table: Some(name.to_string()),
                            });
                        }
                        ast::FunctionArgExpr::TableRef(_) => {
                            return Err(Error::unsupported("TABLE argument in scalar function"));
                        }
                    },
                    ast::FunctionArg::Named { arg, .. } => {
                        if let ast::FunctionArgExpr::Expr(e) = arg {
                            args.push(ExprPlanner::plan_expr(e, schema)?);
                        }
                    }
                    ast::FunctionArg::ExprNamed { arg, .. } => {
                        if let ast::FunctionArgExpr::Expr(e) = arg {
                            args.push(ExprPlanner::plan_expr(e, schema)?);
                        }
                    }
                }
            }
            Ok(args)
        }
    }
}

pub fn extract_trunc_args(func: &ast::Function, schema: &PlanSchema) -> Result<Vec<Expr>> {
    match &func.args {
        ast::FunctionArguments::None => Ok(vec![]),
        ast::FunctionArguments::Subquery(_) => {
            Err(Error::unsupported("Subquery function arguments"))
        }
        ast::FunctionArguments::List(list) => {
            let mut args = Vec::new();
            for (i, arg) in list.args.iter().enumerate() {
                match arg {
                    ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                        ast::FunctionArgExpr::Expr(e) => {
                            if i == 1 {
                                let part_str = extract_date_part_string(e)?;
                                args.push(Expr::Literal(Literal::String(part_str)));
                            } else {
                                args.push(ExprPlanner::plan_expr(e, schema)?);
                            }
                        }
                        ast::FunctionArgExpr::Wildcard => {
                            args.push(Expr::Wildcard { table: None });
                        }
                        ast::FunctionArgExpr::QualifiedWildcard(name) => {
                            args.push(Expr::Wildcard {
                                table: Some(name.to_string()),
                            });
                        }
                        ast::FunctionArgExpr::TableRef(_) => {
                            return Err(Error::unsupported("TABLE argument in scalar function"));
                        }
                    },
                    ast::FunctionArg::Named { arg, .. } => {
                        if let ast::FunctionArgExpr::Expr(e) = arg {
                            args.push(ExprPlanner::plan_expr(e, schema)?);
                        }
                    }
                    ast::FunctionArg::ExprNamed { arg, .. } => {
                        if let ast::FunctionArgExpr::Expr(e) = arg {
                            args.push(ExprPlanner::plan_expr(e, schema)?);
                        }
                    }
                }
            }
            Ok(args)
        }
    }
}

pub fn extract_normalize_args(func: &ast::Function, schema: &PlanSchema) -> Result<Vec<Expr>> {
    match &func.args {
        ast::FunctionArguments::None => Ok(vec![]),
        ast::FunctionArguments::Subquery(_) => {
            Err(Error::unsupported("Subquery function arguments"))
        }
        ast::FunctionArguments::List(list) => {
            let mut args = Vec::new();
            for (i, arg) in list.args.iter().enumerate() {
                match arg {
                    ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                        ast::FunctionArgExpr::Expr(e) => {
                            if i == 1 {
                                let mode = extract_normalize_mode(e)?;
                                args.push(Expr::Literal(Literal::String(mode)));
                            } else {
                                args.push(ExprPlanner::plan_expr(e, schema)?);
                            }
                        }
                        ast::FunctionArgExpr::Wildcard => {
                            args.push(Expr::Wildcard { table: None });
                        }
                        ast::FunctionArgExpr::QualifiedWildcard(name) => {
                            args.push(Expr::Wildcard {
                                table: Some(name.to_string()),
                            });
                        }
                        ast::FunctionArgExpr::TableRef(_) => {
                            return Err(Error::unsupported("TABLE argument in scalar function"));
                        }
                    },
                    ast::FunctionArg::Named { arg, .. } => {
                        if let ast::FunctionArgExpr::Expr(e) = arg {
                            args.push(ExprPlanner::plan_expr(e, schema)?);
                        }
                    }
                    ast::FunctionArg::ExprNamed { arg, .. } => {
                        if let ast::FunctionArgExpr::Expr(e) = arg {
                            args.push(ExprPlanner::plan_expr(e, schema)?);
                        }
                    }
                }
            }
            Ok(args)
        }
    }
}

fn extract_normalize_mode(e: &ast::Expr) -> Result<String> {
    match e {
        ast::Expr::Identifier(ident) => {
            let mode = ident.value.to_uppercase();
            match mode.as_str() {
                "NFC" | "NFKC" | "NFD" | "NFKD" => Ok(mode),
                _ => Err(Error::invalid_query(format!(
                    "Invalid normalization mode: {}. Expected NFC, NFKC, NFD, or NFKD",
                    mode
                ))),
            }
        }
        _ => Err(Error::invalid_query(format!(
            "Normalization mode must be an identifier (NFC, NFKC, NFD, or NFKD), got: {}",
            e
        ))),
    }
}

pub fn extract_date_part_string(e: &ast::Expr) -> Result<String> {
    match e {
        ast::Expr::Identifier(ident) => Ok(ident.value.to_uppercase()),
        ast::Expr::Function(func) => {
            let name = func.name.to_string().to_uppercase();
            if let ast::FunctionArguments::List(list) = &func.args
                && let Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                    ast::Expr::Identifier(day_ident),
                ))) = list.args.first()
            {
                return Ok(format!("{}_{}", name, day_ident.value.to_uppercase()));
            }
            Ok(name)
        }
        _ => Ok(e.to_string().to_uppercase()),
    }
}

pub fn extract_make_interval_args(func: &ast::Function, schema: &PlanSchema) -> Result<Vec<Expr>> {
    let mut years: Option<Expr> = None;
    let mut months: Option<Expr> = None;
    let mut days: Option<Expr> = None;
    let mut hours: Option<Expr> = None;
    let mut minutes: Option<Expr> = None;
    let mut seconds: Option<Expr> = None;

    match &func.args {
        ast::FunctionArguments::List(list) => {
            for (i, arg) in list.args.iter().enumerate() {
                match arg {
                    ast::FunctionArg::Named { name, arg, .. } => {
                        let param_name = name.value.to_uppercase();
                        if let ast::FunctionArgExpr::Expr(e) = arg {
                            let expr = ExprPlanner::plan_expr(e, schema)?;
                            match param_name.as_str() {
                                "YEAR" | "YEARS" => years = Some(expr),
                                "MONTH" | "MONTHS" => months = Some(expr),
                                "DAY" | "DAYS" => days = Some(expr),
                                "HOUR" | "HOURS" => hours = Some(expr),
                                "MINUTE" | "MINUTES" => minutes = Some(expr),
                                "SECOND" | "SECONDS" => seconds = Some(expr),
                                _ => {}
                            }
                        }
                    }
                    ast::FunctionArg::ExprNamed { name, arg, .. } => {
                        let param_name = match name {
                            ast::Expr::Identifier(ident) => ident.value.to_uppercase(),
                            _ => continue,
                        };
                        if let ast::FunctionArgExpr::Expr(e) = arg {
                            let expr = ExprPlanner::plan_expr(e, schema)?;
                            match param_name.as_str() {
                                "YEAR" | "YEARS" => years = Some(expr),
                                "MONTH" | "MONTHS" => months = Some(expr),
                                "DAY" | "DAYS" => days = Some(expr),
                                "HOUR" | "HOURS" => hours = Some(expr),
                                "MINUTE" | "MINUTES" => minutes = Some(expr),
                                "SECOND" | "SECONDS" => seconds = Some(expr),
                                _ => {}
                            }
                        }
                    }
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                        let expr = ExprPlanner::plan_expr(e, schema)?;
                        match i {
                            0 => years = Some(expr),
                            1 => months = Some(expr),
                            2 => days = Some(expr),
                            3 => hours = Some(expr),
                            4 => minutes = Some(expr),
                            5 => seconds = Some(expr),
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
        }
        ast::FunctionArguments::None => {}
        ast::FunctionArguments::Subquery(_) => {
            return Err(Error::unsupported("Subquery function arguments"));
        }
    }

    let zero = Expr::Literal(Literal::Int64(0));
    Ok(vec![
        years.unwrap_or_else(|| zero.clone()),
        months.unwrap_or_else(|| zero.clone()),
        days.unwrap_or_else(|| zero.clone()),
        hours.unwrap_or_else(|| zero.clone()),
        minutes.unwrap_or_else(|| zero.clone()),
        seconds.unwrap_or(zero),
    ])
}
