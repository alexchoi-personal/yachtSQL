#![coverage(off)]

mod aggregates;
mod array;
mod binary;
mod case;
mod columns;
mod context;
mod datetime;
mod functions;
mod interval;
mod literals;
mod scalars;
mod structs;
mod subquery;
mod substitution;
mod types;
mod utils;
mod window;

use array::{plan_array, plan_compound_field_access, plan_in_unnest};
use binary::plan_binary_expr;
use columns::{resolve_column, resolve_compound_identifier};
pub use context::ExprPlanningContext;
use datetime::plan_datetime_field;
use functions::plan_function;
use literals::{plan_binary_op, plan_literal, plan_unary_op};
use sqlparser::ast;
use structs::{plan_struct, plan_tuple};
use subquery::{plan_exists, plan_in_subquery, plan_scalar_subquery};
use types::plan_data_type;
use yachtsql_common::error::{Error, Result};
use yachtsql_ir::{
    BinaryOp, Expr, Literal, LogicalPlan, PlanSchema, ScalarFunction, TrimWhere, UnaryOp,
};

use crate::FunctionDefinition;

pub type SubqueryPlannerFn<'a> = &'a dyn Fn(&ast::Query) -> Result<LogicalPlan>;
pub type UdfResolverFn<'a> = &'a dyn Fn(&str) -> Option<FunctionDefinition>;

pub struct ExprPlanner;

impl ExprPlanner {
    pub fn plan_expr(sql_expr: &ast::Expr, schema: &PlanSchema) -> Result<Expr> {
        Self::plan_expr_with_subquery(sql_expr, schema, None)
    }

    pub fn plan_expr_with_subquery(
        sql_expr: &ast::Expr,
        schema: &PlanSchema,
        subquery_planner: Option<SubqueryPlannerFn>,
    ) -> Result<Expr> {
        Self::plan_expr_full(sql_expr, schema, subquery_planner, &[], None)
    }

    pub fn plan_expr_with_named_windows(
        sql_expr: &ast::Expr,
        schema: &PlanSchema,
        subquery_planner: Option<SubqueryPlannerFn>,
        named_windows: &[ast::NamedWindowDefinition],
    ) -> Result<Expr> {
        Self::plan_expr_full(sql_expr, schema, subquery_planner, named_windows, None)
    }

    pub fn plan_expr_with_udf_resolver(
        sql_expr: &ast::Expr,
        schema: &PlanSchema,
        subquery_planner: Option<SubqueryPlannerFn>,
        named_windows: &[ast::NamedWindowDefinition],
        udf_resolver: Option<UdfResolverFn>,
    ) -> Result<Expr> {
        Self::plan_expr_full(
            sql_expr,
            schema,
            subquery_planner,
            named_windows,
            udf_resolver,
        )
    }

    #[allow(dead_code)]
    pub fn plan_expr_with_context(sql_expr: &ast::Expr, ctx: &ExprPlanningContext) -> Result<Expr> {
        Self::plan_expr_full(
            sql_expr,
            ctx.schema,
            ctx.subquery_planner,
            ctx.named_windows,
            ctx.udf_resolver,
        )
    }

    pub fn plan_binary_op(op: &ast::BinaryOperator) -> Result<BinaryOp> {
        plan_binary_op(op)
    }

    pub fn plan_unary_op(op: &ast::UnaryOperator) -> Result<UnaryOp> {
        plan_unary_op(op)
    }

    pub fn resolve_compound_identifier(parts: &[ast::Ident], schema: &PlanSchema) -> Result<Expr> {
        resolve_compound_identifier(parts, schema)
    }

    pub fn plan_expr_full(
        sql_expr: &ast::Expr,
        schema: &PlanSchema,
        subquery_planner: Option<SubqueryPlannerFn>,
        named_windows: &[ast::NamedWindowDefinition],
        udf_resolver: Option<UdfResolverFn>,
    ) -> Result<Expr> {
        match sql_expr {
            ast::Expr::Identifier(ident) => {
                if ident.value.eq_ignore_ascii_case("DEFAULT") {
                    return Ok(Expr::Default);
                }
                if ident.value.starts_with('@') {
                    return Ok(Expr::Variable {
                        name: ident.value.clone(),
                    });
                }
                resolve_column(&ident.value, None, schema)
            }

            ast::Expr::CompoundIdentifier(parts) => resolve_compound_identifier(parts, schema),

            ast::Expr::Value(val) => {
                if let ast::Value::Placeholder(name) = &val.value {
                    return Ok(Expr::Variable { name: name.clone() });
                }
                Ok(Expr::Literal(plan_literal(&val.value)?))
            }

            ast::Expr::BinaryOp { left, op, right } => plan_binary_expr(
                left,
                op,
                right,
                schema,
                subquery_planner,
                named_windows,
                udf_resolver,
            ),

            ast::Expr::UnaryOp { op, expr } => {
                if matches!(
                    (op, expr.as_ref()),
                    (
                        ast::UnaryOperator::Minus,
                        ast::Expr::Value(ast::ValueWithSpan {
                            value: ast::Value::Number(n, _),
                            ..
                        })
                    ) if n == "9223372036854775808"
                ) {
                    return Ok(Expr::Literal(Literal::Int64(i64::MIN)));
                }
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let op = plan_unary_op(op)?;
                Ok(Expr::UnaryOp {
                    op,
                    expr: Box::new(expr),
                })
            }

            ast::Expr::Function(func) => {
                plan_function(func, schema, subquery_planner, named_windows, udf_resolver)
            }

            ast::Expr::IsNull(inner) => {
                let expr = Self::plan_expr_full(
                    inner,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::IsNull {
                    expr: Box::new(expr),
                    negated: false,
                })
            }

            ast::Expr::IsNotNull(inner) => {
                let expr = Self::plan_expr_full(
                    inner,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::IsNull {
                    expr: Box::new(expr),
                    negated: true,
                })
            }

            ast::Expr::Nested(inner) => {
                Self::plan_expr_full(inner, schema, subquery_planner, named_windows, udf_resolver)
            }

            ast::Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => case::plan_case_expr(
                operand.as_deref(),
                conditions,
                else_result.as_deref(),
                schema,
                subquery_planner,
                named_windows,
                udf_resolver,
            ),

            ast::Expr::Cast {
                expr,
                data_type,
                kind,
                ..
            } => {
                use sqlparser::ast::CastKind;
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let data_type = plan_data_type(data_type)?;
                let safe = matches!(kind, CastKind::SafeCast | CastKind::TryCast);
                Ok(Expr::Cast {
                    expr: Box::new(expr),
                    data_type,
                    safe,
                })
            }

            ast::Expr::InList {
                expr,
                list,
                negated,
            } => {
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let list = list
                    .iter()
                    .map(|e| {
                        Self::plan_expr_full(
                            e,
                            schema,
                            subquery_planner,
                            named_windows,
                            udf_resolver,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::InList {
                    expr: Box::new(expr),
                    list,
                    negated: *negated,
                })
            }

            ast::Expr::InUnnest {
                expr,
                array_expr,
                negated,
            } => plan_in_unnest(
                expr,
                array_expr,
                *negated,
                schema,
                subquery_planner,
                named_windows,
                udf_resolver,
            ),

            ast::Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let low = Self::plan_expr_full(
                    low,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let high = Self::plan_expr_full(
                    high,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::Between {
                    expr: Box::new(expr),
                    low: Box::new(low),
                    high: Box::new(high),
                    negated: *negated,
                })
            }

            ast::Expr::Like {
                expr,
                pattern,
                negated,
                any,
                ..
            } => {
                if *any && let ast::Expr::Tuple(patterns) = pattern.as_ref() {
                    return Self::plan_like_all_any_from_patterns(
                        expr,
                        patterns,
                        *negated,
                        false,
                        false,
                        schema,
                        subquery_planner,
                    );
                }
                if let Some((is_all, patterns)) = Self::extract_all_any_patterns(pattern) {
                    return Self::plan_like_all_any_from_patterns(
                        expr,
                        &patterns,
                        *negated,
                        is_all,
                        false,
                        schema,
                        subquery_planner,
                    );
                }
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let pattern = Self::plan_expr_full(
                    pattern,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::Like {
                    expr: Box::new(expr),
                    pattern: Box::new(pattern),
                    negated: *negated,
                    case_insensitive: false,
                })
            }

            ast::Expr::ILike {
                expr,
                pattern,
                negated,
                any,
                ..
            } => {
                if *any && let ast::Expr::Tuple(patterns) = pattern.as_ref() {
                    return Self::plan_like_all_any_from_patterns(
                        expr,
                        patterns,
                        *negated,
                        false,
                        true,
                        schema,
                        subquery_planner,
                    );
                }
                if let Some((is_all, patterns)) = Self::extract_all_any_patterns(pattern) {
                    return Self::plan_like_all_any_from_patterns(
                        expr,
                        &patterns,
                        *negated,
                        is_all,
                        true,
                        schema,
                        subquery_planner,
                    );
                }
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let pattern = Self::plan_expr_full(
                    pattern,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::Like {
                    expr: Box::new(expr),
                    pattern: Box::new(pattern),
                    negated: *negated,
                    case_insensitive: true,
                })
            }

            ast::Expr::AllOp {
                left,
                compare_op,
                right,
            } => Self::plan_all_any_op(left, compare_op, right, true, schema, subquery_planner),

            ast::Expr::AnyOp {
                left,
                compare_op,
                right,
                ..
            } => Self::plan_all_any_op(left, compare_op, right, false, schema, subquery_planner),

            ast::Expr::IsTrue(inner) => {
                let expr = Self::plan_expr_full(
                    inner,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::BinaryOp {
                    left: Box::new(expr),
                    op: BinaryOp::Eq,
                    right: Box::new(Expr::Literal(Literal::Bool(true))),
                })
            }

            ast::Expr::IsFalse(inner) => {
                let expr = Self::plan_expr_full(
                    inner,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::BinaryOp {
                    left: Box::new(expr),
                    op: BinaryOp::Eq,
                    right: Box::new(Expr::Literal(Literal::Bool(false))),
                })
            }

            ast::Expr::Array(arr) => {
                plan_array(arr, schema, subquery_planner, named_windows, udf_resolver)
            }

            ast::Expr::Interval(interval) => interval::plan_interval(interval, schema),

            ast::Expr::CompoundFieldAccess { root, access_chain } => plan_compound_field_access(
                root,
                access_chain,
                schema,
                subquery_planner,
                named_windows,
                udf_resolver,
            ),

            ast::Expr::TypedString(typed_string) => {
                let ir_data_type = plan_data_type(&typed_string.data_type)?;
                let value_str = match &typed_string.value.value {
                    ast::Value::SingleQuotedString(s) => s.clone(),
                    ast::Value::DoubleQuotedString(s) => s.clone(),
                    _ => format!("{}", typed_string.value.value),
                };
                Ok(Expr::TypedString {
                    data_type: ir_data_type,
                    value: value_str,
                })
            }

            ast::Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let start = substring_from
                    .as_ref()
                    .map(|e| {
                        Self::plan_expr_full(
                            e,
                            schema,
                            subquery_planner,
                            named_windows,
                            udf_resolver,
                        )
                    })
                    .transpose()?
                    .map(Box::new);
                let length = substring_for
                    .as_ref()
                    .map(|e| {
                        Self::plan_expr_full(
                            e,
                            schema,
                            subquery_planner,
                            named_windows,
                            udf_resolver,
                        )
                    })
                    .transpose()?
                    .map(Box::new);
                Ok(Expr::Substring {
                    expr: Box::new(expr),
                    start,
                    length,
                })
            }

            ast::Expr::Trim {
                expr,
                trim_where,
                trim_what,
                trim_characters,
            } => {
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let trim_what = if let Some(e) = trim_what {
                    Some(Box::new(Self::plan_expr_full(
                        e,
                        schema,
                        subquery_planner,
                        named_windows,
                        udf_resolver,
                    )?))
                } else if let Some(chars) = trim_characters {
                    if let Some(first_char) = chars.first() {
                        Some(Box::new(Self::plan_expr_full(
                            first_char,
                            schema,
                            subquery_planner,
                            named_windows,
                            udf_resolver,
                        )?))
                    } else {
                        None
                    }
                } else {
                    None
                };
                let trim_where_ir = match trim_where {
                    Some(ast::TrimWhereField::Both) | None => TrimWhere::Both,
                    Some(ast::TrimWhereField::Leading) => TrimWhere::Leading,
                    Some(ast::TrimWhereField::Trailing) => TrimWhere::Trailing,
                };
                Ok(Expr::Trim {
                    expr: Box::new(expr),
                    trim_what,
                    trim_where: trim_where_ir,
                })
            }

            ast::Expr::IsDistinctFrom(left, right) => {
                let left = Self::plan_expr_full(
                    left,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let right = Self::plan_expr_full(
                    right,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::IsDistinctFrom {
                    left: Box::new(left),
                    right: Box::new(right),
                    negated: false,
                })
            }

            ast::Expr::IsNotDistinctFrom(left, right) => {
                let left = Self::plan_expr_full(
                    left,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                let right = Self::plan_expr_full(
                    right,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::IsDistinctFrom {
                    left: Box::new(left),
                    right: Box::new(right),
                    negated: true,
                })
            }

            ast::Expr::Floor { expr, .. } => {
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::ScalarFunction {
                    name: ScalarFunction::Floor,
                    args: vec![expr],
                })
            }

            ast::Expr::Ceil { expr, .. } => {
                let expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::ScalarFunction {
                    name: ScalarFunction::Ceil,
                    args: vec![expr],
                })
            }

            ast::Expr::Struct { values, .. } => plan_struct(
                values,
                schema,
                subquery_planner,
                named_windows,
                udf_resolver,
            ),

            ast::Expr::Tuple(exprs) => {
                plan_tuple(exprs, schema, subquery_planner, named_windows, udf_resolver)
            }

            ast::Expr::Extract { field, expr, .. } => {
                let ir_field = plan_datetime_field(field)?;
                let ir_expr = Self::plan_expr_full(
                    expr,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::Extract {
                    field: ir_field,
                    expr: Box::new(ir_expr),
                })
            }

            ast::Expr::Exists { subquery, negated } => {
                plan_exists(subquery, *negated, subquery_planner)
            }

            ast::Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => plan_in_subquery(
                expr,
                subquery,
                *negated,
                schema,
                subquery_planner,
                named_windows,
                udf_resolver,
            ),

            ast::Expr::Subquery(query) => plan_scalar_subquery(query, subquery_planner),

            ast::Expr::Lambda(lambda) => {
                let params: Vec<String> = lambda.params.iter().map(|p| p.value.clone()).collect();
                let body = Self::plan_expr_full(
                    &lambda.body,
                    schema,
                    subquery_planner,
                    named_windows,
                    udf_resolver,
                )?;
                Ok(Expr::Lambda {
                    params,
                    body: Box::new(body),
                })
            }

            _ => Err(Error::unsupported(format!(
                "Unsupported expression: {:?}",
                sql_expr
            ))),
        }
    }

    fn plan_all_any_op(
        left: &ast::Expr,
        compare_op: &ast::BinaryOperator,
        right: &ast::Expr,
        is_all: bool,
        schema: &PlanSchema,
        subquery_planner: Option<SubqueryPlannerFn>,
    ) -> Result<Expr> {
        let left_expr = Self::plan_expr_with_subquery(left, schema, subquery_planner)?;

        let patterns = match right {
            ast::Expr::Tuple(exprs) => exprs
                .iter()
                .map(|e| Self::plan_expr_with_subquery(e, schema, subquery_planner))
                .collect::<Result<Vec<_>>>()?,
            ast::Expr::Array(arr) => arr
                .elem
                .iter()
                .map(|e| Self::plan_expr_with_subquery(e, schema, subquery_planner))
                .collect::<Result<Vec<_>>>()?,
            other => vec![Self::plan_expr_with_subquery(
                other,
                schema,
                subquery_planner,
            )?],
        };

        if patterns.is_empty() {
            return Ok(Expr::Literal(Literal::Bool(is_all)));
        }

        let (case_insensitive, is_like) = match compare_op {
            ast::BinaryOperator::PGLikeMatch => (false, true),
            ast::BinaryOperator::PGILikeMatch => (true, true),
            ast::BinaryOperator::PGNotLikeMatch => (false, true),
            ast::BinaryOperator::PGNotILikeMatch => (true, true),
            _ => (false, false),
        };

        let negated = matches!(
            compare_op,
            ast::BinaryOperator::PGNotLikeMatch | ast::BinaryOperator::PGNotILikeMatch
        );

        let comparisons: Vec<Expr> = patterns
            .into_iter()
            .map(|pattern| {
                if is_like {
                    Expr::Like {
                        expr: Box::new(left_expr.clone()),
                        pattern: Box::new(pattern),
                        negated,
                        case_insensitive,
                    }
                } else {
                    let op = plan_binary_op(compare_op).unwrap_or(BinaryOp::Eq);
                    Expr::BinaryOp {
                        left: Box::new(left_expr.clone()),
                        op,
                        right: Box::new(pattern),
                    }
                }
            })
            .collect();

        let combine_op = if is_all { BinaryOp::And } else { BinaryOp::Or };

        let mut result = comparisons[0].clone();
        for comp in comparisons.into_iter().skip(1) {
            result = Expr::BinaryOp {
                left: Box::new(result),
                op: combine_op,
                right: Box::new(comp),
            };
        }

        Ok(result)
    }

    fn extract_all_any_patterns(pattern: &ast::Expr) -> Option<(bool, Vec<ast::Expr>)> {
        match pattern {
            ast::Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                let is_all = match name.as_str() {
                    "ALL" => true,
                    "ANY" | "SOME" => false,
                    _ => return None,
                };

                let patterns = match &func.args {
                    ast::FunctionArguments::List(arg_list) => arg_list
                        .args
                        .iter()
                        .filter_map(|arg| match arg {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                                Some(e.clone())
                            }
                            _ => None,
                        })
                        .collect(),
                    ast::FunctionArguments::None => vec![],
                    ast::FunctionArguments::Subquery(_) => return None,
                };

                Some((is_all, patterns))
            }
            ast::Expr::AllOp { right, .. } => match right.as_ref() {
                ast::Expr::Tuple(exprs) => Some((true, exprs.clone())),
                other => Some((true, vec![other.clone()])),
            },
            ast::Expr::AnyOp { right, .. } => match right.as_ref() {
                ast::Expr::Tuple(exprs) => Some((false, exprs.clone())),
                other => Some((false, vec![other.clone()])),
            },
            _ => None,
        }
    }

    fn plan_like_all_any_from_patterns(
        expr: &ast::Expr,
        pattern_exprs: &[ast::Expr],
        negated: bool,
        is_all: bool,
        case_insensitive: bool,
        schema: &PlanSchema,
        subquery_planner: Option<SubqueryPlannerFn>,
    ) -> Result<Expr> {
        let left_expr = Self::plan_expr_with_subquery(expr, schema, subquery_planner)?;

        if pattern_exprs.is_empty() {
            return Ok(Expr::Literal(Literal::Bool(is_all)));
        }

        let patterns: Vec<Expr> = pattern_exprs
            .iter()
            .map(|e| Self::plan_expr_with_subquery(e, schema, subquery_planner))
            .collect::<Result<Vec<_>>>()?;

        let comparisons: Vec<Expr> = patterns
            .into_iter()
            .map(|pattern| Expr::Like {
                expr: Box::new(left_expr.clone()),
                pattern: Box::new(pattern),
                negated,
                case_insensitive,
            })
            .collect();

        let combine_op = if is_all { BinaryOp::And } else { BinaryOp::Or };

        let mut result = comparisons[0].clone();
        for comp in comparisons.into_iter().skip(1) {
            result = Expr::BinaryOp {
                left: Box::new(result),
                op: combine_op,
                right: Box::new(comp),
            };
        }

        Ok(result)
    }
}
