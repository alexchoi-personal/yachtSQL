#![coverage(off)]

use sqlparser::ast;

use super::super::Planner;
use crate::CatalogProvider;

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub(in crate::planner::query) fn has_aggregates(&self, items: &[ast::SelectItem]) -> bool {
        items.iter().any(|item| match item {
            ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. } => {
                self.is_aggregate_expr(expr)
            }
            _ => false,
        })
    }

    pub(in crate::planner::query) fn is_aggregate_expr(&self, expr: &ast::Expr) -> bool {
        self.check_aggregate_expr_with_catalog(expr)
    }

    fn check_aggregate_expr_with_catalog(&self, expr: &ast::Expr) -> bool {
        match expr {
            ast::Expr::Function(func) => {
                if func.over.is_some() {
                    return false;
                }
                let name = func.name.to_string().to_uppercase();
                if let Some(udf) = self.catalog.get_function(&name)
                    && udf.is_aggregate
                {
                    return true;
                }
                Self::check_aggregate_expr(expr)
            }
            ast::Expr::BinaryOp { left, right, .. } => {
                self.check_aggregate_expr_with_catalog(left)
                    || self.check_aggregate_expr_with_catalog(right)
            }
            ast::Expr::UnaryOp { expr: inner, .. } => self.check_aggregate_expr_with_catalog(inner),
            ast::Expr::Nested(inner) => self.check_aggregate_expr_with_catalog(inner),
            ast::Expr::Cast { expr: inner, .. } => self.check_aggregate_expr_with_catalog(inner),
            _ => Self::check_aggregate_expr(expr),
        }
    }

    pub(in crate::planner::query) fn ast_has_window_expr(expr: &ast::Expr) -> bool {
        match expr {
            ast::Expr::Function(func) => func.over.is_some(),
            ast::Expr::BinaryOp { left, right, .. } => {
                Self::ast_has_window_expr(left) || Self::ast_has_window_expr(right)
            }
            ast::Expr::UnaryOp { expr, .. } => Self::ast_has_window_expr(expr),
            ast::Expr::Nested(inner) => Self::ast_has_window_expr(inner),
            ast::Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                operand
                    .as_ref()
                    .is_some_and(|e| Self::ast_has_window_expr(e))
                    || conditions.iter().any(|cw| {
                        Self::ast_has_window_expr(&cw.condition)
                            || Self::ast_has_window_expr(&cw.result)
                    })
                    || else_result
                        .as_ref()
                        .is_some_and(|e| Self::ast_has_window_expr(e))
            }
            ast::Expr::Cast { expr, .. } => Self::ast_has_window_expr(expr),
            _ => false,
        }
    }

    fn check_aggregate_expr(expr: &ast::Expr) -> bool {
        match expr {
            ast::Expr::Function(func) => {
                if func.over.is_some() {
                    return false;
                }
                let name = func.name.to_string().to_uppercase();
                let is_agg = matches!(
                    name.as_str(),
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
                        | "GROUPING"
                        | "GROUPING_ID"
                        | "LOGICAL_AND"
                        | "BOOL_AND"
                        | "LOGICAL_OR"
                        | "BOOL_OR"
                        | "BIT_AND"
                        | "BIT_OR"
                        | "BIT_XOR"
                        | "APPROX_COUNT_DISTINCT"
                        | "APPROX_QUANTILES"
                        | "APPROX_TOP_COUNT"
                        | "APPROX_TOP_SUM"
                        | "CORR"
                        | "COVAR_POP"
                        | "COVAR_SAMP"
                        | "STDDEV"
                        | "STDDEV_POP"
                        | "STDDEV_SAMP"
                        | "VARIANCE"
                        | "VAR"
                        | "VAR_POP"
                        | "VAR_SAMP"
                );
                if is_agg {
                    return true;
                }
                if let ast::FunctionArguments::List(args) = &func.args {
                    for arg in &args.args {
                        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) = arg
                            && Self::check_aggregate_expr(e)
                        {
                            return true;
                        }
                    }
                }
                false
            }
            ast::Expr::BinaryOp { left, right, .. } => {
                Self::check_aggregate_expr(left) || Self::check_aggregate_expr(right)
            }
            ast::Expr::UnaryOp { expr, .. } => Self::check_aggregate_expr(expr),
            ast::Expr::Nested(inner) => Self::check_aggregate_expr(inner),
            ast::Expr::Cast { expr, .. } => Self::check_aggregate_expr(expr),
            ast::Expr::IsNull(expr) => Self::check_aggregate_expr(expr),
            ast::Expr::IsNotNull(expr) => Self::check_aggregate_expr(expr),
            ast::Expr::IsTrue(expr) => Self::check_aggregate_expr(expr),
            ast::Expr::IsNotTrue(expr) => Self::check_aggregate_expr(expr),
            ast::Expr::IsFalse(expr) => Self::check_aggregate_expr(expr),
            ast::Expr::IsNotFalse(expr) => Self::check_aggregate_expr(expr),
            ast::Expr::IsUnknown(expr) => Self::check_aggregate_expr(expr),
            ast::Expr::IsNotUnknown(expr) => Self::check_aggregate_expr(expr),
            ast::Expr::Between {
                expr, low, high, ..
            } => {
                Self::check_aggregate_expr(expr)
                    || Self::check_aggregate_expr(low)
                    || Self::check_aggregate_expr(high)
            }
            ast::Expr::InList { expr, list, .. } => {
                Self::check_aggregate_expr(expr) || list.iter().any(Self::check_aggregate_expr)
            }
            ast::Expr::Like { expr, pattern, .. } => {
                Self::check_aggregate_expr(expr) || Self::check_aggregate_expr(pattern)
            }
            ast::Expr::ILike { expr, pattern, .. } => {
                Self::check_aggregate_expr(expr) || Self::check_aggregate_expr(pattern)
            }
            ast::Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                operand
                    .as_ref()
                    .is_some_and(|e| Self::check_aggregate_expr(e))
                    || conditions.iter().any(|cw| {
                        Self::check_aggregate_expr(&cw.condition)
                            || Self::check_aggregate_expr(&cw.result)
                    })
                    || else_result
                        .as_ref()
                        .is_some_and(|e| Self::check_aggregate_expr(e))
            }
            ast::Expr::CompoundFieldAccess { root, access_chain } => {
                Self::check_aggregate_expr(root)
                    || access_chain.iter().any(|accessor| match accessor {
                        ast::AccessExpr::Subscript(ast::Subscript::Index { index }) => {
                            Self::check_aggregate_expr(index)
                        }
                        _ => false,
                    })
            }
            _ => false,
        }
    }
}
