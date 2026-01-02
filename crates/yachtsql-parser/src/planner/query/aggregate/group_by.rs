#![coverage(off)]

use sqlparser::ast;
use yachtsql_ir::{Expr, PlanField};

use super::super::Planner;
use crate::CatalogProvider;

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub(in crate::planner::query) fn only_references_fields(
        expr: &Expr,
        fields: &[PlanField],
        group_by_count: usize,
    ) -> bool {
        match expr {
            Expr::Literal(_) => true,
            Expr::Column { name, table, .. } => fields[..group_by_count].iter().any(|f| {
                f.name.eq_ignore_ascii_case(name)
                    && match (&f.table, table) {
                        (Some(t1), Some(t2)) => t1.eq_ignore_ascii_case(t2),
                        (None, None) => true,
                        (Some(_), None) => true,
                        (None, Some(_)) => true,
                    }
            }),
            Expr::Alias { expr, .. } => Self::only_references_fields(expr, fields, group_by_count),
            Expr::BinaryOp { left, right, .. } => {
                Self::only_references_fields(left, fields, group_by_count)
                    && Self::only_references_fields(right, fields, group_by_count)
            }
            Expr::UnaryOp { expr, .. } => {
                Self::only_references_fields(expr, fields, group_by_count)
            }
            Expr::Cast { expr, .. } => Self::only_references_fields(expr, fields, group_by_count),
            Expr::ScalarFunction { args, .. } => args
                .iter()
                .all(|a| Self::only_references_fields(a, fields, group_by_count)),
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                operand
                    .as_ref()
                    .is_none_or(|e| Self::only_references_fields(e, fields, group_by_count))
                    && when_clauses.iter().all(|w| {
                        Self::only_references_fields(&w.condition, fields, group_by_count)
                            && Self::only_references_fields(&w.result, fields, group_by_count)
                    })
                    && else_result
                        .as_ref()
                        .is_none_or(|e| Self::only_references_fields(e, fields, group_by_count))
            }
            _ => false,
        }
    }

    pub(in crate::planner::query) fn remap_to_group_by_indices(
        expr: Expr,
        fields: &[PlanField],
        group_by_count: usize,
    ) -> Expr {
        match expr {
            Expr::Column { name, table, .. } => {
                if let Some(idx) = fields[..group_by_count].iter().position(|f| {
                    f.name.eq_ignore_ascii_case(&name)
                        && match (&f.table, &table) {
                            (Some(t1), Some(t2)) => t1.eq_ignore_ascii_case(t2),
                            _ => true,
                        }
                }) {
                    Expr::Column {
                        table,
                        name,
                        index: Some(idx),
                    }
                } else {
                    Expr::Column {
                        table,
                        name,
                        index: None,
                    }
                }
            }
            Expr::Alias { expr, name } => Expr::Alias {
                expr: Box::new(Self::remap_to_group_by_indices(
                    *expr,
                    fields,
                    group_by_count,
                )),
                name,
            },
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(Self::remap_to_group_by_indices(
                    *left,
                    fields,
                    group_by_count,
                )),
                op,
                right: Box::new(Self::remap_to_group_by_indices(
                    *right,
                    fields,
                    group_by_count,
                )),
            },
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op,
                expr: Box::new(Self::remap_to_group_by_indices(
                    *expr,
                    fields,
                    group_by_count,
                )),
            },
            Expr::Cast {
                expr,
                data_type,
                safe,
            } => Expr::Cast {
                expr: Box::new(Self::remap_to_group_by_indices(
                    *expr,
                    fields,
                    group_by_count,
                )),
                data_type,
                safe,
            },
            Expr::ScalarFunction { name, args } => Expr::ScalarFunction {
                name,
                args: args
                    .into_iter()
                    .map(|a| Self::remap_to_group_by_indices(a, fields, group_by_count))
                    .collect(),
            },
            other => other,
        }
    }

    pub(in crate::planner::query) fn is_constant_expr(expr: &Expr) -> bool {
        match expr {
            Expr::Literal(_) => true,
            Expr::Alias { expr, .. } => Self::is_constant_expr(expr),
            Expr::BinaryOp { left, right, .. } => {
                Self::is_constant_expr(left) && Self::is_constant_expr(right)
            }
            Expr::UnaryOp { expr, .. } => Self::is_constant_expr(expr),
            Expr::Cast { expr, .. } => Self::is_constant_expr(expr),
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                operand.as_ref().is_none_or(|e| Self::is_constant_expr(e))
                    && when_clauses.iter().all(|w| {
                        Self::is_constant_expr(&w.condition) && Self::is_constant_expr(&w.result)
                    })
                    && else_result
                        .as_ref()
                        .is_none_or(|e| Self::is_constant_expr(e))
            }
            Expr::ScalarFunction { args, .. } => args.iter().all(Self::is_constant_expr),
            Expr::Array { elements, .. } => elements.iter().all(Self::is_constant_expr),
            Expr::Struct { fields, .. } => fields.iter().all(|(_, e)| Self::is_constant_expr(e)),
            _ => false,
        }
    }

    pub(in crate::planner::query) fn expr_contains_subquery(expr: &Expr) -> bool {
        match expr {
            Expr::Subquery(_) | Expr::ScalarSubquery(_) | Expr::ArraySubquery(_) => true,
            Expr::Alias { expr, .. } => Self::expr_contains_subquery(expr),
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_contains_subquery(left) || Self::expr_contains_subquery(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_contains_subquery(expr),
            Expr::Cast { expr, .. } => Self::expr_contains_subquery(expr),
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|e| Self::expr_contains_subquery(e))
                    || when_clauses.iter().any(|w| {
                        Self::expr_contains_subquery(&w.condition)
                            || Self::expr_contains_subquery(&w.result)
                    })
                    || else_result
                        .as_ref()
                        .is_some_and(|e| Self::expr_contains_subquery(e))
            }
            Expr::ScalarFunction { args, .. } => args.iter().any(Self::expr_contains_subquery),
            Expr::Array { elements, .. } => elements.iter().any(Self::expr_contains_subquery),
            Expr::Struct { fields, .. } => {
                fields.iter().any(|(_, e)| Self::expr_contains_subquery(e))
            }
            Expr::IsNull { expr, .. } => Self::expr_contains_subquery(expr),
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::expr_contains_subquery(expr)
                    || Self::expr_contains_subquery(low)
                    || Self::expr_contains_subquery(high)
            }
            Expr::InList { expr, list, .. } => {
                Self::expr_contains_subquery(expr) || list.iter().any(Self::expr_contains_subquery)
            }
            _ => false,
        }
    }

    pub(in crate::planner::query) fn group_expr_key(&self, expr: &ast::Expr) -> String {
        match expr {
            ast::Expr::Identifier(ident) => ident.value.to_uppercase(),
            ast::Expr::CompoundIdentifier(parts) => parts
                .iter()
                .map(|p| p.value.to_uppercase())
                .collect::<Vec<_>>()
                .join("."),
            _ => format!("{:?}", expr),
        }
    }

    pub(in crate::planner::query) fn add_group_expr_to_index_map(
        &self,
        all_exprs: &mut Vec<ast::Expr>,
        expr_indices: &mut std::collections::HashMap<String, usize>,
        expr: &ast::Expr,
    ) -> usize {
        let key = self.group_expr_key(expr);
        if let Some(&idx) = expr_indices.get(&key) {
            return idx;
        }
        let idx = all_exprs.len();
        all_exprs.push(expr.clone());
        expr_indices.insert(key, idx);
        idx
    }

    pub(in crate::planner::query) fn add_group_exprs_to_index_map(
        &self,
        all_exprs: &mut Vec<ast::Expr>,
        expr_indices: &mut std::collections::HashMap<String, usize>,
        exprs: &[ast::Expr],
    ) -> Vec<usize> {
        exprs
            .iter()
            .map(|e| self.add_group_expr_to_index_map(all_exprs, expr_indices, e))
            .collect()
    }

    pub(in crate::planner::query) fn expand_rollup_indices(
        &self,
        indices: &[usize],
    ) -> Vec<Vec<usize>> {
        let mut sets = Vec::new();
        for i in (0..=indices.len()).rev() {
            sets.push(indices[..i].to_vec());
        }
        sets
    }

    pub(in crate::planner::query) fn expand_cube_indices(
        &self,
        indices: &[usize],
    ) -> Vec<Vec<usize>> {
        let n = indices.len();
        let mut sets = Vec::new();
        for mask in (0..(1 << n)).rev() {
            let mut set = Vec::new();
            for (i, &idx) in indices.iter().enumerate() {
                if mask & (1 << i) != 0 {
                    set.push(idx);
                }
            }
            sets.push(set);
        }
        sets
    }
}
