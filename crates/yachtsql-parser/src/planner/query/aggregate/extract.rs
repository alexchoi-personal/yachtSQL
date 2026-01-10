#![coverage(off)]

use yachtsql_ir::{Expr, PlanField, PlanSchema};

use super::super::Planner;
use crate::CatalogProvider;

impl<'a, C: CatalogProvider> Planner<'a, C> {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::planner::query::aggregate) fn extract_aggregates_from_expr(
        &self,
        expr: &Expr,
        agg_names: &mut Vec<String>,
        agg_exprs: &mut Vec<Expr>,
        agg_fields: &mut Vec<PlanField>,
        input_schema: &PlanSchema,
        group_by_count: usize,
        group_by_exprs: &[Expr],
    ) -> (Expr, Vec<String>) {
        let mut extracted = Vec::new();
        let replaced = self.replace_aggregates_with_columns(
            expr,
            agg_names,
            agg_exprs,
            agg_fields,
            input_schema,
            group_by_count,
            &mut extracted,
            group_by_exprs,
        );
        (replaced, extracted)
    }

    #[allow(clippy::too_many_arguments)]
    fn replace_aggregates_with_columns(
        &self,
        expr: &Expr,
        agg_names: &mut Vec<String>,
        agg_exprs: &mut Vec<Expr>,
        agg_fields: &mut Vec<PlanField>,
        input_schema: &PlanSchema,
        group_by_count: usize,
        extracted: &mut Vec<String>,
        group_by_exprs: &[Expr],
    ) -> Expr {
        if let Some(idx) = group_by_exprs.iter().position(|gbe| expr == gbe) {
            return Expr::Column {
                table: agg_fields.get(idx).and_then(|f| f.table.clone()),
                name: agg_fields
                    .get(idx)
                    .map(|f| f.name.clone())
                    .unwrap_or_default(),
                index: Some(idx),
            };
        }

        match expr {
            Expr::Aggregate { .. } | Expr::UserDefinedAggregate { .. } => {
                let canonical = Self::canonical_planned_agg_name(expr);
                if let Some(idx) = agg_names.iter().position(|n| n == &canonical) {
                    return Expr::Column {
                        table: None,
                        name: agg_names[idx].clone(),
                        index: Some(group_by_count + idx),
                    };
                }
                let data_type = self.infer_expr_type(expr, input_schema);
                agg_fields.push(PlanField::new(canonical.clone(), data_type));
                agg_exprs.push(expr.clone());
                agg_names.push(canonical.clone());
                extracted.push(canonical.clone());
                Expr::Column {
                    table: None,
                    name: canonical,
                    index: Some(group_by_count + agg_exprs.len() - 1),
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let new_left = self.replace_aggregates_with_columns(
                    left,
                    agg_names,
                    agg_exprs,
                    agg_fields,
                    input_schema,
                    group_by_count,
                    extracted,
                    group_by_exprs,
                );
                let new_right = self.replace_aggregates_with_columns(
                    right,
                    agg_names,
                    agg_exprs,
                    agg_fields,
                    input_schema,
                    group_by_count,
                    extracted,
                    group_by_exprs,
                );
                Expr::BinaryOp {
                    left: Box::new(new_left),
                    op: *op,
                    right: Box::new(new_right),
                }
            }
            Expr::UnaryOp { op, expr: inner } => {
                let new_inner = self.replace_aggregates_with_columns(
                    inner,
                    agg_names,
                    agg_exprs,
                    agg_fields,
                    input_schema,
                    group_by_count,
                    extracted,
                    group_by_exprs,
                );
                Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(new_inner),
                }
            }
            Expr::ScalarFunction { name, args } => {
                let new_args: Vec<Expr> = args
                    .iter()
                    .map(|a| {
                        self.replace_aggregates_with_columns(
                            a,
                            agg_names,
                            agg_exprs,
                            agg_fields,
                            input_schema,
                            group_by_count,
                            extracted,
                            group_by_exprs,
                        )
                    })
                    .collect();
                Expr::ScalarFunction {
                    name: name.clone(),
                    args: new_args,
                }
            }
            Expr::Cast {
                expr: inner,
                data_type,
                safe,
            } => {
                let new_inner = self.replace_aggregates_with_columns(
                    inner,
                    agg_names,
                    agg_exprs,
                    agg_fields,
                    input_schema,
                    group_by_count,
                    extracted,
                    group_by_exprs,
                );
                Expr::Cast {
                    expr: Box::new(new_inner),
                    data_type: data_type.clone(),
                    safe: *safe,
                }
            }
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                let new_operand = operand.as_ref().map(|o| {
                    Box::new(self.replace_aggregates_with_columns(
                        o,
                        agg_names,
                        agg_exprs,
                        agg_fields,
                        input_schema,
                        group_by_count,
                        extracted,
                        group_by_exprs,
                    ))
                });
                let new_whens: Vec<yachtsql_ir::WhenClause> = when_clauses
                    .iter()
                    .map(|w| yachtsql_ir::WhenClause {
                        condition: self.replace_aggregates_with_columns(
                            &w.condition,
                            agg_names,
                            agg_exprs,
                            agg_fields,
                            input_schema,
                            group_by_count,
                            extracted,
                            group_by_exprs,
                        ),
                        result: self.replace_aggregates_with_columns(
                            &w.result,
                            agg_names,
                            agg_exprs,
                            agg_fields,
                            input_schema,
                            group_by_count,
                            extracted,
                            group_by_exprs,
                        ),
                    })
                    .collect();
                let new_else = else_result.as_ref().map(|e| {
                    Box::new(self.replace_aggregates_with_columns(
                        e,
                        agg_names,
                        agg_exprs,
                        agg_fields,
                        input_schema,
                        group_by_count,
                        extracted,
                        group_by_exprs,
                    ))
                });
                Expr::Case {
                    operand: new_operand,
                    when_clauses: new_whens,
                    else_result: new_else,
                }
            }
            Expr::Alias { expr: inner, name } => {
                let new_inner = self.replace_aggregates_with_columns(
                    inner,
                    agg_names,
                    agg_exprs,
                    agg_fields,
                    input_schema,
                    group_by_count,
                    extracted,
                    group_by_exprs,
                );
                Expr::Alias {
                    expr: Box::new(new_inner),
                    name: name.clone(),
                }
            }
            Expr::Window {
                func,
                args,
                partition_by,
                order_by,
                frame,
            } => {
                let new_args: Vec<Expr> = args
                    .iter()
                    .map(|a| {
                        self.replace_aggregates_with_columns(
                            a,
                            agg_names,
                            agg_exprs,
                            agg_fields,
                            input_schema,
                            group_by_count,
                            extracted,
                            group_by_exprs,
                        )
                    })
                    .collect();
                let new_partition_by: Vec<Expr> = partition_by
                    .iter()
                    .map(|e| {
                        self.replace_aggregates_with_columns(
                            e,
                            agg_names,
                            agg_exprs,
                            agg_fields,
                            input_schema,
                            group_by_count,
                            extracted,
                            group_by_exprs,
                        )
                    })
                    .collect();
                let new_order_by: Vec<yachtsql_ir::SortExpr> = order_by
                    .iter()
                    .map(|se| yachtsql_ir::SortExpr {
                        expr: self.replace_aggregates_with_columns(
                            &se.expr,
                            agg_names,
                            agg_exprs,
                            agg_fields,
                            input_schema,
                            group_by_count,
                            extracted,
                            group_by_exprs,
                        ),
                        asc: se.asc,
                        nulls_first: se.nulls_first,
                    })
                    .collect();
                Expr::Window {
                    func: *func,
                    args: new_args,
                    partition_by: new_partition_by,
                    order_by: new_order_by,
                    frame: frame.clone(),
                }
            }
            Expr::IsNull {
                expr: inner,
                negated,
            } => {
                let new_inner = self.replace_aggregates_with_columns(
                    inner,
                    agg_names,
                    agg_exprs,
                    agg_fields,
                    input_schema,
                    group_by_count,
                    extracted,
                    group_by_exprs,
                );
                Expr::IsNull {
                    expr: Box::new(new_inner),
                    negated: *negated,
                }
            }
            Expr::Between {
                expr: inner,
                low,
                high,
                negated,
            } => {
                let new_inner = self.replace_aggregates_with_columns(
                    inner,
                    agg_names,
                    agg_exprs,
                    agg_fields,
                    input_schema,
                    group_by_count,
                    extracted,
                    group_by_exprs,
                );
                let new_low = self.replace_aggregates_with_columns(
                    low,
                    agg_names,
                    agg_exprs,
                    agg_fields,
                    input_schema,
                    group_by_count,
                    extracted,
                    group_by_exprs,
                );
                let new_high = self.replace_aggregates_with_columns(
                    high,
                    agg_names,
                    agg_exprs,
                    agg_fields,
                    input_schema,
                    group_by_count,
                    extracted,
                    group_by_exprs,
                );
                Expr::Between {
                    expr: Box::new(new_inner),
                    low: Box::new(new_low),
                    high: Box::new(new_high),
                    negated: *negated,
                }
            }
            Expr::InList {
                expr: inner,
                list,
                negated,
            } => {
                let new_inner = self.replace_aggregates_with_columns(
                    inner,
                    agg_names,
                    agg_exprs,
                    agg_fields,
                    input_schema,
                    group_by_count,
                    extracted,
                    group_by_exprs,
                );
                let new_list: Vec<Expr> = list
                    .iter()
                    .map(|e| {
                        self.replace_aggregates_with_columns(
                            e,
                            agg_names,
                            agg_exprs,
                            agg_fields,
                            input_schema,
                            group_by_count,
                            extracted,
                            group_by_exprs,
                        )
                    })
                    .collect();
                Expr::InList {
                    expr: Box::new(new_inner),
                    list: new_list,
                    negated: *negated,
                }
            }
            Expr::Like {
                expr: inner,
                pattern,
                negated,
                case_insensitive,
            } => {
                let new_inner = self.replace_aggregates_with_columns(
                    inner,
                    agg_names,
                    agg_exprs,
                    agg_fields,
                    input_schema,
                    group_by_count,
                    extracted,
                    group_by_exprs,
                );
                let new_pattern = self.replace_aggregates_with_columns(
                    pattern,
                    agg_names,
                    agg_exprs,
                    agg_fields,
                    input_schema,
                    group_by_count,
                    extracted,
                    group_by_exprs,
                );
                Expr::Like {
                    expr: Box::new(new_inner),
                    pattern: Box::new(new_pattern),
                    negated: *negated,
                    case_insensitive: *case_insensitive,
                }
            }
            Expr::AggregateWindow {
                func,
                args,
                distinct,
                partition_by,
                order_by,
                frame,
            } => {
                let new_args: Vec<Expr> = args
                    .iter()
                    .map(|a| {
                        self.replace_aggregates_with_columns(
                            a,
                            agg_names,
                            agg_exprs,
                            agg_fields,
                            input_schema,
                            group_by_count,
                            extracted,
                            group_by_exprs,
                        )
                    })
                    .collect();
                let new_partition_by: Vec<Expr> = partition_by
                    .iter()
                    .map(|e| {
                        self.replace_aggregates_with_columns(
                            e,
                            agg_names,
                            agg_exprs,
                            agg_fields,
                            input_schema,
                            group_by_count,
                            extracted,
                            group_by_exprs,
                        )
                    })
                    .collect();
                let new_order_by: Vec<yachtsql_ir::SortExpr> = order_by
                    .iter()
                    .map(|se| yachtsql_ir::SortExpr {
                        expr: self.replace_aggregates_with_columns(
                            &se.expr,
                            agg_names,
                            agg_exprs,
                            agg_fields,
                            input_schema,
                            group_by_count,
                            extracted,
                            group_by_exprs,
                        ),
                        asc: se.asc,
                        nulls_first: se.nulls_first,
                    })
                    .collect();
                Expr::AggregateWindow {
                    func: *func,
                    args: new_args,
                    distinct: *distinct,
                    partition_by: new_partition_by,
                    order_by: new_order_by,
                    frame: frame.clone(),
                }
            }
            Expr::ArrayAccess { array, index } => {
                let new_array = self.replace_aggregates_with_columns(
                    array,
                    agg_names,
                    agg_exprs,
                    agg_fields,
                    input_schema,
                    group_by_count,
                    extracted,
                    group_by_exprs,
                );
                let new_index = self.replace_aggregates_with_columns(
                    index,
                    agg_names,
                    agg_exprs,
                    agg_fields,
                    input_schema,
                    group_by_count,
                    extracted,
                    group_by_exprs,
                );
                Expr::ArrayAccess {
                    array: Box::new(new_array),
                    index: Box::new(new_index),
                }
            }
            _ => expr.clone(),
        }
    }
}
