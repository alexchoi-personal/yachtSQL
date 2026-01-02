#![coverage(off)]

use sqlparser::ast;
use yachtsql_common::error::Result;
use yachtsql_ir::{Expr, LogicalPlan, PlanField, PlanSchema};

use super::Planner;
use crate::CatalogProvider;
use crate::expr_planner::ExprPlanner;

mod canonical;
mod collect;
mod detection;
mod extract;
mod group_by;
mod qualify;

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub(super) fn plan_aggregate_with_order(
        &self,
        input: LogicalPlan,
        select: &ast::Select,
        order_by: Option<&ast::OrderBy>,
    ) -> Result<LogicalPlan> {
        let mut group_by_exprs = Vec::new();
        let mut aggregate_exprs = Vec::new();
        let mut agg_fields = Vec::new();
        let mut agg_canonical_names: Vec<String> = Vec::new();
        let subquery_planner = |query: &ast::Query| self.plan_query(query);
        let udf_resolver = |name: &str| self.catalog.get_function(name);
        let mut grouping_sets: Option<Vec<Vec<usize>>> = None;

        let select_aliases: Vec<(String, &ast::Expr)> = select
            .projection
            .iter()
            .filter_map(|item| {
                if let ast::SelectItem::ExprWithAlias { expr, alias } = item {
                    Some((alias.value.to_uppercase(), expr))
                } else {
                    None
                }
            })
            .collect();

        match &select.group_by {
            ast::GroupByExpr::All(_) => {}
            ast::GroupByExpr::Expressions(exprs, _) => {
                let mut all_exprs: Vec<ast::Expr> = Vec::new();
                let mut expr_indices: std::collections::HashMap<String, usize> =
                    std::collections::HashMap::new();
                let mut sets: Vec<Vec<usize>> = Vec::new();
                let mut has_grouping_modifier = false;
                let mut regular_indices: Vec<usize> = Vec::new();

                for expr in exprs {
                    match expr {
                        ast::Expr::Rollup(rollup_exprs) => {
                            has_grouping_modifier = true;
                            let flat_exprs: Vec<ast::Expr> =
                                rollup_exprs.iter().flatten().cloned().collect();
                            let indices = self.add_group_exprs_to_index_map(
                                &mut all_exprs,
                                &mut expr_indices,
                                &flat_exprs,
                            );
                            let rollup_sets = self.expand_rollup_indices(&indices);
                            sets.extend(rollup_sets);
                        }
                        ast::Expr::Cube(cube_exprs) => {
                            has_grouping_modifier = true;
                            let flat_exprs: Vec<ast::Expr> =
                                cube_exprs.iter().flatten().cloned().collect();
                            let indices = self.add_group_exprs_to_index_map(
                                &mut all_exprs,
                                &mut expr_indices,
                                &flat_exprs,
                            );
                            let cube_sets = self.expand_cube_indices(&indices);
                            sets.extend(cube_sets);
                        }
                        ast::Expr::GroupingSets(sets_exprs) => {
                            has_grouping_modifier = true;
                            for set_vec in sets_exprs {
                                if set_vec.len() == 1 {
                                    match &set_vec[0] {
                                        ast::Expr::Rollup(rollup_exprs) => {
                                            let flat_exprs: Vec<ast::Expr> =
                                                rollup_exprs.iter().flatten().cloned().collect();
                                            let indices = self.add_group_exprs_to_index_map(
                                                &mut all_exprs,
                                                &mut expr_indices,
                                                &flat_exprs,
                                            );
                                            let rollup_sets = self.expand_rollup_indices(&indices);
                                            sets.extend(rollup_sets);
                                            continue;
                                        }
                                        ast::Expr::Cube(cube_exprs) => {
                                            let flat_exprs: Vec<ast::Expr> =
                                                cube_exprs.iter().flatten().cloned().collect();
                                            let indices = self.add_group_exprs_to_index_map(
                                                &mut all_exprs,
                                                &mut expr_indices,
                                                &flat_exprs,
                                            );
                                            let cube_sets = self.expand_cube_indices(&indices);
                                            sets.extend(cube_sets);
                                            continue;
                                        }
                                        _ => {}
                                    }
                                }
                                let indices = self.add_group_exprs_to_index_map(
                                    &mut all_exprs,
                                    &mut expr_indices,
                                    set_vec,
                                );
                                sets.push(indices);
                            }
                        }
                        _ => {
                            let resolved_expr = if let ast::Expr::Identifier(ident) = expr {
                                let name = ident.value.to_uppercase();
                                select_aliases
                                    .iter()
                                    .find(|(alias, _)| alias == &name)
                                    .map(|(_, e)| (*e).clone())
                                    .unwrap_or_else(|| expr.clone())
                            } else {
                                expr.clone()
                            };
                            let idx = self.add_group_expr_to_index_map(
                                &mut all_exprs,
                                &mut expr_indices,
                                &resolved_expr,
                            );
                            regular_indices.push(idx);
                        }
                    }
                }

                if has_grouping_modifier {
                    if !regular_indices.is_empty() {
                        let mut expanded_sets = Vec::new();
                        for set in sets {
                            let mut new_set = regular_indices.clone();
                            new_set.extend(set);
                            expanded_sets.push(new_set);
                        }
                        sets = expanded_sets;
                    }
                    grouping_sets = Some(sets);
                }

                for expr in &all_exprs {
                    let planned = ExprPlanner::plan_expr_with_udf_resolver(
                        expr,
                        input.schema(),
                        Some(&subquery_planner),
                        &select.named_window,
                        Some(&udf_resolver),
                    )?;
                    let name = self.expr_name(expr);
                    let data_type = self.infer_expr_type(&planned, input.schema());
                    let table = match &planned {
                        Expr::Column { table, .. } => table.clone(),
                        Expr::Alias { expr, .. } => match expr.as_ref() {
                            Expr::Column { table, .. } => table.clone(),
                            _ => None,
                        },
                        _ => None,
                    };
                    let mut field = PlanField::new(name, data_type);
                    field.table = table;
                    agg_fields.push(field);
                    group_by_exprs.push(planned);
                }
            }
        }

        let mut final_projection_exprs: Vec<Expr> = Vec::new();
        let mut final_projection_fields: Vec<PlanField> = Vec::new();
        let group_by_count = group_by_exprs.len();

        let mut window_funcs: Vec<Expr> = Vec::new();
        let mut window_expr_indices: Vec<usize> = Vec::new();

        for item in &select.projection {
            match item {
                ast::SelectItem::UnnamedExpr(expr)
                | ast::SelectItem::ExprWithAlias { expr, .. } => {
                    let output_name = match item {
                        ast::SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                        _ => self.expr_name(expr),
                    };

                    if self.is_aggregate_expr(expr) {
                        if Self::is_pure_aggregate_expr(expr) {
                            let planned = ExprPlanner::plan_expr_with_udf_resolver(
                                expr,
                                input.schema(),
                                Some(&subquery_planner),
                                &select.named_window,
                                Some(&udf_resolver),
                            )?;
                            let canonical = Self::canonical_agg_name(expr);
                            let data_type = self.infer_expr_type(&planned, input.schema());
                            agg_fields.push(PlanField::new(canonical.clone(), data_type.clone()));
                            aggregate_exprs.push(planned);
                            agg_canonical_names.push(canonical.clone());

                            let col_idx = group_by_count + aggregate_exprs.len() - 1;
                            final_projection_exprs.push(Expr::Column {
                                table: None,
                                name: canonical,
                                index: Some(col_idx),
                            });
                            final_projection_fields.push(PlanField::new(output_name, data_type));
                        } else {
                            let planned = ExprPlanner::plan_expr_with_udf_resolver(
                                expr,
                                input.schema(),
                                Some(&subquery_planner),
                                &select.named_window,
                                Some(&udf_resolver),
                            )?;
                            let (replaced_expr, _extracted_aggs) = self
                                .extract_aggregates_from_expr(
                                    &planned,
                                    &mut agg_canonical_names,
                                    &mut aggregate_exprs,
                                    &mut agg_fields,
                                    input.schema(),
                                    group_by_count,
                                    &group_by_exprs,
                                );
                            let data_type = self.infer_expr_type(&planned, input.schema());
                            if Self::expr_has_window(&replaced_expr) {
                                window_expr_indices.push(final_projection_exprs.len());
                                if let Some(wf) = Self::extract_window_function(&replaced_expr) {
                                    window_funcs.push(wf);
                                }
                            }
                            final_projection_exprs.push(replaced_expr);
                            final_projection_fields.push(PlanField::new(output_name, data_type));
                        }
                    } else if Self::ast_has_window_expr(expr) {
                        let planned = ExprPlanner::plan_expr_with_udf_resolver(
                            expr,
                            input.schema(),
                            Some(&subquery_planner),
                            &select.named_window,
                            Some(&udf_resolver),
                        )?;
                        let (replaced_window_expr, _) = self.extract_aggregates_from_expr(
                            &planned,
                            &mut agg_canonical_names,
                            &mut aggregate_exprs,
                            &mut agg_fields,
                            input.schema(),
                            group_by_count,
                            &group_by_exprs,
                        );
                        let data_type = self.infer_expr_type(&planned, input.schema());
                        window_expr_indices.push(final_projection_exprs.len());
                        if let Some(wf) = Self::extract_window_function(&replaced_window_expr) {
                            window_funcs.push(wf);
                        }
                        final_projection_exprs.push(replaced_window_expr);
                        final_projection_fields.push(PlanField::new(output_name, data_type));
                    } else {
                        let col_name = self.expr_name(expr);
                        let col_table = self.expr_table(expr);
                        if let Some(idx) = agg_fields.iter().position(|f| {
                            f.name.eq_ignore_ascii_case(&col_name)
                                && match (&f.table, &col_table) {
                                    (Some(t1), Some(t2)) => t1.eq_ignore_ascii_case(t2),
                                    (None, None) => true,
                                    _ => false,
                                }
                        }) {
                            let data_type = agg_fields[idx].data_type.clone();
                            final_projection_exprs.push(Expr::Column {
                                table: col_table.clone(),
                                name: col_name.clone(),
                                index: Some(idx),
                            });
                            final_projection_fields.push(PlanField::new(output_name, data_type));
                        } else if let Some(idx) = agg_fields
                            .iter()
                            .position(|f| f.name.eq_ignore_ascii_case(&col_name))
                        {
                            let data_type = agg_fields[idx].data_type.clone();
                            final_projection_exprs.push(Expr::Column {
                                table: None,
                                name: col_name.clone(),
                                index: Some(idx),
                            });
                            final_projection_fields.push(PlanField::new(output_name, data_type));
                        } else {
                            let planned = ExprPlanner::plan_expr_with_udf_resolver(
                                expr,
                                input.schema(),
                                Some(&subquery_planner),
                                &select.named_window,
                                Some(&udf_resolver),
                            )?;
                            if Self::is_constant_expr(&planned)
                                || Self::only_references_fields(
                                    &planned,
                                    &agg_fields,
                                    group_by_count,
                                )
                            {
                                let remapped = Self::remap_to_group_by_indices(
                                    planned,
                                    &agg_fields,
                                    group_by_count,
                                );
                                let data_type = self.infer_expr_type(
                                    &remapped,
                                    &PlanSchema::from_fields(agg_fields.clone()),
                                );
                                final_projection_exprs.push(remapped);
                                final_projection_fields
                                    .push(PlanField::new(output_name, data_type));
                            } else if Self::expr_contains_subquery(&planned) {
                                let remapped = Self::remap_to_group_by_indices(
                                    planned.clone(),
                                    &agg_fields,
                                    group_by_count,
                                );
                                let data_type = self.infer_expr_type(&planned, input.schema());
                                final_projection_exprs.push(remapped);
                                final_projection_fields
                                    .push(PlanField::new(output_name, data_type));
                            }
                        }
                    }
                }
                ast::SelectItem::Wildcard(_) | ast::SelectItem::QualifiedWildcard(_, _) => {
                    for (i, field) in agg_fields.iter().enumerate() {
                        final_projection_exprs.push(Expr::Column {
                            table: field.table.clone(),
                            name: field.name.clone(),
                            index: Some(i),
                        });
                        final_projection_fields.push(field.clone());
                    }
                }
            }
        }

        if let Some(ref having) = select.having {
            Self::collect_having_aggregates(
                having,
                input.schema(),
                &mut agg_canonical_names,
                &mut aggregate_exprs,
                &mut agg_fields,
            )?;
        }

        if let Some(order_by) = order_by {
            Self::collect_order_by_aggregates(
                order_by,
                input.schema(),
                &mut agg_canonical_names,
                &mut aggregate_exprs,
                &mut agg_fields,
            )?;
        }

        let agg_schema = PlanSchema::from_fields(agg_fields);
        let mut agg_plan = LogicalPlan::Aggregate {
            input: Box::new(input),
            group_by: group_by_exprs,
            aggregates: aggregate_exprs,
            schema: agg_schema.clone(),
            grouping_sets,
        };

        if let Some(ref having) = select.having {
            let predicate = self.plan_having_expr(having, &agg_schema)?;
            agg_plan = LogicalPlan::Filter {
                input: Box::new(agg_plan),
                predicate,
            };
        }

        if let Some(ref qualify) = select.qualify {
            let qualify_predicate = self.plan_qualify_expr_with_agg_schema(qualify, &agg_schema)?;

            if Self::expr_has_window(&qualify_predicate) {
                if let Some(wf) = Self::extract_window_function(&qualify_predicate) {
                    let agg_field_count = agg_plan.schema().fields.len();
                    let mut qualify_window_schema_fields = agg_plan.schema().fields.clone();
                    let qualify_window_type = self.infer_expr_type(&wf, &agg_schema);
                    qualify_window_schema_fields.push(PlanField::new(
                        "__qualify_window_0".to_string(),
                        qualify_window_type,
                    ));
                    let qualify_window_schema =
                        PlanSchema::from_fields(qualify_window_schema_fields.clone());

                    let qualify_window_plan = LogicalPlan::Window {
                        input: Box::new(agg_plan),
                        window_exprs: vec![wf],
                        schema: qualify_window_schema.clone(),
                    };

                    let replaced_predicate = Self::replace_window_with_column(
                        qualify_predicate,
                        "__qualify_window_0",
                        agg_field_count,
                    );

                    agg_plan = LogicalPlan::Qualify {
                        input: Box::new(qualify_window_plan),
                        predicate: replaced_predicate,
                    };
                } else {
                    agg_plan = LogicalPlan::Qualify {
                        input: Box::new(agg_plan),
                        predicate: qualify_predicate,
                    };
                }
            } else {
                agg_plan = LogicalPlan::Qualify {
                    input: Box::new(agg_plan),
                    predicate: qualify_predicate,
                };
            }
        }

        if !window_funcs.is_empty() {
            let agg_field_count = agg_plan.schema().fields.len();
            let mut window_schema_fields = agg_plan.schema().fields.clone();
            for (j, &idx) in window_expr_indices.iter().enumerate() {
                window_schema_fields.push(PlanField::new(
                    format!("__window_{}", j),
                    final_projection_fields[idx].data_type.clone(),
                ));
            }
            let window_schema = PlanSchema::from_fields(window_schema_fields);

            let window_plan = LogicalPlan::Window {
                input: Box::new(agg_plan),
                window_exprs: window_funcs,
                schema: window_schema.clone(),
            };

            let mut new_projection_exprs = Vec::new();
            let mut window_offset = 0usize;
            for (i, expr) in final_projection_exprs.iter().enumerate() {
                if window_expr_indices.contains(&i) {
                    let col_idx = agg_field_count + window_offset;
                    let col_name = format!("__window_{}", window_offset);
                    let replaced =
                        Self::replace_window_with_column(expr.clone(), &col_name, col_idx);
                    new_projection_exprs.push(Self::remap_column_indices(replaced, &window_schema));
                    window_offset += 1;
                } else {
                    new_projection_exprs
                        .push(Self::remap_column_indices(expr.clone(), &window_schema));
                }
            }

            return Ok(LogicalPlan::Project {
                input: Box::new(window_plan),
                expressions: new_projection_exprs,
                schema: PlanSchema::from_fields(final_projection_fields),
            });
        }

        if !final_projection_exprs.is_empty() {
            Ok(LogicalPlan::Project {
                input: Box::new(agg_plan),
                expressions: final_projection_exprs,
                schema: PlanSchema::from_fields(final_projection_fields),
            })
        } else {
            Ok(agg_plan)
        }
    }

    fn is_pure_aggregate_expr(expr: &ast::Expr) -> bool {
        match expr {
            ast::Expr::Function(func) => {
                if func.over.is_some() {
                    return false;
                }
                let name = func.name.to_string().to_uppercase();
                Self::is_aggregate_function_name(&name)
            }
            ast::Expr::Nested(inner) => Self::is_pure_aggregate_expr(inner),
            _ => false,
        }
    }
}
