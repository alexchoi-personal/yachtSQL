#![coverage(off)]

use sqlparser::ast::{self, SetExpr};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, StructField};
use yachtsql_ir::{Expr, LogicalPlan, PlanField, PlanSchema, SetOperationType};

use super::Planner;
use crate::CatalogProvider;
use crate::expr_planner::ExprPlanner;

mod aggregate;
mod cte;
mod export;
mod from;
mod gap_fill;
mod order;
mod params;
mod projection;
mod sample;
mod types;

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub(super) fn plan_query(&self, query: &ast::Query) -> Result<LogicalPlan> {
        let ctes = if let Some(ref with_clause) = query.with {
            Some(self.plan_ctes(with_clause)?)
        } else {
            None
        };

        let mut plan = self.plan_set_expr_with_order(&query.body, query.order_by.as_ref())?;

        if let Some(ref order_by) = query.order_by {
            plan = if let LogicalPlan::Project {
                input,
                expressions,
                schema,
            } = &plan
            {
                let input = input.as_ref();
                let expressions = expressions.clone();
                let schema = schema.clone();
                let has_subquery = expressions.iter().any(contains_subquery);
                if has_subquery {
                    self.plan_order_by_with_aliases(plan, order_by, &expressions, &schema)?
                } else {
                    let sorted = self.plan_order_by_with_aliases(
                        input.clone(),
                        order_by,
                        &expressions,
                        &schema,
                    )?;
                    LogicalPlan::Project {
                        input: Box::new(sorted),
                        expressions,
                        schema,
                    }
                }
            } else {
                self.plan_order_by(plan, order_by)?
            };
        }

        if let Some(ref limit_clause) = query.limit_clause {
            let (limit_val, offset_val) = match limit_clause {
                ast::LimitClause::LimitOffset { limit, offset, .. } => {
                    let l = limit
                        .as_ref()
                        .map(|e| self.extract_limit_value(e))
                        .transpose()?;
                    let o = offset
                        .as_ref()
                        .map(|o| self.extract_offset_value(o))
                        .transpose()?;
                    (l, o)
                }
                ast::LimitClause::OffsetCommaLimit { offset, limit } => {
                    let l = self.extract_limit_value(limit)?;
                    let o = self.extract_limit_value(offset)?;
                    (Some(l), Some(o))
                }
            };
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit: limit_val,
                offset: offset_val,
            };
        }

        if let Some(ctes) = ctes {
            for cte in &ctes {
                self.cte_schemas.borrow_mut().remove(&cte.name);
            }

            plan = LogicalPlan::WithCte {
                ctes,
                body: Box::new(plan),
            };
        }

        Ok(plan)
    }

    fn plan_set_expr(&self, set_expr: &SetExpr) -> Result<LogicalPlan> {
        self.plan_set_expr_with_order(set_expr, None)
    }

    fn plan_set_expr_with_order(
        &self,
        set_expr: &SetExpr,
        order_by: Option<&ast::OrderBy>,
    ) -> Result<LogicalPlan> {
        match set_expr {
            SetExpr::Select(select) => self.plan_select_with_order(select, order_by),
            SetExpr::Values(values) => self.plan_values(values),
            SetExpr::Query(query) => self.plan_query(query),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                let left_plan = self.plan_set_expr(left)?;
                let right_plan = self.plan_set_expr(right)?;

                let ir_op = match op {
                    ast::SetOperator::Union => SetOperationType::Union,
                    ast::SetOperator::Intersect => SetOperationType::Intersect,
                    ast::SetOperator::Except | ast::SetOperator::Minus => SetOperationType::Except,
                };

                let all = matches!(
                    set_quantifier,
                    ast::SetQuantifier::All | ast::SetQuantifier::AllByName
                );

                let schema = left_plan.schema().clone();

                Ok(LogicalPlan::SetOperation {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    op: ir_op,
                    all,
                    schema,
                })
            }
            SetExpr::Insert(stmt) => self.plan_statement(stmt),
            SetExpr::Update(stmt) => self.plan_statement(stmt),
            SetExpr::Delete(stmt) => self.plan_statement(stmt),
            SetExpr::Merge(stmt) => self.plan_statement(stmt),
            _ => Err(Error::unsupported(format!(
                "Unsupported set expression: {:?}",
                set_expr
            ))),
        }
    }

    fn plan_select_with_order(
        &self,
        select: &ast::Select,
        order_by: Option<&ast::OrderBy>,
    ) -> Result<LogicalPlan> {
        let mut plan = self.plan_from(&select.from)?;

        if let Some(ref selection) = select.selection {
            let subquery_planner = |query: &ast::Query| self.plan_query(query);
            let predicate = ExprPlanner::plan_expr_with_subquery(
                selection,
                plan.schema(),
                Some(&subquery_planner),
            )?;
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }

        let has_aggregates = self.has_aggregates(&select.projection);
        let has_group_by =
            !matches!(select.group_by, ast::GroupByExpr::Expressions(ref e, _) if e.is_empty());

        if has_aggregates || has_group_by {
            plan = self.plan_aggregate_with_order(plan, select, order_by)?;
        } else {
            if let Some(ref qualify) = select.qualify {
                let predicate = ExprPlanner::plan_expr(qualify, plan.schema())?;

                if Self::expr_has_window(&predicate) {
                    let window_exprs = Self::extract_all_window_functions(&predicate);
                    if !window_exprs.is_empty() {
                        let window_groups = Self::group_windows_by_spec(&window_exprs);

                        let mut all_window_exprs: Vec<Expr> = Vec::new();
                        let mut current_plan = plan;

                        for group in &window_groups {
                            let _base_field_count = current_plan.schema().fields.len();
                            let mut window_schema_fields = current_plan.schema().fields.clone();
                            for (i, wf) in group.iter().enumerate() {
                                let window_type = self.infer_expr_type(wf, current_plan.schema());
                                window_schema_fields.push(PlanField::new(
                                    format!("__qualify_window_{}", all_window_exprs.len() + i),
                                    window_type,
                                ));
                            }
                            let window_schema = PlanSchema::from_fields(window_schema_fields);

                            current_plan = LogicalPlan::Window {
                                input: Box::new(current_plan),
                                window_exprs: group.clone(),
                                schema: window_schema,
                            };

                            all_window_exprs.extend(group.iter().cloned());
                        }

                        let original_field_count =
                            current_plan.schema().fields.len() - all_window_exprs.len();
                        let replaced_predicate = Self::replace_windows_with_columns(
                            predicate,
                            &all_window_exprs,
                            original_field_count,
                        );

                        plan = LogicalPlan::Qualify {
                            input: Box::new(current_plan),
                            predicate: replaced_predicate,
                        };
                    } else {
                        plan = LogicalPlan::Qualify {
                            input: Box::new(plan),
                            predicate,
                        };
                    }
                } else {
                    plan = LogicalPlan::Qualify {
                        input: Box::new(plan),
                        predicate,
                    };
                }
            }
            plan = self.plan_projection(plan, &select.projection, &select.named_window)?;

            if matches!(
                select.value_table_mode,
                Some(ast::ValueTableMode::AsStruct | ast::ValueTableMode::DistinctAsStruct)
            ) {
                plan = self.wrap_projection_as_struct(plan)?;
            }
        }

        if select.distinct.is_some() {
            plan = LogicalPlan::Distinct {
                input: Box::new(plan),
            };
        }

        Ok(plan)
    }

    pub(super) fn plan_values(&self, values: &ast::Values) -> Result<LogicalPlan> {
        let mut rows = Vec::new();
        let empty_schema = PlanSchema::new();
        let subquery_planner = |query: &ast::Query| self.plan_query(query);

        for row in &values.rows {
            let mut exprs = Vec::new();
            for expr in row {
                exprs.push(ExprPlanner::plan_expr_with_subquery(
                    expr,
                    &empty_schema,
                    Some(&subquery_planner),
                )?);
            }
            rows.push(exprs);
        }

        let schema = if let Some(first_row) = rows.first() {
            let num_cols = first_row.len();
            let mut field_types: Vec<DataType> = vec![DataType::Unknown; num_cols];

            for row in &rows {
                for (i, expr) in row.iter().enumerate() {
                    if i < num_cols && field_types[i] == DataType::Unknown {
                        let data_type = self.infer_expr_type(expr, &empty_schema);
                        if data_type != DataType::Unknown {
                            field_types[i] = data_type;
                        }
                    }
                }
            }

            let fields: Vec<PlanField> = field_types
                .into_iter()
                .enumerate()
                .map(|(i, data_type)| PlanField::new(format!("column{}", i + 1), data_type))
                .collect();
            PlanSchema::from_fields(fields)
        } else {
            PlanSchema::new()
        };

        Ok(LogicalPlan::Values {
            values: rows,
            schema,
        })
    }

    fn expr_name(&self, expr: &ast::Expr) -> String {
        match expr {
            ast::Expr::Identifier(ident) => ident.value.clone(),
            ast::Expr::CompoundIdentifier(parts) => {
                parts.last().map(|p| p.value.clone()).unwrap_or_default()
            }
            ast::Expr::Function(func) => func.name.to_string(),
            _ => format!("{}", expr),
        }
    }

    fn expr_table(&self, expr: &ast::Expr) -> Option<String> {
        match expr {
            ast::Expr::CompoundIdentifier(parts) if parts.len() > 1 => Some(
                parts[..parts.len() - 1]
                    .iter()
                    .map(|p| p.value.clone())
                    .collect::<Vec<_>>()
                    .join("."),
            ),
            _ => None,
        }
    }

    fn wrap_projection_as_struct(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let LogicalPlan::Project {
            input,
            expressions,
            schema,
        } = plan
        else {
            return Err(Error::internal("Expected Project plan for AS STRUCT"));
        };

        let struct_fields: Vec<(Option<String>, Expr)> = expressions
            .into_iter()
            .zip(schema.fields.iter())
            .map(|(expr, field)| (Some(field.name.clone()), expr))
            .collect();

        let struct_expr = Expr::Struct {
            fields: struct_fields,
        };

        let struct_field_types: Vec<StructField> = schema
            .fields
            .iter()
            .map(|f| StructField {
                name: f.name.clone(),
                data_type: f.data_type.clone(),
            })
            .collect();

        let struct_schema = PlanSchema::from_fields(vec![PlanField::new(
            "$struct".to_string(),
            DataType::Struct(struct_field_types),
        )]);

        Ok(LogicalPlan::Project {
            input,
            expressions: vec![struct_expr],
            schema: struct_schema,
        })
    }

    fn extract_limit_value(&self, limit: &ast::Expr) -> Result<usize> {
        match limit {
            ast::Expr::Value(v) => match &v.value {
                ast::Value::Number(n, _) => n
                    .parse()
                    .map_err(|_| Error::parse_error(format!("Invalid LIMIT value: {}", n))),
                _ => Err(Error::parse_error("LIMIT must be a number")),
            },
            _ => Err(Error::parse_error("LIMIT must be a literal number")),
        }
    }

    fn extract_offset_value(&self, offset: &ast::Offset) -> Result<usize> {
        match &offset.value {
            ast::Expr::Value(v) => match &v.value {
                ast::Value::Number(n, _) => n
                    .parse()
                    .map_err(|_| Error::parse_error(format!("Invalid OFFSET value: {}", n))),
                _ => Err(Error::parse_error("OFFSET must be a number")),
            },
            _ => Err(Error::parse_error("OFFSET must be a literal number")),
        }
    }
}

fn contains_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::Subquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::ArraySubquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. } => true,
        Expr::Alias { expr, .. } => contains_subquery(expr),
        Expr::UnaryOp { expr, .. } => contains_subquery(expr),
        Expr::BinaryOp { left, right, .. } => contains_subquery(left) || contains_subquery(right),
        Expr::ScalarFunction { args, .. } => args.iter().any(contains_subquery),
        Expr::Aggregate { args, filter, .. } => {
            args.iter().any(contains_subquery)
                || filter.as_ref().is_some_and(|f| contains_subquery(f))
        }
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            operand.as_ref().is_some_and(|e| contains_subquery(e))
                || when_clauses
                    .iter()
                    .any(|w| contains_subquery(&w.condition) || contains_subquery(&w.result))
                || else_result.as_ref().is_some_and(|e| contains_subquery(e))
        }
        Expr::Cast { expr, .. } => contains_subquery(expr),
        Expr::Between {
            expr, low, high, ..
        } => contains_subquery(expr) || contains_subquery(low) || contains_subquery(high),
        Expr::InList { expr, list, .. } => {
            contains_subquery(expr) || list.iter().any(contains_subquery)
        }
        Expr::IsNull { expr, .. } => contains_subquery(expr),
        _ => false,
    }
}
