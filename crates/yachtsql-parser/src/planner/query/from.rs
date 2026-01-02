#![coverage(off)]

use std::collections::HashMap;

use regex::Regex;
use sqlparser::ast::{self, Statement, TableFactor};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::DataType;
use yachtsql_ir::{Expr, FunctionBody, JoinType, LogicalPlan, PlanField, PlanSchema, UnnestColumn};

use super::super::object_name_to_raw_string;
use super::Planner;
use crate::expr_planner::ExprPlanner;
use crate::{CatalogProvider, parse_sql};

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub(in crate::planner) fn plan_from(
        &self,
        from: &[ast::TableWithJoins],
    ) -> Result<LogicalPlan> {
        if from.is_empty() {
            return Ok(LogicalPlan::Empty {
                schema: PlanSchema::new(),
            });
        }

        let first = &from[0];
        let mut plan = self.plan_table_factor(&first.relation, None)?;

        for join in &first.joins {
            let right = self.plan_table_factor(&join.relation, Some(plan.schema()))?;
            plan = match (&right, &join.join_operator) {
                (
                    LogicalPlan::Unnest {
                        input,
                        columns,
                        schema: unnest_schema,
                    },
                    ast::JoinOperator::CrossJoin(_),
                ) if matches!(input.as_ref(), LogicalPlan::Empty { .. }) => {
                    let combined_schema = plan.schema().clone().merge(unnest_schema.clone());
                    LogicalPlan::Unnest {
                        input: Box::new(plan),
                        columns: columns.clone(),
                        schema: combined_schema,
                    }
                }
                _ => self.plan_join(plan, right, &join.join_operator)?,
            };
        }

        for table_with_joins in from.iter().skip(1) {
            let right = self.plan_table_factor(&table_with_joins.relation, Some(plan.schema()))?;

            plan = match right {
                LogicalPlan::Unnest {
                    input,
                    columns,
                    schema: unnest_schema,
                } if matches!(input.as_ref(), LogicalPlan::Empty { .. }) => {
                    let combined_schema = plan.schema().clone().merge(unnest_schema.clone());
                    LogicalPlan::Unnest {
                        input: Box::new(plan),
                        columns,
                        schema: combined_schema,
                    }
                }
                _ => {
                    let combined_schema = plan.schema().clone().merge(right.schema().clone());
                    LogicalPlan::Join {
                        left: Box::new(plan),
                        right: Box::new(right),
                        join_type: JoinType::Cross,
                        condition: None,
                        schema: combined_schema,
                    }
                }
            };

            for join in &table_with_joins.joins {
                let right = self.plan_table_factor(&join.relation, Some(plan.schema()))?;
                plan = self.plan_join(plan, right, &join.join_operator)?;
            }
        }

        Ok(plan)
    }

    pub(in crate::planner) fn plan_table_factor(
        &self,
        factor: &TableFactor,
        left_schema: Option<&PlanSchema>,
    ) -> Result<LogicalPlan> {
        match factor {
            TableFactor::Table {
                name,
                alias,
                sample,
                args,
                ..
            } => {
                let table_name = object_name_to_raw_string(name);
                let table_name_upper = table_name.to_uppercase();

                let base_plan = if let Some(tbl_args) = args {
                    if table_name_upper == "GAP_FILL" {
                        return self.plan_gap_fill(tbl_args, alias);
                    }
                    if let Some(func_def) = self.catalog.get_function(&table_name) {
                        match &func_def.body {
                            FunctionBody::Sql(body_expr) => match body_expr.as_ref() {
                                Expr::Subquery(subquery_plan) => {
                                    let alias_name = alias.as_ref().map(|a| a.name.value.as_str());

                                    let mut param_bindings: HashMap<String, Expr> = HashMap::new();
                                    for (i, arg) in tbl_args.args.iter().enumerate() {
                                        if i < func_def.parameters.len() {
                                            let param_name =
                                                func_def.parameters[i].name.to_uppercase();
                                            let arg_expr = match arg {
                                                ast::FunctionArg::Unnamed(
                                                    ast::FunctionArgExpr::Expr(e),
                                                ) => ExprPlanner::plan_expr(e, &PlanSchema::new())?,
                                                _ => {
                                                    return Err(Error::unsupported(
                                                        "Unsupported function argument type",
                                                    ));
                                                }
                                            };
                                            param_bindings.insert(param_name, arg_expr);
                                        }
                                    }

                                    let mut plan = Self::substitute_params_in_plan(
                                        subquery_plan.as_ref().clone(),
                                        &param_bindings,
                                    );

                                    if let Some(alias_str) = alias_name {
                                        let schema = self.rename_schema(plan.schema(), alias_str);
                                        plan = LogicalPlan::Project {
                                            input: Box::new(plan.clone()),
                                            expressions: plan
                                                .schema()
                                                .fields
                                                .iter()
                                                .enumerate()
                                                .map(|(i, f)| Expr::Column {
                                                    table: None,
                                                    name: f.name.clone(),
                                                    index: Some(i),
                                                })
                                                .collect(),
                                            schema,
                                        };
                                    }
                                    plan
                                }
                                _ => {
                                    return Err(Error::invalid_query(format!(
                                        "Function {} is not a table function",
                                        table_name
                                    )));
                                }
                            },
                            FunctionBody::SqlQuery(query_str) => {
                                let alias_name = alias.as_ref().map(|a| a.name.value.as_str());

                                let mut param_bindings: HashMap<String, String> = HashMap::new();
                                for (i, arg) in tbl_args.args.iter().enumerate() {
                                    if i < func_def.parameters.len() {
                                        let param_name = func_def.parameters[i].name.to_uppercase();
                                        let arg_str = match arg {
                                            ast::FunctionArg::Unnamed(
                                                ast::FunctionArgExpr::Expr(e),
                                            ) => e.to_string(),
                                            _ => {
                                                return Err(Error::unsupported(
                                                    "Unsupported function argument type",
                                                ));
                                            }
                                        };
                                        param_bindings.insert(param_name, arg_str);
                                    }
                                }

                                let mut substituted_query = query_str.clone();
                                for (param_name, value) in &param_bindings {
                                    let pattern_lower = format!(
                                        r"(?i)\b{}\b",
                                        regex::escape(&param_name.to_lowercase())
                                    );
                                    if let Ok(re) = Regex::new(&pattern_lower) {
                                        substituted_query = re
                                            .replace_all(&substituted_query, value.as_str())
                                            .to_string();
                                    }
                                }

                                let parsed = parse_sql(&substituted_query)?;
                                let query_stmt = parsed.first().ok_or_else(|| {
                                    Error::parse_error("Empty table function query")
                                })?;

                                let mut plan = match query_stmt {
                                    Statement::Query(q) => self.plan_query(q)?,
                                    _ => {
                                        return Err(Error::invalid_query(
                                            "Table function body must be a query".to_string(),
                                        ));
                                    }
                                };

                                if let Some(alias_str) = alias_name {
                                    let schema = self.rename_schema(plan.schema(), alias_str);
                                    plan = LogicalPlan::Project {
                                        input: Box::new(plan.clone()),
                                        expressions: plan
                                            .schema()
                                            .fields
                                            .iter()
                                            .enumerate()
                                            .map(|(i, f)| Expr::Column {
                                                table: None,
                                                name: f.name.clone(),
                                                index: Some(i),
                                            })
                                            .collect(),
                                        schema,
                                    };
                                }
                                plan
                            }
                            _ => {
                                return Err(Error::invalid_query(format!(
                                    "Function {} is not a SQL table function",
                                    table_name
                                )));
                            }
                        }
                    } else {
                        return Err(Error::function_not_found(&table_name));
                    }
                } else if let Some(cte_schema) = self.cte_schemas.borrow().get(&table_name_upper) {
                    let alias_name = alias.as_ref().map(|a| a.name.value.as_str());
                    let schema = if let Some(alias) = alias_name {
                        self.rename_schema(cte_schema, alias)
                    } else {
                        cte_schema.clone()
                    };

                    LogicalPlan::Scan {
                        table_name: table_name_upper,
                        schema,
                        projection: None,
                    }
                } else if let Some(storage_schema) = self.catalog.get_table_schema(&table_name) {
                    let alias_name = alias.as_ref().map(|a| a.name.value.as_str());
                    let schema = self.storage_schema_to_plan_schema(
                        &storage_schema,
                        alias_name.or(Some(&table_name)),
                    );

                    LogicalPlan::Scan {
                        table_name,
                        schema,
                        projection: None,
                    }
                } else if let Some(view_def) = self.catalog.get_view(&table_name) {
                    let view_plan = crate::parse_and_plan(&view_def.query, self.catalog)?;
                    let alias_name = alias.as_ref().map(|a| a.name.value.as_str());

                    if !view_def.column_aliases.is_empty() {
                        let view_schema = view_plan.schema();
                        if view_def.column_aliases.len() != view_schema.fields.len() {
                            return Err(Error::invalid_query(format!(
                                "View column count mismatch: expected {}, got {}",
                                view_schema.fields.len(),
                                view_def.column_aliases.len()
                            )));
                        }
                        let new_fields: Vec<PlanField> = view_schema
                            .fields
                            .iter()
                            .zip(view_def.column_aliases.iter())
                            .map(|(f, alias)| PlanField {
                                name: alias.clone(),
                                data_type: f.data_type.clone(),
                                nullable: f.nullable,
                                table: alias_name.map(String::from),
                            })
                            .collect();
                        let new_schema = PlanSchema { fields: new_fields };
                        let expressions: Vec<Expr> = view_plan
                            .schema()
                            .fields
                            .iter()
                            .enumerate()
                            .map(|(i, f)| Expr::Column {
                                table: f.table.clone(),
                                name: f.name.clone(),
                                index: Some(i),
                            })
                            .collect();
                        LogicalPlan::Project {
                            input: Box::new(view_plan),
                            expressions,
                            schema: new_schema,
                        }
                    } else if let Some(alias) = alias_name {
                        let renamed_schema = self.rename_schema(view_plan.schema(), alias);
                        let expressions: Vec<Expr> = view_plan
                            .schema()
                            .fields
                            .iter()
                            .enumerate()
                            .map(|(i, f)| Expr::Column {
                                table: f.table.clone(),
                                name: f.name.clone(),
                                index: Some(i),
                            })
                            .collect();
                        LogicalPlan::Project {
                            input: Box::new(view_plan),
                            expressions,
                            schema: renamed_schema,
                        }
                    } else {
                        view_plan
                    }
                } else {
                    return Err(Error::table_not_found(&table_name));
                };

                self.apply_sample(base_plan, sample)
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let plan = self.plan_query(subquery)?;
                if let Some(a) = alias {
                    let schema = self.rename_schema(plan.schema(), &a.name.value);
                    Ok(LogicalPlan::Project {
                        input: Box::new(plan.clone()),
                        expressions: plan
                            .schema()
                            .fields
                            .iter()
                            .enumerate()
                            .map(|(i, f)| Expr::Column {
                                table: None,
                                name: f.name.clone(),
                                index: Some(i),
                            })
                            .collect(),
                        schema,
                    })
                } else {
                    Ok(plan)
                }
            }
            TableFactor::UNNEST {
                alias,
                array_exprs,
                with_offset,
                with_offset_alias,
                ..
            } => {
                if array_exprs.is_empty() {
                    return Err(Error::invalid_query(
                        "UNNEST requires at least one array expression",
                    ));
                }

                let element_alias = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| "element".to_string());
                let offset_alias_str = with_offset_alias
                    .as_ref()
                    .map(|a| a.value.clone())
                    .unwrap_or_else(|| "offset".to_string());

                let empty_schema = PlanSchema::new();
                let outer_borrowed = self.outer_schema.borrow();
                let context_schema = match (left_schema, outer_borrowed.as_ref()) {
                    (Some(ls), Some(os)) => {
                        let merged = ls.clone().merge(os.clone());
                        drop(outer_borrowed);
                        let merged_box = Box::new(merged);
                        let merged_ref: &'static PlanSchema = Box::leak(merged_box);
                        merged_ref
                    }
                    (Some(ls), None) => {
                        drop(outer_borrowed);
                        ls
                    }
                    (None, Some(os)) => {
                        let os_clone = os.clone();
                        drop(outer_borrowed);
                        let os_box = Box::new(os_clone);
                        let os_ref: &'static PlanSchema = Box::leak(os_box);
                        os_ref
                    }
                    (None, None) => {
                        drop(outer_borrowed);
                        &empty_schema
                    }
                };
                let array_expr = ExprPlanner::plan_expr(&array_exprs[0], context_schema)?;
                let array_type = self.infer_expr_type(&array_expr, context_schema);
                let element_type = match array_type {
                    DataType::Array(inner) => *inner,
                    _ => DataType::Unknown,
                };

                let unnest_column = UnnestColumn {
                    expr: array_expr,
                    alias: Some(element_alias.clone()),
                    with_offset: *with_offset,
                    offset_alias: if *with_offset {
                        Some(offset_alias_str.clone())
                    } else {
                        None
                    },
                };

                let mut fields = if let DataType::Struct(struct_fields) = &element_type {
                    struct_fields
                        .iter()
                        .map(|sf| {
                            let mut field = PlanField::new(sf.name.clone(), sf.data_type.clone());
                            field.table = Some(element_alias.clone());
                            field
                        })
                        .collect()
                } else {
                    vec![PlanField::new(element_alias.clone(), element_type)]
                };
                if *with_offset {
                    fields.push(PlanField::new(offset_alias_str, DataType::Int64));
                }
                let schema = PlanSchema::from_fields(fields);

                Ok(LogicalPlan::Unnest {
                    input: Box::new(LogicalPlan::Empty {
                        schema: PlanSchema::new(),
                    }),
                    columns: vec![unnest_column],
                    schema,
                })
            }
            _ => Err(Error::unsupported(format!(
                "Unsupported table factor: {:?}",
                factor
            ))),
        }
    }

    fn plan_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        join_op: &ast::JoinOperator,
    ) -> Result<LogicalPlan> {
        let (join_type, condition) = match join_op {
            ast::JoinOperator::Inner(constraint) => (
                JoinType::Inner,
                self.extract_join_condition(constraint, &left, &right)?,
            ),
            ast::JoinOperator::Left(constraint) | ast::JoinOperator::LeftOuter(constraint) => (
                JoinType::Left,
                self.extract_join_condition(constraint, &left, &right)?,
            ),
            ast::JoinOperator::Right(constraint) | ast::JoinOperator::RightOuter(constraint) => (
                JoinType::Right,
                self.extract_join_condition(constraint, &left, &right)?,
            ),
            ast::JoinOperator::FullOuter(constraint) => (
                JoinType::Full,
                self.extract_join_condition(constraint, &left, &right)?,
            ),
            ast::JoinOperator::CrossJoin(_) => (JoinType::Cross, None),
            ast::JoinOperator::Join(constraint) => (
                JoinType::Inner,
                self.extract_join_condition(constraint, &left, &right)?,
            ),
            _ => {
                return Err(Error::unsupported(format!(
                    "Unsupported join type: {:?}",
                    join_op
                )));
            }
        };

        let schema = left.schema().clone().merge(right.schema().clone());

        Ok(LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type,
            condition,
            schema,
        })
    }

    fn extract_join_condition(
        &self,
        constraint: &ast::JoinConstraint,
        left: &LogicalPlan,
        right: &LogicalPlan,
    ) -> Result<Option<Expr>> {
        match constraint {
            ast::JoinConstraint::On(expr) => {
                let combined_schema = left.schema().clone().merge(right.schema().clone());
                Ok(Some(ExprPlanner::plan_expr(expr, &combined_schema)?))
            }
            ast::JoinConstraint::None => Ok(None),
            _ => Err(Error::unsupported(format!(
                "Unsupported join constraint: {:?}",
                constraint
            ))),
        }
    }
}
