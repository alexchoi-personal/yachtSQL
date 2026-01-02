#![coverage(off)]

use sqlparser::ast;
use yachtsql_common::error::{Error, Result};
use yachtsql_ir::{GapFillColumn, GapFillStrategy, LogicalPlan, PlanSchema};

use super::super::object_name_to_raw_string;
use super::Planner;
use crate::CatalogProvider;
use crate::expr_planner::ExprPlanner;

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub(super) fn plan_gap_fill(
        &self,
        args: &ast::TableFunctionArgs,
        _alias: &Option<ast::TableAlias>,
    ) -> Result<LogicalPlan> {
        let mut input_plan: Option<LogicalPlan> = None;
        let mut ts_column: Option<String> = None;
        let mut bucket_width: Option<yachtsql_ir::Expr> = None;
        let mut value_columns: Vec<GapFillColumn> = Vec::new();
        let mut partitioning_columns: Vec<String> = Vec::new();
        let mut origin: Option<yachtsql_ir::Expr> = None;

        for arg in &args.args {
            match arg {
                ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                    ast::FunctionArgExpr::TableRef(table_name) => {
                        let name_str = object_name_to_raw_string(table_name);
                        if let Some(schema) = self.catalog.get_table_schema(&name_str) {
                            let plan_schema =
                                self.storage_schema_to_plan_schema(&schema, Some(&name_str));
                            input_plan = Some(LogicalPlan::Scan {
                                table_name: name_str.clone(),
                                schema: plan_schema,
                                projection: None,
                            });
                        } else {
                            return Err(Error::invalid_query(format!(
                                "Table not found: {}",
                                name_str
                            )));
                        }
                    }
                    ast::FunctionArgExpr::Expr(ast::Expr::Subquery(query)) => {
                        input_plan = Some(self.plan_query(query)?);
                    }
                    ast::FunctionArgExpr::Expr(_) => {}
                    _ => {}
                },
                ast::FunctionArg::Named { name, arg, .. } => {
                    let name_str = name.value.to_lowercase();
                    match name_str.as_str() {
                        "ts_column" => {
                            if let ast::FunctionArgExpr::Expr(ast::Expr::Value(
                                ast::ValueWithSpan {
                                    value: ast::Value::SingleQuotedString(s),
                                    ..
                                },
                            )) = arg
                            {
                                ts_column = Some(s.clone());
                            }
                        }
                        "bucket_width" => {
                            if let ast::FunctionArgExpr::Expr(e) = arg {
                                bucket_width = Some(ExprPlanner::plan_expr(e, &PlanSchema::new())?);
                            }
                        }
                        "value_columns" => {
                            if let ast::FunctionArgExpr::Expr(e) = arg {
                                value_columns = self.parse_gap_fill_value_columns(e)?;
                            }
                        }
                        "partitioning_columns" => {
                            if let ast::FunctionArgExpr::Expr(e) = arg {
                                partitioning_columns = self.parse_gap_fill_partitioning(e)?;
                            }
                        }
                        "origin" => {
                            if let ast::FunctionArgExpr::Expr(e) = arg {
                                origin = Some(ExprPlanner::plan_expr(e, &PlanSchema::new())?);
                            }
                        }
                        _ => {}
                    }
                }
                ast::FunctionArg::ExprNamed { name, arg, .. } => {
                    let name_str = match name {
                        ast::Expr::Identifier(ident) => ident.value.to_lowercase(),
                        _ => continue,
                    };
                    match name_str.as_str() {
                        "ts_column" => {
                            if let ast::FunctionArgExpr::Expr(ast::Expr::Value(
                                ast::ValueWithSpan {
                                    value: ast::Value::SingleQuotedString(s),
                                    ..
                                },
                            )) = arg
                            {
                                ts_column = Some(s.clone());
                            }
                        }
                        "bucket_width" => {
                            if let ast::FunctionArgExpr::Expr(e) = arg {
                                bucket_width = Some(ExprPlanner::plan_expr(e, &PlanSchema::new())?);
                            }
                        }
                        "value_columns" => {
                            if let ast::FunctionArgExpr::Expr(e) = arg {
                                value_columns = self.parse_gap_fill_value_columns(e)?;
                            }
                        }
                        "partitioning_columns" => {
                            if let ast::FunctionArgExpr::Expr(e) = arg {
                                partitioning_columns = self.parse_gap_fill_partitioning(e)?;
                            }
                        }
                        "origin" => {
                            if let ast::FunctionArgExpr::Expr(e) = arg {
                                origin = Some(ExprPlanner::plan_expr(e, &PlanSchema::new())?);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        let input =
            input_plan.ok_or_else(|| Error::invalid_query("GAP_FILL requires a table input"))?;
        let ts_col =
            ts_column.ok_or_else(|| Error::invalid_query("GAP_FILL requires ts_column"))?;
        let bucket =
            bucket_width.ok_or_else(|| Error::invalid_query("GAP_FILL requires bucket_width"))?;

        let input_schema = input.schema().clone();
        let mut output_fields = Vec::new();

        for field in &input_schema.fields {
            if field.name.to_uppercase() == ts_col.to_uppercase() {
                output_fields.push(field.clone());
            }
        }

        for field in &input_schema.fields {
            if partitioning_columns
                .iter()
                .any(|p| p.to_uppercase() == field.name.to_uppercase())
            {
                output_fields.push(field.clone());
            }
        }

        for vc in &value_columns {
            for field in &input_schema.fields {
                if field.name.to_uppercase() == vc.column_name.to_uppercase() {
                    output_fields.push(field.clone());
                    break;
                }
            }
        }

        let schema = PlanSchema {
            fields: output_fields,
        };

        Ok(LogicalPlan::GapFill {
            input: Box::new(input),
            ts_column: ts_col,
            bucket_width: bucket,
            value_columns,
            partitioning_columns,
            origin,
            input_schema: input_schema.clone(),
            schema,
        })
    }

    fn parse_gap_fill_value_columns(&self, expr: &ast::Expr) -> Result<Vec<GapFillColumn>> {
        let mut columns = Vec::new();
        if let ast::Expr::Array(ast::Array { elem, .. }) = expr {
            for e in elem {
                if let ast::Expr::Tuple(items) = e
                    && items.len() == 2
                {
                    let col_name = match &items[0] {
                        ast::Expr::Value(ast::ValueWithSpan {
                            value: ast::Value::SingleQuotedString(s),
                            ..
                        }) => s.clone(),
                        _ => continue,
                    };
                    let strategy = match &items[1] {
                        ast::Expr::Value(ast::ValueWithSpan {
                            value: ast::Value::SingleQuotedString(s),
                            ..
                        }) => match s.to_lowercase().as_str() {
                            "null" => GapFillStrategy::Null,
                            "locf" => GapFillStrategy::Locf,
                            "linear" => GapFillStrategy::Linear,
                            _ => GapFillStrategy::Null,
                        },
                        _ => GapFillStrategy::Null,
                    };
                    columns.push(GapFillColumn {
                        column_name: col_name,
                        strategy,
                    });
                }
            }
        }
        Ok(columns)
    }

    fn parse_gap_fill_partitioning(&self, expr: &ast::Expr) -> Result<Vec<String>> {
        let mut columns = Vec::new();
        if let ast::Expr::Array(ast::Array { elem, .. }) = expr {
            for e in elem {
                if let ast::Expr::Value(ast::ValueWithSpan {
                    value: ast::Value::SingleQuotedString(s),
                    ..
                }) = e
                {
                    columns.push(s.clone());
                }
            }
        }
        Ok(columns)
    }
}
