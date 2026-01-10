#![coverage(off)]

use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};
use datafusion::common::{Result as DFResult, ToDFSchema};
use datafusion::logical_expr::{
    EmptyRelation, Expr as DFExpr, JoinType as DFJoinType, LogicalPlan as DFLogicalPlan,
    LogicalPlanBuilder, SortExpr as DFSortExpr,
};
use yachtsql_common::types::DataType;
use yachtsql_ir::{JoinType, LogicalPlan, PlanSchema, SetOperationType, SortExpr};

use super::expr_converter::convert_expr;

pub fn convert_plan(plan: &LogicalPlan) -> DFResult<DFLogicalPlan> {
    match plan {
        LogicalPlan::Scan { schema, .. } => {
            let arrow_schema = convert_plan_schema(schema);
            Ok(DFLogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: arrow_schema.to_dfschema_ref()?,
            }))
        }

        LogicalPlan::Filter { input, predicate } => {
            let input_plan = convert_plan(input)?;
            let predicate_expr = convert_expr(predicate)?;
            LogicalPlanBuilder::from(input_plan)
                .filter(predicate_expr)?
                .build()
        }

        LogicalPlan::Project {
            input, expressions, ..
        } => {
            let input_plan = convert_plan(input)?;
            let project_exprs: Vec<DFExpr> = expressions
                .iter()
                .map(convert_expr)
                .collect::<DFResult<_>>()?;
            LogicalPlanBuilder::from(input_plan)
                .project(project_exprs)?
                .build()
        }

        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } => {
            let input_plan = convert_plan(input)?;
            let group_exprs: Vec<DFExpr> =
                group_by.iter().map(convert_expr).collect::<DFResult<_>>()?;
            let agg_exprs: Vec<DFExpr> = aggregates
                .iter()
                .map(convert_expr)
                .collect::<DFResult<_>>()?;
            LogicalPlanBuilder::from(input_plan)
                .aggregate(group_exprs, agg_exprs)?
                .build()
        }

        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            ..
        } => {
            let left_plan = convert_plan(left)?;
            let right_plan = convert_plan(right)?;
            let df_join_type = convert_join_type(join_type);

            let mut builder = LogicalPlanBuilder::from(left_plan);

            match condition {
                Some(cond) => {
                    let cond_expr = convert_expr(cond)?;
                    builder = builder.join_on(right_plan, df_join_type, vec![cond_expr])?;
                }
                None => {
                    builder = builder.cross_join(right_plan)?;
                }
            }

            builder.build()
        }

        LogicalPlan::Sort { input, sort_exprs } => {
            let input_plan = convert_plan(input)?;
            let df_sort_exprs: Vec<DFSortExpr> = sort_exprs
                .iter()
                .map(convert_sort_expr)
                .collect::<DFResult<_>>()?;
            LogicalPlanBuilder::from(input_plan)
                .sort(df_sort_exprs)?
                .build()
        }

        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let input_plan = convert_plan(input)?;
            let skip = offset.unwrap_or(0);
            let fetch = *limit;
            LogicalPlanBuilder::from(input_plan)
                .limit(skip, fetch)?
                .build()
        }

        LogicalPlan::Distinct { input } => {
            let input_plan = convert_plan(input)?;
            LogicalPlanBuilder::from(input_plan).distinct()?.build()
        }

        LogicalPlan::Values { values, .. } => {
            let df_values: Vec<Vec<DFExpr>> = values
                .iter()
                .map(|row| row.iter().map(convert_expr).collect::<DFResult<_>>())
                .collect::<DFResult<_>>()?;
            LogicalPlanBuilder::values(df_values)?.build()
        }

        LogicalPlan::Empty { schema } => {
            let arrow_schema = convert_plan_schema(schema);
            Ok(DFLogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: arrow_schema.to_dfschema_ref()?,
            }))
        }

        LogicalPlan::SetOperation {
            left,
            right,
            op,
            all,
            ..
        } => {
            let left_plan = convert_plan(left)?;
            let right_plan = convert_plan(right)?;

            match op {
                SetOperationType::Union => {
                    if *all {
                        LogicalPlanBuilder::from(left_plan)
                            .union(right_plan)?
                            .build()
                    } else {
                        LogicalPlanBuilder::from(left_plan)
                            .union_distinct(right_plan)?
                            .build()
                    }
                }
                SetOperationType::Intersect => {
                    LogicalPlanBuilder::intersect(left_plan, right_plan, *all)
                }
                SetOperationType::Except => LogicalPlanBuilder::except(left_plan, right_plan, *all),
            }
        }

        LogicalPlan::Window {
            input,
            window_exprs,
            ..
        } => {
            let input_plan = convert_plan(input)?;
            let df_window_exprs: Vec<DFExpr> = window_exprs
                .iter()
                .map(convert_expr)
                .collect::<DFResult<_>>()?;
            LogicalPlanBuilder::from(input_plan)
                .window(df_window_exprs)?
                .build()
        }

        LogicalPlan::Qualify { input, predicate } => {
            let input_plan = convert_plan(input)?;
            let predicate_expr = convert_expr(predicate)?;
            LogicalPlanBuilder::from(input_plan)
                .filter(predicate_expr)?
                .build()
        }

        _ => Err(datafusion::common::DataFusionError::NotImplemented(
            format!(
                "Plan conversion not implemented for: {:?}",
                std::mem::discriminant(plan)
            ),
        )),
    }
}

fn convert_plan_schema(schema: &PlanSchema) -> Arc<ArrowSchema> {
    let fields: Vec<ArrowField> = schema
        .fields
        .iter()
        .map(|f| ArrowField::new(f.name.clone(), convert_data_type(&f.data_type), f.nullable))
        .collect();
    Arc::new(ArrowSchema::new(fields))
}

fn convert_data_type(dt: &DataType) -> ArrowDataType {
    match dt {
        DataType::Bool => ArrowDataType::Boolean,
        DataType::Int64 => ArrowDataType::Int64,
        DataType::Float64 => ArrowDataType::Float64,
        DataType::Numeric(_) | DataType::BigNumeric => ArrowDataType::Decimal128(38, 9),
        DataType::String => ArrowDataType::Utf8,
        DataType::Bytes => ArrowDataType::Binary,
        DataType::Date => ArrowDataType::Date32,
        DataType::Time => ArrowDataType::Time64(TimeUnit::Nanosecond),
        DataType::DateTime => ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
        DataType::Timestamp => ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        DataType::Json => ArrowDataType::Utf8,
        DataType::Geography => ArrowDataType::Utf8,
        DataType::Interval => {
            ArrowDataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano)
        }
        DataType::Array(inner) => ArrowDataType::List(Arc::new(ArrowField::new(
            "item",
            convert_data_type(inner),
            true,
        ))),
        DataType::Struct(fields) => {
            let arrow_fields: Vec<ArrowField> = fields
                .iter()
                .map(|sf| ArrowField::new(&sf.name, convert_data_type(&sf.data_type), true))
                .collect();
            ArrowDataType::Struct(arrow_fields.into())
        }
        DataType::Range(_) => ArrowDataType::Utf8,
        DataType::Unknown => ArrowDataType::Utf8,
    }
}

fn convert_join_type(jt: &JoinType) -> DFJoinType {
    match jt {
        JoinType::Inner => DFJoinType::Inner,
        JoinType::Left => DFJoinType::Left,
        JoinType::Right => DFJoinType::Right,
        JoinType::Full => DFJoinType::Full,
        JoinType::Cross => DFJoinType::Inner,
    }
}

fn convert_sort_expr(se: &SortExpr) -> DFResult<DFSortExpr> {
    let expr = convert_expr(&se.expr)?;
    Ok(DFSortExpr {
        expr,
        asc: se.asc,
        nulls_first: se.nulls_first,
    })
}
