#![coverage(off)]

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, TimeUnit};
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::common::{Column, Result as DFResult, ScalarValue, TableReference};
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Case, Cast, Expr as DFExpr, Like, Operator};
use datafusion::prelude::*;
use yachtsql_common::types::DataType;
use yachtsql_ir::{
    AggregateFunction, BinaryOp, DateTimeField, Expr, Literal, ScalarFunction, UnaryOp,
};

pub fn convert_expr(expr: &Expr) -> DFResult<DFExpr> {
    match expr {
        Expr::Literal(lit) => convert_literal(lit),

        Expr::Column { table, name, .. } => {
            let col = match table {
                Some(t) => Column::new(Some(t.clone()), name.clone()),
                None => Column::new_unqualified(name.clone()),
            };
            Ok(DFExpr::Column(col))
        }

        Expr::BinaryOp { left, op, right } => {
            let left_expr = convert_expr(left)?;
            let right_expr = convert_expr(right)?;
            let operator = convert_binary_op(op);
            Ok(DFExpr::BinaryExpr(BinaryExpr::new(
                Box::new(left_expr),
                operator,
                Box::new(right_expr),
            )))
        }

        Expr::UnaryOp { op, expr: inner } => {
            let inner_expr = convert_expr(inner)?;
            match op {
                UnaryOp::Not => Ok(DFExpr::Not(Box::new(inner_expr))),
                UnaryOp::Minus => Ok(DFExpr::Negative(Box::new(inner_expr))),
                UnaryOp::Plus => Ok(inner_expr),
                UnaryOp::BitwiseNot => Ok(DFExpr::Not(Box::new(inner_expr))),
            }
        }

        Expr::ScalarFunction { name, args } => {
            let df_args: Vec<DFExpr> = args.iter().map(convert_expr).collect::<DFResult<_>>()?;
            convert_scalar_function(name, df_args)
        }

        Expr::Aggregate {
            func,
            args,
            distinct,
            filter,
            ..
        } => {
            let df_args: Vec<DFExpr> = args.iter().map(convert_expr).collect::<DFResult<_>>()?;
            let df_filter = filter
                .as_ref()
                .map(|f| convert_expr(f))
                .transpose()?
                .map(Box::new);
            convert_aggregate_function(func, df_args, *distinct, df_filter)
        }

        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            let operand_expr = operand.as_ref().map(|o| convert_expr(o)).transpose()?;
            let when_then: Vec<(Box<DFExpr>, Box<DFExpr>)> = when_clauses
                .iter()
                .map(|wc| {
                    let when = convert_expr(&wc.condition)?;
                    let then = convert_expr(&wc.result)?;
                    Ok((Box::new(when), Box::new(then)))
                })
                .collect::<DFResult<_>>()?;
            let else_expr = else_result
                .as_ref()
                .map(|e| convert_expr(e))
                .transpose()?
                .map(Box::new);

            Ok(DFExpr::Case(Case::new(
                operand_expr.map(Box::new),
                when_then,
                else_expr,
            )))
        }

        Expr::Cast {
            expr: inner,
            data_type,
            safe,
        } => {
            let inner_expr = convert_expr(inner)?;
            let arrow_type = convert_data_type(data_type);
            if *safe {
                Ok(DFExpr::TryCast(datafusion::logical_expr::TryCast::new(
                    Box::new(inner_expr),
                    arrow_type,
                )))
            } else {
                Ok(DFExpr::Cast(Cast::new(Box::new(inner_expr), arrow_type)))
            }
        }

        Expr::IsNull {
            expr: inner,
            negated,
        } => {
            let inner_expr = convert_expr(inner)?;
            if *negated {
                Ok(DFExpr::IsNotNull(Box::new(inner_expr)))
            } else {
                Ok(DFExpr::IsNull(Box::new(inner_expr)))
            }
        }

        Expr::Between {
            expr: inner,
            low,
            high,
            negated,
        } => {
            let inner_expr = convert_expr(inner)?;
            let low_expr = convert_expr(low)?;
            let high_expr = convert_expr(high)?;
            Ok(DFExpr::Between(datafusion::logical_expr::Between::new(
                Box::new(inner_expr),
                *negated,
                Box::new(low_expr),
                Box::new(high_expr),
            )))
        }

        Expr::InList {
            expr: inner,
            list,
            negated,
        } => {
            let inner_expr = convert_expr(inner)?;
            let list_exprs: Vec<DFExpr> = list.iter().map(convert_expr).collect::<DFResult<_>>()?;
            Ok(DFExpr::InList(InList::new(
                Box::new(inner_expr),
                list_exprs,
                *negated,
            )))
        }

        Expr::Like {
            expr: inner,
            pattern,
            negated,
            case_insensitive,
        } => {
            let inner_expr = convert_expr(inner)?;
            let pattern_expr = convert_expr(pattern)?;
            Ok(DFExpr::Like(Like::new(
                *negated,
                Box::new(inner_expr),
                Box::new(pattern_expr),
                None,
                *case_insensitive,
            )))
        }

        Expr::Alias { expr: inner, name } => {
            let inner_expr = convert_expr(inner)?;
            Ok(inner_expr.alias(name.clone()))
        }

        Expr::Array { elements, .. } => {
            let elem_exprs: Vec<DFExpr> =
                elements.iter().map(convert_expr).collect::<DFResult<_>>()?;
            Ok(datafusion::functions_nested::make_array::make_array(
                elem_exprs,
            ))
        }

        Expr::Wildcard { table } => match table {
            Some(t) => Ok(DFExpr::Wildcard {
                qualifier: Some(TableReference::bare(t.clone())),
                options: Default::default(),
            }),
            None => Ok(DFExpr::Wildcard {
                qualifier: None,
                options: Default::default(),
            }),
        },

        Expr::Extract { field, expr: inner } => {
            let inner_expr = convert_expr(inner)?;
            let part = convert_datetime_field(field);
            Ok(datafusion::functions::datetime::date_part().call(vec![lit(part), inner_expr]))
        }

        _ => Err(datafusion::common::DataFusionError::NotImplemented(
            format!(
                "Expression conversion not implemented for: {:?}",
                std::mem::discriminant(expr)
            ),
        )),
    }
}

fn convert_literal(lit: &Literal) -> DFResult<DFExpr> {
    let scalar = match lit {
        Literal::Null => ScalarValue::Null,
        Literal::Bool(v) => ScalarValue::Boolean(Some(*v)),
        Literal::Int64(v) => ScalarValue::Int64(Some(*v)),
        Literal::Float64(v) => ScalarValue::Float64(Some(v.into_inner())),
        Literal::Numeric(v) => {
            ScalarValue::Decimal128(Some(v.mantissa()), 38, v.scale().try_into().unwrap_or(9))
        }
        Literal::BigNumeric(v) => {
            ScalarValue::Decimal128(Some(v.mantissa()), 76, v.scale().try_into().unwrap_or(38))
        }
        Literal::String(v) => ScalarValue::Utf8(Some(v.clone())),
        Literal::Bytes(v) => ScalarValue::Binary(Some(v.clone())),
        Literal::Date(days) => ScalarValue::Date32(Some(*days)),
        Literal::Time(nanos) => ScalarValue::Time64Nanosecond(Some(*nanos)),
        Literal::Datetime(nanos) => ScalarValue::TimestampNanosecond(Some(*nanos), None),
        Literal::Timestamp(nanos) => {
            ScalarValue::TimestampNanosecond(Some(*nanos), Some("UTC".into()))
        }
        Literal::Interval {
            months,
            days,
            nanos,
        } => ScalarValue::IntervalMonthDayNano(Some(
            datafusion::arrow::datatypes::IntervalMonthDayNano::new(*months, *days, *nanos),
        )),
        Literal::Array(elements) => {
            let values: Vec<ScalarValue> = elements
                .iter()
                .map(|e| {
                    if let DFExpr::Literal(sv) = convert_literal(e)? {
                        Ok(sv)
                    } else {
                        Err(datafusion::common::DataFusionError::Internal(
                            "Expected literal".to_string(),
                        ))
                    }
                })
                .collect::<DFResult<_>>()?;
            ScalarValue::List(ScalarValue::new_list_nullable(
                &values,
                &ArrowDataType::Utf8,
            ))
        }
        Literal::Struct(fields) => {
            let mut builder = ScalarStructBuilder::new();
            for (name, lit) in fields {
                if let DFExpr::Literal(sv) = convert_literal(lit)? {
                    let field = ArrowField::new(name.clone(), sv.data_type(), true);
                    builder = builder.with_scalar(field, sv);
                } else {
                    return Err(datafusion::common::DataFusionError::Internal(
                        "Expected literal".to_string(),
                    ));
                }
            }
            builder.build()?
        }
        Literal::Json(v) => ScalarValue::Utf8(Some(v.to_string())),
    };
    Ok(DFExpr::Literal(scalar))
}

fn convert_binary_op(op: &BinaryOp) -> Operator {
    match op {
        BinaryOp::Add => Operator::Plus,
        BinaryOp::Sub => Operator::Minus,
        BinaryOp::Mul => Operator::Multiply,
        BinaryOp::Div => Operator::Divide,
        BinaryOp::Mod => Operator::Modulo,
        BinaryOp::Eq => Operator::Eq,
        BinaryOp::NotEq => Operator::NotEq,
        BinaryOp::Lt => Operator::Lt,
        BinaryOp::LtEq => Operator::LtEq,
        BinaryOp::Gt => Operator::Gt,
        BinaryOp::GtEq => Operator::GtEq,
        BinaryOp::And => Operator::And,
        BinaryOp::Or => Operator::Or,
        BinaryOp::BitwiseAnd => Operator::BitwiseAnd,
        BinaryOp::BitwiseOr => Operator::BitwiseOr,
        BinaryOp::BitwiseXor => Operator::BitwiseXor,
        BinaryOp::ShiftLeft => Operator::BitwiseShiftLeft,
        BinaryOp::ShiftRight => Operator::BitwiseShiftRight,
        BinaryOp::Concat => Operator::StringConcat,
    }
}

fn convert_scalar_function(name: &ScalarFunction, args: Vec<DFExpr>) -> DFResult<DFExpr> {
    use datafusion::functions::core::expr_fn as core;
    use datafusion::functions::math::expr_fn as math;
    use datafusion::functions::string::expr_fn as string;
    use datafusion::functions::unicode::expr_fn as unicode;

    match name {
        ScalarFunction::Upper => Ok(string::upper(args.into_iter().next().unwrap())),
        ScalarFunction::Lower => Ok(string::lower(args.into_iter().next().unwrap())),
        ScalarFunction::Length => Ok(unicode::character_length(args.into_iter().next().unwrap())),
        ScalarFunction::Concat => Ok(datafusion::functions::string::concat().call(args)),
        ScalarFunction::Abs => Ok(math::abs(args.into_iter().next().unwrap())),
        ScalarFunction::Ceil => Ok(math::ceil(args.into_iter().next().unwrap())),
        ScalarFunction::Floor => Ok(math::floor(args.into_iter().next().unwrap())),
        ScalarFunction::Round => {
            let mut iter = args.into_iter();
            let value = iter.next().unwrap();
            match iter.next() {
                Some(places) => Ok(math::round(vec![value, places])),
                None => Ok(math::round(vec![value, lit(0)])),
            }
        }
        ScalarFunction::Sqrt => Ok(math::sqrt(args.into_iter().next().unwrap())),
        ScalarFunction::Power | ScalarFunction::Pow => {
            let mut iter = args.into_iter();
            Ok(math::power(iter.next().unwrap(), iter.next().unwrap()))
        }
        ScalarFunction::Coalesce => Ok(core::coalesce(args)),
        ScalarFunction::NullIf => {
            let mut iter = args.into_iter();
            Ok(core::nullif(iter.next().unwrap(), iter.next().unwrap()))
        }
        _ => Err(datafusion::common::DataFusionError::NotImplemented(
            format!("Scalar function not implemented: {:?}", name),
        )),
    }
}

fn convert_aggregate_function(
    func: &AggregateFunction,
    args: Vec<DFExpr>,
    distinct: bool,
    filter: Option<Box<DFExpr>>,
) -> DFResult<DFExpr> {
    use datafusion::functions_aggregate::*;

    let mut agg_expr = match func {
        AggregateFunction::Count => count::count(args.into_iter().next().unwrap_or(lit(1))),
        AggregateFunction::Sum => sum::sum(args.into_iter().next().unwrap()),
        AggregateFunction::Avg => average::avg(args.into_iter().next().unwrap()),
        AggregateFunction::Min => min_max::min(args.into_iter().next().unwrap()),
        AggregateFunction::Max => min_max::max(args.into_iter().next().unwrap()),
        _ => {
            return Err(datafusion::common::DataFusionError::NotImplemented(
                format!("Aggregate function not implemented: {:?}", func),
            ));
        }
    };

    if distinct {
        agg_expr = agg_expr.distinct().build()?;
    }

    if let Some(f) = filter {
        agg_expr = agg_expr.filter(*f).build()?;
    }

    Ok(agg_expr)
}

fn convert_datetime_field(field: &DateTimeField) -> &'static str {
    match field {
        DateTimeField::Year => "year",
        DateTimeField::Month => "month",
        DateTimeField::Day => "day",
        DateTimeField::Hour => "hour",
        DateTimeField::Minute => "minute",
        DateTimeField::Second => "second",
        DateTimeField::Millisecond => "millisecond",
        DateTimeField::Microsecond => "microsecond",
        DateTimeField::Nanosecond => "nanosecond",
        DateTimeField::Quarter => "quarter",
        DateTimeField::Week(_) => "week",
        DateTimeField::DayOfWeek => "dow",
        DateTimeField::DayOfYear => "doy",
        DateTimeField::IsoYear => "isoyear",
        DateTimeField::IsoWeek => "isoweek",
        DateTimeField::Date => "date",
        DateTimeField::Time => "time",
        DateTimeField::Datetime => "datetime",
        DateTimeField::Timezone => "timezone",
        DateTimeField::TimezoneHour => "timezone_hour",
        DateTimeField::TimezoneMinute => "timezone_minute",
    }
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
