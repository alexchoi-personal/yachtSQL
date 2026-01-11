#![coverage(off)]

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, TimeUnit};
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::common::{
    Column, DataFusionError, Result as DFResult, ScalarValue, TableReference,
};
use datafusion::logical_expr::expr::{Exists, InList, InSubquery};
use datafusion::logical_expr::{
    BinaryExpr, Case, Cast, Expr as DFExpr, ExprFunctionExt, Like, Operator, Subquery, WindowFrame,
    WindowFrameBound, WindowFrameUnits,
};
use datafusion::prelude::*;
use yachtsql_common::types::DataType;
use yachtsql_ir::{
    AggregateFunction, BinaryOp, DateTimeField, Expr, Literal, ScalarFunction, UnaryOp,
    WindowFunction,
};

use super::plan_converter::convert_plan;

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
            if *op == BinaryOp::Div {
                let left_needs_cast = needs_float_cast_for_division(left);
                let right_needs_cast = needs_float_cast_for_division(right);
                let left_final = if left_needs_cast {
                    DFExpr::Cast(datafusion::logical_expr::Cast::new(
                        Box::new(left_expr),
                        datafusion::arrow::datatypes::DataType::Float64,
                    ))
                } else {
                    left_expr
                };
                let right_final = if right_needs_cast {
                    DFExpr::Cast(datafusion::logical_expr::Cast::new(
                        Box::new(right_expr),
                        datafusion::arrow::datatypes::DataType::Float64,
                    ))
                } else {
                    right_expr
                };
                Ok(DFExpr::BinaryExpr(BinaryExpr::new(
                    Box::new(left_final),
                    Operator::Divide,
                    Box::new(right_final),
                )))
            } else {
                let operator = convert_binary_op(op);
                Ok(DFExpr::BinaryExpr(BinaryExpr::new(
                    Box::new(left_expr),
                    operator,
                    Box::new(right_expr),
                )))
            }
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
            order_by,
            limit,
            ignore_nulls,
        } => {
            let df_args: Vec<DFExpr> = args.iter().map(convert_expr).collect::<DFResult<_>>()?;
            let df_filter = filter
                .as_ref()
                .map(|f| convert_expr(f))
                .transpose()?
                .map(Box::new);
            let df_order_by: Vec<datafusion::logical_expr::expr::Sort> = order_by
                .iter()
                .map(|se| {
                    Ok(datafusion::logical_expr::expr::Sort {
                        expr: convert_expr(&se.expr)?,
                        asc: se.asc,
                        nulls_first: se.nulls_first,
                    })
                })
                .collect::<DFResult<_>>()?;
            convert_aggregate_function(
                func,
                df_args,
                *distinct,
                df_filter,
                df_order_by,
                *limit,
                *ignore_nulls,
            )
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

        Expr::UserDefinedAggregate {
            name,
            args,
            distinct,
            filter,
        } => {
            use datafusion::logical_expr::AggregateUDF;
            use datafusion::logical_expr::expr::AggregateFunction;

            let df_args: Vec<DFExpr> = args.iter().map(convert_expr).collect::<DFResult<_>>()?;
            let df_filter = filter
                .as_ref()
                .map(|f| convert_expr(f))
                .transpose()?
                .map(Box::new);

            let udf = AggregateUDF::new_from_impl(PlaceholderUDAF {
                name: name.to_lowercase(),
            });

            let agg = DFExpr::AggregateFunction(AggregateFunction::new_udf(
                Arc::new(udf),
                df_args,
                *distinct,
                df_filter,
                None,
                None,
            ));

            Ok(agg)
        }

        Expr::Window {
            func,
            args,
            partition_by,
            order_by,
            frame,
        } => {
            let df_args: Vec<DFExpr> = args.iter().map(convert_expr).collect::<DFResult<_>>()?;
            let df_partition_by: Vec<DFExpr> = partition_by
                .iter()
                .map(convert_expr)
                .collect::<DFResult<_>>()?;
            let df_order_by: Vec<datafusion::logical_expr::expr::Sort> = order_by
                .iter()
                .map(|se| {
                    Ok(datafusion::logical_expr::expr::Sort {
                        expr: convert_expr(&se.expr)?,
                        asc: se.asc,
                        nulls_first: se.nulls_first,
                    })
                })
                .collect::<DFResult<_>>()?;

            let window_expr = convert_window_function(func, df_args)?;
            let has_order_by = if df_order_by.is_empty() {
                None
            } else {
                Some(true)
            };
            let df_frame = frame
                .as_ref()
                .map(convert_window_frame)
                .unwrap_or_else(|| Ok(WindowFrame::new(has_order_by)))?;

            let result = window_expr
                .partition_by(df_partition_by)
                .order_by(df_order_by)
                .window_frame(df_frame)
                .build()?;

            Ok(result)
        }

        Expr::AggregateWindow {
            func,
            args,
            distinct: _distinct,
            partition_by,
            order_by,
            frame,
        } => {
            let df_args: Vec<DFExpr> = args.iter().map(convert_expr).collect::<DFResult<_>>()?;
            let df_partition_by: Vec<DFExpr> = partition_by
                .iter()
                .map(convert_expr)
                .collect::<DFResult<_>>()?;
            let df_order_by: Vec<datafusion::logical_expr::expr::Sort> = order_by
                .iter()
                .map(|se| {
                    Ok(datafusion::logical_expr::expr::Sort {
                        expr: convert_expr(&se.expr)?,
                        asc: se.asc,
                        nulls_first: se.nulls_first,
                    })
                })
                .collect::<DFResult<_>>()?;

            let udaf = get_aggregate_udaf(func)?;
            let has_order_by = if df_order_by.is_empty() {
                None
            } else {
                Some(false)
            };
            let df_frame = frame
                .as_ref()
                .map(convert_window_frame)
                .unwrap_or_else(|| Ok(WindowFrame::new(has_order_by)))?;

            let window_fn = datafusion::logical_expr::expr::WindowFunction {
                fun: datafusion::logical_expr::WindowFunctionDefinition::AggregateUDF(udaf),
                args: df_args,
                partition_by: df_partition_by,
                order_by: df_order_by,
                window_frame: df_frame,
                null_treatment: None,
            };

            Ok(DFExpr::WindowFunction(window_fn))
        }

        Expr::Substring {
            expr: inner,
            start,
            length,
        } => {
            let inner_expr = convert_expr(inner)?;
            let start_expr = start
                .as_ref()
                .map(|s| convert_expr(s))
                .transpose()?
                .unwrap_or_else(|| lit(1));
            match length {
                Some(l) => {
                    let len_expr = convert_expr(l)?;
                    Ok(datafusion::functions::unicode::substr()
                        .call(vec![inner_expr, start_expr, len_expr]))
                }
                None => Ok(datafusion::functions::unicode::expr_fn::substr(
                    inner_expr, start_expr,
                )),
            }
        }

        Expr::ArrayAccess { array, index } => {
            let array_expr = convert_expr(array)?;
            let index_expr = match index.as_ref() {
                Expr::ScalarFunction {
                    name: ScalarFunction::ArrayOffset | ScalarFunction::SafeOffset,
                    args,
                } if args.len() == 1 => {
                    let idx = convert_expr(&args[0])?;
                    idx + lit(1)
                }
                Expr::ScalarFunction {
                    name: ScalarFunction::ArrayOrdinal | ScalarFunction::SafeOrdinal,
                    args,
                } if args.len() == 1 => convert_expr(&args[0])?,
                _ => convert_expr(index)?,
            };
            Ok(datafusion::functions_array::extract::array_element(
                array_expr, index_expr,
            ))
        }

        Expr::Struct { fields } => {
            let mut df_fields: Vec<DFExpr> = Vec::with_capacity(fields.len() * 2);
            for (i, (name, value)) in fields.iter().enumerate() {
                let df_value = convert_expr(value)?;
                let field_name = match name {
                    Some(n) => n.clone(),
                    None => format!("f{}", i),
                };
                df_fields.push(lit(field_name));
                df_fields.push(df_value);
            }
            Ok(datafusion::functions::core::named_struct().call(df_fields))
        }

        Expr::StructAccess { expr: inner, field } => {
            let inner_expr = convert_expr(inner)?;
            Ok(datafusion::functions::core::expr_fn::get_field(
                inner_expr,
                field.clone(),
            ))
        }

        Expr::TypedString { data_type, value } => {
            let arrow_type = convert_data_type(data_type);
            let str_lit = lit(value.clone());
            Ok(DFExpr::Cast(Cast::new(Box::new(str_lit), arrow_type)))
        }

        Expr::Interval {
            value,
            leading_field,
        } => {
            let value_expr = convert_expr(value)?;
            let field_name = leading_field
                .as_ref()
                .map(convert_datetime_field)
                .unwrap_or("second");
            Ok(
                datafusion::functions::datetime::make_date()
                    .call(vec![value_expr, lit(field_name)]),
            )
        }

        Expr::Position { substr, string } => {
            let substr_expr = convert_expr(substr)?;
            let string_expr = convert_expr(string)?;
            Ok(datafusion::functions::unicode::expr_fn::strpos(
                string_expr,
                substr_expr,
            ))
        }

        Expr::Trim {
            expr: inner,
            trim_what,
            trim_where,
        } => {
            let inner_expr = convert_expr(inner)?;
            let trim_char = trim_what.as_ref().map(|t| convert_expr(t)).transpose()?;
            let args = match trim_char {
                Some(c) => vec![inner_expr, c],
                None => vec![inner_expr],
            };
            use yachtsql_ir::TrimWhere;
            match trim_where {
                TrimWhere::Both => Ok(datafusion::functions::string::expr_fn::btrim(args)),
                TrimWhere::Leading => Ok(datafusion::functions::string::expr_fn::ltrim(args)),
                TrimWhere::Trailing => Ok(datafusion::functions::string::expr_fn::rtrim(args)),
            }
        }

        Expr::IsDistinctFrom {
            left,
            right,
            negated,
        } => {
            let left_expr = convert_expr(left)?;
            let right_expr = convert_expr(right)?;
            let distinct_cmp = DFExpr::BinaryExpr(BinaryExpr::new(
                Box::new(left_expr.clone()),
                Operator::IsDistinctFrom,
                Box::new(right_expr.clone()),
            ));
            if *negated {
                Ok(DFExpr::Not(Box::new(distinct_cmp)))
            } else {
                Ok(distinct_cmp)
            }
        }

        Expr::Exists { subquery, negated } => {
            let df_plan = convert_plan(subquery)?;
            let subq = Subquery {
                subquery: Arc::new(df_plan),
                outer_ref_columns: vec![],
            };
            Ok(DFExpr::Exists(Exists::new(subq, *negated)))
        }

        Expr::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => {
            let inner_expr = convert_expr(inner)?;
            let df_plan = convert_plan(subquery)?;
            let subq = Subquery {
                subquery: Arc::new(df_plan),
                outer_ref_columns: vec![],
            };
            Ok(DFExpr::InSubquery(InSubquery::new(
                Box::new(inner_expr),
                subq,
                *negated,
            )))
        }

        Expr::InUnnest {
            expr: inner,
            array_expr,
            negated,
        } => {
            let inner_expr = convert_expr(inner)?;
            let array_ex = convert_expr(array_expr)?;
            let has_element = datafusion::functions_array::expr_fn::array_has(array_ex, inner_expr);
            if *negated {
                Ok(DFExpr::Not(Box::new(has_element)))
            } else {
                Ok(has_element)
            }
        }

        Expr::ScalarSubquery(subquery) | Expr::Subquery(subquery) => {
            let df_plan = convert_plan(subquery)?;
            let subq = Subquery {
                subquery: Arc::new(df_plan),
                outer_ref_columns: vec![],
            };
            Ok(DFExpr::ScalarSubquery(subq))
        }

        Expr::ArraySubquery(subquery) => {
            let df_plan = convert_plan(subquery)?;
            let subq = Subquery {
                subquery: Arc::new(df_plan),
                outer_ref_columns: vec![],
            };
            Ok(datafusion::functions_aggregate::array_agg::array_agg(
                DFExpr::ScalarSubquery(subq),
            ))
        }

        _ => Err(datafusion::common::DataFusionError::NotImplemented(
            format!(
                "Expression conversion not implemented for: {:?}",
                std::mem::discriminant(expr)
            ),
        )),
    }
}

#[derive(Debug)]
struct PlaceholderUDAF {
    name: String,
}

impl datafusion::logical_expr::AggregateUDFImpl for PlaceholderUDAF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        static SIG: std::sync::OnceLock<datafusion::logical_expr::Signature> =
            std::sync::OnceLock::new();
        SIG.get_or_init(|| {
            datafusion::logical_expr::Signature::variadic_any(
                datafusion::logical_expr::Volatility::Immutable,
            )
        })
    }

    fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
        Ok(ArrowDataType::Int64)
    }

    fn accumulator(
        &self,
        _acc_args: datafusion::logical_expr::function::AccumulatorArgs,
    ) -> DFResult<Box<dyn datafusion::logical_expr::Accumulator>> {
        Err(datafusion::common::DataFusionError::Internal(
            "PlaceholderUDAF should be replaced by actual UDAF at execution time".to_string(),
        ))
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

fn needs_float_cast_for_division(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(Literal::Int64(_)) => true,
        Expr::BinaryOp { left, right, .. } => {
            needs_float_cast_for_division(left) && needs_float_cast_for_division(right)
        }
        Expr::UnaryOp { expr: inner, .. } => needs_float_cast_for_division(inner),
        _ => false,
    }
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
    use datafusion::functions::datetime::expr_fn as datetime;
    use datafusion::functions::math::expr_fn as math;
    use datafusion::functions::string::expr_fn as string;
    use datafusion::functions::unicode::expr_fn as unicode;

    match name {
        ScalarFunction::Upper => Ok(string::upper(args.into_iter().next().unwrap())),
        ScalarFunction::Lower => Ok(string::lower(args.into_iter().next().unwrap())),
        ScalarFunction::Length | ScalarFunction::CharLength => {
            Ok(unicode::character_length(args.into_iter().next().unwrap()))
        }
        ScalarFunction::ByteLength => Ok(string::octet_length(args.into_iter().next().unwrap())),
        ScalarFunction::Concat => Ok(datafusion::functions::string::concat().call(args)),
        ScalarFunction::Trim => Ok(string::btrim(args)),
        ScalarFunction::LTrim => Ok(string::ltrim(args)),
        ScalarFunction::RTrim => Ok(string::rtrim(args)),
        ScalarFunction::Substr => {
            let mut iter = args.into_iter();
            let s = iter.next().unwrap();
            let pos = iter.next().unwrap();
            match iter.next() {
                Some(len) => Ok(datafusion::functions::unicode::substr().call(vec![s, pos, len])),
                None => Ok(unicode::substr(s, pos)),
            }
        }
        ScalarFunction::Replace => {
            let mut iter = args.into_iter();
            Ok(string::replace(
                iter.next().unwrap(),
                iter.next().unwrap(),
                iter.next().unwrap(),
            ))
        }
        ScalarFunction::Reverse => Ok(unicode::reverse(args.into_iter().next().unwrap())),
        ScalarFunction::Left => {
            let mut iter = args.into_iter();
            Ok(unicode::left(iter.next().unwrap(), iter.next().unwrap()))
        }
        ScalarFunction::Right => {
            let mut iter = args.into_iter();
            Ok(unicode::right(iter.next().unwrap(), iter.next().unwrap()))
        }
        ScalarFunction::Repeat => {
            let mut iter = args.into_iter();
            Ok(string::repeat(iter.next().unwrap(), iter.next().unwrap()))
        }
        ScalarFunction::StartsWith => {
            let mut iter = args.into_iter();
            Ok(string::starts_with(
                iter.next().unwrap(),
                iter.next().unwrap(),
            ))
        }
        ScalarFunction::EndsWith => {
            let mut iter = args.into_iter();
            Ok(string::ends_with(
                iter.next().unwrap(),
                iter.next().unwrap(),
            ))
        }
        ScalarFunction::Strpos | ScalarFunction::Instr => {
            let mut iter = args.into_iter();
            Ok(unicode::strpos(iter.next().unwrap(), iter.next().unwrap()))
        }
        ScalarFunction::Initcap => Ok(unicode::initcap(args.into_iter().next().unwrap())),
        ScalarFunction::Lpad => {
            let mut iter = args.into_iter();
            let s = iter.next().unwrap();
            let len = iter.next().unwrap();
            let fill = iter.next().unwrap_or_else(|| lit(" "));
            Ok(unicode::lpad(vec![s, len, fill]))
        }
        ScalarFunction::Rpad => {
            let mut iter = args.into_iter();
            let s = iter.next().unwrap();
            let len = iter.next().unwrap();
            let fill = iter.next().unwrap_or_else(|| lit(" "));
            Ok(unicode::rpad(vec![s, len, fill]))
        }
        ScalarFunction::Ascii => Ok(string::ascii(args.into_iter().next().unwrap())),
        ScalarFunction::Chr => Ok(string::chr(args.into_iter().next().unwrap())),

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
        ScalarFunction::Trunc => {
            let mut iter = args.into_iter();
            let value = iter.next().unwrap();
            match iter.next() {
                Some(places) => Ok(math::trunc(vec![value, places])),
                None => Ok(math::trunc(vec![value, lit(0)])),
            }
        }
        ScalarFunction::Sqrt => Ok(math::sqrt(args.into_iter().next().unwrap())),
        ScalarFunction::Cbrt => Ok(math::cbrt(args.into_iter().next().unwrap())),
        ScalarFunction::Power | ScalarFunction::Pow => {
            let mut iter = args.into_iter();
            Ok(math::power(iter.next().unwrap(), iter.next().unwrap()))
        }
        ScalarFunction::Mod => {
            let mut iter = args.into_iter();
            Ok(iter.next().unwrap() % iter.next().unwrap())
        }
        ScalarFunction::Sign => Ok(math::signum(args.into_iter().next().unwrap())),
        ScalarFunction::Exp => Ok(math::exp(args.into_iter().next().unwrap())),
        ScalarFunction::Ln => Ok(math::ln(args.into_iter().next().unwrap())),
        ScalarFunction::Log => {
            let mut iter = args.into_iter();
            let arg1 = iter.next().unwrap();
            match iter.next() {
                Some(arg2) => Ok(math::log(arg1, arg2)),
                None => Ok(math::ln(arg1)),
            }
        }
        ScalarFunction::Log10 => Ok(math::log10(args.into_iter().next().unwrap())),
        ScalarFunction::Sin => Ok(math::sin(args.into_iter().next().unwrap())),
        ScalarFunction::Cos => Ok(math::cos(args.into_iter().next().unwrap())),
        ScalarFunction::Tan => Ok(math::tan(args.into_iter().next().unwrap())),
        ScalarFunction::Asin => Ok(math::asin(args.into_iter().next().unwrap())),
        ScalarFunction::Acos => Ok(math::acos(args.into_iter().next().unwrap())),
        ScalarFunction::Atan => Ok(math::atan(args.into_iter().next().unwrap())),
        ScalarFunction::Atan2 => {
            let mut iter = args.into_iter();
            Ok(math::atan2(iter.next().unwrap(), iter.next().unwrap()))
        }
        ScalarFunction::Sinh => Ok(math::sinh(args.into_iter().next().unwrap())),
        ScalarFunction::Cosh => Ok(math::cosh(args.into_iter().next().unwrap())),
        ScalarFunction::Tanh => Ok(math::tanh(args.into_iter().next().unwrap())),
        ScalarFunction::Asinh => Ok(math::asinh(args.into_iter().next().unwrap())),
        ScalarFunction::Acosh => Ok(math::acosh(args.into_iter().next().unwrap())),
        ScalarFunction::Atanh => Ok(math::atanh(args.into_iter().next().unwrap())),
        ScalarFunction::Cot => Ok(math::cot(args.into_iter().next().unwrap())),
        ScalarFunction::Csc => {
            let arg = args.into_iter().next().unwrap();
            Ok(lit(1.0) / math::sin(arg))
        }
        ScalarFunction::Sec => {
            let arg = args.into_iter().next().unwrap();
            Ok(lit(1.0) / math::cos(arg))
        }
        ScalarFunction::Coth => {
            let arg = args.into_iter().next().unwrap();
            Ok(lit(1.0) / math::tanh(arg))
        }
        ScalarFunction::Csch => {
            let arg = args.into_iter().next().unwrap();
            Ok(lit(1.0) / math::sinh(arg))
        }
        ScalarFunction::Sech => {
            let arg = args.into_iter().next().unwrap();
            Ok(lit(1.0) / math::cosh(arg))
        }
        ScalarFunction::Pi => Ok(math::pi()),
        ScalarFunction::IsNan => Ok(math::isnan(args.into_iter().next().unwrap())),
        ScalarFunction::IsInf => Ok(math::iszero(args.into_iter().next().unwrap()).not()),
        ScalarFunction::Rand | ScalarFunction::RandCanonical => Ok(math::random()),
        ScalarFunction::Div => {
            let mut iter = args.into_iter();
            let a = iter.next().unwrap();
            let b = iter.next().unwrap();
            Ok(math::trunc(vec![a / b, lit(0)]))
        }
        ScalarFunction::SafeDivide => {
            let mut iter = args.into_iter();
            let a = iter.next().unwrap();
            let b = iter.next().unwrap();
            Ok(when(b.clone().eq(lit(0)), lit(ScalarValue::Null)).otherwise(a / b)?)
        }
        ScalarFunction::IeeeDivide => {
            let mut iter = args.into_iter();
            Ok(iter.next().unwrap() / iter.next().unwrap())
        }

        ScalarFunction::Coalesce => Ok(core::coalesce(args)),
        ScalarFunction::NullIf => {
            let mut iter = args.into_iter();
            Ok(core::nullif(iter.next().unwrap(), iter.next().unwrap()))
        }
        ScalarFunction::Greatest => Ok(core::greatest(args)),
        ScalarFunction::Least => Ok(core::least(args)),
        ScalarFunction::IfNull | ScalarFunction::Ifnull | ScalarFunction::Nvl => {
            let mut iter = args.into_iter();
            let expr = iter.next().unwrap();
            let default = iter.next().unwrap();
            Ok(core::coalesce(vec![expr, default]))
        }
        ScalarFunction::If => {
            let mut iter = args.into_iter();
            let condition = iter.next().unwrap();
            let then_val = iter.next().unwrap();
            let else_val = iter.next().unwrap_or_else(|| lit(ScalarValue::Null));
            Ok(when(condition, then_val).otherwise(else_val)?)
        }
        ScalarFunction::Nvl2 => {
            let mut iter = args.into_iter();
            let expr = iter.next().unwrap();
            let not_null_val = iter.next().unwrap();
            let null_val = iter.next().unwrap();
            Ok(when(expr.clone().is_not_null(), not_null_val).otherwise(null_val)?)
        }
        ScalarFunction::Zeroifnull => {
            let expr = args.into_iter().next().unwrap();
            Ok(core::coalesce(vec![expr, lit(0)]))
        }

        ScalarFunction::CurrentDate => Ok(datetime::current_date()),
        ScalarFunction::CurrentTimestamp => Ok(datetime::now()),
        ScalarFunction::CurrentTime => Ok(datetime::current_time()),

        ScalarFunction::Md5 => Ok(datafusion::functions::crypto::md5().call(args)),
        ScalarFunction::Sha256 => Ok(datafusion::functions::crypto::sha256().call(args)),
        ScalarFunction::Sha512 => Ok(datafusion::functions::crypto::sha512().call(args)),

        ScalarFunction::ToBase64 => Ok(datafusion::functions::encoding::expr_fn::encode(
            args.into_iter().next().unwrap(),
            lit("base64"),
        )),
        ScalarFunction::FromBase64 => Ok(datafusion::functions::encoding::expr_fn::decode(
            args.into_iter().next().unwrap(),
            lit("base64"),
        )),
        ScalarFunction::ToHex => Ok(datafusion::functions::encoding::expr_fn::encode(
            args.into_iter().next().unwrap(),
            lit("hex"),
        )),
        ScalarFunction::FromHex => Ok(datafusion::functions::encoding::expr_fn::decode(
            args.into_iter().next().unwrap(),
            lit("hex"),
        )),

        ScalarFunction::GenerateUuid => Ok(datafusion::functions::string::uuid().call(vec![])),

        ScalarFunction::ArrayLength => Ok(datafusion::functions_array::expr_fn::array_length(
            args.into_iter().next().unwrap(),
        )),
        ScalarFunction::ArrayConcat => Ok(datafusion::functions_array::expr_fn::array_concat(args)),
        ScalarFunction::ArrayReverse => Ok(datafusion::functions_array::expr_fn::array_reverse(
            args.into_iter().next().unwrap(),
        )),
        ScalarFunction::ArrayToString => {
            let mut iter = args.into_iter();
            let array = iter.next().unwrap();
            let delimiter = iter.next().unwrap_or_else(|| lit(","));
            Ok(datafusion::functions_array::expr_fn::array_to_string(
                array, delimiter,
            ))
        }
        ScalarFunction::ArrayContains => {
            let mut iter = args.into_iter();
            let array = iter.next().unwrap();
            let element = iter.next().unwrap();
            Ok(datafusion::functions_array::expr_fn::array_has(
                array, element,
            ))
        }
        ScalarFunction::ArrayFirst => {
            let array = args.into_iter().next().unwrap();
            Ok(datafusion::functions_array::extract::array_element(
                array,
                lit(1),
            ))
        }
        ScalarFunction::ArrayLast => {
            let array = args.into_iter().next().unwrap();
            let len = datafusion::functions_array::expr_fn::array_length(array.clone());
            Ok(datafusion::functions_array::extract::array_element(
                array, len,
            ))
        }
        ScalarFunction::ArrayMin => {
            let array = args.into_iter().next().unwrap();
            Ok(
                datafusion::functions_aggregate::min_max::min_udaf().call(vec![
                    datafusion::functions_array::expr_fn::array_to_string(array, lit(",")),
                ]),
            )
        }
        ScalarFunction::ArrayMax => {
            let array = args.into_iter().next().unwrap();
            Ok(
                datafusion::functions_aggregate::min_max::max_udaf().call(vec![
                    datafusion::functions_array::expr_fn::array_to_string(array, lit(",")),
                ]),
            )
        }
        ScalarFunction::ArraySum => {
            let array = args.into_iter().next().unwrap();
            Ok(datafusion::functions_aggregate::sum::sum_udaf().call(vec![
                datafusion::functions_array::expr_fn::array_to_string(array, lit(",")),
            ]))
        }
        ScalarFunction::ArrayAvg => {
            let array = args.into_iter().next().unwrap();
            Ok(
                datafusion::functions_aggregate::average::avg_udaf().call(vec![
                    datafusion::functions_array::expr_fn::array_to_string(array, lit(",")),
                ]),
            )
        }
        ScalarFunction::ArraySlice => {
            let mut iter = args.into_iter();
            let array = iter.next().unwrap();
            let start = iter.next().unwrap();
            let end = iter.next().unwrap();
            Ok(datafusion::functions_array::extract::array_slice(
                array, start, end, None,
            ))
        }
        ScalarFunction::ArrayFlatten => Ok(datafusion::functions_array::expr_fn::flatten(
            args.into_iter().next().unwrap(),
        )),
        ScalarFunction::ArrayDistinct => Ok(datafusion::functions_array::expr_fn::array_distinct(
            args.into_iter().next().unwrap(),
        )),
        ScalarFunction::ArrayPosition => {
            let mut iter = args.into_iter();
            let array = iter.next().unwrap();
            let element = iter.next().unwrap();
            let start = iter.next();
            match start {
                Some(s) => Ok(datafusion::functions_array::expr_fn::array_position(
                    array, element, s,
                )),
                None => Ok(datafusion::functions_array::expr_fn::array_position(
                    array,
                    element,
                    lit(1),
                )),
            }
        }
        ScalarFunction::ArrayCompact => {
            let array = args.into_iter().next().unwrap();
            Ok(datafusion::functions_array::expr_fn::array_remove_all(
                array,
                lit(ScalarValue::Null),
            ))
        }
        ScalarFunction::ArraySort => {
            let mut iter = args.into_iter();
            let array = iter.next().unwrap();
            let desc = iter.next().unwrap_or_else(|| lit(false));
            let nulls_first = iter.next().unwrap_or_else(|| lit(false));
            Ok(datafusion::functions_array::expr_fn::array_sort(
                array,
                desc,
                nulls_first,
            ))
        }
        ScalarFunction::ArrayZip => Ok(datafusion::functions_array::expr_fn::array_concat(args)),

        ScalarFunction::Format => {
            if args.is_empty() {
                Ok(lit(""))
            } else {
                Ok(string::concat(args))
            }
        }

        ScalarFunction::DateTrunc => {
            let mut iter = args.into_iter();
            let date = iter.next().unwrap();
            let part = iter.next().unwrap_or_else(|| lit("day"));
            let truncated = datetime::date_trunc(part, date);
            Ok(DFExpr::Cast(Cast::new(
                Box::new(truncated),
                ArrowDataType::Date32,
            )))
        }
        ScalarFunction::DatetimeTrunc | ScalarFunction::TimestampTrunc => {
            let mut iter = args.into_iter();
            let ts = iter.next().unwrap();
            let part = iter.next().unwrap_or_else(|| lit("day"));
            Ok(datetime::date_trunc(part, ts))
        }
        ScalarFunction::TimeTrunc => {
            let mut iter = args.into_iter();
            let time = iter.next().unwrap();
            let _part = iter.next().unwrap_or_else(|| lit("second"));
            Ok(time)
        }

        ScalarFunction::TypeOf => Ok(datafusion::functions::core::arrow_typeof().call(args)),

        ScalarFunction::SafeConvertBytesToString => {
            let arg = args.into_iter().next().unwrap();
            Ok(DFExpr::Cast(Cast::new(Box::new(arg), ArrowDataType::Utf8)))
        }

        ScalarFunction::Range => {
            let mut iter = args.into_iter();
            let start = iter.next().unwrap();
            let stop = iter.next().unwrap();
            let step = iter.next().unwrap_or_else(|| lit(1));
            Ok(datafusion::functions_array::expr_fn::range(
                start, stop, step,
            ))
        }

        ScalarFunction::GenerateArray => {
            let mut iter = args.into_iter();
            let start = iter.next().unwrap();
            let stop = iter.next().unwrap();
            let step = iter.next().unwrap_or_else(|| lit(1));
            Ok(datafusion::functions_array::expr_fn::range(
                start, stop, step,
            ))
        }

        ScalarFunction::MakeInterval => {
            let years = args.first().cloned().unwrap_or_else(|| lit(0));
            let months = args.get(1).cloned().unwrap_or_else(|| lit(0));
            let days = args.get(2).cloned().unwrap_or_else(|| lit(0));
            Ok(datetime::make_date(years, months, days))
        }

        ScalarFunction::JsonValue | ScalarFunction::JsonExtractScalar => {
            let mut iter = args.into_iter();
            let json_expr = iter.next().unwrap();
            let path_expr = iter.next().unwrap_or_else(|| lit("$"));
            Ok(json_value_udf().call(vec![json_expr, path_expr]))
        }

        ScalarFunction::JsonExtract => {
            let mut iter = args.into_iter();
            let json_expr = iter.next().unwrap();
            let path_expr = iter.next().unwrap_or_else(|| lit("$"));
            Ok(json_extract_udf().call(vec![json_expr, path_expr]))
        }

        ScalarFunction::JsonQuery => {
            let mut iter = args.into_iter();
            let json_expr = iter.next().unwrap();
            let path_expr = iter.next().unwrap_or_else(|| lit("$"));
            Ok(json_query_udf().call(vec![json_expr, path_expr]))
        }

        ScalarFunction::JsonExtractArray | ScalarFunction::JsonQueryArray => {
            let mut iter = args.into_iter();
            let json_expr = iter.next().unwrap();
            let path_expr = iter.next().unwrap_or_else(|| lit("$"));
            Ok(json_extract_array_udf().call(vec![json_expr, path_expr]))
        }

        ScalarFunction::JsonExtractStringArray | ScalarFunction::JsonValueArray => {
            let mut iter = args.into_iter();
            let json_expr = iter.next().unwrap();
            let path_expr = iter.next().unwrap_or_else(|| lit("$"));
            Ok(json_extract_string_array_udf().call(vec![json_expr, path_expr]))
        }

        ScalarFunction::ToJson => {
            let arg = args.into_iter().next().unwrap();
            Ok(to_json_udf().call(vec![arg]))
        }

        ScalarFunction::ToJsonString => {
            let arg = args.into_iter().next().unwrap();
            Ok(to_json_string_udf().call(vec![arg]))
        }

        ScalarFunction::ParseJson => {
            let arg = args.into_iter().next().unwrap();
            Ok(parse_json_udf().call(vec![arg]))
        }

        ScalarFunction::JsonType => {
            let arg = args.into_iter().next().unwrap();
            Ok(json_type_udf().call(vec![arg]))
        }

        ScalarFunction::JsonKeys => {
            let arg = args.into_iter().next().unwrap();
            Ok(json_keys_udf().call(vec![arg]))
        }

        ScalarFunction::JsonArrayLength => {
            let mut iter = args.into_iter();
            let json_expr = iter.next().unwrap();
            let path_expr = iter.next();
            match path_expr {
                Some(p) => Ok(json_array_length_udf().call(vec![json_expr, p])),
                None => Ok(json_array_length_udf().call(vec![json_expr, lit("$")])),
            }
        }

        ScalarFunction::String => {
            let arg = args.into_iter().next().unwrap();
            Ok(DFExpr::Cast(Cast::new(Box::new(arg), ArrowDataType::Utf8)))
        }

        ScalarFunction::ArrayOffset | ScalarFunction::SafeOffset => {
            let mut iter = args.into_iter();
            let array = iter
                .next()
                .ok_or_else(|| DataFusionError::Plan("OFFSET requires an array argument".into()))?;
            let idx = iter
                .next()
                .ok_or_else(|| DataFusionError::Plan("OFFSET requires an index argument".into()))?;
            Ok(datafusion::functions_array::expr_fn::array_element(
                array, idx,
            ))
        }

        ScalarFunction::Split => {
            let mut iter = args.into_iter();
            let s = iter.next().unwrap();
            let delimiter = iter.next().unwrap_or_else(|| lit(","));
            Ok(datafusion::functions_nested::string::string_to_array(
                s,
                delimiter,
                DFExpr::Literal(ScalarValue::Null),
            ))
        }

        ScalarFunction::DateDiff => {
            let mut iter = args.into_iter();
            let date1 = iter.next().unwrap();
            let date2 = iter.next().unwrap();
            let part = iter.next().unwrap_or_else(|| lit("DAY"));
            Ok(date_diff_udf().call(vec![date1, date2, part]))
        }

        ScalarFunction::RegexpContains => {
            let mut iter = args.into_iter();
            let s = iter.next().unwrap();
            let pattern = iter.next().unwrap();
            Ok(s.like(pattern))
        }

        ScalarFunction::RegexpExtract => {
            let mut iter = args.into_iter();
            let s = iter.next().unwrap();
            let pattern = iter.next().unwrap();
            Ok(datafusion::functions::regex::expr_fn::regexp_match(
                s, pattern, None,
            ))
        }

        ScalarFunction::RegexpReplace => {
            let mut iter = args.into_iter();
            let s = iter.next().unwrap();
            let pattern = iter.next().unwrap();
            let replacement = iter.next().unwrap_or_else(|| lit(""));
            Ok(datafusion::functions::regex::expr_fn::regexp_replace(
                s,
                pattern,
                replacement,
                Some(lit("g")),
            ))
        }

        ScalarFunction::Normalize => {
            let arg = args.into_iter().next().unwrap();
            Ok(arg)
        }

        ScalarFunction::Int64FromJson => {
            let arg = args.into_iter().next().unwrap();
            Ok(int64_from_json_udf().call(vec![arg]))
        }

        ScalarFunction::Float64FromJson => {
            let arg = args.into_iter().next().unwrap();
            Ok(float64_from_json_udf().call(vec![arg]))
        }

        ScalarFunction::BoolFromJson => {
            let arg = args.into_iter().next().unwrap();
            Ok(bool_from_json_udf().call(vec![arg]))
        }

        ScalarFunction::StringFromJson => {
            let arg = args.into_iter().next().unwrap();
            Ok(string_from_json_udf().call(vec![arg]))
        }

        ScalarFunction::CurrentDatetime => Ok(DFExpr::Cast(Cast::new(
            Box::new(datetime::now()),
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
        ))),

        ScalarFunction::DateAdd => {
            let mut iter = args.into_iter();
            let date = iter.next().unwrap();
            let interval = iter.next().unwrap();
            Ok(date + interval)
        }

        ScalarFunction::DateSub => {
            let mut iter = args.into_iter();
            let date = iter.next().unwrap();
            let interval = iter.next().unwrap();
            Ok(date - interval)
        }

        ScalarFunction::FormatDate
        | ScalarFunction::FormatTimestamp
        | ScalarFunction::FormatDatetime
        | ScalarFunction::FormatTime => {
            let mut iter = args.into_iter();
            let format_str = iter.next().unwrap();
            let date_expr = iter.next().unwrap();
            Ok(datafusion::functions::datetime::to_char().call(vec![date_expr, format_str]))
        }

        ScalarFunction::ParseDate => {
            let mut iter = args.into_iter();
            let format_str = iter.next().unwrap();
            let date_str = iter.next().unwrap();
            Ok(datafusion::functions::datetime::to_date().call(vec![date_str, format_str]))
        }

        ScalarFunction::ParseTimestamp => {
            let mut iter = args.into_iter();
            let format_str = iter.next().unwrap();
            let ts_str = iter.next().unwrap();
            Ok(datafusion::functions::datetime::to_timestamp().call(vec![ts_str, format_str]))
        }

        ScalarFunction::ParseDatetime => {
            let mut iter = args.into_iter();
            let format_str = iter.next().unwrap();
            let dt_str = iter.next().unwrap();
            let ts = datafusion::functions::datetime::to_timestamp().call(vec![dt_str, format_str]);
            Ok(DFExpr::Cast(Cast::new(
                Box::new(ts),
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            )))
        }

        ScalarFunction::ParseTime => {
            let mut iter = args.into_iter();
            let _format_str = iter.next().unwrap();
            let time_str = iter.next().unwrap();
            Ok(DFExpr::Cast(Cast::new(
                Box::new(time_str),
                ArrowDataType::Time64(TimeUnit::Nanosecond),
            )))
        }

        ScalarFunction::Date => {
            let mut iter = args.into_iter();
            let arg1 = iter.next().unwrap();
            match iter.next() {
                Some(arg2) => match iter.next() {
                    Some(arg3) => Ok(datetime::make_date(arg1, arg2, arg3)),
                    None => Ok(DFExpr::Cast(Cast::new(
                        Box::new(arg1),
                        ArrowDataType::Date32,
                    ))),
                },
                None => Ok(DFExpr::Cast(Cast::new(
                    Box::new(arg1),
                    ArrowDataType::Date32,
                ))),
            }
        }

        ScalarFunction::Time => {
            let mut iter = args.into_iter();
            let arg1 = iter.next().unwrap();
            match iter.next() {
                Some(arg2) => match iter.next() {
                    Some(arg3) => {
                        let hours_nanos =
                            DFExpr::Cast(Cast::new(Box::new(arg1), ArrowDataType::Int64))
                                * lit(3_600_000_000_000i64);
                        let mins_nanos =
                            DFExpr::Cast(Cast::new(Box::new(arg2), ArrowDataType::Int64))
                                * lit(60_000_000_000i64);
                        let secs_nanos =
                            DFExpr::Cast(Cast::new(Box::new(arg3), ArrowDataType::Int64))
                                * lit(1_000_000_000i64);
                        let total_nanos = hours_nanos + mins_nanos + secs_nanos;
                        Ok(DFExpr::Cast(Cast::new(
                            Box::new(total_nanos),
                            ArrowDataType::Time64(TimeUnit::Nanosecond),
                        )))
                    }
                    None => Ok(DFExpr::Cast(Cast::new(
                        Box::new(arg1),
                        ArrowDataType::Time64(TimeUnit::Nanosecond),
                    ))),
                },
                None => Ok(DFExpr::Cast(Cast::new(
                    Box::new(arg1),
                    ArrowDataType::Time64(TimeUnit::Nanosecond),
                ))),
            }
        }

        ScalarFunction::Datetime => {
            let mut iter = args.into_iter();
            let arg1 = iter.next().unwrap();
            match iter.next() {
                Some(arg2) => match iter.next() {
                    Some(arg3) => match iter.next() {
                        Some(arg4) => match iter.next() {
                            Some(arg5) => match iter.next() {
                                Some(arg6) => {
                                    let date = datetime::make_date(arg1, arg2, arg3);
                                    let hours_nanos = DFExpr::Cast(Cast::new(
                                        Box::new(arg4),
                                        ArrowDataType::Int64,
                                    )) * lit(3_600_000_000_000i64);
                                    let mins_nanos = DFExpr::Cast(Cast::new(
                                        Box::new(arg5),
                                        ArrowDataType::Int64,
                                    )) * lit(60_000_000_000i64);
                                    let secs_nanos = DFExpr::Cast(Cast::new(
                                        Box::new(arg6),
                                        ArrowDataType::Int64,
                                    )) * lit(1_000_000_000i64);
                                    let time_nanos = hours_nanos + mins_nanos + secs_nanos;
                                    let date_ts = DFExpr::Cast(Cast::new(
                                        Box::new(date),
                                        ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                                    ));
                                    let time_interval =
                                        DFExpr::Literal(ScalarValue::IntervalMonthDayNano(Some(
                                            datafusion::arrow::datatypes::IntervalMonthDayNano::new(
                                                0, 0, 0,
                                            ),
                                        )));
                                    Ok(date_ts
                                        + time_interval
                                        + DFExpr::Cast(Cast::new(
                                            Box::new(time_nanos),
                                            ArrowDataType::Duration(TimeUnit::Nanosecond),
                                        )))
                                }
                                None => Ok(DFExpr::Cast(Cast::new(
                                    Box::new(arg1),
                                    ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                                ))),
                            },
                            None => Ok(DFExpr::Cast(Cast::new(
                                Box::new(arg1),
                                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                            ))),
                        },
                        None => {
                            let date_ts = DFExpr::Cast(Cast::new(
                                Box::new(arg1),
                                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                            ));
                            let time_ts = DFExpr::Cast(Cast::new(
                                Box::new(arg2),
                                ArrowDataType::Duration(TimeUnit::Nanosecond),
                            ));
                            Ok(date_ts + time_ts)
                        }
                    },
                    None => Ok(DFExpr::Cast(Cast::new(
                        Box::new(arg1),
                        ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                    ))),
                },
                None => Ok(DFExpr::Cast(Cast::new(
                    Box::new(arg1),
                    ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                ))),
            }
        }

        ScalarFunction::Timestamp => {
            let mut iter = args.into_iter();
            let arg = iter.next().unwrap();
            Ok(DFExpr::Cast(Cast::new(
                Box::new(arg),
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            )))
        }

        ScalarFunction::TimestampMicros => {
            let arg = args.into_iter().next().unwrap();
            Ok(DFExpr::Cast(Cast::new(
                Box::new(arg * lit(1000i64)),
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            )))
        }

        ScalarFunction::TimestampMillis => {
            let arg = args.into_iter().next().unwrap();
            Ok(DFExpr::Cast(Cast::new(
                Box::new(arg * lit(1_000_000i64)),
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            )))
        }

        ScalarFunction::TimestampSeconds => {
            let arg = args.into_iter().next().unwrap();
            Ok(DFExpr::Cast(Cast::new(
                Box::new(arg * lit(1_000_000_000i64)),
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            )))
        }

        ScalarFunction::UnixDate => {
            let arg = args.into_iter().next().unwrap();
            Ok(DFExpr::Cast(Cast::new(Box::new(arg), ArrowDataType::Int64)))
        }

        ScalarFunction::UnixMicros => {
            let arg = args.into_iter().next().unwrap();
            let ts_nanos = DFExpr::Cast(Cast::new(Box::new(arg), ArrowDataType::Int64));
            Ok(ts_nanos / lit(1000i64))
        }

        ScalarFunction::UnixMillis => {
            let arg = args.into_iter().next().unwrap();
            let ts_nanos = DFExpr::Cast(Cast::new(Box::new(arg), ArrowDataType::Int64));
            Ok(ts_nanos / lit(1_000_000i64))
        }

        ScalarFunction::UnixSeconds => {
            let arg = args.into_iter().next().unwrap();
            let ts_nanos = DFExpr::Cast(Cast::new(Box::new(arg), ArrowDataType::Int64));
            Ok(ts_nanos / lit(1_000_000_000i64))
        }

        ScalarFunction::DateFromUnixDate => {
            let arg = args.into_iter().next().unwrap();
            Ok(DFExpr::Cast(Cast::new(
                Box::new(arg),
                ArrowDataType::Date32,
            )))
        }

        ScalarFunction::LastDay => {
            let arg = args.into_iter().next().unwrap();
            let month_truncated = datetime::date_trunc(lit("month"), arg.clone());
            let next_month = month_truncated
                + DFExpr::Literal(ScalarValue::IntervalMonthDayNano(Some(
                    datafusion::arrow::datatypes::IntervalMonthDayNano::new(1, 0, 0),
                )));
            let last_day = next_month
                - DFExpr::Literal(ScalarValue::IntervalMonthDayNano(Some(
                    datafusion::arrow::datatypes::IntervalMonthDayNano::new(0, 1, 0),
                )));
            Ok(DFExpr::Cast(Cast::new(
                Box::new(last_day),
                ArrowDataType::Date32,
            )))
        }

        ScalarFunction::DateBucket => {
            let mut iter = args.into_iter();
            let date = iter.next().unwrap();
            let part = iter.next().unwrap_or_else(|| lit("day"));
            Ok(datetime::date_trunc(part, date))
        }

        ScalarFunction::DatetimeBucket => {
            let mut iter = args.into_iter();
            let dt = iter.next().unwrap();
            let part = iter.next().unwrap_or_else(|| lit("day"));
            Ok(datetime::date_trunc(part, dt))
        }

        ScalarFunction::TimestampBucket => {
            let mut iter = args.into_iter();
            let ts = iter.next().unwrap();
            let part = iter.next().unwrap_or_else(|| lit("day"));
            Ok(datetime::date_trunc(part, ts))
        }

        ScalarFunction::GenerateDateArray => {
            let mut iter = args.into_iter();
            let start = iter.next().unwrap();
            let stop = iter.next().unwrap();
            let _step = iter.next();
            Ok(datafusion::functions_array::expr_fn::range(
                DFExpr::Cast(Cast::new(Box::new(start), ArrowDataType::Int64)),
                DFExpr::Cast(Cast::new(Box::new(stop), ArrowDataType::Int64)) + lit(1i64),
                lit(1i64),
            ))
        }

        ScalarFunction::GenerateTimestampArray => {
            let mut iter = args.into_iter();
            let start = iter.next().unwrap();
            let stop = iter.next().unwrap();
            let _interval = iter.next();
            Ok(datafusion::functions_array::expr_fn::range(
                DFExpr::Cast(Cast::new(Box::new(start), ArrowDataType::Int64)),
                DFExpr::Cast(Cast::new(Box::new(stop), ArrowDataType::Int64)) + lit(1i64),
                lit(86_400_000_000_000i64),
            ))
        }

        ScalarFunction::JustifyDays => {
            let arg = args.into_iter().next().unwrap();
            Ok(arg)
        }

        ScalarFunction::JustifyHours => {
            let arg = args.into_iter().next().unwrap();
            Ok(arg)
        }

        ScalarFunction::JustifyInterval => {
            let arg = args.into_iter().next().unwrap();
            Ok(arg)
        }

        ScalarFunction::LaxBool => {
            let arg = args.into_iter().next().unwrap();
            Ok(lax_bool_udf().call(vec![arg]))
        }

        ScalarFunction::LaxInt64 => {
            let arg = args.into_iter().next().unwrap();
            Ok(lax_int64_udf().call(vec![arg]))
        }

        ScalarFunction::LaxFloat64 => {
            let arg = args.into_iter().next().unwrap();
            Ok(lax_float64_udf().call(vec![arg]))
        }

        ScalarFunction::LaxString => {
            let arg = args.into_iter().next().unwrap();
            Ok(lax_string_udf().call(vec![arg]))
        }

        ScalarFunction::SafeAdd => {
            let mut iter = args.into_iter();
            let left = iter.next().unwrap();
            let right = iter.next().unwrap();
            Ok(left + right)
        }

        ScalarFunction::SafeSubtract => {
            let mut iter = args.into_iter();
            let left = iter.next().unwrap();
            let right = iter.next().unwrap();
            Ok(left - right)
        }

        ScalarFunction::SafeMultiply => {
            let mut iter = args.into_iter();
            let left = iter.next().unwrap();
            let right = iter.next().unwrap();
            Ok(left * right)
        }

        ScalarFunction::SafeNegate => {
            let arg = args.into_iter().next().unwrap();
            Ok(DFExpr::Negative(Box::new(arg)))
        }

        ScalarFunction::Map => {
            let keys: Vec<DFExpr> = args.iter().step_by(2).cloned().collect();
            let values: Vec<DFExpr> = args.iter().skip(1).step_by(2).cloned().collect();
            Ok(datafusion::functions_nested::map::map(keys, values))
        }

        ScalarFunction::MapKeys => {
            let arg = args.into_iter().next().unwrap();
            Ok(datafusion::functions_nested::map_keys::map_keys(arg))
        }

        ScalarFunction::MapValues => {
            let arg = args.into_iter().next().unwrap();
            Ok(datafusion::functions_nested::map_values::map_values(arg))
        }

        ScalarFunction::Custom(func_name) => Err(datafusion::common::DataFusionError::Plan(
            format!("Function not found: {}", func_name),
        )),

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
    order_by: Vec<datafusion::logical_expr::expr::Sort>,
    _limit: Option<usize>,
    ignore_nulls: bool,
) -> DFResult<DFExpr> {
    use datafusion::functions_aggregate::*;

    let mut agg_expr = match func {
        AggregateFunction::Count => count::count(args.into_iter().next().unwrap_or(lit(1))),
        AggregateFunction::Sum => sum::sum(args.into_iter().next().unwrap()),
        AggregateFunction::Avg => average::avg(args.into_iter().next().unwrap()),
        AggregateFunction::Min => min_max::min(args.into_iter().next().unwrap()),
        AggregateFunction::Max => min_max::max(args.into_iter().next().unwrap()),
        AggregateFunction::ArrayAgg => array_agg::array_agg(args.into_iter().next().unwrap()),
        AggregateFunction::StringAgg => {
            let mut iter = args.into_iter();
            let expr = iter.next().unwrap();
            let separator = iter.next().unwrap_or_else(|| lit(","));
            string_agg::string_agg(expr, separator)
        }
        AggregateFunction::Variance | AggregateFunction::VarSamp => {
            variance::var_sample(args.into_iter().next().unwrap())
        }
        AggregateFunction::VarPop => variance::var_pop(args.into_iter().next().unwrap()),
        AggregateFunction::Stddev | AggregateFunction::StddevSamp => {
            stddev::stddev(args.into_iter().next().unwrap())
        }
        AggregateFunction::StddevPop => stddev::stddev_pop(args.into_iter().next().unwrap()),
        AggregateFunction::Corr => {
            let mut iter = args.into_iter();
            correlation::corr(iter.next().unwrap(), iter.next().unwrap())
        }
        AggregateFunction::CovarPop => {
            let mut iter = args.into_iter();
            covariance::covar_pop(iter.next().unwrap(), iter.next().unwrap())
        }
        AggregateFunction::CovarSamp => {
            let mut iter = args.into_iter();
            covariance::covar_samp(iter.next().unwrap(), iter.next().unwrap())
        }
        AggregateFunction::ApproxCountDistinct => {
            approx_distinct::approx_distinct(args.into_iter().next().unwrap())
        }
        AggregateFunction::BitAnd => bit_and_or_xor::bit_and(args.into_iter().next().unwrap()),
        AggregateFunction::BitOr => bit_and_or_xor::bit_or(args.into_iter().next().unwrap()),
        AggregateFunction::BitXor => bit_and_or_xor::bit_xor(args.into_iter().next().unwrap()),
        AggregateFunction::LogicalAnd => bool_and_or::bool_and(args.into_iter().next().unwrap()),
        AggregateFunction::LogicalOr => bool_and_or::bool_or(args.into_iter().next().unwrap()),
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

    if !order_by.is_empty() {
        agg_expr = agg_expr.order_by(order_by).build()?;
    }

    if ignore_nulls {
        agg_expr = agg_expr
            .null_treatment(datafusion::logical_expr::sqlparser::ast::NullTreatment::IgnoreNulls)
            .build()?;
    }

    Ok(agg_expr)
}

fn get_aggregate_udaf(
    func: &AggregateFunction,
) -> DFResult<Arc<datafusion::logical_expr::AggregateUDF>> {
    use datafusion::functions_aggregate::*;

    match func {
        AggregateFunction::Count => Ok(count::count_udaf()),
        AggregateFunction::Sum => Ok(sum::sum_udaf()),
        AggregateFunction::Avg => Ok(average::avg_udaf()),
        AggregateFunction::Min => Ok(min_max::min_udaf()),
        AggregateFunction::Max => Ok(min_max::max_udaf()),
        AggregateFunction::ArrayAgg => Ok(array_agg::array_agg_udaf()),
        AggregateFunction::StringAgg => Ok(string_agg::string_agg_udaf()),
        AggregateFunction::Variance => Ok(variance::var_samp_udaf()),
        AggregateFunction::VarPop => Ok(variance::var_pop_udaf()),
        AggregateFunction::VarSamp => Ok(variance::var_samp_udaf()),
        AggregateFunction::Stddev => Ok(stddev::stddev_udaf()),
        AggregateFunction::StddevPop => Ok(stddev::stddev_pop_udaf()),
        AggregateFunction::StddevSamp => Ok(stddev::stddev_udaf()),
        AggregateFunction::Corr => Ok(correlation::corr_udaf()),
        AggregateFunction::CovarPop => Ok(covariance::covar_pop_udaf()),
        AggregateFunction::CovarSamp => Ok(covariance::covar_samp_udaf()),
        AggregateFunction::ApproxCountDistinct => Ok(approx_distinct::approx_distinct_udaf()),
        _ => Err(datafusion::common::DataFusionError::NotImplemented(
            format!("Aggregate UDAF not implemented: {:?}", func),
        )),
    }
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

fn convert_window_function(func: &WindowFunction, args: Vec<DFExpr>) -> DFResult<DFExpr> {
    use datafusion::functions_window::expr_fn as window_fn;

    match func {
        WindowFunction::RowNumber => Ok(window_fn::row_number()),
        WindowFunction::Rank => Ok(window_fn::rank()),
        WindowFunction::DenseRank => Ok(window_fn::dense_rank()),
        WindowFunction::PercentRank => Ok(window_fn::percent_rank()),
        WindowFunction::CumeDist => Ok(window_fn::cume_dist()),
        WindowFunction::Ntile => {
            let n = args.into_iter().next().unwrap_or(lit(1));
            Ok(window_fn::ntile(n))
        }
        WindowFunction::Lead => {
            let mut iter = args.into_iter();
            let arg = iter.next().unwrap_or(lit(ScalarValue::Null));
            let offset_expr = iter.next();
            let default_expr = iter.next();
            let offset = offset_expr
                .as_ref()
                .and_then(extract_i64_literal)
                .unwrap_or(1);
            let offset_lit = lit(offset);
            let default_lit = default_expr.unwrap_or_else(|| lit(ScalarValue::Null));
            Ok(
                datafusion::functions_window::lead_lag::lead_udwf().call(vec![
                    arg,
                    offset_lit,
                    default_lit,
                ]),
            )
        }
        WindowFunction::Lag => {
            let mut iter = args.into_iter();
            let arg = iter.next().unwrap_or(lit(ScalarValue::Null));
            let offset_expr = iter.next();
            let default_expr = iter.next();
            let offset = offset_expr
                .as_ref()
                .and_then(extract_i64_literal)
                .unwrap_or(1);
            let offset_lit = lit(offset);
            let default_lit = default_expr.unwrap_or_else(|| lit(ScalarValue::Null));
            Ok(
                datafusion::functions_window::lead_lag::lag_udwf().call(vec![
                    arg,
                    offset_lit,
                    default_lit,
                ]),
            )
        }
        WindowFunction::FirstValue => {
            let arg = args.into_iter().next().unwrap_or(lit(ScalarValue::Null));
            Ok(window_fn::first_value(arg))
        }
        WindowFunction::LastValue => {
            let arg = args.into_iter().next().unwrap_or(lit(ScalarValue::Null));
            Ok(window_fn::last_value(arg))
        }
        WindowFunction::NthValue => {
            let mut iter = args.into_iter();
            let arg = iter.next().unwrap_or(lit(ScalarValue::Null));
            let n = iter
                .next()
                .and_then(|e| extract_i64_literal(&e))
                .unwrap_or(1);
            Ok(window_fn::nth_value(arg, n))
        }
    }
}

fn extract_i64_literal(expr: &DFExpr) -> Option<i64> {
    match expr {
        DFExpr::Literal(ScalarValue::Int64(Some(v))) => Some(*v),
        DFExpr::Literal(ScalarValue::Int32(Some(v))) => Some(*v as i64),
        DFExpr::Literal(ScalarValue::Int16(Some(v))) => Some(*v as i64),
        DFExpr::Literal(ScalarValue::Int8(Some(v))) => Some(*v as i64),
        DFExpr::Literal(ScalarValue::UInt64(Some(v))) => Some(*v as i64),
        DFExpr::Literal(ScalarValue::UInt32(Some(v))) => Some(*v as i64),
        DFExpr::Literal(ScalarValue::UInt16(Some(v))) => Some(*v as i64),
        DFExpr::Literal(ScalarValue::UInt8(Some(v))) => Some(*v as i64),
        _ => None,
    }
}

fn convert_window_frame(frame: &yachtsql_ir::WindowFrame) -> DFResult<WindowFrame> {
    let units = match frame.unit {
        yachtsql_ir::WindowFrameUnit::Rows => WindowFrameUnits::Rows,
        yachtsql_ir::WindowFrameUnit::Range => WindowFrameUnits::Range,
        yachtsql_ir::WindowFrameUnit::Groups => WindowFrameUnits::Groups,
    };

    let start_bound = convert_window_frame_bound(&frame.start)?;
    let end_bound = frame
        .end
        .as_ref()
        .map(convert_window_frame_bound)
        .transpose()?
        .unwrap_or(WindowFrameBound::CurrentRow);

    Ok(WindowFrame::new_bounds(units, start_bound, end_bound))
}

fn convert_window_frame_bound(bound: &yachtsql_ir::WindowFrameBound) -> DFResult<WindowFrameBound> {
    match bound {
        yachtsql_ir::WindowFrameBound::CurrentRow => Ok(WindowFrameBound::CurrentRow),
        yachtsql_ir::WindowFrameBound::Preceding(None) => {
            Ok(WindowFrameBound::Preceding(ScalarValue::Null))
        }
        yachtsql_ir::WindowFrameBound::Preceding(Some(n)) => {
            Ok(WindowFrameBound::Preceding(ScalarValue::UInt64(Some(*n))))
        }
        yachtsql_ir::WindowFrameBound::Following(None) => {
            Ok(WindowFrameBound::Following(ScalarValue::Null))
        }
        yachtsql_ir::WindowFrameBound::Following(Some(n)) => {
            Ok(WindowFrameBound::Following(ScalarValue::UInt64(Some(*n))))
        }
    }
}

fn extract_json_path(json: &serde_json::Value, path: &str) -> Option<serde_json::Value> {
    let path = path.trim_start_matches('$');
    let mut current = json.clone();

    for segment in path.split('.').filter(|s| !s.is_empty()) {
        let (key, index) = if segment.contains('[') {
            let parts: Vec<&str> = segment.split('[').collect();
            let key = parts[0];
            let idx_str = parts[1].trim_end_matches(']');
            let idx: usize = match idx_str.parse() {
                Ok(i) => i,
                Err(_) => return None,
            };
            (key, Some(idx))
        } else {
            (segment, None)
        };

        if !key.is_empty() {
            current = match current {
                serde_json::Value::Object(map) => {
                    map.get(key).cloned().unwrap_or(serde_json::Value::Null)
                }
                _ => return None,
            };
        }

        if let Some(idx) = index {
            current = match current {
                serde_json::Value::Array(arr) => {
                    arr.get(idx).cloned().unwrap_or(serde_json::Value::Null)
                }
                _ => return None,
            };
        }
    }

    if current == serde_json::Value::Null {
        None
    } else {
        Some(current)
    }
}

fn json_value_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::arrow::array::{Array, ArrayRef, StringArray};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug)]
    struct JsonValueUdf {
        signature: Signature,
    }

    impl JsonValueUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for JsonValueUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "json_value"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
            Ok(ArrowDataType::Utf8)
        }

        fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
            let json_arr = match &args[0] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };
            let path_arr = match &args[1] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };

            let json_strs = json_arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Internal(
                        "Expected string array for JSON".to_string(),
                    )
                })?;
            let path_strs = path_arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Internal(
                        "Expected string array for path".to_string(),
                    )
                })?;

            let mut builder = datafusion::arrow::array::StringBuilder::new();
            for i in 0..json_strs.len() {
                if json_strs.is_null(i) || path_strs.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let json_str = json_strs.value(i);
                let path = path_strs.value(i);
                match serde_json::from_str::<serde_json::Value>(json_str) {
                    Ok(json) => match extract_json_path(&json, path) {
                        Some(serde_json::Value::String(s)) => builder.append_value(&s),
                        Some(serde_json::Value::Number(n)) => builder.append_value(n.to_string()),
                        Some(serde_json::Value::Bool(b)) => builder.append_value(b.to_string()),
                        _ => builder.append_null(),
                    },
                    Err(_) => builder.append_null(),
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }

    ScalarUDF::from(JsonValueUdf::new())
}

fn json_extract_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::arrow::array::{Array, ArrayRef, StringArray};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug)]
    struct JsonExtractUdf {
        signature: Signature,
    }

    impl JsonExtractUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for JsonExtractUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "json_extract"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
            Ok(ArrowDataType::Utf8)
        }

        fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
            let json_arr = match &args[0] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };
            let path_arr = match &args[1] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };

            let json_strs = json_arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Internal(
                        "Expected string array for JSON".to_string(),
                    )
                })?;
            let path_strs = path_arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Internal(
                        "Expected string array for path".to_string(),
                    )
                })?;

            let mut builder = datafusion::arrow::array::StringBuilder::new();
            for i in 0..json_strs.len() {
                if json_strs.is_null(i) || path_strs.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let json_str = json_strs.value(i);
                let path = path_strs.value(i);
                match serde_json::from_str::<serde_json::Value>(json_str) {
                    Ok(json) => match extract_json_path(&json, path) {
                        Some(v) => builder.append_value(v.to_string()),
                        None => builder.append_null(),
                    },
                    Err(_) => builder.append_null(),
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }

    ScalarUDF::from(JsonExtractUdf::new())
}

fn json_query_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::arrow::array::{Array, ArrayRef, StringArray};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug)]
    struct JsonQueryUdf {
        signature: Signature,
    }

    impl JsonQueryUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for JsonQueryUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "json_query"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
            Ok(ArrowDataType::Utf8)
        }

        fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
            let json_arr = match &args[0] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };
            let path_arr = match &args[1] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };

            let json_strs = json_arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Internal(
                        "Expected string array for JSON".to_string(),
                    )
                })?;
            let path_strs = path_arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Internal(
                        "Expected string array for path".to_string(),
                    )
                })?;

            let mut builder = datafusion::arrow::array::StringBuilder::new();
            for i in 0..json_strs.len() {
                if json_strs.is_null(i) || path_strs.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let json_str = json_strs.value(i);
                let path = path_strs.value(i);
                match serde_json::from_str::<serde_json::Value>(json_str) {
                    Ok(json) => match extract_json_path(&json, path) {
                        Some(v) => builder.append_value(v.to_string()),
                        None => builder.append_null(),
                    },
                    Err(_) => builder.append_null(),
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }

    ScalarUDF::from(JsonQueryUdf::new())
}

fn json_extract_array_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::arrow::array::{Array, ArrayRef, ListBuilder, StringArray, StringBuilder};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug)]
    struct JsonExtractArrayUdf {
        signature: Signature,
    }

    impl JsonExtractArrayUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for JsonExtractArrayUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "json_extract_array"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
            Ok(ArrowDataType::List(Arc::new(ArrowField::new(
                "item",
                ArrowDataType::Utf8,
                true,
            ))))
        }

        fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
            let json_arr = match &args[0] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };
            let path_arr = match &args[1] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };

            let json_strs = json_arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Internal(
                        "Expected string array for JSON".to_string(),
                    )
                })?;
            let path_strs = path_arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Internal(
                        "Expected string array for path".to_string(),
                    )
                })?;

            let mut builder = ListBuilder::new(StringBuilder::new());
            for i in 0..json_strs.len() {
                if json_strs.is_null(i) || path_strs.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let json_str = json_strs.value(i);
                let path = path_strs.value(i);
                match serde_json::from_str::<serde_json::Value>(json_str) {
                    Ok(json) => {
                        let target = if path == "$" {
                            Some(json)
                        } else {
                            extract_json_path(&json, path)
                        };
                        match target {
                            Some(serde_json::Value::Array(arr)) => {
                                for v in arr {
                                    builder.values().append_value(v.to_string());
                                }
                                builder.append(true);
                            }
                            _ => {
                                builder.append(true);
                            }
                        }
                    }
                    Err(_) => {
                        builder.append(true);
                    }
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }

    ScalarUDF::from(JsonExtractArrayUdf::new())
}

fn json_extract_string_array_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::arrow::array::{Array, ArrayRef, ListBuilder, StringArray, StringBuilder};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug)]
    struct JsonExtractStringArrayUdf {
        signature: Signature,
    }

    impl JsonExtractStringArrayUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for JsonExtractStringArrayUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "json_extract_string_array"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
            Ok(ArrowDataType::List(Arc::new(ArrowField::new(
                "item",
                ArrowDataType::Utf8,
                true,
            ))))
        }

        fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
            let json_arr = match &args[0] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };
            let path_arr = match &args[1] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };

            let json_strs = json_arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Internal(
                        "Expected string array for JSON".to_string(),
                    )
                })?;
            let path_strs = path_arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Internal(
                        "Expected string array for path".to_string(),
                    )
                })?;

            let mut builder = ListBuilder::new(StringBuilder::new());
            for i in 0..json_strs.len() {
                if json_strs.is_null(i) || path_strs.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let json_str = json_strs.value(i);
                let path = path_strs.value(i);
                match serde_json::from_str::<serde_json::Value>(json_str) {
                    Ok(json) => {
                        let target = if path == "$" {
                            Some(json)
                        } else {
                            extract_json_path(&json, path)
                        };
                        match target {
                            Some(serde_json::Value::Array(arr)) => {
                                for v in arr {
                                    match v {
                                        serde_json::Value::String(s) => {
                                            builder.values().append_value(&s)
                                        }
                                        serde_json::Value::Number(n) => {
                                            builder.values().append_value(n.to_string())
                                        }
                                        serde_json::Value::Bool(b) => {
                                            builder.values().append_value(b.to_string())
                                        }
                                        serde_json::Value::Null => builder.values().append_null(),
                                        _ => builder.values().append_value(v.to_string()),
                                    }
                                }
                                builder.append(true);
                            }
                            _ => {
                                builder.append(true);
                            }
                        }
                    }
                    Err(_) => {
                        builder.append(true);
                    }
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }

    ScalarUDF::from(JsonExtractStringArrayUdf::new())
}

fn to_json_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::arrow::array::{Array, ArrayRef, StringArray};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug)]
    struct ToJsonUdf {
        signature: Signature,
    }

    impl ToJsonUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for ToJsonUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "to_json"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
            Ok(ArrowDataType::Utf8)
        }

        fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
            let arr = match &args[0] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };

            let strs = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                datafusion::common::DataFusionError::Internal("Expected string array".to_string())
            })?;

            let mut builder = datafusion::arrow::array::StringBuilder::new();
            for i in 0..strs.len() {
                if strs.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let s = strs.value(i);
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(_) => builder.append_value(s),
                    Err(_) => {
                        let json_str =
                            serde_json::to_string(&serde_json::Value::String(s.to_string()))
                                .unwrap_or_else(|_| "null".to_string());
                        builder.append_value(&json_str);
                    }
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }

    ScalarUDF::from(ToJsonUdf::new())
}

fn to_json_string_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::arrow::array::{Array, ArrayRef, StringArray};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug)]
    struct ToJsonStringUdf {
        signature: Signature,
    }

    impl ToJsonStringUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for ToJsonStringUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "to_json_string"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
            Ok(ArrowDataType::Utf8)
        }

        fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
            let arr = match &args[0] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };

            let strs = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                datafusion::common::DataFusionError::Internal("Expected string array".to_string())
            })?;

            let mut builder = datafusion::arrow::array::StringBuilder::new();
            for i in 0..strs.len() {
                if strs.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let s = strs.value(i);
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(_) => builder.append_value(s),
                    Err(_) => {
                        let json_str =
                            serde_json::to_string(&serde_json::Value::String(s.to_string()))
                                .unwrap_or_else(|_| "null".to_string());
                        builder.append_value(&json_str);
                    }
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }

    ScalarUDF::from(ToJsonStringUdf::new())
}

fn parse_json_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::arrow::array::{Array, ArrayRef, StringArray};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug)]
    struct ParseJsonUdf {
        signature: Signature,
    }

    impl ParseJsonUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for ParseJsonUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "parse_json"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
            Ok(ArrowDataType::Utf8)
        }

        fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
            let arr = match &args[0] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };

            let strs = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                datafusion::common::DataFusionError::Internal("Expected string array".to_string())
            })?;

            let mut builder = datafusion::arrow::array::StringBuilder::new();
            for i in 0..strs.len() {
                if strs.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let s = strs.value(i);
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(json) => builder.append_value(json.to_string()),
                    Err(_) => builder.append_null(),
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }

    ScalarUDF::from(ParseJsonUdf::new())
}

fn json_type_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::arrow::array::{Array, ArrayRef, StringArray};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug)]
    struct JsonTypeUdf {
        signature: Signature,
    }

    impl JsonTypeUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for JsonTypeUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "json_type"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
            Ok(ArrowDataType::Utf8)
        }

        fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
            let arr = match &args[0] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };

            let strs = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                datafusion::common::DataFusionError::Internal("Expected string array".to_string())
            })?;

            let mut builder = datafusion::arrow::array::StringBuilder::new();
            for i in 0..strs.len() {
                if strs.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let s = strs.value(i);
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(json) => {
                        let type_name = match json {
                            serde_json::Value::Null => "null",
                            serde_json::Value::Bool(_) => "boolean",
                            serde_json::Value::Number(_) => "number",
                            serde_json::Value::String(_) => "string",
                            serde_json::Value::Array(_) => "array",
                            serde_json::Value::Object(_) => "object",
                        };
                        builder.append_value(type_name);
                    }
                    Err(_) => builder.append_null(),
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }

    ScalarUDF::from(JsonTypeUdf::new())
}

fn json_keys_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::arrow::array::{Array, ArrayRef, ListBuilder, StringArray, StringBuilder};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug)]
    struct JsonKeysUdf {
        signature: Signature,
    }

    impl JsonKeysUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for JsonKeysUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "json_keys"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
            Ok(ArrowDataType::List(Arc::new(ArrowField::new(
                "item",
                ArrowDataType::Utf8,
                true,
            ))))
        }

        fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
            let arr = match &args[0] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };

            let strs = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                datafusion::common::DataFusionError::Internal("Expected string array".to_string())
            })?;

            let mut builder = ListBuilder::new(StringBuilder::new());
            for i in 0..strs.len() {
                if strs.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let s = strs.value(i);
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(serde_json::Value::Object(obj)) => {
                        for key in obj.keys() {
                            builder.values().append_value(key);
                        }
                        builder.append(true);
                    }
                    _ => {
                        builder.append(true);
                    }
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }

    ScalarUDF::from(JsonKeysUdf::new())
}

fn json_array_length_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::arrow::array::{Array, ArrayRef, Int64Builder, StringArray};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug)]
    struct JsonArrayLengthUdf {
        signature: Signature,
    }

    impl JsonArrayLengthUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for JsonArrayLengthUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "json_array_length"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
            Ok(ArrowDataType::Int64)
        }

        fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
            let json_arr = match &args[0] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };
            let path_arr = if args.len() > 1 {
                match &args[1] {
                    ColumnarValue::Array(arr) => arr.clone(),
                    ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
                }
            } else {
                Arc::new(StringArray::from(vec!["$"; num_rows])) as ArrayRef
            };

            let json_strs = json_arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Internal(
                        "Expected string array for JSON".to_string(),
                    )
                })?;
            let path_strs = path_arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Internal(
                        "Expected string array for path".to_string(),
                    )
                })?;

            let mut builder = Int64Builder::new();
            for i in 0..json_strs.len() {
                if json_strs.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let json_str = json_strs.value(i);
                let path = if path_strs.is_null(i) {
                    "$"
                } else {
                    path_strs.value(i)
                };
                match serde_json::from_str::<serde_json::Value>(json_str) {
                    Ok(json) => {
                        let target = if path == "$" {
                            Some(json)
                        } else {
                            extract_json_path(&json, path)
                        };
                        match target {
                            Some(serde_json::Value::Array(arr)) => {
                                builder.append_value(arr.len() as i64);
                            }
                            _ => builder.append_null(),
                        }
                    }
                    Err(_) => builder.append_null(),
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }

    ScalarUDF::from(JsonArrayLengthUdf::new())
}

fn int64_from_json_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::arrow::array::{Array, ArrayRef, Int64Builder, StringArray};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug)]
    struct Int64FromJsonUdf {
        signature: Signature,
    }

    impl Int64FromJsonUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for Int64FromJsonUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "int64"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
            Ok(ArrowDataType::Int64)
        }

        fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
            let arr = match &args[0] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };

            let strs = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                datafusion::common::DataFusionError::Internal("Expected string array".to_string())
            })?;

            let mut builder = Int64Builder::new();
            for i in 0..strs.len() {
                if strs.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let s = strs.value(i);
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(json) => match json {
                        serde_json::Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                builder.append_value(i);
                            } else if let Some(f) = n.as_f64() {
                                builder.append_value(f as i64);
                            } else {
                                builder.append_null();
                            }
                        }
                        serde_json::Value::String(s) => {
                            if let Ok(i) = s.parse::<i64>() {
                                builder.append_value(i);
                            } else {
                                builder.append_null();
                            }
                        }
                        serde_json::Value::Bool(b) => {
                            builder.append_value(if b { 1 } else { 0 });
                        }
                        _ => builder.append_null(),
                    },
                    Err(_) => {
                        if let Ok(i) = s.parse::<i64>() {
                            builder.append_value(i);
                        } else {
                            builder.append_null();
                        }
                    }
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }

    ScalarUDF::from(Int64FromJsonUdf::new())
}

fn float64_from_json_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::arrow::array::{Array, ArrayRef, Float64Builder, StringArray};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug)]
    struct Float64FromJsonUdf {
        signature: Signature,
    }

    impl Float64FromJsonUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for Float64FromJsonUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "float64"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
            Ok(ArrowDataType::Float64)
        }

        fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
            let arr = match &args[0] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };

            let strs = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                datafusion::common::DataFusionError::Internal("Expected string array".to_string())
            })?;

            let mut builder = Float64Builder::new();
            for i in 0..strs.len() {
                if strs.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let s = strs.value(i);
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(json) => match json {
                        serde_json::Value::Number(n) => {
                            if let Some(f) = n.as_f64() {
                                builder.append_value(f);
                            } else {
                                builder.append_null();
                            }
                        }
                        serde_json::Value::String(s) => {
                            if let Ok(f) = s.parse::<f64>() {
                                builder.append_value(f);
                            } else {
                                builder.append_null();
                            }
                        }
                        _ => builder.append_null(),
                    },
                    Err(_) => {
                        if let Ok(f) = s.parse::<f64>() {
                            builder.append_value(f);
                        } else {
                            builder.append_null();
                        }
                    }
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }

    ScalarUDF::from(Float64FromJsonUdf::new())
}

fn bool_from_json_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::arrow::array::{Array, ArrayRef, BooleanBuilder, StringArray};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug)]
    struct BoolFromJsonUdf {
        signature: Signature,
    }

    impl BoolFromJsonUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for BoolFromJsonUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "bool"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
            Ok(ArrowDataType::Boolean)
        }

        fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
            let arr = match &args[0] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };

            let strs = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                datafusion::common::DataFusionError::Internal("Expected string array".to_string())
            })?;

            let mut builder = BooleanBuilder::new();
            for i in 0..strs.len() {
                if strs.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let s = strs.value(i);
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(json) => match json {
                        serde_json::Value::Bool(b) => builder.append_value(b),
                        serde_json::Value::String(s) => match s.to_lowercase().as_str() {
                            "true" => builder.append_value(true),
                            "false" => builder.append_value(false),
                            _ => builder.append_null(),
                        },
                        _ => builder.append_null(),
                    },
                    Err(_) => match s.to_lowercase().as_str() {
                        "true" => builder.append_value(true),
                        "false" => builder.append_value(false),
                        _ => builder.append_null(),
                    },
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }

    ScalarUDF::from(BoolFromJsonUdf::new())
}

fn string_from_json_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::arrow::array::{Array, ArrayRef, StringArray};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug)]
    struct StringFromJsonUdf {
        signature: Signature,
    }

    impl StringFromJsonUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for StringFromJsonUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "string"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
            Ok(ArrowDataType::Utf8)
        }

        fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
            let arr = match &args[0] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };

            let strs = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                datafusion::common::DataFusionError::Internal("Expected string array".to_string())
            })?;

            let mut builder = datafusion::arrow::array::StringBuilder::new();
            for i in 0..strs.len() {
                if strs.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let s = strs.value(i);
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(json) => match json {
                        serde_json::Value::String(s) => builder.append_value(&s),
                        serde_json::Value::Number(n) => builder.append_value(n.to_string()),
                        serde_json::Value::Bool(b) => builder.append_value(b.to_string()),
                        _ => builder.append_value(json.to_string()),
                    },
                    Err(_) => builder.append_value(s),
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }

    ScalarUDF::from(StringFromJsonUdf::new())
}

fn date_diff_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::arrow::array::{
        Array, ArrayRef, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug)]
    struct DateDiffUdf {
        signature: Signature,
    }

    impl DateDiffUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    fn extract_nanos(arr: &ArrayRef, idx: usize) -> Option<i64> {
        if arr.is_null(idx) {
            return None;
        }
        if let Some(ts) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
            return Some(ts.value(idx));
        }
        if let Some(ts) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
            return Some(ts.value(idx) * 1_000);
        }
        if let Some(ts) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
            return Some(ts.value(idx) * 1_000_000);
        }
        if let Some(ts) = arr.as_any().downcast_ref::<TimestampSecondArray>() {
            return Some(ts.value(idx) * 1_000_000_000);
        }
        if let Some(d32) = arr
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Date32Array>()
        {
            return Some(d32.value(idx) as i64 * 86_400_000_000_000);
        }
        if let Some(d64) = arr
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Date64Array>()
        {
            return Some(d64.value(idx) * 1_000_000);
        }
        None
    }

    impl ScalarUDFImpl for DateDiffUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "date_diff_bigquery"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
            Ok(ArrowDataType::Int64)
        }

        fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
            let ts1_arr = match &args[0] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };
            let ts2_arr = match &args[1] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };
            let part_arr = match &args[2] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };

            let parts = part_arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Internal(
                        "Expected string array for date part".to_string(),
                    )
                })?;

            let mut builder = datafusion::arrow::array::Int64Builder::new();
            for i in 0..num_rows {
                let nanos1 = extract_nanos(&ts1_arr, i);
                let nanos2 = extract_nanos(&ts2_arr, i);

                match (nanos1, nanos2) {
                    (Some(n1), Some(n2)) => {
                        let diff_nanos = n1 - n2;
                        let part = parts.value(i).to_uppercase();
                        let result = match part.as_str() {
                            "NANOSECOND" => diff_nanos,
                            "MICROSECOND" => diff_nanos / 1_000,
                            "MILLISECOND" => diff_nanos / 1_000_000,
                            "SECOND" => diff_nanos / 1_000_000_000,
                            "MINUTE" => diff_nanos / 60_000_000_000,
                            "HOUR" => diff_nanos / 3_600_000_000_000,
                            "DAY" => diff_nanos / 86_400_000_000_000,
                            "WEEK" => diff_nanos / (7 * 86_400_000_000_000),
                            "MONTH" => {
                                let days = diff_nanos / 86_400_000_000_000;
                                days / 30
                            }
                            "QUARTER" => {
                                let days = diff_nanos / 86_400_000_000_000;
                                days / 91
                            }
                            "YEAR" => {
                                let days = diff_nanos / 86_400_000_000_000;
                                days / 365
                            }
                            _ => diff_nanos / 86_400_000_000_000,
                        };
                        builder.append_value(result);
                    }
                    _ => builder.append_null(),
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }

    ScalarUDF::from(DateDiffUdf::new())
}

fn lax_bool_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::arrow::array::{Array, ArrayRef, BooleanBuilder, StringArray};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug)]
    struct LaxBoolUdf {
        signature: Signature,
    }

    impl LaxBoolUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for LaxBoolUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "lax_bool"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
            Ok(ArrowDataType::Boolean)
        }

        fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
            let arr = match &args[0] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };

            let mut builder = BooleanBuilder::new();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                if let Some(b) = arr
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                {
                    builder.append_value(b.value(i));
                } else if let Some(i64s) = arr
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                {
                    builder.append_value(i64s.value(i) != 0);
                } else if let Some(f64s) = arr
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Float64Array>()
                {
                    builder.append_value(f64s.value(i) != 0.0);
                } else if let Some(strs) = arr.as_any().downcast_ref::<StringArray>() {
                    let s = strs.value(i).to_lowercase();
                    match s.as_str() {
                        "true" | "1" => builder.append_value(true),
                        "false" | "0" => builder.append_value(false),
                        _ => builder.append_null(),
                    }
                } else {
                    builder.append_null();
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }

    ScalarUDF::from(LaxBoolUdf::new())
}

fn lax_int64_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::arrow::array::{Array, ArrayRef, Int64Builder, StringArray};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug)]
    struct LaxInt64Udf {
        signature: Signature,
    }

    impl LaxInt64Udf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for LaxInt64Udf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "lax_int64"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
            Ok(ArrowDataType::Int64)
        }

        fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
            let arr = match &args[0] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };

            let mut builder = Int64Builder::new();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                if let Some(i64s) = arr
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                {
                    builder.append_value(i64s.value(i));
                } else if let Some(f64s) = arr
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Float64Array>()
                {
                    builder.append_value(f64s.value(i) as i64);
                } else if let Some(b) = arr
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                {
                    builder.append_value(if b.value(i) { 1 } else { 0 });
                } else if let Some(strs) = arr.as_any().downcast_ref::<StringArray>() {
                    match strs.value(i).trim().parse::<i64>() {
                        Ok(v) => builder.append_value(v),
                        Err(_) => match strs.value(i).trim().parse::<f64>() {
                            Ok(v) => builder.append_value(v as i64),
                            Err(_) => builder.append_null(),
                        },
                    }
                } else {
                    builder.append_null();
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }

    ScalarUDF::from(LaxInt64Udf::new())
}

fn lax_float64_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::arrow::array::{Array, ArrayRef, Float64Builder, StringArray};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug)]
    struct LaxFloat64Udf {
        signature: Signature,
    }

    impl LaxFloat64Udf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for LaxFloat64Udf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "lax_float64"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
            Ok(ArrowDataType::Float64)
        }

        fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
            let arr = match &args[0] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };

            let mut builder = Float64Builder::new();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                if let Some(f64s) = arr
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Float64Array>()
                {
                    builder.append_value(f64s.value(i));
                } else if let Some(i64s) = arr
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                {
                    builder.append_value(i64s.value(i) as f64);
                } else if let Some(b) = arr
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                {
                    builder.append_value(if b.value(i) { 1.0 } else { 0.0 });
                } else if let Some(strs) = arr.as_any().downcast_ref::<StringArray>() {
                    match strs.value(i).trim().parse::<f64>() {
                        Ok(v) => builder.append_value(v),
                        Err(_) => builder.append_null(),
                    }
                } else {
                    builder.append_null();
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }

    ScalarUDF::from(LaxFloat64Udf::new())
}

fn lax_string_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug)]
    struct LaxStringUdf {
        signature: Signature,
    }

    impl LaxStringUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for LaxStringUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "lax_string"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[ArrowDataType]) -> DFResult<ArrowDataType> {
            Ok(ArrowDataType::Utf8)
        }

        fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
            let arr = match &args[0] {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows)?,
            };

            let mut builder = StringBuilder::new();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                if let Some(strs) = arr.as_any().downcast_ref::<StringArray>() {
                    builder.append_value(strs.value(i));
                } else if let Some(i64s) = arr
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                {
                    builder.append_value(i64s.value(i).to_string());
                } else if let Some(f64s) = arr
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Float64Array>()
                {
                    builder.append_value(f64s.value(i).to_string());
                } else if let Some(b) = arr
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                {
                    builder.append_value(b.value(i).to_string());
                } else {
                    builder.append_null();
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }

    ScalarUDF::from(LaxStringUdf::new())
}
