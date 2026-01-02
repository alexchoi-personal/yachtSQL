#![coverage(off)]

use sqlparser::ast;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::IntervalValue;
use yachtsql_ir::{Expr, Literal, PlanSchema, UnaryOp};

use super::ExprPlanner;

pub fn plan_interval(interval: &ast::Interval, schema: &PlanSchema) -> Result<Expr> {
    let value_expr = ExprPlanner::plan_expr(&interval.value, schema)?;
    let value = match &value_expr {
        Expr::Literal(Literal::Int64(n)) => *n,
        Expr::UnaryOp {
            op: UnaryOp::Minus,
            expr,
        } => match expr.as_ref() {
            Expr::Literal(Literal::Int64(n)) => -*n,
            _ => {
                return Err(Error::parse_error(
                    "INTERVAL value must be an integer literal",
                ));
            }
        },
        _ => {
            return Err(Error::parse_error(
                "INTERVAL value must be an integer literal",
            ));
        }
    };

    let (months, days, nanos) = match &interval.leading_field {
        Some(ast::DateTimeField::Year) | Some(ast::DateTimeField::Years) => {
            (value as i32 * 12, 0, 0i64)
        }
        Some(ast::DateTimeField::Month) | Some(ast::DateTimeField::Months) => {
            (value as i32, 0, 0i64)
        }
        Some(ast::DateTimeField::Day) | Some(ast::DateTimeField::Days) => (0, value as i32, 0i64),
        Some(ast::DateTimeField::Hour) | Some(ast::DateTimeField::Hours) => (
            0,
            0,
            value * IntervalValue::MICROS_PER_HOUR * IntervalValue::NANOS_PER_MICRO,
        ),
        Some(ast::DateTimeField::Minute) | Some(ast::DateTimeField::Minutes) => (
            0,
            0,
            value * IntervalValue::MICROS_PER_MINUTE * IntervalValue::NANOS_PER_MICRO,
        ),
        Some(ast::DateTimeField::Second) | Some(ast::DateTimeField::Seconds) => (
            0,
            0,
            value * IntervalValue::MICROS_PER_SECOND * IntervalValue::NANOS_PER_MICRO,
        ),
        Some(ast::DateTimeField::Millisecond) | Some(ast::DateTimeField::Milliseconds) => {
            (0, 0, value * 1_000_000)
        }
        Some(ast::DateTimeField::Microsecond) | Some(ast::DateTimeField::Microseconds) => {
            (0, 0, value * 1_000)
        }
        None => (0, value as i32, 0i64),
        _ => {
            return Err(Error::unsupported(format!(
                "Unsupported interval field: {:?}",
                interval.leading_field
            )));
        }
    };

    Ok(Expr::Literal(Literal::Interval {
        months,
        days,
        nanos,
    }))
}
