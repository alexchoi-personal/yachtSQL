#![coverage(off)]

use rust_decimal::Decimal;
use sqlparser::ast;
use yachtsql_common::error::{Error, Result};
use yachtsql_ir::{BinaryOp, Literal, UnaryOp};

use super::utils::{parse_byte_string_escapes, unescape_unicode};

pub fn plan_literal(val: &ast::Value) -> Result<Literal> {
    match val {
        ast::Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(Literal::Int64(i))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(Literal::Float64(ordered_float::OrderedFloat(f)))
            } else if let Ok(d) = n.parse::<Decimal>() {
                Ok(Literal::Numeric(d))
            } else {
                Err(Error::parse_error(format!("Invalid number: {}", n)))
            }
        }
        ast::Value::SingleQuotedString(s)
        | ast::Value::DoubleQuotedString(s)
        | ast::Value::TripleSingleQuotedString(s)
        | ast::Value::TripleDoubleQuotedString(s) => Ok(Literal::String(unescape_unicode(s))),
        ast::Value::SingleQuotedRawStringLiteral(s)
        | ast::Value::DoubleQuotedRawStringLiteral(s)
        | ast::Value::TripleSingleQuotedRawStringLiteral(s)
        | ast::Value::TripleDoubleQuotedRawStringLiteral(s) => Ok(Literal::String(s.clone())),
        ast::Value::Boolean(b) => Ok(Literal::Bool(*b)),
        ast::Value::Null => Ok(Literal::Null),
        ast::Value::HexStringLiteral(s) => {
            let bytes = hex::decode(s)
                .map_err(|e| Error::parse_error(format!("Invalid hex string: {}", e)))?;
            Ok(Literal::Bytes(bytes))
        }
        ast::Value::SingleQuotedByteStringLiteral(s)
        | ast::Value::DoubleQuotedByteStringLiteral(s) => {
            Ok(Literal::Bytes(parse_byte_string_escapes(s)))
        }
        _ => Err(Error::unsupported(format!(
            "Unsupported literal: {:?}",
            val
        ))),
    }
}

pub fn plan_binary_op(op: &ast::BinaryOperator) -> Result<BinaryOp> {
    match op {
        ast::BinaryOperator::Plus => Ok(BinaryOp::Add),
        ast::BinaryOperator::Minus => Ok(BinaryOp::Sub),
        ast::BinaryOperator::Multiply => Ok(BinaryOp::Mul),
        ast::BinaryOperator::Divide => Ok(BinaryOp::Div),
        ast::BinaryOperator::Modulo => Ok(BinaryOp::Mod),
        ast::BinaryOperator::Eq => Ok(BinaryOp::Eq),
        ast::BinaryOperator::NotEq => Ok(BinaryOp::NotEq),
        ast::BinaryOperator::Lt => Ok(BinaryOp::Lt),
        ast::BinaryOperator::LtEq => Ok(BinaryOp::LtEq),
        ast::BinaryOperator::Gt => Ok(BinaryOp::Gt),
        ast::BinaryOperator::GtEq => Ok(BinaryOp::GtEq),
        ast::BinaryOperator::And => Ok(BinaryOp::And),
        ast::BinaryOperator::Or => Ok(BinaryOp::Or),
        ast::BinaryOperator::StringConcat => Ok(BinaryOp::Concat),
        ast::BinaryOperator::BitwiseAnd => Ok(BinaryOp::BitwiseAnd),
        ast::BinaryOperator::BitwiseOr => Ok(BinaryOp::BitwiseOr),
        ast::BinaryOperator::BitwiseXor => Ok(BinaryOp::BitwiseXor),
        ast::BinaryOperator::PGBitwiseShiftLeft => Ok(BinaryOp::ShiftLeft),
        ast::BinaryOperator::PGBitwiseShiftRight => Ok(BinaryOp::ShiftRight),
        _ => Err(Error::unsupported(format!(
            "Unsupported binary operator: {:?}",
            op
        ))),
    }
}

pub fn plan_unary_op(op: &ast::UnaryOperator) -> Result<UnaryOp> {
    match op {
        ast::UnaryOperator::Not => Ok(UnaryOp::Not),
        ast::UnaryOperator::Minus => Ok(UnaryOp::Minus),
        ast::UnaryOperator::Plus => Ok(UnaryOp::Plus),
        ast::UnaryOperator::PGBitwiseNot => Ok(UnaryOp::BitwiseNot),
        _ => Err(Error::unsupported(format!(
            "Unsupported unary operator: {:?}",
            op
        ))),
    }
}
