#![coverage(off)]

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{BinaryOp, Expr};
use yachtsql_storage::{Column, Table};

use crate::columnar_evaluator::ColumnarEvaluator;

pub fn eval_binary_op_ext(
    evaluator: &ColumnarEvaluator,
    left: &Expr,
    op: BinaryOp,
    right: &Expr,
    table: &Table,
) -> Result<Column> {
    let left_col = evaluator.evaluate(left, table)?;
    let right_col = evaluator.evaluate(right, table)?;

    let n = table.row_count();
    let mut results = Vec::with_capacity(n);

    for i in 0..n {
        let l = left_col.get_value(i);
        let r = right_col.get_value(i);
        let result = apply_binary_op_ext(&l, op, &r);
        results.push(result);
    }
    Ok(Column::from_values(&results))
}

fn apply_binary_op_ext(left: &Value, op: BinaryOp, right: &Value) -> Value {
    match (left, op, right) {
        (Value::Null, _, _) | (_, _, Value::Null) => Value::Null,
        (Value::String(l), BinaryOp::Concat, Value::String(r)) => {
            Value::String(format!("{}{}", l, r))
        }
        (Value::Int64(l), BinaryOp::BitwiseAnd, Value::Int64(r)) => Value::Int64(l & r),
        (Value::Int64(l), BinaryOp::BitwiseOr, Value::Int64(r)) => Value::Int64(l | r),
        (Value::Int64(l), BinaryOp::BitwiseXor, Value::Int64(r)) => Value::Int64(l ^ r),
        (Value::Int64(l), BinaryOp::ShiftLeft, Value::Int64(r)) => Value::Int64(l << r),
        (Value::Int64(l), BinaryOp::ShiftRight, Value::Int64(r)) => Value::Int64(l >> r),
        _ => Value::Null,
    }
}
