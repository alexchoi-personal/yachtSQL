#![coverage(off)]

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, UnaryOp};
use yachtsql_storage::{Column, Table};

use crate::columnar_evaluator::ColumnarEvaluator;

pub fn eval_unary_op_ext(
    evaluator: &ColumnarEvaluator,
    op: UnaryOp,
    expr: &Expr,
    table: &Table,
) -> Result<Column> {
    let col = evaluator.evaluate(expr, table)?;

    let n = table.row_count();
    let mut results = Vec::with_capacity(n);

    for i in 0..n {
        let v = col.get_value(i);
        let result = apply_unary_op_ext(op, &v);
        results.push(result);
    }
    Ok(Column::from_values(&results))
}

fn apply_unary_op_ext(op: UnaryOp, value: &Value) -> Value {
    match (op, value) {
        (_, Value::Null) => Value::Null,
        (UnaryOp::BitwiseNot, Value::Int64(n)) => Value::Int64(!n),
        _ => Value::Null,
    }
}
