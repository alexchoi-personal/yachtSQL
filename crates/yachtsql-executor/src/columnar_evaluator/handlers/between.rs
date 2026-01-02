#![coverage(off)]

use yachtsql_common::error::Result;
use yachtsql_ir::Expr;
use yachtsql_storage::{Column, Table};

use crate::columnar_evaluator::ColumnarEvaluator;

pub fn eval_between(
    evaluator: &ColumnarEvaluator,
    expr: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
    table: &Table,
) -> Result<Column> {
    let val_col = evaluator.evaluate(expr, table)?;
    let low_col = evaluator.evaluate(low, table)?;
    let high_col = evaluator.evaluate(high, table)?;

    let ge_low = val_col.binary_ge(&low_col);
    let le_high = val_col.binary_le(&high_col);
    let in_range = ge_low.binary_and(&le_high);

    if negated {
        Ok(in_range.unary_not())
    } else {
        Ok(in_range)
    }
}
