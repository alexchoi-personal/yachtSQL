#![coverage(off)]

use yachtsql_common::error::Result;
use yachtsql_ir::Expr;
use yachtsql_storage::{Column, Table};

use crate::columnar_evaluator::ColumnarEvaluator;

pub fn eval_alias(evaluator: &ColumnarEvaluator, expr: &Expr, table: &Table) -> Result<Column> {
    evaluator.evaluate(expr, table)
}
