#![coverage(off)]

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::Expr;
use yachtsql_storage::{Column, Table};

use crate::columnar_evaluator::ColumnarEvaluator;

pub fn eval_struct(
    evaluator: &ColumnarEvaluator,
    fields: &[(Option<String>, Expr)],
    table: &Table,
) -> Result<Column> {
    let field_cols: Vec<(Option<String>, Column)> = fields
        .iter()
        .map(|(name, expr)| Ok((name.clone(), evaluator.evaluate(expr, table)?)))
        .collect::<Result<_>>()?;

    let n = table.row_count();
    let mut results = Vec::with_capacity(n);

    for i in 0..n {
        let struct_fields: Vec<(String, Value)> = field_cols
            .iter()
            .enumerate()
            .map(|(idx, (name, col))| {
                let field_name = name.clone().unwrap_or_else(|| format!("_field_{}", idx));
                (field_name, col.get_value(i))
            })
            .collect();
        results.push(Value::Struct(struct_fields));
    }

    Ok(Column::from_values(&results))
}
