#![coverage(off)]

use regex::Regex;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::Expr;
use yachtsql_storage::{Column, Table};

use crate::columnar_evaluator::ColumnarEvaluator;

pub fn eval_like(
    evaluator: &ColumnarEvaluator,
    expr: &Expr,
    pattern: &Expr,
    negated: bool,
    case_insensitive: bool,
    table: &Table,
) -> Result<Column> {
    let str_col = evaluator.evaluate(expr, table)?;
    let pattern_col = evaluator.evaluate(pattern, table)?;

    let n = table.row_count();
    let mut results = Vec::with_capacity(n);

    for i in 0..n {
        let s = str_col.get_value(i);
        let p = pattern_col.get_value(i);

        match (&s, &p) {
            (Value::Null, _) | (_, Value::Null) => results.push(Value::Null),
            (Value::String(s), Value::String(p)) => {
                let regex_pattern = like_to_regex(p, case_insensitive);
                let re = Regex::new(&regex_pattern)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid pattern: {}", e)))?;
                let matches = re.is_match(s);
                results.push(Value::Bool(if negated { !matches } else { matches }));
            }
            _ => results.push(Value::Bool(false)),
        }
    }
    Ok(Column::from_values(&results))
}

fn like_to_regex(pattern: &str, case_insensitive: bool) -> String {
    let mut regex = String::new();
    if case_insensitive {
        regex.push_str("(?i)");
    }
    regex.push('^');

    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            '\\' => {
                if let Some(&next) = chars.peek() {
                    regex.push_str(&regex::escape(&next.to_string()));
                    chars.next();
                }
            }
            _ => regex.push_str(&regex::escape(&c.to_string())),
        }
    }
    regex.push('$');
    regex
}
