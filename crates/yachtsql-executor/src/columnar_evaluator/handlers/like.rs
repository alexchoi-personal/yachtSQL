#![coverage(off)]

use regex::RegexBuilder;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::Expr;
use yachtsql_storage::{Column, Table};

use crate::columnar_evaluator::ColumnarEvaluator;

const MAX_PATTERN_LENGTH: usize = 10_000;
const REGEX_SIZE_LIMIT: usize = 10 * 1024 * 1024;

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
                if p.len() > MAX_PATTERN_LENGTH {
                    return Err(Error::InvalidQuery(format!(
                        "LIKE pattern length {} exceeds maximum of {} characters",
                        p.len(),
                        MAX_PATTERN_LENGTH
                    )));
                }
                let regex_pattern = like_to_regex(p, case_insensitive);
                let re = RegexBuilder::new(&regex_pattern)
                    .size_limit(REGEX_SIZE_LIMIT)
                    .build()
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
