#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_storage::Column;

pub fn coalesce(args: &[Column]) -> Result<Column> {
    if args.is_empty() {
        return Err(Error::invalid_query(
            "COALESCE requires at least 1 argument",
        ));
    }

    let len = args[0].len();
    let mut results = Vec::with_capacity(len);

    for i in 0..len {
        let mut found = Value::Null;
        for col in args {
            let val = col.get_value(i);
            if !val.is_null() {
                found = val;
                break;
            }
        }
        results.push(found);
    }

    Ok(Column::from_values(&results))
}

pub fn ifnull(args: &[Column]) -> Result<Column> {
    if args.len() < 2 {
        return Err(Error::invalid_query("IFNULL requires 2 arguments"));
    }

    let expr_col = &args[0];
    let default_col = &args[1];
    let len = expr_col.len();
    let mut results = Vec::with_capacity(len);

    for i in 0..len {
        let val = expr_col.get_value(i);
        if val.is_null() {
            results.push(default_col.get_value(i));
        } else {
            results.push(val);
        }
    }

    Ok(Column::from_values(&results))
}

pub fn nullif(args: &[Column]) -> Result<Column> {
    if args.len() < 2 {
        return Err(Error::invalid_query("NULLIF requires 2 arguments"));
    }

    let expr_col = &args[0];
    let compare_col = &args[1];
    let len = expr_col.len();
    let mut results = Vec::with_capacity(len);

    for i in 0..len {
        let expr_val = expr_col.get_value(i);
        let compare_val = compare_col.get_value(i);

        if values_equal(&expr_val, &compare_val) {
            results.push(Value::Null);
        } else {
            results.push(expr_val);
        }
    }

    Ok(Column::from_values(&results))
}

pub fn greatest(args: &[Column]) -> Result<Column> {
    if args.is_empty() {
        return Err(Error::invalid_query(
            "GREATEST requires at least 1 argument",
        ));
    }

    let len = args[0].len();
    let mut results = Vec::with_capacity(len);

    for i in 0..len {
        let mut has_null = false;
        let mut max: Option<Value> = None;

        for col in args {
            let val = col.get_value(i);
            if val.is_null() {
                has_null = true;
                break;
            }
            max = Some(match max {
                None => val,
                Some(m) => {
                    if val > m {
                        val
                    } else {
                        m
                    }
                }
            });
        }

        if has_null {
            results.push(Value::Null);
        } else {
            results.push(max.unwrap_or(Value::Null));
        }
    }

    Ok(Column::from_values(&results))
}

pub fn least(args: &[Column]) -> Result<Column> {
    if args.is_empty() {
        return Err(Error::invalid_query("LEAST requires at least 1 argument"));
    }

    let len = args[0].len();
    let mut results = Vec::with_capacity(len);

    for i in 0..len {
        let mut has_null = false;
        let mut min: Option<Value> = None;

        for col in args {
            let val = col.get_value(i);
            if val.is_null() {
                has_null = true;
                break;
            }
            min = Some(match min {
                None => val,
                Some(m) => {
                    if val < m {
                        val
                    } else {
                        m
                    }
                }
            });
        }

        if has_null {
            results.push(Value::Null);
        } else {
            results.push(min.unwrap_or(Value::Null));
        }
    }

    Ok(Column::from_values(&results))
}

fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => false,
        (Value::Int64(a), Value::Float64(b)) => (*a as f64 - b.0).abs() < f64::EPSILON,
        (Value::Float64(a), Value::Int64(b)) => (a.0 - *b as f64).abs() < f64::EPSILON,
        _ => left == right,
    }
}
