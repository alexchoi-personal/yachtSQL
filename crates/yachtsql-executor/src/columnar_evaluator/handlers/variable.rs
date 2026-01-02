#![coverage(off)]

use std::collections::HashMap;

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_storage::Column;

pub fn eval_variable(
    variables: Option<&HashMap<String, Value>>,
    system_variables: Option<&HashMap<String, Value>>,
    name: &str,
    row_count: usize,
) -> Result<Column> {
    let value = variables
        .and_then(|v| v.get(name))
        .or_else(|| system_variables.and_then(|v| v.get(name)))
        .cloned()
        .unwrap_or(Value::Null);
    Ok(Column::broadcast(value, row_count))
}
