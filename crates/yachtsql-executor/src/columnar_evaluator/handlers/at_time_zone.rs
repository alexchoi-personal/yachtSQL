#![coverage(off)]

use chrono::TimeZone;
use chrono_tz::Tz;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::Expr;
use yachtsql_storage::{Column, Table};

use crate::columnar_evaluator::ColumnarEvaluator;

pub fn eval_at_time_zone(
    evaluator: &ColumnarEvaluator,
    expr: &Expr,
    time_zone: &Expr,
    table: &Table,
) -> Result<Column> {
    let ts_col = evaluator.evaluate(expr, table)?;
    let tz_col = evaluator.evaluate(time_zone, table)?;

    let n = table.row_count();
    let mut results = Vec::with_capacity(n);

    for i in 0..n {
        let ts = ts_col.get_value(i);
        let tz = tz_col.get_value(i);

        let result = match (&ts, &tz) {
            (Value::Null, _) | (_, Value::Null) => Value::Null,
            (Value::Timestamp(dt), Value::String(tz_name)) => {
                let tz: Tz = tz_name
                    .parse()
                    .map_err(|_| Error::InvalidQuery(format!("Invalid timezone: {}", tz_name)))?;
                let converted = dt.with_timezone(&tz);
                Value::Timestamp(converted.with_timezone(&chrono::Utc))
            }
            (Value::DateTime(dt), Value::String(tz_name)) => {
                let tz: Tz = tz_name
                    .parse()
                    .map_err(|_| Error::InvalidQuery(format!("Invalid timezone: {}", tz_name)))?;
                let converted = tz.from_utc_datetime(dt);
                Value::DateTime(converted.naive_local())
            }
            _ => Value::Null,
        };
        results.push(result);
    }
    Ok(Column::from_values(&results))
}
