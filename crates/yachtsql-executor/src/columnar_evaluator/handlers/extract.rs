#![coverage(off)]

use chrono::{Datelike, Timelike};
use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{DateTimeField, Expr, WeekStartDay};
use yachtsql_storage::{Column, Table};

use crate::columnar_evaluator::ColumnarEvaluator;

pub fn eval_extract(
    evaluator: &ColumnarEvaluator,
    field: DateTimeField,
    expr: &Expr,
    table: &Table,
) -> Result<Column> {
    let col = evaluator.evaluate(expr, table)?;
    let n = table.row_count();
    let mut results = Vec::with_capacity(n);

    for i in 0..n {
        let v = col.get_value(i);
        let extracted = extract_field(&v, &field);
        results.push(extracted);
    }
    Ok(Column::from_values(&results))
}

fn week_number_from_date(date: &chrono::NaiveDate, start_day: WeekStartDay) -> i64 {
    let year_start = match chrono::NaiveDate::from_ymd_opt(date.year(), 1, 1) {
        Some(d) => d,
        None => return 0,
    };
    let start_weekday = match start_day {
        WeekStartDay::Sunday => chrono::Weekday::Sun,
        WeekStartDay::Monday => chrono::Weekday::Mon,
        WeekStartDay::Tuesday => chrono::Weekday::Tue,
        WeekStartDay::Wednesday => chrono::Weekday::Wed,
        WeekStartDay::Thursday => chrono::Weekday::Thu,
        WeekStartDay::Friday => chrono::Weekday::Fri,
        WeekStartDay::Saturday => chrono::Weekday::Sat,
    };
    let days_until_first_start_day = (start_weekday.num_days_from_sunday() as i32
        - year_start.weekday().num_days_from_sunday() as i32
        + 7)
        % 7;
    let first_week_start = year_start + chrono::Duration::days(days_until_first_start_day as i64);
    if *date < first_week_start {
        0
    } else {
        let days_since_first_week = (*date - first_week_start).num_days();
        days_since_first_week / 7 + 1
    }
}

pub fn extract_field(value: &Value, field: &DateTimeField) -> Value {
    match (value, field) {
        (Value::Null, _) => Value::Null,

        (Value::Date(d), DateTimeField::Year) => Value::Int64(d.year() as i64),
        (Value::Date(d), DateTimeField::IsoYear) => Value::Int64(d.iso_week().year() as i64),
        (Value::Date(d), DateTimeField::Quarter) => Value::Int64(((d.month() - 1) / 3 + 1) as i64),
        (Value::Date(d), DateTimeField::Month) => Value::Int64(d.month() as i64),
        (Value::Date(d), DateTimeField::Week(start_day)) => {
            Value::Int64(week_number_from_date(d, *start_day))
        }
        (Value::Date(d), DateTimeField::IsoWeek) => Value::Int64(d.iso_week().week() as i64),
        (Value::Date(d), DateTimeField::Day) => Value::Int64(d.day() as i64),
        (Value::Date(d), DateTimeField::DayOfWeek) => {
            Value::Int64(d.weekday().num_days_from_sunday() as i64 + 1)
        }
        (Value::Date(d), DateTimeField::DayOfYear) => Value::Int64(d.ordinal() as i64),

        (Value::DateTime(dt), DateTimeField::Year) => Value::Int64(dt.year() as i64),
        (Value::DateTime(dt), DateTimeField::IsoYear) => {
            Value::Int64(dt.date().iso_week().year() as i64)
        }
        (Value::DateTime(dt), DateTimeField::Quarter) => {
            Value::Int64(((dt.month() - 1) / 3 + 1) as i64)
        }
        (Value::DateTime(dt), DateTimeField::Month) => Value::Int64(dt.month() as i64),
        (Value::DateTime(dt), DateTimeField::Week(start_day)) => {
            Value::Int64(week_number_from_date(&dt.date(), *start_day))
        }
        (Value::DateTime(dt), DateTimeField::IsoWeek) => {
            Value::Int64(dt.date().iso_week().week() as i64)
        }
        (Value::DateTime(dt), DateTimeField::Day) => Value::Int64(dt.day() as i64),
        (Value::DateTime(dt), DateTimeField::DayOfWeek) => {
            Value::Int64(dt.weekday().num_days_from_sunday() as i64 + 1)
        }
        (Value::DateTime(dt), DateTimeField::DayOfYear) => Value::Int64(dt.ordinal() as i64),
        (Value::DateTime(dt), DateTimeField::Hour) => Value::Int64(dt.hour() as i64),
        (Value::DateTime(dt), DateTimeField::Minute) => Value::Int64(dt.minute() as i64),
        (Value::DateTime(dt), DateTimeField::Second) => Value::Int64(dt.second() as i64),
        (Value::DateTime(dt), DateTimeField::Millisecond) => {
            Value::Int64((dt.nanosecond() / 1_000_000) as i64)
        }
        (Value::DateTime(dt), DateTimeField::Microsecond) => {
            Value::Int64((dt.nanosecond() / 1_000) as i64)
        }
        (Value::DateTime(dt), DateTimeField::Nanosecond) => Value::Int64(dt.nanosecond() as i64),
        (Value::DateTime(dt), DateTimeField::Date) => Value::Date(dt.date()),
        (Value::DateTime(dt), DateTimeField::Time) => Value::Time(dt.time()),

        (Value::Timestamp(ts), DateTimeField::Year) => Value::Int64(ts.year() as i64),
        (Value::Timestamp(ts), DateTimeField::IsoYear) => {
            Value::Int64(ts.date_naive().iso_week().year() as i64)
        }
        (Value::Timestamp(ts), DateTimeField::Quarter) => {
            Value::Int64(((ts.month() - 1) / 3 + 1) as i64)
        }
        (Value::Timestamp(ts), DateTimeField::Month) => Value::Int64(ts.month() as i64),
        (Value::Timestamp(ts), DateTimeField::Week(start_day)) => {
            Value::Int64(week_number_from_date(&ts.date_naive(), *start_day))
        }
        (Value::Timestamp(ts), DateTimeField::IsoWeek) => {
            Value::Int64(ts.date_naive().iso_week().week() as i64)
        }
        (Value::Timestamp(ts), DateTimeField::Day) => Value::Int64(ts.day() as i64),
        (Value::Timestamp(ts), DateTimeField::DayOfWeek) => {
            Value::Int64(ts.weekday().num_days_from_sunday() as i64 + 1)
        }
        (Value::Timestamp(ts), DateTimeField::DayOfYear) => Value::Int64(ts.ordinal() as i64),
        (Value::Timestamp(ts), DateTimeField::Hour) => Value::Int64(ts.hour() as i64),
        (Value::Timestamp(ts), DateTimeField::Minute) => Value::Int64(ts.minute() as i64),
        (Value::Timestamp(ts), DateTimeField::Second) => Value::Int64(ts.second() as i64),
        (Value::Timestamp(ts), DateTimeField::Millisecond) => {
            Value::Int64((ts.nanosecond() / 1_000_000) as i64)
        }
        (Value::Timestamp(ts), DateTimeField::Microsecond) => {
            Value::Int64((ts.nanosecond() / 1_000) as i64)
        }
        (Value::Timestamp(ts), DateTimeField::Nanosecond) => Value::Int64(ts.nanosecond() as i64),
        (Value::Timestamp(ts), DateTimeField::Date) => Value::Date(ts.date_naive()),
        (Value::Timestamp(ts), DateTimeField::Time) => Value::Time(ts.time()),

        (Value::Time(t), DateTimeField::Hour) => Value::Int64(t.hour() as i64),
        (Value::Time(t), DateTimeField::Minute) => Value::Int64(t.minute() as i64),
        (Value::Time(t), DateTimeField::Second) => Value::Int64(t.second() as i64),
        (Value::Time(t), DateTimeField::Millisecond) => {
            Value::Int64((t.nanosecond() / 1_000_000) as i64)
        }
        (Value::Time(t), DateTimeField::Microsecond) => {
            Value::Int64((t.nanosecond() / 1_000) as i64)
        }
        (Value::Time(t), DateTimeField::Nanosecond) => Value::Int64(t.nanosecond() as i64),

        (Value::Interval(iv), DateTimeField::Year) => Value::Int64((iv.months / 12) as i64),
        (Value::Interval(iv), DateTimeField::Month) => Value::Int64((iv.months % 12) as i64),
        (Value::Interval(iv), DateTimeField::Day) => Value::Int64(iv.days as i64),
        (Value::Interval(iv), DateTimeField::Hour) => {
            const NANOS_PER_HOUR: i64 = 60 * 60 * 1_000_000_000;
            Value::Int64(iv.nanos / NANOS_PER_HOUR)
        }
        (Value::Interval(iv), DateTimeField::Minute) => {
            const NANOS_PER_MINUTE: i64 = 60 * 1_000_000_000;
            const NANOS_PER_HOUR: i64 = 60 * NANOS_PER_MINUTE;
            Value::Int64((iv.nanos % NANOS_PER_HOUR) / NANOS_PER_MINUTE)
        }
        (Value::Interval(iv), DateTimeField::Second) => {
            const NANOS_PER_SECOND: i64 = 1_000_000_000;
            const NANOS_PER_MINUTE: i64 = 60 * NANOS_PER_SECOND;
            Value::Int64((iv.nanos % NANOS_PER_MINUTE) / NANOS_PER_SECOND)
        }
        (Value::Interval(iv), DateTimeField::Millisecond) => {
            const NANOS_PER_MS: i64 = 1_000_000;
            const NANOS_PER_SECOND: i64 = 1_000_000_000;
            Value::Int64((iv.nanos % NANOS_PER_SECOND) / NANOS_PER_MS)
        }
        (Value::Interval(iv), DateTimeField::Microsecond) => {
            const NANOS_PER_US: i64 = 1_000;
            const NANOS_PER_MS: i64 = 1_000_000;
            Value::Int64((iv.nanos % NANOS_PER_MS) / NANOS_PER_US)
        }
        (Value::Interval(iv), DateTimeField::Nanosecond) => {
            const NANOS_PER_US: i64 = 1_000;
            Value::Int64(iv.nanos % NANOS_PER_US)
        }

        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveTime};
    use yachtsql_common::types::{DataType, IntervalValue};
    use yachtsql_storage::{Field, FieldMode, Schema};

    use super::*;

    fn make_date_table() -> Table {
        let schema = Schema::from_fields(vec![Field::new(
            "d".to_string(),
            DataType::Date,
            FieldMode::Nullable,
        )]);
        let mut table = Table::new(schema);
        table
            .push_row(vec![Value::Date(
                NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            )])
            .unwrap();
        table
            .push_row(vec![Value::Date(
                NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
            )])
            .unwrap();
        table
    }

    fn make_datetime_table() -> Table {
        let schema = Schema::from_fields(vec![Field::new(
            "dt".to_string(),
            DataType::DateTime,
            FieldMode::Nullable,
        )]);
        let mut table = Table::new(schema);
        table
            .push_row(vec![Value::DateTime(
                NaiveDate::from_ymd_opt(2024, 6, 15)
                    .unwrap()
                    .and_hms_opt(10, 30, 45)
                    .unwrap(),
            )])
            .unwrap();
        table
    }

    fn make_time_table() -> Table {
        let schema = Schema::from_fields(vec![Field::new(
            "t".to_string(),
            DataType::Time,
            FieldMode::Nullable,
        )]);
        let mut table = Table::new(schema);
        table
            .push_row(vec![Value::Time(
                NaiveTime::from_hms_opt(14, 30, 45).unwrap(),
            )])
            .unwrap();
        table
    }

    #[test]
    fn test_extract_year_from_date() {
        let table = make_date_table();
        let evaluator = ColumnarEvaluator::new(table.schema());
        let expr = Expr::Column {
            table: None,
            name: "d".to_string(),
            index: Some(0),
        };
        let result = eval_extract(&evaluator, DateTimeField::Year, &expr, &table).unwrap();
        assert_eq!(result.get_value(0), Value::Int64(2024));
        assert_eq!(result.get_value(1), Value::Int64(2023));
    }

    #[test]
    fn test_extract_month_from_date() {
        let table = make_date_table();
        let evaluator = ColumnarEvaluator::new(table.schema());
        let expr = Expr::Column {
            table: None,
            name: "d".to_string(),
            index: Some(0),
        };
        let result = eval_extract(&evaluator, DateTimeField::Month, &expr, &table).unwrap();
        assert_eq!(result.get_value(0), Value::Int64(6));
        assert_eq!(result.get_value(1), Value::Int64(12));
    }

    #[test]
    fn test_extract_day_from_date() {
        let table = make_date_table();
        let evaluator = ColumnarEvaluator::new(table.schema());
        let expr = Expr::Column {
            table: None,
            name: "d".to_string(),
            index: Some(0),
        };
        let result = eval_extract(&evaluator, DateTimeField::Day, &expr, &table).unwrap();
        assert_eq!(result.get_value(0), Value::Int64(15));
        assert_eq!(result.get_value(1), Value::Int64(31));
    }

    #[test]
    fn test_extract_hour_from_datetime() {
        let table = make_datetime_table();
        let evaluator = ColumnarEvaluator::new(table.schema());
        let expr = Expr::Column {
            table: None,
            name: "dt".to_string(),
            index: Some(0),
        };
        let result = eval_extract(&evaluator, DateTimeField::Hour, &expr, &table).unwrap();
        assert_eq!(result.get_value(0), Value::Int64(10));
    }

    #[test]
    fn test_extract_minute_from_datetime() {
        let table = make_datetime_table();
        let evaluator = ColumnarEvaluator::new(table.schema());
        let expr = Expr::Column {
            table: None,
            name: "dt".to_string(),
            index: Some(0),
        };
        let result = eval_extract(&evaluator, DateTimeField::Minute, &expr, &table).unwrap();
        assert_eq!(result.get_value(0), Value::Int64(30));
    }

    #[test]
    fn test_extract_second_from_time() {
        let table = make_time_table();
        let evaluator = ColumnarEvaluator::new(table.schema());
        let expr = Expr::Column {
            table: None,
            name: "t".to_string(),
            index: Some(0),
        };
        let result = eval_extract(&evaluator, DateTimeField::Second, &expr, &table).unwrap();
        assert_eq!(result.get_value(0), Value::Int64(45));
    }

    #[test]
    fn test_extract_null() {
        let schema = Schema::from_fields(vec![Field::new(
            "x".to_string(),
            DataType::Date,
            FieldMode::Nullable,
        )]);
        let mut table = Table::new(schema);
        table.push_row(vec![Value::Null]).unwrap();
        let evaluator = ColumnarEvaluator::new(table.schema());
        let expr = Expr::Column {
            table: None,
            name: "x".to_string(),
            index: Some(0),
        };
        let result = eval_extract(&evaluator, DateTimeField::Year, &expr, &table).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
    }

    #[test]
    fn test_extract_from_interval() {
        let schema = Schema::from_fields(vec![Field::new(
            "iv".to_string(),
            DataType::Interval,
            FieldMode::Nullable,
        )]);
        let mut table = Table::new(schema);
        table
            .push_row(vec![Value::Interval(IntervalValue {
                months: 14,
                days: 5,
                nanos: 3_600_000_000_000 + 30 * 60_000_000_000,
            })])
            .unwrap();
        let evaluator = ColumnarEvaluator::new(table.schema());
        let expr = Expr::Column {
            table: None,
            name: "iv".to_string(),
            index: Some(0),
        };

        let year_result = eval_extract(&evaluator, DateTimeField::Year, &expr, &table).unwrap();
        assert_eq!(year_result.get_value(0), Value::Int64(1));

        let month_result = eval_extract(&evaluator, DateTimeField::Month, &expr, &table).unwrap();
        assert_eq!(month_result.get_value(0), Value::Int64(2));

        let day_result = eval_extract(&evaluator, DateTimeField::Day, &expr, &table).unwrap();
        assert_eq!(day_result.get_value(0), Value::Int64(5));

        let hour_result = eval_extract(&evaluator, DateTimeField::Hour, &expr, &table).unwrap();
        assert_eq!(hour_result.get_value(0), Value::Int64(1));

        let minute_result = eval_extract(&evaluator, DateTimeField::Minute, &expr, &table).unwrap();
        assert_eq!(minute_result.get_value(0), Value::Int64(30));
    }

    #[test]
    fn test_extract_quarter_from_date() {
        let schema = Schema::from_fields(vec![Field::new(
            "d".to_string(),
            DataType::Date,
            FieldMode::Nullable,
        )]);
        let mut table = Table::new(schema);
        table
            .push_row(vec![Value::Date(
                NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            )])
            .unwrap();
        table
            .push_row(vec![Value::Date(
                NaiveDate::from_ymd_opt(2024, 4, 15).unwrap(),
            )])
            .unwrap();
        table
            .push_row(vec![Value::Date(
                NaiveDate::from_ymd_opt(2024, 7, 15).unwrap(),
            )])
            .unwrap();
        table
            .push_row(vec![Value::Date(
                NaiveDate::from_ymd_opt(2024, 10, 15).unwrap(),
            )])
            .unwrap();
        let evaluator = ColumnarEvaluator::new(table.schema());
        let expr = Expr::Column {
            table: None,
            name: "d".to_string(),
            index: Some(0),
        };
        let result = eval_extract(&evaluator, DateTimeField::Quarter, &expr, &table).unwrap();
        assert_eq!(result.get_value(0), Value::Int64(1));
        assert_eq!(result.get_value(1), Value::Int64(2));
        assert_eq!(result.get_value(2), Value::Int64(3));
        assert_eq!(result.get_value(3), Value::Int64(4));
    }
}
