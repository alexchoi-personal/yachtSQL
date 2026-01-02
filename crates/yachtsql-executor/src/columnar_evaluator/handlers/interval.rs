#![coverage(off)]

use yachtsql_common::error::Result;
use yachtsql_common::types::{IntervalValue, Value};
use yachtsql_ir::DateTimeField;
use yachtsql_storage::Column;

pub fn eval_interval(value: i64, leading_field: DateTimeField, row_count: usize) -> Result<Column> {
    let iv = match leading_field {
        DateTimeField::Year => IntervalValue {
            months: (value * 12) as i32,
            days: 0,
            nanos: 0,
        },
        DateTimeField::Month => IntervalValue {
            months: value as i32,
            days: 0,
            nanos: 0,
        },
        DateTimeField::Day => IntervalValue {
            months: 0,
            days: value as i32,
            nanos: 0,
        },
        DateTimeField::Hour => IntervalValue {
            months: 0,
            days: 0,
            nanos: value * 3_600_000_000_000,
        },
        DateTimeField::Minute => IntervalValue {
            months: 0,
            days: 0,
            nanos: value * 60_000_000_000,
        },
        DateTimeField::Second => IntervalValue {
            months: 0,
            days: 0,
            nanos: value * 1_000_000_000,
        },
        DateTimeField::Millisecond => IntervalValue {
            months: 0,
            days: 0,
            nanos: value * 1_000_000,
        },
        DateTimeField::Microsecond => IntervalValue {
            months: 0,
            days: 0,
            nanos: value * 1_000,
        },
        DateTimeField::Nanosecond => IntervalValue {
            months: 0,
            days: 0,
            nanos: value,
        },
        DateTimeField::IsoYear
        | DateTimeField::Quarter
        | DateTimeField::Week(_)
        | DateTimeField::IsoWeek
        | DateTimeField::DayOfWeek
        | DateTimeField::DayOfYear
        | DateTimeField::Date
        | DateTimeField::Time
        | DateTimeField::Datetime
        | DateTimeField::Timezone
        | DateTimeField::TimezoneHour
        | DateTimeField::TimezoneMinute => IntervalValue {
            months: 0,
            days: 0,
            nanos: 0,
        },
    };
    Ok(Column::from_values(&vec![Value::Interval(iv); row_count]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eval_interval_year() {
        let result = eval_interval(2, DateTimeField::Year, 3).unwrap();
        assert_eq!(result.len(), 3);
        let val = result.get_value(0);
        match val {
            Value::Interval(iv) => {
                assert_eq!(iv.months, 24);
                assert_eq!(iv.days, 0);
                assert_eq!(iv.nanos, 0);
            }
            _ => panic!("Expected Interval"),
        }
    }

    #[test]
    fn test_eval_interval_month() {
        let result = eval_interval(6, DateTimeField::Month, 2).unwrap();
        assert_eq!(result.len(), 2);
        let val = result.get_value(0);
        match val {
            Value::Interval(iv) => {
                assert_eq!(iv.months, 6);
                assert_eq!(iv.days, 0);
                assert_eq!(iv.nanos, 0);
            }
            _ => panic!("Expected Interval"),
        }
    }

    #[test]
    fn test_eval_interval_day() {
        let result = eval_interval(7, DateTimeField::Day, 1).unwrap();
        let val = result.get_value(0);
        match val {
            Value::Interval(iv) => {
                assert_eq!(iv.months, 0);
                assert_eq!(iv.days, 7);
                assert_eq!(iv.nanos, 0);
            }
            _ => panic!("Expected Interval"),
        }
    }

    #[test]
    fn test_eval_interval_hour() {
        let result = eval_interval(3, DateTimeField::Hour, 1).unwrap();
        let val = result.get_value(0);
        match val {
            Value::Interval(iv) => {
                assert_eq!(iv.months, 0);
                assert_eq!(iv.days, 0);
                assert_eq!(iv.nanos, 3 * 3_600_000_000_000);
            }
            _ => panic!("Expected Interval"),
        }
    }

    #[test]
    fn test_eval_interval_minute() {
        let result = eval_interval(30, DateTimeField::Minute, 1).unwrap();
        let val = result.get_value(0);
        match val {
            Value::Interval(iv) => {
                assert_eq!(iv.months, 0);
                assert_eq!(iv.days, 0);
                assert_eq!(iv.nanos, 30 * 60_000_000_000);
            }
            _ => panic!("Expected Interval"),
        }
    }

    #[test]
    fn test_eval_interval_second() {
        let result = eval_interval(45, DateTimeField::Second, 1).unwrap();
        let val = result.get_value(0);
        match val {
            Value::Interval(iv) => {
                assert_eq!(iv.months, 0);
                assert_eq!(iv.days, 0);
                assert_eq!(iv.nanos, 45 * 1_000_000_000);
            }
            _ => panic!("Expected Interval"),
        }
    }
}
