#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{IntervalValue, Value};
use yachtsql_ir::DateTimeField;
use yachtsql_storage::Column;

pub fn eval_interval(value: i64, leading_field: DateTimeField, row_count: usize) -> Result<Column> {
    let iv = match leading_field {
        DateTimeField::Year => {
            let months = value
                .checked_mul(12)
                .and_then(|v| i32::try_from(v).ok())
                .ok_or_else(|| Error::interval_overflow("INTERVAL YEAR", value.to_string()))?;
            IntervalValue {
                months,
                days: 0,
                nanos: 0,
            }
        }
        DateTimeField::Month => {
            let months = i32::try_from(value)
                .map_err(|_| Error::interval_overflow("INTERVAL MONTH", value.to_string()))?;
            IntervalValue {
                months,
                days: 0,
                nanos: 0,
            }
        }
        DateTimeField::Day => {
            let days = i32::try_from(value)
                .map_err(|_| Error::interval_overflow("INTERVAL DAY", value.to_string()))?;
            IntervalValue {
                months: 0,
                days,
                nanos: 0,
            }
        }
        DateTimeField::Hour => {
            let nanos = value
                .checked_mul(3_600_000_000_000)
                .ok_or_else(|| Error::interval_overflow("INTERVAL HOUR", value.to_string()))?;
            IntervalValue {
                months: 0,
                days: 0,
                nanos,
            }
        }
        DateTimeField::Minute => {
            let nanos = value
                .checked_mul(60_000_000_000)
                .ok_or_else(|| Error::interval_overflow("INTERVAL MINUTE", value.to_string()))?;
            IntervalValue {
                months: 0,
                days: 0,
                nanos,
            }
        }
        DateTimeField::Second => {
            let nanos = value
                .checked_mul(1_000_000_000)
                .ok_or_else(|| Error::interval_overflow("INTERVAL SECOND", value.to_string()))?;
            IntervalValue {
                months: 0,
                days: 0,
                nanos,
            }
        }
        DateTimeField::Millisecond => {
            let nanos = value.checked_mul(1_000_000).ok_or_else(|| {
                Error::interval_overflow("INTERVAL MILLISECOND", value.to_string())
            })?;
            IntervalValue {
                months: 0,
                days: 0,
                nanos,
            }
        }
        DateTimeField::Microsecond => {
            let nanos = value.checked_mul(1_000).ok_or_else(|| {
                Error::interval_overflow("INTERVAL MICROSECOND", value.to_string())
            })?;
            IntervalValue {
                months: 0,
                days: 0,
                nanos,
            }
        }
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
