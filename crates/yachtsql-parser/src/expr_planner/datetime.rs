#![coverage(off)]

use sqlparser::ast;
use yachtsql_common::error::{Error, Result};
use yachtsql_ir::{DateTimeField, WeekStartDay};

pub fn is_date_part_keyword(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "YEAR"
            | "ISOYEAR"
            | "QUARTER"
            | "MONTH"
            | "WEEK"
            | "ISOWEEK"
            | "DAY"
            | "DAYOFWEEK"
            | "DAYOFYEAR"
            | "HOUR"
            | "MINUTE"
            | "SECOND"
            | "MILLISECOND"
            | "MICROSECOND"
            | "NANOSECOND"
            | "DATE"
            | "TIME"
            | "DATETIME"
    )
}

pub fn plan_datetime_field(field: &ast::DateTimeField) -> Result<DateTimeField> {
    match field {
        ast::DateTimeField::Year => Ok(DateTimeField::Year),
        ast::DateTimeField::Month => Ok(DateTimeField::Month),
        ast::DateTimeField::Week(opt_ident) => {
            let start_day = match opt_ident {
                Some(ident) => match ident.value.to_uppercase().as_str() {
                    "SUNDAY" => WeekStartDay::Sunday,
                    "MONDAY" => WeekStartDay::Monday,
                    "TUESDAY" => WeekStartDay::Tuesday,
                    "WEDNESDAY" => WeekStartDay::Wednesday,
                    "THURSDAY" => WeekStartDay::Thursday,
                    "FRIDAY" => WeekStartDay::Friday,
                    "SATURDAY" => WeekStartDay::Saturday,
                    _ => {
                        return Err(Error::unsupported(format!(
                            "Unsupported WEEK start day: {}",
                            ident.value
                        )));
                    }
                },
                None => WeekStartDay::Sunday,
            };
            Ok(DateTimeField::Week(start_day))
        }
        ast::DateTimeField::Day => Ok(DateTimeField::Day),
        ast::DateTimeField::DayOfWeek => Ok(DateTimeField::DayOfWeek),
        ast::DateTimeField::DayOfYear => Ok(DateTimeField::DayOfYear),
        ast::DateTimeField::Hour => Ok(DateTimeField::Hour),
        ast::DateTimeField::Minute => Ok(DateTimeField::Minute),
        ast::DateTimeField::Second => Ok(DateTimeField::Second),
        ast::DateTimeField::Millisecond | ast::DateTimeField::Milliseconds => {
            Ok(DateTimeField::Millisecond)
        }
        ast::DateTimeField::Microsecond | ast::DateTimeField::Microseconds => {
            Ok(DateTimeField::Microsecond)
        }
        ast::DateTimeField::Nanosecond | ast::DateTimeField::Nanoseconds => {
            Ok(DateTimeField::Nanosecond)
        }
        ast::DateTimeField::Date => Ok(DateTimeField::Date),
        ast::DateTimeField::Time => Ok(DateTimeField::Time),
        ast::DateTimeField::Datetime => Ok(DateTimeField::Datetime),
        ast::DateTimeField::Quarter => Ok(DateTimeField::Quarter),
        ast::DateTimeField::Isoyear => Ok(DateTimeField::IsoYear),
        ast::DateTimeField::IsoWeek => Ok(DateTimeField::IsoWeek),
        ast::DateTimeField::Timezone => Ok(DateTimeField::Timezone),
        ast::DateTimeField::TimezoneHour => Ok(DateTimeField::TimezoneHour),
        ast::DateTimeField::TimezoneMinute => Ok(DateTimeField::TimezoneMinute),
        _ => Err(Error::unsupported(format!(
            "Unsupported datetime field: {:?}",
            field
        ))),
    }
}
