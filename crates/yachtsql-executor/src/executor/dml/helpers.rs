#![coverage(off)]

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};

pub(crate) fn coerce_value(value: Value, target_type: &DataType) -> Result<Value> {
    match (&value, target_type) {
        (Value::String(s), DataType::Date) => {
            let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|e| Error::InvalidQuery(format!("Invalid date string: {}", e)))?;
            Ok(Value::Date(date))
        }
        (Value::String(s), DataType::Time) => {
            let time = NaiveTime::parse_from_str(s, "%H:%M:%S")
                .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.f"))
                .map_err(|e| Error::InvalidQuery(format!("Invalid time string: {}", e)))?;
            Ok(Value::Time(time))
        }
        (Value::String(s), DataType::Timestamp) => {
            let dt = DateTime::parse_from_rfc3339(s)
                .map(|d| d.with_timezone(&Utc))
                .or_else(|_| {
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                        .or_else(|_| {
                            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                        })
                        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                        .or_else(|_| {
                            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                        })
                        .map(|ndt| ndt.and_utc())
                })
                .map_err(|e| Error::InvalidQuery(format!("Invalid timestamp string: {}", e)))?;
            Ok(Value::Timestamp(dt))
        }
        (Value::String(s), DataType::DateTime) => {
            let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
                .map_err(|e| Error::InvalidQuery(format!("Invalid datetime string: {}", e)))?;
            Ok(Value::DateTime(dt))
        }
        (Value::Int64(n), DataType::Float64) => {
            Ok(Value::Float64(ordered_float::OrderedFloat(*n as f64)))
        }
        (Value::Float64(f), DataType::Int64) => Ok(Value::Int64(f.0 as i64)),
        (Value::Struct(fields), DataType::Struct(target_fields)) => {
            let mut coerced_fields = Vec::with_capacity(fields.len());
            for (i, (_, val)) in fields.iter().enumerate() {
                let (new_name, new_type) = if i < target_fields.len() {
                    (target_fields[i].name.clone(), &target_fields[i].data_type)
                } else {
                    (format!("_field{}", i), &DataType::Unknown)
                };
                let coerced_val = coerce_value(val.clone(), new_type)?;
                coerced_fields.push((new_name, coerced_val));
            }
            Ok(Value::Struct(coerced_fields))
        }
        (Value::Array(elements), DataType::Array(element_type)) => {
            let coerced_elements: Result<Vec<_>> = elements
                .iter()
                .map(|elem| coerce_value(elem.clone(), element_type))
                .collect();
            Ok(Value::Array(coerced_elements?))
        }
        _ => Ok(value),
    }
}

pub(crate) fn update_struct_field(
    current_value: &Value,
    field_path: &[&str],
    new_value: Value,
) -> Value {
    if field_path.is_empty() {
        return new_value;
    }

    let field_name = field_path[0];
    let remaining_path = &field_path[1..];

    match current_value {
        Value::Struct(fields) => {
            let mut new_fields: Vec<(String, Value)> = Vec::with_capacity(fields.len());
            let mut found = false;

            for (name, val) in fields {
                if name.eq_ignore_ascii_case(field_name) {
                    found = true;
                    if remaining_path.is_empty() {
                        new_fields.push((name.clone(), new_value.clone()));
                    } else {
                        let updated_nested =
                            update_struct_field(val, remaining_path, new_value.clone());
                        new_fields.push((name.clone(), updated_nested));
                    }
                } else {
                    new_fields.push((name.clone(), val.clone()));
                }
            }

            if !found && remaining_path.is_empty() {
                new_fields.push((field_name.to_string(), new_value));
            }

            Value::Struct(new_fields)
        }
        _ => current_value.clone(),
    }
}

pub(crate) fn parse_assignment_column(column: &str) -> (String, Vec<String>) {
    let parts: Vec<&str> = column.split('.').collect();
    if parts.len() > 1 {
        (
            parts[0].to_string(),
            parts[1..].iter().map(|s| s.to_string()).collect(),
        )
    } else {
        (column.to_string(), vec![])
    }
}
