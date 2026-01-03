use std::collections::HashSet;

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_storage::{Record, Schema};

use crate::catalog::TableConstraints;

pub struct ConstraintValidator<'a> {
    constraints: &'a TableConstraints,
    schema: &'a Schema,
}

impl<'a> ConstraintValidator<'a> {
    pub fn new(constraints: &'a TableConstraints, schema: &'a Schema) -> Self {
        Self {
            constraints,
            schema,
        }
    }

    pub fn validate_insert(&self, table_name: &str, rows: &[Record]) -> Result<()> {
        self.validate_not_null(table_name, rows)?;
        self.validate_primary_key(table_name, rows)?;
        self.validate_unique(table_name, rows)?;
        Ok(())
    }

    fn validate_not_null(&self, table_name: &str, rows: &[Record]) -> Result<()> {
        for row in rows {
            for col_name in &self.constraints.not_null_columns {
                let idx = match self.schema.field_index(col_name) {
                    Some(i) => i,
                    None => continue,
                };
                let is_null = row.values().get(idx).map(|v| v.is_null()).unwrap_or(true);
                if is_null {
                    return Err(Error::NotNullViolation {
                        table: table_name.to_string(),
                        column: col_name.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    fn validate_primary_key(&self, table_name: &str, rows: &[Record]) -> Result<()> {
        let pk = match &self.constraints.primary_key {
            Some(pk) => pk,
            None => return Ok(()),
        };

        let mut seen: HashSet<Vec<Value>> = HashSet::new();
        for row in rows {
            let mut key = Vec::new();
            for col in &pk.columns {
                let idx = match self.schema.field_index(col) {
                    Some(i) => i,
                    None => continue,
                };
                let val = row.values().get(idx).cloned().unwrap_or(Value::Null);
                if val.is_null() {
                    return Err(Error::PrimaryKeyNullViolation {
                        table: table_name.to_string(),
                        column: col.clone(),
                    });
                }
                key.push(val);
            }
            if !seen.insert(key.clone()) {
                return Err(Error::PrimaryKeyViolation {
                    table: table_name.to_string(),
                    value: format!("{:?}", key),
                });
            }
        }
        Ok(())
    }

    fn validate_unique(&self, table_name: &str, rows: &[Record]) -> Result<()> {
        for constraint in &self.constraints.unique_constraints {
            let mut seen: HashSet<Vec<Value>> = HashSet::new();
            for row in rows {
                let mut key = Vec::new();
                let mut has_null = false;
                for col in &constraint.columns {
                    let idx = match self.schema.field_index(col) {
                        Some(i) => i,
                        None => continue,
                    };
                    let val = row.values().get(idx).cloned().unwrap_or(Value::Null);
                    if val.is_null() {
                        has_null = true;
                        break;
                    }
                    key.push(val);
                }
                if has_null {
                    continue;
                }
                if !seen.insert(key.clone()) {
                    return Err(Error::UniqueViolation {
                        table: table_name.to_string(),
                        constraint: constraint.name.clone(),
                        value: format!("{:?}", key),
                    });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::DataType;
    use yachtsql_storage::Field;

    use super::*;
    use crate::catalog::{PrimaryKeyConstraint, UniqueConstraint};

    fn make_schema() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
            Field::nullable("email", DataType::String),
        ])
    }

    fn make_record(values: Vec<Value>) -> Record {
        Record::from_values(values)
    }

    #[test]
    fn test_validate_not_null_pass() {
        let schema = make_schema();
        let constraints = TableConstraints {
            not_null_columns: vec!["id".to_string()],
            ..Default::default()
        };
        let validator = ConstraintValidator::new(&constraints, &schema);
        let rows = vec![make_record(vec![
            Value::Int64(1),
            Value::String("test".to_string()),
            Value::Null,
        ])];
        assert!(validator.validate_insert("test_table", &rows).is_ok());
    }

    #[test]
    fn test_validate_not_null_fail() {
        let schema = make_schema();
        let constraints = TableConstraints {
            not_null_columns: vec!["id".to_string()],
            ..Default::default()
        };
        let validator = ConstraintValidator::new(&constraints, &schema);
        let rows = vec![make_record(vec![
            Value::Null,
            Value::String("test".to_string()),
            Value::Null,
        ])];
        let result = validator.validate_insert("test_table", &rows);
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::NotNullViolation { table, column } => {
                assert_eq!(table, "test_table");
                assert_eq!(column, "id");
            }
            _ => panic!("expected NotNullViolation"),
        }
    }

    #[test]
    fn test_validate_primary_key_pass() {
        let schema = make_schema();
        let constraints = TableConstraints {
            primary_key: Some(PrimaryKeyConstraint {
                name: Some("pk_id".to_string()),
                columns: vec!["id".to_string()],
            }),
            ..Default::default()
        };
        let validator = ConstraintValidator::new(&constraints, &schema);
        let rows = vec![
            make_record(vec![
                Value::Int64(1),
                Value::String("test1".to_string()),
                Value::Null,
            ]),
            make_record(vec![
                Value::Int64(2),
                Value::String("test2".to_string()),
                Value::Null,
            ]),
        ];
        assert!(validator.validate_insert("test_table", &rows).is_ok());
    }

    #[test]
    fn test_validate_primary_key_null_fail() {
        let schema = make_schema();
        let constraints = TableConstraints {
            primary_key: Some(PrimaryKeyConstraint {
                name: Some("pk_id".to_string()),
                columns: vec!["id".to_string()],
            }),
            ..Default::default()
        };
        let validator = ConstraintValidator::new(&constraints, &schema);
        let rows = vec![make_record(vec![
            Value::Null,
            Value::String("test".to_string()),
            Value::Null,
        ])];
        let result = validator.validate_insert("test_table", &rows);
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::PrimaryKeyNullViolation { table, column } => {
                assert_eq!(table, "test_table");
                assert_eq!(column, "id");
            }
            _ => panic!("expected PrimaryKeyNullViolation"),
        }
    }

    #[test]
    fn test_validate_primary_key_duplicate_fail() {
        let schema = make_schema();
        let constraints = TableConstraints {
            primary_key: Some(PrimaryKeyConstraint {
                name: Some("pk_id".to_string()),
                columns: vec!["id".to_string()],
            }),
            ..Default::default()
        };
        let validator = ConstraintValidator::new(&constraints, &schema);
        let rows = vec![
            make_record(vec![
                Value::Int64(1),
                Value::String("test1".to_string()),
                Value::Null,
            ]),
            make_record(vec![
                Value::Int64(1),
                Value::String("test2".to_string()),
                Value::Null,
            ]),
        ];
        let result = validator.validate_insert("test_table", &rows);
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::PrimaryKeyViolation { table, value } => {
                assert_eq!(table, "test_table");
                assert!(value.contains("1"));
            }
            _ => panic!("expected PrimaryKeyViolation"),
        }
    }

    #[test]
    fn test_validate_unique_pass() {
        let schema = make_schema();
        let constraints = TableConstraints {
            unique_constraints: vec![UniqueConstraint {
                name: "uq_email".to_string(),
                columns: vec!["email".to_string()],
            }],
            ..Default::default()
        };
        let validator = ConstraintValidator::new(&constraints, &schema);
        let rows = vec![
            make_record(vec![
                Value::Int64(1),
                Value::String("test1".to_string()),
                Value::String("a@test.com".to_string()),
            ]),
            make_record(vec![
                Value::Int64(2),
                Value::String("test2".to_string()),
                Value::String("b@test.com".to_string()),
            ]),
        ];
        assert!(validator.validate_insert("test_table", &rows).is_ok());
    }

    #[test]
    fn test_validate_unique_null_allowed() {
        let schema = make_schema();
        let constraints = TableConstraints {
            unique_constraints: vec![UniqueConstraint {
                name: "uq_email".to_string(),
                columns: vec!["email".to_string()],
            }],
            ..Default::default()
        };
        let validator = ConstraintValidator::new(&constraints, &schema);
        let rows = vec![
            make_record(vec![
                Value::Int64(1),
                Value::String("test1".to_string()),
                Value::Null,
            ]),
            make_record(vec![
                Value::Int64(2),
                Value::String("test2".to_string()),
                Value::Null,
            ]),
        ];
        assert!(validator.validate_insert("test_table", &rows).is_ok());
    }

    #[test]
    fn test_validate_unique_fail() {
        let schema = make_schema();
        let constraints = TableConstraints {
            unique_constraints: vec![UniqueConstraint {
                name: "uq_email".to_string(),
                columns: vec!["email".to_string()],
            }],
            ..Default::default()
        };
        let validator = ConstraintValidator::new(&constraints, &schema);
        let rows = vec![
            make_record(vec![
                Value::Int64(1),
                Value::String("test1".to_string()),
                Value::String("same@test.com".to_string()),
            ]),
            make_record(vec![
                Value::Int64(2),
                Value::String("test2".to_string()),
                Value::String("same@test.com".to_string()),
            ]),
        ];
        let result = validator.validate_insert("test_table", &rows);
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::UniqueViolation {
                table,
                constraint,
                value,
            } => {
                assert_eq!(table, "test_table");
                assert_eq!(constraint, "uq_email");
                assert!(value.contains("same@test.com"));
            }
            _ => panic!("expected UniqueViolation"),
        }
    }

    #[test]
    fn test_validate_composite_primary_key() {
        let schema = Schema::from_fields(vec![
            Field::nullable("a", DataType::Int64),
            Field::nullable("b", DataType::Int64),
            Field::nullable("c", DataType::String),
        ]);
        let constraints = TableConstraints {
            primary_key: Some(PrimaryKeyConstraint {
                name: Some("pk_ab".to_string()),
                columns: vec!["a".to_string(), "b".to_string()],
            }),
            ..Default::default()
        };
        let validator = ConstraintValidator::new(&constraints, &schema);
        let rows = vec![
            make_record(vec![
                Value::Int64(1),
                Value::Int64(1),
                Value::String("test".to_string()),
            ]),
            make_record(vec![
                Value::Int64(1),
                Value::Int64(2),
                Value::String("test".to_string()),
            ]),
        ];
        assert!(validator.validate_insert("test_table", &rows).is_ok());

        let duplicate_rows = vec![
            make_record(vec![
                Value::Int64(1),
                Value::Int64(1),
                Value::String("test1".to_string()),
            ]),
            make_record(vec![
                Value::Int64(1),
                Value::Int64(1),
                Value::String("test2".to_string()),
            ]),
        ];
        let result = validator.validate_insert("test_table", &duplicate_rows);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_composite_unique() {
        let schema = Schema::from_fields(vec![
            Field::nullable("a", DataType::Int64),
            Field::nullable("b", DataType::Int64),
            Field::nullable("c", DataType::String),
        ]);
        let constraints = TableConstraints {
            unique_constraints: vec![UniqueConstraint {
                name: "uq_ab".to_string(),
                columns: vec!["a".to_string(), "b".to_string()],
            }],
            ..Default::default()
        };
        let validator = ConstraintValidator::new(&constraints, &schema);
        let rows = vec![
            make_record(vec![
                Value::Int64(1),
                Value::Int64(1),
                Value::String("test".to_string()),
            ]),
            make_record(vec![
                Value::Int64(1),
                Value::Int64(2),
                Value::String("test".to_string()),
            ]),
        ];
        assert!(validator.validate_insert("test_table", &rows).is_ok());
    }

    #[test]
    fn test_no_constraints() {
        let schema = make_schema();
        let constraints = TableConstraints::default();
        let validator = ConstraintValidator::new(&constraints, &schema);
        let rows = vec![
            make_record(vec![Value::Null, Value::Null, Value::Null]),
            make_record(vec![Value::Null, Value::Null, Value::Null]),
        ];
        assert!(validator.validate_insert("test_table", &rows).is_ok());
    }

    #[test]
    fn test_empty_rows() {
        let schema = make_schema();
        let constraints = TableConstraints {
            not_null_columns: vec!["id".to_string()],
            primary_key: Some(PrimaryKeyConstraint {
                name: Some("pk_id".to_string()),
                columns: vec!["id".to_string()],
            }),
            unique_constraints: vec![UniqueConstraint {
                name: "uq_email".to_string(),
                columns: vec!["email".to_string()],
            }],
            ..Default::default()
        };
        let validator = ConstraintValidator::new(&constraints, &schema);
        let rows: Vec<Record> = vec![];
        assert!(validator.validate_insert("test_table", &rows).is_ok());
    }

    #[test]
    fn test_unknown_column_in_constraint() {
        let schema = make_schema();
        let constraints = TableConstraints {
            not_null_columns: vec!["nonexistent".to_string()],
            ..Default::default()
        };
        let validator = ConstraintValidator::new(&constraints, &schema);
        let rows = vec![make_record(vec![
            Value::Int64(1),
            Value::String("test".to_string()),
            Value::Null,
        ])];
        assert!(validator.validate_insert("test_table", &rows).is_ok());
    }
}
