# Constraint System Architecture

This document describes the design for constraint enforcement in YachtSQL.

## Overview

Constraints are rules that enforce data integrity at the database level. YachtSQL will support the following constraint types:

- NOT NULL
- UNIQUE
- PRIMARY KEY
- CHECK

## Data Structures

### ConstraintDefinition

```rust
// In yachtsql-ir/src/plan/ddl.rs (existing)
pub struct TableConstraint {
    pub name: Option<String>,
    pub constraint_type: ConstraintType,
}

pub enum ConstraintType {
    PrimaryKey { columns: Vec<String> },
    ForeignKey { columns: Vec<String>, references_table: String, references_columns: Vec<String> },
    Unique { columns: Vec<String> },
    Check { expr: Expr },
}
```

### Constraint Storage in Catalog

```rust
// In yachtsql-executor/src/catalog.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConstraints {
    pub primary_key: Option<PrimaryKeyConstraint>,
    pub unique_constraints: Vec<UniqueConstraint>,
    pub check_constraints: Vec<CheckConstraint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrimaryKeyConstraint {
    pub name: Option<String>,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueConstraint {
    pub name: Option<String>,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckConstraint {
    pub name: Option<String>,
    pub expr: Expr,
}
```

### Constraint Error Types

```rust
// In yachtsql-common/src/error.rs
pub enum Error {
    // Existing variants...

    NotNullViolation {
        table: String,
        column: String,
    },
    UniqueViolation {
        table: String,
        columns: Vec<String>,
        constraint_name: Option<String>,
    },
    PrimaryKeyViolation {
        table: String,
        columns: Vec<String>,
    },
    CheckViolation {
        table: String,
        constraint_name: Option<String>,
        expr_display: String,
    },
}
```

## NOT NULL Constraint Enforcement

### Storage

NOT NULL is stored as `FieldMode::Required` in the schema:

```rust
// yachtsql-storage/src/schema.rs
pub enum FieldMode {
    Nullable,
    Required,  // NOT NULL
    Repeated,
}
```

### Validation Flow

1. During INSERT:
   - Before pushing row to table, check each column's `FieldMode`
   - If `Required` and value is `Value::Null`, return `NotNullViolation` error

2. During UPDATE:
   - When computing new row values, validate NOT NULL before applying update

```rust
fn validate_not_null(
    schema: &Schema,
    row: &[Value],
    table_name: &str,
) -> Result<()> {
    for (idx, field) in schema.fields().iter().enumerate() {
        match field.mode {
            FieldMode::Required => {
                if row.get(idx).map_or(true, |v| v.is_null()) {
                    return Err(Error::NotNullViolation {
                        table: table_name.to_string(),
                        column: field.name.clone(),
                    });
                }
            }
            FieldMode::Nullable | FieldMode::Repeated => {}
        }
    }
    Ok(())
}
```

## UNIQUE Constraint Enforcement

### Index Structure

```rust
pub struct UniqueIndex {
    constraint_name: Option<String>,
    column_indices: Vec<usize>,
    values: HashSet<Vec<Value>>,
}

impl UniqueIndex {
    pub fn insert(&mut self, key: Vec<Value>) -> Result<(), UniqueViolation> {
        if key.iter().any(|v| v.is_null()) {
            return Ok(());
        }
        if !self.values.insert(key.clone()) {
            return Err(UniqueViolation { key });
        }
        Ok(())
    }

    pub fn remove(&mut self, key: &[Value]) {
        self.values.remove(key);
    }
}
```

### Validation Flow

1. Build index from existing table data on constraint creation
2. During INSERT:
   - Extract key columns from new row
   - Check against existing index + batch duplicates
   - Insert into index if valid

3. During UPDATE:
   - Remove old key from index
   - Check new key against index + batch duplicates
   - Insert new key into index

4. During DELETE:
   - Remove key from index

### Batch Duplicate Detection

When inserting multiple rows, detect duplicates within the batch:

```rust
fn check_batch_duplicates(
    rows: &[Vec<Value>],
    key_indices: &[usize],
    constraint_name: Option<&str>,
) -> Result<()> {
    let mut seen: HashSet<Vec<Value>> = HashSet::new();

    for row in rows {
        let key: Vec<Value> = key_indices
            .iter()
            .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
            .collect();

        if key.iter().any(|v| v.is_null()) {
            continue;
        }

        if !seen.insert(key.clone()) {
            return Err(Error::UniqueViolation {
                table: table_name.to_string(),
                columns: key_column_names.clone(),
                constraint_name: constraint_name.map(String::from),
            });
        }
    }
    Ok(())
}
```

## PRIMARY KEY Constraint Enforcement

PRIMARY KEY = NOT NULL + UNIQUE for all key columns.

### Storage

```rust
// In Catalog
pub struct TableMetadata {
    pub constraints: TableConstraints,
}

// PK is both tracked in TableConstraints and enforced via FieldMode::Required
```

### Validation Flow

1. During CREATE TABLE with PRIMARY KEY:
   - Set all PK columns to `FieldMode::Required`
   - Create unique index for PK columns
   - Store PK constraint in catalog

2. During INSERT:
   - Validate NOT NULL for all PK columns (via FieldMode)
   - Validate uniqueness via PK index

3. During UPDATE:
   - Prevent update of PK columns (or re-validate uniqueness if allowed)

### ALTER TABLE ADD PRIMARY KEY

```rust
fn add_primary_key(
    table: &mut Table,
    columns: &[String],
) -> Result<()> {
    // 1. Verify no NULLs exist in columns
    for col_name in columns {
        let col_idx = table.schema().field_index(col_name)
            .ok_or(Error::ColumnNotFound(col_name.clone()))?;
        let column = table.column(col_idx).unwrap();

        for i in 0..column.len() {
            if column.get_value(i).is_null() {
                return Err(Error::invalid_query(
                    format!("Column {} contains NULL values", col_name)
                ));
            }
        }
    }

    // 2. Verify uniqueness of existing data
    let mut seen: HashSet<Vec<Value>> = HashSet::new();
    for row_idx in 0..table.row_count() {
        let key: Vec<Value> = columns.iter()
            .filter_map(|c| table.schema().field_index(c))
            .map(|i| table.column(i).unwrap().get_value(row_idx))
            .collect();

        if !seen.insert(key) {
            return Err(Error::invalid_query(
                "Duplicate values exist for primary key columns"
            ));
        }
    }

    // 3. Set columns to NOT NULL
    for col_name in columns {
        table.set_column_not_null(col_name)?;
    }

    Ok(())
}
```

## CHECK Constraint Enforcement

### Validation Flow

```rust
fn validate_check_constraint(
    constraint: &CheckConstraint,
    row: &Record,
    schema: &Schema,
    table_name: &str,
) -> Result<()> {
    let evaluator = ValueEvaluator::new(schema);
    let result = evaluator.evaluate(&constraint.expr, row)?;

    match result {
        Value::Bool(true) | Value::Null => Ok(()),
        Value::Bool(false) => Err(Error::CheckViolation {
            table: table_name.to_string(),
            constraint_name: constraint.name.clone(),
            expr_display: format!("{:?}", constraint.expr),
        }),
        _ => Err(Error::invalid_query(
            "CHECK constraint expression must evaluate to BOOL"
        )),
    }
}
```

### INSERT/UPDATE Integration

```rust
fn validate_all_constraints(
    catalog: &Catalog,
    table_name: &str,
    row: &[Value],
    schema: &Schema,
) -> Result<()> {
    // 1. NOT NULL validation
    validate_not_null(schema, row, table_name)?;

    // 2. CHECK constraints
    let record = Record::from_values(row.to_vec());
    if let Some(constraints) = catalog.get_table_constraints(table_name) {
        for check in &constraints.check_constraints {
            validate_check_constraint(check, &record, schema, table_name)?;
        }
    }

    Ok(())
}
```

## Constraint Interaction with DML

### INSERT Execution

```rust
// In yachtsql-executor/src/executor/dml/insert.rs
pub fn execute_insert(...) -> Result<Table> {
    let constraints = catalog.get_table_constraints(table_name);

    // Collect all rows first
    let all_rows: Vec<Vec<Value>> = collect_insert_rows(...)?;

    // 1. Validate NOT NULL for each row
    for row in &all_rows {
        validate_not_null(&target_schema, row, table_name)?;
    }

    // 2. Validate CHECK constraints for each row
    if let Some(ref cons) = constraints {
        for row in &all_rows {
            let record = Record::from_values(row.clone());
            for check in &cons.check_constraints {
                validate_check_constraint(check, &record, &target_schema, table_name)?;
            }
        }
    }

    // 3. Check batch duplicates for UNIQUE/PK
    if let Some(ref cons) = constraints {
        if let Some(ref pk) = cons.primary_key {
            check_batch_duplicates(&all_rows, &pk.column_indices())?;
        }
        for unique in &cons.unique_constraints {
            check_batch_duplicates(&all_rows, &unique.column_indices())?;
        }
    }

    // 4. Check against existing data
    // (done via unique index lookup)

    // 5. Insert all rows
    let target = catalog.get_table_mut(table_name)?;
    for row in all_rows {
        target.push_row(row)?;
    }

    Ok(Table::empty(Schema::new()))
}
```

### UPDATE Execution

```rust
// In yachtsql-executor/src/executor/dml/update.rs
pub fn execute_update(...) -> Result<Table> {
    // 1. Compute new values for each matching row
    let updates: Vec<(usize, Vec<Value>)> = compute_updates(...)?;

    // 2. Validate constraints on new values
    for (_, new_row) in &updates {
        validate_not_null(&schema, new_row, table_name)?;
        validate_check_constraints(constraints, new_row, &schema, table_name)?;
    }

    // 3. Check uniqueness (batch + existing)
    check_update_uniqueness(&updates, constraints, table)?;

    // 4. Apply updates
    for (idx, new_row) in updates {
        table.update_row(idx, new_row)?;
    }

    Ok(Table::empty(Schema::new()))
}
```

### DELETE Execution

DELETE does not require constraint validation but must update unique indices:

```rust
pub fn execute_delete(...) -> Result<Table> {
    // 1. Find rows matching filter
    let indices_to_delete: Vec<usize> = find_matching_rows(...)?;

    // 2. Update unique indices (remove keys)
    update_unique_indices_for_delete(&indices_to_delete, constraints, table)?;

    // 3. Remove rows (in reverse order to maintain indices)
    for idx in indices_to_delete.into_iter().rev() {
        table.remove_row(idx);
    }

    Ok(Table::empty(Schema::new()))
}
```

## Implementation Order

1. Add constraint error types to `yachtsql-common/src/error.rs`
2. Add `TableConstraints` storage to Catalog
3. Implement NOT NULL validation in INSERT
4. Implement NOT NULL validation in UPDATE
5. Implement CHECK constraint validation
6. Implement UNIQUE constraint with index structure
7. Implement PRIMARY KEY constraint
8. Add batch duplicate detection
9. Implement ALTER TABLE ADD/DROP CONSTRAINT

## Testing Strategy

1. Unit tests for each constraint type
2. Integration tests for constraint + DML combinations
3. Tests for batch operations with constraints
4. Error message validation tests
5. Edge cases: NULL handling in UNIQUE, multi-column constraints
