# yachtsql-arrow

Arrow/DataFusion utilities for YachtSQL.

## Features

- Re-exports Arrow and DataFusion types for convenience
- Test utilities including `assert_batch_records_eq!` macro for testing RecordBatch results

## Usage

```rust
use yachtsql_arrow::*;

// Assert RecordBatch equals expected records
let result = session.execute_sql("SELECT id, name FROM users").await?;
assert_batch_records_eq!(result, [
    [1, "Alice"],
    [2, "Bob"],
    [3, "Charlie"],
]);

// Supports null values
assert_batch_records_eq!(result, [
    [1, "Alice"],
    [null, "Bob"],
    [3, null],
]);
```
