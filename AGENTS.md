# Project Instructions

1. Don't simply put `#[ignore]` on failing tests.
   When a test fails because a feature is not yet implemented, implement the missing feature rather than adding
   `#[ignore]` to skip the test.

2. Use `debug_eprintln!` instead of `eprintln!`. Use headers such as `debug_eprintln!("[executor::dml::insert] ..");` on
   executor/dml/insert.rs.

3. Do not write comments unless instructed. Simply write code.

4. Match all enum variants explicitly and exhaustively.

5. Avoid nested if statements.

6. Prefer `match` pattern matching on enum and tuple types.

7. Always use panic! where the invariants fail for easy debugging.

8. Avoid writing nested if/else beyond two layers deep.

9. Always use `assert_batch_records_eq!` from `yachtsql_arrow` for asserting query results.

```rust
use yachtsql_arrow::{assert_batch_records_eq, bytes, date, timestamp, datetime, numeric, array, interval, TestValue};

assert_batch_records_eq!(
    result,
    [
       [1, 1.0, "New York", date(2025, 1, 15), numeric("2.13")],
       [2, 1.0, "Los Angeles", date(2025, 1, 16), numeric("4.11")],
       [3, 1.0, "New York", timestamp(2025, 10, 1, 12, 0, 0), numeric("5.00")],
    ]
);
```

Available helper functions:
- `bytes(b"data")` - binary data
- `date(year, month, day)` - DATE values
- `timestamp(year, month, day, hour, min, sec)` - TIMESTAMP values
- `datetime(year, month, day, hour, min, sec)` - DATETIME values
- `numeric("123.45")` - NUMERIC/DECIMAL values
- `array(vec![...])` - ARRAY values
- `interval()` - INTERVAL values
- Primitives work directly: `1`, `1.0`, `"string"`, `true`, `false`
- Use `TestValue::Null` for NULL values

10. Don't include
    `Generated with [Claude Code](https://claude.com/claude-code) Co-Authored-By: Claude <noreply@anthropic.com>` when
    making a commit.

11. Use cargo nextest, not cargo test.

12. Run llvm-cov each time you finish a task, then reach to coverage to 100%, no exceptions. Write unit tests or write execute_sql() tests in tests/bigquery
