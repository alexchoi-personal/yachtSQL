# yachtsql-datafusion-functions

BigQuery-compatible functions for DataFusion.

## Features

Registers 150+ BigQuery functions with DataFusion's function registry:

- **Scalar Functions**: String, Math, DateTime, Conditional, Conversion
- **Aggregate Functions**: COUNT, SUM, AVG, ARRAY_AGG, STRING_AGG, etc.
- **Window Functions**: ROW_NUMBER, RANK, LAG, LEAD, etc.

## Usage

```rust
use datafusion::prelude::SessionContext;
use yachtsql_datafusion_functions::BigQueryFunctionRegistry;

let ctx = SessionContext::new();
BigQueryFunctionRegistry::register_all(&ctx);

// Now you can use BigQuery functions in SQL
let df = ctx.sql("SELECT UPPER(name), DATE_ADD(created_at, INTERVAL 1 DAY) FROM users").await?;
```
