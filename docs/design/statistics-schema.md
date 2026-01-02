# Statistics Schema for Optimizer

This document describes the design for table and column statistics used by the query optimizer.

## Overview

Statistics provide information about data distribution that the optimizer uses to:

- Estimate cardinality (row counts after operations)
- Choose join algorithms and order
- Select index usage strategies
- Estimate selectivity of predicates

## Data Structures

### TableStats

```rust
// In yachtsql-optimizer/src/stats.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStats {
    pub table_name: String,
    pub row_count: u64,
    pub total_size_bytes: u64,
    pub last_analyzed: chrono::DateTime<chrono::Utc>,
    pub columns: IndexMap<String, ColumnStats>,
}

impl TableStats {
    pub fn empty(table_name: &str) -> Self {
        Self {
            table_name: table_name.to_string(),
            row_count: 0,
            total_size_bytes: 0,
            last_analyzed: chrono::Utc::now(),
            columns: IndexMap::new(),
        }
    }

    pub fn column_stats(&self, name: &str) -> Option<&ColumnStats> {
        self.columns.get(name)
    }

    pub fn selectivity(&self, predicate: &Expr) -> f64 {
        estimate_selectivity(predicate, self)
    }

    pub fn estimated_rows_after_filter(&self, predicate: &Expr) -> u64 {
        let sel = self.selectivity(predicate);
        (self.row_count as f64 * sel).ceil() as u64
    }
}
```

### ColumnStats

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    pub column_name: String,
    pub data_type: DataType,
    pub null_count: u64,
    pub distinct_count: u64,
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
    pub avg_width_bytes: u32,
    pub histogram: Option<Histogram>,
    pub most_common_values: Option<MostCommonValues>,
}

impl ColumnStats {
    pub fn null_fraction(&self, total_rows: u64) -> f64 {
        if total_rows == 0 {
            return 0.0;
        }
        self.null_count as f64 / total_rows as f64
    }

    pub fn distinct_fraction(&self, total_rows: u64) -> f64 {
        if total_rows == 0 {
            return 0.0;
        }
        (self.distinct_count as f64 / total_rows as f64).min(1.0)
    }

    pub fn selectivity_equality(&self, value: &Value, total_rows: u64) -> f64 {
        if self.distinct_count == 0 || total_rows == 0 {
            return DEFAULT_EQUALITY_SELECTIVITY;
        }

        if let Some(ref mcv) = self.most_common_values {
            if let Some(freq) = mcv.frequency(value) {
                return freq;
            }
        }

        if let Some(ref hist) = self.histogram {
            return hist.selectivity_equality(value, self.distinct_count);
        }

        1.0 / self.distinct_count as f64
    }

    pub fn selectivity_range(&self, low: Option<&Value>, high: Option<&Value>) -> f64 {
        if let Some(ref hist) = self.histogram {
            return hist.selectivity_range(low, high);
        }
        DEFAULT_RANGE_SELECTIVITY
    }
}

const DEFAULT_EQUALITY_SELECTIVITY: f64 = 0.1;
const DEFAULT_RANGE_SELECTIVITY: f64 = 0.33;
```

### Histogram Types

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Histogram {
    EquiDepth(EquiDepthHistogram),
    EquiWidth(EquiWidthHistogram),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquiDepthHistogram {
    pub num_buckets: u32,
    pub rows_per_bucket: u64,
    pub bounds: Vec<Value>,
    pub distinct_per_bucket: Vec<u64>,
}

impl EquiDepthHistogram {
    pub fn new(values: &[Value], num_buckets: u32) -> Self {
        let sorted: Vec<Value> = values
            .iter()
            .filter(|v| !v.is_null())
            .cloned()
            .sorted()
            .collect();

        if sorted.is_empty() {
            return Self {
                num_buckets: 0,
                rows_per_bucket: 0,
                bounds: vec![],
                distinct_per_bucket: vec![],
            };
        }

        let rows_per_bucket = sorted.len().div_ceil(num_buckets as usize) as u64;
        let mut bounds = Vec::with_capacity(num_buckets as usize + 1);
        let mut distinct_per_bucket = Vec::with_capacity(num_buckets as usize);

        bounds.push(sorted[0].clone());

        for i in 0..num_buckets as usize {
            let start = i * rows_per_bucket as usize;
            let end = ((i + 1) * rows_per_bucket as usize).min(sorted.len());

            if start >= sorted.len() {
                break;
            }

            let bucket_values = &sorted[start..end];
            let distinct: HashSet<&Value> = bucket_values.iter().collect();
            distinct_per_bucket.push(distinct.len() as u64);

            if end > 0 && end <= sorted.len() {
                bounds.push(sorted[end - 1].clone());
            }
        }

        Self {
            num_buckets,
            rows_per_bucket,
            bounds,
            distinct_per_bucket,
        }
    }

    pub fn selectivity_equality(&self, value: &Value, total_distinct: u64) -> f64 {
        if let Some(bucket_idx) = self.find_bucket(value) {
            let bucket_distinct = self.distinct_per_bucket.get(bucket_idx).copied().unwrap_or(1);
            1.0 / bucket_distinct as f64
        } else {
            1.0 / total_distinct.max(1) as f64
        }
    }

    pub fn selectivity_range(&self, low: Option<&Value>, high: Option<&Value>) -> f64 {
        if self.bounds.is_empty() {
            return 0.5;
        }

        let low_idx = low
            .map(|v| self.find_bucket_position(v))
            .unwrap_or(0);
        let high_idx = high
            .map(|v| self.find_bucket_position(v))
            .unwrap_or(self.num_buckets as usize);

        let total_buckets = self.num_buckets.max(1) as f64;
        ((high_idx - low_idx) as f64) / total_buckets
    }

    fn find_bucket(&self, value: &Value) -> Option<usize> {
        if self.bounds.len() < 2 {
            return None;
        }
        for i in 0..self.bounds.len() - 1 {
            if value >= &self.bounds[i] && value <= &self.bounds[i + 1] {
                return Some(i);
            }
        }
        None
    }

    fn find_bucket_position(&self, value: &Value) -> usize {
        self.bounds.iter().position(|b| value <= b).unwrap_or(self.bounds.len())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquiWidthHistogram {
    pub num_buckets: u32,
    pub min_value: Value,
    pub max_value: Value,
    pub bucket_counts: Vec<u64>,
}

impl EquiWidthHistogram {
    pub fn new_numeric(values: &[Value], num_buckets: u32) -> Option<Self> {
        let numeric_values: Vec<f64> = values
            .iter()
            .filter_map(|v| v.as_f64())
            .collect();

        if numeric_values.is_empty() {
            return None;
        }

        let min = numeric_values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = numeric_values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

        if min == max {
            return Some(Self {
                num_buckets: 1,
                min_value: Value::float64(min),
                max_value: Value::float64(max),
                bucket_counts: vec![numeric_values.len() as u64],
            });
        }

        let bucket_width = (max - min) / num_buckets as f64;
        let mut bucket_counts = vec![0u64; num_buckets as usize];

        for val in numeric_values {
            let bucket = ((val - min) / bucket_width).floor() as usize;
            let bucket = bucket.min(num_buckets as usize - 1);
            bucket_counts[bucket] += 1;
        }

        Some(Self {
            num_buckets,
            min_value: Value::float64(min),
            max_value: Value::float64(max),
            bucket_counts,
        })
    }

    pub fn selectivity_range(&self, low: Option<f64>, high: Option<f64>) -> f64 {
        let min = self.min_value.as_f64().unwrap_or(0.0);
        let max = self.max_value.as_f64().unwrap_or(1.0);
        let range = max - min;

        if range == 0.0 {
            return 1.0;
        }

        let low = low.unwrap_or(min).max(min);
        let high = high.unwrap_or(max).min(max);

        ((high - low) / range).max(0.0).min(1.0)
    }
}
```

### Most Common Values

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MostCommonValues {
    pub values: Vec<Value>,
    pub frequencies: Vec<f64>,
}

impl MostCommonValues {
    pub fn new(values: &[Value], max_entries: usize, total_rows: u64) -> Self {
        let mut counts: HashMap<Value, u64> = HashMap::new();

        for value in values {
            if !value.is_null() {
                *counts.entry(value.clone()).or_insert(0) += 1;
            }
        }

        let mut sorted: Vec<(Value, u64)> = counts.into_iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(&a.1));
        sorted.truncate(max_entries);

        let values: Vec<Value> = sorted.iter().map(|(v, _)| v.clone()).collect();
        let frequencies: Vec<f64> = sorted
            .iter()
            .map(|(_, count)| *count as f64 / total_rows as f64)
            .collect();

        Self { values, frequencies }
    }

    pub fn frequency(&self, value: &Value) -> Option<f64> {
        self.values
            .iter()
            .position(|v| v == value)
            .map(|idx| self.frequencies[idx])
    }

    pub fn total_frequency(&self) -> f64 {
        self.frequencies.iter().sum()
    }
}
```

### Correlation Tracking

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnCorrelation {
    pub column_a: String,
    pub column_b: String,
    pub correlation_coefficient: f64,
}

impl ColumnCorrelation {
    pub fn compute(
        col_a: &Column,
        col_b: &Column,
        col_a_name: &str,
        col_b_name: &str,
    ) -> Option<Self> {
        let pairs: Vec<(f64, f64)> = (0..col_a.len())
            .filter_map(|i| {
                let a = col_a.get_value(i).as_f64()?;
                let b = col_b.get_value(i).as_f64()?;
                Some((a, b))
            })
            .collect();

        if pairs.len() < 2 {
            return None;
        }

        let n = pairs.len() as f64;
        let sum_a: f64 = pairs.iter().map(|(a, _)| a).sum();
        let sum_b: f64 = pairs.iter().map(|(_, b)| b).sum();
        let sum_ab: f64 = pairs.iter().map(|(a, b)| a * b).sum();
        let sum_a2: f64 = pairs.iter().map(|(a, _)| a * a).sum();
        let sum_b2: f64 = pairs.iter().map(|(_, b)| b * b).sum();

        let numerator = n * sum_ab - sum_a * sum_b;
        let denominator = ((n * sum_a2 - sum_a * sum_a) * (n * sum_b2 - sum_b * sum_b)).sqrt();

        if denominator == 0.0 {
            return None;
        }

        Some(Self {
            column_a: col_a_name.to_string(),
            column_b: col_b_name.to_string(),
            correlation_coefficient: numerator / denominator,
        })
    }

    pub fn is_strongly_correlated(&self) -> bool {
        self.correlation_coefficient.abs() > 0.7
    }
}

impl TableStats {
    pub fn correlations(&self) -> Vec<ColumnCorrelation> {
        // Stored separately or computed on demand
        vec![]
    }
}
```

## ANALYZE Statement

### Syntax

```sql
ANALYZE TABLE table_name;
ANALYZE TABLE table_name (column1, column2);
ANALYZE TABLE table_name WITH (num_buckets = 100, mcv_size = 20);
```

### Implementation

```rust
// In yachtsql-ir/src/plan/mod.rs
pub enum LogicalPlan {
    // ... existing variants ...
    Analyze {
        table_name: String,
        columns: Option<Vec<String>>,
        options: AnalyzeOptions,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyzeOptions {
    pub num_histogram_buckets: u32,
    pub mcv_size: u32,
    pub compute_correlations: bool,
    pub sample_percent: Option<f64>,
}

impl Default for AnalyzeOptions {
    fn default() -> Self {
        Self {
            num_histogram_buckets: 100,
            mcv_size: 20,
            compute_correlations: false,
            sample_percent: None,
        }
    }
}
```

### Execution

```rust
// In yachtsql-executor/src/executor/ddl/stats.rs
pub fn execute_analyze(
    catalog: &mut Catalog,
    table_name: &str,
    columns: Option<&[String]>,
    options: &AnalyzeOptions,
) -> Result<TableStats> {
    let table = catalog
        .get_table(table_name)
        .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

    let row_count = table.row_count() as u64;
    let schema = table.schema();

    let columns_to_analyze: Vec<&str> = match columns {
        Some(cols) => cols.iter().map(|s| s.as_str()).collect(),
        None => schema.fields().iter().map(|f| f.name.as_str()).collect(),
    };

    let mut column_stats = IndexMap::new();

    for col_name in columns_to_analyze {
        let col_idx = schema
            .field_index(col_name)
            .ok_or_else(|| Error::ColumnNotFound(col_name.to_string()))?;

        let column = table.column(col_idx).unwrap();
        let field = &schema.fields()[col_idx];

        let stats = compute_column_stats(
            column,
            col_name,
            &field.data_type,
            row_count,
            options,
        );
        column_stats.insert(col_name.to_string(), stats);
    }

    let total_size_bytes = estimate_table_size(table);

    let stats = TableStats {
        table_name: table_name.to_string(),
        row_count,
        total_size_bytes,
        last_analyzed: chrono::Utc::now(),
        columns: column_stats,
    };

    catalog.set_table_stats(table_name, stats.clone());

    Ok(stats)
}

fn compute_column_stats(
    column: &Column,
    name: &str,
    data_type: &DataType,
    total_rows: u64,
    options: &AnalyzeOptions,
) -> ColumnStats {
    let values: Vec<Value> = (0..column.len())
        .map(|i| column.get_value(i))
        .collect();

    let null_count = values.iter().filter(|v| v.is_null()).count() as u64;

    let non_null_values: Vec<&Value> = values.iter().filter(|v| !v.is_null()).collect();

    let distinct_count = {
        let unique: HashSet<&Value> = non_null_values.iter().cloned().collect();
        unique.len() as u64
    };

    let (min_value, max_value) = compute_min_max(&non_null_values);

    let avg_width_bytes = estimate_avg_width(&values, data_type);

    let histogram = if is_histogram_supported(data_type) && non_null_values.len() > 10 {
        let hist_values: Vec<Value> = non_null_values.iter().cloned().cloned().collect();
        Some(Histogram::EquiDepth(EquiDepthHistogram::new(
            &hist_values,
            options.num_histogram_buckets,
        )))
    } else {
        None
    };

    let most_common_values = if non_null_values.len() > options.mcv_size as usize {
        let mcv_values: Vec<Value> = non_null_values.iter().cloned().cloned().collect();
        Some(MostCommonValues::new(&mcv_values, options.mcv_size as usize, total_rows))
    } else {
        None
    };

    ColumnStats {
        column_name: name.to_string(),
        data_type: data_type.clone(),
        null_count,
        distinct_count,
        min_value,
        max_value,
        avg_width_bytes,
        histogram,
        most_common_values,
    }
}

fn is_histogram_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int64
            | DataType::Float64
            | DataType::Numeric(_)
            | DataType::Date
            | DataType::Datetime(_)
            | DataType::Timestamp(_)
            | DataType::String
    )
}
```

## Selectivity Estimation

### Predicate Selectivity

```rust
fn estimate_selectivity(predicate: &Expr, stats: &TableStats) -> f64 {
    match predicate {
        Expr::BinaryOp { left, op, right } => {
            estimate_binary_op_selectivity(left, op, right, stats)
        }
        Expr::IsNull { expr, negated } => {
            estimate_null_selectivity(expr, *negated, stats)
        }
        Expr::InList { expr, list, negated } => {
            estimate_in_list_selectivity(expr, list, *negated, stats)
        }
        Expr::Between { expr, low, high, negated } => {
            estimate_between_selectivity(expr, low, high, *negated, stats)
        }
        Expr::Not { expr } => {
            1.0 - estimate_selectivity(expr, stats)
        }
        Expr::And { left, right } => {
            estimate_selectivity(left, stats) * estimate_selectivity(right, stats)
        }
        Expr::Or { left, right } => {
            let s1 = estimate_selectivity(left, stats);
            let s2 = estimate_selectivity(right, stats);
            s1 + s2 - (s1 * s2)
        }
        _ => DEFAULT_SELECTIVITY,
    }
}

fn estimate_binary_op_selectivity(
    left: &Expr,
    op: &BinaryOp,
    right: &Expr,
    stats: &TableStats,
) -> f64 {
    if let Expr::Column { name, .. } = left {
        if let Some(col_stats) = stats.column_stats(name) {
            if let Expr::Literal(value) = right {
                return match op {
                    BinaryOp::Eq => col_stats.selectivity_equality(value, stats.row_count),
                    BinaryOp::NotEq => 1.0 - col_stats.selectivity_equality(value, stats.row_count),
                    BinaryOp::Lt | BinaryOp::LtEq => {
                        col_stats.selectivity_range(None, Some(value))
                    }
                    BinaryOp::Gt | BinaryOp::GtEq => {
                        col_stats.selectivity_range(Some(value), None)
                    }
                    _ => DEFAULT_SELECTIVITY,
                };
            }
        }
    }
    DEFAULT_SELECTIVITY
}

fn estimate_null_selectivity(
    expr: &Expr,
    negated: bool,
    stats: &TableStats,
) -> f64 {
    if let Expr::Column { name, .. } = expr {
        if let Some(col_stats) = stats.column_stats(name) {
            let null_frac = col_stats.null_fraction(stats.row_count);
            return if negated { 1.0 - null_frac } else { null_frac };
        }
    }
    if negated { 0.9 } else { 0.1 }
}

const DEFAULT_SELECTIVITY: f64 = 0.5;
```

### Join Cardinality Estimation

```rust
fn estimate_join_cardinality(
    left_stats: &TableStats,
    right_stats: &TableStats,
    join_type: JoinType,
    left_key: &str,
    right_key: &str,
) -> u64 {
    let left_rows = left_stats.row_count;
    let right_rows = right_stats.row_count;

    let left_distinct = left_stats
        .column_stats(left_key)
        .map(|s| s.distinct_count)
        .unwrap_or(left_rows);
    let right_distinct = right_stats
        .column_stats(right_key)
        .map(|s| s.distinct_count)
        .unwrap_or(right_rows);

    let max_distinct = left_distinct.max(right_distinct).max(1);

    match join_type {
        JoinType::Inner => {
            (left_rows * right_rows) / max_distinct
        }
        JoinType::Left => {
            left_rows.max((left_rows * right_rows) / max_distinct)
        }
        JoinType::Right => {
            right_rows.max((left_rows * right_rows) / max_distinct)
        }
        JoinType::Full => {
            let inner = (left_rows * right_rows) / max_distinct;
            inner + (left_rows - inner.min(left_rows)) + (right_rows - inner.min(right_rows))
        }
        JoinType::Cross => {
            left_rows * right_rows
        }
    }
}
```

## Statistics Storage in Catalog

```rust
// In yachtsql-executor/src/catalog.rs
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Catalog {
    tables: HashMap<String, Table>,
    table_stats: HashMap<String, TableStats>,
    // ... other fields
}

impl Catalog {
    pub fn get_table_stats(&self, name: &str) -> Option<&TableStats> {
        self.table_stats.get(&name.to_uppercase())
    }

    pub fn set_table_stats(&mut self, name: &str, stats: TableStats) {
        self.table_stats.insert(name.to_uppercase(), stats);
    }

    pub fn invalidate_stats(&mut self, table_name: &str) {
        self.table_stats.remove(&table_name.to_uppercase());
    }
}
```

## Statistics Invalidation

Statistics become stale after DML operations:

```rust
impl Catalog {
    pub fn after_insert(&mut self, table_name: &str, inserted_rows: u64) {
        if let Some(stats) = self.table_stats.get_mut(&table_name.to_uppercase()) {
            stats.row_count += inserted_rows;
            // Mark as potentially stale but don't fully invalidate
        }
    }

    pub fn after_delete(&mut self, table_name: &str, deleted_rows: u64) {
        if let Some(stats) = self.table_stats.get_mut(&table_name.to_uppercase()) {
            stats.row_count = stats.row_count.saturating_sub(deleted_rows);
        }
    }

    pub fn after_truncate(&mut self, table_name: &str) {
        self.invalidate_stats(table_name);
    }
}
```

## Optimizer Integration

```rust
// In yachtsql-optimizer/src/planner/physical_planner.rs
impl PhysicalPlanner {
    pub fn plan_with_stats(
        &self,
        logical: &LogicalPlan,
        catalog: &Catalog,
    ) -> Result<OptimizedLogicalPlan> {
        match logical {
            LogicalPlan::Filter { input, predicate } => {
                let input_plan = self.plan_with_stats(input, catalog)?;
                let stats = self.estimate_stats(&input_plan, catalog);
                let selectivity = stats
                    .map(|s| estimate_selectivity(predicate, &s))
                    .unwrap_or(0.5);

                // Use selectivity to decide filter push-down strategy
                // ...
            }
            LogicalPlan::Join { left, right, join_type, condition, .. } => {
                let left_stats = self.estimate_stats(left, catalog);
                let right_stats = self.estimate_stats(right, catalog);

                // Use stats to choose join order and algorithm
                // Smaller table on build side for hash join
                // ...
            }
            // ... other cases
        }
    }
}
```

## Implementation Order

1. Add basic `TableStats` and `ColumnStats` structures
2. Implement simple statistics collection (row count, distinct count, null count)
3. Add histogram support (equi-depth first)
4. Implement MCV tracking
5. Add ANALYZE statement execution
6. Integrate statistics with optimizer
7. Add correlation tracking
8. Implement statistics invalidation on DML
9. Add sampling for large tables

## Testing Strategy

1. Unit tests for histogram construction and selectivity
2. Unit tests for MCV frequency lookups
3. Integration tests for ANALYZE execution
4. Tests for selectivity estimation accuracy
5. Performance tests for large tables
6. Tests for correlation coefficient computation
