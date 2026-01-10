#![feature(coverage_attribute)]
#![coverage(off)]
#![allow(dead_code)]

use yachtsql::{Result, YachtSQLSession};
use yachtsql_arrow::RecordBatch;
pub use yachtsql_arrow::{
    IntoTestValue, TestValue, assert_batch_records_eq, batch_to_rows, compare_rows,
    convert_to_test_value, extract_value, test_val,
};

pub trait RecordBatchVecExt {
    fn num_rows(&self) -> usize;
    fn row_count(&self) -> usize;
}

impl RecordBatchVecExt for Vec<RecordBatch> {
    fn num_rows(&self) -> usize {
        self.iter().map(|b| b.num_rows()).sum()
    }

    fn row_count(&self) -> usize {
        self.num_rows()
    }
}

pub fn setup_executor() -> YachtSQLSession {
    YachtSQLSession::new()
}

pub fn new_executor() -> YachtSQLSession {
    setup_executor()
}

pub fn assert_float_eq(actual: f64, expected: f64, epsilon: f64) {
    let diff = (actual - expected).abs();
    assert!(
        diff < epsilon,
        "Float values not equal within epsilon: actual={}, expected={}, diff={}, epsilon={}",
        actual,
        expected,
        diff,
        epsilon
    );
}

pub fn assert_error_contains<T>(result: Result<T>, keywords: &[&str]) {
    match result {
        Ok(_) => panic!("Expected error but got Ok result"),
        Err(e) => {
            let error_msg = e.to_string().to_lowercase();
            let found = keywords
                .iter()
                .any(|keyword| error_msg.contains(&keyword.to_lowercase()));
            assert!(
                found,
                "Error message '{}' does not contain any of the expected keywords: {:?}",
                e, keywords
            );
        }
    }
}

pub async fn create_table_with_schema(
    session: &YachtSQLSession,
    table_name: &str,
    columns: &[(&str, &str)],
) -> Result<()> {
    let columns_def = columns
        .iter()
        .map(|(name, type_)| format!("{} {}", name, type_))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("CREATE TABLE {} ({})", table_name, columns_def);
    session.execute_sql(&sql).await?;
    Ok(())
}

pub async fn insert_rows(
    session: &YachtSQLSession,
    table_name: &str,
    rows: Vec<Vec<&str>>,
) -> Result<()> {
    for row in rows {
        let values = row.join(", ");
        let sql = format!("INSERT INTO {} VALUES ({})", table_name, values);
        session.execute_sql(&sql).await?;
    }
    Ok(())
}

pub async fn setup_table_with_id_values(
    session: &YachtSQLSession,
    table_name: &str,
    data: &[(i64, f64)],
) -> Result<()> {
    session
        .execute_sql(&format!(
            "CREATE TABLE {} (id INT64, value FLOAT64)",
            table_name
        ))
        .await?;
    for (id, value) in data {
        session
            .execute_sql(&format!(
                "INSERT INTO {} VALUES ({}, {})",
                table_name, id, value
            ))
            .await?;
    }
    Ok(())
}

pub fn build_repeated_expression(expr: &str, count: usize, separator: &str) -> String {
    vec![expr; count].join(separator)
}

pub fn build_nested_expression(wrapper: &str, inner: &str, depth: usize) -> String {
    let mut result = inner.to_string();
    for _ in 0..depth {
        result = format!("{} ({})", wrapper, result);
    }
    result
}

pub async fn setup_bool_table() -> YachtSQLSession {
    let session = setup_executor();
    session
        .execute_sql("CREATE TABLE bools (id INT64, val BOOL)")
        .await
        .expect("CREATE TABLE should succeed");
    session
}

pub async fn insert_bool(session: &YachtSQLSession, id: i64, val: Option<bool>) {
    let val_str = match val {
        Some(true) => "TRUE",
        Some(false) => "FALSE",
        None => "NULL",
    };
    session
        .execute_sql(&format!("INSERT INTO bools VALUES ({}, {})", id, val_str))
        .await
        .expect("INSERT should succeed");
}

pub async fn setup_table_with_float_values(
    session: &YachtSQLSession,
    table_name: &str,
    values: &[f64],
) -> Result<()> {
    session
        .execute_sql(&format!("CREATE TABLE {} (value FLOAT64)", table_name))
        .await?;
    for value in values {
        session
            .execute_sql(&format!("INSERT INTO {} VALUES ({})", table_name, value))
            .await?;
    }
    Ok(())
}

pub async fn assert_query_error(session: &YachtSQLSession, sql: &str, keywords: &[&str]) {
    assert_error_contains(session.execute_sql(sql).await, keywords);
}

pub fn assert_row_count(batches: &[RecordBatch], expected_count: usize) {
    let actual_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        actual_count, expected_count,
        "Expected {} rows, but got {}",
        expected_count, actual_count
    );
}

pub async fn table_exists(session: &YachtSQLSession, table_name: &str) -> bool {
    let query = format!("SELECT COUNT(*) FROM {}", table_name);
    session.execute_sql(&query).await.is_ok()
}

pub fn assert_error_contains_with_context<T>(result: Result<T>, keywords: &[&str], context: &str) {
    match result {
        Ok(_) => panic!("{}: Expected error but got Ok result", context),
        Err(e) => {
            let error_msg = e.to_string().to_lowercase();
            let found = keywords
                .iter()
                .any(|keyword| error_msg.contains(&keyword.to_lowercase()));
            assert!(
                found,
                "{}: Error '{}' does not contain any of the expected keywords: {:?}",
                context, e, keywords
            );
        }
    }
}

pub fn get_i64(batches: &[RecordBatch], col: usize, row: usize) -> i64 {
    use yachtsql_arrow::array::{Array, Int64Array};

    let mut current_row = row;
    for batch in batches {
        if current_row < batch.num_rows() {
            let array = batch.column(col);
            let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            return int_array.value(current_row);
        }
        current_row -= batch.num_rows();
    }
    panic!("Row {} not found in batches", row);
}

pub fn get_string(batches: &[RecordBatch], col: usize, row: usize) -> String {
    use yachtsql_arrow::array::{Array, BooleanArray, StringArray};

    let mut current_row = row;
    for batch in batches {
        if current_row < batch.num_rows() {
            let array = batch.column(col);
            if let Some(str_array) = array.as_any().downcast_ref::<StringArray>() {
                return str_array.value(current_row).to_string();
            }
            if let Some(bool_array) = array.as_any().downcast_ref::<BooleanArray>() {
                return if bool_array.value(current_row) {
                    "YES".to_string()
                } else {
                    "NO".to_string()
                };
            }
            panic!(
                "Column {} is not STRING or BOOL at row {}",
                col, current_row
            );
        }
        current_row -= batch.num_rows();
    }
    panic!("Row {} not found in batches", row);
}

pub fn get_f64(batches: &[RecordBatch], col: usize, row: usize) -> f64 {
    use yachtsql_arrow::array::{Array, Float64Array};

    let mut current_row = row;
    for batch in batches {
        if current_row < batch.num_rows() {
            let array = batch.column(col);
            let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
            return float_array.value(current_row);
        }
        current_row -= batch.num_rows();
    }
    panic!("Row {} not found in batches", row);
}

pub fn get_bool(batches: &[RecordBatch], col: usize, row: usize) -> bool {
    use yachtsql_arrow::array::{Array, BooleanArray};

    let mut current_row = row;
    for batch in batches {
        if current_row < batch.num_rows() {
            let array = batch.column(col);
            let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            return bool_array.value(current_row);
        }
        current_row -= batch.num_rows();
    }
    panic!("Row {} not found in batches", row);
}

pub fn is_null(batches: &[RecordBatch], col: usize, row: usize) -> bool {
    use yachtsql_arrow::array::Array;

    let mut current_row = row;
    for batch in batches {
        if current_row < batch.num_rows() {
            let array = batch.column(col);
            return array.is_null(current_row);
        }
        current_row -= batch.num_rows();
    }
    panic!("Row {} not found in batches", row);
}

fn find_column_index(batches: &[RecordBatch], col_name: &str) -> usize {
    if batches.is_empty() {
        panic!("No batches to search for column '{}'", col_name);
    }
    let schema = batches[0].schema();
    schema
        .fields()
        .iter()
        .position(|f| f.name() == col_name)
        .unwrap_or_else(|| panic!("Column '{}' not found in schema", col_name))
}

pub fn get_i64_by_name(batches: &[RecordBatch], row: usize, col_name: &str) -> i64 {
    let col = find_column_index(batches, col_name);
    get_i64(batches, col, row)
}

pub fn get_string_by_name(batches: &[RecordBatch], row: usize, col_name: &str) -> String {
    let col = find_column_index(batches, col_name);
    get_string(batches, col, row)
}

pub fn get_f64_by_name(batches: &[RecordBatch], row: usize, col_name: &str) -> f64 {
    let col = find_column_index(batches, col_name);
    get_f64(batches, col, row)
}

pub fn get_bool_by_name(batches: &[RecordBatch], row: usize, col_name: &str) -> bool {
    let col = find_column_index(batches, col_name);
    get_bool(batches, col, row)
}

pub fn is_null_by_name(batches: &[RecordBatch], row: usize, col_name: &str) -> bool {
    let col = find_column_index(batches, col_name);
    is_null(batches, col, row)
}

pub fn assert_error_with_sqlstate<T: std::fmt::Debug>(
    result: Result<T>,
    expected_sqlstate: &str,
    context: &str,
) {
    assert!(result.is_err(), "Expected error for: {}", context);
    let err = result.unwrap_err();
    let err_msg = err.to_string();

    let has_sqlstate = err_msg.contains(expected_sqlstate)
        || err_msg.contains(&format!("[{}]", expected_sqlstate))
        || err_msg.contains(&format!("SQLSTATE[{}]", expected_sqlstate));

    assert!(
        has_sqlstate,
        "Expected SQLSTATE {} in error '{}' (context: {})",
        expected_sqlstate, err_msg, context
    );
}

pub fn column_strings(batches: &[RecordBatch], col_index: usize) -> Vec<String> {
    use yachtsql_arrow::array::{Array, StringArray};

    let mut result = Vec::new();
    for batch in batches {
        let array = batch.column(col_index);
        let str_array = array.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..batch.num_rows() {
            result.push(str_array.value(i).to_string());
        }
    }
    result
}

pub fn column_i64(batches: &[RecordBatch], col_index: usize) -> Vec<i64> {
    use yachtsql_arrow::array::{Array, Int64Array};

    let mut result = Vec::new();
    for batch in batches {
        let array = batch.column(col_index);
        let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..batch.num_rows() {
            result.push(int_array.value(i));
        }
    }
    result
}

pub fn column_nullable_i64(batches: &[RecordBatch], col_index: usize) -> Vec<Option<i64>> {
    use yachtsql_arrow::array::{Array, Int64Array};

    let mut result = Vec::new();
    for batch in batches {
        let array = batch.column(col_index);
        let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..batch.num_rows() {
            if array.is_null(i) {
                result.push(None);
            } else {
                result.push(Some(int_array.value(i)));
            }
        }
    }
    result
}

pub async fn exec_ok(session: &YachtSQLSession, sql: &str) {
    session
        .execute_sql(sql)
        .await
        .unwrap_or_else(|e| panic!("SQL execution failed for '{}': {}", sql, e));
}

pub async fn query(session: &YachtSQLSession, sql: &str) -> Vec<RecordBatch> {
    session
        .execute_sql(sql)
        .await
        .unwrap_or_else(|e| panic!("Query execution failed for '{}': {}", sql, e))
}

pub fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

pub fn num_columns(batches: &[RecordBatch]) -> usize {
    if batches.is_empty() {
        0
    } else {
        batches[0].num_columns()
    }
}

pub fn assert_batch_empty(batches: &[RecordBatch]) {
    let rows = total_rows(batches);
    assert_eq!(rows, 0, "expected zero rows but found {}", rows);
}
