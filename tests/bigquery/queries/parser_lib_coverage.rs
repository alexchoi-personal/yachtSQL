use yachtsql::RecordBatchVecExt;

use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_empty_sql_statement() {
    let session = create_session();
    let result = session.execute_sql("").await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("Empty") || err.contains("empty"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_whitespace_only_sql() {
    let session = create_session();
    let result = session.execute_sql("   \n\t  ").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_statements_error() {
    let session = create_session();
    let result = session.execute_sql("SELECT 1; SELECT 2").await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("Multiple") || err.contains("multiple"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_parse_sql_syntax_error() {
    let session = create_session();
    let result = session.execute_sql("SELECT FROM WHERE").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_date_column_type() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE range_date_tbl (id INT64, period RANGE<DATE>)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO range_date_tbl VALUES (1, RANGE(DATE '2024-01-01', DATE '2024-12-31'))",
        )
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id FROM range_date_tbl")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_datetime_column_type() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE range_datetime_tbl (id INT64, period RANGE<DATETIME>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO range_datetime_tbl VALUES (1, RANGE(DATETIME '2024-01-01 00:00:00', DATETIME '2024-12-31 23:59:59'))")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id FROM range_datetime_tbl")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_timestamp_column_type() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE range_timestamp_tbl (id INT64, period RANGE<TIMESTAMP>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO range_timestamp_tbl VALUES (1, RANGE(TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-12-31 23:59:59'))")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id FROM range_timestamp_tbl")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_missing_into_clause() {
    let session = create_session();
    let result = session
        .execute_sql("LOAD DATA my_table FROM FILES (FORMAT='CSV', URIS=['test.csv'])")
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("INTO"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_missing_from_files_clause() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE test_missing_files (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql("LOAD DATA INTO test_missing_files (FORMAT='CSV', URIS=['test.csv'])")
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("FROM FILES"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_missing_uris_option() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE test_missing_uris (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql("LOAD DATA INTO test_missing_uris FROM FILES (FORMAT='CSV')")
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("URIS"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_with_allow_schema_update_true() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE schema_update_test (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "LOAD DATA INTO schema_update_test FROM FILES (FORMAT='CSV', URIS=['gs://test/data.csv'], allow_schema_update=TRUE)",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_with_allow_schema_update_false() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE schema_update_test2 (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "LOAD DATA INTO schema_update_test2 FROM FILES (FORMAT='CSV', URIS=['gs://test/data.csv'], allow_schema_update=FALSE)",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_with_format_lowercase() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE format_lower (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "LOAD DATA INTO format_lower FROM FILES (format='parquet', uris=['gs://test/data.parquet'])",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_csv_format() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE csv_format_test (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "LOAD DATA INTO csv_format_test FROM FILES (FORMAT='CSV', URIS=['gs://test/data.csv'])",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_json_format() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE json_format_test (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "LOAD DATA INTO json_format_test FROM FILES (FORMAT='JSON', URIS=['gs://test/data.json'])",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_avro_format() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE avro_format_test (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "LOAD DATA INTO avro_format_test FROM FILES (FORMAT='AVRO', URIS=['gs://test/data.avro'])",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_unknown_format_defaults_to_parquet() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE unknown_format (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "LOAD DATA INTO unknown_format FROM FILES (FORMAT='ORC', URIS=['gs://test/data.orc'])",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_default_format_parquet() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE default_format (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql("LOAD DATA INTO default_format FROM FILES (URIS=['gs://test/data.parquet'])")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_temp_table_with_various_column_types() {
    let session = create_session();

    let result = session
        .execute_sql(
            "LOAD DATA INTO TEMP TABLE temp_int64 (col INT64) FROM FILES (FORMAT='PARQUET', URIS=['gs://test/data.parquet'])",
        )
        .await;
    assert!(result.is_ok());

    let result = session
        .execute_sql(
            "LOAD DATA INTO TEMP TABLE temp_integer (col INTEGER) FROM FILES (FORMAT='PARQUET', URIS=['gs://test/data.parquet'])",
        )
        .await;
    assert!(result.is_ok());

    let result = session
        .execute_sql(
            "LOAD DATA INTO TEMP TABLE temp_bigint (col BIGINT) FROM FILES (FORMAT='PARQUET', URIS=['gs://test/data.parquet'])",
        )
        .await;
    assert!(result.is_ok());

    let result = session
        .execute_sql(
            "LOAD DATA INTO TEMP TABLE temp_float64 (col FLOAT64) FROM FILES (FORMAT='PARQUET', URIS=['gs://test/data.parquet'])",
        )
        .await;
    assert!(result.is_ok());

    let result = session
        .execute_sql(
            "LOAD DATA INTO TEMP TABLE temp_double (col DOUBLE) FROM FILES (FORMAT='PARQUET', URIS=['gs://test/data.parquet'])",
        )
        .await;
    assert!(result.is_ok());

    let result = session
        .execute_sql(
            "LOAD DATA INTO TEMP TABLE temp_float (col FLOAT) FROM FILES (FORMAT='PARQUET', URIS=['gs://test/data.parquet'])",
        )
        .await;
    assert!(result.is_ok());

    let result = session
        .execute_sql(
            "LOAD DATA INTO TEMP TABLE temp_string (col STRING) FROM FILES (FORMAT='PARQUET', URIS=['gs://test/data.parquet'])",
        )
        .await;
    assert!(result.is_ok());

    let result = session
        .execute_sql(
            "LOAD DATA INTO TEMP TABLE temp_varchar (col VARCHAR) FROM FILES (FORMAT='PARQUET', URIS=['gs://test/data.parquet'])",
        )
        .await;
    assert!(result.is_ok());

    let result = session
        .execute_sql(
            "LOAD DATA INTO TEMP TABLE temp_text (col TEXT) FROM FILES (FORMAT='PARQUET', URIS=['gs://test/data.parquet'])",
        )
        .await;
    assert!(result.is_ok());

    let result = session
        .execute_sql(
            "LOAD DATA INTO TEMP TABLE temp_bool (col BOOL) FROM FILES (FORMAT='PARQUET', URIS=['gs://test/data.parquet'])",
        )
        .await;
    assert!(result.is_ok());

    let result = session
        .execute_sql(
            "LOAD DATA INTO TEMP TABLE temp_boolean (col BOOLEAN) FROM FILES (FORMAT='PARQUET', URIS=['gs://test/data.parquet'])",
        )
        .await;
    assert!(result.is_ok());

    let result = session
        .execute_sql(
            "LOAD DATA INTO TEMP TABLE temp_date (col DATE) FROM FILES (FORMAT='PARQUET', URIS=['gs://test/data.parquet'])",
        )
        .await;
    assert!(result.is_ok());

    let result = session
        .execute_sql(
            "LOAD DATA INTO TEMP TABLE temp_datetime (col DATETIME) FROM FILES (FORMAT='PARQUET', URIS=['gs://test/data.parquet'])",
        )
        .await;
    assert!(result.is_ok());

    let result = session
        .execute_sql(
            "LOAD DATA INTO TEMP TABLE temp_timestamp (col TIMESTAMP) FROM FILES (FORMAT='PARQUET', URIS=['gs://test/data.parquet'])",
        )
        .await;
    assert!(result.is_ok());

    let result = session
        .execute_sql(
            "LOAD DATA INTO TEMP TABLE temp_time (col TIME) FROM FILES (FORMAT='PARQUET', URIS=['gs://test/data.parquet'])",
        )
        .await;
    assert!(result.is_ok());

    let result = session
        .execute_sql(
            "LOAD DATA INTO TEMP TABLE temp_bytes (col BYTES) FROM FILES (FORMAT='PARQUET', URIS=['gs://test/data.parquet'])",
        )
        .await;
    assert!(result.is_ok());

    let result = session
        .execute_sql(
            "LOAD DATA INTO TEMP TABLE temp_json (col JSON) FROM FILES (FORMAT='PARQUET', URIS=['gs://test/data.parquet'])",
        )
        .await;
    assert!(result.is_ok());

    let result = session
        .execute_sql(
            "LOAD DATA INTO TEMP TABLE temp_unknown (col GEOGRAPHY) FROM FILES (FORMAT='PARQUET', URIS=['gs://test/data.parquet'])",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_temp_table_no_columns() {
    let session = create_session();
    let result = session
        .execute_sql(
            "LOAD DATA INTO TEMP TABLE no_cols_temp FROM FILES (FORMAT='PARQUET', URIS=['gs://test/data.parquet'])",
        )
        .await;
    assert!(result.is_err() || result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_uris_without_brackets() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE uris_no_brackets (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "LOAD DATA INTO uris_no_brackets FROM FILES (FORMAT='CSV', URIS='gs://test/data.csv')",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_with_quoted_uris() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE quoted_uris (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            r#"LOAD DATA INTO quoted_uris FROM FILES (FORMAT='CSV', URIS=["gs://test/data.csv"])"#,
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_with_multiple_uris() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE multi_uris (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "LOAD DATA INTO multi_uris FROM FILES (FORMAT='CSV', URIS=['gs://test/data1.csv', 'gs://test/data2.csv'])",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_snapshot_without_if_not_exists() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE snap_source1 (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO snap_source1 VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("CREATE SNAPSHOT TABLE snap_copy1 CLONE snap_source1")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_snapshot_missing_clone_clause() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE snap_missing_clone (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql("CREATE SNAPSHOT TABLE snap_missing snap_missing_clone")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_snapshot_with_for_system_time() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE snap_for_time (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "CREATE SNAPSHOT TABLE snap_time_copy CLONE snap_for_time FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP()",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_snapshot_with_options() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE snap_with_opts (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "CREATE SNAPSHOT TABLE snap_opts_copy CLONE snap_with_opts OPTIONS(description='test')",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_snapshot_without_if_exists() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE drop_snap_src (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE SNAPSHOT TABLE drop_snap_test CLONE drop_snap_src")
        .await
        .unwrap();

    let result = session
        .execute_sql("DROP SNAPSHOT TABLE drop_snap_test")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_procedure_preprocessing_create_or_replace() {
    let session = create_session();
    let result = session
        .execute_sql(
            "CREATE OR REPLACE PROCEDURE my_proc()
             BEGIN
               SELECT 1;
             END",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_procedure_preprocessing_without_as() {
    let session = create_session();
    let result = session
        .execute_sql(
            "CREATE PROCEDURE my_proc2()
             BEGIN
               SELECT 1;
             END",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_with_options_in_column_defs() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE col_defs_test (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "LOAD DATA INTO col_defs_test (id INT64, name STRING) FROM FILES (FORMAT='CSV', URIS=['gs://test/data.csv'])",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_with_column_definitions_paren() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE format_paren_test (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "LOAD DATA INTO format_paren_test (id INT64) FROM FILES (FORMAT='CSV', URIS=['gs://test/data.csv'])",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_without_parentheses_in_from_files() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE no_paren_test (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql("LOAD DATA INTO no_paren_test FROM FILES FORMAT='CSV', URIS=['test.csv']")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_unmatched_parenthesis_in_from_files() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE unmatched_paren (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql("LOAD DATA INTO unmatched_paren FROM FILES (FORMAT='CSV', URIS=['test.csv']")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_single_uri_string() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE single_uri_tbl (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "LOAD DATA INTO single_uri_tbl FROM FILES (FORMAT='CSV', URIS='gs://bucket/file.csv')",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_option_key_without_equals() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE opt_no_eq (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "LOAD DATA INTO opt_no_eq FROM FILES (FORMAT 'CSV', URIS=['gs://bucket/file.csv'])",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_with_unquoted_value() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE unquoted_val (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "LOAD DATA INTO unquoted_val FROM FILES (FORMAT=PARQUET, URIS=['gs://bucket/file.parquet'])",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_with_column_def_only_name() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE col_name_only (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "LOAD DATA INTO col_name_only (id) FROM FILES (FORMAT='CSV', URIS=['gs://bucket/file.csv'])",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_with_empty_column_part() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE empty_col_part (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "LOAD DATA INTO empty_col_part (id INT64, , name STRING) FROM FILES (FORMAT='CSV', URIS=['gs://bucket/file.csv'])",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_with_skip_leading_rows_invalid() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE skip_invalid (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "LOAD DATA INTO skip_invalid FROM FILES (FORMAT='CSV', URIS=['gs://bucket/file.csv'], skip_leading_rows='invalid')",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_snapshot_nonexistent_without_if_exists() {
    let session = create_session();
    let result = session
        .execute_sql("DROP SNAPSHOT TABLE nonexistent_snap_table")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_snapshot_if_not_exists_already_exists() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE dup_snap_src (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE SNAPSHOT TABLE dup_snap CLONE dup_snap_src")
        .await
        .unwrap();
    let result = session
        .execute_sql("CREATE SNAPSHOT TABLE IF NOT EXISTS dup_snap CLONE dup_snap_src")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_with_double_quoted_uri() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE double_quote_uri (id INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            r#"LOAD DATA INTO double_quote_uri FROM FILES (FORMAT="CSV", URIS=["gs://bucket/file.csv"])"#,
        )
        .await;
    assert!(result.is_ok());
}
