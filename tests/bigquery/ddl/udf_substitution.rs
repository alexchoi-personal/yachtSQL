use yachtsql::RecordBatchVecExt;

use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_is_null() {
    let session = create_session();
    session
        .execute_sql("CREATE FUNCTION check_null(x INT64) RETURNS BOOL AS (x IS NULL)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT check_null(NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
    let result = session.execute_sql("SELECT check_null(5)").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_is_not_null() {
    let session = create_session();
    session
        .execute_sql("CREATE FUNCTION check_not_null(x INT64) RETURNS BOOL AS (x IS NOT NULL)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT check_not_null(NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
    let result = session
        .execute_sql("SELECT check_not_null(5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_between() {
    let session = create_session();
    session
        .execute_sql("CREATE FUNCTION in_range(x INT64, low INT64, high INT64) RETURNS BOOL AS (x BETWEEN low AND high)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT in_range(5, 1, 10)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
    let result = session
        .execute_sql("SELECT in_range(15, 1, 10)")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_not_between() {
    let session = create_session();
    session
        .execute_sql("CREATE FUNCTION out_of_range(x INT64, low INT64, high INT64) RETURNS BOOL AS (x NOT BETWEEN low AND high)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT out_of_range(15, 1, 10)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
    let result = session
        .execute_sql("SELECT out_of_range(5, 1, 10)")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_in_list() {
    let session = create_session();
    session
        .execute_sql("CREATE FUNCTION is_special(x INT64) RETURNS BOOL AS (x IN (1, 2, 3))")
        .await
        .unwrap();
    let result = session.execute_sql("SELECT is_special(2)").await.unwrap();
    assert_table_eq!(result, [[true]]);
    let result = session.execute_sql("SELECT is_special(5)").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_in_list_param() {
    let session = create_session();
    session
        .execute_sql("CREATE FUNCTION matches_value(x INT64, v1 INT64, v2 INT64) RETURNS BOOL AS (x IN (v1, v2))")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT matches_value(5, 5, 10)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
    let result = session
        .execute_sql("SELECT matches_value(7, 5, 10)")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_array_literal() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE FUNCTION make_array(a INT64, b INT64) RETURNS ARRAY<INT64> AS ([a, b])",
        )
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(make_array(1, 2))")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_array_access() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE FUNCTION get_first(arr ARRAY<INT64>) RETURNS INT64 AS (arr[OFFSET(0)])",
        )
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT get_first([10, 20, 30])")
        .await
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_array_access_param_index() {
    let session = create_session();
    session
        .execute_sql("CREATE FUNCTION get_at(arr ARRAY<INT64>, idx INT64) RETURNS INT64 AS (arr[OFFSET(idx)])")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT get_at([10, 20, 30], 1)")
        .await
        .unwrap();
    assert_table_eq!(result, [[20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_like() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE FUNCTION matches_pattern(s STRING, pat STRING) RETURNS BOOL AS (s LIKE pat)",
        )
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT matches_pattern('hello', 'hel%')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
    let result = session
        .execute_sql("SELECT matches_pattern('hello', 'bye%')")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_not_like() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE FUNCTION not_matches(s STRING, pat STRING) RETURNS BOOL AS (s NOT LIKE pat)",
        )
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT not_matches('hello', 'bye%')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_extract() {
    let session = create_session();
    session
        .execute_sql("CREATE FUNCTION get_year(d DATE) RETURNS INT64 AS (EXTRACT(YEAR FROM d))")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT get_year(DATE '2024-06-15')")
        .await
        .unwrap();
    assert_table_eq!(result, [[2024]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_substring() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE FUNCTION get_prefix(s STRING, n INT64) RETURNS STRING AS (SUBSTRING(s, 1, n))",
        )
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT get_prefix('hello', 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [["hel"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_substring_from_position() {
    let session = create_session();
    session
        .execute_sql("CREATE FUNCTION get_suffix(s STRING, start INT64) RETURNS STRING AS (SUBSTRING(s FROM start))")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT get_suffix('hello', 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [["llo"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_trim() {
    let session = create_session();
    session
        .execute_sql("CREATE FUNCTION clean_string(s STRING) RETURNS STRING AS (TRIM(s))")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT clean_string('  hello  ')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_trim_chars() {
    let session = create_session();
    session
        .execute_sql("CREATE FUNCTION strip_chars(s STRING, chars STRING) RETURNS STRING AS (TRIM(s, chars))")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT strip_chars('xxhelloxx', 'x')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_in_unnest() {
    let session = create_session();
    session
        .execute_sql("CREATE FUNCTION is_in_array(x INT64, arr ARRAY<INT64>) RETURNS BOOL AS (x IN UNNEST(arr))")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT is_in_array(2, [1, 2, 3])")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
    let result = session
        .execute_sql("SELECT is_in_array(5, [1, 2, 3])")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_is_distinct_from() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE FUNCTION is_diff(x INT64, y INT64) RETURNS BOOL AS (x IS DISTINCT FROM y)",
        )
        .await
        .unwrap();
    let result = session.execute_sql("SELECT is_diff(1, 2)").await.unwrap();
    assert_table_eq!(result, [[true]]);
    let result = session.execute_sql("SELECT is_diff(1, 1)").await.unwrap();
    assert_table_eq!(result, [[false]]);
    let result = session
        .execute_sql("SELECT is_diff(NULL, NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_is_not_distinct_from() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE FUNCTION is_same(x INT64, y INT64) RETURNS BOOL AS (x IS NOT DISTINCT FROM y)",
        )
        .await
        .unwrap();
    let result = session.execute_sql("SELECT is_same(1, 1)").await.unwrap();
    assert_table_eq!(result, [[true]]);
    let result = session
        .execute_sql("SELECT is_same(NULL, NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
    let result = session
        .execute_sql("SELECT is_same(1, NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_case_operand() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE FUNCTION describe_num(x INT64) RETURNS STRING AS (
                CASE x
                    WHEN 1 THEN 'one'
                    WHEN 2 THEN 'two'
                    ELSE 'other'
                END
            )",
        )
        .await
        .unwrap();
    let result = session.execute_sql("SELECT describe_num(1)").await.unwrap();
    assert_table_eq!(result, [["one"]]);
    let result = session.execute_sql("SELECT describe_num(5)").await.unwrap();
    assert_table_eq!(result, [["other"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_existing_struct_field_names() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE FUNCTION named_struct(a INT64, b INT64)
             RETURNS STRUCT<x INT64, y INT64>
             AS (STRUCT(a AS x, b AS y))",
        )
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT named_struct(1, 2).x")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_position() {
    let session = create_session();
    session
        .execute_sql("CREATE FUNCTION find_pos(haystack STRING, needle STRING) RETURNS INT64 AS (STRPOS(haystack, needle))")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT find_pos('hello world', 'world')")
        .await
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_no_arg_with_default_no_default() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE FUNCTION with_opt(x INT64, y INT64 DEFAULT 10) RETURNS INT64 AS (x + y)",
        )
        .await
        .unwrap();
    let result = session.execute_sql("SELECT with_opt(5)").await.unwrap();
    assert_table_eq!(result, [[15]]);
    let result = session.execute_sql("SELECT with_opt(5, 20)").await.unwrap();
    assert_table_eq!(result, [[25]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_udf_with_aggregate_order_by_param() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE AGGREGATE FUNCTION concat_with_sep(s STRING, sep STRING)
            RETURNS STRING
            AS (STRING_AGG(s, sep))",
        )
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE string_data2 (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO string_data2 VALUES ('a'), ('b'), ('c')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT concat_with_sep(val, '-') FROM string_data2")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let val = records[0].values()[0].clone();
    match val {
        yachtsql::ResultValue::String(s) => {
            assert!(s.contains('-'));
            assert!(s.contains('a'));
            assert!(s.contains('b'));
            assert!(s.contains('c'));
        }
        _ => panic!("Expected string result"),
    }
}
