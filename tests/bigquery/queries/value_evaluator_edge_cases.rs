use yachtsql_arrow::{TestValue, array, assert_batch_records_eq, bytes, date, interval, timestamp};

use crate::assert_table_eq;
use crate::common::{bignumeric, create_session, numeric};

#[tokio::test(flavor = "current_thread")]
async fn test_in_unnest_found() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 2 IN UNNEST([1, 2, 3])")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_unnest_not_found() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 5 IN UNNEST([1, 2, 3])")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_in_unnest() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 5 NOT IN UNNEST([1, 2, 3])")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_unnest_null_value() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULL IN UNNEST([1, 2, 3])")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_unnest_null_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 1 IN UNNEST(CAST(NULL AS ARRAY<INT64>))")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_unnest_with_null_element() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 5 IN UNNEST([1, NULL, 3])")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_unnest_found_with_null_element() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 1 IN UNNEST([1, NULL, 3])")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_in_unnest_with_null_element() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 5 NOT IN UNNEST([1, NULL, 3])")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_list_with_null_element() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 5 IN (1, NULL, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_list_found_with_null_element() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 1 IN (1, NULL, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_list_null_expr() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULL IN (1, 2, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_in_list_with_null_element() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 5 NOT IN (1, NULL, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_in_list_found() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 1 NOT IN (1, 2, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_offset() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [1,2,3][OFFSET(0)]")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_ordinal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [1,2,3][ORDINAL(1)]")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_safe_offset() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [1,2,3][SAFE_OFFSET(10)]")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_safe_ordinal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [1,2,3][SAFE_ORDINAL(10)]")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_negative_index() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [1,2,3][SAFE_OFFSET(-1)]")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_null_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(NULL AS ARRAY<INT64>)[SAFE_OFFSET(0)]")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_null_index() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [1,2,3][SAFE_OFFSET(CAST(NULL AS INT64))]")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_access() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRUCT(1 AS a, 2 AS b).a")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_access_not_found() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRUCT(1 AS a, 2 AS b).c")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_access_null_struct() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(NULL AS STRUCT<a INT64, b INT64>).a")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_access_key() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON '{\"a\": 1}', '$.a')")
        .await
        .unwrap();
    assert_table_eq!(result, [["1"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_access_array_index() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON '[1, 2, 3]', '$[1]')")
        .await
        .unwrap();
    assert_table_eq!(result, [["2"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_first_non_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COALESCE(NULL, NULL, 3, 4)")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_all_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COALESCE(NULL, NULL, NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ifnull_non_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT IFNULL(1, 2)").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ifnull_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT IFNULL(NULL, 2)").await.unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullif_equal() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULLIF(1, 1)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullif_not_equal() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULLIF(1, 2)").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullifzero_zero_int() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULLIFZERO(0)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullifzero_nonzero() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULLIFZERO(5)").await.unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullifzero_zero_float() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULLIFZERO(0.0)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_operand_match() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END")
        .await
        .unwrap();
    assert_table_eq!(result, [["two"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_operand_no_match() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CASE 5 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END")
        .await
        .unwrap();
    assert_table_eq!(result, [["other"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_operand_no_else() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CASE 5 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_condition_true() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CASE WHEN 1 < 2 THEN 'yes' WHEN 1 > 2 THEN 'no' END")
        .await
        .unwrap();
    assert_table_eq!(result, [["yes"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_condition_none_true() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CASE WHEN 1 > 2 THEN 'yes' ELSE 'no' END")
        .await
        .unwrap();
    assert_table_eq!(result, [["no"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_valid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('123' AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[123]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_invalid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('abc' AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_bool_to_int() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(TRUE AS INT64), CAST(FALSE AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_int_to_bool() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(1 AS BOOL), CAST(0 AS BOOL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_string_to_bool() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST('true' AS BOOL), CAST('false' AS BOOL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_numeric_to_int64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(NUMERIC '123.45' AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[123]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_numeric_to_float64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(NUMERIC '123.45' AS FLOAT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[123.45]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_bytes_to_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(b'hello' AS STRING)")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_string_to_bytes() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST('hello' AS BYTES)")
        .await
        .unwrap();
    assert_batch_records_eq!(result, [[bytes(b"hello")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_timestamp_to_date() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(TIMESTAMP '2025-01-15 10:30:00 UTC' AS DATE)")
        .await
        .unwrap();
    assert_batch_records_eq!(result, [[date(2025, 1, 15)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_date_to_timestamp() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(DATE '2025-01-15' AS TIMESTAMP)")
        .await
        .unwrap();
    assert_batch_records_eq!(result, [[timestamp(2025, 1, 15, 0, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_datetime_to_date() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(DATETIME '2025-01-15 10:30:00' AS DATE)")
        .await
        .unwrap();
    assert_batch_records_eq!(result, [[date(2025, 1, 15)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_datetime_to_timestamp() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(DATETIME '2025-01-15 10:30:00' AS TIMESTAMP)")
        .await
        .unwrap();
    assert_batch_records_eq!(result, [[timestamp(2025, 1, 15, 10, 30, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_int_to_numeric() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(123 AS NUMERIC)")
        .await
        .unwrap();
    assert_table_eq!(result, [[numeric("123")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_float_to_numeric() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(123.45 AS NUMERIC)")
        .await
        .unwrap();
    assert_table_eq!(result, [[numeric("123.45")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_string_to_numeric() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST('123.45' AS NUMERIC)")
        .await
        .unwrap();
    assert_table_eq!(result, [[numeric("123.45")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_int_to_bignumeric() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(123 AS BIGNUMERIC)")
        .await
        .unwrap();
    assert_table_eq!(result, [[bignumeric("123")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_numeric_to_bignumeric() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(NUMERIC '123.45' AS BIGNUMERIC)")
        .await
        .unwrap();
    assert_table_eq!(result, [[bignumeric("123.45")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_float_to_bignumeric() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(123.45 AS BIGNUMERIC)")
        .await
        .unwrap();
    assert_table_eq!(result, [[bignumeric("123.45")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_string_to_bignumeric() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST('123.45' AS BIGNUMERIC)")
        .await
        .unwrap();
    assert_table_eq!(result, [[bignumeric("123.45")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_array_elements() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST([1, 2, 3] AS ARRAY<STRING>)")
        .await
        .unwrap();
    let expected = array(vec![
        TestValue::String("1".to_string()),
        TestValue::String("2".to_string()),
        TestValue::String("3".to_string()),
    ]);
    assert_batch_records_eq!(result, [[expected]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unary_minus_numeric() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT -NUMERIC '123.45'")
        .await
        .unwrap();
    assert_table_eq!(result, [[numeric("-123.45")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unary_minus_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT -CAST(NULL AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unary_plus_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT +CAST(NULL AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_operator() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' || ' ' || 'world'")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_operator_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT 'hello' || NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_between_with_null_low() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 5 BETWEEN NULL AND 10")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_between_with_null_high() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 5 BETWEEN 1 AND NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_case_insensitive() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'HELLO' ILIKE '%llo'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_underscore() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' LIKE 'h_llo'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_null_pattern() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' LIKE NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_leading() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRIM(LEADING ' ' FROM '   hello   ')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello   "]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_trailing() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRIM(TRAILING ' ' FROM '   hello   ')")
        .await
        .unwrap();
    assert_table_eq!(result, [["   hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_both() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRIM(BOTH ' ' FROM '   hello   ')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_with_day() {
    let session = create_session();
    let result = session.execute_sql("SELECT INTERVAL 5 DAY").await.unwrap();
    assert_batch_records_eq!(result, [[interval()]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_with_hour() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INTERVAL 12 HOUR")
        .await
        .unwrap();
    assert_batch_records_eq!(result, [[interval()]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(NULL AS INT64) + INTERVAL 0 DAY")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_with_start() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTRING('hello' FROM 2)")
        .await
        .unwrap();
    assert_table_eq!(result, [["ello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_with_length() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTRING('hello' FROM 2 FOR 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [["ell"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_comparison_lteq_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 <= NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_comparison_gteq_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 >= NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_comparison_lt_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 < NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_comparison_gt_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 > NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_comparison_eq_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 = NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_comparison_neq_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 != NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_timestamp_parsing_formats() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST('2025-01-15 10:30:00' AS TIMESTAMP)")
        .await
        .unwrap();
    assert_batch_records_eq!(result, [[timestamp(2025, 1, 15, 10, 30, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_timestamp_parsing_iso_format() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST('2025-01-15T10:30:00' AS TIMESTAMP)")
        .await
        .unwrap();
    assert_batch_records_eq!(result, [[timestamp(2025, 1, 15, 10, 30, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_current_user() {
    let session = create_session();
    let result = session.execute_sql("SELECT CURRENT_USER()").await.unwrap();
    assert_table_eq!(result, [["user"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_session_user() {
    let session = create_session();
    let result = session.execute_sql("SELECT SESSION_USER()").await.unwrap();
    assert_table_eq!(result, [["user"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_collate_function() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COLLATE('hello', 'und:ci') = COLLATE('HELLO', 'und:ci')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_array_access_out_of_bounds() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON '[1, 2]', '$[10]')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_object_key_not_found() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON '{\"a\": 1}', '$.b')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lteq_both_true() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 <= 5").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gteq_both_true() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 >= 5").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lteq_both_false() {
    let session = create_session();
    let result = session.execute_sql("SELECT 6 <= 5").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gteq_both_false() {
    let session = create_session();
    let result = session.execute_sql("SELECT 4 >= 5").await.unwrap();
    assert_table_eq!(result, [[false]]);
}
