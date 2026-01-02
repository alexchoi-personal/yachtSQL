use crate::assert_table_eq;
use crate::common::{create_session, d, n, ts};

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_string_to_int64_valid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('123' AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[123]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_string_to_int64_invalid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('abc' AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_string_to_float64_valid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('1.25' AS FLOAT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[1.25]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_string_to_float64_invalid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('not_a_number' AS FLOAT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_string_to_date_valid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('2024-01-15' AS DATE)")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 15)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_string_to_date_invalid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('invalid-date' AS DATE)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_string_to_timestamp_valid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('2024-01-15 10:30:00' AS TIMESTAMP)")
        .await
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 1, 15, 10, 30, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_string_to_timestamp_invalid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('not-a-timestamp' AS TIMESTAMP)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_int_to_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST(123 AS STRING)")
        .await
        .unwrap();
    assert_table_eq!(result, [["123"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_float_to_int64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST(3.7 AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_overflow() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST(99999999999999999999 AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST(NULL AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_bool_to_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST(TRUE AS STRING)")
        .await
        .unwrap();
    assert_table_eq!(result, [["true"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_string_to_bool_valid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('true' AS BOOL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_string_to_bool_invalid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('maybe' AS BOOL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_in_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, '100'), (2, 'abc'), (3, '200')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE SAFE_CAST(value AS INT64) > 50 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_coalesce_pattern() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COALESCE(SAFE_CAST('abc' AS INT64), 0)")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_bytes_to_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST(b'hello' AS STRING)")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_invalid_utf8_bytes() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST(b'\\xff\\xfe' AS STRING)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_divide_basic() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_DIVIDE(10.0, 2.0)")
        .await
        .unwrap();
    assert_table_eq!(result, [[5.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_divide_by_zero() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_DIVIDE(10.0, 0.0)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_divide_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_DIVIDE(NULL, 2.0)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_multiply_basic() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_MULTIPLY(5, 10)")
        .await
        .unwrap();
    assert_table_eq!(result, [[50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_multiply_overflow() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_MULTIPLY(9223372036854775807, 2)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_add_basic() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_ADD(10, 20)")
        .await
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_add_overflow() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_ADD(9223372036854775807, 1)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_subtract_basic() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_SUBTRACT(30, 10)")
        .await
        .unwrap();
    assert_table_eq!(result, [[20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_subtract_overflow() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_SUBTRACT(-9223372036854775808, 1)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_negate_basic() {
    let session = create_session();
    let result = session.execute_sql("SELECT SAFE_NEGATE(10)").await.unwrap();
    assert_table_eq!(result, [[-10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_negate_min_int() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_NEGATE(-9223372036854775808)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_convert_bytes_to_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello world')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_convert_bytes_to_string_invalid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'\\x80\\x81\\x82')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_in_aggregation() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE values (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO values VALUES ('10'), ('20'), ('bad'), ('30')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT SUM(SAFE_CAST(val AS INT64)) FROM values")
        .await
        .unwrap();
    assert_table_eq!(result, [[60]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_numeric() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('123.456' AS NUMERIC)")
        .await
        .unwrap();
    assert_table_eq!(result, [[n("123.456")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_bignumeric() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('12345678901234567890.123456789' AS BIGNUMERIC)")
        .await
        .unwrap();
    assert_table_eq!(result, [[n("12345678901234567890.123456789")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST([1, 2, 3] AS ARRAY<STRING>)")
        .await
        .unwrap();
    assert_table_eq!(result, [[["1", "2", "3"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_offset() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT arr[SAFE_OFFSET(10)] FROM (SELECT [1, 2, 3] AS arr)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_ordinal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT arr[SAFE_ORDINAL(10)] FROM (SELECT [1, 2, 3] AS arr)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_int64_from_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_INT64('123')")
        .await
        .unwrap();
    assert_table_eq!(result, [[123]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_int64_invalid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_INT64('not_a_number')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_int64_from_float() {
    let session = create_session();
    let result = session.execute_sql("SELECT LAX_INT64(3.7)").await.unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_int64_from_bool() {
    let session = create_session();
    let result = session.execute_sql("SELECT LAX_INT64(TRUE)").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_float64_from_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_FLOAT64('3.14')")
        .await
        .unwrap();
    assert_table_eq!(result, [[3.14]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_float64_invalid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_FLOAT64('not_a_number')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_float64_from_int() {
    let session = create_session();
    let result = session.execute_sql("SELECT LAX_FLOAT64(42)").await.unwrap();
    assert_table_eq!(result, [[42.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_bool_from_string_true() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_BOOL('true')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_bool_from_string_false() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_BOOL('false')")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_bool_invalid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_BOOL('maybe')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_bool_from_int() {
    let session = create_session();
    let result = session.execute_sql("SELECT LAX_BOOL(1)").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_bool_from_int_zero() {
    let session = create_session();
    let result = session.execute_sql("SELECT LAX_BOOL(0)").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_string_from_int() {
    let session = create_session();
    let result = session.execute_sql("SELECT LAX_STRING(123)").await.unwrap();
    assert_table_eq!(result, [["123"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_string_from_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_STRING(3.14)")
        .await
        .unwrap();
    assert_table_eq!(result, [["3.14"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_string_from_bool() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_STRING(TRUE)")
        .await
        .unwrap();
    assert_table_eq!(result, [["true"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_string_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_STRING(NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cosine_distance() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COSINE_DISTANCE([1.0, 0.0], [0.0, 1.0])")
        .await
        .unwrap();
    assert_table_eq!(result, [[1.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cosine_distance_same_vector() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COSINE_DISTANCE([1.0, 2.0, 3.0], [1.0, 2.0, 3.0])")
        .await
        .unwrap();
    assert_table_eq!(result, [[0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cosine_distance_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COSINE_DISTANCE(NULL, [1.0, 2.0])")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_euclidean_distance() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EUCLIDEAN_DISTANCE([0.0, 0.0], [3.0, 4.0])")
        .await
        .unwrap();
    assert_table_eq!(result, [[5.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_euclidean_distance_same_point() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EUCLIDEAN_DISTANCE([1.0, 2.0], [1.0, 2.0])")
        .await
        .unwrap();
    assert_table_eq!(result, [[0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_euclidean_distance_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EUCLIDEAN_DISTANCE(NULL, [1.0, 2.0])")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}
