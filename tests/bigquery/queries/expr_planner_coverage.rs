use chrono::Timelike;

use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_i64_min_value() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT -9223372036854775808")
        .await
        .unwrap();
    assert_table_eq!(result, [[-9223372036854775808_i64]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_i64_min_value_in_expression() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT -9223372036854775808 + 1")
        .await
        .unwrap();
    assert_table_eq!(result, [[-9223372036854775807_i64]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_true_literal() {
    let session = create_session();
    let result = session.execute_sql("SELECT TRUE IS TRUE").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_true_false_value() {
    let session = create_session();
    let result = session.execute_sql("SELECT FALSE IS TRUE").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_true_null_value() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL IS TRUE").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_true_expression() {
    let session = create_session();
    let result = session.execute_sql("SELECT (1 = 1) IS TRUE").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_false_literal() {
    let session = create_session();
    let result = session.execute_sql("SELECT FALSE IS FALSE").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_false_true_value() {
    let session = create_session();
    let result = session.execute_sql("SELECT TRUE IS FALSE").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_false_null_value() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL IS FALSE").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_false_expression() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT (1 = 2) IS FALSE")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_true_in_where_clause() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE flags (id INT64, active BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO flags VALUES (1, true), (2, false), (3, NULL)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id FROM flags WHERE active IS TRUE ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_false_in_where_clause() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE flags (id INT64, active BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO flags VALUES (1, true), (2, false), (3, NULL)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id FROM flags WHERE active IS FALSE ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ilike_basic() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'Hello' ILIKE 'hello'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ilike_with_wildcard() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'HELLO WORLD' ILIKE 'hello%'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ilike_case_mismatch_with_percent() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'Apple Pie' ILIKE '%PIE'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ilike_underscore_wildcard() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'CAT' ILIKE 'c_t'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ilike_no_match() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'Hello' ILIKE 'world'")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_ilike() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'Hello' NOT ILIKE 'world'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_ilike_match() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'Hello' NOT ILIKE 'HELLO'")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ilike_in_where_clause() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE names (name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO names VALUES ('Alice'), ('ALICE'), ('Bob'), ('BOB')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT name FROM names WHERE name ILIKE 'alice' ORDER BY name")
        .await
        .unwrap();
    assert_table_eq!(result, [["ALICE"], ["Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ilike_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULL ILIKE 'test'")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_triple_single_quoted_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT '''Hello\nWorld'''")
        .await
        .unwrap();
    assert_table_eq!(result, [["Hello\nWorld"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_triple_double_quoted_string() {
    let session = create_session();
    let result = session
        .execute_sql(
            r#"SELECT """Hello
World""""#,
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["Hello\nWorld"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_triple_quoted_string_with_quotes() {
    let session = create_session();
    let result = session
        .execute_sql(r#"SELECT '''He said "hello"'''"#)
        .await
        .unwrap();
    assert_table_eq!(result, [[r#"He said "hello""#]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_raw_single_quoted_string() {
    let session = create_session();
    let result = session
        .execute_sql(r"SELECT r'Hello\nWorld'")
        .await
        .unwrap();
    assert_table_eq!(result, [[r"Hello\nWorld"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_raw_double_quoted_string() {
    let session = create_session();
    let result = session
        .execute_sql(r#"SELECT r"Hello\nWorld""#)
        .await
        .unwrap();
    assert_table_eq!(result, [[r"Hello\nWorld"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_raw_string_preserves_backslash() {
    let session = create_session();
    let result = session
        .execute_sql(r"SELECT r'C:\Users\name'")
        .await
        .unwrap();
    assert_table_eq!(result, [[r"C:\Users\name"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_raw_triple_single_quoted_string() {
    let session = create_session();
    let result = session
        .execute_sql(r"SELECT r'''Hello\nWorld'''")
        .await
        .unwrap();
    assert_table_eq!(result, [[r"Hello\nWorld"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_raw_triple_double_quoted_string() {
    let session = create_session();
    let result = session
        .execute_sql(r#"SELECT r"""Hello\nWorld""""#)
        .await
        .unwrap();
    assert_table_eq!(result, [[r"Hello\nWorld"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_floor_expression() {
    let session = create_session();
    let result = session.execute_sql("SELECT FLOOR(3.7)").await.unwrap();
    assert_table_eq!(result, [[3.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_floor_negative() {
    let session = create_session();
    let result = session.execute_sql("SELECT FLOOR(-3.2)").await.unwrap();
    assert_table_eq!(result, [[-4.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_floor_integer() {
    let session = create_session();
    let result = session.execute_sql("SELECT FLOOR(5.0)").await.unwrap();
    assert_table_eq!(result, [[5.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ceil_expression() {
    let session = create_session();
    let result = session.execute_sql("SELECT CEIL(3.2)").await.unwrap();
    assert_table_eq!(result, [[4.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ceil_negative() {
    let session = create_session();
    let result = session.execute_sql("SELECT CEIL(-3.7)").await.unwrap();
    assert_table_eq!(result, [[-3.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ceil_integer() {
    let session = create_session();
    let result = session.execute_sql("SELECT CEIL(5.0)").await.unwrap();
    assert_table_eq!(result, [[5.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ceiling_alias() {
    let session = create_session();
    let result = session.execute_sql("SELECT CEILING(3.2)").await.unwrap();
    assert_table_eq!(result, [[4.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_both_explicit() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRIM(BOTH ' ' FROM '  hello  ')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_leading() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRIM(LEADING ' ' FROM '  hello  ')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello  "]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_trailing() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRIM(TRAILING ' ' FROM '  hello  ')")
        .await
        .unwrap();
    assert_table_eq!(result, [["  hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_leading_custom_char() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRIM(LEADING '*' FROM '***hello***')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello***"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_trailing_custom_char() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRIM(TRAILING '*' FROM '***hello***')")
        .await
        .unwrap();
    assert_table_eq!(result, [["***hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_both_custom_char() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRIM(BOTH '*' FROM '***hello***')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_isoweek() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(ISOWEEK FROM DATE '2024-01-08')")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_isoyear() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(ISOYEAR FROM DATE '2024-12-31')")
        .await
        .unwrap();
    assert_table_eq!(result, [[2025]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_quarter() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(QUARTER FROM DATE '2024-06-15')")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_dayofweek() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAYOFWEEK FROM DATE '2024-01-08')")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_dayofyear() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAYOFYEAR FROM DATE '2024-02-01')")
        .await
        .unwrap();
    assert_table_eq!(result, [[32]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_millisecond() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MILLISECOND FROM TIMESTAMP '2024-01-01 10:30:45.123')")
        .await
        .unwrap();
    assert_table_eq!(result, [[123]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_microsecond() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MICROSECOND FROM TIMESTAMP '2024-01-01 10:30:45.123456')")
        .await
        .unwrap();
    assert_table_eq!(result, [[123456]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_date_from_timestamp() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DATE FROM TIMESTAMP '2024-01-15 10:30:00')")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let date_val = &records[0].values()[0];
    assert_eq!(date_val.as_date().unwrap().to_string(), "2024-01-15");
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_time_from_timestamp() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(TIME FROM TIMESTAMP '2024-01-15 10:30:45')")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let time_val = &records[0].values()[0];
    assert_eq!(time_val.as_time().unwrap().to_string(), "10:30:45");
}

#[tokio::test(flavor = "current_thread")]
async fn test_hex_string_literal() {
    let session = create_session();
    let result = session.execute_sql("SELECT X'48454C4C4F'").await.unwrap();
    let records = result.to_records().unwrap();
    let bytes_val = records[0].values()[0].as_bytes().unwrap();
    assert_eq!(bytes_val, b"HELLO");
}

#[tokio::test(flavor = "current_thread")]
async fn test_hex_string_literal_lowercase() {
    let session = create_session();
    let result = session.execute_sql("SELECT X'68656c6c6f'").await.unwrap();
    let records = result.to_records().unwrap();
    let bytes_val = records[0].values()[0].as_bytes().unwrap();
    assert_eq!(bytes_val, b"hello");
}

#[tokio::test(flavor = "current_thread")]
async fn test_unary_plus() {
    let session = create_session();
    let result = session.execute_sql("SELECT +5").await.unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unary_plus_expression() {
    let session = create_session();
    let result = session.execute_sql("SELECT +(3 + 2)").await.unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_from_for() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTRING('hello world' FROM 1 FOR 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_from_only() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTRING('hello world' FROM 7)")
        .await
        .unwrap();
    assert_table_eq!(result, [["world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_for_only() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTRING('hello world' FOR 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_unnest_basic() {
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
async fn test_in_unnest_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'apple' IN UNNEST(['apple', 'banana', 'cherry'])")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_unnest_with_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, tags ARRAY<STRING>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, ['a', 'b', 'c']), (2, ['d', 'e', 'f'])")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id FROM items WHERE 'b' IN UNNEST(tags)")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_year() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-15' + INTERVAL 1 YEAR")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let date_val = &records[0].values()[0];
    assert_eq!(date_val.as_date().unwrap().to_string(), "2025-01-15");
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_month() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-15' + INTERVAL 3 MONTH")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let date_val = &records[0].values()[0];
    assert_eq!(date_val.as_date().unwrap().to_string(), "2024-04-15");
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_day() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-15' + INTERVAL 10 DAY")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let date_val = &records[0].values()[0];
    assert_eq!(date_val.as_date().unwrap().to_string(), "2024-01-25");
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_hour() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP '2024-01-15 10:00:00' + INTERVAL 3 HOUR")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let ts_val = &records[0].values()[0];
    let ts = ts_val.as_timestamp().unwrap();
    assert_eq!(ts.hour(), 13);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_minute() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP '2024-01-15 10:00:00' + INTERVAL 30 MINUTE")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let ts_val = &records[0].values()[0];
    let ts = ts_val.as_timestamp().unwrap();
    assert_eq!(ts.minute(), 30);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_second() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP '2024-01-15 10:00:00' + INTERVAL 45 SECOND")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let ts_val = &records[0].values()[0];
    let ts = ts_val.as_timestamp().unwrap();
    assert_eq!(ts.second(), 45);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_negative() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-15' + INTERVAL -5 DAY")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let date_val = &records[0].values()[0];
    assert_eq!(date_val.as_date().unwrap().to_string(), "2024-01-10");
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_millisecond() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(INTERVAL 500 MILLISECOND)")
        .await
        .unwrap();
    assert_table_eq!(result, [["INTERVAL"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_microsecond() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(INTERVAL 500 MICROSECOND)")
        .await
        .unwrap();
    assert_table_eq!(result, [["INTERVAL"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typed_string_date() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-06-15'")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let date_val = &records[0].values()[0];
    assert_eq!(date_val.as_date().unwrap().to_string(), "2024-06-15");
}

#[tokio::test(flavor = "current_thread")]
async fn test_typed_string_time() {
    let session = create_session();
    let result = session.execute_sql("SELECT TIME '10:30:45'").await.unwrap();
    let records = result.to_records().unwrap();
    let time_val = &records[0].values()[0];
    assert_eq!(time_val.as_time().unwrap().to_string(), "10:30:45");
}

#[tokio::test(flavor = "current_thread")]
async fn test_typed_string_datetime() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME '2024-06-15 10:30:45'")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let dt_val = &records[0].values()[0];
    assert!(dt_val.as_datetime().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn test_typed_string_timestamp() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP '2024-06-15 10:30:45'")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let ts_val = &records[0].values()[0];
    assert!(ts_val.as_timestamp().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn test_typed_string_numeric() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NUMERIC '123.45'")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let num_val = &records[0].values()[0];
    assert!(num_val.as_numeric().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn test_typed_string_json() {
    let session = create_session();
    let result = session
        .execute_sql(r#"SELECT JSON '{"a": 1, "b": 2}'"#)
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let json_val = &records[0].values()[0];
    assert!(json_val.as_json().is_some());
}
