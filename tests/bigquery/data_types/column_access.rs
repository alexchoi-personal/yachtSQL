use crate::assert_table_eq;
use crate::common::{create_session, d, dt, n, time, ts};

#[tokio::test(flavor = "current_thread")]
async fn test_bool_column_access_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (true), (NULL), (false)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM t ORDER BY val NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[null], [false], [true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_column_access_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (42), (NULL), (-100)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM t ORDER BY val NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[null], [-100], [42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_float64_column_access_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (3.14), (NULL), (-2.5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM t ORDER BY val NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[null], [-2.5], [3.14]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_column_access_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NUMERIC '123.45'), (NULL), (NUMERIC '-99.9')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM t ORDER BY val NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[null], [n("-99.9")], [n("123.45")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_column_access_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('hello'), (NULL), ('world')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM t ORDER BY val NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[null], ["hello"], ["world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_column_access_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val BYTES)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (b'abc'), (NULL), (b'xyz')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val IS NULL FROM t ORDER BY val NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[true], [false], [false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_date_column_access_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val DATE)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('2024-01-15'), (NULL), ('2023-06-30')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM t ORDER BY val NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[null], [d(2023, 6, 30)], [d(2024, 1, 15)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_time_column_access_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val TIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('10:30:00'), (NULL), ('23:59:59')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM t ORDER BY val NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[null], [time(10, 30, 0)], [time(23, 59, 59)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_column_access_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val DATETIME)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO t VALUES ('2024-01-15 10:30:00'), (NULL), ('2023-06-30 23:59:59')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM t ORDER BY val NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [null],
            [dt(2023, 6, 30, 23, 59, 59)],
            [dt(2024, 1, 15, 10, 30, 0)]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_timestamp_column_access_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val TIMESTAMP)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (TIMESTAMP '2024-01-15 10:30:00'), (NULL), (TIMESTAMP '2023-06-30 23:59:59')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM t ORDER BY val NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [null],
            [ts(2023, 6, 30, 23, 59, 59)],
            [ts(2024, 1, 15, 10, 30, 0)]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_column_access_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val JSON)")
        .await
        .unwrap();
    session
        .execute_sql(r#"INSERT INTO t VALUES (JSON '{"key": "value"}'), (NULL), (JSON '[1,2,3]')"#)
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val IS NULL FROM t ORDER BY CAST(val AS STRING) NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[true], [false], [false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_column_access_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ([1, 2]), (NULL), ([3])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val IS NULL FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[false], [true], [false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_column_access_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val STRUCT<name STRING, age INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (STRUCT('Alice', 30)), (NULL), (STRUCT('Bob', 25))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val IS NULL FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[false], [true], [false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_geography_column_access_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val GEOGRAPHY)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (ST_GEOGPOINT(0, 0)), (NULL), (ST_GEOGPOINT(1, 1))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val IS NULL FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[false], [true], [false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_column_access_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val INTERVAL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (INTERVAL 1 DAY), (NULL), (INTERVAL 2 HOUR)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val IS NULL FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[false], [true], [false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_column_access_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val RANGE<DATE>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (RANGE(DATE '2024-01-01', DATE '2024-12-31')), (NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val IS NULL FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[false], [true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_all_null_bool_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL), (NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_all_null_int64_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL), (NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_all_null_float64_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL), (NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_all_null_numeric_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL), (NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_all_null_string_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL), (NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_all_null_bytes_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val BYTES)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL), (NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_all_null_date_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val DATE)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL), (NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_all_null_time_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val TIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL), (NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_all_null_datetime_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val DATETIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL), (NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_all_null_timestamp_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val TIMESTAMP)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL), (NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_all_null_json_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val JSON)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL), (NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_all_null_array_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL), (NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_all_null_struct_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val STRUCT<x INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL), (NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_all_null_geography_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val GEOGRAPHY)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL), (NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_all_null_interval_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val INTERVAL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL), (NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_all_null_range_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val RANGE<DATE>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL), (NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_bool() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, true)")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = false WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_int64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 100)")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = 999 WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[999]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_float64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 3.14)")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = 9.99 WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[9.99]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_numeric() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, NUMERIC '123.45')")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = NUMERIC '999.99' WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[n("999.99")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_string() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 'hello')")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = 'updated' WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [["updated"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_bytes() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val BYTES)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, b'abc')")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = b'xyz' WHERE id = 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val IS NOT NULL FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_date() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val DATE)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, DATE '2024-01-01')")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = DATE '2025-12-25' WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[d(2025, 12, 25)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_time() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val TIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, TIME '10:00:00')")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = TIME '12:00:00' WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[time(12, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_datetime() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val DATETIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, DATETIME '2024-01-01 10:00:00')")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = DATETIME '2025-12-25 12:00:00' WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[dt(2025, 12, 25, 12, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_timestamp() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val TIMESTAMP)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, TIMESTAMP '2024-01-01 10:00:00 UTC')")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = TIMESTAMP '2025-12-25 12:00:00 UTC' WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[ts(2025, 12, 25, 12, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_json() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val JSON)")
        .await
        .unwrap();
    session
        .execute_sql(r#"INSERT INTO t VALUES (1, JSON '{"a": 1}')"#)
        .await
        .unwrap();
    session
        .execute_sql(r#"UPDATE t SET val = JSON '{"new": "json"}' WHERE id = 1"#)
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val IS NOT NULL FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_array() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, [1, 2])")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = [100] WHERE id = 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(val) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_struct() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val STRUCT<name STRING>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, STRUCT('Alice'))")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = STRUCT('Bob') WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val.name FROM t").await.unwrap();
    assert_table_eq!(result, [["Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_geography() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val GEOGRAPHY)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, ST_GEOGPOINT(0, 0))")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = ST_GEOGPOINT(5, 5) WHERE id = 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val IS NOT NULL FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_interval() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val INTERVAL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, INTERVAL 1 DAY)")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = INTERVAL 10 DAY WHERE id = 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT EXTRACT(DAY FROM val) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_range() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val RANGE<DATE>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, RANGE(DATE '2024-01-01', DATE '2024-06-30'))")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = RANGE(DATE '2025-01-01', DATE '2025-12-31') WHERE id = 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT RANGE_START(val) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2025, 1, 1)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_to_null_bool() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, true)")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = NULL WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_to_null_int64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 100)")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = NULL WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_to_null_float64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 3.14)")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = NULL WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_to_null_string() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 'hello')")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = NULL WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_to_null_bytes() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val BYTES)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, b'abc')")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = NULL WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_to_null_date() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val DATE)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, DATE '2024-01-01')")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = NULL WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_to_null_time() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val TIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, TIME '10:00:00')")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = NULL WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_to_null_datetime() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val DATETIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, DATETIME '2024-01-01 10:00:00')")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = NULL WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_to_null_timestamp() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val TIMESTAMP)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, TIMESTAMP '2024-01-01 10:00:00 UTC')")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = NULL WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_to_null_json() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val JSON)")
        .await
        .unwrap();
    session
        .execute_sql(r#"INSERT INTO t VALUES (1, JSON '{"a": 1}')"#)
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = NULL WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_to_null_array() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, [1, 2])")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = NULL WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_to_null_struct() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val STRUCT<x INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, STRUCT(100))")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = NULL WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_to_null_geography() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val GEOGRAPHY)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, ST_GEOGPOINT(0, 0))")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = NULL WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_to_null_interval() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val INTERVAL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, INTERVAL 1 DAY)")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = NULL WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_to_null_range() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val RANGE<DATE>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, RANGE(DATE '2024-01-01', DATE '2024-12-31'))")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = NULL WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_from_null_to_value_bool() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, NULL)")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = true WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_from_null_to_value_int64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, NULL)")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = 999 WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[999]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_update_from_null_to_value_string() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, NULL)")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = 'restored' WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [["restored"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multi_column_mixed_types_access() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE t (
            id INT64,
            bool_col BOOL,
            int_col INT64,
            float_col FLOAT64,
            numeric_col NUMERIC,
            string_col STRING,
            date_col DATE
        )",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO t VALUES
            (1, true, 100, 3.14, 123.45, 'hello', '2024-01-01'),
            (2, NULL, NULL, NULL, NULL, NULL, NULL)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, bool_col, int_col, float_col, string_col, date_col FROM t ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, true, 100, 3.14, "hello", d(2024, 1, 1)],
            [2, null, null, null, null, null]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_access_after_filter() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 'a'), (2, NULL), (3, 'c')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, val FROM t WHERE val IS NOT NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [3, "c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_access_with_limit() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM t ORDER BY val LIMIT 3")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_access_with_offset() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM t ORDER BY val LIMIT 2 OFFSET 2")
        .await
        .unwrap();
    assert_table_eq!(result, [[3], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_access_aggregate_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10), (NULL), (30), (NULL), (50)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(val), SUM(val), AVG(val), MIN(val), MAX(val) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[3, 90, 30.0, 10, 50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_access_in_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE a (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE b (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO a VALUES (1, 'one'), (2, NULL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO b VALUES (1, 100), (2, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a.id, a.val, b.val FROM a JOIN b ON a.id = b.id ORDER BY a.id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "one", 100], [2, null, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_access_in_subquery() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 100), (2, NULL), (3, 300)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM (SELECT id, val FROM t WHERE val IS NOT NULL) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_access_in_cte() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 'a'), (2, NULL), (3, 'c')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH filtered AS (SELECT id, val FROM t WHERE val IS NOT NULL)
         SELECT id, val FROM filtered ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [3, "c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_access_in_union() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE a (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE b (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO a VALUES (1), (NULL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO b VALUES (NULL), (2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM a UNION ALL SELECT val FROM b ORDER BY val NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[null], [null], [1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_access_group_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (category STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('A', 10), ('A', NULL), ('B', 20), (NULL, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT category, SUM(val) as total FROM t GROUP BY category ORDER BY category NULLS FIRST",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[null, 30], ["A", 10], ["B", 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bignumeric_column_access() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val BIGNUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO t VALUES (12345678901234567890.123456789012345678901234567890123456789)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val IS NOT NULL FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}
