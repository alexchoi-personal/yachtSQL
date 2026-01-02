use crate::assert_table_eq;
use crate::common::{bytes, create_session, d, dt, n, time, ts};

#[tokio::test(flavor = "current_thread")]
async fn test_insert_single_row() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 'apple')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name FROM items")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "apple"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_multiple_rows() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name FROM items ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "apple"], [2, "banana"], [3, "cherry"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_single_row() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 'apple'), (2, 'banana')")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE items SET name = 'apricot' WHERE id = 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name FROM items ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "apricot"], [2, "banana"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_multiple_rows() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, category STRING, price INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO items VALUES (1, 'fruit', 10), (2, 'fruit', 20), (3, 'vegetable', 15)",
        )
        .await
        .unwrap();
    session
        .execute_sql("UPDATE items SET price = price * 2 WHERE category = 'fruit'")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, price FROM items ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 20], [2, 40], [3, 15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_all_rows() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, active BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, false), (2, false), (3, false)")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE items SET active = true")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, active FROM items ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, true], [2, true], [3, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_single_row() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry')")
        .await
        .unwrap();
    session
        .execute_sql("DELETE FROM items WHERE id = 2")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name FROM items ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "apple"], [3, "cherry"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_multiple_rows() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, category STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 'fruit'), (2, 'fruit'), (3, 'vegetable')")
        .await
        .unwrap();
    session
        .execute_sql("DELETE FROM items WHERE category = 'fruit'")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, category FROM items")
        .await
        .unwrap();
    assert_table_eq!(result, [[3, "vegetable"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_all_rows() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1), (2), (3)")
        .await
        .unwrap();
    session.execute_sql("DELETE FROM items").await.unwrap();

    let result = session.execute_sql("SELECT * FROM items").await.unwrap();
    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_bool() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, true), (2, false)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, val FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, true], [2, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_int64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 42), (2, -100), (3, 9223372036854775807)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, val FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 42], [2, -100], [3, 9223372036854775807_i64]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_float64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 3.14), (2, -2.5), (3, 1.5e10)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, val FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 3.14], [2, -2.5], [3, 1.5e10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_string() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 'hello'), (2, 'world'), (3, '')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, val FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "hello"], [2, "world"], [3, ""]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_bytes() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val BYTES)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, b'hello'), (2, b'world')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, val FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, bytes(b"hello")], [2, bytes(b"world")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_date() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val DATE)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, '2024-01-15'), (2, '2024-12-31')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, val FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, d(2024, 1, 15)], [2, d(2024, 12, 31)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_time() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val TIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, '10:30:00'), (2, '23:59:59')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, val FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, time(10, 30, 0)], [2, time(23, 59, 59)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_datetime() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val DATETIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, '2024-01-15 10:30:00'), (2, '2024-12-31 23:59:59')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, val FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, dt(2024, 1, 15, 10, 30, 0)],
            [2, dt(2024, 12, 31, 23, 59, 59)]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_timestamp() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val TIMESTAMP)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, '2024-01-15 10:30:00'), (2, '2024-12-31 23:59:59')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, val FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, ts(2024, 1, 15, 10, 30, 0)],
            [2, ts(2024, 12, 31, 23, 59, 59)]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_numeric() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, NUMERIC '123.456'), (2, NUMERIC '-99.99')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, val FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, n("123.456")], [2, n("-99.99")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_json() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val JSON)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, JSON '{\"key\": \"value\"}'), (2, JSON '[1, 2, 3]')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, JSON_VALUE(val, '$.key') FROM t WHERE id = 1")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "value"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_array() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, [10, 20, 30]), (2, [100])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, val FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, [10, 20, 30]], [2, [100]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_struct() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val STRUCT<name STRING, age INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, STRUCT('Alice' AS name, 30 AS age)), (2, STRUCT('Bob' AS name, 25 AS age))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, val.name, val.age FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice", 30], [2, "Bob", 25]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_geography() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val GEOGRAPHY)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, ST_GEOGPOINT(-122.4194, 37.7749))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, ST_ASTEXT(val) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "POINT(-122.4194 37.7749)"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_interval() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val INTERVAL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, INTERVAL 5 DAY), (2, INTERVAL 10 HOUR)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, EXTRACT(DAY FROM val) FROM t WHERE id = 1")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_bool() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, true), (2, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE val IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_int64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 42), (2, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE val IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_float64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 3.14), (2, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE val IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_string() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 'hello'), (2, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE val IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_bytes() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val BYTES)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, b'hello'), (2, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE val IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_date() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val DATE)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, '2024-01-15'), (2, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE val IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_time() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val TIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, '10:30:00'), (2, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE val IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_datetime() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val DATETIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, '2024-01-15 10:30:00'), (2, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE val IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_timestamp() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val TIMESTAMP)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, '2024-01-15 10:30:00'), (2, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE val IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_numeric() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 123.456), (2, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE val IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_json() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val JSON)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, JSON '{\"a\": 1}'), (2, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE val IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_array() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, [1, 2, 3]), (2, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE val IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_struct() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val STRUCT<name STRING, age INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, STRUCT('Alice' AS name, 30 AS age)), (2, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE val IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_geography() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val GEOGRAPHY)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, ST_GEOGPOINT(-122.4194, 37.7749)), (2, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE val IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_interval() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val INTERVAL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, INTERVAL 5 DAY), (2, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE val IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_table_basic() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING, active BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', true)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name, active FROM users")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice", true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_table_all_types() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE all_types (
                col_bool BOOL,
                col_int64 INT64,
                col_float64 FLOAT64,
                col_string STRING,
                col_bytes BYTES,
                col_date DATE,
                col_time TIME,
                col_datetime DATETIME,
                col_timestamp TIMESTAMP,
                col_numeric NUMERIC,
                col_json JSON,
                col_array ARRAY<INT64>,
                col_struct STRUCT<x INT64, y INT64>,
                col_geography GEOGRAPHY,
                col_interval INTERVAL
            )",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO all_types VALUES (
                true,
                42,
                3.14,
                'hello',
                b'world',
                '2024-01-15',
                '10:30:00',
                '2024-01-15 10:30:00',
                '2024-01-15 10:30:00',
                123.456,
                JSON '{\"key\": \"value\"}',
                [1, 2, 3],
                STRUCT(10 AS x, 20 AS y),
                ST_GEOGPOINT(-122.4194, 37.7749),
                INTERVAL 5 DAY
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT col_bool, col_int64, col_string FROM all_types")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, 42, "hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .await
        .unwrap();
    session
        .execute_sql("ALTER TABLE users ADD COLUMN age INT64")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE users SET age = 30 WHERE id = 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name, age FROM users")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice", 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_drop_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING, age INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', 30)")
        .await
        .unwrap();
    session
        .execute_sql("ALTER TABLE users DROP COLUMN age")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name FROM users")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_rename_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .await
        .unwrap();
    session
        .execute_sql("ALTER TABLE users RENAME COLUMN name TO full_name")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, full_name FROM users")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_rename_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE old_name (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO old_name VALUES (1, 'Alice')")
        .await
        .unwrap();
    session
        .execute_sql("ALTER TABLE old_name RENAME TO new_name")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name FROM new_name")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregation_count_int64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*), COUNT(val) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[3, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregation_sum_int64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT SUM(val) FROM t").await.unwrap();
    assert_table_eq!(result, [[60]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregation_sum_float64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 1.5), (2, 2.5), (3, 3.0)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT SUM(val) FROM t").await.unwrap();
    assert_table_eq!(result, [[7.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregation_sum_numeric() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 10.50), (2, 20.25), (3, 30.75)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT SUM(val) FROM t").await.unwrap();
    assert_table_eq!(result, [[n("61.50")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregation_avg_int64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT AVG(val) FROM t").await.unwrap();
    assert_table_eq!(result, [[20.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregation_avg_float64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 1.0), (2, 2.0), (3, 3.0)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT AVG(val) FROM t").await.unwrap();
    assert_table_eq!(result, [[2.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregation_min_max_int64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 100), (2, 50), (3, 200)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT MIN(val), MAX(val) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[50, 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregation_min_max_string() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 'banana'), (2, 'apple'), (3, 'cherry')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT MIN(val), MAX(val) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [["apple", "cherry"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregation_min_max_date() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val DATE)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, '2024-06-15'), (2, '2024-01-01'), (3, '2024-12-31')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT MIN(val), MAX(val) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 1), d(2024, 12, 31)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregation_group_by_string() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (category STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('A', 10), ('B', 20), ('A', 30), ('B', 40)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, SUM(val) FROM t GROUP BY category ORDER BY category")
        .await
        .unwrap();
    assert_table_eq!(result, [["A", 40], ["B", 60]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregation_group_by_date() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (sale_date DATE, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO t VALUES ('2024-01-01', 100), ('2024-01-01', 200), ('2024-01-02', 150)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT sale_date, SUM(amount) FROM t GROUP BY sale_date ORDER BY sale_date")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 1), 300], [d(2024, 1, 2), 150]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregation_with_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT SUM(val), AVG(val), COUNT(val) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[40, 20.0, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregation_array_agg() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (category STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('A', 1), ('A', 2), ('A', 3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(ARRAY_AGG(val)) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregation_string_agg() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT STRING_AGG(name, ',') FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [["a,b,c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_select_from_another_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE source (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE target (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO source VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO target SELECT * FROM source")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name FROM target ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"], [2, "Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_with_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 'hello'), (2, 'world')")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = NULL WHERE id = 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE val IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_array_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, vals ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, [1, 2, 3])")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET vals = [10, 20, 30] WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT id, vals FROM t").await.unwrap();
    assert_table_eq!(result, [[1, [10, 20, 30]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_struct_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, info STRUCT<name STRING, age INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, STRUCT('Alice' AS name, 30 AS age))")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET info = STRUCT('Alicia' AS name, 31 AS age) WHERE id = 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, info.name, info.age FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alicia", 31]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_with_complex_condition() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, category STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 'A', 10), (2, 'B', 20), (3, 'A', 30), (4, 'B', 40)")
        .await
        .unwrap();
    session
        .execute_sql("DELETE FROM t WHERE category = 'A' AND val > 15")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_inserts_updates_deletes() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 10)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (2, 20)")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = 15 WHERE id = 1")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (3, 30)")
        .await
        .unwrap();
    session
        .execute_sql("DELETE FROM t WHERE id = 2")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET val = val + 5")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, val FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 20], [3, 35]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_column_conversion_via_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE left_t (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE right_t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO left_t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO right_t VALUES (1, 100), (2, 200), (4, 400)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT l.id, l.name, r.value FROM left_t l JOIN right_t r ON l.id = r.id ORDER BY l.id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "a", 100], [2, "b", 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_column_conversion_left_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE left_t (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE right_t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO left_t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO right_t VALUES (1, 100), (2, 200)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT l.id, l.name, r.value FROM left_t l LEFT JOIN right_t r ON l.id = r.id ORDER BY l.id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "a", 100], [2, "b", 200], [3, "c", null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_field_access_in_subquery() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, category STRING, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 'A', 100), (2, 'B', 200), (3, 'A', 150)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, amount FROM t WHERE amount > (SELECT AVG(amount) FROM t) ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[2, 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_field_access_correlated_subquery() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE orders (id INT64, customer_id INT64, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE customers (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT c.name, (SELECT SUM(amount) FROM orders o WHERE o.customer_id = c.id) as total FROM customers c ORDER BY c.name")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice", 300], ["Bob", 150]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_with_nested_struct() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, info STRUCT<name STRING, address STRUCT<city STRING, zip STRING>>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, STRUCT('Alice' AS name, STRUCT('NYC' AS city, '10001' AS zip) AS address))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, info.name, info.address.city FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice", "NYC"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_with_array_of_struct() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, items ARRAY<STRUCT<name STRING, qty INT64>>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, [STRUCT('apple' AS name, 5 AS qty), STRUCT('banana' AS name, 3 AS qty)])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, ARRAY_LENGTH(items) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_update_with_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, name STRING, score INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 'Alice', 80), (2, 'Bob', 90), (3, 'Charlie', 85)")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE t SET score = score + 10, name = UPPER(name) WHERE score >= 85")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name, score FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[1, "Alice", 80], [2, "BOB", 100], [3, "CHARLIE", 95]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_merge_all_clauses() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE target (id INT64, name STRING, status STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE source (id INT64, name STRING, active BOOL)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO target VALUES (1, 'a', 'active'), (2, 'b', 'active'), (3, 'c', 'active')",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO source VALUES (2, 'b_new', true), (3, 'c_new', false), (4, 'd', true)",
        )
        .await
        .unwrap();

    session
        .execute_sql("MERGE INTO target T USING source S ON T.id = S.id WHEN MATCHED AND S.active = false THEN DELETE WHEN MATCHED THEN UPDATE SET name = S.name WHEN NOT MATCHED THEN INSERT (id, name, status) VALUES (S.id, S.name, 'new')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name, status FROM target ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[1, "a", "active"], [2, "b_new", "active"], [4, "d", "new"]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_delete_with_subquery() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE products (id INT64, name STRING, price INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE blacklist (product_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO products VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO blacklist VALUES (2)")
        .await
        .unwrap();

    session
        .execute_sql("DELETE FROM products WHERE id IN (SELECT product_id FROM blacklist)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM products ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_update_with_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE products (id INT64, category STRING, price INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO products VALUES (1, 'A', 100), (2, 'B', 200), (3, 'A', 300)")
        .await
        .unwrap();

    session
        .execute_sql("UPDATE products SET price = price * 2 WHERE category = 'A'")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, price FROM products ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 200], [2, 200], [3, 600]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_window_function_with_partition() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sales (id INT64, region STRING, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES (1, 'East', 100), (2, 'East', 200), (3, 'West', 150), (4, 'West', 250)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, region, amount, SUM(amount) OVER (PARTITION BY region) as region_total FROM sales ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, "East", 100, 300],
            [2, "East", 200, 300],
            [3, "West", 150, 400],
            [4, "West", 250, 400]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_cte_with_recursive() {
    let session = create_session();

    let result = session
        .execute_sql("WITH RECURSIVE nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 5) SELECT n FROM nums ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3], [4], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_values_clause() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t (SELECT * FROM UNNEST([STRUCT(1 AS id, 'a' AS name), STRUCT(2 AS id, 'b' AS name)]))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [2, "b"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_cross_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t1 (a INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE t2 (b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t1 VALUES (1), (2)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t2 VALUES (10), (20)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a, b FROM t1 CROSS JOIN t2 ORDER BY a, b")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [1, 20], [2, 10], [2, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_full_outer_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE left_t (id INT64, lval STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE right_t (id INT64, rval STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO left_t VALUES (1, 'a'), (2, 'b')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO right_t VALUES (2, 'x'), (3, 'y')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COALESCE(l.id, r.id) as id, l.lval, r.rval FROM left_t l FULL OUTER JOIN right_t r ON l.id = r.id ORDER BY 1")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "a", null], [2, "b", "x"], [3, null, "y"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_unnest_array() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, tags ARRAY<STRING>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, ['a', 'b']), (2, ['c'])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, tag FROM t, UNNEST(tags) AS tag ORDER BY id, tag")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [1, "b"], [2, "c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_empty_table_operations() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, name STRING)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM t").await.unwrap();
    assert_table_eq!(result, []);

    let result = session.execute_sql("SELECT COUNT(*) FROM t").await.unwrap();
    assert_table_eq!(result, [[0]]);

    session
        .execute_sql("UPDATE t SET name = 'x' WHERE id = 1")
        .await
        .unwrap();

    session
        .execute_sql("DELETE FROM t WHERE id = 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM t").await.unwrap();
    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_large_batch_insert() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .await
        .unwrap();

    let mut values = String::new();
    for i in 1..=100 {
        if i > 1 {
            values.push_str(", ");
        }
        values.push_str(&format!("({}, {})", i, i * 10));
    }
    session
        .execute_sql(&format!("INSERT INTO t VALUES {}", values))
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*), SUM(val), MIN(id), MAX(id) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[100, 50500, 1, 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_multiple_columns_all_types() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (c_bool BOOL, c_int64 INT64, c_float64 FLOAT64, c_string STRING, c_bytes BYTES, c_date DATE, c_time TIME, c_datetime DATETIME, c_timestamp TIMESTAMP, c_numeric NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (true, 42, 3.14, 'hello', b'world', '2024-01-15', '10:30:00', '2024-01-15 10:30:00', '2024-01-15 10:30:00', 123.45)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT c_bool, c_int64, c_string FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, 42, "hello"]]);

    session
        .execute_sql("UPDATE t SET c_int64 = c_int64 * 2, c_string = UPPER(c_string)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT c_int64, c_string FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[84, "HELLO"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_self_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE employees (id INT64, name STRING, manager_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO employees VALUES (1, 'CEO', NULL), (2, 'VP', 1), (3, 'Manager', 2), (4, 'Staff', 3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT e.name AS employee, m.name AS manager FROM employees e LEFT JOIN employees m ON e.manager_id = m.id ORDER BY e.id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["CEO", null],
            ["VP", "CEO"],
            ["Manager", "VP"],
            ["Staff", "Manager"]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_group_by_multiple_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sales (region STRING, product STRING, qty INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES ('East', 'A', 10), ('East', 'B', 20), ('West', 'A', 15), ('West', 'A', 25), ('East', 'A', 5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT region, product, SUM(qty) as total FROM sales GROUP BY region, product ORDER BY region, product")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [["East", "A", 15], ["East", "B", 20], ["West", "A", 40]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_distinct() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (category STRING, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO t VALUES ('A', 'x'), ('B', 'y'), ('A', 'x'), ('A', 'z'), ('B', 'y')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT category, name FROM t ORDER BY category, name")
        .await
        .unwrap();
    assert_table_eq!(result, [["A", "x"], ["A", "z"], ["B", "y"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_order_by_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 30), (2, NULL), (3, 10), (4, NULL), (5, 20)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, val FROM t ORDER BY val NULLS FIRST, id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2, null], [4, null], [3, 10], [5, 20], [1, 30]]);

    let result = session
        .execute_sql("SELECT id, val FROM t ORDER BY val NULLS LAST, id")
        .await
        .unwrap();
    assert_table_eq!(result, [[3, 10], [5, 20], [1, 30], [2, null], [4, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_limit_offset() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t ORDER BY id LIMIT 3")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);

    let result = session
        .execute_sql("SELECT id FROM t ORDER BY id LIMIT 3 OFFSET 2")
        .await
        .unwrap();
    assert_table_eq!(result, [[3], [4], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_having_clause() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE orders (customer_id INT64, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO orders VALUES (1, 100), (1, 200), (2, 50), (2, 60), (3, 500)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id HAVING SUM(amount) > 100 ORDER BY customer_id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 300], [2, 110], [3, 500]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_case_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, score INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 95), (2, 75), (3, 55), (4, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' WHEN score >= 50 THEN 'C' ELSE 'F' END as grade FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "A"], [2, "B"], [3, "C"], [4, "F"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_record_coalesce_and_nullif() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, a INT64, b INT64, c INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, NULL, NULL, 30), (2, 10, NULL, 30), (3, 10, 20, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, COALESCE(a, b, c) as first_val FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 30], [2, 10], [3, 10]]);

    let result = session
        .execute_sql("SELECT id, NULLIF(c, 30) as nullified FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, null], [2, null], [3, null]]);
}
