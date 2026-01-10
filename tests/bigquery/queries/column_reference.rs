use yachtsql::RecordBatchVecExt;

use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_date_part_keyword_as_string_literal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE_TRUNC(DATE '2024-06-15', MONTH)")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let date_val = &records[0].values()[0];
    assert_eq!(date_val.as_date().unwrap().to_string(), "2024-06-01");
}

#[tokio::test(flavor = "current_thread")]
async fn test_date_part_keyword_year() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE_TRUNC(DATE '2024-06-15', YEAR)")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let date_val = &records[0].values()[0];
    assert_eq!(date_val.as_date().unwrap().to_string(), "2024-01-01");
}

#[tokio::test(flavor = "current_thread")]
async fn test_date_part_keyword_week() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE_TRUNC(DATE '2024-06-15', WEEK)")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let date_val = &records[0].values()[0];
    assert!(date_val.as_date().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_column_dot_access() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, info STRUCT<name STRING, age INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, STRUCT('Alice' AS name, 30 AS age))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT info.name, info.age FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice", 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_column_nested_dot_access() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (data STRUCT<inner_struct STRUCT<val INT64>>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (STRUCT(STRUCT(42 AS val) AS inner_struct))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT data.inner_struct.val FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualified_struct_column_dot_access() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, info STRUCT<name STRING, age INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, STRUCT('Bob' AS name, 25 AS age))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT t.info.name, t.info.age FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [["Bob", 25]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualified_struct_with_alias() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (data STRUCT<x INT64, y INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (STRUCT(10 AS x, 20 AS y))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT i.data.x, i.data.y FROM items AS i")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_column_dot_access() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, data JSON)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, JSON '{\"name\": \"Alice\", \"value\": 100}')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT STRING(data.name), INT64(data.value) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice", 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_column_nested_dot_access() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (data JSON)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (JSON '{\"outer\": {\"inner\": {\"val\": 42}}}')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT INT64(data.outer.inner.val) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualified_json_column_dot_access() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, info JSON)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, JSON '{\"city\": \"NYC\"}')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT STRING(t.info.city) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [["NYC"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualified_json_with_alias() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE records (data JSON)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO records VALUES (JSON '{\"a\": {\"b\": 99}}')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT INT64(r.data.a.b) FROM records AS r")
        .await
        .unwrap();
    assert_table_eq!(result, [[99]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variable_struct_access() {
    let session = create_session();
    session
        .execute_sql("DECLARE @var STRUCT<a INT64, b STRING> DEFAULT STRUCT(10 AS a, 'hello' AS b)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT @var.a, @var.b").await.unwrap();
    assert_table_eq!(result, [[10, "hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variable_nested_struct_access() {
    let session = create_session();
    session
        .execute_sql(
            "DECLARE @data STRUCT<outer STRUCT<inner INT64>> DEFAULT STRUCT(STRUCT(42 AS inner) AS outer)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT @data.outer.inner")
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_as_struct_basic() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE employees (id INT64, name STRING, salary INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO employees VALUES (1, 'Alice', 50000), (2, 'Bob', 60000)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT e FROM employees AS e ORDER BY e.id")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    assert_eq!(records.len(), 2);
    let first_struct = records[0].values()[0].as_struct().unwrap();
    assert_eq!(first_struct.len(), 3);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_join_qualified_access() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE a (id INT64, data STRUCT<val INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE b (aid INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO a VALUES (1, STRUCT(100 AS val))")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO b VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a.data.val, b.name FROM a JOIN b ON a.id = b.aid")
        .await
        .unwrap();
    assert_table_eq!(result, [[100, "test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_join_qualified_access() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t1 (id INT64, meta JSON)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE t2 (t1_id INT64, extra STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t1 VALUES (1, JSON '{\"key\": \"val\"}')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t2 VALUES (1, 'extra')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT STRING(t1.meta.key), t2.extra FROM t1 JOIN t2 ON t1.id = t2.t1_id")
        .await
        .unwrap();
    assert_table_eq!(result, [["val", "extra"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_column_in_filter() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (info STRUCT<category STRING, value INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (STRUCT('A' AS category, 10 AS value)), (STRUCT('B' AS category, 20 AS value))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT info.value FROM t WHERE info.category = 'A'")
        .await
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_column_in_order_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (data STRUCT<priority INT64, name STRING>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (STRUCT(3 AS priority, 'C' AS name)), (STRUCT(1 AS priority, 'A' AS name)), (STRUCT(2 AS priority, 'B' AS name))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT data.name FROM t ORDER BY data.priority")
        .await
        .unwrap();
    assert_table_eq!(result, [["A"], ["B"], ["C"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_column_in_group_by() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE sales (product STRUCT<category STRING, name STRING>, amount INT64)",
        )
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES (STRUCT('A' AS category, 'x' AS name), 10), (STRUCT('A' AS category, 'y' AS name), 20), (STRUCT('B' AS category, 'z' AS name), 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT product.category, SUM(amount) FROM sales GROUP BY product.category ORDER BY product.category")
        .await
        .unwrap();
    assert_table_eq!(result, [["A", 30], ["B", 30]]);
}
