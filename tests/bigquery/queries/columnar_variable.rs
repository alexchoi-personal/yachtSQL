use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_variable_integer() {
    let session = create_session();
    session
        .execute_sql("DECLARE x INT64 DEFAULT 42")
        .await
        .unwrap();
    let result = session.execute_sql("SELECT x").await.unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variable_string() {
    let session = create_session();
    session
        .execute_sql("DECLARE name STRING DEFAULT 'hello'")
        .await
        .unwrap();
    let result = session.execute_sql("SELECT name").await.unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variable_float() {
    let session = create_session();
    session
        .execute_sql("DECLARE pi FLOAT64 DEFAULT 3.14159")
        .await
        .unwrap();
    let result = session.execute_sql("SELECT pi").await.unwrap();
    assert_table_eq!(result, [[3.14159]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variable_boolean() {
    let session = create_session();
    session
        .execute_sql("DECLARE flag BOOL DEFAULT TRUE")
        .await
        .unwrap();
    let result = session.execute_sql("SELECT flag").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variable_null_default() {
    let session = create_session();
    session.execute_sql("DECLARE x INT64").await.unwrap();
    let result = session.execute_sql("SELECT x").await.unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variable_set_value() {
    let session = create_session();
    session.execute_sql("DECLARE x INT64").await.unwrap();
    session.execute_sql("SET x = 100").await.unwrap();
    let result = session.execute_sql("SELECT x").await.unwrap();
    assert_table_eq!(result, [[100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variable_update_value() {
    let session = create_session();
    session
        .execute_sql("DECLARE counter INT64 DEFAULT 0")
        .await
        .unwrap();
    session.execute_sql("SET counter = 5").await.unwrap();
    session
        .execute_sql("SET counter = counter + 1")
        .await
        .unwrap();
    let result = session.execute_sql("SELECT counter").await.unwrap();
    assert_table_eq!(result, [[6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variable_in_expression() {
    let session = create_session();
    session
        .execute_sql("DECLARE a INT64 DEFAULT 10")
        .await
        .unwrap();
    session
        .execute_sql("DECLARE b INT64 DEFAULT 20")
        .await
        .unwrap();
    let result = session.execute_sql("SELECT a + b").await.unwrap();
    assert_table_eq!(result, [[30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variable_in_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();
    session
        .execute_sql("DECLARE threshold INT64 DEFAULT 15")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE value > threshold ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variable_set_from_query() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nums (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nums VALUES (10), (20), (30)")
        .await
        .unwrap();
    session.execute_sql("DECLARE total INT64").await.unwrap();
    session
        .execute_sql("SET total = (SELECT SUM(val) FROM nums)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT total").await.unwrap();
    assert_table_eq!(result, [[60]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variable_multiple_declarations() {
    let session = create_session();
    session.execute_sql("DECLARE a, b, c INT64").await.unwrap();
    session.execute_sql("SET a = 1").await.unwrap();
    session.execute_sql("SET b = 2").await.unwrap();
    session.execute_sql("SET c = 3").await.unwrap();

    let result = session.execute_sql("SELECT a, b, c").await.unwrap();
    assert_table_eq!(result, [[1, 2, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variable_in_function() {
    let session = create_session();
    session
        .execute_sql("DECLARE s STRING DEFAULT 'hello'")
        .await
        .unwrap();
    let result = session.execute_sql("SELECT UPPER(s)").await.unwrap();
    assert_table_eq!(result, [["HELLO"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variable_array() {
    let session = create_session();
    session
        .execute_sql("DECLARE arr ARRAY<INT64> DEFAULT [1, 2, 3]")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(arr)")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variable_conditional_set() {
    let session = create_session();
    session
        .execute_sql("DECLARE x INT64 DEFAULT 5")
        .await
        .unwrap();
    session
        .execute_sql("SET x = IF(x > 3, x * 2, x)")
        .await
        .unwrap();
    let result = session.execute_sql("SELECT x").await.unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variable_date() {
    let session = create_session();
    session
        .execute_sql("DECLARE d DATE DEFAULT DATE '2024-01-15'")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT EXTRACT(DAY FROM d)")
        .await
        .unwrap();
    assert_table_eq!(result, [[15]]);
}
