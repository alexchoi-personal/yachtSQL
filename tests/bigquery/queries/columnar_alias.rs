use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_alias_simple_value() {
    let session = create_session();
    let result = session.execute_sql("SELECT 42 AS answer").await.unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alias_string_literal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello world' AS greeting")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alias_expression() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 10 * 5 + 3 AS calculated")
        .await
        .unwrap();
    assert_table_eq!(result, [[53]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alias_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL AS nothing").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alias_function_result() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT UPPER('test') AS uppercased")
        .await
        .unwrap();
    assert_table_eq!(result, [["TEST"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alias_in_subquery() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT sq.val FROM (SELECT 100 AS val) AS sq")
        .await
        .unwrap();
    assert_table_eq!(result, [[100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alias_with_case() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END AS decision")
        .await
        .unwrap();
    assert_table_eq!(result, [["yes"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alias_preserves_type() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 3.14 AS pi_approx, pi_approx + 0 FROM (SELECT 3.14 AS pi_approx)")
        .await
        .unwrap();
    assert_table_eq!(result, [[3.14, 3.14]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alias_boolean() {
    let session = create_session();
    let result = session.execute_sql("SELECT TRUE AS flag").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alias_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [1, 2, 3] AS numbers")
        .await
        .unwrap();
    assert_table_eq!(result, [[[1, 2, 3]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alias_struct() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRUCT(1 AS a, 2 AS b).a AS first_field")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alias_from_table_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 'apple'), (2, 'banana')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id AS item_id, name AS item_name FROM items ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "apple"], [2, "banana"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alias_reuse_in_expression() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT x, x * 2 AS doubled FROM (SELECT 5 AS x)")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, 10]]);
}
