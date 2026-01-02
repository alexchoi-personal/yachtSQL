use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_struct_single_field() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRUCT(42 AS value).value")
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_multiple_fields() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRUCT(1 AS a, 'hello' AS b, TRUE AS c).b")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_unnamed_fields() {
    let session = create_session();
    let result = session.execute_sql("SELECT STRUCT(1, 2, 3)").await.unwrap();
    assert_table_eq!(result, [[(1, 2, 3)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_mixed_named_unnamed() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRUCT(1 AS a, 2).a")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_with_expression() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRUCT(10 + 5 AS sum, 10 * 5 AS product).sum")
        .await
        .unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_with_function() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRUCT(UPPER('hello') AS upper, LENGTH('hello') AS len).upper")
        .await
        .unwrap();
    assert_table_eq!(result, [["HELLO"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_with_null_field() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRUCT(1 AS a, NULL AS b).b IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_nested() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRUCT(STRUCT(1 AS x, 2 AS y) AS point, 'center' AS name).point.x")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_with_array_field() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(STRUCT([1,2,3] AS arr, 'test' AS name).arr)")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_comparison_equal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRUCT(1 AS a, 2 AS b) = STRUCT(1 AS a, 2 AS b)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_comparison_not_equal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRUCT(1 AS a, 2 AS b) = STRUCT(1 AS a, 3 AS b)")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_from_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE people (id INT64, name STRING, age INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO people VALUES (1, 'Alice', 30), (2, 'Bob', 25)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT STRUCT(name AS n, age AS a).n FROM people ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_in_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [STRUCT(1 AS x), STRUCT(2 AS x), STRUCT(3 AS x)][2].x")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_float_field() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRUCT(3.14 AS pi, 2.71 AS e).pi")
        .await
        .unwrap();
    assert_table_eq!(result, [[3.14]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_boolean_field() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRUCT(TRUE AS flag, 'enabled' AS status).flag")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_deeply_nested() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT STRUCT(STRUCT(STRUCT(42 AS deep) AS level2) AS level1).level1.level2.deep",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
}
