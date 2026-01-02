use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_array_literal_empty() {
    let session = create_session();
    let result = session.execute_sql("SELECT []").await.unwrap();
    assert_table_eq!(result, [[[]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_literal_single_element() {
    let session = create_session();
    let result = session.execute_sql("SELECT [42]").await.unwrap();
    assert_table_eq!(result, [[[42]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_literal_multiple_integers() {
    let session = create_session();
    let result = session.execute_sql("SELECT [1, 2, 3, 4, 5]").await.unwrap();
    assert_table_eq!(result, [[[1, 2, 3, 4, 5]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_literal_strings() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ['apple', 'banana', 'cherry']")
        .await
        .unwrap();
    assert_table_eq!(result, [[["apple", "banana", "cherry"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_literal_floats() {
    let session = create_session();
    let result = session.execute_sql("SELECT [1.1, 2.2, 3.3]").await.unwrap();
    assert_table_eq!(result, [[[1.1, 2.2, 3.3]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_literal_booleans() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [TRUE, FALSE, TRUE]")
        .await
        .unwrap();
    assert_table_eq!(result, [[[true, false, true]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_literal_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT [1, NULL, 3]").await.unwrap();
    assert_table_eq!(result, [[[1, null, 3]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_literal_expressions() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [1 + 1, 2 * 2, 3 * 3]")
        .await
        .unwrap();
    assert_table_eq!(result, [[[2, 4, 9]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_literal_function_calls() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [UPPER('a'), UPPER('b'), UPPER('c')]")
        .await
        .unwrap();
    assert_table_eq!(result, [[["A", "B", "C"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_literal_nested() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [[1, 2], [3, 4], [5, 6]]")
        .await
        .unwrap();
    assert_table_eq!(result, [[[[1, 2], [3, 4], [5, 6]]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_literal_from_table_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE points (x INT64, y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO points VALUES (1, 2), (3, 4)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT [x, y] AS coords FROM points ORDER BY x")
        .await
        .unwrap();
    assert_table_eq!(result, [[[1, 2]], [[3, 4]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_literal_with_cast() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [CAST(1 AS FLOAT64), CAST(2 AS FLOAT64)]")
        .await
        .unwrap();
    assert_table_eq!(result, [[[1.0, 2.0]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_literal_in_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE id IN UNNEST([1, 3]) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_concat_literal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_CONCAT([1, 2], [3, 4])")
        .await
        .unwrap();
    assert_table_eq!(result, [[[1, 2, 3, 4]]]);
}
