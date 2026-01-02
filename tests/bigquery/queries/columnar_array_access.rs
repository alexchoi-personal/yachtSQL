use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_first_element() {
    let session = create_session();
    let result = session.execute_sql("SELECT [10, 20, 30][1]").await.unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_middle_element() {
    let session = create_session();
    let result = session.execute_sql("SELECT [10, 20, 30][2]").await.unwrap();
    assert_table_eq!(result, [[20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_last_element() {
    let session = create_session();
    let result = session.execute_sql("SELECT [10, 20, 30][3]").await.unwrap();
    assert_table_eq!(result, [[30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_out_of_bounds() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [10, 20, 30][10]")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_zero_index() {
    let session = create_session();
    let result = session.execute_sql("SELECT [10, 20, 30][0]").await.unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_negative_index() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [10, 20, 30][-1]")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_offset_first() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [10, 20, 30][OFFSET(0)]")
        .await
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_offset_second() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [10, 20, 30][OFFSET(1)]")
        .await
        .unwrap();
    assert_table_eq!(result, [[20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_ordinal_first() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [10, 20, 30][ORDINAL(1)]")
        .await
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_ordinal_third() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [10, 20, 30][ORDINAL(3)]")
        .await
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_safe_offset_out_of_bounds() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [10, 20, 30][SAFE_OFFSET(10)]")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_safe_ordinal_out_of_bounds() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [10, 20, 30][SAFE_ORDINAL(10)]")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_string_elements() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ['apple', 'banana', 'cherry'][2]")
        .await
        .unwrap();
    assert_table_eq!(result, [["banana"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_null_array() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (arr ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT arr[1] FROM t").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_default_index() {
    let session = create_session();
    session.execute_sql("DECLARE idx INT64").await.unwrap();
    let result = session.execute_sql("SELECT [1, 2, 3][idx]").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_with_expression_index() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [10, 20, 30][1 + 1]")
        .await
        .unwrap();
    assert_table_eq!(result, [[20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_nested() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [[1, 2], [3, 4], [5, 6]][2][1]")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_from_table_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, values ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, [100, 200, 300]), (2, [400, 500, 600])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, values[1] AS first FROM data ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 100], [2, 400]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_null_element() {
    let session = create_session();
    let result = session.execute_sql("SELECT [1, NULL, 3][2]").await.unwrap();
    assert_table_eq!(result, [[null]]);
}
