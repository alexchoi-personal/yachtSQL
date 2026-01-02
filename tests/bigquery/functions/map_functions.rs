use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_map_basic() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(MAP('a', 1, 'b', 2))")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_map_empty() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(MAP())")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_map_single_pair() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(MAP('key', 'value'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_map_keys_basic() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT MAP_KEYS(MAP('a', 1, 'b', 2))")
        .await
        .unwrap();
    assert_table_eq!(result, [[["a", "b"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_map_keys_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT MAP_KEYS(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_map_values_basic() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT MAP_VALUES(MAP('a', 1, 'b', 2))")
        .await
        .unwrap();
    assert_table_eq!(result, [[[1, 2]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_map_values_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT MAP_VALUES(NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_map_with_int_keys() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(MAP(1, 'one', 2, 'two', 3, 'three'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_map_with_float_values() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(MAP('x', 1.5, 'y', 2.5))")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}
