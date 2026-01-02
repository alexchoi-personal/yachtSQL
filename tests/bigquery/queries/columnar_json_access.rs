use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_json_key_access_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON '{\"name\": \"Alice\"}', '$.name')")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_key_access_number() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON '{\"age\": 30}', '$.age')")
        .await
        .unwrap();
    assert_table_eq!(result, [["30"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_nested_key_access() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON '{\"user\": {\"name\": \"Bob\"}}', '$.user.name')")
        .await
        .unwrap();
    assert_table_eq!(result, [["Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_array_index_access() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON '{\"items\": [\"a\", \"b\", \"c\"]}', '$.items[1]')")
        .await
        .unwrap();
    assert_table_eq!(result, [["b"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_array_first_element() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON '{\"nums\": [10, 20, 30]}', '$.nums[0]')")
        .await
        .unwrap();
    assert_table_eq!(result, [["10"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_missing_key() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON '{\"name\": \"Alice\"}', '$.age')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_null_value() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON '{\"value\": null}', '$.value')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_deeply_nested() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON '{\"a\": {\"b\": {\"c\": \"deep\"}}}', '$.a.b.c')")
        .await
        .unwrap();
    assert_table_eq!(result, [["deep"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_array_of_objects() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON '{\"users\": [{\"name\": \"A\"}, {\"name\": \"B\"}]}', '$.users[1].name')")
        .await
        .unwrap();
    assert_table_eq!(result, [["B"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_access_with_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, json_col JSON)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO data VALUES (1, JSON '{\"val\": 100}'), (2, JSON '{\"val\": 200}')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, JSON_VALUE(json_col, '$.val') FROM data ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "100"], [2, "200"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_query_object() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT JSON_QUERY(JSON '{\"user\": {\"name\": \"Alice\", \"age\": 30}}', '$.user')",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["{\"age\":30,\"name\":\"Alice\"}"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_query_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_QUERY(JSON '{\"items\": [1, 2, 3]}', '$.items')")
        .await
        .unwrap();
    assert_table_eq!(result, [["[1,2,3]"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_access_null_json() {
    let session = create_session();
    session.execute_sql("DECLARE j JSON").await.unwrap();
    let result = session
        .execute_sql("SELECT JSON_VALUE(j, '$.key')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_array_out_of_bounds() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON '{\"arr\": [1, 2]}', '$.arr[10]')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}
