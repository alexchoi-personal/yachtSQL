use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_string_concat_operator() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' || ' ' || 'world'")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_concat_empty() {
    let session = create_session();
    let result = session.execute_sql("SELECT 'test' || ''").await.unwrap();
    assert_table_eq!(result, [["test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_concat_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT 'test' || NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_with_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE names (first STRING, last STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO names VALUES ('John', 'Doe'), ('Jane', 'Smith')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT first || ' ' || last AS full_name FROM names ORDER BY first")
        .await
        .unwrap();
    assert_table_eq!(result, [["Jane Smith"], ["John Doe"]]);
}
