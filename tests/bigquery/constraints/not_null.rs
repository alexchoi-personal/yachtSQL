use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_not_null_allows_non_null_values() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE users (id INT64 NOT NULL, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM users").await.unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullable_column_accepts_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE users (id INT64 NOT NULL, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM users WHERE name IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_tracks_not_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE test (id INT64 NOT NULL, value STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO test VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, value FROM test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "test"]]);
}
