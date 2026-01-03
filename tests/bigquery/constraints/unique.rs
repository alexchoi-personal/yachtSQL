use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_unique_constraint_insert() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE users (id INT64, email STRING UNIQUE, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, 'alice@test.com', 'Alice')")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (2, 'bob@test.com', 'Bob')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM users")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unique_null_handling() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE users (id INT64, email STRING UNIQUE, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, NULL, 'Alice')")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (2, NULL, 'Bob')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM users WHERE email IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_composite_unique_insert() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE registrations (user_id INT64, event_id INT64, registered_at TIMESTAMP, UNIQUE (user_id, event_id))")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO registrations VALUES (1, 100, CURRENT_TIMESTAMP())")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO registrations VALUES (1, 101, CURRENT_TIMESTAMP())")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO registrations VALUES (2, 100, CURRENT_TIMESTAMP())")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM registrations")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}
