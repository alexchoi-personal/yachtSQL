use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_primary_key_basic_insert() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE users (id INT64 PRIMARY KEY, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (2, 'Bob')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM users ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"], [2, "Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_composite_primary_key_insert() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE order_items (order_id INT64, item_id INT64, quantity INT64, PRIMARY KEY (order_id, item_id))")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO order_items VALUES (1, 1, 5)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO order_items VALUES (1, 2, 3)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO order_items VALUES (2, 1, 7)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM order_items")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}
