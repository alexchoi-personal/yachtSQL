use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_unary_minus_integer() {
    let session = create_session();
    let result = session.execute_sql("SELECT -42").await.unwrap();
    assert_table_eq!(result, [[-42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unary_minus_negative() {
    let session = create_session();
    let result = session.execute_sql("SELECT -(-42)").await.unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unary_minus_float() {
    let session = create_session();
    let result = session.execute_sql("SELECT -3.14").await.unwrap();
    assert_table_eq!(result, [[-3.14]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unary_plus() {
    let session = create_session();
    let result = session.execute_sql("SELECT +42").await.unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_not_true() {
    let session = create_session();
    let result = session.execute_sql("SELECT NOT TRUE").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_not_false() {
    let session = create_session();
    let result = session.execute_sql("SELECT NOT FALSE").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_not_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT NOT NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_not_expression() {
    let session = create_session();
    let result = session.execute_sql("SELECT NOT (5 > 3)").await.unwrap();
    assert_table_eq!(result, [[false]]);
}
