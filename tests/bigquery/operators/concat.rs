use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_concat_strings() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' || ' ' || 'world'")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_two_strings() {
    let session = create_session();
    let result = session.execute_sql("SELECT 'foo' || 'bar'").await.unwrap();
    assert_table_eq!(result, [["foobar"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_empty_strings() {
    let session = create_session();
    let result = session.execute_sql("SELECT '' || ''").await.unwrap();
    assert_table_eq!(result, [[""]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_with_empty_left() {
    let session = create_session();
    let result = session.execute_sql("SELECT '' || 'abc'").await.unwrap();
    assert_table_eq!(result, [["abc"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_with_empty_right() {
    let session = create_session();
    let result = session.execute_sql("SELECT 'abc' || ''").await.unwrap();
    assert_table_eq!(result, [["abc"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_null_left() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL || 'world'").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_null_right() {
    let session = create_session();
    let result = session.execute_sql("SELECT 'hello' || NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_both_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(NULL AS STRING) || CAST(NULL AS STRING)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_string_and_int() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'value: ' || CAST(42 AS STRING)")
        .await
        .unwrap();
    assert_table_eq!(result, [["value: 42"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_string_and_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'pi: ' || CAST(3.14 AS STRING)")
        .await
        .unwrap();
    assert_table_eq!(result, [["pi: 3.14"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_multiple_strings() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'a' || 'b' || 'c' || 'd'")
        .await
        .unwrap();
    assert_table_eq!(result, [["abcd"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_with_column() {
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
        .execute_sql("SELECT first || ' ' || last FROM names ORDER BY first")
        .await
        .unwrap();
    assert_table_eq!(result, [["Jane Smith"], ["John Doe"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_with_nullable_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE names (first STRING, last STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO names VALUES ('John', 'Doe'), ('Jane', NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT first || ' ' || last FROM names ORDER BY first")
        .await
        .unwrap();
    assert_table_eq!(result, [[null], ["John Doe"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_unicode() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'Hello, ' || 'World!'")
        .await
        .unwrap();
    assert_table_eq!(result, [["Hello, World!"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_special_chars() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'tab\\t' || 'newline\\n'")
        .await
        .unwrap();
    assert_table_eq!(result, [["tab\tnewline\n"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_in_where_clause() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE prefixes (prefix STRING, suffix STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO prefixes VALUES ('hello', 'world'), ('foo', 'bar')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT prefix FROM prefixes WHERE prefix || suffix = 'helloworld'")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_add_strings_coercion() {
    let session = create_session();
    let result = session.execute_sql("SELECT 'abc' + 'def'").await.unwrap();
    assert_table_eq!(result, [["abcdef"]]);
}
