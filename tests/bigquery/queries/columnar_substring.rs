use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_substring_from_start() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTRING('hello world', 1, 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_from_middle() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTRING('hello world', 7, 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [["world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_no_length() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTRING('hello world', 7)")
        .await
        .unwrap();
    assert_table_eq!(result, [["world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_negative_position() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTRING('hello world', -5)")
        .await
        .unwrap();
    assert_table_eq!(result, [["world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_negative_with_length() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTRING('hello world', -5, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [["wor"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_zero_position() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTRING('hello', 0, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [["hel"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_length_exceeds() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTRING('hello', 1, 100)")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_start_beyond_end() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTRING('hello', 100)")
        .await
        .unwrap();
    assert_table_eq!(result, [[""]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_zero_length() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTRING('hello', 2, 0)")
        .await
        .unwrap();
    assert_table_eq!(result, [[""]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_null_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTRING(NULL, 1, 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_null_position() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTRING('hello', NULL, 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_null_length() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTRING('hello', 2, NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [["ello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_unicode() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTRING('mañana', 3, 4)")
        .await
        .unwrap();
    assert_table_eq!(result, [["ñana"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_emoji() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LENGTH(SUBSTRING('hello', 1, 3))")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_empty_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTRING('', 1, 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[""]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_single_char() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTRING('abcdef', 3, 1)")
        .await
        .unwrap();
    assert_table_eq!(result, [["c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_with_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, text STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 'hello world'), (2, 'foo bar baz')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, SUBSTRING(text, 1, 5) FROM data ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "hello"], [2, "foo b"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substr_alias() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTR('hello world', 1, 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substring_bytes() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(SUBSTRING(b'hello world', 1, 5))")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}
