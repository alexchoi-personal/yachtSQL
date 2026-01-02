use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_literal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_hex_literal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'\\x48\\x65\\x6c\\x6c\\x6f')")
        .await
        .unwrap();
    assert_table_eq!(result, [["Hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_empty() {
    let session = create_session();
    let result = session.execute_sql("SELECT LENGTH(b'')").await.unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_in_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bindata (id INT64, data BYTES)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bindata VALUES (1, b'hello'), (2, b'world')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM bindata ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_length() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LENGTH(b'hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_concat() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(CONCAT(b'hello', b' ', b'world'))")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_substr() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(SUBSTR(b'hello', 2, 3))")
        .await
        .unwrap();
    assert_table_eq!(result, [["ell"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_to_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_to_bytes() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(CAST('hello' AS BYTES))")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_comparison() {
    let session = create_session();
    let result = session.execute_sql("SELECT b'abc' < b'abd'").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_equality() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT b'hello' = b'hello'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bindata (id INT64, data BYTES)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bindata VALUES (1, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM bindata WHERE data IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_from_base64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE64('aGVsbG8='))")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_to_base64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TO_BASE64(b'hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [["aGVsbG8="]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_from_hex() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_HEX('48656c6c6f'))")
        .await
        .unwrap();
    assert_table_eq!(result, [["Hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_to_hex() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TO_HEX(b'Hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [["48656c6c6f"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_left() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(LEFT(b'hello', 3))")
        .await
        .unwrap();
    assert_table_eq!(result, [["hel"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_right() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(RIGHT(b'hello', 3))")
        .await
        .unwrap();
    assert_table_eq!(result, [["llo"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_reverse() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(REVERSE(b'hello'))")
        .await
        .unwrap();
    assert_table_eq!(result, [["olleh"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_in_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bindata (id INT64, data BYTES)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bindata VALUES (1, b'hello'), (2, b'world'), (3, b'hello')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM bindata WHERE data = b'hello' ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_order_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bindata (id INT64, data BYTES)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bindata VALUES (1, b'c'), (2, b'a'), (3, b'b')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM bindata ORDER BY data")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [3], [1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_newline_escape() {
    let session = create_session();
    let result = session.execute_sql("SELECT LENGTH(b'\\n')").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_tab_escape() {
    let session = create_session();
    let result = session.execute_sql("SELECT LENGTH(b'\\t')").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_carriage_return_escape() {
    let session = create_session();
    let result = session.execute_sql("SELECT LENGTH(b'\\r')").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_backslash_escape() {
    let session = create_session();
    let result = session.execute_sql("SELECT LENGTH(b'\\\\')").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_single_quote_escape() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LENGTH(b\"\\'\")")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_double_quote_escape() {
    let session = create_session();
    let result = session.execute_sql("SELECT LENGTH(b'\\\"')").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_unknown_escape_sequence() {
    let session = create_session();
    let result = session.execute_sql("SELECT LENGTH(b'\\z')").await.unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_mixed_escapes() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LENGTH(b'hello\\nworld\\t!')")
        .await
        .unwrap();
    assert_table_eq!(result, [[13]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_uppercase_hex() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'\\X48\\X45\\X4C\\X4C\\X4F')")
        .await
        .unwrap();
    assert_table_eq!(result, [["HELLO"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_hex_escape_no_digits() {
    let session = create_session();
    let result = session.execute_sql("SELECT LENGTH(b'\\x')").await.unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_hex_escape_one_digit() {
    let session = create_session();
    let result = session.execute_sql("SELECT LENGTH(b'\\x4')").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_hex_escape_non_hex_char() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LENGTH(b'\\xGH')")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}
