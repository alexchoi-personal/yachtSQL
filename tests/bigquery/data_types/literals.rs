use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_double_quoted_string() {
    let session = create_session();
    let result = session.execute_sql("SELECT \"hello\"").await.unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_double_quoted_string_with_single_quote() {
    let session = create_session();
    let result = session.execute_sql("SELECT \"it's a test\"").await.unwrap();
    assert_table_eq!(result, [["it's a test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_triple_single_quoted_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT '''hello world'''")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_triple_single_quoted_multiline() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT '''line1\nline2'''")
        .await
        .unwrap();
    assert_table_eq!(result, [["line1\nline2"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_triple_double_quoted_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT \"\"\"hello world\"\"\"")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_triple_double_quoted_with_single_quote() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT \"\"\"it's a test\"\"\"")
        .await
        .unwrap();
    assert_table_eq!(result, [["it's a test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_raw_string_single_quoted() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT r'hello\\nworld'")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello\\nworld"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_raw_string_double_quoted() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT r\"hello\\nworld\"")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello\\nworld"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_raw_string_uppercase_r() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT R'hello\\nworld'")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello\\nworld"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_raw_string_triple_single_quoted() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT r'''hello\\nworld'''")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello\\nworld"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_raw_string_triple_double_quoted() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT r\"\"\"hello\\nworld\"\"\"")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello\\nworld"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_raw_string_with_backslash() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT r'C:\\path\\to\\file'")
        .await
        .unwrap();
    assert_table_eq!(result, [["C:\\path\\to\\file"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_hex_string_literal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(x'48656c6c6f')")
        .await
        .unwrap();
    assert_table_eq!(result, [["Hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_hex_string_literal_uppercase() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(X'48454C4C4F')")
        .await
        .unwrap();
    assert_table_eq!(result, [["HELLO"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_hex_string_empty() {
    let session = create_session();
    let result = session.execute_sql("SELECT LENGTH(x'')").await.unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_float_literal() {
    let session = create_session();
    let result = session.execute_sql("SELECT 3.14159").await.unwrap();
    assert_table_eq!(result, [[3.14159]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_float_literal_scientific_notation() {
    let session = create_session();
    let result = session.execute_sql("SELECT 1.5e10 > 0").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_float_literal_negative_exponent() {
    let session = create_session();
    let result = session.execute_sql("SELECT 2.5e-3 < 1").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_literal_large() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 99999999999999999999999999999999999999 > 0")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_integer_literal() {
    let session = create_session();
    let result = session.execute_sql("SELECT 42").await.unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_integer_literal_negative() {
    let session = create_session();
    let result = session.execute_sql("SELECT -42").await.unwrap();
    assert_table_eq!(result, [[-42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_integer_literal_max() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 9223372036854775807 > 0")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_integer_literal_min() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT -9223372036854775808 < 0")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_literal_true() {
    let session = create_session();
    let result = session.execute_sql("SELECT TRUE").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_literal_false() {
    let session = create_session();
    let result = session.execute_sql("SELECT FALSE").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_literal() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL IS NULL").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_byte_string_single_quoted() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_byte_string_double_quoted() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b\"hello\")")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_unicode_escape() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT '\\u0048\\u0069'")
        .await
        .unwrap();
    assert_table_eq!(result, [["Hi"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_unicode_escape_uppercase() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT '\\U00000048\\U00000069'")
        .await
        .unwrap();
    assert_table_eq!(result, [["Hi"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_escape_newline() {
    let session = create_session();
    let result = session.execute_sql("SELECT 'hello\\nworld'").await.unwrap();
    assert_table_eq!(result, [["hello\nworld"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_escape_tab() {
    let session = create_session();
    let result = session.execute_sql("SELECT 'hello\\tworld'").await.unwrap();
    assert_table_eq!(result, [["hello\tworld"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_escape_carriage_return() {
    let session = create_session();
    let result = session.execute_sql("SELECT 'hello\\rworld'").await.unwrap();
    assert_table_eq!(result, [["hello\rworld"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_escape_backslash() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello\\\\world'")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello\\world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_escape_single_quote() {
    let session = create_session();
    let result = session.execute_sql("SELECT 'it\\'s'").await.unwrap();
    assert_table_eq!(result, [["it's"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_escape_double_quote() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'say \\\"hi\\\"'")
        .await
        .unwrap();
    assert_table_eq!(result, [["say \"hi\""]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_byte_string_escape_hex() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'\\x48\\x69')")
        .await
        .unwrap();
    assert_table_eq!(result, [["Hi"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_byte_string_escape_newline() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello\\nworld')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello\nworld"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_byte_string_escape_tab() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello\\tworld')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello\tworld"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_byte_string_escape_carriage_return() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello\\rworld')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello\rworld"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_byte_string_escape_backslash() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello\\\\world')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello\\world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_byte_string_escape_single_quote() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b\"it's\")")
        .await
        .unwrap();
    assert_table_eq!(result, [["it's"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_byte_string_escape_double_quote() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'say \\\"hi\\\"')")
        .await
        .unwrap();
    assert_table_eq!(result, [["say \"hi\""]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_literals_in_expression() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' || ' ' || \"world\"")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_float_with_leading_zero() {
    let session = create_session();
    let result = session.execute_sql("SELECT 0.5").await.unwrap();
    assert_table_eq!(result, [[0.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_float_without_leading_zero() {
    let session = create_session();
    let result = session.execute_sql("SELECT .5").await.unwrap();
    assert_table_eq!(result, [[0.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_float_with_trailing_zero() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5.0").await.unwrap();
    assert_table_eq!(result, [[5.0]]);
}
