use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_contains() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_CONTAINS('hello world', 'hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_contains_false() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_CONTAINS('hello world', 'goodbye')")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_contains_pattern() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_CONTAINS('abc123', '[0-9]+')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_extract() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_EXTRACT('email@example.com', r'@(.+)')")
        .await
        .unwrap();
    assert_table_eq!(result, [["example.com"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_extract_all() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('a1b2c3', '[0-9]'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_replace() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_REPLACE('hello world', 'world', 'there')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello there"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_replace_pattern() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_REPLACE('abc123def', '[0-9]+', 'X')")
        .await
        .unwrap();
    assert_table_eq!(result, [["abcXdef"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_instr() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_INSTR('hello world', 'world')")
        .await
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_instr_not_found() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_INSTR('hello world', 'foo')")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_substr() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_SUBSTR('hello123world', '[0-9]+')")
        .await
        .unwrap();
    assert_table_eq!(result, [["123"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_contains_in_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE emails (id INT64, email STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO emails VALUES (1, 'user@gmail.com'), (2, 'user@yahoo.com'), (3, 'user@example.org')").await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id FROM emails WHERE REGEXP_CONTAINS(email, 'gmail|yahoo') ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_CONTAINS(NULL, 'pattern')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_case_insensitive() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_CONTAINS('HELLO', '(?i)hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_extract_with_group() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_EXTRACT('John Smith', r'(\\w+) (\\w+)', 2)")
        .await
        .unwrap();
    assert_table_eq!(result, [["Smith"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_replace_with_backreference() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_REPLACE('John Smith', r'(\\w+) (\\w+)', r'\\2, \\1')")
        .await
        .unwrap();
    assert_table_eq!(result, [["Smith, John"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_contains_null_pattern() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_CONTAINS('hello', NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_extract_no_match() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_EXTRACT('hello world', r'foo(bar)')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_extract_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_EXTRACT(NULL, r'pattern')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_extract_all_with_groups() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_EXTRACT_ALL('foo bar baz', r'(\\w+)')")
        .await
        .unwrap();
    assert_table_eq!(result, [[["foo", "bar", "baz"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_extract_all_no_match() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_EXTRACT_ALL('hello world', r'[0-9]+')")
        .await
        .unwrap();
    assert_table_eq!(result, [[[]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_extract_all_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_EXTRACT_ALL(NULL, r'pattern')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_instr_pattern_at_start() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_INSTR('hello world', '^hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_instr_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_INSTR(NULL, 'pattern')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_instr_null_pattern() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_INSTR('hello', NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_substr_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_SUBSTR(NULL, 'pattern')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_substr_null_pattern() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_SUBSTR('hello', NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_substr_no_match() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_SUBSTR('hello world', '[0-9]+')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_replace_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_REPLACE(NULL, 'a', 'b')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_replace_multiple_occurrences() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_REPLACE('aaa bbb aaa', 'aaa', 'ccc')")
        .await
        .unwrap();
    assert_table_eq!(result, [["ccc bbb ccc"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_replace_multiple_backreferences() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_REPLACE('a1b2c3', r'(\\w)(\\d)', r'\\2\\1')")
        .await
        .unwrap();
    assert_table_eq!(result, [["1a2b3c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_extract_group_zero() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_EXTRACT('John Smith', r'\\w+ \\w+', 0)")
        .await
        .unwrap();
    assert_table_eq!(result, [["John Smith"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_extract_group_first() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_EXTRACT('John Smith', r'(\\w+) (\\w+)', 1)")
        .await
        .unwrap();
    assert_table_eq!(result, [["John"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_contains_word_boundary() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_CONTAINS('hello world', r'\\bworld\\b')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_contains_special_chars() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_CONTAINS('test.value', r'\\.')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_extract_email() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_EXTRACT('Contact us at test@example.com', r'[\\w.+-]+@[\\w.-]+\\.[\\w]{2,}')")
        .await
        .unwrap();
    assert_table_eq!(result, [["test@example.com"]]);
}
