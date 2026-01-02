use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_trim_both_spaces() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRIM('   hello   ')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_no_spaces() {
    let session = create_session();
    let result = session.execute_sql("SELECT TRIM('hello')").await.unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_left_only() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRIM('   hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_right_only() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRIM('hello   ')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_custom_chars() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRIM('***hello***', '*')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_multiple_custom_chars() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRIM('xyzABCxyz', 'xyz')")
        .await
        .unwrap();
    assert_table_eq!(result, [["ABC"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_null_string() {
    let session = create_session();
    let result = session.execute_sql("SELECT TRIM(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_null_chars() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRIM('hello', NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_empty_string() {
    let session = create_session();
    let result = session.execute_sql("SELECT TRIM('')").await.unwrap();
    assert_table_eq!(result, [[""]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ltrim_spaces() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LTRIM('   hello   ')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello   "]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ltrim_custom_chars() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LTRIM('***hello***', '*')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello***"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ltrim_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT LTRIM(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_rtrim_spaces() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT RTRIM('   hello   ')")
        .await
        .unwrap();
    assert_table_eq!(result, [["   hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_rtrim_custom_chars() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT RTRIM('***hello***', '*')")
        .await
        .unwrap();
    assert_table_eq!(result, [["***hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_rtrim_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT RTRIM(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_tabs_newlines() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRIM('\t\nhello\t\n', '\t\n')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_with_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, text STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, '  hello  '), (2, '**world**')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, TRIM(text) FROM data WHERE id = 1")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "hello"]]);

    let result2 = session
        .execute_sql("SELECT id, TRIM(text, '*') FROM data WHERE id = 2")
        .await
        .unwrap();
    assert_table_eq!(result2, [[2, "world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_unicode() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRIM('  hëllo  ')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hëllo"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_all_whitespace() {
    let session = create_session();
    let result = session.execute_sql("SELECT TRIM('     ')").await.unwrap();
    assert_table_eq!(result, [[""]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_mixed_whitespace() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRIM(' \t hello \n ')")
        .await
        .unwrap();
    assert_table_eq!(result, [["\t hello \n"]]);
}
