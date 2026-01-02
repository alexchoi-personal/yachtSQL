use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_md5() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TO_HEX(MD5('hello'))")
        .await
        .unwrap();
    assert_table_eq!(result, [["5d41402abc4b2a76b9719d911017c592"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sha256() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LENGTH(SHA256('hello'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[32]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sha512() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LENGTH(SHA512('hello'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[64]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_md5_with_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE users (name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO users VALUES ('alice'), ('bob')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name, TO_HEX(MD5(name)) AS hash FROM users ORDER BY name")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["alice", "6384e2b2184bcbf58eccf10ca7a6563c"],
            ["bob", "9f9d51bc70ef21ca5c14f307980a29d8"]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_hash_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT MD5(NULL) IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sha1() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LENGTH(SHA1('hello'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sha1_hex() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TO_HEX(SHA1('hello'))")
        .await
        .unwrap();
    assert_table_eq!(result, [["aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sha1_bytes() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LENGTH(SHA1(b'hello'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sha1_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SHA1(NULL) IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sha256_hex() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TO_HEX(SHA256('hello'))")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [["2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_sha256_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SHA256(NULL) IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sha512_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SHA512(NULL) IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_farm_fingerprint() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FARM_FINGERPRINT('hello') IS NOT NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_farm_fingerprint_bytes() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FARM_FINGERPRINT(b'hello') IS NOT NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_farm_fingerprint_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FARM_FINGERPRINT(NULL) IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_farm_fingerprint_consistent() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FARM_FINGERPRINT('test') = FARM_FINGERPRINT('test')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_farm_fingerprint_different() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FARM_FINGERPRINT('test1') != FARM_FINGERPRINT('test2')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_md5_bytes() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TO_HEX(MD5(b'hello'))")
        .await
        .unwrap();
    assert_table_eq!(result, [["5d41402abc4b2a76b9719d911017c592"]]);
}
