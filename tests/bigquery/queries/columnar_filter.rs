use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_between_integers() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE scores (id INT64, score INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO scores VALUES (1, 50), (2, 75), (3, 90), (4, 30), (5, 100)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM scores WHERE score BETWEEN 40 AND 80 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_between_floats() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE measurements (id INT64, value FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO measurements VALUES (1, 1.5), (2, 2.5), (3, 3.5), (4, 4.5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM measurements WHERE value BETWEEN 2.0 AND 3.5 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_between_strings() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE words (id INT64, word STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO words VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry'), (4, 'date')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM words WHERE word BETWEEN 'banana' AND 'date' ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [3], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_not_between() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 5), (2, 15), (3, 25), (4, 35)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE value NOT BETWEEN 10 AND 30 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_between_with_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nullable (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1, 10), (2, NULL), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM nullable WHERE value BETWEEN 5 AND 25 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_between_column_bounds() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ranges (id INT64, value INT64, low INT64, high INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ranges VALUES (1, 15, 10, 20), (2, 5, 10, 20), (3, 25, 10, 20)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM ranges WHERE value BETWEEN low AND high ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_in_list_integers() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, category INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM items WHERE category IN (10, 30) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_in_list_strings() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE products (id INT64, status STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO products VALUES (1, 'active'), (2, 'pending'), (3, 'inactive'), (4, 'active')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM products WHERE status IN ('active', 'pending') ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_not_in_list() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, type_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 1), (2, 2), (3, 3), (4, 4)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM items WHERE type_id NOT IN (2, 4) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_in_list_with_null_value() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, NULL), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE val IN (10, 30) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_in_list_with_null_in_list() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE val IN (10, NULL, 30) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_in_list_single_element() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 100), (2, 200), (3, 100)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE val IN (100) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_is_distinct_from_basic() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE pairs (id INT64, a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO pairs VALUES (1, 1, 1), (2, 1, 2), (3, NULL, 1), (4, NULL, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM pairs WHERE a IS DISTINCT FROM b ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_is_not_distinct_from_basic() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE pairs (id INT64, a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO pairs VALUES (1, 1, 1), (2, 1, 2), (3, NULL, 1), (4, NULL, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM pairs WHERE a IS NOT DISTINCT FROM b ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_is_distinct_from_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 10), (2, NULL), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM items WHERE value IS DISTINCT FROM NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_is_not_distinct_from_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 10), (2, NULL), (3, 30), (4, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM items WHERE value IS NOT DISTINCT FROM NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_is_distinct_from_strings() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE strings (id INT64, a STRING, b STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO strings VALUES (1, 'foo', 'foo'), (2, 'foo', 'bar'), (3, NULL, 'baz')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM strings WHERE a IS DISTINCT FROM b ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_like_simple() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE names (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO names VALUES (1, 'John'), (2, 'Jane'), (3, 'Bob'), (4, 'Jennifer')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM names WHERE name LIKE 'J%' ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_like_underscore() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE codes (id INT64, code STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO codes VALUES (1, 'A1'), (2, 'A12'), (3, 'B1'), (4, 'A2')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM codes WHERE code LIKE 'A_' ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_not_like() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE files (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO files VALUES (1, 'doc.txt'), (2, 'img.png'), (3, 'data.txt'), (4, 'video.mp4')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM files WHERE name NOT LIKE '%.txt' ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_like_with_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 'test'), (2, NULL), (3, 'testing')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE val LIKE 'test%' ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_in_unnest_basic() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, tags ARRAY<STRING>)")
        .await
        .unwrap();
    session
        .execute_sql(
            r#"INSERT INTO items VALUES (1, ['a', 'b']), (2, ['c', 'd']), (3, ['a', 'e'])"#,
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM items WHERE 'a' IN UNNEST(tags) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_not_in_unnest() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, tags ARRAY<STRING>)")
        .await
        .unwrap();
    session
        .execute_sql(
            r#"INSERT INTO items VALUES (1, ['a', 'b']), (2, ['c', 'd']), (3, ['a', 'e'])"#,
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM items WHERE 'a' NOT IN UNNEST(tags) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_in_unnest_integers() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, nums ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, [1, 2, 3]), (2, [4, 5, 6]), (3, [1, 5, 9])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE 1 IN UNNEST(nums) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_is_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nullable (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1, 10), (2, NULL), (3, 30), (4, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM nullable WHERE value IS NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_is_not_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nullable (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1, 10), (2, NULL), (3, 30), (4, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM nullable WHERE value IS NOT NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_case_in_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 10), (2, 50), (3, 100)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id FROM items WHERE CASE WHEN value > 30 THEN 'high' ELSE 'low' END = 'high' ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_combined_between_and_in() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE products (id INT64, price FLOAT64, category STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO products VALUES (1, 50.0, 'A'), (2, 150.0, 'B'), (3, 250.0, 'A'), (4, 350.0, 'C')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id FROM products WHERE price BETWEEN 100.0 AND 300.0 AND category IN ('A', 'B') ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_combined_is_distinct_and_like() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE records (id INT64, name STRING, status STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO records VALUES (1, 'test_item', 'active'), (2, 'prod_item', NULL), (3, 'test_other', 'active')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id FROM records WHERE name LIKE 'test%' AND status IS NOT DISTINCT FROM 'active' ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_or_with_different_operators() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 5, 'alice'), (2, 50, 'bob'), (3, 100, 'alice'), (4, 15, 'charlie')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id FROM data WHERE value BETWEEN 10 AND 20 OR name LIKE 'alice%' ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_nested_and_or() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO items VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300), (4, 10, 400)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id FROM items WHERE (a IN (10, 20) AND b > 150) OR (a = 30 AND b < 350) ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [3], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_with_scalar_function() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO users VALUES (1, 'John'), (2, 'JANE'), (3, 'bob')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM users WHERE UPPER(name) LIKE 'J%' ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_coalesce_in_predicate() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, NULL), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE COALESCE(value, 0) > 5 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_arithmetic_in_predicate() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nums (id INT64, a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nums VALUES (1, 10, 5), (2, 20, 10), (3, 30, 15)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM nums WHERE a - b BETWEEN 5 AND 12 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_cast_in_predicate() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, '10'), (2, '50'), (3, '100')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE CAST(val AS INT64) BETWEEN 20 AND 80 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_not_operator() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE flags (id INT64, active BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO flags VALUES (1, true), (2, false), (3, true), (4, false)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM flags WHERE NOT active ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_negated_combined() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, status STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO items VALUES (1, 'active', 10), (2, 'pending', 20), (3, 'active', 30)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id FROM items WHERE NOT (status = 'pending' OR value > 25) ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_multiple_between() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ranges (id INT64, x INT64, y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ranges VALUES (1, 5, 50), (2, 15, 150), (3, 25, 250)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id FROM ranges WHERE x BETWEEN 10 AND 30 AND y BETWEEN 100 AND 200 ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_in_list_with_expressions() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nums (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nums VALUES (1, 4), (2, 6), (3, 8), (4, 10)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM nums WHERE value IN (2 * 2, 2 * 3, 2 * 5) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_between_with_expressions() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 5), (2, 10), (3, 15), (4, 20)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE value BETWEEN 2 * 3 AND 4 * 4 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_empty_result() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE value BETWEEN 100 AND 200")
        .await
        .unwrap();
    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_all_rows_match() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE value IN (10, 20, 30) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_large_in_list() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1), (5), (10), (50), (100)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id FROM data WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15) ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [5], [10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_complex_expression_chain() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE complex (id INT64, a INT64, b STRING, c FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO complex VALUES (1, 10, 'foo', 1.5), (2, 20, 'bar', 2.5), (3, 30, 'foo', 3.5), (4, 40, 'baz', 4.5)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id FROM complex
             WHERE a BETWEEN 15 AND 35
             AND b IN ('foo', 'bar')
             AND c IS NOT DISTINCT FROM c
             ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_is_distinct_from_column_comparison() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE compare (id INT64, a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO compare VALUES (1, 10, 10), (2, 10, 20), (3, NULL, 20), (4, NULL, NULL), (5, 30, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM compare WHERE a IS NOT DISTINCT FROM b ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_between_dates() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE events (id INT64, event_date DATE)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO events VALUES (1, DATE '2024-01-15'), (2, DATE '2024-02-15'), (3, DATE '2024-03-15')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id FROM events WHERE event_date BETWEEN DATE '2024-01-01' AND DATE '2024-02-28' ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_like_middle_pattern() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE strings (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO strings VALUES (1, 'hello_world'), (2, 'hello_there'), (3, 'goodbye_world')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM strings WHERE val LIKE '%_world%' ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_columnar_filter_like_exact_match() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 'test'), (2, 'test1'), (3, 'atest')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE name LIKE 'test' ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}
