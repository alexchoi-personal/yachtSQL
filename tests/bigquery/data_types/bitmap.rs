use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_is_null_filter_basic() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 100), (2, NULL), (3, 300), (4, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE value IS NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_not_null_filter_basic() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 100), (2, NULL), (3, 300), (4, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE value IS NOT NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_null_string_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 'Alice'), (2, NULL), (3, 'Bob')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE name IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_null_combined_with_and() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO t VALUES (1, NULL, NULL), (2, NULL, 10), (3, 10, NULL), (4, 10, 20)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE a IS NULL AND b IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_null_combined_with_or() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO t VALUES (1, NULL, NULL), (2, NULL, 10), (3, 10, NULL), (4, 10, 20)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE a IS NULL OR b IS NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_two_args() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COALESCE(NULL, 42)")
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_three_args_first_not_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COALESCE(10, NULL, 42)")
        .await
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_three_args_middle_not_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COALESCE(NULL, 20, 42)")
        .await
        .unwrap();
    assert_table_eq!(result, [[20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_three_args_last_not_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COALESCE(NULL, NULL, 42)")
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_all_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COALESCE(NULL, NULL, NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_with_strings() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COALESCE(NULL, 'hello', 'world')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_five_args() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COALESCE(NULL, NULL, NULL, NULL, 99)")
        .await
        .unwrap();
    assert_table_eq!(result, [[99]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_with_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 100), (2, NULL), (3, 300)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, COALESCE(value, -1) AS val FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 100], [2, -1], [3, 300]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ifnull_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT IFNULL(NULL, 100)")
        .await
        .unwrap();
    assert_table_eq!(result, [[100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ifnull_with_non_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT IFNULL(50, 100)").await.unwrap();
    assert_table_eq!(result, [[50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ifnull_with_strings() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT IFNULL(NULL, 'default')")
        .await
        .unwrap();
    assert_table_eq!(result, [["default"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ifnull_with_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 'Alice'), (2, NULL), (3, 'Bob')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, IFNULL(name, 'Unknown') AS name FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"], [2, "Unknown"], [3, "Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullif_equal() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULLIF(10, 10)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullif_not_equal() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULLIF(10, 20)").await.unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullif_with_strings() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULLIF('hello', 'hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullif_with_null_first() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULLIF(NULL, 10)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullif_with_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 0), (2, 5), (3, 0)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, NULLIF(val, 0) AS val FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, null], [2, 5], [3, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_greatest_with_one_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT GREATEST(1, NULL, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_greatest_all_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT GREATEST(NULL, NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_greatest_no_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT GREATEST(1, 5, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_least_with_one_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LEAST(1, NULL, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_least_all_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LEAST(NULL, NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_least_no_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT LEAST(1, 5, 3)").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_greatest_with_column_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, a INT64, b INT64, c INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 10, 20, 30), (2, NULL, 50, 40), (3, 100, NULL, 90)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, GREATEST(a, b, c) AS max_val FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 30], [2, null], [3, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_plus_number() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 + NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_minus_number() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL - 5").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_multiply_number() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 * NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_divide_number() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL / 5").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_plus_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL + NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_arithmetic_with_null_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 10, 20), (2, NULL, 30), (3, 40, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, a + b AS sum FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 30], [2, null], [3, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_equals_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL = NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_not_equals_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL != NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_value_equals_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 = NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_value_not_equals_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 != NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_less_than_value() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL < 5").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_greater_than_value() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL > 5").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_comparison_with_null_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 100), (2, NULL), (3, 50)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE value > 60 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_in_group_by_single_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (category STRING, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('A', 10), ('A', 20), (NULL, 30), ('B', 40), (NULL, 50)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, SUM(amount) AS total FROM t GROUP BY category ORDER BY category NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(result, [["A", 30], ["B", 40], [null, 80]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_in_group_by_count() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('A', 1), ('A', 2), (NULL, 3), (NULL, 4), ('B', 5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, COUNT(*) AS cnt FROM t GROUP BY category ORDER BY category NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(result, [["A", 2], ["B", 1], [null, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_in_group_by_multiple_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a STRING, b STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO t VALUES ('X', 'Y', 1), ('X', NULL, 2), (NULL, 'Y', 3), (NULL, NULL, 4)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a, b, SUM(value) AS total FROM t GROUP BY a, b ORDER BY a NULLS LAST, b NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["X", "Y", 1],
            ["X", null, 2],
            [null, "Y", 3],
            [null, null, 4]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_star_includes_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 100), (2, NULL), (3, 300), (4, NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT COUNT(*) FROM t").await.unwrap();
    assert_table_eq!(result, [[4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_column_excludes_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 100), (2, NULL), (3, 300), (4, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(value) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_star_vs_column_comparison() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO t VALUES (1, 10, 20), (2, NULL, 30), (3, 40, NULL), (4, NULL, NULL)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) AS total, COUNT(a) AS cnt_a, COUNT(b) AS cnt_b FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[4, 2, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_star_grouped_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('A', 10), ('A', NULL), (NULL, 20), (NULL, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, COUNT(*) AS cnt_all, COUNT(value) AS cnt_val FROM t GROUP BY category ORDER BY category NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(result, [["A", 2, 1], [null, 2, 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_inner_join_null_key_no_match() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE left_t (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE right_t (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO left_t VALUES (1, 'A'), (NULL, 'B'), (3, 'C')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO right_t VALUES (1, 'X'), (NULL, 'Y'), (3, 'Z')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.val, r.val FROM left_t l INNER JOIN right_t r ON l.id = r.id ORDER BY l.val",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["A", "X"], ["C", "Z"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_left_join_null_key() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE left_t (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE right_t (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO left_t VALUES (1, 'A'), (NULL, 'B'), (3, 'C')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO right_t VALUES (1, 'X'), (3, 'Z')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.val, r.val FROM left_t l LEFT JOIN right_t r ON l.id = r.id ORDER BY l.val",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["A", "X"], ["B", null], ["C", "Z"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_left_join_produces_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE orders (id INT64, user_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO orders VALUES (100, 1), (101, 1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT u.name, o.id FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.name, o.id NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Alice", 100],
            ["Alice", 101],
            ["Bob", null],
            ["Charlie", null]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_right_join_produces_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE orders (id INT64, user_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO orders VALUES (100, 1)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT o.id, u.name FROM orders o RIGHT JOIN users u ON o.user_id = u.id ORDER BY u.name")
        .await
        .unwrap();
    assert_table_eq!(result, [[100, "Alice"], [null, "Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_full_outer_join_produces_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE left_t (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE right_t (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO left_t VALUES (1, 'A'), (2, 'B')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO right_t VALUES (2, 'X'), (3, 'Y')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT l.val, r.val FROM left_t l FULL OUTER JOIN right_t r ON l.id = r.id ORDER BY l.val NULLS LAST, r.val NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(result, [["A", null], ["B", "X"], [null, "Y"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_anti_join_with_null_check() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE orders (id INT64, user_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO orders VALUES (100, 1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT u.name FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE o.id IS NULL ORDER BY u.name")
        .await
        .unwrap();
    assert_table_eq!(result, [["Bob"], ["Charlie"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_with_null_safe_coalesce() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, name STRING, category_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE categories (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO items VALUES (1, 'Widget', 10), (2, 'Gadget', NULL), (3, 'Gizmo', 20)",
        )
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO categories VALUES (10, 'Electronics'), (20, 'Tools')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT i.name, COALESCE(c.name, 'Uncategorized') AS category FROM items i LEFT JOIN categories c ON i.category_id = c.id ORDER BY i.id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Widget", "Electronics"],
            ["Gadget", "Uncategorized"],
            ["Gizmo", "Tools"]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_sum_ignores_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10), (NULL), (20), (NULL), (30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT SUM(value) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[60]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_avg_ignores_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10), (NULL), (20), (NULL), (30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT AVG(value) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[20.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_max_ignores_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10), (NULL), (20), (NULL), (30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT MAX(value) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_min_ignores_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10), (NULL), (20), (NULL), (30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT MIN(value) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_all_null_returns_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL), (NULL), (NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT SUM(value), AVG(value), MAX(value), MIN(value) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[null, null, null, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_in_case_when() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 100), (2, NULL), (3, 50)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, CASE WHEN value IS NULL THEN 'missing' ELSE 'present' END AS status FROM t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "present"], [2, "missing"], [3, "present"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_in_case_value() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CASE NULL WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END")
        .await
        .unwrap();
    assert_table_eq!(result, [["other"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_in_in_list() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULL IN (1, 2, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_value_in_list_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 2 IN (1, NULL, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_value_in_list_found_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 2 IN (1, NULL, 2)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_not_in_list() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULL NOT IN (1, 2, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_between() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULL BETWEEN 1 AND 10")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_between_with_null_bound() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 5 BETWEEN NULL AND 10")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_concat_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CONCAT('hello', NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1), (NULL), (2), (NULL), (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT value FROM t ORDER BY value NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_union_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t1 (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE t2 (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t1 VALUES (1), (NULL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t2 VALUES (NULL), (2)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT value FROM t1 UNION DISTINCT SELECT value FROM t2 ORDER BY value NULLS LAST",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_union_all_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t1 (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE t2 (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t1 VALUES (1), (NULL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t2 VALUES (NULL), (2)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT value FROM t1 UNION ALL SELECT value FROM t2 ORDER BY value NULLS LAST",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [null], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_nulls_first() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 30), (2, NULL), (3, 10), (4, NULL), (5, 20)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t ORDER BY value NULLS FIRST, id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [4], [3], [5], [1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_nulls_last() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 30), (2, NULL), (3, 10), (4, NULL), (5, 20)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t ORDER BY value NULLS LAST, id")
        .await
        .unwrap();
    assert_table_eq!(result, [[3], [5], [1], [2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_and_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULL AND TRUE, NULL AND FALSE, TRUE AND NULL, FALSE AND NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[null, false, null, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_or_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULL OR TRUE, NULL OR FALSE, TRUE OR NULL, FALSE OR NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, null, true, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT NOT NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_if_null_condition() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT IF(NULL, 'yes', 'no')")
        .await
        .unwrap();
    assert_table_eq!(result, [["no"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_agg_ignores_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('a'), (NULL), ('b'), (NULL), ('c')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT STRING_AGG(value, ',') FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [["a,b,c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_includes_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1), (NULL), (2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_AGG(value ORDER BY value NULLS LAST) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[[1, 2, null]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_ignore_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1), (NULL), (2), (NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_AGG(value IGNORE NULLS ORDER BY value) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[[1, 2]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_large_dataset_null_pattern() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE large_t (id INT64, value INT64)")
        .await
        .unwrap();

    let mut values = Vec::new();
    for i in 1..=100 {
        if i % 3 == 0 {
            values.push(format!("({}, NULL)", i));
        } else {
            values.push(format!("({}, {})", i, i * 10));
        }
    }
    let insert_sql = format!("INSERT INTO large_t VALUES {}", values.join(", "));
    session.execute_sql(&insert_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM large_t WHERE value IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[33]]);

    let result = session
        .execute_sql("SELECT COUNT(*) FROM large_t WHERE value IS NOT NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[67]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_delete_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE del_t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO del_t VALUES (1, 10), (2, NULL), (3, 30), (4, NULL), (5, 50)")
        .await
        .unwrap();

    session
        .execute_sql("DELETE FROM del_t WHERE value IS NULL")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, value FROM del_t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [3, 30], [5, 50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_delete_middle_rows() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE del_mid_t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO del_mid_t VALUES (1, NULL), (2, 20), (3, NULL), (4, 40), (5, NULL)",
        )
        .await
        .unwrap();

    session
        .execute_sql("DELETE FROM del_mid_t WHERE id = 2 OR id = 4")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM del_mid_t WHERE value IS NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_update_null_to_value() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE upd_t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO upd_t VALUES (1, NULL), (2, 20), (3, NULL)")
        .await
        .unwrap();

    session
        .execute_sql("UPDATE upd_t SET value = 999 WHERE value IS NULL")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, value FROM upd_t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 999], [2, 20], [3, 999]]);

    let result = session
        .execute_sql("SELECT COUNT(*) FROM upd_t WHERE value IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_update_value_to_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE upd2_t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO upd2_t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    session
        .execute_sql("UPDATE upd2_t SET value = NULL WHERE id = 2")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM upd2_t WHERE value IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_truncate_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE trunc_t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO trunc_t VALUES (1, NULL), (2, 20), (3, NULL)")
        .await
        .unwrap();

    session.execute_sql("TRUNCATE TABLE trunc_t").await.unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM trunc_t")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_insert_after_delete() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ins_del_t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ins_del_t VALUES (1, NULL), (2, 20)")
        .await
        .unwrap();

    session
        .execute_sql("DELETE FROM ins_del_t WHERE id = 1")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO ins_del_t VALUES (3, NULL), (4, 40)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM ins_del_t WHERE value IS NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_word_boundary_65_rows() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE wb_t (id INT64, value INT64)")
        .await
        .unwrap();

    let mut values = Vec::new();
    for i in 1..=65 {
        if i == 64 || i == 65 {
            values.push(format!("({}, NULL)", i));
        } else {
            values.push(format!("({}, {})", i, i));
        }
    }
    let insert_sql = format!("INSERT INTO wb_t VALUES {}", values.join(", "));
    session.execute_sql(&insert_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT id FROM wb_t WHERE value IS NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[64], [65]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_word_boundary_128_rows() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE wb128_t (id INT64, value INT64)")
        .await
        .unwrap();

    let mut values = Vec::new();
    for i in 1..=128 {
        if i == 63 || i == 64 || i == 65 || i == 127 || i == 128 {
            values.push(format!("({}, NULL)", i));
        } else {
            values.push(format!("({}, {})", i, i));
        }
    }
    let insert_sql = format!("INSERT INTO wb128_t VALUES {}", values.join(", "));
    session.execute_sql(&insert_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM wb128_t WHERE value IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_delete_across_word_boundary() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE del_wb_t (id INT64, value INT64)")
        .await
        .unwrap();

    let mut values = Vec::new();
    for i in 1..=70 {
        if i == 63 || i == 64 {
            values.push(format!("({}, NULL)", i));
        } else {
            values.push(format!("({}, {})", i, i));
        }
    }
    let insert_sql = format!("INSERT INTO del_wb_t VALUES {}", values.join(", "));
    session.execute_sql(&insert_sql).await.unwrap();

    session
        .execute_sql("DELETE FROM del_wb_t WHERE id <= 62")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM del_wb_t WHERE value IS NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[63], [64]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_multiple_null_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE multi_null_t (id INT64, a INT64, b STRING, c FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO multi_null_t VALUES
             (1, NULL, 'x', 1.0),
             (2, 2, NULL, 2.0),
             (3, 3, 'y', NULL),
             (4, NULL, NULL, NULL),
             (5, 5, 'z', 5.0)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM multi_null_t WHERE a IS NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [4]]);

    let result = session
        .execute_sql("SELECT id FROM multi_null_t WHERE b IS NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [4]]);

    let result = session
        .execute_sql("SELECT id FROM multi_null_t WHERE c IS NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[3], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_all_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE all_null_t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO all_null_t VALUES (1, NULL), (2, NULL), (3, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM all_null_t WHERE value IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);

    let result = session
        .execute_sql("SELECT COUNT(*) FROM all_null_t WHERE value IS NOT NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_no_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE no_null_t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO no_null_t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM no_null_t WHERE value IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);

    let result = session
        .execute_sql("SELECT COUNT(*) FROM no_null_t WHERE value IS NOT NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_alternating_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE alt_null_t (id INT64, value INT64)")
        .await
        .unwrap();

    let mut values = Vec::new();
    for i in 1..=20 {
        if i % 2 == 0 {
            values.push(format!("({}, NULL)", i));
        } else {
            values.push(format!("({}, {})", i, i * 10));
        }
    }
    let insert_sql = format!("INSERT INTO alt_null_t VALUES {}", values.join(", "));
    session.execute_sql(&insert_sql).await.unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM alt_null_t WHERE value IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[10]]);

    let result = session
        .execute_sql("SELECT SUM(value) FROM alt_null_t")
        .await
        .unwrap();
    assert_table_eq!(result, [[1000.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_insert_select_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE src_t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE dst_t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO src_t VALUES (1, NULL), (2, 20), (3, NULL)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO dst_t SELECT * FROM src_t")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM dst_t WHERE value IS NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_union_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE union_t1 (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE union_t2 (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO union_t1 VALUES (1, NULL), (2, 20)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO union_t2 VALUES (3, 30), (4, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id FROM (
                SELECT * FROM union_t1
                UNION ALL
                SELECT * FROM union_t2
            ) WHERE value IS NULL ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_subquery_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE outer_t (id INT64, ref_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE inner_t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO outer_t VALUES (1, 10), (2, 20), (3, NULL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO inner_t VALUES (10, 100), (20, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT o.id FROM outer_t o
             WHERE o.ref_id IN (SELECT id FROM inner_t WHERE value IS NOT NULL)
             ORDER BY o.id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_window_function_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE win_t (id INT64, grp STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO win_t VALUES
             (1, 'A', 10),
             (2, 'A', NULL),
             (3, 'A', 30),
             (4, 'B', NULL),
             (5, 'B', 50)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, SUM(value) OVER (PARTITION BY grp ORDER BY id) as running_sum
             FROM win_t ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[1, 10.0], [2, 10.0], [3, 40.0], [4, 0.0], [5, 50.0]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_cte_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE cte_t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO cte_t VALUES (1, NULL), (2, 20), (3, NULL), (4, 40)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH null_rows AS (
                SELECT id FROM cte_t WHERE value IS NULL
            )
            SELECT COUNT(*) FROM null_rows",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_merge_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE target_t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE source_t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO target_t VALUES (1, NULL), (2, 20)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO source_t VALUES (1, 100), (3, NULL)")
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO target_t t
             USING source_t s ON t.id = s.id
             WHEN MATCHED THEN UPDATE SET value = s.value
             WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, value FROM target_t ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 100], [2, 20], [3, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_first_value_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE fv_t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO fv_t VALUES (1, NULL), (2, 20), (3, NULL), (4, 40)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, FIRST_VALUE(value) OVER (ORDER BY id) as first_val
             FROM fv_t ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, null], [2, null], [3, null], [4, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_last_value_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE lv_t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO lv_t VALUES (1, 10), (2, NULL), (3, 30), (4, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, LAST_VALUE(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as last_val
             FROM lv_t ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [2, null], [3, 30], [4, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitmap_count_distinct_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE cd_t (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO cd_t VALUES (1), (NULL), (2), (NULL), (1), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(DISTINCT value) FROM cd_t")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}
