use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_basic_int_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT * FROM UNNEST([1, 2, 3, 4, 5]) AS n ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3], [4], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_basic_string_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT * FROM UNNEST(['hello', 'world', 'test']) AS s ORDER BY s")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"], ["test"], ["world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_basic_float_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT * FROM UNNEST([1.1, 2.2, 3.3]) AS f ORDER BY f")
        .await
        .unwrap();
    assert_table_eq!(result, [[1.1], [2.2], [3.3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_basic_bool_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT * FROM UNNEST([TRUE, FALSE, TRUE]) AS b ORDER BY b DESC")
        .await
        .unwrap();
    assert_table_eq!(result, [[true], [true], [false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_with_offset_int_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT elem, off FROM UNNEST([10, 20, 30, 40]) AS elem WITH OFFSET AS off ORDER BY off")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 0], [20, 1], [30, 2], [40, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_with_offset_string_array() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT val, idx FROM UNNEST(['a', 'b', 'c']) AS val WITH OFFSET AS idx ORDER BY idx",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["a", 0], ["b", 1], ["c", 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_with_offset_default_alias() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT * FROM UNNEST([100, 200, 300]) WITH OFFSET ORDER BY offset")
        .await
        .unwrap();
    assert_table_eq!(result, [[100, 0], [200, 1], [300, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_with_offset_filter_by_offset() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT elem, off FROM UNNEST([10, 20, 30, 40, 50]) AS elem WITH OFFSET AS off WHERE off > 1 ORDER BY off")
        .await
        .unwrap();
    assert_table_eq!(result, [[30, 2], [40, 3], [50, 4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_with_offset_order_by_elem() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT elem, off FROM UNNEST([30, 10, 20]) AS elem WITH OFFSET AS off ORDER BY elem",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 1], [20, 2], [30, 0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_struct_array_two_fields() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT a, b FROM UNNEST([STRUCT(1 AS a, 'x' AS b), STRUCT(2 AS a, 'y' AS b), STRUCT(3 AS a, 'z' AS b)]) ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "x"], [2, "y"], [3, "z"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_struct_array_three_fields() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT id, name, score FROM UNNEST([STRUCT(1 AS id, 'Alice' AS name, 95 AS score), STRUCT(2 AS id, 'Bob' AS name, 88 AS score)]) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice", 95], [2, "Bob", 88]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_struct_array_with_offset() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT x, y, pos FROM UNNEST([STRUCT(10 AS x, 20 AS y), STRUCT(30 AS x, 40 AS y)]) WITH OFFSET AS pos ORDER BY pos")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 20, 0], [30, 40, 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_struct_array_mixed_types() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT id, name, active FROM UNNEST([STRUCT(1 AS id, 'A' AS name, TRUE AS active), STRUCT(2 AS id, 'B' AS name, FALSE AS active)]) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "A", true], [2, "B", false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_struct_array_alias_access() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT s.key, s.val FROM UNNEST([STRUCT('k1' AS key, 100 AS val), STRUCT('k2' AS key, 200 AS val)]) AS s ORDER BY s.val")
        .await
        .unwrap();
    assert_table_eq!(result, [["k1", 100], ["k2", 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_empty_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COUNT(*) FROM UNNEST(CAST([] AS ARRAY<INT64>)) AS n")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_null_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COUNT(*) FROM UNNEST(CAST(NULL AS ARRAY<INT64>)) AS n")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_array_with_nulls() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT elem FROM UNNEST([1, NULL, 3, NULL, 5]) AS elem ORDER BY elem NULLS FIRST",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[null], [null], [1], [3], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_multiple_arrays_cross_join() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT a, b FROM UNNEST([1, 2]) AS a, UNNEST(['x', 'y']) AS b ORDER BY a, b")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "x"], [1, "y"], [2, "x"], [2, "y"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_three_arrays_cross_join() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT a, b, c FROM UNNEST([1, 2]) AS a, UNNEST(['x']) AS b, UNNEST([true, false]) AS c ORDER BY a, c DESC")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, "x", true],
            [1, "x", false],
            [2, "x", true],
            [2, "x", false]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_multiple_with_offsets() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT a, off1, b, off2 FROM UNNEST([10, 20]) AS a WITH OFFSET AS off1, UNNEST(['p', 'q']) AS b WITH OFFSET AS off2 ORDER BY off1, off2")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [10, 0, "p", 0],
            [10, 0, "q", 1],
            [20, 1, "p", 0],
            [20, 1, "q", 1]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_correlated_from_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE arr_data (id INT64, arr ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO arr_data VALUES (1, [10, 20, 30]), (2, [40, 50])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT d.id, elem FROM arr_data d, UNNEST(d.arr) AS elem ORDER BY d.id, elem")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [1, 20], [1, 30], [2, 40], [2, 50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_correlated_with_offset() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE names_arr (id INT64, names ARRAY<STRING>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO names_arr VALUES (1, ['Alice', 'Bob']), (2, ['Charlie'])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT n.id, name, pos FROM names_arr n, UNNEST(n.names) AS name WITH OFFSET AS pos ORDER BY n.id, pos")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice", 0], [1, "Bob", 1], [2, "Charlie", 0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_correlated_multiple_arrays() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE multi_arr (id INT64, nums ARRAY<INT64>, strs ARRAY<STRING>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO multi_arr VALUES (1, [10, 20], ['a', 'b'])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT m.id, n, s FROM multi_arr m, UNNEST(m.nums) AS n, UNNEST(m.strs) AS s ORDER BY n, s")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[1, 10, "a"], [1, 10, "b"], [1, 20, "a"], [1, 20, "b"]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_cross_join_explicit() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE base (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO base VALUES (1, 'A'), (2, 'B')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT b.id, b.name, n FROM base b CROSS JOIN UNNEST([100, 200]) AS n ORDER BY b.id, n")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[1, "A", 100], [1, "A", 200], [2, "B", 100], [2, "B", 200]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_with_aggregation() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUM(n) AS total FROM UNNEST([1, 2, 3, 4, 5]) AS n")
        .await
        .unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_with_count() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COUNT(*) AS cnt FROM UNNEST([10, 20, 30, 40]) AS n")
        .await
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_with_avg() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT AVG(n) AS avg_val FROM UNNEST([10, 20, 30]) AS n")
        .await
        .unwrap();
    assert_table_eq!(result, [[20.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_with_group_by() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT val, COUNT(*) AS cnt FROM UNNEST([1, 1, 2, 2, 2, 3]) AS val GROUP BY val ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 2], [2, 3], [3, 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_in_subquery() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT * FROM (SELECT n, n * 2 AS doubled FROM UNNEST([1, 2, 3]) AS n) ORDER BY n",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 2], [2, 4], [3, 6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_in_cte() {
    let session = create_session();
    let result = session
        .execute_sql("WITH expanded AS (SELECT n FROM UNNEST([5, 10, 15]) AS n) SELECT n, n + 1 AS plus_one FROM expanded ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, 6], [10, 11], [15, 16]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_generate_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT n FROM UNNEST(GENERATE_ARRAY(1, 5)) AS n ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3], [4], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_generate_array_with_step() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT n FROM UNNEST(GENERATE_ARRAY(0, 10, 2)) AS n ORDER BY CAST(n AS INT64)",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[0], [2], [4], [6], [8], [10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_with_expression() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT n, n * n AS squared FROM UNNEST([1, 2, 3, 4]) AS n ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 1], [2, 4], [3, 9], [4, 16]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_with_case() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT n, CASE WHEN n > 2 THEN 'high' ELSE 'low' END AS level FROM UNNEST([1, 2, 3, 4]) AS n ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "low"], [2, "low"], [3, "high"], [4, "high"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_join_with_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE lookup (val INT64, desc STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO lookup VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT n, l.desc FROM UNNEST([1, 2, 3]) AS n INNER JOIN lookup l ON n = l.val ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "one"], [2, "two"], [3, "three"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_left_join_with_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE values_lookup (val INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO values_lookup VALUES (1, 'first'), (3, 'third')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT n, l.name FROM UNNEST([1, 2, 3]) AS n LEFT JOIN values_lookup l ON n = l.val ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "first"], [2, null], [3, "third"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_distinct() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DISTINCT n FROM UNNEST([1, 2, 1, 3, 2, 1, 3]) AS n ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_limit() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT n FROM UNNEST([10, 20, 30, 40, 50]) AS n LIMIT 3")
        .await
        .unwrap();
    assert_table_eq!(result, [[10], [20], [30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_offset_in_limit() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT n FROM UNNEST([10, 20, 30, 40, 50]) AS n LIMIT 2 OFFSET 2")
        .await
        .unwrap();
    assert_table_eq!(result, [[30], [40]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_nested_array_flatten() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT arr FROM UNNEST([[1, 2], [3, 4], [5, 6]]) AS arr ORDER BY arr[OFFSET(0)]",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[[1, 2]], [[3, 4]], [[5, 6]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_single_element() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT n FROM UNNEST([42]) AS n")
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_large_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS n")
        .await
        .unwrap();
    assert_table_eq!(result, [[1000]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_with_string_agg() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRING_AGG(s, '-') AS joined FROM UNNEST(['a', 'b', 'c']) AS s")
        .await
        .unwrap();
    assert_table_eq!(result, [["a-b-c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_with_array_agg() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_AGG(n ORDER BY n DESC) AS reversed FROM UNNEST([1, 2, 3]) AS n")
        .await
        .unwrap();
    assert_table_eq!(result, [[[3, 2, 1]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_in_exists() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT i.id FROM items i WHERE EXISTS (SELECT 1 FROM UNNEST([1, 3, 5]) AS n WHERE n = i.id) ORDER BY i.id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_in_not_exists() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nums (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nums VALUES (1), (2), (3), (4)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT n.id FROM nums n WHERE NOT EXISTS (SELECT 1 FROM UNNEST([2, 4]) AS x WHERE x = n.id) ORDER BY n.id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_in_scalar_subquery() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE with_arr (id INT64, arr ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO with_arr VALUES (1, [10, 20, 30]), (2, [40, 50])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT w.id, (SELECT STRING_AGG(CAST(elem AS STRING), '-') FROM UNNEST(w.arr) AS elem) AS elems FROM with_arr w ORDER BY w.id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "10-20-30"], [2, "40-50"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_struct_with_null_field() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT a, b FROM UNNEST([STRUCT(1 AS a, 'x' AS b), STRUCT(2 AS a, CAST(NULL AS STRING) AS b)]) ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "x"], [2, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_date_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(d AS STRING) AS date_str FROM UNNEST([DATE '2024-01-01', DATE '2024-06-15', DATE '2024-12-31']) AS d ORDER BY d")
        .await
        .unwrap();
    assert_table_eq!(result, [["2024-01-01"], ["2024-06-15"], ["2024-12-31"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_timestamp_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(ts AS STRING) FROM UNNEST([TIMESTAMP '2024-01-01 10:00:00', TIMESTAMP '2024-06-15 12:30:00']) AS ts ORDER BY ts")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["2024-01-01 10:00:00.000000 UTC"],
            ["2024-06-15 12:30:00.000000 UTC"]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_where_filter() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT n FROM UNNEST([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) AS n WHERE n % 2 = 0 ORDER BY n",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [4], [6], [8], [10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_multiple_where_conditions() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT n FROM UNNEST([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) AS n WHERE n > 3 AND n < 8 ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[4], [5], [6], [7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_or_filter() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT n FROM UNNEST([1, 2, 3, 4, 5]) AS n WHERE n = 1 OR n = 5 ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_having_clause() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT category, SUM(val) AS total FROM UNNEST([STRUCT('A' AS category, 10 AS val), STRUCT('A' AS category, 20 AS val), STRUCT('B' AS category, 5 AS val)]) GROUP BY category HAVING SUM(val) > 10 ORDER BY category")
        .await
        .unwrap();
    assert_table_eq!(result, [["A", 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_union_all() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT n FROM UNNEST([1, 2]) AS n UNION ALL SELECT n FROM UNNEST([3, 4]) AS n ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_intersect() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT n FROM UNNEST([1, 2, 3, 4]) AS n INTERSECT DISTINCT SELECT n FROM UNNEST([2, 3, 5]) AS n ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_except() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT n FROM UNNEST([1, 2, 3, 4]) AS n EXCEPT DISTINCT SELECT n FROM UNNEST([2, 4]) AS n ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_window_function() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT n, SUM(n) OVER (ORDER BY n) AS running_sum FROM UNNEST([1, 2, 3, 4]) AS n ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 1], [2, 3], [3, 6], [4, 10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_row_number() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT n, ROW_NUMBER() OVER (ORDER BY n) AS rn FROM UNNEST([30, 10, 20]) AS n ORDER BY rn")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 1], [20, 2], [30, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_rank() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT n, RANK() OVER (ORDER BY n) AS rnk FROM UNNEST([1, 2, 2, 3]) AS n ORDER BY n, rnk")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 1], [2, 2], [2, 2], [3, 4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_bytes_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT b FROM UNNEST([b'hello', b'world']) AS b ORDER BY b")
        .await
        .unwrap();
    assert_table_eq!(result, [[b"hello"], [b"world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_in_tvf() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE FUNCTION expand_array(arr ARRAY<INT64>) RETURNS TABLE<elem INT64> AS (SELECT elem FROM UNNEST(arr) AS elem)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM expand_array([10, 20, 30]) ORDER BY elem")
        .await
        .unwrap();
    assert_table_eq!(result, [[10], [20], [30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_with_coalesce() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT COALESCE(n, -1) AS val FROM UNNEST([1, NULL, 3, NULL, 5]) AS n ORDER BY val",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[-1], [-1], [1], [3], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_with_if() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT n, IF(n > 2, 'big', 'small') AS size FROM UNNEST([1, 2, 3, 4]) AS n ORDER BY n",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "small"], [2, "small"], [3, "big"], [4, "big"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_with_nullif() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULLIF(n, 3) AS val FROM UNNEST([1, 2, 3, 4]) AS n ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [null], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_default_element_name() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT element FROM UNNEST([100, 200, 300]) ORDER BY element")
        .await
        .unwrap();
    assert_table_eq!(result, [[100], [200], [300]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_in_in_clause() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE products (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO products VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name FROM products WHERE id IN UNNEST([1, 3]) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "A"], [3, "C"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_not_in() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items_notIn (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items_notIn VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM items_notIn WHERE id NOT IN UNNEST([2, 4]) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_safe_offset_access() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT arr[SAFE_OFFSET(0)] AS first, arr[SAFE_OFFSET(5)] AS missing FROM UNNEST([[1, 2, 3]]) AS arr")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_struct_nested_field() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT person.name, person.age FROM UNNEST([STRUCT(STRUCT('Alice' AS name, 30 AS age) AS person), STRUCT(STRUCT('Bob' AS name, 25 AS age) AS person)]) ORDER BY person.age")
        .await
        .unwrap();
    assert_table_eq!(result, [["Bob", 25], ["Alice", 30]]);
}
