use crate::assert_table_eq;
use crate::common::{create_session, n};

#[tokio::test(flavor = "current_thread")]
async fn test_int64_column_addition() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, 5), (20, 15), (-5, 10)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a + b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[5], [15], [35]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_column_subtraction() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, 5), (20, 15), (5, 10)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a - b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[-5], [5], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_column_multiplication() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (3, 4), (5, 6), (-2, 7)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a * b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[-14], [12], [30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_column_division() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (20, 4), (15, 3), (100, 10)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT CAST(a / b AS INT64) FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[5], [5], [10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_float64_column_arithmetic() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a FLOAT64, b FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10.5, 2.5), (3.0, 1.5), (8.0, 2.0)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a + b, a - b, a * b, a / b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [4.5, 1.5, 4.5, 2.0],
            [10.0, 6.0, 16.0, 4.0],
            [13.0, 8.0, 26.25, 4.2]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_column_arithmetic() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a NUMERIC, b NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10.25, 2.50), (5.00, 2.50)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a + b, a - b, a * b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [n("7.5"), n("2.5"), n("12.5")],
            [n("12.75"), n("7.75"), n("25.625")]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_mixed_numeric_types_arithmetic() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (i INT64, f FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, 2.5), (5, 1.5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT i + f, i * f FROM t ORDER BY i")
        .await
        .unwrap();
    assert_table_eq!(result, [[6.5, 7.5], [12.5, 25.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_column_equality() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, 10), (20, 15), (5, 5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a = b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[true], [true], [false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_column_not_equal() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, 10), (20, 15), (5, 5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a != b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[false], [false], [true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_column_less_than() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (5, 10), (10, 10), (15, 10)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a < b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[true], [false], [false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_column_less_than_or_equal() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (5, 10), (10, 10), (15, 10)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a <= b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[true], [true], [false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_column_greater_than() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (5, 10), (10, 10), (15, 10)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a > b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[false], [false], [true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_column_greater_than_or_equal() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (5, 10), (10, 10), (15, 10)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a >= b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[false], [true], [true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_float64_column_comparison() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a FLOAT64, b FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1.5, 2.5), (2.5, 2.5), (3.5, 2.5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a < b, a = b, a > b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [true, false, false],
            [false, true, false],
            [false, false, true]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_column_comparison() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a STRING, b STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('apple', 'banana'), ('cat', 'cat'), ('dog', 'cat')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a < b, a = b, a > b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [true, false, false],
            [false, true, false],
            [false, false, true]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_column_and() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a BOOL, b BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (true, true), (true, false), (false, false)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a AND b FROM t ORDER BY a DESC, b DESC")
        .await
        .unwrap();
    assert_table_eq!(result, [[true], [false], [false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_column_or() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a BOOL, b BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (true, true), (true, false), (false, false)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a OR b FROM t ORDER BY a DESC, b DESC")
        .await
        .unwrap();
    assert_table_eq!(result, [[true], [true], [false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_column_not() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (true), (false)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT NOT a FROM t ORDER BY a DESC")
        .await
        .unwrap();
    assert_table_eq!(result, [[false], [true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_compound_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a BOOL, b BOOL, c BOOL)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO t VALUES (true, true, false), (true, false, true), (false, true, true)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT (a AND b) OR c FROM t ORDER BY a DESC, b DESC")
        .await
        .unwrap();
    assert_table_eq!(result, [[true], [true], [true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_concat_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (first_name STRING, last_name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('John', 'Doe'), ('Jane', 'Smith')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT CONCAT(first_name, ' ', last_name) FROM t ORDER BY first_name")
        .await
        .unwrap();
    assert_table_eq!(result, [["Jane Smith"], ["John Doe"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_length_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('hello'), ('world!'), ('')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT LENGTH(val) FROM t ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[0], [5], [6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_concat_operator_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a STRING, b STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('foo', 'bar'), ('hello', 'world')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a || b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [["foobar"], ["helloworld"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_arithmetic_propagation() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, NULL), (NULL, 5), (NULL, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a + b, a - b, a * b FROM t")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[null, null, null], [null, null, null], [null, null, null]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_comparison() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, NULL), (NULL, 5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a = b, a < b, a > b FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[null, null, null], [null, null, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_boolean_and_propagation() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a BOOL, b BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (true, NULL), (false, NULL), (NULL, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a AND b FROM t ORDER BY a DESC NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(result, [[null], [false], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_boolean_or_propagation() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a BOOL, b BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (true, NULL), (false, NULL), (NULL, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a OR b FROM t ORDER BY a DESC NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(result, [[true], [null], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_string_concat() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a STRING, b STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('hello', NULL), (NULL, 'world')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT CONCAT(a, b) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[null], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_to_float64_coercion() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (i INT64, f FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, 3.0), (7, 2.0)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT i / f FROM t ORDER BY i")
        .await
        .unwrap();
    assert_table_eq!(result, [[3.5], [3.3333333333333335]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_to_numeric_coercion() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (i INT64, n NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, 2.5), (5, 1.25)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT CAST(i AS NUMERIC) + n FROM t ORDER BY i")
        .await
        .unwrap();
    assert_table_eq!(result, [[n("6.25")], [n("12.5")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_comparison_type_coercion() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (i INT64, f FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, 10.0), (5, 5.5), (7, 6.9)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT i = f, i < f, i > f FROM t ORDER BY i")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [false, true, false],
            [false, false, true],
            [true, false, false]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_when_column_simple() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT CASE WHEN val = 1 THEN 'one' WHEN val = 2 THEN 'two' ELSE 'other' END FROM t ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [["one"], ["two"], ["other"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_when_column_comparison() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, 5), (5, 10), (7, 7)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT CASE WHEN a > b THEN 'greater' WHEN a < b THEN 'less' ELSE 'equal' END FROM t ORDER BY a",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["less"], ["equal"], ["greater"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_when_column_arithmetic() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10), (20), (30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT CASE WHEN val > 15 THEN val * 2 ELSE val END FROM t ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[10], [40], [60]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_when_with_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1), (NULL), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT CASE WHEN val IS NULL THEN 'null' ELSE 'not null' END FROM t ORDER BY val NULLS FIRST",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["null"], ["not null"], ["not null"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_by_computed_addition() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, 5), (20, 15), (5, 2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a, b FROM t WHERE a + b > 20 ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[20, 15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_by_computed_multiplication() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (price INT64, quantity INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, 5), (20, 3), (5, 10)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT price, quantity FROM t WHERE price * quantity >= 50 ORDER BY price")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, 10], [10, 5], [20, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_by_computed_string_length() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('a'), ('hello'), ('world!')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM t WHERE LENGTH(name) > 3 ORDER BY name")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"], ["world!"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_by_boolean_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64, active BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, 5, true), (20, 15, false), (5, 2, true)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a FROM t WHERE active AND a > 5 ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_by_comparison_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, 5), (5, 10), (7, 7)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a, b FROM t WHERE a >= b ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[7, 7], [10, 5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_by_case_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT val FROM t WHERE CASE WHEN val % 2 = 0 THEN true ELSE false END ORDER BY val",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_combined_arithmetic_and_comparison() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (x INT64, y INT64, z INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 2, 3), (4, 5, 6), (7, 8, 9)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT x + y AS sum, (x + y) > z AS exceeds FROM t ORDER BY x")
        .await
        .unwrap();
    assert_table_eq!(result, [[3, false], [9, true], [15, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_case_expressions() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (score INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (95), (75), (55), (35)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT CASE
                WHEN score >= 90 THEN 'A'
                WHEN score >= 70 THEN 'B'
                WHEN score >= 50 THEN 'C'
                ELSE 'F'
            END AS grade FROM t ORDER BY score",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["F"], ["C"], ["B"], ["A"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_null_in_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1), (NULL), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val IS NULL, val IS NOT NULL FROM t ORDER BY val NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, false], [false, true], [false, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_with_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 10), (NULL, 20), (NULL, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COALESCE(a, b, 0) FROM t ORDER BY b NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [20], [0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullif_with_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, 10), (20, 10), (30, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT NULLIF(a, b) FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[null], [20], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ifnull_with_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 100), (NULL, 200)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT IFNULL(a, b) FROM t ORDER BY b")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_complex_boolean_with_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a BOOL, b BOOL, c BOOL)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO t VALUES (true, false, NULL), (NULL, true, false), (false, NULL, true)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT (a OR b) AND NOT COALESCE(c, false) FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[true], [true], [false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_modulo_column_operation() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, 3), (15, 4), (20, 6)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a % b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unary_minus_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10), (-5), (0)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT -val FROM t ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[5], [0], [-10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_date_comparison() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (d1 DATE, d2 DATE)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO t VALUES ('2024-01-01', '2024-01-15'), ('2024-06-01', '2024-03-01')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT d1 < d2, d1 = d2, d1 > d2 FROM t ORDER BY d1")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, false, false], [false, false, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_timestamp_comparison() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (ts1 TIMESTAMP, ts2 TIMESTAMP)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO t VALUES ('2024-01-01 10:00:00', '2024-01-01 12:00:00'), ('2024-01-01 15:00:00', '2024-01-01 15:00:00')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ts1 < ts2, ts1 = ts2 FROM t ORDER BY ts1")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, false], [false, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_combined_filter_with_or() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, 'foo'), (20, 'bar'), (5, 'baz')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a, b FROM t WHERE a > 15 OR b = 'baz' ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, "baz"], [20, "bar"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_combined_filter_with_and() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, 'foo'), (20, 'bar'), (5, 'foo')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a, b FROM t WHERE a > 5 AND b = 'foo' ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, "foo"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_expression_in_order_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (2, 10), (5, 2), (3, 5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a, b FROM t ORDER BY a * b")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, 2], [3, 5], [2, 10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_expression_in_group_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1), (2), (3), (4), (5), (6)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val % 2 AS remainder, COUNT(*) AS cnt FROM t GROUP BY val % 2 ORDER BY remainder")
        .await
        .unwrap();
    assert_table_eq!(result, [[0, 3], [1, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_abs_on_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (-10), (5), (-3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ABS(val) FROM t ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[10], [3], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_round_on_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1.234), (5.678), (9.999)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ROUND(val, 1) FROM t ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[1.2], [5.7], [10.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_column_equality() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a BYTES, b BYTES)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (b'abc', b'abc'), (b'abc', b'xyz'), (b'123', b'123')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a = b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[true], [true], [false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_column_comparison() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a BYTES, b BYTES)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (b'aaa', b'bbb'), (b'ccc', b'ccc'), (b'ddd', b'aaa')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a < b, a <= b, a > b, a >= b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [true, true, false, false],
            [false, true, false, true],
            [false, false, true, true]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_bytes_comparison_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a BYTES, b BYTES)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (b'abc', NULL), (NULL, b'xyz')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a = b, a < b, a > b FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[null, null, null], [null, null, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_column_equality() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INTERVAL, b INTERVAL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (INTERVAL 1 YEAR, INTERVAL 1 YEAR), (INTERVAL 2 MONTH, INTERVAL 3 MONTH)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a = b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[true], [false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_column_comparison() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INTERVAL, b INTERVAL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (INTERVAL 1 MONTH, INTERVAL 2 MONTH), (INTERVAL 3 MONTH, INTERVAL 1 MONTH)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a < b, a <= b, a > b, a >= b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[true, true, false, false], [false, false, true, true]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_comparison_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INTERVAL, b INTERVAL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (INTERVAL 1 YEAR, NULL), (NULL, INTERVAL 2 MONTH)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a = b, a < b, a > b FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[null, null, null], [null, null, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_time_column_equality() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a TIME, b TIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('10:00:00', '10:00:00'), ('10:00:00', '12:00:00')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a = b FROM t ORDER BY a, b")
        .await
        .unwrap();
    assert_table_eq!(result, [[true], [false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_time_column_comparison() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a TIME, b TIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('08:00:00', '12:00:00'), ('15:00:00', '10:00:00'), ('12:00:00', '12:00:00')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a < b, a <= b, a > b, a >= b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [true, true, false, false],
            [false, true, false, true],
            [false, false, true, true]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_time_comparison_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a TIME, b TIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('10:00:00', NULL), (NULL, '12:00:00')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a = b, a < b, a > b FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[null, null, null], [null, null, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_column_equality() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a DATETIME, b DATETIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('2024-01-01 10:00:00', '2024-01-01 10:00:00'), ('2024-01-01 10:00:00', '2024-01-02 10:00:00')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a = b FROM t ORDER BY a, b")
        .await
        .unwrap();
    assert_table_eq!(result, [[true], [false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_column_comparison() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a DATETIME, b DATETIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('2024-01-01 08:00:00', '2024-01-01 12:00:00'), ('2024-01-01 15:00:00', '2024-01-01 10:00:00'), ('2024-01-01 12:00:00', '2024-01-01 12:00:00')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a < b, a <= b, a > b, a >= b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [true, true, false, false],
            [false, true, false, true],
            [false, false, true, true]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_comparison_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a DATETIME, b DATETIME)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO t VALUES ('2024-01-01 10:00:00', NULL), (NULL, '2024-01-02 10:00:00')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a = b, a < b, a > b FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[null, null, null], [null, null, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_division_non_zero() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a NUMERIC, b NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (20.0, 2.0), (30.0, 3.0)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a / b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[n("10")], [n("10")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_div_function() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, 3), (20, 4)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DIV(a, b) FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[3], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_division_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a FLOAT64, b FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10.0, NULL), (NULL, 2.0), (20.0, 5.0)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a / b FROM t ORDER BY a NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[null], [null], [4.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_column_comparison() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a NUMERIC, b NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1.5, 2.5), (3.5, 3.5), (5.5, 4.5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a < b, a <= b, a > b, a >= b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [true, true, false, false],
            [false, true, false, true],
            [false, false, true, true]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_comparison_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a NUMERIC, b NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10.5, NULL), (NULL, 20.5), (30.5, 30.5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a = b, a < b, a > b FROM t ORDER BY a NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[null, null, null], [null, null, null], [true, false, false]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_negation_float64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (3.14), (-2.71), (0.0)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT -val FROM t ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[2.71], [0.0], [-3.14]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_negation_numeric() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10.5), (-5.25), (0)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT -val FROM t ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[n("5.25")], [n("0")], [n("-10.5")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_negation_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10), (NULL), (-5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT -val FROM t ORDER BY val NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[null], [5], [-10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_not_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (true), (NULL), (false)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT NOT val FROM t ORDER BY val NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[null], [true], [false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bool_column_equality() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a BOOL, b BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (true, true), (true, false), (false, false)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a = b FROM t ORDER BY a DESC, b DESC")
        .await
        .unwrap();
    assert_table_eq!(result, [[true], [false], [true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bool_comparison_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a BOOL, b BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (true, NULL), (NULL, false), (true, true)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a = b FROM t ORDER BY a NULLS FIRST, b NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[null], [null], [true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_date_column_comparison_detailed() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a DATE, b DATE)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('2024-01-01', '2024-06-01'), ('2024-06-01', '2024-06-01'), ('2024-12-01', '2024-06-01')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a < b, a <= b, a > b, a >= b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [true, true, false, false],
            [false, true, false, true],
            [false, false, true, true]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_date_comparison_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a DATE, b DATE)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('2024-01-01', NULL), (NULL, '2024-06-01')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a = b, a < b, a > b FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[null, null, null], [null, null, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_timestamp_column_comparison_detailed() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a TIMESTAMP, b TIMESTAMP)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('2024-01-01 10:00:00', '2024-01-01 15:00:00'), ('2024-01-01 15:00:00', '2024-01-01 15:00:00'), ('2024-01-01 20:00:00', '2024-01-01 15:00:00')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a < b, a <= b, a > b, a >= b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [true, true, false, false],
            [false, true, false, true],
            [false, false, true, true]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_timestamp_comparison_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a TIMESTAMP, b TIMESTAMP)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO t VALUES ('2024-01-01 10:00:00', NULL), (NULL, '2024-01-01 15:00:00')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a = b, a < b, a > b FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, [[null, null, null], [null, null, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_all_false_mask() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM t WHERE val > 100")
        .await
        .unwrap();
    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_all_null_mask() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, NULL), (2, NULL), (3, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a FROM t WHERE a = b")
        .await
        .unwrap();
    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_type_coercion() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, b FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10, 1.5), (NULL, 2.5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COALESCE(CAST(a AS FLOAT64), b) FROM t ORDER BY b")
        .await
        .unwrap();
    assert_table_eq!(result, [[10.0], [2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_all_nulls_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL), (NULL), (NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val + 1 FROM t").await.unwrap();
    assert_table_eq!(result, [[null], [null], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_and_with_all_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a BOOL, b BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL, NULL), (true, NULL), (false, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a AND b FROM t ORDER BY a NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[null], [false], [null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_or_with_all_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a BOOL, b BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL, NULL), (true, NULL), (false, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a OR b FROM t ORDER BY a NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[null], [null], [true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_column_not_equal() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a STRING, b STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('hello', 'hello'), ('foo', 'bar')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a != b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[true], [false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_float64_not_equal() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a FLOAT64, b FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1.5, 1.5), (2.5, 3.5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a != b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[false], [true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_not_equal() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a NUMERIC, b NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10.5, 10.5), (20.5, 30.5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a != b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[false], [true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_complex_and_or_null_propagation() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a BOOL, b BOOL, c BOOL)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO t VALUES (true, false, true), (false, true, false), (true, true, false)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT (a AND b) OR c, a OR (b AND c) FROM t ORDER BY a, b")
        .await
        .unwrap();
    assert_table_eq!(result, [[false, false], [true, true], [true, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiply_numeric_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a NUMERIC, b NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10.0, 2.0), (3.0, 4.0), (5.0, 5.0)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a * b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[n("12")], [n("25")], [n("20")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_subtract_numeric_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a NUMERIC, b NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10.0, 3.0), (8.0, 2.0), (15.0, 5.0)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a - b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[n("6")], [n("7")], [n("10")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_divide_numeric_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a NUMERIC, b NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (20.0, 4.0), (30.0, 6.0), (50.0, 10.0)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a / b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[n("5")], [n("5")], [n("5")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gather_with_null_values() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, 'a'), (2, NULL), (3, 'c'), (4, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM t ORDER BY id DESC")
        .await
        .unwrap();
    assert_table_eq!(result, [[null], ["c"], [null], ["a"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_float64_infinity_operations() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST('inf' AS FLOAT64) + 1, CAST('-inf' AS FLOAT64) - 1, CAST('inf' AS FLOAT64) > 0")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_with_mixed_boolean_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INT64, flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, true), (2, false), (3, NULL), (4, true)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a FROM t WHERE flag ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_broadcast_empty_result() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val, 100 AS const_val FROM t")
        .await
        .unwrap();
    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extend_columns_via_union() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t1 (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE t2 (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t1 VALUES (1), (2)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t2 VALUES (3), (4)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM t1 UNION ALL SELECT val FROM t2 ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_complex_comparison() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t (a INTERVAL, b INTERVAL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (INTERVAL 1 MONTH, INTERVAL 1 MONTH), (INTERVAL 2 MONTH, INTERVAL 1 MONTH)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a <= b, a >= b FROM t ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, true], [false, true]]);
}
