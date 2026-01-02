use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_simple_column_substitution() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION get_multiplied(factor INT64)
            RETURNS TABLE<id INT64, result INT64>
            AS (
                SELECT id, id * factor AS result
                FROM UNNEST([STRUCT(1 AS id), STRUCT(2), STRUCT(3)])
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM get_multiplied(10) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [2, 20], [3, 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_binary_op_substitution() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION add_offset(offset_val INT64)
            RETURNS TABLE<x INT64, y INT64>
            AS (
                SELECT x, x + offset_val AS y
                FROM UNNEST([STRUCT(5 AS x), STRUCT(10)])
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM add_offset(100) ORDER BY x")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, 105], [10, 110]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_filter_substitution() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION filter_greater(threshold INT64)
            RETURNS TABLE<value INT64>
            AS (
                SELECT value
                FROM UNNEST([STRUCT(1 AS value), STRUCT(5), STRUCT(10), STRUCT(15)])
                WHERE value > threshold
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM filter_greater(6) ORDER BY value")
        .await
        .unwrap();
    assert_table_eq!(result, [[10], [15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_values_substitution() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION with_values(val INT64)
            RETURNS TABLE<a INT64, b INT64>
            AS (
                SELECT val AS a, val * 2 AS b
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM with_values(7)")
        .await
        .unwrap();
    assert_table_eq!(result, [[7, 14]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_case_when_substitution() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION categorize(threshold INT64)
            RETURNS TABLE<value INT64, category STRING>
            AS (
                SELECT value,
                       CASE WHEN value > threshold THEN 'high'
                            WHEN value = threshold THEN 'equal'
                            ELSE 'low' END AS category
                FROM UNNEST([STRUCT(5 AS value), STRUCT(10), STRUCT(15)])
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM categorize(10) ORDER BY value")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, "low"], [10, "equal"], [15, "high"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_case_with_operand() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION match_value(target INT64)
            RETURNS TABLE<id INT64, matched STRING>
            AS (
                SELECT id,
                       CASE target
                           WHEN 1 THEN 'one'
                           WHEN 2 THEN 'two'
                           ELSE 'other'
                       END AS matched
                FROM UNNEST([STRUCT(1 AS id), STRUCT(2)])
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM match_value(2) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "two"], [2, "two"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_case_without_else() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION case_no_else(threshold INT64)
            RETURNS TABLE<value INT64, result STRING>
            AS (
                SELECT value,
                       CASE WHEN value > threshold THEN 'above' END AS result
                FROM UNNEST([STRUCT(5 AS value), STRUCT(15)])
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM case_no_else(10) ORDER BY value")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, null], [15, "above"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_case_with_else() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION with_else_param(default_val INT64)
            RETURNS TABLE<id INT64, result INT64>
            AS (
                SELECT id,
                       CASE WHEN id > 100 THEN id ELSE default_val END AS result
                FROM UNNEST([STRUCT(1 AS id), STRUCT(200)])
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM with_else_param(999) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 999], [200, 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_unary_op_substitution() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION negate(val INT64)
            RETURNS TABLE<original INT64, negated INT64>
            AS (
                SELECT val AS original, -val AS negated
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM negate(42)")
        .await
        .unwrap();
    assert_table_eq!(result, [[42, -42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_scalar_function_substitution() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION abs_func(val INT64)
            RETURNS TABLE<result INT64>
            AS (
                SELECT ABS(val) AS result
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM abs_func(-123)")
        .await
        .unwrap();
    assert_table_eq!(result, [[123]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_nested_scalar_functions() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION nested_func(x INT64, y INT64)
            RETURNS TABLE<result FLOAT64>
            AS (
                SELECT SQRT(CAST(x * x + y * y AS FLOAT64)) AS result
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM nested_func(3, 4)")
        .await
        .unwrap();
    assert_table_eq!(result, [[5.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_cast_substitution() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION cast_param(val INT64)
            RETURNS TABLE<as_string STRING>
            AS (
                SELECT CAST(val AS STRING) AS as_string
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM cast_param(42)")
        .await
        .unwrap();
    assert_table_eq!(result, [["42"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_safe_cast_substitution() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION safe_cast_param(val STRING)
            RETURNS TABLE<result INT64>
            AS (
                SELECT SAFE_CAST(val AS INT64) AS result
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM safe_cast_param('123')")
        .await
        .unwrap();
    assert_table_eq!(result, [[123]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_in_list_substitution() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION check_in_list(val INT64)
            RETURNS TABLE<id INT64, is_in_list BOOL>
            AS (
                SELECT id, val IN (1, 2, 3) AS is_in_list
                FROM UNNEST([STRUCT(1 AS id)])
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM check_in_list(2)")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, true]]);

    let result2 = session
        .execute_sql("SELECT * FROM check_in_list(5)")
        .await
        .unwrap();
    assert_table_eq!(result2, [[1, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_in_list_expr_substitution() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION filter_in_range(min_val INT64, max_val INT64)
            RETURNS TABLE<value INT64>
            AS (
                SELECT value
                FROM UNNEST([STRUCT(1 AS value), STRUCT(5), STRUCT(10)])
                WHERE value IN (min_val, max_val)
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM filter_in_range(1, 10) ORDER BY value")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_is_null_substitution() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION check_null(val INT64)
            RETURNS TABLE<is_null BOOL>
            AS (
                SELECT val IS NULL AS is_null
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM check_null(NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);

    let result2 = session
        .execute_sql("SELECT * FROM check_null(1)")
        .await
        .unwrap();
    assert_table_eq!(result2, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_is_not_null_substitution() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION check_not_null(val INT64)
            RETURNS TABLE<is_not_null BOOL>
            AS (
                SELECT val IS NOT NULL AS is_not_null
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM check_not_null(42)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_between_substitution() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION check_between(val INT64, low INT64, high INT64)
            RETURNS TABLE<in_range BOOL>
            AS (
                SELECT val BETWEEN low AND high AS in_range
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM check_between(5, 1, 10)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);

    let result2 = session
        .execute_sql("SELECT * FROM check_between(15, 1, 10)")
        .await
        .unwrap();
    assert_table_eq!(result2, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_between_not_negated() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION check_not_between(val INT64, low INT64, high INT64)
            RETURNS TABLE<out_of_range BOOL>
            AS (
                SELECT val NOT BETWEEN low AND high AS out_of_range
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM check_not_between(15, 1, 10)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_struct_substitution() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION make_struct(x INT64, y INT64)
            RETURNS TABLE<s STRUCT<a INT64, b INT64>>
            AS (
                SELECT STRUCT(x AS a, y AS b) AS s
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT s.a, s.b FROM make_struct(10, 20)")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_array_substitution() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION make_array(a INT64, b INT64, c INT64)
            RETURNS TABLE<arr ARRAY<INT64>>
            AS (
                SELECT [a, b, c] AS arr
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT arr FROM make_array(1, 2, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [[[1, 2, 3]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_alias_substitution() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION with_alias(val INT64)
            RETURNS TABLE<doubled INT64>
            AS (
                SELECT val * 2 AS doubled
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT doubled FROM with_alias(21)")
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_multiple_params() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION multi_param(a INT64, b INT64, c INT64)
            RETURNS TABLE<sum INT64, product INT64>
            AS (
                SELECT a + b + c AS sum, a * b * c AS product
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM multi_param(2, 3, 4)")
        .await
        .unwrap();
    assert_table_eq!(result, [[9, 24]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_nested_expressions() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION nested_expr(x INT64, y INT64)
            RETURNS TABLE<result INT64>
            AS (
                SELECT ((x + y) * (x - y)) + (x * y) AS result
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM nested_expr(5, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [[31]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_string_param() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION greet(name STRING)
            RETURNS TABLE<greeting STRING>
            AS (
                SELECT CONCAT('Hello, ', name, '!') AS greeting
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM greet('World')")
        .await
        .unwrap();
    assert_table_eq!(result, [["Hello, World!"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_case_insensitive_param() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION case_test(MyParam INT64)
            RETURNS TABLE<result INT64>
            AS (
                SELECT myparam * 2 AS result
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM case_test(21)")
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_project_filter_chain() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION project_filter(multiplier INT64, threshold INT64)
            RETURNS TABLE<id INT64, value INT64>
            AS (
                SELECT id, id * multiplier AS value
                FROM UNNEST([STRUCT(1 AS id), STRUCT(2), STRUCT(3), STRUCT(4), STRUCT(5)])
                WHERE id * multiplier > threshold
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM project_filter(10, 25) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[3, 30], [4, 40], [5, 50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_complex_filter() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION complex_filter(min_val INT64, max_val INT64, divisor INT64)
            RETURNS TABLE<value INT64>
            AS (
                SELECT value
                FROM UNNEST([STRUCT(1 AS value), STRUCT(6), STRUCT(12), STRUCT(15), STRUCT(20)])
                WHERE value >= min_val AND value <= max_val AND MOD(value, divisor) = 0
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM complex_filter(5, 18, 3) ORDER BY value")
        .await
        .unwrap();
    assert_table_eq!(result, [[6], [12], [15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_with_coalesce() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION with_default(val INT64, default_val INT64)
            RETURNS TABLE<result INT64>
            AS (
                SELECT COALESCE(val, default_val) AS result
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM with_default(NULL, 99)")
        .await
        .unwrap();
    assert_table_eq!(result, [[99]]);

    let result2 = session
        .execute_sql("SELECT * FROM with_default(42, 99)")
        .await
        .unwrap();
    assert_table_eq!(result2, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_param_in_subexpr() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION param_subexpr(base INT64, power INT64)
            RETURNS TABLE<result FLOAT64>
            AS (
                SELECT POW(CAST(base AS FLOAT64), CAST(power AS FLOAT64)) AS result
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM param_subexpr(2, 10)")
        .await
        .unwrap();
    assert_table_eq!(result, [[1024.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_passthrough_literal() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION with_literal(x INT64)
            RETURNS TABLE<a INT64, b INT64, c INT64>
            AS (
                SELECT x AS a, 100 AS b, x + 100 AS c
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM with_literal(5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, 100, 105]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_boolean_param() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION bool_filter(include_high BOOL)
            RETURNS TABLE<value INT64>
            AS (
                SELECT value
                FROM UNNEST([STRUCT(1 AS value), STRUCT(50), STRUCT(100)])
                WHERE (include_high AND value >= 50) OR (NOT include_high AND value < 50)
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM bool_filter(TRUE) ORDER BY value")
        .await
        .unwrap();
    assert_table_eq!(result, [[50], [100]]);

    let result2 = session
        .execute_sql("SELECT * FROM bool_filter(FALSE) ORDER BY value")
        .await
        .unwrap();
    assert_table_eq!(result2, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_float_param() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION scale_float(factor FLOAT64)
            RETURNS TABLE<result FLOAT64>
            AS (
                SELECT 10.0 * factor AS result
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM scale_float(2.5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[25.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_nested_case() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION nested_case(threshold INT64, low_mult INT64, high_mult INT64)
            RETURNS TABLE<value INT64, result INT64>
            AS (
                SELECT value,
                       CASE WHEN value < threshold
                            THEN value * low_mult
                            ELSE CASE WHEN value = threshold
                                      THEN value
                                      ELSE value * high_mult
                                 END
                       END AS result
                FROM UNNEST([STRUCT(5 AS value), STRUCT(10), STRUCT(15)])
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM nested_case(10, 2, 3) ORDER BY value")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, 10], [10, 10], [15, 45]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_with_join() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION get_user_by_id(user_id INT64)
            RETURNS TABLE<id INT64, name STRING>
            AS (
                SELECT id, name FROM users WHERE id = user_id
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM get_user_by_id(2)")
        .await
        .unwrap();
    assert_table_eq!(result, [[2, "Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_values_multiple_rows() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION gen_range(start_val INT64, end_val INT64)
            RETURNS TABLE<n INT64>
            AS (
                SELECT n FROM UNNEST([start_val, start_val + 1, start_val + 2]) AS n
                WHERE n <= end_val
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM gen_range(10, 12) ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[10], [11], [12]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_deeply_nested_binary_ops() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION deep_calc(a INT64, b INT64, c INT64, d INT64)
            RETURNS TABLE<result INT64>
            AS (
                SELECT (((a + b) * c) - d) + ((a * b) - (c + d)) AS result
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM deep_calc(1, 2, 3, 4)")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_function_array_with_params() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION array_sum(x INT64, y INT64, z INT64)
            RETURNS TABLE<total INT64>
            AS (
                SELECT (SELECT SUM(elem) FROM UNNEST([x, y, z]) AS elem) AS total
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM array_sum(10, 20, 30)")
        .await
        .unwrap();
    assert_table_eq!(result, [[60]]);
}
