use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_binary_arithmetic_addition() {
    let session = create_session();
    let result = session.execute_sql("SELECT 10 + 5").await.unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_arithmetic_subtraction() {
    let session = create_session();
    let result = session.execute_sql("SELECT 10 - 3").await.unwrap();
    assert_table_eq!(result, [[7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_arithmetic_multiplication() {
    let session = create_session();
    let result = session.execute_sql("SELECT 7 * 6").await.unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_arithmetic_division() {
    let session = create_session();
    let result = session.execute_sql("SELECT 20 / 4").await.unwrap();
    assert_table_eq!(result, [[5.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_arithmetic_modulo() {
    let session = create_session();
    let result = session.execute_sql("SELECT 17 % 5").await.unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_comparison_equal() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 = 5").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_comparison_not_equal() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 != 3").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_comparison_not_equal_alternate() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 <> 3").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_comparison_less_than() {
    let session = create_session();
    let result = session.execute_sql("SELECT 3 < 5").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_comparison_greater_than() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 > 3").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_comparison_less_than_or_equal() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 <= 5").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_comparison_greater_than_or_equal() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 >= 5").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_logical_and() {
    let session = create_session();
    let result = session.execute_sql("SELECT TRUE AND TRUE").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_logical_and_false() {
    let session = create_session();
    let result = session.execute_sql("SELECT TRUE AND FALSE").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_logical_or() {
    let session = create_session();
    let result = session.execute_sql("SELECT FALSE OR TRUE").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_logical_or_false() {
    let session = create_session();
    let result = session.execute_sql("SELECT FALSE OR FALSE").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unary_not_true() {
    let session = create_session();
    let result = session.execute_sql("SELECT NOT TRUE").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unary_not_false() {
    let session = create_session();
    let result = session.execute_sql("SELECT NOT FALSE").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unary_not_expression() {
    let session = create_session();
    let result = session.execute_sql("SELECT NOT (5 > 10)").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unary_negation() {
    let session = create_session();
    let result = session.execute_sql("SELECT -5").await.unwrap();
    assert_table_eq!(result, [[-5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unary_double_negation() {
    let session = create_session();
    let result = session.execute_sql("SELECT -(-10)").await.unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unary_positive() {
    let session = create_session();
    let result = session.execute_sql("SELECT +5").await.unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_when_simple() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END")
        .await
        .unwrap();
    assert_table_eq!(result, [["yes"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_when_else() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CASE WHEN 1 = 2 THEN 'yes' ELSE 'no' END")
        .await
        .unwrap();
    assert_table_eq!(result, [["no"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_when_multiple_conditions() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT CASE WHEN 1 = 2 THEN 'first' WHEN 2 = 2 THEN 'second' ELSE 'third' END",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["second"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_when_no_else() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CASE WHEN 1 = 2 THEN 'match' END")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_when_with_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE statuses (id INT64, status STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO statuses VALUES (1, 'active'), (2, 'inactive'), (3, 'pending')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, CASE WHEN status = 'active' THEN 1 WHEN status = 'inactive' THEN 0 ELSE -1 END AS code FROM statuses ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 1], [2, 0], [3, -1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_simple_form() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END")
        .await
        .unwrap();
    assert_table_eq!(result, [["two"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_int_to_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(123 AS STRING)")
        .await
        .unwrap();
    assert_table_eq!(result, [["123"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_string_to_int() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST('456' AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[456]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_int_to_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(10 AS FLOAT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[10.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_float_to_int() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(3.7 AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_bool_to_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(TRUE AS STRING)")
        .await
        .unwrap();
    assert_table_eq!(result, [["true"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_valid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('123' AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[123]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_invalid_returns_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('abc' AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_null_input() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST(NULL AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_list_integers() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 3 IN (1, 2, 3, 4, 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_list_not_found() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 10 IN (1, 2, 3, 4, 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_list_strings() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'apple' IN ('apple', 'banana', 'cherry')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_in_list() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 10 NOT IN (1, 2, 3, 4, 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_in_list_found() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 3 NOT IN (1, 2, 3, 4, 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_list_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULL IN (1, 2, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_where_clause() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE numbers (n INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO numbers VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT n FROM numbers WHERE n IN (2, 4) ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_between_integers() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 5 BETWEEN 1 AND 10")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_between_outside_range() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 15 BETWEEN 1 AND 10")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_between_inclusive_lower() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 1 BETWEEN 1 AND 10")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_between_inclusive_upper() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 10 BETWEEN 1 AND 10")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_between() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 15 NOT BETWEEN 1 AND 10")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_between_in_range() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 5 NOT BETWEEN 1 AND 10")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_between_strings() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'banana' BETWEEN 'apple' AND 'cherry'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_between_with_expressions() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 6 BETWEEN 2 * 2 AND 3 * 3")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_prefix() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' LIKE 'hel%'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_suffix() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' LIKE '%llo'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_contains() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' LIKE '%ell%'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_single_char_wildcard() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'cat' LIKE 'c_t'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_no_match() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' LIKE 'world%'")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_case_sensitive() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'Hello' LIKE 'hello'")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_like() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' NOT LIKE 'world%'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_like_no_match() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' NOT LIKE 'hel%'")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_in_where_clause() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE names (name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO names VALUES ('alice'), ('alex'), ('bob'), ('anna')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM names WHERE name LIKE 'al%' ORDER BY name")
        .await
        .unwrap();
    assert_table_eq!(result, [["alex"], ["alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL IS NULL").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_null_not_null_value() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 IS NULL").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_not_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 IS NOT NULL").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_not_null_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULL IS NOT NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_null_in_where_clause() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nullable (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1, 10), (2, NULL), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM nullable WHERE val IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_not_null_in_where_clause() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nullable (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1, 10), (2, NULL), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM nullable WHERE val IS NOT NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_arithmetic() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ((2 + 3) * 4) - 10")
        .await
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_logical() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT (TRUE AND FALSE) OR (TRUE AND TRUE)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_comparison_and_logical() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT (5 > 3) AND (10 < 20)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_case_in_expression() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT CASE WHEN (1 + 1) = 2 THEN CASE WHEN (2 * 2) = 4 THEN 'nested' ELSE 'no' END ELSE 'outer' END",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["nested"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_function_calls() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ABS(ROUND(-3.7, 0))")
        .await
        .unwrap();
    assert_table_eq!(result, [[4.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_complex_nested_expression() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT CASE WHEN 5 BETWEEN 1 AND 10 AND 'a' IN ('a', 'b', 'c') THEN 'match' ELSE 'no' END",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["match"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_expression_alias_simple() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 1 + 2 AS sum_result")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_expression_alias_without_as() {
    let session = create_session();
    let result = session.execute_sql("SELECT 10 * 5 product").await.unwrap();
    assert_table_eq!(result, [[50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_expression_aliases() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 1 + 1 AS two, 2 * 2 AS four, 3 * 3 AS nine")
        .await
        .unwrap();
    assert_table_eq!(result, [[2, 4, 9]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_expression_alias() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END AS answer")
        .await
        .unwrap();
    assert_table_eq!(result, [["yes"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alias_in_order_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE values_tbl (x INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO values_tbl VALUES (3), (1), (2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT x AS value FROM values_tbl ORDER BY value")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_complex_expression_with_alias() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE products (id INT64, price FLOAT64, quantity INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO products VALUES (1, 10.0, 5), (2, 20.0, 3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, price * quantity AS total_value FROM products ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 50.0], [2, 60.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_operator_precedence_mult_before_add() {
    let session = create_session();
    let result = session.execute_sql("SELECT 2 + 3 * 4").await.unwrap();
    assert_table_eq!(result, [[14]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_operator_precedence_parentheses_override() {
    let session = create_session();
    let result = session.execute_sql("SELECT (2 + 3) * 4").await.unwrap();
    assert_table_eq!(result, [[20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_operator_precedence_and_before_or() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRUE OR FALSE AND FALSE")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_operator_precedence_not_highest() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NOT FALSE AND TRUE")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_propagation_arithmetic() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 + NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_propagation_comparison() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 = NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_and_with_null_true() {
    let session = create_session();
    let result = session.execute_sql("SELECT TRUE AND NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_and_with_null_false() {
    let session = create_session();
    let result = session.execute_sql("SELECT FALSE AND NULL").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_or_with_null_true() {
    let session = create_session();
    let result = session.execute_sql("SELECT TRUE OR NULL").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_or_with_null_false() {
    let session = create_session();
    let result = session.execute_sql("SELECT FALSE OR NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT NOT NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_expression() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COALESCE(NULL, NULL, 'default')")
        .await
        .unwrap();
    assert_table_eq!(result, [["default"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ifnull_expression() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT IFNULL(NULL, 'fallback')")
        .await
        .unwrap();
    assert_table_eq!(result, [["fallback"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullif_expression() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULLIF(5, 5)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullif_different_values() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULLIF(5, 10)").await.unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_if_expression() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT IF(1 > 0, 'positive', 'non-positive')")
        .await
        .unwrap();
    assert_table_eq!(result, [["positive"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_if_expression_false() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT IF(1 < 0, 'negative', 'non-negative')")
        .await
        .unwrap();
    assert_table_eq!(result, [["non-negative"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_complex_where_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, status STRING, price FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO items VALUES (1, 'active', 100.0), (2, 'inactive', 50.0), (3, 'active', 200.0), (4, 'pending', 75.0)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id FROM items WHERE (status = 'active' AND price > 150.0) OR (status = 'pending' AND price < 100.0) ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[3], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_expression_with_string_concatenation() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CONCAT('Hello', ' ', 'World')")
        .await
        .unwrap();
    assert_table_eq!(result, [["Hello World"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_expression_with_string_operators() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'Hello' || ' ' || 'World'")
        .await
        .unwrap();
    assert_table_eq!(result, [["Hello World"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_expression_with_length() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LENGTH('Hello') > 3")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_expression_result() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 5 > 3 AND 10 < 20 AND 'a' = 'a'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_between_in_case() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT CASE WHEN 50 BETWEEN 0 AND 100 THEN 'in range' ELSE 'out of range' END",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["in range"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_list_in_case() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT CASE WHEN 'apple' IN ('apple', 'banana') THEN 'fruit' ELSE 'other' END",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["fruit"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_in_case() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CASE WHEN 'hello' LIKE 'hel%' THEN 'matches' ELSE 'no match' END")
        .await
        .unwrap();
    assert_table_eq!(result, [["matches"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_triple_comparison() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 5 >= 3 AND 5 <= 10 AND 5 != 7")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_in_comparison() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST('10' AS INT64) > 5")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_in_arithmetic() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST('10' AS INT64) + CAST('20' AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_not() {
    let session = create_session();
    let result = session.execute_sql("SELECT NOT NOT TRUE").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_expression_with_subquery() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE values_tbl (v INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO values_tbl VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT 10 > (SELECT MAX(v) FROM values_tbl)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_subquery() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ids (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ids VALUES (1), (3), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT 3 IN (SELECT id FROM ids)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_in_subquery() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ids (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ids VALUES (1), (3), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT 4 NOT IN (SELECT id FROM ids)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_deeply_nested_parentheses() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT (((1 + 2) * (3 + 4)) - (5 * 2))")
        .await
        .unwrap();
    assert_table_eq!(result, [[11]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mixed_types_in_case() {
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
        .execute_sql(
            "SELECT id, CASE WHEN val IS NULL THEN 'missing' WHEN val > 20 THEN 'high' ELSE 'low' END AS category FROM data ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "low"], [2, "missing"], [3, "high"]]);
}
