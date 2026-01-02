use crate::assert_table_eq;
use crate::common::{
    IntoValue, bignumeric, create_session, date, datetime, null, numeric, st, time, timestamp,
};

#[tokio::test(flavor = "current_thread")]
async fn test_select_star_from_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING, age INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM users ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice", 30], [2, "Bob", 25]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_star_empty_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE empty (id INT64, name STRING)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM empty").await.unwrap();
    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_specific_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE employees (id INT64, name STRING, dept STRING, salary INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 100000), (2, 'Bob', 'Sales', 80000)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name, salary FROM employees ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice", 100000], ["Bob", 80000]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_single_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 'apple'), (2, 'banana')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM items ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [["apple"], ["banana"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_columns_reordered() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (a INT64, b INT64, c INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 2, 3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT c, a, b FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[3, 1, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_arithmetic_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO numbers VALUES (10, 5), (20, 3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a + b, a - b, a * b FROM numbers ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[15, 5, 50], [23, 17, 60]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_string_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE names (first_name STRING, last_name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO names VALUES ('John', 'Doe'), ('Jane', 'Smith')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT CONCAT(first_name, ' ', last_name) FROM names ORDER BY first_name")
        .await
        .unwrap();
    assert_table_eq!(result, [["Jane Smith"], ["John Doe"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_case_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE scores (id INT64, score INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO scores VALUES (1, 90), (2, 70), (3, 50)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, CASE WHEN score >= 80 THEN 'A' WHEN score >= 60 THEN 'B' ELSE 'C' END AS grade FROM scores ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "A"], [2, "B"], [3, "C"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_alias_as() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE products (id INT64, price INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO products VALUES (1, 100)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id AS product_id, price AS unit_price FROM products")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_alias_without_as() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (42)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val result FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_expression_with_alias() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE prices (base INT64, tax INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO prices VALUES (100, 10)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT base + tax AS total_price FROM prices")
        .await
        .unwrap();
    assert_table_eq!(result, [[110]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_star_except_single_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE orders (order_id INT64, item STRING, quantity INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO orders VALUES (1, 'widget', 100)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * EXCEPT (order_id) FROM orders")
        .await
        .unwrap();
    assert_table_eq!(result, [["widget", 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_star_except_multiple_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (a INT64, b INT64, c INT64, d INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 2, 3, 4)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * EXCEPT (b, d) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_star_replace_single_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE inventory (item STRING, quantity INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO inventory VALUES ('apple', 50)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * REPLACE (quantity * 2 AS quantity) FROM inventory")
        .await
        .unwrap();
    assert_table_eq!(result, [["apple", 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_star_replace_multiple_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (a INT64, b INT64, c INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (10, 20, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * REPLACE (a + 1 AS a, c * 2 AS c) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[11, 20, 60]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_star_except_and_replace() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (a INT64, b INT64, c INT64, d INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 2, 3, 4)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * EXCEPT (d) REPLACE (a * 10 AS a) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 2, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_distinct_basic() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE colors (color STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO colors VALUES ('red'), ('blue'), ('red'), ('green'), ('blue')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT color FROM colors ORDER BY color")
        .await
        .unwrap();
    assert_table_eq!(result, [["blue"], ["green"], ["red"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_distinct_multiple_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE pairs (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO pairs VALUES (1, 2), (1, 3), (1, 2), (2, 3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT a, b FROM pairs ORDER BY a, b")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 2], [1, 3], [2, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_distinct_with_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nullable (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1), (NULL), (1), (NULL), (2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT val FROM nullable ORDER BY val NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[null()], [1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_computed_column_arithmetic() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sales (units INT64, price_per_unit INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES (5, 20), (10, 15)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT units * price_per_unit AS total FROM sales ORDER BY total")
        .await
        .unwrap();
    assert_table_eq!(result, [[100], [150]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_computed_column_function() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE strings (s STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO strings VALUES ('hello'), ('world')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT UPPER(s) AS upper_s, LENGTH(s) AS len FROM strings ORDER BY s")
        .await
        .unwrap();
    assert_table_eq!(result, [["HELLO", 5], ["WORLD", 5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_computed_column_nested_function() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (-5), (10)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ABS(val) + 1 AS adjusted FROM data ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[6], [11]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_struct_field_access() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE records (id INT64, info STRUCT<name STRING, details STRUCT<city STRING, zip INT64>>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO records VALUES (1, STRUCT('Alice' AS name, STRUCT('NYC' AS city, 10001 AS zip) AS details))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, info.name, info.details.city FROM records")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice", "NYC"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_field_access_basic() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE users (id INT64, profile STRUCT<name STRING, age INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO users VALUES (1, STRUCT('Bob' AS name, 30 AS age)), (2, STRUCT('Carol' AS name, 25 AS age))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT profile.name, profile.age FROM users ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [["Bob", 30], ["Carol", 25]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_field_in_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE employees (id INT64, salary STRUCT<base INT64, bonus INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO employees VALUES (1, STRUCT(100000 AS base, 10000 AS bonus))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT salary.base + salary.bonus AS total FROM employees")
        .await
        .unwrap();
    assert_table_eq!(result, [[110000]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_element_access_offset() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, values ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, [10, 20, 30])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT values[OFFSET(0)], values[OFFSET(2)] FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_element_access_ordinal() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, items ARRAY<STRING>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, ['a', 'b', 'c'])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT items[ORDINAL(1)], items[ORDINAL(3)] FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [["a", "c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_element_access_direct_index() {
    let session = create_session();
    let result = session.execute_sql("SELECT [10, 20, 30][1]").await.unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_in_struct_access() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, container STRUCT<items ARRAY<INT64>>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, STRUCT([100, 200, 300] AS items))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT container.items[OFFSET(1)] FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_literal_values() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 42, 'hello', 3.14, true, NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[42, "hello", 3.14, true, null()]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_coalesce() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nullable (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1, NULL), (2, 100)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, COALESCE(val, 0) AS val FROM nullable ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 0], [2, 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_if_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, 20)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, IF(val > 15, 'high', 'low') AS category FROM data ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "low"], [2, "high"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_qualified_star() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t1 (a INT64, b STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE t2 (c INT64, d STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t1 VALUES (1, 'x')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t2 VALUES (2, 'y')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT t1.* FROM t1 CROSS JOIN t2")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "x"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_duplicate_column_with_alias() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val, val AS val2, val * 2 AS val_doubled FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, 5, 10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_nullif() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 0), (2, 5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, NULLIF(val, 0) AS non_zero FROM data ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, null()], [2, 5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_cast() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (42)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT CAST(val AS STRING) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [["42"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_safe_cast() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('abc' AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null()]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_projection_preserves_order() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ordered (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ordered VALUES (3, 'c'), (1, 'a'), (2, 'b')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM ordered ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [["a"], ["b"], ["c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_wildcard_expansion() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, info STRUCT<name STRING, age INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, STRUCT('Alice' AS name, 30 AS age))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT info.* FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice", 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_wildcard_multiple_fields() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE records (id INT64, person STRUCT<first_name STRING, last_name STRING, age INT64, city STRING>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO records VALUES (1, STRUCT('John' AS first_name, 'Doe' AS last_name, 25 AS age, 'NYC' AS city))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT person.* FROM records")
        .await
        .unwrap();
    assert_table_eq!(result, [["John", "Doe", 25, "NYC"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_wildcard_with_other_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, info STRUCT<x INT64, y INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, STRUCT(10 AS x, 20 AS y))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, info.* FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_function_in_binary_op() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sales (id INT64, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES (1, 100), (2, 200), (3, 300)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, amount + ROW_NUMBER() OVER (ORDER BY id) AS adjusted FROM sales ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 101], [2, 202], [3, 303]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_function_in_case_condition() {
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
        .execute_sql("SELECT id, CASE WHEN ROW_NUMBER() OVER (ORDER BY id) > 1 THEN 'not first' ELSE 'first' END AS position FROM data ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "first"], [2, "not first"], [3, "not first"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_function_in_case_result() {
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
        .execute_sql("SELECT id, CASE WHEN val > 15 THEN ROW_NUMBER() OVER (ORDER BY id) ELSE 0 END AS rn FROM data ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 0], [2, 2], [3, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_function_in_case_else() {
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
        .execute_sql("SELECT id, CASE WHEN val > 100 THEN 999 ELSE ROW_NUMBER() OVER (ORDER BY id) END AS rn FROM data ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 1], [2, 2], [3, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_function_with_cast() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, CAST(ROW_NUMBER() OVER (ORDER BY id) AS STRING) AS rn_str FROM data ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "1"], [2, "2"], [3, "3"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_function_in_scalar_function() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, ABS(ROW_NUMBER() OVER (ORDER BY id) - 2) AS distance FROM data ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 1], [2, 0], [3, 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_function_with_alias() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM data ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 1], [2, 2], [3, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_function_with_unary_op() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, -ROW_NUMBER() OVER (ORDER BY id) AS neg_rn FROM data ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, -1], [2, -2], [3, -3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_projection_with_date_literal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-15'")
        .await
        .unwrap();
    assert_table_eq!(result, [[date(2024, 1, 15)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_projection_with_time_literal() {
    let session = create_session();
    let result = session.execute_sql("SELECT TIME '12:30:45'").await.unwrap();
    assert_table_eq!(result, [[time(12, 30, 45)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_projection_with_timestamp_literal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP '2024-01-15 12:30:45'")
        .await
        .unwrap();
    assert_table_eq!(result, [[timestamp(2024, 1, 15, 12, 30, 45)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_projection_with_datetime_literal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME '2024-01-15 12:30:45'")
        .await
        .unwrap();
    assert_table_eq!(result, [[datetime(2024, 1, 15, 12, 30, 45)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_projection_with_numeric_literal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NUMERIC '123.456'")
        .await
        .unwrap();
    assert_table_eq!(result, [[numeric("123.456")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_projection_with_bignumeric_literal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT BIGNUMERIC '12345678901234567890.123456789'")
        .await
        .unwrap();
    assert_table_eq!(result, [[bignumeric("12345678901234567890.123456789")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_projection_with_bytes_literal() {
    let session = create_session();
    let result = session.execute_sql("SELECT B'hello'").await.unwrap();
    assert_table_eq!(result, [[b"hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_projection_with_json_literal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON '{\"key\": \"value\"}'")
        .await
        .unwrap();
    assert_table_eq!(result, [["{\"key\":\"value\"}"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_projection_with_array_literal() {
    let session = create_session();
    let result = session.execute_sql("SELECT [1, 2, 3]").await.unwrap();
    assert_table_eq!(result, [[[1, 2, 3]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_projection_with_struct_literal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRUCT(1 AS a, 'hello' AS b)")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[st(vec![("a", 1.into_value()), ("b", "hello".into_value())])]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_op_type_inference_float() {
    let session = create_session();
    let result = session.execute_sql("SELECT 1 + 2.5").await.unwrap();
    assert_table_eq!(result, [[3.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_op_subtraction() {
    let session = create_session();
    let result = session.execute_sql("SELECT 10 - 3").await.unwrap();
    assert_table_eq!(result, [[7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_op_multiplication() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 * 4").await.unwrap();
    assert_table_eq!(result, [[20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_op_comparison_returns_bool() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 1 < 2, 3 > 2, 1 = 1, 1 != 2, 2 <= 2, 3 >= 2")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, true, true, true, true, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_op_logical_returns_bool() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT true AND true, false OR true")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_op_concat() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' || ' ' || 'world'")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_op_modulo() {
    let session = create_session();
    let result = session.execute_sql("SELECT 10 % 3").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_op_division() {
    let session = create_session();
    let result = session.execute_sql("SELECT 10 / 4").await.unwrap();
    assert_table_eq!(result, [[2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_in_right_side_of_binary_op() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 100), (2, 200), (3, 300)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, val - ROW_NUMBER() OVER (ORDER BY id) AS adjusted FROM data ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 99], [2, 198], [3, 297]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_window_functions_in_projection() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 100), (2, 200), (3, 300)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn, SUM(val) OVER (ORDER BY id) AS running_sum FROM data ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 1, 100], [2, 2, 300], [3, 3, 600]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_non_window_and_window_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, name STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, name, val, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM data ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "a", 10, 1], [2, "b", 20, 2], [3, "c", 30, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_projection_preserves_column_from_qualified_wildcard() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t1 (a INT64, b STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE t2 (c INT64, d STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t1 VALUES (1, 'x'), (2, 'y')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t2 VALUES (10, 'p'), (20, 'q')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT t2.* FROM t1 CROSS JOIN t2 ORDER BY t1.a, t2.c")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, "p"], [20, "q"], [10, "p"], [20, "q"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualified_wildcard_with_alias() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (x INT64, y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT d.* FROM data AS d")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_with_operand_and_window() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, category STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 'A'), (2, 'B'), (3, 'A')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, CASE category WHEN 'A' THEN ROW_NUMBER() OVER (ORDER BY id) ELSE 0 END AS rn FROM data ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 1], [2, 0], [3, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_window_in_expression() {
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
        .execute_sql(
            "SELECT id, val * 2 + SUM(val) OVER (ORDER BY id) AS computed FROM data ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 30], [2, 70], [3, 120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_with_multiple_window_args() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, GREATEST(ROW_NUMBER() OVER (ORDER BY id), 2) AS max_val FROM data ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 2], [2, 2], [3, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_struct_wildcard() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, outer_s STRUCT<inner_s STRUCT<a INT64, b STRING>, c INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, STRUCT(STRUCT(10 AS a, 'hello' AS b) AS inner_s, 20 AS c))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT outer_s.inner_s.* FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, "hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_remap_column_with_table_qualifier() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t1 (a INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE t2 (a INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t1 VALUES (1)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t2 VALUES (2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT t1.a + ROW_NUMBER() OVER (ORDER BY t1.a) AS val FROM t1 CROSS JOIN t2")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_wildcard_on_non_struct_error() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT (id + 1).* FROM data").await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("non-struct"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_wildcard_on_array_expression_error() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT ([1, 2, 3]).* FROM data").await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("non-struct"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_function_in_case_operand() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, CASE ROW_NUMBER() OVER (ORDER BY id) WHEN 1 THEN 'first' WHEN 2 THEN 'second' ELSE 'other' END AS pos FROM data ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "first"], [2, "second"], [3, "other"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitwise_xor_type_inference() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 5 ^ 3 AS xor_result, TYPEOF(5 ^ 3) AS xor_type")
        .await
        .unwrap();
    assert_table_eq!(result, [[6, "INT64"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_type_inference_in_arithmetic() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NUMERIC '1.5' + NUMERIC '2.5' AS result, TYPEOF(NUMERIC '1.5' + NUMERIC '2.5') AS result_type")
        .await
        .unwrap();
    assert_table_eq!(result, [[numeric("4.0"), "NUMERIC"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_in_nested_case_operand() {
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
        .execute_sql("SELECT id, CASE CASE ROW_NUMBER() OVER (ORDER BY id) WHEN 1 THEN 'A' ELSE 'B' END WHEN 'A' THEN val ELSE val * 2 END AS computed FROM data ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [2, 40], [3, 60]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_remap_column_indices_with_cast() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 100), (2, 200)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, CAST(val + ROW_NUMBER() OVER (ORDER BY id) AS STRING) AS result FROM data ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "101"], [2, "202"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_remap_column_with_scalar_function() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, COALESCE(ROW_NUMBER() OVER (ORDER BY id), 0) AS rn FROM data ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 1], [2, 2], [3, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_except_with_case_insensitive_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (ID INT64, Name STRING, VALUE INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 'test', 100)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * EXCEPT (id, VALUE) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [["test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_replace_with_case_insensitive_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (ID INT64, Name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * REPLACE (id * 10 AS ID) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, "test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitwise_and_type_inference() {
    let session = create_session();
    let result = session.execute_sql("SELECT TYPEOF(5 & 3)").await.unwrap();
    assert_table_eq!(result, [["INT64"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bitwise_or_type_inference() {
    let session = create_session();
    let result = session.execute_sql("SELECT TYPEOF(5 | 3)").await.unwrap();
    assert_table_eq!(result, [["INT64"]]);
}
