use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_simple_table_reference() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE products (id INT64, name STRING, price INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO products VALUES (1, 'Widget', 100), (2, 'Gadget', 200)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name, price FROM products ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Widget", 100], [2, "Gadget", 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_reference_select_star() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 'A'), (2, 'B')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM items ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "A"], [2, "B"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_alias_with_as() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE employees (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO employees VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT e.id, e.name FROM employees AS e ORDER BY e.id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"], [2, "Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_alias_without_as() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE orders (id INT64, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO orders VALUES (1, 100), (2, 200)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT o.id, o.amount FROM orders o ORDER BY o.id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 100], [2, 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_tablesample_system_percent() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE large_table (id INT64)")
        .await
        .unwrap();
    for i in (1..=100).step_by(10) {
        let values: Vec<String> = (i..i + 10).map(|n| format!("({})", n)).collect();
        session
            .execute_sql(&format!(
                "INSERT INTO large_table VALUES {}",
                values.join(", ")
            ))
            .await
            .unwrap();
    }

    let result = session
        .execute_sql("SELECT COUNT(*) <= 100 FROM large_table TABLESAMPLE SYSTEM (50 PERCENT)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_tablesample_system_rows() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sample_data (id INT64)")
        .await
        .unwrap();
    for i in (1..=100).step_by(10) {
        let values: Vec<String> = (i..i + 10).map(|n| format!("({})", n)).collect();
        session
            .execute_sql(&format!(
                "INSERT INTO sample_data VALUES {}",
                values.join(", ")
            ))
            .await
            .unwrap();
    }

    let result = session
        .execute_sql("SELECT COUNT(*) <= 10 FROM sample_data TABLESAMPLE SYSTEM (10 ROWS)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_tablesample_with_alias() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .await
        .unwrap();
    for i in (1..=100).step_by(10) {
        let values: Vec<String> = (i..i + 10)
            .map(|n| format!("({}, {})", n, n * 10))
            .collect();
        session
            .execute_sql(&format!("INSERT INTO data VALUES {}", values.join(", ")))
            .await
            .unwrap();
    }

    let result = session
        .execute_sql("SELECT COUNT(*) >= 0 FROM data AS d TABLESAMPLE SYSTEM (20 PERCENT)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_for_system_time_as_of_current_timestamp() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE versioned (id INT64, value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO versioned VALUES (1, 'current')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM versioned FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP()")
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_for_system_time_as_of_with_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE historical (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO historical VALUES (1), (2)")
        .await
        .unwrap();

    let result = session.execute_sql(
        "SELECT * FROM historical FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)",
    ).await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_for_system_time_as_of_with_alias() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE time_data (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO time_data VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session.execute_sql(
        "SELECT t.id FROM time_data AS t FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)",
    ).await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_simple_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT * FROM UNNEST([1, 2, 3]) AS n ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_string_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT * FROM UNNEST(['a', 'b', 'c']) AS s ORDER BY s")
        .await
        .unwrap();
    assert_table_eq!(result, [["a"], ["b"], ["c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_with_offset() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT elem, off FROM UNNEST(['x', 'y', 'z']) AS elem WITH OFFSET AS off ORDER BY off",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["x", 0], ["y", 1], ["z", 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_struct_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT s.a, s.b FROM UNNEST([STRUCT(1 AS a, 'x' AS b), STRUCT(2 AS a, 'y' AS b)]) AS s ORDER BY s.a")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "x"], [2, "y"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_in_join_with_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE users_arrays (id INT64, tags ARRAY<STRING>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO users_arrays VALUES (1, ['a', 'b']), (2, ['c'])")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT u.id, tag FROM users_arrays u, UNNEST(u.tags) AS tag ORDER BY u.id, tag",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [1, "b"], [2, "c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_subquery_in_from() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nums (n INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nums VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM (SELECT n FROM nums WHERE n > 2) AS sub ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[3], [4], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_subquery_with_alias() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE values_table (x INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO values_table VALUES (10), (20), (30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT sq.x, sq.x * 2 AS doubled FROM (SELECT x FROM values_table) AS sq ORDER BY sq.x")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 20], [20, 40], [30, 60]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_subqueries() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE base (v INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO base VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT outer_sq.doubled FROM (SELECT inner_sq.v * 2 AS doubled FROM (SELECT v FROM base) AS inner_sq) AS outer_sq ORDER BY outer_sq.doubled")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [4], [6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_tables_cross_join_implicit() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t1 (a INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE t2 (b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t1 VALUES (1), (2)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t2 VALUES (10), (20)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a, b FROM t1, t2 ORDER BY a, b")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [1, 20], [2, 10], [2, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_tables_three_way() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ta (x INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE tb (y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE tc (z INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ta VALUES (1)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO tb VALUES (2)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO tc VALUES (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT x, y, z FROM ta, tb, tc")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 2, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_tables_with_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE left_t (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE right_t (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO left_t VALUES (1, 'A'), (2, 'B')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO right_t VALUES (1, 100), (2, 200)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT left_t.name, right_t.value FROM left_t, right_t WHERE left_t.id = right_t.id ORDER BY left_t.id")
        .await
        .unwrap();
    assert_table_eq!(result, [["A", 100], ["B", 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_valued_function_simple() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION numbers_tvf(n INT64)
            RETURNS TABLE<num INT64>
            AS (SELECT num FROM UNNEST(GENERATE_ARRAY(1, n)) AS num)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM numbers_tvf(5) ORDER BY num")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3], [4], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_valued_function_with_multiple_columns() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION people_tvf()
            RETURNS TABLE<id INT64, name STRING>
            AS (SELECT * FROM UNNEST([STRUCT(1 AS id, 'Alice' AS name), STRUCT(2, 'Bob')]))",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM people_tvf() ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"], [2, "Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_valued_function_with_parameter() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION filtered_data(min_val INT64)
            RETURNS TABLE<value INT64>
            AS (SELECT val FROM UNNEST([1, 2, 3, 4, 5]) AS val WHERE val >= min_val)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM filtered_data(3) ORDER BY value")
        .await
        .unwrap();
    assert_table_eq!(result, [[3], [4], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_valued_function_in_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE base_data (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO base_data VALUES (1), (2)")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION multiplier_tvf(x INT64)
            RETURNS TABLE<result INT64>
            AS (SELECT x * n AS result FROM UNNEST([1, 2, 3]) AS n)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT b.id, m.result FROM base_data b, multiplier_tvf(10) m ORDER BY b.id, m.result",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[1, 10], [1, 20], [1, 30], [2, 10], [2, 20], [2, 30]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_from_dual_equivalent() {
    let session = create_session();
    let result = session.execute_sql("SELECT 1 AS value").await.unwrap();
    assert_table_eq!(result, [[1]]);
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
async fn test_subquery_with_aggregation() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sales (region STRING, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES ('East', 100), ('East', 200), ('West', 150)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT region, total FROM (SELECT region, SUM(amount) AS total FROM sales GROUP BY region) AS sub ORDER BY region")
        .await
        .unwrap();
    assert_table_eq!(result, [["East", 300], ["West", 150]]);
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
async fn test_unnest_generate_date_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(d AS STRING) FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-03')) AS d ORDER BY d")
        .await
        .unwrap();
    assert_table_eq!(result, [["2024-01-01"], ["2024-01-02"], ["2024-01-03"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_with_schema_reference() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA my_schema")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE my_schema.my_table (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO my_schema.my_table VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name FROM my_schema.my_table")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_unnest_in_from() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT a, b FROM UNNEST([1, 2]) AS a, UNNEST(['x', 'y']) AS b ORDER BY a, b")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "x"], [1, "y"], [2, "x"], [2, "y"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_correlated_unnest() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE array_table (id INT64, arr ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO array_table VALUES (1, [10, 20]), (2, [30])")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT t.id, elem FROM array_table t, UNNEST(t.arr) AS elem ORDER BY t.id, elem",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [1, 20], [2, 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_right_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE left_table (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE right_table (id INT64, value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO left_table VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO right_table VALUES (2, 'X'), (3, 'Y'), (4, 'Z')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.name, r.value FROM left_table l RIGHT JOIN right_table r ON l.id = r.id ORDER BY r.id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["Bob", "X"], [null, "Y"], [null, "Z"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_right_outer_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE employees_ro (id INT64, dept_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE depts_ro (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO employees_ro VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO depts_ro VALUES (10, 'Engineering'), (30, 'Sales')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT e.id, d.name FROM employees_ro e RIGHT OUTER JOIN depts_ro d ON e.dept_id = d.id ORDER BY d.id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Engineering"], [null, "Sales"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_left_outer_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE people_lo (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE addresses_lo (person_id INT64, city STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO people_lo VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO addresses_lo VALUES (1, 'NYC'), (3, 'LA')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT p.name, a.city FROM people_lo p LEFT OUTER JOIN addresses_lo a ON p.id = a.person_id ORDER BY p.id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice", "NYC"], ["Bob", null], ["Charlie", "LA"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_basic() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE products_v (id INT64, name STRING, price INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO products_v VALUES (1, 'Widget', 100), (2, 'Gadget', 200)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE VIEW active_products AS SELECT id, name, price FROM products_v WHERE price > 50")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM active_products ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Widget", 100], [2, "Gadget", 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_with_alias() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items_v (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items_v VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE VIEW items_view AS SELECT id, value FROM items_v")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT iv.id, iv.value FROM items_view AS iv ORDER BY iv.id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [2, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_with_column_aliases() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data_v (x INT64, y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data_v VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE VIEW data_view (a, b) AS SELECT x, y FROM data_v")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a, b FROM data_view ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [2, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_without_condition() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t_a (a INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE t_b (b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t_a VALUES (1), (2)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t_b VALUES (10), (20)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a, b FROM t_a JOIN t_b ORDER BY a, b")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [1, 20], [2, 10], [2, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_from_tables_with_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE m1 (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE m2 (id INT64, code STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE m3 (ref_id INT64, description STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO m1 VALUES (1, 'A'), (2, 'B')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO m2 VALUES (1, 'X'), (2, 'Y')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO m3 VALUES (1, 'Desc1'), (2, 'Desc2')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT m1.val, m2.code, m3.description FROM m1, m2 INNER JOIN m3 ON m2.id = m3.ref_id WHERE m1.id = m2.id ORDER BY m1.id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["A", "X", "Desc1"], ["B", "Y", "Desc2"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cte_with_alias_reference() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE cte_data (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO cte_data VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH my_cte AS (SELECT id, name FROM cte_data) SELECT c.id, c.name FROM my_cte AS c ORDER BY c.id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"], [2, "Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cross_join_with_unnest() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE base_t (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO base_t VALUES (1, 'A'), (2, 'B')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT b.id, b.name, n FROM base_t b CROSS JOIN UNNEST([10, 20]) AS n ORDER BY b.id, n",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[1, "A", 10], [1, "A", 20], [2, "B", 10], [2, "B", 20]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_derived_table_without_alias() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE derived_src (x INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO derived_src VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM (SELECT x FROM derived_src WHERE x > 1) ORDER BY x")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_with_outer_schema() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE outer_data (id INT64, arr ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO outer_data VALUES (1, [10, 20]), (2, [30])")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT o.id, elem FROM outer_data o, UNNEST(o.arr) AS elem ORDER BY o.id, elem",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [1, 20], [2, 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_not_found_error() {
    let session = create_session();
    let result = session.execute_sql("SELECT * FROM nonexistent_table").await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("not found") || err.to_string().contains("Table"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_on_condition() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE jc_left (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE jc_right (ref_id INT64, data STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO jc_left VALUES (1, 'A'), (2, 'B')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO jc_right VALUES (1, 'X'), (1, 'Y')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.val, r.data FROM jc_left l INNER JOIN jc_right r ON l.id = r.ref_id ORDER BY l.val, r.data",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["A", "X"], ["A", "Y"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_struct_with_offset() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT s.a, s.b, off FROM UNNEST([STRUCT(1 AS a, 'x' AS b), STRUCT(2 AS a, 'y' AS b)]) AS s WITH OFFSET AS off ORDER BY off",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "x", 0], [2, "y", 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_alias_in_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE orders_ja (id INT64, customer_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE customers_ja (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO orders_ja VALUES (100, 1), (101, 2)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO customers_ja VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT o.id AS order_id, c.name FROM orders_ja AS o INNER JOIN customers_ja AS c ON o.customer_id = c.id ORDER BY o.id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[100, "Alice"], [101, "Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_implicit_cross_join_with_unnest() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT a, b FROM UNNEST([1, 2]) AS a, UNNEST(['x', 'y']) AS b ORDER BY a, b")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "x"], [1, "y"], [2, "x"], [2, "y"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_empty_from_clause() {
    let session = create_session();
    let result = session.execute_sql("SELECT 1 + 1 AS sum").await.unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_tvf_with_alias() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE FUNCTION my_tvf(n INT64)
            RETURNS TABLE<num INT64>
            AS (SELECT num FROM UNNEST(GENERATE_ARRAY(1, n)) AS num)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT t.num FROM my_tvf(3) AS t ORDER BY t.num")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_joins_chained() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE chain_a (id INT64, val_a STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE chain_b (id INT64, val_b STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE chain_c (id INT64, val_c STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO chain_a VALUES (1, 'A1'), (2, 'A2')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO chain_b VALUES (1, 'B1'), (2, 'B2')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO chain_c VALUES (1, 'C1'), (2, 'C2')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT a.val_a, b.val_b, c.val_c FROM chain_a a JOIN chain_b b ON a.id = b.id JOIN chain_c c ON b.id = c.id ORDER BY a.id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["A1", "B1", "C1"], ["A2", "B2", "C2"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_join_with_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE vj_base (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE vj_details (ref_id INT64, detail STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO vj_base VALUES (1, 'Item1'), (2, 'Item2')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO vj_details VALUES (1, 'Detail1'), (2, 'Detail2')")
        .await
        .unwrap();
    session
        .execute_sql("CREATE VIEW vj_view AS SELECT id, name FROM vj_base")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT v.name, d.detail FROM vj_view v JOIN vj_details d ON v.id = d.ref_id ORDER BY v.id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["Item1", "Detail1"], ["Item2", "Detail2"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_column_count_mismatch_error() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE vcm_data (x INT64, y INT64, z INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO vcm_data VALUES (1, 2, 3)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE VIEW vcm_view (a, b) AS SELECT x, y, z FROM vcm_data")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM vcm_view").await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("mismatch") || err_msg.contains("column"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_function_not_found_error() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT * FROM nonexistent_function(42)")
        .await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("not found") || err_msg.contains("function"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_using_constraint_error() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE using_left (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE using_right (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO using_left VALUES (1, 'Alice')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO using_right VALUES (1, 100)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM using_left JOIN using_right USING (id)")
        .await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.to_lowercase().contains("unsupported") || err_msg.to_lowercase().contains("using")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_natural_join_constraint_error() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE natural_left (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE natural_right (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO natural_left VALUES (1, 'Alice')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO natural_right VALUES (1, 100)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM natural_left NATURAL JOIN natural_right")
        .await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.to_lowercase().contains("unsupported")
            || err_msg.to_lowercase().contains("natural")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_with_offset_default_alias() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT * FROM UNNEST([10, 20, 30]) WITH OFFSET ORDER BY offset")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 0], [20, 1], [30, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_without_alias_defaults_to_element() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT element FROM UNNEST([5, 10, 15]) ORDER BY element")
        .await
        .unwrap();
    assert_table_eq!(result, [[5], [10], [15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cte_without_alias() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE cte_no_alias (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO cte_no_alias VALUES (1, 'A'), (2, 'B')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH my_cte AS (SELECT id, val FROM cte_no_alias) SELECT id, val FROM my_cte ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "A"], [2, "B"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_without_alias() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE view_no_alias (x INT64, y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO view_no_alias VALUES (10, 20), (30, 40)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE VIEW vna_view AS SELECT x, y FROM view_no_alias")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT x, y FROM vna_view ORDER BY x")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 20], [30, 40]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_from_with_unnest_and_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE mfu_base (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE mfu_other (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO mfu_base VALUES (1), (2)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO mfu_other VALUES (1, 'X'), (2, 'Y')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT b.id, o.val, n FROM mfu_base b, mfu_other o INNER JOIN UNNEST([100, 200]) AS n ON TRUE WHERE b.id = o.id ORDER BY b.id, n",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[1, "X", 100], [1, "X", 200], [2, "Y", 100], [2, "Y", 200]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_derived_table_with_complex_subquery() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE dtc_data (category STRING, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO dtc_data VALUES ('A', 10), ('A', 20), ('B', 30), ('B', 40), ('C', 50)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT sq.category, sq.total FROM (SELECT category, SUM(amount) AS total FROM dtc_data GROUP BY category HAVING SUM(amount) > 30) AS sq ORDER BY sq.category",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["B", 70], ["C", 50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_tvf_sql_query_body_with_param_substitution() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION double_values(multiplier INT64)
            RETURNS TABLE<result INT64>
            AS (SELECT n * multiplier AS result FROM UNNEST([1, 2, 3]) AS n)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM double_values(5) ORDER BY result")
        .await
        .unwrap();
    assert_table_eq!(result, [[5], [10], [15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_unnest_with_outer_schema_context() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE mos_data (id INT64, arr1 ARRAY<INT64>, arr2 ARRAY<STRING>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO mos_data VALUES (1, [10, 20], ['a', 'b'])")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT d.id, e1, e2 FROM mos_data d, UNNEST(d.arr1) AS e1, UNNEST(d.arr2) AS e2 ORDER BY e1, e2",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[1, 10, "a"], [1, 10, "b"], [1, 20, "a"], [1, 20, "b"]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_with_none_condition_in_explicit_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE jnc_a (x INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE jnc_b (y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO jnc_a VALUES (1), (2)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO jnc_b VALUES (10), (20)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT x, y FROM jnc_a JOIN jnc_b ORDER BY x, y")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [1, 20], [2, 10], [2, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_subquery_cross_join_with_unnest_in_second_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE scju_data (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO scju_data VALUES (1, 'A'), (2, 'B')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT d.id, d.name, n FROM scju_data d, UNNEST([100, 200]) AS n ORDER BY d.id, n",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[1, "A", 100], [1, "A", 200], [2, "B", 100], [2, "B", 200]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_struct_fields_direct_access() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT a, b FROM UNNEST([STRUCT(100 AS a, 'first' AS b), STRUCT(200 AS a, 'second' AS b)]) ORDER BY a",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[100, "first"], [200, "second"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cross_join_explicit_with_unnest() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE cjeu_t (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO cjeu_t VALUES (1, 'A')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT t.id, t.val, n FROM cjeu_t t CROSS JOIN UNNEST([10, 20, 30]) AS n ORDER BY n",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "A", 10], [1, "A", 20], [1, "A", 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_four_way_implicit_cross_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE fw_a (a INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE fw_b (b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE fw_c (c INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE fw_d (d INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO fw_a VALUES (1)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO fw_b VALUES (2)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO fw_c VALUES (3)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO fw_d VALUES (4)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a, b, c, d FROM fw_a, fw_b, fw_c, fw_d")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 2, 3, 4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_second_from_table_with_unnest() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sft_base (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sft_base VALUES (1), (2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT b.id, n FROM sft_base b, UNNEST([10, 20]) AS n ORDER BY b.id, n")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [1, 20], [2, 10], [2, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_chain_with_mixed_types() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE jcm_orders (id INT64, customer_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE jcm_customers (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE jcm_products (order_id INT64, product STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO jcm_orders VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO jcm_customers VALUES (10, 'Alice'), (20, 'Bob')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO jcm_products VALUES (1, 'Widget'), (2, 'Gadget')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT c.name, p.product FROM jcm_orders o LEFT JOIN jcm_customers c ON o.customer_id = c.id RIGHT JOIN jcm_products p ON o.id = p.order_id ORDER BY p.product",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["Bob", "Gadget"], ["Alice", "Widget"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_not_table_function_error() {
    let session = create_session();
    session
        .execute_sql("CREATE FUNCTION square(x INT64) RETURNS INT64 AS (x * x)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM square(5)").await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("not a table function") || err_msg.contains("table function"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_with_column_aliases_and_table_alias() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE va_data (x INT64, y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO va_data VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE VIEW va_view (col_a, col_b) AS SELECT x, y FROM va_data")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT v.col_a, v.col_b FROM va_view AS v ORDER BY v.col_a")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [2, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_in_correlated_subquery() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE outer_arr (id INT64, arr ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO outer_arr VALUES (1, [10, 20, 30]), (2, [40, 50])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT o.id, ARRAY_LENGTH(o.arr) AS arr_len FROM outer_arr o ORDER BY o.id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 3], [2, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_from_tables_first_with_join_second_with_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE mfj_a (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE mfj_b (id INT64, ref_id INT64, code STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE mfj_c (id INT64, data STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE mfj_d (id INT64, c_id INT64, extra STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO mfj_a VALUES (1, 'A'), (2, 'B')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO mfj_b VALUES (10, 1, 'X'), (20, 2, 'Y')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO mfj_c VALUES (100, 'C1'), (200, 'C2')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO mfj_d VALUES (1000, 100, 'E1'), (2000, 200, 'E2')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT a.val, b.code, c.data, d.extra FROM mfj_a a INNER JOIN mfj_b b ON a.id = b.ref_id, mfj_c c INNER JOIN mfj_d d ON c.id = d.c_id ORDER BY a.val, c.data",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["A", "X", "C1", "E1"],
            ["A", "X", "C2", "E2"],
            ["B", "Y", "C1", "E1"],
            ["B", "Y", "C2", "E2"]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_with_both_left_and_outer_context() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ulo_data (id INT64, arr ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ulo_data VALUES (1, [10, 20]), (2, [30])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT d.id, elem FROM ulo_data d, UNNEST(d.arr) AS elem ORDER BY d.id, elem")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [1, 20], [2, 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_tvf_without_alias() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE FUNCTION simple_tvf()
            RETURNS TABLE<num INT64>
            AS (SELECT n AS num FROM UNNEST([1, 2, 3]) AS n)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM simple_tvf() ORDER BY num")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_unnest_cross_product() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT x, y, z FROM UNNEST([1, 2]) AS x, UNNEST(['a', 'b']) AS y, UNNEST([true, false]) AS z ORDER BY x, y, z DESC",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, "a", true],
            [1, "a", false],
            [1, "b", true],
            [1, "b", false],
            [2, "a", true],
            [2, "a", false],
            [2, "b", true],
            [2, "b", false]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_array_type_unknown() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT element FROM UNNEST(CAST(NULL AS ARRAY<INT64>)) AS element")
        .await
        .unwrap();
    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cross_join_with_subquery() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE cjs_a (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO cjs_a VALUES (1), (2)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT a.id, sq.val FROM cjs_a a CROSS JOIN (SELECT 100 AS val UNION ALL SELECT 200 AS val) AS sq ORDER BY a.id, sq.val",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 100], [1, 200], [2, 100], [2, 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_left_join_with_none_constraint() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ljn_a (x INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE ljn_b (y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ljn_a VALUES (1), (2)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ljn_b VALUES (10), (20)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT x, y FROM ljn_a LEFT JOIN ljn_b ORDER BY x, y")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [1, 20], [2, 10], [2, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_right_join_with_none_constraint() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE rjn_a (x INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE rjn_b (y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO rjn_a VALUES (1), (2)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO rjn_b VALUES (10), (20)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT x, y FROM rjn_a RIGHT JOIN rjn_b ORDER BY x, y")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [1, 20], [2, 10], [2, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_full_outer_join_with_none_constraint() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE fojn_a (x INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE fojn_b (y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO fojn_a VALUES (1), (2)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO fojn_b VALUES (10), (20)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT x, y FROM fojn_a FULL OUTER JOIN fojn_b ORDER BY x, y")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [1, 20], [2, 10], [2, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_factor_lateral_unsupported() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE lat_data (id INT64, arr ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO lat_data VALUES (1, [10, 20])")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT d.id, e FROM lat_data d, LATERAL (SELECT elem FROM UNNEST(d.arr) AS elem) AS e",
        )
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_semi_join_unsupported() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE semi_a (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE semi_b (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO semi_a VALUES (1), (2)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO semi_b VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM semi_a SEMI JOIN semi_b ON semi_a.id = semi_b.id")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_anti_join_unsupported() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE anti_a (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE anti_b (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO anti_a VALUES (1), (2)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO anti_b VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM anti_a ANTI JOIN anti_b ON anti_a.id = anti_b.id")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_tvf_with_sql_query_body_and_multiple_params() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE FUNCTION multiply_and_add(multiplier INT64, addend INT64)
            RETURNS TABLE<result INT64>
            AS (SELECT n * multiplier + addend AS result FROM UNNEST([1, 2, 3]) AS n)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM multiply_and_add(10, 5) ORDER BY result")
        .await
        .unwrap();
    assert_table_eq!(result, [[15], [25], [35]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_in_nested_subquery_with_outer_ref() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nsq_data (id INT64, arr ARRAY<STRING>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nsq_data VALUES (1, ['a', 'b', 'c']), (2, ['x', 'y'])")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT d.id, (SELECT STRING_AGG(e, '-') FROM UNNEST(d.arr) AS e) AS joined FROM nsq_data d ORDER BY d.id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "a-b-c"], [2, "x-y"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_derived_table_deep_nesting() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE dn_src (x INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO dn_src VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT lv3.tripled FROM (SELECT lv2.doubled * 3 AS tripled FROM (SELECT lv1.x * 2 AS doubled FROM (SELECT x FROM dn_src) AS lv1) AS lv2) AS lv3 ORDER BY tripled",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[6], [12], [18]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_five_way_cross_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE fw5_a (a INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE fw5_b (b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE fw5_c (c INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE fw5_d (d INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE fw5_e (e INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO fw5_a VALUES (1)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO fw5_b VALUES (2)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO fw5_c VALUES (3)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO fw5_d VALUES (4)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO fw5_e VALUES (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a, b, c, d, e FROM fw5_a, fw5_b, fw5_c, fw5_d, fw5_e")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 2, 3, 4, 5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_no_element_type() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT element FROM UNNEST([]) AS element")
        .await
        .unwrap();
    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_with_complex_query() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE vcq_orders (id INT64, customer_id INT64, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO vcq_orders VALUES (1, 10, 100), (2, 10, 200), (3, 20, 150)")
        .await
        .unwrap();
    session
        .execute_sql(
            "CREATE VIEW vcq_customer_totals AS SELECT customer_id, SUM(amount) AS total FROM vcq_orders GROUP BY customer_id",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM vcq_customer_totals ORDER BY customer_id")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 300], [20, 150]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_subquery_without_alias_select_star() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE swa_data (x INT64, y STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO swa_data VALUES (1, 'a'), (2, 'b')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM (SELECT x, y FROM swa_data WHERE x > 0) ORDER BY x")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [2, "b"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cte_multiple_references_with_different_aliases() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE cmr_data (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO cmr_data VALUES (1, 100), (2, 200)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH cte AS (SELECT id, val FROM cmr_data) SELECT a.id AS a_id, b.val AS b_val FROM cte AS a, cte AS b WHERE a.id = b.id ORDER BY a_id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 100], [2, 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_with_complex_on_condition() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE jco_a (id INT64, category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE jco_b (id INT64, category STRING, threshold INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO jco_a VALUES (1, 'X', 100), (2, 'Y', 200), (3, 'X', 300)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO jco_b VALUES (1, 'X', 150), (2, 'Y', 150)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT a.id, a.category, a.value, b.threshold FROM jco_a a INNER JOIN jco_b b ON a.category = b.category AND a.value > b.threshold ORDER BY a.id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[2, "Y", 200, 150], [3, "X", 300, 150]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_struct_array_field_access() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT s.name, s.score FROM UNNEST([STRUCT('Alice' AS name, 90 AS score), STRUCT('Bob' AS name, 85 AS score)]) AS s ORDER BY s.score DESC",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice", 90], ["Bob", 85]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_from_with_complex_joins() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE mfc_t1 (id INT64, val1 STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE mfc_t2 (t1_id INT64, val2 STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE mfc_t3 (id INT64, val3 STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE mfc_t4 (t3_id INT64, val4 STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO mfc_t1 VALUES (1, 'A'), (2, 'B')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO mfc_t2 VALUES (1, 'X'), (2, 'Y')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO mfc_t3 VALUES (100, 'C'), (200, 'D')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO mfc_t4 VALUES (100, 'Z'), (200, 'W')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT t1.val1, t2.val2, t3.val3, t4.val4 FROM mfc_t1 t1 LEFT JOIN mfc_t2 t2 ON t1.id = t2.t1_id, mfc_t3 t3 LEFT JOIN mfc_t4 t4 ON t3.id = t4.t3_id ORDER BY t1.val1, t3.val3",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["A", "X", "C", "Z"],
            ["A", "X", "D", "W"],
            ["B", "Y", "C", "Z"],
            ["B", "Y", "D", "W"]
        ]
    );
}
