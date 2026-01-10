use yachtsql::{RecordBatchVecExt, YachtSQLSession};

use crate::assert_table_eq;
use crate::common::create_session;

async fn setup_tables(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING, dept_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE departments (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 1), (4, 'Diana', NULL)").await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'Marketing')",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_inner_join() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql("SELECT u.name, d.name FROM users u INNER JOIN departments d ON u.dept_id = d.id ORDER BY u.name").await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", "Engineering"],
            ["Bob", "Sales"],
            ["Charlie", "Engineering"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_left_join() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql("SELECT u.name, d.name FROM users u LEFT JOIN departments d ON u.dept_id = d.id ORDER BY u.name").await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", "Engineering"],
            ["Bob", "Sales"],
            ["Charlie", "Engineering"],
            ["Diana", null],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_full_outer_join() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql("SELECT u.name, d.name FROM users u FULL OUTER JOIN departments d ON u.dept_id = d.id ORDER BY u.name NULLS LAST, d.name NULLS LAST").await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", "Engineering"],
            ["Bob", "Sales"],
            ["Charlie", "Engineering"],
            ["Diana", null],
            [null, "Marketing"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_cross_join() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE a (x INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE b (y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO a VALUES (1), (2)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO b VALUES (10), (20)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT x, y FROM a CROSS JOIN b ORDER BY x, y")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 10], [1, 20], [2, 10], [2, 20],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_self_join() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE employees (id INT64, name STRING, manager_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO employees VALUES (1, 'Alice', NULL), (2, 'Bob', 1), (3, 'Charlie', 1)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT e.name, m.name FROM employees e LEFT JOIN employees m ON e.manager_id = m.id ORDER BY e.name").await
        .unwrap();

    assert_table_eq!(
        result,
        [["Alice", null], ["Bob", "Alice"], ["Charlie", "Alice"],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_self_join_with_inequality() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE nums (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nums VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a.id AS a_id, b.id AS b_id FROM nums a JOIN nums b ON a.id < b.id ORDER BY a_id, b_id").await
        .unwrap();

    assert_table_eq!(result, [[1, 2], [1, 3], [2, 3],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cte_self_join_with_inequality() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE nums (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nums VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("WITH nums_cte AS (SELECT id, val FROM nums) SELECT a.id AS a_id, b.id AS b_id FROM nums_cte a JOIN nums_cte b ON a.id < b.id ORDER BY a_id, b_id").await
        .unwrap();

    assert_table_eq!(result, [[1, 2], [1, 3], [2, 3],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cte_self_join_with_compound_condition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE order_items (order_id INT64, product_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO order_items VALUES (1, 100), (1, 200), (1, 300), (2, 100), (2, 400)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("WITH order_products AS (SELECT DISTINCT order_id, product_id FROM order_items) SELECT op1.order_id, op1.product_id AS p1, op2.product_id AS p2 FROM order_products op1 JOIN order_products op2 ON op1.order_id = op2.order_id AND op1.product_id < op2.product_id ORDER BY op1.order_id, p1, p2").await
        .unwrap();

    assert_table_eq!(
        result,
        [[1, 100, 200], [1, 100, 300], [1, 200, 300], [2, 100, 400],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_cte_self_join_with_additional_joins() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE oi (order_id INT64, product_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE prods (product_id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO oi VALUES (1, 100), (1, 200), (1, 300), (2, 100), (2, 400)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO prods VALUES (100, 'A'), (200, 'B'), (300, 'C'), (400, 'D')")
        .await
        .unwrap();

    let result = session
        .execute_sql("WITH order_products AS (SELECT DISTINCT order_id, product_id FROM oi) SELECT p1.name AS product_1, p2.name AS product_2, COUNT(*) AS times_bought_together FROM order_products op1 JOIN order_products op2 ON op1.order_id = op2.order_id AND op1.product_id < op2.product_id JOIN prods p1 ON op1.product_id = p1.product_id JOIN prods p2 ON op2.product_id = p2.product_id GROUP BY p1.name, p2.name ORDER BY product_1, product_2").await
        .unwrap();

    assert_table_eq!(
        result,
        [["A", "B", 1], ["A", "C", 1], ["A", "D", 1], ["B", "C", 1],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_cte_self_join_with_having() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE oi2 (order_id INT64, product_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE prods2 (product_id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO oi2 VALUES (1, 100), (1, 200), (1, 300), (2, 100), (2, 400)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO prods2 VALUES (100, 'A'), (200, 'B'), (300, 'C'), (400, 'D')")
        .await
        .unwrap();

    let result = session
        .execute_sql("WITH order_products AS (SELECT DISTINCT order_id, product_id FROM oi2) SELECT p1.name AS product_1, p2.name AS product_2, COUNT(*) AS times_bought_together FROM order_products op1 JOIN order_products op2 ON op1.order_id = op2.order_id AND op1.product_id < op2.product_id JOIN prods2 p1 ON op1.product_id = p1.product_id JOIN prods2 p2 ON op2.product_id = p2.product_id GROUP BY p1.name, p2.name HAVING COUNT(*) >= 1 ORDER BY times_bought_together DESC, product_1, product_2").await
        .unwrap();

    assert_table_eq!(
        result,
        [["A", "B", 1], ["A", "C", 1], ["A", "D", 1], ["B", "C", 1],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_anti_join_pattern() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE users3 (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE orders3 (id INT64, user_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO users3 VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO orders3 VALUES (100, 1), (101, 1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT u.id, u.name FROM users3 u LEFT JOIN orders3 o ON u.id = o.user_id WHERE o.id IS NULL ORDER BY u.id").await
        .unwrap();

    assert_table_eq!(result, [[2, "Bob"], [3, "Charlie"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cross_join_anti_pattern() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE customers4 (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE customer_prefs (customer_id INT64, category STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE all_cats (category STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO customers4 VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO customer_prefs VALUES (1, 'Electronics'), (1, 'Books')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO all_cats VALUES ('Electronics'), ('Books'), ('Clothing')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT c.id, c.name, ac.category AS missing
             FROM customers4 c
             CROSS JOIN all_cats ac
             LEFT JOIN customer_prefs cp ON c.id = cp.customer_id AND ac.category = cp.category
             WHERE cp.customer_id IS NULL
             ORDER BY c.id, missing",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, "Alice", "Clothing"],
            [2, "Bob", "Books"],
            [2, "Bob", "Clothing"],
            [2, "Bob", "Electronics"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_cross_join_anti_pattern_with_ctes() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE customers5 (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE customer_prefs5 (customer_id INT64, category STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE all_cats5 (category STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO customers5 VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO customer_prefs5 VALUES (1, 'Electronics'), (1, 'Books')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO all_cats5 VALUES ('Electronics'), ('Books'), ('Clothing')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH prefs AS (SELECT DISTINCT customer_id, category FROM customer_prefs5),
                  cats AS (SELECT DISTINCT category FROM all_cats5)
             SELECT c.id, c.name, ac.category AS missing
             FROM customers5 c
             CROSS JOIN cats ac
             LEFT JOIN prefs cp ON c.id = cp.customer_id AND ac.category = cp.category
             WHERE cp.customer_id IS NULL
             ORDER BY c.id, missing",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, "Alice", "Clothing"],
            [2, "Bob", "Books"],
            [2, "Bob", "Clothing"],
            [2, "Bob", "Electronics"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_cross_sell_pattern() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE cust6 (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE cust_cats6 (customer_id INT64, category STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE all_cats6 (category STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO cust6 VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO cust_cats6 VALUES (1, 'Electronics'), (1, 'Books')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO all_cats6 VALUES ('Electronics'), ('Books'), ('Clothing')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH customer_categories AS (SELECT DISTINCT customer_id, category FROM cust_cats6),
                  all_categories AS (SELECT DISTINCT category FROM all_cats6)
             SELECT
                c.id,
                c.name,
                STRING_AGG(cc.category, ', ') AS purchased,
                STRING_AGG(ac.category, ', ') AS missing
             FROM cust6 c
             LEFT JOIN customer_categories cc ON c.id = cc.customer_id
             CROSS JOIN all_categories ac
             LEFT JOIN customer_categories cc2 ON c.id = cc2.customer_id AND ac.category = cc2.category
             WHERE cc2.customer_id IS NULL
             GROUP BY c.id, c.name
             HAVING COUNT(DISTINCT cc.category) > 0
             ORDER BY c.id"
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [[1, "Alice", "Electronics, Books", "Clothing, Clothing"],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_cte_used_twice() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE data7 (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data7 VALUES (1, 'A'), (2, 'B'), (3, 'C')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH cte AS (SELECT id, val FROM data7)
             SELECT a.id AS a_id, a.val AS a_val, b.id AS b_id, b.val AS b_val
             FROM cte a
             JOIN cte b ON a.id = b.id
             ORDER BY a.id",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [[1, "A", 1, "A"], [2, "B", 2, "B"], [3, "C", 3, "C"],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_left_joins_to_same_cte() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE base8 (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE lookup8 (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO base8 VALUES (1), (2)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO lookup8 VALUES (1, 'A'), (2, 'B')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH lkp AS (SELECT id, val FROM lookup8)
             SELECT b.id, a.val AS a_val, c.val AS c_val
             FROM base8 b
             LEFT JOIN lkp a ON b.id = a.id
             LEFT JOIN lkp c ON b.id = c.id
             ORDER BY b.id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "A", "A"], [2, "B", "B"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cross_sell_pattern_no_group() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE cust9 (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE cust_cats9 (customer_id INT64, category STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE all_cats9 (category STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO cust9 VALUES (1, 'Alice')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO cust_cats9 VALUES (1, 'Electronics'), (1, 'Books')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO all_cats9 VALUES ('Electronics'), ('Books'), ('Clothing')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH customer_categories AS (SELECT DISTINCT customer_id, category FROM cust_cats9),
                  all_categories AS (SELECT DISTINCT category FROM all_cats9)
             SELECT c.id, cc.category AS purchased, ac.category AS all_cat, cc2.category AS check_cat
             FROM cust9 c
             LEFT JOIN customer_categories cc ON c.id = cc.customer_id
             CROSS JOIN all_categories ac
             LEFT JOIN customer_categories cc2 ON c.id = cc2.customer_id AND ac.category = cc2.category
             ORDER BY c.id, purchased, all_cat"
        ).await
        .unwrap();

    println!("Result row count: {}", result.row_count());
    for r in result.to_records().unwrap() {
        println!("  {:?}", r.values());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn test_cross_sell_pattern_with_where() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE cust10 (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE cust_cats10 (customer_id INT64, category STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE all_cats10 (category STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO cust10 VALUES (1, 'Alice')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO cust_cats10 VALUES (1, 'Electronics'), (1, 'Books')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO all_cats10 VALUES ('Electronics'), ('Books'), ('Clothing')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH customer_categories AS (SELECT DISTINCT customer_id, category FROM cust_cats10),
                  all_categories AS (SELECT DISTINCT category FROM all_cats10)
             SELECT c.id, cc.category AS purchased, ac.category AS missing_cat
             FROM cust10 c
             LEFT JOIN customer_categories cc ON c.id = cc.customer_id
             CROSS JOIN all_categories ac
             LEFT JOIN customer_categories cc2 ON c.id = cc2.customer_id AND ac.category = cc2.category
             WHERE cc2.customer_id IS NULL
             ORDER BY c.id, purchased, missing_cat"
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [[1, "Books", "Clothing"], [1, "Electronics", "Clothing"],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_cross_sell_pattern_with_group() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE cust11 (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE cust_cats11 (customer_id INT64, category STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE all_cats11 (category STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO cust11 VALUES (1, 'Alice')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO cust_cats11 VALUES (1, 'Electronics'), (1, 'Books')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO all_cats11 VALUES ('Electronics'), ('Books'), ('Clothing')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH customer_categories AS (SELECT DISTINCT customer_id, category FROM cust_cats11),
                  all_categories AS (SELECT DISTINCT category FROM all_cats11)
             SELECT
                c.id,
                c.name,
                STRING_AGG(cc.category, ', ') AS purchased,
                STRING_AGG(ac.category, ', ') AS missing
             FROM cust11 c
             LEFT JOIN customer_categories cc ON c.id = cc.customer_id
             CROSS JOIN all_categories ac
             LEFT JOIN customer_categories cc2 ON c.id = cc2.customer_id AND ac.category = cc2.category
             WHERE cc2.customer_id IS NULL
             GROUP BY c.id, c.name
             ORDER BY c.id"
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [[1, "Alice", "Electronics, Books", "Clothing, Clothing"],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_cross_sell_pattern_with_having() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE cust12 (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE cust_cats12 (customer_id INT64, category STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE all_cats12 (category STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO cust12 VALUES (1, 'Alice')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO cust_cats12 VALUES (1, 'Electronics'), (1, 'Books')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO all_cats12 VALUES ('Electronics'), ('Books'), ('Clothing')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH customer_categories AS (SELECT DISTINCT customer_id, category FROM cust_cats12),
                  all_categories AS (SELECT DISTINCT category FROM all_cats12)
             SELECT
                c.id,
                c.name,
                STRING_AGG(cc.category, ', ') AS purchased,
                STRING_AGG(ac.category, ', ') AS missing
             FROM cust12 c
             LEFT JOIN customer_categories cc ON c.id = cc.customer_id
             CROSS JOIN all_categories ac
             LEFT JOIN customer_categories cc2 ON c.id = cc2.customer_id AND ac.category = cc2.category
             WHERE cc2.customer_id IS NULL
             GROUP BY c.id, c.name
             HAVING COUNT(DISTINCT cc.category) > 0
             ORDER BY c.id"
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [[1, "Alice", "Electronics, Books", "Clothing, Clothing"],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_with_where_clause() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql("SELECT u.name, d.name FROM users u INNER JOIN departments d ON u.dept_id = d.id WHERE d.name = 'Engineering' ORDER BY u.name").await
        .unwrap();

    assert_table_eq!(
        result,
        [["Alice", "Engineering"], ["Charlie", "Engineering"],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_multiple_conditions() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE orders (id INT64, user_id INT64, product_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE products (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 101), (3, 2, 100)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO products VALUES (100, 'Widget'), (101, 'Gadget')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT o.id, p.name FROM orders o INNER JOIN products p ON o.product_id = p.id WHERE o.user_id = 1 ORDER BY o.id").await
        .unwrap();

    assert_table_eq!(result, [[1, "Widget"], [2, "Gadget"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_three_tables() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE customers (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE orders (id INT64, customer_id INT64, product_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE products (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO orders VALUES (1, 1, 100), (2, 2, 101)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO products VALUES (100, 'Widget'), (101, 'Gadget')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT c.name, p.name FROM customers c INNER JOIN orders o ON c.id = o.customer_id INNER JOIN products p ON o.product_id = p.id ORDER BY c.name").await
        .unwrap();

    assert_table_eq!(result, [["Alice", "Widget"], ["Bob", "Gadget"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_hash_join_null_keys_do_not_match() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t1 (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE t2 (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t1 VALUES (1, 'A'), (NULL, 'B'), (3, 'C')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t2 VALUES (1, 'X'), (NULL, 'Y'), (3, 'Z')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT t1.val, t2.val FROM t1 INNER JOIN t2 ON t1.id = t2.id ORDER BY t1.val")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", "X"], ["C", "Z"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_hash_join_multiple_keys() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE sales (region STRING, product STRING, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE targets (region STRING, product STRING, target INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO sales VALUES ('East', 'Widget', 100), ('West', 'Widget', 150), ('East', 'Gadget', 200)",
        ).await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO targets VALUES ('East', 'Widget', 90), ('West', 'Widget', 140), ('East', 'Gadget', 180)",
        ).await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT s.region, s.product, s.amount, t.target
             FROM sales s
             INNER JOIN targets t ON s.region = t.region AND s.product = t.product
             ORDER BY s.region, s.product",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", "Gadget", 200, 180],
            ["East", "Widget", 100, 90],
            ["West", "Widget", 150, 140],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_hash_join_duplicate_keys() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE orders_dup (id INT64, customer_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE customers_dup (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO orders_dup VALUES (1, 1), (2, 1), (3, 2)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO customers_dup VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT o.id, c.name
             FROM orders_dup o
             INNER JOIN customers_dup c ON o.customer_id = c.id
             ORDER BY o.id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "Alice"], [2, "Alice"], [3, "Bob"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_hash_join_many_to_many() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE left_many (key INT64, left_val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE right_many (key INT64, right_val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO left_many VALUES (1, 'L1'), (1, 'L2'), (2, 'L3')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO right_many VALUES (1, 'R1'), (1, 'R2'), (2, 'R3')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.left_val, r.right_val
             FROM left_many l
             INNER JOIN right_many r ON l.key = r.key
             ORDER BY l.left_val, r.right_val",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["L1", "R1"],
            ["L1", "R2"],
            ["L2", "R1"],
            ["L2", "R2"],
            ["L3", "R3"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_hash_join_cte_to_cte() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE events (user_id INT64, event_type STRING, event_date DATE)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO events VALUES
             (1, 'login', '2024-01-01'),
             (1, 'purchase', '2024-01-02'),
             (2, 'login', '2024-01-01'),
             (2, 'login', '2024-01-03'),
             (3, 'login', '2024-01-05')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH first_login AS (
                SELECT user_id, MIN(event_date) AS first_date
                FROM events
                WHERE event_type = 'login'
                GROUP BY user_id
             ),
             purchases AS (
                SELECT user_id, event_date AS purchase_date
                FROM events
                WHERE event_type = 'purchase'
             )
             SELECT fl.user_id, CAST(fl.first_date AS STRING), CAST(p.purchase_date AS STRING)
             FROM first_login fl
             INNER JOIN purchases p ON fl.user_id = p.user_id
             ORDER BY fl.user_id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "2024-01-01", "2024-01-02"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_hash_join_cte_referencing_another_cte() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE logins (user_id INT64, login_date DATE)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO logins VALUES
             (1, '2024-01-01'),
             (1, '2024-01-08'),
             (1, '2024-01-15'),
             (2, '2024-01-01'),
             (2, '2024-01-10')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH first_login AS (
                SELECT user_id, MIN(login_date) AS first_date
                FROM logins
                GROUP BY user_id
             ),
             weekly_activity AS (
                SELECT l.user_id, f.first_date, l.login_date
                FROM logins l
                INNER JOIN first_login f ON l.user_id = f.user_id
             )
             SELECT user_id, COUNT(*) AS login_count
             FROM weekly_activity
             GROUP BY user_id
             ORDER BY user_id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 3], [2, 2],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_hash_join_large_dataset() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE large_left (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE large_right (id INT64, val INT64)")
        .await
        .unwrap();

    for i in (0..100).step_by(10) {
        let values: Vec<String> = (i..i + 10)
            .map(|x| format!("({}, {})", x, x * 10))
            .collect();
        session
            .execute_sql(&format!(
                "INSERT INTO large_left VALUES {}",
                values.join(", ")
            ))
            .await
            .unwrap();
        session
            .execute_sql(&format!(
                "INSERT INTO large_right VALUES {}",
                values.join(", ")
            ))
            .await
            .unwrap();
    }

    let result = session
        .execute_sql(
            "SELECT COUNT(*) AS cnt, SUM(l.val) AS left_sum, SUM(r.val) AS right_sum
             FROM large_left l
             INNER JOIN large_right r ON l.id = r.id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[100, 49500, 49500],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_hash_join_string_keys() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE products_str (code STRING, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE inventory_str (code STRING, quantity INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO products_str VALUES ('ABC', 'Widget'), ('DEF', 'Gadget'), ('GHI', 'Gizmo')",
        ).await
        .unwrap();
    session
        .execute_sql("INSERT INTO inventory_str VALUES ('ABC', 100), ('DEF', 50), ('XYZ', 200)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT p.name, i.quantity
             FROM products_str p
             INNER JOIN inventory_str i ON p.code = i.code
             ORDER BY p.name",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Gadget", 50], ["Widget", 100],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_hash_join_float_keys() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE measurements (reading FLOAT64, label STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE thresholds (threshold FLOAT64, status STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO measurements VALUES (1.5, 'A'), (2.5, 'B'), (3.5, 'C')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO thresholds VALUES (1.5, 'Low'), (2.5, 'Medium'), (3.5, 'High')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT m.label, t.status
             FROM measurements m
             INNER JOIN thresholds t ON m.reading = t.threshold
             ORDER BY m.label",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["A", "Low"], ["B", "Medium"], ["C", "High"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_hash_join_no_matches() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE set_a (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE set_b (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO set_a VALUES (1), (2), (3)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO set_b VALUES (4), (5), (6)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a.id, b.id FROM set_a a INNER JOIN set_b b ON a.id = b.id")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn test_hash_join_preserves_column_order() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t_left (a INT64, b STRING, c FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE t_right (x INT64, y STRING, z FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t_left VALUES (1, 'hello', 1.5)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t_right VALUES (1, 'world', 2.5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM t_left INNER JOIN t_right ON t_left.a = t_right.x")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "hello", 1.5, 1, "world", 2.5],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_right_join() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql("SELECT u.name, d.name AS dept_name FROM users u RIGHT JOIN departments d ON u.dept_id = d.id ORDER BY dept_name, u.name NULLS LAST").await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", "Engineering"],
            ["Charlie", "Engineering"],
            [null, "Marketing"],
            ["Bob", "Sales"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_right_join_no_matches() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE left_r1 (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE right_r1 (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO left_r1 VALUES (1, 'A'), (2, 'B')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO right_r1 VALUES (10, 'X'), (20, 'Y')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT l.val, r.val FROM left_r1 l RIGHT JOIN right_r1 r ON l.id = r.id ORDER BY r.val")
        .await
        .unwrap();

    assert_table_eq!(result, [[null, "X"], [null, "Y"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_empty_left_table() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE empty_left (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE nonempty_right (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nonempty_right VALUES (1, 'A'), (2, 'B')")
        .await
        .unwrap();

    let inner_result = session
        .execute_sql(
            "SELECT l.val, r.val FROM empty_left l INNER JOIN nonempty_right r ON l.id = r.id",
        )
        .await
        .unwrap();
    assert_eq!(inner_result.row_count(), 0);

    let left_result = session
        .execute_sql(
            "SELECT l.val, r.val FROM empty_left l LEFT JOIN nonempty_right r ON l.id = r.id",
        )
        .await
        .unwrap();
    assert_eq!(left_result.row_count(), 0);

    let right_result = session
        .execute_sql("SELECT l.val, r.val FROM empty_left l RIGHT JOIN nonempty_right r ON l.id = r.id ORDER BY r.val")
        .await
        .unwrap();
    assert_table_eq!(right_result, [[null, "A"], [null, "B"],]);

    let full_result = session
        .execute_sql("SELECT l.val, r.val FROM empty_left l FULL OUTER JOIN nonempty_right r ON l.id = r.id ORDER BY r.val")
        .await
        .unwrap();
    assert_table_eq!(full_result, [[null, "A"], [null, "B"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_empty_right_table() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE nonempty_left (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE empty_right (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nonempty_left VALUES (1, 'A'), (2, 'B')")
        .await
        .unwrap();

    let inner_result = session
        .execute_sql(
            "SELECT l.val, r.val FROM nonempty_left l INNER JOIN empty_right r ON l.id = r.id",
        )
        .await
        .unwrap();
    assert_eq!(inner_result.row_count(), 0);

    let left_result = session
        .execute_sql("SELECT l.val, r.val FROM nonempty_left l LEFT JOIN empty_right r ON l.id = r.id ORDER BY l.val")
        .await
        .unwrap();
    assert_table_eq!(left_result, [["A", null], ["B", null],]);

    let right_result = session
        .execute_sql(
            "SELECT l.val, r.val FROM nonempty_left l RIGHT JOIN empty_right r ON l.id = r.id",
        )
        .await
        .unwrap();
    assert_eq!(right_result.row_count(), 0);

    let full_result = session
        .execute_sql("SELECT l.val, r.val FROM nonempty_left l FULL OUTER JOIN empty_right r ON l.id = r.id ORDER BY l.val")
        .await
        .unwrap();
    assert_table_eq!(full_result, [["A", null], ["B", null],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_both_empty_tables() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE empty_t1 (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE empty_t2 (id INT64, val STRING)")
        .await
        .unwrap();

    let inner_result = session
        .execute_sql("SELECT * FROM empty_t1 INNER JOIN empty_t2 ON empty_t1.id = empty_t2.id")
        .await
        .unwrap();
    assert_eq!(inner_result.row_count(), 0);

    let left_result = session
        .execute_sql("SELECT * FROM empty_t1 LEFT JOIN empty_t2 ON empty_t1.id = empty_t2.id")
        .await
        .unwrap();
    assert_eq!(left_result.row_count(), 0);

    let right_result = session
        .execute_sql("SELECT * FROM empty_t1 RIGHT JOIN empty_t2 ON empty_t1.id = empty_t2.id")
        .await
        .unwrap();
    assert_eq!(right_result.row_count(), 0);

    let full_result = session
        .execute_sql("SELECT * FROM empty_t1 FULL OUTER JOIN empty_t2 ON empty_t1.id = empty_t2.id")
        .await
        .unwrap();
    assert_eq!(full_result.row_count(), 0);

    let cross_result = session
        .execute_sql("SELECT * FROM empty_t1 CROSS JOIN empty_t2")
        .await
        .unwrap();
    assert_eq!(cross_result.row_count(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_date_keys() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE events_date (event_date DATE, event_name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE holidays (holiday_date DATE, holiday_name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO events_date VALUES ('2024-01-01', 'NewYear'), ('2024-12-25', 'Xmas'), ('2024-07-04', 'July4')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO holidays VALUES ('2024-01-01', 'New Year Day'), ('2024-12-25', 'Christmas')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT e.event_name, h.holiday_name
             FROM events_date e
             INNER JOIN holidays h ON e.event_date = h.holiday_date
             ORDER BY e.event_name",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["NewYear", "New Year Day"], ["Xmas", "Christmas"],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_timestamp_keys() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE log_entries (ts TIMESTAMP, message STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE alerts (alert_ts TIMESTAMP, alert_type STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO log_entries VALUES (TIMESTAMP '2024-01-15 10:30:00 UTC', 'Error'), (TIMESTAMP '2024-01-15 11:00:00 UTC', 'Info')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO alerts VALUES (TIMESTAMP '2024-01-15 10:30:00 UTC', 'Critical')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.message, a.alert_type
             FROM log_entries l
             INNER JOIN alerts a ON l.ts = a.alert_ts",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Error", "Critical"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_datetime_keys() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE bookings (booking_dt DATETIME, room STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE special_rates (rate_dt DATETIME, rate FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bookings VALUES (DATETIME '2024-06-01 14:00:00', 'Room A'), (DATETIME '2024-06-02 10:00:00', 'Room B')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO special_rates VALUES (DATETIME '2024-06-01 14:00:00', 99.99)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT b.room, r.rate
             FROM bookings b
             INNER JOIN special_rates r ON b.booking_dt = r.rate_dt",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Room A", 99.99],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_time_keys() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE schedule (slot_time TIME, activity STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE breaks (break_time TIME, break_type STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO schedule VALUES (TIME '09:00:00', 'Meeting'), (TIME '12:00:00', 'Lunch'), (TIME '15:00:00', 'Review')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO breaks VALUES (TIME '12:00:00', 'Lunch Break'), (TIME '15:00:00', 'Coffee Break')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT s.activity, b.break_type
             FROM schedule s
             INNER JOIN breaks b ON s.slot_time = b.break_time
             ORDER BY s.activity",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Lunch", "Lunch Break"], ["Review", "Coffee Break"],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_boolean_keys() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE items (id INT64, is_active BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE status_labels (is_active BOOL, label STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, true), (2, false), (3, true), (4, NULL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO status_labels VALUES (true, 'Active'), (false, 'Inactive')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT i.id, s.label
             FROM items i
             INNER JOIN status_labels s ON i.is_active = s.is_active
             ORDER BY i.id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "Active"], [2, "Inactive"], [3, "Active"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_bytes_keys() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE data_chunks (chunk_hash BYTES, content STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE checksums (checksum BYTES, verified BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data_chunks VALUES (b'abc', 'Content A'), (b'def', 'Content B')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO checksums VALUES (b'abc', true), (b'xyz', false)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT d.content, c.verified
             FROM data_chunks d
             INNER JOIN checksums c ON d.chunk_hash = c.checksum",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Content A", true],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_numeric_keys() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE prices (price NUMERIC, item STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE discounts (threshold NUMERIC, discount_pct FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO prices VALUES (NUMERIC '99.99', 'Widget'), (NUMERIC '149.99', 'Gadget'), (NUMERIC '199.99', 'Device')")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO discounts VALUES (NUMERIC '99.99', 5.0), (NUMERIC '199.99', 15.0)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT p.item, d.discount_pct
             FROM prices p
             INNER JOIN discounts d ON p.price = d.threshold
             ORDER BY p.item",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Device", 15.0], ["Widget", 5.0],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_full_outer_join_no_matches() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE set_left (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE set_right (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO set_left VALUES (1, 'A'), (2, 'B')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO set_right VALUES (10, 'X'), (20, 'Y')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.id, l.val, r.id, r.val
             FROM set_left l
             FULL OUTER JOIN set_right r ON l.id = r.id
             ORDER BY l.id NULLS LAST, r.id NULLS LAST",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, "A", null, null],
            [2, "B", null, null],
            [null, null, 10, "X"],
            [null, null, 20, "Y"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_full_outer_join_partial_matches() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE partial_left (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE partial_right (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO partial_left VALUES (1, 'A'), (2, 'B'), (3, 'C')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO partial_right VALUES (2, 'X'), (3, 'Y'), (4, 'Z')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.id, l.val, r.id, r.val
             FROM partial_left l
             FULL OUTER JOIN partial_right r ON l.id = r.id
             ORDER BY COALESCE(l.id, r.id)",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, "A", null, null],
            [2, "B", 2, "X"],
            [3, "C", 3, "Y"],
            [null, null, 4, "Z"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_with_or_condition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE or_left (id INT64, alt_id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE or_right (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO or_left VALUES (1, 10, 'A'), (2, 20, 'B'), (3, 30, 'C')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO or_right VALUES (1, 'X'), (20, 'Y'), (100, 'Z')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.val, r.val
             FROM or_left l
             INNER JOIN or_right r ON l.id = r.id OR l.alt_id = r.id
             ORDER BY l.val",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["A", "X"], ["B", "Y"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_with_and_condition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE and_left (id INT64, category STRING, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE and_right (id INT64, category STRING, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO and_left VALUES (1, 'A', 'L1'), (1, 'B', 'L2'), (2, 'A', 'L3')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO and_right VALUES (1, 'A', 'R1'), (1, 'C', 'R2'), (2, 'A', 'R3')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.val, r.val
             FROM and_left l
             INNER JOIN and_right r ON l.id = r.id AND l.category = r.category
             ORDER BY l.val",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["L1", "R1"], ["L3", "R3"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_with_expression_condition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE expr_left (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE expr_right (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO expr_left VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO expr_right VALUES (2, 5), (4, 10), (6, 15)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.id, l.val, r.id, r.val
             FROM expr_left l
             INNER JOIN expr_right r ON l.id * 2 = r.id
             ORDER BY l.id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 10, 2, 5], [2, 20, 4, 10], [3, 30, 6, 15],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_inequality_condition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE ineq_left (id INT64, min_val INT64, max_val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE ineq_right (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ineq_left VALUES (1, 0, 10), (2, 10, 20)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ineq_right VALUES (100, 5), (101, 15), (102, 25)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.id, r.id, r.val
             FROM ineq_left l
             INNER JOIN ineq_right r ON r.val >= l.min_val AND r.val < l.max_val
             ORDER BY l.id, r.id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 100, 5], [2, 101, 15],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cross_join_with_where() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE cross_left (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE cross_right (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO cross_left VALUES (1, 'A'), (2, 'B')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO cross_right VALUES (1, 'X'), (2, 'Y'), (3, 'Z')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.val, r.val
             FROM cross_left l
             CROSS JOIN cross_right r
             WHERE l.id < r.id
             ORDER BY l.val, r.val",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["A", "Y"], ["A", "Z"], ["B", "Z"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_left_join_with_null_condition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE null_left (id INT64, parent_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE null_right (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO null_left VALUES (1, NULL), (2, 1), (3, NULL), (4, 2)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO null_right VALUES (1, 'Parent1'), (2, 'Parent2')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.id, r.name
             FROM null_left l
             LEFT JOIN null_right r ON l.parent_id = r.id
             ORDER BY l.id",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [[1, null], [2, "Parent1"], [3, null], [4, "Parent2"],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_join_types_combined() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t_a (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE t_b (id INT64, a_id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE t_c (id INT64, val STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO t_a VALUES (1, 'A1'), (2, 'A2')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t_b VALUES (10, 1, 'B1'), (20, 1, 'B2'), (30, 3, 'B3')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t_c VALUES (10, 'C1'), (40, 'C4')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT a.val, b.val, c.val
             FROM t_a a
             INNER JOIN t_b b ON a.id = b.a_id
             LEFT JOIN t_c c ON b.id = c.id
             ORDER BY a.val, b.val",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["A1", "B1", "C1"], ["A1", "B2", null],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_four_way_join() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE w1 (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE w2 (id INT64, w1_id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE w3 (id INT64, w2_id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE w4 (id INT64, w3_id INT64, val STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO w1 VALUES (1, 'W1')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO w2 VALUES (10, 1, 'W2')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO w3 VALUES (100, 10, 'W3')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO w4 VALUES (1000, 100, 'W4')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT w1.val, w2.val, w3.val, w4.val
             FROM w1
             INNER JOIN w2 ON w1.id = w2.w1_id
             INNER JOIN w3 ON w2.id = w3.w2_id
             INNER JOIN w4 ON w3.id = w4.w3_id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["W1", "W2", "W3", "W4"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_with_subquery() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE main_data (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO main_data VALUES (1, 10), (2, 20), (3, 15), (4, 25)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT m.id, m.val, s.avg_val
             FROM main_data m
             INNER JOIN (SELECT AVG(val) as avg_val FROM main_data) s ON m.val > s.avg_val
             ORDER BY m.id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[2, 20, 17.5], [4, 25, 17.5],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_inner_join_on_single_key() {
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
        .execute_sql("INSERT INTO using_left VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO using_right VALUES (1, 100), (3, 300)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.id, l.name, r.value
             FROM using_left l
             INNER JOIN using_right r ON l.id = r.id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "Alice", 100],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_left_join_equivalent_to_using() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE using_l2 (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE using_r2 (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO using_l2 VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO using_r2 VALUES (1, 100), (3, 300)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.id, l.name, r.value
             FROM using_l2 l
             LEFT JOIN using_r2 r ON l.id = r.id
             ORDER BY l.id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "Alice", 100], [2, "Bob", null],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_with_function_in_condition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE func_left (name STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE func_right (name STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO func_left VALUES ('Alice', 10), ('BOB', 20)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO func_right VALUES ('alice', 100), ('bob', 200)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.name, r.val
             FROM func_left l
             INNER JOIN func_right r ON LOWER(l.name) = LOWER(r.name)
             ORDER BY l.name",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Alice", 100], ["BOB", 200],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_all_nulls_no_match() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE all_null_left (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE all_null_right (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO all_null_left VALUES (NULL, 'A'), (NULL, 'B')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO all_null_right VALUES (NULL, 'X'), (NULL, 'Y')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.val, r.val
             FROM all_null_left l
             INNER JOIN all_null_right r ON l.id = r.id",
        )
        .await
        .unwrap();

    assert_eq!(result.row_count(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cross_join_single_row() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE single_left (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE single_right (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO single_left VALUES ('L')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO single_right VALUES ('R')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT l.val, r.val FROM single_left l CROSS JOIN single_right r")
        .await
        .unwrap();

    assert_table_eq!(result, [["L", "R"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_with_lateral_subquery_alias() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE orders_lat (id INT64, customer_id INT64, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO orders_lat VALUES (1, 100, 50), (2, 100, 75), (3, 200, 100)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT customer_id, total
             FROM (SELECT customer_id, SUM(amount) as total FROM orders_lat GROUP BY customer_id) t
             ORDER BY customer_id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[100, 125], [200, 100],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_right_join_multiple_matches() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE r_multi_left (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE r_multi_right (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO r_multi_left VALUES (1, 'A'), (1, 'B'), (2, 'C')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO r_multi_right VALUES (1, 'X'), (3, 'Y')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.val, r.val
             FROM r_multi_left l
             RIGHT JOIN r_multi_right r ON l.id = r.id
             ORDER BY r.val, l.val NULLS LAST",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["A", "X"], ["B", "X"], [null, "Y"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_full_join_multiple_matches_both_sides() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE full_m_left (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE full_m_right (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO full_m_left VALUES (1, 'A'), (1, 'B')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO full_m_right VALUES (1, 'X'), (1, 'Y')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.val, r.val
             FROM full_m_left l
             FULL OUTER JOIN full_m_right r ON l.id = r.id
             ORDER BY l.val, r.val",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["A", "X"], ["A", "Y"], ["B", "X"], ["B", "Y"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_inner_join_without_condition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE no_cond_left (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE no_cond_right (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO no_cond_left VALUES ('A'), ('B')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO no_cond_right VALUES ('X'), ('Y')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.val, r.val
             FROM no_cond_left l
             JOIN no_cond_right r ON TRUE
             ORDER BY l.val, r.val",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["A", "X"], ["A", "Y"], ["B", "X"], ["B", "Y"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_left_join_without_condition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE no_cond_l2 (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE no_cond_r2 (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO no_cond_l2 VALUES ('A')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO no_cond_r2 VALUES ('X'), ('Y')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.val, r.val
             FROM no_cond_l2 l
             LEFT JOIN no_cond_r2 r ON TRUE
             ORDER BY r.val",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["A", "X"], ["A", "Y"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_right_join_without_condition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE no_cond_l3 (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE no_cond_r3 (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO no_cond_l3 VALUES ('A')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO no_cond_r3 VALUES ('X'), ('Y')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.val, r.val
             FROM no_cond_l3 l
             RIGHT JOIN no_cond_r3 r ON TRUE
             ORDER BY r.val",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["A", "X"], ["A", "Y"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_full_join_without_condition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE no_cond_l4 (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE no_cond_r4 (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO no_cond_l4 VALUES ('A')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO no_cond_r4 VALUES ('X'), ('Y')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT l.val, r.val
             FROM no_cond_l4 l
             FULL OUTER JOIN no_cond_r4 r ON TRUE
             ORDER BY l.val, r.val",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["A", "X"], ["A", "Y"],]);
}
