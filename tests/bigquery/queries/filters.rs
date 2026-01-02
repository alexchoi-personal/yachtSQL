use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::create_session;

async fn setup_orders_table(session: &YachtSQLSession) {
    session
        .execute_sql(
            "CREATE TABLE orders (id INT64, customer_id INT64, amount FLOAT64, status STRING)",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO orders VALUES
            (1, 100, 150.0, 'completed'),
            (2, 100, 200.0, 'pending'),
            (3, 101, 75.0, 'completed'),
            (4, 102, 300.0, 'cancelled'),
            (5, 101, 125.0, 'pending')",
        )
        .await
        .unwrap();
}

async fn setup_customers_table(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE customers (id INT64, name STRING, tier STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO customers VALUES
            (100, 'Alice', 'gold'),
            (101, 'Bob', 'silver'),
            (102, 'Charlie', 'bronze')",
        )
        .await
        .unwrap();
}

async fn setup_products_categories(session: &YachtSQLSession) {
    session
        .execute_sql(
            "CREATE TABLE products (id INT64, name STRING, category_id INT64, price FLOAT64)",
        )
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE categories (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO products VALUES
            (1, 'Laptop', 1, 999.0),
            (2, 'Mouse', 1, 25.0),
            (3, 'Desk', 2, 200.0),
            (4, 'Chair', 2, 150.0),
            (5, 'Monitor', 1, 350.0)",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO categories VALUES (1, 'Electronics'), (2, 'Furniture'), (3, 'Books')",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_simple_equality() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM orders WHERE status = 'completed' ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_numeric_comparison() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM orders WHERE amount > 150.0 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_compound_and() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders WHERE customer_id = 100 AND status = 'completed' ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_compound_or() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders WHERE status = 'completed' OR status = 'pending' ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [2], [3], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_complex_boolean() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE (customer_id = 100 OR customer_id = 101) AND amount > 100.0
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [2], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_not_operator() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM orders WHERE NOT status = 'cancelled' ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [2], [3], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_in_list() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM orders WHERE status IN ('completed', 'pending') ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [2], [3], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_not_in_list() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM orders WHERE status NOT IN ('cancelled') ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [2], [3], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_between() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM orders WHERE amount BETWEEN 100.0 AND 200.0 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [2], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_in_subquery_simple() {
    let session = create_session();
    setup_orders_table(&session).await;
    setup_customers_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE customer_id IN (SELECT id FROM customers WHERE tier = 'gold')
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_not_in_subquery() {
    let session = create_session();
    setup_orders_table(&session).await;
    setup_customers_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE customer_id NOT IN (SELECT id FROM customers WHERE tier = 'gold')
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[3], [4], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_exists_subquery() {
    let session = create_session();
    setup_orders_table(&session).await;
    setup_customers_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT name FROM customers c
            WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.status = 'completed')
            ORDER BY name",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_not_exists_subquery() {
    let session = create_session();
    setup_orders_table(&session).await;
    setup_customers_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT name FROM customers c
            WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)
            ORDER BY name",
        )
        .await
        .unwrap();

    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_exists_with_no_matches() {
    let session = create_session();
    setup_orders_table(&session).await;

    session
        .execute_sql("CREATE TABLE empty_table (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE EXISTS (SELECT 1 FROM empty_table)
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_scalar_subquery_comparison() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE amount > (SELECT AVG(amount) FROM orders)
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_scalar_subquery_equals() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE amount = (SELECT MAX(amount) FROM orders)",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_correlated_subquery_count() {
    let session = create_session();
    setup_orders_table(&session).await;
    setup_customers_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT name FROM customers c
            WHERE (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) > 1
            ORDER BY name",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_correlated_subquery_max() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT o1.id FROM orders o1
            WHERE o1.amount = (SELECT MAX(o2.amount) FROM orders o2 WHERE o2.customer_id = o1.customer_id)
            ORDER BY o1.id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[2], [4], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_nested_and_or() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE (status = 'completed' AND amount > 100.0)
               OR (status = 'pending' AND customer_id = 100)
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_triple_and() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE customer_id = 101 AND status = 'pending' AND amount > 100.0
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_mixed_operators() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE customer_id IN (100, 101)
              AND status <> 'cancelled'
              AND amount BETWEEN 100.0 AND 250.0
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [2], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_like_pattern() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM orders WHERE status LIKE 'comp%' ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_not_like_pattern() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM orders WHERE status NOT LIKE '%ed' ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[2], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_is_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nullable_orders (id INT64, discount FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable_orders VALUES (1, 10.0), (2, NULL), (3, 5.0)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM nullable_orders WHERE discount IS NULL")
        .await
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_is_not_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nullable_orders (id INT64, discount FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable_orders VALUES (1, 10.0), (2, NULL), (3, 5.0)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM nullable_orders WHERE discount IS NOT NULL ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_subquery_with_and() {
    let session = create_session();
    setup_orders_table(&session).await;
    setup_customers_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE customer_id IN (SELECT id FROM customers WHERE tier = 'gold')
              AND status = 'pending'
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_subquery_with_or() {
    let session = create_session();
    setup_orders_table(&session).await;
    setup_customers_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE customer_id IN (SELECT id FROM customers WHERE tier = 'gold')
               OR status = 'cancelled'
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_multiple_subqueries() {
    let session = create_session();
    setup_orders_table(&session).await;
    setup_customers_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE customer_id IN (SELECT id FROM customers WHERE tier IN ('gold', 'silver'))
              AND amount > (SELECT AVG(amount) FROM orders)
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_nested_subquery() {
    let session = create_session();
    setup_orders_table(&session).await;
    setup_customers_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE customer_id IN (
                SELECT id FROM customers
                WHERE id IN (SELECT customer_id FROM orders WHERE amount > 200.0)
            )
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_exists_correlated_with_aggregate() {
    let session = create_session();
    setup_orders_table(&session).await;
    setup_customers_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT name FROM customers c
            WHERE EXISTS (
                SELECT 1 FROM orders o
                WHERE o.customer_id = c.id
                GROUP BY o.customer_id
                HAVING SUM(o.amount) > 200.0
            )
            ORDER BY name",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Charlie"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_with_function_in_condition() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM orders WHERE UPPER(status) = 'COMPLETED' ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_with_arithmetic_expression() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM orders WHERE amount * 1.1 > 200.0 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_in_subquery_with_join() {
    let session = create_session();
    setup_products_categories(&session).await;

    let result = session
        .execute_sql(
            "SELECT p.name FROM products p
            WHERE p.category_id IN (
                SELECT c.id FROM categories c WHERE c.name = 'Electronics'
            )
            ORDER BY p.name",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Laptop"], ["Monitor"], ["Mouse"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_exists_categories_with_products() {
    let session = create_session();
    setup_products_categories(&session).await;

    let result = session
        .execute_sql(
            "SELECT c.name FROM categories c
            WHERE EXISTS (SELECT 1 FROM products p WHERE p.category_id = c.id)
            ORDER BY c.name",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics"], ["Furniture"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_not_exists_unused_categories() {
    let session = create_session();
    setup_products_categories(&session).await;

    let result = session
        .execute_sql(
            "SELECT c.name FROM categories c
            WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.category_id = c.id)
            ORDER BY c.name",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Books"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_with_case_when() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE CASE WHEN amount > 200.0 THEN 'high' ELSE 'low' END = 'high'
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_coalesce_in_condition() {
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
        .execute_sql("SELECT id FROM data WHERE COALESCE(val, 0) > 5 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_ifnull_in_condition() {
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
        .execute_sql("SELECT id FROM data WHERE IFNULL(val, 20) = 20 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_subquery_empty_result() {
    let session = create_session();
    setup_orders_table(&session).await;
    setup_customers_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE customer_id IN (SELECT id FROM customers WHERE name = 'NonExistent')
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_scalar_subquery_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE empty_nums (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE test_data (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO test_data VALUES (1, 10), (2, 20)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id FROM test_data
            WHERE val > (SELECT MAX(val) FROM empty_nums)
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_exists_true_simple() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1), (2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT EXISTS(SELECT 1 FROM data) AS has_data")
        .await
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_exists_false_empty() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE empty_data (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT EXISTS(SELECT 1 FROM empty_data) AS has_data")
        .await
        .unwrap();

    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_correlated_with_outer_column_in_function() {
    let session = create_session();
    setup_orders_table(&session).await;
    setup_customers_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT name FROM customers c
            WHERE EXISTS (
                SELECT 1 FROM orders o
                WHERE o.customer_id = c.id AND LOWER(o.status) = 'completed'
            )
            ORDER BY name",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_combined_in_and_exists() {
    let session = create_session();
    setup_orders_table(&session).await;
    setup_customers_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT name FROM customers c
            WHERE c.tier IN ('gold', 'silver')
              AND EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.amount > 100.0)
            ORDER BY name",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_deep_nested_boolean() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE ((status = 'completed' OR status = 'pending')
                   AND (amount > 100.0 OR customer_id = 101))
               OR (status = 'cancelled' AND amount > 250.0)
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [2], [3], [4], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_not_in_subquery_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, category INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE selected_categories (cat_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO selected_categories VALUES (10), (20)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id FROM items
            WHERE category NOT IN (SELECT cat_id FROM selected_categories)
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_with_cast_in_subquery() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE str_ids (id STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE int_ids (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO str_ids VALUES ('1'), ('2'), ('3')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO int_ids VALUES (1), (4)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id FROM str_ids
            WHERE CAST(id AS INT64) IN (SELECT id FROM int_ids)
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["1"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_correlated_exists_multiple_conditions() {
    let session = create_session();
    setup_orders_table(&session).await;
    setup_customers_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT name FROM customers c
            WHERE EXISTS (
                SELECT 1 FROM orders o
                WHERE o.customer_id = c.id
                  AND o.status = 'completed'
                  AND o.amount > 50.0
            )
            ORDER BY name",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_scalar_subquery_in_arithmetic() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE amount - (SELECT MIN(amount) FROM orders) > 100.0
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_in_array_subquery() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, tags ARRAY<STRING>)")
        .await
        .unwrap();
    session
        .execute_sql(r#"INSERT INTO data VALUES (1, ['a', 'b']), (2, ['c', 'd']), (3, ['a', 'e'])"#)
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE 'a' IN UNNEST(tags) ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_with_nullif() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 0), (2, 5), (3, 0)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE NULLIF(val, 0) IS NOT NULL ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_correlated_sum_comparison() {
    let session = create_session();
    setup_orders_table(&session).await;
    setup_customers_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT name FROM customers c
            WHERE (SELECT SUM(amount) FROM orders o WHERE o.customer_id = c.id) > 300.0
            ORDER BY name",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_multiple_exists() {
    let session = create_session();
    setup_orders_table(&session).await;
    setup_customers_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT name FROM customers c
            WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.status = 'completed')
              AND EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.status = 'pending')
            ORDER BY name",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_comparison_chain() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE amount >= 75.0 AND amount <= 200.0 AND amount != 150.0
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[2], [3], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_not_with_parentheses() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE NOT (status = 'cancelled' OR status = 'pending')
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_subquery_with_distinct() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE customer_id IN (SELECT DISTINCT customer_id FROM orders WHERE amount > 100.0)
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [2], [3], [4], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_subquery_with_order_by_limit() {
    let session = create_session();
    setup_orders_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id FROM orders
            WHERE customer_id IN (
                SELECT customer_id FROM orders ORDER BY amount DESC LIMIT 2
            )
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [2], [4]]);
}
