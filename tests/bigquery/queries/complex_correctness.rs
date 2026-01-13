use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::{create_session, d};

async fn setup_ecommerce(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE customers (id INT64, name STRING, region STRING, tier STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE products (id INT64, name STRING, category STRING, price INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "CREATE TABLE orders (id INT64, customer_id INT64, order_date DATE, status STRING)",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "CREATE TABLE order_items (order_id INT64, product_id INT64, quantity INT64, unit_price INT64)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO customers VALUES
            (1, 'Alice', 'East', 'Gold'),
            (2, 'Bob', 'West', 'Silver'),
            (3, 'Charlie', 'East', 'Gold'),
            (4, 'Diana', 'West', 'Bronze'),
            (5, 'Eve', 'North', 'Gold')",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO products VALUES
            (101, 'Laptop', 'Electronics', 1000),
            (102, 'Phone', 'Electronics', 600),
            (103, 'Desk', 'Furniture', 300),
            (104, 'Chair', 'Furniture', 150),
            (105, 'Monitor', 'Electronics', 400)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO orders VALUES
            (1001, 1, DATE '2024-01-15', 'completed'),
            (1002, 1, DATE '2024-02-20', 'completed'),
            (1003, 2, DATE '2024-01-25', 'completed'),
            (1004, 3, DATE '2024-03-10', 'completed'),
            (1005, 3, DATE '2024-03-15', 'pending'),
            (1006, 4, DATE '2024-02-05', 'cancelled'),
            (1007, 5, DATE '2024-01-10', 'completed')",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO order_items VALUES
            (1001, 101, 1, 1000),
            (1001, 104, 2, 150),
            (1002, 102, 1, 600),
            (1003, 103, 1, 300),
            (1003, 104, 4, 150),
            (1004, 101, 2, 1000),
            (1004, 105, 1, 400),
            (1005, 102, 1, 600),
            (1006, 101, 1, 1000),
            (1007, 105, 3, 400)",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_basic_join_and_aggregation() {
    let session = create_session();
    setup_ecommerce(&session).await;

    let result = session
        .execute_sql(
            "SELECT
                o.customer_id,
                SUM(oi.quantity * oi.unit_price) AS total
            FROM orders o
            JOIN order_items oi ON o.id = oi.order_id
            WHERE o.status = 'completed'
            GROUP BY o.customer_id
            ORDER BY o.customer_id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 1900], [2, 900], [3, 2400], [5, 1200],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_function_with_cte_and_subquery() {
    let session = create_session();
    setup_ecommerce(&session).await;

    let result = session
        .execute_sql(
            "WITH order_totals AS (
                SELECT
                    o.customer_id,
                    o.order_date,
                    SUM(oi.quantity * oi.unit_price) AS total
                FROM orders o
                JOIN order_items oi ON o.id = oi.order_id
                WHERE o.status = 'completed'
                GROUP BY o.customer_id, o.order_date
            )
            SELECT
                c.name,
                ot.order_date,
                ot.total,
                SUM(ot.total) OVER (PARTITION BY c.id ORDER BY ot.order_date) AS running_total,
                ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY ot.order_date) AS order_num
            FROM customers c
            JOIN order_totals ot ON c.id = ot.customer_id
            ORDER BY c.name, ot.order_date",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", d(2024, 1, 15), 1300, 1300, 1],
            ["Alice", d(2024, 2, 20), 600, 1900, 2],
            ["Bob", d(2024, 1, 25), 900, 900, 1],
            ["Charlie", d(2024, 3, 10), 2400, 2400, 1],
            ["Eve", d(2024, 1, 10), 1200, 1200, 1],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_cte_with_window_and_filter() {
    let session = create_session();
    setup_ecommerce(&session).await;

    let result = session
        .execute_sql(
            "WITH product_sales AS (
                SELECT
                    p.id AS product_id,
                    p.name AS product_name,
                    p.category,
                    SUM(oi.quantity) AS units_sold,
                    SUM(oi.quantity * oi.unit_price) AS revenue
                FROM products p
                JOIN order_items oi ON p.id = oi.product_id
                JOIN orders o ON oi.order_id = o.id
                WHERE o.status = 'completed'
                GROUP BY p.id, p.name, p.category
            ),
            ranked_products AS (
                SELECT
                    product_name,
                    category,
                    units_sold,
                    revenue,
                    RANK() OVER (PARTITION BY category ORDER BY revenue DESC) AS category_rank
                FROM product_sales
            )
            SELECT product_name, category, units_sold, revenue, category_rank
            FROM ranked_products
            WHERE category_rank = 1
            ORDER BY revenue DESC",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Laptop", "Electronics", 3, 3000, 1],
            ["Chair", "Furniture", 6, 900, 1],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_complex_join_with_exists_and_not_exists() {
    let session = create_session();
    setup_ecommerce(&session).await;

    let result = session
        .execute_sql(
            "SELECT c.name, c.tier
            FROM customers c
            WHERE EXISTS (
                SELECT 1 FROM orders o
                WHERE o.customer_id = c.id AND o.status = 'completed'
            )
            AND NOT EXISTS (
                SELECT 1 FROM orders o
                WHERE o.customer_id = c.id AND o.status = 'cancelled'
            )
            ORDER BY c.name",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", "Gold"],
            ["Bob", "Silver"],
            ["Charlie", "Gold"],
            ["Eve", "Gold"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_multi_level_aggregation_with_having() {
    let session = create_session();
    setup_ecommerce(&session).await;

    let result = session
        .execute_sql(
            "WITH category_sales AS (
                SELECT
                    p.category,
                    c.region,
                    SUM(oi.quantity * oi.unit_price) AS revenue
                FROM products p
                JOIN order_items oi ON p.id = oi.product_id
                JOIN orders o ON oi.order_id = o.id
                JOIN customers c ON o.customer_id = c.id
                WHERE o.status = 'completed'
                GROUP BY p.category, c.region
                HAVING SUM(oi.quantity * oi.unit_price) > 500
            )
            SELECT
                category,
                COUNT(DISTINCT region) AS region_count,
                SUM(revenue) AS total_revenue
            FROM category_sales
            GROUP BY category
            ORDER BY total_revenue DESC",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 2, 5200], ["Furniture", 1, 900],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_with_complex_ordering() {
    let session = create_session();
    setup_ecommerce(&session).await;

    let result = session
        .execute_sql(
            "SELECT
                c.name,
                ARRAY_AGG(p.name ORDER BY o.order_date, p.name) AS products_purchased
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
            JOIN order_items oi ON o.id = oi.order_id
            JOIN products p ON oi.product_id = p.id
            WHERE o.status = 'completed'
            GROUP BY c.id, c.name
            ORDER BY c.name",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", ["Chair", "Laptop", "Phone"]],
            ["Bob", ["Chair", "Desk"]],
            ["Charlie", ["Laptop", "Monitor"]],
            ["Eve", ["Monitor"]],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_union_all_with_different_aggregations() {
    let session = create_session();
    setup_ecommerce(&session).await;

    let result = session
        .execute_sql(
            "SELECT 'By Region' AS grouping, region AS dimension, SUM(total) AS value
            FROM (
                SELECT c.region, SUM(oi.quantity * oi.unit_price) AS total
                FROM customers c
                JOIN orders o ON c.id = o.customer_id
                JOIN order_items oi ON o.id = oi.order_id
                WHERE o.status = 'completed'
                GROUP BY c.region
            )
            GROUP BY region
            UNION ALL
            SELECT 'By Tier' AS grouping, tier AS dimension, SUM(total) AS value
            FROM (
                SELECT c.tier, SUM(oi.quantity * oi.unit_price) AS total
                FROM customers c
                JOIN orders o ON c.id = o.customer_id
                JOIN order_items oi ON o.id = oi.order_id
                WHERE o.status = 'completed'
                GROUP BY c.tier
            )
            GROUP BY tier
            ORDER BY grouping, value DESC",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["By Region", "East", 4300],
            ["By Region", "North", 1200],
            ["By Region", "West", 900],
            ["By Tier", "Gold", 5500],
            ["By Tier", "Silver", 900],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_frame_with_rows_between() {
    let session = create_session();
    setup_ecommerce(&session).await;

    let result = session
        .execute_sql(
            "WITH daily_orders AS (
                SELECT
                    o.order_date,
                    SUM(oi.quantity * oi.unit_price) AS daily_total
                FROM orders o
                JOIN order_items oi ON o.id = oi.order_id
                WHERE o.status = 'completed'
                GROUP BY o.order_date
            )
            SELECT
                order_date,
                daily_total,
                AVG(daily_total) OVER (
                    ORDER BY order_date
                    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
                ) AS moving_avg
            FROM daily_orders
            ORDER BY order_date",
        )
        .await
        .unwrap();

    let records = result.to_records().unwrap();
    assert_eq!(records.len(), 5);
    assert!(records[0].values()[2].as_f64().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn test_ntile_and_percent_rank() {
    let session = create_session();
    setup_ecommerce(&session).await;

    let result = session
        .execute_sql(
            "WITH customer_totals AS (
                SELECT
                    c.id,
                    c.name,
                    COALESCE(SUM(oi.quantity * oi.unit_price), 0) AS total_spent
                FROM customers c
                LEFT JOIN orders o ON c.id = o.customer_id AND o.status = 'completed'
                LEFT JOIN order_items oi ON o.id = oi.order_id
                GROUP BY c.id, c.name
            )
            SELECT
                name,
                total_spent,
                NTILE(3) OVER (ORDER BY total_spent DESC) AS spending_tier
            FROM customer_totals
            ORDER BY total_spent DESC",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", 2400, 1],
            ["Alice", 1900, 1],
            ["Eve", 1200, 2],
            ["Bob", 900, 2],
            ["Diana", 0, 3],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_first_value_last_value() {
    let session = create_session();
    setup_ecommerce(&session).await;

    let result = session
        .execute_sql(
            "WITH customer_orders AS (
                SELECT
                    c.id AS customer_id,
                    c.name,
                    o.order_date,
                    SUM(oi.quantity * oi.unit_price) AS order_total
                FROM customers c
                JOIN orders o ON c.id = o.customer_id
                JOIN order_items oi ON o.id = oi.order_id
                WHERE o.status = 'completed'
                GROUP BY c.id, c.name, o.order_date
            )
            SELECT DISTINCT
                name,
                FIRST_VALUE(order_total) OVER (
                    PARTITION BY customer_id
                    ORDER BY order_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS first_order_total,
                LAST_VALUE(order_total) OVER (
                    PARTITION BY customer_id
                    ORDER BY order_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS last_order_total
            FROM customer_orders
            ORDER BY name",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1300, 600],
            ["Bob", 900, 900],
            ["Charlie", 2400, 2400],
            ["Eve", 1200, 1200],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_complex_null_handling() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE data_with_nulls (id INT64, grp STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO data_with_nulls VALUES
            (1, 'A', 10),
            (2, 'A', NULL),
            (3, 'A', 30),
            (4, 'B', NULL),
            (5, 'B', NULL),
            (6, NULL, 60),
            (7, NULL, NULL)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                COALESCE(grp, 'Unknown') AS group_name,
                COUNT(*) AS total_rows,
                COUNT(val) AS non_null_vals,
                SUM(val) AS sum_val,
                AVG(val) AS avg_val
            FROM data_with_nulls
            GROUP BY grp
            ORDER BY group_name NULLS LAST",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["A", 3, 2, 40, 20.0],
            ["B", 2, 0, null, null],
            ["Unknown", 2, 1, 60, 60.0],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_in_join_conditions() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE left_nullable (id INT64, code STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE right_lookup (code STRING, description STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO left_nullable VALUES (1, 'A'), (2, NULL), (3, 'B'), (4, NULL)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO right_lookup VALUES ('A', 'Alpha'), ('B', 'Beta'), ('DEFAULT', 'Default Value')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                l.id,
                COALESCE(l.code, 'DEFAULT') AS effective_code,
                r.description
            FROM left_nullable l
            LEFT JOIN right_lookup r ON COALESCE(l.code, 'DEFAULT') = r.code
            ORDER BY l.id",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, "A", "Alpha"],
            [2, "DEFAULT", "Default Value"],
            [3, "B", "Beta"],
            [4, "DEFAULT", "Default Value"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_recursive_cte_with_aggregation() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE org_chart (id INT64, name STRING, manager_id INT64, salary INT64)",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO org_chart VALUES
            (1, 'CEO', NULL, 500000),
            (2, 'VP Engineering', 1, 300000),
            (3, 'VP Sales', 1, 280000),
            (4, 'Engineer Lead', 2, 180000),
            (5, 'Engineer 1', 4, 120000),
            (6, 'Engineer 2', 4, 115000),
            (7, 'Sales Lead', 3, 150000),
            (8, 'Sales Rep', 7, 80000)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH RECURSIVE org_tree AS (
                SELECT id, name, manager_id, salary, 0 AS depth, id AS root_id
                FROM org_chart
                WHERE manager_id IS NULL
                UNION ALL
                SELECT o.id, o.name, o.manager_id, o.salary, t.depth + 1, t.root_id
                FROM org_chart o
                JOIN org_tree t ON o.manager_id = t.id
            )
            SELECT
                depth,
                COUNT(*) AS employee_count,
                SUM(salary) AS total_salary,
                CAST(AVG(salary) AS INT64) AS avg_salary
            FROM org_tree
            GROUP BY depth
            ORDER BY depth",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [0, 1, 500000, 500000],
            [1, 2, 580000, 290000],
            [2, 2, 330000, 165000],
            [3, 3, 315000, 105000],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_dense_rank_with_ties() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE scores (player STRING, score INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO scores VALUES
            ('Alice', 100),
            ('Bob', 95),
            ('Charlie', 95),
            ('Diana', 90),
            ('Eve', 90),
            ('Frank', 85)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                player,
                score,
                RANK() OVER (ORDER BY score DESC) AS rank,
                DENSE_RANK() OVER (ORDER BY score DESC) AS dense_rank,
                ROW_NUMBER() OVER (ORDER BY score DESC, player) AS row_num
            FROM scores
            ORDER BY score DESC, player",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 100, 1, 1, 1],
            ["Bob", 95, 2, 2, 2],
            ["Charlie", 95, 2, 2, 3],
            ["Diana", 90, 4, 3, 4],
            ["Eve", 90, 4, 3, 5],
            ["Frank", 85, 6, 4, 6],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_safe_offset_first_element() {
    let session = create_session();
    setup_ecommerce(&session).await;

    let result = session
        .execute_sql(
            "SELECT
                c.name,
                ARRAY_AGG(p.name ORDER BY oi.unit_price DESC LIMIT 1)[SAFE_OFFSET(0)] AS most_expensive_product
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
            JOIN order_items oi ON o.id = oi.order_id
            JOIN products p ON oi.product_id = p.id
            WHERE o.status = 'completed'
            GROUP BY c.id, c.name
            ORDER BY c.name",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", "Laptop"],
            ["Bob", "Desk"],
            ["Charlie", "Laptop"],
            ["Eve", "Monitor"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_safe_offset_with_nulls() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE events (user_id INT64, event_type STRING, event_time INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO events VALUES
            (1, 'login', 100),
            (1, 'purchase', 200),
            (1, 'logout', 300),
            (2, 'login', 150),
            (2, NULL, 250),
            (3, NULL, 100)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                user_id,
                ARRAY_AGG(event_type IGNORE NULLS ORDER BY event_time ASC LIMIT 1)[SAFE_OFFSET(0)] AS first_event,
                ARRAY_AGG(event_type IGNORE NULLS ORDER BY event_time DESC LIMIT 1)[SAFE_OFFSET(0)] AS last_event
            FROM events
            GROUP BY user_id
            ORDER BY user_id",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, "login", "logout"],
            [2, "login", "login"],
            [3, null, null],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_multiple_columns_pattern() {
    let session = create_session();
    setup_ecommerce(&session).await;

    let result = session
        .execute_sql(
            "SELECT
                c.name,
                ARRAY_AGG(o.order_date ORDER BY o.order_date ASC LIMIT 1)[SAFE_OFFSET(0)] AS first_order_date,
                ARRAY_AGG(o.order_date ORDER BY o.order_date DESC LIMIT 1)[SAFE_OFFSET(0)] AS last_order_date,
                ARRAY_AGG(oi.unit_price ORDER BY o.order_date ASC LIMIT 1)[SAFE_OFFSET(0)] AS first_order_price
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
            JOIN order_items oi ON o.id = oi.order_id
            WHERE o.status = 'completed'
            GROUP BY c.id, c.name
            ORDER BY c.name",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", d(2024, 1, 15), d(2024, 2, 20), 1000],
            ["Bob", d(2024, 1, 25), d(2024, 1, 25), 300],
            ["Charlie", d(2024, 3, 10), d(2024, 3, 10), 1000],
            ["Eve", d(2024, 1, 10), d(2024, 1, 10), 400],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_in_subquery() {
    let session = create_session();
    setup_ecommerce(&session).await;

    let result = session
        .execute_sql(
            "SELECT
                name,
                first_product,
                total_spent
            FROM (
                SELECT
                    c.id,
                    c.name,
                    ARRAY_AGG(p.name ORDER BY o.order_date, p.name LIMIT 1)[SAFE_OFFSET(0)] AS first_product,
                    SUM(oi.quantity * oi.unit_price) AS total_spent
                FROM customers c
                JOIN orders o ON c.id = o.customer_id
                JOIN order_items oi ON o.id = oi.order_id
                JOIN products p ON oi.product_id = p.id
                WHERE o.status = 'completed'
                GROUP BY c.id, c.name
            )
            WHERE total_spent > 1000
            ORDER BY total_spent DESC",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", "Laptop", 2400],
            ["Alice", "Chair", 1900],
            ["Eve", "Monitor", 1200],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_with_struct_access() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE user_sessions (
                user_id INT64,
                session_start DATETIME,
                device STRING,
                country STRING
            )",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO user_sessions VALUES
            (1, DATETIME '2024-01-01 10:00:00', 'mobile', 'US'),
            (1, DATETIME '2024-01-02 11:00:00', 'desktop', 'US'),
            (1, DATETIME '2024-01-03 09:00:00', 'mobile', 'CA'),
            (2, DATETIME '2024-01-01 08:00:00', 'tablet', 'UK'),
            (2, DATETIME '2024-01-02 14:00:00', 'mobile', 'UK'),
            (3, DATETIME '2024-01-05 16:00:00', 'desktop', 'DE')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                user_id,
                ARRAY_AGG(device ORDER BY session_start ASC LIMIT 1)[SAFE_OFFSET(0)] AS first_device,
                ARRAY_AGG(device ORDER BY session_start DESC LIMIT 1)[SAFE_OFFSET(0)] AS last_device,
                ARRAY_AGG(country ORDER BY session_start ASC LIMIT 1)[SAFE_OFFSET(0)] AS first_country
            FROM user_sessions
            GROUP BY user_id
            ORDER BY user_id",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, "mobile", "mobile", "US"],
            [2, "tablet", "mobile", "UK"],
            [3, "desktop", "desktop", "DE"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_combined_with_window() {
    let session = create_session();
    setup_ecommerce(&session).await;

    let result = session
        .execute_sql(
            "WITH customer_products AS (
                SELECT
                    c.id AS customer_id,
                    c.name,
                    c.region,
                    ARRAY_AGG(p.name ORDER BY oi.unit_price DESC LIMIT 1)[SAFE_OFFSET(0)] AS top_product,
                    SUM(oi.quantity * oi.unit_price) AS total_spent
                FROM customers c
                JOIN orders o ON c.id = o.customer_id
                JOIN order_items oi ON o.id = oi.order_id
                JOIN products p ON oi.product_id = p.id
                WHERE o.status = 'completed'
                GROUP BY c.id, c.name, c.region
            )
            SELECT
                name,
                region,
                top_product,
                total_spent,
                RANK() OVER (PARTITION BY region ORDER BY total_spent DESC) AS region_rank
            FROM customer_products
            ORDER BY region, region_rank",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", "East", "Laptop", 2400, 1],
            ["Alice", "East", "Laptop", 1900, 2],
            ["Eve", "North", "Monitor", 1200, 1],
            ["Bob", "West", "Desk", 900, 1],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_empty_result_safe_offset() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE sparse_data (grp STRING, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO sparse_data VALUES
            ('A', 'x'),
            ('A', 'y'),
            ('B', NULL),
            ('C', NULL)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                grp,
                ARRAY_AGG(val IGNORE NULLS ORDER BY val LIMIT 1)[SAFE_OFFSET(0)] AS first_val
            FROM sparse_data
            GROUP BY grp
            ORDER BY grp",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["A", "x"], ["B", null], ["C", null],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_with_distinct_and_order() {
    let session = create_session();
    setup_ecommerce(&session).await;

    let result = session
        .execute_sql(
            "SELECT
                c.region,
                ARRAY_AGG(DISTINCT p.category ORDER BY p.category) AS categories,
                ARRAY_LENGTH(ARRAY_AGG(DISTINCT p.category ORDER BY p.category)) AS category_count
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
            JOIN order_items oi ON o.id = oi.order_id
            JOIN products p ON oi.product_id = p.id
            WHERE o.status = 'completed'
            GROUP BY c.region
            ORDER BY c.region",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", ["Electronics", "Furniture"], 2],
            ["North", ["Electronics"], 1],
            ["West", ["Furniture"], 1],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_nested_in_case() {
    let session = create_session();
    setup_ecommerce(&session).await;

    let result = session
        .execute_sql(
            "SELECT
                c.name,
                CASE
                    WHEN ARRAY_LENGTH(ARRAY_AGG(DISTINCT p.category)) > 1 THEN 'Multi-category'
                    ELSE ARRAY_AGG(DISTINCT p.category)[SAFE_OFFSET(0)]
                END AS purchase_type
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
            JOIN order_items oi ON o.id = oi.order_id
            JOIN products p ON oi.product_id = p.id
            WHERE o.status = 'completed'
            GROUP BY c.id, c.name
            ORDER BY c.name",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", "Multi-category"],
            ["Bob", "Furniture"],
            ["Charlie", "Electronics"],
            ["Eve", "Electronics"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_attribution_pattern() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE touchpoints (
                user_id INT64,
                channel STRING,
                campaign STRING,
                touch_time DATETIME,
                conversion BOOL
            )",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO touchpoints VALUES
            (1, 'organic', 'brand', DATETIME '2024-01-01 10:00:00', false),
            (1, 'paid', 'winter_sale', DATETIME '2024-01-02 11:00:00', false),
            (1, 'email', 'newsletter', DATETIME '2024-01-03 09:00:00', true),
            (2, 'social', 'influencer', DATETIME '2024-01-01 08:00:00', false),
            (2, 'paid', 'retarget', DATETIME '2024-01-02 14:00:00', true),
            (3, 'direct', NULL, DATETIME '2024-01-05 16:00:00', true)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                user_id,
                ARRAY_AGG(channel ORDER BY touch_time ASC LIMIT 1)[SAFE_OFFSET(0)] AS first_touch_channel,
                ARRAY_AGG(channel ORDER BY touch_time DESC LIMIT 1)[SAFE_OFFSET(0)] AS last_touch_channel,
                ARRAY_AGG(campaign IGNORE NULLS ORDER BY touch_time ASC LIMIT 1)[SAFE_OFFSET(0)] AS first_campaign,
                ARRAY_AGG(campaign IGNORE NULLS ORDER BY touch_time DESC LIMIT 1)[SAFE_OFFSET(0)] AS last_campaign
            FROM touchpoints
            GROUP BY user_id
            ORDER BY user_id",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, "organic", "email", "brand", "newsletter"],
            [2, "social", "paid", "influencer", "retarget"],
            [3, "direct", "direct", null, null],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_dedup_pattern() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE versioned_records (
                id INT64,
                version INT64,
                value STRING,
                updated_at DATETIME
            )",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO versioned_records VALUES
            (1, 1, 'old_value', DATETIME '2024-01-01 10:00:00'),
            (1, 2, 'new_value', DATETIME '2024-01-02 10:00:00'),
            (2, 1, 'only_value', DATETIME '2024-01-01 10:00:00'),
            (3, 1, 'first', DATETIME '2024-01-01 10:00:00'),
            (3, 2, 'second', DATETIME '2024-01-02 10:00:00'),
            (3, 3, 'latest', DATETIME '2024-01-03 10:00:00')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                id,
                ARRAY_AGG(value ORDER BY version DESC LIMIT 1)[SAFE_OFFSET(0)] AS latest_value,
                ARRAY_AGG(version ORDER BY version DESC LIMIT 1)[SAFE_OFFSET(0)] AS latest_version
            FROM versioned_records
            GROUP BY id
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [[1, "new_value", 2], [2, "only_value", 1], [3, "latest", 3],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_offset_out_of_bounds() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE small_arrays (id INT64, vals ARRAY<STRING>)")
        .await
        .unwrap();
    session
        .execute_sql(
            r#"INSERT INTO small_arrays VALUES
            (1, ['a', 'b', 'c']),
            (2, ['x']),
            (3, [])"#,
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                id,
                vals[SAFE_OFFSET(0)] AS first,
                vals[SAFE_OFFSET(1)] AS second,
                vals[SAFE_OFFSET(5)] AS sixth
            FROM small_arrays
            ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, "a", "b", null],
            [2, "x", null, null],
            [3, null, null, null],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_ordinal_pattern() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE ranked_items (grp STRING, item STRING, score INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO ranked_items VALUES
            ('A', 'gold', 100),
            ('A', 'silver', 80),
            ('A', 'bronze', 60),
            ('B', 'first', 90),
            ('B', 'second', 70),
            ('C', 'only', 50)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                grp,
                ARRAY_AGG(item ORDER BY score DESC)[SAFE_ORDINAL(1)] AS first_place,
                ARRAY_AGG(item ORDER BY score DESC)[SAFE_ORDINAL(2)] AS second_place,
                ARRAY_AGG(item ORDER BY score DESC)[SAFE_ORDINAL(3)] AS third_place
            FROM ranked_items
            GROUP BY grp
            ORDER BY grp",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["A", "gold", "silver", "bronze"],
            ["B", "first", "second", null],
            ["C", "only", null, null],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_desc_ordering_simple() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE simple_events (user_id INT64, event_type STRING, event_time INT64)",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO simple_events VALUES
            (1, 'login', 100),
            (1, 'purchase', 200),
            (1, 'logout', 300)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                user_id,
                ARRAY_AGG(event_type ORDER BY event_time DESC) AS events_desc
            FROM simple_events
            GROUP BY user_id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, ["logout", "purchase", "login"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_asc_and_desc_together() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE events2 (user_id INT64, event_type STRING, event_time INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO events2 VALUES
            (1, 'login', 100),
            (1, 'purchase', 200),
            (1, 'logout', 300)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                user_id,
                ARRAY_AGG(event_type ORDER BY event_time ASC) AS events_asc,
                ARRAY_AGG(event_type ORDER BY event_time DESC) AS events_desc
            FROM events2
            GROUP BY user_id",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [[
            1,
            ["login", "purchase", "logout"],
            ["logout", "purchase", "login"]
        ]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_with_limit_and_both_orders() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE events3 (user_id INT64, event_type STRING, event_time INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO events3 VALUES
            (1, 'login', 100),
            (1, 'purchase', 200),
            (1, 'logout', 300)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                user_id,
                ARRAY_AGG(event_type ORDER BY event_time ASC LIMIT 1)[SAFE_OFFSET(0)] AS first_event,
                ARRAY_AGG(event_type ORDER BY event_time DESC LIMIT 1)[SAFE_OFFSET(0)] AS last_event
            FROM events3
            GROUP BY user_id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "login", "logout"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_single_desc_limit() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE events4 (user_id INT64, event_type STRING, event_time INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO events4 VALUES
            (1, 'login', 100),
            (1, 'purchase', 200),
            (1, 'logout', 300)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                user_id,
                ARRAY_AGG(event_type ORDER BY event_time DESC LIMIT 1)[SAFE_OFFSET(0)] AS last_event
            FROM events4
            GROUP BY user_id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "logout"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_limit_order_debug() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE events5 (user_id INT64, event_type STRING, event_time INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO events5 VALUES
            (1, 'login', 100),
            (1, 'purchase', 200),
            (1, 'logout', 300)",
        )
        .await
        .unwrap();

    let asc_result = session
        .execute_sql(
            "SELECT
                user_id,
                ARRAY_AGG(event_type ORDER BY event_time ASC LIMIT 1)[SAFE_OFFSET(0)] AS first_event
            FROM events5
            GROUP BY user_id",
        )
        .await
        .unwrap();

    let desc_result = session
        .execute_sql(
            "SELECT
                user_id,
                ARRAY_AGG(event_type ORDER BY event_time DESC LIMIT 1)[SAFE_OFFSET(0)] AS last_event
            FROM events5
            GROUP BY user_id",
        )
        .await
        .unwrap();

    assert_table_eq!(asc_result, [[1, "login"]]);
    assert_table_eq!(desc_result, [[1, "logout"]]);
}
