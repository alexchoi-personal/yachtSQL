use yachtsql::{Table, Value, YachtSQLEngine, YachtSQLSession};

fn create_session_with_optimizer(rule: &str, enabled: bool) -> YachtSQLSession {
    let engine = YachtSQLEngine::new();
    let session = engine.create_session();
    session
        .session()
        .set_variable("PARALLEL_EXECUTION", Value::Bool(true));
    session
        .session()
        .set_variable("OPTIMIZER_LEVEL", Value::String("FULL".to_string()));
    session.session().set_variable(rule, Value::Bool(enabled));
    session
}

fn sort_table(table: &Table) -> Table {
    if table.row_count() == 0 {
        return table.clone();
    }
    let num_cols = table.schema().fields().len();
    let mut indices: Vec<usize> = (0..table.row_count()).collect();
    indices.sort_by(|&a, &b| {
        for col_idx in 0..num_cols {
            let col = table.column(col_idx).unwrap();
            let va = col.get_value(a);
            let vb = col.get_value(b);
            match va.partial_cmp(&vb) {
                Some(std::cmp::Ordering::Equal) => continue,
                Some(ord) => return ord,
                None => {
                    let a_null = matches!(va, Value::Null);
                    let b_null = matches!(vb, Value::Null);
                    match (a_null, b_null) {
                        (true, false) => return std::cmp::Ordering::Greater,
                        (false, true) => return std::cmp::Ordering::Less,
                        _ => continue,
                    }
                }
            }
        }
        std::cmp::Ordering::Equal
    });
    table.reorder_by_indices(&indices).unwrap()
}

fn tables_equal_unordered(a: &Table, b: &Table) -> bool {
    if a.row_count() != b.row_count() || a.schema().fields().len() != b.schema().fields().len() {
        return false;
    }
    let sorted_a = sort_table(a);
    let sorted_b = sort_table(b);
    sorted_a == sorted_b
}

async fn assert_optimizer_equivalence(
    session_on: &YachtSQLSession,
    session_off: &YachtSQLSession,
    query: &str,
) {
    let result_on = session_on.execute_sql(query).await.unwrap();
    let result_off = session_off.execute_sql(query).await.unwrap();

    if !tables_equal_unordered(&result_on, &result_off) {
        panic!(
            "Optimizer equivalence failed for query:\n{}\n\nWith optimizer ON:\n{:?}\n\nWith optimizer OFF:\n{:?}",
            query, result_on, result_off
        );
    }
}

async fn setup_test_tables(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE customers (id INT64, name STRING, country STRING, tier STRING, created_year INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE orders (id INT64, customer_id INT64, product_id INT64, amount FLOAT64, status STRING, order_date DATE)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE products (id INT64, name STRING, price FLOAT64, category STRING, supplier_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "CREATE TABLE suppliers (id INT64, name STRING, country STRING, rating FLOAT64)",
        )
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE order_items (id INT64, order_id INT64, product_id INT64, quantity INT64, unit_price FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE categories (id INT64, name STRING, parent_id INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO customers VALUES
            (1, 'Alice', 'USA', 'gold', 2020),
            (2, 'Bob', 'Canada', 'silver', 2019),
            (3, 'Charlie', 'USA', 'gold', 2021),
            (4, 'Diana', 'UK', 'bronze', 2020),
            (5, 'Eve', 'Canada', 'gold', 2018),
            (6, 'Frank', 'USA', 'silver', 2022),
            (7, 'Grace', 'UK', 'gold', 2019),
            (8, 'Henry', 'Germany', 'bronze', 2021),
            (9, 'Ivy', 'France', 'silver', 2020),
            (10, 'Jack', 'USA', 'gold', 2017)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO orders VALUES
            (1, 1, 1, 100.0, 'completed', DATE '2023-01-15'),
            (2, 1, 2, 200.0, 'pending', DATE '2023-02-20'),
            (3, 2, 1, 150.0, 'completed', DATE '2023-01-25'),
            (4, 3, 3, 75.0, 'cancelled', DATE '2023-03-10'),
            (5, 3, 2, 300.0, 'completed', DATE '2023-03-15'),
            (6, 4, 4, 50.0, 'pending', DATE '2023-04-01'),
            (7, 5, 1, 250.0, 'completed', DATE '2023-04-10'),
            (8, 6, 5, 175.0, 'completed', DATE '2023-05-05'),
            (9, 7, 2, 225.0, 'pending', DATE '2023-05-15'),
            (10, 8, 3, 125.0, 'completed', DATE '2023-06-01'),
            (11, 9, 4, 350.0, 'completed', DATE '2023-06-15'),
            (12, 10, 5, 400.0, 'pending', DATE '2023-07-01'),
            (13, 1, 3, 180.0, 'completed', DATE '2023-07-10'),
            (14, 2, 4, 90.0, 'cancelled', DATE '2023-07-20'),
            (15, 3, 5, 275.0, 'completed', DATE '2023-08-01')",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO products VALUES
            (1, 'Widget', 10.0, 'Electronics', 1),
            (2, 'Gadget', 25.0, 'Electronics', 1),
            (3, 'Gizmo', 15.0, 'Tools', 2),
            (4, 'Thingamajig', 30.0, 'Tools', 2),
            (5, 'Doohickey', 5.0, 'Misc', 3),
            (6, 'Contraption', 45.0, 'Electronics', 1),
            (7, 'Apparatus', 60.0, 'Tools', 3),
            (8, 'Device', 35.0, 'Electronics', 2)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO suppliers VALUES
            (1, 'TechCorp', 'USA', 4.5),
            (2, 'ToolMaster', 'Germany', 4.2),
            (3, 'MiscSupply', 'China', 3.8),
            (4, 'GlobalParts', 'UK', 4.0)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO order_items VALUES
            (1, 1, 1, 2, 10.0),
            (2, 1, 2, 1, 25.0),
            (3, 2, 2, 3, 25.0),
            (4, 3, 1, 5, 10.0),
            (5, 4, 3, 2, 15.0),
            (6, 5, 2, 4, 25.0),
            (7, 5, 3, 1, 15.0),
            (8, 6, 4, 1, 30.0),
            (9, 7, 1, 10, 10.0),
            (10, 8, 5, 5, 5.0),
            (11, 9, 2, 2, 25.0),
            (12, 10, 3, 3, 15.0),
            (13, 11, 4, 4, 30.0),
            (14, 12, 5, 8, 5.0),
            (15, 13, 3, 6, 15.0)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO categories VALUES
            (1, 'All', NULL),
            (2, 'Electronics', 1),
            (3, 'Tools', 1),
            (4, 'Misc', 1),
            (5, 'Computers', 2),
            (6, 'Power Tools', 3)",
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_trivial_predicate_removal_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_TRIVIAL_PREDICATE", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_TRIVIAL_PREDICATE", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT * FROM customers WHERE 1 = 1",
        "SELECT * FROM customers WHERE TRUE",
        "SELECT id, name FROM customers WHERE id > 0 AND 1 = 1",
        "SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id WHERE 1 = 1 AND c.country = 'USA'",
        "SELECT * FROM (SELECT * FROM customers WHERE TRUE) sub WHERE 1 = 1",
        "SELECT country, COUNT(*) FROM customers WHERE 1 = 1 GROUP BY country HAVING TRUE",
        "SELECT c.name, SUM(o.amount) as total FROM customers c JOIN orders o ON c.id = o.customer_id WHERE TRUE GROUP BY c.name HAVING 1 = 1",
        "WITH cte AS (SELECT * FROM customers WHERE 1 = 1) SELECT * FROM cte WHERE TRUE",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}

#[tokio::test]
async fn test_filter_pushdown_project_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_FILTER_PUSHDOWN_PROJECT", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_FILTER_PUSHDOWN_PROJECT", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT id, name FROM customers WHERE id > 2",
        "SELECT name, country FROM customers WHERE country = 'USA'",
        "SELECT id * 2 as double_id FROM customers WHERE id < 4",
        "SELECT name, UPPER(country) as upper_country FROM customers WHERE tier = 'gold' AND created_year >= 2020",
        "SELECT sub.name FROM (SELECT id, name, country FROM customers) sub WHERE sub.id > 5",
        "SELECT name, amount FROM (SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id) sub WHERE amount > 150",
        "SELECT category, total_price FROM (SELECT category, SUM(price) as total_price FROM products GROUP BY category) sub WHERE total_price > 50",
        "SELECT name, tier, created_year FROM (SELECT * FROM customers WHERE country IN ('USA', 'UK')) sub WHERE tier != 'bronze'",
        "SELECT x.name FROM (SELECT id, name FROM (SELECT * FROM customers) inner_sub) x WHERE x.id < 5",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}

#[tokio::test]
async fn test_filter_merging_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_FILTER_MERGING", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_FILTER_MERGING", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT * FROM customers WHERE id > 1 AND id < 8",
        "SELECT * FROM orders WHERE amount > 50 AND status = 'completed'",
        "SELECT * FROM customers WHERE country = 'USA' AND name LIKE 'A%'",
        "SELECT * FROM customers WHERE id > 2 AND country = 'USA' AND tier = 'gold'",
        "SELECT * FROM orders WHERE customer_id > 1 AND customer_id < 5 AND amount > 100 AND status != 'cancelled'",
        "SELECT * FROM products WHERE price > 10 AND price < 50 AND category = 'Electronics' AND supplier_id IN (1, 2)",
        "SELECT c.* FROM customers c WHERE c.id > 3 AND c.country IN ('USA', 'UK') AND c.tier = 'gold' AND c.created_year >= 2019",
        "SELECT * FROM order_items WHERE quantity > 1 AND quantity < 10 AND unit_price > 5 AND unit_price < 30",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}

#[tokio::test]
async fn test_predicate_simplification_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_PREDICATE_SIMPLIFICATION", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_PREDICATE_SIMPLIFICATION", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT * FROM customers WHERE id = id",
        "SELECT * FROM customers WHERE NOT (id < 2)",
        "SELECT * FROM orders WHERE amount > 0 OR amount <= 0",
        "SELECT * FROM customers WHERE NOT NOT (country = 'USA')",
        "SELECT * FROM orders WHERE NOT (status != 'completed')",
        "SELECT * FROM products WHERE price = price AND category = category",
        "SELECT * FROM customers WHERE (id > 5 OR id <= 5) AND country = 'USA'",
        "SELECT * FROM orders WHERE NOT (amount < 100 AND amount >= 100)",
        "SELECT c.* FROM customers c JOIN orders o ON c.id = o.customer_id WHERE c.id = c.id AND o.amount = o.amount",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}

#[tokio::test]
async fn test_project_merging_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_PROJECT_MERGING", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_PROJECT_MERGING", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT name FROM (SELECT id, name FROM customers)",
        "SELECT doubled FROM (SELECT id * 2 as doubled FROM customers)",
        "SELECT upper_name FROM (SELECT UPPER(name) as upper_name FROM customers)",
        "SELECT mid_name FROM (SELECT name as mid_name FROM (SELECT id, name FROM customers)) sub",
        "SELECT country FROM (SELECT name, country FROM (SELECT * FROM customers) a) b",
        "SELECT total FROM (SELECT amount * quantity as total FROM (SELECT o.amount, oi.quantity FROM orders o JOIN order_items oi ON o.id = oi.order_id) sub)",
        "SELECT category, avg_price FROM (SELECT category, AVG(price) as avg_price FROM (SELECT * FROM products) sub GROUP BY category)",
        "SELECT c FROM (SELECT b as c FROM (SELECT a as b FROM (SELECT id as a FROM customers)))",
        "SELECT name, total FROM (SELECT c.name, SUM(o.amount) as total FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name)",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}

#[tokio::test]
async fn test_distinct_elimination_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_DISTINCT_ELIMINATION", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_DISTINCT_ELIMINATION", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT DISTINCT id FROM customers",
        "SELECT DISTINCT country FROM customers",
        "SELECT DISTINCT status FROM orders",
        "SELECT DISTINCT country, tier FROM customers",
        "SELECT DISTINCT category FROM products",
        "SELECT DISTINCT c.country FROM customers c JOIN orders o ON c.id = o.customer_id",
        "SELECT DISTINCT customer_id, status FROM orders WHERE amount > 100",
        "SELECT DISTINCT supplier_id, category FROM products WHERE price > 20",
        "SELECT DISTINCT c.country, c.tier FROM customers c JOIN orders o ON c.id = o.customer_id WHERE o.status = 'completed'",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}

#[tokio::test]
async fn test_cross_to_hash_join_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_CROSS_TO_HASH_JOIN", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_CROSS_TO_HASH_JOIN", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT c.name, o.amount FROM customers c, orders o WHERE c.id = o.customer_id",
        "SELECT c.name, o.status FROM customers c, orders o WHERE c.id = o.customer_id AND o.amount > 100",
        "SELECT c.name, p.name as product FROM customers c, orders o, products p WHERE c.id = o.customer_id AND o.product_id = p.id",
        "SELECT c.name, p.name, s.name as supplier FROM customers c, orders o, products p, suppliers s WHERE c.id = o.customer_id AND o.product_id = p.id AND p.supplier_id = s.id",
        "SELECT c.name, SUM(o.amount) as total FROM customers c, orders o WHERE c.id = o.customer_id GROUP BY c.name",
        "SELECT p.category, COUNT(*) as order_count FROM products p, orders o WHERE p.id = o.product_id GROUP BY p.category",
        "SELECT c.country, p.category, SUM(o.amount) FROM customers c, orders o, products p WHERE c.id = o.customer_id AND o.product_id = p.id GROUP BY c.country, p.category",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}

#[tokio::test]
async fn test_filter_pushdown_join_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_FILTER_PUSHDOWN_JOIN", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_FILTER_PUSHDOWN_JOIN", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id WHERE c.country = 'USA'",
        "SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id WHERE o.status = 'completed'",
        "SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id WHERE c.id > 2 AND o.amount > 100",
        "SELECT c.name, p.name as product FROM customers c JOIN orders o ON c.id = o.customer_id JOIN products p ON o.product_id = p.id WHERE c.tier = 'gold' AND p.category = 'Electronics'",
        "SELECT c.name, o.amount, p.price FROM customers c JOIN orders o ON c.id = o.customer_id JOIN products p ON o.product_id = p.id WHERE c.country = 'USA' AND o.status = 'completed' AND p.price > 15",
        "SELECT c.name, s.name as supplier FROM customers c JOIN orders o ON c.id = o.customer_id JOIN products p ON o.product_id = p.id JOIN suppliers s ON p.supplier_id = s.id WHERE c.tier = 'gold' AND s.rating > 4.0",
        "SELECT c.name, oi.quantity FROM customers c JOIN orders o ON c.id = o.customer_id JOIN order_items oi ON o.id = oi.order_id WHERE c.created_year >= 2020 AND oi.quantity > 2",
        "SELECT c.country, COUNT(*) as cnt FROM customers c JOIN orders o ON c.id = o.customer_id WHERE c.tier IN ('gold', 'silver') AND o.amount > 150 GROUP BY c.country",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}

#[tokio::test]
async fn test_filter_pushdown_aggregate_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_FILTER_PUSHDOWN_AGGREGATE", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_FILTER_PUSHDOWN_AGGREGATE", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT country, COUNT(*) as cnt FROM customers GROUP BY country HAVING COUNT(*) > 1",
        "SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id HAVING SUM(amount) > 200",
        "SELECT status, AVG(amount) as avg_amt FROM orders GROUP BY status HAVING AVG(amount) > 100",
        "SELECT category, COUNT(*) as cnt, SUM(price) as total FROM products GROUP BY category HAVING COUNT(*) >= 2 AND SUM(price) > 30",
        "SELECT c.country, COUNT(*) as orders, SUM(o.amount) as total FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.country HAVING COUNT(*) > 2",
        "SELECT supplier_id, AVG(price) as avg_price, MAX(price) as max_price FROM products GROUP BY supplier_id HAVING AVG(price) > 15 AND MAX(price) < 50",
        "SELECT order_id, SUM(quantity * unit_price) as line_total FROM order_items GROUP BY order_id HAVING SUM(quantity * unit_price) > 50",
        "SELECT c.tier, COUNT(DISTINCT o.id) as orders, AVG(o.amount) as avg_order FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.tier HAVING COUNT(DISTINCT o.id) >= 3",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}

#[tokio::test]
async fn test_sort_elimination_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_SORT_ELIMINATION", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_SORT_ELIMINATION", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT id FROM customers ORDER BY id",
        "SELECT DISTINCT country FROM customers ORDER BY country",
        "SELECT name, country FROM customers ORDER BY name, country",
        "SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id ORDER BY c.name, o.amount",
        "SELECT country, COUNT(*) as cnt FROM customers GROUP BY country ORDER BY country",
        "SELECT category, SUM(price) as total FROM products GROUP BY category ORDER BY category",
        "SELECT * FROM (SELECT id, name FROM customers ORDER BY id) sub ORDER BY id",
    ];

    for query in queries {
        let result_on = session_on.execute_sql(query).await.unwrap();
        let result_off = session_off.execute_sql(query).await.unwrap();
        assert_eq!(
            result_on, result_off,
            "Sort elimination equivalence failed for: {}",
            query
        );
    }
}

#[tokio::test]
async fn test_limit_pushdown_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_LIMIT_PUSHDOWN", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_LIMIT_PUSHDOWN", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT id, name FROM customers ORDER BY id LIMIT 3",
        "SELECT * FROM orders ORDER BY amount DESC LIMIT 5",
        "SELECT country, COUNT(*) as cnt FROM customers GROUP BY country ORDER BY cnt DESC LIMIT 2",
        "SELECT c.name, SUM(o.amount) as total FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name ORDER BY total DESC LIMIT 5",
        "SELECT * FROM products ORDER BY price LIMIT 3 OFFSET 2",
        "SELECT category, AVG(price) as avg FROM products GROUP BY category ORDER BY avg DESC LIMIT 2",
        "SELECT c.country, COUNT(*) as cnt FROM customers c JOIN orders o ON c.id = o.customer_id WHERE o.status = 'completed' GROUP BY c.country ORDER BY cnt DESC LIMIT 3",
    ];

    for query in queries {
        let result_on = session_on.execute_sql(query).await.unwrap();
        let result_off = session_off.execute_sql(query).await.unwrap();
        assert_eq!(
            result_on, result_off,
            "Limit pushdown equivalence failed for: {}",
            query
        );
    }
}

#[tokio::test]
async fn test_topn_pushdown_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_TOPN_PUSHDOWN", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_TOPN_PUSHDOWN", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT * FROM customers ORDER BY id LIMIT 3",
        "SELECT * FROM orders ORDER BY amount DESC LIMIT 5",
        "SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id ORDER BY total DESC LIMIT 3",
        "SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id ORDER BY o.amount DESC LIMIT 5",
        "SELECT p.category, COUNT(*) as cnt FROM products p JOIN orders o ON p.id = o.product_id GROUP BY p.category ORDER BY cnt DESC LIMIT 2",
        "SELECT c.name, COUNT(*) as orders, SUM(o.amount) as total FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name ORDER BY total DESC LIMIT 5",
        "SELECT * FROM (SELECT * FROM customers WHERE country = 'USA') sub ORDER BY id LIMIT 2",
    ];

    for query in queries {
        let result_on = session_on.execute_sql(query).await.unwrap();
        let result_off = session_off.execute_sql(query).await.unwrap();
        assert_eq!(
            result_on, result_off,
            "TopN pushdown equivalence failed for: {}",
            query
        );
    }
}

#[tokio::test]
async fn test_subquery_unnesting_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_SUBQUERY_UNNESTING", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_SUBQUERY_UNNESTING", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT * FROM customers WHERE id IN (SELECT customer_id FROM orders)",
        "SELECT * FROM customers WHERE id NOT IN (SELECT customer_id FROM orders WHERE status = 'cancelled')",
        "SELECT * FROM products WHERE id IN (SELECT product_id FROM orders WHERE amount > 150)",
        "SELECT * FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE product_id IN (SELECT id FROM products WHERE category = 'Electronics'))",
        "SELECT * FROM suppliers WHERE id IN (SELECT supplier_id FROM products WHERE id IN (SELECT product_id FROM orders))",
        "SELECT * FROM customers WHERE country IN (SELECT country FROM suppliers WHERE rating > 4.0)",
        "SELECT * FROM products WHERE category IN (SELECT DISTINCT category FROM products WHERE price > 20)",
        "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE tier = 'gold') AND product_id IN (SELECT id FROM products WHERE price > 10)",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}

#[tokio::test]
async fn test_decorrelation_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_DECORRELATION", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_DECORRELATION", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT c.name, (SELECT SUM(o.amount) FROM orders o WHERE o.customer_id = c.id) as total FROM customers c",
        "SELECT c.name, (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) as order_count FROM customers c",
        "SELECT c.name, (SELECT AVG(o.amount) FROM orders o WHERE o.customer_id = c.id) as avg_amount FROM customers c",
        "SELECT c.name, (SELECT MIN(o.amount) FROM orders o WHERE o.customer_id = c.id) as min_order FROM customers c",
        "SELECT c.name, (SELECT MAX(o.amount) FROM orders o WHERE o.customer_id = c.id) as max_order FROM customers c",
        "SELECT p.name, (SELECT COUNT(*) FROM order_items oi WHERE oi.product_id = p.id) as times_ordered FROM products p",
        "SELECT p.name, (SELECT SUM(oi.quantity) FROM order_items oi WHERE oi.product_id = p.id) as total_quantity FROM products p",
        "SELECT s.name, (SELECT COUNT(*) FROM products p WHERE p.supplier_id = s.id) as product_count FROM suppliers s",
        "SELECT c.name, c.country, (SELECT SUM(o.amount) FROM orders o WHERE o.customer_id = c.id) as total, (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) as cnt FROM customers c WHERE c.tier = 'gold'",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}

#[tokio::test]
async fn test_aggregate_pushdown_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_AGGREGATE_PUSHDOWN", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_AGGREGATE_PUSHDOWN", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT country, SUM(id) as total FROM customers GROUP BY country",
        "SELECT customer_id, COUNT(*) as cnt, SUM(amount) as total FROM orders GROUP BY customer_id",
        "SELECT status, MIN(amount), MAX(amount) FROM orders GROUP BY status",
        "SELECT category, supplier_id, COUNT(*) as cnt, AVG(price) as avg FROM products GROUP BY category, supplier_id",
        "SELECT c.country, SUM(o.amount) as total FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.country",
        "SELECT p.category, COUNT(*) as orders, SUM(o.amount) as revenue FROM products p JOIN orders o ON p.id = o.product_id GROUP BY p.category",
        "SELECT c.tier, p.category, COUNT(*) as cnt FROM customers c JOIN orders o ON c.id = o.customer_id JOIN products p ON o.product_id = p.id GROUP BY c.tier, p.category",
        "SELECT order_id, COUNT(*) as items, SUM(quantity) as units, SUM(quantity * unit_price) as total FROM order_items GROUP BY order_id",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}

#[tokio::test]
async fn test_outer_to_inner_join_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_OUTER_TO_INNER_JOIN", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_OUTER_TO_INNER_JOIN", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT c.name, o.amount FROM customers c LEFT JOIN orders o ON c.id = o.customer_id WHERE o.amount > 100",
        "SELECT c.name, o.status FROM customers c LEFT JOIN orders o ON c.id = o.customer_id WHERE o.status = 'completed'",
        "SELECT c.name, p.name as product FROM customers c LEFT JOIN orders o ON c.id = o.customer_id LEFT JOIN products p ON o.product_id = p.id WHERE p.category = 'Electronics'",
        "SELECT c.name, o.amount, p.price FROM customers c LEFT JOIN orders o ON c.id = o.customer_id LEFT JOIN products p ON o.product_id = p.id WHERE o.amount > 100 AND p.price > 10",
        "SELECT c.country, COUNT(*) FROM customers c LEFT JOIN orders o ON c.id = o.customer_id WHERE o.id IS NOT NULL GROUP BY c.country",
        "SELECT p.category, SUM(o.amount) FROM products p LEFT JOIN orders o ON p.id = o.product_id WHERE o.status = 'completed' GROUP BY p.category",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}

#[tokio::test]
async fn test_predicate_inference_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_PREDICATE_INFERENCE", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_PREDICATE_INFERENCE", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id WHERE c.id = 1",
        "SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id WHERE o.customer_id = 2",
        "SELECT c.name, o.amount, p.name FROM customers c JOIN orders o ON c.id = o.customer_id JOIN products p ON o.product_id = p.id WHERE c.id = 3",
        "SELECT c.name, p.name FROM customers c JOIN orders o ON c.id = o.customer_id JOIN products p ON o.product_id = p.id WHERE o.product_id = 1",
        "SELECT c.country, s.country FROM customers c JOIN orders o ON c.id = o.customer_id JOIN products p ON o.product_id = p.id JOIN suppliers s ON p.supplier_id = s.id WHERE c.country = 'USA'",
        "SELECT * FROM order_items oi JOIN orders o ON oi.order_id = o.id WHERE oi.order_id = 5",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}

#[tokio::test]
async fn test_join_elimination_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_JOIN_ELIMINATION", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_JOIN_ELIMINATION", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT c.name FROM customers c JOIN customers c2 ON c.id = c2.id",
        "SELECT c.* FROM customers c LEFT JOIN customers c2 ON c.id = c2.id",
        "SELECT o.* FROM orders o JOIN orders o2 ON o.id = o2.id WHERE o.status = 'completed'",
        "SELECT p.name, p.price FROM products p LEFT JOIN products p2 ON p.id = p2.id WHERE p.category = 'Electronics'",
        "SELECT c.name, c.country FROM customers c JOIN customers c2 ON c.id = c2.id WHERE c.tier = 'gold'",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}

#[tokio::test]
async fn test_sort_pushdown_project_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_SORT_PUSHDOWN_PROJECT", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_SORT_PUSHDOWN_PROJECT", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT name FROM customers ORDER BY name",
        "SELECT id, name FROM customers ORDER BY id DESC",
        "SELECT name, country FROM customers ORDER BY country, name",
        "SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id ORDER BY c.name",
        "SELECT category, SUM(price) as total FROM products GROUP BY category ORDER BY total DESC",
        "SELECT name FROM (SELECT * FROM customers WHERE tier = 'gold') sub ORDER BY name",
        "SELECT * FROM (SELECT name, country FROM customers) sub ORDER BY country, name",
    ];

    for query in queries {
        let result_on = session_on.execute_sql(query).await.unwrap();
        let result_off = session_off.execute_sql(query).await.unwrap();
        assert_eq!(
            result_on, result_off,
            "Sort pushdown project equivalence failed for: {}",
            query
        );
    }
}

#[tokio::test]
async fn test_empty_propagation_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_EMPTY_PROPAGATION", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_EMPTY_PROPAGATION", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT * FROM customers WHERE FALSE",
        "SELECT * FROM customers WHERE 1 = 0",
        "SELECT * FROM (SELECT * FROM customers WHERE FALSE) sub",
        "SELECT * FROM customers c JOIN (SELECT * FROM orders WHERE FALSE) o ON c.id = o.customer_id",
        "SELECT country, COUNT(*) FROM customers WHERE 1 = 0 GROUP BY country",
        "SELECT * FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE FALSE)",
        "SELECT c.name FROM customers c WHERE EXISTS (SELECT 1 FROM orders WHERE FALSE)",
        "WITH empty_cte AS (SELECT * FROM customers WHERE FALSE) SELECT * FROM empty_cte",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}

#[tokio::test]
async fn test_short_circuit_ordering_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_SHORT_CIRCUIT_ORDERING", true);
    let session_off = create_session_with_optimizer("OPTIMIZER_SHORT_CIRCUIT_ORDERING", false);
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT * FROM customers WHERE id < 3 AND country = 'USA'",
        "SELECT * FROM orders WHERE status = 'completed' AND amount > 100",
        "SELECT * FROM customers WHERE name LIKE 'A%' OR id = 1",
        "SELECT * FROM products WHERE price > 20.0 AND category = 'Electronics' AND supplier_id = 1",
        "SELECT * FROM orders WHERE (status = 'completed' OR status = 'pending') AND amount > 100.0 AND customer_id < 5",
        "SELECT * FROM customers WHERE tier = 'gold' AND (country = 'USA' OR country = 'UK') AND created_year >= 2019",
        "SELECT c.* FROM customers c JOIN orders o ON c.id = o.customer_id WHERE c.country = 'USA' AND o.status = 'completed' AND o.amount > 150.0",
        "SELECT * FROM products WHERE (category = 'Electronics' OR category = 'Tools') AND price BETWEEN 10.0 AND 40.0 AND supplier_id IN (1, 2)",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}

#[tokio::test]
async fn test_complex_multi_optimizer_queries() {
    let session_on = create_session_with_optimizer("OPTIMIZER_LEVEL", true);
    let session_off = YachtSQLEngine::new().create_session();
    session_off
        .session()
        .set_variable("OPTIMIZER_LEVEL", Value::String("NONE".to_string()));
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "WITH gold_customers AS (
            SELECT c.*, COUNT(o.id) as order_count, SUM(o.amount) as total_spent
            FROM customers c
            LEFT JOIN orders o ON c.id = o.customer_id
            WHERE c.tier = 'gold'
            GROUP BY c.id, c.name, c.country, c.tier, c.created_year
        )
        SELECT * FROM gold_customers WHERE total_spent > 200",

        "SELECT c.name, c.country,
               (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) as order_count,
               (SELECT SUM(amount) FROM orders o WHERE o.customer_id = c.id) as total
        FROM customers c
        WHERE c.id IN (SELECT customer_id FROM orders WHERE status = 'completed')",

        "SELECT p.category, COUNT(DISTINCT o.customer_id) as unique_customers, SUM(o.amount) as revenue
        FROM products p
        JOIN orders o ON p.id = o.product_id
        JOIN customers c ON o.customer_id = c.id
        WHERE c.tier IN ('gold', 'silver') AND o.status = 'completed'
        GROUP BY p.category
        HAVING COUNT(DISTINCT o.customer_id) >= 2",

        "SELECT c.country, p.category, SUM(oi.quantity * oi.unit_price) as total_value
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        JOIN order_items oi ON o.id = oi.order_id
        JOIN products p ON oi.product_id = p.id
        WHERE o.status != 'cancelled'
        GROUP BY c.country, p.category",

        "SELECT sub.country, sub.total_orders, sub.avg_order_value
        FROM (
            SELECT c.country, COUNT(*) as total_orders, AVG(o.amount) as avg_order_value
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
            WHERE o.status = 'completed'
            GROUP BY c.country
        ) sub
        WHERE sub.total_orders >= 2",

        "SELECT s.name as supplier, COUNT(DISTINCT p.id) as products, COUNT(DISTINCT o.id) as orders
        FROM suppliers s
        JOIN products p ON s.id = p.supplier_id
        JOIN orders o ON p.id = o.product_id
        GROUP BY s.name",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}

#[tokio::test]
async fn test_window_function_optimizer_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_LEVEL", true);
    let session_off = YachtSQLEngine::new().create_session();
    session_off
        .session()
        .set_variable("OPTIMIZER_LEVEL", Value::String("NONE".to_string()));
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT name, country, ROW_NUMBER() OVER (PARTITION BY country ORDER BY name) as rn FROM customers",
        "SELECT id, amount, SUM(amount) OVER (ORDER BY id) as running_total FROM orders",
        "SELECT c.name, o.amount, RANK() OVER (PARTITION BY c.id ORDER BY o.amount DESC) as order_rank
         FROM customers c JOIN orders o ON c.id = o.customer_id",
        "SELECT name, price, category, AVG(price) OVER (PARTITION BY category) as category_avg FROM products",
        "SELECT customer_id, amount, LAG(amount) OVER (PARTITION BY customer_id ORDER BY id) as prev_amount FROM orders",
        "SELECT id, amount, status, FIRST_VALUE(id) OVER (PARTITION BY status ORDER BY amount DESC) as top_order FROM orders",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}

#[tokio::test]
async fn test_set_operations_optimizer_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_LEVEL", true);
    let session_off = YachtSQLEngine::new().create_session();
    session_off
        .session()
        .set_variable("OPTIMIZER_LEVEL", Value::String("NONE".to_string()));
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "SELECT country FROM customers WHERE tier = 'gold' UNION SELECT country FROM suppliers",
        "SELECT id FROM customers UNION ALL SELECT customer_id FROM orders",
        "SELECT country FROM customers INTERSECT SELECT country FROM suppliers",
        "SELECT country FROM customers EXCEPT SELECT country FROM suppliers WHERE rating < 4.0",
        "SELECT name FROM customers WHERE country = 'USA' UNION SELECT name FROM customers WHERE tier = 'gold'",
        "(SELECT category FROM products WHERE price > 20) UNION (SELECT category FROM products WHERE supplier_id = 1)",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}

#[tokio::test]
async fn test_cte_optimizer_equivalence() {
    let session_on = create_session_with_optimizer("OPTIMIZER_LEVEL", true);
    let session_off = YachtSQLEngine::new().create_session();
    session_off
        .session()
        .set_variable("OPTIMIZER_LEVEL", Value::String("NONE".to_string()));
    setup_test_tables(&session_on).await;
    setup_test_tables(&session_off).await;

    let queries = [
        "WITH usa_customers AS (SELECT * FROM customers WHERE country = 'USA')
         SELECT * FROM usa_customers WHERE tier = 'gold'",

        "WITH order_totals AS (SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id)
         SELECT c.name, ot.total FROM customers c JOIN order_totals ot ON c.id = ot.customer_id",

        "WITH product_orders AS (
            SELECT p.id, p.name, p.category, COUNT(o.id) as order_count
            FROM products p LEFT JOIN orders o ON p.id = o.product_id
            GROUP BY p.id, p.name, p.category
         )
         SELECT * FROM product_orders WHERE order_count > 0",

        "WITH tier_stats AS (
            SELECT c.tier, COUNT(*) as customers, SUM(o.amount) as revenue
            FROM customers c JOIN orders o ON c.id = o.customer_id
            GROUP BY c.tier
         )
         SELECT * FROM tier_stats WHERE revenue > 500",

        "WITH cte1 AS (SELECT * FROM customers WHERE tier = 'gold'),
              cte2 AS (SELECT * FROM orders WHERE status = 'completed')
         SELECT c.name, o.amount FROM cte1 c JOIN cte2 o ON c.id = o.customer_id",
    ];

    for query in queries {
        assert_optimizer_equivalence(&session_on, &session_off, query).await;
    }
}
