use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_merge_not_matched_by_source_update() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE target (id INT64, name STRING, active BOOL)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE source (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO target VALUES (1, 'Active', true), (2, 'Inactive', true), (3, 'Other', true)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO source VALUES (1, 'Active')")
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO target T
            USING source S
            ON T.id = S.id
            WHEN MATCHED THEN UPDATE SET name = S.name
            WHEN NOT MATCHED BY SOURCE THEN UPDATE SET active = false",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, active FROM target ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, true], [2, false], [3, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_not_matched_by_source_with_condition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE target (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE source (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO target VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO source VALUES (1)")
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO target T
            USING source S
            ON T.id = S.id
            WHEN NOT MATCHED BY SOURCE AND T.value > 15 THEN UPDATE SET value = 0",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, value FROM target ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 10], [2, 0], [3, 0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_with_derived_source_alias() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE target (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO target VALUES (1, 10), (2, 20)")
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO target T
            USING (SELECT 1 AS id, 100 AS value UNION ALL SELECT 3, 300) AS S
            ON T.id = S.id
            WHEN MATCHED THEN UPDATE SET value = S.value
            WHEN NOT MATCHED THEN INSERT (id, value) VALUES (S.id, S.value)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, value FROM target ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 100], [2, 20], [3, 300]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_with_alias() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE products (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO products VALUES (1, 'Widget'), (2, 'Gadget'), (3, 'Gizmo')")
        .await
        .unwrap();

    session
        .execute_sql("DELETE FROM products AS p WHERE p.id > 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM products")
        .await
        .unwrap();

    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_with_alias_no_where() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE counters (id INT64, count INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO counters VALUES (1, 0), (2, 0)")
        .await
        .unwrap();

    session
        .execute_sql("UPDATE counters AS c SET count = c.count + 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, count FROM counters ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 1], [2, 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_insert_row_from_derived() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE target (id INT64, name STRING, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO target T
            USING (SELECT 1 AS id, 'first' AS name, 100 AS value) AS S
            ON T.id = S.id
            WHEN NOT MATCHED THEN INSERT ROW",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name, value FROM target")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "first", 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_multiple_not_matched_clauses() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE target (id INT64, category STRING, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE source (id INT64, category STRING, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO source VALUES (1, 'A', 100), (2, 'B', 200), (3, 'C', 300)")
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO target T
            USING source S
            ON T.id = S.id
            WHEN NOT MATCHED AND S.category = 'A' THEN INSERT (id, category, value) VALUES (S.id, 'CategoryA', S.value)
            WHEN NOT MATCHED AND S.category = 'B' THEN INSERT (id, category, value) VALUES (S.id, 'CategoryB', S.value)
            WHEN NOT MATCHED THEN INSERT (id, category, value) VALUES (S.id, 'Other', S.value)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, category FROM target ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "CategoryA"], [2, "CategoryB"], [3, "Other"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_from_with_alias_reference() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE orders (id INT64, total FLOAT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE discounts (order_id INT64, discount_pct FLOAT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO orders VALUES (1, 100.0), (2, 200.0)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO discounts VALUES (1, 0.1), (2, 0.2)")
        .await
        .unwrap();

    session
        .execute_sql(
            "UPDATE orders o
            SET total = o.total * (1 - d.discount_pct)
            FROM discounts d
            WHERE o.id = d.order_id",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, total FROM orders ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 90.0], [2, 160.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_complex_on_condition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE target (id INT64, category STRING, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE source (id INT64, category STRING, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO target VALUES (1, 'A', 10), (2, 'B', 20)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO source VALUES (1, 'A', 100), (2, 'C', 200)")
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO target T
            USING source S
            ON T.id = S.id AND T.category = S.category
            WHEN MATCHED THEN UPDATE SET value = S.value
            WHEN NOT MATCHED BY TARGET THEN INSERT (id, category, value) VALUES (S.id, S.category, S.value)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, category, value FROM target ORDER BY id, category")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "A", 100], [2, "B", 20], [2, "C", 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_empty_insert_values() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE target (id INT64 DEFAULT 0, name STRING DEFAULT 'default')")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE source (dummy INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO source VALUES (1)")
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO target T
            USING source S
            ON FALSE
            WHEN NOT MATCHED THEN INSERT ROW",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM target")
        .await
        .unwrap();

    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_without_from_with_alias() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE items (id INT64, active BOOL)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO items VALUES (1, true), (2, false), (3, true)")
        .await
        .unwrap();

    session
        .execute_sql("DELETE items i WHERE i.active = false")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM items ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_not_matched_by_source_delete_with_condition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE target (id INT64, status STRING)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE source (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO target VALUES (1, 'active'), (2, 'inactive'), (3, 'active')")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO source VALUES (1)")
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO target T
            USING source S
            ON T.id = S.id
            WHEN NOT MATCHED BY SOURCE AND T.status = 'inactive' THEN DELETE",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM target ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_source_without_alias() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE target (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE source (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO source VALUES (1, 100)")
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO target
            USING source
            ON target.id = source.id
            WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, value FROM target")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_target_without_alias() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE target (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE source (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO target VALUES (1, 10)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO source VALUES (1, 100)")
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO target
            USING source S
            ON target.id = S.id
            WHEN MATCHED THEN UPDATE SET value = S.value",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, value FROM target")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_no_from_clause() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE items (id INT64, quantity INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO items VALUES (1, 10), (2, 20)")
        .await
        .unwrap();

    session
        .execute_sql("UPDATE items SET quantity = quantity * 2")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, quantity FROM items ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 20], [2, 40]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_derived_subquery_no_alias() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE target (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO target T
            USING (SELECT 1 AS id, 100 AS value) src
            ON T.id = src.id
            WHEN NOT MATCHED THEN INSERT (id, value) VALUES (src.id, src.value)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, value FROM target")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_multiple_matched_clauses_with_conditions() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE target (id INT64, status STRING, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE source (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO target VALUES (1, 'pending', 10), (2, 'active', 20), (3, 'inactive', 30)",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO source VALUES (1, 100), (2, 200), (3, 300)")
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO target T
            USING source S
            ON T.id = S.id
            WHEN MATCHED AND T.status = 'pending' THEN UPDATE SET status = 'active', value = S.value
            WHEN MATCHED AND T.status = 'inactive' THEN DELETE
            WHEN MATCHED THEN UPDATE SET value = S.value",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, status, value FROM target ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "active", 100], [2, "active", 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_with_correlated_subquery() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE orders (id INT64, customer_id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE customers (id INT64, active BOOL)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO orders VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO customers VALUES (10, true), (20, false), (30, true)")
        .await
        .unwrap();

    session
        .execute_sql(
            "DELETE FROM orders
            WHERE customer_id IN (SELECT id FROM customers WHERE active = false)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM orders ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_with_subquery_in_where() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE products (id INT64, price FLOAT64, category STRING)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE promotions (category STRING, discount FLOAT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO products VALUES (1, 100.0, 'electronics'), (2, 50.0, 'clothing'), (3, 200.0, 'electronics')")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO promotions VALUES ('electronics', 0.1)")
        .await
        .unwrap();

    session
        .execute_sql(
            "UPDATE products
            SET price = price * 0.9
            WHERE category IN (SELECT category FROM promotions)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, price FROM products ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 90.0], [2, 50.0], [3, 180.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_not_matched_by_source_multiple_conditions() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE target (id INT64, value INT64, status STRING)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE source (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO target VALUES (1, 100, 'active'), (2, 50, 'active'), (3, 25, 'active'), (4, 200, 'inactive')")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO source VALUES (1)")
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO target T
            USING source S
            ON T.id = S.id
            WHEN NOT MATCHED BY SOURCE AND T.value < 30 THEN DELETE
            WHEN NOT MATCHED BY SOURCE AND T.status = 'inactive' THEN UPDATE SET status = 'archived'
            WHEN NOT MATCHED BY SOURCE THEN UPDATE SET status = 'orphaned'",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, status FROM target ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "active"], [2, "orphaned"], [4, "archived"]]);
}
