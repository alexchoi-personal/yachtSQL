use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::create_session;

async fn setup_table(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE items (id INT64, name STRING, price INT64, category STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 'Apple', 100, 'Fruit'), (2, 'Banana', 50, 'Fruit'), (3, 'Carrot', 75, 'Vegetable'), (4, 'Date', 200, 'Fruit'), (5, 'Eggplant', 125, 'Vegetable')")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_projection_field_name() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql("SELECT name, price FROM items ORDER BY price DESC LIMIT 3")
        .await
        .unwrap();
    assert_table_eq!(result, [["Date", 200], ["Eggplant", 125], ["Apple", 100],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_column_not_in_projection() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql("SELECT name FROM items ORDER BY id DESC LIMIT 3")
        .await
        .unwrap();
    assert_table_eq!(result, [["Eggplant"], ["Date"], ["Carrot"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_compound_identifier_alias_match() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql(
            "SELECT name, price * 2 AS doubled FROM items ORDER BY items.doubled DESC LIMIT 3",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["Date", 400], ["Eggplant", 250], ["Apple", 200],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_compound_identifier_field_match() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql("SELECT name, price FROM items ORDER BY items.price ASC LIMIT 3")
        .await
        .unwrap();
    assert_table_eq!(result, [["Banana", 50], ["Carrot", 75], ["Apple", 100],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_compound_identifier_fallback_to_input() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql("SELECT name FROM items ORDER BY items.category, items.id LIMIT 5")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [["Apple"], ["Banana"], ["Date"], ["Carrot"], ["Eggplant"],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_non_identifier_expression() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql("SELECT name, price FROM items ORDER BY price * 2 + id DESC LIMIT 3")
        .await
        .unwrap();
    assert_table_eq!(result, [["Date", 200], ["Eggplant", 125], ["Apple", 100],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_aggregate_not_in_input_schema() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql("SELECT category, AVG(price) AS avg_price FROM items GROUP BY category ORDER BY AVG(price) DESC")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [["Fruit", 116.66666666666667_f64], ["Vegetable", 100.0_f64],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_aggregate_alias_resolution() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql(
            "SELECT category, COUNT(*) AS cnt FROM items GROUP BY category ORDER BY cnt DESC",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["Fruit", 3], ["Vegetable", 2],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_case_with_operand() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql(
            "SELECT name, category FROM items ORDER BY CASE category WHEN 'Fruit' THEN 1 ELSE 2 END, price DESC",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Date", "Fruit"],
            ["Apple", "Fruit"],
            ["Banana", "Fruit"],
            ["Eggplant", "Vegetable"],
            ["Carrot", "Vegetable"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_case_with_column_reference() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql(
            "SELECT name, price FROM items ORDER BY CASE WHEN price > 100 THEN price ELSE price * 10 END DESC LIMIT 3",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["Apple", 100], ["Carrot", 75], ["Banana", 50],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_binary_op_with_columns() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql("SELECT name, id, price FROM items ORDER BY price - id DESC LIMIT 3")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [["Date", 4, 200], ["Eggplant", 5, 125], ["Apple", 1, 100],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_unary_op_with_column() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql("SELECT name, price FROM items ORDER BY -price ASC LIMIT 3")
        .await
        .unwrap();
    assert_table_eq!(result, [["Date", 200], ["Eggplant", 125], ["Apple", 100],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_scalar_function_with_column() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql("SELECT name, price FROM items ORDER BY ABS(price - 100) ASC LIMIT 3")
        .await
        .unwrap();
    assert_table_eq!(result, [["Apple", 100], ["Carrot", 75], ["Eggplant", 125],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_cast_with_column() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql(
            "SELECT name, price FROM items ORDER BY CAST(price AS FLOAT64) / 10.0 DESC LIMIT 3",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["Date", 200], ["Eggplant", 125], ["Apple", 100],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_literal_expression() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql("SELECT name FROM items ORDER BY 1 + 1, name ASC LIMIT 3")
        .await
        .unwrap();
    assert_table_eq!(result, [["Apple"], ["Banana"], ["Carrot"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_desc_nulls_last() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nullable (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1, 30), (2, NULL), (3, 10), (4, NULL), (5, 20)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id FROM nullable ORDER BY value DESC NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [5], [3], [2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_asc_nulls_first() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nullable (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1, 30), (2, NULL), (3, 10)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id FROM nullable ORDER BY value NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [3], [1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_with_subquery() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql("SELECT name, price FROM items WHERE price > 50 ORDER BY price DESC LIMIT 3")
        .await
        .unwrap();
    assert_table_eq!(result, [["Date", 200], ["Eggplant", 125], ["Apple", 100],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_with_alias_expression() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql("SELECT name, price + id AS total FROM items ORDER BY total DESC LIMIT 3")
        .await
        .unwrap();
    assert_table_eq!(result, [["Date", 204], ["Eggplant", 130], ["Apple", 101],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_mixed_directions() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql("SELECT name, category, price FROM items ORDER BY category ASC, price DESC")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Date", "Fruit", 200],
            ["Apple", "Fruit", 100],
            ["Banana", "Fruit", 50],
            ["Eggplant", "Vegetable", 125],
            ["Carrot", "Vegetable", 75],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_simple_column_on_non_project() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (x INT64, y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, 20), (3, 15)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT * FROM data ORDER BY y DESC")
        .await
        .unwrap();
    assert_table_eq!(result, [[2, 20], [3, 15], [1, 10],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_with_values_clause() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT * FROM (VALUES (3, 'c'), (1, 'a'), (2, 'b')) AS t ORDER BY column1")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [2, "b"], [3, "c"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_set_operation() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t1 (x INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE t2 (x INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t1 VALUES (3), (1)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t2 VALUES (2), (4)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT x FROM t1 UNION ALL SELECT x FROM t2 ORDER BY x")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3], [4],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_column_with_no_index_match() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql("SELECT id, name FROM items ORDER BY 1 + price, name")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [2, "Banana"],
            [3, "Carrot"],
            [1, "Apple"],
            [5, "Eggplant"],
            [4, "Date"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_nested_function() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql(
            "SELECT name, price FROM items ORDER BY ABS(CAST(price AS INT64) - 100) LIMIT 3",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["Apple", 100], ["Carrot", 75], ["Eggplant", 125],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_min_aggregate_direct() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql("SELECT category, MIN(price) AS min_price FROM items GROUP BY category ORDER BY MIN(price) DESC")
        .await
        .unwrap();
    assert_table_eq!(result, [["Vegetable", 75], ["Fruit", 50],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_max_aggregate_direct() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql("SELECT category, MAX(price) AS max_price FROM items GROUP BY category ORDER BY MAX(price) ASC")
        .await
        .unwrap();
    assert_table_eq!(result, [["Vegetable", 125], ["Fruit", 200],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_case_without_else() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql(
            "SELECT name, price FROM items ORDER BY CASE WHEN price > 100 THEN 1 WHEN price > 50 THEN 2 END NULLS LAST, name",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Date", 200],
            ["Eggplant", 125],
            ["Apple", 100],
            ["Carrot", 75],
            ["Banana", 50],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_with_distinct() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql("SELECT DISTINCT category FROM items ORDER BY category DESC")
        .await
        .unwrap();
    assert_table_eq!(result, [["Vegetable"], ["Fruit"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_complex_expression_with_alias() {
    let session = create_session();
    setup_table(&session).await;
    let result = session
        .execute_sql(
            "SELECT name, price * 2 + id AS score FROM items ORDER BY price * 2 + id DESC LIMIT 3",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["Date", 404], ["Eggplant", 255], ["Apple", 201],]);
}
