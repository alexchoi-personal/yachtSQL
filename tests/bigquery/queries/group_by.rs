use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::{create_session, null};

async fn setup_sales_table(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE sales (id INT64, product STRING, category STRING, amount INT64, quantity INT64)").await
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES (1, 'Widget', 'Electronics', 100, 2), (2, 'Gadget', 'Electronics', 200, 1), (3, 'Chair', 'Furniture', 150, 3), (4, 'Table', 'Furniture', 300, 1), (5, 'Widget', 'Electronics', 100, 5)").await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_single_column() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 400], ["Furniture", 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_count() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, COUNT(*) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 3], ["Furniture", 2],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_avg() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, AVG(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", 133.33333333333334], ["Furniture", 225.0]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_min_max() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, MIN(amount), MAX(amount) FROM sales GROUP BY category ORDER BY category").await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", 100, 200], ["Furniture", 150, 300],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_multiple_columns() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, product, SUM(quantity) FROM sales GROUP BY category, product ORDER BY category, product").await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "Gadget", 1],
            ["Electronics", "Widget", 7],
            ["Furniture", "Chair", 3],
            ["Furniture", "Table", 1],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_having() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) FROM sales GROUP BY category HAVING SUM(amount) > 400 ORDER BY category").await
        .unwrap();

    assert_table_eq!(result, [["Furniture", 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_having_count() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, COUNT(*) FROM sales GROUP BY category HAVING COUNT(*) > 2 ORDER BY category").await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 3],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_where_and_having() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) FROM sales WHERE quantity > 1 GROUP BY category HAVING SUM(amount) > 100 ORDER BY category").await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 200], ["Furniture", 150],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_all_rows_same_group() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE items (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (10), (20), (30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT SUM(value), COUNT(*), AVG(value) FROM items")
        .await
        .unwrap();

    assert_table_eq!(result, [[60, 3, 20.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_null_values() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE data (category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES ('A', 10), ('A', 20), (NULL, 30), (NULL, 40)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, SUM(value) FROM data GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 30], [null(), 70]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_distinct() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, COUNT(DISTINCT product) FROM sales GROUP BY category ORDER BY category").await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 2], ["Furniture", 2],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_cast_in_projection() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CAST(category AS STRING) AS cat_str, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "Electronics", 400],
            ["Furniture", "Furniture", 450],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_scalar_function_in_projection() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, UPPER(category), SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "ELECTRONICS", 400],
            ["Furniture", "FURNITURE", 450],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_unary_negation_in_projection() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, -SUM(amount) AS neg_total FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", -400], ["Furniture", -450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_case_expression_in_projection() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CASE WHEN category = 'Electronics' THEN 'Tech' ELSE 'Other' END AS type, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", "Tech", 400], ["Furniture", "Other", 450],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_case_operand_in_projection() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CASE category WHEN 'Electronics' THEN 1 WHEN 'Furniture' THEN 2 END AS cat_num, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 1, 400], ["Furniture", 2, 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_constant_expression() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, 'constant', SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "constant", 400],
            ["Furniture", "constant", 450],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_constant_unary() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT category, -1, SUM(amount) FROM sales GROUP BY category ORDER BY category",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", -1, 400], ["Furniture", -1, 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_constant_cast() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CAST(100 AS FLOAT64), SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", 100.0, 400], ["Furniture", 100.0, 450],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_constant_binary_op() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT category, 1 + 2, SUM(amount) FROM sales GROUP BY category ORDER BY category",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 3, 400], ["Furniture", 3, 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_constant_function() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CONCAT('prefix_', 'suffix'), SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "prefix_suffix", 400],
            ["Furniture", "prefix_suffix", 450],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_constant_case() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CASE 1 WHEN 1 THEN 'one' ELSE 'other' END, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", "one", 400], ["Furniture", "one", 450],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_constant_array() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, [1, 2, 3], SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", [1, 2, 3], 400],
            ["Furniture", [1, 2, 3], 450],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_constant_struct() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, STRUCT(1 AS a, 2 AS b), SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", (1, 2), 400], ["Furniture", (1, 2), 450],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_qualified_column() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT sales.category, SUM(sales.amount) FROM sales GROUP BY sales.category ORDER BY sales.category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 400], ["Furniture", 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_subquery_in_projection() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, (SELECT MAX(amount) FROM sales) AS max_all, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", 300, 400], ["Furniture", 300, 450],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_binary_op_on_group_column() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, category || '_suffix' AS cat_suffix, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "Electronics_suffix", 400],
            ["Furniture", "Furniture_suffix", 450],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_alias_in_projection() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category AS cat, UPPER(category) AS upper_cat, SUM(amount) AS total FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "ELECTRONICS", 400],
            ["Furniture", "FURNITURE", 450],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_complex_expression() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT UPPER(category), SUM(amount) FROM sales GROUP BY UPPER(category) ORDER BY SUM(amount)")
        .await
        .unwrap();

    assert_table_eq!(result, [["ELECTRONICS", 400], ["FURNITURE", 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_nested_scalar_functions() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, LOWER(category) AS lower_cat, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "electronics", 400],
            ["Furniture", "furniture", 450],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_column_with_table_alias() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT s.category, SUM(s.amount) FROM sales AS s GROUP BY s.category ORDER BY s.category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 400], ["Furniture", 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_concat_function() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CONCAT(category, '_group'), SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "Electronics_group", 400],
            ["Furniture", "Furniture_group", 450],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_length_function() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, LENGTH(category), SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 11, 400], ["Furniture", 9, 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_case_else_group_column() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CASE WHEN 1 = 2 THEN 'never' ELSE category END AS cat2, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "Electronics", 400],
            ["Furniture", "Furniture", 450],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_is_null_in_projection() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE data (category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES ('A', 10), ('A', NULL), ('B', 20), ('B', 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, value IS NULL AS is_value_null, COUNT(*) FROM data GROUP BY category, value IS NULL ORDER BY category, is_value_null")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", false, 1], ["A", true, 1], ["B", false, 2],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_between_expression() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT amount BETWEEN 100 AND 200 AS in_range, COUNT(*) FROM sales GROUP BY amount BETWEEN 100 AND 200 ORDER BY in_range")
        .await
        .unwrap();

    assert_table_eq!(result, [[false, 1], [true, 4],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_in_list_expression() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product IN ('Widget', 'Gadget') AS is_electronic, COUNT(*) FROM sales GROUP BY product IN ('Widget', 'Gadget') ORDER BY is_electronic")
        .await
        .unwrap();

    assert_table_eq!(result, [[false, 2], [true, 3],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_array_subquery_in_projection() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, ARRAY(SELECT p.product FROM sales p WHERE p.category = sales.category LIMIT 1) AS products, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", ["Widget"], 400.0],
            ["Furniture", ["Chair"], 450.0],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_subquery_exists_check() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) FROM sales WHERE EXISTS(SELECT 1 FROM sales s2 WHERE s2.category = sales.category) GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 400.0], ["Furniture", 450.0],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_in_subquery_where() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) FROM sales WHERE category IN (SELECT DISTINCT category FROM sales WHERE amount > 150) GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 400.0], ["Furniture", 450.0],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_compound_identifier_select() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT s.category, s.product, SUM(s.amount) FROM sales s GROUP BY s.category, s.product ORDER BY s.category, s.product")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "Gadget", 200],
            ["Electronics", "Widget", 200],
            ["Furniture", "Chair", 150],
            ["Furniture", "Table", 300],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_not_between() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, amount NOT BETWEEN 100 AND 150 AS outside_range, COUNT(*) FROM sales GROUP BY category, amount NOT BETWEEN 100 AND 150 ORDER BY category, outside_range")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", false, 2],
            ["Electronics", true, 1],
            ["Furniture", false, 1],
            ["Furniture", true, 1],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_not_in_list() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product NOT IN ('Widget', 'Chair') AS is_other, COUNT(*) FROM sales GROUP BY product NOT IN ('Widget', 'Chair') ORDER BY is_other")
        .await
        .unwrap();

    assert_table_eq!(result, [[false, 3], [true, 2],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_case_in_between() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CASE WHEN amount BETWEEN 100 AND 200 THEN 'Low' ELSE 'High' END AS tier, COUNT(*) FROM sales GROUP BY category, CASE WHEN amount BETWEEN 100 AND 200 THEN 'Low' ELSE 'High' END ORDER BY category, tier")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "Low", 3],
            ["Furniture", "High", 1],
            ["Furniture", "Low", 1],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_array_in_projection() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, [1, 2] AS nums, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", [1, 2], 400], ["Furniture", [1, 2], 450],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_is_not_null() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE data (category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES ('A', 10), ('A', NULL), ('B', NULL), ('B', 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, value IS NOT NULL AS has_value, COUNT(*) FROM data GROUP BY category, value IS NOT NULL ORDER BY category, has_value")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["A", false, 1],
            ["A", true, 1],
            ["B", false, 1],
            ["B", true, 1],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_column_unqualified_reference() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 400], ["Furniture", 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_column_both_tables_specified() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT sales.category AS cat, sales.product AS prod, SUM(sales.amount) FROM sales GROUP BY sales.category, sales.product ORDER BY cat, prod")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "Gadget", 200],
            ["Electronics", "Widget", 200],
            ["Furniture", "Chair", 150],
            ["Furniture", "Table", 300],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_unary_on_group_column() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, NOT (category = 'Electronics') AS is_not_electronics, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", false, 400], ["Furniture", true, 450],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_cast_on_group_column() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT amount, CAST(amount AS STRING) AS amount_str, COUNT(*) FROM sales GROUP BY amount ORDER BY amount")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [100, "100", 2],
            [150, "150", 1],
            [200, "200", 1],
            [300, "300", 1],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_nested_binary_ops() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, (amount * 2) + 10 AS transformed, COUNT(*) FROM sales GROUP BY category, (amount * 2) + 10 ORDER BY category, transformed")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", 210, 2],
            ["Electronics", 410, 1],
            ["Furniture", 310, 1],
            ["Furniture", 610, 1],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_aliased_constant() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, 42 AS constant_value, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 42, 400], ["Furniture", 42, 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_case_no_else() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CASE WHEN category = 'Electronics' THEN 'Tech' END AS tech_only, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", "Tech", 400], ["Furniture", null(), 450],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_case_when_group_column() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CASE WHEN category = 'Electronics' AND amount > 100 THEN 'High Tech' WHEN category = 'Electronics' THEN 'Low Tech' ELSE 'Other' END AS desc, SUM(amount) FROM sales GROUP BY category, CASE WHEN category = 'Electronics' AND amount > 100 THEN 'High Tech' WHEN category = 'Electronics' THEN 'Low Tech' ELSE 'Other' END ORDER BY category, desc")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "High Tech", 200],
            ["Electronics", "Low Tech", 200],
            ["Furniture", "Other", 450],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_subquery_in_case() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CASE WHEN SUM(amount) > (SELECT AVG(amount) * 2 FROM sales) THEN 'High' ELSE 'Low' END AS level, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", "High", 400], ["Furniture", "High", 450],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_subquery_in_binary_op() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) + (SELECT COUNT(*) FROM sales) AS total_plus_count FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 405], ["Furniture", 455],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_subquery_in_unary_op() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, -(SELECT MAX(amount) FROM sales) AS neg_max, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", -300, 400], ["Furniture", -300, 450],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_subquery_in_cast() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CAST((SELECT MAX(amount) FROM sales) AS STRING) AS max_str, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", "300", 400], ["Furniture", "300", 450],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_subquery_in_alias() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, (SELECT MAX(amount) FROM sales) AS aliased_max, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", 300, 400], ["Furniture", 300, 450],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_column_index_remap() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, product, category || '-' || product AS combined, SUM(amount) FROM sales GROUP BY category, product ORDER BY category, product")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "Gadget", "Electronics-Gadget", 200],
            ["Electronics", "Widget", "Electronics-Widget", 200],
            ["Furniture", "Chair", "Furniture-Chair", 150],
            ["Furniture", "Table", "Furniture-Table", 300],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_expression_with_default_case() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT ABS(amount) AS abs_amount, SUM(quantity) FROM sales GROUP BY ABS(amount) ORDER BY abs_amount")
        .await
        .unwrap();

    assert_table_eq!(result, [[100, 7], [150, 3], [200, 1], [300, 1],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_qualified_table_column_both_specified() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT sales.category, sales.product, UPPER(sales.category) AS cat_upper, SUM(sales.amount) FROM sales GROUP BY sales.category, sales.product ORDER BY sales.category, sales.product")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "Gadget", "ELECTRONICS", 200],
            ["Electronics", "Widget", "ELECTRONICS", 200],
            ["Furniture", "Chair", "FURNITURE", 150],
            ["Furniture", "Table", "FURNITURE", 300],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_struct_in_projection() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, STRUCT(1 AS x, 2 AS y) AS const_struct, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", (1, 2), 400], ["Furniture", (1, 2), 450],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_array_containing_subquery() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, ARRAY(SELECT s.product FROM sales s WHERE s.category = sales.category ORDER BY s.product LIMIT 1) AS products, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", ["Gadget"], 400.0],
            ["Furniture", ["Chair"], 450.0],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_is_null_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (grp STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES ('A', 1), ('A', NULL), ('B', 2), ('B', 3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, val IS NULL AS is_null_val, COUNT(*) FROM data GROUP BY grp, val IS NULL ORDER BY grp, is_null_val")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", false, 1], ["A", true, 1], ["B", false, 2],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_between_in_grouping() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, amount BETWEEN 100 AND 200 AS in_range, COUNT(*) FROM sales GROUP BY category, amount BETWEEN 100 AND 200 ORDER BY category, in_range")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", true, 3],
            ["Furniture", false, 1],
            ["Furniture", true, 1],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_in_list_in_grouping() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, category IN ('Electronics', 'Other') AS is_match, SUM(amount) FROM sales GROUP BY category, category IN ('Electronics', 'Other') ORDER BY category, is_match")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", true, 400], ["Furniture", false, 450],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_struct_containing_group_col() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, STRUCT(1 AS x, 2 AS y) AS const_struct, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", (1, 2), 400], ["Furniture", (1, 2), 450],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_case_subquery_in_else() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CASE WHEN 1 = 2 THEN 0 ELSE (SELECT MAX(amount) FROM sales) END AS max_else, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", 300, 400], ["Furniture", 300, 450],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_case_subquery_in_when() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CASE WHEN SUM(amount) > (SELECT AVG(amount) FROM sales) THEN 'High' WHEN SUM(amount) > 100 THEN 'Medium' END AS level, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", "High", 400], ["Furniture", "High", 450],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_scalar_function_expression() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CONCAT(category, '_total') AS concat_suffix, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "Electronics_total", 400],
            ["Furniture", "Furniture_total", 450],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_aliased_column_expression() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category AS cat, category || '_suffix' AS cat_with_suffix, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "Electronics_suffix", 400],
            ["Furniture", "Furniture_suffix", 450],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_mixed_qualified_unqualified_columns() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT sales.category, product, sales.category || '-' || product AS combo, SUM(amount) FROM sales GROUP BY sales.category, product ORDER BY sales.category, product")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "Gadget", "Electronics-Gadget", 200],
            ["Electronics", "Widget", "Electronics-Widget", 200],
            ["Furniture", "Chair", "Furniture-Chair", 150],
            ["Furniture", "Table", "Furniture-Table", 300],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_unqualified_in_group_qualified_in_select() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT sales.category AS cat, SUM(sales.amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 400], ["Furniture", 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_qualified_in_group_unqualified_in_select() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, UPPER(category) AS upper_cat, SUM(amount) FROM sales GROUP BY sales.category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "ELECTRONICS", 400],
            ["Furniture", "FURNITURE", 450],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_alias_in_expression() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, (category || '_alias') AS aliased_cat, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "Electronics_alias", 400],
            ["Furniture", "Furniture_alias", 450],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_case_operand_with_subquery() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CASE (SELECT 1) WHEN 1 THEN 'one' ELSE 'other' END AS case_val, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", "one", 400], ["Furniture", "one", 450],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_binary_op_constant_expr() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, 1 + 2 AS const_sum, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 3, 400], ["Furniture", 3, 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_constant_struct_in_constant_expr() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, STRUCT('a' AS x, 'b' AS y) AS const_struct, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", ("a", "b"), 400],
            ["Furniture", ("a", "b"), 450],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_unary_op_in_having() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) FROM sales GROUP BY category HAVING NOT SUM(amount) < 400 ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 400], ["Furniture", 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_binary_op_subquery_in_when() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CASE WHEN SUM(amount) + (SELECT 1) > 400 THEN 'High' ELSE 'Low' END AS level, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", "High", 400], ["Furniture", "High", 450],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_constant_array_expr() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, [1, 2, 3] AS arr, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", [1, 2, 3], 400],
            ["Furniture", [1, 2, 3], 450],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_table_alias_column_qualified() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT s.category, s.product, CONCAT(s.category, '-', s.product) AS combined, SUM(s.amount) FROM sales s GROUP BY s.category, s.product ORDER BY s.category, s.product")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "Gadget", "Electronics-Gadget", 200],
            ["Electronics", "Widget", "Electronics-Widget", 200],
            ["Furniture", "Chair", "Furniture-Chair", 150],
            ["Furniture", "Table", "Furniture-Table", 300],
        ]
    );
}
