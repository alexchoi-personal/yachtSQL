use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::{create_session, d, null};

async fn setup_sales_table(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE sales (id INT64, product STRING, category STRING, region STRING, amount INT64, quantity INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES (1, 'Widget', 'Electronics', 'East', 100, 2), (2, 'Gadget', 'Electronics', 'East', 200, 1), (3, 'Chair', 'Furniture', 'West', 150, 3), (4, 'Table', 'Furniture', 'West', 300, 1), (5, 'Widget', 'Electronics', 'West', 100, 5), (6, 'Gadget', 'Electronics', 'West', 250, 2)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_simple_group_by() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT category, SUM(amount) AS total FROM sales GROUP BY category ORDER BY category",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 650], ["Furniture", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_multiple_columns() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, region, SUM(amount) AS total FROM sales GROUP BY category, region ORDER BY category, region")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "East", 300],
            ["Electronics", "West", 350],
            ["Furniture", "West", 450],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_expression() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT UPPER(category) AS cat_upper, SUM(amount) AS total FROM sales GROUP BY UPPER(category) ORDER BY cat_upper")
        .await
        .unwrap();

    assert_table_eq!(result, [["ELECTRONICS", 650], ["FURNITURE", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_arithmetic_expression() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT amount * quantity AS revenue_bucket, COUNT(*) AS cnt FROM sales GROUP BY amount * quantity ORDER BY revenue_bucket")
        .await
        .unwrap();

    assert_table_eq!(result, [[200, 2], [300, 1], [450, 1], [500, 2],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_case_expression() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT CASE WHEN amount >= 200 THEN 'High' ELSE 'Low' END AS price_tier, SUM(amount) AS total FROM sales GROUP BY CASE WHEN amount >= 200 THEN 'High' ELSE 'Low' END ORDER BY price_tier")
        .await
        .unwrap();

    assert_table_eq!(result, [["High", 750], ["Low", 350]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_rollup() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, region, SUM(amount) AS total FROM sales GROUP BY ROLLUP(category, region) ORDER BY category NULLS LAST, region NULLS LAST")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "East", 300],
            ["Electronics", "West", 350],
            ["Electronics", null(), 650],
            ["Furniture", "West", 450],
            ["Furniture", null(), 450],
            [null(), null(), 1100],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_cube() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, region, SUM(amount) AS total FROM sales GROUP BY CUBE(category, region) ORDER BY category NULLS LAST, region NULLS LAST")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "East", 300],
            ["Electronics", "West", 350],
            ["Electronics", null(), 650],
            ["Furniture", "West", 450],
            ["Furniture", null(), 450],
            [null(), "East", 300],
            [null(), "West", 800],
            [null(), null(), 1100],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_grouping_sets_basic() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, region, SUM(amount) AS total FROM sales GROUP BY GROUPING SETS ((category), (region)) ORDER BY category NULLS LAST, region NULLS LAST")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", null(), 650],
            ["Furniture", null(), 450],
            [null(), "East", 300],
            [null(), "West", 800],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_grouping_sets_with_empty_set() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, region, SUM(amount) AS total FROM sales GROUP BY GROUPING SETS ((category), (region), ()) ORDER BY category NULLS LAST, region NULLS LAST")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", null(), 650],
            ["Furniture", null(), 450],
            [null(), "East", 300],
            [null(), "West", 800],
            [null(), null(), 1100],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_grouping_sets_multiple_columns() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, region, SUM(amount) AS total FROM sales GROUP BY GROUPING SETS ((category, region), (category)) ORDER BY category NULLS LAST, region NULLS LAST")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "East", 300],
            ["Electronics", "West", 350],
            ["Electronics", null(), 650],
            ["Furniture", "West", 450],
            ["Furniture", null(), 450],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_having_clause_with_sum() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) AS total FROM sales GROUP BY category HAVING SUM(amount) > 500 ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 650]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_having_clause_with_count() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, COUNT(*) AS cnt FROM sales GROUP BY category HAVING COUNT(*) > 3 ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_having_clause_with_avg() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, AVG(amount) AS avg_amt FROM sales GROUP BY category HAVING AVG(amount) > 200 ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Furniture", 225.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_having_clause_complex() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) AS total, COUNT(*) AS cnt FROM sales GROUP BY category HAVING SUM(amount) > 400 AND COUNT(*) >= 2 ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 650, 4], ["Furniture", 450, 2],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_aggregates_in_select() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) AS total, COUNT(*) AS cnt, AVG(amount) AS avg_amt, MIN(amount) AS min_amt, MAX(amount) AS max_amt FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", 650, 4, 162.5, 100, 250],
            ["Furniture", 450, 2, 225.0, 150, 300],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_aggregates_same_column() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) AS sum_amt, SUM(amount * 2) AS sum_double, SUM(amount) / COUNT(*) AS manual_avg FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", 650, 1300, 162.5],
            ["Furniture", 450, 900, 225.0],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregates_with_expressions() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount * quantity) AS revenue, AVG(amount * quantity) AS avg_revenue FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", 1400, 350.0], ["Furniture", 750, 375.0],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregates_in_order_by() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) AS total FROM sales GROUP BY category ORDER BY SUM(amount) DESC")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 650], ["Furniture", 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregates_in_order_by_with_alias() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT category, SUM(amount) AS total FROM sales GROUP BY category ORDER BY total ASC",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Furniture", 450], ["Electronics", 650],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregates_in_order_by_multiple() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales GROUP BY category, region ORDER BY COUNT(*) DESC, SUM(amount) DESC")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Furniture", "West", 450, 2],
            ["Electronics", "West", 350, 2],
            ["Electronics", "East", 300, 2],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_distinct() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, COUNT(DISTINCT product) AS unique_products FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 2], ["Furniture", 2],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sum_distinct() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(DISTINCT amount) AS unique_sum FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 650], ["Furniture", 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_avg_distinct() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, AVG(DISTINCT amount) AS unique_avg FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 162.5], ["Furniture", 225.0],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_distinct_aggregates() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, COUNT(DISTINCT product) AS unique_products, COUNT(DISTINCT region) AS unique_regions, SUM(DISTINCT amount) AS unique_sum FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", 2, 2, 650], ["Furniture", 2, 1, 450],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_and_aggregate_same_query() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) AS total, ROW_NUMBER() OVER (ORDER BY SUM(amount) DESC) AS rank FROM sales GROUP BY category ORDER BY rank")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 650, 1], ["Furniture", 450, 2],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_sum_over_aggregated_result() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) AS total, SUM(SUM(amount)) OVER () AS grand_total FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", 650, 1100], ["Furniture", 450, 1100],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_partition_over_aggregated() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, region, SUM(amount) AS total, SUM(SUM(amount)) OVER (PARTITION BY category) AS category_total FROM sales GROUP BY category, region ORDER BY category, region")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "East", 300, 650],
            ["Electronics", "West", 350, 650],
            ["Furniture", "West", 450, 450],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_running_total_over_aggregated() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, region, SUM(amount) AS total, SUM(SUM(amount)) OVER (ORDER BY category, region) AS running_total FROM sales GROUP BY category, region ORDER BY category, region")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "East", 300, 300],
            ["Electronics", "West", 350, 650],
            ["Furniture", "West", 450, 1100],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_percent_of_total_with_window() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) AS total, ROUND(100.0 * SUM(amount) / SUM(SUM(amount)) OVER (), 1) AS pct_of_total FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", 650, 59.1], ["Furniture", 450, 40.9],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_where_filter() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) AS total FROM sales WHERE region = 'West' GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 350], ["Furniture", 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_where_and_having() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) AS total FROM sales WHERE quantity > 1 GROUP BY category HAVING SUM(amount) >= 200 ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_without_group_by() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT SUM(amount) AS total, COUNT(*) AS cnt, AVG(amount) AS avg_amt FROM sales",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1100, 6, 183.33333333333334]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_count_star_vs_column() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE data (category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO data VALUES ('A', 10), ('A', NULL), ('B', 20), ('B', NULL), ('B', NULL)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, COUNT(*) AS cnt_all, COUNT(value) AS cnt_value FROM data GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 2, 1], ["B", 3, 1],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_rollup_with_grouping_function() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, region, SUM(amount) AS total, GROUPING(category) AS g_cat, GROUPING(region) AS g_region FROM sales GROUP BY ROLLUP(category, region) ORDER BY category NULLS LAST, region NULLS LAST")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "East", 300, 0, 0],
            ["Electronics", "West", 350, 0, 0],
            ["Electronics", null(), 650, 0, 1],
            ["Furniture", "West", 450, 0, 0],
            ["Furniture", null(), 450, 0, 1],
            [null(), null(), 1100, 1, 1],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_cube_with_grouping_id() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, region, SUM(amount) AS total, GROUPING_ID(category, region) AS gid FROM sales GROUP BY CUBE(category, region) ORDER BY gid, category, region")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "East", 300, 0],
            ["Electronics", "West", 350, 0],
            ["Furniture", "West", 450, 0],
            ["Electronics", null(), 650, 1],
            ["Furniture", null(), 450, 1],
            [null(), "East", 300, 2],
            [null(), "West", 800, 2],
            [null(), null(), 1100, 3],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_having_with_new_aggregate_not_in_select() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) AS total FROM sales GROUP BY category HAVING AVG(amount) > 160 ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 650], ["Furniture", 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_having_with_unary_not_aggregate() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) AS total FROM sales GROUP BY category HAVING NOT (SUM(amount) < 500) ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 650]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_having_with_nested_aggregate() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) AS total FROM sales GROUP BY category HAVING ((SUM(amount) > 400)) ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 650], ["Furniture", 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_unary_op_with_aggregate() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) AS total FROM sales GROUP BY category ORDER BY -SUM(amount), category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 650], ["Furniture", 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_nested_aggregate() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) AS total FROM sales GROUP BY category ORDER BY (SUM(amount)) DESC, category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 650], ["Furniture", 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_binary_op_with_aggregates() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) AS total, COUNT(*) AS cnt FROM sales GROUP BY category ORDER BY SUM(amount) * COUNT(*) DESC, category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 650, 4], ["Furniture", 450, 2],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_cast_aggregate() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) AS total FROM sales GROUP BY category ORDER BY CAST(SUM(amount) AS FLOAT64), category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 650], ["Furniture", 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_concat_operation() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE concat_data (grp STRING, prefix STRING, suffix STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO concat_data VALUES ('A', 'hello', 'world'), ('A', 'foo', 'bar'), ('B', 'test', 'data')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, MIN(CONCAT(prefix, suffix)) AS min_concat FROM concat_data GROUP BY grp ORDER BY grp")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", "foobar"], ["B", "testdata"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_distinct_in_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE distinct_expr (grp STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO distinct_expr VALUES ('A', 1), ('A', 1), ('A', 2), ('B', 3), ('B', 3), ('B', 4)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, SUM(DISTINCT val) + COUNT(DISTINCT val) AS combined FROM distinct_expr GROUP BY grp ORDER BY grp")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 6.0], ["B", 12.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_qualified_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE qualified_data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO qualified_data VALUES (1, 100), (1, 200), (2, 300)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT qualified_data.id, SUM(qualified_data.value) AS total FROM qualified_data GROUP BY qualified_data.id ORDER BY qualified_data.id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 300], [2, 300]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_having_with_qualified_aggregate() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE having_qual (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO having_qual VALUES (1, 100), (1, 200), (2, 50)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT having_qual.id, SUM(having_qual.value) FROM having_qual GROUP BY having_qual.id HAVING SUM(having_qual.value) > 100 ORDER BY having_qual.id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 300]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_bytes_literal() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bytes_data (grp STRING, data BYTES)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO bytes_data VALUES ('A', b'hello'), ('A', b'world'), ('B', b'test')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, COUNT(data) AS cnt FROM bytes_data GROUP BY grp ORDER BY grp")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 2], ["B", 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_numeric_literal() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE numeric_data (grp STRING, val NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO numeric_data VALUES ('A', NUMERIC '1.23'), ('A', NUMERIC '4.56'), ('B', NUMERIC '7.89')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, COUNT(*) AS cnt FROM numeric_data WHERE val > NUMERIC '2.00' GROUP BY grp ORDER BY grp")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 1], ["B", 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_date_literal() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE date_data (grp STRING, dt DATE)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO date_data VALUES ('A', DATE '2023-01-01'), ('A', DATE '2023-06-15'), ('B', DATE '2023-12-31')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, MIN(dt) AS min_date FROM date_data GROUP BY grp ORDER BY grp")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", d(2023, 1, 1)], ["B", d(2023, 12, 31)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_datetime_literal() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE datetime_data (grp STRING, dt DATETIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO datetime_data VALUES ('A', DATETIME '2023-01-01 10:00:00'), ('A', DATETIME '2023-01-01 14:00:00'), ('B', DATETIME '2023-06-15 08:30:00')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, COUNT(*) AS cnt FROM datetime_data WHERE dt > DATETIME '2023-01-01 12:00:00' GROUP BY grp ORDER BY grp")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 1], ["B", 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_time_literal() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE time_data (grp STRING, tm TIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO time_data VALUES ('A', TIME '08:00:00'), ('A', TIME '14:30:00'), ('B', TIME '20:00:00')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, COUNT(*) AS cnt FROM time_data WHERE tm > TIME '12:00:00' GROUP BY grp ORDER BY grp")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 1], ["B", 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_timestamp_literal() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE timestamp_data (grp STRING, ts TIMESTAMP)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO timestamp_data VALUES ('A', TIMESTAMP '2023-01-01 00:00:00 UTC'), ('A', TIMESTAMP '2023-06-15 12:00:00 UTC'), ('B', TIMESTAMP '2023-12-31 23:59:59 UTC')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, COUNT(*) AS cnt FROM timestamp_data WHERE ts > TIMESTAMP '2023-06-01 00:00:00 UTC' GROUP BY grp ORDER BY grp")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 1], ["B", 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_json_literal() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE json_data (grp STRING, data JSON)")
        .await
        .unwrap();
    session
        .execute_sql(r#"INSERT INTO json_data VALUES ('A', JSON '{"a": 1}'), ('A', JSON '{"b": 2}'), ('B', JSON '{"c": 3}')"#)
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, COUNT(data) AS cnt FROM json_data GROUP BY grp ORDER BY grp")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 2], ["B", 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_bignumeric_literal() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bignumeric_data (grp STRING, val BIGNUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bignumeric_data VALUES ('A', BIGNUMERIC '1234567890.123456789012345678901234567890'), ('B', BIGNUMERIC '9876543210.987654321098765432109876543210')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, COUNT(*) AS cnt FROM bignumeric_data GROUP BY grp ORDER BY grp")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 1], ["B", 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_array_filter() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE array_filter_data (grp STRING, vals ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO array_filter_data VALUES ('A', [1, 2, 3]), ('A', [4, 5]), ('B', [6, 7, 8, 9])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, SUM(ARRAY_LENGTH(vals)) AS total_len FROM array_filter_data GROUP BY grp ORDER BY grp")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 5], ["B", 4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_struct_access() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE struct_data (grp STRING, info STRUCT<name STRING, value INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO struct_data VALUES ('A', STRUCT('x', 10)), ('A', STRUCT('y', 20)), ('B', STRUCT('z', 30))")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT grp, SUM(info.value) AS total FROM struct_data GROUP BY grp ORDER BY grp",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 30], ["B", 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_safe_cast_in_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE safe_cast_data (grp STRING, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO safe_cast_data VALUES ('A', '100'), ('A', '200'), ('B', '300')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, SUM(SAFE_CAST(val AS INT64)) AS total FROM safe_cast_data GROUP BY grp ORDER BY grp")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 300], ["B", 300]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_scalar_function_in_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE scalar_func_data (grp STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO scalar_func_data VALUES ('A', -10), ('A', 20), ('B', -30)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT grp, SUM(ABS(val)) AS abs_sum FROM scalar_func_data GROUP BY grp ORDER BY grp",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 30], ["B", 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_with_aggregate_binary_op() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE order_bin_data (grp STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO order_bin_data VALUES ('A', 10), ('A', 20), ('B', 50), ('B', 60)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, SUM(val) AS total FROM order_bin_data GROUP BY grp ORDER BY SUM(val) + COUNT(*), grp")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 30.0], ["B", 110.0]]);
}
