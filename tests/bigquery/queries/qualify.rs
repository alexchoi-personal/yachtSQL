use crate::assert_table_eq;
use crate::common::create_session;

async fn setup_sales_table(session: &yachtsql::YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE sales (id INT64, product STRING, region STRING, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES (1, 'Widget', 'East', 100), (2, 'Widget', 'East', 150), (3, 'Widget', 'West', 200), (4, 'Gadget', 'East', 80), (5, 'Gadget', 'West', 120), (6, 'Gadget', 'West', 90)").await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_row_number() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, region, amount FROM sales QUALIFY ROW_NUMBER() OVER (PARTITION BY product ORDER BY amount DESC) = 1 ORDER BY product").await
        .unwrap();
    assert_table_eq!(
        result,
        [[5, "Gadget", "West", 120], [3, "Widget", "West", 200],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_rank() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY RANK() OVER (ORDER BY amount DESC) <= 3 ORDER BY amount DESC").await
        .unwrap();
    assert_table_eq!(
        result,
        [[3, "Widget", 200], [2, "Widget", 150], [5, "Gadget", 120],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_dense_rank() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE scores (id INT64, name STRING, score INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO scores VALUES (1, 'Alice', 100), (2, 'Bob', 100), (3, 'Charlie', 90), (4, 'Diana', 90), (5, 'Eve', 80)").await
        .unwrap();

    let result = session
        .execute_sql("SELECT name, score FROM scores QUALIFY DENSE_RANK() OVER (ORDER BY score DESC) <= 2 ORDER BY name").await
        .unwrap();
    assert_table_eq!(
        result,
        [["Alice", 100], ["Bob", 100], ["Charlie", 90], ["Diana", 90],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_with_where() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales WHERE region = 'East' QUALIFY ROW_NUMBER() OVER (ORDER BY amount DESC) = 1").await
        .unwrap();
    assert_table_eq!(result, [[2, "Widget", 150]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_with_group_by() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT region, SUM(amount) AS total FROM sales GROUP BY region QUALIFY ROW_NUMBER() OVER (ORDER BY SUM(amount) DESC) = 1").await
        .unwrap();
    assert_table_eq!(result, [["West", 410]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_ntile() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id, amount FROM sales QUALIFY NTILE(3) OVER (ORDER BY amount) = 1 ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[4, 80], [6, 90]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_percent_rank() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, amount FROM sales QUALIFY PERCENT_RANK() OVER (ORDER BY amount) >= 0.5 ORDER BY id").await
        .unwrap();
    assert_table_eq!(result, [[2, 150], [3, 200], [5, 120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_cume_dist() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, amount FROM sales QUALIFY CUME_DIST() OVER (ORDER BY amount) <= 0.5 ORDER BY id").await
        .unwrap();
    assert_table_eq!(result, [[1, 100], [4, 80], [6, 90]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_lag() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, 20), (3, 15), (4, 25)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, value FROM data QUALIFY value > LAG(value) OVER (ORDER BY id) ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[2, 20], [4, 25]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_lead() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, 20), (3, 15), (4, 25)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, value FROM data QUALIFY value < LEAD(value) OVER (ORDER BY id) ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [3, 15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_first_value() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY amount = FIRST_VALUE(amount) OVER (PARTITION BY product ORDER BY amount DESC) ORDER BY product").await
        .unwrap();
    assert_table_eq!(result, [[5, "Gadget", 120], [3, "Widget", 200],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_last_value() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY amount = LAST_VALUE(amount) OVER (PARTITION BY product ORDER BY amount ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ORDER BY product").await
        .unwrap();
    assert_table_eq!(result, [[5, "Gadget", 120], [3, "Widget", 200],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_multiple_conditions() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, region, amount FROM sales QUALIFY ROW_NUMBER() OVER (PARTITION BY product ORDER BY amount DESC) <= 2 AND ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) = 1 ORDER BY id").await
        .unwrap();
    assert_table_eq!(
        result,
        [[2, "Widget", "East", 150], [3, "Widget", "West", 200],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_with_cte() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY product ORDER BY amount DESC) AS rn FROM sales) SELECT id, product, amount FROM ranked QUALIFY rn = 1 ORDER BY product").await
        .unwrap();
    assert_table_eq!(result, [[5, "Gadget", 120], [3, "Widget", 200],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_with_aggregation() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) AS total, AVG(amount) AS avg_amount FROM sales GROUP BY product QUALIFY SUM(amount) = MAX(SUM(amount)) OVER ()").await
        .unwrap();
    assert_table_eq!(result, [["Widget", 450, 150.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_not_unary_op() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY NOT (ROW_NUMBER() OVER (PARTITION BY product ORDER BY amount DESC) > 1) ORDER BY product")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, "Gadget", 120], [3, "Widget", 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_nested_expression() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY (ROW_NUMBER() OVER (PARTITION BY product ORDER BY amount DESC) = 1) ORDER BY product")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, "Gadget", 120], [3, "Widget", 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_or_operator() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY ROW_NUMBER() OVER (PARTITION BY product ORDER BY amount DESC) = 1 OR ROW_NUMBER() OVER (PARTITION BY product ORDER BY amount) = 1 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, "Widget", 100],
            [3, "Widget", 200],
            [4, "Gadget", 80],
            [5, "Gadget", 120]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_scalar_function() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, UPPER(product) AS upper_product FROM sales QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(product) ORDER BY id) = 1 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Widget", "WIDGET"], [4, "Gadget", "GADGET"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_literal_comparison() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY ROW_NUMBER() OVER (ORDER BY amount DESC) <= 2 AND 1 = 1 ORDER BY amount DESC")
        .await
        .unwrap();
    assert_table_eq!(result, [[3, "Widget", 200], [2, "Widget", 150]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_window_with_aggregate_partition() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY ROW_NUMBER() OVER (ORDER BY SUM(amount) DESC) = 1")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_window_aggregate_with_unary_op_args() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(-amount) as neg_total FROM sales GROUP BY product QUALIFY ROW_NUMBER() OVER (ORDER BY SUM(-amount)) = 1")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", -450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_deeply_nested() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY (((ROW_NUMBER() OVER (PARTITION BY product ORDER BY amount DESC) = 1))) ORDER BY product")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, "Gadget", 120], [3, "Widget", 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_complex_binary_chain() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY ROW_NUMBER() OVER (ORDER BY id) >= 2 AND ROW_NUMBER() OVER (ORDER BY id) <= 4 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[2, "Widget", 150], [3, "Widget", 200], [4, "Gadget", 80]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_aggregate_window_sum_over() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY SUM(amount) = MAX(SUM(amount)) OVER ()")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_count_over_partition() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, COUNT(*) as cnt FROM sales GROUP BY product QUALIFY COUNT(*) >= 3 ORDER BY product")
        .await
        .unwrap();
    assert_table_eq!(result, [["Gadget", 3], ["Widget", 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_nth_value() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY amount = NTH_VALUE(amount, 2) OVER (PARTITION BY product ORDER BY amount DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2, "Widget", 150], [6, "Gadget", 90]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_with_case_expression() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, CASE WHEN amount > 100 THEN 'high' ELSE 'low' END as tier FROM sales QUALIFY ROW_NUMBER() OVER (PARTITION BY CASE WHEN amount > 100 THEN 'high' ELSE 'low' END ORDER BY id) = 1 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Widget", "low"], [2, "Widget", "high"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_minus_unary() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY -SUM(amount) <= -400")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_aggregate_window_sum_over_partition() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY amount = MAX(amount) OVER (PARTITION BY product) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[3, "Widget", 200], [5, "Gadget", 120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_aggregate_window_avg_over() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY amount > AVG(amount) OVER () ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2, "Widget", 150], [3, "Widget", 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_aggregate_window_with_order_by() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY SUM(amount) OVER (PARTITION BY product ORDER BY id) > 200 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[2, "Widget", 150], [3, "Widget", 200], [6, "Gadget", 90]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_aggregate_window_count_over() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY COUNT(*) OVER (PARTITION BY product) = 3 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, "Widget", 100],
            [2, "Widget", 150],
            [3, "Widget", 200],
            [4, "Gadget", 80],
            [5, "Gadget", 120],
            [6, "Gadget", 90]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_aggregate_window_min_over() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY amount = MIN(amount) OVER (PARTITION BY product) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Widget", 100], [4, "Gadget", 80]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_window_with_binary_op_in_args() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY SUM(amount) = MAX(SUM(amount) + 0) OVER () ORDER BY product")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_window_with_unary_op_in_partition() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY ROW_NUMBER() OVER (ORDER BY -SUM(amount)) = 1")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_with_column_reference() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY ROW_NUMBER() OVER (ORDER BY id) <= 3 AND amount > 50 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[1, "Widget", 100], [2, "Widget", 150], [3, "Widget", 200]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_aggregate_window_distinct() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO items VALUES (1, 'A', 10), (2, 'A', 10), (3, 'A', 20), (4, 'B', 30)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, category, value FROM items QUALIFY COUNT(DISTINCT value) OVER (PARTITION BY category) > 1 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "A", 10], [2, "A", 10], [3, "A", 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_with_arithmetic_in_window_partition() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY ROW_NUMBER() OVER (PARTITION BY amount / 100 ORDER BY id) = 1 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, "Widget", 100],
            [2, "Widget", 150],
            [3, "Widget", 200],
            [4, "Gadget", 80],
            [5, "Gadget", 120],
            [6, "Gadget", 90]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_with_arithmetic_in_window_order() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY ROW_NUMBER() OVER (ORDER BY amount * -1) = 1")
        .await
        .unwrap();
    assert_table_eq!(result, [[3, "Widget", 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_aggregate_window_with_frame() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY amount >= AVG(amount) OVER (PARTITION BY product ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[2, "Widget", 150], [3, "Widget", 200], [5, "Gadget", 120]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_with_grouped_aggregate_in_window_partition() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY ROW_NUMBER() OVER (PARTITION BY SUM(amount) > 400 ORDER BY product) = 1 ORDER BY product")
        .await
        .unwrap();
    assert_table_eq!(result, [["Gadget", 290], ["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_with_grouped_aggregate_in_window_order() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY ROW_NUMBER() OVER (ORDER BY SUM(amount) - 100 DESC) = 1")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_with_nested_aggregate_window() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY SUM(amount) >= MIN(SUM(amount)) OVER () + 100 ORDER BY product")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_window_value_range() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY ROW_NUMBER() OVER (ORDER BY id) >= 2 AND ROW_NUMBER() OVER (ORDER BY id) <= 4 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[2, "Widget", 150], [3, "Widget", 200], [4, "Gadget", 80]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_equality_check() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY ROW_NUMBER() OVER (ORDER BY id) = 1 OR ROW_NUMBER() OVER (ORDER BY id) = 3 OR ROW_NUMBER() OVER (ORDER BY id) = 5 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[1, "Widget", 100], [3, "Widget", 200], [5, "Gadget", 120]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_with_default_value() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nullable_data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable_data VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, value FROM nullable_data QUALIFY LAG(value, 1, 0) OVER (ORDER BY id) > 0 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2, 20], [3, 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_with_lead_default() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nullable_data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable_data VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, value FROM nullable_data QUALIFY LEAD(value, 1, 0) OVER (ORDER BY id) > 0 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [2, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_with_null_handling() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nullable_data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable_data VALUES (1, 10), (2, NULL), (3, 30), (4, 40)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, value FROM nullable_data QUALIFY LAG(value, 1, 0) OVER (ORDER BY id) > 0 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2, null], [4, 40]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_with_product_filter() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY (product = 'Widget' AND ROW_NUMBER() OVER (PARTITION BY product ORDER BY amount DESC) = 1) OR product = 'Gadget' ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [3, "Widget", 200],
            [4, "Gadget", 80],
            [5, "Gadget", 120],
            [6, "Gadget", 90]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_multiple_aggregate_windows() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY amount > AVG(amount) OVER (PARTITION BY product) AND amount < MAX(amount) OVER () ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, "Gadget", 120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_or_with_aggregate_windows() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY amount = MIN(amount) OVER (PARTITION BY product) OR amount = MAX(amount) OVER (PARTITION BY product) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, "Widget", 100],
            [3, "Widget", 200],
            [4, "Gadget", 80],
            [5, "Gadget", 120]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_aggregate_window_with_aggregate_in_args() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY MAX(SUM(amount)) OVER () = SUM(amount)")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_aggregate_window_with_aggregate_in_partition() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY ROW_NUMBER() OVER (PARTITION BY SUM(amount) > 300 ORDER BY product) = 1 ORDER BY product")
        .await
        .unwrap();
    assert_table_eq!(result, [["Gadget", 290], ["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_aggregate_window_with_aggregate_in_order_by() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY ROW_NUMBER() OVER (ORDER BY SUM(amount) DESC) = 1")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_aggregate_window_with_binary_op_in_args() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY MAX(SUM(amount) * 2) OVER () > 800 ORDER BY product")
        .await
        .unwrap();
    assert_table_eq!(result, [["Gadget", 290], ["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_aggregate_window_with_unary_op_in_args() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY MIN(-SUM(amount)) OVER () = -SUM(amount)")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_window_with_aggregate_binary_in_partition() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY ROW_NUMBER() OVER (PARTITION BY SUM(amount) + 10 > 400 ORDER BY product) = 1 ORDER BY product")
        .await
        .unwrap();
    assert_table_eq!(result, [["Gadget", 290], ["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_window_with_aggregate_unary_in_partition() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY ROW_NUMBER() OVER (PARTITION BY -SUM(amount) < -300 ORDER BY product) = 1 ORDER BY product")
        .await
        .unwrap();
    assert_table_eq!(result, [["Gadget", 290], ["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_window_with_aggregate_binary_in_order() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY ROW_NUMBER() OVER (ORDER BY SUM(amount) * 1 DESC) = 1")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_window_with_aggregate_unary_in_order() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY ROW_NUMBER() OVER (ORDER BY -SUM(amount) ASC) = 1")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_nested_window_aggregate() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, AVG(amount) as avg_amt FROM sales GROUP BY product QUALIFY AVG(amount) > AVG(AVG(amount)) OVER () ORDER BY product")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 150.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_sum_window_with_frame() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY SUM(amount) OVER (PARTITION BY product ORDER BY id ROWS UNBOUNDED PRECEDING) > 200 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[2, "Widget", 150], [3, "Widget", 200], [6, "Gadget", 90]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_window_with_scalar_in_qualify() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY ABS(amount - 100) < 50 AND ROW_NUMBER() OVER (PARTITION BY product ORDER BY id) = 1 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Widget", 100], [4, "Gadget", 80]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_bare_aggregate_direct_match() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY SUM(amount) > 400")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_bare_aggregate_count_star() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, COUNT(*) as cnt FROM sales GROUP BY product QUALIFY COUNT(*) = 3 ORDER BY product")
        .await
        .unwrap();
    assert_table_eq!(result, [["Gadget", 3], ["Widget", 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_bare_aggregate_avg() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, AVG(amount) as avg_amt FROM sales GROUP BY product QUALIFY AVG(amount) = 150")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 150.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_bare_aggregate_min() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, MIN(amount) as min_amt FROM sales GROUP BY product QUALIFY MIN(amount) < 100 ORDER BY product")
        .await
        .unwrap();
    assert_table_eq!(result, [["Gadget", 80]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_bare_aggregate_max() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, MAX(amount) as max_amt FROM sales GROUP BY product QUALIFY MAX(amount) = 200")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_binary_with_window_and_scalar() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY ROW_NUMBER() OVER (ORDER BY id) = 1 AND amount > 50")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Widget", 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_window_eq_comparison() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id, product, amount FROM sales QUALIFY ROW_NUMBER() OVER (ORDER BY id) = 2",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[2, "Widget", 150]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_window_gt_comparison() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY ROW_NUMBER() OVER (ORDER BY id) > 4 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, "Gadget", 120], [6, "Gadget", 90]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_aggregate_with_distinct() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items_distinct (id INT64, category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items_distinct VALUES (1, 'A', 10), (2, 'A', 10), (3, 'A', 20), (4, 'B', 30), (5, 'B', 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, COUNT(DISTINCT value) as unique_vals FROM items_distinct GROUP BY category QUALIFY COUNT(DISTINCT value) > 1")
        .await
        .unwrap();
    assert_table_eq!(result, [["A", 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_bare_aggregate_or_condition() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY SUM(amount) > 400 OR SUM(amount) < 300 ORDER BY product")
        .await
        .unwrap();
    assert_table_eq!(result, [["Gadget", 290], ["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_aggregate_in_nested_expr() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY (SUM(amount) > 400)")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_aggregate_unary_not() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY NOT (SUM(amount) < 400)")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_window_with_aggregate_args_nested() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY MAX((SUM(amount))) OVER () = SUM(amount)")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_scalar_function_no_window() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY ABS(amount - 100) = 0")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Widget", 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_coalesce_in_condition() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nullable_vals (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable_vals VALUES (1, 10), (2, NULL), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, COALESCE(val, 0) as fixed_val FROM nullable_vals QUALIFY COALESCE(val, 0) > 0 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [3, 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_complex_nested_binary() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY ((SUM(amount) > 200) AND (SUM(amount) < 500)) ORDER BY product")
        .await
        .unwrap();
    assert_table_eq!(result, [["Gadget", 290], ["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_deeply_nested_unary() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY NOT (NOT (SUM(amount) > 400))")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_window_with_nested_partition() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY ROW_NUMBER() OVER (PARTITION BY (SUM(amount) > 300) ORDER BY product) = 1 ORDER BY product")
        .await
        .unwrap();
    assert_table_eq!(result, [["Gadget", 290], ["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_window_with_nested_order() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY ROW_NUMBER() OVER (ORDER BY (SUM(amount))) = 1")
        .await
        .unwrap();
    assert_table_eq!(result, [["Gadget", 290]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_aggregate_window_with_nested_args() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) as total FROM sales GROUP BY product QUALIFY SUM((SUM(amount))) OVER () > 700 ORDER BY product")
        .await
        .unwrap();
    assert_table_eq!(result, [["Gadget", 290], ["Widget", 450]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_literal_expression() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY 1 = 1 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, "Widget", 100],
            [2, "Widget", 150],
            [3, "Widget", 200],
            [4, "Gadget", 80],
            [5, "Gadget", 120],
            [6, "Gadget", 90]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_column_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bool_data (id INT64, flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bool_data VALUES (1, TRUE), (2, FALSE), (3, TRUE)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, flag FROM bool_data QUALIFY flag ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, true], [3, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_is_null_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE null_data (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO null_data VALUES (1, 10), (2, NULL), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, val FROM null_data QUALIFY val IS NOT NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [3, 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_between_expression() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id, product, amount FROM sales QUALIFY amount BETWEEN 90 AND 150 ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, "Widget", 100],
            [2, "Widget", 150],
            [5, "Gadget", 120],
            [6, "Gadget", 90]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_in_list_expression() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id, product, amount FROM sales QUALIFY amount IN (100, 200) ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Widget", 100], [3, "Widget", 200]]);
}
