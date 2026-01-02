use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::create_session;

async fn setup_tables(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE sales (id INT64, employee STRING, department STRING, amount INT64, sale_date DATE)").await
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES (1, 'Alice', 'Electronics', 1000, '2024-01-01'), (2, 'Bob', 'Electronics', 1500, '2024-01-02'), (3, 'Alice', 'Electronics', 2000, '2024-01-03'), (4, 'Charlie', 'Clothing', 800, '2024-01-01'), (5, 'Diana', 'Clothing', 1200, '2024-01-02')").await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_row_number() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn FROM sales ORDER BY rn",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 2000, 1],
            ["Bob", 1500, 2],
            ["Diana", 1200, 3],
            ["Alice", 1000, 4],
            ["Charlie", 800, 5],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_row_number_with_partition() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, department, amount, ROW_NUMBER() OVER (PARTITION BY department ORDER BY amount DESC) AS rn FROM sales ORDER BY department, rn",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Diana", "Clothing", 1200, 1],
            ["Charlie", "Clothing", 800, 2],
            ["Alice", "Electronics", 2000, 1],
            ["Bob", "Electronics", 1500, 2],
            ["Alice", "Electronics", 1000, 3],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_rank() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE scores (name STRING, score INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO scores VALUES ('A', 100), ('B', 100), ('C', 90), ('D', 80)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT name, score, RANK() OVER (ORDER BY score DESC) AS rank FROM scores ORDER BY rank, name",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [["A", 100, 1], ["B", 100, 1], ["C", 90, 3], ["D", 80, 4],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_dense_rank() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE scores (name STRING, score INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO scores VALUES ('A', 100), ('B', 100), ('C', 90), ('D', 80)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT name, score, DENSE_RANK() OVER (ORDER BY score DESC) AS drank FROM scores ORDER BY drank, name",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [["A", 100, 1], ["B", 100, 1], ["C", 90, 2], ["D", 80, 3],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_ntile() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, NTILE(2) OVER (ORDER BY amount DESC) AS bucket FROM sales ORDER BY bucket, amount DESC",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 2000, 1],
            ["Bob", 1500, 1],
            ["Diana", 1200, 1],
            ["Alice", 1000, 2],
            ["Charlie", 800, 2],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_lag() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, LAG(amount) OVER (ORDER BY id) AS prev_amount FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, null],
            ["Bob", 1500, 1000],
            ["Alice", 2000, 1500],
            ["Charlie", 800, 2000],
            ["Diana", 1200, 800],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_lag_with_offset() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, LAG(amount, 2) OVER (ORDER BY id) AS prev2_amount FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, null],
            ["Bob", 1500, null],
            ["Alice", 2000, 1000],
            ["Charlie", 800, 1500],
            ["Diana", 1200, 2000],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_lag_with_default() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, LAG(amount, 1, 0) OVER (ORDER BY id) AS prev_amount FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 0],
            ["Bob", 1500, 1000],
            ["Alice", 2000, 1500],
            ["Charlie", 800, 2000],
            ["Diana", 1200, 800],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_lead() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, LEAD(amount) OVER (ORDER BY id) AS next_amount FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 1500],
            ["Bob", 1500, 2000],
            ["Alice", 2000, 800],
            ["Charlie", 800, 1200],
            ["Diana", 1200, null],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_first_value() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, department, amount, FIRST_VALUE(employee) OVER (PARTITION BY department ORDER BY amount DESC) AS top_seller FROM sales ORDER BY department, id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", "Clothing", 800, "Diana"],
            ["Diana", "Clothing", 1200, "Diana"],
            ["Alice", "Electronics", 1000, "Alice"],
            ["Bob", "Electronics", 1500, "Alice"],
            ["Alice", "Electronics", 2000, "Alice"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_last_value() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, department, amount, LAST_VALUE(employee) OVER (PARTITION BY department ORDER BY amount DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lowest_seller FROM sales ORDER BY department, id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", "Clothing", 800, "Charlie"],
            ["Diana", "Clothing", 1200, "Charlie"],
            ["Alice", "Electronics", 1000, "Alice"],
            ["Bob", "Electronics", 1500, "Alice"],
            ["Alice", "Electronics", 2000, "Alice"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_sum_over() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, SUM(amount) OVER (ORDER BY id) AS running_total FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 1000],
            ["Bob", 1500, 2500],
            ["Alice", 2000, 4500],
            ["Charlie", 800, 5300],
            ["Diana", 1200, 6500],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_avg_over_partition() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, department, amount, AVG(amount) OVER (PARTITION BY department) AS dept_avg FROM sales ORDER BY department, id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", "Clothing", 800, 1000.0],
            ["Diana", "Clothing", 1200, 1000.0],
            ["Alice", "Electronics", 1000, 1500.0],
            ["Bob", "Electronics", 1500, 1500.0],
            ["Alice", "Electronics", 2000, 1500.0],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_over() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, COUNT(*) OVER (PARTITION BY employee) AS sale_count FROM sales ORDER BY employee, id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 2],
            ["Alice", 2],
            ["Bob", 1],
            ["Charlie", 1],
            ["Diana", 1],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_min_max_over() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, MIN(amount) OVER () AS min_sale, MAX(amount) OVER () AS max_sale FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 800, 2000],
            ["Bob", 1500, 800, 2000],
            ["Alice", 2000, 800, 2000],
            ["Charlie", 800, 800, 2000],
            ["Diana", 1200, 800, 2000],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_frame_rows() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, SUM(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS window_sum FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 2500],
            ["Bob", 1500, 4500],
            ["Alice", 2000, 4300],
            ["Charlie", 800, 4000],
            ["Diana", 1200, 2000],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_frame_range() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, SUM(amount) OVER (ORDER BY amount RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumsum FROM sales ORDER BY amount",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", 800, 800],
            ["Alice", 1000, 1800],
            ["Diana", 1200, 3000],
            ["Bob", 1500, 4500],
            ["Alice", 2000, 6500],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_window_functions() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn, RANK() OVER (ORDER BY amount DESC) AS rnk, SUM(amount) OVER () AS total FROM sales ORDER BY rn",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 2000, 1, 1, 6500],
            ["Bob", 1500, 2, 2, 6500],
            ["Diana", 1200, 3, 3, 6500],
            ["Alice", 1000, 4, 4, 6500],
            ["Charlie", 800, 5, 5, 6500],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_percent_rank() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, PERCENT_RANK() OVER (ORDER BY amount) AS prank FROM sales ORDER BY amount",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", 800, 0.0],
            ["Alice", 1000, 0.25],
            ["Diana", 1200, 0.5],
            ["Bob", 1500, 0.75],
            ["Alice", 2000, 1.0],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_cume_dist() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, CUME_DIST() OVER (ORDER BY amount) AS cdist FROM sales ORDER BY amount",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", 800, 0.2],
            ["Alice", 1000, 0.4],
            ["Diana", 1200, 0.6],
            ["Bob", 1500, 0.8],
            ["Alice", 2000, 1.0],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_named_window() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, SUM(amount) OVER w AS running_sum
             FROM sales
             WINDOW w AS (ORDER BY amount)
             ORDER BY amount",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", 800, 800],
            ["Alice", 1000, 1800],
            ["Diana", 1200, 3000],
            ["Bob", 1500, 4500],
            ["Alice", 2000, 6500],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_named_window_with_partition() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, department, amount, ROW_NUMBER() OVER w AS rn
             FROM sales
             WINDOW w AS (PARTITION BY department ORDER BY amount DESC)
             ORDER BY department, rn",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Diana", "Clothing", 1200, 1],
            ["Charlie", "Clothing", 800, 2],
            ["Alice", "Electronics", 2000, 1],
            ["Bob", "Electronics", 1500, 2],
            ["Alice", "Electronics", 1000, 3],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_named_window_with_rows_between() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, SUM(amount) OVER w AS rolling_sum
             FROM sales
             WINDOW w AS (ORDER BY amount ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
             ORDER BY amount",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", 800, 800],
            ["Alice", 1000, 1800],
            ["Diana", 1200, 2200],
            ["Bob", 1500, 2700],
            ["Alice", 2000, 3500],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_nth_value() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, NTH_VALUE(employee, 2) OVER (ORDER BY amount DESC) AS second_highest FROM sales ORDER BY amount DESC",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 2000, "Bob"],
            ["Bob", 1500, "Bob"],
            ["Diana", 1200, "Bob"],
            ["Alice", 1000, "Bob"],
            ["Charlie", 800, "Bob"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_nth_value_with_partition() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, department, amount, NTH_VALUE(amount, 2) OVER (PARTITION BY department ORDER BY amount DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_amount FROM sales ORDER BY department, amount DESC",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Diana", "Clothing", 1200, 800],
            ["Charlie", "Clothing", 800, 800],
            ["Alice", "Electronics", 2000, 1500],
            ["Bob", "Electronics", 1500, 1500],
            ["Alice", "Electronics", 1000, 1500],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_nth_value_out_of_bounds() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, department, NTH_VALUE(employee, 10) OVER (PARTITION BY department ORDER BY amount DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nth FROM sales ORDER BY department, id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", "Clothing", null],
            ["Diana", "Clothing", null],
            ["Alice", "Electronics", null],
            ["Bob", "Electronics", null],
            ["Alice", "Electronics", null],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_lead_with_offset_and_default() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, LEAD(amount, 2, -1) OVER (ORDER BY id) AS lead2 FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 2000],
            ["Bob", 1500, 800],
            ["Alice", 2000, 1200],
            ["Charlie", 800, -1],
            ["Diana", 1200, -1],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_rank_without_order_by() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql("SELECT employee, amount, RANK() OVER () AS rnk FROM sales ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 1],
            ["Bob", 1500, 1],
            ["Alice", 2000, 1],
            ["Charlie", 800, 1],
            ["Diana", 1200, 1],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_dense_rank_without_order_by() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql("SELECT employee, amount, DENSE_RANK() OVER () AS drnk FROM sales ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 1],
            ["Bob", 1500, 1],
            ["Alice", 2000, 1],
            ["Charlie", 800, 1],
            ["Diana", 1200, 1],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_percent_rank_single_row() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE single_row (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO single_row VALUES (1, 100)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, value, PERCENT_RANK() OVER (ORDER BY value) AS prank FROM single_row",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 100, 0.0],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cume_dist_with_ties() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE scores (id INT64, score INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO scores VALUES (1, 100), (2, 100), (3, 90), (4, 90), (5, 80)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, score, CUME_DIST() OVER (ORDER BY score DESC) AS cdist FROM scores ORDER BY score DESC, id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, 100, 1.0],
            [2, 100, 1.0],
            [3, 90, 0.6],
            [4, 90, 0.6],
            [5, 80, 0.2],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_frame_current_row_to_following() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, SUM(amount) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS window_sum FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 4500],
            ["Bob", 1500, 4300],
            ["Alice", 2000, 4000],
            ["Charlie", 800, 2000],
            ["Diana", 1200, 1200],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_frame_unbounded_following() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, SUM(amount) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS window_sum FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 6500],
            ["Bob", 1500, 5500],
            ["Alice", 2000, 4000],
            ["Charlie", 800, 2000],
            ["Diana", 1200, 1200],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_frame_following_to_following() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, SUM(amount) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS window_sum FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 3500.0],
            ["Bob", 1500, 2800.0],
            ["Alice", 2000, 2000.0],
            ["Charlie", 800, 1200.0],
            ["Diana", 1200, null],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_frame_preceding_to_preceding() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, SUM(amount) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS window_sum FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 1000.0],
            ["Bob", 1500, 1000.0],
            ["Alice", 2000, 2500.0],
            ["Charlie", 800, 3500.0],
            ["Diana", 1200, 2800.0],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_avg_window_with_frame() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, AVG(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS window_avg FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 1250.0],
            ["Bob", 1500, 1500.0],
            ["Alice", 2000, 1433.3333333333333],
            ["Charlie", 800, 1333.3333333333333],
            ["Diana", 1200, 1000.0],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_min_window_with_frame() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, MIN(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS window_min FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 1000],
            ["Bob", 1500, 1000],
            ["Alice", 2000, 800],
            ["Charlie", 800, 800],
            ["Diana", 1200, 800],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_max_window_with_frame() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, MAX(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS window_max FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 1500],
            ["Bob", 1500, 2000],
            ["Alice", 2000, 2000],
            ["Charlie", 800, 2000],
            ["Diana", 1200, 1200],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_window_with_frame() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, COUNT(*) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS window_count FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 2],
            ["Bob", 1500, 3],
            ["Alice", 2000, 3],
            ["Charlie", 800, 3],
            ["Diana", 1200, 2],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_column_window_with_nulls() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE data_with_nulls (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO data_with_nulls VALUES (1, 10), (2, NULL), (3, 30), (4, NULL), (5, 50)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, value, COUNT(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS window_count FROM data_with_nulls ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, 10, 1],
            [2, null, 2],
            [3, 30, 1],
            [4, null, 2],
            [5, 50, 1],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_ntile_uneven_distribution() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE items (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E'), (6, 'F'), (7, 'G')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, name, NTILE(3) OVER (ORDER BY id) AS bucket FROM items ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, "A", 1],
            [2, "B", 1],
            [3, "C", 1],
            [4, "D", 2],
            [5, "E", 2],
            [6, "F", 3],
            [7, "G", 3],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_ntile_with_partition() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, department, amount, NTILE(2) OVER (PARTITION BY department ORDER BY amount DESC) AS bucket FROM sales ORDER BY department, bucket, amount DESC",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Diana", "Clothing", 1200, 1],
            ["Charlie", "Clothing", 800, 2],
            ["Alice", "Electronics", 2000, 1],
            ["Bob", "Electronics", 1500, 1],
            ["Alice", "Electronics", 1000, 2],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_lag_with_partition() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, department, amount, LAG(amount, 1, 0) OVER (PARTITION BY department ORDER BY id) AS prev_amount FROM sales ORDER BY department, id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", "Clothing", 800, 0],
            ["Diana", "Clothing", 1200, 800],
            ["Alice", "Electronics", 1000, 0],
            ["Bob", "Electronics", 1500, 1000],
            ["Alice", "Electronics", 2000, 1500],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_lead_with_partition() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, department, amount, LEAD(amount, 1, 0) OVER (PARTITION BY department ORDER BY id) AS next_amount FROM sales ORDER BY department, id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", "Clothing", 800, 1200],
            ["Diana", "Clothing", 1200, 0],
            ["Alice", "Electronics", 1000, 1500],
            ["Bob", "Electronics", 1500, 2000],
            ["Alice", "Electronics", 2000, 0],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_first_value_with_order_desc() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, FIRST_VALUE(employee) OVER (ORDER BY amount) AS lowest_seller FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, "Charlie"],
            ["Bob", 1500, "Charlie"],
            ["Alice", 2000, "Charlie"],
            ["Charlie", 800, "Charlie"],
            ["Diana", 1200, "Charlie"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_last_value_without_frame() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, LAST_VALUE(employee) OVER (ORDER BY amount DESC) AS last_emp FROM sales ORDER BY amount DESC",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 2000, "Charlie"],
            ["Bob", 1500, "Charlie"],
            ["Diana", 1200, "Charlie"],
            ["Alice", 1000, "Charlie"],
            ["Charlie", 800, "Charlie"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_last_value_with_current_row_frame() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, LAST_VALUE(employee) OVER (ORDER BY amount DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_emp FROM sales ORDER BY amount DESC",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 2000, "Alice"],
            ["Bob", 1500, "Bob"],
            ["Diana", 1200, "Diana"],
            ["Alice", 1000, "Alice"],
            ["Charlie", 800, "Charlie"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_window_with_order_by_running_sum() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, department, amount, SUM(amount) OVER (PARTITION BY department ORDER BY id) AS running_sum FROM sales ORDER BY department, id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", "Clothing", 800, 800],
            ["Diana", "Clothing", 1200, 2000],
            ["Alice", "Electronics", 1000, 1000],
            ["Bob", "Electronics", 1500, 2500],
            ["Alice", "Electronics", 2000, 4500],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_window_with_order_by_running_count() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, department, amount, COUNT(*) OVER (PARTITION BY department ORDER BY id) AS running_count FROM sales ORDER BY department, id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", "Clothing", 800, 1],
            ["Diana", "Clothing", 1200, 2],
            ["Alice", "Electronics", 1000, 1],
            ["Bob", "Electronics", 1500, 2],
            ["Alice", "Electronics", 2000, 3],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_partition_keys() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE multi_partition (id INT64, cat1 STRING, cat2 STRING, value INT64)",
        )
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO multi_partition VALUES (1, 'A', 'X', 10), (2, 'A', 'X', 20), (3, 'A', 'Y', 30), (4, 'B', 'X', 40), (5, 'B', 'Y', 50)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, cat1, cat2, value, SUM(value) OVER (PARTITION BY cat1, cat2) AS sum_val FROM multi_partition ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, "A", "X", 10, 30],
            [2, "A", "X", 20, 30],
            [3, "A", "Y", 30, 30],
            [4, "B", "X", 40, 40],
            [5, "B", "Y", 50, 50],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_row_number_with_multiple_order_keys() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, department, amount, ROW_NUMBER() OVER (ORDER BY department, amount DESC) AS rn FROM sales ORDER BY rn",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Diana", "Clothing", 1200, 1],
            ["Charlie", "Clothing", 800, 2],
            ["Alice", "Electronics", 2000, 3],
            ["Bob", "Electronics", 1500, 4],
            ["Alice", "Electronics", 1000, 5],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_rank_with_ties_in_partition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE tied_scores (id INT64, team STRING, score INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO tied_scores VALUES (1, 'A', 100), (2, 'A', 100), (3, 'A', 90), (4, 'B', 80), (5, 'B', 80), (6, 'B', 70)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, team, score, RANK() OVER (PARTITION BY team ORDER BY score DESC) AS rnk FROM tied_scores ORDER BY team, rnk, id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, "A", 100, 1],
            [2, "A", 100, 1],
            [3, "A", 90, 3],
            [4, "B", 80, 1],
            [5, "B", 80, 1],
            [6, "B", 70, 3],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_dense_rank_with_ties_in_partition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE tied_scores2 (id INT64, team STRING, score INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO tied_scores2 VALUES (1, 'A', 100), (2, 'A', 100), (3, 'A', 90), (4, 'B', 80), (5, 'B', 80), (6, 'B', 70)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, team, score, DENSE_RANK() OVER (PARTITION BY team ORDER BY score DESC) AS drnk FROM tied_scores2 ORDER BY team, drnk, id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, "A", 100, 1],
            [2, "A", 100, 1],
            [3, "A", 90, 2],
            [4, "B", 80, 1],
            [5, "B", 80, 1],
            [6, "B", 70, 2],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_percent_rank_with_partition() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, department, amount, PERCENT_RANK() OVER (PARTITION BY department ORDER BY amount) AS prank FROM sales ORDER BY department, amount",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", "Clothing", 800, 0.0],
            ["Diana", "Clothing", 1200, 1.0],
            ["Alice", "Electronics", 1000, 0.0],
            ["Bob", "Electronics", 1500, 0.5],
            ["Alice", "Electronics", 2000, 1.0],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_cume_dist_with_partition() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, department, amount, CUME_DIST() OVER (PARTITION BY department ORDER BY amount) AS cdist FROM sales ORDER BY department, amount",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", "Clothing", 800, 0.5],
            ["Diana", "Clothing", 1200, 1.0],
            ["Alice", "Electronics", 1000, 0.3333333333333333],
            ["Bob", "Electronics", 1500, 0.6666666666666666],
            ["Alice", "Electronics", 2000, 1.0],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_empty_frame_returns_null() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, SUM(amount) OVER (ORDER BY id ROWS BETWEEN 10 PRECEDING AND 5 PRECEDING) AS empty_sum FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 1000.0],
            ["Bob", 1500, 1000.0],
            ["Alice", 2000, 1000.0],
            ["Charlie", 800, 1000.0],
            ["Diana", 1200, 1000.0],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_function_in_expression() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, amount - LAG(amount, 1, amount) OVER (ORDER BY id) AS diff FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 0],
            ["Bob", 1500, 500],
            ["Alice", 2000, 500],
            ["Charlie", 800, -1200],
            ["Diana", 1200, 400],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_with_descending_order() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn FROM sales ORDER BY rn",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 2000, 1],
            ["Bob", 1500, 2],
            ["Diana", 1200, 3],
            ["Alice", 1000, 4],
            ["Charlie", 800, 5],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_avg_window_with_nulls() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE nullable_values (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable_values VALUES (1, 10), (2, NULL), (3, 30), (4, 40)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, value, AVG(value) OVER () AS avg_val FROM nullable_values ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, 10, 26.666666666666668],
            [2, null, 26.666666666666668],
            [3, 30, 26.666666666666668],
            [4, 40, 26.666666666666668],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_sum_window_with_nulls() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE nullable_sums (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable_sums VALUES (1, 10), (2, NULL), (3, 30), (4, 40)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, value, SUM(value) OVER () AS sum_val FROM nullable_sums ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [[1, 10, 80], [2, null, 80], [3, 30, 80], [4, 40, 80],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_min_window_with_all_nulls_frame() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE all_nulls (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO all_nulls VALUES (1, NULL), (2, NULL), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, value, MIN(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS min_val FROM all_nulls ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(result, [[1, null, null], [2, null, null], [3, 30, null],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_max_window_with_all_nulls_frame() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE all_nulls_max (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO all_nulls_max VALUES (1, NULL), (2, NULL), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, value, MAX(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS max_val FROM all_nulls_max ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(result, [[1, null, null], [2, null, null], [3, 30, null],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_in_case_expression() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, CASE WHEN ROW_NUMBER() OVER (ORDER BY amount DESC) <= 2 THEN 'top' ELSE 'other' END AS tier FROM sales ORDER BY amount DESC",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 2000, "top"],
            ["Bob", 1500, "top"],
            ["Diana", 1200, "other"],
            ["Alice", 1000, "other"],
            ["Charlie", 800, "other"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_with_unary_minus() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, -SUM(amount) OVER (ORDER BY id) AS neg_running_total FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, -1000.0],
            ["Bob", 1500, -2500.0],
            ["Alice", 2000, -4500.0],
            ["Charlie", 800, -5300.0],
            ["Diana", 1200, -6500.0],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_with_alias() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, ROW_NUMBER() OVER (ORDER BY amount DESC) AS rank_num FROM sales ORDER BY rank_num",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 2000, 1],
            ["Bob", 1500, 2],
            ["Diana", 1200, 3],
            ["Alice", 1000, 4],
            ["Charlie", 800, 5],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_with_cast() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, CAST(ROW_NUMBER() OVER (ORDER BY amount DESC) AS STRING) AS rank_str FROM sales ORDER BY amount DESC",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 2000, "1"],
            ["Bob", 1500, "2"],
            ["Diana", 1200, "3"],
            ["Alice", 1000, "4"],
            ["Charlie", 800, "5"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_in_scalar_function() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, ABS(amount - LAG(amount, 1, amount) OVER (ORDER BY id)) AS abs_diff FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 0],
            ["Bob", 1500, 500],
            ["Alice", 2000, 500],
            ["Charlie", 800, 1200],
            ["Diana", 1200, 400],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_binary_op_expression() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, amount + ROW_NUMBER() OVER (ORDER BY id) AS amount_plus_rn FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 1001],
            ["Bob", 1500, 1502],
            ["Alice", 2000, 2003],
            ["Charlie", 800, 804],
            ["Diana", 1200, 1205],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_window_running_avg() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, AVG(amount) OVER (ORDER BY id) AS running_avg FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 1000.0],
            ["Bob", 1500, 1250.0],
            ["Alice", 2000, 1500.0],
            ["Charlie", 800, 1325.0],
            ["Diana", 1200, 1300.0],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_window_running_min() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, MIN(amount) OVER (ORDER BY id) AS running_min FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 1000],
            ["Bob", 1500, 1000],
            ["Alice", 2000, 1000],
            ["Charlie", 800, 800],
            ["Diana", 1200, 800],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_window_running_max() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, MAX(amount) OVER (ORDER BY id) AS running_max FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 1000],
            ["Bob", 1500, 1500],
            ["Alice", 2000, 2000],
            ["Charlie", 800, 2000],
            ["Diana", 1200, 2000],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_with_peer_groups() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE peer_data (id INT64, group_val INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO peer_data VALUES (1, 10, 100), (2, 10, 200), (3, 20, 300), (4, 20, 400), (5, 30, 500)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, group_val, value, SUM(value) OVER (ORDER BY group_val) AS running_sum FROM peer_data ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, 10, 100, 300],
            [2, 10, 200, 300],
            [3, 20, 300, 1000],
            [4, 20, 400, 1000],
            [5, 30, 500, 1500],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_float_values() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE float_data (id INT64, value FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO float_data VALUES (1, 1.5), (2, 2.5), (3, 3.5), (4, 4.5)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, value, SUM(value) OVER (ORDER BY id) AS running_sum FROM float_data ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [[1, 1.5, 1.5], [2, 2.5, 4.0], [3, 3.5, 7.5], [4, 4.5, 12.0],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_ntile_larger_than_partition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE small_data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO small_data VALUES (1, 10), (2, 20)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, value, NTILE(5) OVER (ORDER BY id) AS bucket FROM small_data ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 10, 1], [2, 20, 2],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lag_offset_larger_than_partition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE lag_data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO lag_data VALUES (1, 10), (2, 20)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, value, LAG(value, 10, -999) OVER (ORDER BY id) AS lagged FROM lag_data ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(result, [[1, 10, -999], [2, 20, -999],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lead_offset_larger_than_partition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE lead_data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO lead_data VALUES (1, 10), (2, 20)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, value, LEAD(value, 10, -999) OVER (ORDER BY id) AS led FROM lead_data ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(result, [[1, 10, -999], [2, 20, -999],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nth_value_first() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, NTH_VALUE(employee, 1) OVER (ORDER BY amount DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, "Alice"],
            ["Bob", 1500, "Alice"],
            ["Alice", 2000, "Alice"],
            ["Charlie", 800, "Alice"],
            ["Diana", 1200, "Alice"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_window_empty_frame() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE frame_test (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO frame_test VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, value, AVG(value) OVER (ORDER BY id ROWS BETWEEN 10 PRECEDING AND 5 PRECEDING) AS avg_val FROM frame_test ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(result, [[1, 10, 10.0], [2, 20, 10.0], [3, 30, 10.0],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_star_window() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, COUNT(*) OVER () AS total_count FROM sales ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 5],
            ["Bob", 1500, 5],
            ["Alice", 2000, 5],
            ["Charlie", 800, 5],
            ["Diana", 1200, 5],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_with_empty_table() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE empty_table (id INT64, value INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, value, SUM(value) OVER () AS total FROM empty_table ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_distinct_windows() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, ROW_NUMBER() OVER (ORDER BY id) AS rn_by_id, ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn_by_amount FROM sales ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 1, 4],
            ["Bob", 1500, 2, 2],
            ["Alice", 2000, 3, 1],
            ["Charlie", 800, 4, 5],
            ["Diana", 1200, 5, 3],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_case_else_clause() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT employee, amount, CASE WHEN amount > 2000 THEN 'high' ELSE CAST(RANK() OVER (ORDER BY amount DESC) AS STRING) END AS ranking FROM sales ORDER BY amount DESC",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 2000, "1"],
            ["Bob", 1500, "2"],
            ["Diana", 1200, "3"],
            ["Alice", 1000, "4"],
            ["Charlie", 800, "5"],
        ]
    );
}
