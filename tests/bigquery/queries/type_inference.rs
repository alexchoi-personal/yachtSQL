use yachtsql::RecordBatchVecExt;

use crate::assert_table_eq;
use crate::common::{create_session, d, dt, n, time, ts};

#[tokio::test(flavor = "current_thread")]
async fn test_binary_op_int64_float64_coercion() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 + 2.5").await.unwrap();
    assert_table_eq!(result, [[n("7.5")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_op_float64_int64_coercion() {
    let session = create_session();
    let result = session.execute_sql("SELECT 2.5 * 4").await.unwrap();
    assert_table_eq!(result, [[n("10.0")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_op_both_int64() {
    let session = create_session();
    let result = session.execute_sql("SELECT 10 - 3").await.unwrap();
    assert_table_eq!(result, [[7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_op_concat_returns_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' || ' ' || 'world'")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_op_comparison_returns_bool() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 5 > 3, 5 < 3, 5 = 5, 5 != 3, 5 >= 5, 5 <= 6")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, false, true, true, true, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_binary_op_logical_returns_bool() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT true AND false, true OR false")
        .await
        .unwrap();
    assert_table_eq!(result, [[false, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unary_op_not_returns_bool() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NOT true, NOT false")
        .await
        .unwrap();
    assert_table_eq!(result, [[false, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unary_op_negative_preserves_type() {
    let session = create_session();
    let result = session.execute_sql("SELECT -5, -3.14").await.unwrap();
    assert_table_eq!(result, [[-5, n("-3.14")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_count_returns_int64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_count_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_count_test VALUES (1), (2), (3)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT COUNT(*), COUNT(val) FROM ti_count_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[3, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_count_if_returns_int64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_count_if_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_count_if_test VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT COUNTIF(val > 2) FROM ti_count_if_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_min_max_preserve_type_int() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_minmax_int (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_minmax_int VALUES (10), (20), (5)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT MIN(val), MAX(val) FROM ti_minmax_int")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_min_max_preserve_type_string() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_minmax_str (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_minmax_str VALUES ('apple'), ('banana'), ('cherry')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT MIN(val), MAX(val) FROM ti_minmax_str")
        .await
        .unwrap();
    assert_table_eq!(result, [["apple", "cherry"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_avg_returns_float64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_avg_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_avg_test VALUES (10), (20), (30)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT AVG(val) FROM ti_avg_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[20.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_sum_returns_float64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_sum_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_sum_test VALUES (10), (20), (30)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT SUM(val) FROM ti_sum_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[60.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_stddev_returns_float64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_stddev_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_stddev_test VALUES (2), (4), (4), (4), (5), (5), (7), (9)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT ROUND(STDDEV(val), 2) FROM ti_stddev_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[2.14]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_variance_returns_float64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_var_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_var_test VALUES (2), (4), (4), (4), (5), (5), (7), (9)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT ROUND(VARIANCE(val), 2) FROM ti_var_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[4.57]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_string_agg_returns_string() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_str_agg_test (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_str_agg_test VALUES ('a'), ('b'), ('c')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT STRING_AGG(val, ',') FROM ti_str_agg_test")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let val = records[0].values()[0].as_str().unwrap();
    assert!(val.contains('a') && val.contains('b') && val.contains('c'));
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_array_agg_returns_array() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_arr_agg_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_arr_agg_test VALUES (1), (2), (3)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(ARRAY_AGG(val)) FROM ti_arr_agg_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_logical_and_returns_bool() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_logical_and_test (val BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_logical_and_test VALUES (true), (true), (true)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT LOGICAL_AND(val) FROM ti_logical_and_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_logical_or_returns_bool() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_logical_or_test (val BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_logical_or_test VALUES (false), (false), (true)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT LOGICAL_OR(val) FROM ti_logical_or_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_bit_and_returns_int64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_bit_and_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_bit_and_test VALUES (7), (3), (5)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT BIT_AND(val) FROM ti_bit_and_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_bit_or_returns_int64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_bit_or_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_bit_or_test VALUES (1), (2), (4)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT BIT_OR(val) FROM ti_bit_or_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_bit_xor_returns_int64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_bit_xor_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_bit_xor_test VALUES (5), (3)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT BIT_XOR(val) FROM ti_bit_xor_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_approx_count_distinct_returns_int64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_approx_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_approx_test VALUES (1), (2), (2), (3), (3), (3)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT APPROX_COUNT_DISTINCT(val) FROM ti_approx_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_row_number_returns_int64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_win_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_win_test VALUES (30), (10), (20)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "SELECT val, ROW_NUMBER() OVER (ORDER BY val) AS rn FROM ti_win_test ORDER BY val",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 1], [20, 2], [30, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_rank_returns_int64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_rank_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_rank_test VALUES (10), (10), (20)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "SELECT val, RANK() OVER (ORDER BY val) AS rnk FROM ti_rank_test ORDER BY val, rnk",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 1], [10, 1], [20, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_dense_rank_returns_int64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_dense_rank_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_dense_rank_test VALUES (10), (10), (20)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT val, DENSE_RANK() OVER (ORDER BY val) AS drnk FROM ti_dense_rank_test ORDER BY val, drnk")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 1], [10, 1], [20, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_ntile_returns_int64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_ntile_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_ntile_test VALUES (1), (2), (3), (4)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "SELECT val, NTILE(2) OVER (ORDER BY val) AS bucket FROM ti_ntile_test ORDER BY val",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 1], [2, 1], [3, 2], [4, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_percent_rank_returns_float64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_prank_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_prank_test VALUES (10), (20), (30)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT val, PERCENT_RANK() OVER (ORDER BY val) AS prnk FROM ti_prank_test ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 0.0], [20, 0.5], [30, 1.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_cume_dist_returns_float64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_cume_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_cume_test VALUES (10), (20), (30)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT val, ROUND(CUME_DIST() OVER (ORDER BY val), 2) AS cd FROM ti_cume_test ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 0.33], [20, 0.67], [30, 1.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_lead_lag_preserve_type() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_lead_lag_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_lead_lag_test VALUES (10), (20), (30)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT val, LAG(val) OVER (ORDER BY val) AS prev_val, LEAD(val) OVER (ORDER BY val) AS next_val FROM ti_lead_lag_test ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, null, 20], [20, 10, 30], [30, 20, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_first_last_value_preserve_type() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_first_last_test (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_first_last_test VALUES ('a'), ('b'), ('c')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT val, FIRST_VALUE(val) OVER (ORDER BY val) AS first_v, LAST_VALUE(val) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_v FROM ti_first_last_test ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [["a", "a", "c"], ["b", "a", "c"], ["c", "a", "c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_nth_value_preserve_type() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_nth_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_nth_test VALUES (10), (20), (30)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT val, NTH_VALUE(val, 2) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_v FROM ti_nth_test ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 20], [20, 20], [30, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_aggregate_count_returns_int64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_win_count_test (grp STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_win_count_test VALUES ('a', 1), ('a', 2), ('b', 3)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT grp, val, COUNT(*) OVER (PARTITION BY grp) AS cnt FROM ti_win_count_test ORDER BY grp, val")
        .await
        .unwrap();
    assert_table_eq!(result, [["a", 1, 2], ["a", 2, 2], ["b", 3, 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_aggregate_sum_returns_int() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_win_sum_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_win_sum_test VALUES (10), (20), (30)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT val, SUM(val) OVER (ORDER BY val) AS running_sum FROM ti_win_sum_test ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 10], [20, 30], [30, 60]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_aggregate_avg_returns_float64() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_win_avg_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_win_avg_test VALUES (10), (20), (30)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT val, AVG(val) OVER () AS avg_val FROM ti_win_avg_test ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 20.0], [20, 20.0], [30, 20.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_aggregate_min_max_preserve_type() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_win_minmax_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_win_minmax_test VALUES (10), (20), (30)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT val, MIN(val) OVER () AS min_v, MAX(val) OVER () AS max_v FROM ti_win_minmax_test ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 10, 30], [20, 10, 30], [30, 10, 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_when_infers_result_type() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END")
        .await
        .unwrap();
    assert_table_eq!(result, [["yes"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_else_only() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CASE WHEN 1 = 2 THEN 'never' ELSE 42 END")
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_date_returns_date() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DATE FROM TIMESTAMP '2024-06-15 10:30:00')")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 6, 15)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_time_returns_time() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(TIME FROM DATETIME '2024-06-15 10:30:45')")
        .await
        .unwrap();
    assert_table_eq!(result, [[time(10, 30, 45)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_year_returns_int64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(YEAR FROM DATE '2024-06-15')")
        .await
        .unwrap();
    assert_table_eq!(result, [[2024]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_month_returns_int64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MONTH FROM DATE '2024-06-15')")
        .await
        .unwrap();
    assert_table_eq!(result, [[6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_day_returns_int64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAY FROM DATE '2024-06-15')")
        .await
        .unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_hour_returns_int64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(HOUR FROM TIME '10:30:45')")
        .await
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_minute_returns_int64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MINUTE FROM TIME '10:30:45')")
        .await
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_second_returns_int64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(SECOND FROM TIME '10:30:45')")
        .await
        .unwrap();
    assert_table_eq!(result, [[45]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_dayofweek_returns_int64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAYOFWEEK FROM DATE '2024-06-15')")
        .await
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_dayofyear_returns_int64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAYOFYEAR FROM DATE '2024-06-15')")
        .await
        .unwrap();
    assert_table_eq!(result, [[167]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_quarter_returns_int64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(QUARTER FROM DATE '2024-06-15')")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_week_returns_int64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(WEEK FROM DATE '2024-06-15')")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let week = records[0].values()[0].as_i64().unwrap();
    assert!((1..=53).contains(&week));
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_literal_infers_element_type() {
    let session = create_session();
    let result = session.execute_sql("SELECT [1, 2, 3]").await.unwrap();
    assert_table_eq!(result, [[[1, 2, 3]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_with_explicit_type() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY<STRING>['a', 'b', 'c']")
        .await
        .unwrap();
    assert_table_eq!(result, [[["a", "b", "c"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_returns_element_type() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [10, 20, 30][OFFSET(1)]")
        .await
        .unwrap();
    assert_table_eq!(result, [[20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_string_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ['a', 'b', 'c'][OFFSET(0)]")
        .await
        .unwrap();
    assert_table_eq!(result, [["a"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_with_named_fields() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRUCT(1 AS id, 'test' AS name)")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let s = records[0].values()[0].as_struct().unwrap();
    assert_eq!(s[0].0, "id");
    assert_eq!(s[0].1.as_i64().unwrap(), 1);
    assert_eq!(s[1].0, "name");
    assert_eq!(s[1].1.as_str().unwrap(), "test");
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_with_unnamed_fields() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRUCT(1, 'test', true)")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let s = records[0].values()[0].as_struct().unwrap();
    assert_eq!(s.len(), 3);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_access_field_type() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT STRUCT(42 AS num, 'hello' AS txt).num, STRUCT(42 AS num, 'hello' AS txt).txt",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[42, "hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_struct_access() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRUCT(STRUCT(1 AS x, 2 AS y) AS inner, 'outer' AS name).inner.x")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_from_table_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_struct_col_test (data STRUCT<a INT64, b STRING>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_struct_col_test VALUES (STRUCT(100 AS a, 'hello' AS b))")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT data.a, data.b FROM ti_struct_col_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[100, "hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_subquery_single_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_arr_sub_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_arr_sub_test VALUES (1), (2), (3)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT ARRAY(SELECT val FROM ti_arr_sub_test ORDER BY val)")
        .await
        .unwrap();
    assert_table_eq!(result, [[[1, 2, 3]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_subquery_multi_column_as_struct() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_arr_struct_test (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_arr_struct_test VALUES (1, 'a'), (2, 'b')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT ARRAY(SELECT AS STRUCT id, name FROM ti_arr_struct_test ORDER BY id)")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let arr = records[0].values()[0].as_array().unwrap();
    assert_eq!(arr.len(), 2);
    let s0 = arr[0].as_struct().unwrap();
    assert_eq!(s0[0].1.as_i64().unwrap(), 1);
    assert_eq!(s0[1].1.as_str().unwrap(), "a");
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_expression_type() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(123 AS STRING), CAST('456' AS INT64), CAST(1 AS BOOL)")
        .await
        .unwrap();
    assert_table_eq!(result, [["123", 456, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_null_returns_bool() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULL IS NULL, 1 IS NULL, 'x' IS NOT NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, false, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_list_returns_bool() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 1 IN (1, 2, 3), 5 IN (1, 2, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_between_returns_bool() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 5 BETWEEN 1 AND 10, 15 BETWEEN 1 AND 10")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_returns_bool() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' LIKE 'hel%', 'hello' LIKE 'xyz%'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_distinct_from_returns_bool() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT 1 IS DISTINCT FROM 2, 1 IS DISTINCT FROM 1, NULL IS DISTINCT FROM NULL",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[true, false, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_exists_returns_bool() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_exists_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_exists_test VALUES (1)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT EXISTS(SELECT 1 FROM ti_exists_test), EXISTS(SELECT 1 FROM ti_exists_test WHERE val > 100)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_subquery_returns_bool() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_in_sub_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_in_sub_test VALUES (1), (2), (3)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "SELECT 2 IN (SELECT val FROM ti_in_sub_test), 5 IN (SELECT val FROM ti_in_sub_test)",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[true, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_unnest_returns_bool() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 2 IN UNNEST([1, 2, 3]), 5 IN UNNEST([1, 2, 3])")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alias_preserves_type() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 42 AS my_number, 'hello' AS my_string")
        .await
        .unwrap();
    assert_table_eq!(result, [[42, "hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typed_string_date() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-06-15'")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 6, 15)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typed_string_time() {
    let session = create_session();
    let result = session.execute_sql("SELECT TIME '10:30:45'").await.unwrap();
    assert_table_eq!(result, [[time(10, 30, 45)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typed_string_datetime() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME '2024-06-15 10:30:45'")
        .await
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 15, 10, 30, 45)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typed_string_timestamp() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP '2024-06-15 10:30:45'")
        .await
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 6, 15, 10, 30, 45)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_date_returns_date() {
    let session = create_session();
    let result = session.execute_sql("SELECT CURRENT_DATE()").await.unwrap();
    let records = result.to_records().unwrap();
    assert!(records[0].values()[0].as_date().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_time_returns_time() {
    let session = create_session();
    let result = session.execute_sql("SELECT CURRENT_TIME()").await.unwrap();
    let records = result.to_records().unwrap();
    assert!(records[0].values()[0].as_time().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_datetime_returns_datetime() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CURRENT_DATETIME()")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    assert!(records[0].values()[0].as_datetime().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_timestamp_returns_timestamp() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CURRENT_TIMESTAMP()")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    assert!(records[0].values()[0].as_timestamp().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_length_returns_int64() {
    let session = create_session();
    let result = session.execute_sql("SELECT LENGTH('hello')").await.unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_upper_returns_string() {
    let session = create_session();
    let result = session.execute_sql("SELECT UPPER('hello')").await.unwrap();
    assert_table_eq!(result, [["HELLO"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_sqrt_returns_float64() {
    let session = create_session();
    let result = session.execute_sql("SELECT SQRT(16)").await.unwrap();
    assert_table_eq!(result, [[4.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_abs_preserves_type() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ABS(-5), ABS(-3.14)")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, n("3.14")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_coalesce_preserves_type() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COALESCE(NULL, 42), COALESCE(NULL, 'default')")
        .await
        .unwrap();
    assert_table_eq!(result, [[42, "default"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_if_preserves_type() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT IF(true, 100, 200), IF(false, 'yes', 'no')")
        .await
        .unwrap();
    assert_table_eq!(result, [[100, "no"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_ifnull_preserves_type() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT IFNULL(NULL, 42), IFNULL(100, 42)")
        .await
        .unwrap();
    assert_table_eq!(result, [[42, 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_nullif_preserves_type() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULLIF(5, 5), NULLIF(5, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null, 5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_split_returns_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SPLIT('a,b,c', ',')")
        .await
        .unwrap();
    assert_table_eq!(result, [[["a", "b", "c"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_array_length_returns_int64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_LENGTH([1, 2, 3, 4, 5])")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_array_to_string_returns_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_TO_STRING([1, 2, 3], '-')")
        .await
        .unwrap();
    assert_table_eq!(result, [["1-2-3"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_generate_array_returns_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT GENERATE_ARRAY(1, 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[[1, 2, 3, 4, 5]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_starts_with_returns_bool() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STARTS_WITH('hello', 'hel'), STARTS_WITH('hello', 'xyz')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_ends_with_returns_bool() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ENDS_WITH('hello', 'llo'), ENDS_WITH('hello', 'xyz')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_contains_returns_bool() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT CONTAINS_SUBSTR('hello world', 'wor'), CONTAINS_SUBSTR('hello world', 'xyz')",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[true, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_regexp_contains_returns_bool() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_CONTAINS('hello', 'h.l'), REGEXP_CONTAINS('hello', 'x.y')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_to_json_returns_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TO_JSON_STRING(STRUCT(1 AS a, 'b' AS b))")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    let json_str = records[0].values()[0].as_str().unwrap();
    assert!(json_str.contains("\"a\"") && json_str.contains("\"b\""));
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_function_parse_json_returns_json() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE(PARSE_JSON('{\"key\": \"value\"}'), '$.key')")
        .await
        .unwrap();
    assert_table_eq!(result, [["value"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sql_type_numeric_with_precision_scale() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_numeric_ps_test (val NUMERIC(10, 2))")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_numeric_ps_test VALUES (123.45)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT ROUND(val, 2) FROM ti_numeric_ps_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[n("123.45")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sql_type_array_with_element_type() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_array_type_test (vals ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_array_type_test VALUES ([1, 2, 3])")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT vals FROM ti_array_type_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[[1, 2, 3]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sql_type_struct_with_fields() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_struct_type_test (data STRUCT<id INT64, name STRING>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_struct_type_test VALUES (STRUCT(1 AS id, 'test' AS name))")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT data.id, data.name FROM ti_struct_type_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sql_type_json() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_json_type_test (data JSON)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_json_type_test VALUES (JSON '{\"key\": \"value\"}')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT JSON_VALUE(data, '$.key') FROM ti_json_type_test")
        .await
        .unwrap();
    assert_table_eq!(result, [["value"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sql_type_bytes() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_bytes_type_test (data BYTES)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_bytes_type_test VALUES (b'hello')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT LENGTH(data) FROM ti_bytes_type_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ddl_primary_key_constraint() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_pk_test (id INT64, name STRING, PRIMARY KEY (id))")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_pk_test VALUES (1, 'test')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT * FROM ti_pk_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ddl_not_null_constraint() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_nn_test (id INT64 NOT NULL, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_nn_test VALUES (1, 'test')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT * FROM ti_nn_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_index_resolution() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_idx_test (a INT64, b STRING, c FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_idx_test VALUES (1, 'x', 1.5), (2, 'y', 2.5)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT a, b, c FROM ti_idx_test ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "x", 1.5], [2, "y", 2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_name_resolution() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_name_test (first_col INT64, second_col STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_name_test VALUES (100, 'hello')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT first_col, second_col FROM ti_name_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[100, "hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_rename() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_rename_test (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_rename_test VALUES (1, 'a')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT t.id, t.val FROM ti_rename_test AS t")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "a"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_grouping_function() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_grouping_test (a STRING, b STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO ti_grouping_test VALUES ('x', 'p', 1), ('x', 'q', 2), ('y', 'p', 3)",
        )
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT a, b, SUM(val), GROUPING(a) FROM ti_grouping_test GROUP BY ROLLUP(a, b) ORDER BY a NULLS LAST, b NULLS LAST")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    assert!(!records.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn test_stddev_pop_samp() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_stddev_ps_test (val FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_stddev_ps_test VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "SELECT ROUND(STDDEV_POP(val), 2), ROUND(STDDEV_SAMP(val), 2) FROM ti_stddev_ps_test",
        )
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    assert_eq!(records.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn test_var_pop_samp() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_var_ps_test (val FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_var_ps_test VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT ROUND(VAR_POP(val), 2), ROUND(VAR_SAMP(val), 2) FROM ti_var_ps_test")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    assert_eq!(records.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn test_corr_covar() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_corr_test (x FLOAT64, y FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_corr_test VALUES (1, 2), (2, 4), (3, 6), (4, 8)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT ROUND(CORR(x, y), 2) FROM ti_corr_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_covar_pop_samp() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_covar_test (x FLOAT64, y FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_covar_test VALUES (1, 2), (2, 4), (3, 6), (4, 8)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "SELECT ROUND(COVAR_POP(x, y), 2), ROUND(COVAR_SAMP(x, y), 2) FROM ti_covar_test",
        )
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    assert_eq!(records.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn test_type_of_function() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(1), TYPEOF('hello'), TYPEOF(true), TYPEOF(1.5)")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    assert_eq!(records.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn test_from_base64_returns_bytes() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LENGTH(FROM_BASE64('aGVsbG8='))")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_to_base64_returns_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TO_BASE64(b'hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [["aGVsbG8="]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_from_hex_returns_bytes() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LENGTH(FROM_HEX('48454c4c4f'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_to_hex_returns_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TO_HEX(b'hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [["68656c6c6f"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_make_interval_returns_interval() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT MAKE_INTERVAL(1, 2, 3)")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    assert!(records[0].values()[0].as_interval().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn test_generate_uuid_returns_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LENGTH(GENERATE_UUID())")
        .await
        .unwrap();
    assert_table_eq!(result, [[36]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_md5_sha_returns_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LENGTH(MD5('hello')), LENGTH(SHA1('hello')), LENGTH(SHA256('hello'))")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    assert_eq!(records.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_query_returns_json() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT JSON_VALUE(JSON_QUERY(PARSE_JSON('{\"a\": {\"b\": 1}}'), '$.a'), '$.b')",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["1"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_concat_returns_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_CONCAT([1, 2], [3, 4])")
        .await
        .unwrap();
    assert_table_eq!(result, [[[1, 2, 3, 4]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_reverse_returns_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_REVERSE([1, 2, 3])")
        .await
        .unwrap();
    assert_table_eq!(result, [[[3, 2, 1]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_date_add_sub_returns_date() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE_ADD(DATE '2024-01-01', INTERVAL 10 DAY), DATE_SUB(DATE '2024-01-15', INTERVAL 5 DAY)")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 11), d(2024, 1, 10)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_date_diff_returns_int64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE_DIFF(DATE '2024-01-15', DATE '2024-01-10', DAY)")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_date_trunc_returns_date() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE_TRUNC(DATE '2024-06-15', MONTH)")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 6, 1)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_date_returns_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT_DATE('%Y-%m-%d', DATE '2024-06-15')")
        .await
        .unwrap();
    assert_table_eq!(result, [["2024-06-15"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_parse_date_returns_date() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT PARSE_DATE('%Y-%m-%d', '2024-06-15')")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 6, 15)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_last_day_returns_date() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAST_DAY(DATE '2024-02-15')")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 2, 29)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unix_date_returns_int64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT UNIX_DATE(DATE '1970-01-02')")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_date_from_unix_date_returns_date() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE_FROM_UNIX_DATE(1)")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(1970, 1, 2)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_farm_fingerprint_returns_int64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FARM_FINGERPRINT('hello') != 0")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_chr_returns_string() {
    let session = create_session();
    let result = session.execute_sql("SELECT CHR(65)").await.unwrap();
    assert_table_eq!(result, [["A"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ascii_returns_int64() {
    let session = create_session();
    let result = session.execute_sql("SELECT ASCII('A')").await.unwrap();
    assert_table_eq!(result, [[65]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bit_count_returns_int64() {
    let session = create_session();
    let result = session.execute_sql("SELECT BIT_COUNT(7)").await.unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_pi_returns_float64() {
    let session = create_session();
    let result = session.execute_sql("SELECT ROUND(PI(), 5)").await.unwrap();
    assert_table_eq!(result, [[3.14159]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_any_value_aggregate() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE ti_any_val_test (grp STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO ti_any_val_test VALUES ('a', 1), ('a', 2), ('b', 3)")
        .await
        .unwrap();
    let result = session
        .execute_sql(
            "SELECT grp, ANY_VALUE(val) IS NOT NULL FROM ti_any_val_test GROUP BY grp ORDER BY grp",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["a", true], ["b", true]]);
}
