use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::create_session;

async fn setup_table(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE numbers (id INT64, value INT64, category STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO numbers VALUES (1, 10, 'A'), (2, 20, 'A'), (3, 30, 'B'), (4, 40, 'B'), (5, 50, 'B')").await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_star() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT COUNT(*) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_column() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT COUNT(value) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_with_nulls() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE nullable (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1), (NULL), (3), (NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*), COUNT(value) FROM nullable")
        .await
        .unwrap();

    assert_table_eq!(result, [[4, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sum() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT SUM(value) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[150]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sum_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sum_nulls (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sum_nulls VALUES (10), (NULL), (20), (NULL), (30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT SUM(value) FROM sum_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[60]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sum_all_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sum_all_nulls (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sum_all_nulls VALUES (NULL), (NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT SUM(value) FROM sum_all_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_avg() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT AVG(value) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[30.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_avg_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE avg_nulls (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO avg_nulls VALUES (10), (NULL), (20), (NULL), (30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT AVG(value) FROM avg_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[20.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_min() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT MIN(value) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_min_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE min_nulls (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO min_nulls VALUES (NULL), (30), (10), (NULL), (20)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT MIN(value) FROM min_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_max() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT MAX(value) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_max_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE max_nulls (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO max_nulls VALUES (NULL), (30), (10), (NULL), (50)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT MAX(value) FROM max_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_aggregates() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT COUNT(*), SUM(value), MIN(value), MAX(value) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[5, 150, 10, 50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_where() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT SUM(value) FROM numbers WHERE value > 20")
        .await
        .unwrap();

    assert_table_eq!(result, [[120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_empty_result() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT SUM(value) FROM numbers WHERE value > 100")
        .await
        .unwrap();

    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_empty_result() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT COUNT(*) FROM numbers WHERE value > 100")
        .await
        .unwrap();

    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sum_with_expression() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT SUM(value * 2) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[300]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_distinct() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT COUNT(DISTINCT category) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_distinct_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE distinct_nulls (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO distinct_nulls VALUES (1), (2), (1), (NULL), (2), (NULL), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(DISTINCT value) FROM distinct_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sum_distinct() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sum_distinct (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sum_distinct VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT SUM(DISTINCT value) FROM sum_distinct")
        .await
        .unwrap();

    assert_table_eq!(result, [[6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_avg_distinct() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE avg_distinct (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO avg_distinct VALUES (10), (20), (30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT AVG(DISTINCT value) FROM avg_distinct")
        .await
        .unwrap();

    assert_table_eq!(result, [[20.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_if() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT COUNT_IF(value > 20) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_countif() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT COUNTIF(value > 20) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_countif_all_false() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT COUNTIF(value > 100) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_countif_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE countif_nulls (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO countif_nulls VALUES (10), (NULL), (30), (NULL), (50)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNTIF(value > 20) FROM countif_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sum_if() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT SUM_IF(value, value > 20) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sumif() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT SUMIF(value, value > 20) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sumif_all_false() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT SUMIF(value, value > 100) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_avg_if() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT AVG_IF(value, value > 20) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[40.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_avgif() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT AVGIF(value, value > 20) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[40.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_or_all_true() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE logical_data (flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO logical_data VALUES (TRUE), (TRUE), (TRUE)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT LOGICAL_OR(flag) FROM logical_data")
        .await
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_or_mixed() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE logical_mixed (flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO logical_mixed VALUES (FALSE), (FALSE), (TRUE)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT LOGICAL_OR(flag) FROM logical_mixed")
        .await
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_or_all_false() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE logical_false (flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO logical_false VALUES (FALSE), (FALSE), (FALSE)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT LOGICAL_OR(flag) FROM logical_false")
        .await
        .unwrap();

    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_or_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE logical_or_nulls (flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO logical_or_nulls VALUES (FALSE), (NULL), (TRUE), (NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT LOGICAL_OR(flag) FROM logical_or_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_or_only_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE logical_or_only_nulls (flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO logical_or_only_nulls VALUES (NULL), (NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT LOGICAL_OR(flag) FROM logical_or_only_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_and_all_true() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE logical_and_true (flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO logical_and_true VALUES (TRUE), (TRUE), (TRUE)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT LOGICAL_AND(flag) FROM logical_and_true")
        .await
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_and_mixed() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE logical_and_mixed (flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO logical_and_mixed VALUES (TRUE), (TRUE), (FALSE)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT LOGICAL_AND(flag) FROM logical_and_mixed")
        .await
        .unwrap();

    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_and_all_false() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE logical_and_false (flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO logical_and_false VALUES (FALSE), (FALSE), (FALSE)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT LOGICAL_AND(flag) FROM logical_and_false")
        .await
        .unwrap();

    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_and_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE logical_and_nulls (flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO logical_and_nulls VALUES (TRUE), (NULL), (TRUE)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT LOGICAL_AND(flag) FROM logical_and_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_and_with_null_and_false() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE logical_and_null_false (flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO logical_and_null_false VALUES (TRUE), (NULL), (FALSE)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT LOGICAL_AND(flag) FROM logical_and_null_false")
        .await
        .unwrap();

    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_and_only_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE logical_and_only_nulls (flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO logical_and_only_nulls VALUES (NULL), (NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT LOGICAL_AND(flag) FROM logical_and_only_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bit_and_aggregate() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bits_and (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bits_and VALUES (7), (3), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT BIT_AND(val) FROM bits_and")
        .await
        .unwrap();

    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bit_and_all_ones() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bits_and_ones (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bits_and_ones VALUES (15), (15), (15)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT BIT_AND(val) FROM bits_and_ones")
        .await
        .unwrap();

    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bit_and_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bits_and_nulls (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bits_and_nulls VALUES (7), (NULL), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT BIT_AND(val) FROM bits_and_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bit_or_aggregate() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bits_or (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bits_or VALUES (1), (2), (4)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT BIT_OR(val) FROM bits_or")
        .await
        .unwrap();

    assert_table_eq!(result, [[7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bit_or_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bits_or_nulls (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bits_or_nulls VALUES (1), (NULL), (4)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT BIT_OR(val) FROM bits_or_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bit_xor_aggregate() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bits_xor (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bits_xor VALUES (5), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT BIT_XOR(val) FROM bits_xor")
        .await
        .unwrap();

    assert_table_eq!(result, [[6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bit_xor_cancellation() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bits_xor_cancel (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bits_xor_cancel VALUES (5), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT BIT_XOR(val) FROM bits_xor_cancel")
        .await
        .unwrap();

    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bit_xor_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bits_xor_nulls (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bits_xor_nulls VALUES (5), (NULL), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT BIT_XOR(val) FROM bits_xor_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_basic() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE arr_agg (category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO arr_agg VALUES ('A', 1), ('A', 2), ('B', 3)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT category, ARRAY_AGG(value ORDER BY value) FROM arr_agg GROUP BY category ORDER BY category",
        ).await
        .unwrap();

    assert_table_eq!(result, [["A", [1, 2]], ["B", [3]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE arr_agg_nulls (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO arr_agg_nulls VALUES (1), (NULL), (2), (NULL), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_AGG(value ORDER BY value NULLS LAST) FROM arr_agg_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[[1, 2, 3, null, null]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_ignore_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE arr_agg_ignore (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO arr_agg_ignore VALUES (1), (NULL), (2), (NULL), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_AGG(value IGNORE NULLS ORDER BY value) FROM arr_agg_ignore")
        .await
        .unwrap();

    assert_table_eq!(result, [[[1, 2, 3]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_order_by_desc() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE arr_agg_desc (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO arr_agg_desc VALUES (3), (1), (2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_AGG(value ORDER BY value DESC) FROM arr_agg_desc")
        .await
        .unwrap();

    assert_table_eq!(result, [[[3, 2, 1]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_with_limit() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE arr_agg_limit (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO arr_agg_limit VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_AGG(value ORDER BY value LIMIT 3) FROM arr_agg_limit")
        .await
        .unwrap();

    assert_table_eq!(result, [[[1, 2, 3]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_agg_basic() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE str_agg (value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO str_agg VALUES ('a'), ('b'), ('c')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT STRING_AGG(value, ',') FROM str_agg")
        .await
        .unwrap();

    assert_table_eq!(result, [["a,b,c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_agg_with_order() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE str_agg_order (value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO str_agg_order VALUES ('a'), ('b'), ('c')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT STRING_AGG(value, '-' ORDER BY value) FROM str_agg_order")
        .await
        .unwrap();

    assert_table_eq!(result, [["a-b-c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_agg_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE str_agg_nulls (value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO str_agg_nulls VALUES ('a'), (NULL), ('b'), (NULL), ('c')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT STRING_AGG(value, ',') FROM str_agg_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [["a,b,c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_agg_group_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE str_agg_group (category STRING, value STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO str_agg_group VALUES ('X', 'a'), ('X', 'b'), ('Y', 'c'), ('Y', 'd')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, STRING_AGG(value, ',' ORDER BY value) FROM str_agg_group GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["X", "a,b"], ["Y", "c,d"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_listagg() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT LISTAGG(category, ',') FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [["A,A,B,B,B"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_xmlagg() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE xml_data (content STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO xml_data VALUES ('<item>1</item>'), ('<item>2</item>'), ('<item>3</item>')").await
        .unwrap();

    let result = session
        .execute_sql("SELECT XMLAGG(content) FROM xml_data")
        .await
        .unwrap();

    assert_table_eq!(result, [["<item>1</item><item>2</item><item>3</item>"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_grouping_function_rollup() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE grouping_test (product STRING, region STRING, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO grouping_test VALUES ('A', 'East', 100), ('A', 'West', 150), ('B', 'East', 200)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT product, region, SUM(amount) AS total, GROUPING(product) AS gp, GROUPING(region) AS gr FROM grouping_test GROUP BY ROLLUP(product, region) ORDER BY product NULLS LAST, region NULLS LAST")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["A", "East", 100, 0, 0],
            ["A", "West", 150, 0, 0],
            ["A", null, 250, 0, 1],
            ["B", "East", 200, 0, 0],
            ["B", null, 200, 0, 1],
            [null, null, 450, 1, 1],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_grouping_id_cube() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE grouping_id_test (product STRING, region STRING, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO grouping_id_test VALUES ('A', 'East', 100), ('A', 'West', 150), ('B', 'East', 200)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT product, region, SUM(amount) AS total, GROUPING_ID(product, region) AS gid FROM grouping_id_test GROUP BY CUBE(product, region) ORDER BY gid, product, region")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["A", "East", 100, 0],
            ["A", "West", 150, 0],
            ["B", "East", 200, 0],
            ["A", null, 250, 1],
            ["B", null, 200, 1],
            [null, "East", 300, 2],
            [null, "West", 150, 2],
            [null, null, 450, 3],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_variance_basic() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE var_data (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO var_data VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT VARIANCE(val) FROM var_data")
        .await
        .unwrap();

    assert_table_eq!(result, [[2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variance_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE var_nulls (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO var_nulls VALUES (1), (NULL), (2), (3), (NULL), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT VARIANCE(val) FROM var_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_var_pop() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE var_pop_data (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO var_pop_data VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT VAR_POP(val) FROM var_pop_data")
        .await
        .unwrap();

    assert_table_eq!(result, [[2.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_var_samp() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE var_samp_data (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO var_samp_data VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT VAR_SAMP(val) FROM var_samp_data")
        .await
        .unwrap();

    assert_table_eq!(result, [[2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_stddev_basic() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE stddev_data (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO stddev_data VALUES (2), (4), (4), (4), (5), (5), (7), (9)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ROUND(STDDEV(val), 6) FROM stddev_data")
        .await
        .unwrap();

    assert_table_eq!(result, [[2.138090]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_stddev_pop() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE stddev_pop_data (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO stddev_pop_data VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ROUND(STDDEV_POP(val), 6) FROM stddev_pop_data")
        .await
        .unwrap();

    assert_table_eq!(result, [[1.414214]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_stddev_samp() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE stddev_samp_data (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO stddev_samp_data VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ROUND(STDDEV_SAMP(val), 6) FROM stddev_samp_data")
        .await
        .unwrap();

    assert_table_eq!(result, [[1.581139]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_corr_perfect() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE corr_data (x INT64, y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO corr_data VALUES (1, 2), (2, 4), (3, 6), (4, 8), (5, 10)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ROUND(CORR(x, y), 6) FROM corr_data")
        .await
        .unwrap();

    assert_table_eq!(result, [[1.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_corr_negative() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE corr_neg (x INT64, y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO corr_neg VALUES (1, 10), (2, 8), (3, 6), (4, 4), (5, 2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ROUND(CORR(x, y), 6) FROM corr_neg")
        .await
        .unwrap();

    assert_table_eq!(result, [[-1.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_corr_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE corr_nulls (x INT64, y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO corr_nulls VALUES (1, 2), (2, NULL), (3, 6), (NULL, 8), (5, 10)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ROUND(CORR(x, y), 6) FROM corr_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[1.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_covar_pop() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE covar_pop_data (x INT64, y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO covar_pop_data VALUES (1, 2), (2, 4), (3, 6)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ROUND(COVAR_POP(x, y), 6) FROM covar_pop_data")
        .await
        .unwrap();

    assert_table_eq!(result, [[1.333333]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_covar_samp() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE covar_samp_data (x INT64, y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO covar_samp_data VALUES (1, 2), (2, 4), (3, 6)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COVAR_SAMP(x, y) FROM covar_samp_data")
        .await
        .unwrap();

    assert_table_eq!(result, [[2.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_covar_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE covar_nulls (x INT64, y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO covar_nulls VALUES (1, 2), (2, NULL), (3, 6)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COVAR_SAMP(x, y) FROM covar_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[4.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_group_by() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(value), COUNT(*), AVG(value) FROM numbers GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 30, 2, 15.0], ["B", 120, 3, 40.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_having() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(value) AS total FROM numbers GROUP BY category HAVING SUM(value) > 50 ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["B", 120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_min_string() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE min_str (value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO min_str VALUES ('banana'), ('apple'), ('cherry')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT MIN(value) FROM min_str")
        .await
        .unwrap();

    assert_table_eq!(result, [["apple"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_max_string() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE max_str (value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO max_str VALUES ('banana'), ('apple'), ('cherry')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT MAX(value) FROM max_str")
        .await
        .unwrap();

    assert_table_eq!(result, [["cherry"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_any_value() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE any_val (grp STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO any_val VALUES ('A', 1), ('A', 2), ('B', 3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, ANY_VALUE(value) IS NOT NULL AS has_value FROM any_val GROUP BY grp ORDER BY grp")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", true], ["B", true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_aggregates_with_group_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE flags (grp STRING, flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO flags VALUES ('A', TRUE), ('A', TRUE), ('B', TRUE), ('B', FALSE)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, LOGICAL_AND(flag) AS all_true, LOGICAL_OR(flag) AS any_true FROM flags GROUP BY grp ORDER BY grp")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", true, true], ["B", false, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bit_aggregates_with_group_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bit_grp (grp STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bit_grp VALUES ('A', 7), ('A', 3), ('B', 1), ('B', 2), ('B', 4)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, BIT_AND(val) AS band, BIT_OR(val) AS bor, BIT_XOR(val) AS bxor FROM bit_grp GROUP BY grp ORDER BY grp")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 3, 7, 4], ["B", 0, 7, 7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_statistical_with_group_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE stat_grp (region STRING, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO stat_grp VALUES ('East', 100), ('East', 150), ('East', 200), ('West', 50), ('West', 75), ('West', 100)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT region, ROUND(STDDEV(amount), 2) AS std FROM stat_grp GROUP BY region ORDER BY region")
        .await
        .unwrap();

    assert_table_eq!(result, [["East", 50.0], ["West", 25.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_single_value_statistics() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE single_stat (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO single_stat VALUES (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT VARIANCE(val), STDDEV(val) FROM single_stat")
        .await
        .unwrap();

    assert_table_eq!(result, [[null, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_empty_table_statistics() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE empty_stat (val INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*), SUM(val), AVG(val), VARIANCE(val) FROM empty_stat")
        .await
        .unwrap();

    assert_table_eq!(result, [[0, null, null, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_aggregate() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT SUM(total) FROM (SELECT category, SUM(value) AS total FROM numbers GROUP BY category) AS sub")
        .await
        .unwrap();

    assert_table_eq!(result, [[150]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_case() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT SUM(CASE WHEN category = 'A' THEN value ELSE 0 END) AS sum_a, SUM(CASE WHEN category = 'B' THEN value ELSE 0 END) AS sum_b FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[30, 120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_binary_op_sum_plus_count() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT SUM(value) + COUNT(*) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[155]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_binary_op_multiple_operations() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT (SUM(value) - MIN(value)) * 2 FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[280]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_binary_op_avg_divided() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT AVG(value) / COUNT(*) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[6.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_binary_op_in_group_by() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(value) + COUNT(*) AS combined FROM numbers GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 32], ["B", 123]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_unary_op_negation() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT -SUM(value) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[-150]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_unary_op_not() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bool_agg (val BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bool_agg VALUES (TRUE), (TRUE), (FALSE)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT NOT LOGICAL_AND(val) FROM bool_agg")
        .await
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_unary_op_in_group_by() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category, -AVG(value) AS neg_avg FROM numbers GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", -15.0], ["B", -40.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_cast_to_float() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT CAST(SUM(value) AS FLOAT64) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[150.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_cast_to_string() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT CAST(COUNT(*) AS STRING) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [["5"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_safe_cast() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT SAFE_CAST(SUM(value) AS STRING) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [["150"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_cast_in_group_by() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CAST(SUM(value) AS FLOAT64) AS total FROM numbers GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 30.0], ["B", 120.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_case_with_operand() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT CASE COUNT(*) WHEN 5 THEN 'five' WHEN 3 THEN 'three' ELSE 'other' END FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [["five"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_case_when_with_aggregate_condition() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT CASE WHEN SUM(value) > 100 THEN 'high' WHEN SUM(value) > 50 THEN 'medium' ELSE 'low' END FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [["high"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_case_with_aggregate_result() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT CASE WHEN COUNT(*) > 3 THEN SUM(value) ELSE AVG(value) END FROM numbers",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[150.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_case_with_aggregate_else() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT CASE WHEN COUNT(*) < 3 THEN 0 ELSE MAX(value) END FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_alias() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT SUM(value) AS total_value, COUNT(*) AS row_count FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[150, 5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_nested_alias() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT SUM(value) + COUNT(*) AS combined_result FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[155]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_having_binary_op_and() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(value) FROM numbers GROUP BY category HAVING SUM(value) > 20 AND COUNT(*) > 1 ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 30], ["B", 120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_having_binary_op_or() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(value) FROM numbers GROUP BY category HAVING SUM(value) > 100 OR COUNT(*) = 2 ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 30], ["B", 120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_having_unary_op_not() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(value) FROM numbers GROUP BY category HAVING NOT SUM(value) < 50 ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["B", 120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_having_nested_expression() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(value) FROM numbers GROUP BY category HAVING (SUM(value) > 50) ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["B", 120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_having_complex_nested() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(value) FROM numbers GROUP BY category HAVING ((SUM(value) > 25) AND (COUNT(*) >= 2)) ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 30], ["B", 120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_aggregate() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT category, SUM(value) FROM numbers GROUP BY category ORDER BY SUM(value) DESC",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["B", 120], ["A", 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_aggregate_unary_op() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category, -SUM(value) AS neg_sum FROM numbers GROUP BY category ORDER BY neg_sum")
        .await
        .unwrap();

    assert_table_eq!(result, [["B", -120.0], ["A", -30.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_aggregate_nested() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT category, SUM(value) FROM numbers GROUP BY category ORDER BY (SUM(value)) DESC",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["B", 120], ["A", 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_aggregate_cast() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CAST(SUM(value) AS FLOAT64) AS total FROM numbers GROUP BY category ORDER BY total DESC")
        .await
        .unwrap();

    assert_table_eq!(result, [["B", 120.0], ["A", 30.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_is_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE null_agg (grp STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO null_agg VALUES ('A', 1), ('A', NULL), ('B', NULL), ('B', NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT grp, SUM(val) IS NULL AS is_null FROM null_agg GROUP BY grp ORDER BY grp",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["A", false], ["B", true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_is_not_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE notnull_agg (grp STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO notnull_agg VALUES ('A', 1), ('A', 2), ('B', NULL), ('B', NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, SUM(val) IS NOT NULL AS is_not_null FROM notnull_agg GROUP BY grp ORDER BY grp")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", true], ["B", false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_between() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(value) BETWEEN 25 AND 35 AS in_range FROM numbers GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", true], ["B", false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_not_between() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(value) NOT BETWEEN 25 AND 35 AS out_range FROM numbers GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", false], ["B", true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_between_with_aggregates() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT AVG(value) BETWEEN MIN(value) AND MAX(value) AS in_range FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_in_list() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category, CAST(SUM(value) AS INT64) IN (CAST(30 AS INT64), CAST(60 AS INT64), CAST(90 AS INT64), CAST(120 AS INT64)) AS in_list FROM numbers GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", true], ["B", true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_not_in_list() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(value) NOT IN (25, 50, 75, 100) AS not_in_list FROM numbers GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", true], ["B", true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_like() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE like_agg (grp STRING, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO like_agg VALUES ('A', 'abc'), ('A', 'abd'), ('B', 'xyz'), ('B', 'xyw')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, STRING_AGG(name, ',') LIKE 'ab%' AS starts_ab FROM like_agg GROUP BY grp ORDER BY grp")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", true], ["B", false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_not_like() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE notlike_agg (grp STRING, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO notlike_agg VALUES ('A', 'abc'), ('A', 'abd'), ('B', 'xyz'), ('B', 'xyw')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, STRING_AGG(name, ',') NOT LIKE 'ab%' AS not_starts_ab FROM notlike_agg GROUP BY grp ORDER BY grp")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", false], ["B", true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_scalar_function() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT ROUND(AVG(value), 2) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[30.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_scalar_function_multiple_args() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT COALESCE(SUM(value), 0) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[150]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_scalar_function_with_aggregates() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT GREATEST(SUM(value), COUNT(*) * 20) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[150]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_scalar_function_nested() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT ABS(MIN(value) - MAX(value)) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[40]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_aggregate_sum() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT id, value, SUM(value) OVER (ORDER BY id) AS running_sum FROM numbers ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, 10, 10],
            [2, 20, 30],
            [3, 30, 60],
            [4, 40, 100],
            [5, 50, 150]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_aggregate_count() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT id, value, COUNT(*) OVER (PARTITION BY category) AS cat_count FROM numbers ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [[1, 10, 2], [2, 20, 2], [3, 30, 3], [4, 40, 3], [5, 50, 3]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_aggregate_avg() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT id, value, AVG(value) OVER (PARTITION BY category) AS cat_avg FROM numbers ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, 10, 15.0],
            [2, 20, 15.0],
            [3, 30, 40.0],
            [4, 40, 40.0],
            [5, 50, 40.0]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_aggregate_min_max() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT id, value, MIN(value) OVER (PARTITION BY category) AS cat_min, MAX(value) OVER (PARTITION BY category) AS cat_max FROM numbers ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, 10, 10, 20],
            [2, 20, 10, 20],
            [3, 30, 30, 50],
            [4, 40, 30, 50],
            [5, 50, 30, 50]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_function_row_number() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT id, value, ROW_NUMBER() OVER (ORDER BY value DESC) AS rn FROM numbers ORDER BY rn")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [[5, 50, 1], [4, 40, 2], [3, 30, 3], [2, 20, 4], [1, 10, 5]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_function_rank() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE rank_data (id INT64, score INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO rank_data VALUES (1, 100), (2, 100), (3, 90), (4, 90), (5, 80)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, score, RANK() OVER (ORDER BY score DESC) AS rnk FROM rank_data ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [[1, 100, 1], [2, 100, 1], [3, 90, 3], [4, 90, 3], [5, 80, 5]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_function_dense_rank() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE dense_rank_data (id INT64, score INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO dense_rank_data VALUES (1, 100), (2, 100), (3, 90), (4, 90), (5, 80)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, score, DENSE_RANK() OVER (ORDER BY score DESC) AS drnk FROM dense_rank_data ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [[1, 100, 1], [2, 100, 1], [3, 90, 2], [4, 90, 2], [5, 80, 3]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_function_lead() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id, value, LEAD(value) OVER (ORDER BY id) AS next_val FROM numbers ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, 10, 20],
            [2, 20, 30],
            [3, 30, 40],
            [4, 40, 50],
            [5, 50, null]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_function_lag() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id, value, LAG(value) OVER (ORDER BY id) AS prev_val FROM numbers ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, 10, null],
            [2, 20, 10],
            [3, 30, 20],
            [4, 40, 30],
            [5, 50, 40]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_function_first_value() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT id, value, FIRST_VALUE(value) OVER (PARTITION BY category ORDER BY id) AS first_val FROM numbers ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, 10, 10],
            [2, 20, 10],
            [3, 30, 30],
            [4, 40, 30],
            [5, 50, 30]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_function_last_value() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT id, value, LAST_VALUE(value) OVER (PARTITION BY category ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_val FROM numbers ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, 10, 20],
            [2, 20, 20],
            [3, 30, 50],
            [4, 40, 50],
            [5, 50, 50]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_duplicate_aggregate_extraction() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT SUM(value), SUM(value) + 10, SUM(value) * 2 FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[150, 160, 300]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_duplicate_aggregate_in_group_by() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(value), SUM(value) + COUNT(*), SUM(value) * 2 FROM numbers GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 30, 32, 60], ["B", 120, 123, 240]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_group_by_expression_match() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(value) FROM numbers GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 30], ["B", 120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_complex_aggregate_expression() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT CASE WHEN SUM(value) > 100 THEN CAST(AVG(value) + COUNT(*) AS STRING) ELSE 'low' END FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [["35"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_having_with_aggregate_arithmetic() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(value) FROM numbers GROUP BY category HAVING SUM(value) + COUNT(*) > 100 ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["B", 120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_having_with_multiple_aggregates() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category FROM numbers GROUP BY category HAVING SUM(value) >= MIN(value) + MAX(value) ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A"], ["B"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_with_aggregate() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE arr_acc (grp STRING, vals ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO arr_acc VALUES ('A', [1, 2, 3]), ('A', [4, 5, 6]), ('B', [7, 8, 9])",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, ARRAY_AGG(vals[OFFSET(0)] ORDER BY vals[OFFSET(0)]) AS first_elements FROM arr_acc GROUP BY grp ORDER BY grp")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", [1, 4]], ["B", [7]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_in_subquery() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT * FROM (SELECT category, SUM(value) AS total FROM numbers GROUP BY category) WHERE total > 50 ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["B", 120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_scalar_subquery() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(value), (SELECT AVG(value) FROM numbers) AS overall_avg FROM numbers GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 30, 30.0], ["B", 120, 30.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_approx_count_distinct() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT APPROX_COUNT_DISTINCT(category) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_min_if() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT MIN_IF(value, value > 20) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_minif() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT MINIF(value, value > 20) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_max_if() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT MAX_IF(value, value < 40) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_maxif() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT MAXIF(value, value < 40) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_with_frame_rows() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT id, value, SUM(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS rolling_sum FROM numbers ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, 10, 30],
            [2, 20, 60],
            [3, 30, 90],
            [4, 40, 120],
            [5, 50, 90]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_with_frame_range() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT id, value, SUM(value) OVER (ORDER BY value RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum FROM numbers ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, 10, 10],
            [2, 20, 30],
            [3, 30, 60],
            [4, 40, 100],
            [5, 50, 150]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_ntile() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id, value, NTILE(2) OVER (ORDER BY value) AS bucket FROM numbers ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [[1, 10, 1], [2, 20, 1], [3, 30, 1], [4, 40, 2], [5, 50, 2]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_percent_rank() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE pr_data (id INT64, score INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO pr_data VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, score, PERCENT_RANK() OVER (ORDER BY score) AS prank FROM pr_data ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, 10, 0.0],
            [2, 20, 0.25],
            [3, 30, 0.5],
            [4, 40, 0.75],
            [5, 50, 1.0]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_cume_dist() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE cd_data (id INT64, score INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO cd_data VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, score, CUME_DIST() OVER (ORDER BY score) AS cdist FROM cd_data ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, 10, 0.2],
            [2, 20, 0.4],
            [3, 30, 0.6],
            [4, 40, 0.8],
            [5, 50, 1.0]
        ]
    );
}
