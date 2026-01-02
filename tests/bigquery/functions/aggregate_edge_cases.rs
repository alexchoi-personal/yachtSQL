use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::create_session;

async fn setup_edge_case_table(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE edge_data (id INT64, category STRING, value INT64, score FLOAT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO edge_data VALUES (1, 'A', 10, 1.5, 'alpha'), (2, 'A', 20, 2.5, 'beta'), (3, 'A', 10, 3.5, 'gamma'), (4, 'B', 30, 4.5, 'delta'), (5, 'B', 40, 5.5, 'epsilon'), (6, 'B', 30, 6.5, 'zeta'), (7, 'C', NULL, 7.5, 'eta'), (8, 'C', 50, NULL, 'theta')")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_distinct_with_nulls_in_group() {
    let session = create_session();
    setup_edge_case_table(&session).await;

    let result = session
        .execute_sql("SELECT category, COUNT(DISTINCT value) FROM edge_data GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 2], ["B", 2], ["C", 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sum_distinct_without_duplicates() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sum_dist (category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO sum_dist VALUES ('A', 10), ('A', 20), ('A', 30), ('B', 15), ('B', 25)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, SUM(DISTINCT value) FROM sum_dist GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 60], ["B", 40]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_avg_distinct_without_duplicates() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE avg_dist (category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO avg_dist VALUES ('A', 10), ('A', 20), ('A', 30), ('B', 15), ('B', 25)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, AVG(DISTINCT value) FROM avg_dist GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 20.0], ["B", 20.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_with_order_by_asc() {
    let session = create_session();
    setup_edge_case_table(&session).await;

    let result = session
        .execute_sql("SELECT category, ARRAY_AGG(name ORDER BY name ASC) FROM edge_data GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["A", ["alpha", "beta", "gamma"]],
            ["B", ["delta", "epsilon", "zeta"]],
            ["C", ["eta", "theta"]]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_with_order_by_multiple_keys() {
    let session = create_session();
    setup_edge_case_table(&session).await;

    let result = session
        .execute_sql("SELECT category, ARRAY_AGG(name ORDER BY value, id DESC) FROM edge_data WHERE category = 'A' GROUP BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", ["gamma", "alpha", "beta"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_with_limit_and_order() {
    let session = create_session();
    setup_edge_case_table(&session).await;

    let result = session
        .execute_sql("SELECT category, ARRAY_AGG(name ORDER BY id LIMIT 2) FROM edge_data GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["A", ["alpha", "beta"]],
            ["B", ["delta", "epsilon"]],
            ["C", ["eta", "theta"]]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_ignore_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE null_data (category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO null_data VALUES ('A', 1), ('A', NULL), ('A', 3), ('B', NULL), ('B', NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, ARRAY_AGG(value IGNORE NULLS ORDER BY value) FROM null_data GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", [1, 3]], ["B", []]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_respect_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE null_data2 (category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO null_data2 VALUES ('A', 1), ('A', NULL), ('A', 3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, ARRAY_AGG(value ORDER BY value NULLS LAST) FROM null_data2 GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", [1, 3, null]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_agg_with_order_by() {
    let session = create_session();
    setup_edge_case_table(&session).await;

    let result = session
        .execute_sql("SELECT category, STRING_AGG(name, ',' ORDER BY name) FROM edge_data GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["A", "alpha,beta,gamma"],
            ["B", "delta,epsilon,zeta"],
            ["C", "eta,theta"]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_agg_with_order_by_numeric() {
    let session = create_session();
    setup_edge_case_table(&session).await;

    let result = session
        .execute_sql("SELECT category, STRING_AGG(name, '-' ORDER BY id) FROM edge_data GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["A", "alpha-beta-gamma"],
            ["B", "delta-epsilon-zeta"],
            ["C", "eta-theta"]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_agg_empty_separator() {
    let session = create_session();
    setup_edge_case_table(&session).await;

    let result = session
        .execute_sql("SELECT category, STRING_AGG(name, '' ORDER BY id) FROM edge_data WHERE category = 'A' GROUP BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", "alphabetagamma"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_distinct_count_aggregates() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE multi_distinct (category STRING, product STRING, region STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO multi_distinct VALUES ('A', 'X', 'East'), ('A', 'Y', 'East'), ('A', 'X', 'West'), ('B', 'Z', 'North'), ('B', 'Z', 'South')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, COUNT(DISTINCT product) AS unique_products, COUNT(DISTINCT region) AS unique_regions FROM multi_distinct GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 2, 2], ["B", 1, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_and_non_distinct_same_query() {
    let session = create_session();
    setup_edge_case_table(&session).await;

    let result = session
        .execute_sql("SELECT category, COUNT(DISTINCT value) AS distinct_cnt, COUNT(value) AS total_cnt FROM edge_data GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 2, 3], ["B", 2, 3], ["C", 1, 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_distinct_all_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE all_nulls (category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO all_nulls VALUES ('A', NULL), ('A', NULL), ('A', NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, COUNT(DISTINCT value) FROM all_nulls GROUP BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sum_distinct_all_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sum_all_nulls (category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sum_all_nulls VALUES ('A', NULL), ('A', NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, SUM(DISTINCT value) FROM sum_all_nulls GROUP BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_distinct_single_value() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE single_val (category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO single_val VALUES ('A', 5), ('A', 5), ('A', 5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, COUNT(DISTINCT value) FROM single_val GROUP BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_limit_larger_than_data() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE small_data (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO small_data VALUES (1), (2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_AGG(value ORDER BY value LIMIT 10) FROM small_data")
        .await
        .unwrap();

    assert_table_eq!(result, [[[1, 2]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_limit_zero() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE limit_zero (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO limit_zero VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_AGG(value ORDER BY value LIMIT 0) FROM limit_zero")
        .await
        .unwrap();

    assert_table_eq!(result, [[[]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_agg_single_value() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE single_str (value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO single_str VALUES ('only')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT STRING_AGG(value, ',') FROM single_str")
        .await
        .unwrap();

    assert_table_eq!(result, [["only"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_agg_all_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE str_all_nulls (value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO str_all_nulls VALUES (NULL), (NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT STRING_AGG(value, ',') FROM str_all_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variance_single_value() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE var_single (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO var_single VALUES (42)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT VARIANCE(value), VAR_POP(value) FROM var_single")
        .await
        .unwrap();

    assert_table_eq!(result, [[null, 0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_stddev_single_value() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE stddev_single (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO stddev_single VALUES (42)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT STDDEV(value), STDDEV_POP(value) FROM stddev_single")
        .await
        .unwrap();

    assert_table_eq!(result, [[null, 0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_corr_single_pair() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE corr_single (x INT64, y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO corr_single VALUES (1, 2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT CORR(x, y) FROM corr_single")
        .await
        .unwrap();

    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_covariance_single_pair() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE covar_single (x INT64, y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO covar_single VALUES (1, 2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COVAR_SAMP(x, y), COVAR_POP(x, y) FROM covar_single")
        .await
        .unwrap();

    assert_table_eq!(result, [[null, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_distinct_strings() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE str_distinct (value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO str_distinct VALUES ('apple'), ('banana'), ('apple'), ('Cherry'), ('banana')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(DISTINCT value) FROM str_distinct")
        .await
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_distinct_floats() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE float_distinct (value FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO float_distinct VALUES (1.1), (2.2), (1.1), (3.3), (2.2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(DISTINCT value) FROM float_distinct")
        .await
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_min_if_with_group_by() {
    let session = create_session();
    setup_edge_case_table(&session).await;

    let result = session
        .execute_sql("SELECT category, MIN_IF(value, value > 15) FROM edge_data GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 20], ["B", 30], ["C", 50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_max_if_with_group_by() {
    let session = create_session();
    setup_edge_case_table(&session).await;

    let result = session
        .execute_sql("SELECT category, MAX_IF(value, value < 35) FROM edge_data GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 20], ["B", 30], ["C", null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_min_if_all_false_condition() {
    let session = create_session();
    setup_edge_case_table(&session).await;

    let result = session
        .execute_sql("SELECT category, MIN_IF(value, value > 1000) FROM edge_data GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", null], ["B", null], ["C", null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_max_if_all_false_condition() {
    let session = create_session();
    setup_edge_case_table(&session).await;

    let result = session
        .execute_sql("SELECT category, MAX_IF(value, value < 0) FROM edge_data GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", null], ["B", null], ["C", null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bit_aggregates_empty_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE empty_bits (value INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT BIT_AND(value), BIT_OR(value), BIT_XOR(value) FROM empty_bits")
        .await
        .unwrap();

    assert_table_eq!(result, [[null, null, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bit_aggregates_single_value() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE single_bit (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO single_bit VALUES (7)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT BIT_AND(value), BIT_OR(value), BIT_XOR(value) FROM single_bit")
        .await
        .unwrap();

    assert_table_eq!(result, [[7, 7, 7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_aggregates_empty_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE empty_bool (flag BOOL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT LOGICAL_AND(flag), LOGICAL_OR(flag) FROM empty_bool")
        .await
        .unwrap();

    assert_table_eq!(result, [[null, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_aggregates_single_value_true() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE single_bool_true (flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO single_bool_true VALUES (TRUE)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT LOGICAL_AND(flag), LOGICAL_OR(flag) FROM single_bool_true")
        .await
        .unwrap();

    assert_table_eq!(result, [[true, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_aggregates_single_value_false() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE single_bool_false (flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO single_bool_false VALUES (FALSE)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT LOGICAL_AND(flag), LOGICAL_OR(flag) FROM single_bool_false")
        .await
        .unwrap();

    assert_table_eq!(result, [[false, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sum_if_with_null_values() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sumif_nulls (value INT64, flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO sumif_nulls VALUES (10, TRUE), (NULL, TRUE), (30, FALSE), (40, TRUE)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT SUM_IF(value, flag) FROM sumif_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_avg_if_with_null_values() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE avgif_nulls (value INT64, flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO avgif_nulls VALUES (10, TRUE), (NULL, TRUE), (30, FALSE), (40, TRUE)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT AVG_IF(value, flag) FROM avgif_nulls")
        .await
        .unwrap();

    assert_table_eq!(result, [[25.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_if_all_null_conditions() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE countif_null_cond (value INT64, flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO countif_null_cond VALUES (10, NULL), (20, NULL), (30, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNTIF(flag) FROM countif_null_cond")
        .await
        .unwrap();

    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_with_complex_order() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE complex_order (name STRING, priority INT64, created INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO complex_order VALUES ('a', 1, 100), ('b', 2, 50), ('c', 1, 75), ('d', 2, 200)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT ARRAY_AGG(name ORDER BY priority ASC, created DESC) FROM complex_order",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[["a", "c", "d", "b"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_distinct_with_expressions() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE expr_count_distinct (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO expr_count_distinct VALUES (1), (2), (3), (4), (5), (6)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(DISTINCT value % 3) FROM expr_count_distinct")
        .await
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_filter_in_having() {
    let session = create_session();
    setup_edge_case_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(value) FROM edge_data GROUP BY category HAVING COUNT(DISTINCT value) > 1 ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 40], ["B", 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_array_aggs_different_orders() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE multi_order (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO multi_order VALUES (3), (1), (2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_AGG(value ORDER BY value ASC) AS asc_arr, ARRAY_AGG(value ORDER BY value DESC) AS desc_arr FROM multi_order")
        .await
        .unwrap();

    assert_table_eq!(result, [[[1, 2, 3], [3, 2, 1]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_any_value_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE any_nulls (category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO any_nulls VALUES ('A', NULL), ('A', 10), ('B', NULL), ('B', NULL)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, ANY_VALUE(value) IS NULL FROM any_nulls GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", false], ["B", true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_agg_with_special_characters() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE special_chars (value STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            r#"INSERT INTO special_chars VALUES ('hello, world'), ('foo; bar'), ('test|data')"#,
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT STRING_AGG(value, ' | ') FROM special_chars")
        .await
        .unwrap();

    assert_table_eq!(result, [["hello, world | foo; bar | test|data"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_distinct_with_case_sensitive_strings() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE case_sens (value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO case_sens VALUES ('Apple'), ('apple'), ('APPLE'), ('banana')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(DISTINCT value) FROM case_sens")
        .await
        .unwrap();

    assert_table_eq!(result, [[4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_min_max_with_string_edge_cases() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE str_minmax (value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO str_minmax VALUES (''), ('a'), ('A'), ('z'), ('Z'), ('0'), ('9')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT MIN(value), MAX(value) FROM str_minmax")
        .await
        .unwrap();

    assert_table_eq!(result, [["", "z"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_coalesce() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE coalesce_agg (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO coalesce_agg VALUES (NULL), (NULL), (NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COALESCE(SUM(value), 0), COALESCE(AVG(value), -1) FROM coalesce_agg")
        .await
        .unwrap();

    assert_table_eq!(result, [[0, -1.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_aggregates_in_expression() {
    let session = create_session();
    setup_edge_case_table(&session).await;

    let result = session
        .execute_sql("SELECT category, (SUM(value) - MIN(value)) / NULLIF(COUNT(*) - 1, 0) FROM edge_data WHERE value IS NOT NULL GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 15.0], ["B", 35.0], ["C", null]]);
}
