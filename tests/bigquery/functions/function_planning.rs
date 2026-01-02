use crate::assert_table_eq;
use crate::common::{create_session, d, dt, null, tm};

#[tokio::test(flavor = "current_thread")]
async fn test_normalize_nfd() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NORMALIZE('\\u00e9', NFD)")
        .await
        .unwrap();
    assert_table_eq!(result, [["\u{0065}\u{0301}"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_normalize_nfkd() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NORMALIZE('\\u2126', NFKD)")
        .await
        .unwrap();
    assert_table_eq!(result, [["\u{03A9}"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_normalize_nfc_explicit() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NORMALIZE('caf\\u0065\\u0301', NFC)")
        .await
        .unwrap();
    assert_table_eq!(result, [["caf\u{00E9}"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_make_interval_positional_args() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(YEAR FROM MAKE_INTERVAL(1, 0, 0, 0, 0, 0))")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_make_interval_positional_args_months() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MONTH FROM MAKE_INTERVAL(0, 3, 0, 0, 0, 0))")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_make_interval_positional_args_days() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAY FROM MAKE_INTERVAL(0, 0, 15, 0, 0, 0))")
        .await
        .unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_make_interval_positional_args_hours() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(HOUR FROM MAKE_INTERVAL(0, 0, 0, 12, 0, 0))")
        .await
        .unwrap();
    assert_table_eq!(result, [[12]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_make_interval_positional_args_minutes() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MINUTE FROM MAKE_INTERVAL(0, 0, 0, 0, 45, 0))")
        .await
        .unwrap();
    assert_table_eq!(result, [[45]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_make_interval_positional_args_seconds() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(SECOND FROM MAKE_INTERVAL(0, 0, 0, 0, 0, 30))")
        .await
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_make_interval_plural_years() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(YEAR FROM MAKE_INTERVAL(years => 2))")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_make_interval_plural_months() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MONTH FROM MAKE_INTERVAL(months => 6))")
        .await
        .unwrap();
    assert_table_eq!(result, [[6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_make_interval_plural_days() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAY FROM MAKE_INTERVAL(days => 10))")
        .await
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_make_interval_plural_hours() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(HOUR FROM MAKE_INTERVAL(hours => 8))")
        .await
        .unwrap();
    assert_table_eq!(result, [[8]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_make_interval_plural_minutes() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MINUTE FROM MAKE_INTERVAL(minutes => 15))")
        .await
        .unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_make_interval_plural_seconds() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(SECOND FROM MAKE_INTERVAL(seconds => 59))")
        .await
        .unwrap();
    assert_table_eq!(result, [[59]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_make_interval_mixed_singular_plural() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAY FROM MAKE_INTERVAL(year => 1, months => 2, days => 3))")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_make_interval_no_args() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAY FROM MAKE_INTERVAL())")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_trunc_week_monday() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_TRUNC(DATETIME '2024-06-15 14:30:00', WEEK(MONDAY))")
        .await
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 10, 0, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_trunc_week_sunday() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_TRUNC(DATETIME '2024-06-15 14:30:00', WEEK(SUNDAY))")
        .await
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 9, 0, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_trunc_week_tuesday() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_TRUNC(DATETIME '2024-06-15 14:30:00', WEEK(TUESDAY))")
        .await
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 11, 0, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_trunc_week_wednesday() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_TRUNC(DATETIME '2024-06-15 14:30:00', WEEK(WEDNESDAY))")
        .await
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 12, 0, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_trunc_week_thursday() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_TRUNC(DATETIME '2024-06-15 14:30:00', WEEK(THURSDAY))")
        .await
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 13, 0, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_trunc_week_friday() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_TRUNC(DATETIME '2024-06-15 14:30:00', WEEK(FRIDAY))")
        .await
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 14, 0, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_trunc_week_saturday() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_TRUNC(DATETIME '2024-06-15 14:30:00', WEEK(SATURDAY))")
        .await
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 15, 0, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_time_trunc_hour() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TIME_TRUNC(TIME '14:30:45', HOUR)")
        .await
        .unwrap();
    assert_table_eq!(result, [[tm(14, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_qualified_wildcard() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE count_test (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO count_test VALUES (1, 10), (2, 20), (3, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(count_test.*) FROM count_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_order_by_asc() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE asc_order (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO asc_order VALUES (3), (1), (2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_AGG(value ORDER BY value ASC) FROM asc_order")
        .await
        .unwrap();
    assert_table_eq!(result, [[[1, 2, 3]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_order_by_desc() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE desc_order (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO desc_order VALUES (1), (3), (2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_AGG(value ORDER BY value DESC) FROM desc_order")
        .await
        .unwrap();
    assert_table_eq!(result, [[[3, 2, 1]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_limit_and_order() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE limit_order (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO limit_order VALUES (5), (3), (1), (4), (2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_AGG(value ORDER BY value DESC LIMIT 3) FROM limit_order")
        .await
        .unwrap();
    assert_table_eq!(result, [[[5, 4, 3]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_date_trunc_year() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE_TRUNC(DATE '2024-06-15', YEAR)")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 1)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_date_trunc_quarter() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE_TRUNC(DATE '2024-06-15', QUARTER)")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 4, 1)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_date_trunc_day() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE_TRUNC(DATE '2024-06-15', DAY)")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 6, 15)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_trunc_year() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_TRUNC(DATETIME '2024-06-15 14:30:00', YEAR)")
        .await
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 1, 1, 0, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_trunc_quarter() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_TRUNC(DATETIME '2024-06-15 14:30:00', QUARTER)")
        .await
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 4, 1, 0, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_trunc_day() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_TRUNC(DATETIME '2024-06-15 14:30:00', DAY)")
        .await
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 15, 0, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_trunc_hour() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_TRUNC(DATETIME '2024-06-15 14:30:45', HOUR)")
        .await
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 15, 14, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_trunc_minute() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_TRUNC(DATETIME '2024-06-15 14:30:45', MINUTE)")
        .await
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 15, 14, 30, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_trunc_second() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_TRUNC(DATETIME '2024-06-15 14:30:45.123', SECOND)")
        .await
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 15, 14, 30, 45)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_trunc_isoweek() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_TRUNC(DATETIME '2024-06-15 14:30:00', ISOWEEK)")
        .await
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 10, 0, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_trunc_isoyear() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_TRUNC(DATETIME '2015-06-15 14:30:00', ISOYEAR)")
        .await
        .unwrap();
    assert_table_eq!(result, [[dt(2014, 12, 29, 0, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_normalize_default_nfc() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NORMALIZE('caf\\u0065\\u0301')")
        .await
        .unwrap();
    assert_table_eq!(result, [["caf\u{00E9}"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_multiple_order_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE multi_order (category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO multi_order VALUES ('A', 3), ('B', 1), ('A', 1), ('B', 2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_AGG(category ORDER BY category, value) FROM multi_order")
        .await
        .unwrap();
    assert_table_eq!(result, [[["A", "A", "B", "B"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_ignore_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE agg_nulls (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO agg_nulls VALUES (1), (NULL), (3), (NULL), (2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_AGG(value IGNORE NULLS ORDER BY value) FROM agg_nulls")
        .await
        .unwrap();
    assert_table_eq!(result, [[[1, 2, 3]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_respect_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE agg_respect (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO agg_respect VALUES (1), (NULL), (2)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT ARRAY_AGG(value RESPECT NULLS ORDER BY value NULLS LAST) FROM agg_respect",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[[1, 2, null()]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_sum_with_distinct() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE win_sum (id INT64, category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO win_sum VALUES (1, 'A', 10), (2, 'A', 10), (3, 'A', 20), (4, 'B', 30)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, SUM(value) OVER (PARTITION BY category) AS total FROM win_sum ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 40], [2, 40], [3, 40], [4, 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_first_value_window() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE first_val (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO first_val VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, FIRST_VALUE(value) OVER (ORDER BY id) AS first FROM first_val ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [2, 10], [3, 10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_last_value_window() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE last_val (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO last_val VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, LAST_VALUE(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last FROM last_val ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 30], [2, 30], [3, 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_timestamp_trunc_week_monday() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2024-06-15 14:30:00', WEEK(MONDAY))")
        .await
        .unwrap();
    let expected = crate::common::ts(2024, 6, 10, 0, 0, 0);
    assert_table_eq!(result, [[expected]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_timestamp_trunc_week_sunday() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2024-06-15 14:30:00', WEEK(SUNDAY))")
        .await
        .unwrap();
    let expected = crate::common::ts(2024, 6, 9, 0, 0, 0);
    assert_table_eq!(result, [[expected]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sql_udf_function_planning() {
    let session = create_session();
    session
        .execute_sql("CREATE FUNCTION double_val(x INT64) RETURNS INT64 AS (x * 2)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT double_val(5)").await.unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sql_udf_with_multiple_params() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE FUNCTION add_vals(a INT64, b INT64, c INT64) RETURNS INT64 AS (a + b + c)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT add_vals(1, 2, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [[6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_time_trunc_minute() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TIME_TRUNC(TIME '14:30:45', MINUTE)")
        .await
        .unwrap();
    assert_table_eq!(result, [[tm(14, 30, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_time_trunc_second() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TIME_TRUNC(TIME '14:30:45', SECOND)")
        .await
        .unwrap();
    assert_table_eq!(result, [[tm(14, 30, 45)]]);
}
