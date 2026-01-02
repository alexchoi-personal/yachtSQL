use crate::assert_table_eq;
use crate::common::{create_session, d};

#[tokio::test(flavor = "current_thread")]
async fn test_interval_year_basic() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(YEAR FROM INTERVAL 5 YEAR)")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_month_basic() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MONTH FROM INTERVAL 8 MONTH)")
        .await
        .unwrap();
    assert_table_eq!(result, [[8]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_day_basic() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAY FROM INTERVAL 15 DAY)")
        .await
        .unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_hour_basic() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(HOUR FROM INTERVAL 24 HOUR)")
        .await
        .unwrap();
    assert_table_eq!(result, [[24]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_minute_basic() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MINUTE FROM INTERVAL 45 MINUTE)")
        .await
        .unwrap();
    assert_table_eq!(result, [[45]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_second_basic() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(SECOND FROM INTERVAL 30 SECOND)")
        .await
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_millisecond() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MILLISECOND FROM INTERVAL 500 MILLISECOND)")
        .await
        .unwrap();
    assert_table_eq!(result, [[500]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_microsecond() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MICROSECOND FROM INTERVAL 750 MICROSECOND)")
        .await
        .unwrap();
    assert_table_eq!(result, [[750]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_date_addition_day() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-01' + INTERVAL 15 DAY")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 16)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_date_addition_month() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-15' + INTERVAL 3 MONTH")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 4, 15)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_date_addition_year() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-06-15' + INTERVAL 2 YEAR")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2026, 6, 15)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_date_subtraction() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-03-20' - INTERVAL 15 DAY")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 3, 5)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_negative() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-15' + INTERVAL -10 DAY")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 5)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_timestamp_addition_hour() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-01-15 10:00:00' + INTERVAL 5 HOUR)")
        .await
        .unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_timestamp_addition_minute() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT EXTRACT(MINUTE FROM TIMESTAMP '2024-01-15 10:30:00' + INTERVAL 45 MINUTE)",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_with_table_data() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE events (id INT64, event_date DATE, days_offset INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO events VALUES (1, '2024-01-01', 5), (2, '2024-02-01', 10)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, event_date + MAKE_INTERVAL(day => days_offset) AS future_date FROM events ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, d(2024, 1, 6)], [2, d(2024, 2, 11)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_comparison_greater() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INTERVAL 10 DAY > INTERVAL 5 DAY")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_comparison_less() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INTERVAL 3 HOUR < INTERVAL 5 HOUR")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_comparison_equal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INTERVAL 24 HOUR = INTERVAL 1 DAY")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_multiplication() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAY FROM INTERVAL 3 DAY * 4)")
        .await
        .unwrap();
    assert_table_eq!(result, [[12]]);
}
