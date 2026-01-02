use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_add_interval_to_date() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-15' + INTERVAL 10 DAY")
        .await
        .unwrap();
    use crate::common::d;
    assert_table_eq!(result, [[d(2024, 1, 25)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_add_interval_to_date_months() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-15' + INTERVAL 2 MONTH")
        .await
        .unwrap();
    use crate::common::d;
    assert_table_eq!(result, [[d(2024, 3, 15)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_add_interval_to_date_years() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-15' + INTERVAL 1 YEAR")
        .await
        .unwrap();
    use crate::common::d;
    assert_table_eq!(result, [[d(2025, 1, 15)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sub_interval_from_date() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-15' - INTERVAL 10 DAY")
        .await
        .unwrap();
    use crate::common::d;
    assert_table_eq!(result, [[d(2024, 1, 5)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sub_interval_months_from_date() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-03-15' - INTERVAL 2 MONTH")
        .await
        .unwrap();
    use crate::common::d;
    assert_table_eq!(result, [[d(2024, 1, 15)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_add_interval_to_datetime() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME '2024-01-15 10:30:00' + INTERVAL 5 DAY")
        .await
        .unwrap();
    use crate::common::dt;
    assert_table_eq!(result, [[dt(2024, 1, 20, 10, 30, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_add_interval_hours_to_datetime() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME '2024-01-15 10:30:00' + INTERVAL 5 HOUR")
        .await
        .unwrap();
    use crate::common::dt;
    assert_table_eq!(result, [[dt(2024, 1, 15, 15, 30, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_add_interval_minutes_to_datetime() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME '2024-01-15 10:30:00' + INTERVAL 45 MINUTE")
        .await
        .unwrap();
    use crate::common::dt;
    assert_table_eq!(result, [[dt(2024, 1, 15, 11, 15, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_add_interval_seconds_to_datetime() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME '2024-01-15 10:30:00' + INTERVAL 90 SECOND")
        .await
        .unwrap();
    use crate::common::dt;
    assert_table_eq!(result, [[dt(2024, 1, 15, 10, 31, 30)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sub_interval_from_datetime() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME '2024-01-15 10:30:00' - INTERVAL 5 DAY")
        .await
        .unwrap();
    use crate::common::dt;
    assert_table_eq!(result, [[dt(2024, 1, 10, 10, 30, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sub_interval_hours_from_datetime() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME '2024-01-15 10:30:00' - INTERVAL 5 HOUR")
        .await
        .unwrap();
    use crate::common::dt;
    assert_table_eq!(result, [[dt(2024, 1, 15, 5, 30, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_add_interval_to_timestamp() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP '2024-01-15 10:30:00 UTC' + INTERVAL 5 DAY")
        .await
        .unwrap();
    use crate::common::ts;
    assert_table_eq!(result, [[ts(2024, 1, 20, 10, 30, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sub_interval_from_timestamp() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP '2024-01-15 10:30:00 UTC' - INTERVAL 5 DAY")
        .await
        .unwrap();
    use crate::common::ts;
    assert_table_eq!(result, [[ts(2024, 1, 10, 10, 30, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_add_two_intervals() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT (DATE '2024-01-15' + INTERVAL 5 DAY) + INTERVAL 3 DAY")
        .await
        .unwrap();
    use crate::common::d;
    assert_table_eq!(result, [[d(2024, 1, 23)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sub_two_intervals() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT (DATE '2024-01-15' + INTERVAL 10 DAY) - INTERVAL 3 DAY")
        .await
        .unwrap();
    use crate::common::d;
    assert_table_eq!(result, [[d(2024, 1, 22)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_date_cross_month_boundary() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-25' + INTERVAL 10 DAY")
        .await
        .unwrap();
    use crate::common::d;
    assert_table_eq!(result, [[d(2024, 2, 4)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_date_cross_year_boundary() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-12-25' + INTERVAL 10 DAY")
        .await
        .unwrap();
    use crate::common::d;
    assert_table_eq!(result, [[d(2025, 1, 4)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_date_subtract_cross_month() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-02-05' - INTERVAL 10 DAY")
        .await
        .unwrap();
    use crate::common::d;
    assert_table_eq!(result, [[d(2024, 1, 26)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_date_subtract_cross_year() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-05' - INTERVAL 10 DAY")
        .await
        .unwrap();
    use crate::common::d;
    assert_table_eq!(result, [[d(2023, 12, 26)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_cross_day_boundary() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME '2024-01-15 23:30:00' + INTERVAL 2 HOUR")
        .await
        .unwrap();
    use crate::common::dt;
    assert_table_eq!(result, [[dt(2024, 1, 16, 1, 30, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_add_interval_with_null_date() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(NULL AS DATE) + INTERVAL 5 DAY")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_add_null_interval_to_date() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-15' + CAST(NULL AS INTERVAL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_addition() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-15' + (INTERVAL 5 DAY + INTERVAL 3 DAY)")
        .await
        .unwrap();
    use crate::common::d;
    assert_table_eq!(result, [[d(2024, 1, 23)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_subtraction() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-15' + (INTERVAL 10 DAY - INTERVAL 3 DAY)")
        .await
        .unwrap();
    use crate::common::d;
    assert_table_eq!(result, [[d(2024, 1, 22)]]);
}
