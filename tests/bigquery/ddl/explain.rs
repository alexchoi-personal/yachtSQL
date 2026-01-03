use crate::common::create_session;

async fn setup_test_table(session: &yachtsql::YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE test_data (id INT64, name STRING, amount FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO test_data VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 150.0)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_explain_simple_select() {
    let session = create_session();
    setup_test_table(&session).await;

    let result = session
        .execute_sql("EXPLAIN SELECT * FROM test_data")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 2);
    let schema = result.schema();
    assert_eq!(schema.field_count(), 2);
    assert_eq!(schema.fields()[0].name, "plan_type");
    assert_eq!(schema.fields()[1].name, "plan");
}

#[tokio::test(flavor = "current_thread")]
async fn test_explain_with_filter() {
    let session = create_session();
    setup_test_table(&session).await;

    let result = session
        .execute_sql("EXPLAIN SELECT * FROM test_data WHERE id > 1")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_explain_with_join() {
    let session = create_session();
    setup_test_table(&session).await;
    session
        .execute_sql("CREATE TABLE other_data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO other_data VALUES (1, 10), (2, 20)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPLAIN SELECT t.name, o.value FROM test_data t JOIN other_data o ON t.id = o.id",
        )
        .await
        .unwrap();

    assert_eq!(result.row_count(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_explain_with_aggregation() {
    let session = create_session();
    setup_test_table(&session).await;

    let result = session
        .execute_sql("EXPLAIN SELECT name, SUM(amount) FROM test_data GROUP BY name")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_explain_with_order_by() {
    let session = create_session();
    setup_test_table(&session).await;

    let result = session
        .execute_sql("EXPLAIN SELECT * FROM test_data ORDER BY amount DESC")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_explain_with_limit() {
    let session = create_session();
    setup_test_table(&session).await;

    let result = session
        .execute_sql("EXPLAIN SELECT * FROM test_data LIMIT 1")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_explain_returns_string_plans() {
    let session = create_session();
    setup_test_table(&session).await;

    let result = session
        .execute_sql("EXPLAIN SELECT * FROM test_data")
        .await
        .unwrap();

    let records = result.to_records().unwrap();
    assert_eq!(records.len(), 2);

    let first_type = &records[0].values()[0];
    let second_type = &records[1].values()[0];

    assert!(first_type.as_str().unwrap().contains("logical"));
    assert!(second_type.as_str().unwrap().contains("physical"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_explain_with_subquery() {
    let session = create_session();
    setup_test_table(&session).await;

    let result = session
        .execute_sql("EXPLAIN SELECT * FROM test_data WHERE id IN (SELECT id FROM test_data WHERE amount > 100)")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_explain_with_cte() {
    let session = create_session();
    setup_test_table(&session).await;

    let result = session
        .execute_sql("EXPLAIN WITH high_amount AS (SELECT * FROM test_data WHERE amount > 100) SELECT * FROM high_amount")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_explain_analyze_simple() {
    let session = create_session();
    setup_test_table(&session).await;

    let result = session
        .execute_sql("EXPLAIN ANALYZE SELECT * FROM test_data")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 4);
}

#[tokio::test(flavor = "current_thread")]
async fn test_explain_analyze_shows_execution_stats() {
    let session = create_session();
    setup_test_table(&session).await;

    let result = session
        .execute_sql("EXPLAIN ANALYZE SELECT * FROM test_data")
        .await
        .unwrap();

    let records = result.to_records().unwrap();
    assert_eq!(records.len(), 4);

    let types: Vec<String> = records
        .iter()
        .map(|r| r.values()[0].as_str().unwrap().to_string())
        .collect();
    assert!(types.iter().any(|t| t.contains("execution_time")));
    assert!(types.iter().any(|t| t.contains("rows_returned")));
}

#[tokio::test(flavor = "current_thread")]
async fn test_explain_union() {
    let session = create_session();
    setup_test_table(&session).await;

    let result = session
        .execute_sql("EXPLAIN SELECT id FROM test_data UNION ALL SELECT id FROM test_data")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_explain_distinct() {
    let session = create_session();
    setup_test_table(&session).await;

    let result = session
        .execute_sql("EXPLAIN SELECT DISTINCT name FROM test_data")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 2);
}
