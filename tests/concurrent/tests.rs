use std::sync::Arc;

use super::harness::{
    ConcurrentTestHarness, TaskResult, create_test_executor, setup_test_table,
    setup_test_table_with_data,
};

#[tokio::test]
async fn test_concurrent_selects_same_table() {
    let executor = create_test_executor();
    setup_test_table_with_data(&executor, "test_select", 10).await;

    let harness = ConcurrentTestHarness::from_executor(executor, 4);

    let queries: Vec<String> = (0..4)
        .map(|_| "SELECT * FROM test_select".to_string())
        .collect();

    let results = harness.run_concurrent_queries(queries).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 4);
    assert_eq!(metrics.successful_tasks, 4);
    assert!(!metrics.data_race_detected);
}

#[tokio::test]
async fn test_concurrent_inserts_same_table() {
    let executor = create_test_executor();
    setup_test_table(&executor, "test_insert").await;

    let harness = ConcurrentTestHarness::from_executor(executor, 2);

    let queries: Vec<String> = (0..4)
        .map(|i| {
            format!(
                "INSERT INTO test_insert VALUES ({}, 'name_{}', {})",
                i, i, i as f64
            )
        })
        .collect();

    let results = harness.run_concurrent_queries(queries).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 4);
    assert!(!metrics.data_race_detected);

    let is_consistent = harness.verify_table_consistency("test_insert").await;
    assert!(is_consistent.is_ok());
}

#[tokio::test]
async fn test_concurrent_updates_same_table() {
    let executor = create_test_executor();
    setup_test_table_with_data(&executor, "test_update", 3).await;

    let harness = ConcurrentTestHarness::from_executor(executor, 2);

    let queries: Vec<String> = (0..3)
        .map(|i| {
            format!(
                "UPDATE test_update SET name = 'updated_{}' WHERE id = {}",
                i, i
            )
        })
        .collect();

    let results = harness.run_concurrent_queries(queries).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 3);
    assert!(!metrics.data_race_detected);
}

#[tokio::test]
async fn test_concurrent_deletes_same_table() {
    let executor = create_test_executor();
    setup_test_table_with_data(&executor, "test_delete", 5).await;

    let harness = ConcurrentTestHarness::from_executor(executor, 2);

    let queries: Vec<String> = (0..3)
        .map(|i| format!("DELETE FROM test_delete WHERE id = {}", i))
        .collect();

    let results = harness.run_concurrent_queries(queries).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 3);
    assert!(!metrics.data_race_detected);
}

#[tokio::test]
async fn test_read_write_contention() {
    let executor = create_test_executor();
    setup_test_table_with_data(&executor, "test_rw", 5).await;

    let harness = ConcurrentTestHarness::from_executor(executor, 8);

    let results = harness.run_read_write_contention("test_rw", 5, 3).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 8);
    assert!(!metrics.data_race_detected);
}

#[tokio::test]
async fn test_write_write_contention() {
    let executor = create_test_executor();
    setup_test_table(&executor, "test_ww").await;

    let harness = ConcurrentTestHarness::from_executor(executor, 10);

    let results = harness.run_write_write_contention("test_ww", 10).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 10);
    assert!(!metrics.data_race_detected);
}

#[tokio::test]
async fn test_concurrent_create_tables() {
    let executor = create_test_executor();
    let harness = ConcurrentTestHarness::from_executor(executor, 4);

    let queries: Vec<String> = (0..4)
        .map(|i| {
            format!(
                "CREATE TABLE concurrent_table_{} (id INT64, name STRING)",
                i
            )
        })
        .collect();

    let results = harness.run_concurrent_queries(queries).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 4);
    assert!(!metrics.data_race_detected);
}

#[tokio::test]
async fn test_concurrent_drop_tables() {
    let executor = create_test_executor();

    for i in 0..4 {
        executor
            .execute_sql(&format!("CREATE TABLE drop_table_{} (id INT64)", i))
            .await
            .ok();
    }

    let harness = ConcurrentTestHarness::from_executor(executor, 4);

    let queries: Vec<String> = (0..4)
        .map(|i| format!("DROP TABLE IF EXISTS drop_table_{}", i))
        .collect();

    let results = harness.run_concurrent_queries(queries).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 4);
    assert!(!metrics.data_race_detected);
}

#[tokio::test]
async fn test_concurrent_mixed_ddl_dml() {
    let executor = create_test_executor();
    executor
        .execute_sql("CREATE TABLE mixed_ops (id INT64, name STRING)")
        .await
        .ok();

    let harness = ConcurrentTestHarness::from_executor(executor, 6);

    let queries = vec![
        "SELECT * FROM mixed_ops".to_string(),
        "INSERT INTO mixed_ops VALUES (1, 'a')".to_string(),
        "SELECT COUNT(*) FROM mixed_ops".to_string(),
        "INSERT INTO mixed_ops VALUES (2, 'b')".to_string(),
        "UPDATE mixed_ops SET name = 'updated' WHERE id = 1".to_string(),
        "SELECT * FROM mixed_ops".to_string(),
    ];

    let results = harness.run_concurrent_queries(queries).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 6);
    assert!(!metrics.data_race_detected);
}

#[tokio::test]
async fn test_concurrent_aggregations() {
    let executor = create_test_executor();
    setup_test_table_with_data(&executor, "test_agg", 100).await;

    let harness = ConcurrentTestHarness::from_executor(executor, 4);

    let queries = vec![
        "SELECT COUNT(*) FROM test_agg".to_string(),
        "SELECT SUM(value) FROM test_agg".to_string(),
        "SELECT AVG(value) FROM test_agg".to_string(),
        "SELECT MAX(value), MIN(value) FROM test_agg".to_string(),
    ];

    let results = harness.run_concurrent_queries(queries).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 4);
    assert_eq!(metrics.successful_tasks, 4);
    assert!(!metrics.data_race_detected);
}

#[tokio::test]
async fn test_concurrent_joins() {
    let executor = create_test_executor();
    setup_test_table_with_data(&executor, "join_left", 10).await;
    setup_test_table_with_data(&executor, "join_right", 10).await;

    let harness = ConcurrentTestHarness::from_executor(executor, 4);

    let queries: Vec<String> = (0..4)
        .map(|_| {
            "SELECT l.id, r.name FROM join_left l JOIN join_right r ON l.id = r.id".to_string()
        })
        .collect();

    let results = harness.run_concurrent_queries(queries).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 4);
    assert!(!metrics.data_race_detected);
}

#[tokio::test]
async fn test_concurrent_subqueries() {
    let executor = create_test_executor();
    setup_test_table_with_data(&executor, "outer_table", 10).await;
    setup_test_table_with_data(&executor, "inner_table", 10).await;

    let harness = ConcurrentTestHarness::from_executor(executor, 4);

    let queries: Vec<String> = (0..4)
        .map(|_| {
            "SELECT * FROM outer_table WHERE id IN (SELECT id FROM inner_table WHERE value > 2)"
                .to_string()
        })
        .collect();

    let results = harness.run_concurrent_queries(queries).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 4);
    assert!(!metrics.data_race_detected);
}

#[tokio::test]
async fn test_concurrent_group_by() {
    let executor = create_test_executor();
    setup_test_table_with_data(&executor, "group_table", 20).await;

    let harness = ConcurrentTestHarness::from_executor(executor, 4);

    let queries: Vec<String> = (0..4)
        .map(|_| "SELECT name, COUNT(*) FROM group_table GROUP BY name".to_string())
        .collect();

    let results = harness.run_concurrent_queries(queries).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 4);
    assert!(!metrics.data_race_detected);
}

#[tokio::test]
async fn test_concurrent_order_by() {
    let executor = create_test_executor();
    setup_test_table_with_data(&executor, "order_table", 50).await;

    let harness = ConcurrentTestHarness::from_executor(executor, 4);

    let queries = vec![
        "SELECT * FROM order_table ORDER BY id ASC".to_string(),
        "SELECT * FROM order_table ORDER BY value DESC".to_string(),
        "SELECT * FROM order_table ORDER BY name ASC".to_string(),
        "SELECT * FROM order_table ORDER BY id DESC, value ASC".to_string(),
    ];

    let results = harness.run_concurrent_queries(queries).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 4);
    assert_eq!(metrics.successful_tasks, 4);
    assert!(!metrics.data_race_detected);
}

#[tokio::test]
async fn test_concurrent_limit_offset() {
    let executor = create_test_executor();
    setup_test_table_with_data(&executor, "limit_table", 100).await;

    let harness = ConcurrentTestHarness::from_executor(executor, 4);

    let queries = vec![
        "SELECT * FROM limit_table LIMIT 10".to_string(),
        "SELECT * FROM limit_table LIMIT 10 OFFSET 20".to_string(),
        "SELECT * FROM limit_table LIMIT 5 OFFSET 50".to_string(),
        "SELECT * FROM limit_table LIMIT 20".to_string(),
    ];

    let results = harness.run_concurrent_queries(queries).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 4);
    assert_eq!(metrics.successful_tasks, 4);
    assert!(!metrics.data_race_detected);
}

#[tokio::test]
async fn test_high_concurrency_stress() {
    let executor = create_test_executor();
    setup_test_table_with_data(&executor, "stress_table", 10).await;

    let harness = ConcurrentTestHarness::from_executor(executor, 4);

    let queries: Vec<String> = (0..12)
        .map(|i| {
            if i % 3 == 0 {
                "SELECT * FROM stress_table".to_string()
            } else if i % 3 == 1 {
                format!(
                    "INSERT INTO stress_table VALUES ({}, 'stress_{}', {})",
                    100 + i,
                    i,
                    i as f64
                )
            } else {
                "SELECT COUNT(*) FROM stress_table".to_string()
            }
        })
        .collect();

    let results = harness.run_concurrent_queries(queries).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 12);
    assert!(!metrics.data_race_detected);
}

#[tokio::test]
async fn test_concurrent_distinct() {
    let executor = create_test_executor();
    setup_test_table_with_data(&executor, "distinct_table", 20).await;

    let harness = ConcurrentTestHarness::from_executor(executor, 4);

    let queries: Vec<String> = (0..4)
        .map(|_| "SELECT DISTINCT name FROM distinct_table".to_string())
        .collect();

    let results = harness.run_concurrent_queries(queries).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 4);
    assert!(!metrics.data_race_detected);
}

#[tokio::test]
async fn test_concurrent_union() {
    let executor = create_test_executor();
    setup_test_table_with_data(&executor, "union_a", 10).await;
    setup_test_table_with_data(&executor, "union_b", 10).await;

    let harness = ConcurrentTestHarness::from_executor(executor, 4);

    let queries: Vec<String> = (0..4)
        .map(|_| "SELECT id, name FROM union_a UNION ALL SELECT id, name FROM union_b".to_string())
        .collect();

    let results = harness.run_concurrent_queries(queries).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 4);
    assert!(!metrics.data_race_detected);
}

#[tokio::test]
async fn test_concurrent_with_closures() {
    let executor = create_test_executor();
    setup_test_table_with_data(&executor, "closure_table", 10).await;

    let harness = ConcurrentTestHarness::from_executor(executor, 4);

    let tasks: Vec<_> = (0..4)
        .map(|i| {
            move |exec: Arc<yachtsql_executor::AsyncQueryExecutor>| async move {
                let query = format!("SELECT * FROM closure_table WHERE id = {}", i);
                exec.execute_sql(&query).await
            }
        })
        .collect();

    let results = harness.run_concurrent_with_executor(tasks).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 4);
    assert!(!metrics.data_race_detected);
}

#[tokio::test]
async fn test_reset_metrics() {
    let executor = create_test_executor();
    setup_test_table(&executor, "reset_test").await;

    let harness = ConcurrentTestHarness::from_executor(executor, 4);

    let queries: Vec<String> = (0..4).map(|_| "SELECT 1".to_string()).collect();
    let _results = harness.run_concurrent_queries(queries).await;

    harness.reset_metrics();

    let queries2: Vec<String> = (0..2).map(|_| "SELECT 2".to_string()).collect();
    let results2 = harness.run_concurrent_queries(queries2).await;
    let metrics = harness.assert_no_data_races_task_result(&results2);

    assert_eq!(metrics.total_tasks, 2);
}

#[tokio::test]
async fn test_task_result_types() {
    let executor = create_test_executor();
    let harness = ConcurrentTestHarness::from_executor(executor, 4);

    let queries = vec![
        "SELECT 1".to_string(),
        "SELECT * FROM nonexistent_table".to_string(),
    ];

    let results = harness.run_concurrent_queries(queries).await;

    assert!(matches!(results[0], TaskResult::Success(_)));
    assert!(matches!(results[1], TaskResult::Error(_)));
}

#[tokio::test]
async fn test_verify_table_consistency() {
    let executor = create_test_executor();
    setup_test_table_with_data(&executor, "consistency_test", 10).await;

    let harness = ConcurrentTestHarness::from_executor(executor, 4);

    let result = harness.verify_table_consistency("consistency_test").await;
    assert!(result.is_ok());
    assert!(result.unwrap());
}

#[tokio::test]
async fn test_concurrent_case_expressions() {
    let executor = create_test_executor();
    setup_test_table_with_data(&executor, "case_table", 20).await;

    let harness = ConcurrentTestHarness::from_executor(executor, 4);

    let queries: Vec<String> = (0..4)
        .map(|_| {
            "SELECT id, CASE WHEN value > 5 THEN 'high' ELSE 'low' END as level FROM case_table"
                .to_string()
        })
        .collect();

    let results = harness.run_concurrent_queries(queries).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 4);
    assert!(!metrics.data_race_detected);
}

#[tokio::test]
async fn test_concurrent_null_handling() {
    let executor = create_test_executor();
    executor
        .execute_sql("CREATE TABLE null_table (id INT64, name STRING, value FLOAT64)")
        .await
        .ok();
    executor
        .execute_sql("INSERT INTO null_table VALUES (1, NULL, 1.0)")
        .await
        .ok();
    executor
        .execute_sql("INSERT INTO null_table VALUES (2, 'test', NULL)")
        .await
        .ok();
    executor
        .execute_sql("INSERT INTO null_table VALUES (NULL, 'null_id', 3.0)")
        .await
        .ok();

    let harness = ConcurrentTestHarness::from_executor(executor, 4);

    let queries = vec![
        "SELECT * FROM null_table WHERE name IS NULL".to_string(),
        "SELECT * FROM null_table WHERE value IS NOT NULL".to_string(),
        "SELECT COALESCE(name, 'default') FROM null_table".to_string(),
        "SELECT IFNULL(value, 0) FROM null_table".to_string(),
    ];

    let results = harness.run_concurrent_queries(queries).await;
    let metrics = harness.assert_no_data_races_task_result(&results);

    assert_eq!(metrics.total_tasks, 4);
    assert!(!metrics.data_race_detected);
}
