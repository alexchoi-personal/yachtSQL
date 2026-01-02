use crate::assert_table_eq;
use crate::common::{create_session, dt, null};

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_table_not_found_error() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE nonexistent_table,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('signal', 'locf')]
            )",
        )
        .await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("not found") || err_msg.contains("Table"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_missing_ts_column_error() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test (
                time DATETIME,
                val INT64
            )",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO gap_test VALUES
            (DATETIME '2024-01-01 10:00:00', 10)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test,
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'locf')]
            )",
        )
        .await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("ts_column"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_missing_bucket_width_error() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test2 (
                time DATETIME,
                val INT64
            )",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO gap_test2 VALUES
            (DATETIME '2024-01-01 10:00:00', 10)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test2,
                ts_column => 'time',
                value_columns => [('val', 'locf')]
            )",
        )
        .await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("bucket_width"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_unknown_strategy_defaults_to_null() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test3 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:34:01', 74),
                    STRUCT(DATETIME '2023-11-01 09:36:00', 77)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test3,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [
                    ('val', 'unknown_strategy')
                ]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert!(result.get_row(0).unwrap().values()[1].is_null());
    assert_eq!(result.get_row(1).unwrap().values()[1].as_i64().unwrap(), 77);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_with_empty_value_columns() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test4 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:34:01', 74),
                    STRUCT(DATETIME '2023-11-01 09:36:00', 77)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test4,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => []
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_with_empty_partitioning_columns() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test5 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:34:01', 74),
                    STRUCT(DATETIME '2023-11-01 09:36:00', 77)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test5,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                partitioning_columns => [],
                value_columns => [('val', 'locf')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_with_subquery_input() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                (SELECT DATETIME '2023-11-01 09:34:01' AS time, 74 AS val
                 UNION ALL
                 SELECT DATETIME '2023-11-01 09:36:00' AS time, 77 AS val),
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'linear')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [dt(2023, 11, 1, 9, 35, 0), 75],
            [dt(2023, 11, 1, 9, 36, 0), 77]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_null_strategy_explicit() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test6 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, signal INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:34:01', 74),
                    STRUCT(DATETIME '2023-11-01 09:36:00', 77)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT time, signal
            FROM GAP_FILL(
                TABLE gap_test6,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('signal', 'null')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert!(result.get_row(0).unwrap().values()[1].is_null());
    assert_eq!(result.get_row(1).unwrap().values()[1].as_i64().unwrap(), 77);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_with_origin_parameter() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test7 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:34:30', 74),
                    STRUCT(DATETIME '2023-11-01 09:36:30', 77)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT time, val
            FROM GAP_FILL(
                TABLE gap_test7,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'null')],
                origin => DATETIME '2023-11-01 09:30:30'
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [dt(2023, 11, 1, 9, 34, 30), 74],
            [dt(2023, 11, 1, 9, 35, 30), null()],
            [dt(2023, 11, 1, 9, 36, 30), 77]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_multiple_partitioning_columns() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test8 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<region STRING, device_id INT64, time DATETIME, signal INT64>>[
                    STRUCT('US', 1, DATETIME '2023-11-01 09:35:00', 82),
                    STRUCT('US', 1, DATETIME '2023-11-01 09:36:00', 84),
                    STRUCT('US', 2, DATETIME '2023-11-01 09:35:00', 90),
                    STRUCT('EU', 1, DATETIME '2023-11-01 09:35:00', 75)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test8,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                partitioning_columns => ['region', 'device_id'],
                value_columns => [('signal', 'locf')]
            )
            ORDER BY region, device_id, time",
        )
        .await
        .unwrap();
    assert!(result.num_rows() >= 4);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_case_insensitive_column_matching() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test9 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<TIME_COL DATETIME, VALUE_COL INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:34:01', 74),
                    STRUCT(DATETIME '2023-11-01 09:36:00', 77)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test9,
                ts_column => 'time_col',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('value_col', 'locf')]
            )
            ORDER BY TIME_COL",
        )
        .await
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_linear_interpolation() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test10 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 100),
                    STRUCT(DATETIME '2023-11-01 09:03:00', 130)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test10,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'linear')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [dt(2023, 11, 1, 9, 0, 0), 100],
            [dt(2023, 11, 1, 9, 1, 0), 110],
            [dt(2023, 11, 1, 9, 2, 0), 120],
            [dt(2023, 11, 1, 9, 3, 0), 130]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_locf_strategy() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test11 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 100),
                    STRUCT(DATETIME '2023-11-01 09:03:00', 200)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test11,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'LOCF')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [dt(2023, 11, 1, 9, 0, 0), 100],
            [dt(2023, 11, 1, 9, 1, 0), 100],
            [dt(2023, 11, 1, 9, 2, 0), 100],
            [dt(2023, 11, 1, 9, 3, 0), 200]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_with_no_gaps() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test12 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 10),
                    STRUCT(DATETIME '2023-11-01 09:01:00', 20),
                    STRUCT(DATETIME '2023-11-01 09:02:00', 30)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test12,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'null')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [dt(2023, 11, 1, 9, 0, 0), 10],
            [dt(2023, 11, 1, 9, 1, 0), 20],
            [dt(2023, 11, 1, 9, 2, 0), 30]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_mixed_strategies() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test13 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, temp INT64, humidity INT64, status STRING>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 20, 50, 'OK'),
                    STRUCT(DATETIME '2023-11-01 09:03:00', 26, 44, 'OK')
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test13,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [
                    ('temp', 'linear'),
                    ('humidity', 'linear'),
                    ('status', 'locf')
                ]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [dt(2023, 11, 1, 9, 0, 0), 20, 50, "OK"],
            [dt(2023, 11, 1, 9, 1, 0), 22, 48, "OK"],
            [dt(2023, 11, 1, 9, 2, 0), 24, 46, "OK"],
            [dt(2023, 11, 1, 9, 3, 0), 26, 44, "OK"]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_with_larger_bucket_width() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test14 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 10),
                    STRUCT(DATETIME '2023-11-01 09:15:00', 20),
                    STRUCT(DATETIME '2023-11-01 09:45:00', 40)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test14,
                ts_column => 'time',
                bucket_width => INTERVAL 15 MINUTE,
                value_columns => [('val', 'locf')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [dt(2023, 11, 1, 9, 0, 0), 10],
            [dt(2023, 11, 1, 9, 15, 0), 20],
            [dt(2023, 11, 1, 9, 30, 0), 20],
            [dt(2023, 11, 1, 9, 45, 0), 40]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_hour_interval() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test15 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 10),
                    STRUCT(DATETIME '2023-11-01 12:00:00', 40)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test15,
                ts_column => 'time',
                bucket_width => INTERVAL 1 HOUR,
                value_columns => [('val', 'linear')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [dt(2023, 11, 1, 9, 0, 0), 10],
            [dt(2023, 11, 1, 10, 0, 0), 20],
            [dt(2023, 11, 1, 11, 0, 0), 30],
            [dt(2023, 11, 1, 12, 0, 0), 40]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_partitions_independent() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test16 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<device_id INT64, time DATETIME, val INT64>>[
                    STRUCT(1, DATETIME '2023-11-01 09:00:00', 10),
                    STRUCT(1, DATETIME '2023-11-01 09:02:00', 30),
                    STRUCT(2, DATETIME '2023-11-01 09:01:00', 100),
                    STRUCT(2, DATETIME '2023-11-01 09:02:00', 200)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test16,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                partitioning_columns => ['device_id'],
                value_columns => [('val', 'linear')]
            )
            ORDER BY device_id, time",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [dt(2023, 11, 1, 9, 0, 0), 1, 10],
            [dt(2023, 11, 1, 9, 1, 0), 1, 20],
            [dt(2023, 11, 1, 9, 2, 0), 1, 30],
            [dt(2023, 11, 1, 9, 1, 0), 2, 100],
            [dt(2023, 11, 1, 9, 2, 0), 2, 200]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_missing_table_input_error() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'locf')]
            )",
        )
        .await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("table input") || err_msg.contains("GAP_FILL"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_with_unknown_named_arg() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test17 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 100),
                    STRUCT(DATETIME '2023-11-01 09:02:00', 200)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test17,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'locf')],
                unknown_param => 'some_value'
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_non_string_column_name_in_value_columns() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test18 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 100),
                    STRUCT(DATETIME '2023-11-01 09:02:00', 200)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test18,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [(123, 'locf')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_non_string_strategy_in_value_columns() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test19 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 100),
                    STRUCT(DATETIME '2023-11-01 09:03:00', 200)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test19,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 123)]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert!(result.get_row(1).unwrap().values()[1].is_null());
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_single_tuple_wrong_size_in_value_columns() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test20 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 100),
                    STRUCT(DATETIME '2023-11-01 09:02:00', 200)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test20,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'locf', 'extra')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_non_array_value_columns() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test21 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 100),
                    STRUCT(DATETIME '2023-11-01 09:02:00', 200)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test21,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => 'not_an_array'
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_non_array_partitioning_columns() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test22 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 100),
                    STRUCT(DATETIME '2023-11-01 09:02:00', 200)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test22,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                partitioning_columns => 'not_an_array',
                value_columns => [('val', 'locf')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_non_string_partitioning_column() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test23 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, device_id INT64, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 1, 100),
                    STRUCT(DATETIME '2023-11-01 09:02:00', 1, 200)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test23,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                partitioning_columns => [123],
                value_columns => [('val', 'locf')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_non_string_ts_column() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test24 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 100),
                    STRUCT(DATETIME '2023-11-01 09:02:00', 200)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test24,
                ts_column => 123,
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'locf')]
            )",
        )
        .await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("ts_column"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_with_uppercase_strategy() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test25 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 100),
                    STRUCT(DATETIME '2023-11-01 09:03:00', 200)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test25,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'LINEAR')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [dt(2023, 11, 1, 9, 0, 0), 100],
            [dt(2023, 11, 1, 9, 1, 0), 133],
            [dt(2023, 11, 1, 9, 2, 0), 167],
            [dt(2023, 11, 1, 9, 3, 0), 200]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_with_uppercase_null_strategy() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test26 AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 100),
                    STRUCT(DATETIME '2023-11-01 09:03:00', 200)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test26,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'NULL')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert!(result.get_row(1).unwrap().values()[1].is_null());
    assert!(result.get_row(2).unwrap().values()[1].is_null());
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_with_timestamp_column() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test_ts AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time TIMESTAMP, val INT64>>[
                    STRUCT(TIMESTAMP '2023-11-01 09:00:00', 100),
                    STRUCT(TIMESTAMP '2023-11-01 09:03:00', 200)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test_ts,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'linear')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 4);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_with_date_column() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test_date AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 00:00:00', 100),
                    STRUCT(DATETIME '2023-11-04 00:00:00', 400)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test_date,
                ts_column => 'time',
                bucket_width => INTERVAL 1 DAY,
                value_columns => [('val', 'linear')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 4);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_float64_interpolation() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test_float AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val FLOAT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 10.0),
                    STRUCT(DATETIME '2023-11-01 09:02:00', 30.0)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test_float,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'linear')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    let mid_val = result.get_row(1).unwrap().values()[1].as_f64().unwrap();
    assert!((mid_val - 20.0).abs() < 0.01);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_mixed_int_float_interpolation() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test_mixed AS
            SELECT DATETIME '2023-11-01 09:00:00' AS time, 10.0 AS val
            UNION ALL
            SELECT DATETIME '2023-11-01 09:02:00' AS time, 30.0 AS val",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test_mixed,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'linear')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 3);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_locf_with_initial_null() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test_locf_null AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', NULL),
                    STRUCT(DATETIME '2023-11-01 09:02:00', 100)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test_locf_null,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'locf')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 3);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_linear_with_null_boundary() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test_lin_null AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', NULL),
                    STRUCT(DATETIME '2023-11-01 09:03:00', 100)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test_lin_null,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'linear')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 4);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_second_interval() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test_sec AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 10),
                    STRUCT(DATETIME '2023-11-01 09:00:03', 40)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test_sec,
                ts_column => 'time',
                bucket_width => INTERVAL 1 SECOND,
                value_columns => [('val', 'linear')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 4);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_day_interval() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test_day AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 100),
                    STRUCT(DATETIME '2023-11-05 09:00:00', 500)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test_day,
                ts_column => 'time',
                bucket_width => INTERVAL 1 DAY,
                value_columns => [('val', 'linear')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert!(result.num_rows() >= 4);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_string_value_locf() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test_str AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, status STRING>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 'active'),
                    STRUCT(DATETIME '2023-11-01 09:03:00', 'inactive')
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test_str,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('status', 'locf')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 4);
    assert_eq!(
        result.get_row(1).unwrap().values()[1].as_str().unwrap(),
        "active"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_string_value_linear_falls_back_to_null() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test_str_lin AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, status STRING>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 'active'),
                    STRUCT(DATETIME '2023-11-01 09:02:00', 'inactive')
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test_str_lin,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('status', 'linear')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    assert!(result.get_row(1).unwrap().values()[1].is_null());
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_with_origin_timestamp() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test_origin_ts AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time TIMESTAMP, val INT64>>[
                    STRUCT(TIMESTAMP '2023-11-01 09:34:30', 74),
                    STRUCT(TIMESTAMP '2023-11-01 09:36:30', 77)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test_origin_ts,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'null')],
                origin => TIMESTAMP '2023-11-01 09:30:30'
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 3);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_empty_result() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test_empty AS
            SELECT DATETIME '2023-11-01 09:00:00' AS time, 100 AS val
            WHERE FALSE",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test_empty,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'locf')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_single_row() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test_single AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 100)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test_single,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'locf')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_data_not_on_bucket_boundary() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test_offset AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:30', 100),
                    STRUCT(DATETIME '2023-11-01 09:02:30', 300)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test_offset,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'linear')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_non_tuple_in_value_columns() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test_non_tuple AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 100),
                    STRUCT(DATETIME '2023-11-01 09:02:00', 200)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test_non_tuple,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => ['val']
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_column_not_in_schema() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test_no_col AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 100),
                    STRUCT(DATETIME '2023-11-01 09:02:00', 200)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test_no_col,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('nonexistent', 'locf')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_partition_not_in_schema() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE gap_test_no_part AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<time DATETIME, val INT64>>[
                    STRUCT(DATETIME '2023-11-01 09:00:00', 100),
                    STRUCT(DATETIME '2023-11-01 09:02:00', 200)
                ]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE gap_test_no_part,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                partitioning_columns => ['nonexistent'],
                value_columns => [('val', 'locf')]
            )
            ORDER BY time",
        )
        .await
        .unwrap();
    assert!(result.num_rows() >= 2);
}
