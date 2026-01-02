use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::{create_session, d, dt, n, time, ts};

async fn setup_empty_table(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE empty_table (id INT64, name STRING)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_empty_table() {
    let session = create_session();
    setup_empty_table(&session).await;
    let result = session
        .execute_sql("SELECT id, name FROM empty_table ORDER BY id ASC")
        .await
        .unwrap();
    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_empty_table_desc() {
    let session = create_session();
    setup_empty_table(&session).await;
    let result = session
        .execute_sql("SELECT id, name FROM empty_table ORDER BY id DESC")
        .await
        .unwrap();
    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_date_asc() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE dates (id INT64, d DATE)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO dates VALUES (1, DATE '2023-01-15'), (2, DATE '2022-06-01'), (3, DATE '2024-03-20')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, d FROM dates ORDER BY d ASC")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[2, d(2022, 6, 1)], [1, d(2023, 1, 15)], [3, d(2024, 3, 20)]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_date_desc() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE dates (id INT64, d DATE)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO dates VALUES (1, DATE '2023-01-15'), (2, DATE '2022-06-01'), (3, DATE '2024-03-20')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, d FROM dates ORDER BY d DESC")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[3, d(2024, 3, 20)], [1, d(2023, 1, 15)], [2, d(2022, 6, 1)]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_timestamp_asc() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE timestamps (id INT64, ts TIMESTAMP)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO timestamps VALUES (1, TIMESTAMP '2023-01-15 10:30:00 UTC'), (2, TIMESTAMP '2023-01-15 08:00:00 UTC'), (3, TIMESTAMP '2023-01-15 14:45:00 UTC')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, ts FROM timestamps ORDER BY ts ASC")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [2, ts(2023, 1, 15, 8, 0, 0)],
            [1, ts(2023, 1, 15, 10, 30, 0)],
            [3, ts(2023, 1, 15, 14, 45, 0)]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_timestamp_desc() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE timestamps (id INT64, ts TIMESTAMP)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO timestamps VALUES (1, TIMESTAMP '2023-01-15 10:30:00 UTC'), (2, TIMESTAMP '2023-01-15 08:00:00 UTC'), (3, TIMESTAMP '2023-01-15 14:45:00 UTC')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, ts FROM timestamps ORDER BY ts DESC")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [3, ts(2023, 1, 15, 14, 45, 0)],
            [1, ts(2023, 1, 15, 10, 30, 0)],
            [2, ts(2023, 1, 15, 8, 0, 0)]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_datetime_asc() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE datetimes (id INT64, dt DATETIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO datetimes VALUES (1, DATETIME '2023-05-10 12:00:00'), (2, DATETIME '2023-05-10 09:30:00'), (3, DATETIME '2023-05-10 18:15:00')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, dt FROM datetimes ORDER BY dt ASC")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [2, dt(2023, 5, 10, 9, 30, 0)],
            [1, dt(2023, 5, 10, 12, 0, 0)],
            [3, dt(2023, 5, 10, 18, 15, 0)]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_datetime_desc() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE datetimes (id INT64, dt DATETIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO datetimes VALUES (1, DATETIME '2023-05-10 12:00:00'), (2, DATETIME '2023-05-10 09:30:00'), (3, DATETIME '2023-05-10 18:15:00')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, dt FROM datetimes ORDER BY dt DESC")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [3, dt(2023, 5, 10, 18, 15, 0)],
            [1, dt(2023, 5, 10, 12, 0, 0)],
            [2, dt(2023, 5, 10, 9, 30, 0)]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_time_asc() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE times (id INT64, t TIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO times VALUES (1, TIME '14:30:00'), (2, TIME '09:15:00'), (3, TIME '22:00:00')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, t FROM times ORDER BY t ASC")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [2, time(9, 15, 0)],
            [1, time(14, 30, 0)],
            [3, time(22, 0, 0)]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_time_desc() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE times (id INT64, t TIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO times VALUES (1, TIME '14:30:00'), (2, TIME '09:15:00'), (3, TIME '22:00:00')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, t FROM times ORDER BY t DESC")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [3, time(22, 0, 0)],
            [1, time(14, 30, 0)],
            [2, time(9, 15, 0)]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_numeric_asc() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE numerics (id INT64, n NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO numerics VALUES (1, NUMERIC '123.456'), (2, NUMERIC '99.99'), (3, NUMERIC '500.001')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, n FROM numerics ORDER BY n ASC")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[2, n("99.99")], [1, n("123.456")], [3, n("500.001")]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_numeric_desc() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE numerics (id INT64, n NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO numerics VALUES (1, NUMERIC '123.456'), (2, NUMERIC '99.99'), (3, NUMERIC '500.001')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, n FROM numerics ORDER BY n DESC")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[3, n("500.001")], [1, n("123.456")], [2, n("99.99")]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_bool_asc() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bools (id INT64, b BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bools VALUES (1, TRUE), (2, FALSE), (3, TRUE), (4, FALSE)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, b FROM bools ORDER BY b ASC, id ASC")
        .await
        .unwrap();
    assert_table_eq!(result, [[2, false], [4, false], [1, true], [3, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_bool_desc() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bools (id INT64, b BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bools VALUES (1, TRUE), (2, FALSE), (3, TRUE), (4, FALSE)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, b FROM bools ORDER BY b DESC, id ASC")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, true], [3, true], [2, false], [4, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_bytes_asc() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bytes_table (id INT64, b BYTES)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bytes_table VALUES (1, b'\\x03\\x02\\x01'), (2, b'\\x01\\x02\\x03'), (3, b'\\x02\\x02\\x02')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id FROM bytes_table ORDER BY b ASC")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [3], [1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_bytes_desc() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bytes_table (id INT64, b BYTES)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bytes_table VALUES (1, b'\\x03\\x02\\x01'), (2, b'\\x01\\x02\\x03'), (3, b'\\x02\\x02\\x02')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id FROM bytes_table ORDER BY b DESC")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_float64_asc() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE floats (id INT64, f FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO floats VALUES (1, 3.14159), (2, 2.71828), (3, 1.41421)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, f FROM floats ORDER BY f ASC")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[3, 1.41421_f64], [2, 2.71828_f64], [1, 3.14159_f64]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_float64_desc() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE floats (id INT64, f FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO floats VALUES (1, 3.14159), (2, 2.71828), (3, 1.41421)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, f FROM floats ORDER BY f DESC")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[1, 3.14159_f64], [2, 2.71828_f64], [3, 1.41421_f64]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_mixed_int_float_asc() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT * FROM (SELECT 1 AS id, CAST(10 AS INT64) AS val UNION ALL SELECT 2, CAST(5.5 AS FLOAT64) UNION ALL SELECT 3, CAST(7 AS INT64)) ORDER BY val ASC",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[2, 5.5_f64], [3, 7.0_f64], [1, 10.0_f64]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_mixed_int_float_desc() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT * FROM (SELECT 1 AS id, CAST(10 AS INT64) AS val UNION ALL SELECT 2, CAST(5.5 AS FLOAT64) UNION ALL SELECT 3, CAST(7 AS INT64)) ORDER BY val DESC",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10.0_f64], [3, 7.0_f64], [2, 5.5_f64]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_multiple_keys_three_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE multi (a INT64, b INT64, c INT64, d STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO multi VALUES (1, 1, 1, 'a'), (1, 1, 2, 'b'), (1, 2, 1, 'c'), (2, 1, 1, 'd')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT d FROM multi ORDER BY a ASC, b ASC, c ASC")
        .await
        .unwrap();
    assert_table_eq!(result, [["a"], ["b"], ["c"], ["d"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_multiple_keys_mixed_directions() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE multi (a INT64, b INT64, c INT64, d STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO multi VALUES (1, 1, 1, 'a'), (1, 1, 2, 'b'), (1, 2, 1, 'c'), (2, 1, 1, 'd')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT d FROM multi ORDER BY a DESC, b ASC, c DESC")
        .await
        .unwrap();
    assert_table_eq!(result, [["d"], ["b"], ["a"], ["c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_nulls_in_multiple_keys() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE multi_null (a INT64, b INT64, id STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO multi_null VALUES (1, NULL, 'x'), (1, 2, 'y'), (NULL, 1, 'z'), (1, 1, 'w')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id FROM multi_null ORDER BY a ASC NULLS LAST, b ASC NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(result, [["w"], ["y"], ["x"], ["z"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_nulls_in_multiple_keys_mixed_nulls_handling() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE multi_null (a INT64, b INT64, id STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO multi_null VALUES (1, NULL, 'x'), (1, 2, 'y'), (NULL, 1, 'z'), (1, 1, 'w')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id FROM multi_null ORDER BY a ASC NULLS FIRST, b DESC NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(result, [["z"], ["y"], ["w"], ["x"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_both_null_values() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nulls (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nulls VALUES (1, NULL), (2, NULL), (3, 10)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id FROM nulls ORDER BY val ASC NULLS LAST, id ASC")
        .await
        .unwrap();
    assert_table_eq!(result, [[3], [1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_all_equal_values() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE same (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO same VALUES (1, 5), (2, 5), (3, 5)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, val FROM same ORDER BY val ASC")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 3);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_string_lexicographic() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE strings (id INT64, s STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO strings VALUES (1, 'banana'), (2, 'apple'), (3, 'cherry'), (4, 'Apple')",
        )
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, s FROM strings ORDER BY s ASC")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[4, "Apple"], [2, "apple"], [1, "banana"], [3, "cherry"]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_date_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE dates (id INT64, d DATE)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO dates VALUES (1, DATE '2023-01-15'), (2, NULL), (3, DATE '2022-06-01')",
        )
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, d FROM dates ORDER BY d ASC NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[2, null], [3, d(2022, 6, 1)], [1, d(2023, 1, 15)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_timestamp_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE timestamps (id INT64, ts TIMESTAMP)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO timestamps VALUES (1, TIMESTAMP '2023-01-15 10:30:00 UTC'), (2, NULL), (3, TIMESTAMP '2023-01-15 08:00:00 UTC')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, ts FROM timestamps ORDER BY ts DESC NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, ts(2023, 1, 15, 10, 30, 0)],
            [3, ts(2023, 1, 15, 8, 0, 0)],
            [2, null]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_numeric_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE numerics (id INT64, n NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO numerics VALUES (1, NUMERIC '123.45'), (2, NULL), (3, NUMERIC '67.89')",
        )
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, n FROM numerics ORDER BY n ASC NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(result, [[3, n("67.89")], [1, n("123.45")], [2, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_single_row() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE single (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO single VALUES (1, 100)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, val FROM single ORDER BY val DESC")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_float64_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE floats (id INT64, f FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO floats VALUES (1, 3.14), (2, NULL), (3, 2.71)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, f FROM floats ORDER BY f ASC NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[2, null], [3, 2.71_f64], [1, 3.14_f64]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_bool_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bools (id INT64, b BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bools VALUES (1, TRUE), (2, NULL), (3, FALSE)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, b FROM bools ORDER BY b ASC NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(result, [[3, false], [1, true], [2, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_expression_in_order_by_addition() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nums (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nums VALUES (10, 5), (3, 20), (8, 8)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT a, b FROM nums ORDER BY a + b ASC")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 5], [8, 8], [3, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_expression_in_order_by_multiplication() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nums (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nums VALUES (2, 5), (3, 2), (1, 10)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT a, b FROM nums ORDER BY a * b DESC")
        .await
        .unwrap();
    assert_table_eq!(result, [[2, 5], [1, 10], [3, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_expression_modulo() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nums (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nums VALUES (1, 10), (2, 7), (3, 15), (4, 8)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, val FROM nums ORDER BY MOD(val, 3) ASC, val ASC")
        .await
        .unwrap();
    assert_table_eq!(result, [[3, 15], [2, 7], [1, 10], [4, 8]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_time_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE times (id INT64, t TIME)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO times VALUES (1, TIME '14:30:00'), (2, NULL), (3, TIME '09:15:00')",
        )
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, t FROM times ORDER BY t ASC NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[3, time(9, 15, 0)], [1, time(14, 30, 0)], [2, null]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_datetime_with_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE datetimes (id INT64, dt DATETIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO datetimes VALUES (1, DATETIME '2023-05-10 12:00:00'), (2, NULL), (3, DATETIME '2023-05-10 09:30:00')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id, dt FROM datetimes ORDER BY dt DESC NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [2, null],
            [1, dt(2023, 5, 10, 12, 0, 0)],
            [3, dt(2023, 5, 10, 9, 30, 0)]
        ]
    );
}
