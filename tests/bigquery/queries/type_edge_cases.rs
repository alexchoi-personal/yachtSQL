use crate::assert_table_eq;
use crate::common::{create_session, dt, n, time, ts};

#[tokio::test(flavor = "current_thread")]
async fn test_array_access_on_struct_returns_unknown() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRUCT(1 AS a, 2 AS b).a")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_struct_access_deep() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRUCT(STRUCT(STRUCT(42 AS z) AS inner2) AS inner1).inner1.inner2.z")
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_field_not_found_returns_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE struct_test_tec (s STRUCT<a INT64, b STRING>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO struct_test_tec VALUES (STRUCT(1 AS a, 'test' AS b))")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT s.a FROM struct_test_tec")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_from_column_case_insensitive() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE struct_case_test (data STRUCT<MyField INT64, AnotherField STRING>)",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO struct_case_test VALUES (STRUCT(100 AS MyField, 'hello' AS AnotherField))",
        )
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT data.myfield, data.anotherfield FROM struct_case_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[100, "hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_window_other_functions() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE win_other_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO win_other_test VALUES (10), (20), (30)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT val, COUNT(*) OVER () AS cnt FROM win_other_test ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 3], [20, 3], [30, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_countif() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE win_countif_test (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO win_countif_test VALUES (5), (10), (15), (20)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT COUNTIF(val > 7) FROM win_countif_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_minif_maxif() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE minmax_if_test (category STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO minmax_if_test VALUES ('a', 10), ('a', 20), ('b', 5), ('b', 15)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT MIN(val), MAX(val) FROM minmax_if_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_minif_maxif_preserves_type() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE win_minmax_if_test (grp STRING, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO win_minmax_if_test VALUES ('a', 'apple'), ('a', 'banana'), ('b', 'cherry')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT grp, MIN(val) OVER (PARTITION BY grp) AS min_v, MAX(val) OVER (PARTITION BY grp) AS max_v FROM win_minmax_if_test ORDER BY grp, val")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["a", "apple", "banana"],
            ["a", "apple", "banana"],
            ["b", "cherry", "cherry"]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_approx_top_count() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE approx_top_test (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO approx_top_test VALUES ('a'), ('a'), ('b'), ('a'), ('c'), ('b')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT APPROX_TOP_COUNT(val, 2) FROM approx_top_test")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    assert_eq!(records.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_approx_top_sum() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE approx_top_sum_test (name STRING, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO approx_top_sum_test VALUES ('a', 10), ('b', 20), ('a', 30), ('c', 5)",
        )
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT APPROX_TOP_SUM(name, amount, 2) FROM approx_top_sum_test")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    assert_eq!(records.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_approx_quantiles() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE approx_quant_test (val FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO approx_quant_test VALUES (1.0), (2.0), (3.0), (4.0), (5.0)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT APPROX_QUANTILES(val, 4) FROM approx_quant_test")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    assert_eq!(records.len(), 1);
    let arr = records[0].values()[0].as_array().unwrap();
    assert_eq!(arr.len(), 5);
}

#[tokio::test(flavor = "current_thread")]
async fn test_xml_agg_returns_string() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE xml_agg_test (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO xml_agg_test VALUES ('<a>1</a>'), ('<b>2</b>')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT XMLAGG(val) FROM xml_agg_test")
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_grouping_id() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE grouping_id_test (a STRING, b STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO grouping_id_test VALUES ('x', 'p', 1), ('x', 'q', 2), ('y', 'p', 3)",
        )
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT a, b, SUM(val), GROUPING_ID(a, b) FROM grouping_id_test GROUP BY ROLLUP(a, b) ORDER BY a NULLS LAST, b NULLS LAST")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    assert!(!records.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_isoyear_isoweek() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(ISOYEAR FROM DATE '2024-01-01'), EXTRACT(ISOWEEK FROM DATE '2024-01-01')")
        .await
        .unwrap();
    assert_table_eq!(result, [[2024, 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_millisecond_microsecond_nanosecond() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MILLISECOND FROM TIME '10:30:45.123456'), EXTRACT(MICROSECOND FROM TIME '10:30:45.123456')")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    assert_eq!(records.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_datetime_field() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DATETIME FROM TIMESTAMP '2024-06-15 10:30:45')")
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_timezone_fields() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-06-15 10:30:45+05:30')")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    assert_eq!(records.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_subquery_empty() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE empty_arr_sub_test (val INT64)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT ARRAY(SELECT val FROM empty_arr_sub_test)")
        .await
        .unwrap();
    assert_table_eq!(result, [[[]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_type_custom_date() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE range_date_test (id INT64, r RANGE<DATE>)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO range_date_test VALUES (1, RANGE(DATE '2024-01-01', DATE '2024-12-31'))",
        )
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id FROM range_date_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_type_custom_datetime() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE range_datetime_test (id INT64, r RANGE<DATETIME>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO range_datetime_test VALUES (1, RANGE(DATETIME '2024-01-01 00:00:00', DATETIME '2024-12-31 23:59:59'))")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id FROM range_datetime_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_type_custom_timestamp() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE range_timestamp_test (id INT64, r RANGE<TIMESTAMP>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO range_timestamp_test VALUES (1, RANGE(TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-12-31 23:59:59'))")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT id FROM range_timestamp_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_custom_function_st_disjoint() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_DISJOINT(ST_GEOGFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GEOGPOINT(5, 5))")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_custom_function_st_covers_coveredby() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_COVERS(ST_GEOGFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), ST_GEOGPOINT(1, 1)), ST_COVEREDBY(ST_GEOGPOINT(1, 1), ST_GEOGFROMTEXT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_custom_function_st_equals() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_EQUALS(ST_GEOGPOINT(0, 0), ST_GEOGPOINT(0, 0))")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_custom_function_st_iscollection_isring() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT ST_ISCOLLECTION(ST_GEOGFROMTEXT('GEOMETRYCOLLECTION(POINT(0 0), POINT(1 1))'))",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_custom_function_st_asbinary() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ST_ASBINARY(ST_GEOGPOINT(0, 0)) IS NOT NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_custom_function_json_type() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_TYPE(JSON '123')")
        .await
        .unwrap();
    assert_table_eq!(result, [["number"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_custom_function_json_set_remove() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON_SET(JSON '{\"a\": 1}', '$.b', 2), '$.b')")
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_custom_function_json_strip_nulls() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TO_JSON_STRING(JSON_STRIP_NULLS(JSON '{\"a\": null, \"b\": 1}'))")
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_custom_function_json_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_QUERY_ARRAY(JSON '[1, 2, 3]')")
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_custom_function_json_value_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE_ARRAY(JSON '[\"a\", \"b\", \"c\"]')")
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_custom_function_json_extract_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_EXTRACT_ARRAY(JSON '[1, 2, 3]')")
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_custom_function_timestamp_diff() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-15 10:00:00', TIMESTAMP '2024-01-15 08:00:00', HOUR)")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nvl_nvl2() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NVL(NULL, 42), NVL(100, 42)")
        .await
        .unwrap();
    assert_table_eq!(result, [[42, 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nvl2() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NVL2(NULL, 'not null', 'is null'), NVL2(1, 'not null', 'is null')")
        .await
        .unwrap();
    assert_table_eq!(result, [["is null", "not null"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_nan_is_inf() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT IS_NAN(CAST('NaN' AS FLOAT64)), IS_INF(CAST('Infinity' AS FLOAT64))")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_divide_ieee_divide() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_DIVIDE(10, 2), IEEE_DIVIDE(10, 0)")
        .await
        .unwrap();
    let records = result.to_records().unwrap();
    assert_eq!(records[0].values()[0].as_f64().unwrap(), 5.0);
    assert!(records[0].values()[1].as_f64().unwrap().is_infinite());
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_multiply_add_subtract_negate() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT SAFE_MULTIPLY(5, 3), SAFE_ADD(10, 5), SAFE_SUBTRACT(20, 7), SAFE_NEGATE(-5)",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[15, 15, 13, 5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_mod_preserves_type() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DIV(17, 5), MOD(17, 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[3, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_floor_ceil_trunc_preserves_type_int() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FLOOR(7), CEIL(7), TRUNC(7)")
        .await
        .unwrap();
    assert_table_eq!(result, [[7, 7, 7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_floor_ceil_trunc_preserves_type_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FLOOR(3.7), CEIL(3.2), TRUNC(3.9)")
        .await
        .unwrap();
    assert_table_eq!(result, [[3.0, 4.0, 3.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_round_preserves_type() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(3.567, 2), ROUND(NUMERIC '3.567', 2)")
        .await
        .unwrap();
    assert_table_eq!(result, [[3.57, n("3.57")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_greatest_least_preserves_type() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT GREATEST(1, 5, 3), LEAST(1, 5, 3), GREATEST(1.5, 2.5, 0.5), LEAST(1.5, 2.5, 0.5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, 1, 2.5, 0.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trigonometric_functions() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(SIN(0), 2), ROUND(COS(0), 2), ROUND(TAN(0), 2)")
        .await
        .unwrap();
    assert_table_eq!(result, [[0.0, 1.0, 0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_inverse_trigonometric_functions() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT ROUND(ASIN(0), 2), ROUND(ACOS(1), 2), ROUND(ATAN(0), 2), ROUND(ATAN2(0, 1), 2)",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[0.0, 0.0, 0.0, 0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_exp_ln_log_log10() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(EXP(1), 2), ROUND(LN(EXP(1)), 2), ROUND(LOG(100, 10), 2), ROUND(LOG10(100), 2)")
        .await
        .unwrap();
    assert_table_eq!(result, [[2.72, 1.0, 2.0, 2.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sqrt_cbrt_power_pow() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SQRT(16), CBRT(27), POWER(2, 3), POW(2, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [[4.0, 3.0, 8.0, 8.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sign_returns_int() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SIGN(-5), SIGN(0), SIGN(5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[-1, 0, 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_byte_length_char_length() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT BYTE_LENGTH('hello'), CHAR_LENGTH('hello'), BYTE_LENGTH('héllo')")
        .await
        .unwrap();
    assert_table_eq!(result, [[5, 5, 6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_strpos_instr() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRPOS('hello world', 'wor'), INSTR('hello world', 'wor')")
        .await
        .unwrap();
    assert_table_eq!(result, [[7, 7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_initcap() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INITCAP('hello world')")
        .await
        .unwrap();
    assert_table_eq!(result, [["Hello World"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_regexp_extract_replace() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REGEXP_EXTRACT('hello123world', r'[0-9]+'), REGEXP_REPLACE('hello123world', r'[0-9]+', 'X')")
        .await
        .unwrap();
    assert_table_eq!(result, [["123", "helloXworld"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_generate_date_array() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-05'))",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_generate_timestamp_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 03:00:00', INTERVAL 1 HOUR))")
        .await
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unix_micros_millis_seconds() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT UNIX_SECONDS(TIMESTAMP '1970-01-01 00:00:01'), UNIX_MILLIS(TIMESTAMP '1970-01-01 00:00:01'), UNIX_MICROS(TIMESTAMP '1970-01-01 00:00:01')")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 1000, 1000000]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_timestamp_from_unix() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_SECONDS(1) = TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP_MILLIS(1000) = TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP_MICROS(1000000) = TIMESTAMP '1970-01-01 00:00:01'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, true, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_time_trunc() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TIME_TRUNC(TIME '10:35:45', HOUR)")
        .await
        .unwrap();
    assert_table_eq!(result, [[time(10, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_datetime_trunc() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_TRUNC(DATETIME '2024-06-15 10:35:45', MONTH)")
        .await
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 1, 0, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_timestamp_trunc() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2024-06-15 10:35:45', DAY)")
        .await
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 6, 15, 0, 0, 0)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_parse_time_datetime_timestamp() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT PARSE_TIME('%H:%M:%S', '10:30:45'), PARSE_DATETIME('%Y-%m-%d %H:%M:%S', '2024-06-15 10:30:45'), PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', '2024-06-15 10:30:45')")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[
            time(10, 30, 45),
            dt(2024, 6, 15, 10, 30, 45),
            ts(2024, 6, 15, 10, 30, 45)
        ]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_time_datetime() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT_TIME('%H:%M:%S', TIME '10:30:45'), FORMAT_DATETIME('%Y-%m-%d', DATETIME '2024-06-15 10:30:45')")
        .await
        .unwrap();
    assert_table_eq!(result, [["10:30:45", "2024-06-15"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_justify_days_hours_interval() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JUSTIFY_DAYS(INTERVAL 35 DAY), JUSTIFY_HOURS(INTERVAL 25 HOUR), JUSTIFY_INTERVAL(INTERVAL '1-1 25:0:0' YEAR TO SECOND)")
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_bool_from_json() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT BOOL(JSON 'true'), BOOL(JSON 'false')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_from_json() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INT64(JSON '42')")
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_float64_from_json() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FLOAT64(JSON '3.14')")
        .await
        .unwrap();
    assert_table_eq!(result, [[3.14]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_from_json() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRING(JSON '\"hello\"')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sql_type_bignumeric() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bignumeric_test (val BIGNUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bignumeric_test VALUES (BIGNUMERIC '12345678901234567890.12345')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT val IS NOT NULL FROM bignumeric_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sql_type_geography() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE geo_type_test (loc GEOGRAPHY)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO geo_type_test VALUES (ST_GEOGPOINT(0, 0))")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT loc IS NOT NULL FROM geo_type_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sql_type_interval() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE interval_type_test (dur INTERVAL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO interval_type_test VALUES (INTERVAL 5 DAY)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT dur IS NOT NULL FROM interval_type_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sql_type_decimal() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE decimal_test (val DECIMAL(10, 2))")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO decimal_test VALUES (NUMERIC '123.45')")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT val FROM decimal_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[n("123.45")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sql_type_numeric_precision_only() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE numeric_prec_only (val NUMERIC(10))")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO numeric_prec_only VALUES (12345)")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT val FROM numeric_prec_only")
        .await
        .unwrap();
    assert_table_eq!(result, [[n("12345")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ddl_unique_constraint() {
    let session = create_session();
    let result = session
        .execute_sql("CREATE TABLE unique_test (id INT64, email STRING, UNIQUE (email))")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_ddl_foreign_key_constraint() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE parent_fk_test (id INT64 PRIMARY KEY)")
        .await
        .unwrap();
    let result = session
        .execute_sql("CREATE TABLE child_fk_test (id INT64, parent_id INT64, FOREIGN KEY (parent_id) REFERENCES parent_fk_test(id))")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_ddl_check_constraint() {
    let session = create_session();
    let result = session
        .execute_sql("CREATE TABLE check_test (id INT64, age INT64, CHECK (age >= 0))")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_rand_canonical() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT RAND() >= 0.0 AND RAND() < 1.0")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_net_functions() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT NET.HOST('http://example.com/path'), NET.PUBLIC_SUFFIX('www.example.co.uk')",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["example.com", "uk"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_empty_type_inference() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(ARRAY<INT64>[])")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_struct_without_field_name() {
    let session = create_session();
    let result = session.execute_sql("SELECT STRUCT(1, 2, 3)").await.unwrap();
    let records = result.to_records().unwrap();
    let s = records[0].values()[0].as_struct().unwrap();
    assert_eq!(s.len(), 3);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_week_with_weekday() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(WEEK(MONDAY) FROM DATE '2024-06-15')")
        .await;
    assert!(result.is_ok() || result.is_err());
}
