use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_int64_from_json_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INT64(JSON '3.7')")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_from_json_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INT64(JSON '\"456\"')")
        .await
        .unwrap();
    assert_table_eq!(result, [[456]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_from_json_bool() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INT64(JSON 'true')")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_from_json_bool_false() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INT64(JSON 'false')")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_from_json_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INT64(CAST(NULL AS JSON))")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_float64_from_json_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FLOAT64(JSON '\"2.718\"')")
        .await
        .unwrap();
    assert_table_eq!(result, [[2.718]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_float64_from_json_int() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FLOAT64(JSON '42')")
        .await
        .unwrap();
    assert_table_eq!(result, [[42.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_float64_from_json_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FLOAT64(CAST(NULL AS JSON))")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bool_from_json_string_true() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT BOOL(JSON '\"true\"')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bool_from_json_string_false() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT BOOL(JSON '\"false\"')")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bool_from_json_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT BOOL(CAST(NULL AS JSON))")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_from_json_number() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRING(JSON '123')")
        .await
        .unwrap();
    assert_table_eq!(result, [["123"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_from_json_bool() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRING(JSON 'true')")
        .await
        .unwrap();
    assert_table_eq!(result, [["true"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_from_json_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRING(CAST(NULL AS JSON))")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_int64_from_json_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_INT64(JSON '3.9')")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_int64_from_json_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_INT64(CAST(NULL AS JSON))")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_float64_from_json() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_FLOAT64(JSON '\"3.14\"')")
        .await
        .unwrap();
    assert_table_eq!(result, [[3.14]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_float64_from_json_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_FLOAT64(CAST(NULL AS JSON))")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_bool_from_json_int() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_BOOL(JSON '1')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_bool_from_json_int_zero() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_BOOL(JSON '0')")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_bool_from_json_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_BOOL(JSON '1.5')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_bool_from_json_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_BOOL(CAST(NULL AS JSON))")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_bool_from_json_string_one() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_BOOL(JSON '\"1\"')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_bool_from_json_string_zero() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_BOOL(JSON '\"0\"')")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_string_from_json() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_STRING(JSON '\"hello world\"')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_string_from_json_object() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_STRING(JSON '{\"a\": 1}')")
        .await
        .unwrap();
    assert_table_eq!(result, [["{\"a\":1}"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_string_from_json_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_STRING(CAST(NULL AS JSON))")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_uppercase_e() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT('%.2E', 12345.0)")
        .await
        .unwrap();
    assert_table_eq!(result, [["1.23E+04"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_g_switch_to_scientific() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT('%.2g', 0.00001)")
        .await
        .unwrap();
    assert_table_eq!(result, [["1.00e-05"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_uppercase_g() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT('%.2G', 0.00001)")
        .await
        .unwrap();
    assert_table_eq!(result, [["1.00E-05"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_percent_specifier() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT('%p', 0.25)")
        .await
        .unwrap();
    assert_table_eq!(result, [["25"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_type_specifier() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT('%t', 42)")
        .await
        .unwrap();
    assert_table_eq!(result, [["42"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_width_no_zero_pad() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT('%5d', 42)")
        .await
        .unwrap();
    assert_table_eq!(result, [["   42"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_negative_number_with_grouping() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT(\"%'d\", -1000000)")
        .await
        .unwrap();
    assert_table_eq!(result, [["-1,000,000"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_scientific_zero() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT('%.2e', 0.0)")
        .await
        .unwrap();
    assert_table_eq!(result, [["0.00e+00"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_i_specifier() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT('%i', 42)")
        .await
        .unwrap();
    assert_table_eq!(result, [["42"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_uppercase_f() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT('%.2F', 3.14159)")
        .await
        .unwrap();
    assert_table_eq!(result, [["3.14"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_session_user_returns_string() {
    let session = create_session();
    let result = session.execute_sql("SELECT SESSION_USER()").await.unwrap();
    assert_table_eq!(result, [["user"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_pi_function() {
    let session = create_session();
    let result = session.execute_sql("SELECT ROUND(PI(), 5)").await.unwrap();
    assert_table_eq!(result, [[3.14159]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mod_with_null_first() {
    let session = create_session();
    let result = session.execute_sql("SELECT MOD(NULL, 3)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mod_with_null_second() {
    let session = create_session();
    let result = session.execute_sql("SELECT MOD(10, NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_with_null_first() {
    let session = create_session();
    let result = session.execute_sql("SELECT DIV(NULL, 3)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_with_null_second() {
    let session = create_session();
    let result = session.execute_sql("SELECT DIV(10, NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ieee_divide_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT IEEE_DIVIDE(NULL, 2.0)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ieee_divide_int_by_int() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT IEEE_DIVIDE(10, 4)")
        .await
        .unwrap();
    assert_table_eq!(result, [[2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_divide_int_by_int() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_DIVIDE(10, 4)")
        .await
        .unwrap();
    assert_table_eq!(result, [[2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_divide_int_by_zero() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_DIVIDE(10, 0)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_divide_int_by_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_DIVIDE(10, 4.0)")
        .await
        .unwrap();
    assert_table_eq!(result, [[2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_divide_int_by_float_zero() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_DIVIDE(10, 0.0)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_divide_float_by_int() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_DIVIDE(10.0, 4)")
        .await
        .unwrap();
    assert_table_eq!(result, [[2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_divide_float_by_int_zero() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_DIVIDE(10.0, 0)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_multiply_float64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_MULTIPLY(2.5, 4.0)")
        .await
        .unwrap();
    assert_table_eq!(result, [[10.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_multiply_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_MULTIPLY(NULL, 4)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_add_float64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_ADD(2.5, 3.5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[6.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_add_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_ADD(NULL, 4)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_subtract_float64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_SUBTRACT(10.5, 3.5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[7.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_subtract_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_SUBTRACT(NULL, 4)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_negate_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_NEGATE(3.14)")
        .await
        .unwrap();
    assert_table_eq!(result, [[-3.14]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_negate_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_NEGATE(NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_nan_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT IS_NAN(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_nan_with_int() {
    let session = create_session();
    let result = session.execute_sql("SELECT IS_NAN(42)").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_inf_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT IS_INF(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_inf_with_int() {
    let session = create_session();
    let result = session.execute_sql("SELECT IS_INF(42)").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sign_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT SIGN(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sign_with_float_positive() {
    let session = create_session();
    let result = session.execute_sql("SELECT SIGN(3.14)").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sign_with_float_negative() {
    let session = create_session();
    let result = session.execute_sql("SELECT SIGN(-3.14)").await.unwrap();
    assert_table_eq!(result, [[-1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sign_with_float_zero() {
    let session = create_session();
    let result = session.execute_sql("SELECT SIGN(0.0)").await.unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_abs_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT ABS(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_floor_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT FLOOR(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ceil_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT CEIL(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_round_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT ROUND(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sqrt_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT SQRT(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cbrt_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT CBRT(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cbrt_float() {
    let session = create_session();
    let result = session.execute_sql("SELECT CBRT(8.0)").await.unwrap();
    assert_table_eq!(result, [[2.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_power_with_null_base() {
    let session = create_session();
    let result = session.execute_sql("SELECT POWER(NULL, 2)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_power_with_null_exp() {
    let session = create_session();
    let result = session.execute_sql("SELECT POWER(2, NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_exp_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT EXP(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_exp_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(EXP(2.0), 4)")
        .await
        .unwrap();
    assert_table_eq!(result, [[7.3891]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ln_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT LN(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ln_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(LN(10.0), 4)")
        .await
        .unwrap();
    assert_table_eq!(result, [[2.3026]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_log_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT LOG(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_log_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(LOG(1000.0, 10), 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[3.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_log10_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT LOG10(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_log10_float() {
    let session = create_session();
    let result = session.execute_sql("SELECT LOG10(1000.0)").await.unwrap();
    assert_table_eq!(result, [[3.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trunc_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT TRUNC(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trunc_int() {
    let session = create_session();
    let result = session.execute_sql("SELECT TRUNC(42)").await.unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_float() {
    let session = create_session();
    let result = session.execute_sql("SELECT DIV(20.0, 3.0)").await.unwrap();
    assert_table_eq!(result, [[6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mod_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(MOD(20.5, 3.0), 1)")
        .await
        .unwrap();
    assert_table_eq!(result, [[2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_floor_int() {
    let session = create_session();
    let result = session.execute_sql("SELECT FLOOR(42)").await.unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ceil_int() {
    let session = create_session();
    let result = session.execute_sql("SELECT CEIL(42)").await.unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_round_int() {
    let session = create_session();
    let result = session.execute_sql("SELECT ROUND(42)").await.unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sqrt_float() {
    let session = create_session();
    let result = session.execute_sql("SELECT SQRT(2.0)").await.unwrap();
    assert_table_eq!(result, [[1.4142135623730951]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_sin_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT SIN(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_cos_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT COS(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_tan_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT TAN(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_asin_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT ASIN(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_acos_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT ACOS(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_atan_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT ATAN(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_atan2_with_null_y() {
    let session = create_session();
    let result = session.execute_sql("SELECT ATAN2(NULL, 1)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_atan2_with_null_x() {
    let session = create_session();
    let result = session.execute_sql("SELECT ATAN2(1, NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_sinh_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT SINH(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_cosh_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT COSH(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_tanh_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT TANH(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_asinh_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT ASINH(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_acosh_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT ACOSH(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_atanh_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT ATANH(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_cot_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT COT(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_csc_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT CSC(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_sec_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT SEC(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_coth_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT COTH(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_csch_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT CSCH(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_sech_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT SECH(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_sin_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(SIN(1.0), 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[0.84147]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_cos_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(COS(1.0), 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[0.5403]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_tan_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(TAN(1.0), 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[1.55741]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_asin_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(ASIN(0.5), 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[0.5236]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_acos_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(ACOS(0.5), 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[1.0472]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_atan_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(ATAN(1.0), 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[0.7854]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_atan2_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(ATAN2(1.0, 2.0), 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[0.46365]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_sinh_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(SINH(1.0), 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[1.1752]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_cosh_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(COSH(1.0), 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[1.54308]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_tanh_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(TANH(1.0), 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[0.76159]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_asinh_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(ASINH(1.0), 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[0.88137]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_acosh_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(ACOSH(2.0), 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[1.31696]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_atanh_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(ATANH(0.5), 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[0.54931]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_cot_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(COT(0.5), 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[1.83049]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_csc_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(CSC(0.5), 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[2.08583]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_sec_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(SEC(0.5), 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[1.13949]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_coth_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(COTH(0.5), 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[2.16395]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_csch_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(CSCH(0.5), 4)")
        .await
        .unwrap();
    assert_table_eq!(result, [[1.919]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trig_sech_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(SECH(0.5), 4)")
        .await
        .unwrap();
    assert_table_eq!(result, [[0.8868]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cosine_distance_int_elements() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(COSINE_DISTANCE([1, 2], [3, 4]), 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[0.01613]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cosine_distance_with_null_element() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COSINE_DISTANCE([1.0, NULL], [3.0, 4.0])")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_euclidean_distance_int_elements() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EUCLIDEAN_DISTANCE([0, 0], [3, 4])")
        .await
        .unwrap();
    assert_table_eq!(result, [[5.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_euclidean_distance_with_null_element() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EUCLIDEAN_DISTANCE([1.0, NULL], [3.0, 4.0])")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_greatest_empty() {
    let session = create_session();
    let result = session.execute_sql("SELECT GREATEST(5)").await.unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_least_empty() {
    let session = create_session();
    let result = session.execute_sql("SELECT LEAST(5)").await.unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_bucket_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT RANGE_BUCKET(25.5, [0.0, 10.0, 20.0, 30.0, 40.0])")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_bucket_with_null_array_element() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT RANGE_BUCKET(25, [0, 10, NULL, 30, 40])")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}
