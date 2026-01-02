use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_add_null_left() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL + 5").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_add_null_right() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 + NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sub_null_left() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL - 5").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sub_null_right() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 - NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mul_null_left() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL * 5").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mul_null_right() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 * NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_null_left() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL / 5").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_null_right() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 / NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mod_null_left() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL % 5").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mod_null_right() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 % NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_by_zero_int() {
    let session = create_session();
    let result = session.execute_sql("SELECT 10 / 0").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_by_zero_float() {
    let session = create_session();
    let result = session.execute_sql("SELECT 10.0 / 0.0").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_mod_by_zero() {
    let session = create_session();
    let result = session.execute_sql("SELECT 10 % 0").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_add_float64_values() {
    let session = create_session();
    let result = session.execute_sql("SELECT 1.5 + 2.5").await.unwrap();
    assert_table_eq!(result, [[4.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sub_float64_values() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5.5 - 2.5").await.unwrap();
    assert_table_eq!(result, [[3.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mul_float64_values() {
    let session = create_session();
    let result = session.execute_sql("SELECT 2.5 * 4.0").await.unwrap();
    assert_table_eq!(result, [[10.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_float64_values() {
    let session = create_session();
    let result = session.execute_sql("SELECT 10.0 / 4.0").await.unwrap();
    assert_table_eq!(result, [[2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_add_int_float_mixed() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 + 2.5").await.unwrap();
    assert_table_eq!(result, [[7.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_add_float_int_mixed() {
    let session = create_session();
    let result = session.execute_sql("SELECT 2.5 + 5").await.unwrap();
    assert_table_eq!(result, [[7.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sub_int_float_mixed() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 - 2.5").await.unwrap();
    assert_table_eq!(result, [[2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sub_float_int_mixed() {
    let session = create_session();
    let result = session.execute_sql("SELECT 7.5 - 5").await.unwrap();
    assert_table_eq!(result, [[2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mul_int_float_mixed() {
    let session = create_session();
    let result = session.execute_sql("SELECT 4 * 2.5").await.unwrap();
    assert_table_eq!(result, [[10.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mul_float_int_mixed() {
    let session = create_session();
    let result = session.execute_sql("SELECT 2.5 * 4").await.unwrap();
    assert_table_eq!(result, [[10.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_int_float_mixed() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 / 2.0").await.unwrap();
    assert_table_eq!(result, [[2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_float_int_mixed() {
    let session = create_session();
    let result = session.execute_sql("SELECT 10.0 / 4").await.unwrap();
    assert_table_eq!(result, [[2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mod_float_values() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5.5 % 2.0").await.unwrap();
    assert_table_eq!(result, [[1.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_add_numeric_values() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NUMERIC '1.5' + NUMERIC '2.5'")
        .await
        .unwrap();
    use crate::common::n;
    assert_table_eq!(result, [[n("4.0")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sub_numeric_values() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NUMERIC '5.5' - NUMERIC '2.5'")
        .await
        .unwrap();
    use crate::common::n;
    assert_table_eq!(result, [[n("3.0")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mul_numeric_values() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NUMERIC '2.5' * NUMERIC '4.0'")
        .await
        .unwrap();
    use crate::common::n;
    assert_table_eq!(result, [[n("10.00")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_numeric_values() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NUMERIC '10.0' / NUMERIC '4.0'")
        .await
        .unwrap();
    use crate::common::n;
    assert_table_eq!(result, [[n("2.5")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_numeric_by_zero() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NUMERIC '10.0' / NUMERIC '0.0'")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_mod_numeric_values() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NUMERIC '10' % NUMERIC '3'")
        .await
        .unwrap();
    use crate::common::n;
    assert_table_eq!(result, [[n("1")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mod_numeric_by_zero() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NUMERIC '10' % NUMERIC '0'")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_sub_string_from_float() {
    let session = create_session();
    let result = session.execute_sql("SELECT 10.0 - '3.5'").await.unwrap();
    assert_table_eq!(result, [[6.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sub_float_from_string() {
    let session = create_session();
    let result = session.execute_sql("SELECT '10.5' - 3.5").await.unwrap();
    assert_table_eq!(result, [[7.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sub_string_from_int() {
    let session = create_session();
    let result = session.execute_sql("SELECT 10 - '3'").await.unwrap();
    assert_table_eq!(result, [[7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sub_int_from_string() {
    let session = create_session();
    let result = session.execute_sql("SELECT '10' - 3").await.unwrap();
    assert_table_eq!(result, [[7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sub_string_from_int_float_coerce() {
    let session = create_session();
    let result = session.execute_sql("SELECT 10 - '3.5'").await.unwrap();
    assert_table_eq!(result, [[6.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sub_int_from_string_float_coerce() {
    let session = create_session();
    let result = session.execute_sql("SELECT '10.5' - 3").await.unwrap();
    assert_table_eq!(result, [[7.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sub_two_strings_as_floats() {
    let session = create_session();
    let result = session.execute_sql("SELECT '10.5' - '3.5'").await.unwrap();
    assert_table_eq!(result, [[7.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mul_two_strings_as_floats() {
    let session = create_session();
    let result = session.execute_sql("SELECT '2.5' * '4.0'").await.unwrap();
    assert_table_eq!(result, [[10.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mul_string_and_int() {
    let session = create_session();
    let result = session.execute_sql("SELECT '2.5' * 4").await.unwrap();
    assert_table_eq!(result, [[10.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mul_int_and_string() {
    let session = create_session();
    let result = session.execute_sql("SELECT 4 * '2.5'").await.unwrap();
    assert_table_eq!(result, [[10.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mul_string_and_float() {
    let session = create_session();
    let result = session.execute_sql("SELECT '2.5' * 4.0").await.unwrap();
    assert_table_eq!(result, [[10.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mul_float_and_string() {
    let session = create_session();
    let result = session.execute_sql("SELECT 4.0 * '2.5'").await.unwrap();
    assert_table_eq!(result, [[10.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_int_by_string() {
    let session = create_session();
    let result = session.execute_sql("SELECT 10 / '2.5'").await.unwrap();
    assert_table_eq!(result, [[4.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_string_by_int() {
    let session = create_session();
    let result = session.execute_sql("SELECT '10.0' / 4").await.unwrap();
    assert_table_eq!(result, [[2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_string_by_int_zero() {
    let session = create_session();
    let result = session.execute_sql("SELECT '10.0' / 0").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_int_by_string_zero() {
    let session = create_session();
    let result = session.execute_sql("SELECT 10 / '0.0'").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_float_by_string() {
    let session = create_session();
    let result = session.execute_sql("SELECT 10.0 / '2.5'").await.unwrap();
    assert_table_eq!(result, [[4.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_string_by_float() {
    let session = create_session();
    let result = session.execute_sql("SELECT '10.0' / 4.0").await.unwrap();
    assert_table_eq!(result, [[2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_string_by_float_zero() {
    let session = create_session();
    let result = session.execute_sql("SELECT '10.0' / 0.0").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_float_by_string_zero() {
    let session = create_session();
    let result = session.execute_sql("SELECT 10.0 / '0.0'").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_two_strings() {
    let session = create_session();
    let result = session.execute_sql("SELECT '10.0' / '2.5'").await.unwrap();
    assert_table_eq!(result, [[4.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_two_strings_by_zero() {
    let session = create_session();
    let result = session.execute_sql("SELECT '10.0' / '0.0'").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_large_int_addition() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 9223372036854775806 + 1")
        .await
        .unwrap();
    assert_table_eq!(result, [[9223372036854775807i64]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_negative_int_subtraction() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT -9223372036854775807 - 1")
        .await
        .unwrap();
    assert_table_eq!(result, [[-9223372036854775808i64]]);
}
