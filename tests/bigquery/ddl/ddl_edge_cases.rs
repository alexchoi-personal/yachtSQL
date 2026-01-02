use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_create_schema_authorization() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA AUTHORIZATION admin_user")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_schema_named_authorization() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA my_schema AUTHORIZATION admin_user")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE my_schema.data (id INT64)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT 1 FROM my_schema.data").await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_schema_rename() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA rename_me")
        .await
        .unwrap();

    session
        .execute_sql("ALTER SCHEMA rename_me RENAME TO renamed_schema")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_schema_owner_to() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA owner_schema")
        .await
        .unwrap();

    session
        .execute_sql("ALTER SCHEMA owner_schema OWNER TO new_owner")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_rename_as() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE old_table (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO old_table VALUES (42)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE old_table RENAME AS new_table_name")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM new_table_name")
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_table_empty_error() {
    let session = create_session();

    let result = session.execute_sql("DROP TABLE").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_truncate_empty_error() {
    let session = create_session();

    let result = session.execute_sql("TRUNCATE TABLE").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_unsupported_language() {
    let session = create_session();

    let result = session
        .execute_sql(
            "CREATE FUNCTION unsupported_lang(x INT64)
            RETURNS INT64
            LANGUAGE SCALA
            AS 'x + 1'",
        )
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_js_function_missing_returns() {
    let session = create_session();

    let result = session
        .execute_sql(
            r#"CREATE FUNCTION js_no_returns(x INT64)
            LANGUAGE js
            AS r"""return x;""""#,
        )
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_sql_udf_missing_body() {
    let session = create_session();

    let result = session
        .execute_sql("CREATE FUNCTION no_body(x INT64) RETURNS INT64")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_js_udf_missing_body() {
    let session = create_session();

    let result = session
        .execute_sql(
            "CREATE FUNCTION js_no_body(x INT64)
            RETURNS INT64
            LANGUAGE js",
        )
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_python_udf_missing_body() {
    let session = create_session();

    let result = session
        .execute_sql(
            "CREATE FUNCTION py_no_body(x INT64)
            RETURNS INT64
            LANGUAGE python",
        )
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_schema_empty_name() {
    let session = create_session();

    let result = session.execute_sql("DROP SCHEMA").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_view_empty_name() {
    let session = create_session();

    let result = session.execute_sql("DROP VIEW").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_multiple_operations() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE multi_alter (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "ALTER TABLE multi_alter
            ADD COLUMN name STRING,
            ADD COLUMN age INT64,
            ADD COLUMN email STRING",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO multi_alter VALUES (1, 'Alice', 30, 'alice@example.com')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name, age FROM multi_alter")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice", 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_no_operations() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE no_ops_table (id INT64)")
        .await
        .unwrap();

    let result = session.execute_sql("ALTER TABLE no_ops_table").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_table_function_nested_subquery() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION nested_subquery_tvf(multiplier INT64)
            RETURNS TABLE<x INT64, y INT64>
            AS (
                (SELECT n AS x, n * multiplier AS y FROM UNNEST([1, 2, 3]) AS n)
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT x, y FROM nested_subquery_tvf(10) ORDER BY x")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [2, 20], [3, 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_schema_with_numeric_option() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA numeric_opts OPTIONS(max_time_travel_hours = 168)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_set_default_collate() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE collate_table (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE collate_table SET DEFAULT COLLATE 'und:ci'")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO collate_table VALUES (1, 'Test')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM collate_table")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_table_with_clone_branch() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE clone_source (id INT64, value STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO clone_source VALUES (1, 'a'), (2, 'b')")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE clone_dest CLONE clone_source")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM clone_dest ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [2, "b"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_table_with_copy_branch() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE copy_source (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO copy_source VALUES (10, 'x'), (20, 'y')")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE copy_dest COPY copy_source")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM copy_dest ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, "x"], [20, "y"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_schema_dollar_quoted_option() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE SCHEMA dollar_schema OPTIONS(description = $$A schema with special 'chars'$$)",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_unsupported_object_type() {
    let session = create_session();

    let result = session.execute_sql("DROP SEQUENCE my_seq").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_sql_no_explicit_language() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION implicit_sql(x INT64) RETURNS INT64 AS (x * 2)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT implicit_sql(5)").await.unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_infer_return_type() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION infer_type(x INT64) AS (x + 1)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT infer_type(10)").await.unwrap();
    assert_table_eq!(result, [[11]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_table_with_column_collate_options() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE col_collate_opts (
                id INT64,
                ci_name STRING OPTIONS(collate = 'und:ci')
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO col_collate_opts VALUES (1, 'Test')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ci_name FROM col_collate_opts WHERE ci_name = 'TEST'")
        .await
        .unwrap();
    assert_table_eq!(result, [["Test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_column_unsupported_operation() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE unsupported_alter (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "ALTER TABLE unsupported_alter ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY",
        )
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_unsupported_operation() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE unsupported_ops (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql("ALTER TABLE unsupported_ops ENABLE TRIGGER ALL")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_sql_function_with_as_after_options() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE FUNCTION after_opts(x INT64)
            RETURNS INT64
            OPTIONS (description = 'test')
            AS (x + 100)",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT after_opts(5)").await.unwrap();
    assert_table_eq!(result, [[105]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_js_function_triple_quoted() {
    let session = create_session();

    session
        .execute_sql(
            r#"CREATE FUNCTION js_triple(x FLOAT64)
            RETURNS FLOAT64
            LANGUAGE js
            AS """
                return x * 2;
            """"#,
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT js_triple(5.0)").await.unwrap();
    assert_table_eq!(result, [[10.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_python_function_triple_double_quoted() {
    let session = create_session();

    session
        .execute_sql(
            r###"CREATE FUNCTION py_triple(x INT64)
            RETURNS INT64
            LANGUAGE python
            AS r"""
def py_triple(x):
    return x * 3
""""###,
        )
        .await
        .unwrap();
}
