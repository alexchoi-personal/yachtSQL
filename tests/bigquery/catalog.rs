use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_create_schema_error_when_exists() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA test_schema")
        .await
        .unwrap();

    let result = session.execute_sql("CREATE SCHEMA test_schema").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_drop_schema_error_when_not_exists() {
    let session = create_session();

    let result = session.execute_sql("DROP SCHEMA nonexistent_schema").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_drop_schema_restrict_with_objects() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA schema_with_objects")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE schema_with_objects.table1 (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql("DROP SCHEMA schema_with_objects RESTRICT")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_undrop_schema_error_no_dropped() {
    let session = create_session();

    let result = session.execute_sql("UNDROP SCHEMA never_existed").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_undrop_schema_error_exists() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA existing_schema")
        .await
        .unwrap();

    let result = session.execute_sql("UNDROP SCHEMA existing_schema").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_create_table_error_when_exists() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE duplicate_table (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql("CREATE TABLE duplicate_table (id INT64)")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_drop_table_error_when_not_exists() {
    let session = create_session();

    let result = session.execute_sql("DROP TABLE nonexistent_table").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_rename_table() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE old_name (id INT64, value STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO old_name VALUES (1, 'test')")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE old_name RENAME TO new_name")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM old_name").await;
    assert!(result.is_err());

    let result = session.execute_sql("SELECT * FROM new_name").await.unwrap();
    assert_table_eq!(result, [[1, "test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_rename_table_error_not_exists() {
    let session = create_session();

    let result = session
        .execute_sql("ALTER TABLE nonexistent RENAME TO something")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_rename_table_error_target_exists() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE source_table (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE target_table (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql("ALTER TABLE source_table RENAME TO target_table")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_create_function_error_when_exists() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION duplicate_fn(x INT64) RETURNS INT64 AS (x)")
        .await
        .unwrap();

    let result = session
        .execute_sql("CREATE FUNCTION duplicate_fn(x INT64) RETURNS INT64 AS (x)")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_drop_function_error_when_not_exists() {
    let session = create_session();

    let result = session.execute_sql("DROP FUNCTION nonexistent_fn").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_create_procedure_error_when_exists() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE duplicate_proc()
            BEGIN
                SELECT 1;
            END",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "CREATE PROCEDURE duplicate_proc()
            BEGIN
                SELECT 2;
            END",
        )
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_drop_procedure_error_when_not_exists() {
    let session = create_session();

    let result = session.execute_sql("DROP PROCEDURE nonexistent_proc").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_create_view_error_when_exists() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE view_base (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE VIEW duplicate_view AS SELECT * FROM view_base")
        .await
        .unwrap();

    let result = session
        .execute_sql("CREATE VIEW duplicate_view AS SELECT * FROM view_base")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_drop_view_error_when_not_exists() {
    let session = create_session();

    let result = session.execute_sql("DROP VIEW nonexistent_view").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_schema_search_path_resolution() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA search_schema")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE search_schema.users (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO search_schema.users VALUES (1, 'Alice')")
        .await
        .unwrap();

    session
        .execute_sql("SET search_path TO search_schema")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT name FROM users").await.unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_search_path_qualified_still_works() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA path_schema1")
        .await
        .unwrap();

    session
        .execute_sql("CREATE SCHEMA path_schema2")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE path_schema1.data (val INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE path_schema2.data (val INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO path_schema1.data VALUES (100)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO path_schema2.data VALUES (200)")
        .await
        .unwrap();

    session
        .execute_sql("SET search_path TO path_schema1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM data").await.unwrap();
    assert_table_eq!(result, [[100]]);

    let result = session
        .execute_sql("SELECT val FROM path_schema2.data")
        .await
        .unwrap();
    assert_table_eq!(result, [[200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_table_in_dropped_schema() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA dropped_schema")
        .await
        .unwrap();

    session
        .execute_sql("DROP SCHEMA dropped_schema")
        .await
        .unwrap();

    let result = session
        .execute_sql("CREATE TABLE dropped_schema.new_table (id INT64)")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_undrop_schema_recovers_tables() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA recoverable")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE recoverable.table1 (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE recoverable.table2 (value STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO recoverable.table1 VALUES (1), (2)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO recoverable.table2 VALUES ('a'), ('b')")
        .await
        .unwrap();

    session
        .execute_sql("DROP SCHEMA recoverable CASCADE")
        .await
        .unwrap();

    session
        .execute_sql("UNDROP SCHEMA recoverable")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM recoverable.table1 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);

    let result = session
        .execute_sql("SELECT * FROM recoverable.table2 ORDER BY value")
        .await
        .unwrap();
    assert_table_eq!(result, [["a"], ["b"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_transaction_rollback() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE txn_test (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO txn_test VALUES (1, 100)")
        .await
        .unwrap();

    session.execute_sql("BEGIN TRANSACTION").await.unwrap();

    session
        .execute_sql("UPDATE txn_test SET value = 999 WHERE id = 1")
        .await
        .unwrap();

    session.execute_sql("ROLLBACK TRANSACTION").await.unwrap();

    let result = session
        .execute_sql("SELECT value FROM txn_test WHERE id = 1")
        .await
        .unwrap();
    assert_table_eq!(result, [[100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_transaction_commit() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE commit_test (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO commit_test VALUES (1, 100)")
        .await
        .unwrap();

    session.execute_sql("BEGIN TRANSACTION").await.unwrap();

    session
        .execute_sql("UPDATE commit_test SET value = 200 WHERE id = 1")
        .await
        .unwrap();

    session.execute_sql("COMMIT TRANSACTION").await.unwrap();

    let result = session
        .execute_sql("SELECT value FROM commit_test WHERE id = 1")
        .await
        .unwrap();
    assert_table_eq!(result, [[200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_column_default_values() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE default_test (
                id INT64,
                status STRING DEFAULT 'pending',
                count INT64 DEFAULT 0
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO default_test (id) VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, status, count FROM default_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "pending", 0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_column_default_expression() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE default_expr_test (
                id INT64,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO default_expr_test (id) VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM default_expr_test WHERE created_at IS NOT NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_view_exists_check() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE view_base_check (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE VIEW view_check AS SELECT * FROM view_base_check")
        .await
        .unwrap();

    let result = session
        .execute_sql("CREATE VIEW IF NOT EXISTS view_check AS SELECT id FROM view_base_check")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_function_exists_check() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION fn_check(x INT64) RETURNS INT64 AS (x)")
        .await
        .unwrap();

    let result = session
        .execute_sql("CREATE FUNCTION IF NOT EXISTS fn_check(x INT64) RETURNS INT64 AS (x * 2)")
        .await;
    assert!(result.is_ok());

    let result = session.execute_sql("SELECT fn_check(5)").await.unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_alter_schema_nonexistent() {
    let session = create_session();

    let result = session
        .execute_sql("ALTER SCHEMA nonexistent SET OPTIONS(description='test')")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_case_insensitive_table() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE CaseSensitive (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO casesensitive VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM CASESENSITIVE")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_case_insensitive_schema() {
    let session = create_session();

    session.execute_sql("CREATE SCHEMA MySchema").await.unwrap();

    session
        .execute_sql("CREATE TABLE myschema.test (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO MYSCHEMA.test VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM MySchema.Test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_case_insensitive_function() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION MyFunc(x INT64) RETURNS INT64 AS (x)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT myfunc(5)").await.unwrap();
    assert_table_eq!(result, [[5]]);

    let result = session.execute_sql("SELECT MYFUNC(10)").await.unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_replace_table() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE replaceable (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO replaceable VALUES (1, 'original')")
        .await
        .unwrap();

    session
        .execute_sql("CREATE OR REPLACE TABLE replaceable (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO replaceable VALUES (2, 200)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, value FROM replaceable")
        .await
        .unwrap();
    assert_table_eq!(result, [[2, 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_table_in_schema_qualified() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA qualified_schema")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE qualified_schema.qualified_table (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO qualified_schema.qualified_table VALUES (42)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM qualified_schema.qualified_table")
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_create_schema_clears_dropped() {
    let session = create_session();

    session.execute_sql("CREATE SCHEMA reusable").await.unwrap();

    session
        .execute_sql("CREATE TABLE reusable.data (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO reusable.data VALUES (1)")
        .await
        .unwrap();

    session
        .execute_sql("DROP SCHEMA reusable CASCADE")
        .await
        .unwrap();

    session.execute_sql("CREATE SCHEMA reusable").await.unwrap();

    let result = session.execute_sql("SELECT * FROM reusable.data").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_drop_view_if_exists_succeeds() {
    let session = create_session();

    let result = session
        .execute_sql("DROP VIEW IF EXISTS nonexistent_view")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_or_replace_function() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION replaceable_fn(x INT64) RETURNS INT64 AS (x)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT replaceable_fn(5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);

    session
        .execute_sql("CREATE OR REPLACE FUNCTION replaceable_fn(x INT64) RETURNS INT64 AS (x * 2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT replaceable_fn(5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_or_replace_procedure() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE replaceable_proc()
            BEGIN
                SELECT 1;
            END",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("CALL replaceable_proc()")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);

    session
        .execute_sql(
            "CREATE OR REPLACE PROCEDURE replaceable_proc()
            BEGIN
                SELECT 2;
            END",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("CALL replaceable_proc()")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_or_replace_view() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE view_source (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO view_source VALUES (1), (2)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE VIEW replaceable_view AS SELECT * FROM view_source")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM replaceable_view")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);

    session
        .execute_sql(
            "CREATE OR REPLACE VIEW replaceable_view AS SELECT id FROM view_source WHERE id = 1",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM replaceable_view")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_nested_transaction_behavior() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE nested_txn (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO nested_txn VALUES (1, 100)")
        .await
        .unwrap();

    session.execute_sql("BEGIN TRANSACTION").await.unwrap();

    session
        .execute_sql("INSERT INTO nested_txn VALUES (2, 200)")
        .await
        .unwrap();

    session.execute_sql("COMMIT TRANSACTION").await.unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM nested_txn")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_view_with_column_aliases() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE alias_source (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO alias_source VALUES (1, 'Alice')")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE VIEW alias_view (user_id, user_name) AS SELECT id, name FROM alias_source",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT user_name FROM alias_view")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_function_from_catalog_provider() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION provider_fn(x INT64) RETURNS INT64 AS (x + 10)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT provider_fn(5)").await.unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_view_from_catalog_provider() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE provider_source (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO provider_source VALUES (42)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE VIEW provider_view AS SELECT * FROM provider_source")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM provider_view")
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_table_schema_from_catalog_provider() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE schema_test (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO schema_test VALUES (1, 'test', 3.14)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name, value FROM schema_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "test", 3.14]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_multiple_column_defaults() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE multi_defaults (
                id INT64,
                status STRING DEFAULT 'active',
                priority INT64 DEFAULT 5,
                label STRING DEFAULT 'none'
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO multi_defaults (id, priority) VALUES (1, 10)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, status, priority, label FROM multi_defaults")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "active", 10, "none"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_search_path_with_unqualified_table() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE default_table (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO default_table VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM default_table")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_drop_function_if_exists() {
    let session = create_session();

    let result = session
        .execute_sql("DROP FUNCTION IF EXISTS nonexistent_function")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_drop_procedure_if_exists() {
    let session = create_session();

    let result = session
        .execute_sql("DROP PROCEDURE IF EXISTS nonexistent_procedure")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_aggregate_function() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE AGGREGATE FUNCTION custom_sum(x INT64)
            RETURNS INT64
            AS (SUM(x))",
        )
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE agg_test (value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO agg_test VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT custom_sum(value) FROM agg_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_temp_function() {
    let session = create_session();

    session
        .execute_sql("CREATE TEMP FUNCTION temp_fn(x INT64) RETURNS INT64 AS (x * 3)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT temp_fn(5)").await.unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_schema_with_multiple_options() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE SCHEMA multi_option_schema OPTIONS(
                description = 'Test schema',
                location = 'US'
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE multi_option_schema.test (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT 1 FROM multi_option_schema.test")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_schema_options_are_stored() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA stored_opts OPTIONS(description = 'Initial description')")
        .await
        .unwrap();

    session
        .execute_sql("ALTER SCHEMA stored_opts SET OPTIONS(description = 'Updated description')")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE stored_opts.verify (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT 1 FROM stored_opts.verify")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_drop_empty_schema() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA empty_schema")
        .await
        .unwrap();

    session
        .execute_sql("DROP SCHEMA empty_schema")
        .await
        .unwrap();

    let result = session
        .execute_sql("CREATE TABLE empty_schema.test (id INT64)")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_table_not_found_error() {
    let session = create_session();

    let result = session.execute_sql("SELECT * FROM does_not_exist").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_function_not_found_error() {
    let session = create_session();

    let result = session.execute_sql("SELECT undefined_fn(1)").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_procedure_not_found_error() {
    let session = create_session();

    let result = session.execute_sql("CALL undefined_proc()").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_view_not_found_error() {
    let session = create_session();

    let result = session.execute_sql("SELECT * FROM undefined_view").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_replace_table_not_found() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE existing_table (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO existing_table VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM existing_table")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_get_column_default_not_found() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE default_test_col (
                id INT64,
                status STRING DEFAULT 'active'
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO default_test_col (id, status) VALUES (1, 'custom')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, status FROM default_test_col")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "custom"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_undrop_schema_if_not_exists_no_dropped() {
    let session = create_session();

    let result = session
        .execute_sql("UNDROP SCHEMA IF NOT EXISTS never_existed_schema")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_create_schema_with_options_if_not_exists() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA opts_schema OPTIONS(description='first')")
        .await
        .unwrap();

    let result = session
        .execute_sql("CREATE SCHEMA IF NOT EXISTS opts_schema OPTIONS(description='second')")
        .await;
    assert!(result.is_ok());

    session
        .execute_sql("CREATE TABLE opts_schema.test (id INT64)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT 1 FROM opts_schema.test").await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_insert_table_with_schema_prefix() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA ins_schema")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE ins_schema.data (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO ins_schema.data VALUES (42)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM ins_schema.data")
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_insert_table_dropped_schema_error() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA drop_ins_schema")
        .await
        .unwrap();

    session
        .execute_sql("DROP SCHEMA drop_ins_schema")
        .await
        .unwrap();

    let result = session
        .execute_sql("CREATE TABLE drop_ins_schema.new_table (id INT64)")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_procedure_if_not_exists() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE ine_proc()
            BEGIN
                SELECT 1;
            END",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "CREATE PROCEDURE IF NOT EXISTS ine_proc()
            BEGIN
                SELECT 2;
            END",
        )
        .await;
    assert!(result.is_ok());

    let result = session.execute_sql("CALL ine_proc()").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_table_mut_update() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE mut_table (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO mut_table VALUES (1, 100)")
        .await
        .unwrap();

    session
        .execute_sql("UPDATE mut_table SET value = 200 WHERE id = 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT value FROM mut_table WHERE id = 1")
        .await
        .unwrap();
    assert_table_eq!(result, [[200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_table_exists_check() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE exist_check (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql("CREATE TABLE IF NOT EXISTS exist_check (id INT64)")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_search_path_first_schema() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA sp_schema1")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE sp_schema1.users (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO sp_schema1.users VALUES (1)")
        .await
        .unwrap();

    session
        .execute_sql("SET search_path TO sp_schema1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT id FROM users").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_drop_table_with_search_path() {
    let session = create_session();

    session.execute_sql("CREATE SCHEMA drop_sp").await.unwrap();

    session
        .execute_sql("CREATE TABLE drop_sp.to_drop (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("SET search_path TO drop_sp")
        .await
        .unwrap();

    session.execute_sql("DROP TABLE to_drop").await.unwrap();

    let result = session.execute_sql("SELECT * FROM to_drop").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_rename_table_qualified() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA rename_sp")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE rename_sp.old_table (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO rename_sp.old_table VALUES (5)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE rename_sp.old_table RENAME TO new_table")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM new_table")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_get_table_defaults_qualified() {
    let session = create_session();

    session.execute_sql("CREATE SCHEMA def_sp").await.unwrap();

    session
        .execute_sql(
            "CREATE TABLE def_sp.defaults_table (
                id INT64,
                status STRING DEFAULT 'pending'
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO def_sp.defaults_table (id) VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, status FROM def_sp.defaults_table")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "pending"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_catalog_rollback_no_transaction() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE rollback_test (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO rollback_test VALUES (1)")
        .await
        .unwrap();

    session.execute_sql("ROLLBACK TRANSACTION").await.unwrap();

    let result = session
        .execute_sql("SELECT id FROM rollback_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}
