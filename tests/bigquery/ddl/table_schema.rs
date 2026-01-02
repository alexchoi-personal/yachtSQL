use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_schema_validate_unique_column_names() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE unique_cols (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO unique_cols VALUES (1, 'test', 3.14)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name, value FROM unique_cols")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "test", 3.14]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_mode_required_not_null() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE required_mode (id INT64 NOT NULL, name STRING NOT NULL)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO required_mode VALUES (1, 'Alice')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name FROM required_mode")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_mode_nullable() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE nullable_mode (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO nullable_mode VALUES (1, NULL)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO nullable_mode VALUES (NULL, 'Bob')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM nullable_mode ORDER BY id NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, null], [null, "Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_mode_repeated_array() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE repeated_mode (id INT64, tags ARRAY<STRING>)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO repeated_mode VALUES (1, ['a', 'b', 'c'])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, ARRAY_LENGTH(tags) FROM repeated_mode")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_compatibility_same_structure() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE compat_t1 (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE compat_t2 (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO compat_t1 VALUES (1, 'Alice')")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO compat_t2 SELECT * FROM compat_t1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM compat_t2")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_with_default_value() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE default_vals (id INT64, status STRING DEFAULT 'pending', priority INT64 DEFAULT 1)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO default_vals (id) VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, status, priority FROM default_vals")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "pending", 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_with_collation() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE collate_field (id INT64, name STRING COLLATE 'und:ci')")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO collate_field VALUES (1, 'Alice')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name FROM collate_field WHERE name = 'ALICE'")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_with_description() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE desc_field (id INT64 OPTIONS(description='Primary key'), name STRING OPTIONS(description='User name'))")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO desc_field VALUES (1, 'Alice')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name FROM desc_field")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_add_field_preserves_data() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE add_field_test (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO add_field_test VALUES (1, 'Alice')")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE add_field_test ADD COLUMN age INT64")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM add_field_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice", null]]);

    session
        .execute_sql("INSERT INTO add_field_test VALUES (2, 'Bob', 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM add_field_test ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice", null], [2, "Bob", 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_remove_field() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE remove_field_test (id INT64, name STRING, age INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO remove_field_test VALUES (1, 'Alice', 30)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE remove_field_test DROP COLUMN age")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM remove_field_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_rename_field() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE rename_field_test (id INT64, old_name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO rename_field_test VALUES (1, 'Alice')")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE rename_field_test RENAME COLUMN old_name TO new_name")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, new_name FROM rename_field_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_index_by_name() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE field_index_test (a INT64, b STRING, c FLOAT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO field_index_test VALUES (1, 'test', 3.14)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT b FROM field_index_test")
        .await
        .unwrap();
    assert_table_eq!(result, [["test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_qualified_access_with_alias() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE qualified_access (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO qualified_access VALUES (1, 'Alice')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT t.id, t.name FROM qualified_access AS t")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_qualified_access_with_table_name() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE tbl_access (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO tbl_access VALUES (1, 'Alice')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT tbl_access.id, tbl_access.name FROM tbl_access")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_empty_table() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE empty_table (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM empty_table")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_single_field() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE single_field (value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO single_field VALUES (42)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM single_field")
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_many_fields() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE many_fields (f1 INT64, f2 INT64, f3 INT64, f4 INT64, f5 INT64, f6 INT64, f7 INT64, f8 INT64, f9 INT64, f10 INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO many_fields VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT f1, f5, f10 FROM many_fields")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 5, 10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_is_compatible_with_insert_select() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE source_compat (id INT64, value STRING)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE dest_compat (id INT64, value STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO source_compat VALUES (1, 'a'), (2, 'b')")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO dest_compat SELECT * FROM source_compat")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM dest_compat ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [2, "b"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_from_fields_via_ctas() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE ctas_fields AS SELECT 1 AS id, 'test' AS name, 3.14 AS value")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name, value FROM ctas_fields")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "test", 3.14]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_validate_with_alter_table() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE validate_alter (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE validate_alter ADD COLUMN name STRING")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO validate_alter VALUES (1, 'Alice')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM validate_alter")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_types_all_basic() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE basic_types (
                col_bool BOOL,
                col_int INT64,
                col_float FLOAT64,
                col_string STRING,
                col_bytes BYTES,
                col_date DATE,
                col_time TIME,
                col_datetime DATETIME,
                col_timestamp TIMESTAMP
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO basic_types VALUES (
                TRUE,
                42,
                3.14,
                'hello',
                b'world',
                DATE '2024-01-15',
                TIME '10:30:00',
                DATETIME '2024-01-15 10:30:00',
                TIMESTAMP '2024-01-15 10:30:00 UTC'
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT col_bool, col_int, col_string FROM basic_types")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, 42, "hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_with_struct_field() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE struct_schema (
                id INT64,
                data STRUCT<name STRING, age INT64>
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO struct_schema VALUES (1, STRUCT('Alice', 30))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, data.name, data.age FROM struct_schema")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice", 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_with_array_of_struct_field() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE array_struct_schema (
                id INT64,
                items ARRAY<STRUCT<name STRING, qty INT64>>
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO array_struct_schema VALUES (1, [STRUCT('Widget', 5), STRUCT('Gadget', 3)])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, ARRAY_LENGTH(items) FROM array_struct_schema")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_count_after_operations() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE field_count_test (a INT64, b STRING)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE field_count_test ADD COLUMN c FLOAT64")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE field_count_test ADD COLUMN d BOOL")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE field_count_test DROP COLUMN b")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO field_count_test VALUES (1, 3.14, TRUE)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a, c, d FROM field_count_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 3.14, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_case_insensitive_column_names() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE case_test (ID INT64, Name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO case_test VALUES (1, 'Alice')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name FROM case_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);

    let result2 = session
        .execute_sql("SELECT ID, NAME FROM case_test")
        .await
        .unwrap();
    assert_table_eq!(result2, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_join_field_resolution() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE join_a (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE join_b (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO join_a VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO join_b VALUES (1, 100), (2, 200)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT a.name, b.value FROM join_a a JOIN join_b b ON a.id = b.id ORDER BY a.name",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice", 100], ["Bob", 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_dotted_qualified_name() {
    let session = create_session();

    session.execute_sql("CREATE SCHEMA mydb").await.unwrap();

    session
        .execute_sql("CREATE TABLE mydb.dotted_table (id INT64, value STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO mydb.dotted_table VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, value FROM mydb.dotted_table")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_mixed_field_modes() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE mixed_modes (
                required_field INT64 NOT NULL,
                nullable_field STRING,
                repeated_field ARRAY<INT64>
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO mixed_modes VALUES (1, NULL, [1, 2, 3])")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO mixed_modes VALUES (2, 'test', [])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT required_field, nullable_field, ARRAY_LENGTH(repeated_field) FROM mixed_modes ORDER BY required_field")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, null, 3], [2, "test", 0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_lookup_ambiguous() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE lookup_a (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE lookup_b (id INT64, value STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO lookup_a VALUES (1, 'Alice')")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO lookup_b VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a.id, b.value FROM lookup_a a JOIN lookup_b b ON a.id = b.id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_qualified_with_project_dataset_table() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE proj.dataset.qualified_table (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO proj.dataset.qualified_table VALUES (1, 'Alice')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT proj.dataset.qualified_table.id FROM proj.dataset.qualified_table")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_case_insensitive_qualified() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE CasedTable (ID INT64, Name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO CasedTable VALUES (1, 'Alice')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT casedtable.id, CASEDTABLE.name FROM casedtable")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_self_join_with_aliases() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE employees (id INT64, name STRING, manager_id INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO employees VALUES (1, 'Alice', NULL), (2, 'Bob', 1), (3, 'Charlie', 1)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT e.name AS employee, m.name AS manager
             FROM employees e
             LEFT JOIN employees m ON e.manager_id = m.id
             ORDER BY e.name",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [["Alice", null], ["Bob", "Alice"], ["Charlie", "Alice"]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_multiple_table_qualified_columns() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE orders (order_id INT64, customer_id INT64, amount FLOAT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE customers (customer_id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE products (product_id INT64, order_id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO customers VALUES (1, 'Alice')")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO orders VALUES (100, 1, 99.99)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO products VALUES (1000, 100, 'Widget')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT c.name, o.amount, p.name as product
             FROM customers c
             JOIN orders o ON c.customer_id = o.customer_id
             JOIN products p ON o.order_id = p.order_id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice", 99.99, "Widget"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_validate_duplicate_column_select() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE dup_select (a INT64, b INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO dup_select VALUES (1, 2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a, b, a + b as total FROM dup_select")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 2, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_is_compatible_union() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE union_a (id INT64, value STRING)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE union_b (id INT64, value STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO union_a VALUES (1, 'a'), (2, 'b')")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO union_b VALUES (3, 'c'), (4, 'd')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM union_a UNION ALL SELECT * FROM union_b ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [2, "b"], [3, "c"], [4, "d"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_with_default_null() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE default_null (id INT64, optional_field STRING DEFAULT NULL)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO default_null (id) VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM default_null")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_with_numeric_default() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE num_default (id INT64, count INT64 DEFAULT 0, ratio FLOAT64 DEFAULT 1.0)",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO num_default (id) VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM num_default")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 0, 1.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_repeated_empty_array() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE empty_array (id INT64, items ARRAY<INT64>)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO empty_array VALUES (1, [])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, ARRAY_LENGTH(items) FROM empty_array")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_repeated_null_elements() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE nullable_array (id INT64, values ARRAY<STRING>)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO nullable_array VALUES (1, ['a', NULL, 'b'])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, ARRAY_LENGTH(values) FROM nullable_array")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_collation_case_insensitive_comparison() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE ci_collation (id INT64, name STRING COLLATE 'und:ci')")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO ci_collation VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name FROM ci_collation WHERE name = 'ALICE'")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_subquery_field_resolution() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE outer_tbl (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE inner_tbl (id INT64, ref_id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO outer_tbl VALUES (1, 100), (2, 200)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO inner_tbl VALUES (10, 1), (20, 2)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT o.value
             FROM outer_tbl o
             WHERE o.id IN (SELECT ref_id FROM inner_tbl WHERE id > 15)
             ORDER BY o.value",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_cross_join_field_resolution() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE left_cross (a INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE right_cross (b INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO left_cross VALUES (1), (2)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO right_cross VALUES (10), (20)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a, b FROM left_cross CROSS JOIN right_cross ORDER BY a, b")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [1, 20], [2, 10], [2, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_nested_struct_field_access() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE nested_access (
                id INT64,
                data STRUCT<
                    level1 STRUCT<
                        level2 STRUCT<
                            value STRING
                        >
                    >
                >
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO nested_access VALUES (1, STRUCT(STRUCT(STRUCT('nested_value'))))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT data.level1.level2.value FROM nested_access")
        .await
        .unwrap();
    assert_table_eq!(result, [["nested_value"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_array_of_struct_field_access() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE array_struct_access (
                id INT64,
                items ARRAY<STRUCT<name STRING, qty INT64>>
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO array_struct_access VALUES (1, [STRUCT('Widget', 5), STRUCT('Gadget', 3)])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT items[OFFSET(0)].name, items[OFFSET(1)].qty FROM array_struct_access")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget", 3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_rename_field_preserves_data() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE rename_preserve (id INT64, old_name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO rename_preserve VALUES (1, 'data1'), (2, 'data2')")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE rename_preserve RENAME COLUMN old_name TO new_name")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, new_name FROM rename_preserve ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "data1"], [2, "data2"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_remove_field_preserves_other_data() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE remove_preserve (a INT64, b STRING, c FLOAT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO remove_preserve VALUES (1, 'x', 1.5), (2, 'y', 2.5)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE remove_preserve DROP COLUMN b")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a, c FROM remove_preserve ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 1.5], [2, 2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_is_empty_with_ctas() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE empty_source (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE empty_ctas AS SELECT * FROM empty_source WHERE 1=0")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM empty_ctas")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_index_with_subquery_alias() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE subquery_alias (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO subquery_alias VALUES (1, 100), (2, 200)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT sq.id, sq.value
             FROM (SELECT id, value FROM subquery_alias WHERE value > 150) AS sq",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[2, 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_field_with_source_table_cross_schema() {
    let session = create_session();

    session.execute_sql("CREATE SCHEMA schema_a").await.unwrap();

    session.execute_sql("CREATE SCHEMA schema_b").await.unwrap();

    session
        .execute_sql("CREATE TABLE schema_a.users (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE schema_b.orders (order_id INT64, user_id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO schema_a.users VALUES (1, 'Alice')")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO schema_b.orders VALUES (100, 1)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT u.name, o.order_id
             FROM schema_a.users u
             JOIN schema_b.orders o ON u.id = o.user_id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice", 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_compatibility_different_modes() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE mode_a (id INT64 NOT NULL, value STRING)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE mode_b (id INT64, value STRING NOT NULL)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO mode_a VALUES (1, 'a')")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO mode_b SELECT id, value FROM mode_a")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM mode_b").await.unwrap();
    assert_table_eq!(result, [[1, "a"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_cte_field_resolution() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE cte_data (id INT64, category STRING, amount INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO cte_data VALUES (1, 'A', 100), (2, 'A', 200), (3, 'B', 150)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH category_totals AS (
                SELECT category, SUM(amount) as total
                FROM cte_data
                GROUP BY category
            )
            SELECT ct.category, ct.total
            FROM category_totals ct
            ORDER BY ct.category",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["A", 300], ["B", 150]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_window_function_field_resolution() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE window_data (id INT64, dept STRING, salary INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO window_data VALUES (1, 'Sales', 50000), (2, 'Sales', 60000), (3, 'Eng', 70000)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, dept, salary, SUM(salary) OVER (PARTITION BY dept) as dept_total
             FROM window_data
             ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, "Sales", 50000, 110000],
            [2, "Sales", 60000, 110000],
            [3, "Eng", 70000, 70000]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_lateral_join_field_resolution() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE lateral_main (id INT64, items ARRAY<INT64>)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO lateral_main VALUES (1, [10, 20, 30]), (2, [40, 50])")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT m.id, item
             FROM lateral_main m, UNNEST(m.items) AS item
             ORDER BY m.id, item",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [1, 20], [1, 30], [2, 40], [2, 50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_star_expansion_with_table_qualifier() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE star_a (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE star_b (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO star_a VALUES (1, 'Alice')")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO star_b VALUES (1, 100)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a.*, b.value FROM star_a a JOIN star_b b ON a.id = b.id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice", 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_except_columns() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE except_test (a INT64, b STRING, c FLOAT64, d BOOL)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO except_test VALUES (1, 'x', 1.5, TRUE)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * EXCEPT (b, d) FROM except_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 1.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_replace_columns() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE replace_test (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO replace_test VALUES (1, 100)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * REPLACE (value * 2 AS value) FROM replace_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 200]]);
}
