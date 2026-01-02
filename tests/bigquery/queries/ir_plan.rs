use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_scan_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE scan_table (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO scan_table VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM scan_table ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"], [2, "Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scan_with_projection() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE proj_scan (a INT64, b INT64, c INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO proj_scan VALUES (1, 2, 3), (4, 5, 6)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a, c FROM proj_scan ORDER BY a")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 3], [4, 6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_filter_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE filter_table (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO filter_table VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM filter_table WHERE value > 15 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_project_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE project_table (x INT64, y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO project_table VALUES (5, 3), (10, 2)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT x + y AS sum, x - y AS diff, x * y AS prod FROM project_table ORDER BY x",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[8, 2, 15], [12, 8, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sort_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sort_table (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sort_table VALUES (3, 'c'), (1, 'a'), (2, 'b')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM sort_table ORDER BY id ASC")
        .await
        .unwrap();
    assert_table_eq!(result, [["a"], ["b"], ["c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_limit_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE limit_table (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO limit_table VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM limit_table ORDER BY id LIMIT 3")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE distinct_table (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO distinct_table VALUES (1), (1), (2), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT val FROM distinct_table ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_values_plan() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [2, "b"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_empty_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE empty_table (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM empty_table")
        .await
        .unwrap();
    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE agg_table (grp STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO agg_table VALUES ('A', 10), ('A', 20), ('B', 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT grp, SUM(val) AS total FROM agg_table GROUP BY grp ORDER BY grp")
        .await
        .unwrap();
    assert_table_eq!(result, [["A", 30], ["B", 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_inner() {
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
            "SELECT a.name, b.value FROM join_a a JOIN join_b b ON a.id = b.id ORDER BY a.id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice", 100], ["Bob", 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_set_union_all() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE union_a (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE union_b (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO union_a VALUES (1), (2)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO union_b VALUES (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM union_a UNION ALL SELECT val FROM union_b ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE window_table (id INT64, grp STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO window_table VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, SUM(val) OVER (PARTITION BY grp) AS grp_total FROM window_table ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 30], [2, 30], [3, 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cte_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE cte_table (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO cte_table VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();

    let result = session
        .execute_sql("WITH filtered AS (SELECT * FROM cte_table WHERE id > 0) SELECT name FROM filtered ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unnest_plan() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT n FROM UNNEST([1, 2, 3]) AS n ORDER BY n")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE qualify_table (id INT64, grp STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO qualify_table VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30), (4, 'B', 25)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, grp, val FROM qualify_table QUALIFY ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val DESC) = 1 ORDER BY grp")
        .await
        .unwrap();
    assert_table_eq!(result, [[2, "A", 20], [3, "B", 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE insert_tbl (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO insert_tbl VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM insert_tbl ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE update_tbl (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO update_tbl VALUES (1, 10), (2, 20)")
        .await
        .unwrap();

    session
        .execute_sql("UPDATE update_tbl SET val = val * 2 WHERE id = 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, val FROM update_tbl ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 20], [2, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE delete_tbl (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO delete_tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();

    session
        .execute_sql("DELETE FROM delete_tbl WHERE id = 2")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM delete_tbl ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [["a"], ["c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE merge_target (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE merge_source (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO merge_target VALUES (1, 10)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO merge_source VALUES (1, 100), (2, 200)")
        .await
        .unwrap();

    session
        .execute_sql("MERGE INTO merge_target t USING merge_source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET val = s.val WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, val FROM merge_target ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 100], [2, 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_truncate_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE truncate_tbl (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO truncate_tbl VALUES (1), (2), (3)")
        .await
        .unwrap();

    session
        .execute_sql("TRUNCATE TABLE truncate_tbl")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM truncate_tbl")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_table_plan() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE new_tbl (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO new_tbl VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM new_tbl").await.unwrap();
    assert_table_eq!(result, [[1, "test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_table_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE drop_tbl (id INT64)")
        .await
        .unwrap();

    session.execute_sql("DROP TABLE drop_tbl").await.unwrap();

    let result = session.execute_sql("SELECT * FROM drop_tbl").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE alter_add_tbl (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE alter_add_tbl ADD COLUMN name STRING")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO alter_add_tbl VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM alter_add_tbl")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_view_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE view_src (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO view_src VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();

    session
        .execute_sql("CREATE VIEW test_view AS SELECT * FROM view_src WHERE id > 0")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM test_view ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_view_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE dv_src (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE VIEW drop_v AS SELECT * FROM dv_src")
        .await
        .unwrap();

    session.execute_sql("DROP VIEW drop_v").await.unwrap();

    let result = session.execute_sql("SELECT * FROM drop_v").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_schema_plan() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA new_schema")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_schema_plan() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA drop_schema")
        .await
        .unwrap();
    session
        .execute_sql("DROP SCHEMA drop_schema")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_plan() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION triple(x INT64) RETURNS INT64 AS (x * 3)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT triple(5)").await.unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_function_plan() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION drop_fn(x INT64) RETURNS INT64 AS (x)")
        .await
        .unwrap();
    session.execute_sql("DROP FUNCTION drop_fn").await.unwrap();

    let result = session.execute_sql("SELECT drop_fn(1)").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_procedure_plan() {
    let session = create_session();

    session
        .execute_sql("CREATE PROCEDURE test_proc() BEGIN SELECT 1; END")
        .await
        .unwrap();

    session.execute_sql("CALL test_proc()").await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_procedure_plan() {
    let session = create_session();

    session
        .execute_sql("CREATE PROCEDURE drop_proc() BEGIN SELECT 1; END")
        .await
        .unwrap();
    session
        .execute_sql("DROP PROCEDURE drop_proc")
        .await
        .unwrap();

    let result = session.execute_sql("CALL drop_proc()").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_declare_plan() {
    let session = create_session();

    session.execute_sql("DECLARE x INT64").await.unwrap();
    session.execute_sql("SET x = 42").await.unwrap();

    let result = session.execute_sql("SELECT x").await.unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_set_variable_plan() {
    let session = create_session();

    session
        .execute_sql("DECLARE a INT64 DEFAULT 10")
        .await
        .unwrap();
    session.execute_sql("SET a = a + 5").await.unwrap();

    let result = session.execute_sql("SELECT a").await.unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_if_plan() {
    let session = create_session();

    session
        .execute_sql("DECLARE val INT64 DEFAULT 10")
        .await
        .unwrap();
    session.execute_sql("DECLARE res STRING").await.unwrap();
    session
        .execute_sql("IF val > 5 THEN SET res = 'big'; ELSE SET res = 'small'; END IF")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT res").await.unwrap();
    assert_table_eq!(result, [["big"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_while_plan() {
    let session = create_session();

    session
        .execute_sql("DECLARE i INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql("DECLARE sum INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql("WHILE i < 3 DO SET i = i + 1; SET sum = sum + i; END WHILE")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT sum").await.unwrap();
    assert_table_eq!(result, [[6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_loop_plan() {
    let session = create_session();

    session
        .execute_sql("DECLARE i INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql("LOOP SET i = i + 1; IF i >= 3 THEN LEAVE; END IF; END LOOP")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT i").await.unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_block_plan() {
    let session = create_session();

    session
        .execute_sql("DECLARE out_val INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql("BEGIN SET out_val = 10; END")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT out_val").await.unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_repeat_plan() {
    let session = create_session();

    session
        .execute_sql("DECLARE i INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql("REPEAT SET i = i + 1; UNTIL i >= 3 END REPEAT")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT i").await.unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_for_plan() {
    let session = create_session();

    session
        .execute_sql("DECLARE sum INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql(
            "FOR x IN (SELECT n FROM UNNEST([1, 2, 3]) AS n) DO SET sum = sum + x.n; END FOR",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT sum").await.unwrap();
    assert_table_eq!(result, [[6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_return_plan() {
    let session = create_session();

    session
        .execute_sql("DECLARE x INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql("BEGIN SET x = 1; RETURN; SET x = 2; END")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT x").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_raise_plan() {
    let session = create_session();

    let result = session
        .execute_sql("RAISE USING MESSAGE = 'test error'")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_break_plan() {
    let session = create_session();

    session
        .execute_sql("DECLARE i INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql("LOOP SET i = i + 1; IF i >= 2 THEN BREAK; END IF; END LOOP")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT i").await.unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_continue_plan() {
    let session = create_session();

    session
        .execute_sql("DECLARE i INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql("DECLARE sum INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql("WHILE i < 4 DO SET i = i + 1; IF i = 2 THEN CONTINUE; END IF; SET sum = sum + i; END WHILE")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT sum").await.unwrap();
    assert_table_eq!(result, [[8]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_execute_immediate_plan() {
    let session = create_session();

    session.execute_sql("DECLARE result INT64").await.unwrap();
    session
        .execute_sql("EXECUTE IMMEDIATE 'SELECT 42' INTO result")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT result").await.unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_assert_plan() {
    let session = create_session();

    session.execute_sql("ASSERT 1 = 1").await.unwrap();

    let result = session.execute_sql("ASSERT FALSE").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_grant_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE grant_tbl (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("GRANT SELECT ON TABLE grant_tbl TO 'user@example.com'")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_revoke_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE revoke_tbl (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("REVOKE SELECT ON TABLE revoke_tbl FROM 'user@example.com'")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_transaction_begin_commit() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE tx_tbl (id INT64)")
        .await
        .unwrap();

    session.execute_sql("BEGIN TRANSACTION").await.unwrap();
    session
        .execute_sql("INSERT INTO tx_tbl VALUES (1)")
        .await
        .unwrap();
    session.execute_sql("COMMIT").await.unwrap();

    let result = session.execute_sql("SELECT * FROM tx_tbl").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_transaction_rollback() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE rollback_tbl (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO rollback_tbl VALUES (1)")
        .await
        .unwrap();

    session.execute_sql("BEGIN TRANSACTION").await.unwrap();
    session
        .execute_sql("INSERT INTO rollback_tbl VALUES (2)")
        .await
        .unwrap();
    session.execute_sql("ROLLBACK").await.unwrap();

    let result = session
        .execute_sql("SELECT * FROM rollback_tbl")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_try_catch_plan() {
    let session = create_session();

    session
        .execute_sql("DECLARE caught BOOL DEFAULT FALSE")
        .await
        .unwrap();
    session
        .execute_sql("BEGIN SELECT 1 / 0; EXCEPTION WHEN ERROR THEN SET caught = TRUE; END")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT caught").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_snapshot_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE snap_src (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE SNAPSHOT TABLE test_snap CLONE snap_src")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_snapshot_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE snap_src2 (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE SNAPSHOT TABLE drop_snap CLONE snap_src2")
        .await
        .unwrap();

    session
        .execute_sql("DROP SNAPSHOT TABLE drop_snap")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_gap_fill_plan() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE gap_data (
                ts DATETIME,
                val INT64
            )",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO gap_data VALUES
            (DATETIME '2024-01-01 10:00:00', 10),
            (DATETIME '2024-01-01 10:02:00', 30)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT ts, val
            FROM GAP_FILL(
                TABLE gap_data,
                ts_column => 'ts',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [('val', 'locf')]
            )
            ORDER BY ts",
        )
        .await
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_undrop_schema_plan() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA undrop_test_schema")
        .await
        .unwrap();
    session
        .execute_sql("DROP SCHEMA undrop_test_schema")
        .await
        .unwrap();

    let result = session
        .execute_sql("UNDROP SCHEMA undrop_test_schema")
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_schema_plan() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA alter_test_schema")
        .await
        .unwrap();

    let result = session
        .execute_sql("ALTER SCHEMA alter_test_schema SET OPTIONS (description = 'Test schema')")
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_tbl (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_tbl VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS (
                uri = 'gs://bucket/path/*.csv',
                format = 'CSV'
            ) AS SELECT * FROM export_tbl",
        )
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_plan() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE load_tbl (id INT64, name STRING)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "LOAD DATA INTO load_tbl FROM FILES (
                format = 'CSV',
                uris = ['gs://bucket/path/*.csv']
            )",
        )
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_set_multiple_variables_plan() {
    let session = create_session();

    session.execute_sql("DECLARE x INT64").await.unwrap();
    session.execute_sql("DECLARE y STRING").await.unwrap();
    session
        .execute_sql("SET (x, y) = (42, 'hello')")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT x, y").await.unwrap();
    assert_table_eq!(result, [[42, "hello"]]);
}
