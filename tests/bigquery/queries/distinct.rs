use crate::assert_table_eq;
use crate::common::{create_session, null};

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_single_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE colors (id INT64, color STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO colors VALUES (1, 'red'), (2, 'blue'), (3, 'red'), (4, 'green'), (5, 'blue')").await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT color FROM colors ORDER BY color")
        .await
        .unwrap();
    assert_table_eq!(result, [["blue"], ["green"], ["red"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_multiple_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE orders (customer STRING, product STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO orders VALUES ('alice', 'apple'), ('bob', 'banana'), ('alice', 'apple'), ('alice', 'banana')").await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT customer, product FROM orders ORDER BY customer, product")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [["alice", "apple"], ["alice", "banana"], ["bob", "banana"]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_with_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nullable (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1), (NULL), (2), (NULL), (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT value FROM nullable ORDER BY value NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[null()], [1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_distinct() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (category STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES ('a'), ('b'), ('a'), ('c'), ('b'), ('a')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(DISTINCT category) FROM items")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_with_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE products (name STRING, price INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO products VALUES ('a', 10), ('b', 20), ('a', 15), ('c', 10), ('b', 25)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT name FROM products WHERE price > 10 ORDER BY name")
        .await
        .unwrap();
    assert_table_eq!(result, [["a"], ["b"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_with_order_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE scores (score INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO scores VALUES (100), (50), (100), (75), (50)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT score FROM scores ORDER BY score DESC")
        .await
        .unwrap();
    assert_table_eq!(result, [[100], [75], [50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_with_limit() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1), (2), (1), (3), (2), (4)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT val FROM data ORDER BY val LIMIT 2")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_all_same() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE same (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO same VALUES (5), (5), (5), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT val FROM same")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_all_different() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE diff (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO diff VALUES (1), (2), (3), (4)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT val FROM diff ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_empty_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE empty_table (val INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT val FROM empty_table")
        .await
        .unwrap();
    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_with_float() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE floats (val FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO floats VALUES (1.1), (2.2), (1.1), (3.3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT val FROM floats ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[1.1], [2.2], [3.3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_with_bool() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bools (val BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bools VALUES (true), (false), (true), (false), (true)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT val FROM bools ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[false], [true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_with_bytes() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bytes_table (val BYTES)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bytes_table VALUES (b'abc'), (b'def'), (b'abc')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(DISTINCT val) FROM bytes_table")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_with_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nums (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nums VALUES (1), (2), (3), (4), (5), (6)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT MOD(val, 2) AS remainder FROM nums ORDER BY remainder")
        .await
        .unwrap();
    assert_table_eq!(result, [[0], [1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_with_subquery() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE parent (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO parent VALUES (1), (1), (2), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM (SELECT DISTINCT id FROM parent) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_with_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE a (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE b (id INT64, other STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO a VALUES (1, 'x'), (1, 'x'), (2, 'y')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO b VALUES (1, 'p'), (2, 'q')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT a.val FROM a JOIN b ON a.id = b.id ORDER BY a.val")
        .await
        .unwrap();
    assert_table_eq!(result, [["x"], ["y"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_on_aggregation() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sales (region STRING, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO sales VALUES ('East', 100), ('West', 100), ('East', 200), ('West', 200)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT DISTINCT SUM(amount) AS total FROM sales GROUP BY region ORDER BY total",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[300]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_with_nulls_in_multiple_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE multi_null (a INT64, b STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO multi_null VALUES (1, 'x'), (NULL, 'x'), (1, NULL), (NULL, NULL), (1, 'x')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT a, b FROM multi_null ORDER BY a NULLS FIRST, b NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[null(), null()], [null(), "x"], [1, null()], [1, "x"]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_with_case() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE categorized (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO categorized VALUES (1), (5), (10), (15), (20)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT DISTINCT CASE WHEN val < 10 THEN 'low' ELSE 'high' END AS category FROM categorized ORDER BY category",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["high"], ["low"]]);
}
