use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_different_order_by_directions() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE sales (id INT64, product STRING, amount INT64, sale_date DATE)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO sales VALUES
            (1, 'A', 100, DATE '2024-01-01'),
            (2, 'A', 200, DATE '2024-01-02'),
            (3, 'A', 150, DATE '2024-01-03'),
            (4, 'B', 300, DATE '2024-01-01'),
            (5, 'B', 250, DATE '2024-01-02')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                product,
                ARRAY_AGG(amount ORDER BY sale_date ASC) AS amounts_chronological,
                ARRAY_AGG(amount ORDER BY sale_date DESC) AS amounts_reverse,
                ARRAY_AGG(amount ORDER BY amount ASC) AS amounts_by_value_asc,
                ARRAY_AGG(amount ORDER BY amount DESC) AS amounts_by_value_desc
            FROM sales
            GROUP BY product
            ORDER BY product",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [
                "A",
                [100, 200, 150],
                [150, 200, 100],
                [100, 150, 200],
                [200, 150, 100]
            ],
            ["B", [300, 250], [250, 300], [250, 300], [300, 250]],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_different_limits() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE items (category STRING, item STRING, priority INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO items VALUES
            ('X', 'a', 1), ('X', 'b', 2), ('X', 'c', 3), ('X', 'd', 4), ('X', 'e', 5),
            ('Y', 'p', 1), ('Y', 'q', 2), ('Y', 'r', 3)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                category,
                ARRAY_AGG(item ORDER BY priority LIMIT 1) AS top_1,
                ARRAY_AGG(item ORDER BY priority LIMIT 2) AS top_2,
                ARRAY_AGG(item ORDER BY priority LIMIT 3) AS top_3,
                ARRAY_AGG(item ORDER BY priority) AS all_items
            FROM items
            GROUP BY category
            ORDER BY category",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [
                "X",
                ["a"],
                ["a", "b"],
                ["a", "b", "c"],
                ["a", "b", "c", "d", "e"]
            ],
            ["Y", ["p"], ["p", "q"], ["p", "q", "r"], ["p", "q", "r"]],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_ignore_nulls_variations() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE nullable_data (grp STRING, val STRING, seq INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO nullable_data VALUES
            ('A', NULL, 1), ('A', 'first', 2), ('A', NULL, 3), ('A', 'second', 4), ('A', NULL, 5),
            ('B', 'only', 1), ('B', NULL, 2)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                grp,
                ARRAY_AGG(val ORDER BY seq) AS with_nulls,
                ARRAY_AGG(val IGNORE NULLS ORDER BY seq) AS without_nulls,
                ARRAY_AGG(val ORDER BY seq DESC) AS with_nulls_desc,
                ARRAY_AGG(val IGNORE NULLS ORDER BY seq DESC) AS without_nulls_desc
            FROM nullable_data
            GROUP BY grp
            ORDER BY grp",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [
                "A",
                [null, "first", null, "second", null],
                ["first", "second"],
                [null, "second", null, "first", null],
                ["second", "first"]
            ],
            ["B", ["only", null], ["only"], [null, "only"], ["only"]],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_distinct_vs_non_distinct() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE duplicates (grp STRING, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO duplicates VALUES
            ('A', 'x'), ('A', 'y'), ('A', 'x'), ('A', 'z'), ('A', 'y'), ('A', 'x'),
            ('B', 'p'), ('B', 'p'), ('B', 'q')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                grp,
                ARRAY_AGG(val ORDER BY val) AS all_values,
                ARRAY_AGG(DISTINCT val ORDER BY val) AS distinct_values
            FROM duplicates
            GROUP BY grp
            ORDER BY grp",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["A", ["x", "x", "x", "y", "y", "z"], ["x", "y", "z"]],
            ["B", ["p", "p", "q"], ["p", "q"]],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_count_variations() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE mixed (grp STRING, a INT64, b STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO mixed VALUES
            ('X', 1, 'foo'), ('X', 2, 'foo'), ('X', 1, 'bar'), ('X', NULL, 'baz'),
            ('Y', 5, NULL), ('Y', 5, 'qux')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                grp,
                COUNT(*) AS count_all,
                COUNT(a) AS count_a,
                COUNT(b) AS count_b,
                COUNT(DISTINCT a) AS count_distinct_a,
                COUNT(DISTINCT b) AS count_distinct_b
            FROM mixed
            GROUP BY grp
            ORDER BY grp",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["X", 4, 3, 4, 2, 3], ["Y", 2, 2, 1, 1, 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sum_avg_on_different_columns() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE metrics (grp STRING, x INT64, y INT64, z INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO metrics VALUES
            ('A', 10, 100, 1000),
            ('A', 20, 200, 2000),
            ('B', 5, 50, 500)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                grp,
                SUM(x) AS sum_x,
                SUM(y) AS sum_y,
                SUM(z) AS sum_z,
                AVG(x) AS avg_x,
                AVG(y) AS avg_y,
                AVG(z) AS avg_z
            FROM metrics
            GROUP BY grp
            ORDER BY grp",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["A", 30.0, 300.0, 3000.0, 15.0, 150.0, 1500.0],
            ["B", 5.0, 50.0, 500.0, 5.0, 50.0, 500.0],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_min_max_on_different_columns() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE ranges (grp STRING, low INT64, mid INT64, high INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO ranges VALUES
            ('A', 1, 50, 100),
            ('A', 5, 40, 90),
            ('A', 3, 60, 95),
            ('B', 10, 20, 30)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                grp,
                MIN(low) AS min_low,
                MAX(low) AS max_low,
                MIN(mid) AS min_mid,
                MAX(mid) AS max_mid,
                MIN(high) AS min_high,
                MAX(high) AS max_high
            FROM ranges
            GROUP BY grp
            ORDER BY grp",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["A", 1, 5, 40, 60, 90, 100], ["B", 10, 10, 20, 20, 30, 30]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_agg_different_separators() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE words (grp STRING, word STRING, seq INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO words VALUES
            ('A', 'hello', 1), ('A', 'world', 2), ('A', 'foo', 3),
            ('B', 'one', 1), ('B', 'two', 2)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                grp,
                STRING_AGG(word, ', ' ORDER BY seq) AS comma_sep,
                STRING_AGG(word, '-' ORDER BY seq) AS dash_sep,
                STRING_AGG(word, '' ORDER BY seq) AS no_sep,
                STRING_AGG(word, ' | ' ORDER BY seq) AS pipe_sep
            FROM words
            GROUP BY grp
            ORDER BY grp",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [
                "A",
                "hello, world, foo",
                "hello-world-foo",
                "helloworldfoo",
                "hello | world | foo"
            ],
            ["B", "one, two", "one-two", "onetwo", "one | two"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_row_number_different_orders() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE scores (player STRING, score INT64, game_date DATE)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO scores VALUES
            ('Alice', 100, DATE '2024-01-01'),
            ('Alice', 150, DATE '2024-01-02'),
            ('Alice', 120, DATE '2024-01-03'),
            ('Bob', 200, DATE '2024-01-01'),
            ('Bob', 180, DATE '2024-01-02')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                player,
                score,
                ROW_NUMBER() OVER (PARTITION BY player ORDER BY score ASC) AS rank_by_score_asc,
                ROW_NUMBER() OVER (PARTITION BY player ORDER BY score DESC) AS rank_by_score_desc,
                ROW_NUMBER() OVER (PARTITION BY player ORDER BY game_date ASC) AS rank_by_date_asc,
                ROW_NUMBER() OVER (PARTITION BY player ORDER BY game_date DESC) AS rank_by_date_desc
            FROM scores
            ORDER BY player, score",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 100, 1, 3, 1, 3],
            ["Alice", 120, 2, 2, 3, 1],
            ["Alice", 150, 3, 1, 2, 2],
            ["Bob", 180, 1, 2, 2, 1],
            ["Bob", 200, 2, 1, 1, 2],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_rank_dense_rank_same_partition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE rankings (team STRING, player STRING, points INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO rankings VALUES
            ('Red', 'A', 100),
            ('Red', 'B', 100),
            ('Red', 'C', 90),
            ('Red', 'D', 80),
            ('Blue', 'X', 50),
            ('Blue', 'Y', 50),
            ('Blue', 'Z', 50)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                team,
                player,
                points,
                RANK() OVER (PARTITION BY team ORDER BY points DESC) AS rnk,
                DENSE_RANK() OVER (PARTITION BY team ORDER BY points DESC) AS dense_rnk,
                ROW_NUMBER() OVER (PARTITION BY team ORDER BY points DESC) AS row_num
            FROM rankings
            ORDER BY team, points DESC, player",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Blue", "X", 50, 1, 1, 1],
            ["Blue", "Y", 50, 1, 1, 2],
            ["Blue", "Z", 50, 1, 1, 3],
            ["Red", "A", 100, 1, 1, 1],
            ["Red", "B", 100, 1, 1, 2],
            ["Red", "C", 90, 3, 2, 3],
            ["Red", "D", 80, 4, 3, 4],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_different_partition_by() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE transactions (region STRING, category STRING, amount INT64, tx_date DATE)",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO transactions VALUES
            ('East', 'A', 100, DATE '2024-01-01'),
            ('East', 'A', 150, DATE '2024-01-02'),
            ('East', 'B', 200, DATE '2024-01-01'),
            ('West', 'A', 120, DATE '2024-01-01'),
            ('West', 'B', 180, DATE '2024-01-01')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                region,
                category,
                amount,
                SUM(amount) OVER (PARTITION BY region) AS region_total,
                SUM(amount) OVER (PARTITION BY category) AS category_total,
                SUM(amount) OVER (PARTITION BY region, category) AS region_category_total,
                SUM(amount) OVER () AS grand_total
            FROM transactions
            ORDER BY region, category, amount",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", "A", 100, 450, 370, 250, 750],
            ["East", "A", 150, 450, 370, 250, 750],
            ["East", "B", 200, 450, 380, 200, 750],
            ["West", "A", 120, 300, 370, 120, 750],
            ["West", "B", 180, 300, 380, 180, 750],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_window_first_last_value_different_orders() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE events (grp STRING, event STRING, ts INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO events VALUES
            ('A', 'start', 1),
            ('A', 'middle', 2),
            ('A', 'end', 3),
            ('B', 'begin', 10),
            ('B', 'finish', 20)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                grp,
                event,
                ts,
                FIRST_VALUE(event) OVER (PARTITION BY grp ORDER BY ts ASC) AS first_by_ts_asc,
                LAST_VALUE(event) OVER (PARTITION BY grp ORDER BY ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_by_ts_asc,
                FIRST_VALUE(event) OVER (PARTITION BY grp ORDER BY ts DESC) AS first_by_ts_desc,
                LAST_VALUE(event) OVER (PARTITION BY grp ORDER BY ts DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_by_ts_desc
            FROM events
            ORDER BY grp, ts",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["A", "start", 1, "start", "end", "end", "start"],
            ["A", "middle", 2, "start", "end", "end", "start"],
            ["A", "end", 3, "start", "end", "end", "start"],
            ["B", "begin", 10, "begin", "finish", "finish", "begin"],
            ["B", "finish", 20, "begin", "finish", "finish", "begin"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_lead_lag_different_offsets() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE sequence (grp STRING, val INT64, pos INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO sequence VALUES
            ('A', 10, 1), ('A', 20, 2), ('A', 30, 3), ('A', 40, 4), ('A', 50, 5),
            ('B', 100, 1), ('B', 200, 2), ('B', 300, 3)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                grp,
                val,
                LAG(val, 1) OVER (PARTITION BY grp ORDER BY pos) AS lag_1,
                LAG(val, 2) OVER (PARTITION BY grp ORDER BY pos) AS lag_2,
                LEAD(val, 1) OVER (PARTITION BY grp ORDER BY pos) AS lead_1,
                LEAD(val, 2) OVER (PARTITION BY grp ORDER BY pos) AS lead_2
            FROM sequence
            ORDER BY grp, pos",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["A", 10, null, null, 20, 30],
            ["A", 20, 10, null, 30, 40],
            ["A", 30, 20, 10, 40, 50],
            ["A", 40, 30, 20, 50, null],
            ["A", 50, 40, 30, null, null],
            ["B", 100, null, null, 200, 300],
            ["B", 200, 100, null, 300, null],
            ["B", 300, 200, 100, null, null],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_expressions_in_different_positions() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE data (grp STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO data VALUES
            ('A', 10), ('A', 20), ('A', 30),
            ('B', 5), ('B', 15)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                grp,
                SUM(val) AS total,
                SUM(val) * 2 AS total_doubled,
                SUM(val) + 100 AS total_plus_100,
                SUM(val) - MIN(val) AS total_minus_min,
                MAX(val) - MIN(val) AS range
            FROM data
            GROUP BY grp
            ORDER BY grp",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["A", 60.0, 120.0, 160.0, 50.0, 20],
            ["B", 20.0, 40.0, 120.0, 15.0, 10],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_same_aggregate_different_filter() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE filtered (grp STRING, val INT64, category STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO filtered VALUES
            ('X', 10, 'A'), ('X', 20, 'B'), ('X', 30, 'A'), ('X', 40, 'B'),
            ('Y', 5, 'A'), ('Y', 15, 'B')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                grp,
                SUM(val) AS total,
                SUMIF(val, category = 'A') AS sum_a,
                SUMIF(val, category = 'B') AS sum_b,
                COUNTIF(category = 'A') AS count_a,
                COUNTIF(category = 'B') AS count_b
            FROM filtered
            GROUP BY grp
            ORDER BY grp",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["X", 100.0, 40.0, 60.0, 2, 2], ["Y", 20.0, 5.0, 15.0, 1, 1]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_aggregates_in_case() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE categorized (grp STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO categorized VALUES
            ('A', 10), ('A', 20), ('A', 30), ('A', 40), ('A', 50),
            ('B', 5), ('B', 10)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                grp,
                SUM(val) AS total,
                CASE
                    WHEN SUM(val) > 100 THEN 'high'
                    WHEN SUM(val) > 50 THEN 'medium'
                    ELSE 'low'
                END AS category,
                CASE
                    WHEN AVG(val) > 25 THEN 'above_avg'
                    ELSE 'below_avg'
                END AS avg_category,
                CASE
                    WHEN MAX(val) - MIN(val) > 30 THEN 'wide_range'
                    ELSE 'narrow_range'
                END AS range_category
            FROM categorized
            GROUP BY grp
            ORDER BY grp",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["A", 150.0, "high", "above_avg", "wide_range"],
            ["B", 15.0, "low", "below_avg", "narrow_range"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_subquery_with_different_aggregates() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE base (grp STRING, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO base VALUES
            ('A', 10), ('A', 20), ('A', 30),
            ('B', 100), ('B', 200)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                grp,
                total,
                avg_val,
                total - (SELECT SUM(val) FROM base) AS diff_from_grand_total,
                avg_val - (SELECT AVG(val) FROM base) AS diff_from_grand_avg
            FROM (
                SELECT grp, SUM(val) AS total, AVG(val) AS avg_val
                FROM base
                GROUP BY grp
            )
            ORDER BY grp",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["A", 60.0, 20.0, -300.0, -52.0],
            ["B", 300.0, 150.0, -60.0, 78.0],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_ctes_with_aggregates() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE source (region STRING, product STRING, sales INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO source VALUES
            ('North', 'A', 100), ('North', 'B', 150),
            ('South', 'A', 200), ('South', 'B', 250),
            ('East', 'A', 120), ('East', 'B', 180)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH region_totals AS (
                SELECT region, SUM(sales) AS region_total
                FROM source
                GROUP BY region
            ),
            product_totals AS (
                SELECT product, SUM(sales) AS product_total
                FROM source
                GROUP BY product
            ),
            grand_total AS (
                SELECT SUM(sales) AS total FROM source
            )
            SELECT
                r.region,
                r.region_total,
                (SELECT MAX(region_total) FROM region_totals) AS max_region,
                (SELECT MIN(region_total) FROM region_totals) AS min_region,
                g.total AS grand_total
            FROM region_totals r
            CROSS JOIN grand_total g
            ORDER BY r.region",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", 300.0, 450.0, 250.0, 1000.0],
            ["North", 250.0, 450.0, 250.0, 1000.0],
            ["South", 450.0, 450.0, 250.0, 1000.0],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_having_with_multiple_conditions() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE products (category STRING, price INT64, quantity INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO products VALUES
            ('A', 10, 5), ('A', 20, 3), ('A', 30, 2),
            ('B', 100, 1), ('B', 50, 2),
            ('C', 5, 10), ('C', 3, 20), ('C', 2, 30)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                category,
                SUM(price * quantity) AS revenue,
                AVG(price) AS avg_price,
                SUM(quantity) AS total_qty
            FROM products
            GROUP BY category
            HAVING SUM(price * quantity) > 100 AND AVG(price) < 50
            ORDER BY category",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["A", 170.0, 20.0, 10.0],
            ["C", 170.0, 3.3333333333333335, 60.0]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_combined_variations() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE complex_agg (grp STRING, val STRING, priority INT64, active INT64)",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO complex_agg VALUES
            ('X', 'a', 3, 1), ('X', 'b', 1, 1), ('X', 'c', 2, 0), ('X', 'a', 4, 1),
            ('Y', 'p', 1, 1), ('Y', 'q', 2, 0), ('Y', 'p', 3, 1)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                grp,
                ARRAY_AGG(val ORDER BY priority) AS by_priority,
                ARRAY_AGG(DISTINCT val ORDER BY val) AS distinct_sorted,
                ARRAY_AGG(val ORDER BY priority LIMIT 2) AS top_2_priority,
                ARRAY_AGG(DISTINCT val ORDER BY val LIMIT 2) AS top_2_distinct
            FROM complex_agg
            GROUP BY grp
            ORDER BY grp",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [
                "X",
                ["b", "c", "a", "a"],
                ["a", "b", "c"],
                ["b", "c"],
                ["a", "b"]
            ],
            ["Y", ["p", "q", "p"], ["p", "q"], ["p", "q"], ["p", "q"]],
        ]
    );
}
