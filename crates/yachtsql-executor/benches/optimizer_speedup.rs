use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;
use yachtsql_executor::AsyncQueryExecutor;

fn setup_data(executor: &AsyncQueryExecutor, rt: &Runtime, scale: usize) {
    rt.block_on(async {
        executor
            .execute_sql(
                "CREATE TABLE users (
                    id INT64,
                    name STRING,
                    status STRING,
                    country STRING,
                    score INT64
                )",
            )
            .await
            .unwrap();

        executor
            .execute_sql(
                "CREATE TABLE orders (
                    id INT64,
                    user_id INT64,
                    amount INT64,
                    status STRING,
                    region STRING
                )",
            )
            .await
            .unwrap();

        executor
            .execute_sql(
                "CREATE TABLE items (
                    id INT64,
                    order_id INT64,
                    product STRING,
                    quantity INT64,
                    price INT64
                )",
            )
            .await
            .unwrap();

        let statuses = ["active", "inactive", "pending"];
        let countries = ["US", "UK", "DE", "JP", "FR"];
        let regions = ["North", "South", "East", "West"];
        let products = ["A", "B", "C", "D", "E"];

        let batch_size = 1000;
        for batch_start in (1..=scale).step_by(batch_size) {
            let batch_end = std::cmp::min(batch_start + batch_size - 1, scale);

            let user_values: Vec<String> = (batch_start..=batch_end)
                .map(|i| {
                    let status = statuses[i % statuses.len()];
                    let country = countries[i % countries.len()];
                    format!(
                        "({}, 'user{}', '{}', '{}', {})",
                        i,
                        i,
                        status,
                        country,
                        i % 100
                    )
                })
                .collect();

            executor
                .execute_sql(&format!(
                    "INSERT INTO users VALUES {}",
                    user_values.join(", ")
                ))
                .await
                .unwrap();
        }

        let orders_per_user = 3;
        let total_orders = scale * orders_per_user;
        for batch_start in (1..=total_orders).step_by(batch_size) {
            let batch_end = std::cmp::min(batch_start + batch_size - 1, total_orders);

            let order_values: Vec<String> = (batch_start..=batch_end)
                .map(|i| {
                    let user_id = ((i - 1) / orders_per_user) + 1;
                    let status = statuses[i % statuses.len()];
                    let region = regions[i % regions.len()];
                    format!(
                        "({}, {}, {}, '{}', '{}')",
                        i,
                        user_id,
                        (i * 10) % 1000,
                        status,
                        region
                    )
                })
                .collect();

            executor
                .execute_sql(&format!(
                    "INSERT INTO orders VALUES {}",
                    order_values.join(", ")
                ))
                .await
                .unwrap();
        }

        let items_per_order = 2;
        let total_items = total_orders * items_per_order;
        for batch_start in (1..=total_items).step_by(batch_size) {
            let batch_end = std::cmp::min(batch_start + batch_size - 1, total_items);

            let item_values: Vec<String> = (batch_start..=batch_end)
                .map(|i| {
                    let order_id = ((i - 1) / items_per_order) + 1;
                    let product = products[i % products.len()];
                    format!(
                        "({}, {}, '{}', {}, {})",
                        i,
                        order_id,
                        product,
                        (i % 5) + 1,
                        (i % 100) + 10
                    )
                })
                .collect();

            executor
                .execute_sql(&format!(
                    "INSERT INTO items VALUES {}",
                    item_values.join(", ")
                ))
                .await
                .unwrap();
        }
    });
}

fn disable_all_rules(executor: &AsyncQueryExecutor, rt: &Runtime) {
    rt.block_on(async {
        executor.clear_plan_cache();
        executor
            .execute_sql("SET OPTIMIZER_TRIVIAL_PREDICATE = false")
            .await
            .unwrap();
        executor
            .execute_sql("SET OPTIMIZER_EMPTY_PROPAGATION = false")
            .await
            .unwrap();
        executor
            .execute_sql("SET OPTIMIZER_FILTER_MERGING = false")
            .await
            .unwrap();
        executor
            .execute_sql("SET OPTIMIZER_PREDICATE_SIMPLIFICATION = false")
            .await
            .unwrap();
        executor
            .execute_sql("SET OPTIMIZER_PROJECT_MERGING = false")
            .await
            .unwrap();
        executor
            .execute_sql("SET OPTIMIZER_DISTINCT_ELIMINATION = false")
            .await
            .unwrap();
        executor
            .execute_sql("SET OPTIMIZER_CROSS_TO_HASH_JOIN = false")
            .await
            .unwrap();
        executor
            .execute_sql("SET OPTIMIZER_OUTER_TO_INNER_JOIN = false")
            .await
            .unwrap();
        executor
            .execute_sql("SET OPTIMIZER_FILTER_PUSHDOWN_AGGREGATE = false")
            .await
            .unwrap();
        executor
            .execute_sql("SET OPTIMIZER_FILTER_PUSHDOWN_JOIN = false")
            .await
            .unwrap();
        executor
            .execute_sql("SET OPTIMIZER_SORT_ELIMINATION = false")
            .await
            .unwrap();
        executor
            .execute_sql("SET OPTIMIZER_LIMIT_PUSHDOWN = false")
            .await
            .unwrap();
        executor
            .execute_sql("SET OPTIMIZER_TOPN_PUSHDOWN = false")
            .await
            .unwrap();
        executor
            .execute_sql("SET OPTIMIZER_PREDICATE_INFERENCE = false")
            .await
            .unwrap();
        executor
            .execute_sql("SET OPTIMIZER_SHORT_CIRCUIT_ORDERING = false")
            .await
            .unwrap();
        executor
            .execute_sql("SET OPTIMIZER_FILTER_PUSHDOWN_PROJECT = false")
            .await
            .unwrap();
        executor
            .execute_sql("SET OPTIMIZER_SORT_PUSHDOWN_PROJECT = false")
            .await
            .unwrap();
    });
}

fn enable_rules(executor: &AsyncQueryExecutor, rt: &Runtime, rules: &[&str]) {
    rt.block_on(async {
        executor.clear_plan_cache();
        for rule in rules {
            executor
                .execute_sql(&format!("SET {} = true", rule))
                .await
                .unwrap();
        }
    });
}

struct RuleBench {
    name: &'static str,
    query: &'static str,
    variants: &'static [(&'static str, &'static [&'static str])],
}

const RULES: &[RuleBench] = &[
    RuleBench {
        name: "trivial_predicate",
        query: "SELECT * FROM users WHERE 1 = 1 AND TRUE",
        variants: &[("off", &[]), ("on", &["OPTIMIZER_TRIVIAL_PREDICATE"])],
    },
    RuleBench {
        name: "empty_propagation",
        query: "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE FALSE",
        variants: &[
            ("off", &[]),
            ("trivial_only", &["OPTIMIZER_TRIVIAL_PREDICATE"]),
            (
                "trivial+empty",
                &["OPTIMIZER_TRIVIAL_PREDICATE", "OPTIMIZER_EMPTY_PROPAGATION"],
            ),
        ],
    },
    RuleBench {
        name: "filter_merging",
        query: "WITH base AS (SELECT * FROM users WHERE status = 'active') SELECT * FROM base WHERE country = 'US' AND score > 50",
        variants: &[
            ("off", &[]),
            ("pushdown_only", &["OPTIMIZER_FILTER_PUSHDOWN_PROJECT"]),
            (
                "pushdown+merging",
                &[
                    "OPTIMIZER_FILTER_PUSHDOWN_PROJECT",
                    "OPTIMIZER_FILTER_MERGING",
                ],
            ),
        ],
    },
    RuleBench {
        name: "predicate_simplification",
        query: "SELECT * FROM users WHERE (status = 'active' AND status = 'active') OR (score > 10 AND score > 10)",
        variants: &[
            ("off", &[]),
            ("on", &["OPTIMIZER_PREDICATE_SIMPLIFICATION"]),
        ],
    },
    RuleBench {
        name: "project_merging",
        query: "SELECT x FROM (SELECT y as x FROM (SELECT z as y FROM (SELECT name as z FROM users)))",
        variants: &[("off", &[]), ("on", &["OPTIMIZER_PROJECT_MERGING"])],
    },
    RuleBench {
        name: "distinct_elimination",
        query: "SELECT DISTINCT * FROM (SELECT DISTINCT * FROM (SELECT DISTINCT id, name FROM users))",
        variants: &[("off", &[]), ("on", &["OPTIMIZER_DISTINCT_ELIMINATION"])],
    },
    RuleBench {
        name: "sort_elimination",
        query: "WITH ordered AS (SELECT * FROM users ORDER BY score) SELECT * FROM ordered ORDER BY score",
        variants: &[
            ("off", &[]),
            ("pushdown_only", &["OPTIMIZER_SORT_PUSHDOWN_PROJECT"]),
            (
                "pushdown+elimination",
                &[
                    "OPTIMIZER_SORT_PUSHDOWN_PROJECT",
                    "OPTIMIZER_SORT_ELIMINATION",
                ],
            ),
        ],
    },
    RuleBench {
        name: "limit_pushdown",
        query: "SELECT name FROM (SELECT name FROM users) LIMIT 10",
        variants: &[("off", &[]), ("on", &["OPTIMIZER_LIMIT_PUSHDOWN"])],
    },
    RuleBench {
        name: "topn_pushdown",
        query: "SELECT * FROM (SELECT * FROM users UNION ALL SELECT * FROM users) ORDER BY score DESC LIMIT 10",
        variants: &[("off", &[]), ("on", &["OPTIMIZER_TOPN_PUSHDOWN"])],
    },
    RuleBench {
        name: "filter_pushdown_agg",
        query: "SELECT country, cnt FROM (SELECT country, COUNT(*) as cnt FROM users GROUP BY country) WHERE country = 'US'",
        variants: &[
            ("off", &[]),
            ("pushdown_proj_only", &["OPTIMIZER_FILTER_PUSHDOWN_PROJECT"]),
            (
                "pushdown_proj+agg",
                &[
                    "OPTIMIZER_FILTER_PUSHDOWN_PROJECT",
                    "OPTIMIZER_FILTER_PUSHDOWN_AGGREGATE",
                ],
            ),
        ],
    },
    RuleBench {
        name: "filter_pushdown_join",
        query: "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE u.country = 'US' AND o.status = 'active'",
        variants: &[("off", &[]), ("on", &["OPTIMIZER_FILTER_PUSHDOWN_JOIN"])],
    },
];

fn bench_rule_speedup(c: &mut Criterion) {
    let mut group = c.benchmark_group("rule_speedup");
    group.sample_size(20);

    let scale = 5000;

    for rule in RULES {
        let rt = Runtime::new().unwrap();
        let executor = AsyncQueryExecutor::new();
        setup_data(&executor, &rt, scale);

        for (variant_name, rules_to_enable) in rule.variants {
            disable_all_rules(&executor, &rt);
            enable_rules(&executor, &rt, rules_to_enable);
            group.bench_with_input(
                BenchmarkId::new(format!("{}/{}", rule.name, variant_name), scale),
                &scale,
                |b, _| {
                    b.to_async(&rt)
                        .iter(|| async { executor.execute_sql(rule.query).await.unwrap() });
                },
            );
        }
    }

    group.finish();
}

fn bench_all_rules_combined(c: &mut Criterion) {
    let mut group = c.benchmark_group("all_rules");
    group.sample_size(20);

    for scale in [1000, 5000, 10000] {
        let rt = Runtime::new().unwrap();
        let executor = AsyncQueryExecutor::new();
        setup_data(&executor, &rt, scale);

        let query = "
            WITH active_users AS (
                SELECT id, name, country
                FROM users
                WHERE status = 'active'
            ),
            user_orders AS (
                SELECT u.id, u.name, u.country, COUNT(*) as order_count, SUM(o.amount) as total
                FROM active_users u
                JOIN orders o ON u.id = o.user_id
                WHERE o.status = 'active'
                GROUP BY u.id, u.name, u.country
            )
            SELECT country, SUM(total) as revenue, COUNT(*) as customers
            FROM user_orders
            GROUP BY country
            ORDER BY revenue DESC
            LIMIT 5
        ";

        disable_all_rules(&executor, &rt);
        group.bench_with_input(BenchmarkId::new("all_off", scale), &scale, |b, _| {
            b.to_async(&rt)
                .iter(|| async { executor.execute_sql(query).await.unwrap() });
        });

        rt.block_on(async {
            executor.clear_plan_cache();
            executor
                .execute_sql("SET OPTIMIZER_TRIVIAL_PREDICATE = true")
                .await
                .unwrap();
            executor
                .execute_sql("SET OPTIMIZER_EMPTY_PROPAGATION = true")
                .await
                .unwrap();
            executor
                .execute_sql("SET OPTIMIZER_FILTER_MERGING = true")
                .await
                .unwrap();
            executor
                .execute_sql("SET OPTIMIZER_PREDICATE_SIMPLIFICATION = true")
                .await
                .unwrap();
            executor
                .execute_sql("SET OPTIMIZER_PROJECT_MERGING = true")
                .await
                .unwrap();
            executor
                .execute_sql("SET OPTIMIZER_DISTINCT_ELIMINATION = true")
                .await
                .unwrap();
            executor
                .execute_sql("SET OPTIMIZER_CROSS_TO_HASH_JOIN = true")
                .await
                .unwrap();
            executor
                .execute_sql("SET OPTIMIZER_OUTER_TO_INNER_JOIN = true")
                .await
                .unwrap();
            executor
                .execute_sql("SET OPTIMIZER_FILTER_PUSHDOWN_AGGREGATE = true")
                .await
                .unwrap();
            executor
                .execute_sql("SET OPTIMIZER_FILTER_PUSHDOWN_JOIN = true")
                .await
                .unwrap();
            executor
                .execute_sql("SET OPTIMIZER_SORT_ELIMINATION = true")
                .await
                .unwrap();
            executor
                .execute_sql("SET OPTIMIZER_LIMIT_PUSHDOWN = true")
                .await
                .unwrap();
            executor
                .execute_sql("SET OPTIMIZER_TOPN_PUSHDOWN = true")
                .await
                .unwrap();
            executor
                .execute_sql("SET OPTIMIZER_PREDICATE_INFERENCE = true")
                .await
                .unwrap();
            executor
                .execute_sql("SET OPTIMIZER_SHORT_CIRCUIT_ORDERING = true")
                .await
                .unwrap();
            executor
                .execute_sql("SET OPTIMIZER_FILTER_PUSHDOWN_PROJECT = true")
                .await
                .unwrap();
            executor
                .execute_sql("SET OPTIMIZER_SORT_PUSHDOWN_PROJECT = true")
                .await
                .unwrap();
        });

        group.bench_with_input(BenchmarkId::new("all_on", scale), &scale, |b, _| {
            b.to_async(&rt)
                .iter(|| async { executor.execute_sql(query).await.unwrap() });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_rule_speedup, bench_all_rules_combined);

criterion_main!(benches);
