use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;
use yachtsql_executor::YachtSQLSession;

fn setup_data(session: &YachtSQLSession, rt: &Runtime, scale: usize) {
    rt.block_on(async {
        session
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

        session
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

        session
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

            session
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

            session
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

            session
                .execute_sql(&format!(
                    "INSERT INTO items VALUES {}",
                    item_values.join(", ")
                ))
                .await
                .unwrap();
        }
    });
}

fn bench_query_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_performance");
    group.sample_size(20);

    for scale in [1000, 5000, 10000] {
        let rt = Runtime::new().unwrap();
        let session = YachtSQLSession::new();
        setup_data(&session, &rt, scale);

        group.bench_with_input(BenchmarkId::new("simple_select", scale), &scale, |b, _| {
            b.to_async(&rt).iter(|| async {
                session
                    .execute_sql("SELECT * FROM users WHERE status = 'active'")
                    .await
                    .unwrap()
            });
        });

        group.bench_with_input(BenchmarkId::new("join_query", scale), &scale, |b, _| {
            b.to_async(&rt)
                .iter(|| async {
                    session.execute_sql(
                        "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE u.country = 'US'"
                    ).await.unwrap()
                });
        });

        group.bench_with_input(BenchmarkId::new("aggregate_query", scale), &scale, |b, _| {
            b.to_async(&rt)
                .iter(|| async {
                    session.execute_sql(
                        "SELECT country, COUNT(*) as cnt, SUM(score) as total_score FROM users GROUP BY country"
                    ).await.unwrap()
                });
        });

        group.bench_with_input(BenchmarkId::new("complex_cte", scale), &scale, |b, _| {
            b.to_async(&rt)
                .iter(|| async {
                    session.execute_sql(
                        "WITH active_users AS (
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
                        LIMIT 5"
                    ).await.unwrap()
                });
        });
    }

    group.finish();
}

fn bench_filter_pushdown_effect(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_pushdown");
    group.sample_size(10);

    for scale in [10000, 25000, 50000] {
        let rt = Runtime::new().unwrap();
        let session = YachtSQLSession::new();
        setup_data(&session, &rt, scale);

        group.bench_with_input(BenchmarkId::new("filtered_join", scale), &scale, |b, _| {
            b.to_async(&rt).iter(|| async {
                session
                    .execute_sql(
                        "SELECT u.name, u.country, o.amount
                         FROM users u
                         JOIN orders o ON u.id = o.user_id
                         WHERE u.country = 'US' AND o.status = 'active' AND u.score < 10",
                    )
                    .await
                    .unwrap()
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_query_performance,
    bench_filter_pushdown_effect
);

criterion_main!(benches);
