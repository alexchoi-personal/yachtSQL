use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;
use yachtsql_executor::YachtSQLSession;

fn setup_ecommerce_schema(session: &YachtSQLSession, scale: usize, rt: &Runtime) {
    rt.block_on(async {
        session
            .execute_sql(
                "CREATE TABLE customers (
                    customer_id INT64,
                    name STRING,
                    email STRING,
                    signup_date DATE,
                    country STRING,
                    segment STRING
                )",
            )
            .await
            .unwrap();

        session
            .execute_sql(
                "CREATE TABLE products (
                    product_id INT64,
                    name STRING,
                    category STRING,
                    subcategory STRING,
                    price INT64,
                    cost INT64
                )",
            )
            .await
            .unwrap();

        session
            .execute_sql(
                "CREATE TABLE orders (
                    order_id INT64,
                    customer_id INT64,
                    order_date DATE,
                    status STRING,
                    shipping_country STRING
                )",
            )
            .await
            .unwrap();

        session
            .execute_sql(
                "CREATE TABLE order_items (
                    order_item_id INT64,
                    order_id INT64,
                    product_id INT64,
                    quantity INT64,
                    unit_price INT64,
                    discount_pct INT64
                )",
            )
            .await
            .unwrap();

        let countries = ["USA", "Canada", "UK", "Germany", "France", "Japan", "Australia"];
        let segments = ["Premium", "Standard", "New", "VIP"];
        let categories = [
            ("Electronics", "Computers", 1200, 800),
            ("Electronics", "Accessories", 50, 25),
            ("Electronics", "Phones", 800, 500),
            ("Furniture", "Seating", 350, 200),
            ("Furniture", "Desks", 600, 350),
            ("Office", "Supplies", 25, 10),
            ("Office", "Equipment", 150, 80),
        ];
        let statuses = ["Completed", "Completed", "Completed", "Pending", "Cancelled"];

        let batch_size = 500;
        for batch_start in (1..=scale).step_by(batch_size) {
            let batch_end = std::cmp::min(batch_start + batch_size - 1, scale);

            let customer_values: Vec<String> = (batch_start..=batch_end)
                .map(|i| {
                    let country = countries[i % countries.len()];
                    let segment = segments[i % segments.len()];
                    let month = (i % 12) + 1;
                    let day = (i % 28) + 1;
                    format!(
                        "({}, 'Customer{}', 'customer{}@email.com', DATE '2023-{:02}-{:02}', '{}', '{}')",
                        i, i, i, month, day, country, segment
                    )
                })
                .collect();

            session
                .execute_sql(&format!("INSERT INTO customers VALUES {}", customer_values.join(", ")))
                .await
                .unwrap();
        }

        let product_values: Vec<String> = categories
            .iter()
            .enumerate()
            .map(|(i, (cat, subcat, price, cost))| {
                format!(
                    "({}, 'Product{}', '{}', '{}', {}, {})",
                    i + 1, i + 1, cat, subcat, price, cost
                )
            })
            .collect();
        session
            .execute_sql(&format!("INSERT INTO products VALUES {}", product_values.join(", ")))
            .await
            .unwrap();

        let orders_per_customer = 3;
        let total_orders = scale * orders_per_customer;

        for batch_start in (1..=total_orders).step_by(batch_size) {
            let batch_end = std::cmp::min(batch_start + batch_size - 1, total_orders);

            let order_values: Vec<String> = (batch_start..=batch_end)
                .map(|i| {
                    let customer_id = ((i - 1) / orders_per_customer) + 1;
                    let country = countries[i % countries.len()];
                    let status = statuses[i % statuses.len()];
                    let month = (i % 12) + 1;
                    let day = (i % 28) + 1;
                    format!(
                        "({}, {}, DATE '2024-{:02}-{:02}', '{}', '{}')",
                        i, customer_id, month, day, status, country
                    )
                })
                .collect();

            session
                .execute_sql(&format!("INSERT INTO orders VALUES {}", order_values.join(", ")))
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
                    let product_id = (i % categories.len()) + 1;
                    let quantity = (i % 5) + 1;
                    let (_, _, price, _) = categories[product_id - 1];
                    let discount = (i % 4) * 5;
                    format!(
                        "({}, {}, {}, {}, {}, {})",
                        i, order_id, product_id, quantity, price, discount
                    )
                })
                .collect();

            session
                .execute_sql(&format!("INSERT INTO order_items VALUES {}", item_values.join(", ")))
                .await
                .unwrap();
        }
    });
}

fn bench_complex_analytical_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("analytical_queries");
    group.sample_size(10);

    for scale in [500, 1000, 2000].iter() {
        let rt = Runtime::new().unwrap();
        let session = YachtSQLSession::new();
        setup_ecommerce_schema(&session, *scale, &rt);

        group.bench_with_input(BenchmarkId::new("datafusion", scale), scale, |b, _| {
            b.to_async(&rt).iter(|| async {
                session
                    .execute_sql(
                        "WITH category_aggregates AS (
                            SELECT
                                p.category,
                                p.subcategory,
                                SUM(oi.quantity) AS units_sold,
                                SUM(oi.quantity * oi.unit_price) AS revenue,
                                SUM(oi.quantity * (oi.unit_price - p.cost)) AS profit
                            FROM order_items oi
                            JOIN products p ON oi.product_id = p.product_id
                            JOIN orders o ON oi.order_id = o.order_id
                            WHERE o.status = 'Completed'
                            GROUP BY p.category, p.subcategory
                        )
                        SELECT
                            category,
                            subcategory,
                            units_sold,
                            revenue,
                            profit,
                            ROUND(CAST(profit AS FLOAT64) * 100.0 / CAST(revenue AS FLOAT64), 2) AS margin_pct
                        FROM category_aggregates
                        ORDER BY revenue DESC",
                    )
                    .await
                    .unwrap()
            });
        });
    }

    group.finish();
}

fn bench_customer_lifetime_value(c: &mut Criterion) {
    let mut group = c.benchmark_group("customer_ltv");
    group.sample_size(10);

    for scale in [500, 1000, 2000].iter() {
        let rt = Runtime::new().unwrap();
        let session = YachtSQLSession::new();
        setup_ecommerce_schema(&session, *scale, &rt);

        group.bench_with_input(BenchmarkId::new("datafusion", scale), scale, |b, _| {
            b.to_async(&rt).iter(|| async {
                session
                    .execute_sql(
                        "WITH customer_orders AS (
                            SELECT
                                c.customer_id,
                                c.name,
                                c.segment,
                                c.signup_date,
                                COUNT(DISTINCT o.order_id) AS order_count,
                                SUM(oi.quantity * oi.unit_price) AS total_revenue,
                                MIN(o.order_date) AS first_order,
                                MAX(o.order_date) AS last_order
                            FROM customers c
                            LEFT JOIN orders o ON c.customer_id = o.customer_id AND o.status = 'Completed'
                            LEFT JOIN order_items oi ON o.order_id = oi.order_id
                            GROUP BY c.customer_id, c.name, c.segment, c.signup_date
                        )
                        SELECT
                            customer_id,
                            name,
                            segment,
                            order_count,
                            total_revenue,
                            CASE WHEN order_count > 0 THEN total_revenue / order_count ELSE 0 END AS avg_order_value
                        FROM customer_orders
                        ORDER BY total_revenue DESC NULLS LAST
                        LIMIT 100",
                    )
                    .await
                    .unwrap()
            });
        });
    }

    group.finish();
}

fn bench_revenue_with_window_functions(c: &mut Criterion) {
    let mut group = c.benchmark_group("window_functions");
    group.sample_size(10);

    for scale in [500, 1000, 2000].iter() {
        let rt = Runtime::new().unwrap();
        let session = YachtSQLSession::new();
        setup_ecommerce_schema(&session, *scale, &rt);

        group.bench_with_input(BenchmarkId::new("datafusion", scale), scale, |b, _| {
            b.to_async(&rt).iter(|| async {
                session
                    .execute_sql(
                        "WITH product_revenue AS (
                            SELECT
                                p.product_id,
                                p.name,
                                p.category,
                                SUM(oi.quantity * oi.unit_price) AS revenue
                            FROM order_items oi
                            JOIN products p ON oi.product_id = p.product_id
                            JOIN orders o ON oi.order_id = o.order_id
                            WHERE o.status = 'Completed'
                            GROUP BY p.product_id, p.name, p.category
                        ),
                        total AS (
                            SELECT SUM(revenue) AS total_revenue FROM product_revenue
                        )
                        SELECT
                            pr.name,
                            pr.category,
                            pr.revenue,
                            ROUND(CAST(pr.revenue AS FLOAT64) * 100.0 / CAST(t.total_revenue AS FLOAT64), 2) AS pct_of_total,
                            RANK() OVER (PARTITION BY pr.category ORDER BY pr.revenue DESC) AS category_rank
                        FROM product_revenue pr
                        CROSS JOIN total t
                        ORDER BY pr.revenue DESC",
                    )
                    .await
                    .unwrap()
            });
        });
    }

    group.finish();
}

fn bench_multi_cte_union(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_cte_union");
    group.sample_size(10);

    for scale in [500, 1000, 2000].iter() {
        let rt = Runtime::new().unwrap();
        let session = YachtSQLSession::new();
        setup_ecommerce_schema(&session, *scale, &rt);

        group.bench_with_input(BenchmarkId::new("datafusion", scale), scale, |b, _| {
            b.to_async(&rt).iter(|| async {
                session
                    .execute_sql(
                        "WITH
                        customer_metrics AS (
                            SELECT
                                c.customer_id,
                                c.segment,
                                COUNT(DISTINCT o.order_id) AS orders,
                                COALESCE(SUM(oi.quantity * oi.unit_price), 0) AS revenue
                            FROM customers c
                            LEFT JOIN orders o ON c.customer_id = o.customer_id AND o.status = 'Completed'
                            LEFT JOIN order_items oi ON o.order_id = oi.order_id
                            GROUP BY c.customer_id, c.segment
                        ),
                        segment_summary AS (
                            SELECT
                                segment,
                                COUNT(*) AS customer_count,
                                SUM(orders) AS total_orders,
                                SUM(revenue) AS total_revenue
                            FROM customer_metrics
                            GROUP BY segment
                        )
                        SELECT segment, customer_count, total_orders, total_revenue
                        FROM segment_summary
                        ORDER BY total_revenue DESC",
                    )
                    .await
                    .unwrap()
            });
        });
    }

    group.finish();
}

fn bench_large_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_join");
    group.sample_size(10);

    let rt = Runtime::new().unwrap();
    let session = YachtSQLSession::new();

    rt.block_on(async {
        session
            .execute_sql("CREATE TABLE bench_left (id INT64, value INT64)")
            .await
            .unwrap();
        session
            .execute_sql("CREATE TABLE bench_right (id INT64, data STRING)")
            .await
            .unwrap();

        for batch in 0..100 {
            let values: Vec<String> = (1..=1000)
                .map(|i| {
                    let id = batch * 1000 + i;
                    format!("({}, {})", id, id * 10)
                })
                .collect();
            session
                .execute_sql(&format!(
                    "INSERT INTO bench_left VALUES {}",
                    values.join(",")
                ))
                .await
                .unwrap();
        }
        for batch in 0..100 {
            let values: Vec<String> = (1..=1000)
                .map(|i| {
                    let id = batch * 1000 + i;
                    format!("({}, 'data{}')", id, id)
                })
                .collect();
            session
                .execute_sql(&format!(
                    "INSERT INTO bench_right VALUES {}",
                    values.join(",")
                ))
                .await
                .unwrap();
        }
    });

    group.bench_function("datafusion_100k_join", |b| {
        b.to_async(&rt).iter(|| async {
            session
                .execute_sql(
                    "SELECT l.id, l.value, r.data FROM bench_left l JOIN bench_right r ON l.id = r.id",
                )
                .await
                .unwrap()
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_large_join,
    bench_complex_analytical_queries,
    bench_customer_lifetime_value,
    bench_revenue_with_window_functions,
    bench_multi_cte_union
);
criterion_main!(benches);
