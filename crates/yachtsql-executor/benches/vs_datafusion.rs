#![allow(clippy::needless_range_loop)]

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use tokio::runtime::Runtime;
use yachtsql_executor::YachtSQLSession;

const SCALES: &[usize] = &[1_000, 10_000, 100_000];

fn create_yachtsql_data(session: &YachtSQLSession, rt: &Runtime, scale: usize) {
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

        let statuses = ["active", "inactive", "pending"];
        let countries = ["US", "UK", "DE", "JP", "FR"];
        let regions = ["North", "South", "East", "West"];

        let batch_size = 5000;
        for batch_start in (1..=scale).step_by(batch_size) {
            let batch_end = std::cmp::min(batch_start + batch_size - 1, scale);

            let user_values: Vec<String> = (batch_start..=batch_end)
                .map(|i| {
                    format!(
                        "({}, 'user{}', '{}', '{}', {})",
                        i,
                        i,
                        statuses[i % statuses.len()],
                        countries[i % countries.len()],
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
                    format!(
                        "({}, {}, {}, '{}', '{}')",
                        i,
                        user_id,
                        (i * 10) % 1000,
                        statuses[i % statuses.len()],
                        regions[i % regions.len()]
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
    });
}

fn create_datafusion_data(ctx: &SessionContext, rt: &Runtime, scale: usize) {
    rt.block_on(async {
        let statuses = ["active", "inactive", "pending"];
        let countries = ["US", "UK", "DE", "JP", "FR"];
        let regions = ["North", "South", "East", "West"];

        let user_ids: Vec<i64> = (1..=scale as i64).collect();
        let user_names: Vec<String> = (1..=scale).map(|i| format!("user{}", i)).collect();
        let user_statuses: Vec<&str> = (1..=scale).map(|i| statuses[i % statuses.len()]).collect();
        let user_countries: Vec<&str> = (1..=scale)
            .map(|i| countries[i % countries.len()])
            .collect();
        let user_scores: Vec<i64> = (1..=scale).map(|i| (i % 100) as i64).collect();

        let user_schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("status", ArrowDataType::Utf8, false),
            Field::new("country", ArrowDataType::Utf8, false),
            Field::new("score", ArrowDataType::Int64, false),
        ]));

        let user_batch = RecordBatch::try_new(
            user_schema.clone(),
            vec![
                Arc::new(Int64Array::from(user_ids)),
                Arc::new(StringArray::from(user_names)),
                Arc::new(StringArray::from(user_statuses)),
                Arc::new(StringArray::from(user_countries)),
                Arc::new(Int64Array::from(user_scores)),
            ],
        )
        .unwrap();

        let user_table = MemTable::try_new(user_schema, vec![vec![user_batch]]).unwrap();
        ctx.register_table("users", Arc::new(user_table)).unwrap();

        let orders_per_user = 3;
        let total_orders = scale * orders_per_user;

        let order_ids: Vec<i64> = (1..=total_orders as i64).collect();
        let order_user_ids: Vec<i64> = (1..=total_orders)
            .map(|i| (((i - 1) / orders_per_user) + 1) as i64)
            .collect();
        let order_amounts: Vec<i64> = (1..=total_orders)
            .map(|i| ((i * 10) % 1000) as i64)
            .collect();
        let order_statuses: Vec<&str> = (1..=total_orders)
            .map(|i| statuses[i % statuses.len()])
            .collect();
        let order_regions: Vec<&str> = (1..=total_orders)
            .map(|i| regions[i % regions.len()])
            .collect();

        let order_schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("user_id", ArrowDataType::Int64, false),
            Field::new("amount", ArrowDataType::Int64, false),
            Field::new("status", ArrowDataType::Utf8, false),
            Field::new("region", ArrowDataType::Utf8, false),
        ]));

        let order_batch = RecordBatch::try_new(
            order_schema.clone(),
            vec![
                Arc::new(Int64Array::from(order_ids)),
                Arc::new(Int64Array::from(order_user_ids)),
                Arc::new(Int64Array::from(order_amounts)),
                Arc::new(StringArray::from(order_statuses)),
                Arc::new(StringArray::from(order_regions)),
            ],
        )
        .unwrap();

        let order_table = MemTable::try_new(order_schema, vec![vec![order_batch]]).unwrap();
        ctx.register_table("orders", Arc::new(order_table)).unwrap();
    });
}

fn bench_simple_select(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple_select");
    group.sample_size(20);

    for scale in SCALES {
        let rt = Runtime::new().unwrap();

        let session = YachtSQLSession::new();
        create_yachtsql_data(&session, &rt, *scale);

        group.bench_with_input(BenchmarkId::new("yachtsql", scale), scale, |b, _| {
            b.to_async(&rt).iter(|| async {
                session
                    .execute_sql("SELECT * FROM users WHERE status = 'active'")
                    .await
                    .unwrap()
            });
        });

        let ctx = SessionContext::new();
        create_datafusion_data(&ctx, &rt, *scale);

        group.bench_with_input(BenchmarkId::new("datafusion_raw", scale), scale, |b, _| {
            b.to_async(&rt).iter(|| async {
                ctx.sql("SELECT * FROM users WHERE status = 'active'")
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap()
            });
        });
    }

    group.finish();
}

fn bench_join_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_query");
    group.sample_size(20);

    for scale in SCALES {
        let rt = Runtime::new().unwrap();

        let session = YachtSQLSession::new();
        create_yachtsql_data(&session, &rt, *scale);

        group.bench_with_input(BenchmarkId::new("yachtsql", scale), scale, |b, _| {
            b.to_async(&rt).iter(|| async {
                session
                    .execute_sql(
                        "SELECT u.name, o.amount
                         FROM users u
                         JOIN orders o ON u.id = o.user_id
                         WHERE u.country = 'US'",
                    )
                    .await
                    .unwrap()
            });
        });

        let ctx = SessionContext::new();
        create_datafusion_data(&ctx, &rt, *scale);

        group.bench_with_input(BenchmarkId::new("datafusion_raw", scale), scale, |b, _| {
            b.to_async(&rt).iter(|| async {
                ctx.sql(
                    "SELECT u.name, o.amount
                     FROM users u
                     JOIN orders o ON u.id = o.user_id
                     WHERE u.country = 'US'",
                )
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
            });
        });
    }

    group.finish();
}

fn bench_aggregate_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregate_query");
    group.sample_size(20);

    for scale in SCALES {
        let rt = Runtime::new().unwrap();

        let session = YachtSQLSession::new();
        create_yachtsql_data(&session, &rt, *scale);

        group.bench_with_input(BenchmarkId::new("yachtsql", scale), scale, |b, _| {
            b.to_async(&rt).iter(|| async {
                session
                    .execute_sql(
                        "SELECT country, COUNT(*) as cnt, SUM(score) as total
                         FROM users
                         GROUP BY country",
                    )
                    .await
                    .unwrap()
            });
        });

        let ctx = SessionContext::new();
        create_datafusion_data(&ctx, &rt, *scale);

        group.bench_with_input(BenchmarkId::new("datafusion_raw", scale), scale, |b, _| {
            b.to_async(&rt).iter(|| async {
                ctx.sql(
                    "SELECT country, COUNT(*) as cnt, SUM(score) as total
                     FROM users
                     GROUP BY country",
                )
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_simple_select,
    bench_join_query,
    bench_aggregate_query
);

criterion_main!(benches);
