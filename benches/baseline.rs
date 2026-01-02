use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use tokio::runtime::Runtime;
use yachtsql::{YachtSQLEngine, YachtSQLSession};

fn create_session() -> YachtSQLSession {
    YachtSQLEngine::new().create_session()
}

fn setup_table(session: &YachtSQLSession, rt: &Runtime, row_count: usize) {
    rt.block_on(async {
        session
            .execute_sql(
                "CREATE TABLE test_data (id INT64, name STRING, value FLOAT64, category STRING)",
            )
            .await
            .unwrap();

        let batch_size = 500;
        for batch_start in (1..=row_count).step_by(batch_size) {
            let batch_end = std::cmp::min(batch_start + batch_size - 1, row_count);
            let values: Vec<String> = (batch_start..=batch_end)
                .map(|i| format!("({}, 'name{}', {}.{}, 'cat{}')", i, i, i, i % 100, i % 10))
                .collect();
            session
                .execute_sql(&format!(
                    "INSERT INTO test_data VALUES {}",
                    values.join(", ")
                ))
                .await
                .unwrap();
        }
    });
}

fn setup_join_tables(session: &YachtSQLSession, rt: &Runtime, left_rows: usize, right_rows: usize) {
    rt.block_on(async {
        session
            .execute_sql("CREATE TABLE left_table (id INT64, value INT64)")
            .await
            .unwrap();
        session
            .execute_sql("CREATE TABLE right_table (id INT64, data STRING)")
            .await
            .unwrap();

        let batch_size = 500;
        for batch_start in (1..=left_rows).step_by(batch_size) {
            let batch_end = std::cmp::min(batch_start + batch_size - 1, left_rows);
            let values: Vec<String> = (batch_start..=batch_end)
                .map(|i| format!("({}, {})", i, i * 10))
                .collect();
            session
                .execute_sql(&format!(
                    "INSERT INTO left_table VALUES {}",
                    values.join(", ")
                ))
                .await
                .unwrap();
        }

        for batch_start in (1..=right_rows).step_by(batch_size) {
            let batch_end = std::cmp::min(batch_start + batch_size - 1, right_rows);
            let values: Vec<String> = (batch_start..=batch_end)
                .map(|i| format!("({}, 'data{}')", i, i))
                .collect();
            session
                .execute_sql(&format!(
                    "INSERT INTO right_table VALUES {}",
                    values.join(", ")
                ))
                .await
                .unwrap();
        }
    });
}

fn calculate_percentiles(mut latencies: Vec<Duration>) -> (Duration, Duration, Duration) {
    latencies.sort();
    let len = latencies.len();
    let p50 = latencies[len * 50 / 100];
    let p95 = latencies[len * 95 / 100];
    let p99 = latencies[len * 99 / 100];
    (p50, p95, p99)
}

fn bench_concurrent_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_reads");
    group.sample_size(20);

    for num_readers in [2, 4, 8] {
        let rt = Runtime::new().unwrap();
        let session = create_session();
        setup_table(&session, &rt, 10_000);

        group.bench_with_input(
            BenchmarkId::new("readers", num_readers),
            &num_readers,
            |b, &readers| {
                b.iter(|| {
                    let handles: Vec<_> = (0..readers)
                        .map(|_| {
                            let s = create_session();
                            rt.block_on(async {
                                s.execute_sql("CREATE TABLE test_data (id INT64, name STRING, value FLOAT64, category STRING)")
                                    .await
                                    .unwrap();
                                for batch in (1..=10_000).step_by(500) {
                                    let end = std::cmp::min(batch + 499, 10_000);
                                    let values: Vec<String> = (batch..=end)
                                        .map(|i| format!("({}, 'name{}', {}.{}, 'cat{}')", i, i, i, i % 100, i % 10))
                                        .collect();
                                    s.execute_sql(&format!("INSERT INTO test_data VALUES {}", values.join(", ")))
                                        .await
                                        .unwrap();
                                }
                            });
                            std::thread::spawn(move || {
                                let rt = Runtime::new().unwrap();
                                let start = Instant::now();
                                rt.block_on(async {
                                    for _ in 0..10 {
                                        black_box(
                                            s.execute_sql("SELECT * FROM test_data WHERE id > 5000")
                                                .await
                                                .unwrap(),
                                        );
                                    }
                                });
                                start.elapsed()
                            })
                        })
                        .collect();

                    let latencies: Vec<Duration> = handles.into_iter().map(|h| h.join().unwrap()).collect();
                    let (p50, p95, p99) = calculate_percentiles(latencies);
                    black_box((p50, p95, p99))
                })
            },
        );
    }

    group.finish();
}

fn bench_concurrent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_writes");
    group.sample_size(20);

    for num_writers in [2, 4] {
        group.bench_with_input(
            BenchmarkId::new("writers", num_writers),
            &num_writers,
            |b, &writers| {
                b.iter(|| {
                    let counter = Arc::new(AtomicU64::new(0));
                    let handles: Vec<_> = (0..writers)
                        .map(|_| {
                            let cnt = Arc::clone(&counter);
                            std::thread::spawn(move || {
                                let rt = Runtime::new().unwrap();
                                let s = create_session();
                                rt.block_on(async {
                                    s.execute_sql("CREATE TABLE test_data (id INT64, name STRING, value FLOAT64, category STRING)")
                                        .await
                                        .unwrap();
                                });
                                let start = Instant::now();
                                rt.block_on(async {
                                    for _ in 0..100 {
                                        let id = cnt.fetch_add(1, Ordering::Relaxed);
                                        s.execute_sql(&format!(
                                            "INSERT INTO test_data VALUES ({}, 'name{}', {}.5, 'cat1')",
                                            id, id, id
                                        ))
                                        .await
                                        .unwrap();
                                    }
                                });
                                start.elapsed()
                            })
                        })
                        .collect();

                    let latencies: Vec<Duration> = handles.into_iter().map(|h| h.join().unwrap()).collect();
                    let (p50, p95, p99) = calculate_percentiles(latencies);
                    black_box((p50, p95, p99))
                })
            },
        );
    }

    group.finish();
}

fn bench_query_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_throughput");
    group.sample_size(50);
    group.throughput(Throughput::Elements(1));

    let rt = Runtime::new().unwrap();
    let session = create_session();
    setup_table(&session, &rt, 1_000);

    group.bench_function("simple_select_qps", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                session
                    .execute_sql("SELECT id, name FROM test_data WHERE id < 100")
                    .await
                    .unwrap(),
            )
        })
    });

    group.bench_function("aggregation_qps", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                session
                    .execute_sql(
                        "SELECT category, COUNT(*), AVG(value) FROM test_data GROUP BY category",
                    )
                    .await
                    .unwrap(),
            )
        })
    });

    group.finish();
}

fn bench_dml_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("dml_latency");
    group.sample_size(10);

    group.bench_function("insert_1k_rows", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let rt = Runtime::new().unwrap();
                let session = create_session();
                rt.block_on(async {
                    session
                        .execute_sql(
                            "CREATE TABLE insert_test (id INT64, name STRING, value FLOAT64)",
                        )
                        .await
                        .unwrap();
                });

                let values: Vec<String> = (1..=1000)
                    .map(|i| format!("({}, 'name{}', {}.5)", i, i, i))
                    .collect();

                let start = Instant::now();
                rt.block_on(async {
                    session
                        .execute_sql(&format!(
                            "INSERT INTO insert_test VALUES {}",
                            values.join(", ")
                        ))
                        .await
                        .unwrap();
                });
                total += start.elapsed();
            }
            total
        })
    });

    group.bench_function("update_1k_rows", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let rt = Runtime::new().unwrap();
                let session = create_session();
                rt.block_on(async {
                    session
                        .execute_sql(
                            "CREATE TABLE update_test (id INT64, name STRING, value FLOAT64)",
                        )
                        .await
                        .unwrap();
                    let values: Vec<String> = (1..=1000)
                        .map(|i| format!("({}, 'name{}', {}.5)", i, i, i))
                        .collect();
                    session
                        .execute_sql(&format!(
                            "INSERT INTO update_test VALUES {}",
                            values.join(", ")
                        ))
                        .await
                        .unwrap();
                });

                let start = Instant::now();
                rt.block_on(async {
                    session
                        .execute_sql("UPDATE update_test SET value = value + 1.0 WHERE id <= 1000")
                        .await
                        .unwrap();
                });
                total += start.elapsed();
            }
            total
        })
    });

    group.bench_function("delete_1k_rows", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let rt = Runtime::new().unwrap();
                let session = create_session();
                rt.block_on(async {
                    session
                        .execute_sql(
                            "CREATE TABLE delete_test (id INT64, name STRING, value FLOAT64)",
                        )
                        .await
                        .unwrap();
                    let values: Vec<String> = (1..=1000)
                        .map(|i| format!("({}, 'name{}', {}.5)", i, i, i))
                        .collect();
                    session
                        .execute_sql(&format!(
                            "INSERT INTO delete_test VALUES {}",
                            values.join(", ")
                        ))
                        .await
                        .unwrap();
                });

                let start = Instant::now();
                rt.block_on(async {
                    session
                        .execute_sql("DELETE FROM delete_test WHERE id <= 1000")
                        .await
                        .unwrap();
                });
                total += start.elapsed();
            }
            total
        })
    });

    group.finish();
}

fn bench_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_usage");
    group.sample_size(10);

    for row_count in [100_000, 1_000_000] {
        group.bench_with_input(
            BenchmarkId::new("rows", row_count),
            &row_count,
            |b, &rows| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let rt = Runtime::new().unwrap();
                        let session = create_session();

                        let start = Instant::now();
                        rt.block_on(async {
                            session
                                .execute_sql("CREATE TABLE memory_test (id INT64, name STRING, value FLOAT64, category STRING)")
                                .await
                                .unwrap();

                            let batch_size = 1000;
                            for batch_start in (1..=rows).step_by(batch_size) {
                                let batch_end = std::cmp::min(batch_start + batch_size - 1, rows);
                                let values: Vec<String> = (batch_start..=batch_end)
                                    .map(|i| format!("({}, 'name{}', {}.{}, 'cat{}')", i, i, i, i % 100, i % 10))
                                    .collect();
                                session
                                    .execute_sql(&format!("INSERT INTO memory_test VALUES {}", values.join(", ")))
                                    .await
                                    .unwrap();
                            }

                            black_box(
                                session
                                    .execute_sql("SELECT COUNT(*) FROM memory_test")
                                    .await
                                    .unwrap(),
                            );
                        });
                        total += start.elapsed();
                    }
                    total
                })
            },
        );
    }

    group.finish();
}

fn bench_concurrent_mixed_workloads(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_mixed");
    group.sample_size(10);

    group.bench_function("4_writers_p99", |b| {
        b.iter(|| {
            let counter = Arc::new(AtomicU64::new(0));
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    let cnt = Arc::clone(&counter);
                    std::thread::spawn(move || {
                        let rt = Runtime::new().unwrap();
                        let s = create_session();
                        rt.block_on(async {
                            s.execute_sql(
                                "CREATE TABLE mixed_test (id INT64, name STRING, value FLOAT64)",
                            )
                            .await
                            .unwrap();
                        });
                        let start = Instant::now();
                        rt.block_on(async {
                            for _ in 0..50 {
                                let id = cnt.fetch_add(1, Ordering::Relaxed);
                                s.execute_sql(&format!(
                                    "INSERT INTO mixed_test VALUES ({}, 'name{}', {}.5)",
                                    id, id, id
                                ))
                                .await
                                .unwrap();
                            }
                        });
                        start.elapsed()
                    })
                })
                .collect();

            let latencies: Vec<Duration> = handles.into_iter().map(|h| h.join().unwrap()).collect();
            let (_, _, p99) = calculate_percentiles(latencies);
            black_box(p99)
        })
    });

    group.bench_function("10_readers_1_writer_p99", |b| {
        b.iter(|| {
            let counter = Arc::new(AtomicU64::new(0));
            let mut handles = Vec::new();

            for _ in 0..10 {
                let handle = std::thread::spawn(move || {
                    let rt = Runtime::new().unwrap();
                    let s = create_session();
                    rt.block_on(async {
                        s.execute_sql("CREATE TABLE reader_test (id INT64, value INT64)")
                            .await
                            .unwrap();
                        let values: Vec<String> =
                            (1..=1000).map(|i| format!("({}, {})", i, i)).collect();
                        s.execute_sql(&format!(
                            "INSERT INTO reader_test VALUES {}",
                            values.join(", ")
                        ))
                        .await
                        .unwrap();
                    });
                    let start = Instant::now();
                    rt.block_on(async {
                        for _ in 0..20 {
                            black_box(
                                s.execute_sql("SELECT * FROM reader_test WHERE id > 500")
                                    .await
                                    .unwrap(),
                            );
                        }
                    });
                    start.elapsed()
                });
                handles.push(handle);
            }

            let cnt = Arc::clone(&counter);
            let writer_handle = std::thread::spawn(move || {
                let rt = Runtime::new().unwrap();
                let s = create_session();
                rt.block_on(async {
                    s.execute_sql("CREATE TABLE writer_test (id INT64, value INT64)")
                        .await
                        .unwrap();
                });
                let start = Instant::now();
                rt.block_on(async {
                    for _ in 0..50 {
                        let id = cnt.fetch_add(1, Ordering::Relaxed);
                        s.execute_sql(&format!("INSERT INTO writer_test VALUES ({}, {})", id, id))
                            .await
                            .unwrap();
                    }
                });
                start.elapsed()
            });
            handles.push(writer_handle);

            let latencies: Vec<Duration> = handles.into_iter().map(|h| h.join().unwrap()).collect();
            let (_, _, p99) = calculate_percentiles(latencies);
            black_box(p99)
        })
    });

    group.finish();
}

fn bench_cold_warm_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_performance");
    group.sample_size(20);

    group.bench_function("cold_start_first_query", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let rt = Runtime::new().unwrap();
                let session = create_session();
                rt.block_on(async {
                    session
                        .execute_sql("CREATE TABLE cold_test (id INT64, value STRING)")
                        .await
                        .unwrap();
                    let values: Vec<String> = (1..=5000)
                        .map(|i| format!("({}, 'value{}')", i, i))
                        .collect();
                    session
                        .execute_sql(&format!(
                            "INSERT INTO cold_test VALUES {}",
                            values.join(", ")
                        ))
                        .await
                        .unwrap();
                });

                let start = Instant::now();
                rt.block_on(async {
                    black_box(
                        session
                            .execute_sql("SELECT * FROM cold_test WHERE id > 2500")
                            .await
                            .unwrap(),
                    );
                });
                total += start.elapsed();
            }
            total
        })
    });

    group.bench_function("warm_cache_query", |b| {
        let rt = Runtime::new().unwrap();
        let session = create_session();
        rt.block_on(async {
            session
                .execute_sql("CREATE TABLE warm_test (id INT64, value STRING)")
                .await
                .unwrap();
            let values: Vec<String> = (1..=5000)
                .map(|i| format!("({}, 'value{}')", i, i))
                .collect();
            session
                .execute_sql(&format!(
                    "INSERT INTO warm_test VALUES {}",
                    values.join(", ")
                ))
                .await
                .unwrap();
            session
                .execute_sql("SELECT * FROM warm_test WHERE id > 2500")
                .await
                .unwrap();
        });

        b.to_async(&rt).iter(|| async {
            black_box(
                session
                    .execute_sql("SELECT * FROM warm_test WHERE id > 2500")
                    .await
                    .unwrap(),
            )
        })
    });

    group.finish();
}

fn bench_join_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_performance");
    group.sample_size(10);

    group.bench_function("hash_join_1m_rows", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let rt = Runtime::new().unwrap();
                let session = create_session();
                setup_join_tables(&session, &rt, 100_000, 100_000);

                let start = Instant::now();
                rt.block_on(async {
                    let result = session
                        .execute_sql(
                            "SELECT l.id, l.value, r.data FROM left_table l JOIN right_table r ON l.id = r.id",
                        )
                        .await
                        .unwrap();
                    assert_eq!(result.row_count(), 100_000);
                    black_box(result);
                });
                total += start.elapsed();
            }
            total
        })
    });

    group.bench_function("nested_loop_join_10k_rows", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let rt = Runtime::new().unwrap();
                let session = create_session();
                setup_join_tables(&session, &rt, 100, 100);

                let start = Instant::now();
                rt.block_on(async {
                    let result = session
                        .execute_sql(
                            "SELECT l.id, l.value, r.data FROM left_table l CROSS JOIN right_table r",
                        )
                        .await
                        .unwrap();
                    assert_eq!(result.row_count(), 10_000);
                    black_box(result);
                });
                total += start.elapsed();
            }
            total
        })
    });

    group.bench_function("multi_table_join", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let rt = Runtime::new().unwrap();
                let session = create_session();
                rt.block_on(async {
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

                    let order_values: Vec<String> = (1..=1000)
                        .map(|i| format!("({}, {}, {}.50)", i, i % 100 + 1, i * 10))
                        .collect();
                    session
                        .execute_sql(&format!("INSERT INTO orders VALUES {}", order_values.join(", ")))
                        .await
                        .unwrap();

                    let customer_values: Vec<String> = (1..=100)
                        .map(|i| format!("({}, 'Customer{}')", i, i))
                        .collect();
                    session
                        .execute_sql(&format!("INSERT INTO customers VALUES {}", customer_values.join(", ")))
                        .await
                        .unwrap();

                    let product_values: Vec<String> = (1..=2000)
                        .map(|i| format!("({}, {}, 'Product{}')", i, i % 1000 + 1, i))
                        .collect();
                    session
                        .execute_sql(&format!("INSERT INTO products VALUES {}", product_values.join(", ")))
                        .await
                        .unwrap();
                });

                let start = Instant::now();
                rt.block_on(async {
                    let result = session
                        .execute_sql(
                            "SELECT c.name, o.order_id, p.name as product_name, o.amount
                             FROM customers c
                             JOIN orders o ON c.customer_id = o.customer_id
                             JOIN products p ON o.order_id = p.order_id",
                        )
                        .await
                        .unwrap();
                    black_box(result);
                });
                total += start.elapsed();
            }
            total
        })
    });

    group.finish();
}

fn bench_aggregation_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregation_performance");
    group.sample_size(20);

    let rt = Runtime::new().unwrap();
    let session = create_session();
    setup_table(&session, &rt, 100_000);

    group.bench_function("group_by_count", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                session
                    .execute_sql("SELECT category, COUNT(*) FROM test_data GROUP BY category")
                    .await
                    .unwrap(),
            )
        })
    });

    group.bench_function("group_by_sum_avg", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                session
                    .execute_sql(
                        "SELECT category, SUM(value), AVG(value) FROM test_data GROUP BY category",
                    )
                    .await
                    .unwrap(),
            )
        })
    });

    group.bench_function("group_by_with_having", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                session
                    .execute_sql(
                        "SELECT category, COUNT(*) as cnt FROM test_data GROUP BY category HAVING COUNT(*) > 5000",
                    )
                    .await
                    .unwrap(),
            )
        })
    });

    group.finish();
}

fn bench_window_functions(c: &mut Criterion) {
    let mut group = c.benchmark_group("window_functions");
    group.sample_size(10);

    let rt = Runtime::new().unwrap();
    let session = create_session();
    setup_table(&session, &rt, 10_000);

    group.bench_function("row_number", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                session
                    .execute_sql(
                        "SELECT id, category, ROW_NUMBER() OVER (PARTITION BY category ORDER BY id) as rn FROM test_data",
                    )
                    .await
                    .unwrap(),
            )
        })
    });

    group.bench_function("running_sum", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                session
                    .execute_sql(
                        "SELECT id, value, SUM(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_sum FROM test_data WHERE id <= 1000",
                    )
                    .await
                    .unwrap(),
            )
        })
    });

    group.bench_function("rank_dense_rank", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                session
                    .execute_sql(
                        "SELECT id, category, value, RANK() OVER (PARTITION BY category ORDER BY value DESC) as rnk, DENSE_RANK() OVER (PARTITION BY category ORDER BY value DESC) as dense_rnk FROM test_data WHERE id <= 1000",
                    )
                    .await
                    .unwrap(),
            )
        })
    });

    group.finish();
}

fn bench_subquery_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("subquery_performance");
    group.sample_size(20);

    let rt = Runtime::new().unwrap();
    let session = create_session();
    setup_table(&session, &rt, 10_000);

    group.bench_function("scalar_subquery", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                session
                    .execute_sql(
                        "SELECT id, name, value, (SELECT AVG(value) FROM test_data) as avg_value FROM test_data WHERE id <= 100",
                    )
                    .await
                    .unwrap(),
            )
        })
    });

    group.bench_function("in_subquery", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                session
                    .execute_sql(
                        "SELECT * FROM test_data WHERE category IN (SELECT DISTINCT category FROM test_data WHERE id < 100)",
                    )
                    .await
                    .unwrap(),
            )
        })
    });

    group.bench_function("exists_subquery", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                session
                    .execute_sql(
                        "SELECT * FROM test_data t1 WHERE EXISTS (SELECT 1 FROM test_data t2 WHERE t2.category = t1.category AND t2.id < 100) AND t1.id <= 1000",
                    )
                    .await
                    .unwrap(),
            )
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_concurrent_reads,
    bench_concurrent_writes,
    bench_query_throughput,
    bench_dml_latency,
    bench_memory_usage,
    bench_concurrent_mixed_workloads,
    bench_cold_warm_cache,
    bench_join_performance,
    bench_aggregation_performance,
    bench_window_functions,
    bench_subquery_performance,
);
criterion_main!(benches);
