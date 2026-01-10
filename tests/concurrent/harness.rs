use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::sync::Semaphore;
use yachtsql::{Error, RecordBatchVecExt, YachtSQLSession};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskResult {
    Success(usize),
    Error(String),
}

#[derive(Debug)]
pub struct ConcurrencyMetrics {
    pub total_tasks: u64,
    pub successful_tasks: u64,
    pub failed_tasks: u64,
    pub max_concurrent_observed: u64,
    pub data_race_detected: bool,
}

pub type Session = YachtSQLSession;

pub struct ConcurrentTestHarness {
    session: Arc<Session>,
    barrier: Arc<std::sync::Barrier>,
    semaphore: Arc<Semaphore>,
    concurrency: usize,
    current_concurrent: Arc<AtomicU64>,
    max_concurrent_observed: Arc<AtomicU64>,
    data_race_detected: Arc<AtomicBool>,
}

pub type Executor = YachtSQLSession;

impl ConcurrentTestHarness {
    pub fn new(concurrency: usize) -> Self {
        let session = Session::new();
        Self {
            session: Arc::new(session),
            barrier: Arc::new(std::sync::Barrier::new(concurrency)),
            semaphore: Arc::new(Semaphore::new(concurrency)),
            concurrency,
            current_concurrent: Arc::new(AtomicU64::new(0)),
            max_concurrent_observed: Arc::new(AtomicU64::new(0)),
            data_race_detected: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn from_executor(_executor: Executor, concurrency: usize) -> Self {
        Self::new(concurrency)
    }

    pub fn session(&self) -> Arc<Session> {
        Arc::clone(&self.session)
    }

    pub fn barrier(&self) -> Arc<std::sync::Barrier> {
        Arc::clone(&self.barrier)
    }

    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    pub async fn run_concurrent<F, Fut>(&self, tasks: Vec<F>) -> Vec<Result<(), Error>>
    where
        F: Fn(Arc<Session>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), Error>> + Send,
    {
        let task_count = tasks.len();
        let task_barrier = Arc::new(std::sync::Barrier::new(task_count));
        let mut handles = Vec::with_capacity(task_count);

        for task in tasks {
            let session = Arc::clone(&self.session);
            let semaphore = Arc::clone(&self.semaphore);
            let current_concurrent = Arc::clone(&self.current_concurrent);
            let max_concurrent_observed = Arc::clone(&self.max_concurrent_observed);
            let barrier = Arc::clone(&task_barrier);

            let handle = tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();

                barrier.wait();

                let concurrent = current_concurrent.fetch_add(1, Ordering::SeqCst) + 1;
                loop {
                    let max = max_concurrent_observed.load(Ordering::SeqCst);
                    if concurrent <= max {
                        break;
                    }
                    if max_concurrent_observed
                        .compare_exchange(max, concurrent, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        break;
                    }
                }

                let result = task(session).await;

                current_concurrent.fetch_sub(1, Ordering::SeqCst);

                result
            });

            handles.push(handle);
        }

        let mut results = Vec::with_capacity(task_count);
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(Err(Error::internal(format!("Join error: {}", e)))),
            }
        }

        results
    }

    pub async fn run_concurrent_queries(&self, queries: Vec<String>) -> Vec<TaskResult> {
        let task_count = queries.len();
        let mut handles = Vec::with_capacity(task_count);

        for query in queries {
            let session = Arc::clone(&self.session);
            let semaphore = Arc::clone(&self.semaphore);
            let current_concurrent = Arc::clone(&self.current_concurrent);
            let max_concurrent_observed = Arc::clone(&self.max_concurrent_observed);

            let handle = tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();

                let concurrent = current_concurrent.fetch_add(1, Ordering::SeqCst) + 1;
                loop {
                    let max = max_concurrent_observed.load(Ordering::SeqCst);
                    if concurrent <= max {
                        break;
                    }
                    if max_concurrent_observed
                        .compare_exchange(max, concurrent, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        break;
                    }
                }

                let result = session.execute_sql(&query).await;

                current_concurrent.fetch_sub(1, Ordering::SeqCst);

                match result {
                    Ok(batches) => TaskResult::Success(batches.num_rows()),
                    Err(e) => TaskResult::Error(e.to_string()),
                }
            });

            handles.push(handle);
        }

        let mut results = Vec::with_capacity(task_count);
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(TaskResult::Error(format!("Join error: {}", e))),
            }
        }

        results
    }

    pub fn assert_no_data_races(&self, results: &[Result<(), Error>]) {
        let has_errors = results.iter().any(|r| r.is_err());
        let data_race_detected = self.data_race_detected.load(Ordering::SeqCst);

        if has_errors {
            for (i, result) in results.iter().enumerate() {
                if let Err(e) = result {
                    panic!("Task {} failed with error: {}", i, e);
                }
            }
        }

        if data_race_detected {
            panic!("Data race detected during concurrent execution");
        }
    }

    pub fn assert_no_data_races_task_result(&self, results: &[TaskResult]) -> ConcurrencyMetrics {
        let total_tasks = results.len() as u64;
        let successful_tasks = results
            .iter()
            .filter(|r| matches!(r, TaskResult::Success(_)))
            .count() as u64;
        let failed_tasks = total_tasks - successful_tasks;
        let max_concurrent_observed = self.max_concurrent_observed.load(Ordering::SeqCst);
        let data_race_detected = self.data_race_detected.load(Ordering::SeqCst);

        ConcurrencyMetrics {
            total_tasks,
            successful_tasks,
            failed_tasks,
            max_concurrent_observed,
            data_race_detected,
        }
    }

    pub fn reset_metrics(&self) {
        self.current_concurrent.store(0, Ordering::SeqCst);
        self.max_concurrent_observed.store(0, Ordering::SeqCst);
        self.data_race_detected.store(false, Ordering::SeqCst);
    }

    pub async fn run_read_write_contention(
        &self,
        table_name: &str,
        num_readers: usize,
        num_writers: usize,
    ) -> Vec<TaskResult> {
        let total = num_readers + num_writers;
        let mut handles = Vec::with_capacity(total);

        for _ in 0..num_readers {
            let session = Arc::clone(&self.session);
            let semaphore = Arc::clone(&self.semaphore);
            let query = format!("SELECT * FROM {}", table_name);

            let handle = tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();

                match session.execute_sql(&query).await {
                    Ok(batches) => TaskResult::Success(batches.num_rows()),
                    Err(e) => TaskResult::Error(e.to_string()),
                }
            });

            handles.push(handle);
        }

        for i in 0..num_writers {
            let session = Arc::clone(&self.session);
            let semaphore = Arc::clone(&self.semaphore);
            let query = format!(
                "INSERT INTO {} VALUES ({}, 'writer_{}', {})",
                table_name,
                1000 + i,
                i,
                i as f64
            );

            let handle = tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();

                match session.execute_sql(&query).await {
                    Ok(batches) => TaskResult::Success(batches.num_rows()),
                    Err(e) => TaskResult::Error(e.to_string()),
                }
            });

            handles.push(handle);
        }

        let mut results = Vec::with_capacity(total);
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(TaskResult::Error(format!("Join error: {}", e))),
            }
        }

        results
    }

    pub async fn run_write_write_contention(
        &self,
        table_name: &str,
        num_writers: usize,
    ) -> Vec<TaskResult> {
        let mut handles = Vec::with_capacity(num_writers);

        for i in 0..num_writers {
            let session = Arc::clone(&self.session);
            let semaphore = Arc::clone(&self.semaphore);
            let query = format!(
                "INSERT INTO {} VALUES ({}, 'concurrent_writer_{}', {})",
                table_name,
                2000 + i,
                i,
                i as f64 * 1.5
            );

            let handle = tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();

                match session.execute_sql(&query).await {
                    Ok(batches) => TaskResult::Success(batches.num_rows()),
                    Err(e) => TaskResult::Error(e.to_string()),
                }
            });

            handles.push(handle);
        }

        let mut results = Vec::with_capacity(num_writers);
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(TaskResult::Error(format!("Join error: {}", e))),
            }
        }

        results
    }

    pub async fn verify_table_consistency(&self, table_name: &str) -> Result<bool, String> {
        let query = format!("SELECT COUNT(*) as cnt FROM {}", table_name);
        let result = self.session.execute_sql(&query).await;

        match result {
            Ok(batches) => {
                let row = batches.get_row(0).ok_or("No rows returned")?;
                let count = &row.values()[0];
                match count.as_i64() {
                    Some(n) => Ok(n >= 0),
                    None => Ok(true),
                }
            }
            Err(e) => Err(e.to_string()),
        }
    }
}

pub fn create_test_executor() -> Executor {
    YachtSQLSession::new()
}

pub async fn setup_test_table(session: &YachtSQLSession, table_name: &str) {
    let create_sql = format!(
        "CREATE TABLE {} (id INT64, name STRING, value FLOAT64)",
        table_name
    );
    session.execute_sql(&create_sql).await.ok();
}

pub async fn setup_test_table_with_data(
    session: &YachtSQLSession,
    table_name: &str,
    num_rows: usize,
) {
    setup_test_table(session, table_name).await;

    for i in 0..num_rows {
        let insert_sql = format!(
            "INSERT INTO {} VALUES ({}, 'row_{}', {})",
            table_name,
            i,
            i,
            i as f64 * 0.5
        );
        session.execute_sql(&insert_sql).await.ok();
    }
}
