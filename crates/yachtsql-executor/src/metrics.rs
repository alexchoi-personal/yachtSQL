use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub struct QueryMetrics {
    pub query_count: AtomicU64,
    pub total_execution_time_us: AtomicU64,
    pub slow_query_count: AtomicU64,
    pub error_count: AtomicU64,
    slow_query_threshold_ms: u64,
}

impl QueryMetrics {
    pub fn new() -> Self {
        Self {
            query_count: AtomicU64::new(0),
            total_execution_time_us: AtomicU64::new(0),
            slow_query_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            slow_query_threshold_ms: 1000,
        }
    }

    pub fn with_slow_query_threshold(mut self, threshold_ms: u64) -> Self {
        self.slow_query_threshold_ms = threshold_ms;
        self
    }

    pub fn record_query(&self, duration: Duration, is_error: bool) {
        self.query_count.fetch_add(1, Ordering::Relaxed);
        let micros = duration.as_micros() as u64;
        self.total_execution_time_us
            .fetch_add(micros, Ordering::Relaxed);

        if is_error {
            self.error_count.fetch_add(1, Ordering::Relaxed);
        }

        if duration.as_millis() as u64 >= self.slow_query_threshold_ms {
            self.slow_query_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn get_query_count(&self) -> u64 {
        self.query_count.load(Ordering::Relaxed)
    }

    pub fn get_total_execution_time_us(&self) -> u64 {
        self.total_execution_time_us.load(Ordering::Relaxed)
    }

    pub fn get_slow_query_count(&self) -> u64 {
        self.slow_query_count.load(Ordering::Relaxed)
    }

    pub fn get_error_count(&self) -> u64 {
        self.error_count.load(Ordering::Relaxed)
    }

    pub fn get_average_execution_time_us(&self) -> u64 {
        let count = self.get_query_count();
        if count == 0 {
            return 0;
        }
        self.get_total_execution_time_us() / count
    }

    pub fn reset(&self) {
        self.query_count.store(0, Ordering::Relaxed);
        self.total_execution_time_us.store(0, Ordering::Relaxed);
        self.slow_query_count.store(0, Ordering::Relaxed);
        self.error_count.store(0, Ordering::Relaxed);
    }
}

impl Default for QueryMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_new() {
        let metrics = QueryMetrics::new();
        assert_eq!(metrics.get_query_count(), 0);
        assert_eq!(metrics.get_total_execution_time_us(), 0);
        assert_eq!(metrics.get_slow_query_count(), 0);
        assert_eq!(metrics.get_error_count(), 0);
    }

    #[test]
    fn test_record_query() {
        let metrics = QueryMetrics::new();
        metrics.record_query(Duration::from_millis(100), false);
        assert_eq!(metrics.get_query_count(), 1);
        assert_eq!(metrics.get_total_execution_time_us(), 100_000);
        assert_eq!(metrics.get_slow_query_count(), 0);
        assert_eq!(metrics.get_error_count(), 0);
    }

    #[test]
    fn test_record_slow_query() {
        let metrics = QueryMetrics::new();
        metrics.record_query(Duration::from_millis(1500), false);
        assert_eq!(metrics.get_query_count(), 1);
        assert_eq!(metrics.get_slow_query_count(), 1);
    }

    #[test]
    fn test_record_error() {
        let metrics = QueryMetrics::new();
        metrics.record_query(Duration::from_millis(50), true);
        assert_eq!(metrics.get_query_count(), 1);
        assert_eq!(metrics.get_error_count(), 1);
    }

    #[test]
    fn test_average_execution_time() {
        let metrics = QueryMetrics::new();
        metrics.record_query(Duration::from_millis(100), false);
        metrics.record_query(Duration::from_millis(200), false);
        assert_eq!(metrics.get_average_execution_time_us(), 150_000);
    }

    #[test]
    fn test_average_execution_time_empty() {
        let metrics = QueryMetrics::new();
        assert_eq!(metrics.get_average_execution_time_us(), 0);
    }

    #[test]
    fn test_reset() {
        let metrics = QueryMetrics::new();
        metrics.record_query(Duration::from_millis(100), true);
        metrics.record_query(Duration::from_millis(1500), false);
        metrics.reset();
        assert_eq!(metrics.get_query_count(), 0);
        assert_eq!(metrics.get_total_execution_time_us(), 0);
        assert_eq!(metrics.get_slow_query_count(), 0);
        assert_eq!(metrics.get_error_count(), 0);
    }

    #[test]
    fn test_custom_slow_threshold() {
        let metrics = QueryMetrics::new().with_slow_query_threshold(500);
        metrics.record_query(Duration::from_millis(400), false);
        assert_eq!(metrics.get_slow_query_count(), 0);
        metrics.record_query(Duration::from_millis(600), false);
        assert_eq!(metrics.get_slow_query_count(), 1);
    }
}
