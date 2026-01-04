use rustc_hash::FxHashMap;
use yachtsql_common::types::Value;

#[derive(Debug, Clone, Default)]
pub struct TableStats {
    pub row_count: usize,
    pub column_stats: FxHashMap<String, ColumnStats>,
    pub correlations: FxHashMap<(String, String), f64>,
}

#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub distinct_count: usize,
    pub null_count: usize,
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
}

impl TableStats {
    pub fn new(row_count: usize) -> Self {
        Self {
            row_count,
            column_stats: FxHashMap::default(),
            correlations: FxHashMap::default(),
        }
    }

    pub fn estimate_selectivity(&self, column: &str, op: &str) -> f64 {
        match self.column_stats.get(column) {
            Some(stats) if stats.distinct_count > 0 => match op {
                "=" => 1.0 / stats.distinct_count as f64,
                "<" | ">" | "<=" | ">=" => 0.33,
                "!=" | "<>" => 1.0 - (1.0 / stats.distinct_count as f64),
                _ => 0.5,
            },
            _ => 0.5,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_stats_new() {
        let stats = TableStats::new(1000);
        assert_eq!(stats.row_count, 1000);
        assert!(stats.column_stats.is_empty());
        assert!(stats.correlations.is_empty());
    }

    #[test]
    fn test_table_stats_default() {
        let stats = TableStats::default();
        assert_eq!(stats.row_count, 0);
        assert!(stats.column_stats.is_empty());
        assert!(stats.correlations.is_empty());
    }

    #[test]
    fn test_estimate_selectivity_equality() {
        let mut stats = TableStats::new(1000);
        stats.column_stats.insert(
            "id".to_string(),
            ColumnStats {
                distinct_count: 100,
                null_count: 0,
                min_value: Some(Value::Int64(1)),
                max_value: Some(Value::Int64(100)),
            },
        );

        let selectivity = stats.estimate_selectivity("id", "=");
        assert!((selectivity - 0.01).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_range() {
        let mut stats = TableStats::new(1000);
        stats.column_stats.insert(
            "price".to_string(),
            ColumnStats {
                distinct_count: 50,
                null_count: 0,
                min_value: Some(Value::float64(0.0)),
                max_value: Some(Value::float64(100.0)),
            },
        );

        assert!((stats.estimate_selectivity("price", "<") - 0.33).abs() < 0.0001);
        assert!((stats.estimate_selectivity("price", ">") - 0.33).abs() < 0.0001);
        assert!((stats.estimate_selectivity("price", "<=") - 0.33).abs() < 0.0001);
        assert!((stats.estimate_selectivity("price", ">=") - 0.33).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_not_equal() {
        let mut stats = TableStats::new(1000);
        stats.column_stats.insert(
            "status".to_string(),
            ColumnStats {
                distinct_count: 10,
                null_count: 0,
                min_value: None,
                max_value: None,
            },
        );

        let selectivity_ne = stats.estimate_selectivity("status", "!=");
        assert!((selectivity_ne - 0.9).abs() < 0.0001);

        let selectivity_diamond = stats.estimate_selectivity("status", "<>");
        assert!((selectivity_diamond - 0.9).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_unknown_operator() {
        let mut stats = TableStats::new(1000);
        stats.column_stats.insert(
            "col".to_string(),
            ColumnStats {
                distinct_count: 20,
                null_count: 0,
                min_value: None,
                max_value: None,
            },
        );

        let selectivity = stats.estimate_selectivity("col", "LIKE");
        assert!((selectivity - 0.5).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_unknown_column() {
        let stats = TableStats::new(1000);
        let selectivity = stats.estimate_selectivity("unknown_col", "=");
        assert!((selectivity - 0.5).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_zero_distinct() {
        let mut stats = TableStats::new(1000);
        stats.column_stats.insert(
            "empty_col".to_string(),
            ColumnStats {
                distinct_count: 0,
                null_count: 1000,
                min_value: None,
                max_value: None,
            },
        );

        let selectivity = stats.estimate_selectivity("empty_col", "=");
        assert!((selectivity - 0.5).abs() < 0.0001);
    }

    #[test]
    fn test_column_stats_with_values() {
        let col_stats = ColumnStats {
            distinct_count: 100,
            null_count: 5,
            min_value: Some(Value::Int64(1)),
            max_value: Some(Value::Int64(100)),
        };

        assert_eq!(col_stats.distinct_count, 100);
        assert_eq!(col_stats.null_count, 5);
        assert_eq!(col_stats.min_value, Some(Value::Int64(1)));
        assert_eq!(col_stats.max_value, Some(Value::Int64(100)));
    }

    #[test]
    fn test_table_stats_with_correlations() {
        let mut stats = TableStats::new(1000);
        stats
            .correlations
            .insert(("col_a".to_string(), "col_b".to_string()), 0.85);
        stats
            .correlations
            .insert(("col_a".to_string(), "col_c".to_string()), -0.5);

        assert_eq!(
            stats
                .correlations
                .get(&("col_a".to_string(), "col_b".to_string())),
            Some(&0.85)
        );
        assert_eq!(
            stats
                .correlations
                .get(&("col_a".to_string(), "col_c".to_string())),
            Some(&-0.5)
        );
    }

    #[test]
    fn test_estimate_selectivity_single_distinct() {
        let mut stats = TableStats::new(1000);
        stats.column_stats.insert(
            "constant_col".to_string(),
            ColumnStats {
                distinct_count: 1,
                null_count: 0,
                min_value: Some(Value::String("constant".to_string())),
                max_value: Some(Value::String("constant".to_string())),
            },
        );

        let selectivity_eq = stats.estimate_selectivity("constant_col", "=");
        assert!((selectivity_eq - 1.0).abs() < 0.0001);

        let selectivity_ne = stats.estimate_selectivity("constant_col", "!=");
        assert!((selectivity_ne - 0.0).abs() < 0.0001);
    }
}
