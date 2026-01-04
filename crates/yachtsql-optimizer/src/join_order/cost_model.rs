use rustc_hash::FxHashMap;

use super::join_graph::JoinEdge;
use crate::stats::{ColumnStats, TableStats};

#[derive(Debug, Clone)]
pub struct JoinCost {
    pub output_rows: usize,
    pub total_cost: f64,
}

#[derive(Debug, Clone)]
pub struct EnhancedJoinCost {
    pub output_rows: usize,
    pub cpu_cost: f64,
    pub memory_cost: f64,
    pub io_cost: f64,
    pub total_cost: f64,
}

pub struct CostModel {
    table_stats: FxHashMap<String, TableStats>,
    default_row_count: usize,
}

impl CostModel {
    pub fn new() -> Self {
        Self {
            table_stats: FxHashMap::default(),
            default_row_count: 1000,
        }
    }

    pub fn with_stats(table_stats: FxHashMap<String, TableStats>) -> Self {
        let normalized: FxHashMap<String, TableStats> = table_stats
            .into_iter()
            .map(|(k, v)| (k.to_uppercase(), v))
            .collect();
        Self {
            table_stats: normalized,
            default_row_count: 1000,
        }
    }

    pub fn estimate_base_cardinality(&self, table_name: &str) -> usize {
        self.table_stats
            .get(&table_name.to_uppercase())
            .map(|s| s.row_count)
            .unwrap_or(self.default_row_count)
    }

    pub fn get_table_stats(&self, table_name: &str) -> Option<&TableStats> {
        self.table_stats.get(&table_name.to_uppercase())
    }

    pub fn get_column_stats(&self, table_name: &str, column_name: &str) -> Option<&ColumnStats> {
        self.table_stats
            .get(&table_name.to_uppercase())
            .and_then(|ts| ts.column_stats.get(column_name))
    }

    fn estimate_edge_selectivity(&self, edge: &JoinEdge) -> f64 {
        edge.selectivity_estimate
    }

    fn estimate_join_selectivity(&self, edges: &[&JoinEdge]) -> f64 {
        if edges.is_empty() {
            return 1.0;
        }
        edges
            .iter()
            .map(|e| self.estimate_edge_selectivity(e))
            .product()
    }

    pub fn estimate_join_cost(
        &self,
        left_rows: usize,
        right_rows: usize,
        edges: &[&JoinEdge],
    ) -> JoinCost {
        let selectivity = self.estimate_join_selectivity(edges);
        let output_rows = ((left_rows as f64) * (right_rows as f64) * selectivity).ceil() as usize;

        let cross_join_penalty = if edges.is_empty() { 1000.0 } else { 1.0 };

        let build_cost = right_rows as f64;
        let probe_cost = left_rows as f64;
        let output_cost = output_rows as f64;

        JoinCost {
            output_rows: output_rows.max(1),
            total_cost: (build_cost + probe_cost + output_cost) * cross_join_penalty,
        }
    }

    pub fn estimate_join_cost_enhanced(
        &self,
        left_rows: usize,
        right_rows: usize,
        edges: &[&JoinEdge],
        available_memory: usize,
    ) -> EnhancedJoinCost {
        let selectivity = self.estimate_join_selectivity(edges);
        let output_rows = ((left_rows as f64) * (right_rows as f64) * selectivity).ceil() as usize;

        let hash_cost = right_rows as f64 * 1.0;
        let probe_cost = left_rows as f64 * 1.2;
        let output_cost = output_rows as f64 * 0.5;
        let cpu_cost = hash_cost + probe_cost + output_cost;

        let row_size = 100;
        let hash_table_size = right_rows * row_size;
        let memory_cost = if hash_table_size > available_memory {
            (hash_table_size as f64 / available_memory as f64) * 10.0
        } else {
            1.0
        };

        let io_cost = if hash_table_size > available_memory {
            (hash_table_size - available_memory) as f64 * 0.01
        } else {
            0.0
        };

        let cross_penalty = if edges.is_empty() { 1000.0 } else { 0.0 };

        EnhancedJoinCost {
            output_rows: output_rows.max(1),
            cpu_cost,
            memory_cost,
            io_cost,
            total_cost: cpu_cost + memory_cost + io_cost + cross_penalty,
        }
    }

    pub fn estimate_equality_selectivity(
        &self,
        left_table: &str,
        left_col: &str,
        right_table: &str,
        right_col: &str,
    ) -> f64 {
        let left_distinct = self
            .get_column_stats(left_table, left_col)
            .map(|s| s.distinct_count)
            .unwrap_or(100);
        let right_distinct = self
            .get_column_stats(right_table, right_col)
            .map(|s| s.distinct_count)
            .unwrap_or(100);

        1.0 / (left_distinct.max(right_distinct) as f64)
    }
}

impl Default for CostModel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_ir::Expr;

    use super::*;

    fn create_test_edge(selectivity: f64) -> JoinEdge {
        JoinEdge {
            left_relation: 0,
            right_relation: 1,
            predicate: Expr::Literal(yachtsql_ir::Literal::Bool(true)),
            selectivity_estimate: selectivity,
        }
    }

    fn create_cost_model_with_stats() -> CostModel {
        let mut table_stats = FxHashMap::default();

        let mut orders_stats = TableStats::new(10000);
        orders_stats.column_stats.insert(
            "id".to_string(),
            ColumnStats {
                distinct_count: 10000,
                null_count: 0,
                min_value: None,
                max_value: None,
            },
        );
        orders_stats.column_stats.insert(
            "customer_id".to_string(),
            ColumnStats {
                distinct_count: 1000,
                null_count: 0,
                min_value: None,
                max_value: None,
            },
        );
        table_stats.insert("ORDERS".to_string(), orders_stats);

        let mut customers_stats = TableStats::new(1000);
        customers_stats.column_stats.insert(
            "id".to_string(),
            ColumnStats {
                distinct_count: 1000,
                null_count: 0,
                min_value: None,
                max_value: None,
            },
        );
        table_stats.insert("CUSTOMERS".to_string(), customers_stats);

        CostModel::with_stats(table_stats)
    }

    #[test]
    fn test_enhanced_join_cost_normal_join() {
        let cost_model = CostModel::new();
        let edge = create_test_edge(0.01);
        let edges: Vec<&JoinEdge> = vec![&edge];

        let cost = cost_model.estimate_join_cost_enhanced(1000, 500, &edges, 1_000_000);

        assert_eq!(cost.output_rows, 5000);
        let expected_cpu = 500.0 * 1.0 + 1000.0 * 1.2 + 5000.0 * 0.5;
        assert!((cost.cpu_cost - expected_cpu).abs() < 0.001);
        assert!((cost.memory_cost - 1.0).abs() < 0.001);
        assert!((cost.io_cost - 0.0).abs() < 0.001);
        assert!((cost.total_cost - (expected_cpu + 1.0)).abs() < 0.001);
    }

    #[test]
    fn test_enhanced_join_cost_cross_join_penalty() {
        let cost_model = CostModel::new();
        let edges: Vec<&JoinEdge> = vec![];

        let cost = cost_model.estimate_join_cost_enhanced(100, 100, &edges, 1_000_000);

        assert_eq!(cost.output_rows, 10000);
        let expected_cpu = 100.0 * 1.0 + 100.0 * 1.2 + 10000.0 * 0.5;
        assert!((cost.cpu_cost - expected_cpu).abs() < 0.001);
        assert!(cost.total_cost >= 1000.0);
        let expected_total = expected_cpu + 1.0 + 0.0 + 1000.0;
        assert!((cost.total_cost - expected_total).abs() < 0.001);
    }

    #[test]
    fn test_enhanced_join_cost_memory_spill() {
        let cost_model = CostModel::new();
        let edge = create_test_edge(0.01);
        let edges: Vec<&JoinEdge> = vec![&edge];

        let right_rows = 10000;
        let available_memory = 500_000;
        let cost =
            cost_model.estimate_join_cost_enhanced(1000, right_rows, &edges, available_memory);

        let row_size = 100;
        let hash_table_size = right_rows * row_size;
        assert!(hash_table_size > available_memory);

        let expected_memory_cost = (hash_table_size as f64 / available_memory as f64) * 10.0;
        assert!((cost.memory_cost - expected_memory_cost).abs() < 0.001);

        let expected_io_cost = (hash_table_size - available_memory) as f64 * 0.01;
        assert!((cost.io_cost - expected_io_cost).abs() < 0.001);
        assert!(cost.io_cost > 0.0);
    }

    #[test]
    fn test_enhanced_join_cost_no_memory_spill() {
        let cost_model = CostModel::new();
        let edge = create_test_edge(0.01);
        let edges: Vec<&JoinEdge> = vec![&edge];

        let right_rows = 1000;
        let available_memory = 1_000_000;
        let cost =
            cost_model.estimate_join_cost_enhanced(1000, right_rows, &edges, available_memory);

        let row_size = 100;
        let hash_table_size = right_rows * row_size;
        assert!(hash_table_size <= available_memory);

        assert!((cost.memory_cost - 1.0).abs() < 0.001);
        assert!((cost.io_cost - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_estimate_equality_selectivity_with_known_stats() {
        let cost_model = create_cost_model_with_stats();

        let selectivity =
            cost_model.estimate_equality_selectivity("orders", "customer_id", "customers", "id");

        assert!((selectivity - (1.0 / 1000.0)).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_equality_selectivity_uses_max_distinct() {
        let cost_model = create_cost_model_with_stats();

        let selectivity =
            cost_model.estimate_equality_selectivity("orders", "id", "customers", "id");

        assert!((selectivity - (1.0 / 10000.0)).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_equality_selectivity_fallback() {
        let cost_model = CostModel::new();

        let selectivity = cost_model.estimate_equality_selectivity(
            "unknown_table",
            "col1",
            "another_unknown",
            "col2",
        );

        assert!((selectivity - (1.0 / 100.0)).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_equality_selectivity_partial_stats() {
        let cost_model = create_cost_model_with_stats();

        let selectivity = cost_model.estimate_equality_selectivity(
            "orders",
            "customer_id",
            "unknown_table",
            "col",
        );

        assert!((selectivity - (1.0 / 1000.0)).abs() < 0.0001);
    }

    #[test]
    fn test_get_table_stats() {
        let cost_model = create_cost_model_with_stats();

        let stats = cost_model.get_table_stats("orders");
        assert!(stats.is_some());
        assert_eq!(stats.unwrap().row_count, 10000);

        let stats = cost_model.get_table_stats("nonexistent");
        assert!(stats.is_none());
    }

    #[test]
    fn test_get_column_stats() {
        let cost_model = create_cost_model_with_stats();

        let stats = cost_model.get_column_stats("orders", "id");
        assert!(stats.is_some());
        assert_eq!(stats.unwrap().distinct_count, 10000);

        let stats = cost_model.get_column_stats("orders", "nonexistent");
        assert!(stats.is_none());

        let stats = cost_model.get_column_stats("nonexistent", "id");
        assert!(stats.is_none());
    }

    #[test]
    fn test_enhanced_join_cost_minimum_output_rows() {
        let cost_model = CostModel::new();
        let edge = create_test_edge(0.0001);
        let edges: Vec<&JoinEdge> = vec![&edge];

        let cost = cost_model.estimate_join_cost_enhanced(1, 1, &edges, 1_000_000);

        assert!(cost.output_rows >= 1);
    }

    #[test]
    fn test_enhanced_join_cost_multiple_edges() {
        let cost_model = CostModel::new();
        let edge1 = create_test_edge(0.1);
        let edge2 = create_test_edge(0.1);
        let edges: Vec<&JoinEdge> = vec![&edge1, &edge2];

        let cost = cost_model.estimate_join_cost_enhanced(1000, 1000, &edges, 1_000_000);

        let selectivity = 0.1 * 0.1;
        let raw_output = 1000.0_f64 * 1000.0 * selectivity;
        let expected_output = raw_output.ceil() as usize;
        assert_eq!(cost.output_rows, expected_output.max(1));
    }
}
