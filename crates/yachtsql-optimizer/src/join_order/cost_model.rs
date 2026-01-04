#![coverage(off)]

use std::collections::HashMap;

use super::join_graph::JoinEdge;
use crate::stats::TableStats;

#[derive(Debug, Clone)]
pub struct JoinCost {
    pub output_rows: usize,
    pub total_cost: f64,
}

pub struct CostModel {
    table_stats: HashMap<String, TableStats>,
    default_row_count: usize,
}

impl CostModel {
    pub fn new() -> Self {
        Self {
            table_stats: HashMap::new(),
            default_row_count: 1000,
        }
    }

    pub fn with_stats(table_stats: HashMap<String, TableStats>) -> Self {
        Self {
            table_stats,
            default_row_count: 1000,
        }
    }

    pub fn estimate_base_cardinality(&self, table_name: &str) -> usize {
        self.table_stats
            .get(table_name)
            .map(|s| s.row_count)
            .unwrap_or(self.default_row_count)
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
}

impl Default for CostModel {
    fn default() -> Self {
        Self::new()
    }
}
