#![feature(coverage_attribute)]
#![coverage(off)]

mod join_order;
mod optimized_logical_plan;
mod planner;
pub mod stats;
#[cfg(test)]
mod tests;

pub use join_order::{CostModel, GreedyJoinReorderer, JoinGraph, PredicateCollector};
pub use optimized_logical_plan::{OptimizedLogicalPlan, SampleType};
pub use planner::{PhysicalPlanner, ProjectionPushdown};
pub use stats::{ColumnStats, TableStats};
use yachtsql_common::error::Result;
use yachtsql_ir::LogicalPlan;

pub fn optimize(logical: &LogicalPlan) -> Result<OptimizedLogicalPlan> {
    let reordered = maybe_reorder_joins(logical);
    let plan_to_optimize = reordered.as_ref().unwrap_or(logical);
    let physical_plan = PhysicalPlanner::new().plan(plan_to_optimize)?;
    Ok(ProjectionPushdown::optimize(physical_plan))
}

fn maybe_reorder_joins(plan: &LogicalPlan) -> Option<LogicalPlan> {
    let cost_model = CostModel::new();
    let graph = PredicateCollector::build_join_graph(plan, &cost_model)?;

    if graph.relations().len() < 2 {
        return None;
    }

    let original_schema = plan.schema().clone();
    let reorderer = GreedyJoinReorderer::new(cost_model);
    Some(reorderer.reorder(&graph, &original_schema))
}
