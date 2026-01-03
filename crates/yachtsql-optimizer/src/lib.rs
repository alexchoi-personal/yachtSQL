#![feature(coverage_attribute)]
#![coverage(off)]

mod join_order;
mod optimized_logical_plan;
mod planner;
pub mod stats;
#[cfg(test)]
mod tests;

pub use join_order::{
    CostModel, GreedyJoinReorderer, JoinEdge, JoinGraph, JoinRelation, PredicateCollector,
};
pub use optimized_logical_plan::{OptimizedLogicalPlan, SampleType};
pub use planner::{PhysicalPlanner, ProjectionPushdown};
pub use stats::{ColumnStats, TableStats};
use yachtsql_common::error::Result;
use yachtsql_ir::LogicalPlan;

#[derive(Clone, Debug)]
pub struct OptimizerSettings {
    pub join_reorder: bool,
    pub filter_pushdown: bool,
    pub projection_pushdown: bool,
}

impl Default for OptimizerSettings {
    fn default() -> Self {
        Self::all_enabled()
    }
}

impl OptimizerSettings {
    pub fn all_enabled() -> Self {
        Self {
            join_reorder: true,
            filter_pushdown: true,
            projection_pushdown: true,
        }
    }

    pub fn all_disabled() -> Self {
        Self {
            join_reorder: false,
            filter_pushdown: false,
            projection_pushdown: false,
        }
    }
}

pub fn optimize(logical: &LogicalPlan) -> Result<OptimizedLogicalPlan> {
    optimize_with_settings(logical, &OptimizerSettings::all_enabled())
}

pub fn optimize_with_settings(
    logical: &LogicalPlan,
    settings: &OptimizerSettings,
) -> Result<OptimizedLogicalPlan> {
    let reordered = if settings.join_reorder {
        maybe_reorder_joins(logical)
    } else {
        None
    };

    let plan_to_optimize = reordered.as_ref().unwrap_or(logical);
    let planner = PhysicalPlanner::with_settings(settings.filter_pushdown);
    let physical_plan = planner.plan(plan_to_optimize)?;

    if settings.projection_pushdown {
        Ok(ProjectionPushdown::optimize(physical_plan))
    } else {
        Ok(physical_plan)
    }
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
