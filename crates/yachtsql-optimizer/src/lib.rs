#![feature(coverage_attribute)]
#![coverage(off)]

mod join_order;
mod optimized_logical_plan;
mod planner;
pub mod stats;
#[cfg(test)]
mod test_utils;
#[cfg(test)]
mod tests;

use std::collections::HashMap;

pub use join_order::{
    CostModel, GreedyJoinReorderer, JoinEdge, JoinGraph, JoinRelation, PredicateCollector,
};
pub use optimized_logical_plan::{OptimizedLogicalPlan, SampleType};
pub use planner::{PhysicalPlanner, ProjectionPushdown, fold_constants};
pub use stats::{ColumnStats, TableStats};
use yachtsql_common::error::Result;
use yachtsql_ir::LogicalPlan;

#[derive(Clone, Debug)]
pub struct OptimizerSettings {
    pub join_reorder: bool,
    pub filter_pushdown: bool,
    pub projection_pushdown: bool,
    pub table_stats: HashMap<String, TableStats>,
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
            table_stats: HashMap::new(),
        }
    }

    pub fn all_disabled() -> Self {
        Self {
            join_reorder: false,
            filter_pushdown: false,
            projection_pushdown: false,
            table_stats: HashMap::new(),
        }
    }

    pub fn with_table_stats(mut self, stats: HashMap<String, TableStats>) -> Self {
        self.table_stats = stats;
        self
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
        maybe_reorder_joins(logical, &settings.table_stats)
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

fn maybe_reorder_joins(
    plan: &LogicalPlan,
    table_stats: &HashMap<String, TableStats>,
) -> Option<LogicalPlan> {
    if table_stats.is_empty() {
        return None;
    }

    let join_subtree = PredicateCollector::find_join_subtree(plan);

    if PredicateCollector::has_non_equality_join_predicates(join_subtree) {
        return None;
    }

    let cost_model = CostModel::with_stats(table_stats.clone());
    let graph = PredicateCollector::build_join_graph(join_subtree, &cost_model)?;

    if graph.relations().len() < 2 {
        return None;
    }

    let original_schema = join_subtree.schema().clone();
    let reorderer = GreedyJoinReorderer::new(cost_model);
    let reordered_joins = reorderer.reorder(&graph, &original_schema);

    Some(wrap_with_outer_nodes(plan, reordered_joins))
}

fn wrap_with_outer_nodes(original: &LogicalPlan, reordered_inner: LogicalPlan) -> LogicalPlan {
    match original {
        LogicalPlan::Project {
            input,
            expressions,
            schema,
        } => LogicalPlan::Project {
            input: Box::new(wrap_with_outer_nodes(input, reordered_inner)),
            expressions: expressions.clone(),
            schema: schema.clone(),
        },
        LogicalPlan::Sort { input, sort_exprs } => LogicalPlan::Sort {
            input: Box::new(wrap_with_outer_nodes(input, reordered_inner)),
            sort_exprs: sort_exprs.clone(),
        },
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => LogicalPlan::Limit {
            input: Box::new(wrap_with_outer_nodes(input, reordered_inner)),
            limit: *limit,
            offset: *offset,
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(wrap_with_outer_nodes(input, reordered_inner)),
        },
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(wrap_with_outer_nodes(input, reordered_inner)),
            predicate: predicate.clone(),
        },
        _ => reordered_inner,
    }
}
