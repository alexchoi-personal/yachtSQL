#![feature(coverage_attribute)]
#![coverage(off)]

mod join_order;
mod optimized_logical_plan;
mod pass;
mod planner;
pub mod stats;
#[cfg(test)]
mod test_utils;
#[cfg(test)]
mod tests;

pub use join_order::{
    CostModel, GreedyJoinReorderer, JoinEdge, JoinGraph, JoinRelation, PredicateCollector,
};
pub use optimized_logical_plan::{
    BoundType, ExecutionHints, PARALLEL_ROW_THRESHOLD, PhysicalPlan, SampleType,
};
pub use pass::{OptimizationPass, PassOverhead, PassTarget};
pub use planner::{
    PhysicalPlanner, ProjectionPushdown, apply_empty_propagation, apply_filter_merging,
    apply_trivial_predicate_removal, fold_constants,
};
use rustc_hash::FxHashMap;
pub use stats::{ColumnStats, TableStats};
use yachtsql_common::error::Result;
use yachtsql_ir::LogicalPlan;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum OptimizationLevel {
    None,
    Basic,
    #[default]
    Standard,
    Aggressive,
    Full,
}

#[derive(Clone, Debug)]
pub struct OptimizerSettings {
    pub level: OptimizationLevel,
    pub table_stats: FxHashMap<String, TableStats>,
    pub disable_join_reorder: bool,
}

impl Default for OptimizerSettings {
    fn default() -> Self {
        Self::with_level(OptimizationLevel::Standard)
    }
}

impl OptimizerSettings {
    pub fn with_level(level: OptimizationLevel) -> Self {
        Self {
            level,
            table_stats: FxHashMap::default(),
            disable_join_reorder: false,
        }
    }

    pub fn all_enabled() -> Self {
        Self::with_level(OptimizationLevel::Standard)
    }

    pub fn all_disabled() -> Self {
        Self::with_level(OptimizationLevel::None)
    }

    pub fn with_table_stats(mut self, stats: FxHashMap<String, TableStats>) -> Self {
        self.table_stats = stats;
        self
    }

    fn join_reorder_enabled(&self) -> bool {
        !self.disable_join_reorder && self.level >= OptimizationLevel::Standard
    }

    fn filter_pushdown_enabled(&self) -> bool {
        self.level >= OptimizationLevel::Basic
    }

    fn projection_pushdown_enabled(&self) -> bool {
        self.level >= OptimizationLevel::Basic
    }
}

impl PartialOrd for OptimizationLevel {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OptimizationLevel {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_ord = match self {
            OptimizationLevel::None => 0,
            OptimizationLevel::Basic => 1,
            OptimizationLevel::Standard => 2,
            OptimizationLevel::Aggressive => 3,
            OptimizationLevel::Full => 4,
        };
        let other_ord = match other {
            OptimizationLevel::None => 0,
            OptimizationLevel::Basic => 1,
            OptimizationLevel::Standard => 2,
            OptimizationLevel::Aggressive => 3,
            OptimizationLevel::Full => 4,
        };
        self_ord.cmp(&other_ord)
    }
}

pub fn optimize(logical: &LogicalPlan) -> Result<PhysicalPlan> {
    optimize_with_settings(logical, &OptimizerSettings::all_enabled())
}

pub fn optimize_with_settings(
    logical: &LogicalPlan,
    settings: &OptimizerSettings,
) -> Result<PhysicalPlan> {
    let reordered = if settings.join_reorder_enabled() {
        maybe_reorder_joins(logical, &settings.table_stats)
    } else {
        None
    };

    let plan_to_optimize = reordered.as_ref().unwrap_or(logical);
    let planner = PhysicalPlanner::with_settings(settings.filter_pushdown_enabled());
    let mut physical_plan = planner.plan(plan_to_optimize)?;

    if settings.level >= OptimizationLevel::Basic {
        physical_plan = apply_trivial_predicate_removal(physical_plan);
        physical_plan = apply_empty_propagation(physical_plan);
        physical_plan = apply_filter_merging(physical_plan);
    }

    if settings.projection_pushdown_enabled() {
        Ok(ProjectionPushdown::optimize(physical_plan))
    } else {
        Ok(physical_plan)
    }
}

fn maybe_reorder_joins(
    plan: &LogicalPlan,
    table_stats: &FxHashMap<String, TableStats>,
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
    let reordered_joins = reorderer.reorder(&graph, &original_schema).ok()?;

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
