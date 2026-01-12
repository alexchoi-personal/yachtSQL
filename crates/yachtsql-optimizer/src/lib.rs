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
    PhysicalPlanner, ProjectionPushdown, apply_cross_to_hash_join, apply_distinct_elimination,
    apply_empty_propagation, apply_filter_merging, apply_filter_pushdown_aggregate,
    apply_filter_pushdown_join, apply_filter_pushdown_project, apply_join_elimination,
    apply_limit_pushdown, apply_outer_to_inner_join, apply_predicate_inference,
    apply_predicate_simplification, apply_project_merging, apply_short_circuit_ordering,
    apply_sort_elimination, apply_sort_pushdown_project, apply_topn_pushdown,
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

#[derive(Clone, Debug, Default)]
pub struct RuleFlags {
    pub trivial_predicate_removal: Option<bool>,
    pub empty_propagation: Option<bool>,
    pub filter_pushdown_project: Option<bool>,
    pub sort_pushdown_project: Option<bool>,
    pub filter_merging: Option<bool>,
    pub predicate_simplification: Option<bool>,
    pub project_merging: Option<bool>,
    pub distinct_elimination: Option<bool>,
    pub cross_to_hash_join: Option<bool>,
    pub outer_to_inner_join: Option<bool>,
    pub filter_pushdown_aggregate: Option<bool>,
    pub filter_pushdown_join: Option<bool>,
    pub sort_elimination: Option<bool>,
    pub limit_pushdown: Option<bool>,
    pub topn_pushdown: Option<bool>,
    pub predicate_inference: Option<bool>,
    pub short_circuit_ordering: Option<bool>,
    pub join_elimination: Option<bool>,
}

impl RuleFlags {
    pub fn all_enabled() -> Self {
        Self {
            trivial_predicate_removal: Some(true),
            empty_propagation: Some(true),
            filter_pushdown_project: Some(true),
            sort_pushdown_project: Some(true),
            filter_merging: Some(true),
            predicate_simplification: Some(true),
            project_merging: Some(true),
            distinct_elimination: Some(true),
            cross_to_hash_join: Some(true),
            outer_to_inner_join: Some(true),
            filter_pushdown_aggregate: Some(true),
            filter_pushdown_join: Some(true),
            sort_elimination: Some(true),
            limit_pushdown: Some(true),
            topn_pushdown: Some(true),
            predicate_inference: Some(true),
            short_circuit_ordering: Some(true),
            join_elimination: Some(true),
        }
    }

    pub fn all_disabled() -> Self {
        Self {
            trivial_predicate_removal: Some(false),
            empty_propagation: Some(false),
            filter_pushdown_project: Some(false),
            sort_pushdown_project: Some(false),
            filter_merging: Some(false),
            predicate_simplification: Some(false),
            project_merging: Some(false),
            distinct_elimination: Some(false),
            cross_to_hash_join: Some(false),
            outer_to_inner_join: Some(false),
            filter_pushdown_aggregate: Some(false),
            filter_pushdown_join: Some(false),
            sort_elimination: Some(false),
            limit_pushdown: Some(false),
            topn_pushdown: Some(false),
            predicate_inference: Some(false),
            short_circuit_ordering: Some(false),
            join_elimination: Some(false),
        }
    }

    pub fn only(rule: &str) -> Self {
        let mut flags = Self::all_disabled();
        match rule {
            "trivial_predicate_removal" => flags.trivial_predicate_removal = Some(true),
            "empty_propagation" => flags.empty_propagation = Some(true),
            "filter_pushdown_project" => flags.filter_pushdown_project = Some(true),
            "sort_pushdown_project" => flags.sort_pushdown_project = Some(true),
            "filter_merging" => flags.filter_merging = Some(true),
            "predicate_simplification" => flags.predicate_simplification = Some(true),
            "project_merging" => flags.project_merging = Some(true),
            "distinct_elimination" => flags.distinct_elimination = Some(true),
            "cross_to_hash_join" => flags.cross_to_hash_join = Some(true),
            "outer_to_inner_join" => flags.outer_to_inner_join = Some(true),
            "filter_pushdown_aggregate" => flags.filter_pushdown_aggregate = Some(true),
            "filter_pushdown_join" => flags.filter_pushdown_join = Some(true),
            "sort_elimination" => flags.sort_elimination = Some(true),
            "limit_pushdown" => flags.limit_pushdown = Some(true),
            "topn_pushdown" => flags.topn_pushdown = Some(true),
            "predicate_inference" => flags.predicate_inference = Some(true),
            "short_circuit_ordering" => flags.short_circuit_ordering = Some(true),
            "join_elimination" => flags.join_elimination = Some(true),
            _ => {}
        }
        flags
    }
}

#[derive(Clone, Debug)]
pub struct OptimizerSettings {
    pub level: OptimizationLevel,
    pub table_stats: FxHashMap<String, TableStats>,
    pub disable_join_reorder: bool,
    pub rules: RuleFlags,
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
            rules: RuleFlags::default(),
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

    pub fn with_rules(mut self, rules: RuleFlags) -> Self {
        self.rules = rules;
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

    fn rule_enabled(&self, flag: Option<bool>, min_level: OptimizationLevel) -> bool {
        match flag {
            Some(enabled) => enabled,
            None => self.level >= min_level,
        }
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

    if settings.rule_enabled(
        settings.rules.trivial_predicate_removal,
        OptimizationLevel::Basic,
    ) {
        physical_plan = apply_trivial_predicate_removal(physical_plan);
    }
    if settings.rule_enabled(settings.rules.empty_propagation, OptimizationLevel::Basic) {
        physical_plan = apply_empty_propagation(physical_plan);
    }
    if settings.rule_enabled(settings.rules.project_merging, OptimizationLevel::Basic) {
        physical_plan = apply_project_merging(physical_plan);
    }
    if settings.rule_enabled(
        settings.rules.filter_pushdown_project,
        OptimizationLevel::Basic,
    ) {
        physical_plan = apply_filter_pushdown_project(physical_plan);
    }
    if settings.rule_enabled(
        settings.rules.sort_pushdown_project,
        OptimizationLevel::Basic,
    ) {
        physical_plan = apply_sort_pushdown_project(physical_plan);
    }
    if settings.rule_enabled(settings.rules.filter_merging, OptimizationLevel::Basic) {
        physical_plan = apply_filter_merging(physical_plan);
    }
    if settings.rule_enabled(
        settings.rules.predicate_simplification,
        OptimizationLevel::Basic,
    ) {
        physical_plan = apply_predicate_simplification(physical_plan);
    }
    if settings.rule_enabled(
        settings.rules.distinct_elimination,
        OptimizationLevel::Basic,
    ) {
        physical_plan = apply_distinct_elimination(physical_plan);
    }
    if settings.rule_enabled(
        settings.rules.cross_to_hash_join,
        OptimizationLevel::Standard,
    ) {
        physical_plan = apply_cross_to_hash_join(physical_plan);
    }
    if settings.rule_enabled(
        settings.rules.outer_to_inner_join,
        OptimizationLevel::Standard,
    ) {
        physical_plan = apply_outer_to_inner_join(physical_plan);
    }
    if settings.rule_enabled(settings.rules.join_elimination, OptimizationLevel::Standard) {
        physical_plan = apply_join_elimination(physical_plan);
    }
    if settings.rule_enabled(
        settings.rules.filter_pushdown_join,
        OptimizationLevel::Standard,
    ) {
        physical_plan = apply_filter_pushdown_join(physical_plan);
    }
    if settings.rule_enabled(
        settings.rules.filter_pushdown_aggregate,
        OptimizationLevel::Standard,
    ) {
        physical_plan = apply_filter_pushdown_aggregate(physical_plan);
    }
    if settings.rule_enabled(settings.rules.sort_elimination, OptimizationLevel::Standard) {
        physical_plan = apply_sort_elimination(physical_plan);
    }
    if settings.rule_enabled(settings.rules.limit_pushdown, OptimizationLevel::Standard) {
        physical_plan = apply_limit_pushdown(physical_plan);
    }
    if settings.rule_enabled(settings.rules.topn_pushdown, OptimizationLevel::Standard) {
        physical_plan = apply_topn_pushdown(physical_plan);
    }
    if settings.rule_enabled(
        settings.rules.predicate_inference,
        OptimizationLevel::Aggressive,
    ) {
        physical_plan = apply_predicate_inference(physical_plan);
    }
    if settings.rule_enabled(
        settings.rules.short_circuit_ordering,
        OptimizationLevel::Aggressive,
    ) {
        physical_plan = apply_short_circuit_ordering(physical_plan);
    }
    if settings.rule_enabled(
        settings.rules.filter_pushdown_aggregate,
        OptimizationLevel::Aggressive,
    ) {
        physical_plan = apply_filter_pushdown_aggregate(physical_plan);
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
