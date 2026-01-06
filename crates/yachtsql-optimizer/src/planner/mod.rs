#![coverage(off)]

pub mod constant_folding;
pub(crate) mod cte_optimization;
pub(crate) mod empty_propagation;
mod equi_join;
pub(crate) mod filter_merging;
mod into_logical;
mod physical_planner;
pub(crate) mod predicate;
pub(crate) mod projection_pushdown;
pub(crate) mod trivial_predicate;

pub use constant_folding::fold_constants;
pub use empty_propagation::apply_empty_propagation;
pub use filter_merging::apply_filter_merging;
pub use physical_planner::PhysicalPlanner;
pub use projection_pushdown::ProjectionPushdown;
pub use trivial_predicate::apply_trivial_predicate_removal;
