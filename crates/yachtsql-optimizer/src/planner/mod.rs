#![coverage(off)]

pub mod constant_folding;
pub(crate) mod cte_optimization;
mod equi_join;
mod into_logical;
mod physical_planner;
pub(crate) mod predicate;
pub(crate) mod projection_pushdown;

pub use constant_folding::fold_constants;
pub use physical_planner::PhysicalPlanner;
pub use projection_pushdown::ProjectionPushdown;
