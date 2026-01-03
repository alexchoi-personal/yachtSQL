#![coverage(off)]

mod equi_join;
mod into_logical;
mod physical_planner;
pub(crate) mod predicate;
pub(crate) mod projection_pushdown;

pub use physical_planner::PhysicalPlanner;
pub use projection_pushdown::ProjectionPushdown;
