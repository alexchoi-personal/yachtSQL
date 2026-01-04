#![feature(coverage_attribute)]
#![coverage(off)]
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(clippy::type_complexity)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::only_used_in_recursion)]
#![allow(clippy::if_same_then_else)]
#![allow(clippy::manual_strip)]
#![allow(clippy::wildcard_enum_match_arm)]

mod catalog;
mod columnar_evaluator;
mod error;
mod executor;
mod js_udf;
mod plan;
mod py_udf;
pub mod scalar_functions;
mod session;
pub mod value_evaluator;

mod async_executor;
mod concurrent_catalog;
mod concurrent_session;
mod metrics;
mod physical_planner;
mod plan_cache;

use std::num::NonZeroUsize;

pub use async_executor::AsyncQueryExecutor;
pub use catalog::{Catalog, ColumnDefault, UserFunction, UserProcedure, ViewDef};
pub use columnar_evaluator::ColumnarEvaluator;
pub use concurrent_catalog::{ConcurrentCatalog, TableLockSet};
pub use concurrent_session::ConcurrentSession;
pub use error::{Error, Result};
pub use executor::plan_schema_to_schema;
use lru::LruCache;
pub use physical_planner::PhysicalPlanner;
pub use session::Session;
pub use value_evaluator::{UserFunctionDef, ValueEvaluator, cast_value};
use yachtsql_optimizer::OptimizedLogicalPlan;
pub use yachtsql_storage::{Record, Table};

const PLAN_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(10000).unwrap();

fn default_plan_cache() -> LruCache<String, OptimizedLogicalPlan> {
    LruCache::new(PLAN_CACHE_SIZE)
}

fn is_cacheable_plan(plan: &OptimizedLogicalPlan) -> bool {
    match plan {
        OptimizedLogicalPlan::TableScan { .. }
        | OptimizedLogicalPlan::Sample { .. }
        | OptimizedLogicalPlan::Filter { .. }
        | OptimizedLogicalPlan::Project { .. }
        | OptimizedLogicalPlan::NestedLoopJoin { .. }
        | OptimizedLogicalPlan::CrossJoin { .. }
        | OptimizedLogicalPlan::HashJoin { .. }
        | OptimizedLogicalPlan::HashAggregate { .. }
        | OptimizedLogicalPlan::Sort { .. }
        | OptimizedLogicalPlan::Limit { .. }
        | OptimizedLogicalPlan::TopN { .. }
        | OptimizedLogicalPlan::Distinct { .. }
        | OptimizedLogicalPlan::Union { .. }
        | OptimizedLogicalPlan::Intersect { .. }
        | OptimizedLogicalPlan::Except { .. }
        | OptimizedLogicalPlan::Window { .. }
        | OptimizedLogicalPlan::Unnest { .. }
        | OptimizedLogicalPlan::Qualify { .. }
        | OptimizedLogicalPlan::WithCte { .. }
        | OptimizedLogicalPlan::Values { .. }
        | OptimizedLogicalPlan::Empty { .. } => true,

        OptimizedLogicalPlan::Insert { .. }
        | OptimizedLogicalPlan::Update { .. }
        | OptimizedLogicalPlan::Delete { .. }
        | OptimizedLogicalPlan::Merge { .. }
        | OptimizedLogicalPlan::CreateTable { .. }
        | OptimizedLogicalPlan::DropTable { .. }
        | OptimizedLogicalPlan::AlterTable { .. }
        | OptimizedLogicalPlan::Truncate { .. }
        | OptimizedLogicalPlan::CreateView { .. }
        | OptimizedLogicalPlan::DropView { .. }
        | OptimizedLogicalPlan::CreateSchema { .. }
        | OptimizedLogicalPlan::DropSchema { .. }
        | OptimizedLogicalPlan::UndropSchema { .. }
        | OptimizedLogicalPlan::AlterSchema { .. }
        | OptimizedLogicalPlan::CreateFunction { .. }
        | OptimizedLogicalPlan::DropFunction { .. }
        | OptimizedLogicalPlan::CreateProcedure { .. }
        | OptimizedLogicalPlan::DropProcedure { .. }
        | OptimizedLogicalPlan::Call { .. }
        | OptimizedLogicalPlan::ExportData { .. }
        | OptimizedLogicalPlan::LoadData { .. }
        | OptimizedLogicalPlan::Declare { .. }
        | OptimizedLogicalPlan::SetVariable { .. }
        | OptimizedLogicalPlan::SetMultipleVariables { .. }
        | OptimizedLogicalPlan::If { .. }
        | OptimizedLogicalPlan::While { .. }
        | OptimizedLogicalPlan::Loop { .. }
        | OptimizedLogicalPlan::Block { .. }
        | OptimizedLogicalPlan::Repeat { .. }
        | OptimizedLogicalPlan::For { .. }
        | OptimizedLogicalPlan::Return { .. }
        | OptimizedLogicalPlan::Raise { .. }
        | OptimizedLogicalPlan::ExecuteImmediate { .. }
        | OptimizedLogicalPlan::Break { .. }
        | OptimizedLogicalPlan::Continue { .. }
        | OptimizedLogicalPlan::CreateSnapshot { .. }
        | OptimizedLogicalPlan::DropSnapshot { .. }
        | OptimizedLogicalPlan::Assert { .. }
        | OptimizedLogicalPlan::Grant { .. }
        | OptimizedLogicalPlan::Revoke { .. }
        | OptimizedLogicalPlan::BeginTransaction
        | OptimizedLogicalPlan::Commit
        | OptimizedLogicalPlan::Rollback
        | OptimizedLogicalPlan::TryCatch { .. }
        | OptimizedLogicalPlan::GapFill { .. }
        | OptimizedLogicalPlan::Explain { .. } => false,
    }
}

fn invalidates_cache(plan: &OptimizedLogicalPlan) -> bool {
    match plan {
        OptimizedLogicalPlan::CreateTable { .. }
        | OptimizedLogicalPlan::DropTable { .. }
        | OptimizedLogicalPlan::AlterTable { .. }
        | OptimizedLogicalPlan::Truncate { .. }
        | OptimizedLogicalPlan::CreateView { .. }
        | OptimizedLogicalPlan::DropView { .. }
        | OptimizedLogicalPlan::CreateSchema { .. }
        | OptimizedLogicalPlan::DropSchema { .. }
        | OptimizedLogicalPlan::UndropSchema { .. }
        | OptimizedLogicalPlan::AlterSchema { .. }
        | OptimizedLogicalPlan::CreateFunction { .. }
        | OptimizedLogicalPlan::DropFunction { .. }
        | OptimizedLogicalPlan::CreateProcedure { .. }
        | OptimizedLogicalPlan::DropProcedure { .. }
        | OptimizedLogicalPlan::CreateSnapshot { .. }
        | OptimizedLogicalPlan::DropSnapshot { .. } => true,

        OptimizedLogicalPlan::TableScan { .. }
        | OptimizedLogicalPlan::Sample { .. }
        | OptimizedLogicalPlan::Filter { .. }
        | OptimizedLogicalPlan::Project { .. }
        | OptimizedLogicalPlan::NestedLoopJoin { .. }
        | OptimizedLogicalPlan::CrossJoin { .. }
        | OptimizedLogicalPlan::HashJoin { .. }
        | OptimizedLogicalPlan::HashAggregate { .. }
        | OptimizedLogicalPlan::Sort { .. }
        | OptimizedLogicalPlan::Limit { .. }
        | OptimizedLogicalPlan::TopN { .. }
        | OptimizedLogicalPlan::Distinct { .. }
        | OptimizedLogicalPlan::Union { .. }
        | OptimizedLogicalPlan::Intersect { .. }
        | OptimizedLogicalPlan::Except { .. }
        | OptimizedLogicalPlan::Window { .. }
        | OptimizedLogicalPlan::Unnest { .. }
        | OptimizedLogicalPlan::Qualify { .. }
        | OptimizedLogicalPlan::WithCte { .. }
        | OptimizedLogicalPlan::Values { .. }
        | OptimizedLogicalPlan::Empty { .. }
        | OptimizedLogicalPlan::Insert { .. }
        | OptimizedLogicalPlan::Update { .. }
        | OptimizedLogicalPlan::Delete { .. }
        | OptimizedLogicalPlan::Merge { .. }
        | OptimizedLogicalPlan::Call { .. }
        | OptimizedLogicalPlan::ExportData { .. }
        | OptimizedLogicalPlan::LoadData { .. }
        | OptimizedLogicalPlan::Declare { .. }
        | OptimizedLogicalPlan::SetVariable { .. }
        | OptimizedLogicalPlan::SetMultipleVariables { .. }
        | OptimizedLogicalPlan::If { .. }
        | OptimizedLogicalPlan::While { .. }
        | OptimizedLogicalPlan::Loop { .. }
        | OptimizedLogicalPlan::Block { .. }
        | OptimizedLogicalPlan::Repeat { .. }
        | OptimizedLogicalPlan::For { .. }
        | OptimizedLogicalPlan::Return { .. }
        | OptimizedLogicalPlan::Raise { .. }
        | OptimizedLogicalPlan::ExecuteImmediate { .. }
        | OptimizedLogicalPlan::Break { .. }
        | OptimizedLogicalPlan::Continue { .. }
        | OptimizedLogicalPlan::Assert { .. }
        | OptimizedLogicalPlan::Grant { .. }
        | OptimizedLogicalPlan::Revoke { .. }
        | OptimizedLogicalPlan::BeginTransaction
        | OptimizedLogicalPlan::Commit
        | OptimizedLogicalPlan::Rollback
        | OptimizedLogicalPlan::TryCatch { .. }
        | OptimizedLogicalPlan::GapFill { .. }
        | OptimizedLogicalPlan::Explain { .. } => false,
    }
}
