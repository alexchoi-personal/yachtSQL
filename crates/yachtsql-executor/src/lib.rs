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
use yachtsql_optimizer::PhysicalPlan;
pub use yachtsql_storage::{Record, Table};

const PLAN_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(10000).unwrap();

fn default_plan_cache() -> LruCache<String, PhysicalPlan> {
    LruCache::new(PLAN_CACHE_SIZE)
}

fn is_cacheable_plan(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::TableScan { .. }
        | PhysicalPlan::Sample { .. }
        | PhysicalPlan::Filter { .. }
        | PhysicalPlan::Project { .. }
        | PhysicalPlan::NestedLoopJoin { .. }
        | PhysicalPlan::CrossJoin { .. }
        | PhysicalPlan::HashJoin { .. }
        | PhysicalPlan::HashAggregate { .. }
        | PhysicalPlan::Sort { .. }
        | PhysicalPlan::Limit { .. }
        | PhysicalPlan::TopN { .. }
        | PhysicalPlan::Distinct { .. }
        | PhysicalPlan::Union { .. }
        | PhysicalPlan::Intersect { .. }
        | PhysicalPlan::Except { .. }
        | PhysicalPlan::Window { .. }
        | PhysicalPlan::Unnest { .. }
        | PhysicalPlan::Qualify { .. }
        | PhysicalPlan::WithCte { .. }
        | PhysicalPlan::Values { .. }
        | PhysicalPlan::Empty { .. } => true,

        PhysicalPlan::Insert { .. }
        | PhysicalPlan::Update { .. }
        | PhysicalPlan::Delete { .. }
        | PhysicalPlan::Merge { .. }
        | PhysicalPlan::CreateTable { .. }
        | PhysicalPlan::DropTable { .. }
        | PhysicalPlan::AlterTable { .. }
        | PhysicalPlan::Truncate { .. }
        | PhysicalPlan::CreateView { .. }
        | PhysicalPlan::DropView { .. }
        | PhysicalPlan::CreateSchema { .. }
        | PhysicalPlan::DropSchema { .. }
        | PhysicalPlan::UndropSchema { .. }
        | PhysicalPlan::AlterSchema { .. }
        | PhysicalPlan::CreateFunction { .. }
        | PhysicalPlan::DropFunction { .. }
        | PhysicalPlan::CreateProcedure { .. }
        | PhysicalPlan::DropProcedure { .. }
        | PhysicalPlan::Call { .. }
        | PhysicalPlan::ExportData { .. }
        | PhysicalPlan::LoadData { .. }
        | PhysicalPlan::Declare { .. }
        | PhysicalPlan::SetVariable { .. }
        | PhysicalPlan::SetMultipleVariables { .. }
        | PhysicalPlan::If { .. }
        | PhysicalPlan::While { .. }
        | PhysicalPlan::Loop { .. }
        | PhysicalPlan::Block { .. }
        | PhysicalPlan::Repeat { .. }
        | PhysicalPlan::For { .. }
        | PhysicalPlan::Return { .. }
        | PhysicalPlan::Raise { .. }
        | PhysicalPlan::ExecuteImmediate { .. }
        | PhysicalPlan::Break { .. }
        | PhysicalPlan::Continue { .. }
        | PhysicalPlan::CreateSnapshot { .. }
        | PhysicalPlan::DropSnapshot { .. }
        | PhysicalPlan::Assert { .. }
        | PhysicalPlan::Grant { .. }
        | PhysicalPlan::Revoke { .. }
        | PhysicalPlan::BeginTransaction
        | PhysicalPlan::Commit
        | PhysicalPlan::Rollback
        | PhysicalPlan::TryCatch { .. }
        | PhysicalPlan::GapFill { .. }
        | PhysicalPlan::Explain { .. } => false,
    }
}

fn invalidates_cache(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::CreateTable { .. }
        | PhysicalPlan::DropTable { .. }
        | PhysicalPlan::AlterTable { .. }
        | PhysicalPlan::Truncate { .. }
        | PhysicalPlan::CreateView { .. }
        | PhysicalPlan::DropView { .. }
        | PhysicalPlan::CreateSchema { .. }
        | PhysicalPlan::DropSchema { .. }
        | PhysicalPlan::UndropSchema { .. }
        | PhysicalPlan::AlterSchema { .. }
        | PhysicalPlan::CreateFunction { .. }
        | PhysicalPlan::DropFunction { .. }
        | PhysicalPlan::CreateProcedure { .. }
        | PhysicalPlan::DropProcedure { .. }
        | PhysicalPlan::CreateSnapshot { .. }
        | PhysicalPlan::DropSnapshot { .. } => true,

        PhysicalPlan::TableScan { .. }
        | PhysicalPlan::Sample { .. }
        | PhysicalPlan::Filter { .. }
        | PhysicalPlan::Project { .. }
        | PhysicalPlan::NestedLoopJoin { .. }
        | PhysicalPlan::CrossJoin { .. }
        | PhysicalPlan::HashJoin { .. }
        | PhysicalPlan::HashAggregate { .. }
        | PhysicalPlan::Sort { .. }
        | PhysicalPlan::Limit { .. }
        | PhysicalPlan::TopN { .. }
        | PhysicalPlan::Distinct { .. }
        | PhysicalPlan::Union { .. }
        | PhysicalPlan::Intersect { .. }
        | PhysicalPlan::Except { .. }
        | PhysicalPlan::Window { .. }
        | PhysicalPlan::Unnest { .. }
        | PhysicalPlan::Qualify { .. }
        | PhysicalPlan::WithCte { .. }
        | PhysicalPlan::Values { .. }
        | PhysicalPlan::Empty { .. }
        | PhysicalPlan::Insert { .. }
        | PhysicalPlan::Update { .. }
        | PhysicalPlan::Delete { .. }
        | PhysicalPlan::Merge { .. }
        | PhysicalPlan::Call { .. }
        | PhysicalPlan::ExportData { .. }
        | PhysicalPlan::LoadData { .. }
        | PhysicalPlan::Declare { .. }
        | PhysicalPlan::SetVariable { .. }
        | PhysicalPlan::SetMultipleVariables { .. }
        | PhysicalPlan::If { .. }
        | PhysicalPlan::While { .. }
        | PhysicalPlan::Loop { .. }
        | PhysicalPlan::Block { .. }
        | PhysicalPlan::Repeat { .. }
        | PhysicalPlan::For { .. }
        | PhysicalPlan::Return { .. }
        | PhysicalPlan::Raise { .. }
        | PhysicalPlan::ExecuteImmediate { .. }
        | PhysicalPlan::Break { .. }
        | PhysicalPlan::Continue { .. }
        | PhysicalPlan::Assert { .. }
        | PhysicalPlan::Grant { .. }
        | PhysicalPlan::Revoke { .. }
        | PhysicalPlan::BeginTransaction
        | PhysicalPlan::Commit
        | PhysicalPlan::Rollback
        | PhysicalPlan::TryCatch { .. }
        | PhysicalPlan::GapFill { .. }
        | PhysicalPlan::Explain { .. } => false,
    }
}
