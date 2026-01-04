#![coverage(off)]

use std::num::NonZeroUsize;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use debug_print::debug_eprintln;
use lazy_static::lazy_static;
use regex::Regex;
use tracing::{debug, info, instrument};
use yachtsql_common::error::Result;
use yachtsql_ir::LogicalPlan;
use yachtsql_optimizer::OptimizerSettings;
use yachtsql_storage::Table;

use crate::concurrent_catalog::ConcurrentCatalog;
use crate::concurrent_session::ConcurrentSession;
use crate::executor::concurrent::ConcurrentPlanExecutor;
use crate::metrics::QueryMetrics;
use crate::physical_planner::PhysicalPlanner;
use crate::plan_cache::{CacheInvalidation, PlanCache, get_cache_invalidation};

const PLAN_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(10000).unwrap();

fn preprocess_range_types(sql: &str) -> String {
    lazy_static! {
        static ref RANGE_TYPE_RE: Regex =
            Regex::new(r"(?i)\bRANGE\s*<\s*(DATE|DATETIME|TIMESTAMP)\s*>")
                .expect("RANGE_TYPE_RE pattern is valid");
    }
    RANGE_TYPE_RE.replace_all(sql, "RANGE_$1").to_string()
}

fn is_cacheable_plan(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Scan { .. }
            | LogicalPlan::Sample { .. }
            | LogicalPlan::Filter { .. }
            | LogicalPlan::Project { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Distinct { .. }
            | LogicalPlan::SetOperation { .. }
            | LogicalPlan::Window { .. }
            | LogicalPlan::Unnest { .. }
            | LogicalPlan::Qualify { .. }
            | LogicalPlan::WithCte { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::Empty { .. }
            | LogicalPlan::GapFill { .. }
    )
}

pub struct AsyncQueryExecutor {
    catalog: Arc<ConcurrentCatalog>,
    session: Arc<ConcurrentSession>,
    plan_cache: Arc<RwLock<PlanCache>>,
    metrics: Arc<QueryMetrics>,
}

impl AsyncQueryExecutor {
    pub fn new() -> Self {
        Self {
            catalog: Arc::new(ConcurrentCatalog::new()),
            session: Arc::new(ConcurrentSession::new()),
            plan_cache: Arc::new(RwLock::new(PlanCache::new(PLAN_CACHE_SIZE))),
            metrics: Arc::new(QueryMetrics::new()),
        }
    }

    pub fn from_catalog_and_session(
        catalog: ConcurrentCatalog,
        session: ConcurrentSession,
    ) -> Self {
        Self {
            catalog: Arc::new(catalog),
            session: Arc::new(session),
            plan_cache: Arc::new(RwLock::new(PlanCache::new(PLAN_CACHE_SIZE))),
            metrics: Arc::new(QueryMetrics::new()),
        }
    }

    fn get_optimizer_settings(&self) -> OptimizerSettings {
        let table_stats = self.catalog.collect_table_stats();
        OptimizerSettings {
            join_reorder: self
                .session
                .get_variable("OPTIMIZER_JOIN_REORDER")
                .and_then(|v| v.as_bool())
                .unwrap_or(true),
            filter_pushdown: self
                .session
                .get_variable("OPTIMIZER_FILTER_PUSHDOWN")
                .and_then(|v| v.as_bool())
                .unwrap_or(true),
            projection_pushdown: self
                .session
                .get_variable("OPTIMIZER_PROJECTION_PUSHDOWN")
                .and_then(|v| v.as_bool())
                .unwrap_or(true),
            table_stats,
        }
    }

    pub fn clear_plan_cache(&self) {
        let mut cache = self.plan_cache.write().unwrap_or_else(|e| e.into_inner());
        cache.clear();
    }

    #[instrument(skip(self), fields(sql_length = sql.len()))]
    pub async fn execute_sql(&self, sql: &str) -> Result<Table> {
        let sql = preprocess_range_types(sql);
        debug!(sql = %sql, "Executing SQL query");
        let start = Instant::now();

        let logical = {
            let mut cache = self.plan_cache.write().unwrap_or_else(|e| e.into_inner());
            match cache.get(&sql) {
                Some(plan) => plan,
                None => {
                    let plan = yachtsql_parser::parse_and_plan(&sql, self)?;
                    if is_cacheable_plan(&plan) {
                        cache.insert(&sql, plan.clone());
                    }
                    plan
                }
            }
        };

        let settings = self.get_optimizer_settings();
        let physical = yachtsql_optimizer::optimize_with_settings(&logical, &settings)?;

        debug!("Query planned, executing");

        let planner = PhysicalPlanner::new(&self.catalog, &self.session);
        let executor_plan = planner.plan(&physical);
        let accesses = executor_plan.extract_table_accesses();

        let mut tables = self.catalog.acquire_table_locks(&accesses)?;
        tables.set_catalog(Arc::clone(&self.catalog));

        let executor = ConcurrentPlanExecutor::new(
            Arc::clone(&self.catalog),
            Arc::clone(&self.session),
            tables,
        );
        let (result, executor) = tokio::task::spawn_blocking(move || {
            let result = executor.execute_plan(&executor_plan);
            (result, executor)
        })
        .await
        .map_err(|e| yachtsql_common::error::Error::internal(e.to_string()))?;

        executor.tables.commit_writes();

        match get_cache_invalidation(&logical) {
            CacheInvalidation::All => {
                let mut cache = self.plan_cache.write().unwrap_or_else(|e| e.into_inner());
                cache.clear();
            }
            CacheInvalidation::Objects(objects) if !objects.is_empty() => {
                let mut cache = self.plan_cache.write().unwrap_or_else(|e| e.into_inner());
                cache.invalidate_objects(&objects);
            }
            _ => {}
        }

        let elapsed = start.elapsed();
        let is_error = result.is_err();
        self.metrics.record_query(elapsed, is_error);

        if let Ok(ref res) = result {
            info!(row_count = res.row_count(), "Query executed successfully");
        }

        if elapsed.as_millis() >= 1000 {
            debug_eprintln!(
                "[async_executor::execute_sql] Slow query detected: {:?} - SQL: {}",
                elapsed,
                &sql[..sql.len().min(100)]
            );
        }

        result
    }

    pub async fn execute_batch(&self, queries: Vec<String>) -> Vec<Result<Table>> {
        let mut results = Vec::with_capacity(queries.len());
        for sql in queries {
            results.push(self.execute_sql(&sql).await);
        }
        results
    }

    pub fn catalog(&self) -> &ConcurrentCatalog {
        &self.catalog
    }

    pub fn session(&self) -> &ConcurrentSession {
        &self.session
    }

    pub fn metrics(&self) -> &QueryMetrics {
        &self.metrics
    }

    pub fn with_slow_query_threshold(mut self, threshold_ms: u64) -> Self {
        self.metrics = Arc::new(QueryMetrics::new().with_slow_query_threshold(threshold_ms));
        self
    }
}

impl Clone for AsyncQueryExecutor {
    fn clone(&self) -> Self {
        Self {
            catalog: Arc::clone(&self.catalog),
            session: Arc::clone(&self.session),
            plan_cache: Arc::clone(&self.plan_cache),
            metrics: Arc::clone(&self.metrics),
        }
    }
}

impl Default for AsyncQueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl yachtsql_parser::CatalogProvider for AsyncQueryExecutor {
    fn get_table_schema(&self, name: &str) -> Option<yachtsql_storage::Schema> {
        self.catalog.get_table_schema(name)
    }

    fn get_view(&self, name: &str) -> Option<yachtsql_parser::ViewDefinition> {
        self.catalog
            .get_view(name)
            .map(|v| yachtsql_parser::ViewDefinition {
                query: v.query,
                column_aliases: v.column_aliases,
            })
    }

    fn get_function(&self, name: &str) -> Option<yachtsql_parser::FunctionDefinition> {
        self.catalog
            .get_function(name)
            .map(|f| yachtsql_parser::FunctionDefinition {
                name: f.name.clone(),
                parameters: f.parameters.clone(),
                return_type: f.return_type.clone(),
                body: f.body.clone(),
                is_aggregate: f.is_aggregate,
            })
    }
}
