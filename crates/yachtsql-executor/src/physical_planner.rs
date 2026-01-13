#![coverage(off)]

use crate::concurrent_catalog::ConcurrentCatalog;
use crate::concurrent_session::ConcurrentSession;
use crate::plan::{
    BoundType, ExecutionHints, PARALLEL_ROW_THRESHOLD, PhysicalPlan, PhysicalPlanExt,
};

pub struct PhysicalPlanner<'a> {
    catalog: &'a ConcurrentCatalog,
    session: &'a ConcurrentSession,
}

impl<'a> PhysicalPlanner<'a> {
    pub fn new(catalog: &'a ConcurrentCatalog, session: &'a ConcurrentSession) -> Self {
        Self { catalog, session }
    }

    pub(crate) fn plan(&self, logical: &PhysicalPlan) -> PhysicalPlan {
        let mut plan = logical.clone();
        plan.populate_row_counts(self.catalog);
        self.compute_hints(&mut plan);
        plan
    }

    fn is_parallel_enabled(&self) -> bool {
        if let Some(val) = self.session.get_variable("PARALLEL_EXECUTION") {
            return val.as_bool().unwrap_or(true);
        }
        if let Some(val) = self.session.get_system_variable("PARALLEL_EXECUTION") {
            return val.as_bool().unwrap_or(true);
        }
        match std::env::var("YACHTSQL_PARALLEL_EXECUTION") {
            Ok(val) => !val.eq_ignore_ascii_case("false") && val != "0",
            Err(_) => true,
        }
    }

    fn get_parallel_row_threshold(&self) -> u64 {
        if let Some(val) = self.session.get_variable("PARALLEL_ROW_THRESHOLD")
            && let Some(n) = val.as_i64()
        {
            return n.max(0) as u64;
        }
        PARALLEL_ROW_THRESHOLD
    }

    fn compute_hints(&self, plan: &mut PhysicalPlan) {
        let parallel_enabled = self.is_parallel_enabled();
        let row_threshold = self.get_parallel_row_threshold();
        self.compute_hints_recursive(plan, parallel_enabled, row_threshold);
    }

    fn compute_hints_recursive(
        &self,
        plan: &mut PhysicalPlan,
        parallel_enabled: bool,
        row_threshold: u64,
    ) {
        match plan {
            PhysicalPlan::NestedLoopJoin {
                left, right, hints, ..
            }
            | PhysicalPlan::HashJoin {
                left, right, hints, ..
            } => {
                self.compute_hints_recursive(left, parallel_enabled, row_threshold);
                self.compute_hints_recursive(right, parallel_enabled, row_threshold);
                *hints = self.binary_join_hints(left, right, parallel_enabled, row_threshold);
            }

            PhysicalPlan::CrossJoin {
                left, right, hints, ..
            } => {
                self.compute_hints_recursive(left, parallel_enabled, row_threshold);
                self.compute_hints_recursive(right, parallel_enabled, row_threshold);
                let bound = Self::binary_bound_type(left, right);
                let should_parallelize =
                    left.estimate_rows() >= row_threshold && right.estimate_rows() >= row_threshold;
                *hints = ExecutionHints {
                    parallel: parallel_enabled && should_parallelize && bound == BoundType::Compute,
                    bound_type: bound,
                    estimated_rows: left.estimate_rows().saturating_mul(right.estimate_rows()),
                };
            }

            PhysicalPlan::Intersect {
                left, right, hints, ..
            } => {
                self.compute_hints_recursive(left, parallel_enabled, row_threshold);
                self.compute_hints_recursive(right, parallel_enabled, row_threshold);
                let bound = Self::binary_bound_type(left, right);
                let should_parallelize =
                    left.estimate_rows() >= row_threshold && right.estimate_rows() >= row_threshold;
                *hints = ExecutionHints {
                    parallel: parallel_enabled && should_parallelize && bound == BoundType::Compute,
                    bound_type: bound,
                    estimated_rows: left.estimate_rows().min(right.estimate_rows()),
                };
            }

            PhysicalPlan::Except {
                left, right, hints, ..
            } => {
                self.compute_hints_recursive(left, parallel_enabled, row_threshold);
                self.compute_hints_recursive(right, parallel_enabled, row_threshold);
                let bound = Self::binary_bound_type(left, right);
                let should_parallelize =
                    left.estimate_rows() >= row_threshold && right.estimate_rows() >= row_threshold;
                *hints = ExecutionHints {
                    parallel: parallel_enabled && should_parallelize && bound == BoundType::Compute,
                    bound_type: bound,
                    estimated_rows: left.estimate_rows(),
                };
            }

            PhysicalPlan::Union { inputs, hints, .. } => {
                for input in inputs.iter_mut() {
                    self.compute_hints_recursive(input, parallel_enabled, row_threshold);
                }
                let bound = Self::union_bound_type(inputs);
                let should_parallelize = inputs.len() >= 2
                    && inputs
                        .iter()
                        .filter(|p| p.estimate_rows() >= row_threshold)
                        .count()
                        >= 2;
                *hints = ExecutionHints {
                    parallel: parallel_enabled && should_parallelize && bound == BoundType::Compute,
                    bound_type: bound,
                    estimated_rows: inputs.iter().map(|p| p.estimate_rows()).sum(),
                };
            }

            PhysicalPlan::HashAggregate { input, hints, .. }
            | PhysicalPlan::Window { input, hints, .. }
            | PhysicalPlan::Sort { input, hints, .. } => {
                self.compute_hints_recursive(input, parallel_enabled, row_threshold);
                *hints = ExecutionHints {
                    parallel: false,
                    bound_type: BoundType::Compute,
                    estimated_rows: input.estimate_rows(),
                };
            }

            PhysicalPlan::WithCte {
                ctes,
                body,
                parallel_ctes,
                hints,
            } => {
                self.compute_hints_recursive(body, parallel_enabled, row_threshold);
                *parallel_ctes = self.compute_cte_parallelism(ctes, parallel_enabled);
                *hints = ExecutionHints {
                    parallel: !parallel_ctes.is_empty(),
                    bound_type: body.bound_type(),
                    estimated_rows: body.estimate_rows(),
                };
            }

            PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::TopN { input, .. }
            | PhysicalPlan::Sample { input, .. }
            | PhysicalPlan::Distinct { input, .. }
            | PhysicalPlan::Qualify { input, .. }
            | PhysicalPlan::Unnest { input, .. } => {
                self.compute_hints_recursive(input, parallel_enabled, row_threshold);
            }

            PhysicalPlan::Insert { source, .. } => {
                self.compute_hints_recursive(source, parallel_enabled, row_threshold);
            }

            PhysicalPlan::Update {
                from: Some(from), ..
            } => {
                self.compute_hints_recursive(from, parallel_enabled, row_threshold);
            }

            PhysicalPlan::Merge { source, .. } => {
                self.compute_hints_recursive(source, parallel_enabled, row_threshold);
            }

            _ => {}
        }
    }

    fn binary_join_hints(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        parallel_enabled: bool,
        row_threshold: u64,
    ) -> ExecutionHints {
        let bound = Self::binary_bound_type(left, right);
        let should_parallelize =
            left.estimate_rows() >= row_threshold && right.estimate_rows() >= row_threshold;
        ExecutionHints {
            parallel: parallel_enabled && should_parallelize && bound == BoundType::Compute,
            bound_type: bound,
            estimated_rows: left.estimate_rows().saturating_add(right.estimate_rows()),
        }
    }

    fn compute_cte_parallelism(
        &self,
        ctes: &[yachtsql_ir::CteDefinition],
        parallel_enabled: bool,
    ) -> Vec<usize> {
        let row_threshold = self.get_parallel_row_threshold();
        if !parallel_enabled {
            return vec![];
        }
        ctes.iter()
            .enumerate()
            .filter(|(_, cte)| !cte.recursive)
            .filter(|(_, cte)| {
                if let Ok(mut plan) = yachtsql_optimizer::optimize(&cte.query) {
                    plan.populate_row_counts(self.catalog);
                    plan.bound_type() == BoundType::Compute && plan.estimate_rows() >= row_threshold
                } else {
                    false
                }
            })
            .map(|(i, _)| i)
            .collect()
    }

    fn binary_bound_type(_left: &PhysicalPlan, _right: &PhysicalPlan) -> BoundType {
        BoundType::Compute
    }

    fn union_bound_type(inputs: &[PhysicalPlan]) -> BoundType {
        if inputs.len() >= 2 {
            BoundType::Compute
        } else {
            BoundType::Memory
        }
    }
}
