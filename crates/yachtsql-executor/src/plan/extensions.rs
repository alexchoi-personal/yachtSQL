#![coverage(off)]

use std::collections::HashSet;

use yachtsql_optimizer::{PARALLEL_ROW_THRESHOLD, PhysicalPlan};

use super::{AccessType, TableAccessSet};
use crate::concurrent_catalog::ConcurrentCatalog;

pub trait PhysicalPlanExt {
    fn extract_table_accesses(&self) -> TableAccessSet;
    fn populate_row_counts(&mut self, catalog: &ConcurrentCatalog);
}

impl PhysicalPlanExt for PhysicalPlan {
    fn extract_table_accesses(&self) -> TableAccessSet {
        let mut accesses = TableAccessSet::new();
        let mut cte_names = HashSet::new();
        collect_accesses(self, &mut accesses, &mut cte_names);
        accesses
    }

    fn populate_row_counts(&mut self, catalog: &ConcurrentCatalog) {
        populate_row_counts_impl(self, catalog);
    }
}

fn collect_accesses(
    plan: &PhysicalPlan,
    accesses: &mut TableAccessSet,
    cte_names: &mut HashSet<String>,
) {
    match plan {
        PhysicalPlan::TableScan { table_name, .. } => {
            let table_upper = table_name.to_uppercase();
            if !cte_names.contains(&table_upper) {
                accesses.add_read(table_name.clone());
            }
        }

        PhysicalPlan::Sample { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::TopN { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::Window { input, .. }
        | PhysicalPlan::Unnest { input, .. }
        | PhysicalPlan::Qualify { input, .. }
        | PhysicalPlan::HashAggregate { input, .. } => {
            collect_accesses(input, accesses, cte_names);
        }

        PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::CrossJoin { left, right, .. }
        | PhysicalPlan::HashJoin { left, right, .. }
        | PhysicalPlan::Intersect { left, right, .. }
        | PhysicalPlan::Except { left, right, .. } => {
            collect_accesses(left, accesses, cte_names);
            collect_accesses(right, accesses, cte_names);
        }

        PhysicalPlan::Union { inputs, .. } => {
            for input in inputs {
                collect_accesses(input, accesses, cte_names);
            }
        }

        PhysicalPlan::WithCte { ctes, body, .. } => {
            for cte in ctes {
                cte_names.insert(cte.name.to_uppercase());
                if let Ok(physical_cte) = yachtsql_optimizer::optimize(&cte.query) {
                    collect_accesses(&physical_cte, accesses, cte_names);
                }
            }
            collect_accesses(body, accesses, cte_names);
        }

        PhysicalPlan::Insert {
            table_name, source, ..
        } => {
            accesses.add_write(table_name.clone());
            collect_accesses(source, accesses, cte_names);
        }

        PhysicalPlan::Update { table_name, .. } => {
            accesses.add_write(table_name.clone());
        }

        PhysicalPlan::Delete { table_name, .. } => {
            accesses.add_write(table_name.clone());
        }

        PhysicalPlan::Merge {
            target_table,
            source,
            ..
        } => {
            accesses.add_write(target_table.clone());
            collect_accesses(source, accesses, cte_names);
        }

        PhysicalPlan::Truncate { table_name } => {
            accesses.add_write(table_name.clone());
        }

        PhysicalPlan::AlterTable {
            table_name,
            if_exists,
            ..
        } => {
            if *if_exists {
                accesses.add_write_optional(table_name.clone());
            } else {
                accesses.add_write(table_name.clone());
            }
        }

        PhysicalPlan::LoadData {
            table_name,
            temp_table,
            ..
        } => {
            if *temp_table {
                accesses.add_write_optional(table_name.clone());
            } else {
                accesses.add_write(table_name.clone());
            }
        }

        PhysicalPlan::CreateSnapshot { source_name, .. } => {
            accesses.add_read(source_name.clone());
        }

        PhysicalPlan::CreateView { query, .. } => {
            collect_accesses(query, accesses, cte_names);
        }

        PhysicalPlan::ExportData { query, .. } => {
            collect_accesses(query, accesses, cte_names);
        }

        PhysicalPlan::For { query, body, .. } => {
            collect_accesses(query, accesses, cte_names);
            for stmt in body {
                collect_accesses(stmt, accesses, cte_names);
            }
        }

        PhysicalPlan::If {
            then_branch,
            else_branch,
            ..
        } => {
            for stmt in then_branch {
                collect_accesses(stmt, accesses, cte_names);
            }
            if let Some(else_stmts) = else_branch {
                for stmt in else_stmts {
                    collect_accesses(stmt, accesses, cte_names);
                }
            }
        }

        PhysicalPlan::While { body, .. }
        | PhysicalPlan::Loop { body, .. }
        | PhysicalPlan::Block { body, .. }
        | PhysicalPlan::Repeat { body, .. } => {
            for stmt in body {
                collect_accesses(stmt, accesses, cte_names);
            }
        }

        PhysicalPlan::CreateProcedure { body, .. } => {
            for stmt in body {
                collect_accesses(stmt, accesses, cte_names);
            }
        }

        PhysicalPlan::CreateTable { query, .. } => {
            if let Some(q) = query {
                collect_accesses(q, accesses, cte_names);
            }
        }

        PhysicalPlan::DropTable { .. }
        | PhysicalPlan::DropView { .. }
        | PhysicalPlan::CreateSchema { .. }
        | PhysicalPlan::DropSchema { .. }
        | PhysicalPlan::UndropSchema { .. }
        | PhysicalPlan::AlterSchema { .. }
        | PhysicalPlan::CreateFunction { .. }
        | PhysicalPlan::DropFunction { .. }
        | PhysicalPlan::DropProcedure { .. }
        | PhysicalPlan::Call { .. }
        | PhysicalPlan::Declare { .. }
        | PhysicalPlan::SetVariable { .. }
        | PhysicalPlan::SetMultipleVariables { .. }
        | PhysicalPlan::Return { .. }
        | PhysicalPlan::Raise { .. }
        | PhysicalPlan::ExecuteImmediate { .. }
        | PhysicalPlan::Break { .. }
        | PhysicalPlan::Continue { .. }
        | PhysicalPlan::DropSnapshot { .. }
        | PhysicalPlan::Assert { .. }
        | PhysicalPlan::Grant { .. }
        | PhysicalPlan::Revoke { .. }
        | PhysicalPlan::BeginTransaction
        | PhysicalPlan::Commit
        | PhysicalPlan::Rollback
        | PhysicalPlan::TryCatch { .. }
        | PhysicalPlan::Values { .. }
        | PhysicalPlan::Empty { .. }
        | PhysicalPlan::GapFill { .. }
        | PhysicalPlan::Explain { .. } => {}
    }
}

fn populate_row_counts_impl(plan: &mut PhysicalPlan, catalog: &ConcurrentCatalog) {
    match plan {
        PhysicalPlan::TableScan {
            table_name,
            row_count,
            ..
        } => {
            if let Some(handle) = catalog.get_table_handle(table_name) {
                *row_count = Some(handle.read().row_count() as u64);
            }
        }
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::TopN { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::Window { input, .. }
        | PhysicalPlan::Unnest { input, .. }
        | PhysicalPlan::Qualify { input, .. }
        | PhysicalPlan::Sample { input, .. }
        | PhysicalPlan::GapFill { input, .. } => {
            populate_row_counts_impl(input, catalog);
        }
        PhysicalPlan::HashAggregate { input, .. } => {
            populate_row_counts_impl(input, catalog);
        }
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            parallel,
            ..
        }
        | PhysicalPlan::CrossJoin {
            left,
            right,
            parallel,
            ..
        }
        | PhysicalPlan::HashJoin {
            left,
            right,
            parallel,
            ..
        } => {
            populate_row_counts_impl(left, catalog);
            populate_row_counts_impl(right, catalog);
            *parallel = PhysicalPlan::should_parallelize(left, right);
        }
        PhysicalPlan::Union {
            inputs, parallel, ..
        } => {
            for input in inputs.iter_mut() {
                populate_row_counts_impl(input, catalog);
            }
            *parallel = PhysicalPlan::should_parallelize_union(inputs);
        }
        PhysicalPlan::Intersect {
            left,
            right,
            parallel,
            ..
        }
        | PhysicalPlan::Except {
            left,
            right,
            parallel,
            ..
        } => {
            populate_row_counts_impl(left, catalog);
            populate_row_counts_impl(right, catalog);
            *parallel = PhysicalPlan::should_parallelize(left, right);
        }
        PhysicalPlan::WithCte {
            ctes,
            body,
            parallel_ctes,
            ..
        } => {
            populate_row_counts_impl(body, catalog);
            *parallel_ctes = ctes
                .iter()
                .enumerate()
                .filter(|(_, cte)| !cte.recursive)
                .filter(|(_, cte)| {
                    if let Ok(mut optimized) = yachtsql_optimizer::optimize(&cte.query) {
                        populate_row_counts_impl(&mut optimized, catalog);
                        optimized.estimate_rows() >= PARALLEL_ROW_THRESHOLD
                    } else {
                        false
                    }
                })
                .map(|(i, _)| i)
                .collect();
        }
        PhysicalPlan::Values { .. }
        | PhysicalPlan::Empty { .. }
        | PhysicalPlan::CreateTable { .. }
        | PhysicalPlan::DropTable { .. }
        | PhysicalPlan::AlterTable { .. }
        | PhysicalPlan::Truncate { .. }
        | PhysicalPlan::Insert { .. }
        | PhysicalPlan::Update { .. }
        | PhysicalPlan::Delete { .. }
        | PhysicalPlan::Merge { .. }
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
        | PhysicalPlan::SetVariable { .. }
        | PhysicalPlan::SetMultipleVariables { .. }
        | PhysicalPlan::BeginTransaction
        | PhysicalPlan::Commit
        | PhysicalPlan::Rollback
        | PhysicalPlan::ExportData { .. }
        | PhysicalPlan::LoadData { .. }
        | PhysicalPlan::CreateSnapshot { .. }
        | PhysicalPlan::DropSnapshot { .. }
        | PhysicalPlan::Grant { .. }
        | PhysicalPlan::Revoke { .. }
        | PhysicalPlan::Raise { .. }
        | PhysicalPlan::Block { .. }
        | PhysicalPlan::If { .. }
        | PhysicalPlan::While { .. }
        | PhysicalPlan::Loop { .. }
        | PhysicalPlan::Repeat { .. }
        | PhysicalPlan::For { .. }
        | PhysicalPlan::Declare { .. }
        | PhysicalPlan::Return { .. }
        | PhysicalPlan::ExecuteImmediate { .. }
        | PhysicalPlan::Break { .. }
        | PhysicalPlan::Continue { .. }
        | PhysicalPlan::TryCatch { .. }
        | PhysicalPlan::Assert { .. }
        | PhysicalPlan::Explain { .. } => {}
    }
}
