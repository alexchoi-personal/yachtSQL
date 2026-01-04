#![coverage(off)]

use rustc_hash::{FxHashMap, FxHashSet};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::PlanSchema;
use yachtsql_storage::{Column, Table};

use super::ConcurrentPlanExecutor;
use crate::executor::plan_schema_to_schema;
use crate::plan::PhysicalPlan;

impl ConcurrentPlanExecutor {
    pub(crate) fn execute_union(
        &self,
        inputs: &[PhysicalPlan],
        all: bool,
        schema: &PlanSchema,
        parallel: bool,
    ) -> Result<Table> {
        use rayon::prelude::*;

        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);
        let mut seen: FxHashSet<Vec<Value>> = FxHashSet::default();

        let tables: Vec<Table> = if parallel && inputs.len() > 1 {
            inputs
                .par_iter()
                .map(|input| self.execute_plan(input))
                .collect::<Result<Vec<_>>>()?
        } else {
            let mut tables = Vec::with_capacity(inputs.len());
            for input in inputs {
                tables.push(self.execute_plan(input)?);
            }
            tables
        };

        for table in tables {
            let n = table.row_count();
            let columns: Vec<&Column> = table.columns().iter().map(|(_, c)| c.as_ref()).collect();
            for row_idx in 0..n {
                let values: Vec<Value> = columns.iter().map(|c| c.get_value(row_idx)).collect();
                if all || seen.insert(values.clone()) {
                    result.push_row(values)?;
                }
            }
        }

        Ok(result)
    }

    pub(crate) fn execute_intersect(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        all: bool,
        schema: &PlanSchema,
        parallel: bool,
    ) -> Result<Table> {
        let (left_table, right_table) = if parallel {
            let (l, r) = rayon::join(|| self.execute_plan(left), || self.execute_plan(right));
            (l?, r?)
        } else {
            (self.execute_plan(left)?, self.execute_plan(right)?)
        };
        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);

        let mut right_set: FxHashMap<Vec<Value>, usize> = FxHashMap::default();
        let right_n = right_table.row_count();
        let right_columns: Vec<&Column> = right_table
            .columns()
            .iter()
            .map(|(_, c)| c.as_ref())
            .collect();
        for row_idx in 0..right_n {
            let values: Vec<Value> = right_columns.iter().map(|c| c.get_value(row_idx)).collect();
            *right_set.entry(values).or_insert(0) += 1;
        }

        let mut seen: FxHashSet<Vec<Value>> = FxHashSet::default();
        let left_n = left_table.row_count();
        let left_columns: Vec<&Column> = left_table
            .columns()
            .iter()
            .map(|(_, c)| c.as_ref())
            .collect();
        for row_idx in 0..left_n {
            let values: Vec<Value> = left_columns.iter().map(|c| c.get_value(row_idx)).collect();
            if let Some(count) = right_set.get_mut(&values)
                && *count > 0
                && (all || seen.insert(values.clone()))
            {
                result.push_row(values)?;
                if all {
                    *count -= 1;
                }
            }
        }

        Ok(result)
    }

    pub(crate) fn execute_except(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        all: bool,
        schema: &PlanSchema,
        parallel: bool,
    ) -> Result<Table> {
        let (left_table, right_table) = if parallel {
            let (l, r) = rayon::join(|| self.execute_plan(left), || self.execute_plan(right));
            (l?, r?)
        } else {
            (self.execute_plan(left)?, self.execute_plan(right)?)
        };
        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);

        let mut right_set: FxHashMap<Vec<Value>, usize> = FxHashMap::default();
        let right_n = right_table.row_count();
        let right_columns: Vec<&Column> = right_table
            .columns()
            .iter()
            .map(|(_, c)| c.as_ref())
            .collect();
        for row_idx in 0..right_n {
            let values: Vec<Value> = right_columns.iter().map(|c| c.get_value(row_idx)).collect();
            *right_set.entry(values).or_insert(0) += 1;
        }

        let mut seen: FxHashSet<Vec<Value>> = FxHashSet::default();
        let left_n = left_table.row_count();
        let left_columns: Vec<&Column> = left_table
            .columns()
            .iter()
            .map(|(_, c)| c.as_ref())
            .collect();
        for row_idx in 0..left_n {
            let values: Vec<Value> = left_columns.iter().map(|c| c.get_value(row_idx)).collect();
            let in_right = right_set.get_mut(&values).map(|c| {
                if *c > 0 {
                    *c -= 1;
                    true
                } else {
                    false
                }
            });

            if !in_right.unwrap_or(false) && (all || seen.insert(values.clone())) {
                result.push_row(values)?;
            }
        }

        Ok(result)
    }
}
