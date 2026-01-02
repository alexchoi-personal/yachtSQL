#![coverage(off)]

use std::collections::{HashMap, HashSet};

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::PlanSchema;
use yachtsql_storage::Table;

use super::{PlanExecutor, plan_schema_to_schema};
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub(crate) fn execute_union(
        &mut self,
        inputs: &[PhysicalPlan],
        all: bool,
        schema: &PlanSchema,
    ) -> Result<Table> {
        let result_schema = plan_schema_to_schema(schema);

        if inputs.is_empty() {
            return Ok(Table::empty(result_schema));
        }

        let tables: Vec<Table> = inputs
            .iter()
            .map(|input| self.execute_plan(input))
            .collect::<Result<_>>()?;

        if all {
            let table_refs: Vec<&Table> = tables.iter().collect();
            Ok(Table::concat_tables(result_schema, &table_refs))
        } else {
            let table_refs: Vec<&Table> = tables.iter().collect();
            let combined = Table::concat_tables(result_schema, &table_refs);
            self.deduplicate_table(&combined)
        }
    }

    fn deduplicate_table(&self, table: &Table) -> Result<Table> {
        let n = table.row_count();
        if n == 0 {
            return Ok(table.clone());
        }

        let columns: Vec<_> = table.columns().iter().map(|(_, c)| c).collect();
        let mut seen: HashSet<Vec<RowKey>> = HashSet::new();
        let mut unique_indices = Vec::new();

        for i in 0..n {
            let key: Vec<RowKey> = columns
                .iter()
                .map(|c| RowKey::from(&c.get_value(i)))
                .collect();
            if seen.insert(key) {
                unique_indices.push(i);
            }
        }

        Ok(table.gather_rows(&unique_indices))
    }

    pub(crate) fn execute_intersect(
        &mut self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        all: bool,
        schema: &PlanSchema,
    ) -> Result<Table> {
        let left_table = self.execute_plan(left)?;
        let right_table = self.execute_plan(right)?;
        let left_n = left_table.row_count();
        let right_n = right_table.row_count();

        let _result_schema = plan_schema_to_schema(schema);

        let left_columns: Vec<_> = left_table.columns().iter().map(|(_, c)| c).collect();
        let right_columns: Vec<_> = right_table.columns().iter().map(|(_, c)| c).collect();

        if all {
            let mut right_counts: HashMap<Vec<RowKey>, usize> = HashMap::new();
            for i in 0..right_n {
                let key: Vec<RowKey> = right_columns
                    .iter()
                    .map(|c| RowKey::from(&c.get_value(i)))
                    .collect();
                *right_counts.entry(key).or_insert(0) += 1;
            }

            let mut result_indices = Vec::new();
            for i in 0..left_n {
                let key: Vec<RowKey> = left_columns
                    .iter()
                    .map(|c| RowKey::from(&c.get_value(i)))
                    .collect();
                if let Some(count) = right_counts.get_mut(&key)
                    && *count > 0
                {
                    result_indices.push(i);
                    *count -= 1;
                }
            }

            Ok(left_table.gather_rows(&result_indices))
        } else {
            let right_rows: HashSet<Vec<RowKey>> = (0..right_n)
                .map(|i| {
                    right_columns
                        .iter()
                        .map(|c| RowKey::from(&c.get_value(i)))
                        .collect()
                })
                .collect();

            let mut seen: HashSet<Vec<RowKey>> = HashSet::new();
            let mut result_indices = Vec::new();
            for i in 0..left_n {
                let key: Vec<RowKey> = left_columns
                    .iter()
                    .map(|c| RowKey::from(&c.get_value(i)))
                    .collect();
                if right_rows.contains(&key) && seen.insert(key) {
                    result_indices.push(i);
                }
            }

            Ok(left_table.gather_rows(&result_indices))
        }
    }

    pub(crate) fn execute_except(
        &mut self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        all: bool,
        schema: &PlanSchema,
    ) -> Result<Table> {
        let left_table = self.execute_plan(left)?;
        let right_table = self.execute_plan(right)?;
        let left_n = left_table.row_count();
        let right_n = right_table.row_count();

        let _result_schema = plan_schema_to_schema(schema);

        let left_columns: Vec<_> = left_table.columns().iter().map(|(_, c)| c).collect();
        let right_columns: Vec<_> = right_table.columns().iter().map(|(_, c)| c).collect();

        if all {
            let mut right_counts: HashMap<Vec<RowKey>, usize> = HashMap::new();
            for i in 0..right_n {
                let key: Vec<RowKey> = right_columns
                    .iter()
                    .map(|c| RowKey::from(&c.get_value(i)))
                    .collect();
                *right_counts.entry(key).or_insert(0) += 1;
            }

            let mut result_indices = Vec::new();
            for i in 0..left_n {
                let key: Vec<RowKey> = left_columns
                    .iter()
                    .map(|c| RowKey::from(&c.get_value(i)))
                    .collect();
                if let Some(count) = right_counts.get_mut(&key)
                    && *count > 0
                {
                    *count -= 1;
                    continue;
                }
                result_indices.push(i);
            }

            Ok(left_table.gather_rows(&result_indices))
        } else {
            let right_rows: HashSet<Vec<RowKey>> = (0..right_n)
                .map(|i| {
                    right_columns
                        .iter()
                        .map(|c| RowKey::from(&c.get_value(i)))
                        .collect()
                })
                .collect();

            let mut seen: HashSet<Vec<RowKey>> = HashSet::new();
            let mut result_indices = Vec::new();
            for i in 0..left_n {
                let key: Vec<RowKey> = left_columns
                    .iter()
                    .map(|c| RowKey::from(&c.get_value(i)))
                    .collect();
                if !right_rows.contains(&key) && seen.insert(key) {
                    result_indices.push(i);
                }
            }

            Ok(left_table.gather_rows(&result_indices))
        }
    }
}

#[derive(Hash, Eq, PartialEq)]
enum RowKey {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(u64),
    String(String),
    Bytes(Vec<u8>),
    Other(String),
}

impl From<&Value> for RowKey {
    fn from(v: &Value) -> Self {
        match v {
            Value::Null => RowKey::Null,
            Value::Bool(b) => RowKey::Bool(*b),
            Value::Int64(n) => RowKey::Int64(*n),
            Value::Float64(f) => RowKey::Float64(f.0.to_bits()),
            Value::String(s) => RowKey::String(s.clone()),
            Value::Bytes(b) => RowKey::Bytes(b.clone()),
            _ => RowKey::Other(format!("{:?}", v)),
        }
    }
}
