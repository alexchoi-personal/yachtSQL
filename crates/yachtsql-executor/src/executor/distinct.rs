#![coverage(off)]

use std::collections::HashSet;

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_storage::Table;

use super::PlanExecutor;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub(crate) fn execute_distinct(&mut self, input: &PhysicalPlan) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let n = input_table.row_count();

        if n == 0 {
            return Ok(input_table);
        }

        let columns: Vec<_> = input_table.columns().iter().map(|(_, c)| c).collect();
        let mut seen: HashSet<Vec<ValueKey>> = HashSet::new();
        let mut unique_indices = Vec::new();

        for i in 0..n {
            let key: Vec<ValueKey> = columns
                .iter()
                .map(|c| ValueKey::from(&c.get_value(i)))
                .collect();
            if seen.insert(key) {
                unique_indices.push(i);
            }
        }

        Ok(input_table.gather_rows(&unique_indices))
    }
}

#[derive(Hash, Eq, PartialEq)]
enum ValueKey {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(u64),
    String(String),
    Bytes(Vec<u8>),
    Other(String),
}

impl From<&Value> for ValueKey {
    fn from(v: &Value) -> Self {
        match v {
            Value::Null => ValueKey::Null,
            Value::Bool(b) => ValueKey::Bool(*b),
            Value::Int64(n) => ValueKey::Int64(*n),
            Value::Float64(f) => ValueKey::Float64(f.0.to_bits()),
            Value::String(s) => ValueKey::String(s.clone()),
            Value::Bytes(b) => ValueKey::Bytes(b.clone()),
            _ => ValueKey::Other(format!("{:?}", v)),
        }
    }
}
