#![coverage(off)]

mod extensions;

use std::collections::BTreeMap;

pub use extensions::PhysicalPlanExt;
pub use yachtsql_optimizer::{
    BoundType, ExecutionHints, PARALLEL_ROW_THRESHOLD, PhysicalPlan, SampleType,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessType {
    Read,
    Write,
    WriteOptional,
}

#[derive(Debug, Clone, Default)]
pub struct TableAccessSet {
    pub accesses: BTreeMap<String, AccessType>,
}

impl TableAccessSet {
    pub fn new() -> Self {
        Self {
            accesses: BTreeMap::new(),
        }
    }

    pub fn add_read(&mut self, table_name: String) {
        self.accesses.entry(table_name).or_insert(AccessType::Read);
    }

    pub fn add_write(&mut self, table_name: String) {
        self.accesses.insert(table_name, AccessType::Write);
    }

    pub fn add_write_optional(&mut self, table_name: String) {
        self.accesses
            .entry(table_name)
            .or_insert(AccessType::WriteOptional);
    }

    pub fn is_empty(&self) -> bool {
        self.accesses.is_empty()
    }
}
