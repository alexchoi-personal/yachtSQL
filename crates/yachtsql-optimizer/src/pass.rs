use yachtsql_common::error::Result;
use yachtsql_ir::LogicalPlan;

use crate::PhysicalPlan;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PassTarget {
    Logical,
    Physical,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PassOverhead {
    Negligible,
    Low,
    Medium,
    High,
}

pub trait OptimizationPass: Send + Sync {
    fn name(&self) -> &'static str;

    fn target(&self) -> PassTarget;

    fn overhead(&self) -> PassOverhead;

    fn dependencies(&self) -> &'static [&'static str] {
        &[]
    }

    fn apply_logical(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        Ok(plan)
    }

    fn apply_physical(&self, plan: PhysicalPlan) -> Result<PhysicalPlan> {
        Ok(plan)
    }
}
