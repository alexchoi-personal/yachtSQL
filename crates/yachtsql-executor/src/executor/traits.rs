#![coverage(off)]

use async_trait::async_trait;
use yachtsql_common::error::Result;
use yachtsql_storage::Table;

use crate::plan::PhysicalPlan;

#[async_trait]
pub trait PlanRunner: Send {
    async fn run(&mut self, plan: &PhysicalPlan) -> Result<Table>;
}
