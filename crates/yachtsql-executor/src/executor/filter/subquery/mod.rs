#![coverage(off)]

mod column_indices;
mod evaluation;
mod outer_refs;
mod substitution;
mod value_utils;

use yachtsql_common::error::Result;
use yachtsql_ir::Expr;
use yachtsql_storage::Table;

use super::PlanExecutor;

impl<'a> PlanExecutor<'a> {
    pub(super) fn execute_filter_with_subquery(
        &mut self,
        input: &Table,
        predicate: &Expr,
    ) -> Result<Table> {
        self.execute_filter_subquery_inner(input, predicate)
    }
}
