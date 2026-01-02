#![coverage(off)]

mod subquery;

use yachtsql_common::error::Result;
use yachtsql_ir::Expr;
use yachtsql_storage::Table;

use super::PlanExecutor;
use crate::columnar_evaluator::ColumnarEvaluator;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub(crate) fn execute_filter(
        &mut self,
        input: &PhysicalPlan,
        predicate: &Expr,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let schema = input_table.schema().clone();

        if Self::expr_contains_subquery(predicate) {
            self.execute_filter_with_subquery(&input_table, predicate)
        } else {
            let evaluator = ColumnarEvaluator::new(&schema)
                .with_variables(&self.variables)
                .with_system_variables(self.session.system_variables())
                .with_user_functions(&self.user_function_defs);

            let mask = evaluator.evaluate(predicate, &input_table)?;
            Ok(input_table.filter_by_mask(&mask))
        }
    }

    pub fn expr_contains_subquery(expr: &Expr) -> bool {
        match expr {
            Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::Subquery(_)
            | Expr::ScalarSubquery(_)
            | Expr::ArraySubquery(_) => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_contains_subquery(left) || Self::expr_contains_subquery(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_contains_subquery(expr),
            Expr::ScalarFunction { args, .. } => args.iter().any(Self::expr_contains_subquery),
            _ => false,
        }
    }
}
