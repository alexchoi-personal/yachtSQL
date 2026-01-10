#![coverage(off)]

mod expr_converter;
mod plan_converter;

use datafusion::common::Result as DFResult;
use datafusion::logical_expr::LogicalPlan as DFLogicalPlan;
use datafusion::prelude::Expr as DFExpr;
pub use expr_converter::convert_expr;
pub use plan_converter::convert_plan;
use yachtsql_ir::{Expr, LogicalPlan};

pub struct DataFusionConverter;

impl DataFusionConverter {
    pub fn convert_expr(expr: &Expr) -> DFResult<DFExpr> {
        expr_converter::convert_expr(expr)
    }

    pub fn convert_plan(plan: &LogicalPlan) -> DFResult<DFLogicalPlan> {
        plan_converter::convert_plan(plan)
    }
}
