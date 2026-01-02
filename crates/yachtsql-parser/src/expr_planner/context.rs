#![coverage(off)]

use sqlparser::ast;
use yachtsql_ir::PlanSchema;

use super::{SubqueryPlannerFn, UdfResolverFn};

#[allow(dead_code)]
pub struct ExprPlanningContext<'a> {
    pub schema: &'a PlanSchema,
    pub subquery_planner: Option<SubqueryPlannerFn<'a>>,
    pub named_windows: &'a [ast::NamedWindowDefinition],
    pub udf_resolver: Option<UdfResolverFn<'a>>,
}

#[allow(dead_code)]
impl<'a> ExprPlanningContext<'a> {
    pub fn new(schema: &'a PlanSchema) -> Self {
        Self {
            schema,
            subquery_planner: None,
            named_windows: &[],
            udf_resolver: None,
        }
    }

    pub fn with_subquery_planner(mut self, planner: SubqueryPlannerFn<'a>) -> Self {
        self.subquery_planner = Some(planner);
        self
    }

    pub fn with_named_windows(mut self, windows: &'a [ast::NamedWindowDefinition]) -> Self {
        self.named_windows = windows;
        self
    }

    pub fn with_udf_resolver(mut self, resolver: UdfResolverFn<'a>) -> Self {
        self.udf_resolver = Some(resolver);
        self
    }
}
