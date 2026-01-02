#![coverage(off)]

use sqlparser::ast::{TableSampleKind, TableSampleUnit};
use yachtsql_common::error::{Error, Result};
use yachtsql_ir::{LogicalPlan, SampleType};

use super::Planner;
use crate::CatalogProvider;

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub(super) fn apply_sample(
        &self,
        plan: LogicalPlan,
        sample: &Option<TableSampleKind>,
    ) -> Result<LogicalPlan> {
        let sample_spec = match sample {
            Some(TableSampleKind::BeforeTableAlias(s)) => s,
            Some(TableSampleKind::AfterTableAlias(s)) => s,
            None => return Ok(plan),
        };

        let quantity = match &sample_spec.quantity {
            Some(q) => q,
            None => return Ok(plan),
        };

        let sample_value = self.evaluate_sample_value(&quantity.value)?;

        let sample_type = match &quantity.unit {
            Some(TableSampleUnit::Rows) => SampleType::Rows,
            Some(TableSampleUnit::Percent) | None => SampleType::Percent,
        };

        Ok(LogicalPlan::Sample {
            input: Box::new(plan),
            sample_type,
            sample_value,
        })
    }

    fn evaluate_sample_value(&self, expr: &sqlparser::ast::Expr) -> Result<i64> {
        match expr {
            sqlparser::ast::Expr::Value(v) => match &v.value {
                sqlparser::ast::Value::Number(n, _) => n
                    .parse::<i64>()
                    .map_err(|_| Error::InvalidQuery(format!("Invalid sample value: {}", n))),
                _ => Err(Error::InvalidQuery("Sample value must be a number".into())),
            },
            _ => Err(Error::InvalidQuery("Sample value must be a number".into())),
        }
    }
}
