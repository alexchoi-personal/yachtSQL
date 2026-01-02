#![coverage(off)]

use rand::Rng;
use yachtsql_common::error::Result;
use yachtsql_optimizer::SampleType;
use yachtsql_storage::Table;

use super::PlanExecutor;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub(crate) fn execute_sample(
        &mut self,
        input: &PhysicalPlan,
        sample_type: &SampleType,
        sample_value: i64,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let n = input_table.row_count();

        let indices: Vec<usize> = match sample_type {
            SampleType::Rows => {
                let limit = (sample_value.max(0) as usize).min(n);
                (0..limit).collect()
            }
            SampleType::Percent => {
                if sample_value <= 0 {
                    Vec::new()
                } else if sample_value >= 100 {
                    (0..n).collect()
                } else {
                    let rate = sample_value as f64 / 100.0;
                    let mut rng = rand::thread_rng();
                    (0..n).filter(|_| rng.gen_range(0.0..1.0) < rate).collect()
                }
            }
        };

        Ok(input_table.gather_rows(&indices))
    }
}
