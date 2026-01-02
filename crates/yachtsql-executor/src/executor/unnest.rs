#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{PlanSchema, UnnestColumn};
use yachtsql_storage::Table;

use super::{PlanExecutor, plan_schema_to_schema};
use crate::columnar_evaluator::ColumnarEvaluator;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub(crate) fn execute_unnest(
        &mut self,
        input: &PhysicalPlan,
        columns: &[UnnestColumn],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let input_schema = input_table.schema().clone();
        let n = input_table.row_count();

        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);

        if n == 0 && !columns.is_empty() {
            return Ok(result);
        }

        let evaluator = ColumnarEvaluator::new(&input_schema)
            .with_variables(&self.variables)
            .with_system_variables(self.session.system_variables())
            .with_user_functions(&self.user_function_defs);

        let input_columns: Vec<_> = input_table
            .columns()
            .iter()
            .map(|(_, c)| c.as_ref())
            .collect();

        for i in 0..n {
            let base_values: Vec<Value> = input_columns.iter().map(|c| c.get_value(i)).collect();

            if columns.is_empty() {
                result.push_row(base_values)?;
                continue;
            }

            let first_col = &columns[0];
            let array_col = evaluator.evaluate(&first_col.expr, &input_table)?;
            let array_val = array_col.get_value(i);

            Self::unnest_array(&array_val, first_col, &base_values, &mut result)?;
        }

        Ok(result)
    }

    fn unnest_array(
        array_val: &Value,
        unnest_col: &UnnestColumn,
        base_values: &[Value],
        result: &mut Table,
    ) -> Result<()> {
        match array_val {
            Value::Array(elements) => {
                for (idx, elem) in elements.iter().enumerate() {
                    let mut row = base_values.to_vec();
                    match elem {
                        Value::Struct(struct_fields) => {
                            for (_, value) in struct_fields {
                                row.push(value.clone());
                            }
                        }
                        _ => {
                            row.push(elem.clone());
                        }
                    }
                    if unnest_col.with_offset {
                        row.push(Value::Int64(idx as i64));
                    }
                    result.push_row(row)?;
                }
            }
            Value::Null => {}
            _ => {
                return Err(Error::InvalidQuery("UNNEST requires array argument".into()));
            }
        }
        Ok(())
    }
}
