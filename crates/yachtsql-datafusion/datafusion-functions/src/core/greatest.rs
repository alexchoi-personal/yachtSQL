use crate::core::greatest_least_utils::GreatestLeastOperator;
use arrow::array::{make_comparator, Array, BooleanArray};
use arrow::compute::kernels::cmp;
use arrow::compute::SortOptions;
use arrow::datatypes::DataType;
use arrow_buffer::{BooleanBuffer, NullBuffer};
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_doc::Documentation;
use datafusion_expr::scalar_doc_sections::DOC_SECTION_CONDITIONAL;
use datafusion_expr::ColumnarValue;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::OnceLock;

const SORT_OPTIONS: SortOptions = SortOptions {
    descending: false,
    nulls_first: true,
};

#[derive(Debug)]
pub struct GreatestFunc {
    signature: Signature,
}

impl Default for GreatestFunc {
    fn default() -> Self {
        GreatestFunc::new()
    }
}

impl GreatestFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl GreatestLeastOperator for GreatestFunc {
    const NAME: &'static str = "greatest";

    fn keep_scalar<'a>(
        lhs: &'a ScalarValue,
        rhs: &'a ScalarValue,
    ) -> Result<&'a ScalarValue> {
        if lhs.is_null() {
            return Ok(lhs);
        }
        if rhs.is_null() {
            return Ok(rhs);
        }

        if !lhs.data_type().is_nested() {
            return if lhs >= rhs { Ok(lhs) } else { Ok(rhs) };
        }

        let cmp = make_comparator(
            lhs.to_array()?.as_ref(),
            rhs.to_array()?.as_ref(),
            SORT_OPTIONS,
        )?;

        if cmp(0, 0).is_ge() {
            Ok(lhs)
        } else {
            Ok(rhs)
        }
    }

    fn get_indexes_to_keep(lhs: &dyn Array, rhs: &dyn Array) -> Result<BooleanArray> {
        if !lhs.data_type().is_nested()
            && lhs.logical_null_count() == 0
            && rhs.logical_null_count() == 0
        {
            return cmp::gt_eq(&lhs, &rhs).map_err(|e| e.into());
        }

        if lhs.len() != rhs.len() {
            return internal_err!(
                "All arrays should have the same length for greatest comparison"
            );
        }

        let cmp = make_comparator(lhs, rhs, SORT_OPTIONS)?;

        let values = BooleanBuffer::collect_bool(lhs.len(), |i| cmp(i, i).is_ge());

        let nulls = match (lhs.nulls(), rhs.nulls()) {
            (Some(lhs_nulls), Some(rhs_nulls)) => {
                Some(NullBuffer::new(lhs_nulls.inner() & rhs_nulls.inner()))
            }
            (Some(nulls), None) | (None, Some(nulls)) => Some(nulls.clone()),
            (None, None) => None,
        };

        Ok(BooleanArray::new(values, nulls))
    }
}

impl ScalarUDFImpl for GreatestFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "greatest"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        super::greatest_least_utils::execute_conditional::<Self>(args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let coerced_type =
            super::greatest_least_utils::find_coerced_type::<Self>(arg_types)?;

        Ok(vec![coerced_type; arg_types.len()])
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_greatest_doc())
    }
}
static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_greatest_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder(
            DOC_SECTION_CONDITIONAL,
            "Returns the greatest value in a list of expressions. Returns _null_ if any expression is _null_.",
            "greatest(expression1[, ..., expression_n])")
            .with_sql_example(r#"```sql
> select greatest(4, 7, 5);
+---------------------------+
| greatest(4,7,5)           |
+---------------------------+
| 7                         |
+---------------------------+
```"#,
            )
            .with_argument(
                "expression1, expression_n",
                "Expressions to compare and return the greatest value. Can be a constant, column, or function, and any combination of arithmetic operators. Pass as many expression arguments as necessary."
            )
            .build()
    })
}

#[cfg(test)]
mod test {
    use crate::core;
    use arrow::datatypes::DataType;
    use datafusion_expr::ScalarUDFImpl;

    #[test]
    fn test_greatest_return_types_without_common_supertype_in_arg_type() {
        let greatest = core::greatest::GreatestFunc::new();
        let return_type = greatest
            .coerce_types(&[DataType::Decimal128(10, 3), DataType::Decimal128(10, 4)])
            .unwrap();
        assert_eq!(
            return_type,
            vec![DataType::Decimal128(11, 4), DataType::Decimal128(11, 4)]
        );
    }
}
