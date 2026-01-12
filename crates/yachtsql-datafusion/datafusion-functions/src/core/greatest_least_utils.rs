use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::DataType;
use arrow_buffer::NullBuffer;
use datafusion_common::{internal_err, plan_err, Result, ScalarValue};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::type_coercion::binary::type_union_resolution;
use std::sync::Arc;

pub(super) trait GreatestLeastOperator {
    const NAME: &'static str;

    fn keep_scalar<'a>(
        lhs: &'a ScalarValue,
        rhs: &'a ScalarValue,
    ) -> Result<&'a ScalarValue>;

    fn get_indexes_to_keep(lhs: &dyn Array, rhs: &dyn Array) -> Result<BooleanArray>;
}

fn keep_array<Op: GreatestLeastOperator>(
    lhs: ArrayRef,
    rhs: ArrayRef,
) -> Result<ArrayRef> {
    let keep_lhs = Op::get_indexes_to_keep(lhs.as_ref(), rhs.as_ref())?;

    let result = zip(&keep_lhs, &lhs, &rhs)?;

    let result = match keep_lhs.nulls() {
        Some(mask_nulls) => {
            let result_nulls = match result.nulls() {
                Some(result_nulls) => {
                    NullBuffer::new(result_nulls.inner() & mask_nulls.inner())
                }
                None => mask_nulls.clone(),
            };
            result.to_data().into_builder().nulls(Some(result_nulls)).build().map(arrow::array::make_array)?
        }
        None => result,
    };

    Ok(result)
}

pub(super) fn execute_conditional<Op: GreatestLeastOperator>(
    args: &[ColumnarValue],
) -> Result<ColumnarValue> {
    if args.is_empty() {
        return internal_err!(
            "{} was called with no arguments. It requires at least 1.",
            Op::NAME
        );
    }

    if args.len() == 1 {
        return Ok(args[0].clone());
    }

    let (scalars, arrays): (Vec<_>, Vec<_>) = args.iter().partition(|x| match x {
        ColumnarValue::Scalar(_) => true,
        ColumnarValue::Array(_) => false,
    });

    let mut arrays_iter = arrays.iter().map(|x| match x {
        ColumnarValue::Array(a) => a,
        _ => unreachable!(),
    });

    let first_array = arrays_iter.next();

    let mut result: ArrayRef;

    if !scalars.is_empty() {
        let mut scalars_iter = scalars.iter().map(|x| match x {
            ColumnarValue::Scalar(s) => s,
            _ => unreachable!(),
        });

        let mut result_scalar = scalars_iter.next().unwrap();

        for scalar in scalars_iter {
            result_scalar = Op::keep_scalar(result_scalar, scalar)?;
        }

        if arrays.is_empty() {
            return Ok(ColumnarValue::Scalar(result_scalar.clone()));
        }

        let first_array = first_array.unwrap();

        result = keep_array::<Op>(
            Arc::clone(first_array),
            result_scalar.to_array_of_size(first_array.len())?,
        )?;
    } else {
        result = Arc::clone(first_array.unwrap());
    }

    for array in arrays_iter {
        result = keep_array::<Op>(Arc::clone(array), result)?;
    }

    Ok(ColumnarValue::Array(result))
}

pub(super) fn find_coerced_type<Op: GreatestLeastOperator>(
    data_types: &[DataType],
) -> Result<DataType> {
    if data_types.is_empty() {
        plan_err!(
            "{} was called without any arguments. It requires at least 1.",
            Op::NAME
        )
    } else if let Some(coerced_type) = type_union_resolution(data_types) {
        Ok(coerced_type)
    } else {
        plan_err!("Cannot find a common type for arguments")
    }
}
