#![coverage(off)]

pub mod math;
pub mod nulls;
pub mod string;

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::ScalarFunction;
use yachtsql_storage::Column;

pub fn dispatch_vectorized(
    func: &ScalarFunction,
    args: &[Column],
    row_count: usize,
) -> Result<Column> {
    match func {
        ScalarFunction::Abs => math::abs(args),
        ScalarFunction::Floor => math::floor(args),
        ScalarFunction::Ceil => math::ceil(args),
        ScalarFunction::Round => math::round(args),
        ScalarFunction::Sqrt => math::sqrt(args),
        ScalarFunction::Power => math::power(args),
        ScalarFunction::Pow => math::power(args),
        ScalarFunction::Log => math::log(args),
        ScalarFunction::Log10 => math::log10(args),
        ScalarFunction::Exp => math::exp(args),
        ScalarFunction::Sign => math::sign(args),

        ScalarFunction::Upper => string::upper(args),
        ScalarFunction::Lower => string::lower(args),
        ScalarFunction::Length => string::length(args),
        ScalarFunction::ByteLength => string::byte_length(args),
        ScalarFunction::Trim => string::trim(args),
        ScalarFunction::LTrim => string::ltrim(args),
        ScalarFunction::RTrim => string::rtrim(args),
        ScalarFunction::Substr => string::substr(args),

        ScalarFunction::Coalesce => nulls::coalesce(args),
        ScalarFunction::IfNull | ScalarFunction::Ifnull => nulls::ifnull(args),
        ScalarFunction::NullIf => nulls::nullif(args),
        ScalarFunction::Greatest => nulls::greatest(args),
        ScalarFunction::Least => nulls::least(args),

        _ => scalar_fallback(func, args, row_count),
    }
}

fn scalar_fallback(func: &ScalarFunction, args: &[Column], row_count: usize) -> Result<Column> {
    let n = args.first().map(|c| c.len()).unwrap_or(row_count);
    let mut results = Vec::with_capacity(n);
    for i in 0..n {
        let arg_vals: Vec<Value> = args.iter().map(|c| c.get_value(i)).collect();
        results.push(super::dispatch(func, &arg_vals)?);
    }
    Ok(Column::from_values(&results))
}
