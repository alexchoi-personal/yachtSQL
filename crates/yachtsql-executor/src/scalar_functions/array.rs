#![coverage(off)]

use std::cmp::Ordering;

use rust_decimal::prelude::ToPrimitive;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_array_length(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Array(arr)) => Ok(Value::Int64(arr.len() as i64)),
        _ => Err(Error::InvalidQuery(
            "ARRAY_LENGTH requires array argument".into(),
        )),
    }
}

pub fn fn_array_to_string(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ARRAY_TO_STRING requires at least 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) => Ok(Value::Null),
        (Value::Array(arr), Value::String(sep)) => {
            let strs: Vec<String> = arr
                .iter()
                .filter(|v| !v.is_null())
                .map(|v| format!("{}", v))
                .collect();
            Ok(Value::String(strs.join(sep)))
        }
        _ => Err(Error::InvalidQuery(
            "ARRAY_TO_STRING requires array and string arguments".into(),
        )),
    }
}

pub fn fn_array_concat(args: &[Value]) -> Result<Value> {
    let mut result = Vec::new();
    for arg in args {
        match arg {
            Value::Null => continue,
            Value::Array(arr) => result.extend(arr.clone()),
            _ => {
                return Err(Error::InvalidQuery(
                    "ARRAY_CONCAT requires array arguments".into(),
                ));
            }
        }
    }
    Ok(Value::Array(result))
}

pub fn fn_array_reverse(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Array(arr)) => {
            let mut reversed = arr.clone();
            reversed.reverse();
            Ok(Value::Array(reversed))
        }
        _ => Err(Error::InvalidQuery(
            "ARRAY_REVERSE requires array argument".into(),
        )),
    }
}

pub fn fn_array_contains(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ARRAY_CONTAINS requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Array(arr), target) => {
            let contains = arr.iter().any(|v| values_equal(v, target));
            Ok(Value::Bool(contains))
        }
        _ => Err(Error::InvalidQuery(
            "ARRAY_CONTAINS requires array as first argument".into(),
        )),
    }
}

const MAX_GENERATE_ARRAY_SIZE: usize = 1_000_000;

pub fn fn_generate_array(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "GENERATE_ARRAY requires at least 2 arguments".into(),
        ));
    }
    let start = match &args[0] {
        Value::Int64(n) => *n,
        _ => {
            return Err(Error::InvalidQuery(
                "GENERATE_ARRAY requires integer arguments".into(),
            ));
        }
    };
    let end = match &args[1] {
        Value::Int64(n) => *n,
        _ => {
            return Err(Error::InvalidQuery(
                "GENERATE_ARRAY requires integer arguments".into(),
            ));
        }
    };
    let step = args.get(2).and_then(|v| v.as_i64()).unwrap_or(1);
    if step == 0 {
        return Err(Error::InvalidQuery(
            "GENERATE_ARRAY step cannot be zero".into(),
        ));
    }

    let estimated_size = if step > 0 && end >= start {
        ((end - start) / step + 1) as usize
    } else if step < 0 && start >= end {
        ((start - end) / (-step) + 1) as usize
    } else {
        0
    };

    if estimated_size > MAX_GENERATE_ARRAY_SIZE {
        return Err(Error::InvalidQuery(format!(
            "GENERATE_ARRAY would produce {} elements, exceeding maximum of {}",
            estimated_size, MAX_GENERATE_ARRAY_SIZE
        )));
    }

    let mut result = Vec::with_capacity(estimated_size);
    let mut i = start;
    if step > 0 {
        while i <= end {
            result.push(Value::Int64(i));
            i += step;
        }
    } else {
        while i >= end {
            result.push(Value::Int64(i));
            i += step;
        }
    }
    Ok(Value::Array(result))
}

pub fn fn_array_transform(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ARRAY_TRANSFORM requires 2 arguments".into(),
        ));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Array(arr) => Ok(Value::Array(arr.clone())),
        _ => Err(Error::InvalidQuery(
            "ARRAY_TRANSFORM requires array argument".into(),
        )),
    }
}

pub fn fn_array_filter(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ARRAY_FILTER requires 2 arguments".into(),
        ));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Array(arr) => Ok(Value::Array(arr.clone())),
        _ => Err(Error::InvalidQuery(
            "ARRAY_FILTER requires array argument".into(),
        )),
    }
}

pub fn fn_array_includes(args: &[Value]) -> Result<Value> {
    fn_array_contains(args)
}

pub fn fn_array_includes_any(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ARRAY_INCLUDES_ANY requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Array(arr1), Value::Array(arr2)) => {
            let includes_any = arr1
                .iter()
                .any(|v1| arr2.iter().any(|v2| values_equal(v1, v2)));
            Ok(Value::Bool(includes_any))
        }
        _ => Err(Error::InvalidQuery(
            "ARRAY_INCLUDES_ANY requires two array arguments".into(),
        )),
    }
}

pub fn fn_array_includes_all(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ARRAY_INCLUDES_ALL requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Array(arr1), Value::Array(arr2)) => {
            let includes_all = arr2
                .iter()
                .all(|v2| arr1.iter().any(|v1| values_equal(v1, v2)));
            Ok(Value::Bool(includes_all))
        }
        _ => Err(Error::InvalidQuery(
            "ARRAY_INCLUDES_ALL requires two array arguments".into(),
        )),
    }
}

pub fn fn_array_first(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Array(arr)) => Ok(arr.first().cloned().unwrap_or(Value::Null)),
        _ => Err(Error::InvalidQuery(
            "ARRAY_FIRST requires array argument".into(),
        )),
    }
}

pub fn fn_array_first_n(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ARRAY_FIRST_N requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Array(arr), Value::Int64(n)) => {
            let count = (*n).max(0) as usize;
            let result: Vec<Value> = arr.iter().take(count).cloned().collect();
            Ok(Value::Array(result))
        }
        _ => Err(Error::InvalidQuery(
            "ARRAY_FIRST_N requires array and integer arguments".into(),
        )),
    }
}

pub fn fn_array_last(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Array(arr)) => Ok(arr.last().cloned().unwrap_or(Value::Null)),
        _ => Err(Error::InvalidQuery(
            "ARRAY_LAST requires array argument".into(),
        )),
    }
}

pub fn fn_array_last_n(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ARRAY_LAST_N requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Array(arr), Value::Int64(n)) => {
            let count = (*n).max(0) as usize;
            let skip = arr.len().saturating_sub(count);
            let result: Vec<Value> = arr.iter().skip(skip).cloned().collect();
            Ok(Value::Array(result))
        }
        _ => Err(Error::InvalidQuery(
            "ARRAY_LAST_N requires array and integer arguments".into(),
        )),
    }
}

pub fn fn_array_min(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Array(arr)) => {
            let non_null: Vec<&Value> = arr.iter().filter(|v| !v.is_null()).collect();
            if non_null.is_empty() {
                return Ok(Value::Null);
            }
            let min = non_null
                .into_iter()
                .min_by(|a, b| compare_values(a, b))
                .cloned()
                .unwrap_or(Value::Null);
            Ok(min)
        }
        _ => Err(Error::InvalidQuery(
            "ARRAY_MIN requires array argument".into(),
        )),
    }
}

pub fn fn_array_max(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Array(arr)) => {
            let non_null: Vec<&Value> = arr.iter().filter(|v| !v.is_null()).collect();
            if non_null.is_empty() {
                return Ok(Value::Null);
            }
            let max = non_null
                .into_iter()
                .max_by(|a, b| compare_values(a, b))
                .cloned()
                .unwrap_or(Value::Null);
            Ok(max)
        }
        _ => Err(Error::InvalidQuery(
            "ARRAY_MAX requires array argument".into(),
        )),
    }
}

pub fn fn_array_sum(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Array(arr)) => {
            let mut sum_i64: i64 = 0;
            let mut sum_f64: f64 = 0.0;
            let mut has_float = false;
            let mut has_value = false;

            for v in arr {
                match v {
                    Value::Null => continue,
                    Value::Int64(n) => {
                        sum_i64 += n;
                        has_value = true;
                    }
                    Value::Float64(f) => {
                        sum_f64 += f.0;
                        has_float = true;
                        has_value = true;
                    }
                    Value::Numeric(d) => {
                        sum_f64 += d.to_f64().unwrap_or(0.0);
                        has_float = true;
                        has_value = true;
                    }
                    _ => {
                        return Err(Error::InvalidQuery(
                            "ARRAY_SUM requires numeric array".into(),
                        ));
                    }
                }
            }

            if !has_value {
                return Ok(Value::Null);
            }

            if has_float {
                Ok(Value::Float64(ordered_float::OrderedFloat(
                    sum_f64 + sum_i64 as f64,
                )))
            } else {
                Ok(Value::Int64(sum_i64))
            }
        }
        _ => Err(Error::InvalidQuery(
            "ARRAY_SUM requires array argument".into(),
        )),
    }
}

pub fn fn_array_avg(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Array(arr)) => {
            let mut sum: f64 = 0.0;
            let mut count: usize = 0;

            for v in arr {
                match v {
                    Value::Null => continue,
                    Value::Int64(n) => {
                        sum += *n as f64;
                        count += 1;
                    }
                    Value::Float64(f) => {
                        sum += f.0;
                        count += 1;
                    }
                    Value::Numeric(d) => {
                        sum += d.to_f64().unwrap_or(0.0);
                        count += 1;
                    }
                    _ => {
                        return Err(Error::InvalidQuery(
                            "ARRAY_AVG requires numeric array".into(),
                        ));
                    }
                }
            }

            if count == 0 {
                return Ok(Value::Null);
            }

            Ok(Value::Float64(ordered_float::OrderedFloat(
                sum / count as f64,
            )))
        }
        _ => Err(Error::InvalidQuery(
            "ARRAY_AVG requires array argument".into(),
        )),
    }
}

pub fn fn_array_offset(args: &[Value]) -> Result<Value> {
    if args.len() == 1 {
        return Ok(args[0].clone());
    }
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "OFFSET requires 1 or 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Array(arr), Value::Int64(idx)) => {
            if *idx < 0 || *idx as usize >= arr.len() {
                Err(Error::InvalidQuery(format!(
                    "Array index {} out of bounds for array of length {}",
                    idx,
                    arr.len()
                )))
            } else {
                Ok(arr[*idx as usize].clone())
            }
        }
        _ => Err(Error::InvalidQuery(
            "OFFSET requires array and integer arguments".into(),
        )),
    }
}

pub fn fn_array_ordinal(args: &[Value]) -> Result<Value> {
    if args.len() == 1 {
        return Ok(args[0].clone());
    }
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ORDINAL requires 1 or 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Array(arr), Value::Int64(idx)) => {
            let zero_based = *idx - 1;
            if zero_based < 0 || zero_based as usize >= arr.len() {
                Err(Error::InvalidQuery(format!(
                    "Array ordinal {} out of bounds for array of length {}",
                    idx,
                    arr.len()
                )))
            } else {
                Ok(arr[zero_based as usize].clone())
            }
        }
        _ => Err(Error::InvalidQuery(
            "ORDINAL requires array and integer arguments".into(),
        )),
    }
}

pub fn fn_array_slice(args: &[Value]) -> Result<Value> {
    if args.len() < 3 {
        return Err(Error::InvalidQuery(
            "ARRAY_SLICE requires array, start, and end arguments".into(),
        ));
    }
    match (&args[0], &args[1], &args[2]) {
        (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => Ok(Value::Null),
        (Value::Array(arr), Value::Int64(start), Value::Int64(end)) => {
            let start_idx = if *start < 0 {
                (arr.len() as i64 + start).max(0) as usize
            } else if *start == 0 {
                0
            } else {
                ((*start - 1) as usize).min(arr.len())
            };
            let end_idx = if *end < 0 {
                (arr.len() as i64 + end).max(0) as usize
            } else {
                (*end as usize).min(arr.len())
            };
            if start_idx >= end_idx {
                Ok(Value::Array(vec![]))
            } else {
                Ok(Value::Array(arr[start_idx..end_idx].to_vec()))
            }
        }
        _ => Err(Error::InvalidQuery(
            "ARRAY_SLICE expects array and integer arguments".into(),
        )),
    }
}

pub fn fn_array_flatten(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Array(arr)) => {
            let mut result = Vec::new();
            for v in arr {
                match v {
                    Value::Array(inner) => result.extend(inner.clone()),
                    other => result.push(other.clone()),
                }
            }
            Ok(Value::Array(result))
        }
        _ => Err(Error::InvalidQuery(
            "ARRAY_FLATTEN requires array argument".into(),
        )),
    }
}

pub fn fn_array_distinct(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Array(arr)) => {
            let mut result: Vec<Value> = Vec::new();
            for v in arr {
                if !result.iter().any(|r| values_equal(r, v)) {
                    result.push(v.clone());
                }
            }
            Ok(Value::Array(result))
        }
        _ => Err(Error::InvalidQuery(
            "ARRAY_DISTINCT requires array argument".into(),
        )),
    }
}

pub fn fn_array_position(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "ARRAY_POSITION requires 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Array(arr), target) => {
            for (i, v) in arr.iter().enumerate() {
                if values_equal(v, target) {
                    return Ok(Value::Int64((i + 1) as i64));
                }
            }
            Ok(Value::Int64(0))
        }
        _ => Err(Error::InvalidQuery(
            "ARRAY_POSITION requires array as first argument".into(),
        )),
    }
}

pub fn fn_array_compact(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Array(arr)) => {
            let result: Vec<Value> = arr.iter().filter(|v| !v.is_null()).cloned().collect();
            Ok(Value::Array(result))
        }
        _ => Err(Error::InvalidQuery(
            "ARRAY_COMPACT requires array argument".into(),
        )),
    }
}

pub fn fn_array_sort(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Array(arr)) => {
            let mut result = arr.clone();
            result.sort_by(compare_values);
            Ok(Value::Array(result))
        }
        _ => Err(Error::InvalidQuery(
            "ARRAY_SORT requires array argument".into(),
        )),
    }
}

pub fn fn_array_zip(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Array(vec![]));
    }

    let arrays: Vec<&Vec<Value>> = args
        .iter()
        .filter_map(|arg| {
            if let Value::Array(arr) = arg {
                Some(arr)
            } else {
                None
            }
        })
        .collect();

    if arrays.len() != args.len() {
        return Err(Error::InvalidQuery(
            "ARRAY_ZIP requires all arguments to be arrays".into(),
        ));
    }

    let max_len = arrays.iter().map(|a| a.len()).max().unwrap_or(0);
    let mut result = Vec::with_capacity(max_len);

    for i in 0..max_len {
        let mut struct_fields: Vec<(String, Value)> = Vec::with_capacity(arrays.len());
        for (j, arr) in arrays.iter().enumerate() {
            let field_name = format!("_field{}", j);
            let value = arr.get(i).cloned().unwrap_or(Value::Null);
            struct_fields.push((field_name, value));
        }
        result.push(Value::Struct(struct_fields));
    }

    Ok(Value::Array(result))
}

pub fn fn_safe_offset(args: &[Value]) -> Result<Value> {
    if args.len() == 1 {
        return Ok(args[0].clone());
    }
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "SAFE_OFFSET requires 1 or 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Array(arr), Value::Int64(idx)) => {
            if *idx < 0 || *idx as usize >= arr.len() {
                Ok(Value::Null)
            } else {
                Ok(arr[*idx as usize].clone())
            }
        }
        _ => Err(Error::InvalidQuery(
            "SAFE_OFFSET requires array and integer arguments".into(),
        )),
    }
}

pub fn fn_safe_ordinal(args: &[Value]) -> Result<Value> {
    if args.len() == 1 {
        return Ok(args[0].clone());
    }
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "SAFE_ORDINAL requires 1 or 2 arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Array(arr), Value::Int64(idx)) => {
            let zero_based = *idx - 1;
            if zero_based < 0 || zero_based as usize >= arr.len() {
                Ok(Value::Null)
            } else {
                Ok(arr[zero_based as usize].clone())
            }
        }
        _ => Err(Error::InvalidQuery(
            "SAFE_ORDINAL requires array and integer arguments".into(),
        )),
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Int64(x), Value::Int64(y)) => x == y,
        (Value::Int64(x), Value::Float64(y)) => (*x as f64) == y.0,
        (Value::Float64(x), Value::Int64(y)) => x.0 == (*y as f64),
        (Value::Float64(x), Value::Float64(y)) => x == y,
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Numeric(x), Value::Numeric(y)) => x == y,
        (Value::Date(x), Value::Date(y)) => x == y,
        (Value::Time(x), Value::Time(y)) => x == y,
        (Value::Timestamp(x), Value::Timestamp(y)) => x == y,
        (Value::DateTime(x), Value::DateTime(y)) => x == y,
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(a, b)| values_equal(a, b))
        }
        (Value::Struct(x), Value::Struct(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y.iter())
                    .all(|((n1, v1), (n2, v2))| n1 == n2 && values_equal(v1, v2))
        }
        _ => false,
    }
}

fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Int64(x), Value::Int64(y)) => x.cmp(y),
        (Value::Int64(x), Value::Float64(y)) => {
            let fx = *x as f64;
            fx.partial_cmp(&y.0).unwrap_or(Ordering::Equal)
        }
        (Value::Float64(x), Value::Int64(y)) => {
            let fy = *y as f64;
            x.0.partial_cmp(&fy).unwrap_or(Ordering::Equal)
        }
        (Value::Float64(x), Value::Float64(y)) => x.cmp(y),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Numeric(x), Value::Numeric(y)) => x.cmp(y),
        (Value::Date(x), Value::Date(y)) => x.cmp(y),
        (Value::Time(x), Value::Time(y)) => x.cmp(y),
        (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
        (Value::DateTime(x), Value::DateTime(y)) => x.cmp(y),
        _ => Ordering::Equal,
    }
}

pub fn fn_unnest(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Array(arr)) => Ok(Value::Array(arr.clone())),
        _ => Err(Error::InvalidQuery("UNNEST requires ARRAY argument".into())),
    }
}

pub fn fn_array_enumerate(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Array(arr)) => {
            let indices: Vec<Value> = (1..=arr.len()).map(|i| Value::Int64(i as i64)).collect();
            Ok(Value::Array(indices))
        }
        _ => Err(Error::InvalidQuery(
            "arrayEnumerate requires ARRAY argument".into(),
        )),
    }
}
