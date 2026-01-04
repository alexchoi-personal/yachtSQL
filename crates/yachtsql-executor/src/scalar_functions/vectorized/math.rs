#![coverage(off)]

use aligned_vec::AVec;
use yachtsql_common::error::{Error, Result};
use yachtsql_storage::{Column, NullBitmap};

pub fn abs(args: &[Column]) -> Result<Column> {
    let col = args
        .first()
        .ok_or_else(|| Error::invalid_query("ABS requires 1 argument"))?;
    match col {
        Column::Int64 { data, nulls } => {
            let mut result_data = AVec::with_capacity(64, data.len());
            for &v in data.as_slice() {
                result_data.push(v.saturating_abs());
            }
            Ok(Column::Int64 {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        Column::Float64 { data, nulls } => {
            let mut result_data = AVec::with_capacity(64, data.len());
            for &v in data.as_slice() {
                result_data.push(v.abs());
            }
            Ok(Column::Float64 {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        Column::Numeric { data, nulls } => {
            let result_data: Vec<_> = data.iter().map(|v| v.abs()).collect();
            Ok(Column::Numeric {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        _ => Err(Error::invalid_query("ABS requires numeric argument")),
    }
}

pub fn floor(args: &[Column]) -> Result<Column> {
    let col = args
        .first()
        .ok_or_else(|| Error::invalid_query("FLOOR requires 1 argument"))?;
    match col {
        Column::Int64 { data, nulls } => Ok(Column::Int64 {
            data: data.clone(),
            nulls: nulls.clone(),
        }),
        Column::Float64 { data, nulls } => {
            let mut result_data = AVec::with_capacity(64, data.len());
            for &v in data.as_slice() {
                result_data.push(v.floor());
            }
            Ok(Column::Float64 {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        Column::Numeric { data, nulls } => {
            let result_data: Vec<_> = data.iter().map(|v| v.floor()).collect();
            Ok(Column::Numeric {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        _ => Err(Error::invalid_query("FLOOR requires numeric argument")),
    }
}

pub fn ceil(args: &[Column]) -> Result<Column> {
    let col = args
        .first()
        .ok_or_else(|| Error::invalid_query("CEIL requires 1 argument"))?;
    match col {
        Column::Int64 { data, nulls } => Ok(Column::Int64 {
            data: data.clone(),
            nulls: nulls.clone(),
        }),
        Column::Float64 { data, nulls } => {
            let mut result_data = AVec::with_capacity(64, data.len());
            for &v in data.as_slice() {
                result_data.push(v.ceil());
            }
            Ok(Column::Float64 {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        Column::Numeric { data, nulls } => {
            let result_data: Vec<_> = data.iter().map(|v| v.ceil()).collect();
            Ok(Column::Numeric {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        _ => Err(Error::invalid_query("CEIL requires numeric argument")),
    }
}

pub fn round(args: &[Column]) -> Result<Column> {
    let col = args
        .first()
        .ok_or_else(|| Error::invalid_query("ROUND requires at least 1 argument"))?;
    let precision = args
        .get(1)
        .map(|c| c.get_value(0).as_i64().unwrap_or(0))
        .unwrap_or(0);

    match col {
        Column::Int64 { data, nulls } => Ok(Column::Int64 {
            data: data.clone(),
            nulls: nulls.clone(),
        }),
        Column::Float64 { data, nulls } => {
            let mult = 10f64.powi(precision as i32);
            let mut result_data = AVec::with_capacity(64, data.len());
            for &v in data.as_slice() {
                result_data.push((v * mult).round() / mult);
            }
            Ok(Column::Float64 {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        Column::Numeric { data, nulls } => {
            let result_data: Vec<_> = data
                .iter()
                .map(|v| v.round_dp(precision.max(0) as u32))
                .collect();
            Ok(Column::Numeric {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        _ => Err(Error::invalid_query("ROUND requires numeric argument")),
    }
}

pub fn sqrt(args: &[Column]) -> Result<Column> {
    let col = args
        .first()
        .ok_or_else(|| Error::invalid_query("SQRT requires 1 argument"))?;
    match col {
        Column::Int64 { data, nulls } => {
            let mut result_data = AVec::with_capacity(64, data.len());
            for &v in data.as_slice() {
                result_data.push((v as f64).sqrt());
            }
            Ok(Column::Float64 {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        Column::Float64 { data, nulls } => {
            let mut result_data = AVec::with_capacity(64, data.len());
            for &v in data.as_slice() {
                result_data.push(v.sqrt());
            }
            Ok(Column::Float64 {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        _ => Err(Error::invalid_query("SQRT requires numeric argument")),
    }
}

pub fn power(args: &[Column]) -> Result<Column> {
    if args.len() < 2 {
        return Err(Error::invalid_query("POWER requires 2 arguments"));
    }
    let base_col = &args[0];
    let exp_col = &args[1];
    let len = base_col.len();

    let mut result_data = AVec::with_capacity(64, len);
    let mut result_nulls = NullBitmap::new();

    for i in 0..len {
        if base_col.is_null(i) || exp_col.is_null(i) {
            result_data.push(0.0);
            result_nulls.push(true);
            continue;
        }
        let base = match base_col {
            Column::Int64 { data, .. } => data[i] as f64,
            Column::Float64 { data, .. } => data[i],
            _ => {
                return Err(Error::invalid_query("POWER requires numeric arguments"));
            }
        };
        let exp = match exp_col {
            Column::Int64 { data, .. } => data[i] as f64,
            Column::Float64 { data, .. } => data[i],
            _ => {
                return Err(Error::invalid_query("POWER requires numeric arguments"));
            }
        };
        result_data.push(base.powf(exp));
        result_nulls.push(false);
    }

    Ok(Column::Float64 {
        data: result_data,
        nulls: result_nulls,
    })
}

pub fn log(args: &[Column]) -> Result<Column> {
    let col = args
        .first()
        .ok_or_else(|| Error::invalid_query("LOG requires at least 1 argument"))?;
    let base = args
        .get(1)
        .map(|c| {
            let v = c.get_value(0);
            match v {
                yachtsql_common::types::Value::Int64(n) => n as f64,
                yachtsql_common::types::Value::Float64(f) => f.0,
                _ => 10.0,
            }
        })
        .unwrap_or(10.0);

    match col {
        Column::Int64 { data, nulls } => {
            let mut result_data = AVec::with_capacity(64, data.len());
            for &v in data.as_slice() {
                result_data.push((v as f64).log(base));
            }
            Ok(Column::Float64 {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        Column::Float64 { data, nulls } => {
            let mut result_data = AVec::with_capacity(64, data.len());
            for &v in data.as_slice() {
                result_data.push(v.log(base));
            }
            Ok(Column::Float64 {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        _ => Err(Error::invalid_query("LOG requires numeric argument")),
    }
}

pub fn log10(args: &[Column]) -> Result<Column> {
    let col = args
        .first()
        .ok_or_else(|| Error::invalid_query("LOG10 requires 1 argument"))?;
    match col {
        Column::Int64 { data, nulls } => {
            let mut result_data = AVec::with_capacity(64, data.len());
            for &v in data.as_slice() {
                result_data.push((v as f64).log10());
            }
            Ok(Column::Float64 {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        Column::Float64 { data, nulls } => {
            let mut result_data = AVec::with_capacity(64, data.len());
            for &v in data.as_slice() {
                result_data.push(v.log10());
            }
            Ok(Column::Float64 {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        _ => Err(Error::invalid_query("LOG10 requires numeric argument")),
    }
}

pub fn exp(args: &[Column]) -> Result<Column> {
    let col = args
        .first()
        .ok_or_else(|| Error::invalid_query("EXP requires 1 argument"))?;
    match col {
        Column::Int64 { data, nulls } => {
            let mut result_data = AVec::with_capacity(64, data.len());
            for &v in data.as_slice() {
                result_data.push((v as f64).exp());
            }
            Ok(Column::Float64 {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        Column::Float64 { data, nulls } => {
            let mut result_data = AVec::with_capacity(64, data.len());
            for &v in data.as_slice() {
                result_data.push(v.exp());
            }
            Ok(Column::Float64 {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        _ => Err(Error::invalid_query("EXP requires numeric argument")),
    }
}

pub fn sign(args: &[Column]) -> Result<Column> {
    let col = args
        .first()
        .ok_or_else(|| Error::invalid_query("SIGN requires 1 argument"))?;
    match col {
        Column::Int64 { data, nulls } => {
            let mut result_data = AVec::with_capacity(64, data.len());
            for &v in data.as_slice() {
                result_data.push(v.signum());
            }
            Ok(Column::Int64 {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        Column::Float64 { data, nulls } => {
            let mut result_data = AVec::with_capacity(64, data.len());
            for &v in data.as_slice() {
                let sign = if v > 0.0 {
                    1
                } else if v < 0.0 {
                    -1
                } else {
                    0
                };
                result_data.push(sign);
            }
            Ok(Column::Int64 {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        _ => Err(Error::invalid_query("SIGN requires numeric argument")),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rust_decimal::Decimal;

    use super::*;

    fn make_int64_col(values: &[i64]) -> Column {
        let mut data = AVec::with_capacity(64, values.len());
        for &v in values {
            data.push(v);
        }
        Column::Int64 {
            data,
            nulls: NullBitmap::new_valid(values.len()),
        }
    }

    fn make_float64_col(values: &[f64]) -> Column {
        let mut data = AVec::with_capacity(64, values.len());
        for &v in values {
            data.push(v);
        }
        Column::Float64 {
            data,
            nulls: NullBitmap::new_valid(values.len()),
        }
    }

    fn make_numeric_col(values: &[&str]) -> Column {
        let data: Vec<Decimal> = values
            .iter()
            .map(|s| Decimal::from_str(s).unwrap())
            .collect();
        Column::Numeric {
            data,
            nulls: NullBitmap::new_valid(values.len()),
        }
    }

    #[test]
    fn test_abs_int64() {
        let col = make_int64_col(&[-5, 0, 10, -100]);
        let result = abs(&[col]).unwrap();
        match result {
            Column::Int64 { data, .. } => {
                assert_eq!(data.as_slice(), &[5, 0, 10, 100]);
            }
            _ => panic!("Expected Int64 column"),
        }
    }

    #[test]
    fn test_abs_float64() {
        let col = make_float64_col(&[-5.5, 0.0, 10.5, -100.1]);
        let result = abs(&[col]).unwrap();
        match result {
            Column::Float64 { data, .. } => {
                assert_eq!(data.as_slice(), &[5.5, 0.0, 10.5, 100.1]);
            }
            _ => panic!("Expected Float64 column"),
        }
    }

    #[test]
    fn test_abs_numeric() {
        let col = make_numeric_col(&["-5.5", "0", "10.5"]);
        let result = abs(&[col]).unwrap();
        match result {
            Column::Numeric { data, .. } => {
                assert_eq!(data[0], Decimal::from_str("5.5").unwrap());
                assert_eq!(data[1], Decimal::from_str("0").unwrap());
                assert_eq!(data[2], Decimal::from_str("10.5").unwrap());
            }
            _ => panic!("Expected Numeric column"),
        }
    }

    #[test]
    fn test_abs_empty_args() {
        let result = abs(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_floor_int64() {
        let col = make_int64_col(&[5, 0, -10]);
        let result = floor(&[col]).unwrap();
        match result {
            Column::Int64 { data, .. } => {
                assert_eq!(data.as_slice(), &[5, 0, -10]);
            }
            _ => panic!("Expected Int64 column"),
        }
    }

    #[test]
    fn test_floor_float64() {
        let col = make_float64_col(&[5.7, 0.0, -10.3]);
        let result = floor(&[col]).unwrap();
        match result {
            Column::Float64 { data, .. } => {
                assert_eq!(data.as_slice(), &[5.0, 0.0, -11.0]);
            }
            _ => panic!("Expected Float64 column"),
        }
    }

    #[test]
    fn test_floor_numeric() {
        let col = make_numeric_col(&["5.7", "0", "-10.3"]);
        let result = floor(&[col]).unwrap();
        match result {
            Column::Numeric { data, .. } => {
                assert_eq!(data[0], Decimal::from_str("5").unwrap());
                assert_eq!(data[1], Decimal::from_str("0").unwrap());
                assert_eq!(data[2], Decimal::from_str("-11").unwrap());
            }
            _ => panic!("Expected Numeric column"),
        }
    }

    #[test]
    fn test_ceil_int64() {
        let col = make_int64_col(&[5, 0, -10]);
        let result = ceil(&[col]).unwrap();
        match result {
            Column::Int64 { data, .. } => {
                assert_eq!(data.as_slice(), &[5, 0, -10]);
            }
            _ => panic!("Expected Int64 column"),
        }
    }

    #[test]
    fn test_ceil_float64() {
        let col = make_float64_col(&[5.1, 0.0, -10.9]);
        let result = ceil(&[col]).unwrap();
        match result {
            Column::Float64 { data, .. } => {
                assert_eq!(data.as_slice(), &[6.0, 0.0, -10.0]);
            }
            _ => panic!("Expected Float64 column"),
        }
    }

    #[test]
    fn test_ceil_numeric() {
        let col = make_numeric_col(&["5.1", "0", "-10.9"]);
        let result = ceil(&[col]).unwrap();
        match result {
            Column::Numeric { data, .. } => {
                assert_eq!(data[0], Decimal::from_str("6").unwrap());
                assert_eq!(data[1], Decimal::from_str("0").unwrap());
                assert_eq!(data[2], Decimal::from_str("-10").unwrap());
            }
            _ => panic!("Expected Numeric column"),
        }
    }

    #[test]
    fn test_round_float64_default() {
        let col = make_float64_col(&[5.456, 0.0, -10.555]);
        let result = round(&[col]).unwrap();
        match result {
            Column::Float64 { data, .. } => {
                assert_eq!(data.as_slice(), &[5.0, 0.0, -11.0]);
            }
            _ => panic!("Expected Float64 column"),
        }
    }

    #[test]
    fn test_round_float64_with_precision() {
        let col = make_float64_col(&[5.456, 0.0, -10.555]);
        let prec = make_int64_col(&[2]);
        let result = round(&[col, prec]).unwrap();
        match result {
            Column::Float64 { data, .. } => {
                assert!((data[0] - 5.46).abs() < 1e-10);
                assert!((data[1] - 0.0).abs() < 1e-10);
                assert!((data[2] - -10.56).abs() < 1e-10);
            }
            _ => panic!("Expected Float64 column"),
        }
    }

    #[test]
    fn test_round_numeric() {
        let col = make_numeric_col(&["5.456", "0", "-10.555"]);
        let prec = make_int64_col(&[2]);
        let result = round(&[col, prec]).unwrap();
        match result {
            Column::Numeric { data, .. } => {
                assert_eq!(data[0], Decimal::from_str("5.46").unwrap());
                assert_eq!(data[1], Decimal::from_str("0.00").unwrap());
                assert_eq!(data[2], Decimal::from_str("-10.56").unwrap());
            }
            _ => panic!("Expected Numeric column"),
        }
    }

    #[test]
    fn test_sqrt_int64() {
        let col = make_int64_col(&[4, 9, 16, 25]);
        let result = sqrt(&[col]).unwrap();
        match result {
            Column::Float64 { data, .. } => {
                assert_eq!(data.as_slice(), &[2.0, 3.0, 4.0, 5.0]);
            }
            _ => panic!("Expected Float64 column"),
        }
    }

    #[test]
    fn test_sqrt_float64() {
        let col = make_float64_col(&[4.0, 9.0, 16.0, 25.0]);
        let result = sqrt(&[col]).unwrap();
        match result {
            Column::Float64 { data, .. } => {
                assert_eq!(data.as_slice(), &[2.0, 3.0, 4.0, 5.0]);
            }
            _ => panic!("Expected Float64 column"),
        }
    }

    #[test]
    fn test_power_float64() {
        let base = make_float64_col(&[2.0, 3.0, 10.0]);
        let exp = make_float64_col(&[3.0, 2.0, 2.0]);
        let result = power(&[base, exp]).unwrap();
        match result {
            Column::Float64 { data, .. } => {
                assert_eq!(data.as_slice(), &[8.0, 9.0, 100.0]);
            }
            _ => panic!("Expected Float64 column"),
        }
    }

    #[test]
    fn test_power_int64() {
        let base = make_int64_col(&[2, 3, 10]);
        let exp = make_int64_col(&[3, 2, 2]);
        let result = power(&[base, exp]).unwrap();
        match result {
            Column::Float64 { data, .. } => {
                assert_eq!(data.as_slice(), &[8.0, 9.0, 100.0]);
            }
            _ => panic!("Expected Float64 column"),
        }
    }

    #[test]
    fn test_power_missing_args() {
        let base = make_float64_col(&[2.0]);
        let result = power(&[base]);
        assert!(result.is_err());
    }

    #[test]
    fn test_log_default_base() {
        let col = make_float64_col(&[10.0, 100.0, 1000.0]);
        let result = log(&[col]).unwrap();
        match result {
            Column::Float64 { data, .. } => {
                assert!((data[0] - 1.0).abs() < 1e-10);
                assert!((data[1] - 2.0).abs() < 1e-10);
                assert!((data[2] - 3.0).abs() < 1e-10);
            }
            _ => panic!("Expected Float64 column"),
        }
    }

    #[test]
    fn test_log_custom_base() {
        let col = make_float64_col(&[8.0, 16.0, 32.0]);
        let base = make_float64_col(&[2.0]);
        let result = log(&[col, base]).unwrap();
        match result {
            Column::Float64 { data, .. } => {
                assert!((data[0] - 3.0).abs() < 1e-10);
                assert!((data[1] - 4.0).abs() < 1e-10);
                assert!((data[2] - 5.0).abs() < 1e-10);
            }
            _ => panic!("Expected Float64 column"),
        }
    }

    #[test]
    fn test_log_int64() {
        let col = make_int64_col(&[10, 100, 1000]);
        let result = log(&[col]).unwrap();
        match result {
            Column::Float64 { data, .. } => {
                assert!((data[0] - 1.0).abs() < 1e-10);
                assert!((data[1] - 2.0).abs() < 1e-10);
                assert!((data[2] - 3.0).abs() < 1e-10);
            }
            _ => panic!("Expected Float64 column"),
        }
    }

    #[test]
    fn test_log10_float64() {
        let col = make_float64_col(&[10.0, 100.0, 1000.0]);
        let result = log10(&[col]).unwrap();
        match result {
            Column::Float64 { data, .. } => {
                assert!((data[0] - 1.0).abs() < 1e-10);
                assert!((data[1] - 2.0).abs() < 1e-10);
                assert!((data[2] - 3.0).abs() < 1e-10);
            }
            _ => panic!("Expected Float64 column"),
        }
    }

    #[test]
    fn test_log10_int64() {
        let col = make_int64_col(&[10, 100, 1000]);
        let result = log10(&[col]).unwrap();
        match result {
            Column::Float64 { data, .. } => {
                assert!((data[0] - 1.0).abs() < 1e-10);
                assert!((data[1] - 2.0).abs() < 1e-10);
                assert!((data[2] - 3.0).abs() < 1e-10);
            }
            _ => panic!("Expected Float64 column"),
        }
    }

    #[test]
    fn test_exp_float64() {
        let col = make_float64_col(&[0.0, 1.0, 2.0]);
        let result = exp(&[col]).unwrap();
        match result {
            Column::Float64 { data, .. } => {
                assert!((data[0] - 1.0).abs() < 1e-10);
                assert!((data[1] - std::f64::consts::E).abs() < 1e-10);
                assert!((data[2] - std::f64::consts::E.powi(2)).abs() < 1e-10);
            }
            _ => panic!("Expected Float64 column"),
        }
    }

    #[test]
    fn test_exp_int64() {
        let col = make_int64_col(&[0, 1, 2]);
        let result = exp(&[col]).unwrap();
        match result {
            Column::Float64 { data, .. } => {
                assert!((data[0] - 1.0).abs() < 1e-10);
                assert!((data[1] - std::f64::consts::E).abs() < 1e-10);
                assert!((data[2] - std::f64::consts::E.powi(2)).abs() < 1e-10);
            }
            _ => panic!("Expected Float64 column"),
        }
    }

    #[test]
    fn test_sign_int64() {
        let col = make_int64_col(&[-5, 0, 10]);
        let result = sign(&[col]).unwrap();
        match result {
            Column::Int64 { data, .. } => {
                assert_eq!(data.as_slice(), &[-1, 0, 1]);
            }
            _ => panic!("Expected Int64 column"),
        }
    }

    #[test]
    fn test_sign_float64() {
        let col = make_float64_col(&[-5.5, 0.0, 10.5]);
        let result = sign(&[col]).unwrap();
        match result {
            Column::Int64 { data, .. } => {
                assert_eq!(data.as_slice(), &[-1, 0, 1]);
            }
            _ => panic!("Expected Int64 column"),
        }
    }

    #[test]
    fn test_invalid_type_errors() {
        let string_col = Column::String {
            data: vec!["hello".to_string()],
            nulls: NullBitmap::new_valid(1),
        };
        assert!(abs(std::slice::from_ref(&string_col)).is_err());
        assert!(floor(std::slice::from_ref(&string_col)).is_err());
        assert!(ceil(std::slice::from_ref(&string_col)).is_err());
        assert!(round(std::slice::from_ref(&string_col)).is_err());
        assert!(sqrt(std::slice::from_ref(&string_col)).is_err());
        assert!(log(std::slice::from_ref(&string_col)).is_err());
        assert!(log10(std::slice::from_ref(&string_col)).is_err());
        assert!(exp(std::slice::from_ref(&string_col)).is_err());
        assert!(sign(std::slice::from_ref(&string_col)).is_err());
    }

    #[test]
    fn test_power_with_nulls() {
        let mut base_nulls = NullBitmap::new_valid(3);
        base_nulls.set_null(1);
        let mut base_data = AVec::with_capacity(64, 3);
        base_data.push(2.0);
        base_data.push(0.0);
        base_data.push(10.0);
        let base_col = Column::Float64 {
            data: base_data,
            nulls: base_nulls,
        };
        let exp_col = make_float64_col(&[3.0, 2.0, 2.0]);
        let result = power(&[base_col, exp_col]).unwrap();
        match result {
            Column::Float64 { data, nulls } => {
                assert!((data[0] - 8.0).abs() < 1e-10);
                assert!(nulls.is_null(1));
                assert!((data[2] - 100.0).abs() < 1e-10);
            }
            _ => panic!("Expected Float64 column"),
        }
    }

    #[test]
    fn test_power_exp_null() {
        let base_col = make_float64_col(&[2.0, 3.0, 10.0]);
        let mut exp_nulls = NullBitmap::new_valid(3);
        exp_nulls.set_null(0);
        let mut exp_data = AVec::with_capacity(64, 3);
        exp_data.push(0.0);
        exp_data.push(2.0);
        exp_data.push(2.0);
        let exp_col = Column::Float64 {
            data: exp_data,
            nulls: exp_nulls,
        };
        let result = power(&[base_col, exp_col]).unwrap();
        match result {
            Column::Float64 { data, nulls } => {
                assert!(nulls.is_null(0));
                assert!((data[1] - 9.0).abs() < 1e-10);
                assert!((data[2] - 100.0).abs() < 1e-10);
            }
            _ => panic!("Expected Float64 column"),
        }
    }

    #[test]
    fn test_power_with_string_base() {
        let base_col = Column::String {
            data: vec!["2".to_string()],
            nulls: NullBitmap::new_valid(1),
        };
        let exp_col = make_float64_col(&[3.0]);
        let result = power(&[base_col, exp_col]);
        assert!(result.is_err());
    }

    #[test]
    fn test_power_with_string_exp() {
        let base_col = make_float64_col(&[2.0]);
        let exp_col = Column::String {
            data: vec!["3".to_string()],
            nulls: NullBitmap::new_valid(1),
        };
        let result = power(&[base_col, exp_col]);
        assert!(result.is_err());
    }

    #[test]
    fn test_log_with_int_base() {
        let col = make_float64_col(&[8.0, 16.0, 32.0]);
        let base = make_int64_col(&[2]);
        let result = log(&[col, base]).unwrap();
        match result {
            Column::Float64 { data, .. } => {
                assert!((data[0] - 3.0).abs() < 1e-10);
                assert!((data[1] - 4.0).abs() < 1e-10);
                assert!((data[2] - 5.0).abs() < 1e-10);
            }
            _ => panic!("Expected Float64 column"),
        }
    }

    #[test]
    fn test_log_with_invalid_base_type() {
        let col = make_float64_col(&[10.0]);
        let base = Column::String {
            data: vec!["2".to_string()],
            nulls: NullBitmap::new_valid(1),
        };
        let result = log(&[col, base]).unwrap();
        match result {
            Column::Float64 { data, .. } => {
                assert!((data[0] - 1.0).abs() < 1e-10);
            }
            _ => panic!("Expected Float64 column"),
        }
    }

    #[test]
    fn test_round_int64() {
        let col = make_int64_col(&[5, 0, -10]);
        let result = round(&[col]).unwrap();
        match result {
            Column::Int64 { data, .. } => {
                assert_eq!(data.as_slice(), &[5, 0, -10]);
            }
            _ => panic!("Expected Int64 column"),
        }
    }

    #[test]
    fn test_empty_arg_errors() {
        assert!(floor(&[]).is_err());
        assert!(ceil(&[]).is_err());
        assert!(round(&[]).is_err());
        assert!(sqrt(&[]).is_err());
        assert!(log(&[]).is_err());
        assert!(log10(&[]).is_err());
        assert!(exp(&[]).is_err());
        assert!(sign(&[]).is_err());
    }

    #[test]
    fn test_power_mixed_types() {
        let base = make_int64_col(&[2, 3]);
        let exp = make_float64_col(&[3.0, 2.0]);
        let result = power(&[base, exp]).unwrap();
        match result {
            Column::Float64 { data, .. } => {
                assert!((data[0] - 8.0).abs() < 1e-10);
                assert!((data[1] - 9.0).abs() < 1e-10);
            }
            _ => panic!("Expected Float64 column"),
        }
    }
}
