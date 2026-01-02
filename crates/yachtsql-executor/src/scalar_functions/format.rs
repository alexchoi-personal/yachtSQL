#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

fn format_with_grouping(n: i64) -> String {
    let sign = if n < 0 { "-" } else { "" };
    let abs_n = n.abs();
    let s = abs_n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    format!("{}{}", sign, result.chars().rev().collect::<String>())
}

fn format_scientific(f: f64, precision: usize, uppercase: bool) -> String {
    if f == 0.0 {
        let e_char = if uppercase { 'E' } else { 'e' };
        return format!("{:.prec$}{}+00", 0.0, e_char, prec = precision);
    }
    let exp = f.abs().log10().floor() as i32;
    let mantissa = f / 10_f64.powi(exp);
    let e_char = if uppercase { 'E' } else { 'e' };
    let sign = if exp >= 0 { '+' } else { '-' };
    format!(
        "{:.prec$}{}{}{:02}",
        mantissa,
        e_char,
        sign,
        exp.abs(),
        prec = precision
    )
}

fn format_value_for_format(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => f.0.to_string(),
        Value::Numeric(n) => n.to_string(),
        Value::BigNumeric(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Bytes(b) => format!("{:?}", b),
        Value::Date(d) => d.to_string(),
        Value::Time(t) => t.to_string(),
        Value::DateTime(dt) => dt.to_string(),
        Value::Timestamp(ts) => ts.to_rfc3339(),
        Value::Json(j) => j.to_string(),
        Value::Array(arr) => format!("{:?}", arr),
        Value::Struct(fields) => format!("{:?}", fields),
        Value::Geography(g) => g.clone(),
        Value::Interval(i) => format!("{:?}", i),
        Value::Range(r) => format!("{:?}", r),
        Value::Default => "DEFAULT".to_string(),
    }
}

pub fn fn_format(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery(
            "FORMAT requires at least 1 argument".into(),
        ));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(fmt) => {
            let format_args = &args[1..];
            let mut arg_index = 0;
            let mut result = String::new();
            let mut chars = fmt.chars().peekable();

            while let Some(c) = chars.next() {
                if c == '%' {
                    if chars.peek() == Some(&'%') {
                        chars.next();
                        result.push('%');
                        continue;
                    }

                    let mut zero_pad = false;
                    let mut use_grouping = false;
                    let mut width: Option<usize> = None;
                    let mut precision: Option<usize> = None;

                    while let Some(&ch) = chars.peek() {
                        if ch == '0' && width.is_none() {
                            zero_pad = true;
                            chars.next();
                        } else if ch == '\'' {
                            use_grouping = true;
                            chars.next();
                        } else {
                            break;
                        }
                    }

                    let mut width_str = String::new();
                    while let Some(&ch) = chars.peek() {
                        if ch.is_ascii_digit() {
                            width_str.push(ch);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    if !width_str.is_empty() {
                        width = width_str.parse().ok();
                    }

                    if chars.peek() == Some(&'.') {
                        chars.next();
                        let mut prec_str = String::new();
                        while let Some(&ch) = chars.peek() {
                            if ch.is_ascii_digit() {
                                prec_str.push(ch);
                                chars.next();
                            } else {
                                break;
                            }
                        }
                        if !prec_str.is_empty() {
                            precision = prec_str.parse().ok();
                        }
                    }

                    if let Some(&spec) = chars.peek() {
                        chars.next();
                        let val = format_args.get(arg_index);
                        arg_index += 1;

                        let formatted = match spec {
                            's' => val.map(format_value_for_format).unwrap_or_default(),
                            'd' | 'i' => {
                                let n = val.and_then(|v| v.as_i64()).unwrap_or(0);
                                let s = if use_grouping {
                                    format_with_grouping(n)
                                } else {
                                    n.to_string()
                                };
                                if let Some(w) = width {
                                    if zero_pad {
                                        format!("{:0>width$}", s, width = w)
                                    } else {
                                        format!("{:>width$}", s, width = w)
                                    }
                                } else {
                                    s
                                }
                            }
                            'f' | 'F' => {
                                let f = val.and_then(|v| v.as_f64()).unwrap_or(0.0);
                                let prec = precision.unwrap_or(6);
                                format!("{:.prec$}", f, prec = prec)
                            }
                            'e' => {
                                let f = val.and_then(|v| v.as_f64()).unwrap_or(0.0);
                                let prec = precision.unwrap_or(6);
                                format_scientific(f, prec, false)
                            }
                            'E' => {
                                let f = val.and_then(|v| v.as_f64()).unwrap_or(0.0);
                                let prec = precision.unwrap_or(6);
                                format_scientific(f, prec, true)
                            }
                            'g' | 'G' => {
                                let f = val.and_then(|v| v.as_f64()).unwrap_or(0.0);
                                let prec = precision.unwrap_or(6);
                                if f.abs() >= 1e-4 && f.abs() < 10_f64.powi(prec as i32) {
                                    format!("{:.prec$}", f, prec = prec)
                                } else {
                                    format_scientific(f, prec, spec == 'G')
                                }
                            }
                            'o' => {
                                let n = val.and_then(|v| v.as_i64()).unwrap_or(0);
                                format!("{:o}", n)
                            }
                            'x' => {
                                let n = val.and_then(|v| v.as_i64()).unwrap_or(0);
                                format!("{:x}", n)
                            }
                            'X' => {
                                let n = val.and_then(|v| v.as_i64()).unwrap_or(0);
                                format!("{:X}", n)
                            }
                            't' | 'T' => val.map(format_value_for_format).unwrap_or_default(),
                            'p' | 'P' => {
                                let f = val.and_then(|v| v.as_f64()).unwrap_or(0.0);
                                format!("{}", f * 100.0)
                            }
                            _ => format!("%{}", spec),
                        };
                        result.push_str(&formatted);
                    }
                } else {
                    result.push(c);
                }
            }
            Ok(Value::String(result))
        }
        _ => Err(Error::InvalidQuery("FORMAT requires string format".into())),
    }
}

pub fn fn_error(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::InvalidQuery("ERROR requires 1 argument".into()));
    }
    match &args[0] {
        Value::Null => Err(Error::InvalidQuery("NULL".into())),
        Value::String(msg) => Err(Error::InvalidQuery(msg.clone())),
        v => Err(Error::InvalidQuery(format_value_for_format(v))),
    }
}

pub fn fn_session_user(_args: &[Value]) -> Result<Value> {
    Ok(Value::String("anonymous".to_string()))
}
