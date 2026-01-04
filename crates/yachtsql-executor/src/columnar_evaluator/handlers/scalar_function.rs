#![coverage(off)]

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, FunctionBody, ScalarFunction};
use yachtsql_storage::{Column, Record, Table};

use crate::columnar_evaluator::ColumnarEvaluator;
use crate::js_udf::evaluate_js_function;
use crate::py_udf::evaluate_py_function;
use crate::scalar_functions;
use crate::value_evaluator::ValueEvaluator;

pub fn eval_scalar_function(
    evaluator: &ColumnarEvaluator,
    func: &ScalarFunction,
    args: &[Expr],
    table: &Table,
) -> Result<Column> {
    let arg_cols: Vec<Column> = args
        .iter()
        .map(|a| evaluator.evaluate(a, table))
        .collect::<Result<_>>()?;

    let n = table.row_count();
    if let ScalarFunction::Custom(name) = func {
        let mut results = Vec::with_capacity(n);
        let value_eval = create_value_evaluator(evaluator, table);
        for i in 0..n {
            let arg_vals: Vec<Value> = arg_cols.iter().map(|c| c.get_value(i)).collect();
            let row_values: Vec<Value> = table
                .columns()
                .iter()
                .map(|(_, c)| c.get_value(i))
                .collect();
            let record = Record::from_values(row_values);
            let result = eval_custom_scalar_function(&value_eval, name, &arg_vals, &record)?;
            results.push(result);
        }
        Ok(Column::from_values(&results))
    } else {
        scalar_functions::vectorized::dispatch_vectorized(func, &arg_cols, n)
    }
}

fn create_value_evaluator<'a>(
    evaluator: &'a ColumnarEvaluator<'a>,
    table: &'a Table,
) -> ValueEvaluator<'a> {
    let mut ve = ValueEvaluator::new(table.schema());
    if let Some(vars) = evaluator.variables() {
        ve = ve.with_variables(vars);
    }
    if let Some(sys_vars) = evaluator.system_variables() {
        ve = ve.with_system_variables(sys_vars);
    }
    if let Some(udf) = evaluator.user_functions() {
        ve = ve.with_user_functions(udf);
    }
    ve
}

fn eval_custom_scalar_function(
    evaluator: &ValueEvaluator,
    name: &str,
    args: &[Value],
    _record: &Record,
) -> Result<Value> {
    let upper = name.to_uppercase();
    match upper.as_str() {
        "COALESCE" => {
            for arg in args {
                if !arg.is_null() {
                    return Ok(arg.clone());
                }
            }
            Ok(Value::Null)
        }
        "IFNULL" => {
            if args.len() >= 2 {
                if args[0].is_null() {
                    Ok(args[1].clone())
                } else {
                    Ok(args[0].clone())
                }
            } else {
                Ok(Value::Null)
            }
        }
        "NULLIF" => {
            if args.len() >= 2 && args[0] == args[1] {
                Ok(Value::Null)
            } else if !args.is_empty() {
                Ok(args[0].clone())
            } else {
                Ok(Value::Null)
            }
        }
        "RANGE_CONTAINS" => scalar_functions::range::fn_range_contains(args),
        "HLL_COUNT.EXTRACT" | "HLL_COUNT_EXTRACT" => {
            if args.is_empty() {
                return Ok(Value::Null);
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::String(sketch) => {
                    if let Some(n_part) = sketch.split(':').find(|s| s.starts_with('n'))
                        && let Ok(count) = n_part[1..].parse::<i64>()
                    {
                        return Ok(Value::Int64(count));
                    }
                    Ok(Value::Int64(0))
                }
                _ => Ok(Value::Null),
            }
        }
        "KEYS.NEW_KEYSET" => scalar_functions::crypto::fn_keys_new_keyset(args),
        "AEAD.ENCRYPT" => scalar_functions::crypto::fn_aead_encrypt(args),
        "AEAD.DECRYPT_BYTES" => scalar_functions::crypto::fn_aead_decrypt_bytes(args),
        "AEAD.DECRYPT_STRING" => scalar_functions::crypto::fn_aead_decrypt_string(args),
        _ if upper.starts_with("ST_") => eval_geo_function(&upper, args),
        _ if upper.starts_with("NET.") => eval_net_function(&upper, args),
        _ => {
            if let Some(funcs) = evaluator.user_functions()
                && let Some(func_def) = funcs.get(&upper)
            {
                match &func_def.body {
                    FunctionBody::Sql(expr) => {
                        let mut local_vars = rustc_hash::FxHashMap::default();
                        for (i, param) in func_def.parameters.iter().enumerate() {
                            let val = args.get(i).cloned().unwrap_or(Value::Null);
                            local_vars.insert(param.to_uppercase(), val);
                        }
                        let empty_schema = yachtsql_storage::Schema::new();
                        let func_evaluator = ValueEvaluator::new(&empty_schema)
                            .with_variables(&local_vars)
                            .with_user_functions(funcs);
                        let empty_record = Record::new();
                        return func_evaluator.evaluate(expr.as_ref(), &empty_record);
                    }
                    FunctionBody::JavaScript(code) => {
                        let result = evaluate_js_function(code, &func_def.parameters, args)
                            .map_err(Error::Internal)?;
                        return Ok(result);
                    }
                    FunctionBody::Language { name: lang, code } => {
                        let lang_upper = lang.to_uppercase();
                        if lang_upper == "JS" || lang_upper == "JAVASCRIPT" {
                            let result = evaluate_js_function(code, &func_def.parameters, args)
                                .map_err(Error::Internal)?;
                            return Ok(result);
                        }
                        if lang_upper == "PYTHON" || lang_upper == "PY" {
                            let result = evaluate_py_function(code, &func_def.parameters, args)
                                .map_err(Error::Internal)?;
                            return Ok(result);
                        }
                        return Err(Error::unsupported(format!(
                            "Language '{}' not supported for function: {}",
                            lang, name
                        )));
                    }
                    FunctionBody::SqlQuery(_) => {
                        return Err(Error::unsupported(format!(
                            "SQL query function body not yet supported: {}",
                            name
                        )));
                    }
                }
            }
            Err(Error::unsupported(format!(
                "Custom scalar function '{}' not implemented",
                name
            )))
        }
    }
}

fn eval_geo_function(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "ST_GEOGFROMTEXT" | "ST_GEOGRAPHYFROMTEXT" => {
            scalar_functions::geo::fn_st_geogfromtext(args)
        }
        "ST_GEOGPOINT" | "ST_GEOGRAPHYPOINT" => scalar_functions::geo::fn_st_geogpoint(args),
        "ST_ASTEXT" => scalar_functions::geo::fn_st_astext(args),
        "ST_DISTANCE" => scalar_functions::geo::fn_st_distance(args),
        "ST_AREA" => scalar_functions::geo::fn_st_area(args),
        "ST_LENGTH" => scalar_functions::geo::fn_st_length(args),
        "ST_PERIMETER" => scalar_functions::geo::fn_st_perimeter(args),
        "ST_CONTAINS" => scalar_functions::geo::fn_st_contains(args),
        "ST_INTERSECTS" => scalar_functions::geo::fn_st_intersects(args),
        "ST_WITHIN" => scalar_functions::geo::fn_st_within(args),
        "ST_DWITHIN" => scalar_functions::geo::fn_st_dwithin(args),
        "ST_X" => scalar_functions::geo::fn_st_x(args),
        "ST_Y" => scalar_functions::geo::fn_st_y(args),
        "ST_CENTROID" => scalar_functions::geo::fn_st_centroid(args),
        "ST_BUFFER" => scalar_functions::geo::fn_st_buffer(args),
        "ST_UNION" => scalar_functions::geo::fn_st_union(args),
        "ST_INTERSECTION" => scalar_functions::geo::fn_st_intersection(args),
        "ST_DIFFERENCE" => scalar_functions::geo::fn_st_difference(args),
        _ => Err(yachtsql_common::error::Error::unsupported(format!(
            "Geography function '{}' not implemented",
            name
        ))),
    }
}

fn eval_net_function(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "NET.IP_FROM_STRING" => scalar_functions::net::fn_net_ip_from_string(args),
        "NET.SAFE_IP_FROM_STRING" => scalar_functions::net::fn_net_safe_ip_from_string(args),
        "NET.IP_TO_STRING" => scalar_functions::net::fn_net_ip_to_string(args),
        "NET.HOST" => scalar_functions::net::fn_net_host(args),
        "NET.PUBLIC_SUFFIX" => scalar_functions::net::fn_net_public_suffix(args),
        "NET.REG_DOMAIN" => scalar_functions::net::fn_net_reg_domain(args),
        "NET.IP_IN_NET" => scalar_functions::net::fn_net_ip_in_net(args),
        "NET.MAKE_NET" => scalar_functions::net::fn_net_make_net(args),
        "NET.IP_IS_PRIVATE" => scalar_functions::net::fn_net_ip_is_private(args),
        "NET.IPV4_FROM_INT64" => scalar_functions::net::fn_net_ipv4_from_int64(args),
        "NET.IPV4_TO_INT64" => scalar_functions::net::fn_net_ipv4_to_int64(args),
        "NET.IP_NET_MASK" => scalar_functions::net::fn_net_ip_net_mask(args),
        "NET.IP_TRUNC" => scalar_functions::net::fn_net_ip_trunc(args),
        _ => Err(yachtsql_common::error::Error::unsupported(format!(
            "Net function '{}' not implemented",
            name
        ))),
    }
}
