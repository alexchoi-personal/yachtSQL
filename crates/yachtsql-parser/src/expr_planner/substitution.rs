#![coverage(off)]

use rustc_hash::FxHashMap;
use yachtsql_common::types::DataType;
use yachtsql_ir::plan::FunctionArg;
use yachtsql_ir::{Expr, ScalarFunction, SortExpr, WhenClause};

pub fn substitute_parameters(expr: &Expr, params: &[FunctionArg], args: &[Expr]) -> Expr {
    let mut param_map: FxHashMap<String, Expr> = FxHashMap::default();
    for (i, param) in params.iter().enumerate() {
        let value = if i < args.len() {
            args[i].clone()
        } else if let Some(default) = &param.default {
            default.clone()
        } else {
            continue;
        };
        param_map.insert(param.name.to_uppercase(), value);
    }
    let param_ref_map: FxHashMap<String, &Expr> =
        param_map.iter().map(|(k, v)| (k.clone(), v)).collect();

    substitute_expr(expr, &param_ref_map)
}

pub fn apply_struct_field_names(expr: Expr, return_type: &DataType) -> Expr {
    match (&expr, return_type) {
        (Expr::Struct { fields }, DataType::Struct(struct_fields)) => {
            let new_fields: Vec<(Option<String>, Expr)> = fields
                .iter()
                .enumerate()
                .map(|(i, (existing_name, field_expr))| {
                    let field_name = if existing_name.is_some() {
                        existing_name.clone()
                    } else if i < struct_fields.len() {
                        Some(struct_fields[i].name.clone())
                    } else {
                        None
                    };
                    (field_name, field_expr.clone())
                })
                .collect();
            Expr::Struct { fields: new_fields }
        }
        _ => expr,
    }
}

pub fn substitute_expr(expr: &Expr, param_map: &FxHashMap<String, &Expr>) -> Expr {
    match expr {
        Expr::Column { name, table, index } => {
            let upper_name = name.to_uppercase();
            if table.is_none()
                && let Some(replacement) = param_map.get(&upper_name)
            {
                return (*replacement).clone();
            }
            Expr::Column {
                name: name.clone(),
                table: table.clone(),
                index: *index,
            }
        }
        Expr::Aggregate {
            func,
            args,
            distinct,
            filter,
            order_by,
            limit,
            ignore_nulls,
        } => Expr::Aggregate {
            func: *func,
            args: args.iter().map(|a| substitute_expr(a, param_map)).collect(),
            distinct: *distinct,
            filter: filter
                .as_ref()
                .map(|f| Box::new(substitute_expr(f, param_map))),
            order_by: order_by
                .iter()
                .map(|o| SortExpr {
                    expr: substitute_expr(&o.expr, param_map),
                    asc: o.asc,
                    nulls_first: o.nulls_first,
                })
                .collect(),
            limit: *limit,
            ignore_nulls: *ignore_nulls,
        },
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(substitute_expr(left, param_map)),
            op: *op,
            right: Box::new(substitute_expr(right, param_map)),
        },
        Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(substitute_expr(inner, param_map)),
        },
        Expr::ScalarFunction { name, args } => {
            if let ScalarFunction::Custom(func_name) = name
                && let Some(replacement) = param_map.get(&func_name.to_uppercase())
                && let Expr::Lambda {
                    params: lambda_params,
                    body: lambda_body,
                } = replacement
            {
                let substituted_args: Vec<Expr> =
                    args.iter().map(|a| substitute_expr(a, param_map)).collect();
                let lambda_param_map: FxHashMap<String, &Expr> = lambda_params
                    .iter()
                    .zip(substituted_args.iter())
                    .map(|(p, a)| (p.to_uppercase(), a))
                    .collect();
                return substitute_expr(lambda_body, &lambda_param_map);
            }
            Expr::ScalarFunction {
                name: name.clone(),
                args: args.iter().map(|a| substitute_expr(a, param_map)).collect(),
            }
        }
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => Expr::Case {
            operand: operand
                .as_ref()
                .map(|o| Box::new(substitute_expr(o, param_map))),
            when_clauses: when_clauses
                .iter()
                .map(|wc| WhenClause {
                    condition: substitute_expr(&wc.condition, param_map),
                    result: substitute_expr(&wc.result, param_map),
                })
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|e| Box::new(substitute_expr(e, param_map))),
        },
        Expr::Cast {
            expr: inner,
            data_type,
            safe,
        } => Expr::Cast {
            expr: Box::new(substitute_expr(inner, param_map)),
            data_type: data_type.clone(),
            safe: *safe,
        },
        Expr::Alias { expr: inner, name } => Expr::Alias {
            expr: Box::new(substitute_expr(inner, param_map)),
            name: name.clone(),
        },
        Expr::IsNull {
            expr: inner,
            negated,
        } => Expr::IsNull {
            expr: Box::new(substitute_expr(inner, param_map)),
            negated: *negated,
        },
        Expr::Between {
            expr: inner,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(substitute_expr(inner, param_map)),
            low: Box::new(substitute_expr(low, param_map)),
            high: Box::new(substitute_expr(high, param_map)),
            negated: *negated,
        },
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(substitute_expr(inner, param_map)),
            list: list.iter().map(|e| substitute_expr(e, param_map)).collect(),
            negated: *negated,
        },
        Expr::Struct { fields } => Expr::Struct {
            fields: fields
                .iter()
                .map(|(name, e)| (name.clone(), substitute_expr(e, param_map)))
                .collect(),
        },
        Expr::StructAccess { expr: inner, field } => Expr::StructAccess {
            expr: Box::new(substitute_expr(inner, param_map)),
            field: field.clone(),
        },
        Expr::Array {
            elements,
            element_type,
        } => Expr::Array {
            elements: elements
                .iter()
                .map(|e| substitute_expr(e, param_map))
                .collect(),
            element_type: element_type.clone(),
        },
        Expr::ArrayAccess { array, index } => Expr::ArrayAccess {
            array: Box::new(substitute_expr(array, param_map)),
            index: Box::new(substitute_expr(index, param_map)),
        },
        Expr::Window {
            func,
            args,
            partition_by,
            order_by,
            frame,
        } => Expr::Window {
            func: *func,
            args: args.iter().map(|a| substitute_expr(a, param_map)).collect(),
            partition_by: partition_by
                .iter()
                .map(|e| substitute_expr(e, param_map))
                .collect(),
            order_by: order_by
                .iter()
                .map(|o| SortExpr {
                    expr: substitute_expr(&o.expr, param_map),
                    asc: o.asc,
                    nulls_first: o.nulls_first,
                })
                .collect(),
            frame: frame.clone(),
        },
        Expr::AggregateWindow {
            func,
            args,
            distinct,
            partition_by,
            order_by,
            frame,
        } => Expr::AggregateWindow {
            func: *func,
            args: args.iter().map(|a| substitute_expr(a, param_map)).collect(),
            distinct: *distinct,
            partition_by: partition_by
                .iter()
                .map(|e| substitute_expr(e, param_map))
                .collect(),
            order_by: order_by
                .iter()
                .map(|o| SortExpr {
                    expr: substitute_expr(&o.expr, param_map),
                    asc: o.asc,
                    nulls_first: o.nulls_first,
                })
                .collect(),
            frame: frame.clone(),
        },
        Expr::Like {
            expr: inner,
            pattern,
            negated,
            case_insensitive,
        } => Expr::Like {
            expr: Box::new(substitute_expr(inner, param_map)),
            pattern: Box::new(substitute_expr(pattern, param_map)),
            negated: *negated,
            case_insensitive: *case_insensitive,
        },
        Expr::Extract { field, expr: inner } => Expr::Extract {
            field: *field,
            expr: Box::new(substitute_expr(inner, param_map)),
        },
        Expr::Substring {
            expr: inner,
            start,
            length,
        } => Expr::Substring {
            expr: Box::new(substitute_expr(inner, param_map)),
            start: start
                .as_ref()
                .map(|s| Box::new(substitute_expr(s, param_map))),
            length: length
                .as_ref()
                .map(|l| Box::new(substitute_expr(l, param_map))),
        },
        Expr::Trim {
            expr: inner,
            trim_what,
            trim_where,
        } => Expr::Trim {
            expr: Box::new(substitute_expr(inner, param_map)),
            trim_what: trim_what
                .as_ref()
                .map(|t| Box::new(substitute_expr(t, param_map))),
            trim_where: *trim_where,
        },
        Expr::Position { substr, string } => Expr::Position {
            substr: Box::new(substitute_expr(substr, param_map)),
            string: Box::new(substitute_expr(string, param_map)),
        },
        Expr::Overlay {
            expr: inner,
            overlay_what,
            overlay_from,
            overlay_for,
        } => Expr::Overlay {
            expr: Box::new(substitute_expr(inner, param_map)),
            overlay_what: Box::new(substitute_expr(overlay_what, param_map)),
            overlay_from: Box::new(substitute_expr(overlay_from, param_map)),
            overlay_for: overlay_for
                .as_ref()
                .map(|f| Box::new(substitute_expr(f, param_map))),
        },
        Expr::Interval {
            value,
            leading_field,
        } => Expr::Interval {
            value: Box::new(substitute_expr(value, param_map)),
            leading_field: *leading_field,
        },
        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => Expr::AtTimeZone {
            timestamp: Box::new(substitute_expr(timestamp, param_map)),
            time_zone: Box::new(substitute_expr(time_zone, param_map)),
        },
        Expr::JsonAccess { expr: inner, path } => Expr::JsonAccess {
            expr: Box::new(substitute_expr(inner, param_map)),
            path: path.clone(),
        },
        Expr::InUnnest {
            expr: inner,
            array_expr,
            negated,
        } => Expr::InUnnest {
            expr: Box::new(substitute_expr(inner, param_map)),
            array_expr: Box::new(substitute_expr(array_expr, param_map)),
            negated: *negated,
        },
        Expr::UserDefinedAggregate {
            name,
            args,
            distinct,
            filter,
        } => Expr::UserDefinedAggregate {
            name: name.clone(),
            args: args.iter().map(|a| substitute_expr(a, param_map)).collect(),
            distinct: *distinct,
            filter: filter
                .as_ref()
                .map(|f| Box::new(substitute_expr(f, param_map))),
        },
        Expr::IsDistinctFrom {
            left,
            right,
            negated,
        } => Expr::IsDistinctFrom {
            left: Box::new(substitute_expr(left, param_map)),
            right: Box::new(substitute_expr(right, param_map)),
            negated: *negated,
        },
        Expr::Lambda { params, body } => Expr::Lambda {
            params: params.clone(),
            body: Box::new(substitute_expr(body, param_map)),
        },
        other => other.clone(),
    }
}
