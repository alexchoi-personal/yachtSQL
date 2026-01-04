use rustc_hash::{FxHashMap, FxHashSet};
use yachtsql_ir::{BinaryOp, Expr, JoinType, Literal};

use crate::join_order::CostModel;

#[derive(PartialEq, Clone, Copy)]
pub enum PredicateSide {
    Left,
    Right,
    Both,
}

pub fn classify_predicate_side(expr: &Expr, left_schema_len: usize) -> Option<PredicateSide> {
    match expr {
        Expr::Column {
            index: Some(idx), ..
        } => {
            if *idx < left_schema_len {
                Some(PredicateSide::Left)
            } else {
                Some(PredicateSide::Right)
            }
        }
        Expr::Column { index: None, .. } => None,
        Expr::BinaryOp { left, right, .. } => {
            let left_side = classify_predicate_side(left, left_schema_len);
            let right_side = classify_predicate_side(right, left_schema_len);
            match (left_side, right_side) {
                (None, None) => Some(PredicateSide::Both),
                (Some(l), None) => Some(l),
                (None, Some(r)) => Some(r),
                (Some(PredicateSide::Left), Some(PredicateSide::Left)) => Some(PredicateSide::Left),
                (Some(PredicateSide::Right), Some(PredicateSide::Right)) => {
                    Some(PredicateSide::Right)
                }
                _ => Some(PredicateSide::Both),
            }
        }
        Expr::UnaryOp { expr, .. } => classify_predicate_side(expr, left_schema_len),
        Expr::IsNull { expr, .. } => classify_predicate_side(expr, left_schema_len),
        Expr::ScalarFunction { args, .. } => {
            let mut result: Option<PredicateSide> = None;
            for arg in args {
                match (result, classify_predicate_side(arg, left_schema_len)) {
                    (None, side) => result = side,
                    (Some(PredicateSide::Left), Some(PredicateSide::Left)) => {}
                    (Some(PredicateSide::Right), Some(PredicateSide::Right)) => {}
                    (Some(_), Some(_)) => return Some(PredicateSide::Both),
                    (Some(s), None) => result = Some(s),
                }
            }
            result.or(Some(PredicateSide::Both))
        }
        Expr::Literal(_) => None,
        Expr::Cast { expr, .. } => classify_predicate_side(expr, left_schema_len),
        Expr::Like { expr, pattern, .. } => {
            let expr_side = classify_predicate_side(expr, left_schema_len);
            let pattern_side = classify_predicate_side(pattern, left_schema_len);
            match (expr_side, pattern_side) {
                (None, s) | (s, None) => s,
                (Some(PredicateSide::Left), Some(PredicateSide::Left)) => Some(PredicateSide::Left),
                (Some(PredicateSide::Right), Some(PredicateSide::Right)) => {
                    Some(PredicateSide::Right)
                }
                _ => Some(PredicateSide::Both),
            }
        }
        Expr::InList { expr, list, .. } => {
            let mut result = classify_predicate_side(expr, left_schema_len);
            for item in list {
                match (result, classify_predicate_side(item, left_schema_len)) {
                    (None, side) => result = side,
                    (Some(PredicateSide::Left), Some(PredicateSide::Left)) => {}
                    (Some(PredicateSide::Right), Some(PredicateSide::Right)) => {}
                    (Some(_), Some(_)) => return Some(PredicateSide::Both),
                    (Some(s), None) => result = Some(s),
                }
            }
            result.or(Some(PredicateSide::Both))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            let sides = [
                classify_predicate_side(expr, left_schema_len),
                classify_predicate_side(low, left_schema_len),
                classify_predicate_side(high, left_schema_len),
            ];
            let mut result: Option<PredicateSide> = None;
            for side in sides.into_iter().flatten() {
                match (result, side) {
                    (None, s) => result = Some(s),
                    (Some(PredicateSide::Left), PredicateSide::Left) => {}
                    (Some(PredicateSide::Right), PredicateSide::Right) => {}
                    _ => return Some(PredicateSide::Both),
                }
            }
            result.or(Some(PredicateSide::Both))
        }
        _ => Some(PredicateSide::Both),
    }
}

pub fn split_and_predicates(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
        } => {
            let mut result = split_and_predicates(left);
            result.extend(split_and_predicates(right));
            result
        }
        other => vec![other.clone()],
    }
}

pub fn combine_predicates(predicates: Vec<Expr>) -> Option<Expr> {
    predicates.into_iter().reduce(|acc, pred| Expr::BinaryOp {
        left: Box::new(acc),
        op: BinaryOp::And,
        right: Box::new(pred),
    })
}

pub fn adjust_predicate_indices(expr: &Expr, offset: usize) -> Expr {
    match expr {
        Expr::Column { table, name, index } => Expr::Column {
            table: table.clone(),
            name: name.clone(),
            index: index.map(|i| i.saturating_sub(offset)),
        },
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(adjust_predicate_indices(left, offset)),
            op: *op,
            right: Box::new(adjust_predicate_indices(right, offset)),
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(adjust_predicate_indices(expr, offset)),
        },
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(adjust_predicate_indices(expr, offset)),
            negated: *negated,
        },
        Expr::ScalarFunction { name, args } => Expr::ScalarFunction {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| adjust_predicate_indices(a, offset))
                .collect(),
        },
        Expr::Cast {
            expr,
            data_type,
            safe,
        } => Expr::Cast {
            expr: Box::new(adjust_predicate_indices(expr, offset)),
            data_type: data_type.clone(),
            safe: *safe,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => Expr::Like {
            expr: Box::new(adjust_predicate_indices(expr, offset)),
            pattern: Box::new(adjust_predicate_indices(pattern, offset)),
            negated: *negated,
            case_insensitive: *case_insensitive,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(adjust_predicate_indices(expr, offset)),
            list: list
                .iter()
                .map(|i| adjust_predicate_indices(i, offset))
                .collect(),
            negated: *negated,
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(adjust_predicate_indices(expr, offset)),
            low: Box::new(adjust_predicate_indices(low, offset)),
            high: Box::new(adjust_predicate_indices(high, offset)),
            negated: *negated,
        },
        other => other.clone(),
    }
}

pub fn collect_column_indices(expr: &Expr) -> FxHashSet<usize> {
    let mut indices = FxHashSet::default();
    collect_column_indices_into(expr, &mut indices);
    indices
}

pub fn collect_column_indices_into(expr: &Expr, indices: &mut FxHashSet<usize>) {
    match expr {
        Expr::Column {
            index: Some(idx), ..
        } => {
            indices.insert(*idx);
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_column_indices_into(left, indices);
            collect_column_indices_into(right, indices);
        }
        Expr::UnaryOp { expr, .. } => {
            collect_column_indices_into(expr, indices);
        }
        Expr::IsNull { expr, .. } => {
            collect_column_indices_into(expr, indices);
        }
        Expr::IsDistinctFrom { left, right, .. } => {
            collect_column_indices_into(left, indices);
            collect_column_indices_into(right, indices);
        }
        Expr::ScalarFunction { args, .. } => {
            for arg in args {
                collect_column_indices_into(arg, indices);
            }
        }
        Expr::Cast { expr, .. } => {
            collect_column_indices_into(expr, indices);
        }
        Expr::Alias { expr, .. } => {
            collect_column_indices_into(expr, indices);
        }
        Expr::Like { expr, pattern, .. } => {
            collect_column_indices_into(expr, indices);
            collect_column_indices_into(pattern, indices);
        }
        Expr::InList { expr, list, .. } => {
            collect_column_indices_into(expr, indices);
            for item in list {
                collect_column_indices_into(item, indices);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_column_indices_into(expr, indices);
            collect_column_indices_into(low, indices);
            collect_column_indices_into(high, indices);
        }
        Expr::Case {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_column_indices_into(op, indices);
            }
            for wc in when_clauses {
                collect_column_indices_into(&wc.condition, indices);
                collect_column_indices_into(&wc.result, indices);
            }
            if let Some(else_expr) = else_result {
                collect_column_indices_into(else_expr, indices);
            }
        }
        Expr::Extract { expr, .. } => {
            collect_column_indices_into(expr, indices);
        }
        Expr::Substring {
            expr,
            start,
            length,
            ..
        } => {
            collect_column_indices_into(expr, indices);
            if let Some(s) = start {
                collect_column_indices_into(s, indices);
            }
            if let Some(l) = length {
                collect_column_indices_into(l, indices);
            }
        }
        Expr::Trim {
            expr, trim_what, ..
        } => {
            collect_column_indices_into(expr, indices);
            if let Some(tw) = trim_what {
                collect_column_indices_into(tw, indices);
            }
        }
        Expr::Position { substr, string } => {
            collect_column_indices_into(substr, indices);
            collect_column_indices_into(string, indices);
        }
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            collect_column_indices_into(expr, indices);
            collect_column_indices_into(overlay_what, indices);
            collect_column_indices_into(overlay_from, indices);
            if let Some(f) = overlay_for {
                collect_column_indices_into(f, indices);
            }
        }
        Expr::ArrayAccess { array, index, .. } => {
            collect_column_indices_into(array, indices);
            collect_column_indices_into(index, indices);
        }
        Expr::StructAccess { expr, .. } => {
            collect_column_indices_into(expr, indices);
        }
        Expr::Array { elements, .. } => {
            for elem in elements {
                collect_column_indices_into(elem, indices);
            }
        }
        Expr::Struct { fields, .. } => {
            for (_, field_expr) in fields {
                collect_column_indices_into(field_expr, indices);
            }
        }
        Expr::Aggregate {
            args,
            filter,
            order_by,
            ..
        } => {
            for arg in args {
                collect_column_indices_into(arg, indices);
            }
            if let Some(f) = filter {
                collect_column_indices_into(f, indices);
            }
            for ob in order_by {
                collect_column_indices_into(&ob.expr, indices);
            }
        }
        Expr::UserDefinedAggregate { args, filter, .. } => {
            for arg in args {
                collect_column_indices_into(arg, indices);
            }
            if let Some(f) = filter {
                collect_column_indices_into(f, indices);
            }
        }
        Expr::Window {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_column_indices_into(arg, indices);
            }
            for pb in partition_by {
                collect_column_indices_into(pb, indices);
            }
            for ob in order_by {
                collect_column_indices_into(&ob.expr, indices);
            }
        }
        Expr::AggregateWindow {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_column_indices_into(arg, indices);
            }
            for pb in partition_by {
                collect_column_indices_into(pb, indices);
            }
            for ob in order_by {
                collect_column_indices_into(&ob.expr, indices);
            }
        }
        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => {
            collect_column_indices_into(timestamp, indices);
            collect_column_indices_into(time_zone, indices);
        }
        Expr::JsonAccess { expr, .. } => {
            collect_column_indices_into(expr, indices);
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            collect_column_indices_into(expr, indices);
            collect_column_indices_into(array_expr, indices);
        }
        Expr::InSubquery { expr, .. } => {
            collect_column_indices_into(expr, indices);
        }
        Expr::Lambda { body, .. } => {
            collect_column_indices_into(body, indices);
        }
        Expr::Interval { value, .. } => {
            collect_column_indices_into(value, indices);
        }
        Expr::Column { index: None, .. }
        | Expr::Literal(_)
        | Expr::Parameter { .. }
        | Expr::Variable { .. }
        | Expr::Placeholder { .. }
        | Expr::TypedString { .. }
        | Expr::Wildcard { .. }
        | Expr::Default
        | Expr::Subquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::ArraySubquery(_)
        | Expr::Exists { .. } => {}
    }
}

pub fn classify_predicates_for_join(
    join_type: JoinType,
    predicates: &[Expr],
    left_schema_len: usize,
) -> (Vec<Expr>, Vec<Expr>, Vec<Expr>) {
    let mut pushable_left = Vec::new();
    let mut pushable_right = Vec::new();
    let mut post_join = Vec::new();

    for pred in predicates {
        let side = classify_predicate_side(pred, left_schema_len);

        match (join_type, side) {
            (JoinType::Inner, Some(PredicateSide::Left)) => {
                pushable_left.push(pred.clone());
            }
            (JoinType::Inner, Some(PredicateSide::Right)) => {
                pushable_right.push(adjust_predicate_indices(pred, left_schema_len));
            }
            (JoinType::Left, Some(PredicateSide::Left)) => {
                pushable_left.push(pred.clone());
            }
            (JoinType::Right, Some(PredicateSide::Right)) => {
                pushable_right.push(adjust_predicate_indices(pred, left_schema_len));
            }
            _ => {
                post_join.push(pred.clone());
            }
        }
    }

    (pushable_left, pushable_right, post_join)
}

pub fn build_aggregate_output_to_input_map(group_by: &[Expr]) -> FxHashMap<usize, usize> {
    let mut map = FxHashMap::default();
    for (output_idx, expr) in group_by.iter().enumerate() {
        if let Expr::Column {
            index: Some(input_idx),
            ..
        } = expr
        {
            map.insert(output_idx, *input_idx);
        }
    }
    map
}

pub fn remap_predicate_indices(
    expr: &Expr,
    output_to_input: &FxHashMap<usize, usize>,
) -> Option<Expr> {
    match expr {
        Expr::Column {
            table,
            name,
            index: Some(idx),
        } => output_to_input.get(idx).map(|&new_idx| Expr::Column {
            table: table.clone(),
            name: name.clone(),
            index: Some(new_idx),
        }),
        Expr::Column { index: None, .. } => Some(expr.clone()),
        Expr::Literal(_) => Some(expr.clone()),
        Expr::BinaryOp { left, op, right } => {
            let new_left = remap_predicate_indices(left, output_to_input)?;
            let new_right = remap_predicate_indices(right, output_to_input)?;
            Some(Expr::BinaryOp {
                left: Box::new(new_left),
                op: *op,
                right: Box::new(new_right),
            })
        }
        Expr::UnaryOp { op, expr } => {
            let new_expr = remap_predicate_indices(expr, output_to_input)?;
            Some(Expr::UnaryOp {
                op: *op,
                expr: Box::new(new_expr),
            })
        }
        Expr::IsNull { expr, negated } => {
            let new_expr = remap_predicate_indices(expr, output_to_input)?;
            Some(Expr::IsNull {
                expr: Box::new(new_expr),
                negated: *negated,
            })
        }
        Expr::IsDistinctFrom {
            left,
            right,
            negated,
        } => {
            let new_left = remap_predicate_indices(left, output_to_input)?;
            let new_right = remap_predicate_indices(right, output_to_input)?;
            Some(Expr::IsDistinctFrom {
                left: Box::new(new_left),
                right: Box::new(new_right),
                negated: *negated,
            })
        }
        Expr::Cast {
            expr,
            data_type,
            safe,
        } => {
            let new_expr = remap_predicate_indices(expr, output_to_input)?;
            Some(Expr::Cast {
                expr: Box::new(new_expr),
                data_type: data_type.clone(),
                safe: *safe,
            })
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => {
            let new_expr = remap_predicate_indices(expr, output_to_input)?;
            let new_pattern = remap_predicate_indices(pattern, output_to_input)?;
            Some(Expr::Like {
                expr: Box::new(new_expr),
                pattern: Box::new(new_pattern),
                negated: *negated,
                case_insensitive: *case_insensitive,
            })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let new_expr = remap_predicate_indices(expr, output_to_input)?;
            let new_list: Option<Vec<_>> = list
                .iter()
                .map(|e| remap_predicate_indices(e, output_to_input))
                .collect();
            Some(Expr::InList {
                expr: Box::new(new_expr),
                list: new_list?,
                negated: *negated,
            })
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let new_expr = remap_predicate_indices(expr, output_to_input)?;
            let new_low = remap_predicate_indices(low, output_to_input)?;
            let new_high = remap_predicate_indices(high, output_to_input)?;
            Some(Expr::Between {
                expr: Box::new(new_expr),
                low: Box::new(new_low),
                high: Box::new(new_high),
                negated: *negated,
            })
        }
        Expr::ScalarFunction { name, args } => {
            let new_args: Option<Vec<_>> = args
                .iter()
                .map(|a| remap_predicate_indices(a, output_to_input))
                .collect();
            Some(Expr::ScalarFunction {
                name: name.clone(),
                args: new_args?,
            })
        }
        _ => None,
    }
}

pub fn can_push_through_aggregate(predicate: &Expr, num_group_by_cols: usize) -> bool {
    let pred_columns = collect_column_indices(predicate);
    pred_columns.iter().all(|&idx| idx < num_group_by_cols)
}

pub fn can_push_through_window(predicate: &Expr, input_schema_len: usize) -> bool {
    let pred_columns = collect_column_indices(predicate);
    pred_columns.iter().all(|&idx| idx < input_schema_len)
}

#[allow(dead_code)]
pub fn estimate_predicate_selectivity(expr: &Expr, _cost_model: &CostModel) -> f64 {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            estimate_binary_op_selectivity(left, *op, right, _cost_model)
        }
        Expr::IsNull { negated, .. } => {
            if *negated {
                0.99
            } else {
                0.01
            }
        }
        Expr::InList { list, negated, .. } => {
            let base_selectivity = (list.len() as f64 * 0.1).min(0.5);
            if *negated {
                1.0 - base_selectivity
            } else {
                base_selectivity
            }
        }
        Expr::Like {
            pattern, negated, ..
        } => {
            let base_selectivity = estimate_like_selectivity(pattern);
            if *negated {
                1.0 - base_selectivity
            } else {
                base_selectivity
            }
        }
        Expr::Between { negated, .. } => {
            let base_selectivity = 0.25;
            if *negated {
                1.0 - base_selectivity
            } else {
                base_selectivity
            }
        }
        Expr::UnaryOp { expr, .. } => estimate_predicate_selectivity(expr, _cost_model),
        _ => 0.5,
    }
}

#[allow(dead_code)]
fn estimate_binary_op_selectivity(
    _left: &Expr,
    op: BinaryOp,
    _right: &Expr,
    _cost_model: &CostModel,
) -> f64 {
    match op {
        BinaryOp::Eq => 0.1,
        BinaryOp::NotEq => 0.9,
        BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => 0.3,
        BinaryOp::And => {
            let left_sel = estimate_predicate_selectivity(_left, _cost_model);
            let right_sel = estimate_predicate_selectivity(_right, _cost_model);
            left_sel * right_sel
        }
        BinaryOp::Or => {
            let left_sel = estimate_predicate_selectivity(_left, _cost_model);
            let right_sel = estimate_predicate_selectivity(_right, _cost_model);
            (left_sel + right_sel - left_sel * right_sel).min(1.0)
        }
        BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::Mod
        | BinaryOp::Concat
        | BinaryOp::BitwiseAnd
        | BinaryOp::BitwiseOr
        | BinaryOp::BitwiseXor
        | BinaryOp::ShiftLeft
        | BinaryOp::ShiftRight => 0.5,
    }
}

#[allow(dead_code)]
fn estimate_like_selectivity(pattern: &Expr) -> f64 {
    match pattern {
        Expr::Literal(Literal::String(s)) => {
            if s.starts_with('%') {
                0.5
            } else {
                0.1
            }
        }
        _ => 0.5,
    }
}

#[allow(dead_code)]
pub fn combine_predicates_ordered(
    mut predicates: Vec<Expr>,
    cost_model: &CostModel,
) -> Option<Expr> {
    if predicates.is_empty() {
        return None;
    }

    predicates.sort_by(|a, b| {
        let sel_a = estimate_predicate_selectivity(a, cost_model);
        let sel_b = estimate_predicate_selectivity(b, cost_model);
        sel_a
            .partial_cmp(&sel_b)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut result = predicates.remove(0);
    for pred in predicates {
        result = Expr::BinaryOp {
            left: Box::new(result),
            op: BinaryOp::And,
            right: Box::new(pred),
        };
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_eq_predicate(col_name: &str, value: i64) -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: None,
                name: col_name.to_string(),
                index: Some(0),
            }),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Literal::Int64(value))),
        }
    }

    fn make_lt_predicate(col_name: &str, value: i64) -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: None,
                name: col_name.to_string(),
                index: Some(0),
            }),
            op: BinaryOp::Lt,
            right: Box::new(Expr::Literal(Literal::Int64(value))),
        }
    }

    fn make_is_null_predicate(col_name: &str) -> Expr {
        Expr::IsNull {
            expr: Box::new(Expr::Column {
                table: None,
                name: col_name.to_string(),
                index: Some(0),
            }),
            negated: false,
        }
    }

    fn make_is_not_null_predicate(col_name: &str) -> Expr {
        Expr::IsNull {
            expr: Box::new(Expr::Column {
                table: None,
                name: col_name.to_string(),
                index: Some(0),
            }),
            negated: true,
        }
    }

    fn make_in_list_predicate(col_name: &str, values: Vec<i64>) -> Expr {
        Expr::InList {
            expr: Box::new(Expr::Column {
                table: None,
                name: col_name.to_string(),
                index: Some(0),
            }),
            list: values
                .into_iter()
                .map(|v| Expr::Literal(Literal::Int64(v)))
                .collect(),
            negated: false,
        }
    }

    fn make_like_predicate(col_name: &str, pattern: &str) -> Expr {
        Expr::Like {
            expr: Box::new(Expr::Column {
                table: None,
                name: col_name.to_string(),
                index: Some(0),
            }),
            pattern: Box::new(Expr::Literal(Literal::String(pattern.to_string()))),
            negated: false,
            case_insensitive: false,
        }
    }

    fn make_between_predicate(col_name: &str, low: i64, high: i64) -> Expr {
        Expr::Between {
            expr: Box::new(Expr::Column {
                table: None,
                name: col_name.to_string(),
                index: Some(0),
            }),
            low: Box::new(Expr::Literal(Literal::Int64(low))),
            high: Box::new(Expr::Literal(Literal::Int64(high))),
            negated: false,
        }
    }

    #[test]
    fn test_estimate_selectivity_equality() {
        let cost_model = CostModel::new();
        let pred = make_eq_predicate("id", 42);
        let selectivity = estimate_predicate_selectivity(&pred, &cost_model);
        assert!((selectivity - 0.1).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_range() {
        let cost_model = CostModel::new();
        let pred = make_lt_predicate("price", 100);
        let selectivity = estimate_predicate_selectivity(&pred, &cost_model);
        assert!((selectivity - 0.3).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_is_null() {
        let cost_model = CostModel::new();
        let pred = make_is_null_predicate("optional_col");
        let selectivity = estimate_predicate_selectivity(&pred, &cost_model);
        assert!((selectivity - 0.01).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_is_not_null() {
        let cost_model = CostModel::new();
        let pred = make_is_not_null_predicate("optional_col");
        let selectivity = estimate_predicate_selectivity(&pred, &cost_model);
        assert!((selectivity - 0.99).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_in_list() {
        let cost_model = CostModel::new();
        let pred = make_in_list_predicate("status", vec![1, 2, 3]);
        let selectivity = estimate_predicate_selectivity(&pred, &cost_model);
        assert!((selectivity - 0.3).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_in_list_capped() {
        let cost_model = CostModel::new();
        let pred = make_in_list_predicate("status", vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let selectivity = estimate_predicate_selectivity(&pred, &cost_model);
        assert!((selectivity - 0.5).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_like_prefix() {
        let cost_model = CostModel::new();
        let pred = make_like_predicate("name", "John%");
        let selectivity = estimate_predicate_selectivity(&pred, &cost_model);
        assert!((selectivity - 0.1).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_like_no_prefix() {
        let cost_model = CostModel::new();
        let pred = make_like_predicate("name", "%Smith%");
        let selectivity = estimate_predicate_selectivity(&pred, &cost_model);
        assert!((selectivity - 0.5).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_between() {
        let cost_model = CostModel::new();
        let pred = make_between_predicate("age", 18, 30);
        let selectivity = estimate_predicate_selectivity(&pred, &cost_model);
        assert!((selectivity - 0.25).abs() < 0.0001);
    }

    #[test]
    fn test_combine_predicates_ordered_empty() {
        let cost_model = CostModel::new();
        let result = combine_predicates_ordered(vec![], &cost_model);
        assert!(result.is_none());
    }

    #[test]
    fn test_combine_predicates_ordered_single() {
        let cost_model = CostModel::new();
        let pred = make_eq_predicate("id", 42);
        let result = combine_predicates_ordered(vec![pred.clone()], &cost_model);
        assert_eq!(result, Some(pred));
    }

    #[test]
    fn test_combine_predicates_ordered_most_selective_first() {
        let cost_model = CostModel::new();

        let is_null_pred = make_is_null_predicate("optional_col");
        let eq_pred = make_eq_predicate("id", 42);
        let range_pred = make_lt_predicate("price", 100);
        let like_no_prefix = make_like_predicate("name", "%Smith%");

        let predicates = vec![
            like_no_prefix.clone(),
            range_pred.clone(),
            eq_pred.clone(),
            is_null_pred.clone(),
        ];

        let result = combine_predicates_ordered(predicates, &cost_model).unwrap();

        if let Expr::BinaryOp { left, op, right } = &result {
            assert_eq!(*op, BinaryOp::And);

            if let Expr::BinaryOp {
                left: inner_left, ..
            } = left.as_ref()
            {
                if let Expr::BinaryOp {
                    left: innermost, ..
                } = inner_left.as_ref()
                {
                    assert_eq!(innermost.as_ref(), &is_null_pred);
                } else {
                    panic!("Expected innermost to be BinaryOp");
                }
            } else {
                panic!("Expected inner_left to be BinaryOp");
            }

            assert_eq!(right.as_ref(), &like_no_prefix);
        } else {
            panic!("Expected BinaryOp");
        }
    }

    #[test]
    fn test_combine_predicates_ordered_ordering() {
        let cost_model = CostModel::new();

        let is_null = make_is_null_predicate("a");
        let eq = make_eq_predicate("b", 1);
        let between = make_between_predicate("c", 1, 10);
        let lt = make_lt_predicate("d", 100);
        let like_no_prefix = make_like_predicate("e", "%x%");
        let is_not_null = make_is_not_null_predicate("f");

        let predicates = vec![
            is_not_null.clone(),
            like_no_prefix.clone(),
            lt.clone(),
            between.clone(),
            eq.clone(),
            is_null.clone(),
        ];

        let result = combine_predicates_ordered(predicates, &cost_model).unwrap();

        let mut collected = Vec::new();
        fn collect_predicates(expr: &Expr, collected: &mut Vec<Expr>) {
            match expr {
                Expr::BinaryOp {
                    left,
                    op: BinaryOp::And,
                    right,
                } => {
                    collect_predicates(left, collected);
                    collected.push(right.as_ref().clone());
                }
                other => collected.push(other.clone()),
            }
        }
        collect_predicates(&result, &mut collected);

        assert_eq!(collected.len(), 6);
        assert_eq!(collected[0], is_null);
        assert_eq!(collected[1], eq);
        assert_eq!(collected[2], between);
        assert_eq!(collected[3], lt);
        assert_eq!(collected[4], like_no_prefix);
        assert_eq!(collected[5], is_not_null);
    }

    #[test]
    fn test_original_combine_predicates_still_works() {
        let pred1 = make_eq_predicate("a", 1);
        let pred2 = make_eq_predicate("b", 2);
        let pred3 = make_eq_predicate("c", 3);

        let result = combine_predicates(vec![pred1.clone(), pred2.clone(), pred3.clone()]);

        assert!(result.is_some());
        let combined = result.unwrap();

        if let Expr::BinaryOp { left, op, right } = &combined {
            assert_eq!(*op, BinaryOp::And);
            assert_eq!(right.as_ref(), &pred3);

            if let Expr::BinaryOp {
                left: inner_left,
                op: inner_op,
                right: inner_right,
            } = left.as_ref()
            {
                assert_eq!(*inner_op, BinaryOp::And);
                assert_eq!(inner_left.as_ref(), &pred1);
                assert_eq!(inner_right.as_ref(), &pred2);
            } else {
                panic!("Expected inner BinaryOp");
            }
        } else {
            panic!("Expected BinaryOp");
        }
    }

    #[test]
    fn test_estimate_selectivity_and_combines() {
        let cost_model = CostModel::new();
        let eq1 = make_eq_predicate("a", 1);
        let eq2 = make_eq_predicate("b", 2);

        let combined = Expr::BinaryOp {
            left: Box::new(eq1),
            op: BinaryOp::And,
            right: Box::new(eq2),
        };

        let selectivity = estimate_predicate_selectivity(&combined, &cost_model);
        assert!((selectivity - 0.01).abs() < 0.0001);
    }

    #[test]
    fn test_estimate_selectivity_or_combines() {
        let cost_model = CostModel::new();
        let eq1 = make_eq_predicate("a", 1);
        let eq2 = make_eq_predicate("b", 2);

        let combined = Expr::BinaryOp {
            left: Box::new(eq1),
            op: BinaryOp::Or,
            right: Box::new(eq2),
        };

        let selectivity = estimate_predicate_selectivity(&combined, &cost_model);
        let expected = 0.1 + 0.1 - 0.1 * 0.1;
        assert!((selectivity - expected).abs() < 0.0001);
    }
}
