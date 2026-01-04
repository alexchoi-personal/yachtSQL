#![coverage(off)]

use std::collections::{HashMap, HashSet};

use yachtsql_ir::{BinaryOp, Expr, JoinType};

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

pub fn collect_column_indices(expr: &Expr) -> HashSet<usize> {
    let mut indices = HashSet::new();
    collect_column_indices_into(expr, &mut indices);
    indices
}

pub fn collect_column_indices_into(expr: &Expr, indices: &mut HashSet<usize>) {
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

pub fn build_aggregate_output_to_input_map(group_by: &[Expr]) -> HashMap<usize, usize> {
    let mut map = HashMap::new();
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
    output_to_input: &HashMap<usize, usize>,
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
