#![coverage(off)]

use yachtsql_ir::{BinaryOp, Expr};

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
