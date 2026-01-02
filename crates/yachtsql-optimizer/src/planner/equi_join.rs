#![coverage(off)]

use yachtsql_ir::{BinaryOp, Expr};

pub fn extract_equi_join_keys(
    condition: &Expr,
    left_schema_len: usize,
) -> Option<(Vec<Expr>, Vec<Expr>)> {
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();

    if !collect_equi_keys(condition, left_schema_len, &mut left_keys, &mut right_keys) {
        return None;
    }

    if left_keys.is_empty() {
        return None;
    }

    Some((left_keys, right_keys))
}

fn collect_equi_keys(
    expr: &Expr,
    left_schema_len: usize,
    left_keys: &mut Vec<Expr>,
    right_keys: &mut Vec<Expr>,
) -> bool {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOp::And => {
                collect_equi_keys(left, left_schema_len, left_keys, right_keys)
                    && collect_equi_keys(right, left_schema_len, left_keys, right_keys)
            }
            BinaryOp::Eq => {
                let left_side = classify_expr_side(left, left_schema_len);
                let right_side = classify_expr_side(right, left_schema_len);

                match (left_side, right_side) {
                    (Some(ExprSide::Left), Some(ExprSide::Right)) => {
                        left_keys.push((**left).clone());
                        right_keys.push(adjust_right_expr(right, left_schema_len));
                        true
                    }
                    (Some(ExprSide::Right), Some(ExprSide::Left)) => {
                        left_keys.push((**right).clone());
                        right_keys.push(adjust_right_expr(left, left_schema_len));
                        true
                    }
                    _ => false,
                }
            }
            _ => false,
        },
        _ => false,
    }
}

#[derive(PartialEq)]
enum ExprSide {
    Left,
    Right,
}

fn classify_expr_side(expr: &Expr, left_schema_len: usize) -> Option<ExprSide> {
    match expr {
        Expr::Column {
            index: Some(idx), ..
        } => {
            if *idx < left_schema_len {
                Some(ExprSide::Left)
            } else {
                Some(ExprSide::Right)
            }
        }
        Expr::Column { index: None, .. } => None,
        _ => None,
    }
}

pub fn adjust_right_expr(expr: &Expr, left_schema_len: usize) -> Expr {
    match expr {
        Expr::Column { table, name, index } => Expr::Column {
            table: table.clone(),
            name: name.clone(),
            index: index.map(|i| i.saturating_sub(left_schema_len)),
        },
        other => other.clone(),
    }
}
