use ordered_float::OrderedFloat;
use yachtsql_ir::{BinaryOp, Expr, Literal, SortExpr, UnaryOp, WhenClause};

pub fn fold_constants(expr: &Expr) -> Expr {
    match expr {
        Expr::Literal(_) => expr.clone(),

        Expr::Column { .. } => expr.clone(),

        Expr::BinaryOp { left, op, right } => fold_binary_op(left, *op, right),

        Expr::UnaryOp { op, expr: inner } => fold_unary_op(*op, inner),

        Expr::ScalarFunction { name, args } => {
            let folded_args: Vec<Expr> = args.iter().map(fold_constants).collect();
            Expr::ScalarFunction {
                name: name.clone(),
                args: folded_args,
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
            args: args.iter().map(fold_constants).collect(),
            distinct: *distinct,
            filter: filter.as_ref().map(|f| Box::new(fold_constants(f))),
            order_by: order_by.iter().map(fold_sort_expr).collect(),
            limit: *limit,
            ignore_nulls: *ignore_nulls,
        },

        Expr::UserDefinedAggregate {
            name,
            args,
            distinct,
            filter,
        } => Expr::UserDefinedAggregate {
            name: name.clone(),
            args: args.iter().map(fold_constants).collect(),
            distinct: *distinct,
            filter: filter.as_ref().map(|f| Box::new(fold_constants(f))),
        },

        Expr::Window {
            func,
            args,
            partition_by,
            order_by,
            frame,
        } => Expr::Window {
            func: *func,
            args: args.iter().map(fold_constants).collect(),
            partition_by: partition_by.iter().map(fold_constants).collect(),
            order_by: order_by.iter().map(fold_sort_expr).collect(),
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
            args: args.iter().map(fold_constants).collect(),
            distinct: *distinct,
            partition_by: partition_by.iter().map(fold_constants).collect(),
            order_by: order_by.iter().map(fold_sort_expr).collect(),
            frame: frame.clone(),
        },

        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => fold_case(operand, when_clauses, else_result),

        Expr::Cast {
            expr: inner,
            data_type,
            safe,
        } => Expr::Cast {
            expr: Box::new(fold_constants(inner)),
            data_type: data_type.clone(),
            safe: *safe,
        },

        Expr::IsNull {
            expr: inner,
            negated,
        } => {
            let folded = fold_constants(inner);
            match &folded {
                Expr::Literal(Literal::Null) => Expr::Literal(Literal::Bool(!negated)),
                Expr::Literal(_) => Expr::Literal(Literal::Bool(*negated)),
                _ => Expr::IsNull {
                    expr: Box::new(folded),
                    negated: *negated,
                },
            }
        }

        Expr::IsDistinctFrom {
            left,
            right,
            negated,
        } => Expr::IsDistinctFrom {
            left: Box::new(fold_constants(left)),
            right: Box::new(fold_constants(right)),
            negated: *negated,
        },

        Expr::InList {
            expr: inner,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(fold_constants(inner)),
            list: list.iter().map(fold_constants).collect(),
            negated: *negated,
        },

        Expr::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => Expr::InSubquery {
            expr: Box::new(fold_constants(inner)),
            subquery: subquery.clone(),
            negated: *negated,
        },

        Expr::InUnnest {
            expr: inner,
            array_expr,
            negated,
        } => Expr::InUnnest {
            expr: Box::new(fold_constants(inner)),
            array_expr: Box::new(fold_constants(array_expr)),
            negated: *negated,
        },

        Expr::Exists { subquery, negated } => Expr::Exists {
            subquery: subquery.clone(),
            negated: *negated,
        },

        Expr::Between {
            expr: inner,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(fold_constants(inner)),
            low: Box::new(fold_constants(low)),
            high: Box::new(fold_constants(high)),
            negated: *negated,
        },

        Expr::Like {
            expr: inner,
            pattern,
            negated,
            case_insensitive,
        } => Expr::Like {
            expr: Box::new(fold_constants(inner)),
            pattern: Box::new(fold_constants(pattern)),
            negated: *negated,
            case_insensitive: *case_insensitive,
        },

        Expr::Extract { field, expr: inner } => Expr::Extract {
            field: *field,
            expr: Box::new(fold_constants(inner)),
        },

        Expr::Substring {
            expr: inner,
            start,
            length,
        } => Expr::Substring {
            expr: Box::new(fold_constants(inner)),
            start: start.as_ref().map(|s| Box::new(fold_constants(s))),
            length: length.as_ref().map(|l| Box::new(fold_constants(l))),
        },

        Expr::Trim {
            expr: inner,
            trim_what,
            trim_where,
        } => Expr::Trim {
            expr: Box::new(fold_constants(inner)),
            trim_what: trim_what.as_ref().map(|t| Box::new(fold_constants(t))),
            trim_where: *trim_where,
        },

        Expr::Position { substr, string } => Expr::Position {
            substr: Box::new(fold_constants(substr)),
            string: Box::new(fold_constants(string)),
        },

        Expr::Overlay {
            expr: inner,
            overlay_what,
            overlay_from,
            overlay_for,
        } => Expr::Overlay {
            expr: Box::new(fold_constants(inner)),
            overlay_what: Box::new(fold_constants(overlay_what)),
            overlay_from: Box::new(fold_constants(overlay_from)),
            overlay_for: overlay_for.as_ref().map(|o| Box::new(fold_constants(o))),
        },

        Expr::Array {
            elements,
            element_type,
        } => Expr::Array {
            elements: elements.iter().map(fold_constants).collect(),
            element_type: element_type.clone(),
        },

        Expr::ArrayAccess { array, index } => Expr::ArrayAccess {
            array: Box::new(fold_constants(array)),
            index: Box::new(fold_constants(index)),
        },

        Expr::Struct { fields } => Expr::Struct {
            fields: fields
                .iter()
                .map(|(name, e)| (name.clone(), fold_constants(e)))
                .collect(),
        },

        Expr::StructAccess { expr: inner, field } => Expr::StructAccess {
            expr: Box::new(fold_constants(inner)),
            field: field.clone(),
        },

        Expr::TypedString { data_type, value } => Expr::TypedString {
            data_type: data_type.clone(),
            value: value.clone(),
        },

        Expr::Interval {
            value,
            leading_field,
        } => Expr::Interval {
            value: Box::new(fold_constants(value)),
            leading_field: *leading_field,
        },

        Expr::Alias { expr: inner, name } => {
            let folded = fold_constants(inner);
            Expr::Alias {
                expr: Box::new(folded),
                name: name.clone(),
            }
        }

        Expr::Wildcard { table } => Expr::Wildcard {
            table: table.clone(),
        },

        Expr::Subquery(subquery) => Expr::Subquery(subquery.clone()),

        Expr::ScalarSubquery(subquery) => Expr::ScalarSubquery(subquery.clone()),

        Expr::ArraySubquery(subquery) => Expr::ArraySubquery(subquery.clone()),

        Expr::Parameter { name } => Expr::Parameter { name: name.clone() },

        Expr::Variable { name } => Expr::Variable { name: name.clone() },

        Expr::Placeholder { id } => Expr::Placeholder { id: id.clone() },

        Expr::Lambda { params, body } => Expr::Lambda {
            params: params.clone(),
            body: Box::new(fold_constants(body)),
        },

        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => Expr::AtTimeZone {
            timestamp: Box::new(fold_constants(timestamp)),
            time_zone: Box::new(fold_constants(time_zone)),
        },

        Expr::JsonAccess { expr: inner, path } => Expr::JsonAccess {
            expr: Box::new(fold_constants(inner)),
            path: path.clone(),
        },

        Expr::Default => Expr::Default,
    }
}

fn fold_sort_expr(sort_expr: &SortExpr) -> SortExpr {
    SortExpr {
        expr: fold_constants(&sort_expr.expr),
        asc: sort_expr.asc,
        nulls_first: sort_expr.nulls_first,
    }
}

fn fold_binary_op(left: &Expr, op: BinaryOp, right: &Expr) -> Expr {
    let left_folded = fold_constants(left);
    let right_folded = fold_constants(right);

    if let (Expr::Literal(l), Expr::Literal(r)) = (&left_folded, &right_folded)
        && let Some(result) = evaluate_binary_op(l, op, r)
    {
        return Expr::Literal(result);
    }

    match op {
        BinaryOp::And => fold_and(&left_folded, &right_folded),
        BinaryOp::Or => fold_or(&left_folded, &right_folded),
        _ => Expr::BinaryOp {
            left: Box::new(left_folded),
            op,
            right: Box::new(right_folded),
        },
    }
}

fn fold_and(left: &Expr, right: &Expr) -> Expr {
    match (left, right) {
        (Expr::Literal(Literal::Bool(true)), r) => r.clone(),
        (l, Expr::Literal(Literal::Bool(true))) => l.clone(),
        (Expr::Literal(Literal::Bool(false)), _) => Expr::Literal(Literal::Bool(false)),
        (_, Expr::Literal(Literal::Bool(false))) => Expr::Literal(Literal::Bool(false)),
        (Expr::Literal(Literal::Null), _) | (_, Expr::Literal(Literal::Null)) => Expr::BinaryOp {
            left: Box::new(left.clone()),
            op: BinaryOp::And,
            right: Box::new(right.clone()),
        },
        _ => Expr::BinaryOp {
            left: Box::new(left.clone()),
            op: BinaryOp::And,
            right: Box::new(right.clone()),
        },
    }
}

fn fold_or(left: &Expr, right: &Expr) -> Expr {
    match (left, right) {
        (Expr::Literal(Literal::Bool(false)), r) => r.clone(),
        (l, Expr::Literal(Literal::Bool(false))) => l.clone(),
        (Expr::Literal(Literal::Bool(true)), _) => Expr::Literal(Literal::Bool(true)),
        (_, Expr::Literal(Literal::Bool(true))) => Expr::Literal(Literal::Bool(true)),
        (Expr::Literal(Literal::Null), _) | (_, Expr::Literal(Literal::Null)) => Expr::BinaryOp {
            left: Box::new(left.clone()),
            op: BinaryOp::Or,
            right: Box::new(right.clone()),
        },
        _ => Expr::BinaryOp {
            left: Box::new(left.clone()),
            op: BinaryOp::Or,
            right: Box::new(right.clone()),
        },
    }
}

fn fold_unary_op(op: UnaryOp, inner: &Expr) -> Expr {
    let folded = fold_constants(inner);

    match op {
        UnaryOp::Not => match &folded {
            Expr::Literal(Literal::Bool(b)) => Expr::Literal(Literal::Bool(!b)),
            Expr::Literal(Literal::Null) => Expr::Literal(Literal::Null),
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: double_negated,
            } => (**double_negated).clone(),
            _ => Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(folded),
            },
        },
        UnaryOp::Minus => match &folded {
            Expr::Literal(Literal::Int64(n)) => Expr::Literal(Literal::Int64(-n)),
            Expr::Literal(Literal::Float64(f)) => {
                Expr::Literal(Literal::Float64(OrderedFloat(-f.0)))
            }
            Expr::Literal(Literal::Numeric(d)) => Expr::Literal(Literal::Numeric(-*d)),
            Expr::Literal(Literal::BigNumeric(d)) => Expr::Literal(Literal::BigNumeric(-*d)),
            Expr::Literal(Literal::Null) => Expr::Literal(Literal::Null),
            _ => Expr::UnaryOp {
                op: UnaryOp::Minus,
                expr: Box::new(folded),
            },
        },
        UnaryOp::Plus => match &folded {
            Expr::Literal(Literal::Int64(_))
            | Expr::Literal(Literal::Float64(_))
            | Expr::Literal(Literal::Numeric(_))
            | Expr::Literal(Literal::BigNumeric(_))
            | Expr::Literal(Literal::Null) => folded,
            _ => Expr::UnaryOp {
                op: UnaryOp::Plus,
                expr: Box::new(folded),
            },
        },
        UnaryOp::BitwiseNot => match &folded {
            Expr::Literal(Literal::Int64(n)) => Expr::Literal(Literal::Int64(!n)),
            Expr::Literal(Literal::Null) => Expr::Literal(Literal::Null),
            _ => Expr::UnaryOp {
                op: UnaryOp::BitwiseNot,
                expr: Box::new(folded),
            },
        },
    }
}

fn fold_case(
    operand: &Option<Box<Expr>>,
    when_clauses: &[WhenClause],
    else_result: &Option<Box<Expr>>,
) -> Expr {
    let folded_operand = operand.as_ref().map(|o| Box::new(fold_constants(o)));

    let mut folded_when_clauses = Vec::new();
    for clause in when_clauses {
        let folded_condition = fold_constants(&clause.condition);
        let folded_result = fold_constants(&clause.result);

        match &folded_condition {
            Expr::Literal(Literal::Bool(true)) if folded_operand.is_none() => {
                return folded_result;
            }
            Expr::Literal(Literal::Bool(false)) if folded_operand.is_none() => {
                continue;
            }
            _ => {
                folded_when_clauses.push(WhenClause {
                    condition: folded_condition,
                    result: folded_result,
                });
            }
        }
    }

    let folded_else = else_result.as_ref().map(|e| Box::new(fold_constants(e)));

    if folded_when_clauses.is_empty() {
        return folded_else
            .map(|e| *e)
            .unwrap_or(Expr::Literal(Literal::Null));
    }

    Expr::Case {
        operand: folded_operand,
        when_clauses: folded_when_clauses,
        else_result: folded_else,
    }
}

fn evaluate_binary_op(left: &Literal, op: BinaryOp, right: &Literal) -> Option<Literal> {
    if matches!(left, Literal::Null) || matches!(right, Literal::Null) {
        return match op {
            BinaryOp::And => {
                if matches!(left, Literal::Bool(false)) || matches!(right, Literal::Bool(false)) {
                    Some(Literal::Bool(false))
                } else {
                    None
                }
            }
            BinaryOp::Or => {
                if matches!(left, Literal::Bool(true)) || matches!(right, Literal::Bool(true)) {
                    Some(Literal::Bool(true))
                } else {
                    None
                }
            }
            _ => None,
        };
    }

    match (left, op, right) {
        (Literal::Int64(l), BinaryOp::Add, Literal::Int64(r)) => {
            l.checked_add(*r).map(Literal::Int64)
        }
        (Literal::Int64(l), BinaryOp::Sub, Literal::Int64(r)) => {
            l.checked_sub(*r).map(Literal::Int64)
        }
        (Literal::Int64(l), BinaryOp::Mul, Literal::Int64(r)) => {
            l.checked_mul(*r).map(Literal::Int64)
        }
        (Literal::Int64(l), BinaryOp::Div, Literal::Int64(r)) => {
            if *r != 0 {
                l.checked_div(*r).map(Literal::Int64)
            } else {
                None
            }
        }
        (Literal::Int64(l), BinaryOp::Mod, Literal::Int64(r)) => {
            if *r != 0 {
                l.checked_rem(*r).map(Literal::Int64)
            } else {
                None
            }
        }

        (Literal::Float64(l), BinaryOp::Add, Literal::Float64(r)) => {
            Some(Literal::Float64(OrderedFloat(l.0 + r.0)))
        }
        (Literal::Float64(l), BinaryOp::Sub, Literal::Float64(r)) => {
            Some(Literal::Float64(OrderedFloat(l.0 - r.0)))
        }
        (Literal::Float64(l), BinaryOp::Mul, Literal::Float64(r)) => {
            Some(Literal::Float64(OrderedFloat(l.0 * r.0)))
        }
        (Literal::Float64(l), BinaryOp::Div, Literal::Float64(r)) => {
            if r.0 != 0.0 {
                Some(Literal::Float64(OrderedFloat(l.0 / r.0)))
            } else {
                None
            }
        }
        (Literal::Float64(l), BinaryOp::Mod, Literal::Float64(r)) => {
            if r.0 != 0.0 {
                Some(Literal::Float64(OrderedFloat(l.0 % r.0)))
            } else {
                None
            }
        }

        (Literal::Numeric(l), BinaryOp::Add, Literal::Numeric(r)) => {
            l.checked_add(*r).map(Literal::Numeric)
        }
        (Literal::Numeric(l), BinaryOp::Sub, Literal::Numeric(r)) => {
            l.checked_sub(*r).map(Literal::Numeric)
        }
        (Literal::Numeric(l), BinaryOp::Mul, Literal::Numeric(r)) => {
            l.checked_mul(*r).map(Literal::Numeric)
        }
        (Literal::Numeric(l), BinaryOp::Div, Literal::Numeric(r)) => {
            if !r.is_zero() {
                l.checked_div(*r).map(Literal::Numeric)
            } else {
                None
            }
        }
        (Literal::Numeric(l), BinaryOp::Mod, Literal::Numeric(r)) => {
            if !r.is_zero() {
                l.checked_rem(*r).map(Literal::Numeric)
            } else {
                None
            }
        }

        (Literal::BigNumeric(l), BinaryOp::Add, Literal::BigNumeric(r)) => {
            l.checked_add(*r).map(Literal::BigNumeric)
        }
        (Literal::BigNumeric(l), BinaryOp::Sub, Literal::BigNumeric(r)) => {
            l.checked_sub(*r).map(Literal::BigNumeric)
        }
        (Literal::BigNumeric(l), BinaryOp::Mul, Literal::BigNumeric(r)) => {
            l.checked_mul(*r).map(Literal::BigNumeric)
        }
        (Literal::BigNumeric(l), BinaryOp::Div, Literal::BigNumeric(r)) => {
            if !r.is_zero() {
                l.checked_div(*r).map(Literal::BigNumeric)
            } else {
                None
            }
        }
        (Literal::BigNumeric(l), BinaryOp::Mod, Literal::BigNumeric(r)) => {
            if !r.is_zero() {
                l.checked_rem(*r).map(Literal::BigNumeric)
            } else {
                None
            }
        }

        (Literal::String(l), BinaryOp::Concat, Literal::String(r)) => {
            Some(Literal::String(format!("{}{}", l, r)))
        }

        (Literal::Int64(l), BinaryOp::Eq, Literal::Int64(r)) => Some(Literal::Bool(l == r)),
        (Literal::Int64(l), BinaryOp::NotEq, Literal::Int64(r)) => Some(Literal::Bool(l != r)),
        (Literal::Int64(l), BinaryOp::Lt, Literal::Int64(r)) => Some(Literal::Bool(l < r)),
        (Literal::Int64(l), BinaryOp::LtEq, Literal::Int64(r)) => Some(Literal::Bool(l <= r)),
        (Literal::Int64(l), BinaryOp::Gt, Literal::Int64(r)) => Some(Literal::Bool(l > r)),
        (Literal::Int64(l), BinaryOp::GtEq, Literal::Int64(r)) => Some(Literal::Bool(l >= r)),

        (Literal::Float64(l), BinaryOp::Eq, Literal::Float64(r)) => Some(Literal::Bool(l == r)),
        (Literal::Float64(l), BinaryOp::NotEq, Literal::Float64(r)) => Some(Literal::Bool(l != r)),
        (Literal::Float64(l), BinaryOp::Lt, Literal::Float64(r)) => Some(Literal::Bool(l < r)),
        (Literal::Float64(l), BinaryOp::LtEq, Literal::Float64(r)) => Some(Literal::Bool(l <= r)),
        (Literal::Float64(l), BinaryOp::Gt, Literal::Float64(r)) => Some(Literal::Bool(l > r)),
        (Literal::Float64(l), BinaryOp::GtEq, Literal::Float64(r)) => Some(Literal::Bool(l >= r)),

        (Literal::Numeric(l), BinaryOp::Eq, Literal::Numeric(r)) => Some(Literal::Bool(l == r)),
        (Literal::Numeric(l), BinaryOp::NotEq, Literal::Numeric(r)) => Some(Literal::Bool(l != r)),
        (Literal::Numeric(l), BinaryOp::Lt, Literal::Numeric(r)) => Some(Literal::Bool(l < r)),
        (Literal::Numeric(l), BinaryOp::LtEq, Literal::Numeric(r)) => Some(Literal::Bool(l <= r)),
        (Literal::Numeric(l), BinaryOp::Gt, Literal::Numeric(r)) => Some(Literal::Bool(l > r)),
        (Literal::Numeric(l), BinaryOp::GtEq, Literal::Numeric(r)) => Some(Literal::Bool(l >= r)),

        (Literal::BigNumeric(l), BinaryOp::Eq, Literal::BigNumeric(r)) => {
            Some(Literal::Bool(l == r))
        }
        (Literal::BigNumeric(l), BinaryOp::NotEq, Literal::BigNumeric(r)) => {
            Some(Literal::Bool(l != r))
        }
        (Literal::BigNumeric(l), BinaryOp::Lt, Literal::BigNumeric(r)) => {
            Some(Literal::Bool(l < r))
        }
        (Literal::BigNumeric(l), BinaryOp::LtEq, Literal::BigNumeric(r)) => {
            Some(Literal::Bool(l <= r))
        }
        (Literal::BigNumeric(l), BinaryOp::Gt, Literal::BigNumeric(r)) => {
            Some(Literal::Bool(l > r))
        }
        (Literal::BigNumeric(l), BinaryOp::GtEq, Literal::BigNumeric(r)) => {
            Some(Literal::Bool(l >= r))
        }

        (Literal::String(l), BinaryOp::Eq, Literal::String(r)) => Some(Literal::Bool(l == r)),
        (Literal::String(l), BinaryOp::NotEq, Literal::String(r)) => Some(Literal::Bool(l != r)),
        (Literal::String(l), BinaryOp::Lt, Literal::String(r)) => Some(Literal::Bool(l < r)),
        (Literal::String(l), BinaryOp::LtEq, Literal::String(r)) => Some(Literal::Bool(l <= r)),
        (Literal::String(l), BinaryOp::Gt, Literal::String(r)) => Some(Literal::Bool(l > r)),
        (Literal::String(l), BinaryOp::GtEq, Literal::String(r)) => Some(Literal::Bool(l >= r)),

        (Literal::Bool(l), BinaryOp::Eq, Literal::Bool(r)) => Some(Literal::Bool(l == r)),
        (Literal::Bool(l), BinaryOp::NotEq, Literal::Bool(r)) => Some(Literal::Bool(l != r)),
        (Literal::Bool(l), BinaryOp::And, Literal::Bool(r)) => Some(Literal::Bool(*l && *r)),
        (Literal::Bool(l), BinaryOp::Or, Literal::Bool(r)) => Some(Literal::Bool(*l || *r)),

        (Literal::Int64(l), BinaryOp::BitwiseAnd, Literal::Int64(r)) => Some(Literal::Int64(l & r)),
        (Literal::Int64(l), BinaryOp::BitwiseOr, Literal::Int64(r)) => Some(Literal::Int64(l | r)),
        (Literal::Int64(l), BinaryOp::BitwiseXor, Literal::Int64(r)) => Some(Literal::Int64(l ^ r)),
        (Literal::Int64(l), BinaryOp::ShiftLeft, Literal::Int64(r)) => {
            if *r >= 0 && *r < 64 {
                Some(Literal::Int64(l << r))
            } else {
                None
            }
        }
        (Literal::Int64(l), BinaryOp::ShiftRight, Literal::Int64(r)) => {
            if *r >= 0 && *r < 64 {
                Some(Literal::Int64(l >> r))
            } else {
                None
            }
        }

        (Literal::Date(l), BinaryOp::Eq, Literal::Date(r)) => Some(Literal::Bool(l == r)),
        (Literal::Date(l), BinaryOp::NotEq, Literal::Date(r)) => Some(Literal::Bool(l != r)),
        (Literal::Date(l), BinaryOp::Lt, Literal::Date(r)) => Some(Literal::Bool(l < r)),
        (Literal::Date(l), BinaryOp::LtEq, Literal::Date(r)) => Some(Literal::Bool(l <= r)),
        (Literal::Date(l), BinaryOp::Gt, Literal::Date(r)) => Some(Literal::Bool(l > r)),
        (Literal::Date(l), BinaryOp::GtEq, Literal::Date(r)) => Some(Literal::Bool(l >= r)),

        (Literal::Timestamp(l), BinaryOp::Eq, Literal::Timestamp(r)) => Some(Literal::Bool(l == r)),
        (Literal::Timestamp(l), BinaryOp::NotEq, Literal::Timestamp(r)) => {
            Some(Literal::Bool(l != r))
        }
        (Literal::Timestamp(l), BinaryOp::Lt, Literal::Timestamp(r)) => Some(Literal::Bool(l < r)),
        (Literal::Timestamp(l), BinaryOp::LtEq, Literal::Timestamp(r)) => {
            Some(Literal::Bool(l <= r))
        }
        (Literal::Timestamp(l), BinaryOp::Gt, Literal::Timestamp(r)) => Some(Literal::Bool(l > r)),
        (Literal::Timestamp(l), BinaryOp::GtEq, Literal::Timestamp(r)) => {
            Some(Literal::Bool(l >= r))
        }

        (Literal::Datetime(l), BinaryOp::Eq, Literal::Datetime(r)) => Some(Literal::Bool(l == r)),
        (Literal::Datetime(l), BinaryOp::NotEq, Literal::Datetime(r)) => {
            Some(Literal::Bool(l != r))
        }
        (Literal::Datetime(l), BinaryOp::Lt, Literal::Datetime(r)) => Some(Literal::Bool(l < r)),
        (Literal::Datetime(l), BinaryOp::LtEq, Literal::Datetime(r)) => Some(Literal::Bool(l <= r)),
        (Literal::Datetime(l), BinaryOp::Gt, Literal::Datetime(r)) => Some(Literal::Bool(l > r)),
        (Literal::Datetime(l), BinaryOp::GtEq, Literal::Datetime(r)) => Some(Literal::Bool(l >= r)),

        (Literal::Time(l), BinaryOp::Eq, Literal::Time(r)) => Some(Literal::Bool(l == r)),
        (Literal::Time(l), BinaryOp::NotEq, Literal::Time(r)) => Some(Literal::Bool(l != r)),
        (Literal::Time(l), BinaryOp::Lt, Literal::Time(r)) => Some(Literal::Bool(l < r)),
        (Literal::Time(l), BinaryOp::LtEq, Literal::Time(r)) => Some(Literal::Bool(l <= r)),
        (Literal::Time(l), BinaryOp::Gt, Literal::Time(r)) => Some(Literal::Bool(l > r)),
        (Literal::Time(l), BinaryOp::GtEq, Literal::Time(r)) => Some(Literal::Bool(l >= r)),

        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use ordered_float::OrderedFloat;
    use rust_decimal::Decimal;

    use super::*;

    fn col(name: &str) -> Expr {
        Expr::Column {
            table: None,
            name: name.to_string(),
            index: None,
        }
    }

    fn lit_i64(v: i64) -> Expr {
        Expr::Literal(Literal::Int64(v))
    }

    fn lit_f64(v: f64) -> Expr {
        Expr::Literal(Literal::Float64(OrderedFloat(v)))
    }

    fn lit_bool(v: bool) -> Expr {
        Expr::Literal(Literal::Bool(v))
    }

    fn lit_string(v: &str) -> Expr {
        Expr::Literal(Literal::String(v.to_string()))
    }

    fn lit_null() -> Expr {
        Expr::Literal(Literal::Null)
    }

    fn binary(left: Expr, op: BinaryOp, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    fn unary(op: UnaryOp, expr: Expr) -> Expr {
        Expr::UnaryOp {
            op,
            expr: Box::new(expr),
        }
    }

    #[test]
    fn fold_integer_addition() {
        let expr = binary(lit_i64(1), BinaryOp::Add, lit_i64(1));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_i64(2));
    }

    #[test]
    fn fold_integer_subtraction() {
        let expr = binary(lit_i64(10), BinaryOp::Sub, lit_i64(3));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_i64(7));
    }

    #[test]
    fn fold_integer_multiplication() {
        let expr = binary(lit_i64(4), BinaryOp::Mul, lit_i64(5));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_i64(20));
    }

    #[test]
    fn fold_integer_division() {
        let expr = binary(lit_i64(20), BinaryOp::Div, lit_i64(4));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_i64(5));
    }

    #[test]
    fn fold_integer_division_by_zero_not_folded() {
        let expr = binary(lit_i64(10), BinaryOp::Div, lit_i64(0));
        let result = fold_constants(&expr);
        assert_eq!(result, binary(lit_i64(10), BinaryOp::Div, lit_i64(0)));
    }

    #[test]
    fn fold_integer_modulo() {
        let expr = binary(lit_i64(17), BinaryOp::Mod, lit_i64(5));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_i64(2));
    }

    #[test]
    fn fold_float_addition() {
        let expr = binary(lit_f64(1.5), BinaryOp::Add, lit_f64(2.5));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_f64(4.0));
    }

    #[test]
    fn fold_float_subtraction() {
        let expr = binary(lit_f64(5.0), BinaryOp::Sub, lit_f64(1.5));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_f64(3.5));
    }

    #[test]
    fn fold_float_multiplication() {
        let expr = binary(lit_f64(2.0), BinaryOp::Mul, lit_f64(3.5));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_f64(7.0));
    }

    #[test]
    fn fold_float_division() {
        let expr = binary(lit_f64(10.0), BinaryOp::Div, lit_f64(2.0));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_f64(5.0));
    }

    #[test]
    fn fold_true_and_x_simplifies_to_x() {
        let expr = binary(lit_bool(true), BinaryOp::And, col("x"));
        let result = fold_constants(&expr);
        assert_eq!(result, col("x"));
    }

    #[test]
    fn fold_x_and_true_simplifies_to_x() {
        let expr = binary(col("x"), BinaryOp::And, lit_bool(true));
        let result = fold_constants(&expr);
        assert_eq!(result, col("x"));
    }

    #[test]
    fn fold_false_and_x_simplifies_to_false() {
        let expr = binary(lit_bool(false), BinaryOp::And, col("x"));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(false));
    }

    #[test]
    fn fold_x_and_false_simplifies_to_false() {
        let expr = binary(col("x"), BinaryOp::And, lit_bool(false));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(false));
    }

    #[test]
    fn fold_true_or_x_simplifies_to_true() {
        let expr = binary(lit_bool(true), BinaryOp::Or, col("x"));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(true));
    }

    #[test]
    fn fold_x_or_true_simplifies_to_true() {
        let expr = binary(col("x"), BinaryOp::Or, lit_bool(true));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(true));
    }

    #[test]
    fn fold_false_or_x_simplifies_to_x() {
        let expr = binary(lit_bool(false), BinaryOp::Or, col("x"));
        let result = fold_constants(&expr);
        assert_eq!(result, col("x"));
    }

    #[test]
    fn fold_x_or_false_simplifies_to_x() {
        let expr = binary(col("x"), BinaryOp::Or, lit_bool(false));
        let result = fold_constants(&expr);
        assert_eq!(result, col("x"));
    }

    #[test]
    fn fold_not_true_simplifies_to_false() {
        let expr = unary(UnaryOp::Not, lit_bool(true));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(false));
    }

    #[test]
    fn fold_not_false_simplifies_to_true() {
        let expr = unary(UnaryOp::Not, lit_bool(false));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(true));
    }

    #[test]
    fn fold_not_not_x_simplifies_to_x() {
        let expr = unary(UnaryOp::Not, unary(UnaryOp::Not, col("x")));
        let result = fold_constants(&expr);
        assert_eq!(result, col("x"));
    }

    #[test]
    fn fold_not_null_returns_null() {
        let expr = unary(UnaryOp::Not, lit_null());
        let result = fold_constants(&expr);
        assert_eq!(result, lit_null());
    }

    #[test]
    fn col_equals_col_not_simplified_due_to_null_semantics() {
        let expr = binary(col("x"), BinaryOp::Eq, col("x"));
        let result = fold_constants(&expr);
        assert_eq!(result, binary(col("x"), BinaryOp::Eq, col("x")));
    }

    #[test]
    fn null_equals_null_not_simplified() {
        let expr = binary(lit_null(), BinaryOp::Eq, lit_null());
        let result = fold_constants(&expr);
        assert_eq!(result, binary(lit_null(), BinaryOp::Eq, lit_null()));
    }

    #[test]
    fn fold_nested_expressions() {
        let expr = binary(
            binary(lit_i64(1), BinaryOp::Add, lit_i64(2)),
            BinaryOp::Mul,
            binary(lit_i64(3), BinaryOp::Add, lit_i64(4)),
        );
        let result = fold_constants(&expr);
        assert_eq!(result, lit_i64(21));
    }

    #[test]
    fn fold_deeply_nested_and_or() {
        let expr = binary(
            binary(lit_bool(true), BinaryOp::And, col("a")),
            BinaryOp::Or,
            binary(lit_bool(false), BinaryOp::And, col("b")),
        );
        let result = fold_constants(&expr);
        assert_eq!(result, col("a"));
    }

    #[test]
    fn fold_literal_integer_comparison_eq() {
        let expr = binary(lit_i64(5), BinaryOp::Eq, lit_i64(5));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(true));
    }

    #[test]
    fn fold_literal_integer_comparison_lt() {
        let expr = binary(lit_i64(3), BinaryOp::Lt, lit_i64(5));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(true));
    }

    #[test]
    fn fold_literal_integer_comparison_gt() {
        let expr = binary(lit_i64(5), BinaryOp::Gt, lit_i64(3));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(true));
    }

    #[test]
    fn fold_literal_integer_comparison_lteq() {
        let expr = binary(lit_i64(5), BinaryOp::LtEq, lit_i64(5));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(true));
    }

    #[test]
    fn fold_literal_integer_comparison_gteq() {
        let expr = binary(lit_i64(5), BinaryOp::GtEq, lit_i64(5));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(true));
    }

    #[test]
    fn fold_literal_integer_comparison_noteq() {
        let expr = binary(lit_i64(5), BinaryOp::NotEq, lit_i64(3));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(true));
    }

    #[test]
    fn fold_string_concatenation() {
        let expr = binary(lit_string("hello"), BinaryOp::Concat, lit_string(" world"));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_string("hello world"));
    }

    #[test]
    fn fold_string_comparison_eq() {
        let expr = binary(lit_string("abc"), BinaryOp::Eq, lit_string("abc"));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(true));
    }

    #[test]
    fn fold_string_comparison_lt() {
        let expr = binary(lit_string("abc"), BinaryOp::Lt, lit_string("abd"));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(true));
    }

    #[test]
    fn fold_bool_comparison() {
        let expr = binary(lit_bool(true), BinaryOp::Eq, lit_bool(true));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(true));
    }

    #[test]
    fn fold_unary_minus_integer() {
        let expr = unary(UnaryOp::Minus, lit_i64(5));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_i64(-5));
    }

    #[test]
    fn fold_unary_minus_float() {
        let expr = unary(UnaryOp::Minus, lit_f64(2.5));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_f64(-2.5));
    }

    #[test]
    fn fold_unary_plus_preserves_value() {
        let expr = unary(UnaryOp::Plus, lit_i64(5));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_i64(5));
    }

    #[test]
    fn fold_bitwise_not() {
        let expr = unary(UnaryOp::BitwiseNot, lit_i64(0));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_i64(-1));
    }

    #[test]
    fn fold_bitwise_and() {
        let expr = binary(lit_i64(0b1100), BinaryOp::BitwiseAnd, lit_i64(0b1010));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_i64(0b1000));
    }

    #[test]
    fn fold_bitwise_or() {
        let expr = binary(lit_i64(0b1100), BinaryOp::BitwiseOr, lit_i64(0b1010));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_i64(0b1110));
    }

    #[test]
    fn fold_bitwise_xor() {
        let expr = binary(lit_i64(0b1100), BinaryOp::BitwiseXor, lit_i64(0b1010));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_i64(0b0110));
    }

    #[test]
    fn fold_shift_left() {
        let expr = binary(lit_i64(1), BinaryOp::ShiftLeft, lit_i64(4));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_i64(16));
    }

    #[test]
    fn fold_shift_right() {
        let expr = binary(lit_i64(16), BinaryOp::ShiftRight, lit_i64(2));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_i64(4));
    }

    #[test]
    fn fold_is_null_on_null_literal() {
        let expr = Expr::IsNull {
            expr: Box::new(lit_null()),
            negated: false,
        };
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(true));
    }

    #[test]
    fn fold_is_not_null_on_null_literal() {
        let expr = Expr::IsNull {
            expr: Box::new(lit_null()),
            negated: true,
        };
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(false));
    }

    #[test]
    fn fold_is_null_on_non_null_literal() {
        let expr = Expr::IsNull {
            expr: Box::new(lit_i64(5)),
            negated: false,
        };
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(false));
    }

    #[test]
    fn fold_is_not_null_on_non_null_literal() {
        let expr = Expr::IsNull {
            expr: Box::new(lit_i64(5)),
            negated: true,
        };
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(true));
    }

    #[test]
    fn fold_case_when_true_returns_result() {
        let expr = Expr::Case {
            operand: None,
            when_clauses: vec![WhenClause {
                condition: lit_bool(true),
                result: lit_i64(42),
            }],
            else_result: Some(Box::new(lit_i64(0))),
        };
        let result = fold_constants(&expr);
        assert_eq!(result, lit_i64(42));
    }

    #[test]
    fn fold_case_when_false_skipped() {
        let expr = Expr::Case {
            operand: None,
            when_clauses: vec![
                WhenClause {
                    condition: lit_bool(false),
                    result: lit_i64(1),
                },
                WhenClause {
                    condition: lit_bool(true),
                    result: lit_i64(2),
                },
            ],
            else_result: Some(Box::new(lit_i64(0))),
        };
        let result = fold_constants(&expr);
        assert_eq!(result, lit_i64(2));
    }

    #[test]
    fn fold_case_all_false_returns_else() {
        let expr = Expr::Case {
            operand: None,
            when_clauses: vec![WhenClause {
                condition: lit_bool(false),
                result: lit_i64(1),
            }],
            else_result: Some(Box::new(lit_i64(99))),
        };
        let result = fold_constants(&expr);
        assert_eq!(result, lit_i64(99));
    }

    #[test]
    fn fold_case_no_else_returns_null() {
        let expr = Expr::Case {
            operand: None,
            when_clauses: vec![WhenClause {
                condition: lit_bool(false),
                result: lit_i64(1),
            }],
            else_result: None,
        };
        let result = fold_constants(&expr);
        assert_eq!(result, lit_null());
    }

    #[test]
    fn fold_alias_folds_inner_expression() {
        let expr = Expr::Alias {
            expr: Box::new(binary(lit_i64(2), BinaryOp::Add, lit_i64(3))),
            name: "sum".to_string(),
        };
        let result = fold_constants(&expr);
        assert_eq!(
            result,
            Expr::Alias {
                expr: Box::new(lit_i64(5)),
                name: "sum".to_string()
            }
        );
    }

    #[test]
    fn fold_mixed_expression_preserves_columns() {
        let expr = binary(
            binary(lit_i64(1), BinaryOp::Add, lit_i64(2)),
            BinaryOp::Mul,
            col("x"),
        );
        let result = fold_constants(&expr);
        assert_eq!(result, binary(lit_i64(3), BinaryOp::Mul, col("x")));
    }

    #[test]
    fn fold_null_and_false_is_false() {
        let expr = binary(lit_null(), BinaryOp::And, lit_bool(false));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(false));
    }

    #[test]
    fn fold_false_and_null_is_false() {
        let expr = binary(lit_bool(false), BinaryOp::And, lit_null());
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(false));
    }

    #[test]
    fn fold_null_or_true_is_true() {
        let expr = binary(lit_null(), BinaryOp::Or, lit_bool(true));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(true));
    }

    #[test]
    fn fold_true_or_null_is_true() {
        let expr = binary(lit_bool(true), BinaryOp::Or, lit_null());
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(true));
    }

    #[test]
    fn fold_null_and_true_is_null() {
        let expr = binary(lit_null(), BinaryOp::And, lit_bool(true));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_null());
    }

    #[test]
    fn fold_null_or_false_is_null() {
        let expr = binary(lit_null(), BinaryOp::Or, lit_bool(false));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_null());
    }

    #[test]
    fn fold_numeric_addition() {
        let expr = binary(
            Expr::Literal(Literal::Numeric(Decimal::new(15, 1))),
            BinaryOp::Add,
            Expr::Literal(Literal::Numeric(Decimal::new(25, 1))),
        );
        let result = fold_constants(&expr);
        assert_eq!(result, Expr::Literal(Literal::Numeric(Decimal::new(40, 1))));
    }

    #[test]
    fn fold_numeric_comparison() {
        let expr = binary(
            Expr::Literal(Literal::Numeric(Decimal::new(100, 2))),
            BinaryOp::Eq,
            Expr::Literal(Literal::Numeric(Decimal::new(100, 2))),
        );
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(true));
    }

    #[test]
    fn fold_column_not_simplified() {
        let expr = col("x");
        let result = fold_constants(&expr);
        assert_eq!(result, col("x"));
    }

    #[test]
    fn fold_literal_not_simplified() {
        let expr = lit_i64(42);
        let result = fold_constants(&expr);
        assert_eq!(result, lit_i64(42));
    }

    #[test]
    fn fold_where_1_equals_1() {
        let expr = binary(lit_i64(1), BinaryOp::Eq, lit_i64(1));
        let result = fold_constants(&expr);
        assert_eq!(result, lit_bool(true));
    }

    #[test]
    fn fold_deeply_nested_arithmetic() {
        let expr = binary(
            binary(
                binary(lit_i64(1), BinaryOp::Add, lit_i64(2)),
                BinaryOp::Add,
                binary(lit_i64(3), BinaryOp::Add, lit_i64(4)),
            ),
            BinaryOp::Add,
            lit_i64(5),
        );
        let result = fold_constants(&expr);
        assert_eq!(result, lit_i64(15));
    }
}
