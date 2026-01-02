#![coverage(off)]

use sqlparser::ast;
use yachtsql_ir::{BinaryOp, Expr, Literal};

use super::super::Planner;
use crate::CatalogProvider;

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub(in crate::planner::query::aggregate) fn canonical_agg_name(expr: &ast::Expr) -> String {
        format!("{}", expr).to_uppercase().replace(' ', "")
    }

    pub(in crate::planner::query::aggregate) fn agg_func_to_sql_name(
        func: &yachtsql_ir::AggregateFunction,
    ) -> &'static str {
        use yachtsql_ir::AggregateFunction;
        match func {
            AggregateFunction::Count => "COUNT",
            AggregateFunction::Sum => "SUM",
            AggregateFunction::Avg => "AVG",
            AggregateFunction::Min => "MIN",
            AggregateFunction::Max => "MAX",
            AggregateFunction::ArrayAgg => "ARRAY_AGG",
            AggregateFunction::StringAgg => "STRING_AGG",
            AggregateFunction::XmlAgg => "XMLAGG",
            AggregateFunction::AnyValue => "ANY_VALUE",
            AggregateFunction::CountIf => "COUNTIF",
            AggregateFunction::SumIf => "SUMIF",
            AggregateFunction::AvgIf => "AVGIF",
            AggregateFunction::MinIf => "MINIF",
            AggregateFunction::MaxIf => "MAXIF",
            AggregateFunction::Grouping => "GROUPING",
            AggregateFunction::GroupingId => "GROUPING_ID",
            AggregateFunction::LogicalAnd => "LOGICAL_AND",
            AggregateFunction::LogicalOr => "LOGICAL_OR",
            AggregateFunction::BitAnd => "BIT_AND",
            AggregateFunction::BitOr => "BIT_OR",
            AggregateFunction::BitXor => "BIT_XOR",
            AggregateFunction::Variance => "VARIANCE",
            AggregateFunction::Stddev => "STDDEV",
            AggregateFunction::StddevPop => "STDDEV_POP",
            AggregateFunction::StddevSamp => "STDDEV_SAMP",
            AggregateFunction::VarPop => "VAR_POP",
            AggregateFunction::VarSamp => "VAR_SAMP",
            AggregateFunction::Corr => "CORR",
            AggregateFunction::CovarPop => "COVAR_POP",
            AggregateFunction::CovarSamp => "COVAR_SAMP",
            AggregateFunction::ApproxCountDistinct => "APPROX_COUNT_DISTINCT",
            AggregateFunction::ApproxQuantiles => "APPROX_QUANTILES",
            AggregateFunction::ApproxTopCount => "APPROX_TOP_COUNT",
            AggregateFunction::ApproxTopSum => "APPROX_TOP_SUM",
        }
    }

    pub(in crate::planner::query) fn canonical_planned_agg_name(expr: &Expr) -> String {
        match expr {
            Expr::Aggregate {
                func,
                args,
                distinct,
                ..
            } => {
                let func_name = Self::agg_func_to_sql_name(func);
                let args_str = args
                    .iter()
                    .map(|a| Self::canonical_planned_expr_name(a))
                    .collect::<Vec<_>>()
                    .join(",");
                if *distinct {
                    format!("{}(DISTINCT{})", func_name, args_str)
                } else {
                    format!("{}({})", func_name, args_str)
                }
            }
            _ => format!("{:?}", expr),
        }
    }

    pub(in crate::planner::query::aggregate) fn canonical_planned_expr_name(expr: &Expr) -> String {
        match expr {
            Expr::Column { table, name, .. } => {
                if let Some(t) = table {
                    format!("{}.{}", t.to_uppercase(), name.to_uppercase())
                } else {
                    name.to_uppercase()
                }
            }
            Expr::Aggregate { .. } => Self::canonical_planned_agg_name(expr),
            Expr::BinaryOp { left, op, right } => {
                let op_str = match op {
                    BinaryOp::Add => "+",
                    BinaryOp::Sub => "-",
                    BinaryOp::Mul => "*",
                    BinaryOp::Div => "/",
                    BinaryOp::Mod => "%",
                    BinaryOp::Eq => "=",
                    BinaryOp::NotEq => "!=",
                    BinaryOp::Lt => "<",
                    BinaryOp::LtEq => "<=",
                    BinaryOp::Gt => ">",
                    BinaryOp::GtEq => ">=",
                    BinaryOp::And => "AND",
                    BinaryOp::Or => "OR",
                    BinaryOp::BitwiseAnd => "&",
                    BinaryOp::BitwiseOr => "|",
                    BinaryOp::BitwiseXor => "^",
                    BinaryOp::ShiftLeft => "<<",
                    BinaryOp::ShiftRight => ">>",
                    BinaryOp::Concat => "||",
                };
                format!(
                    "({}{}{})",
                    Self::canonical_planned_expr_name(left),
                    op_str,
                    Self::canonical_planned_expr_name(right)
                )
            }
            Expr::UnaryOp { op, expr: inner } => {
                let op_str = match op {
                    yachtsql_ir::UnaryOp::Not => "NOT ",
                    yachtsql_ir::UnaryOp::Minus => "-",
                    yachtsql_ir::UnaryOp::Plus => "+",
                    yachtsql_ir::UnaryOp::BitwiseNot => "~",
                };
                format!("{}{}", op_str, Self::canonical_planned_expr_name(inner))
            }
            Expr::Literal(lit) => match lit {
                Literal::Null => "NULL".to_string(),
                Literal::Bool(b) => b.to_string().to_uppercase(),
                Literal::Int64(n) => n.to_string(),
                Literal::Float64(f) => format!("{}", f),
                Literal::String(s) => format!("'{}'", s),
                Literal::Bytes(b) => format!("b'{}'", String::from_utf8_lossy(b)),
                Literal::Date(d) => format!("DATE'{}'", d),
                Literal::Datetime(dt) => format!("DATETIME'{}'", dt),
                Literal::Time(t) => format!("TIME'{}'", t),
                Literal::Timestamp(ts) => format!("TIMESTAMP'{}'", ts),
                Literal::Interval { .. } => "INTERVAL".to_string(),
                Literal::Numeric(n) => format!("NUMERIC'{}'", n),
                Literal::BigNumeric(n) => format!("BIGNUMERIC'{}'", n),
                Literal::Json(j) => format!("JSON'{}'", j),
                Literal::Array(_) => "ARRAY".to_string(),
                Literal::Struct(_) => "STRUCT".to_string(),
            },
            Expr::Cast {
                expr: inner,
                data_type,
                safe,
            } => {
                let type_str = format!("{:?}", data_type).to_uppercase();
                if *safe {
                    format!(
                        "SAFE_CAST({} AS {})",
                        Self::canonical_planned_expr_name(inner),
                        type_str
                    )
                } else {
                    format!(
                        "CAST({} AS {})",
                        Self::canonical_planned_expr_name(inner),
                        type_str
                    )
                }
            }
            Expr::ScalarFunction { name, args } => {
                let args_str = args
                    .iter()
                    .map(|a| Self::canonical_planned_expr_name(a))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("{:?}({})", name, args_str).to_uppercase()
            }
            Expr::Alias { expr: inner, .. } => Self::canonical_planned_expr_name(inner),
            _ => format!("{:?}", expr).to_uppercase(),
        }
    }

    pub(in crate::planner::query::aggregate) fn canonical_agg_name_matches(
        name: &str,
        canonical: &str,
    ) -> bool {
        let name_normalized = name.to_uppercase().replace(' ', "");
        name_normalized == canonical
    }
}
