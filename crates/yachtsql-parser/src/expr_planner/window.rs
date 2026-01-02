#![coverage(off)]

use sqlparser::ast;
use yachtsql_common::error::{Error, Result};
use yachtsql_ir::{
    Expr, PlanSchema, SortExpr, WindowFrame, WindowFrameBound, WindowFrameUnit, WindowFunction,
};

use super::ExprPlanner;

pub fn try_window_function(name: &str) -> Option<WindowFunction> {
    match name {
        "ROW_NUMBER" => Some(WindowFunction::RowNumber),
        "RANK" => Some(WindowFunction::Rank),
        "DENSE_RANK" => Some(WindowFunction::DenseRank),
        "PERCENT_RANK" => Some(WindowFunction::PercentRank),
        "CUME_DIST" => Some(WindowFunction::CumeDist),
        "NTILE" => Some(WindowFunction::Ntile),
        "LEAD" => Some(WindowFunction::Lead),
        "LAG" => Some(WindowFunction::Lag),
        "FIRST_VALUE" => Some(WindowFunction::FirstValue),
        "LAST_VALUE" => Some(WindowFunction::LastValue),
        "NTH_VALUE" => Some(WindowFunction::NthValue),
        _ => None,
    }
}

pub fn plan_window_spec(
    over: &ast::WindowType,
    schema: &PlanSchema,
    named_windows: &[ast::NamedWindowDefinition],
) -> Result<(Vec<Expr>, Vec<SortExpr>, Option<WindowFrame>)> {
    match over {
        ast::WindowType::WindowSpec(spec) => plan_window_spec_inner(spec, schema),
        ast::WindowType::NamedWindow(name) => {
            let name_str = name.value.to_uppercase();
            for def in named_windows {
                if def.0.value.to_uppercase() == name_str {
                    return match &def.1 {
                        ast::NamedWindowExpr::WindowSpec(spec) => {
                            plan_window_spec_inner(spec, schema)
                        }
                        ast::NamedWindowExpr::NamedWindow(ref_name) => plan_window_spec(
                            &ast::WindowType::NamedWindow(ref_name.clone()),
                            schema,
                            named_windows,
                        ),
                    };
                }
            }
            Err(Error::invalid_query(format!(
                "Named window '{}' not found",
                name.value
            )))
        }
    }
}

fn plan_window_spec_inner(
    spec: &ast::WindowSpec,
    schema: &PlanSchema,
) -> Result<(Vec<Expr>, Vec<SortExpr>, Option<WindowFrame>)> {
    let partition_by = spec
        .partition_by
        .iter()
        .map(|e| ExprPlanner::plan_expr(e, schema))
        .collect::<Result<Vec<_>>>()?;

    let order_by = spec
        .order_by
        .iter()
        .map(|ob| {
            let expr = ExprPlanner::plan_expr(&ob.expr, schema)?;
            Ok(SortExpr {
                expr,
                asc: ob.options.asc.unwrap_or(true),
                nulls_first: ob.options.nulls_first.unwrap_or(false),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let frame = spec.window_frame.as_ref().map(|f| WindowFrame {
        unit: match f.units {
            ast::WindowFrameUnits::Rows => WindowFrameUnit::Rows,
            ast::WindowFrameUnits::Range => WindowFrameUnit::Range,
            ast::WindowFrameUnits::Groups => WindowFrameUnit::Groups,
        },
        start: plan_window_bound(&f.start_bound),
        end: f.end_bound.as_ref().map(plan_window_bound),
    });

    Ok((partition_by, order_by, frame))
}

pub fn plan_window_bound(bound: &ast::WindowFrameBound) -> WindowFrameBound {
    match bound {
        ast::WindowFrameBound::CurrentRow => WindowFrameBound::CurrentRow,
        ast::WindowFrameBound::Preceding(None) => WindowFrameBound::Preceding(None),
        ast::WindowFrameBound::Preceding(Some(e)) => {
            if let ast::Expr::Value(v) = e.as_ref()
                && let ast::Value::Number(n, _) = &v.value
                && let Ok(num) = n.parse::<u64>()
            {
                return WindowFrameBound::Preceding(Some(num));
            }
            WindowFrameBound::Preceding(None)
        }
        ast::WindowFrameBound::Following(None) => WindowFrameBound::Following(None),
        ast::WindowFrameBound::Following(Some(e)) => {
            if let ast::Expr::Value(v) = e.as_ref()
                && let ast::Value::Number(n, _) = &v.value
                && let Ok(num) = n.parse::<u64>()
            {
                return WindowFrameBound::Following(Some(num));
            }
            WindowFrameBound::Following(None)
        }
    }
}
