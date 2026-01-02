#![coverage(off)]

use yachtsql_ir::AggregateFunction;

pub fn try_aggregate_function(name: &str) -> Option<AggregateFunction> {
    match name {
        "COUNT" => Some(AggregateFunction::Count),
        "SUM" => Some(AggregateFunction::Sum),
        "AVG" => Some(AggregateFunction::Avg),
        "MIN" => Some(AggregateFunction::Min),
        "MAX" => Some(AggregateFunction::Max),
        "ARRAY_AGG" => Some(AggregateFunction::ArrayAgg),
        "STRING_AGG" | "LISTAGG" => Some(AggregateFunction::StringAgg),
        "XMLAGG" => Some(AggregateFunction::XmlAgg),
        "ANY_VALUE" => Some(AggregateFunction::AnyValue),
        "COUNTIF" | "COUNT_IF" => Some(AggregateFunction::CountIf),
        "SUMIF" | "SUM_IF" => Some(AggregateFunction::SumIf),
        "AVGIF" | "AVG_IF" => Some(AggregateFunction::AvgIf),
        "MINIF" | "MIN_IF" => Some(AggregateFunction::MinIf),
        "MAXIF" | "MAX_IF" => Some(AggregateFunction::MaxIf),
        "GROUPING" => Some(AggregateFunction::Grouping),
        "GROUPING_ID" => Some(AggregateFunction::GroupingId),
        "LOGICAL_AND" | "BOOL_AND" => Some(AggregateFunction::LogicalAnd),
        "LOGICAL_OR" | "BOOL_OR" => Some(AggregateFunction::LogicalOr),
        "BIT_AND" => Some(AggregateFunction::BitAnd),
        "BIT_OR" => Some(AggregateFunction::BitOr),
        "BIT_XOR" => Some(AggregateFunction::BitXor),
        "APPROX_COUNT_DISTINCT" => Some(AggregateFunction::ApproxCountDistinct),
        "APPROX_QUANTILES" => Some(AggregateFunction::ApproxQuantiles),
        "APPROX_TOP_COUNT" => Some(AggregateFunction::ApproxTopCount),
        "APPROX_TOP_SUM" => Some(AggregateFunction::ApproxTopSum),
        "CORR" => Some(AggregateFunction::Corr),
        "COVAR_POP" => Some(AggregateFunction::CovarPop),
        "COVAR_SAMP" => Some(AggregateFunction::CovarSamp),
        "STDDEV" => Some(AggregateFunction::Stddev),
        "STDDEV_POP" => Some(AggregateFunction::StddevPop),
        "STDDEV_SAMP" => Some(AggregateFunction::StddevSamp),
        "VARIANCE" | "VAR" => Some(AggregateFunction::Variance),
        "VAR_POP" => Some(AggregateFunction::VarPop),
        "VAR_SAMP" => Some(AggregateFunction::VarSamp),
        _ => None,
    }
}
