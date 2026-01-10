use datafusion::prelude::SessionContext;

pub fn register_all(_ctx: &SessionContext) {
    // TODO: Register aggregate functions
    // COUNT, SUM, AVG, MIN, MAX (mostly handled by DataFusion)
    // ARRAY_AGG, STRING_AGG
    // COUNT_IF, SUM_IF, AVG_IF (conditional aggregates)
    // ANY_VALUE
    // APPROX_COUNT_DISTINCT, APPROX_QUANTILES, APPROX_TOP_COUNT
    // CORR, COVAR_POP, COVAR_SAMP
    // STDDEV, STDDEV_POP, STDDEV_SAMP
    // VARIANCE, VAR_POP, VAR_SAMP
    // BIT_AND, BIT_OR, BIT_XOR
    // LOGICAL_AND, LOGICAL_OR
}
