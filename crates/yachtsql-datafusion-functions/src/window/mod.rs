use datafusion::prelude::SessionContext;

pub fn register_all(_ctx: &SessionContext) {
    // TODO: Register window functions
    // ROW_NUMBER, RANK, DENSE_RANK (mostly handled by DataFusion)
    // LAG, LEAD
    // FIRST_VALUE, LAST_VALUE, NTH_VALUE
    // NTILE
    // PERCENTILE_CONT, PERCENTILE_DISC
    // CUME_DIST, PERCENT_RANK
}
