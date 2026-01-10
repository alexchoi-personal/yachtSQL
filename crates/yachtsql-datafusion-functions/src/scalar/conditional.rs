use datafusion::prelude::SessionContext;

pub fn register(_ctx: &SessionContext) {
    // TODO: Register conditional functions
    // COALESCE, IF, IFNULL, NULLIF, NVL
    // CASE WHEN (handled by DataFusion natively)
}
