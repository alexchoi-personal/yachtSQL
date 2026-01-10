use datafusion::prelude::SessionContext;

pub fn register(_ctx: &SessionContext) {
    // TODO: Register conversion functions
    // CAST (handled by DataFusion natively)
    // SAFE_CAST - BigQuery-specific, returns NULL on error
    // TO_JSON, TO_JSON_STRING
    // PARSE_JSON
}
