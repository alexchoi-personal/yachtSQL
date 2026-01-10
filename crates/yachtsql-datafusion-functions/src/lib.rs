#![doc = include_str!("../README.md")]

pub mod aggregate;
pub mod scalar;
pub mod window;

use datafusion::prelude::SessionContext;

pub struct BigQueryFunctionRegistry;

impl BigQueryFunctionRegistry {
    pub fn register_all(ctx: &SessionContext) {
        Self::register_scalar_functions(ctx);
        Self::register_aggregate_functions(ctx);
        Self::register_window_functions(ctx);
    }

    pub fn register_scalar_functions(ctx: &SessionContext) {
        scalar::register_all(ctx);
    }

    pub fn register_aggregate_functions(ctx: &SessionContext) {
        aggregate::register_all(ctx);
    }

    pub fn register_window_functions(ctx: &SessionContext) {
        window::register_all(ctx);
    }
}
