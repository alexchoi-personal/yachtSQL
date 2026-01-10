mod conditional;
mod conversion;
mod datetime;
mod math;
mod string;

use datafusion::prelude::SessionContext;

pub fn register_all(ctx: &SessionContext) {
    string::register(ctx);
    math::register(ctx);
    datetime::register(ctx);
    conditional::register(ctx);
    conversion::register(ctx);
}
