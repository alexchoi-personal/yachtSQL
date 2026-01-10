#![feature(coverage_attribute)]

mod error;
mod js_udf;
mod py_udf;
mod session;

pub use datafusion::arrow::record_batch::RecordBatch;
pub use datafusion::prelude::*;
pub use error::{Error, Result};
pub use session::YachtSQLSession;
