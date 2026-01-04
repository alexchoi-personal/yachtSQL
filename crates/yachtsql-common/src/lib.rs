//! Common types and error handling for YachtSQL (BigQuery dialect).

#![feature(coverage_attribute)]
#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![warn(rustdoc::broken_intra_doc_links)]
#![allow(missing_docs)]

pub mod error;
pub mod result;
pub mod types;

pub use error::{Error, Result};
pub use result::{ColumnInfo, QueryResult, Row};
