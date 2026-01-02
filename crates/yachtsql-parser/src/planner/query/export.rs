#![coverage(off)]

use sqlparser::ast;
use yachtsql_common::error::{Error, Result};
use yachtsql_ir::{ExportFormat, ExportOptions, LogicalPlan};

use super::Planner;
use crate::CatalogProvider;

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub(in crate::planner) fn plan_export_data(
        &self,
        export_data: &ast::ExportData,
    ) -> Result<LogicalPlan> {
        let query = self.plan_query(&export_data.query)?;

        let mut uri = String::new();
        let mut format = ExportFormat::Parquet;
        let mut compression = None;
        let mut field_delimiter = None;
        let mut header = None;
        let mut overwrite = false;

        for option in &export_data.options {
            if let ast::SqlOption::KeyValue { key, value } = option {
                let key_str = key.value.to_uppercase();
                let value_str = Self::option_value_to_string(value);

                match key_str.as_str() {
                    "URI" => uri = value_str,
                    "FORMAT" => {
                        format = match value_str.to_uppercase().as_str() {
                            "CSV" => ExportFormat::Csv,
                            "JSON" => ExportFormat::Json,
                            "AVRO" => ExportFormat::Avro,
                            "PARQUET" => ExportFormat::Parquet,
                            _ => ExportFormat::Parquet,
                        };
                    }
                    "COMPRESSION" => compression = Some(value_str),
                    "FIELD_DELIMITER" => field_delimiter = Some(value_str),
                    "HEADER" => header = Some(value_str.to_uppercase() == "TRUE"),
                    "OVERWRITE" => overwrite = value_str.to_uppercase() == "TRUE",
                    _ => {}
                }
            }
        }

        if uri.is_empty() {
            return Err(Error::parse_error("EXPORT DATA requires uri option"));
        }

        let options = ExportOptions {
            uri,
            format,
            compression,
            field_delimiter,
            header,
            overwrite,
        };

        Ok(LogicalPlan::ExportData {
            options,
            query: Box::new(query),
        })
    }

    fn option_value_to_string(expr: &ast::Expr) -> String {
        match expr {
            ast::Expr::Value(v) => match &v.value {
                ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => s.clone(),
                ast::Value::Boolean(b) => b.to_string(),
                ast::Value::Number(n, _) => n.clone(),
                _ => format!("{}", v),
            },
            ast::Expr::Identifier(ident) => ident.value.clone(),
            _ => format!("{}", expr),
        }
    }
}
