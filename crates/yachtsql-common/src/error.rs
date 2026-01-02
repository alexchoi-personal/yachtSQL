#![coverage(off)]

use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub enum Error {
    ParseError(String),
    InvalidQuery(String),
    RaisedException(String),
    TableNotFound(String),
    FunctionNotFound(String),
    ColumnNotFound(String),
    AmbiguousColumn(String),
    TypeMismatch { expected: String, actual: String },
    SchemaMismatch(String),
    UnsupportedFeature(String),
    UnsupportedStatement(String),
    UnsupportedExpression(String),
    InvalidLiteral(String),
    InvalidFunction(String),
    DivisionByZero,
    Overflow,
    Internal(String),
}

impl Error {
    pub fn parse_error(msg: impl Into<String>) -> Self {
        Error::ParseError(msg.into())
    }

    pub fn invalid_query(msg: impl Into<String>) -> Self {
        Error::InvalidQuery(msg.into())
    }

    pub fn raised_exception(msg: impl Into<String>) -> Self {
        Error::RaisedException(msg.into())
    }

    pub fn table_not_found(name: impl Into<String>) -> Self {
        Error::TableNotFound(name.into())
    }

    pub fn function_not_found(name: impl Into<String>) -> Self {
        Error::FunctionNotFound(name.into())
    }

    pub fn column_not_found(name: impl Into<String>) -> Self {
        Error::ColumnNotFound(name.into())
    }

    pub fn ambiguous_column(name: impl Into<String>) -> Self {
        Error::AmbiguousColumn(name.into())
    }

    pub fn type_mismatch(expected: impl Into<String>, actual: impl Into<String>) -> Self {
        Error::TypeMismatch {
            expected: expected.into(),
            actual: actual.into(),
        }
    }

    pub fn type_mismatch_msg(msg: impl Into<String>) -> Self {
        let msg = msg.into();
        Error::InvalidQuery(format!("Type mismatch: {}", msg))
    }

    pub fn schema_mismatch(msg: impl Into<String>) -> Self {
        Error::SchemaMismatch(msg.into())
    }

    pub fn unsupported(msg: impl Into<String>) -> Self {
        Error::UnsupportedFeature(msg.into())
    }

    pub fn unsupported_statement(msg: impl Into<String>) -> Self {
        Error::UnsupportedStatement(msg.into())
    }

    pub fn unsupported_expression(msg: impl Into<String>) -> Self {
        Error::UnsupportedExpression(msg.into())
    }

    pub fn invalid_literal(msg: impl Into<String>) -> Self {
        Error::InvalidLiteral(msg.into())
    }

    pub fn invalid_function(msg: impl Into<String>) -> Self {
        Error::InvalidFunction(msg.into())
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Error::Internal(msg.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ParseError(msg) => write!(f, "Parse error: {}", msg),
            Error::InvalidQuery(msg) => write!(f, "Invalid query: {}", msg),
            Error::RaisedException(msg) => write!(f, "{}", msg),
            Error::TableNotFound(name) => write!(f, "Table not found: {}", name),
            Error::FunctionNotFound(name) => write!(f, "Function not found: {}", name),
            Error::ColumnNotFound(name) => write!(f, "Column not found: {}", name),
            Error::AmbiguousColumn(name) => write!(f, "Ambiguous column: {}", name),
            Error::TypeMismatch { expected, actual } => {
                write!(f, "Type mismatch: expected {}, got {}", expected, actual)
            }
            Error::SchemaMismatch(msg) => write!(f, "Schema mismatch: {}", msg),
            Error::UnsupportedFeature(msg) => write!(f, "Unsupported feature: {}", msg),
            Error::UnsupportedStatement(msg) => write!(f, "Unsupported statement: {}", msg),
            Error::UnsupportedExpression(msg) => write!(f, "Unsupported expression: {}", msg),
            Error::InvalidLiteral(msg) => write!(f, "Invalid literal: {}", msg),
            Error::InvalidFunction(msg) => write!(f, "Invalid function: {}", msg),
            Error::DivisionByZero => write!(f, "Division by zero"),
            Error::Overflow => write!(f, "Numeric overflow"),
            Error::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_constructors() {
        let e = Error::parse_error("parse error");
        assert!(matches!(e, Error::ParseError(_)));

        let e = Error::invalid_query("invalid query");
        assert!(matches!(e, Error::InvalidQuery(_)));

        let e = Error::raised_exception("exception message");
        assert!(matches!(e, Error::RaisedException(_)));

        let e = Error::table_not_found("my_table");
        assert!(matches!(e, Error::TableNotFound(_)));

        let e = Error::function_not_found("my_function");
        assert!(matches!(e, Error::FunctionNotFound(_)));

        let e = Error::column_not_found("my_column");
        assert!(matches!(e, Error::ColumnNotFound(_)));

        let e = Error::ambiguous_column("ambiguous_col");
        assert!(matches!(e, Error::AmbiguousColumn(_)));

        let e = Error::type_mismatch("INT64", "STRING");
        match e {
            Error::TypeMismatch { expected, actual } => {
                assert_eq!(expected, "INT64");
                assert_eq!(actual, "STRING");
            }
            _ => panic!("expected TypeMismatch"),
        }

        let e = Error::type_mismatch_msg("type mismatch message");
        assert!(matches!(e, Error::InvalidQuery(_)));

        let e = Error::schema_mismatch("schema mismatch");
        assert!(matches!(e, Error::SchemaMismatch(_)));

        let e = Error::unsupported("unsupported feature");
        assert!(matches!(e, Error::UnsupportedFeature(_)));

        let e = Error::unsupported_statement("unsupported statement");
        assert!(matches!(e, Error::UnsupportedStatement(_)));

        let e = Error::unsupported_expression("unsupported expression");
        assert!(matches!(e, Error::UnsupportedExpression(_)));

        let e = Error::invalid_literal("invalid literal");
        assert!(matches!(e, Error::InvalidLiteral(_)));

        let e = Error::invalid_function("invalid function");
        assert!(matches!(e, Error::InvalidFunction(_)));

        let e = Error::internal("internal error");
        assert!(matches!(e, Error::Internal(_)));
    }

    #[test]
    fn test_error_display() {
        assert_eq!(
            format!("{}", Error::ParseError("test".to_string())),
            "Parse error: test"
        );
        assert_eq!(
            format!("{}", Error::InvalidQuery("test".to_string())),
            "Invalid query: test"
        );
        assert_eq!(
            format!("{}", Error::RaisedException("test".to_string())),
            "test"
        );
        assert_eq!(
            format!("{}", Error::TableNotFound("test".to_string())),
            "Table not found: test"
        );
        assert_eq!(
            format!("{}", Error::FunctionNotFound("test".to_string())),
            "Function not found: test"
        );
        assert_eq!(
            format!("{}", Error::ColumnNotFound("test".to_string())),
            "Column not found: test"
        );
        assert_eq!(
            format!("{}", Error::AmbiguousColumn("test".to_string())),
            "Ambiguous column: test"
        );
        assert_eq!(
            format!(
                "{}",
                Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: "STRING".to_string()
                }
            ),
            "Type mismatch: expected INT64, got STRING"
        );
        assert_eq!(
            format!("{}", Error::SchemaMismatch("test".to_string())),
            "Schema mismatch: test"
        );
        assert_eq!(
            format!("{}", Error::UnsupportedFeature("test".to_string())),
            "Unsupported feature: test"
        );
        assert_eq!(
            format!("{}", Error::UnsupportedStatement("test".to_string())),
            "Unsupported statement: test"
        );
        assert_eq!(
            format!("{}", Error::UnsupportedExpression("test".to_string())),
            "Unsupported expression: test"
        );
        assert_eq!(
            format!("{}", Error::InvalidLiteral("test".to_string())),
            "Invalid literal: test"
        );
        assert_eq!(
            format!("{}", Error::InvalidFunction("test".to_string())),
            "Invalid function: test"
        );
        assert_eq!(format!("{}", Error::DivisionByZero), "Division by zero");
        assert_eq!(format!("{}", Error::Overflow), "Numeric overflow");
        assert_eq!(
            format!("{}", Error::Internal("test".to_string())),
            "Internal error: test"
        );
    }

    #[test]
    fn test_error_debug() {
        let e = Error::ParseError("test".to_string());
        let debug_str = format!("{:?}", e);
        assert!(debug_str.contains("ParseError"));
    }

    #[test]
    fn test_error_clone() {
        let e = Error::ParseError("test".to_string());
        let e2 = e.clone();
        assert!(matches!(e2, Error::ParseError(_)));
    }

    #[test]
    fn test_error_is_std_error() {
        let e: Box<dyn std::error::Error> = Box::new(Error::ParseError("test".to_string()));
        assert!(e.to_string().contains("Parse error"));
    }
}
