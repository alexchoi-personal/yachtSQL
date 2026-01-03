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
    TypeMismatch {
        expected: String,
        actual: String,
    },
    SchemaMismatch(String),
    UnsupportedFeature(String),
    UnsupportedStatement(String),
    UnsupportedExpression(String),
    InvalidLiteral(String),
    InvalidFunction(String),
    DivisionByZero,
    Overflow,
    Internal(String),
    IntervalOverflow {
        operation: String,
        value: String,
    },
    NumericOverflow {
        operation: String,
    },
    DivisionByZeroWithContext {
        context: String,
    },
    TypeCoercionFailed {
        from: String,
        to: String,
        value: String,
    },
    DateTimeError {
        operation: String,
        reason: String,
    },
    ExtractError {
        field: String,
        from_type: String,
    },
    AggregateError {
        function: String,
        reason: String,
    },
    ScriptingError {
        script: String,
        reason: String,
    },
    UdfError {
        function: String,
        reason: String,
    },
    NetworkError {
        operation: String,
        reason: String,
    },
    JsonAccessError {
        path: String,
        reason: String,
    },
    IoError {
        operation: String,
        message: String,
    },
    IndexOutOfBounds {
        index: usize,
        length: usize,
    },
    NullValue {
        context: String,
    },
    InvalidArgument {
        function: String,
        argument: String,
        reason: String,
    },
    OutOfRange {
        value: String,
        typ: String,
    },
    RegexError {
        pattern: String,
        reason: String,
    },
    NotNullViolation {
        table: String,
        column: String,
    },
    UniqueViolation {
        table: String,
        constraint: String,
        value: String,
    },
    PrimaryKeyViolation {
        table: String,
        value: String,
    },
    PrimaryKeyNullViolation {
        table: String,
        column: String,
    },
    CheckViolation {
        table: String,
        constraint: String,
        expression: String,
    },
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

    pub fn interval_overflow(operation: impl Into<String>, value: impl Into<String>) -> Self {
        Error::IntervalOverflow {
            operation: operation.into(),
            value: value.into(),
        }
    }

    pub fn numeric_overflow(operation: impl Into<String>) -> Self {
        Error::NumericOverflow {
            operation: operation.into(),
        }
    }

    pub fn division_by_zero_ctx(context: impl Into<String>) -> Self {
        Error::DivisionByZeroWithContext {
            context: context.into(),
        }
    }

    pub fn type_coercion_failed(
        from: impl Into<String>,
        to: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        Error::TypeCoercionFailed {
            from: from.into(),
            to: to.into(),
            value: value.into(),
        }
    }

    pub fn datetime_error(operation: impl Into<String>, reason: impl Into<String>) -> Self {
        Error::DateTimeError {
            operation: operation.into(),
            reason: reason.into(),
        }
    }

    pub fn extract_error(field: impl Into<String>, from_type: impl Into<String>) -> Self {
        Error::ExtractError {
            field: field.into(),
            from_type: from_type.into(),
        }
    }

    pub fn aggregate_error(function: impl Into<String>, reason: impl Into<String>) -> Self {
        Error::AggregateError {
            function: function.into(),
            reason: reason.into(),
        }
    }

    pub fn scripting_error(script: impl Into<String>, reason: impl Into<String>) -> Self {
        Error::ScriptingError {
            script: script.into(),
            reason: reason.into(),
        }
    }

    pub fn udf_error(function: impl Into<String>, reason: impl Into<String>) -> Self {
        Error::UdfError {
            function: function.into(),
            reason: reason.into(),
        }
    }

    pub fn network_error(operation: impl Into<String>, reason: impl Into<String>) -> Self {
        Error::NetworkError {
            operation: operation.into(),
            reason: reason.into(),
        }
    }

    pub fn json_access_error(path: impl Into<String>, reason: impl Into<String>) -> Self {
        Error::JsonAccessError {
            path: path.into(),
            reason: reason.into(),
        }
    }

    pub fn io_error(operation: impl Into<String>, message: impl Into<String>) -> Self {
        Error::IoError {
            operation: operation.into(),
            message: message.into(),
        }
    }

    pub fn index_out_of_bounds(index: usize, length: usize) -> Self {
        Error::IndexOutOfBounds { index, length }
    }

    pub fn null_value(context: impl Into<String>) -> Self {
        Error::NullValue {
            context: context.into(),
        }
    }

    pub fn invalid_argument(
        function: impl Into<String>,
        argument: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Error::InvalidArgument {
            function: function.into(),
            argument: argument.into(),
            reason: reason.into(),
        }
    }

    pub fn out_of_range(value: impl Into<String>, typ: impl Into<String>) -> Self {
        Error::OutOfRange {
            value: value.into(),
            typ: typ.into(),
        }
    }

    pub fn regex_error(pattern: impl Into<String>, reason: impl Into<String>) -> Self {
        Error::RegexError {
            pattern: pattern.into(),
            reason: reason.into(),
        }
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
            Error::IntervalOverflow { operation, value } => {
                write!(f, "Interval overflow in {}: {}", operation, value)
            }
            Error::NumericOverflow { operation } => {
                write!(f, "Numeric overflow in {}", operation)
            }
            Error::DivisionByZeroWithContext { context } => {
                write!(f, "Division by zero: {}", context)
            }
            Error::TypeCoercionFailed { from, to, value } => {
                write!(f, "Cannot coerce {} from {} to {}", value, from, to)
            }
            Error::DateTimeError { operation, reason } => {
                write!(f, "DateTime error in {}: {}", operation, reason)
            }
            Error::ExtractError { field, from_type } => {
                write!(f, "Cannot extract {} from {}", field, from_type)
            }
            Error::AggregateError { function, reason } => {
                write!(f, "Aggregate error in {}: {}", function, reason)
            }
            Error::ScriptingError { script, reason } => {
                write!(f, "Scripting error in {}: {}", script, reason)
            }
            Error::UdfError { function, reason } => {
                write!(f, "UDF error in {}: {}", function, reason)
            }
            Error::NetworkError { operation, reason } => {
                write!(f, "Network error in {}: {}", operation, reason)
            }
            Error::JsonAccessError { path, reason } => {
                write!(f, "JSON access error at {}: {}", path, reason)
            }
            Error::IoError { operation, message } => {
                write!(f, "I/O error in {}: {}", operation, message)
            }
            Error::IndexOutOfBounds { index, length } => {
                write!(f, "Index {} out of bounds for length {}", index, length)
            }
            Error::NullValue { context } => {
                write!(f, "Null value encountered: {}", context)
            }
            Error::InvalidArgument {
                function,
                argument,
                reason,
            } => {
                write!(
                    f,
                    "Invalid argument '{}' for {}: {}",
                    argument, function, reason
                )
            }
            Error::OutOfRange { value, typ } => {
                write!(f, "Value {} out of range for type {}", value, typ)
            }
            Error::RegexError { pattern, reason } => {
                write!(f, "Regex error in pattern '{}': {}", pattern, reason)
            }
            Error::NotNullViolation { table, column } => {
                write!(
                    f,
                    "NOT NULL constraint violation: column '{}' in table '{}' cannot be null",
                    column, table
                )
            }
            Error::UniqueViolation {
                table,
                constraint,
                value,
            } => {
                write!(
                    f,
                    "UNIQUE constraint '{}' violation in table '{}': duplicate value {}",
                    constraint, table, value
                )
            }
            Error::PrimaryKeyViolation { table, value } => {
                write!(
                    f,
                    "PRIMARY KEY violation in table '{}': duplicate value {}",
                    table, value
                )
            }
            Error::PrimaryKeyNullViolation { table, column } => {
                write!(
                    f,
                    "PRIMARY KEY violation: column '{}' in table '{}' cannot be null",
                    column, table
                )
            }
            Error::CheckViolation {
                table,
                constraint,
                expression,
            } => {
                write!(
                    f,
                    "CHECK constraint '{}' violation in table '{}': expression {} evaluated to false",
                    constraint, table, expression
                )
            }
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
