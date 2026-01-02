#![coverage(off)]

use yachtsql_common::types::Value;

#[derive(Clone)]
pub(crate) struct LogicalAndAccumulator(pub(crate) Option<bool>);

impl LogicalAndAccumulator {
    pub(crate) fn new() -> Self {
        Self(None)
    }

    pub(crate) fn accumulate(&mut self, value: &Value) {
        if let Some(b) = value.as_bool() {
            self.0 = Some(self.0.unwrap_or(true) && b);
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        self.0.map(Value::Bool).unwrap_or(Value::Null)
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        self.0 = match (self.0, other.0) {
            (Some(x), Some(y)) => Some(x && y),
            (Some(x), None) => Some(x),
            (None, Some(y)) => Some(y),
            (None, None) => None,
        };
    }
}

#[derive(Clone)]
pub(crate) struct LogicalOrAccumulator(pub(crate) Option<bool>);

impl LogicalOrAccumulator {
    pub(crate) fn new() -> Self {
        Self(None)
    }

    pub(crate) fn accumulate(&mut self, value: &Value) {
        if let Some(b) = value.as_bool() {
            self.0 = Some(self.0.unwrap_or(false) || b);
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        self.0.map(Value::Bool).unwrap_or(Value::Bool(false))
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        self.0 = match (self.0, other.0) {
            (Some(x), Some(y)) => Some(x || y),
            (Some(x), None) => Some(x),
            (None, Some(y)) => Some(y),
            (None, None) => None,
        };
    }
}
