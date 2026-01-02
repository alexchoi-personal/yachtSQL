#![coverage(off)]

use yachtsql_common::types::Value;

#[derive(Clone)]
pub(crate) struct BitAndAccumulator(pub(crate) Option<i64>);

impl BitAndAccumulator {
    pub(crate) fn new() -> Self {
        Self(None)
    }

    pub(crate) fn accumulate(&mut self, value: &Value) {
        if let Some(v) = value.as_i64() {
            self.0 = Some(match self.0 {
                Some(a) => a & v,
                None => v,
            });
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        self.0.map(Value::Int64).unwrap_or(Value::Null)
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        self.0 = match (self.0, other.0) {
            (Some(x), Some(y)) => Some(x & y),
            (Some(x), None) => Some(x),
            (None, Some(y)) => Some(y),
            (None, None) => None,
        };
    }
}

#[derive(Clone)]
pub(crate) struct BitOrAccumulator(pub(crate) Option<i64>);

impl BitOrAccumulator {
    pub(crate) fn new() -> Self {
        Self(None)
    }

    pub(crate) fn accumulate(&mut self, value: &Value) {
        if let Some(v) = value.as_i64() {
            self.0 = Some(match self.0 {
                Some(a) => a | v,
                None => v,
            });
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        self.0.map(Value::Int64).unwrap_or(Value::Null)
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        self.0 = match (self.0, other.0) {
            (Some(x), Some(y)) => Some(x | y),
            (Some(x), None) => Some(x),
            (None, Some(y)) => Some(y),
            (None, None) => None,
        };
    }
}

#[derive(Clone)]
pub(crate) struct BitXorAccumulator(pub(crate) Option<i64>);

impl BitXorAccumulator {
    pub(crate) fn new() -> Self {
        Self(None)
    }

    pub(crate) fn accumulate(&mut self, value: &Value) {
        if let Some(v) = value.as_i64() {
            self.0 = Some(match self.0 {
                Some(a) => a ^ v,
                None => v,
            });
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        self.0.map(Value::Int64).unwrap_or(Value::Null)
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        self.0 = match (self.0, other.0) {
            (Some(x), Some(y)) => Some(x ^ y),
            (Some(x), None) => Some(x),
            (None, Some(y)) => Some(y),
            (None, None) => None,
        };
    }
}
