#![coverage(off)]

use ordered_float::OrderedFloat;
use yachtsql_common::types::Value;

use super::utils::value_to_f64;

#[derive(Clone)]
pub(crate) struct CountIfAccumulator(pub(crate) i64);

impl CountIfAccumulator {
    pub(crate) fn new() -> Self {
        Self(0)
    }

    pub(crate) fn accumulate(&mut self, value: &Value) {
        if let Some(true) = value.as_bool() {
            self.0 += 1;
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        Value::Int64(self.0)
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        self.0 += other.0;
    }
}

#[derive(Clone)]
pub(crate) struct SumIfAccumulator(pub(crate) Option<f64>);

impl SumIfAccumulator {
    pub(crate) fn new() -> Self {
        Self(None)
    }

    pub(crate) fn accumulate_conditional(&mut self, value: &Value, condition: bool) {
        if !condition {
            return;
        }
        if let Some(v) = value_to_f64(value) {
            self.0 = Some(self.0.unwrap_or(0.0) + v);
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        self.0
            .map(|s| Value::Float64(OrderedFloat(s)))
            .unwrap_or(Value::Null)
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        self.0 = match (self.0, other.0) {
            (Some(x), Some(y)) => Some(x + y),
            (Some(x), None) => Some(x),
            (None, Some(y)) => Some(y),
            (None, None) => None,
        };
    }
}

#[derive(Clone)]
pub(crate) struct AvgIfAccumulator {
    pub(crate) sum: f64,
    pub(crate) count: i64,
}

impl AvgIfAccumulator {
    pub(crate) fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }

    pub(crate) fn accumulate_conditional(&mut self, value: &Value, condition: bool) {
        if !condition {
            return;
        }
        if let Some(v) = value_to_f64(value) {
            self.sum += v;
            self.count += 1;
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        if self.count > 0 {
            Value::Float64(OrderedFloat(self.sum / self.count as f64))
        } else {
            Value::Null
        }
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        self.sum += other.sum;
        self.count += other.count;
    }
}

#[derive(Clone)]
pub(crate) struct MinIfAccumulator(pub(crate) Option<Value>);

impl MinIfAccumulator {
    pub(crate) fn new() -> Self {
        Self(None)
    }

    pub(crate) fn accumulate_conditional(&mut self, value: &Value, condition: bool) {
        if !condition {
            return;
        }
        if !value.is_null() {
            self.0 = Some(match self.0.take() {
                Some(m) => {
                    if value < &m {
                        value.clone()
                    } else {
                        m
                    }
                }
                None => value.clone(),
            });
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        self.0.clone().unwrap_or(Value::Null)
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        self.0 = match (self.0.take(), &other.0) {
            (Some(x), Some(y)) => Some(if &x < y { x } else { y.clone() }),
            (Some(x), None) => Some(x),
            (None, Some(y)) => Some(y.clone()),
            (None, None) => None,
        };
    }
}

#[derive(Clone)]
pub(crate) struct MaxIfAccumulator(pub(crate) Option<Value>);

impl MaxIfAccumulator {
    pub(crate) fn new() -> Self {
        Self(None)
    }

    pub(crate) fn accumulate_conditional(&mut self, value: &Value, condition: bool) {
        if !condition {
            return;
        }
        if !value.is_null() {
            self.0 = Some(match self.0.take() {
                Some(m) => {
                    if value > &m {
                        value.clone()
                    } else {
                        m
                    }
                }
                None => value.clone(),
            });
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        self.0.clone().unwrap_or(Value::Null)
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        self.0 = match (self.0.take(), &other.0) {
            (Some(x), Some(y)) => Some(if &x > y { x } else { y.clone() }),
            (Some(x), None) => Some(x),
            (None, Some(y)) => Some(y.clone()),
            (None, None) => None,
        };
    }
}
