#![coverage(off)]

use std::collections::HashMap;

use ordered_float::OrderedFloat;
use yachtsql_common::types::Value;

use super::utils::value_to_f64;

#[derive(Clone)]
pub(crate) struct ApproxQuantilesAccumulator {
    pub(crate) values: Vec<f64>,
    pub(crate) num_quantiles: usize,
}

impl ApproxQuantilesAccumulator {
    pub(crate) fn new(num_quantiles: usize) -> Self {
        Self {
            values: Vec::new(),
            num_quantiles,
        }
    }

    pub(crate) fn accumulate(&mut self, value: &Value) {
        if value.is_null() {
            return;
        }
        if let Some(x) = value_to_f64(value) {
            self.values.push(x);
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        if self.values.is_empty() {
            return Value::Array(vec![]);
        }
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = sorted.len();
        let mut quantiles = Vec::with_capacity(self.num_quantiles + 1);
        for i in 0..=self.num_quantiles {
            let pos = (i as f64 / self.num_quantiles as f64) * (n - 1) as f64;
            let idx = pos.floor() as usize;
            let frac = pos - idx as f64;
            let val = if idx + 1 < n {
                sorted[idx] * (1.0 - frac) + sorted[idx + 1] * frac
            } else {
                sorted[idx]
            };
            quantiles.push(Value::Float64(OrderedFloat(val)));
        }
        Value::Array(quantiles)
    }
}

#[derive(Clone)]
pub(crate) struct ApproxTopCountAccumulator {
    pub(crate) counts: HashMap<String, i64>,
    pub(crate) top_n: usize,
}

impl ApproxTopCountAccumulator {
    pub(crate) fn new(top_n: usize) -> Self {
        Self {
            counts: HashMap::new(),
            top_n,
        }
    }

    pub(crate) fn accumulate(&mut self, value: &Value) {
        if !value.is_null() {
            let key = format!("{:?}", value);
            *self.counts.entry(key).or_insert(0) += 1;
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        let mut entries: Vec<_> = self.counts.iter().collect();
        entries.sort_by(|a, b| b.1.cmp(a.1));
        entries.truncate(self.top_n);
        let result: Vec<Value> = entries
            .into_iter()
            .map(|(key, count)| {
                let parsed_val = if key.starts_with("String(\"") && key.ends_with("\")") {
                    Value::String(key[8..key.len() - 2].to_string())
                } else if key.starts_with("Int64(") && key.ends_with(")") {
                    key[6..key.len() - 1]
                        .parse::<i64>()
                        .map(Value::Int64)
                        .unwrap_or_else(|_| Value::String(key.clone()))
                } else {
                    Value::String(key.clone())
                };
                Value::Struct(vec![
                    ("value".to_string(), parsed_val),
                    ("count".to_string(), Value::Int64(*count)),
                ])
            })
            .collect();
        Value::Array(result)
    }
}

#[derive(Clone)]
pub(crate) struct ApproxTopSumAccumulator {
    pub(crate) sums: HashMap<String, f64>,
    pub(crate) top_n: usize,
}

impl ApproxTopSumAccumulator {
    pub(crate) fn new(top_n: usize) -> Self {
        Self {
            sums: HashMap::new(),
            top_n,
        }
    }

    pub(crate) fn accumulate_weighted(&mut self, value: &Value, weight: &Value) {
        if value.is_null() || weight.is_null() {
            return;
        }
        let key = format!("{:?}", value);
        let w = value_to_f64(weight).unwrap_or(0.0);
        *self.sums.entry(key).or_insert(0.0) += w;
    }

    pub(crate) fn finalize(&self) -> Value {
        let mut entries: Vec<_> = self.sums.iter().collect();
        entries.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap_or(std::cmp::Ordering::Equal));
        entries.truncate(self.top_n);
        let result: Vec<Value> = entries
            .into_iter()
            .map(|(key, sum)| {
                let parsed_val = if key.starts_with("String(\"") && key.ends_with("\")") {
                    Value::String(key[8..key.len() - 2].to_string())
                } else if key.starts_with("Int64(") && key.ends_with(")") {
                    key[6..key.len() - 1]
                        .parse::<i64>()
                        .map(Value::Int64)
                        .unwrap_or_else(|_| Value::String(key.clone()))
                } else {
                    Value::String(key.clone())
                };
                Value::Struct(vec![
                    ("value".to_string(), parsed_val),
                    ("sum".to_string(), Value::Float64(OrderedFloat(*sum))),
                ])
            })
            .collect();
        Value::Array(result)
    }
}
