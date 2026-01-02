#![coverage(off)]

use yachtsql_common::types::Value;

#[derive(Clone)]
pub(crate) struct ArrayAggAccumulator {
    pub(crate) items: Vec<(Value, Vec<(Value, bool)>)>,
    pub(crate) ignore_nulls: bool,
    pub(crate) limit: Option<usize>,
}

impl ArrayAggAccumulator {
    pub(crate) fn new(ignore_nulls: bool, limit: Option<usize>) -> Self {
        Self {
            items: Vec::new(),
            ignore_nulls,
            limit,
        }
    }

    pub(crate) fn accumulate_array_agg(&mut self, value: &Value, sort_keys: Vec<(Value, bool)>) {
        if self.ignore_nulls && value.is_null() {
            return;
        }
        self.items.push((value.clone(), sort_keys));
    }

    pub(crate) fn finalize(&self) -> Value {
        let mut sorted_items = self.items.clone();
        if !sorted_items.is_empty() && !sorted_items[0].1.is_empty() {
            sorted_items.sort_by(|a, b| {
                for ((val_a, asc_a), (val_b, _)) in a.1.iter().zip(b.1.iter()) {
                    let cmp = val_a
                        .partial_cmp(val_b)
                        .unwrap_or(std::cmp::Ordering::Equal);
                    let cmp = if *asc_a { cmp } else { cmp.reverse() };
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        let result_items: Vec<Value> = if let Some(lim) = self.limit {
            sorted_items.into_iter().take(lim).map(|(v, _)| v).collect()
        } else {
            sorted_items.into_iter().map(|(v, _)| v).collect()
        };
        Value::Array(result_items)
    }
}

#[derive(Clone)]
pub(crate) struct StringAggAccumulator {
    pub(crate) values: Vec<String>,
    pub(crate) separator: String,
}

impl StringAggAccumulator {
    pub(crate) fn new(separator: String) -> Self {
        Self {
            values: Vec::new(),
            separator,
        }
    }

    pub(crate) fn accumulate(&mut self, value: &Value) {
        if let Some(s) = value.as_str() {
            self.values.push(s.to_string());
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        if self.values.is_empty() {
            Value::Null
        } else {
            Value::String(self.values.join(&self.separator))
        }
    }
}

#[derive(Clone)]
pub(crate) struct CountDistinctAccumulator(pub(crate) Vec<Value>);

impl CountDistinctAccumulator {
    pub(crate) fn new() -> Self {
        Self(Vec::new())
    }

    pub(crate) fn accumulate(&mut self, value: &Value) {
        if !value.is_null() && !self.0.contains(value) {
            self.0.push(value.clone());
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        Value::Int64(self.0.len() as i64)
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        for v in &other.0 {
            if !self.0.contains(v) {
                self.0.push(v.clone());
            }
        }
    }
}
