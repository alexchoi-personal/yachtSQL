#![coverage(off)]

use yachtsql_common::types::Value;

#[derive(Clone)]
pub(crate) struct GroupingAccumulator {
    pub(crate) value: i64,
}

impl GroupingAccumulator {
    pub(crate) fn new() -> Self {
        Self { value: 0 }
    }

    pub(crate) fn set_value(&mut self, value: i64) {
        self.value = value;
    }

    pub(crate) fn finalize(&self) -> Value {
        Value::Int64(self.value)
    }
}

#[derive(Clone)]
pub(crate) struct GroupingIdAccumulator {
    pub(crate) value: i64,
}

impl GroupingIdAccumulator {
    pub(crate) fn new() -> Self {
        Self { value: 0 }
    }

    pub(crate) fn set_value(&mut self, value: i64) {
        self.value = value;
    }

    pub(crate) fn finalize(&self) -> Value {
        Value::Int64(self.value)
    }
}
