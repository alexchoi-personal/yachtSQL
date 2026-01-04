#![coverage(off)]

use aligned_vec::AVec;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

use super::Column;
use crate::NullBitmap;

impl Column {
    pub fn broadcast(value: Value, len: usize) -> Self {
        if len == 0 {
            return Column::new(&value.data_type());
        }

        match value {
            Value::Null => Column::Int64 {
                data: AVec::from_iter(64, std::iter::repeat_n(0, len)),
                nulls: NullBitmap::new_null(len),
            },
            Value::Bool(v) => Column::Bool {
                data: vec![v; len],
                nulls: NullBitmap::new_valid(len),
            },
            Value::Int64(v) => Column::Int64 {
                data: AVec::from_iter(64, std::iter::repeat_n(v, len)),
                nulls: NullBitmap::new_valid(len),
            },
            Value::Float64(v) => Column::Float64 {
                data: AVec::from_iter(64, std::iter::repeat_n(v.0, len)),
                nulls: NullBitmap::new_valid(len),
            },
            Value::Numeric(v) => Column::Numeric {
                data: vec![v; len],
                nulls: NullBitmap::new_valid(len),
            },
            Value::BigNumeric(v) => Column::Numeric {
                data: vec![v; len],
                nulls: NullBitmap::new_valid(len),
            },
            Value::String(v) => Column::String {
                data: vec![v; len],
                nulls: NullBitmap::new_valid(len),
            },
            Value::Bytes(v) => Column::Bytes {
                data: vec![v; len],
                nulls: NullBitmap::new_valid(len),
            },
            Value::Date(v) => Column::Date {
                data: vec![v; len],
                nulls: NullBitmap::new_valid(len),
            },
            Value::Time(v) => Column::Time {
                data: vec![v; len],
                nulls: NullBitmap::new_valid(len),
            },
            Value::DateTime(v) => Column::DateTime {
                data: vec![v; len],
                nulls: NullBitmap::new_valid(len),
            },
            Value::Timestamp(v) => Column::Timestamp {
                data: vec![v; len],
                nulls: NullBitmap::new_valid(len),
            },
            Value::Json(v) => Column::Json {
                data: vec![v; len],
                nulls: NullBitmap::new_valid(len),
            },
            Value::Array(v) => Column::Array {
                element_type: if v.is_empty() {
                    yachtsql_common::types::DataType::Unknown
                } else {
                    v[0].data_type()
                },
                data: vec![v; len],
                nulls: NullBitmap::new_valid(len),
            },
            Value::Struct(v) => Column::Struct {
                fields: v
                    .iter()
                    .map(|(n, val)| (n.clone(), val.data_type()))
                    .collect(),
                data: vec![v; len],
                nulls: NullBitmap::new_valid(len),
            },
            Value::Geography(v) => Column::Geography {
                data: vec![v; len],
                nulls: NullBitmap::new_valid(len),
            },
            Value::Interval(v) => Column::Interval {
                data: vec![v; len],
                nulls: NullBitmap::new_valid(len),
            },
            Value::Range(v) => Column::Range {
                element_type: yachtsql_common::types::DataType::Unknown,
                data: vec![v; len],
                nulls: NullBitmap::new_valid(len),
            },
            Value::Default => Column::Int64 {
                data: AVec::from_iter(64, std::iter::repeat_n(0, len)),
                nulls: NullBitmap::new_null(len),
            },
        }
    }

    pub fn gather(&self, indices: &[usize]) -> Result<Self> {
        let len = self.len();
        if let Some(&max_idx) = indices.iter().max()
            && max_idx >= len
        {
            return Err(Error::internal(format!(
                "gather: index {} out of bounds for column of length {}",
                max_idx, len
            )));
        }
        Ok(match self {
            Column::Bool { data, nulls } => {
                let mut new_data = Vec::with_capacity(indices.len());
                let mut new_nulls = NullBitmap::new();
                for &idx in indices {
                    new_data.push(data[idx]);
                    new_nulls.push(nulls.is_null(idx));
                }
                Column::Bool {
                    data: new_data,
                    nulls: new_nulls,
                }
            }
            Column::Int64 { data, nulls } => {
                let mut new_data = AVec::new(64);
                let mut new_nulls = NullBitmap::new();
                for &idx in indices {
                    new_data.push(data[idx]);
                    new_nulls.push(nulls.is_null(idx));
                }
                Column::Int64 {
                    data: new_data,
                    nulls: new_nulls,
                }
            }
            Column::Float64 { data, nulls } => {
                let mut new_data = AVec::new(64);
                let mut new_nulls = NullBitmap::new();
                for &idx in indices {
                    new_data.push(data[idx]);
                    new_nulls.push(nulls.is_null(idx));
                }
                Column::Float64 {
                    data: new_data,
                    nulls: new_nulls,
                }
            }
            Column::Numeric { data, nulls } => {
                let mut new_data = Vec::with_capacity(indices.len());
                let mut new_nulls = NullBitmap::new();
                for &idx in indices {
                    new_data.push(data[idx]);
                    new_nulls.push(nulls.is_null(idx));
                }
                Column::Numeric {
                    data: new_data,
                    nulls: new_nulls,
                }
            }
            Column::String { data, nulls } => {
                let mut new_data = Vec::with_capacity(indices.len());
                let mut new_nulls = NullBitmap::new();
                for &idx in indices {
                    new_data.push(data[idx].clone());
                    new_nulls.push(nulls.is_null(idx));
                }
                Column::String {
                    data: new_data,
                    nulls: new_nulls,
                }
            }
            Column::Bytes { data, nulls } => {
                let mut new_data = Vec::with_capacity(indices.len());
                let mut new_nulls = NullBitmap::new();
                for &idx in indices {
                    new_data.push(data[idx].clone());
                    new_nulls.push(nulls.is_null(idx));
                }
                Column::Bytes {
                    data: new_data,
                    nulls: new_nulls,
                }
            }
            Column::Date { data, nulls } => {
                let mut new_data = Vec::with_capacity(indices.len());
                let mut new_nulls = NullBitmap::new();
                for &idx in indices {
                    new_data.push(data[idx]);
                    new_nulls.push(nulls.is_null(idx));
                }
                Column::Date {
                    data: new_data,
                    nulls: new_nulls,
                }
            }
            Column::Time { data, nulls } => {
                let mut new_data = Vec::with_capacity(indices.len());
                let mut new_nulls = NullBitmap::new();
                for &idx in indices {
                    new_data.push(data[idx]);
                    new_nulls.push(nulls.is_null(idx));
                }
                Column::Time {
                    data: new_data,
                    nulls: new_nulls,
                }
            }
            Column::DateTime { data, nulls } => {
                let mut new_data = Vec::with_capacity(indices.len());
                let mut new_nulls = NullBitmap::new();
                for &idx in indices {
                    new_data.push(data[idx]);
                    new_nulls.push(nulls.is_null(idx));
                }
                Column::DateTime {
                    data: new_data,
                    nulls: new_nulls,
                }
            }
            Column::Timestamp { data, nulls } => {
                let mut new_data = Vec::with_capacity(indices.len());
                let mut new_nulls = NullBitmap::new();
                for &idx in indices {
                    new_data.push(data[idx]);
                    new_nulls.push(nulls.is_null(idx));
                }
                Column::Timestamp {
                    data: new_data,
                    nulls: new_nulls,
                }
            }
            Column::Json { data, nulls } => {
                let mut new_data = Vec::with_capacity(indices.len());
                let mut new_nulls = NullBitmap::new();
                for &idx in indices {
                    new_data.push(data[idx].clone());
                    new_nulls.push(nulls.is_null(idx));
                }
                Column::Json {
                    data: new_data,
                    nulls: new_nulls,
                }
            }
            Column::Array {
                data,
                nulls,
                element_type,
            } => {
                let mut new_data = Vec::with_capacity(indices.len());
                let mut new_nulls = NullBitmap::new();
                for &idx in indices {
                    new_data.push(data[idx].clone());
                    new_nulls.push(nulls.is_null(idx));
                }
                Column::Array {
                    data: new_data,
                    nulls: new_nulls,
                    element_type: element_type.clone(),
                }
            }
            Column::Struct {
                data,
                nulls,
                fields,
            } => {
                let mut new_data = Vec::with_capacity(indices.len());
                let mut new_nulls = NullBitmap::new();
                for &idx in indices {
                    new_data.push(data[idx].clone());
                    new_nulls.push(nulls.is_null(idx));
                }
                Column::Struct {
                    data: new_data,
                    nulls: new_nulls,
                    fields: fields.clone(),
                }
            }
            Column::Geography { data, nulls } => {
                let mut new_data = Vec::with_capacity(indices.len());
                let mut new_nulls = NullBitmap::new();
                for &idx in indices {
                    new_data.push(data[idx].clone());
                    new_nulls.push(nulls.is_null(idx));
                }
                Column::Geography {
                    data: new_data,
                    nulls: new_nulls,
                }
            }
            Column::Interval { data, nulls } => {
                let mut new_data = Vec::with_capacity(indices.len());
                let mut new_nulls = NullBitmap::new();
                for &idx in indices {
                    new_data.push(data[idx].clone());
                    new_nulls.push(nulls.is_null(idx));
                }
                Column::Interval {
                    data: new_data,
                    nulls: new_nulls,
                }
            }
            Column::Range {
                data,
                nulls,
                element_type,
            } => {
                let mut new_data = Vec::with_capacity(indices.len());
                let mut new_nulls = NullBitmap::new();
                for &idx in indices {
                    new_data.push(data[idx].clone());
                    new_nulls.push(nulls.is_null(idx));
                }
                Column::Range {
                    data: new_data,
                    nulls: new_nulls,
                    element_type: element_type.clone(),
                }
            }
        })
    }

    pub fn filter_by_mask(&self, mask: &Column) -> Result<Self> {
        let Column::Bool {
            data: mask_data,
            nulls: mask_nulls,
        } = mask
        else {
            return Err(Error::internal(
                "filter_by_mask requires a Bool column as mask",
            ));
        };

        let mut indices = Vec::new();
        for (i, &val) in mask_data.iter().enumerate() {
            if val && !mask_nulls.is_null(i) {
                indices.push(i);
            }
        }
        self.gather(&indices)
    }

    pub fn from_values(values: &[Value]) -> Self {
        if values.is_empty() {
            return Column::new(&yachtsql_common::types::DataType::Unknown);
        }

        let first_non_null = values.iter().find(|v| !matches!(v, Value::Null));
        let data_type = first_non_null
            .map(|v| v.data_type())
            .unwrap_or(yachtsql_common::types::DataType::Int64);

        let mut column = Column::new(&data_type);
        for value in values {
            let _ = column.push(value.clone());
        }
        column
    }

    pub fn extend(&mut self, other: &Column) -> Result<()> {
        match (self, other) {
            (
                Column::Bool {
                    data: d1,
                    nulls: n1,
                },
                Column::Bool {
                    data: d2,
                    nulls: n2,
                },
            ) => {
                d1.extend_from_slice(d2);
                n1.extend(n2);
            }
            (
                Column::Int64 {
                    data: d1,
                    nulls: n1,
                },
                Column::Int64 {
                    data: d2,
                    nulls: n2,
                },
            ) => {
                for &v in d2.as_slice() {
                    d1.push(v);
                }
                n1.extend(n2);
            }
            (
                Column::Float64 {
                    data: d1,
                    nulls: n1,
                },
                Column::Float64 {
                    data: d2,
                    nulls: n2,
                },
            ) => {
                for &v in d2.as_slice() {
                    d1.push(v);
                }
                n1.extend(n2);
            }
            (
                Column::Numeric {
                    data: d1,
                    nulls: n1,
                },
                Column::Numeric {
                    data: d2,
                    nulls: n2,
                },
            ) => {
                d1.extend_from_slice(d2);
                n1.extend(n2);
            }
            (
                Column::String {
                    data: d1,
                    nulls: n1,
                },
                Column::String {
                    data: d2,
                    nulls: n2,
                },
            ) => {
                d1.extend(d2.iter().cloned());
                n1.extend(n2);
            }
            (
                Column::Bytes {
                    data: d1,
                    nulls: n1,
                },
                Column::Bytes {
                    data: d2,
                    nulls: n2,
                },
            ) => {
                d1.extend(d2.iter().cloned());
                n1.extend(n2);
            }
            (
                Column::Date {
                    data: d1,
                    nulls: n1,
                },
                Column::Date {
                    data: d2,
                    nulls: n2,
                },
            ) => {
                d1.extend_from_slice(d2);
                n1.extend(n2);
            }
            (
                Column::Time {
                    data: d1,
                    nulls: n1,
                },
                Column::Time {
                    data: d2,
                    nulls: n2,
                },
            ) => {
                d1.extend_from_slice(d2);
                n1.extend(n2);
            }
            (
                Column::DateTime {
                    data: d1,
                    nulls: n1,
                },
                Column::DateTime {
                    data: d2,
                    nulls: n2,
                },
            ) => {
                d1.extend_from_slice(d2);
                n1.extend(n2);
            }
            (
                Column::Timestamp {
                    data: d1,
                    nulls: n1,
                },
                Column::Timestamp {
                    data: d2,
                    nulls: n2,
                },
            ) => {
                d1.extend_from_slice(d2);
                n1.extend(n2);
            }
            (
                Column::Json {
                    data: d1,
                    nulls: n1,
                },
                Column::Json {
                    data: d2,
                    nulls: n2,
                },
            ) => {
                d1.extend(d2.iter().cloned());
                n1.extend(n2);
            }
            (
                Column::Array {
                    data: d1,
                    nulls: n1,
                    ..
                },
                Column::Array {
                    data: d2,
                    nulls: n2,
                    ..
                },
            ) => {
                d1.extend(d2.iter().cloned());
                n1.extend(n2);
            }
            (
                Column::Struct {
                    data: d1,
                    nulls: n1,
                    ..
                },
                Column::Struct {
                    data: d2,
                    nulls: n2,
                    ..
                },
            ) => {
                d1.extend(d2.iter().cloned());
                n1.extend(n2);
            }
            (
                Column::Geography {
                    data: d1,
                    nulls: n1,
                },
                Column::Geography {
                    data: d2,
                    nulls: n2,
                },
            ) => {
                d1.extend(d2.iter().cloned());
                n1.extend(n2);
            }
            (
                Column::Interval {
                    data: d1,
                    nulls: n1,
                },
                Column::Interval {
                    data: d2,
                    nulls: n2,
                },
            ) => {
                d1.extend_from_slice(d2);
                n1.extend(n2);
            }
            (
                Column::Range {
                    data: d1,
                    nulls: n1,
                    ..
                },
                Column::Range {
                    data: d2,
                    nulls: n2,
                    ..
                },
            ) => {
                d1.extend(d2.iter().cloned());
                n1.extend(n2);
            }
            (s, o) => {
                return Err(Error::internal(format!(
                    "Cannot extend columns of different types: {:?} and {:?}",
                    s.data_type(),
                    o.data_type()
                )));
            }
        }
        Ok(())
    }
}

impl Column {
    fn validate_binary_op_lengths(&self, other: &Column, op_name: &str) -> Result<()> {
        if self.len() != other.len() {
            return Err(Error::internal(format!(
                "{}: column length mismatch ({} vs {})",
                op_name,
                self.len(),
                other.len()
            )));
        }
        Ok(())
    }

    pub fn binary_add(&self, other: &Column) -> Result<Self> {
        self.validate_binary_op_lengths(other, "binary_add")?;
        Ok(match (self, other) {
            (Column::Int64 { data: l, nulls: ln }, Column::Int64 { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = AVec::with_capacity(64, len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(0);
                        result_nulls.push(true);
                    } else {
                        match l[i].checked_add(r[i]) {
                            Some(v) => {
                                result_data.push(v);
                                result_nulls.push(false);
                            }
                            None => {
                                result_data.push(0);
                                result_nulls.push(true);
                            }
                        }
                    }
                }
                Column::Int64 {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Float64 { data: l, nulls: ln }, Column::Float64 { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = AVec::with_capacity(64, len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(0.0);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] + r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Float64 {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Numeric { data: l, nulls: ln }, Column::Numeric { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(rust_decimal::Decimal::ZERO);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] + r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Numeric {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            _ => {
                return Err(Error::internal(format!(
                    "binary_add: incompatible column types {:?} and {:?}",
                    self.data_type(),
                    other.data_type()
                )));
            }
        })
    }

    pub fn binary_sub(&self, other: &Column) -> Result<Self> {
        self.validate_binary_op_lengths(other, "binary_sub")?;
        Ok(match (self, other) {
            (Column::Int64 { data: l, nulls: ln }, Column::Int64 { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = AVec::with_capacity(64, len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(0);
                        result_nulls.push(true);
                    } else {
                        match l[i].checked_sub(r[i]) {
                            Some(v) => {
                                result_data.push(v);
                                result_nulls.push(false);
                            }
                            None => {
                                result_data.push(0);
                                result_nulls.push(true);
                            }
                        }
                    }
                }
                Column::Int64 {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Float64 { data: l, nulls: ln }, Column::Float64 { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = AVec::with_capacity(64, len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(0.0);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] - r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Float64 {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Numeric { data: l, nulls: ln }, Column::Numeric { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(rust_decimal::Decimal::ZERO);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] - r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Numeric {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            _ => {
                return Err(Error::internal(format!(
                    "binary_sub: incompatible column types {:?} and {:?}",
                    self.data_type(),
                    other.data_type()
                )));
            }
        })
    }

    pub fn binary_mul(&self, other: &Column) -> Result<Self> {
        self.validate_binary_op_lengths(other, "binary_mul")?;
        Ok(match (self, other) {
            (Column::Int64 { data: l, nulls: ln }, Column::Int64 { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = AVec::with_capacity(64, len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(0);
                        result_nulls.push(true);
                    } else {
                        match l[i].checked_mul(r[i]) {
                            Some(v) => {
                                result_data.push(v);
                                result_nulls.push(false);
                            }
                            None => {
                                result_data.push(0);
                                result_nulls.push(true);
                            }
                        }
                    }
                }
                Column::Int64 {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Float64 { data: l, nulls: ln }, Column::Float64 { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = AVec::with_capacity(64, len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(0.0);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] * r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Float64 {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Numeric { data: l, nulls: ln }, Column::Numeric { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(rust_decimal::Decimal::ZERO);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] * r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Numeric {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            _ => {
                return Err(Error::internal(format!(
                    "binary_mul: incompatible column types {:?} and {:?}",
                    self.data_type(),
                    other.data_type()
                )));
            }
        })
    }

    pub fn binary_div(&self, other: &Column) -> Result<Self> {
        self.validate_binary_op_lengths(other, "binary_div")?;
        Ok(match (self, other) {
            (Column::Int64 { data: l, nulls: ln }, Column::Int64 { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = AVec::with_capacity(64, len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) || r[i] == 0 {
                        result_data.push(0);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] / r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Int64 {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Float64 { data: l, nulls: ln }, Column::Float64 { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = AVec::with_capacity(64, len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(0.0);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] / r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Float64 {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Numeric { data: l, nulls: ln }, Column::Numeric { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) || r[i].is_zero() {
                        result_data.push(rust_decimal::Decimal::ZERO);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] / r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Numeric {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            _ => {
                return Err(Error::internal(format!(
                    "binary_div: incompatible column types {:?} and {:?}",
                    self.data_type(),
                    other.data_type()
                )));
            }
        })
    }

    pub fn binary_eq(&self, other: &Column) -> Result<Self> {
        self.validate_binary_op_lengths(other, "binary_eq")?;
        Ok(match (self, other) {
            (Column::Int64 { data: l, nulls: ln }, Column::Int64 { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] == r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Float64 { data: l, nulls: ln }, Column::Float64 { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] == r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::String { data: l, nulls: ln }, Column::String { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] == r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Bool { data: l, nulls: ln }, Column::Bool { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] == r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Bytes { data: l, nulls: ln }, Column::Bytes { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] == r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Interval { data: l, nulls: ln }, Column::Interval { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] == r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Date { data: l, nulls: ln }, Column::Date { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] == r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Time { data: l, nulls: ln }, Column::Time { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] == r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::DateTime { data: l, nulls: ln }, Column::DateTime { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] == r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (
                Column::Timestamp { data: l, nulls: ln },
                Column::Timestamp { data: r, nulls: rn },
            ) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] == r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            _ => {
                return Err(Error::internal(format!(
                    "binary_eq: incompatible column types {:?} and {:?}",
                    self.data_type(),
                    other.data_type()
                )));
            }
        })
    }

    pub fn binary_ne(&self, other: &Column) -> Result<Self> {
        let eq_result = self.binary_eq(other)?;
        eq_result.unary_not()
    }

    pub fn binary_lt(&self, other: &Column) -> Result<Self> {
        self.validate_binary_op_lengths(other, "binary_lt")?;
        Ok(match (self, other) {
            (Column::Int64 { data: l, nulls: ln }, Column::Int64 { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] < r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Float64 { data: l, nulls: ln }, Column::Float64 { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] < r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::String { data: l, nulls: ln }, Column::String { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] < r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Bytes { data: l, nulls: ln }, Column::Bytes { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] < r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Numeric { data: l, nulls: ln }, Column::Numeric { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] < r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Interval { data: l, nulls: ln }, Column::Interval { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        let lt = (l[i].months, l[i].days, l[i].nanos)
                            < (r[i].months, r[i].days, r[i].nanos);
                        result_data.push(lt);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Date { data: l, nulls: ln }, Column::Date { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] < r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Time { data: l, nulls: ln }, Column::Time { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] < r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::DateTime { data: l, nulls: ln }, Column::DateTime { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] < r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (
                Column::Timestamp { data: l, nulls: ln },
                Column::Timestamp { data: r, nulls: rn },
            ) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] < r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            _ => {
                return Err(Error::internal(format!(
                    "binary_lt: incompatible column types {:?} and {:?}",
                    self.data_type(),
                    other.data_type()
                )));
            }
        })
    }

    pub fn binary_le(&self, other: &Column) -> Result<Self> {
        self.validate_binary_op_lengths(other, "binary_le")?;
        Ok(match (self, other) {
            (Column::Int64 { data: l, nulls: ln }, Column::Int64 { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] <= r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Float64 { data: l, nulls: ln }, Column::Float64 { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] <= r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::String { data: l, nulls: ln }, Column::String { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] <= r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Bytes { data: l, nulls: ln }, Column::Bytes { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] <= r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Numeric { data: l, nulls: ln }, Column::Numeric { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] <= r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Interval { data: l, nulls: ln }, Column::Interval { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        let le = (l[i].months, l[i].days, l[i].nanos)
                            <= (r[i].months, r[i].days, r[i].nanos);
                        result_data.push(le);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Date { data: l, nulls: ln }, Column::Date { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] <= r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::Time { data: l, nulls: ln }, Column::Time { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] <= r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (Column::DateTime { data: l, nulls: ln }, Column::DateTime { data: r, nulls: rn }) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] <= r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            (
                Column::Timestamp { data: l, nulls: ln },
                Column::Timestamp { data: r, nulls: rn },
            ) => {
                let len = l.len();
                let mut result_data = Vec::with_capacity(len);
                let mut result_nulls = NullBitmap::new();
                for i in 0..len {
                    if ln.is_null(i) || rn.is_null(i) {
                        result_data.push(false);
                        result_nulls.push(true);
                    } else {
                        result_data.push(l[i] <= r[i]);
                        result_nulls.push(false);
                    }
                }
                Column::Bool {
                    data: result_data,
                    nulls: result_nulls,
                }
            }
            _ => {
                return Err(Error::internal(format!(
                    "binary_le: incompatible column types {:?} and {:?}",
                    self.data_type(),
                    other.data_type()
                )));
            }
        })
    }

    pub fn binary_gt(&self, other: &Column) -> Result<Self> {
        other.binary_lt(self)
    }

    pub fn binary_ge(&self, other: &Column) -> Result<Self> {
        other.binary_le(self)
    }

    pub fn binary_and(&self, other: &Column) -> Result<Self> {
        self.validate_binary_op_lengths(other, "binary_and")?;
        let (left, right) = match (self, other) {
            (Column::Bool { data: l, nulls: ln }, Column::Bool { data: r, nulls: rn }) => {
                (Some((l, ln)), Some((r, rn)))
            }
            (Column::Bool { data: l, nulls: ln }, _) if other.is_all_null() => {
                (Some((l, ln)), None)
            }
            (_, Column::Bool { data: r, nulls: rn }) if self.is_all_null() => (None, Some((r, rn))),
            (_, _) if self.is_all_null() && other.is_all_null() => (None, None),
            _ => {
                return Err(Error::internal(format!(
                    "binary_and: requires Bool columns, got {:?} and {:?}",
                    self.data_type(),
                    other.data_type()
                )));
            }
        };

        let len = self.len();
        let mut result_data = Vec::with_capacity(len);
        let mut result_nulls = NullBitmap::new();

        for i in 0..len {
            let (l_null, l_val) = match left {
                Some((data, nulls)) => (nulls.is_null(i), data[i]),
                None => (true, false),
            };
            let (r_null, r_val) = match right {
                Some((data, nulls)) => (nulls.is_null(i), data[i]),
                None => (true, false),
            };

            if l_null && r_null {
                result_data.push(false);
                result_nulls.push(true);
            } else if (!l_null && !l_val) || (!r_null && !r_val) {
                result_data.push(false);
                result_nulls.push(false);
            } else if l_null || r_null {
                result_data.push(false);
                result_nulls.push(true);
            } else {
                result_data.push(l_val && r_val);
                result_nulls.push(false);
            }
        }

        Ok(Column::Bool {
            data: result_data,
            nulls: result_nulls,
        })
    }

    pub fn binary_or(&self, other: &Column) -> Result<Self> {
        self.validate_binary_op_lengths(other, "binary_or")?;
        let (left, right) = match (self, other) {
            (Column::Bool { data: l, nulls: ln }, Column::Bool { data: r, nulls: rn }) => {
                (Some((l, ln)), Some((r, rn)))
            }
            (Column::Bool { data: l, nulls: ln }, _) if other.is_all_null() => {
                (Some((l, ln)), None)
            }
            (_, Column::Bool { data: r, nulls: rn }) if self.is_all_null() => (None, Some((r, rn))),
            (_, _) if self.is_all_null() && other.is_all_null() => (None, None),
            _ => {
                return Err(Error::internal(format!(
                    "binary_or: requires Bool columns, got {:?} and {:?}",
                    self.data_type(),
                    other.data_type()
                )));
            }
        };

        let len = self.len();
        let mut result_data = Vec::with_capacity(len);
        let mut result_nulls = NullBitmap::new();

        for i in 0..len {
            let (l_null, l_val) = match left {
                Some((data, nulls)) => (nulls.is_null(i), data[i]),
                None => (true, false),
            };
            let (r_null, r_val) = match right {
                Some((data, nulls)) => (nulls.is_null(i), data[i]),
                None => (true, false),
            };

            if l_null && r_null {
                result_data.push(false);
                result_nulls.push(true);
            } else if (!l_null && l_val) || (!r_null && r_val) {
                result_data.push(true);
                result_nulls.push(false);
            } else if l_null || r_null {
                result_data.push(false);
                result_nulls.push(true);
            } else {
                result_data.push(l_val || r_val);
                result_nulls.push(false);
            }
        }

        Ok(Column::Bool {
            data: result_data,
            nulls: result_nulls,
        })
    }

    pub fn unary_not(&self) -> Result<Self> {
        match self {
            Column::Bool { data, nulls } => {
                let result_data: Vec<bool> = data.iter().map(|v| !v).collect();
                Ok(Column::Bool {
                    data: result_data,
                    nulls: nulls.clone(),
                })
            }
            _ => Err(Error::type_mismatch_msg("unary_not: requires Bool column")),
        }
    }

    pub fn unary_neg(&self) -> Result<Self> {
        Ok(match self {
            Column::Int64 { data, nulls } => {
                let mut result_data = AVec::with_capacity(64, data.len());
                for v in data.iter() {
                    result_data.push(-v);
                }
                Column::Int64 {
                    data: result_data,
                    nulls: nulls.clone(),
                }
            }
            Column::Float64 { data, nulls } => {
                let mut result_data = AVec::with_capacity(64, data.len());
                for v in data.iter() {
                    result_data.push(-v);
                }
                Column::Float64 {
                    data: result_data,
                    nulls: nulls.clone(),
                }
            }
            Column::Numeric { data, nulls } => {
                let result_data: Vec<_> = data.iter().map(|v| -v).collect();
                Column::Numeric {
                    data: result_data,
                    nulls: nulls.clone(),
                }
            }
            _ => {
                return Err(Error::internal(format!(
                    "unary_neg: requires numeric column, got {:?}",
                    self.data_type()
                )));
            }
        })
    }

    pub fn is_null_mask(&self) -> Self {
        let len = self.len();
        let mut result_data = Vec::with_capacity(len);
        let result_nulls = NullBitmap::new_valid(len);
        for i in 0..len {
            result_data.push(self.is_null(i));
        }
        Column::Bool {
            data: result_data,
            nulls: result_nulls,
        }
    }

    pub fn is_not_null_mask(&self) -> Self {
        let len = self.len();
        let mut result_data = Vec::with_capacity(len);
        let result_nulls = NullBitmap::new_valid(len);
        for i in 0..len {
            result_data.push(!self.is_null(i));
        }
        Column::Bool {
            data: result_data,
            nulls: result_nulls,
        }
    }

    pub fn coerce_to_type(&self, target_type: &yachtsql_common::types::DataType) -> Self {
        use yachtsql_common::types::DataType;

        if self.data_type() == *target_type {
            return self.clone();
        }

        let self_type = self.data_type();
        let compatible = match (&self_type, target_type) {
            (DataType::Numeric(_), DataType::Numeric(_)) => true,
            (DataType::Array(a), DataType::Array(b)) => a == b,
            (DataType::Struct(a), DataType::Struct(b)) => a == b,
            (DataType::Range(a), DataType::Range(b)) => a == b,
            _ => false,
        };

        if compatible {
            return self.clone();
        }

        let len = self.len();
        let mut result = Column::new(target_type);
        for i in 0..len {
            let value = self.get_value(i);
            let _ = result.push(value);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
    use rust_decimal::Decimal;
    use yachtsql_common::types::{DataType, IntervalValue, RangeValue};

    use super::*;

    #[test]
    fn test_broadcast_int64() {
        let col = Column::broadcast(Value::Int64(42), 5);
        assert_eq!(col.len(), 5);
        for i in 0..5 {
            assert_eq!(col.get_value(i), Value::Int64(42));
        }
    }

    #[test]
    fn test_broadcast_null() {
        let col = Column::broadcast(Value::Null, 3);
        assert_eq!(col.len(), 3);
        for i in 0..3 {
            assert_eq!(col.get_value(i), Value::Null);
        }
    }

    #[test]
    fn test_broadcast_empty() {
        let col = Column::broadcast(Value::Int64(42), 0);
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_broadcast_bool() {
        let col = Column::broadcast(Value::Bool(true), 3);
        assert_eq!(col.len(), 3);
        for i in 0..3 {
            assert_eq!(col.get_value(i), Value::Bool(true));
        }
    }

    #[test]
    fn test_broadcast_float64() {
        let col = Column::broadcast(Value::float64(3.15), 2);
        assert_eq!(col.len(), 2);
        for i in 0..2 {
            assert_eq!(col.get_value(i), Value::float64(3.15));
        }
    }

    #[test]
    fn test_broadcast_numeric() {
        let col = Column::broadcast(Value::Numeric(Decimal::new(123, 2)), 2);
        assert_eq!(col.len(), 2);
        for i in 0..2 {
            assert_eq!(col.get_value(i), Value::Numeric(Decimal::new(123, 2)));
        }
    }

    #[test]
    fn test_broadcast_bignumeric() {
        let col = Column::broadcast(Value::BigNumeric(Decimal::new(999, 3)), 2);
        assert_eq!(col.len(), 2);
        for i in 0..2 {
            assert_eq!(col.get_value(i), Value::Numeric(Decimal::new(999, 3)));
        }
    }

    #[test]
    fn test_broadcast_string() {
        let col = Column::broadcast(Value::String("hello".to_string()), 3);
        assert_eq!(col.len(), 3);
        for i in 0..3 {
            assert_eq!(col.get_value(i), Value::String("hello".to_string()));
        }
    }

    #[test]
    fn test_broadcast_bytes() {
        let col = Column::broadcast(Value::Bytes(vec![1, 2, 3]), 2);
        assert_eq!(col.len(), 2);
        for i in 0..2 {
            assert_eq!(col.get_value(i), Value::Bytes(vec![1, 2, 3]));
        }
    }

    #[test]
    fn test_broadcast_date() {
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let col = Column::broadcast(Value::Date(date), 2);
        assert_eq!(col.len(), 2);
        for i in 0..2 {
            assert_eq!(col.get_value(i), Value::Date(date));
        }
    }

    #[test]
    fn test_broadcast_time() {
        let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let col = Column::broadcast(Value::Time(time), 2);
        assert_eq!(col.len(), 2);
        for i in 0..2 {
            assert_eq!(col.get_value(i), Value::Time(time));
        }
    }

    #[test]
    fn test_broadcast_datetime() {
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 30, 0)
            .unwrap();
        let col = Column::broadcast(Value::DateTime(dt), 2);
        assert_eq!(col.len(), 2);
        for i in 0..2 {
            assert_eq!(col.get_value(i), Value::DateTime(dt));
        }
    }

    #[test]
    fn test_broadcast_timestamp() {
        let ts = Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 0).unwrap();
        let col = Column::broadcast(Value::Timestamp(ts), 2);
        assert_eq!(col.len(), 2);
        for i in 0..2 {
            assert_eq!(col.get_value(i), Value::Timestamp(ts));
        }
    }

    #[test]
    fn test_broadcast_json() {
        let json = serde_json::json!({"key": "value"});
        let col = Column::broadcast(Value::Json(json.clone()), 2);
        assert_eq!(col.len(), 2);
        for i in 0..2 {
            assert_eq!(col.get_value(i), Value::Json(json.clone()));
        }
    }

    #[test]
    fn test_broadcast_array_empty() {
        let col = Column::broadcast(Value::Array(vec![]), 2);
        assert_eq!(col.len(), 2);
        for i in 0..2 {
            assert_eq!(col.get_value(i), Value::Array(vec![]));
        }
    }

    #[test]
    fn test_broadcast_array_non_empty() {
        let arr = vec![Value::Int64(1), Value::Int64(2)];
        let col = Column::broadcast(Value::Array(arr.clone()), 2);
        assert_eq!(col.len(), 2);
        for i in 0..2 {
            assert_eq!(col.get_value(i), Value::Array(arr.clone()));
        }
    }

    #[test]
    fn test_broadcast_struct() {
        let s = vec![
            ("name".to_string(), Value::String("test".to_string())),
            ("age".to_string(), Value::Int64(25)),
        ];
        let col = Column::broadcast(Value::Struct(s.clone()), 2);
        assert_eq!(col.len(), 2);
        for i in 0..2 {
            assert_eq!(col.get_value(i), Value::Struct(s.clone()));
        }
    }

    #[test]
    fn test_broadcast_geography() {
        let col = Column::broadcast(Value::Geography("POINT(0 0)".to_string()), 2);
        assert_eq!(col.len(), 2);
        for i in 0..2 {
            assert_eq!(col.get_value(i), Value::Geography("POINT(0 0)".to_string()));
        }
    }

    #[test]
    fn test_broadcast_interval() {
        let interval = IntervalValue {
            months: 1,
            days: 2,
            nanos: 3000000000,
        };
        let col = Column::broadcast(Value::Interval(interval.clone()), 2);
        assert_eq!(col.len(), 2);
        for i in 0..2 {
            assert_eq!(col.get_value(i), Value::Interval(interval.clone()));
        }
    }

    #[test]
    fn test_broadcast_range() {
        let range = RangeValue::new(Some(Value::Int64(1)), Some(Value::Int64(10)));
        let col = Column::broadcast(Value::Range(range.clone()), 2);
        assert_eq!(col.len(), 2);
        for i in 0..2 {
            assert_eq!(col.get_value(i), Value::Range(range.clone()));
        }
    }

    #[test]
    fn test_broadcast_default() {
        let col = Column::broadcast(Value::Default, 3);
        assert_eq!(col.len(), 3);
        for i in 0..3 {
            assert_eq!(col.get_value(i), Value::Null);
        }
    }

    #[test]
    fn test_gather() {
        let col = Column::Int64 {
            data: AVec::from_iter(64, vec![10, 20, 30, 40, 50]),
            nulls: NullBitmap::new_valid(5),
        };
        let gathered = col.gather(&[4, 2, 0]).unwrap();
        assert_eq!(gathered.len(), 3);
        assert_eq!(gathered.get_value(0), Value::Int64(50));
        assert_eq!(gathered.get_value(1), Value::Int64(30));
        assert_eq!(gathered.get_value(2), Value::Int64(10));
    }

    #[test]
    fn test_gather_bool() {
        let col = Column::Bool {
            data: vec![true, false, true, false],
            nulls: NullBitmap::new_valid(4),
        };
        let gathered = col.gather(&[3, 1]).unwrap();
        assert_eq!(gathered.len(), 2);
        assert_eq!(gathered.get_value(0), Value::Bool(false));
        assert_eq!(gathered.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_gather_float64() {
        let col = Column::Float64 {
            data: AVec::from_iter(64, vec![1.1, 2.2, 3.3]),
            nulls: NullBitmap::new_valid(3),
        };
        let gathered = col.gather(&[2, 0]).unwrap();
        assert_eq!(gathered.len(), 2);
        assert_eq!(gathered.get_value(0), Value::float64(3.3));
        assert_eq!(gathered.get_value(1), Value::float64(1.1));
    }

    #[test]
    fn test_gather_numeric() {
        let col = Column::Numeric {
            data: vec![Decimal::new(100, 2), Decimal::new(200, 2)],
            nulls: NullBitmap::new_valid(2),
        };
        let gathered = col.gather(&[1, 0, 1]).unwrap();
        assert_eq!(gathered.len(), 3);
        assert_eq!(gathered.get_value(0), Value::Numeric(Decimal::new(200, 2)));
    }

    #[test]
    fn test_gather_string() {
        let col = Column::String {
            data: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            nulls: NullBitmap::new_valid(3),
        };
        let gathered = col.gather(&[2, 0]).unwrap();
        assert_eq!(gathered.len(), 2);
        assert_eq!(gathered.get_value(0), Value::String("c".to_string()));
        assert_eq!(gathered.get_value(1), Value::String("a".to_string()));
    }

    #[test]
    fn test_gather_bytes() {
        let col = Column::Bytes {
            data: vec![vec![1], vec![2], vec![3]],
            nulls: NullBitmap::new_valid(3),
        };
        let gathered = col.gather(&[1]).unwrap();
        assert_eq!(gathered.len(), 1);
        assert_eq!(gathered.get_value(0), Value::Bytes(vec![2]));
    }

    #[test]
    fn test_gather_date() {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 2, 1).unwrap();
        let col = Column::Date {
            data: vec![d1, d2],
            nulls: NullBitmap::new_valid(2),
        };
        let gathered = col.gather(&[1, 0]).unwrap();
        assert_eq!(gathered.get_value(0), Value::Date(d2));
        assert_eq!(gathered.get_value(1), Value::Date(d1));
    }

    #[test]
    fn test_gather_time() {
        let t1 = NaiveTime::from_hms_opt(10, 0, 0).unwrap();
        let t2 = NaiveTime::from_hms_opt(11, 0, 0).unwrap();
        let col = Column::Time {
            data: vec![t1, t2],
            nulls: NullBitmap::new_valid(2),
        };
        let gathered = col.gather(&[1]).unwrap();
        assert_eq!(gathered.get_value(0), Value::Time(t2));
    }

    #[test]
    fn test_gather_datetime() {
        let dt = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let col = Column::DateTime {
            data: vec![dt],
            nulls: NullBitmap::new_valid(1),
        };
        let gathered = col.gather(&[0]).unwrap();
        assert_eq!(gathered.get_value(0), Value::DateTime(dt));
    }

    #[test]
    fn test_gather_timestamp() {
        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let col = Column::Timestamp {
            data: vec![ts],
            nulls: NullBitmap::new_valid(1),
        };
        let gathered = col.gather(&[0]).unwrap();
        assert_eq!(gathered.get_value(0), Value::Timestamp(ts));
    }

    #[test]
    fn test_gather_json() {
        let j1 = serde_json::json!({"a": 1});
        let j2 = serde_json::json!({"b": 2});
        let col = Column::Json {
            data: vec![j1.clone(), j2.clone()],
            nulls: NullBitmap::new_valid(2),
        };
        let gathered = col.gather(&[1, 0]).unwrap();
        assert_eq!(gathered.get_value(0), Value::Json(j2));
        assert_eq!(gathered.get_value(1), Value::Json(j1));
    }

    #[test]
    fn test_gather_array() {
        let arr1 = vec![Value::Int64(1)];
        let arr2 = vec![Value::Int64(2)];
        let col = Column::Array {
            data: vec![arr1.clone(), arr2.clone()],
            nulls: NullBitmap::new_valid(2),
            element_type: DataType::Int64,
        };
        let gathered = col.gather(&[1]).unwrap();
        assert_eq!(gathered.get_value(0), Value::Array(arr2));
    }

    #[test]
    fn test_gather_struct() {
        let s1 = vec![("x".to_string(), Value::Int64(1))];
        let s2 = vec![("x".to_string(), Value::Int64(2))];
        let col = Column::Struct {
            data: vec![s1.clone(), s2.clone()],
            nulls: NullBitmap::new_valid(2),
            fields: vec![("x".to_string(), DataType::Int64)],
        };
        let gathered = col.gather(&[0, 1]).unwrap();
        assert_eq!(gathered.get_value(0), Value::Struct(s1));
        assert_eq!(gathered.get_value(1), Value::Struct(s2));
    }

    #[test]
    fn test_gather_geography() {
        let col = Column::Geography {
            data: vec!["POINT(0 0)".to_string(), "POINT(1 1)".to_string()],
            nulls: NullBitmap::new_valid(2),
        };
        let gathered = col.gather(&[1]).unwrap();
        assert_eq!(
            gathered.get_value(0),
            Value::Geography("POINT(1 1)".to_string())
        );
    }

    #[test]
    fn test_gather_interval() {
        let i1 = IntervalValue {
            months: 1,
            days: 0,
            nanos: 0,
        };
        let i2 = IntervalValue {
            months: 2,
            days: 0,
            nanos: 0,
        };
        let col = Column::Interval {
            data: vec![i1.clone(), i2.clone()],
            nulls: NullBitmap::new_valid(2),
        };
        let gathered = col.gather(&[1, 0]).unwrap();
        assert_eq!(gathered.get_value(0), Value::Interval(i2));
        assert_eq!(gathered.get_value(1), Value::Interval(i1));
    }

    #[test]
    fn test_gather_range() {
        let r1 = RangeValue::new(Some(Value::Int64(1)), Some(Value::Int64(5)));
        let r2 = RangeValue::new(Some(Value::Int64(10)), Some(Value::Int64(20)));
        let col = Column::Range {
            data: vec![r1.clone(), r2.clone()],
            nulls: NullBitmap::new_valid(2),
            element_type: DataType::Int64,
        };
        let gathered = col.gather(&[1]).unwrap();
        assert_eq!(gathered.get_value(0), Value::Range(r2));
    }

    #[test]
    fn test_gather_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(false);
        nulls.push(true);
        nulls.push(false);
        let col = Column::Int64 {
            data: AVec::from_iter(64, vec![10, 0, 30]),
            nulls,
        };
        let gathered = col.gather(&[1, 2, 0]).unwrap();
        assert_eq!(gathered.get_value(0), Value::Null);
        assert_eq!(gathered.get_value(1), Value::Int64(30));
        assert_eq!(gathered.get_value(2), Value::Int64(10));
    }

    #[test]
    fn test_filter_by_mask() {
        let col = Column::Int64 {
            data: AVec::from_iter(64, vec![10, 20, 30, 40, 50]),
            nulls: NullBitmap::new_valid(5),
        };
        let mask = Column::Bool {
            data: vec![true, false, true, false, true],
            nulls: NullBitmap::new_valid(5),
        };
        let filtered = col.filter_by_mask(&mask).unwrap();
        assert_eq!(filtered.len(), 3);
        assert_eq!(filtered.get_value(0), Value::Int64(10));
        assert_eq!(filtered.get_value(1), Value::Int64(30));
        assert_eq!(filtered.get_value(2), Value::Int64(50));
    }

    #[test]
    fn test_filter_by_mask_with_null_mask() {
        let col = Column::Int64 {
            data: AVec::from_iter(64, vec![10, 20, 30]),
            nulls: NullBitmap::new_valid(3),
        };
        let mut mask_nulls = NullBitmap::new();
        mask_nulls.push(false);
        mask_nulls.push(true);
        mask_nulls.push(false);
        let mask = Column::Bool {
            data: vec![true, true, false],
            nulls: mask_nulls,
        };
        let filtered = col.filter_by_mask(&mask).unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered.get_value(0), Value::Int64(10));
    }

    #[test]
    fn test_filter_by_mask_non_bool_error() {
        let col = Column::Int64 {
            data: AVec::from_iter(64, vec![1, 2, 3]),
            nulls: NullBitmap::new_valid(3),
        };
        let bad_mask = Column::Int64 {
            data: AVec::from_iter(64, vec![1, 0, 1]),
            nulls: NullBitmap::new_valid(3),
        };
        let result = col.filter_by_mask(&bad_mask);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("filter_by_mask requires a Bool column as mask")
        );
    }

    #[test]
    fn test_from_values() {
        let values = vec![
            Value::Int64(1),
            Value::Int64(2),
            Value::Null,
            Value::Int64(4),
        ];
        let col = Column::from_values(&values);
        assert_eq!(col.len(), 4);
        assert_eq!(col.get_value(0), Value::Int64(1));
        assert_eq!(col.get_value(1), Value::Int64(2));
        assert_eq!(col.get_value(2), Value::Null);
        assert_eq!(col.get_value(3), Value::Int64(4));
    }

    #[test]
    fn test_from_values_empty() {
        let values: Vec<Value> = vec![];
        let col = Column::from_values(&values);
        assert_eq!(col.len(), 0);
    }

    #[test]
    fn test_from_values_all_null() {
        let values = vec![Value::Null, Value::Null, Value::Null];
        let col = Column::from_values(&values);
        assert_eq!(col.len(), 3);
        for i in 0..3 {
            assert_eq!(col.get_value(i), Value::Null);
        }
    }

    #[test]
    fn test_from_values_first_null() {
        let values = vec![Value::Null, Value::String("test".to_string())];
        let col = Column::from_values(&values);
        assert_eq!(col.len(), 2);
        assert_eq!(col.get_value(0), Value::Null);
        assert_eq!(col.get_value(1), Value::String("test".to_string()));
    }

    #[test]
    fn test_extend_bool() {
        let mut col1 = Column::Bool {
            data: vec![true, false],
            nulls: NullBitmap::new_valid(2),
        };
        let col2 = Column::Bool {
            data: vec![false, true],
            nulls: NullBitmap::new_valid(2),
        };
        col1.extend(&col2).unwrap();
        assert_eq!(col1.len(), 4);
        assert_eq!(col1.get_value(2), Value::Bool(false));
        assert_eq!(col1.get_value(3), Value::Bool(true));
    }

    #[test]
    fn test_extend_int64() {
        let mut col1 = Column::Int64 {
            data: AVec::from_iter(64, vec![1, 2]),
            nulls: NullBitmap::new_valid(2),
        };
        let col2 = Column::Int64 {
            data: AVec::from_iter(64, vec![3, 4]),
            nulls: NullBitmap::new_valid(2),
        };
        col1.extend(&col2).unwrap();
        assert_eq!(col1.len(), 4);
        assert_eq!(col1.get_value(2), Value::Int64(3));
        assert_eq!(col1.get_value(3), Value::Int64(4));
    }

    #[test]
    fn test_extend_float64() {
        let mut col1 = Column::Float64 {
            data: AVec::from_iter(64, vec![1.0, 2.0]),
            nulls: NullBitmap::new_valid(2),
        };
        let col2 = Column::Float64 {
            data: AVec::from_iter(64, vec![3.0]),
            nulls: NullBitmap::new_valid(1),
        };
        col1.extend(&col2).unwrap();
        assert_eq!(col1.len(), 3);
        assert_eq!(col1.get_value(2), Value::float64(3.0));
    }

    #[test]
    fn test_extend_numeric() {
        let mut col1 = Column::Numeric {
            data: vec![Decimal::new(100, 2)],
            nulls: NullBitmap::new_valid(1),
        };
        let col2 = Column::Numeric {
            data: vec![Decimal::new(200, 2)],
            nulls: NullBitmap::new_valid(1),
        };
        col1.extend(&col2).unwrap();
        assert_eq!(col1.len(), 2);
        assert_eq!(col1.get_value(1), Value::Numeric(Decimal::new(200, 2)));
    }

    #[test]
    fn test_extend_string() {
        let mut col1 = Column::String {
            data: vec!["a".to_string()],
            nulls: NullBitmap::new_valid(1),
        };
        let col2 = Column::String {
            data: vec!["b".to_string()],
            nulls: NullBitmap::new_valid(1),
        };
        col1.extend(&col2).unwrap();
        assert_eq!(col1.len(), 2);
        assert_eq!(col1.get_value(1), Value::String("b".to_string()));
    }

    #[test]
    fn test_extend_bytes() {
        let mut col1 = Column::Bytes {
            data: vec![vec![1]],
            nulls: NullBitmap::new_valid(1),
        };
        let col2 = Column::Bytes {
            data: vec![vec![2]],
            nulls: NullBitmap::new_valid(1),
        };
        col1.extend(&col2).unwrap();
        assert_eq!(col1.len(), 2);
        assert_eq!(col1.get_value(1), Value::Bytes(vec![2]));
    }

    #[test]
    fn test_extend_date() {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 2, 1).unwrap();
        let mut col1 = Column::Date {
            data: vec![d1],
            nulls: NullBitmap::new_valid(1),
        };
        let col2 = Column::Date {
            data: vec![d2],
            nulls: NullBitmap::new_valid(1),
        };
        col1.extend(&col2).unwrap();
        assert_eq!(col1.len(), 2);
        assert_eq!(col1.get_value(1), Value::Date(d2));
    }

    #[test]
    fn test_extend_time() {
        let t1 = NaiveTime::from_hms_opt(10, 0, 0).unwrap();
        let t2 = NaiveTime::from_hms_opt(11, 0, 0).unwrap();
        let mut col1 = Column::Time {
            data: vec![t1],
            nulls: NullBitmap::new_valid(1),
        };
        let col2 = Column::Time {
            data: vec![t2],
            nulls: NullBitmap::new_valid(1),
        };
        col1.extend(&col2).unwrap();
        assert_eq!(col1.len(), 2);
        assert_eq!(col1.get_value(1), Value::Time(t2));
    }

    #[test]
    fn test_extend_datetime() {
        let dt1 = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let dt2 = NaiveDate::from_ymd_opt(2024, 2, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let mut col1 = Column::DateTime {
            data: vec![dt1],
            nulls: NullBitmap::new_valid(1),
        };
        let col2 = Column::DateTime {
            data: vec![dt2],
            nulls: NullBitmap::new_valid(1),
        };
        col1.extend(&col2).unwrap();
        assert_eq!(col1.len(), 2);
        assert_eq!(col1.get_value(1), Value::DateTime(dt2));
    }

    #[test]
    fn test_extend_timestamp() {
        let ts1 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap();
        let mut col1 = Column::Timestamp {
            data: vec![ts1],
            nulls: NullBitmap::new_valid(1),
        };
        let col2 = Column::Timestamp {
            data: vec![ts2],
            nulls: NullBitmap::new_valid(1),
        };
        col1.extend(&col2).unwrap();
        assert_eq!(col1.len(), 2);
        assert_eq!(col1.get_value(1), Value::Timestamp(ts2));
    }

    #[test]
    fn test_extend_json() {
        let j1 = serde_json::json!({"a": 1});
        let j2 = serde_json::json!({"b": 2});
        let mut col1 = Column::Json {
            data: vec![j1],
            nulls: NullBitmap::new_valid(1),
        };
        let col2 = Column::Json {
            data: vec![j2.clone()],
            nulls: NullBitmap::new_valid(1),
        };
        col1.extend(&col2).unwrap();
        assert_eq!(col1.len(), 2);
        assert_eq!(col1.get_value(1), Value::Json(j2));
    }

    #[test]
    fn test_extend_array() {
        let arr1 = vec![Value::Int64(1)];
        let arr2 = vec![Value::Int64(2)];
        let mut col1 = Column::Array {
            data: vec![arr1],
            nulls: NullBitmap::new_valid(1),
            element_type: DataType::Int64,
        };
        let col2 = Column::Array {
            data: vec![arr2.clone()],
            nulls: NullBitmap::new_valid(1),
            element_type: DataType::Int64,
        };
        col1.extend(&col2).unwrap();
        assert_eq!(col1.len(), 2);
        assert_eq!(col1.get_value(1), Value::Array(arr2));
    }

    #[test]
    fn test_extend_struct() {
        let s1 = vec![("x".to_string(), Value::Int64(1))];
        let s2 = vec![("x".to_string(), Value::Int64(2))];
        let mut col1 = Column::Struct {
            data: vec![s1],
            nulls: NullBitmap::new_valid(1),
            fields: vec![("x".to_string(), DataType::Int64)],
        };
        let col2 = Column::Struct {
            data: vec![s2.clone()],
            nulls: NullBitmap::new_valid(1),
            fields: vec![("x".to_string(), DataType::Int64)],
        };
        col1.extend(&col2).unwrap();
        assert_eq!(col1.len(), 2);
        assert_eq!(col1.get_value(1), Value::Struct(s2));
    }

    #[test]
    fn test_extend_geography() {
        let mut col1 = Column::Geography {
            data: vec!["POINT(0 0)".to_string()],
            nulls: NullBitmap::new_valid(1),
        };
        let col2 = Column::Geography {
            data: vec!["POINT(1 1)".to_string()],
            nulls: NullBitmap::new_valid(1),
        };
        col1.extend(&col2).unwrap();
        assert_eq!(col1.len(), 2);
        assert_eq!(
            col1.get_value(1),
            Value::Geography("POINT(1 1)".to_string())
        );
    }

    #[test]
    fn test_extend_interval() {
        let i1 = IntervalValue {
            months: 1,
            days: 0,
            nanos: 0,
        };
        let i2 = IntervalValue {
            months: 2,
            days: 0,
            nanos: 0,
        };
        let mut col1 = Column::Interval {
            data: vec![i1],
            nulls: NullBitmap::new_valid(1),
        };
        let col2 = Column::Interval {
            data: vec![i2.clone()],
            nulls: NullBitmap::new_valid(1),
        };
        col1.extend(&col2).unwrap();
        assert_eq!(col1.len(), 2);
        assert_eq!(col1.get_value(1), Value::Interval(i2));
    }

    #[test]
    fn test_extend_range() {
        let r1 = RangeValue::new(Some(Value::Int64(1)), Some(Value::Int64(5)));
        let r2 = RangeValue::new(Some(Value::Int64(10)), Some(Value::Int64(20)));
        let mut col1 = Column::Range {
            data: vec![r1],
            nulls: NullBitmap::new_valid(1),
            element_type: DataType::Int64,
        };
        let col2 = Column::Range {
            data: vec![r2.clone()],
            nulls: NullBitmap::new_valid(1),
            element_type: DataType::Int64,
        };
        col1.extend(&col2).unwrap();
        assert_eq!(col1.len(), 2);
        assert_eq!(col1.get_value(1), Value::Range(r2));
    }

    #[test]
    #[should_panic(expected = "Cannot extend columns of different types")]
    fn test_extend_type_mismatch() {
        let mut col1 = Column::Int64 {
            data: AVec::from_iter(64, vec![1]),
            nulls: NullBitmap::new_valid(1),
        };
        let col2 = Column::String {
            data: vec!["a".to_string()],
            nulls: NullBitmap::new_valid(1),
        };
        col1.extend(&col2).unwrap();
    }

    #[test]
    fn test_binary_add() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![1, 2, 3]),
            nulls: NullBitmap::new_valid(3),
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![10, 20, 30]),
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_add(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Int64(11));
        assert_eq!(result.get_value(1), Value::Int64(22));
        assert_eq!(result.get_value(2), Value::Int64(33));
    }

    #[test]
    fn test_binary_add_float64() {
        let a = Column::Float64 {
            data: AVec::from_iter(64, vec![1.5, 2.5]),
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Float64 {
            data: AVec::from_iter(64, vec![0.5, 1.5]),
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_add(&b).unwrap();
        assert_eq!(result.get_value(0), Value::float64(2.0));
        assert_eq!(result.get_value(1), Value::float64(4.0));
    }

    #[test]
    fn test_binary_add_numeric() {
        let a = Column::Numeric {
            data: vec![Decimal::new(100, 2), Decimal::new(200, 2)],
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Numeric {
            data: vec![Decimal::new(50, 2), Decimal::new(100, 2)],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_add(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Numeric(Decimal::new(150, 2)));
        assert_eq!(result.get_value(1), Value::Numeric(Decimal::new(300, 2)));
    }

    #[test]
    fn test_binary_add_with_nulls() {
        let mut nulls_a = NullBitmap::new();
        nulls_a.push(false);
        nulls_a.push(true);
        nulls_a.push(false);
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![1, 0, 3]),
            nulls: nulls_a,
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![10, 20, 30]),
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_add(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Int64(11));
        assert_eq!(result.get_value(1), Value::Null);
        assert_eq!(result.get_value(2), Value::Int64(33));
    }

    #[test]
    fn test_binary_add_float64_with_nulls() {
        let mut nulls_b = NullBitmap::new();
        nulls_b.push(true);
        nulls_b.push(false);
        let a = Column::Float64 {
            data: AVec::from_iter(64, vec![1.0, 2.0]),
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Float64 {
            data: AVec::from_iter(64, vec![0.0, 3.0]),
            nulls: nulls_b,
        };
        let result = a.binary_add(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::float64(5.0));
    }

    #[test]
    fn test_binary_add_numeric_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        let a = Column::Numeric {
            data: vec![Decimal::ZERO],
            nulls,
        };
        let b = Column::Numeric {
            data: vec![Decimal::new(100, 2)],
            nulls: NullBitmap::new_valid(1),
        };
        let result = a.binary_add(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
    }

    #[test]
    #[should_panic(expected = "binary_add: incompatible column types")]
    fn test_binary_add_incompatible() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![1]),
            nulls: NullBitmap::new_valid(1),
        };
        let b = Column::String {
            data: vec!["a".to_string()],
            nulls: NullBitmap::new_valid(1),
        };
        a.binary_add(&b).unwrap();
    }

    #[test]
    fn test_binary_sub() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![10, 20, 30]),
            nulls: NullBitmap::new_valid(3),
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![1, 2, 3]),
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_sub(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Int64(9));
        assert_eq!(result.get_value(1), Value::Int64(18));
        assert_eq!(result.get_value(2), Value::Int64(27));
    }

    #[test]
    fn test_binary_sub_float64() {
        let a = Column::Float64 {
            data: AVec::from_iter(64, vec![5.0, 10.0]),
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Float64 {
            data: AVec::from_iter(64, vec![1.5, 2.5]),
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_sub(&b).unwrap();
        assert_eq!(result.get_value(0), Value::float64(3.5));
        assert_eq!(result.get_value(1), Value::float64(7.5));
    }

    #[test]
    fn test_binary_sub_numeric() {
        let a = Column::Numeric {
            data: vec![Decimal::new(500, 2)],
            nulls: NullBitmap::new_valid(1),
        };
        let b = Column::Numeric {
            data: vec![Decimal::new(200, 2)],
            nulls: NullBitmap::new_valid(1),
        };
        let result = a.binary_sub(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Numeric(Decimal::new(300, 2)));
    }

    #[test]
    fn test_binary_sub_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![0, 10]),
            nulls,
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![5, 3]),
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_sub(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Int64(7));
    }

    #[test]
    fn test_binary_sub_float64_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(false);
        nulls.push(true);
        let a = Column::Float64 {
            data: AVec::from_iter(64, vec![5.0, 0.0]),
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Float64 {
            data: AVec::from_iter(64, vec![2.0, 0.0]),
            nulls,
        };
        let result = a.binary_sub(&b).unwrap();
        assert_eq!(result.get_value(0), Value::float64(3.0));
        assert_eq!(result.get_value(1), Value::Null);
    }

    #[test]
    fn test_binary_sub_numeric_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        let a = Column::Numeric {
            data: vec![Decimal::ZERO],
            nulls: NullBitmap::new_valid(1),
        };
        let b = Column::Numeric {
            data: vec![Decimal::ZERO],
            nulls,
        };
        let result = a.binary_sub(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
    }

    #[test]
    #[should_panic(expected = "binary_sub: incompatible column types")]
    fn test_binary_sub_incompatible() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![1]),
            nulls: NullBitmap::new_valid(1),
        };
        let b = Column::Bool {
            data: vec![true],
            nulls: NullBitmap::new_valid(1),
        };
        a.binary_sub(&b).unwrap();
    }

    #[test]
    fn test_binary_mul() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![2, 3, 4]),
            nulls: NullBitmap::new_valid(3),
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![5, 6, 7]),
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_mul(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Int64(10));
        assert_eq!(result.get_value(1), Value::Int64(18));
        assert_eq!(result.get_value(2), Value::Int64(28));
    }

    #[test]
    fn test_binary_mul_float64() {
        let a = Column::Float64 {
            data: AVec::from_iter(64, vec![2.0, 3.0]),
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Float64 {
            data: AVec::from_iter(64, vec![1.5, 2.0]),
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_mul(&b).unwrap();
        assert_eq!(result.get_value(0), Value::float64(3.0));
        assert_eq!(result.get_value(1), Value::float64(6.0));
    }

    #[test]
    fn test_binary_mul_numeric() {
        let a = Column::Numeric {
            data: vec![Decimal::new(100, 2)],
            nulls: NullBitmap::new_valid(1),
        };
        let b = Column::Numeric {
            data: vec![Decimal::new(200, 2)],
            nulls: NullBitmap::new_valid(1),
        };
        let result = a.binary_mul(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Numeric(Decimal::new(200, 2)));
    }

    #[test]
    fn test_binary_mul_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![0, 5]),
            nulls,
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![10, 10]),
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_mul(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Int64(50));
    }

    #[test]
    fn test_binary_mul_float64_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(false);
        nulls.push(true);
        let a = Column::Float64 {
            data: AVec::from_iter(64, vec![2.0, 0.0]),
            nulls,
        };
        let b = Column::Float64 {
            data: AVec::from_iter(64, vec![3.0, 4.0]),
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_mul(&b).unwrap();
        assert_eq!(result.get_value(0), Value::float64(6.0));
        assert_eq!(result.get_value(1), Value::Null);
    }

    #[test]
    fn test_binary_mul_numeric_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        let a = Column::Numeric {
            data: vec![Decimal::ZERO],
            nulls,
        };
        let b = Column::Numeric {
            data: vec![Decimal::new(100, 2)],
            nulls: NullBitmap::new_valid(1),
        };
        let result = a.binary_mul(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
    }

    #[test]
    #[should_panic(expected = "binary_mul: incompatible column types")]
    fn test_binary_mul_incompatible() {
        let a = Column::Float64 {
            data: AVec::from_iter(64, vec![1.0]),
            nulls: NullBitmap::new_valid(1),
        };
        let b = Column::String {
            data: vec!["a".to_string()],
            nulls: NullBitmap::new_valid(1),
        };
        a.binary_mul(&b).unwrap();
    }

    #[test]
    fn test_binary_div() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![10, 20, 30]),
            nulls: NullBitmap::new_valid(3),
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![2, 5, 10]),
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_div(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Int64(5));
        assert_eq!(result.get_value(1), Value::Int64(4));
        assert_eq!(result.get_value(2), Value::Int64(3));
    }

    #[test]
    fn test_binary_div_by_zero() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![10, 20]),
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![0, 5]),
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_div(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Int64(4));
    }

    #[test]
    fn test_binary_div_float64() {
        let a = Column::Float64 {
            data: AVec::from_iter(64, vec![10.0, 15.0]),
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Float64 {
            data: AVec::from_iter(64, vec![2.0, 3.0]),
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_div(&b).unwrap();
        assert_eq!(result.get_value(0), Value::float64(5.0));
        assert_eq!(result.get_value(1), Value::float64(5.0));
    }

    #[test]
    fn test_binary_div_numeric() {
        let a = Column::Numeric {
            data: vec![Decimal::new(1000, 2)],
            nulls: NullBitmap::new_valid(1),
        };
        let b = Column::Numeric {
            data: vec![Decimal::new(200, 2)],
            nulls: NullBitmap::new_valid(1),
        };
        let result = a.binary_div(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Numeric(Decimal::new(500, 2)));
    }

    #[test]
    fn test_binary_div_numeric_by_zero() {
        let a = Column::Numeric {
            data: vec![Decimal::new(100, 2)],
            nulls: NullBitmap::new_valid(1),
        };
        let b = Column::Numeric {
            data: vec![Decimal::ZERO],
            nulls: NullBitmap::new_valid(1),
        };
        let result = a.binary_div(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
    }

    #[test]
    fn test_binary_div_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![0, 20]),
            nulls,
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![5, 4]),
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_div(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Int64(5));
    }

    #[test]
    fn test_binary_div_float64_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(false);
        nulls.push(true);
        let a = Column::Float64 {
            data: AVec::from_iter(64, vec![10.0, 0.0]),
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Float64 {
            data: AVec::from_iter(64, vec![2.0, 0.0]),
            nulls,
        };
        let result = a.binary_div(&b).unwrap();
        assert_eq!(result.get_value(0), Value::float64(5.0));
        assert_eq!(result.get_value(1), Value::Null);
    }

    #[test]
    fn test_binary_div_numeric_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        let a = Column::Numeric {
            data: vec![Decimal::new(100, 2)],
            nulls,
        };
        let b = Column::Numeric {
            data: vec![Decimal::new(50, 2)],
            nulls: NullBitmap::new_valid(1),
        };
        let result = a.binary_div(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
    }

    #[test]
    #[should_panic(expected = "binary_div: incompatible column types")]
    fn test_binary_div_incompatible() {
        let a = Column::Numeric {
            data: vec![Decimal::ONE],
            nulls: NullBitmap::new_valid(1),
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![1]),
            nulls: NullBitmap::new_valid(1),
        };
        a.binary_div(&b).unwrap();
    }

    #[test]
    fn test_binary_eq() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![1, 2, 3]),
            nulls: NullBitmap::new_valid(3),
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![1, 5, 3]),
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_eq(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
        assert_eq!(result.get_value(2), Value::Bool(true));
    }

    #[test]
    fn test_binary_eq_float64() {
        let a = Column::Float64 {
            data: AVec::from_iter(64, vec![1.0, 2.0]),
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Float64 {
            data: AVec::from_iter(64, vec![1.0, 3.0]),
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_eq(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_binary_eq_string() {
        let a = Column::String {
            data: vec!["a".to_string(), "b".to_string()],
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::String {
            data: vec!["a".to_string(), "c".to_string()],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_eq(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_binary_eq_bool() {
        let a = Column::Bool {
            data: vec![true, false, true],
            nulls: NullBitmap::new_valid(3),
        };
        let b = Column::Bool {
            data: vec![true, true, false],
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_eq(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
        assert_eq!(result.get_value(2), Value::Bool(false));
    }

    #[test]
    fn test_binary_eq_bytes() {
        let a = Column::Bytes {
            data: vec![vec![1, 2], vec![3, 4]],
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Bytes {
            data: vec![vec![1, 2], vec![5, 6]],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_eq(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_binary_eq_interval() {
        let i1 = IntervalValue {
            months: 1,
            days: 2,
            nanos: 3,
        };
        let i2 = IntervalValue {
            months: 1,
            days: 2,
            nanos: 3,
        };
        let i3 = IntervalValue {
            months: 2,
            days: 0,
            nanos: 0,
        };
        let a = Column::Interval {
            data: vec![i1.clone(), i1.clone()],
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Interval {
            data: vec![i2, i3],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_eq(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_binary_eq_date() {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 2, 1).unwrap();
        let a = Column::Date {
            data: vec![d1, d1],
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Date {
            data: vec![d1, d2],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_eq(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_binary_eq_time() {
        let t1 = NaiveTime::from_hms_opt(10, 0, 0).unwrap();
        let t2 = NaiveTime::from_hms_opt(11, 0, 0).unwrap();
        let a = Column::Time {
            data: vec![t1, t1],
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Time {
            data: vec![t1, t2],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_eq(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_binary_eq_datetime() {
        let dt1 = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let dt2 = NaiveDate::from_ymd_opt(2024, 2, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let a = Column::DateTime {
            data: vec![dt1, dt1],
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::DateTime {
            data: vec![dt1, dt2],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_eq(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_binary_eq_timestamp() {
        let ts1 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap();
        let a = Column::Timestamp {
            data: vec![ts1, ts1],
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Timestamp {
            data: vec![ts1, ts2],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_eq(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_binary_eq_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![0, 5]),
            nulls,
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![0, 5]),
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_eq(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    #[should_panic(expected = "binary_eq: incompatible column types")]
    fn test_binary_eq_incompatible() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![1]),
            nulls: NullBitmap::new_valid(1),
        };
        let b = Column::String {
            data: vec!["1".to_string()],
            nulls: NullBitmap::new_valid(1),
        };
        a.binary_eq(&b).unwrap();
    }

    #[test]
    fn test_binary_ne() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![1, 2, 3]),
            nulls: NullBitmap::new_valid(3),
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![1, 5, 3]),
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_ne(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(false));
        assert_eq!(result.get_value(1), Value::Bool(true));
        assert_eq!(result.get_value(2), Value::Bool(false));
    }

    #[test]
    fn test_binary_lt() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![1, 5, 3]),
            nulls: NullBitmap::new_valid(3),
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![2, 3, 3]),
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_lt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
        assert_eq!(result.get_value(2), Value::Bool(false));
    }

    #[test]
    fn test_binary_lt_float64() {
        let a = Column::Float64 {
            data: AVec::from_iter(64, vec![1.0, 5.0]),
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Float64 {
            data: AVec::from_iter(64, vec![2.0, 3.0]),
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_lt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_binary_lt_string() {
        let a = Column::String {
            data: vec!["a".to_string(), "z".to_string()],
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::String {
            data: vec!["b".to_string(), "a".to_string()],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_lt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_binary_lt_bytes() {
        let a = Column::Bytes {
            data: vec![vec![1], vec![5]],
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Bytes {
            data: vec![vec![2], vec![3]],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_lt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_binary_lt_numeric() {
        let a = Column::Numeric {
            data: vec![Decimal::new(100, 2), Decimal::new(500, 2)],
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Numeric {
            data: vec![Decimal::new(200, 2), Decimal::new(300, 2)],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_lt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_binary_lt_interval() {
        let i1 = IntervalValue {
            months: 1,
            days: 0,
            nanos: 0,
        };
        let i2 = IntervalValue {
            months: 2,
            days: 0,
            nanos: 0,
        };
        let a = Column::Interval {
            data: vec![i1.clone(), i2.clone()],
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Interval {
            data: vec![i2, i1],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_lt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_binary_lt_date() {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 2, 1).unwrap();
        let a = Column::Date {
            data: vec![d1, d2],
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Date {
            data: vec![d2, d1],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_lt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_binary_lt_time() {
        let t1 = NaiveTime::from_hms_opt(10, 0, 0).unwrap();
        let t2 = NaiveTime::from_hms_opt(11, 0, 0).unwrap();
        let a = Column::Time {
            data: vec![t1, t2],
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Time {
            data: vec![t2, t1],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_lt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_binary_lt_datetime() {
        let dt1 = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let dt2 = NaiveDate::from_ymd_opt(2024, 2, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let a = Column::DateTime {
            data: vec![dt1, dt2],
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::DateTime {
            data: vec![dt2, dt1],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_lt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_binary_lt_timestamp() {
        let ts1 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap();
        let a = Column::Timestamp {
            data: vec![ts1, ts2],
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Timestamp {
            data: vec![ts2, ts1],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_lt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_binary_lt_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![0, 1]),
            nulls,
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![5, 5]),
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_lt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    #[should_panic(expected = "binary_lt: incompatible column types")]
    fn test_binary_lt_incompatible() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![1]),
            nulls: NullBitmap::new_valid(1),
        };
        let b = Column::Bool {
            data: vec![true],
            nulls: NullBitmap::new_valid(1),
        };
        a.binary_lt(&b).unwrap();
    }

    #[test]
    fn test_binary_le() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![1, 3, 5]),
            nulls: NullBitmap::new_valid(3),
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![2, 3, 4]),
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_le(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(true));
        assert_eq!(result.get_value(2), Value::Bool(false));
    }

    #[test]
    fn test_binary_le_float64() {
        let a = Column::Float64 {
            data: AVec::from_iter(64, vec![1.0, 3.0, 5.0]),
            nulls: NullBitmap::new_valid(3),
        };
        let b = Column::Float64 {
            data: AVec::from_iter(64, vec![2.0, 3.0, 4.0]),
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_le(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(true));
        assert_eq!(result.get_value(2), Value::Bool(false));
    }

    #[test]
    fn test_binary_le_string() {
        let a = Column::String {
            data: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            nulls: NullBitmap::new_valid(3),
        };
        let b = Column::String {
            data: vec!["b".to_string(), "b".to_string(), "a".to_string()],
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_le(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(true));
        assert_eq!(result.get_value(2), Value::Bool(false));
    }

    #[test]
    fn test_binary_le_bytes() {
        let a = Column::Bytes {
            data: vec![vec![1], vec![2], vec![3]],
            nulls: NullBitmap::new_valid(3),
        };
        let b = Column::Bytes {
            data: vec![vec![2], vec![2], vec![1]],
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_le(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(true));
        assert_eq!(result.get_value(2), Value::Bool(false));
    }

    #[test]
    fn test_binary_le_numeric() {
        let a = Column::Numeric {
            data: vec![
                Decimal::new(100, 2),
                Decimal::new(200, 2),
                Decimal::new(300, 2),
            ],
            nulls: NullBitmap::new_valid(3),
        };
        let b = Column::Numeric {
            data: vec![
                Decimal::new(200, 2),
                Decimal::new(200, 2),
                Decimal::new(100, 2),
            ],
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_le(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(true));
        assert_eq!(result.get_value(2), Value::Bool(false));
    }

    #[test]
    fn test_binary_le_interval() {
        let i1 = IntervalValue {
            months: 1,
            days: 0,
            nanos: 0,
        };
        let i2 = IntervalValue {
            months: 1,
            days: 0,
            nanos: 0,
        };
        let i3 = IntervalValue {
            months: 2,
            days: 0,
            nanos: 0,
        };
        let a = Column::Interval {
            data: vec![i1.clone(), i2, i3.clone()],
            nulls: NullBitmap::new_valid(3),
        };
        let b = Column::Interval {
            data: vec![i3, i1.clone(), i1],
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_le(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(true));
        assert_eq!(result.get_value(2), Value::Bool(false));
    }

    #[test]
    fn test_binary_le_date() {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 2, 1).unwrap();
        let a = Column::Date {
            data: vec![d1, d1, d2],
            nulls: NullBitmap::new_valid(3),
        };
        let b = Column::Date {
            data: vec![d2, d1, d1],
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_le(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(true));
        assert_eq!(result.get_value(2), Value::Bool(false));
    }

    #[test]
    fn test_binary_le_time() {
        let t1 = NaiveTime::from_hms_opt(10, 0, 0).unwrap();
        let t2 = NaiveTime::from_hms_opt(11, 0, 0).unwrap();
        let a = Column::Time {
            data: vec![t1, t1, t2],
            nulls: NullBitmap::new_valid(3),
        };
        let b = Column::Time {
            data: vec![t2, t1, t1],
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_le(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(true));
        assert_eq!(result.get_value(2), Value::Bool(false));
    }

    #[test]
    fn test_binary_le_datetime() {
        let dt1 = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let dt2 = NaiveDate::from_ymd_opt(2024, 2, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let a = Column::DateTime {
            data: vec![dt1, dt1, dt2],
            nulls: NullBitmap::new_valid(3),
        };
        let b = Column::DateTime {
            data: vec![dt2, dt1, dt1],
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_le(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(true));
        assert_eq!(result.get_value(2), Value::Bool(false));
    }

    #[test]
    fn test_binary_le_timestamp() {
        let ts1 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap();
        let a = Column::Timestamp {
            data: vec![ts1, ts1, ts2],
            nulls: NullBitmap::new_valid(3),
        };
        let b = Column::Timestamp {
            data: vec![ts2, ts1, ts1],
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_le(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(true));
        assert_eq!(result.get_value(2), Value::Bool(false));
    }

    #[test]
    fn test_binary_le_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![0, 3]),
            nulls,
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![5, 3]),
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_le(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    #[should_panic(expected = "binary_le: incompatible column types")]
    fn test_binary_le_incompatible() {
        let a = Column::Float64 {
            data: AVec::from_iter(64, vec![1.0]),
            nulls: NullBitmap::new_valid(1),
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![1]),
            nulls: NullBitmap::new_valid(1),
        };
        a.binary_le(&b).unwrap();
    }

    #[test]
    fn test_binary_gt() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![3, 1, 2]),
            nulls: NullBitmap::new_valid(3),
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![2, 2, 2]),
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_gt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(false));
        assert_eq!(result.get_value(2), Value::Bool(false));
    }

    #[test]
    fn test_binary_ge() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![3, 2, 1]),
            nulls: NullBitmap::new_valid(3),
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![2, 2, 2]),
            nulls: NullBitmap::new_valid(3),
        };
        let result = a.binary_ge(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(true));
        assert_eq!(result.get_value(2), Value::Bool(false));
    }

    #[test]
    fn test_binary_and_or() {
        let a = Column::Bool {
            data: vec![true, true, false, false],
            nulls: NullBitmap::new_valid(4),
        };
        let b = Column::Bool {
            data: vec![true, false, true, false],
            nulls: NullBitmap::new_valid(4),
        };
        let and_result = a.binary_and(&b).unwrap();
        assert_eq!(and_result.get_value(0), Value::Bool(true));
        assert_eq!(and_result.get_value(1), Value::Bool(false));
        assert_eq!(and_result.get_value(2), Value::Bool(false));
        assert_eq!(and_result.get_value(3), Value::Bool(false));

        let or_result = a.binary_or(&b).unwrap();
        assert_eq!(or_result.get_value(0), Value::Bool(true));
        assert_eq!(or_result.get_value(1), Value::Bool(true));
        assert_eq!(or_result.get_value(2), Value::Bool(true));
        assert_eq!(or_result.get_value(3), Value::Bool(false));
    }

    #[test]
    fn test_binary_and_with_nulls() {
        let mut nulls_a = NullBitmap::new();
        nulls_a.push(true);
        nulls_a.push(true);
        nulls_a.push(false);
        nulls_a.push(false);
        let a = Column::Bool {
            data: vec![false, false, true, false],
            nulls: nulls_a,
        };
        let mut nulls_b = NullBitmap::new();
        nulls_b.push(true);
        nulls_b.push(false);
        nulls_b.push(true);
        nulls_b.push(false);
        let b = Column::Bool {
            data: vec![false, true, false, true],
            nulls: nulls_b,
        };
        let result = a.binary_and(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Null);
        assert_eq!(result.get_value(2), Value::Null);
        assert_eq!(result.get_value(3), Value::Bool(false));
    }

    #[test]
    fn test_binary_and_false_short_circuit() {
        let mut nulls_a = NullBitmap::new();
        nulls_a.push(false);
        nulls_a.push(true);
        let a = Column::Bool {
            data: vec![false, false],
            nulls: nulls_a,
        };
        let mut nulls_b = NullBitmap::new();
        nulls_b.push(true);
        nulls_b.push(false);
        let b = Column::Bool {
            data: vec![false, false],
            nulls: nulls_b,
        };
        let result = a.binary_and(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(false));
        assert_eq!(result.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_binary_or_with_nulls() {
        let mut nulls_a = NullBitmap::new();
        nulls_a.push(true);
        nulls_a.push(true);
        nulls_a.push(false);
        nulls_a.push(false);
        let a = Column::Bool {
            data: vec![false, false, true, false],
            nulls: nulls_a,
        };
        let mut nulls_b = NullBitmap::new();
        nulls_b.push(true);
        nulls_b.push(false);
        nulls_b.push(true);
        nulls_b.push(false);
        let b = Column::Bool {
            data: vec![false, false, false, false],
            nulls: nulls_b,
        };
        let result = a.binary_or(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Null);
        assert_eq!(result.get_value(2), Value::Bool(true));
        assert_eq!(result.get_value(3), Value::Bool(false));
    }

    #[test]
    fn test_binary_or_true_short_circuit() {
        let mut nulls_a = NullBitmap::new();
        nulls_a.push(false);
        nulls_a.push(true);
        let a = Column::Bool {
            data: vec![true, false],
            nulls: nulls_a,
        };
        let mut nulls_b = NullBitmap::new();
        nulls_b.push(true);
        nulls_b.push(false);
        let b = Column::Bool {
            data: vec![false, true],
            nulls: nulls_b,
        };
        let result = a.binary_or(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_and_with_all_null_right() {
        let a = Column::Bool {
            data: vec![true, false],
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![0, 0]),
            nulls: NullBitmap::new_null(2),
        };
        let result = a.binary_and(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_binary_and_with_all_null_left() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![0, 0]),
            nulls: NullBitmap::new_null(2),
        };
        let b = Column::Bool {
            data: vec![true, false],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_and(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(false));
    }

    #[test]
    fn test_binary_and_both_all_null() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![0]),
            nulls: NullBitmap::new_null(1),
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![0]),
            nulls: NullBitmap::new_null(1),
        };
        let result = a.binary_and(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
    }

    #[test]
    fn test_binary_or_with_all_null_right() {
        let a = Column::Bool {
            data: vec![true, false],
            nulls: NullBitmap::new_valid(2),
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![0, 0]),
            nulls: NullBitmap::new_null(2),
        };
        let result = a.binary_or(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Null);
    }

    #[test]
    fn test_binary_or_with_all_null_left() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![0, 0]),
            nulls: NullBitmap::new_null(2),
        };
        let b = Column::Bool {
            data: vec![true, false],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_or(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Bool(true));
        assert_eq!(result.get_value(1), Value::Null);
    }

    #[test]
    fn test_binary_or_both_all_null() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![0]),
            nulls: NullBitmap::new_null(1),
        };
        let b = Column::Int64 {
            data: AVec::from_iter(64, vec![0]),
            nulls: NullBitmap::new_null(1),
        };
        let result = a.binary_or(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
    }

    #[test]
    #[should_panic(expected = "binary_and: requires Bool columns")]
    fn test_binary_and_incompatible() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![1]),
            nulls: NullBitmap::new_valid(1),
        };
        let b = Column::String {
            data: vec!["a".to_string()],
            nulls: NullBitmap::new_valid(1),
        };
        a.binary_and(&b).unwrap();
    }

    #[test]
    #[should_panic(expected = "binary_or: requires Bool columns")]
    fn test_binary_or_incompatible() {
        let a = Column::Int64 {
            data: AVec::from_iter(64, vec![1]),
            nulls: NullBitmap::new_valid(1),
        };
        let b = Column::String {
            data: vec!["a".to_string()],
            nulls: NullBitmap::new_valid(1),
        };
        a.binary_or(&b).unwrap();
    }

    #[test]
    fn test_unary_not() {
        let col = Column::Bool {
            data: vec![true, false, true],
            nulls: NullBitmap::new_valid(3),
        };
        let result = col.unary_not().unwrap();
        assert_eq!(result.get_value(0), Value::Bool(false));
        assert_eq!(result.get_value(1), Value::Bool(true));
        assert_eq!(result.get_value(2), Value::Bool(false));
    }

    #[test]
    fn test_unary_not_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(false);
        nulls.push(true);
        nulls.push(false);
        let col = Column::Bool {
            data: vec![true, false, false],
            nulls,
        };
        let result = col.unary_not().unwrap();
        assert_eq!(result.get_value(0), Value::Bool(false));
        assert_eq!(result.get_value(1), Value::Null);
        assert_eq!(result.get_value(2), Value::Bool(true));
    }

    #[test]
    fn test_unary_not_non_bool() {
        let col = Column::Int64 {
            data: AVec::from_iter(64, vec![1]),
            nulls: NullBitmap::new_valid(1),
        };
        let result = col.unary_not();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unary_not: requires Bool column")
        );
    }

    #[test]
    fn test_unary_neg_int64() {
        let col = Column::Int64 {
            data: AVec::from_iter(64, vec![1, -2, 3]),
            nulls: NullBitmap::new_valid(3),
        };
        let result = col.unary_neg().unwrap();
        assert_eq!(result.get_value(0), Value::Int64(-1));
        assert_eq!(result.get_value(1), Value::Int64(2));
        assert_eq!(result.get_value(2), Value::Int64(-3));
    }

    #[test]
    fn test_unary_neg_float64() {
        let col = Column::Float64 {
            data: AVec::from_iter(64, vec![1.5, -2.5]),
            nulls: NullBitmap::new_valid(2),
        };
        let result = col.unary_neg().unwrap();
        assert_eq!(result.get_value(0), Value::float64(-1.5));
        assert_eq!(result.get_value(1), Value::float64(2.5));
    }

    #[test]
    fn test_unary_neg_numeric() {
        let col = Column::Numeric {
            data: vec![Decimal::new(100, 2), Decimal::new(-200, 2)],
            nulls: NullBitmap::new_valid(2),
        };
        let result = col.unary_neg().unwrap();
        assert_eq!(result.get_value(0), Value::Numeric(Decimal::new(-100, 2)));
        assert_eq!(result.get_value(1), Value::Numeric(Decimal::new(200, 2)));
    }

    #[test]
    fn test_unary_neg_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(false);
        nulls.push(true);
        let col = Column::Int64 {
            data: AVec::from_iter(64, vec![5, 0]),
            nulls,
        };
        let result = col.unary_neg().unwrap();
        assert_eq!(result.get_value(0), Value::Int64(-5));
        assert_eq!(result.get_value(1), Value::Null);
    }

    #[test]
    #[should_panic(expected = "unary_neg: requires numeric column")]
    fn test_unary_neg_non_numeric() {
        let col = Column::String {
            data: vec!["a".to_string()],
            nulls: NullBitmap::new_valid(1),
        };
        col.unary_neg().unwrap();
    }

    #[test]
    fn test_is_null_mask() {
        let mut nulls = NullBitmap::new();
        nulls.push(false);
        nulls.push(true);
        nulls.push(false);
        let col = Column::Int64 {
            data: AVec::from_iter(64, vec![1, 0, 3]),
            nulls,
        };
        let mask = col.is_null_mask();
        assert_eq!(mask.get_value(0), Value::Bool(false));
        assert_eq!(mask.get_value(1), Value::Bool(true));
        assert_eq!(mask.get_value(2), Value::Bool(false));
    }

    #[test]
    fn test_is_not_null_mask() {
        let mut nulls = NullBitmap::new();
        nulls.push(false);
        nulls.push(true);
        nulls.push(false);
        let col = Column::Int64 {
            data: AVec::from_iter(64, vec![1, 0, 3]),
            nulls,
        };
        let mask = col.is_not_null_mask();
        assert_eq!(mask.get_value(0), Value::Bool(true));
        assert_eq!(mask.get_value(1), Value::Bool(false));
        assert_eq!(mask.get_value(2), Value::Bool(true));
    }

    #[test]
    fn test_coerce_to_type_same_type() {
        let col = Column::Int64 {
            data: AVec::from_iter(64, vec![1, 2, 3]),
            nulls: NullBitmap::new_valid(3),
        };
        let result = col.coerce_to_type(&DataType::Int64);
        assert_eq!(result.len(), 3);
        assert_eq!(result.get_value(0), Value::Int64(1));
    }

    #[test]
    fn test_coerce_to_type_numeric_to_numeric() {
        let col = Column::Numeric {
            data: vec![Decimal::new(100, 2)],
            nulls: NullBitmap::new_valid(1),
        };
        let result = col.coerce_to_type(&DataType::Numeric(Some((10, 2))));
        assert_eq!(result.len(), 1);
        assert_eq!(result.get_value(0), Value::Numeric(Decimal::new(100, 2)));
    }

    #[test]
    fn test_coerce_to_type_array_to_array() {
        let arr = vec![Value::Int64(1), Value::Int64(2)];
        let col = Column::Array {
            data: vec![arr.clone()],
            nulls: NullBitmap::new_valid(1),
            element_type: DataType::Int64,
        };
        let result = col.coerce_to_type(&DataType::Array(Box::new(DataType::Int64)));
        assert_eq!(result.len(), 1);
        assert_eq!(result.get_value(0), Value::Array(arr));
    }

    #[test]
    fn test_coerce_to_type_conversion() {
        let col = Column::Int64 {
            data: AVec::from_iter(64, vec![1, 2]),
            nulls: NullBitmap::new_valid(2),
        };
        let result = col.coerce_to_type(&DataType::String);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_coerce_to_type_struct_to_struct() {
        let s = vec![("x".to_string(), Value::Int64(1))];
        let col = Column::Struct {
            data: vec![s.clone()],
            nulls: NullBitmap::new_valid(1),
            fields: vec![("x".to_string(), DataType::Int64)],
        };
        let result = col.coerce_to_type(&DataType::Struct(vec![
            yachtsql_common::types::StructField {
                name: "x".to_string(),
                data_type: DataType::Int64,
            },
        ]));
        assert_eq!(result.len(), 1);
        assert_eq!(result.get_value(0), Value::Struct(s));
    }

    #[test]
    fn test_coerce_to_type_range_to_range() {
        let r = RangeValue::new(Some(Value::Int64(1)), Some(Value::Int64(10)));
        let col = Column::Range {
            data: vec![r.clone()],
            nulls: NullBitmap::new_valid(1),
            element_type: DataType::Int64,
        };
        let result = col.coerce_to_type(&DataType::Range(Box::new(DataType::Int64)));
        assert_eq!(result.len(), 1);
        assert_eq!(result.get_value(0), Value::Range(r));
    }

    #[test]
    fn test_binary_eq_float64_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let a = Column::Float64 {
            data: AVec::from_iter(64, vec![0.0, 5.0]),
            nulls,
        };
        let b = Column::Float64 {
            data: AVec::from_iter(64, vec![0.0, 5.0]),
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_eq(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_eq_string_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let a = Column::String {
            data: vec!["".to_string(), "test".to_string()],
            nulls,
        };
        let b = Column::String {
            data: vec!["".to_string(), "test".to_string()],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_eq(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_eq_bool_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let a = Column::Bool {
            data: vec![false, true],
            nulls,
        };
        let b = Column::Bool {
            data: vec![false, true],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_eq(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_eq_bytes_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let a = Column::Bytes {
            data: vec![vec![], vec![1, 2]],
            nulls,
        };
        let b = Column::Bytes {
            data: vec![vec![], vec![1, 2]],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_eq(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_eq_interval_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let i1 = IntervalValue {
            months: 1,
            days: 0,
            nanos: 0,
        };
        let a = Column::Interval {
            data: vec![i1.clone(), i1.clone()],
            nulls,
        };
        let b = Column::Interval {
            data: vec![i1.clone(), i1],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_eq(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_eq_date_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let d = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let a = Column::Date {
            data: vec![d, d],
            nulls,
        };
        let b = Column::Date {
            data: vec![d, d],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_eq(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_eq_time_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let t = NaiveTime::from_hms_opt(10, 0, 0).unwrap();
        let a = Column::Time {
            data: vec![t, t],
            nulls,
        };
        let b = Column::Time {
            data: vec![t, t],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_eq(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_eq_datetime_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let dt = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let a = Column::DateTime {
            data: vec![dt, dt],
            nulls,
        };
        let b = Column::DateTime {
            data: vec![dt, dt],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_eq(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_eq_timestamp_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let a = Column::Timestamp {
            data: vec![ts, ts],
            nulls,
        };
        let b = Column::Timestamp {
            data: vec![ts, ts],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_eq(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_lt_float64_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let a = Column::Float64 {
            data: AVec::from_iter(64, vec![0.0, 1.0]),
            nulls,
        };
        let b = Column::Float64 {
            data: AVec::from_iter(64, vec![5.0, 5.0]),
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_lt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_lt_string_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let a = Column::String {
            data: vec!["".to_string(), "a".to_string()],
            nulls,
        };
        let b = Column::String {
            data: vec!["z".to_string(), "z".to_string()],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_lt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_lt_bytes_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let a = Column::Bytes {
            data: vec![vec![], vec![1]],
            nulls,
        };
        let b = Column::Bytes {
            data: vec![vec![9], vec![9]],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_lt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_lt_numeric_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let a = Column::Numeric {
            data: vec![Decimal::ZERO, Decimal::new(100, 2)],
            nulls,
        };
        let b = Column::Numeric {
            data: vec![Decimal::new(999, 2), Decimal::new(999, 2)],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_lt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_lt_interval_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let i1 = IntervalValue {
            months: 1,
            days: 0,
            nanos: 0,
        };
        let i2 = IntervalValue {
            months: 5,
            days: 0,
            nanos: 0,
        };
        let a = Column::Interval {
            data: vec![i1.clone(), i1],
            nulls,
        };
        let b = Column::Interval {
            data: vec![i2.clone(), i2],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_lt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_lt_date_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let a = Column::Date {
            data: vec![d1, d1],
            nulls,
        };
        let b = Column::Date {
            data: vec![d2, d2],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_lt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_lt_time_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let t1 = NaiveTime::from_hms_opt(10, 0, 0).unwrap();
        let t2 = NaiveTime::from_hms_opt(23, 0, 0).unwrap();
        let a = Column::Time {
            data: vec![t1, t1],
            nulls,
        };
        let b = Column::Time {
            data: vec![t2, t2],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_lt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_lt_datetime_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let dt1 = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let dt2 = NaiveDate::from_ymd_opt(2024, 12, 31)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let a = Column::DateTime {
            data: vec![dt1, dt1],
            nulls,
        };
        let b = Column::DateTime {
            data: vec![dt2, dt2],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_lt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_lt_timestamp_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let ts1 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 12, 31, 0, 0, 0).unwrap();
        let a = Column::Timestamp {
            data: vec![ts1, ts1],
            nulls,
        };
        let b = Column::Timestamp {
            data: vec![ts2, ts2],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_lt(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_le_float64_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let a = Column::Float64 {
            data: AVec::from_iter(64, vec![0.0, 3.0]),
            nulls,
        };
        let b = Column::Float64 {
            data: AVec::from_iter(64, vec![5.0, 3.0]),
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_le(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_le_string_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let a = Column::String {
            data: vec!["".to_string(), "test".to_string()],
            nulls,
        };
        let b = Column::String {
            data: vec!["z".to_string(), "test".to_string()],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_le(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_le_bytes_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let a = Column::Bytes {
            data: vec![vec![], vec![1, 2]],
            nulls,
        };
        let b = Column::Bytes {
            data: vec![vec![9], vec![1, 2]],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_le(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_le_numeric_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let a = Column::Numeric {
            data: vec![Decimal::ZERO, Decimal::new(100, 2)],
            nulls,
        };
        let b = Column::Numeric {
            data: vec![Decimal::new(999, 2), Decimal::new(100, 2)],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_le(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_le_interval_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let i1 = IntervalValue {
            months: 1,
            days: 0,
            nanos: 0,
        };
        let a = Column::Interval {
            data: vec![i1.clone(), i1.clone()],
            nulls,
        };
        let b = Column::Interval {
            data: vec![i1.clone(), i1],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_le(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_le_date_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let d = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let a = Column::Date {
            data: vec![d, d],
            nulls,
        };
        let b = Column::Date {
            data: vec![d, d],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_le(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_le_time_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let t = NaiveTime::from_hms_opt(10, 0, 0).unwrap();
        let a = Column::Time {
            data: vec![t, t],
            nulls,
        };
        let b = Column::Time {
            data: vec![t, t],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_le(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_le_datetime_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let dt = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let a = Column::DateTime {
            data: vec![dt, dt],
            nulls,
        };
        let b = Column::DateTime {
            data: vec![dt, dt],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_le(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_binary_le_timestamp_with_nulls() {
        let mut nulls = NullBitmap::new();
        nulls.push(true);
        nulls.push(false);
        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let a = Column::Timestamp {
            data: vec![ts, ts],
            nulls,
        };
        let b = Column::Timestamp {
            data: vec![ts, ts],
            nulls: NullBitmap::new_valid(2),
        };
        let result = a.binary_le(&b).unwrap();
        assert_eq!(result.get_value(0), Value::Null);
        assert_eq!(result.get_value(1), Value::Bool(true));
    }

    #[test]
    fn test_coerce_to_type_array_different_element_types() {
        let arr = vec![Value::Int64(1)];
        let col = Column::Array {
            data: vec![arr],
            nulls: NullBitmap::new_valid(1),
            element_type: DataType::Int64,
        };
        let result = col.coerce_to_type(&DataType::Array(Box::new(DataType::String)));
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_coerce_to_type_struct_different_fields() {
        let s = vec![("x".to_string(), Value::Int64(1))];
        let col = Column::Struct {
            data: vec![s],
            nulls: NullBitmap::new_valid(1),
            fields: vec![("x".to_string(), DataType::Int64)],
        };
        let result = col.coerce_to_type(&DataType::Struct(vec![
            yachtsql_common::types::StructField {
                name: "y".to_string(),
                data_type: DataType::String,
            },
        ]));
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_coerce_to_type_range_different_element_types() {
        let r = RangeValue::new(Some(Value::Int64(1)), Some(Value::Int64(10)));
        let col = Column::Range {
            data: vec![r],
            nulls: NullBitmap::new_valid(1),
            element_type: DataType::Int64,
        };
        let result = col.coerce_to_type(&DataType::Range(Box::new(DataType::Date)));
        assert_eq!(result.len(), 1);
    }
}
