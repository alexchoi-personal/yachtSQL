#![coverage(off)]

use std::fmt;

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum FieldMode {
    Required,
    #[default]
    Nullable,
    Repeated,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub mode: FieldMode,
}

impl Field {
    pub fn new(name: impl Into<String>, data_type: DataType, mode: FieldMode) -> Self {
        Self {
            name: name.into(),
            data_type,
            mode,
        }
    }

    pub fn nullable(name: impl Into<String>, data_type: DataType) -> Self {
        Self::new(name, data_type, FieldMode::Nullable)
    }

    pub fn required(name: impl Into<String>, data_type: DataType) -> Self {
        Self::new(name, data_type, FieldMode::Required)
    }

    pub fn is_nullable(&self) -> bool {
        matches!(self.mode, FieldMode::Nullable)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    fields: Vec<Field>,
}

impl Schema {
    pub fn new() -> Self {
        Self { fields: Vec::new() }
    }

    pub fn from_fields(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    pub fn field_by_name(&self, name: &str) -> Option<&Field> {
        self.fields
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(name))
    }

    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Unknown,
    Bool,
    Int64,
    Float64,
    Numeric(Option<(u8, u8)>),
    BigNumeric,
    String,
    Bytes,
    Date,
    DateTime,
    Time,
    Timestamp,
    Geography,
    Json,
    Struct(Vec<StructField>),
    Array(Box<DataType>),
    Interval,
    Range(Box<DataType>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StructField {
    pub name: String,
    pub data_type: DataType,
}

impl DataType {
    pub fn to_bq_type(&self) -> String {
        match self {
            DataType::Unknown => "STRING".to_string(),
            DataType::Bool => "BOOLEAN".to_string(),
            DataType::Int64 => "INT64".to_string(),
            DataType::Float64 => "FLOAT64".to_string(),
            DataType::Numeric(_) => "NUMERIC".to_string(),
            DataType::BigNumeric => "BIGNUMERIC".to_string(),
            DataType::String => "STRING".to_string(),
            DataType::Bytes => "BYTES".to_string(),
            DataType::Date => "DATE".to_string(),
            DataType::DateTime => "DATETIME".to_string(),
            DataType::Time => "TIME".to_string(),
            DataType::Timestamp => "TIMESTAMP".to_string(),
            DataType::Geography => "GEOGRAPHY".to_string(),
            DataType::Json => "JSON".to_string(),
            DataType::Struct(_) => "STRUCT".to_string(),
            DataType::Array(inner) => format!("ARRAY<{}>", inner.to_bq_type()),
            DataType::Interval => "INTERVAL".to_string(),
            DataType::Range(_) => "STRING".to_string(),
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Unknown => write!(f, "UNKNOWN"),
            DataType::Bool => write!(f, "BOOL"),
            DataType::Int64 => write!(f, "INT64"),
            DataType::Float64 => write!(f, "FLOAT64"),
            DataType::Numeric(None) => write!(f, "NUMERIC"),
            DataType::Numeric(Some((p, s))) => write!(f, "NUMERIC({}, {})", p, s),
            DataType::BigNumeric => write!(f, "BIGNUMERIC"),
            DataType::String => write!(f, "STRING"),
            DataType::Bytes => write!(f, "BYTES"),
            DataType::Date => write!(f, "DATE"),
            DataType::DateTime => write!(f, "DATETIME"),
            DataType::Time => write!(f, "TIME"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::Geography => write!(f, "GEOGRAPHY"),
            DataType::Json => write!(f, "JSON"),
            DataType::Struct(fields) => {
                write!(f, "STRUCT<")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{} {}", field.name, field.data_type)?;
                }
                write!(f, ">")
            }
            DataType::Array(inner) => write!(f, "ARRAY<{}>", inner),
            DataType::Interval => write!(f, "INTERVAL"),
            DataType::Range(inner) => write!(f, "RANGE<{}>", inner),
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum Value {
    #[default]
    Null,
    Bool(bool),
    Int64(i64),
    Float64(ordered_float::OrderedFloat<f64>),
    Numeric(Decimal),
    BigNumeric(Decimal),
    String(String),
    Bytes(Vec<u8>),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(chrono::NaiveDateTime),
    Timestamp(DateTime<Utc>),
    Json(serde_json::Value),
    Array(Vec<Value>),
    Struct(Vec<(String, Value)>),
    Geography(String),
    Interval(IntervalValue),
    Range(RangeValue),
    Default,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntervalValue {
    pub months: i32,
    pub days: i32,
    pub nanos: i64,
}

impl IntervalValue {
    pub const MICROS_PER_SECOND: i64 = 1_000_000;
    pub const MICROS_PER_MINUTE: i64 = 60 * Self::MICROS_PER_SECOND;
    pub const MICROS_PER_HOUR: i64 = 60 * Self::MICROS_PER_MINUTE;
    pub const NANOS_PER_MICRO: i64 = 1_000;

    pub fn new(months: i32, days: i32, micros: i64) -> Self {
        Self {
            months,
            days,
            nanos: micros * Self::NANOS_PER_MICRO,
        }
    }

    pub fn from_months(months: i32) -> Self {
        Self {
            months,
            days: 0,
            nanos: 0,
        }
    }

    pub fn from_days(days: i32) -> Self {
        Self {
            months: 0,
            days,
            nanos: 0,
        }
    }

    pub fn from_hours(hours: i64) -> Self {
        Self {
            months: 0,
            days: 0,
            nanos: hours * Self::MICROS_PER_HOUR * Self::NANOS_PER_MICRO,
        }
    }
}

pub type Interval = IntervalValue;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RangeValue {
    pub start: Option<Box<Value>>,
    pub end: Option<Box<Value>>,
}

impl RangeValue {
    pub fn new(start: Option<Value>, end: Option<Value>) -> Self {
        Self {
            start: start.map(Box::new),
            end: end.map(Box::new),
        }
    }

    pub fn start(&self) -> Option<&Value> {
        self.start.as_deref()
    }

    pub fn end(&self) -> Option<&Value> {
        self.end.as_deref()
    }

    pub fn contains(&self, value: &Value) -> bool {
        let after_start = match &self.start {
            Some(start) => value >= start.as_ref(),
            None => true,
        };
        let before_end = match &self.end {
            Some(end) => value < end.as_ref(),
            None => true,
        };
        after_start && before_end
    }

    pub fn overlaps(&self, other: &RangeValue) -> bool {
        let self_start = self.start.as_deref();
        let self_end = self.end.as_deref();
        let other_start = other.start.as_deref();
        let other_end = other.end.as_deref();

        let start_before_other_end = match (self_start, other_end) {
            (Some(s), Some(e)) => s < e,
            _ => true,
        };
        let other_start_before_end = match (other_start, self_end) {
            (Some(s), Some(e)) => s < e,
            _ => true,
        };
        start_before_other_end && other_start_before_end
    }

    pub fn intersect(&self, other: &RangeValue) -> Option<RangeValue> {
        if !self.overlaps(other) {
            return None;
        }

        let new_start = match (&self.start, &other.start) {
            (Some(a), Some(b)) => Some(if a.as_ref() > b.as_ref() {
                a.as_ref().clone()
            } else {
                b.as_ref().clone()
            }),
            (Some(a), None) => Some(a.as_ref().clone()),
            (None, Some(b)) => Some(b.as_ref().clone()),
            (None, None) => None,
        };

        let new_end = match (&self.end, &other.end) {
            (Some(a), Some(b)) => Some(if a.as_ref() < b.as_ref() {
                a.as_ref().clone()
            } else {
                b.as_ref().clone()
            }),
            (Some(a), None) => Some(a.as_ref().clone()),
            (None, Some(b)) => Some(b.as_ref().clone()),
            (None, None) => None,
        };

        Some(RangeValue::new(new_start, new_end))
    }

    pub fn element_type(&self) -> DataType {
        self.start
            .as_ref()
            .map(|v| v.data_type())
            .or_else(|| self.end.as_ref().map(|v| v.data_type()))
            .unwrap_or(DataType::Unknown)
    }
}

impl Value {
    pub fn null() -> Self {
        Value::Null
    }

    pub fn bool_val(v: bool) -> Self {
        Value::Bool(v)
    }

    pub fn int64(v: i64) -> Self {
        Value::Int64(v)
    }

    pub fn float64(v: f64) -> Self {
        Value::Float64(ordered_float::OrderedFloat(v))
    }

    pub fn numeric(v: Decimal) -> Self {
        Value::Numeric(v)
    }

    pub fn string(v: impl Into<String>) -> Self {
        Value::String(v.into())
    }

    pub fn bytes(v: Vec<u8>) -> Self {
        Value::Bytes(v)
    }

    pub fn date(v: NaiveDate) -> Self {
        Value::Date(v)
    }

    pub fn time(v: NaiveTime) -> Self {
        Value::Time(v)
    }

    pub fn datetime(v: chrono::NaiveDateTime) -> Self {
        Value::DateTime(v)
    }

    pub fn timestamp(v: DateTime<Utc>) -> Self {
        Value::Timestamp(v)
    }

    pub fn json(v: serde_json::Value) -> Self {
        Value::Json(v)
    }

    pub fn array(v: Vec<Value>) -> Self {
        Value::Array(v)
    }

    pub fn struct_val(v: Vec<(String, Value)>) -> Self {
        Value::Struct(v)
    }

    pub fn geography(v: impl Into<String>) -> Self {
        Value::Geography(v.into())
    }

    pub fn interval(v: IntervalValue) -> Self {
        Value::Interval(v)
    }

    pub fn interval_from_parts(months: i32, days: i32, nanos: i64) -> Self {
        Value::Interval(IntervalValue {
            months,
            days,
            nanos,
        })
    }

    pub fn range(start: Option<Value>, end: Option<Value>) -> Self {
        Value::Range(RangeValue::new(start, end))
    }

    pub fn range_val(v: RangeValue) -> Self {
        Value::Range(v)
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Unknown,
            Value::Bool(_) => DataType::Bool,
            Value::Int64(_) => DataType::Int64,
            Value::Float64(_) => DataType::Float64,
            Value::Numeric(_) => DataType::Numeric(None),
            Value::BigNumeric(_) => DataType::BigNumeric,
            Value::String(_) => DataType::String,
            Value::Bytes(_) => DataType::Bytes,
            Value::Date(_) => DataType::Date,
            Value::Time(_) => DataType::Time,
            Value::DateTime(_) => DataType::DateTime,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::Json(_) => DataType::Json,
            Value::Array(elements) => {
                let elem_type = elements
                    .first()
                    .map(|v| v.data_type())
                    .unwrap_or(DataType::Unknown);
                DataType::Array(Box::new(elem_type))
            }
            Value::Struct(fields) => {
                let struct_fields = fields
                    .iter()
                    .map(|(name, val)| StructField {
                        name: name.clone(),
                        data_type: val.data_type(),
                    })
                    .collect();
                DataType::Struct(struct_fields)
            }
            Value::Geography(_) => DataType::Geography,
            Value::Interval(_) => DataType::Interval,
            Value::Range(r) => DataType::Range(Box::new(r.element_type())),
            Value::Default => DataType::Unknown,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float64(v) => Some(v.0),
            Value::Int64(v) => Some(*v as f64),
            _ => None,
        }
    }

    pub fn as_numeric(&self) -> Option<Decimal> {
        match self {
            Value::Numeric(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_numeric_ref(&self) -> Option<&Decimal> {
        match self {
            Value::Numeric(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_date(&self) -> Option<NaiveDate> {
        match self {
            Value::Date(d) => Some(*d),
            _ => None,
        }
    }

    pub fn as_time(&self) -> Option<NaiveTime> {
        match self {
            Value::Time(t) => Some(*t),
            _ => None,
        }
    }

    pub fn as_datetime(&self) -> Option<chrono::NaiveDateTime> {
        match self {
            Value::DateTime(dt) => Some(*dt),
            _ => None,
        }
    }

    pub fn as_timestamp(&self) -> Option<DateTime<Utc>> {
        match self {
            Value::Timestamp(ts) => Some(*ts),
            _ => None,
        }
    }

    pub fn as_json(&self) -> Option<&serde_json::Value> {
        match self {
            Value::Json(j) => Some(j),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Value::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn as_struct(&self) -> Option<&[(String, Value)]> {
        match self {
            Value::Struct(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_geography(&self) -> Option<&str> {
        match self {
            Value::Geography(g) => Some(g),
            _ => None,
        }
    }

    pub fn as_interval(&self) -> Option<&IntervalValue> {
        match self {
            Value::Interval(i) => Some(i),
            _ => None,
        }
    }

    pub fn as_range(&self) -> Option<&RangeValue> {
        match self {
            Value::Range(r) => Some(r),
            _ => None,
        }
    }

    pub fn into_string(self) -> Option<String> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn into_bytes(self) -> Option<Vec<u8>> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn into_array(self) -> Option<Vec<Value>> {
        match self {
            Value::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Int64(i) => serde_json::json!(i),
            Value::Float64(f) => serde_json::json!(f.into_inner()),
            Value::Numeric(d) => serde_json::Value::String(d.to_string()),
            Value::BigNumeric(d) => serde_json::Value::String(d.to_string()),
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Bytes(b) => serde_json::Value::String(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                b,
            )),
            Value::Date(d) => serde_json::Value::String(d.to_string()),
            Value::Time(t) => serde_json::Value::String(t.to_string()),
            Value::DateTime(dt) => serde_json::Value::String(dt.to_string()),
            Value::Timestamp(ts) => serde_json::Value::String(ts.to_string()),
            Value::Json(j) => j.clone(),
            Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(|v| v.to_json()).collect())
            }
            Value::Struct(fields) => {
                let obj: serde_json::Map<String, serde_json::Value> = fields
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_json()))
                    .collect();
                serde_json::Value::Object(obj)
            }
            Value::Geography(g) => serde_json::Value::String(g.clone()),
            Value::Interval(i) => serde_json::Value::String(format!("{:?}", i)),
            Value::Range(r) => serde_json::Value::String(format!("{:?}", r)),
            Value::Default => serde_json::Value::Null,
        }
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Int64(v) => write!(f, "{}", v),
            Value::Float64(v) => write!(f, "{}", v),
            Value::Numeric(v) => write!(f, "{}", v),
            Value::BigNumeric(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "'{}'", v),
            Value::Bytes(v) => write!(f, "b'{}'", hex::encode(v)),
            Value::Date(v) => write!(f, "DATE '{}'", v),
            Value::Time(v) => write!(f, "TIME '{}'", v),
            Value::DateTime(v) => write!(f, "DATETIME '{}'", v),
            Value::Timestamp(v) => {
                write!(f, "TIMESTAMP '{}'", v.format("%Y-%m-%d %H:%M:%S%.6f UTC"))
            }
            Value::Json(v) => write!(f, "JSON '{}'", v),
            Value::Array(v) => {
                write!(f, "[")?;
                for (i, elem) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", elem)?;
                }
                write!(f, "]")
            }
            Value::Struct(fields) => {
                write!(f, "STRUCT(")?;
                for (i, (name, val)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {:?}", name, val)?;
                }
                write!(f, ")")
            }
            Value::Geography(v) => write!(f, "GEOGRAPHY '{}'", v),
            Value::Interval(v) => write!(
                f,
                "INTERVAL {} months {} days {} nanos",
                v.months, v.days, v.nanos
            ),
            Value::Range(r) => {
                write!(f, "RANGE(")?;
                match &r.start {
                    Some(s) => write!(f, "{:?}", s)?,
                    None => write!(f, "NULL")?,
                }
                write!(f, ", ")?;
                match &r.end {
                    Some(e) => write!(f, "{:?}", e)?,
                    None => write!(f, "NULL")?,
                }
                write!(f, ")")
            }
            Value::Default => write!(f, "DEFAULT"),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Int64(v) => write!(f, "{}", v),
            Value::Float64(v) => write!(f, "{}", v),
            Value::Numeric(v) => write!(f, "{}", v),
            Value::BigNumeric(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "{}", v),
            Value::Bytes(v) => write!(f, "{}", hex::encode(v)),
            Value::Date(v) => write!(f, "{}", v),
            Value::Time(v) => write!(f, "{}", v),
            Value::DateTime(v) => write!(f, "{}", v),
            Value::Timestamp(v) => write!(f, "{}", v.format("%Y-%m-%d %H:%M:%S%.6f UTC")),
            Value::Json(v) => write!(f, "{}", v),
            Value::Array(v) => {
                write!(f, "[")?;
                for (i, elem) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", elem)?;
                }
                write!(f, "]")
            }
            Value::Struct(fields) => {
                write!(f, "{{")?;
                for (i, (name, val)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", name, val)?;
                }
                write!(f, "}}")
            }
            Value::Geography(v) => write!(f, "{}", v),
            Value::Interval(v) => write!(f, "{}-{} {}", v.months, v.days, v.nanos),
            Value::Range(r) => {
                write!(f, "[")?;
                match &r.start {
                    Some(s) => write!(f, "{}", s)?,
                    None => write!(f, "UNBOUNDED")?,
                }
                write!(f, ", ")?;
                match &r.end {
                    Some(e) => write!(f, "{}", e)?,
                    None => write!(f, "UNBOUNDED")?,
                }
                write!(f, ")")
            }
            Value::Default => write!(f, "DEFAULT"),
        }
    }
}

impl Eq for Value {}

fn hash_json_value<H: std::hash::Hasher>(value: &serde_json::Value, state: &mut H) {
    use std::hash::Hash;
    match value {
        serde_json::Value::Null => 0u8.hash(state),
        serde_json::Value::Bool(b) => {
            1u8.hash(state);
            b.hash(state);
        }
        serde_json::Value::Number(n) => {
            2u8.hash(state);
            if let Some(i) = n.as_i64() {
                i.hash(state);
            } else if let Some(u) = n.as_u64() {
                u.hash(state);
            } else if let Some(f) = n.as_f64() {
                f.to_bits().hash(state);
            }
        }
        serde_json::Value::String(s) => {
            3u8.hash(state);
            s.hash(state);
        }
        serde_json::Value::Array(arr) => {
            4u8.hash(state);
            for item in arr {
                hash_json_value(item, state);
            }
        }
        serde_json::Value::Object(obj) => {
            5u8.hash(state);
            for (k, v) in obj {
                k.hash(state);
                hash_json_value(v, state);
            }
        }
    }
}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Bool(v) => v.hash(state),
            Value::Int64(v) => v.hash(state),
            Value::Float64(v) => v.hash(state),
            Value::Numeric(v) => v.hash(state),
            Value::BigNumeric(v) => v.hash(state),
            Value::String(v) => v.hash(state),
            Value::Bytes(v) => v.hash(state),
            Value::Date(v) => v.hash(state),
            Value::Time(v) => v.hash(state),
            Value::DateTime(v) => v.hash(state),
            Value::Timestamp(v) => v.hash(state),
            Value::Json(v) => hash_json_value(v, state),
            Value::Array(v) => {
                for elem in v {
                    elem.hash(state);
                }
            }
            Value::Struct(fields) => {
                for (name, val) in fields {
                    name.hash(state);
                    val.hash(state);
                }
            }
            Value::Geography(v) => v.hash(state),
            Value::Interval(v) => {
                v.months.hash(state);
                v.days.hash(state);
                v.nanos.hash(state);
            }
            Value::Range(r) => r.hash(state),
            Value::Default => {}
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        if self.is_null() && other.is_null() {
            return Ordering::Equal;
        }
        if self.is_null() {
            return Ordering::Greater;
        }
        if other.is_null() {
            return Ordering::Less;
        }

        match (self, other) {
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
            (Value::Float64(a), Value::Float64(b)) => a.cmp(b),
            (Value::Int64(a), Value::Float64(b)) => ordered_float::OrderedFloat(*a as f64).cmp(b),
            (Value::Float64(a), Value::Int64(b)) => a.cmp(&ordered_float::OrderedFloat(*b as f64)),
            (Value::Numeric(a), Value::Numeric(b)) => a.cmp(b),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
            (Value::Date(a), Value::Date(b)) => a.cmp(b),
            (Value::Time(a), Value::Time(b)) => a.cmp(b),
            (Value::DateTime(a), Value::DateTime(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::{NaiveDate, NaiveTime, TimeZone};
    use rust_decimal::Decimal;

    use super::*;

    #[test]
    fn test_datatype_to_bq_type() {
        assert_eq!(DataType::Unknown.to_bq_type(), "STRING");
        assert_eq!(DataType::Bool.to_bq_type(), "BOOLEAN");
        assert_eq!(DataType::Int64.to_bq_type(), "INT64");
        assert_eq!(DataType::Float64.to_bq_type(), "FLOAT64");
        assert_eq!(DataType::Numeric(None).to_bq_type(), "NUMERIC");
        assert_eq!(DataType::Numeric(Some((10, 2))).to_bq_type(), "NUMERIC");
        assert_eq!(DataType::BigNumeric.to_bq_type(), "BIGNUMERIC");
        assert_eq!(DataType::String.to_bq_type(), "STRING");
        assert_eq!(DataType::Bytes.to_bq_type(), "BYTES");
        assert_eq!(DataType::Date.to_bq_type(), "DATE");
        assert_eq!(DataType::DateTime.to_bq_type(), "DATETIME");
        assert_eq!(DataType::Time.to_bq_type(), "TIME");
        assert_eq!(DataType::Timestamp.to_bq_type(), "TIMESTAMP");
        assert_eq!(DataType::Geography.to_bq_type(), "GEOGRAPHY");
        assert_eq!(DataType::Json.to_bq_type(), "JSON");
        assert_eq!(DataType::Struct(vec![]).to_bq_type(), "STRUCT");
        assert_eq!(
            DataType::Array(Box::new(DataType::Int64)).to_bq_type(),
            "ARRAY<INT64>"
        );
        assert_eq!(DataType::Interval.to_bq_type(), "INTERVAL");
        assert_eq!(
            DataType::Range(Box::new(DataType::Date)).to_bq_type(),
            "STRING"
        );
    }

    #[test]
    fn test_datatype_display() {
        assert_eq!(format!("{}", DataType::Unknown), "UNKNOWN");
        assert_eq!(format!("{}", DataType::Bool), "BOOL");
        assert_eq!(format!("{}", DataType::Int64), "INT64");
        assert_eq!(format!("{}", DataType::Float64), "FLOAT64");
        assert_eq!(format!("{}", DataType::Numeric(None)), "NUMERIC");
        assert_eq!(
            format!("{}", DataType::Numeric(Some((10, 2)))),
            "NUMERIC(10, 2)"
        );
        assert_eq!(format!("{}", DataType::BigNumeric), "BIGNUMERIC");
        assert_eq!(format!("{}", DataType::String), "STRING");
        assert_eq!(format!("{}", DataType::Bytes), "BYTES");
        assert_eq!(format!("{}", DataType::Date), "DATE");
        assert_eq!(format!("{}", DataType::DateTime), "DATETIME");
        assert_eq!(format!("{}", DataType::Time), "TIME");
        assert_eq!(format!("{}", DataType::Timestamp), "TIMESTAMP");
        assert_eq!(format!("{}", DataType::Geography), "GEOGRAPHY");
        assert_eq!(format!("{}", DataType::Json), "JSON");
        assert_eq!(format!("{}", DataType::Interval), "INTERVAL");
        assert_eq!(
            format!("{}", DataType::Array(Box::new(DataType::Int64))),
            "ARRAY<INT64>"
        );
        assert_eq!(
            format!("{}", DataType::Range(Box::new(DataType::Date))),
            "RANGE<DATE>"
        );
        let struct_type = DataType::Struct(vec![
            StructField {
                name: "a".to_string(),
                data_type: DataType::Int64,
            },
            StructField {
                name: "b".to_string(),
                data_type: DataType::String,
            },
        ]);
        assert_eq!(format!("{}", struct_type), "STRUCT<a INT64, b STRING>");
    }

    #[test]
    fn test_interval_value_new() {
        let interval = IntervalValue::new(1, 2, 3_000_000);
        assert_eq!(interval.months, 1);
        assert_eq!(interval.days, 2);
        assert_eq!(interval.nanos, 3_000_000 * IntervalValue::NANOS_PER_MICRO);
    }

    #[test]
    fn test_interval_value_from_months() {
        let interval = IntervalValue::from_months(12);
        assert_eq!(interval.months, 12);
        assert_eq!(interval.days, 0);
        assert_eq!(interval.nanos, 0);
    }

    #[test]
    fn test_interval_value_from_days() {
        let interval = IntervalValue::from_days(30);
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 30);
        assert_eq!(interval.nanos, 0);
    }

    #[test]
    fn test_interval_value_from_hours() {
        let interval = IntervalValue::from_hours(24);
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 0);
        assert_eq!(
            interval.nanos,
            24 * IntervalValue::MICROS_PER_HOUR * IntervalValue::NANOS_PER_MICRO
        );
    }

    #[test]
    fn test_range_value_new() {
        let range = RangeValue::new(Some(Value::Int64(1)), Some(Value::Int64(10)));
        assert_eq!(range.start(), Some(&Value::Int64(1)));
        assert_eq!(range.end(), Some(&Value::Int64(10)));
    }

    #[test]
    fn test_range_value_unbounded() {
        let range = RangeValue::new(None, None);
        assert_eq!(range.start(), None);
        assert_eq!(range.end(), None);
    }

    #[test]
    fn test_range_value_contains() {
        let range = RangeValue::new(Some(Value::Int64(1)), Some(Value::Int64(10)));
        assert!(range.contains(&Value::Int64(1)));
        assert!(range.contains(&Value::Int64(5)));
        assert!(range.contains(&Value::Int64(9)));
        assert!(!range.contains(&Value::Int64(0)));
        assert!(!range.contains(&Value::Int64(10)));
    }

    #[test]
    fn test_range_value_contains_unbounded() {
        let range = RangeValue::new(None, None);
        assert!(range.contains(&Value::Int64(1)));
        assert!(range.contains(&Value::Int64(-1000)));
        assert!(range.contains(&Value::Int64(1000)));
    }

    #[test]
    fn test_range_value_overlaps() {
        let range1 = RangeValue::new(Some(Value::Int64(1)), Some(Value::Int64(10)));
        let range2 = RangeValue::new(Some(Value::Int64(5)), Some(Value::Int64(15)));
        let range3 = RangeValue::new(Some(Value::Int64(10)), Some(Value::Int64(20)));
        let range4 = RangeValue::new(Some(Value::Int64(11)), Some(Value::Int64(20)));

        assert!(range1.overlaps(&range2));
        assert!(!range1.overlaps(&range3));
        assert!(!range1.overlaps(&range4));
    }

    #[test]
    fn test_range_value_intersect() {
        let range1 = RangeValue::new(Some(Value::Int64(1)), Some(Value::Int64(10)));
        let range2 = RangeValue::new(Some(Value::Int64(5)), Some(Value::Int64(15)));
        let range3 = RangeValue::new(Some(Value::Int64(10)), Some(Value::Int64(20)));

        let intersect = range1.intersect(&range2).unwrap();
        assert_eq!(intersect.start(), Some(&Value::Int64(5)));
        assert_eq!(intersect.end(), Some(&Value::Int64(10)));

        assert!(range1.intersect(&range3).is_none());
    }

    #[test]
    fn test_range_value_intersect_with_unbounded() {
        let range1 = RangeValue::new(Some(Value::Int64(5)), None);
        let range2 = RangeValue::new(None, Some(Value::Int64(10)));

        let intersect = range1.intersect(&range2).unwrap();
        assert_eq!(intersect.start(), Some(&Value::Int64(5)));
        assert_eq!(intersect.end(), Some(&Value::Int64(10)));
    }

    #[test]
    fn test_range_value_element_type() {
        let range1 = RangeValue::new(Some(Value::Int64(1)), None);
        assert_eq!(range1.element_type(), DataType::Int64);

        let range2 = RangeValue::new(
            None,
            Some(Value::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())),
        );
        assert_eq!(range2.element_type(), DataType::Date);

        let range3 = RangeValue::new(None, None);
        assert_eq!(range3.element_type(), DataType::Unknown);
    }

    #[test]
    fn test_value_constructors() {
        assert!(Value::null().is_null());
        assert_eq!(Value::bool_val(true), Value::Bool(true));
        assert_eq!(Value::int64(42), Value::Int64(42));
        assert_eq!(Value::float64(3.15).as_f64(), Some(3.15));
        assert_eq!(
            Value::numeric(Decimal::from_str("123.45").unwrap()),
            Value::Numeric(Decimal::from_str("123.45").unwrap())
        );
        assert_eq!(Value::string("hello"), Value::String("hello".to_string()));
        assert_eq!(Value::bytes(vec![1, 2, 3]), Value::Bytes(vec![1, 2, 3]));
        assert_eq!(
            Value::date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            Value::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
        );
        assert_eq!(
            Value::time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
            Value::Time(NaiveTime::from_hms_opt(12, 0, 0).unwrap())
        );
        let dt = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        assert_eq!(Value::datetime(dt), Value::DateTime(dt));
        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        assert_eq!(Value::timestamp(ts), Value::Timestamp(ts));
        assert_eq!(
            Value::json(serde_json::json!({"a": 1})),
            Value::Json(serde_json::json!({"a": 1}))
        );
        assert_eq!(
            Value::array(vec![Value::Int64(1), Value::Int64(2)]),
            Value::Array(vec![Value::Int64(1), Value::Int64(2)])
        );
        assert_eq!(
            Value::struct_val(vec![("a".to_string(), Value::Int64(1))]),
            Value::Struct(vec![("a".to_string(), Value::Int64(1))])
        );
        assert_eq!(
            Value::geography("POINT(0 0)"),
            Value::Geography("POINT(0 0)".to_string())
        );
        let interval = IntervalValue::from_months(12);
        assert_eq!(Value::interval(interval.clone()), Value::Interval(interval));
        assert_eq!(
            Value::interval_from_parts(1, 2, 3),
            Value::Interval(IntervalValue {
                months: 1,
                days: 2,
                nanos: 3
            })
        );
        let range = RangeValue::new(Some(Value::Int64(1)), Some(Value::Int64(10)));
        assert_eq!(
            Value::range(Some(Value::Int64(1)), Some(Value::Int64(10))),
            Value::Range(range.clone())
        );
        assert_eq!(Value::range_val(range.clone()), Value::Range(range));
    }

    #[test]
    fn test_value_data_type() {
        assert_eq!(Value::Null.data_type(), DataType::Unknown);
        assert_eq!(Value::Bool(true).data_type(), DataType::Bool);
        assert_eq!(Value::Int64(42).data_type(), DataType::Int64);
        assert_eq!(Value::float64(3.15).data_type(), DataType::Float64);
        assert_eq!(
            Value::Numeric(Decimal::from_str("123.45").unwrap()).data_type(),
            DataType::Numeric(None)
        );
        assert_eq!(
            Value::BigNumeric(Decimal::from_str("123.45").unwrap()).data_type(),
            DataType::BigNumeric
        );
        assert_eq!(
            Value::String("hello".to_string()).data_type(),
            DataType::String
        );
        assert_eq!(Value::Bytes(vec![1, 2, 3]).data_type(), DataType::Bytes);
        assert_eq!(
            Value::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()).data_type(),
            DataType::Date
        );
        assert_eq!(
            Value::Time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()).data_type(),
            DataType::Time
        );
        let dt = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        assert_eq!(Value::DateTime(dt).data_type(), DataType::DateTime);
        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        assert_eq!(Value::Timestamp(ts).data_type(), DataType::Timestamp);
        assert_eq!(
            Value::Json(serde_json::json!({})).data_type(),
            DataType::Json
        );
        assert_eq!(
            Value::Array(vec![Value::Int64(1)]).data_type(),
            DataType::Array(Box::new(DataType::Int64))
        );
        assert_eq!(
            Value::Array(vec![]).data_type(),
            DataType::Array(Box::new(DataType::Unknown))
        );
        assert_eq!(
            Value::Struct(vec![("a".to_string(), Value::Int64(1))]).data_type(),
            DataType::Struct(vec![StructField {
                name: "a".to_string(),
                data_type: DataType::Int64
            }])
        );
        assert_eq!(
            Value::Geography("POINT(0 0)".to_string()).data_type(),
            DataType::Geography
        );
        assert_eq!(
            Value::Interval(IntervalValue::from_months(12)).data_type(),
            DataType::Interval
        );
        let range = RangeValue::new(Some(Value::Int64(1)), Some(Value::Int64(10)));
        assert_eq!(
            Value::Range(range).data_type(),
            DataType::Range(Box::new(DataType::Int64))
        );
        assert_eq!(Value::Default.data_type(), DataType::Unknown);
    }

    #[test]
    fn test_value_as_accessors() {
        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert_eq!(Value::Bool(false).as_bool(), Some(false));
        assert_eq!(Value::Int64(42).as_bool(), None);

        assert_eq!(Value::Int64(42).as_i64(), Some(42));
        assert_eq!(Value::Bool(true).as_i64(), None);

        assert_eq!(Value::float64(3.15).as_f64(), Some(3.15));
        assert_eq!(Value::Int64(42).as_f64(), Some(42.0));
        assert_eq!(Value::Bool(true).as_f64(), None);

        let dec = Decimal::from_str("123.45").unwrap();
        assert_eq!(Value::Numeric(dec).as_numeric(), Some(dec));
        assert_eq!(Value::Numeric(dec).as_numeric_ref(), Some(&dec));
        assert_eq!(Value::Int64(42).as_numeric(), None);
        assert_eq!(Value::Int64(42).as_numeric_ref(), None);

        assert_eq!(Value::String("hello".to_string()).as_str(), Some("hello"));
        assert_eq!(Value::Int64(42).as_str(), None);

        assert_eq!(Value::Bytes(vec![1, 2, 3]).as_bytes(), Some(&[1, 2, 3][..]));
        assert_eq!(Value::Int64(42).as_bytes(), None);

        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert_eq!(Value::Date(date).as_date(), Some(date));
        assert_eq!(Value::Int64(42).as_date(), None);

        let time = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        assert_eq!(Value::Time(time).as_time(), Some(time));
        assert_eq!(Value::Int64(42).as_time(), None);

        let dt = date.and_hms_opt(12, 0, 0).unwrap();
        assert_eq!(Value::DateTime(dt).as_datetime(), Some(dt));
        assert_eq!(Value::Int64(42).as_datetime(), None);

        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        assert_eq!(Value::Timestamp(ts).as_timestamp(), Some(ts));
        assert_eq!(Value::Int64(42).as_timestamp(), None);

        let json = serde_json::json!({"a": 1});
        assert_eq!(Value::Json(json.clone()).as_json(), Some(&json));
        assert_eq!(Value::Int64(42).as_json(), None);

        let arr = vec![Value::Int64(1), Value::Int64(2)];
        assert_eq!(
            Value::Array(arr.clone()).as_array(),
            Some(&[Value::Int64(1), Value::Int64(2)][..])
        );
        assert_eq!(Value::Int64(42).as_array(), None);

        let fields = vec![("a".to_string(), Value::Int64(1))];
        assert_eq!(
            Value::Struct(fields.clone()).as_struct(),
            Some(&[("a".to_string(), Value::Int64(1))][..])
        );
        assert_eq!(Value::Int64(42).as_struct(), None);

        assert_eq!(
            Value::Geography("POINT(0 0)".to_string()).as_geography(),
            Some("POINT(0 0)")
        );
        assert_eq!(Value::Int64(42).as_geography(), None);

        let interval = IntervalValue::from_months(12);
        assert_eq!(
            Value::Interval(interval.clone()).as_interval(),
            Some(&interval)
        );
        assert_eq!(Value::Int64(42).as_interval(), None);

        let range = RangeValue::new(Some(Value::Int64(1)), Some(Value::Int64(10)));
        assert_eq!(Value::Range(range.clone()).as_range(), Some(&range));
        assert_eq!(Value::Int64(42).as_range(), None);
    }

    #[test]
    fn test_value_into_methods() {
        assert_eq!(
            Value::String("hello".to_string()).into_string(),
            Some("hello".to_string())
        );
        assert_eq!(Value::Int64(42).into_string(), None);

        assert_eq!(
            Value::Bytes(vec![1, 2, 3]).into_bytes(),
            Some(vec![1, 2, 3])
        );
        assert_eq!(Value::Int64(42).into_bytes(), None);

        assert_eq!(
            Value::Array(vec![Value::Int64(1), Value::Int64(2)]).into_array(),
            Some(vec![Value::Int64(1), Value::Int64(2)])
        );
        assert_eq!(Value::Int64(42).into_array(), None);
    }

    #[test]
    fn test_value_to_json() {
        assert_eq!(Value::Null.to_json(), serde_json::Value::Null);
        assert_eq!(Value::Bool(true).to_json(), serde_json::json!(true));
        assert_eq!(Value::Int64(42).to_json(), serde_json::json!(42));
        assert_eq!(Value::float64(3.15).to_json(), serde_json::json!(3.15));
        assert_eq!(
            Value::Numeric(Decimal::from_str("123.45").unwrap()).to_json(),
            serde_json::json!("123.45")
        );
        assert_eq!(
            Value::BigNumeric(Decimal::from_str("123.45").unwrap()).to_json(),
            serde_json::json!("123.45")
        );
        assert_eq!(
            Value::String("hello".to_string()).to_json(),
            serde_json::json!("hello")
        );
        assert_eq!(
            Value::Bytes(vec![1, 2, 3]).to_json(),
            serde_json::json!("AQID")
        );
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert_eq!(Value::Date(date).to_json(), serde_json::json!("2024-01-01"));
        let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        assert_eq!(Value::Time(time).to_json(), serde_json::json!("12:30:45"));
        let dt = date.and_hms_opt(12, 30, 45).unwrap();
        assert_eq!(
            Value::DateTime(dt).to_json(),
            serde_json::json!("2024-01-01 12:30:45")
        );
        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 12, 30, 45).unwrap();
        let ts_json = Value::Timestamp(ts).to_json();
        assert!(ts_json.as_str().unwrap().contains("2024-01-01"));
        let json = serde_json::json!({"a": 1});
        assert_eq!(Value::Json(json.clone()).to_json(), json);
        assert_eq!(
            Value::Array(vec![Value::Int64(1), Value::Int64(2)]).to_json(),
            serde_json::json!([1, 2])
        );
        assert_eq!(
            Value::Struct(vec![("a".to_string(), Value::Int64(1))]).to_json(),
            serde_json::json!({"a": 1})
        );
        assert_eq!(
            Value::Geography("POINT(0 0)".to_string()).to_json(),
            serde_json::json!("POINT(0 0)")
        );
        let interval = IntervalValue::from_months(12);
        let interval_json = Value::Interval(interval).to_json();
        assert!(interval_json.is_string());
        let range = RangeValue::new(Some(Value::Int64(1)), Some(Value::Int64(10)));
        let range_json = Value::Range(range).to_json();
        assert!(range_json.is_string());
        assert_eq!(Value::Default.to_json(), serde_json::Value::Null);
    }

    #[test]
    fn test_value_debug() {
        assert_eq!(format!("{:?}", Value::Null), "NULL");
        assert_eq!(format!("{:?}", Value::Bool(true)), "true");
        assert_eq!(format!("{:?}", Value::Int64(42)), "42");
        assert_eq!(format!("{:?}", Value::float64(3.15)), "3.15");
        assert_eq!(
            format!("{:?}", Value::Numeric(Decimal::from_str("123.45").unwrap())),
            "123.45"
        );
        assert_eq!(
            format!(
                "{:?}",
                Value::BigNumeric(Decimal::from_str("123.45").unwrap())
            ),
            "123.45"
        );
        assert_eq!(
            format!("{:?}", Value::String("hello".to_string())),
            "'hello'"
        );
        assert_eq!(format!("{:?}", Value::Bytes(vec![1, 2, 3])), "b'010203'");
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert_eq!(format!("{:?}", Value::Date(date)), "DATE '2024-01-01'");
        let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        assert_eq!(format!("{:?}", Value::Time(time)), "TIME '12:30:45'");
        let dt = date.and_hms_opt(12, 30, 45).unwrap();
        assert_eq!(
            format!("{:?}", Value::DateTime(dt)),
            "DATETIME '2024-01-01 12:30:45'"
        );
        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 12, 30, 45).unwrap();
        assert_eq!(
            format!("{:?}", Value::Timestamp(ts)),
            "TIMESTAMP '2024-01-01 12:30:45.000000 UTC'"
        );
        assert_eq!(
            format!("{:?}", Value::Json(serde_json::json!({"a": 1}))),
            "JSON '{\"a\":1}'"
        );
        assert_eq!(
            format!("{:?}", Value::Array(vec![Value::Int64(1), Value::Int64(2)])),
            "[1, 2]"
        );
        assert_eq!(
            format!(
                "{:?}",
                Value::Struct(vec![("a".to_string(), Value::Int64(1))])
            ),
            "STRUCT(a: 1)"
        );
        assert_eq!(
            format!("{:?}", Value::Geography("POINT(0 0)".to_string())),
            "GEOGRAPHY 'POINT(0 0)'"
        );
        let interval = IntervalValue {
            months: 1,
            days: 2,
            nanos: 3,
        };
        assert_eq!(
            format!("{:?}", Value::Interval(interval)),
            "INTERVAL 1 months 2 days 3 nanos"
        );
        let range = RangeValue::new(Some(Value::Int64(1)), Some(Value::Int64(10)));
        assert_eq!(format!("{:?}", Value::Range(range)), "RANGE(1, 10)");
        let range_unbounded = RangeValue::new(None, None);
        assert_eq!(
            format!("{:?}", Value::Range(range_unbounded)),
            "RANGE(NULL, NULL)"
        );
        assert_eq!(format!("{:?}", Value::Default), "DEFAULT");
    }

    #[test]
    fn test_value_display() {
        assert_eq!(format!("{}", Value::Null), "NULL");
        assert_eq!(format!("{}", Value::Bool(true)), "true");
        assert_eq!(format!("{}", Value::Int64(42)), "42");
        assert_eq!(format!("{}", Value::float64(3.15)), "3.15");
        assert_eq!(
            format!("{}", Value::Numeric(Decimal::from_str("123.45").unwrap())),
            "123.45"
        );
        assert_eq!(
            format!(
                "{}",
                Value::BigNumeric(Decimal::from_str("123.45").unwrap())
            ),
            "123.45"
        );
        assert_eq!(format!("{}", Value::String("hello".to_string())), "hello");
        assert_eq!(format!("{}", Value::Bytes(vec![1, 2, 3])), "010203");
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert_eq!(format!("{}", Value::Date(date)), "2024-01-01");
        let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        assert_eq!(format!("{}", Value::Time(time)), "12:30:45");
        let dt = date.and_hms_opt(12, 30, 45).unwrap();
        assert_eq!(format!("{}", Value::DateTime(dt)), "2024-01-01 12:30:45");
        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 12, 30, 45).unwrap();
        assert_eq!(
            format!("{}", Value::Timestamp(ts)),
            "2024-01-01 12:30:45.000000 UTC"
        );
        assert_eq!(
            format!("{}", Value::Json(serde_json::json!({"a": 1}))),
            "{\"a\":1}"
        );
        assert_eq!(
            format!("{}", Value::Array(vec![Value::Int64(1), Value::Int64(2)])),
            "[1, 2]"
        );
        assert_eq!(
            format!(
                "{}",
                Value::Struct(vec![("a".to_string(), Value::Int64(1))])
            ),
            "{a: 1}"
        );
        assert_eq!(
            format!("{}", Value::Geography("POINT(0 0)".to_string())),
            "POINT(0 0)"
        );
        let interval = IntervalValue {
            months: 1,
            days: 2,
            nanos: 3,
        };
        assert_eq!(format!("{}", Value::Interval(interval)), "1-2 3");
        let range = RangeValue::new(Some(Value::Int64(1)), Some(Value::Int64(10)));
        assert_eq!(format!("{}", Value::Range(range)), "[1, 10)");
        let range_unbounded = RangeValue::new(None, None);
        assert_eq!(
            format!("{}", Value::Range(range_unbounded)),
            "[UNBOUNDED, UNBOUNDED)"
        );
        assert_eq!(format!("{}", Value::Default), "DEFAULT");
    }

    #[test]
    fn test_value_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_value(v: &Value) -> u64 {
            let mut hasher = DefaultHasher::new();
            v.hash(&mut hasher);
            hasher.finish()
        }

        assert_eq!(hash_value(&Value::Null), hash_value(&Value::Null));
        assert_eq!(
            hash_value(&Value::Bool(true)),
            hash_value(&Value::Bool(true))
        );
        assert_eq!(hash_value(&Value::Int64(42)), hash_value(&Value::Int64(42)));
        assert_ne!(hash_value(&Value::Int64(42)), hash_value(&Value::Int64(43)));
        assert_eq!(
            hash_value(&Value::float64(3.15)),
            hash_value(&Value::float64(3.15))
        );
        assert_eq!(
            hash_value(&Value::String("hello".to_string())),
            hash_value(&Value::String("hello".to_string()))
        );
        assert_eq!(
            hash_value(&Value::Bytes(vec![1, 2, 3])),
            hash_value(&Value::Bytes(vec![1, 2, 3]))
        );
        let dec = Decimal::from_str("123.45").unwrap();
        assert_eq!(
            hash_value(&Value::Numeric(dec)),
            hash_value(&Value::Numeric(dec))
        );
        assert_eq!(
            hash_value(&Value::BigNumeric(dec)),
            hash_value(&Value::BigNumeric(dec))
        );
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert_eq!(
            hash_value(&Value::Date(date)),
            hash_value(&Value::Date(date))
        );
        let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        assert_eq!(
            hash_value(&Value::Time(time)),
            hash_value(&Value::Time(time))
        );
        let dt = date.and_hms_opt(12, 30, 45).unwrap();
        assert_eq!(
            hash_value(&Value::DateTime(dt)),
            hash_value(&Value::DateTime(dt))
        );
        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 12, 30, 45).unwrap();
        assert_eq!(
            hash_value(&Value::Timestamp(ts)),
            hash_value(&Value::Timestamp(ts))
        );
        let json = serde_json::json!({"a": 1});
        assert_eq!(
            hash_value(&Value::Json(json.clone())),
            hash_value(&Value::Json(json))
        );
        let arr = vec![Value::Int64(1), Value::Int64(2)];
        assert_eq!(
            hash_value(&Value::Array(arr.clone())),
            hash_value(&Value::Array(arr))
        );
        let fields = vec![("a".to_string(), Value::Int64(1))];
        assert_eq!(
            hash_value(&Value::Struct(fields.clone())),
            hash_value(&Value::Struct(fields))
        );
        assert_eq!(
            hash_value(&Value::Geography("POINT(0 0)".to_string())),
            hash_value(&Value::Geography("POINT(0 0)".to_string()))
        );
        let interval = IntervalValue::from_months(12);
        assert_eq!(
            hash_value(&Value::Interval(interval.clone())),
            hash_value(&Value::Interval(interval))
        );
        let range = RangeValue::new(Some(Value::Int64(1)), Some(Value::Int64(10)));
        assert_eq!(
            hash_value(&Value::Range(range.clone())),
            hash_value(&Value::Range(range))
        );
        assert_eq!(hash_value(&Value::Default), hash_value(&Value::Default));
    }

    #[test]
    fn test_value_ordering() {
        assert!(Value::Null > Value::Int64(i64::MAX));
        assert!(Value::Int64(i64::MAX) < Value::Null);
        assert_eq!(Value::Null, Value::Null);

        assert!(Value::Int64(1) < Value::Int64(2));
        assert!(Value::Int64(2) > Value::Int64(1));
        assert_eq!(Value::Int64(1), Value::Int64(1));

        assert!(Value::Bool(false) < Value::Bool(true));
        assert!(Value::Bool(true) > Value::Bool(false));

        assert!(Value::float64(1.0) < Value::float64(2.0));
        assert!(Value::float64(2.0) > Value::float64(1.0));

        assert!(Value::Int64(1) < Value::float64(2.0));
        assert!(Value::float64(2.0) > Value::Int64(1));

        let dec1 = Decimal::from_str("1.0").unwrap();
        let dec2 = Decimal::from_str("2.0").unwrap();
        assert!(Value::Numeric(dec1) < Value::Numeric(dec2));

        assert!(Value::String("a".to_string()) < Value::String("b".to_string()));

        assert!(Value::Bytes(vec![1]) < Value::Bytes(vec![2]));

        let date1 = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date2 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        assert!(Value::Date(date1) < Value::Date(date2));

        let time1 = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        let time2 = NaiveTime::from_hms_opt(13, 0, 0).unwrap();
        assert!(Value::Time(time1) < Value::Time(time2));

        let dt1 = date1.and_hms_opt(12, 0, 0).unwrap();
        let dt2 = date2.and_hms_opt(12, 0, 0).unwrap();
        assert!(Value::DateTime(dt1) < Value::DateTime(dt2));

        let ts1 = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 1, 2, 12, 0, 0).unwrap();
        assert!(Value::Timestamp(ts1) < Value::Timestamp(ts2));
    }

    #[test]
    fn test_value_default() {
        let default: Value = Default::default();
        assert!(default.is_null());
    }

    #[test]
    fn test_value_partial_ord() {
        let v1 = Value::Int64(1);
        let v2 = Value::Int64(2);
        assert_eq!(v1.partial_cmp(&v2), Some(std::cmp::Ordering::Less));
    }

    #[test]
    fn test_value_serde() {
        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int64(42),
            Value::float64(3.15),
            Value::Numeric(Decimal::from_str("123.45").unwrap()),
            Value::BigNumeric(Decimal::from_str("999.99").unwrap()),
            Value::String("hello".to_string()),
            Value::Bytes(vec![1, 2, 3]),
            Value::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            Value::Time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
            Value::datetime(
                NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(12, 0, 0)
                    .unwrap(),
            ),
            Value::timestamp(Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap()),
            Value::Json(serde_json::json!({"a": 1})),
            Value::Array(vec![Value::Int64(1), Value::Int64(2)]),
            Value::Struct(vec![("a".to_string(), Value::Int64(1))]),
            Value::Geography("POINT(0 0)".to_string()),
            Value::Interval(IntervalValue::from_months(12)),
            Value::range(Some(Value::Int64(1)), Some(Value::Int64(10))),
            Value::Default,
        ];

        for v in values {
            let json = serde_json::to_string(&v).unwrap();
            let deserialized: Value = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, v);
        }
    }

    #[test]
    fn test_datatype_serde() {
        let types = vec![
            DataType::Unknown,
            DataType::Bool,
            DataType::Int64,
            DataType::Float64,
            DataType::Numeric(None),
            DataType::Numeric(Some((10, 2))),
            DataType::BigNumeric,
            DataType::String,
            DataType::Bytes,
            DataType::Date,
            DataType::DateTime,
            DataType::Time,
            DataType::Timestamp,
            DataType::Geography,
            DataType::Json,
            DataType::Struct(vec![StructField {
                name: "a".to_string(),
                data_type: DataType::Int64,
            }]),
            DataType::Array(Box::new(DataType::Int64)),
            DataType::Interval,
            DataType::Range(Box::new(DataType::Date)),
        ];

        for dt in types {
            let json = serde_json::to_string(&dt).unwrap();
            let deserialized: DataType = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, dt);
        }
    }

    #[test]
    fn test_interval_value_serde() {
        let interval = IntervalValue::new(1, 2, 3_000_000);
        let json = serde_json::to_string(&interval).unwrap();
        let deserialized: IntervalValue = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, interval);
    }

    #[test]
    fn test_range_value_serde() {
        let range = RangeValue::new(Some(Value::Int64(1)), Some(Value::Int64(10)));
        let json = serde_json::to_string(&range).unwrap();
        let deserialized: RangeValue = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, range);
    }

    #[test]
    fn test_struct_field_serde() {
        let field = StructField {
            name: "test".to_string(),
            data_type: DataType::Int64,
        };
        let json = serde_json::to_string(&field).unwrap();
        let deserialized: StructField = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, field);
    }

    #[test]
    fn test_value_ordering_different_types() {
        let int = Value::Int64(10);
        let json = Value::Json(serde_json::json!({}));
        assert_eq!(int.cmp(&json), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_struct_display_empty() {
        let struct_type = DataType::Struct(vec![]);
        assert_eq!(format!("{}", struct_type), "STRUCT<>");
    }
}
