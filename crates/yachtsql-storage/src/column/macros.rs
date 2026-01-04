macro_rules! with_nulls {
    ($col:expr, | $nulls:ident | $body:expr) => {
        match $col {
            Column::Bool { nulls: $nulls, .. }
            | Column::Int64 { nulls: $nulls, .. }
            | Column::Float64 { nulls: $nulls, .. }
            | Column::Numeric { nulls: $nulls, .. }
            | Column::String { nulls: $nulls, .. }
            | Column::Bytes { nulls: $nulls, .. }
            | Column::Date { nulls: $nulls, .. }
            | Column::Time { nulls: $nulls, .. }
            | Column::DateTime { nulls: $nulls, .. }
            | Column::Timestamp { nulls: $nulls, .. }
            | Column::Json { nulls: $nulls, .. }
            | Column::Array { nulls: $nulls, .. }
            | Column::Struct { nulls: $nulls, .. }
            | Column::Geography { nulls: $nulls, .. }
            | Column::Interval { nulls: $nulls, .. }
            | Column::Range { nulls: $nulls, .. } => $body,
        }
    };
}

macro_rules! for_each_variant {
    ($col:expr, | $data:ident, $nulls:ident | $body:expr) => {
        match $col {
            Column::Bool {
                data: $data,
                nulls: $nulls,
            } => $body,
            Column::Int64 {
                data: $data,
                nulls: $nulls,
            } => $body,
            Column::Float64 {
                data: $data,
                nulls: $nulls,
            } => $body,
            Column::Numeric {
                data: $data,
                nulls: $nulls,
            } => $body,
            Column::String {
                data: $data,
                nulls: $nulls,
            } => $body,
            Column::Bytes {
                data: $data,
                nulls: $nulls,
            } => $body,
            Column::Date {
                data: $data,
                nulls: $nulls,
            } => $body,
            Column::Time {
                data: $data,
                nulls: $nulls,
            } => $body,
            Column::DateTime {
                data: $data,
                nulls: $nulls,
            } => $body,
            Column::Timestamp {
                data: $data,
                nulls: $nulls,
            } => $body,
            Column::Json {
                data: $data,
                nulls: $nulls,
            } => $body,
            Column::Array {
                data: $data,
                nulls: $nulls,
                ..
            } => $body,
            Column::Struct {
                data: $data,
                nulls: $nulls,
                ..
            } => $body,
            Column::Geography {
                data: $data,
                nulls: $nulls,
            } => $body,
            Column::Interval {
                data: $data,
                nulls: $nulls,
            } => $body,
            Column::Range {
                data: $data,
                nulls: $nulls,
                ..
            } => $body,
        }
    };
    ($col:expr, | $data:ident | $body:expr) => {
        match $col {
            Column::Bool { data: $data, .. } => $body,
            Column::Int64 { data: $data, .. } => $body,
            Column::Float64 { data: $data, .. } => $body,
            Column::Numeric { data: $data, .. } => $body,
            Column::String { data: $data, .. } => $body,
            Column::Bytes { data: $data, .. } => $body,
            Column::Date { data: $data, .. } => $body,
            Column::Time { data: $data, .. } => $body,
            Column::DateTime { data: $data, .. } => $body,
            Column::Timestamp { data: $data, .. } => $body,
            Column::Json { data: $data, .. } => $body,
            Column::Array { data: $data, .. } => $body,
            Column::Struct { data: $data, .. } => $body,
            Column::Geography { data: $data, .. } => $body,
            Column::Interval { data: $data, .. } => $body,
            Column::Range { data: $data, .. } => $body,
        }
    };
}
