#![coverage(off)]

use yachtsql_common::types::Value;

use super::Column;

impl Column {
    pub fn sum(&self) -> Option<f64> {
        match self {
            Column::Int64 { data, nulls } => {
                let mut sum: i64 = 0;
                let mut has_value = false;
                let chunks = data.chunks_exact(64);
                let remainder = chunks.remainder();

                for (&bitmap_word, chunk) in nulls.words().iter().zip(chunks) {
                    if bitmap_word == 0 {
                        sum += chunk.iter().sum::<i64>();
                        has_value = true;
                    } else if bitmap_word != u64::MAX {
                        let mut valid_mask = !bitmap_word;
                        while valid_mask != 0 {
                            let bit = valid_mask.trailing_zeros() as usize;
                            sum += chunk[bit];
                            has_value = true;
                            valid_mask &= valid_mask - 1;
                        }
                    }
                }

                if !remainder.is_empty() {
                    let last_word = nulls.words().last().copied().unwrap_or(0);
                    if last_word == 0 {
                        sum += remainder.iter().sum::<i64>();
                        has_value = true;
                    } else {
                        for (i, &val) in remainder.iter().enumerate() {
                            if (last_word >> i) & 1 == 0 {
                                sum += val;
                                has_value = true;
                            }
                        }
                    }
                }

                if has_value { Some(sum as f64) } else { None }
            }
            Column::Float64 { data, nulls } => {
                let mut sum: f64 = 0.0;
                let mut has_value = false;
                let chunks = data.chunks_exact(64);
                let remainder = chunks.remainder();

                for (&bitmap_word, chunk) in nulls.words().iter().zip(chunks) {
                    if bitmap_word == 0 {
                        sum += chunk.iter().sum::<f64>();
                        has_value = true;
                    } else if bitmap_word != u64::MAX {
                        let mut valid_mask = !bitmap_word;
                        while valid_mask != 0 {
                            let bit = valid_mask.trailing_zeros() as usize;
                            sum += chunk[bit];
                            has_value = true;
                            valid_mask &= valid_mask - 1;
                        }
                    }
                }

                if !remainder.is_empty() {
                    let last_word = nulls.words().last().copied().unwrap_or(0);
                    if last_word == 0 {
                        sum += remainder.iter().sum::<f64>();
                        has_value = true;
                    } else {
                        for (i, &val) in remainder.iter().enumerate() {
                            if (last_word >> i) & 1 == 0 {
                                sum += val;
                                has_value = true;
                            }
                        }
                    }
                }

                if has_value { Some(sum) } else { None }
            }
            Column::Numeric { data, nulls } => {
                use rust_decimal::prelude::ToPrimitive;
                let null_count = nulls.count_null();
                if null_count == data.len() {
                    None
                } else if null_count == 0 {
                    Some(data.iter().filter_map(|v| v.to_f64()).sum())
                } else {
                    let sum: f64 = data
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| !nulls.is_null(*i))
                        .filter_map(|(_, v)| v.to_f64())
                        .sum();
                    Some(sum)
                }
            }
            Column::Bool { .. }
            | Column::String { .. }
            | Column::Bytes { .. }
            | Column::Date { .. }
            | Column::Time { .. }
            | Column::DateTime { .. }
            | Column::Timestamp { .. }
            | Column::Json { .. }
            | Column::Array { .. }
            | Column::Struct { .. }
            | Column::Geography { .. }
            | Column::Interval { .. }
            | Column::Range { .. } => None,
        }
    }

    pub fn min(&self) -> Option<Value> {
        match self {
            Column::Int64 { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, &v)| v)
                .min()
                .map(Value::Int64),
            Column::Float64 { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, &v)| v)
                .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                .map(Value::float64),
            Column::Numeric { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .min()
                .cloned()
                .map(Value::Numeric),
            Column::String { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .min()
                .cloned()
                .map(Value::String),
            Column::Date { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .min()
                .copied()
                .map(Value::Date),
            Column::Time { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .min()
                .copied()
                .map(Value::Time),
            Column::DateTime { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .min()
                .copied()
                .map(Value::DateTime),
            Column::Timestamp { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .min()
                .copied()
                .map(Value::Timestamp),
            Column::Bool { .. }
            | Column::Bytes { .. }
            | Column::Json { .. }
            | Column::Array { .. }
            | Column::Struct { .. }
            | Column::Geography { .. }
            | Column::Interval { .. }
            | Column::Range { .. } => None,
        }
    }

    pub fn max(&self) -> Option<Value> {
        match self {
            Column::Int64 { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, &v)| v)
                .max()
                .map(Value::Int64),
            Column::Float64 { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, &v)| v)
                .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                .map(Value::float64),
            Column::Numeric { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .max()
                .cloned()
                .map(Value::Numeric),
            Column::String { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .max()
                .cloned()
                .map(Value::String),
            Column::Date { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .max()
                .copied()
                .map(Value::Date),
            Column::Time { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .max()
                .copied()
                .map(Value::Time),
            Column::DateTime { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .max()
                .copied()
                .map(Value::DateTime),
            Column::Timestamp { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .max()
                .copied()
                .map(Value::Timestamp),
            Column::Bool { .. }
            | Column::Bytes { .. }
            | Column::Json { .. }
            | Column::Array { .. }
            | Column::Struct { .. }
            | Column::Geography { .. }
            | Column::Interval { .. }
            | Column::Range { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use aligned_vec::AVec;
    use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
    use rust_decimal::Decimal;

    use super::*;
    use crate::NullBitmap;

    fn create_int64_column(values: Vec<i64>, null_indices: Vec<usize>) -> Column {
        let len = values.len();
        let mut nulls = NullBitmap::new_valid(len);
        for idx in null_indices {
            nulls.set_null(idx);
        }
        let mut data = AVec::new(64);
        for v in values {
            data.push(v);
        }
        Column::Int64 { data, nulls }
    }

    fn create_float64_column(values: Vec<f64>, null_indices: Vec<usize>) -> Column {
        let len = values.len();
        let mut nulls = NullBitmap::new_valid(len);
        for idx in null_indices {
            nulls.set_null(idx);
        }
        let mut data = AVec::new(64);
        for v in values {
            data.push(v);
        }
        Column::Float64 { data, nulls }
    }

    fn create_numeric_column(values: Vec<Decimal>, null_indices: Vec<usize>) -> Column {
        let len = values.len();
        let mut nulls = NullBitmap::new_valid(len);
        for idx in null_indices {
            nulls.set_null(idx);
        }
        Column::Numeric {
            data: values,
            nulls,
        }
    }

    fn create_string_column(values: Vec<String>, null_indices: Vec<usize>) -> Column {
        let len = values.len();
        let mut nulls = NullBitmap::new_valid(len);
        for idx in null_indices {
            nulls.set_null(idx);
        }
        Column::String {
            data: values,
            nulls,
        }
    }

    fn create_date_column(values: Vec<NaiveDate>, null_indices: Vec<usize>) -> Column {
        let len = values.len();
        let mut nulls = NullBitmap::new_valid(len);
        for idx in null_indices {
            nulls.set_null(idx);
        }
        Column::Date {
            data: values,
            nulls,
        }
    }

    fn create_time_column(values: Vec<NaiveTime>, null_indices: Vec<usize>) -> Column {
        let len = values.len();
        let mut nulls = NullBitmap::new_valid(len);
        for idx in null_indices {
            nulls.set_null(idx);
        }
        Column::Time {
            data: values,
            nulls,
        }
    }

    fn create_datetime_column(
        values: Vec<chrono::NaiveDateTime>,
        null_indices: Vec<usize>,
    ) -> Column {
        let len = values.len();
        let mut nulls = NullBitmap::new_valid(len);
        for idx in null_indices {
            nulls.set_null(idx);
        }
        Column::DateTime {
            data: values,
            nulls,
        }
    }

    fn create_timestamp_column(
        values: Vec<chrono::DateTime<Utc>>,
        null_indices: Vec<usize>,
    ) -> Column {
        let len = values.len();
        let mut nulls = NullBitmap::new_valid(len);
        for idx in null_indices {
            nulls.set_null(idx);
        }
        Column::Timestamp {
            data: values,
            nulls,
        }
    }

    fn create_bool_column(values: Vec<bool>, null_indices: Vec<usize>) -> Column {
        let len = values.len();
        let mut nulls = NullBitmap::new_valid(len);
        for idx in null_indices {
            nulls.set_null(idx);
        }
        Column::Bool {
            data: values,
            nulls,
        }
    }

    #[test]
    fn test_sum_int64_all_valid() {
        let col = create_int64_column(vec![1, 2, 3, 4, 5], vec![]);
        assert_eq!(col.sum(), Some(15.0));
    }

    #[test]
    fn test_sum_int64_with_nulls() {
        let col = create_int64_column(vec![1, 2, 3, 4, 5], vec![1, 3]);
        assert_eq!(col.sum(), Some(9.0));
    }

    #[test]
    fn test_sum_int64_all_nulls() {
        let col = create_int64_column(vec![1, 2, 3], vec![0, 1, 2]);
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_int64_empty() {
        let col = create_int64_column(vec![], vec![]);
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_int64_large_data_no_nulls() {
        let values: Vec<i64> = (1..=100).collect();
        let col = create_int64_column(values, vec![]);
        assert_eq!(col.sum(), Some(5050.0));
    }

    #[test]
    fn test_sum_int64_large_data_with_nulls() {
        let values: Vec<i64> = (1..=100).collect();
        let null_indices: Vec<usize> = (0..100).filter(|i| i % 2 == 0).collect();
        let col = create_int64_column(values, null_indices);
        let expected: f64 = (1..=100).filter(|i| i % 2 == 0).map(|i| i as f64).sum();
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_int64_all_nulls_in_chunk() {
        let values: Vec<i64> = vec![0; 64];
        let null_indices: Vec<usize> = (0..64).collect();
        let col = create_int64_column(values, null_indices);
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_int64_remainder_only() {
        let values: Vec<i64> = vec![1, 2, 3];
        let col = create_int64_column(values, vec![]);
        assert_eq!(col.sum(), Some(6.0));
    }

    #[test]
    fn test_sum_int64_remainder_with_nulls() {
        let values: Vec<i64> = vec![1, 2, 3, 4, 5];
        let col = create_int64_column(values, vec![2, 4]);
        assert_eq!(col.sum(), Some(7.0));
    }

    #[test]
    fn test_sum_int64_chunk_plus_remainder() {
        let values: Vec<i64> = (1..=70).collect();
        let col = create_int64_column(values, vec![]);
        let expected: f64 = (1..=70).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_int64_chunk_plus_remainder_with_nulls() {
        let values: Vec<i64> = (1..=70).collect();
        let null_indices = vec![0, 63, 65, 69];
        let expected: f64 = (1..=70)
            .enumerate()
            .filter(|(i, _)| !null_indices.contains(i))
            .map(|(_, v)| v as f64)
            .sum();
        let col = create_int64_column(values, null_indices.clone());
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_float64_all_valid() {
        let col = create_float64_column(vec![1.5, 2.5, 3.0], vec![]);
        assert_eq!(col.sum(), Some(7.0));
    }

    #[test]
    fn test_sum_float64_with_nulls() {
        let col = create_float64_column(vec![1.5, 2.5, 3.0], vec![1]);
        assert_eq!(col.sum(), Some(4.5));
    }

    #[test]
    fn test_sum_float64_all_nulls() {
        let col = create_float64_column(vec![1.0, 2.0], vec![0, 1]);
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_float64_empty() {
        let col = create_float64_column(vec![], vec![]);
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_float64_large_data_no_nulls() {
        let values: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let col = create_float64_column(values, vec![]);
        assert_eq!(col.sum(), Some(5050.0));
    }

    #[test]
    fn test_sum_float64_large_data_with_nulls() {
        let values: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let null_indices: Vec<usize> = (0..100).filter(|i| i % 3 == 0).collect();
        let col = create_float64_column(values, null_indices);
        let expected: f64 = (1..=100)
            .enumerate()
            .filter(|(i, _)| i % 3 != 0)
            .map(|(_, v)| v as f64)
            .sum();
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_float64_chunk_plus_remainder() {
        let values: Vec<f64> = (1..=70).map(|i| i as f64).collect();
        let col = create_float64_column(values, vec![]);
        let expected: f64 = (1..=70).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_float64_all_nulls_in_chunk() {
        let values: Vec<f64> = vec![0.0; 64];
        let null_indices: Vec<usize> = (0..64).collect();
        let col = create_float64_column(values, null_indices);
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_numeric_all_valid() {
        let values = vec![
            Decimal::new(15, 1),
            Decimal::new(25, 1),
            Decimal::new(30, 1),
        ];
        let col = create_numeric_column(values, vec![]);
        assert_eq!(col.sum(), Some(7.0));
    }

    #[test]
    fn test_sum_numeric_with_nulls() {
        let values = vec![
            Decimal::new(100, 0),
            Decimal::new(200, 0),
            Decimal::new(300, 0),
        ];
        let col = create_numeric_column(values, vec![1]);
        assert_eq!(col.sum(), Some(400.0));
    }

    #[test]
    fn test_sum_numeric_all_nulls() {
        let values = vec![Decimal::new(100, 0), Decimal::new(200, 0)];
        let col = create_numeric_column(values, vec![0, 1]);
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_numeric_empty() {
        let col = create_numeric_column(vec![], vec![]);
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_numeric_no_nulls() {
        let values = vec![
            Decimal::new(10, 0),
            Decimal::new(20, 0),
            Decimal::new(30, 0),
        ];
        let col = create_numeric_column(values, vec![]);
        assert_eq!(col.sum(), Some(60.0));
    }

    #[test]
    fn test_sum_bool_returns_none() {
        let col = create_bool_column(vec![true, false, true], vec![]);
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_string_returns_none() {
        let col = create_string_column(vec!["a".to_string(), "b".to_string()], vec![]);
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_min_int64_all_valid() {
        let col = create_int64_column(vec![5, 2, 8, 1, 9], vec![]);
        assert_eq!(col.min(), Some(Value::Int64(1)));
    }

    #[test]
    fn test_min_int64_with_nulls() {
        let col = create_int64_column(vec![5, 2, 8, 1, 9], vec![3]);
        assert_eq!(col.min(), Some(Value::Int64(2)));
    }

    #[test]
    fn test_min_int64_all_nulls() {
        let col = create_int64_column(vec![5, 2, 8], vec![0, 1, 2]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_min_int64_empty() {
        let col = create_int64_column(vec![], vec![]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_min_float64_all_valid() {
        let col = create_float64_column(vec![5.5, 2.2, 8.8, 1.1, 9.9], vec![]);
        assert_eq!(col.min(), Some(Value::float64(1.1)));
    }

    #[test]
    fn test_min_float64_with_nulls() {
        let col = create_float64_column(vec![5.5, 2.2, 8.8, 1.1, 9.9], vec![3]);
        assert_eq!(col.min(), Some(Value::float64(2.2)));
    }

    #[test]
    fn test_min_float64_all_nulls() {
        let col = create_float64_column(vec![5.5, 2.2], vec![0, 1]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_min_numeric_all_valid() {
        let values = vec![
            Decimal::new(55, 1),
            Decimal::new(22, 1),
            Decimal::new(11, 1),
        ];
        let col = create_numeric_column(values, vec![]);
        assert_eq!(col.min(), Some(Value::Numeric(Decimal::new(11, 1))));
    }

    #[test]
    fn test_min_numeric_with_nulls() {
        let values = vec![
            Decimal::new(55, 1),
            Decimal::new(22, 1),
            Decimal::new(11, 1),
        ];
        let col = create_numeric_column(values, vec![2]);
        assert_eq!(col.min(), Some(Value::Numeric(Decimal::new(22, 1))));
    }

    #[test]
    fn test_min_string_all_valid() {
        let col = create_string_column(
            vec![
                "charlie".to_string(),
                "alice".to_string(),
                "bob".to_string(),
            ],
            vec![],
        );
        assert_eq!(col.min(), Some(Value::String("alice".to_string())));
    }

    #[test]
    fn test_min_string_with_nulls() {
        let col = create_string_column(
            vec![
                "charlie".to_string(),
                "alice".to_string(),
                "bob".to_string(),
            ],
            vec![1],
        );
        assert_eq!(col.min(), Some(Value::String("bob".to_string())));
    }

    #[test]
    fn test_min_date_all_valid() {
        let values = vec![
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 10).unwrap(),
            NaiveDate::from_ymd_opt(2024, 5, 20).unwrap(),
        ];
        let col = create_date_column(values, vec![]);
        assert_eq!(
            col.min(),
            Some(Value::Date(NaiveDate::from_ymd_opt(2024, 1, 10).unwrap()))
        );
    }

    #[test]
    fn test_min_date_with_nulls() {
        let values = vec![
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 10).unwrap(),
            NaiveDate::from_ymd_opt(2024, 5, 20).unwrap(),
        ];
        let col = create_date_column(values, vec![1]);
        assert_eq!(
            col.min(),
            Some(Value::Date(NaiveDate::from_ymd_opt(2024, 3, 15).unwrap()))
        );
    }

    #[test]
    fn test_min_time_all_valid() {
        let values = vec![
            NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
            NaiveTime::from_hms_opt(8, 15, 0).unwrap(),
            NaiveTime::from_hms_opt(22, 45, 0).unwrap(),
        ];
        let col = create_time_column(values, vec![]);
        assert_eq!(
            col.min(),
            Some(Value::Time(NaiveTime::from_hms_opt(8, 15, 0).unwrap()))
        );
    }

    #[test]
    fn test_min_time_with_nulls() {
        let values = vec![
            NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
            NaiveTime::from_hms_opt(8, 15, 0).unwrap(),
            NaiveTime::from_hms_opt(22, 45, 0).unwrap(),
        ];
        let col = create_time_column(values, vec![1]);
        assert_eq!(
            col.min(),
            Some(Value::Time(NaiveTime::from_hms_opt(14, 30, 0).unwrap()))
        );
    }

    #[test]
    fn test_min_datetime_all_valid() {
        let values = vec![
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 10)
                .unwrap()
                .and_hms_opt(8, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 5, 20)
                .unwrap()
                .and_hms_opt(15, 0, 0)
                .unwrap(),
        ];
        let col = create_datetime_column(values, vec![]);
        assert_eq!(
            col.min(),
            Some(Value::DateTime(
                NaiveDate::from_ymd_opt(2024, 1, 10)
                    .unwrap()
                    .and_hms_opt(8, 0, 0)
                    .unwrap()
            ))
        );
    }

    #[test]
    fn test_min_timestamp_all_valid() {
        let values = vec![
            Utc.with_ymd_and_hms(2024, 3, 15, 10, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 1, 10, 8, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 5, 20, 15, 0, 0).unwrap(),
        ];
        let col = create_timestamp_column(values, vec![]);
        assert_eq!(
            col.min(),
            Some(Value::Timestamp(
                Utc.with_ymd_and_hms(2024, 1, 10, 8, 0, 0).unwrap()
            ))
        );
    }

    #[test]
    fn test_min_bool_returns_none() {
        let col = create_bool_column(vec![true, false, true], vec![]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_int64_all_valid() {
        let col = create_int64_column(vec![5, 2, 8, 1, 9], vec![]);
        assert_eq!(col.max(), Some(Value::Int64(9)));
    }

    #[test]
    fn test_max_int64_with_nulls() {
        let col = create_int64_column(vec![5, 2, 8, 1, 9], vec![4]);
        assert_eq!(col.max(), Some(Value::Int64(8)));
    }

    #[test]
    fn test_max_int64_all_nulls() {
        let col = create_int64_column(vec![5, 2, 8], vec![0, 1, 2]);
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_max_int64_empty() {
        let col = create_int64_column(vec![], vec![]);
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_max_float64_all_valid() {
        let col = create_float64_column(vec![5.5, 2.2, 8.8, 1.1, 9.9], vec![]);
        assert_eq!(col.max(), Some(Value::float64(9.9)));
    }

    #[test]
    fn test_max_float64_with_nulls() {
        let col = create_float64_column(vec![5.5, 2.2, 8.8, 1.1, 9.9], vec![4]);
        assert_eq!(col.max(), Some(Value::float64(8.8)));
    }

    #[test]
    fn test_max_float64_all_nulls() {
        let col = create_float64_column(vec![5.5, 2.2], vec![0, 1]);
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_max_numeric_all_valid() {
        let values = vec![
            Decimal::new(55, 1),
            Decimal::new(22, 1),
            Decimal::new(99, 1),
        ];
        let col = create_numeric_column(values, vec![]);
        assert_eq!(col.max(), Some(Value::Numeric(Decimal::new(99, 1))));
    }

    #[test]
    fn test_max_numeric_with_nulls() {
        let values = vec![
            Decimal::new(55, 1),
            Decimal::new(22, 1),
            Decimal::new(99, 1),
        ];
        let col = create_numeric_column(values, vec![2]);
        assert_eq!(col.max(), Some(Value::Numeric(Decimal::new(55, 1))));
    }

    #[test]
    fn test_max_string_all_valid() {
        let col = create_string_column(
            vec![
                "charlie".to_string(),
                "alice".to_string(),
                "bob".to_string(),
            ],
            vec![],
        );
        assert_eq!(col.max(), Some(Value::String("charlie".to_string())));
    }

    #[test]
    fn test_max_string_with_nulls() {
        let col = create_string_column(
            vec![
                "charlie".to_string(),
                "alice".to_string(),
                "bob".to_string(),
            ],
            vec![0],
        );
        assert_eq!(col.max(), Some(Value::String("bob".to_string())));
    }

    #[test]
    fn test_max_date_all_valid() {
        let values = vec![
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 10).unwrap(),
            NaiveDate::from_ymd_opt(2024, 5, 20).unwrap(),
        ];
        let col = create_date_column(values, vec![]);
        assert_eq!(
            col.max(),
            Some(Value::Date(NaiveDate::from_ymd_opt(2024, 5, 20).unwrap()))
        );
    }

    #[test]
    fn test_max_date_with_nulls() {
        let values = vec![
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 10).unwrap(),
            NaiveDate::from_ymd_opt(2024, 5, 20).unwrap(),
        ];
        let col = create_date_column(values, vec![2]);
        assert_eq!(
            col.max(),
            Some(Value::Date(NaiveDate::from_ymd_opt(2024, 3, 15).unwrap()))
        );
    }

    #[test]
    fn test_max_time_all_valid() {
        let values = vec![
            NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
            NaiveTime::from_hms_opt(8, 15, 0).unwrap(),
            NaiveTime::from_hms_opt(22, 45, 0).unwrap(),
        ];
        let col = create_time_column(values, vec![]);
        assert_eq!(
            col.max(),
            Some(Value::Time(NaiveTime::from_hms_opt(22, 45, 0).unwrap()))
        );
    }

    #[test]
    fn test_max_time_with_nulls() {
        let values = vec![
            NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
            NaiveTime::from_hms_opt(8, 15, 0).unwrap(),
            NaiveTime::from_hms_opt(22, 45, 0).unwrap(),
        ];
        let col = create_time_column(values, vec![2]);
        assert_eq!(
            col.max(),
            Some(Value::Time(NaiveTime::from_hms_opt(14, 30, 0).unwrap()))
        );
    }

    #[test]
    fn test_max_datetime_all_valid() {
        let values = vec![
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 10)
                .unwrap()
                .and_hms_opt(8, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 5, 20)
                .unwrap()
                .and_hms_opt(15, 0, 0)
                .unwrap(),
        ];
        let col = create_datetime_column(values, vec![]);
        assert_eq!(
            col.max(),
            Some(Value::DateTime(
                NaiveDate::from_ymd_opt(2024, 5, 20)
                    .unwrap()
                    .and_hms_opt(15, 0, 0)
                    .unwrap()
            ))
        );
    }

    #[test]
    fn test_max_timestamp_all_valid() {
        let values = vec![
            Utc.with_ymd_and_hms(2024, 3, 15, 10, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 1, 10, 8, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 5, 20, 15, 0, 0).unwrap(),
        ];
        let col = create_timestamp_column(values, vec![]);
        assert_eq!(
            col.max(),
            Some(Value::Timestamp(
                Utc.with_ymd_and_hms(2024, 5, 20, 15, 0, 0).unwrap()
            ))
        );
    }

    #[test]
    fn test_max_bool_returns_none() {
        let col = create_bool_column(vec![true, false, true], vec![]);
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_sum_int64_negative_values() {
        let col = create_int64_column(vec![-5, 10, -3, 8], vec![]);
        assert_eq!(col.sum(), Some(10.0));
    }

    #[test]
    fn test_sum_float64_negative_values() {
        let col = create_float64_column(vec![-5.5, 10.0, -3.5, 8.0], vec![]);
        assert_eq!(col.sum(), Some(9.0));
    }

    #[test]
    fn test_min_int64_negative_values() {
        let col = create_int64_column(vec![5, -2, 8, -10, 3], vec![]);
        assert_eq!(col.min(), Some(Value::Int64(-10)));
    }

    #[test]
    fn test_max_int64_negative_values() {
        let col = create_int64_column(vec![-5, -2, -8, -10, -3], vec![]);
        assert_eq!(col.max(), Some(Value::Int64(-2)));
    }

    #[test]
    fn test_sum_int64_single_element() {
        let col = create_int64_column(vec![42], vec![]);
        assert_eq!(col.sum(), Some(42.0));
    }

    #[test]
    fn test_min_int64_single_element() {
        let col = create_int64_column(vec![42], vec![]);
        assert_eq!(col.min(), Some(Value::Int64(42)));
    }

    #[test]
    fn test_max_int64_single_element() {
        let col = create_int64_column(vec![42], vec![]);
        assert_eq!(col.max(), Some(Value::Int64(42)));
    }

    #[test]
    fn test_sum_int64_single_element_null() {
        let col = create_int64_column(vec![42], vec![0]);
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_min_int64_single_element_null() {
        let col = create_int64_column(vec![42], vec![0]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_int64_single_element_null() {
        let col = create_int64_column(vec![42], vec![0]);
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_min_numeric_all_nulls() {
        let values = vec![Decimal::new(100, 0), Decimal::new(200, 0)];
        let col = create_numeric_column(values, vec![0, 1]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_numeric_all_nulls() {
        let values = vec![Decimal::new(100, 0), Decimal::new(200, 0)];
        let col = create_numeric_column(values, vec![0, 1]);
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_min_string_all_nulls() {
        let col = create_string_column(vec!["a".to_string(), "b".to_string()], vec![0, 1]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_string_all_nulls() {
        let col = create_string_column(vec!["a".to_string(), "b".to_string()], vec![0, 1]);
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_min_date_all_nulls() {
        let values = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
        ];
        let col = create_date_column(values, vec![0, 1]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_date_all_nulls() {
        let values = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
        ];
        let col = create_date_column(values, vec![0, 1]);
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_sum_int64_exactly_64_elements_no_nulls() {
        let values: Vec<i64> = (1..=64).collect();
        let col = create_int64_column(values, vec![]);
        let expected: f64 = (1..=64).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_int64_exactly_64_elements_all_nulls() {
        let values: Vec<i64> = (1..=64).collect();
        let null_indices: Vec<usize> = (0..64).collect();
        let col = create_int64_column(values, null_indices);
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_float64_exactly_64_elements_no_nulls() {
        let values: Vec<f64> = (1..=64).map(|i| i as f64).collect();
        let col = create_float64_column(values, vec![]);
        let expected: f64 = (1..=64).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_float64_exactly_64_elements_all_nulls() {
        let values: Vec<f64> = (1..=64).map(|i| i as f64).collect();
        let null_indices: Vec<usize> = (0..64).collect();
        let col = create_float64_column(values, null_indices);
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_int64_128_elements_with_sparse_nulls() {
        let values: Vec<i64> = (1..=128).collect();
        let null_indices = vec![0, 32, 64, 96, 127];
        let expected: f64 = (1..=128)
            .enumerate()
            .filter(|(i, _)| !null_indices.contains(i))
            .map(|(_, v)| v as f64)
            .sum();
        let col = create_int64_column(values, null_indices);
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_float64_128_elements_with_sparse_nulls() {
        let values: Vec<f64> = (1..=128).map(|i| i as f64).collect();
        let null_indices = vec![0, 32, 64, 96, 127];
        let expected: f64 = (1..=128)
            .enumerate()
            .filter(|(i, _)| !null_indices.contains(i))
            .map(|(_, v)| v as f64)
            .sum();
        let col = create_float64_column(values, null_indices);
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_min_time_all_nulls() {
        let values = vec![
            NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
            NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        ];
        let col = create_time_column(values, vec![0, 1]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_time_all_nulls() {
        let values = vec![
            NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
            NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        ];
        let col = create_time_column(values, vec![0, 1]);
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_min_datetime_all_nulls() {
        let values = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ];
        let col = create_datetime_column(values, vec![0, 1]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_datetime_all_nulls() {
        let values = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ];
        let col = create_datetime_column(values, vec![0, 1]);
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_min_timestamp_all_nulls() {
        let values = vec![
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap(),
        ];
        let col = create_timestamp_column(values, vec![0, 1]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_timestamp_all_nulls() {
        let values = vec![
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap(),
        ];
        let col = create_timestamp_column(values, vec![0, 1]);
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_min_datetime_with_nulls() {
        let values = vec![
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 10)
                .unwrap()
                .and_hms_opt(8, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 5, 20)
                .unwrap()
                .and_hms_opt(15, 0, 0)
                .unwrap(),
        ];
        let col = create_datetime_column(values, vec![1]);
        assert_eq!(
            col.min(),
            Some(Value::DateTime(
                NaiveDate::from_ymd_opt(2024, 3, 15)
                    .unwrap()
                    .and_hms_opt(10, 0, 0)
                    .unwrap()
            ))
        );
    }

    #[test]
    fn test_max_datetime_with_nulls() {
        let values = vec![
            NaiveDate::from_ymd_opt(2024, 3, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 10)
                .unwrap()
                .and_hms_opt(8, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 5, 20)
                .unwrap()
                .and_hms_opt(15, 0, 0)
                .unwrap(),
        ];
        let col = create_datetime_column(values, vec![2]);
        assert_eq!(
            col.max(),
            Some(Value::DateTime(
                NaiveDate::from_ymd_opt(2024, 3, 15)
                    .unwrap()
                    .and_hms_opt(10, 0, 0)
                    .unwrap()
            ))
        );
    }

    #[test]
    fn test_min_timestamp_with_nulls() {
        let values = vec![
            Utc.with_ymd_and_hms(2024, 3, 15, 10, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 1, 10, 8, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 5, 20, 15, 0, 0).unwrap(),
        ];
        let col = create_timestamp_column(values, vec![1]);
        assert_eq!(
            col.min(),
            Some(Value::Timestamp(
                Utc.with_ymd_and_hms(2024, 3, 15, 10, 0, 0).unwrap()
            ))
        );
    }

    #[test]
    fn test_max_timestamp_with_nulls() {
        let values = vec![
            Utc.with_ymd_and_hms(2024, 3, 15, 10, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 1, 10, 8, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 5, 20, 15, 0, 0).unwrap(),
        ];
        let col = create_timestamp_column(values, vec![2]);
        assert_eq!(
            col.max(),
            Some(Value::Timestamp(
                Utc.with_ymd_and_hms(2024, 3, 15, 10, 0, 0).unwrap()
            ))
        );
    }

    #[test]
    fn test_min_string_empty() {
        let col = create_string_column(vec![], vec![]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_string_empty() {
        let col = create_string_column(vec![], vec![]);
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_min_numeric_empty() {
        let col = create_numeric_column(vec![], vec![]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_numeric_empty() {
        let col = create_numeric_column(vec![], vec![]);
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_min_date_empty() {
        let col = create_date_column(vec![], vec![]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_date_empty() {
        let col = create_date_column(vec![], vec![]);
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_min_time_empty() {
        let col = create_time_column(vec![], vec![]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_time_empty() {
        let col = create_time_column(vec![], vec![]);
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_min_datetime_empty() {
        let col = create_datetime_column(vec![], vec![]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_datetime_empty() {
        let col = create_datetime_column(vec![], vec![]);
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_min_timestamp_empty() {
        let col = create_timestamp_column(vec![], vec![]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_timestamp_empty() {
        let col = create_timestamp_column(vec![], vec![]);
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_min_float64_empty() {
        let col = create_float64_column(vec![], vec![]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_float64_empty() {
        let col = create_float64_column(vec![], vec![]);
        assert_eq!(col.max(), None);
    }

    fn create_bytes_column(values: Vec<Vec<u8>>, null_indices: Vec<usize>) -> Column {
        let len = values.len();
        let mut nulls = NullBitmap::new_valid(len);
        for idx in null_indices {
            nulls.set_null(idx);
        }
        Column::Bytes {
            data: values,
            nulls,
        }
    }

    fn create_json_column(values: Vec<serde_json::Value>, null_indices: Vec<usize>) -> Column {
        let len = values.len();
        let mut nulls = NullBitmap::new_valid(len);
        for idx in null_indices {
            nulls.set_null(idx);
        }
        Column::Json {
            data: values,
            nulls,
        }
    }

    fn create_array_column(values: Vec<Vec<Value>>, null_indices: Vec<usize>) -> Column {
        let len = values.len();
        let mut nulls = NullBitmap::new_valid(len);
        for idx in null_indices {
            nulls.set_null(idx);
        }
        Column::Array {
            data: values,
            nulls,
            element_type: yachtsql_common::types::DataType::Int64,
        }
    }

    fn create_struct_column(values: Vec<Vec<(String, Value)>>, null_indices: Vec<usize>) -> Column {
        let len = values.len();
        let mut nulls = NullBitmap::new_valid(len);
        for idx in null_indices {
            nulls.set_null(idx);
        }
        Column::Struct {
            data: values,
            nulls,
            fields: vec![("field".to_string(), yachtsql_common::types::DataType::Int64)],
        }
    }

    fn create_geography_column(values: Vec<String>, null_indices: Vec<usize>) -> Column {
        let len = values.len();
        let mut nulls = NullBitmap::new_valid(len);
        for idx in null_indices {
            nulls.set_null(idx);
        }
        Column::Geography {
            data: values,
            nulls,
        }
    }

    fn create_interval_column(
        values: Vec<yachtsql_common::types::IntervalValue>,
        null_indices: Vec<usize>,
    ) -> Column {
        let len = values.len();
        let mut nulls = NullBitmap::new_valid(len);
        for idx in null_indices {
            nulls.set_null(idx);
        }
        Column::Interval {
            data: values,
            nulls,
        }
    }

    fn create_range_column(
        values: Vec<yachtsql_common::types::RangeValue>,
        null_indices: Vec<usize>,
    ) -> Column {
        let len = values.len();
        let mut nulls = NullBitmap::new_valid(len);
        for idx in null_indices {
            nulls.set_null(idx);
        }
        Column::Range {
            data: values,
            nulls,
            element_type: yachtsql_common::types::DataType::Int64,
        }
    }

    #[test]
    fn test_sum_bytes_returns_none() {
        let col = create_bytes_column(vec![vec![1, 2], vec![3, 4]], vec![]);
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_json_returns_none() {
        let col = create_json_column(
            vec![serde_json::json!({"a": 1}), serde_json::json!({"b": 2})],
            vec![],
        );
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_array_returns_none() {
        let col = create_array_column(vec![vec![Value::Int64(1)], vec![Value::Int64(2)]], vec![]);
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_struct_returns_none() {
        let col = create_struct_column(
            vec![
                vec![("field".to_string(), Value::Int64(1))],
                vec![("field".to_string(), Value::Int64(2))],
            ],
            vec![],
        );
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_geography_returns_none() {
        let col = create_geography_column(
            vec!["POINT(0 0)".to_string(), "POINT(1 1)".to_string()],
            vec![],
        );
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_interval_returns_none() {
        let col = create_interval_column(
            vec![
                yachtsql_common::types::IntervalValue::from_months(12),
                yachtsql_common::types::IntervalValue::from_months(24),
            ],
            vec![],
        );
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_range_returns_none() {
        let col = create_range_column(
            vec![yachtsql_common::types::RangeValue::new(
                Some(Value::Int64(1)),
                Some(Value::Int64(10)),
            )],
            vec![],
        );
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_date_returns_none() {
        let col = create_date_column(
            vec![
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
            ],
            vec![],
        );
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_time_returns_none() {
        let col = create_time_column(
            vec![
                NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
                NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
            ],
            vec![],
        );
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_datetime_returns_none() {
        let col = create_datetime_column(
            vec![
                NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2024, 2, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ],
            vec![],
        );
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_timestamp_returns_none() {
        let col = create_timestamp_column(
            vec![
                Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
                Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap(),
            ],
            vec![],
        );
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_min_bytes_returns_none() {
        let col = create_bytes_column(vec![vec![1, 2], vec![3, 4]], vec![]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_bytes_returns_none() {
        let col = create_bytes_column(vec![vec![1, 2], vec![3, 4]], vec![]);
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_min_json_returns_none() {
        let col = create_json_column(
            vec![serde_json::json!({"a": 1}), serde_json::json!({"b": 2})],
            vec![],
        );
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_json_returns_none() {
        let col = create_json_column(
            vec![serde_json::json!({"a": 1}), serde_json::json!({"b": 2})],
            vec![],
        );
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_min_array_returns_none() {
        let col = create_array_column(vec![vec![Value::Int64(1)], vec![Value::Int64(2)]], vec![]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_array_returns_none() {
        let col = create_array_column(vec![vec![Value::Int64(1)], vec![Value::Int64(2)]], vec![]);
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_min_struct_returns_none() {
        let col = create_struct_column(
            vec![
                vec![("field".to_string(), Value::Int64(1))],
                vec![("field".to_string(), Value::Int64(2))],
            ],
            vec![],
        );
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_struct_returns_none() {
        let col = create_struct_column(
            vec![
                vec![("field".to_string(), Value::Int64(1))],
                vec![("field".to_string(), Value::Int64(2))],
            ],
            vec![],
        );
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_min_geography_returns_none() {
        let col = create_geography_column(
            vec!["POINT(0 0)".to_string(), "POINT(1 1)".to_string()],
            vec![],
        );
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_geography_returns_none() {
        let col = create_geography_column(
            vec!["POINT(0 0)".to_string(), "POINT(1 1)".to_string()],
            vec![],
        );
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_min_interval_returns_none() {
        let col = create_interval_column(
            vec![
                yachtsql_common::types::IntervalValue::from_months(12),
                yachtsql_common::types::IntervalValue::from_months(24),
            ],
            vec![],
        );
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_interval_returns_none() {
        let col = create_interval_column(
            vec![
                yachtsql_common::types::IntervalValue::from_months(12),
                yachtsql_common::types::IntervalValue::from_months(24),
            ],
            vec![],
        );
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_min_range_returns_none() {
        let col = create_range_column(
            vec![yachtsql_common::types::RangeValue::new(
                Some(Value::Int64(1)),
                Some(Value::Int64(10)),
            )],
            vec![],
        );
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_range_returns_none() {
        let col = create_range_column(
            vec![yachtsql_common::types::RangeValue::new(
                Some(Value::Int64(1)),
                Some(Value::Int64(10)),
            )],
            vec![],
        );
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_sum_int64_chunk_with_all_nulls_followed_by_valid_remainder() {
        let mut values: Vec<i64> = vec![0; 64];
        values.extend((1..=6).collect::<Vec<i64>>());
        let null_indices: Vec<usize> = (0..64).collect();
        let col = create_int64_column(values, null_indices);
        assert_eq!(col.sum(), Some(21.0));
    }

    #[test]
    fn test_sum_float64_chunk_with_all_nulls_followed_by_valid_remainder() {
        let mut values: Vec<f64> = vec![0.0; 64];
        values.extend((1..=6).map(|i| i as f64).collect::<Vec<f64>>());
        let null_indices: Vec<usize> = (0..64).collect();
        let col = create_float64_column(values, null_indices);
        assert_eq!(col.sum(), Some(21.0));
    }

    #[test]
    fn test_sum_int64_multiple_chunks_mixed_validity() {
        let values: Vec<i64> = (1..=192).collect();
        let null_indices: Vec<usize> = (0..64).collect();
        let col = create_int64_column(values, null_indices);
        let expected: f64 = (65..=192).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_float64_multiple_chunks_mixed_validity() {
        let values: Vec<f64> = (1..=192).map(|i| i as f64).collect();
        let null_indices: Vec<usize> = (0..64).collect();
        let col = create_float64_column(values, null_indices);
        let expected: f64 = (65..=192).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_int64_remainder_all_nulls() {
        let values: Vec<i64> = (1..=70).collect();
        let null_indices: Vec<usize> = (64..70).collect();
        let col = create_int64_column(values, null_indices);
        let expected: f64 = (1..=64).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_float64_remainder_all_nulls() {
        let values: Vec<f64> = (1..=70).map(|i| i as f64).collect();
        let null_indices: Vec<usize> = (64..70).collect();
        let col = create_float64_column(values, null_indices);
        let expected: f64 = (1..=64).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_int64_only_remainder_all_nulls() {
        let values: Vec<i64> = vec![1, 2, 3, 4, 5];
        let null_indices: Vec<usize> = vec![0, 1, 2, 3, 4];
        let col = create_int64_column(values, null_indices);
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_float64_only_remainder_all_nulls() {
        let values: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let null_indices: Vec<usize> = vec![0, 1, 2, 3, 4];
        let col = create_float64_column(values, null_indices);
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_int64_two_chunks_all_valid() {
        let values: Vec<i64> = (1..=128).collect();
        let col = create_int64_column(values, vec![]);
        let expected: f64 = (1..=128).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_float64_two_chunks_all_valid() {
        let values: Vec<f64> = (1..=128).map(|i| i as f64).collect();
        let col = create_float64_column(values, vec![]);
        let expected: f64 = (1..=128).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_int64_two_chunks_all_nulls() {
        let values: Vec<i64> = (1..=128).collect();
        let null_indices: Vec<usize> = (0..128).collect();
        let col = create_int64_column(values, null_indices);
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_float64_two_chunks_all_nulls() {
        let values: Vec<f64> = (1..=128).map(|i| i as f64).collect();
        let null_indices: Vec<usize> = (0..128).collect();
        let col = create_float64_column(values, null_indices);
        assert_eq!(col.sum(), None);
    }

    #[test]
    fn test_sum_int64_alternating_nulls_in_chunk() {
        let values: Vec<i64> = (1..=64).collect();
        let null_indices: Vec<usize> = (0..64).filter(|i| i % 2 == 0).collect();
        let col = create_int64_column(values, null_indices);
        let expected: f64 = (1..=64)
            .enumerate()
            .filter(|(i, _)| i % 2 != 0)
            .map(|(_, v)| v as f64)
            .sum();
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_float64_alternating_nulls_in_chunk() {
        let values: Vec<f64> = (1..=64).map(|i| i as f64).collect();
        let null_indices: Vec<usize> = (0..64).filter(|i| i % 2 == 0).collect();
        let col = create_float64_column(values, null_indices);
        let expected: f64 = (1..=64)
            .enumerate()
            .filter(|(i, _)| i % 2 != 0)
            .map(|(_, v)| v as f64)
            .sum();
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_int64_single_null_in_chunk() {
        let values: Vec<i64> = (1..=64).collect();
        let col = create_int64_column(values, vec![31]);
        let expected: f64 = (1..=64).sum::<i64>() as f64 - 32.0;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_float64_single_null_in_chunk() {
        let values: Vec<f64> = (1..=64).map(|i| i as f64).collect();
        let col = create_float64_column(values, vec![31]);
        let expected: f64 = (1..=64).sum::<i64>() as f64 - 32.0;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_int64_last_element_of_chunk_null() {
        let values: Vec<i64> = (1..=64).collect();
        let col = create_int64_column(values, vec![63]);
        let expected: f64 = (1..=63).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_float64_last_element_of_chunk_null() {
        let values: Vec<f64> = (1..=64).map(|i| i as f64).collect();
        let col = create_float64_column(values, vec![63]);
        let expected: f64 = (1..=63).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_int64_first_element_of_chunk_null() {
        let values: Vec<i64> = (1..=64).collect();
        let col = create_int64_column(values, vec![0]);
        let expected: f64 = (2..=64).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_float64_first_element_of_chunk_null() {
        let values: Vec<f64> = (1..=64).map(|i| i as f64).collect();
        let col = create_float64_column(values, vec![0]);
        let expected: f64 = (2..=64).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_int64_65_elements_with_null_at_65() {
        let values: Vec<i64> = (1..=65).collect();
        let col = create_int64_column(values, vec![64]);
        let expected: f64 = (1..=64).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_float64_65_elements_with_null_at_65() {
        let values: Vec<f64> = (1..=65).map(|i| i as f64).collect();
        let col = create_float64_column(values, vec![64]);
        let expected: f64 = (1..=64).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_int64_128_plus_remainder_all_valid() {
        let values: Vec<i64> = (1..=135).collect();
        let col = create_int64_column(values, vec![]);
        let expected: f64 = (1..=135).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_float64_128_plus_remainder_all_valid() {
        let values: Vec<f64> = (1..=135).map(|i| i as f64).collect();
        let col = create_float64_column(values, vec![]);
        let expected: f64 = (1..=135).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_int64_three_full_chunks() {
        let values: Vec<i64> = (1..=192).collect();
        let col = create_int64_column(values, vec![]);
        let expected: f64 = (1..=192).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_float64_three_full_chunks() {
        let values: Vec<f64> = (1..=192).map(|i| i as f64).collect();
        let col = create_float64_column(values, vec![]);
        let expected: f64 = (1..=192).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_int64_second_chunk_all_nulls() {
        let values: Vec<i64> = (1..=128).collect();
        let null_indices: Vec<usize> = (64..128).collect();
        let col = create_int64_column(values, null_indices);
        let expected: f64 = (1..=64).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_float64_second_chunk_all_nulls() {
        let values: Vec<f64> = (1..=128).map(|i| i as f64).collect();
        let null_indices: Vec<usize> = (64..128).collect();
        let col = create_float64_column(values, null_indices);
        let expected: f64 = (1..=64).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_int64_first_chunk_all_nulls_second_valid() {
        let values: Vec<i64> = (1..=128).collect();
        let null_indices: Vec<usize> = (0..64).collect();
        let col = create_int64_column(values, null_indices);
        let expected: f64 = (65..=128).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_float64_first_chunk_all_nulls_second_valid() {
        let values: Vec<f64> = (1..=128).map(|i| i as f64).collect();
        let null_indices: Vec<usize> = (0..64).collect();
        let col = create_float64_column(values, null_indices);
        let expected: f64 = (65..=128).sum::<i64>() as f64;
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_int64_sparse_nulls_across_multiple_chunks() {
        let values: Vec<i64> = (1..=200).collect();
        let null_indices = vec![5, 10, 50, 70, 100, 130, 150, 180, 199];
        let col = create_int64_column(values.clone(), null_indices.clone());
        let expected: f64 = values
            .iter()
            .enumerate()
            .filter(|(i, _)| !null_indices.contains(i))
            .map(|(_, &v)| v as f64)
            .sum();
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_float64_sparse_nulls_across_multiple_chunks() {
        let values: Vec<f64> = (1..=200).map(|i| i as f64).collect();
        let null_indices = vec![5, 10, 50, 70, 100, 130, 150, 180, 199];
        let col = create_float64_column(values.clone(), null_indices.clone());
        let expected: f64 = values
            .iter()
            .enumerate()
            .filter(|(i, _)| !null_indices.contains(i))
            .map(|(_, &v)| v)
            .sum();
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_int64_chunk_boundary_nulls() {
        let values: Vec<i64> = (1..=70).collect();
        let null_indices = vec![62, 63, 64, 65];
        let col = create_int64_column(values.clone(), null_indices.clone());
        let expected: f64 = values
            .iter()
            .enumerate()
            .filter(|(i, _)| !null_indices.contains(i))
            .map(|(_, &v)| v as f64)
            .sum();
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_sum_float64_chunk_boundary_nulls() {
        let values: Vec<f64> = (1..=70).map(|i| i as f64).collect();
        let null_indices = vec![62, 63, 64, 65];
        let col = create_float64_column(values.clone(), null_indices.clone());
        let expected: f64 = values
            .iter()
            .enumerate()
            .filter(|(i, _)| !null_indices.contains(i))
            .map(|(_, &v)| v)
            .sum();
        assert_eq!(col.sum(), Some(expected));
    }

    #[test]
    fn test_min_float64_negative_values() {
        let col = create_float64_column(vec![5.5, -2.2, 8.8, -10.1, 3.3], vec![]);
        assert_eq!(col.min(), Some(Value::float64(-10.1)));
    }

    #[test]
    fn test_max_float64_negative_values() {
        let col = create_float64_column(vec![-5.5, -2.2, -8.8, -10.1, -3.3], vec![]);
        assert_eq!(col.max(), Some(Value::float64(-2.2)));
    }

    #[test]
    fn test_min_float64_single_element() {
        let col = create_float64_column(vec![42.5], vec![]);
        assert_eq!(col.min(), Some(Value::float64(42.5)));
    }

    #[test]
    fn test_max_float64_single_element() {
        let col = create_float64_column(vec![42.5], vec![]);
        assert_eq!(col.max(), Some(Value::float64(42.5)));
    }

    #[test]
    fn test_min_float64_single_element_null() {
        let col = create_float64_column(vec![42.5], vec![0]);
        assert_eq!(col.min(), None);
    }

    #[test]
    fn test_max_float64_single_element_null() {
        let col = create_float64_column(vec![42.5], vec![0]);
        assert_eq!(col.max(), None);
    }

    #[test]
    fn test_min_numeric_single_element() {
        let col = create_numeric_column(vec![Decimal::new(425, 1)], vec![]);
        assert_eq!(col.min(), Some(Value::Numeric(Decimal::new(425, 1))));
    }

    #[test]
    fn test_max_numeric_single_element() {
        let col = create_numeric_column(vec![Decimal::new(425, 1)], vec![]);
        assert_eq!(col.max(), Some(Value::Numeric(Decimal::new(425, 1))));
    }

    #[test]
    fn test_min_string_single_element() {
        let col = create_string_column(vec!["hello".to_string()], vec![]);
        assert_eq!(col.min(), Some(Value::String("hello".to_string())));
    }

    #[test]
    fn test_max_string_single_element() {
        let col = create_string_column(vec!["hello".to_string()], vec![]);
        assert_eq!(col.max(), Some(Value::String("hello".to_string())));
    }

    #[test]
    fn test_min_date_single_element() {
        let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let col = create_date_column(vec![date], vec![]);
        assert_eq!(col.min(), Some(Value::Date(date)));
    }

    #[test]
    fn test_max_date_single_element() {
        let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let col = create_date_column(vec![date], vec![]);
        assert_eq!(col.max(), Some(Value::Date(date)));
    }

    #[test]
    fn test_min_time_single_element() {
        let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let col = create_time_column(vec![time], vec![]);
        assert_eq!(col.min(), Some(Value::Time(time)));
    }

    #[test]
    fn test_max_time_single_element() {
        let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let col = create_time_column(vec![time], vec![]);
        assert_eq!(col.max(), Some(Value::Time(time)));
    }

    #[test]
    fn test_min_datetime_single_element() {
        let dt = NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(12, 30, 0)
            .unwrap();
        let col = create_datetime_column(vec![dt], vec![]);
        assert_eq!(col.min(), Some(Value::DateTime(dt)));
    }

    #[test]
    fn test_max_datetime_single_element() {
        let dt = NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(12, 30, 0)
            .unwrap();
        let col = create_datetime_column(vec![dt], vec![]);
        assert_eq!(col.max(), Some(Value::DateTime(dt)));
    }

    #[test]
    fn test_min_timestamp_single_element() {
        let ts = Utc.with_ymd_and_hms(2024, 6, 15, 12, 30, 0).unwrap();
        let col = create_timestamp_column(vec![ts], vec![]);
        assert_eq!(col.min(), Some(Value::Timestamp(ts)));
    }

    #[test]
    fn test_max_timestamp_single_element() {
        let ts = Utc.with_ymd_and_hms(2024, 6, 15, 12, 30, 0).unwrap();
        let col = create_timestamp_column(vec![ts], vec![]);
        assert_eq!(col.max(), Some(Value::Timestamp(ts)));
    }

    #[test]
    fn test_sum_numeric_large_values() {
        let values = vec![Decimal::new(i64::MAX / 2, 0), Decimal::new(i64::MAX / 2, 0)];
        let col = create_numeric_column(values, vec![]);
        let result = col.sum();
        assert!(result.is_some());
    }

    #[test]
    fn test_min_numeric_negative_values() {
        let values = vec![
            Decimal::new(-55, 1),
            Decimal::new(-22, 1),
            Decimal::new(-99, 1),
        ];
        let col = create_numeric_column(values, vec![]);
        assert_eq!(col.min(), Some(Value::Numeric(Decimal::new(-99, 1))));
    }

    #[test]
    fn test_max_numeric_negative_values() {
        let values = vec![
            Decimal::new(-55, 1),
            Decimal::new(-22, 1),
            Decimal::new(-99, 1),
        ];
        let col = create_numeric_column(values, vec![]);
        assert_eq!(col.max(), Some(Value::Numeric(Decimal::new(-22, 1))));
    }

    #[test]
    fn test_min_string_unicode() {
        let col = create_string_column(
            vec!["zebra".to_string(), "alpha".to_string(), "beta".to_string()],
            vec![],
        );
        assert_eq!(col.min(), Some(Value::String("alpha".to_string())));
    }

    #[test]
    fn test_max_string_unicode() {
        let col = create_string_column(
            vec!["zebra".to_string(), "alpha".to_string(), "beta".to_string()],
            vec![],
        );
        assert_eq!(col.max(), Some(Value::String("zebra".to_string())));
    }

    #[test]
    fn test_sum_int64_zero_values() {
        let col = create_int64_column(vec![0, 0, 0, 0, 0], vec![]);
        assert_eq!(col.sum(), Some(0.0));
    }

    #[test]
    fn test_sum_float64_zero_values() {
        let col = create_float64_column(vec![0.0, 0.0, 0.0, 0.0, 0.0], vec![]);
        assert_eq!(col.sum(), Some(0.0));
    }

    #[test]
    fn test_sum_numeric_zero_values() {
        let values = vec![Decimal::new(0, 0), Decimal::new(0, 0), Decimal::new(0, 0)];
        let col = create_numeric_column(values, vec![]);
        assert_eq!(col.sum(), Some(0.0));
    }

    #[test]
    fn test_min_int64_all_same() {
        let col = create_int64_column(vec![42, 42, 42, 42], vec![]);
        assert_eq!(col.min(), Some(Value::Int64(42)));
    }

    #[test]
    fn test_max_int64_all_same() {
        let col = create_int64_column(vec![42, 42, 42, 42], vec![]);
        assert_eq!(col.max(), Some(Value::Int64(42)));
    }

    #[test]
    fn test_min_float64_all_same() {
        let col = create_float64_column(vec![42.5, 42.5, 42.5, 42.5], vec![]);
        assert_eq!(col.min(), Some(Value::float64(42.5)));
    }

    #[test]
    fn test_max_float64_all_same() {
        let col = create_float64_column(vec![42.5, 42.5, 42.5, 42.5], vec![]);
        assert_eq!(col.max(), Some(Value::float64(42.5)));
    }

    #[test]
    fn test_sum_int64_large_negative_sum() {
        let col = create_int64_column(vec![-1000000, -2000000, -3000000], vec![]);
        assert_eq!(col.sum(), Some(-6000000.0));
    }

    #[test]
    fn test_sum_float64_large_negative_sum() {
        let col = create_float64_column(vec![-1000000.5, -2000000.5, -3000000.5], vec![]);
        assert_eq!(col.sum(), Some(-6000001.5));
    }

    #[test]
    fn test_sum_int64_mixed_positive_negative() {
        let col = create_int64_column(vec![100, -50, 200, -75, 25], vec![]);
        assert_eq!(col.sum(), Some(200.0));
    }

    #[test]
    fn test_sum_float64_mixed_positive_negative() {
        let col = create_float64_column(vec![100.5, -50.5, 200.0, -75.0, 25.0], vec![]);
        assert_eq!(col.sum(), Some(200.0));
    }
}
