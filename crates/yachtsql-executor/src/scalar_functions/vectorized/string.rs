#![coverage(off)]

use std::collections::HashSet;

use aligned_vec::AVec;
use yachtsql_common::error::{Error, Result};
use yachtsql_storage::{Column, NullBitmap};

pub fn upper(args: &[Column]) -> Result<Column> {
    let col = args
        .first()
        .ok_or_else(|| Error::invalid_query("UPPER requires 1 argument"))?;
    match col {
        Column::String { data, nulls } => {
            let result_data: Vec<String> = data.iter().map(|s| s.to_uppercase()).collect();
            Ok(Column::String {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        _ => Err(Error::invalid_query("UPPER requires string argument")),
    }
}

pub fn lower(args: &[Column]) -> Result<Column> {
    let col = args
        .first()
        .ok_or_else(|| Error::invalid_query("LOWER requires 1 argument"))?;
    match col {
        Column::String { data, nulls } => {
            let result_data: Vec<String> = data.iter().map(|s| s.to_lowercase()).collect();
            Ok(Column::String {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        _ => Err(Error::invalid_query("LOWER requires string argument")),
    }
}

pub fn length(args: &[Column]) -> Result<Column> {
    let col = args
        .first()
        .ok_or_else(|| Error::invalid_query("LENGTH requires 1 argument"))?;
    match col {
        Column::String { data, nulls } => {
            let mut result_data = AVec::with_capacity(64, data.len());
            for s in data {
                result_data.push(s.chars().count() as i64);
            }
            Ok(Column::Int64 {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        Column::Bytes { data, nulls } => {
            let mut result_data = AVec::with_capacity(64, data.len());
            for b in data {
                result_data.push(b.len() as i64);
            }
            Ok(Column::Int64 {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        Column::Array { data, nulls, .. } => {
            let mut result_data = AVec::with_capacity(64, data.len());
            for a in data {
                result_data.push(a.len() as i64);
            }
            Ok(Column::Int64 {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        _ => Err(Error::invalid_query(
            "LENGTH requires string, bytes, or array argument",
        )),
    }
}

pub fn byte_length(args: &[Column]) -> Result<Column> {
    let col = args
        .first()
        .ok_or_else(|| Error::invalid_query("BYTE_LENGTH requires 1 argument"))?;
    match col {
        Column::String { data, nulls } => {
            let mut result_data = AVec::with_capacity(64, data.len());
            for s in data {
                result_data.push(s.len() as i64);
            }
            Ok(Column::Int64 {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        Column::Bytes { data, nulls } => {
            let mut result_data = AVec::with_capacity(64, data.len());
            for b in data {
                result_data.push(b.len() as i64);
            }
            Ok(Column::Int64 {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        _ => Err(Error::invalid_query(
            "BYTE_LENGTH requires string or bytes argument",
        )),
    }
}

pub fn trim(args: &[Column]) -> Result<Column> {
    let col = args
        .first()
        .ok_or_else(|| Error::invalid_query("TRIM requires 1 argument"))?;
    let chars_to_trim: Option<HashSet<char>> = args.get(1).and_then(|c| {
        if let Column::String { data, nulls } = c
            && !nulls.is_null(0)
            && !data.is_empty()
        {
            return Some(data[0].chars().collect());
        }
        None
    });

    match col {
        Column::String { data, nulls } => {
            let result_data: Vec<String> = match &chars_to_trim {
                Some(char_set) => data
                    .iter()
                    .map(|s| {
                        s.trim_start_matches(|c| char_set.contains(&c))
                            .trim_end_matches(|c| char_set.contains(&c))
                            .to_string()
                    })
                    .collect(),
                None => data.iter().map(|s| s.trim().to_string()).collect(),
            };
            Ok(Column::String {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        _ => Err(Error::invalid_query("TRIM requires string argument")),
    }
}

pub fn ltrim(args: &[Column]) -> Result<Column> {
    let col = args
        .first()
        .ok_or_else(|| Error::invalid_query("LTRIM requires 1 argument"))?;
    let chars_to_trim: Option<HashSet<char>> = args.get(1).and_then(|c| {
        if let Column::String { data, nulls } = c
            && !nulls.is_null(0)
            && !data.is_empty()
        {
            return Some(data[0].chars().collect());
        }
        None
    });

    match col {
        Column::String { data, nulls } => {
            let result_data: Vec<String> = match &chars_to_trim {
                Some(char_set) => data
                    .iter()
                    .map(|s| s.trim_start_matches(|c| char_set.contains(&c)).to_string())
                    .collect(),
                None => data.iter().map(|s| s.trim_start().to_string()).collect(),
            };
            Ok(Column::String {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        _ => Err(Error::invalid_query("LTRIM requires string argument")),
    }
}

pub fn rtrim(args: &[Column]) -> Result<Column> {
    let col = args
        .first()
        .ok_or_else(|| Error::invalid_query("RTRIM requires 1 argument"))?;
    let chars_to_trim: Option<HashSet<char>> = args.get(1).and_then(|c| {
        if let Column::String { data, nulls } = c
            && !nulls.is_null(0)
            && !data.is_empty()
        {
            return Some(data[0].chars().collect());
        }
        None
    });

    match col {
        Column::String { data, nulls } => {
            let result_data: Vec<String> = match &chars_to_trim {
                Some(char_set) => data
                    .iter()
                    .map(|s| s.trim_end_matches(|c| char_set.contains(&c)).to_string())
                    .collect(),
                None => data.iter().map(|s| s.trim_end().to_string()).collect(),
            };
            Ok(Column::String {
                data: result_data,
                nulls: nulls.clone(),
            })
        }
        _ => Err(Error::invalid_query("RTRIM requires string argument")),
    }
}

pub fn substr(args: &[Column]) -> Result<Column> {
    let col = args
        .first()
        .ok_or_else(|| Error::invalid_query("SUBSTR requires at least 1 argument"))?;
    let len = col.len();

    let start_col = args.get(1);
    let length_col = args.get(2);

    match col {
        Column::String { data, nulls } => {
            let mut result_data = Vec::with_capacity(len);
            let mut result_nulls = NullBitmap::new();

            for (i, s) in data.iter().enumerate() {
                if nulls.is_null(i) {
                    result_data.push(String::new());
                    result_nulls.push(true);
                    continue;
                }
                let chars: Vec<char> = s.chars().collect();
                let char_len = chars.len();

                let start_raw = start_col
                    .map(|c| c.get_value(i).as_i64().unwrap_or(1))
                    .unwrap_or(1);

                let substr_len =
                    length_col.and_then(|c| c.get_value(i).as_i64().map(|l| l as usize));

                let start_idx = if start_raw < 0 {
                    char_len.saturating_sub((-start_raw) as usize)
                } else if start_raw == 0 {
                    0
                } else {
                    (start_raw as usize).saturating_sub(1).min(char_len)
                };

                let end_idx = substr_len
                    .map(|l| (start_idx + l).min(char_len))
                    .unwrap_or(char_len);

                result_data.push(chars[start_idx..end_idx].iter().collect());
                result_nulls.push(false);
            }

            Ok(Column::String {
                data: result_data,
                nulls: result_nulls,
            })
        }
        Column::Bytes { data, nulls } => {
            let mut result_data = Vec::with_capacity(len);
            let mut result_nulls = NullBitmap::new();

            for (i, b) in data.iter().enumerate() {
                if nulls.is_null(i) {
                    result_data.push(Vec::new());
                    result_nulls.push(true);
                    continue;
                }
                let byte_len = b.len();

                let start_raw = start_col
                    .map(|c| c.get_value(i).as_i64().unwrap_or(1))
                    .unwrap_or(1);

                let substr_len =
                    length_col.and_then(|c| c.get_value(i).as_i64().map(|l| l as usize));

                let start_idx = if start_raw < 0 {
                    byte_len.saturating_sub((-start_raw) as usize)
                } else if start_raw == 0 {
                    0
                } else {
                    (start_raw as usize).saturating_sub(1).min(byte_len)
                };

                let end_idx = substr_len
                    .map(|l| (start_idx + l).min(byte_len))
                    .unwrap_or(byte_len);

                result_data.push(b[start_idx..end_idx].to_vec());
                result_nulls.push(false);
            }

            Ok(Column::Bytes {
                data: result_data,
                nulls: result_nulls,
            })
        }
        _ => Err(Error::invalid_query(
            "SUBSTR requires string or bytes argument",
        )),
    }
}
