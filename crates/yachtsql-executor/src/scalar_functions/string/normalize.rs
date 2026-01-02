#![coverage(off)]

use unicode_normalization::UnicodeNormalization;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_normalize(args: &[Value]) -> Result<Value> {
    let mode = args
        .get(1)
        .and_then(|v| {
            if let Value::String(s) = v {
                Some(s.to_uppercase())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "NFC".to_string());

    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let normalized: String = match mode.as_str() {
                "NFC" => s.nfc().collect(),
                "NFKC" => s.nfkc().collect(),
                "NFD" => s.nfd().collect(),
                "NFKD" => s.nfkd().collect(),
                _ => {
                    return Err(Error::InvalidQuery(format!(
                        "Invalid normalization mode: {}. Expected NFC, NFKC, NFD, or NFKD",
                        mode
                    )));
                }
            };
            Ok(Value::String(normalized))
        }
        _ => Err(Error::InvalidQuery(
            "NORMALIZE requires string argument".into(),
        )),
    }
}

pub fn fn_normalize_and_casefold(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let normalized: String = s.nfkc().collect();
            let casefolded = normalized.to_lowercase();
            Ok(Value::String(casefolded))
        }
        _ => Err(Error::InvalidQuery(
            "NORMALIZE_AND_CASEFOLD requires string argument".into(),
        )),
    }
}
