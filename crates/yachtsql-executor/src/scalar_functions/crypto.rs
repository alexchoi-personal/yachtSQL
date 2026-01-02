#![coverage(off)]

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use data_encoding::BASE32;
use sha2::Digest;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_md5(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let digest = md5::compute(s.as_bytes());
            Ok(Value::Bytes(digest.to_vec()))
        }
        Some(Value::Bytes(b)) => {
            let digest = md5::compute(b);
            Ok(Value::Bytes(digest.to_vec()))
        }
        _ => Err(Error::InvalidQuery("MD5 requires string argument".into())),
    }
}

pub fn fn_sha1(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let mut hasher = sha1::Sha1::new();
            hasher.update(s.as_bytes());
            let result = hasher.finalize();
            Ok(Value::Bytes(result.to_vec()))
        }
        Some(Value::Bytes(b)) => {
            let mut hasher = sha1::Sha1::new();
            hasher.update(b);
            let result = hasher.finalize();
            Ok(Value::Bytes(result.to_vec()))
        }
        _ => Err(Error::InvalidQuery(
            "SHA1 requires string or bytes argument".into(),
        )),
    }
}

pub fn fn_sha256(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let mut hasher = sha2::Sha256::new();
            hasher.update(s.as_bytes());
            let result = hasher.finalize();
            Ok(Value::Bytes(result.to_vec()))
        }
        Some(Value::Bytes(b)) => {
            let mut hasher = sha2::Sha256::new();
            hasher.update(b);
            let result = hasher.finalize();
            Ok(Value::Bytes(result.to_vec()))
        }
        _ => Err(Error::InvalidQuery(
            "SHA256 requires string or bytes argument".into(),
        )),
    }
}

pub fn fn_sha512(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let mut hasher = sha2::Sha512::new();
            hasher.update(s.as_bytes());
            let result = hasher.finalize();
            Ok(Value::Bytes(result.to_vec()))
        }
        Some(Value::Bytes(b)) => {
            let mut hasher = sha2::Sha512::new();
            hasher.update(b);
            let result = hasher.finalize();
            Ok(Value::Bytes(result.to_vec()))
        }
        _ => Err(Error::InvalidQuery(
            "SHA512 requires string or bytes argument".into(),
        )),
    }
}

pub fn fn_farm_fingerprint(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let mut hasher = DefaultHasher::new();
            s.hash(&mut hasher);
            let hash = hasher.finish() as i64;
            Ok(Value::Int64(hash))
        }
        Some(Value::Bytes(b)) => {
            let mut hasher = DefaultHasher::new();
            b.hash(&mut hasher);
            let hash = hasher.finish() as i64;
            Ok(Value::Int64(hash))
        }
        _ => Err(Error::InvalidQuery(
            "FARM_FINGERPRINT requires STRING or BYTES argument".into(),
        )),
    }
}

pub fn fn_to_base64(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => Ok(Value::String(STANDARD.encode(s.as_bytes()))),
        Some(Value::Bytes(b)) => Ok(Value::String(STANDARD.encode(b))),
        _ => Err(Error::InvalidQuery(
            "TO_BASE64 requires string or bytes argument".into(),
        )),
    }
}

pub fn fn_from_base64(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let decoded = STANDARD
                .decode(s)
                .map_err(|e| Error::InvalidQuery(format!("Invalid base64: {}", e)))?;
            Ok(Value::Bytes(decoded))
        }
        _ => Err(Error::InvalidQuery(
            "FROM_BASE64 requires string argument".into(),
        )),
    }
}

pub fn fn_to_hex(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Bytes(b)) => Ok(Value::String(hex::encode(b))),
        Some(Value::String(s)) => Ok(Value::String(hex::encode(s.as_bytes()))),
        _ => Err(Error::InvalidQuery("TO_HEX requires bytes argument".into())),
    }
}

pub fn fn_from_hex(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let normalized = if s.len() % 2 == 1 {
                format!("0{}", s)
            } else {
                s.clone()
            };
            let decoded = hex::decode(&normalized)
                .map_err(|e| Error::InvalidQuery(format!("Invalid hex: {}", e)))?;
            Ok(Value::Bytes(decoded))
        }
        _ => Err(Error::InvalidQuery(
            "FROM_HEX requires string argument".into(),
        )),
    }
}

pub fn fn_to_base32(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::Bytes(b)) => Ok(Value::String(BASE32.encode(b))),
        Some(Value::String(s)) => Ok(Value::String(BASE32.encode(s.as_bytes()))),
        _ => Err(Error::InvalidQuery(
            "TO_BASE32 requires bytes or string argument".into(),
        )),
    }
}

pub fn fn_from_base32(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(s)) => {
            let decoded = BASE32
                .decode(s.as_bytes())
                .map_err(|e| Error::InvalidQuery(format!("Invalid base32: {}", e)))?;
            Ok(Value::Bytes(decoded))
        }
        _ => Err(Error::InvalidQuery(
            "FROM_BASE32 requires string argument".into(),
        )),
    }
}

pub fn fn_keys_new_keyset(args: &[Value]) -> Result<Value> {
    use rand::RngCore;

    let key_type = match args.first() {
        Some(Value::String(s)) => s.to_uppercase(),
        Some(Value::Null) => return Ok(Value::Null),
        _ => {
            return Err(Error::InvalidQuery(
                "KEYS.NEW_KEYSET requires key type string".into(),
            ));
        }
    };

    let key_size = match key_type.as_str() {
        "AEAD_AES_GCM_256" => 32,
        "AEAD_AES_GCM_128" => 16,
        "DETERMINISTIC_AEAD_AES_SIV_CMAC_256" => 64,
        _ => 32,
    };

    let mut key = vec![0u8; key_size];
    rand::thread_rng().fill_bytes(&mut key);
    Ok(Value::Bytes(key))
}

pub fn fn_aead_encrypt(args: &[Value]) -> Result<Value> {
    use rand::RngCore;

    if args.len() < 3 {
        return Err(Error::InvalidQuery(
            "AEAD.ENCRYPT requires keyset, plaintext, and additional_data".into(),
        ));
    }

    let keyset = match &args[0] {
        Value::Bytes(k) => k,
        Value::Null => return Ok(Value::Null),
        _ => {
            return Err(Error::InvalidQuery(
                "AEAD.ENCRYPT keyset must be BYTES".into(),
            ));
        }
    };

    let plaintext = match &args[1] {
        Value::Bytes(p) => p.clone(),
        Value::String(s) => s.as_bytes().to_vec(),
        Value::Null => return Ok(Value::Null),
        _ => {
            return Err(Error::InvalidQuery(
                "AEAD.ENCRYPT plaintext must be BYTES or STRING".into(),
            ));
        }
    };

    let aad = match &args[2] {
        Value::Bytes(a) => a.clone(),
        Value::String(s) => s.as_bytes().to_vec(),
        Value::Null => Vec::new(),
        _ => Vec::new(),
    };

    let key_bytes: [u8; 32] = if keyset.len() >= 32 {
        keyset[..32].try_into().unwrap()
    } else {
        let mut padded = [0u8; 32];
        padded[..keyset.len()].copy_from_slice(keyset);
        padded
    };

    let cipher = Aes256Gcm::new_from_slice(&key_bytes)
        .map_err(|e| Error::Internal(format!("Failed to create cipher: {}", e)))?;

    let mut nonce_bytes = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(
            nonce,
            aes_gcm::aead::Payload {
                msg: &plaintext,
                aad: &aad,
            },
        )
        .map_err(|e| Error::Internal(format!("Encryption failed: {}", e)))?;

    let mut result = nonce_bytes.to_vec();
    result.extend(ciphertext);
    Ok(Value::Bytes(result))
}

pub fn fn_aead_decrypt_bytes(args: &[Value]) -> Result<Value> {
    if args.len() < 3 {
        return Err(Error::InvalidQuery(
            "AEAD.DECRYPT_BYTES requires keyset, ciphertext, and additional_data".into(),
        ));
    }

    let keyset = match &args[0] {
        Value::Bytes(k) => k,
        Value::Null => return Ok(Value::Null),
        _ => {
            return Err(Error::InvalidQuery(
                "AEAD.DECRYPT_BYTES keyset must be BYTES".into(),
            ));
        }
    };

    let ciphertext = match &args[1] {
        Value::Bytes(c) => c,
        Value::Null => return Ok(Value::Null),
        _ => {
            return Err(Error::InvalidQuery(
                "AEAD.DECRYPT_BYTES ciphertext must be BYTES".into(),
            ));
        }
    };

    let aad = match &args[2] {
        Value::Bytes(a) => a.clone(),
        Value::String(s) => s.as_bytes().to_vec(),
        Value::Null => Vec::new(),
        _ => Vec::new(),
    };

    if ciphertext.len() < 12 {
        return Err(Error::InvalidQuery("Ciphertext too short".into()));
    }

    let key_bytes: [u8; 32] = if keyset.len() >= 32 {
        keyset[..32].try_into().unwrap()
    } else {
        let mut padded = [0u8; 32];
        padded[..keyset.len()].copy_from_slice(keyset);
        padded
    };

    let cipher = Aes256Gcm::new_from_slice(&key_bytes)
        .map_err(|e| Error::Internal(format!("Failed to create cipher: {}", e)))?;

    let nonce = Nonce::from_slice(&ciphertext[..12]);
    let encrypted_data = &ciphertext[12..];

    let plaintext = cipher
        .decrypt(
            nonce,
            aes_gcm::aead::Payload {
                msg: encrypted_data,
                aad: &aad,
            },
        )
        .map_err(|e| Error::Internal(format!("Decryption failed: {}", e)))?;

    Ok(Value::Bytes(plaintext))
}

pub fn fn_aead_decrypt_string(args: &[Value]) -> Result<Value> {
    let decrypted = fn_aead_decrypt_bytes(args)?;
    match decrypted {
        Value::Bytes(b) => {
            let s = String::from_utf8(b)
                .map_err(|e| Error::Internal(format!("Invalid UTF-8: {}", e)))?;
            Ok(Value::String(s))
        }
        other => Ok(other),
    }
}
