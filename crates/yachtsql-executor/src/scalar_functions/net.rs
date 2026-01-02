#![coverage(off)]

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn fn_net_host(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(url) => {
            if let Some(host_start) = url.find("://").map(|i| i + 3) {
                let rest = &url[host_start..];
                let host_end = rest
                    .find('/')
                    .or_else(|| rest.find('?'))
                    .or_else(|| rest.find('#'))
                    .unwrap_or(rest.len());
                let host_port = &rest[..host_end];
                let host = host_port.split(':').next().unwrap_or(host_port);
                Ok(Value::String(host.to_lowercase()))
            } else {
                let host = url.split('/').next().unwrap_or(url);
                let host = host.split(':').next().unwrap_or(host);
                Ok(Value::String(host.to_lowercase()))
            }
        }
        _ => Err(Error::InvalidQuery(
            "NET.HOST expects a string argument".into(),
        )),
    }
}

pub fn fn_net_public_suffix(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(_) => {
            let host_result = fn_net_host(args)?;
            if let Value::String(host) = host_result {
                let parts: Vec<&str> = host.split('.').collect();
                if parts.len() >= 2 {
                    Ok(Value::String(parts[parts.len() - 1].to_string()))
                } else if parts.len() == 1 {
                    Ok(Value::String(parts[0].to_string()))
                } else {
                    Ok(Value::Null)
                }
            } else {
                Ok(Value::Null)
            }
        }
        _ => Err(Error::InvalidQuery(
            "NET.PUBLIC_SUFFIX expects a string argument".into(),
        )),
    }
}

pub fn fn_net_reg_domain(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(_) => {
            let host_result = fn_net_host(args)?;
            if let Value::String(host) = host_result {
                let parts: Vec<&str> = host.split('.').collect();
                if parts.len() >= 2 {
                    Ok(Value::String(format!(
                        "{}.{}",
                        parts[parts.len() - 2],
                        parts[parts.len() - 1]
                    )))
                } else {
                    Ok(Value::String(host))
                }
            } else {
                Ok(Value::Null)
            }
        }
        _ => Err(Error::InvalidQuery(
            "NET.REG_DOMAIN expects a string argument".into(),
        )),
    }
}

pub fn fn_net_ip_from_string(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(s) => match s.parse::<IpAddr>() {
            Ok(IpAddr::V4(ipv4)) => Ok(Value::Bytes(ipv4.octets().to_vec())),
            Ok(IpAddr::V6(ipv6)) => Ok(Value::Bytes(ipv6.octets().to_vec())),
            Err(_) => Err(Error::InvalidQuery(format!("Invalid IP address: {}", s))),
        },
        _ => Err(Error::InvalidQuery(
            "NET.IP_FROM_STRING expects a string argument".into(),
        )),
    }
}

pub fn fn_net_ip_to_string(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Bytes(bytes) => {
            if bytes.len() == 4 {
                let arr: [u8; 4] = bytes[..4].try_into().unwrap();
                Ok(Value::String(Ipv4Addr::from(arr).to_string()))
            } else if bytes.len() == 16 {
                let arr: [u8; 16] = bytes[..16].try_into().unwrap();
                Ok(Value::String(Ipv6Addr::from(arr).to_string()))
            } else {
                Err(Error::InvalidQuery(
                    "NET.IP_TO_STRING expects 4 or 16 bytes".into(),
                ))
            }
        }
        _ => Err(Error::InvalidQuery(
            "NET.IP_TO_STRING expects a bytes argument".into(),
        )),
    }
}

pub fn fn_net_ip_net_mask(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "NET.IP_NET_MASK requires num_bytes and prefix_length arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Int64(num_bytes), Value::Int64(prefix_len)) => {
            let mut mask = vec![0u8; *num_bytes as usize];
            let full_bytes = (*prefix_len / 8) as usize;
            let remaining_bits = (*prefix_len % 8) as u8;

            for (i, byte) in mask.iter_mut().enumerate() {
                if i < full_bytes {
                    *byte = 0xFF;
                } else if i == full_bytes && remaining_bits > 0 {
                    *byte = !((1u8 << (8 - remaining_bits)) - 1);
                }
            }
            Ok(Value::Bytes(mask))
        }
        _ => Err(Error::InvalidQuery(
            "NET.IP_NET_MASK expects integer arguments".into(),
        )),
    }
}

pub fn fn_net_ip_trunc(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "NET.IP_TRUNC requires IP and prefix length arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Bytes(bytes), Value::Int64(prefix_len)) => {
            let mut result = bytes.clone();
            let full_bytes = (*prefix_len / 8) as usize;
            let remaining_bits = (*prefix_len % 8) as u8;

            for (i, byte) in result.iter_mut().enumerate() {
                if i < full_bytes {
                    continue;
                } else if i == full_bytes && remaining_bits > 0 {
                    let mask = !((1u8 << (8 - remaining_bits)) - 1);
                    *byte &= mask;
                } else {
                    *byte = 0;
                }
            }
            Ok(Value::Bytes(result))
        }
        _ => Err(Error::InvalidQuery(
            "NET.IP_TRUNC expects bytes and integer arguments".into(),
        )),
    }
}

pub fn fn_net_safe_ip_from_string(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(s) => match s.parse::<IpAddr>() {
            Ok(IpAddr::V4(ipv4)) => Ok(Value::Bytes(ipv4.octets().to_vec())),
            Ok(IpAddr::V6(ipv6)) => Ok(Value::Bytes(ipv6.octets().to_vec())),
            Err(_) => Ok(Value::Null),
        },
        _ => Ok(Value::Null),
    }
}

pub fn fn_net_ip_in_net(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "NET.IP_IN_NET requires IP and network arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Bytes(ip_bytes), Value::String(cidr)) => {
            let parts: Vec<&str> = cidr.split('/').collect();
            if parts.len() != 2 {
                return Err(Error::InvalidQuery("Invalid CIDR notation".into()));
            }
            let network_ip: IpAddr = parts[0]
                .parse()
                .map_err(|_| Error::InvalidQuery("Invalid network IP".into()))?;
            let prefix_len: u8 = parts[1]
                .parse()
                .map_err(|_| Error::InvalidQuery("Invalid prefix length".into()))?;

            let ip = if ip_bytes.len() == 4 {
                let arr: [u8; 4] = ip_bytes[..4].try_into().unwrap();
                IpAddr::V4(Ipv4Addr::from(arr))
            } else if ip_bytes.len() == 16 {
                let arr: [u8; 16] = ip_bytes[..16].try_into().unwrap();
                IpAddr::V6(Ipv6Addr::from(arr))
            } else {
                return Err(Error::InvalidQuery("Invalid IP bytes length".into()));
            };

            let result = match (ip, network_ip) {
                (IpAddr::V4(ip), IpAddr::V4(net)) => {
                    let mask = if prefix_len >= 32 {
                        u32::MAX
                    } else {
                        !((1u32 << (32 - prefix_len)) - 1)
                    };
                    (u32::from(ip) & mask) == (u32::from(net) & mask)
                }
                (IpAddr::V6(ip), IpAddr::V6(net)) => {
                    let ip_bits = u128::from(ip);
                    let net_bits = u128::from(net);
                    let mask = if prefix_len >= 128 {
                        u128::MAX
                    } else {
                        !((1u128 << (128 - prefix_len)) - 1)
                    };
                    (ip_bits & mask) == (net_bits & mask)
                }
                _ => false,
            };
            Ok(Value::Bool(result))
        }
        _ => Err(Error::InvalidQuery(
            "NET.IP_IN_NET expects bytes and string arguments".into(),
        )),
    }
}

pub fn fn_net_make_net(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(Error::InvalidQuery(
            "NET.MAKE_NET requires IP and prefix_length arguments".into(),
        ));
    }
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Bytes(ip_bytes), Value::Int64(prefix_len)) => {
            let ip_str = if ip_bytes.len() == 4 {
                let arr: [u8; 4] = ip_bytes[..4].try_into().unwrap();
                Ipv4Addr::from(arr).to_string()
            } else if ip_bytes.len() == 16 {
                let arr: [u8; 16] = ip_bytes[..16].try_into().unwrap();
                Ipv6Addr::from(arr).to_string()
            } else {
                return Err(Error::InvalidQuery("Invalid IP bytes length".into()));
            };
            Ok(Value::String(format!("{}/{}", ip_str, prefix_len)))
        }
        _ => Err(Error::InvalidQuery(
            "NET.MAKE_NET expects bytes and integer arguments".into(),
        )),
    }
}

pub fn fn_net_ip_is_private(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Bytes(ip_bytes) => {
            if ip_bytes.len() == 4 {
                let arr: [u8; 4] = ip_bytes[..4].try_into().unwrap();
                let ip = Ipv4Addr::from(arr);
                let octets = ip.octets();
                let is_private = octets[0] == 10
                    || (octets[0] == 172 && (16..=31).contains(&octets[1]))
                    || (octets[0] == 192 && octets[1] == 168)
                    || octets[0] == 127;
                Ok(Value::Bool(is_private))
            } else if ip_bytes.len() == 16 {
                let arr: [u8; 16] = ip_bytes[..16].try_into().unwrap();
                let ip = Ipv6Addr::from(arr);
                let segments = ip.segments();
                let is_private = (segments[0] & 0xfe00) == 0xfc00 || ip.is_loopback();
                Ok(Value::Bool(is_private))
            } else {
                Err(Error::InvalidQuery("Invalid IP bytes length".into()))
            }
        }
        _ => Err(Error::InvalidQuery(
            "NET.IP_IS_PRIVATE expects bytes argument".into(),
        )),
    }
}

pub fn fn_net_ipv4_from_int64(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Int64(n) => {
            if *n < 0 || *n > u32::MAX as i64 {
                return Ok(Value::Null);
            }
            let ip = Ipv4Addr::from(*n as u32);
            Ok(Value::Bytes(ip.octets().to_vec()))
        }
        _ => Err(Error::InvalidQuery(
            "NET.IPV4_FROM_INT64 expects integer argument".into(),
        )),
    }
}

pub fn fn_net_ipv4_to_int64(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Bytes(ip_bytes) => {
            if ip_bytes.len() != 4 {
                return Err(Error::InvalidQuery(
                    "NET.IPV4_TO_INT64 expects 4-byte IPv4 address".into(),
                ));
            }
            let arr: [u8; 4] = ip_bytes[..4].try_into().unwrap();
            Ok(Value::Int64(u32::from_be_bytes(arr) as i64))
        }
        _ => Err(Error::InvalidQuery(
            "NET.IPV4_TO_INT64 expects bytes argument".into(),
        )),
    }
}
