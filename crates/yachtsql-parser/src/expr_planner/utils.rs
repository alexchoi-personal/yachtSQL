#![coverage(off)]

pub fn unescape_unicode(s: &str) -> String {
    s.to_string()
}

pub fn parse_byte_string_escapes(s: &str) -> Vec<u8> {
    let mut result = Vec::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.peek() {
                Some('x') | Some('X') => {
                    chars.next();
                    let mut hex = String::new();
                    for _ in 0..2 {
                        if let Some(&c) = chars.peek()
                            && c.is_ascii_hexdigit()
                        {
                            hex.push(c);
                            chars.next();
                        }
                    }
                    if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                        result.push(byte);
                    }
                }
                Some('n') => {
                    chars.next();
                    result.push(b'\n');
                }
                Some('t') => {
                    chars.next();
                    result.push(b'\t');
                }
                Some('r') => {
                    chars.next();
                    result.push(b'\r');
                }
                Some('\\') => {
                    chars.next();
                    result.push(b'\\');
                }
                Some('\'') => {
                    chars.next();
                    result.push(b'\'');
                }
                Some('"') => {
                    chars.next();
                    result.push(b'"');
                }
                _ => {
                    result.push(b'\\');
                }
            }
        } else {
            let mut buf = [0u8; 4];
            let encoded = c.encode_utf8(&mut buf);
            result.extend_from_slice(encoded.as_bytes());
        }
    }
    result
}
