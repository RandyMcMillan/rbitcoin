//! Minimal hex encode/decode (replaces the `hex` crate).

use std::fmt;

/// Failed to parse a hex string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HexError {
    pub message: &'static str,
}

impl fmt::Display for HexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message)
    }
}

impl std::error::Error for HexError {}

/// Lowercase hex encoding of `data`.
pub fn encode(data: impl AsRef<[u8]>) -> String {
    let data = data.as_ref();
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(data.len() * 2);
    for &b in data {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

/// Decode a hex string (even length, optional `0x` prefix). Accepts a-f/A-F.
pub fn decode(s: &str) -> Result<Vec<u8>, HexError> {
    let s = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")).unwrap_or(s);
    if s.len() % 2 != 0 {
        return Err(HexError {
            message: "odd hex length",
        });
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = from_digit(bytes[i])?;
        let lo = from_digit(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn from_digit(b: u8) -> Result<u8, HexError> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(HexError {
            message: "invalid hex digit",
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let data = [0u8, 1, 0xab, 0xff];
        assert_eq!(encode(&data), "0001abff");
        assert_eq!(decode("0001abff").unwrap(), data);
        assert_eq!(decode("0001ABFF").unwrap(), data);
        assert_eq!(decode("0x0a").unwrap(), vec![0x0a]);
    }

    #[test]
    fn rejects_bad() {
        assert!(decode("0").is_err());
        assert!(decode("zz").is_err());
    }
}
