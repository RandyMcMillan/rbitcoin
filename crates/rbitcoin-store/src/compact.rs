//! Compact integer and flag encoding for schema v2+ records.
//!
//! Bitcoin-style CompactSize for lengths/counts; LEB128 for large positive
//! magnitudes when useful. Flags collapse common constant cases (final
//! sequence, empty script/witness).

use crate::error::StoreError;

/// Byte length of a Bitcoin CompactSize encoding of `n` (no write).
#[inline]
pub fn compact_size_len(n: u64) -> usize {
    if n < 253 {
        1
    } else if n <= u16::MAX as u64 {
        3
    } else if n <= u32::MAX as u64 {
        5
    } else {
        9
    }
}

/// Write Bitcoin CompactSize (unsigned).
pub fn write_compact_size(out: &mut Vec<u8>, n: u64) {
    if n < 253 {
        out.push(n as u8);
    } else if n <= u16::MAX as u64 {
        out.push(253);
        out.extend_from_slice(&(n as u16).to_le_bytes());
    } else if n <= u32::MAX as u64 {
        out.push(254);
        out.extend_from_slice(&(n as u32).to_le_bytes());
    } else {
        out.push(255);
        out.extend_from_slice(&n.to_le_bytes());
    }
}

/// Read CompactSize; returns (value, bytes_consumed).
pub fn read_compact_size(buf: &[u8]) -> Result<(u64, usize), StoreError> {
    if buf.is_empty() {
        return Err(StoreError::Corrupt("compact size empty"));
    }
    match buf[0] {
        n @ 0..=252 => Ok((u64::from(n), 1)),
        253 => {
            if buf.len() < 3 {
                return Err(StoreError::Corrupt("compact size u16 truncated"));
            }
            let v = u16::from_le_bytes([buf[1], buf[2]]);
            Ok((u64::from(v), 3))
        }
        254 => {
            if buf.len() < 5 {
                return Err(StoreError::Corrupt("compact size u32 truncated"));
            }
            let v = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            Ok((u64::from(v), 5))
        }
        255 => {
            if buf.len() < 9 {
                return Err(StoreError::Corrupt("compact size u64 truncated"));
            }
            let v = u64::from_le_bytes(buf[1..9].try_into().unwrap());
            Ok((v, 9))
        }
    }
}

/// Byte length of an unsigned LEB128 encoding of `n` (no write).
#[inline]
pub fn uleb128_len(mut n: u64) -> usize {
    let mut len = 1usize;
    while n >= 0x80 {
        n >>= 7;
        len += 1;
    }
    len
}

/// Unsigned LEB128 (7-bit groups, MSB continuation).
pub fn write_uleb128(out: &mut Vec<u8>, mut n: u64) {
    loop {
        let mut b = (n & 0x7f) as u8;
        n >>= 7;
        if n != 0 {
            b |= 0x80;
        }
        out.push(b);
        if n == 0 {
            break;
        }
    }
}

pub fn read_uleb128(buf: &[u8]) -> Result<(u64, usize), StoreError> {
    let mut result = 0u64;
    let mut shift = 0u32;
    for (i, &b) in buf.iter().enumerate() {
        if shift >= 64 {
            return Err(StoreError::Corrupt("uleb128 overflow"));
        }
        result |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
    }
    Err(StoreError::Corrupt("uleb128 truncated"))
}

/// Input record flags (schema v10).
pub mod input_flags {
    /// `sequence == 0xffff_ffff`
    pub const SEQ_FINAL: u8 = 1 << 0;
    /// Empty `script_sig`
    pub const EMPTY_SCRIPT: u8 = 1 << 1;
    /// Empty witness stack
    pub const EMPTY_WITNESS: u8 = 1 << 2;
    /// Null prevout (coinbase): no create_fk payload; prev_index = 0xffff_ffff
    pub const NULL_PREV: u8 = 1 << 3;
    /// Reserved / unused (v9 and earlier LOCAL_PREV; rejected if set).
    pub const RESERVED4: u8 = 1 << 4;
}

/// Output record flags (schema v5).
pub mod output_flags {
    /// Empty scriptPubKey
    pub const EMPTY_SCRIPT: u8 = 1 << 0;
    /// Script is exactly `OP_TRUE` (0x51) — anyone-can-spend fixture
    pub const OP_TRUE: u8 = 1 << 1;
    /// `spender_field` is a `spenders.body` list head (not a sole spending_tx_fk).
    pub const MULTI_SPENDER: u8 = 1 << 2;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_size_roundtrip() {
        for n in [0u64, 1, 252, 253, 1000, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            write_compact_size(&mut buf, n);
            let (v, used) = read_compact_size(&buf).unwrap();
            assert_eq!(v, n);
            assert_eq!(used, buf.len());
        }
    }

    #[test]
    fn uleb_roundtrip() {
        for n in [0u64, 1, 127, 128, 255, 300, u32::MAX as u64, u64::MAX >> 1] {
            let mut buf = Vec::new();
            write_uleb128(&mut buf, n);
            let (v, used) = read_uleb128(&buf).unwrap();
            assert_eq!(v, n);
            assert_eq!(used, buf.len());
        }
    }

    #[test]
    fn compact_and_uleb_error_paths() {
        assert!(matches!(
            read_compact_size(&[]),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            read_compact_size(&[253, 1]),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            read_compact_size(&[254, 1, 2, 3]),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            read_compact_size(&[255, 1, 2, 3, 4, 5, 6, 7]),
            Err(StoreError::Corrupt(_))
        ));
        // truncated multi-byte uleb128
        assert!(matches!(read_uleb128(&[0x80]), Err(StoreError::Corrupt(_))));
        // overflow: more than 10 continuation groups
        let mut over = vec![0x80u8; 10];
        over.push(0x01);
        assert!(matches!(read_uleb128(&over), Err(StoreError::Corrupt(_))));
        // happy truncated-size boundaries still parse when full
        let (v, n) = read_compact_size(&[253, 0, 1]).unwrap();
        assert_eq!((v, n), (256, 3));
        let (v, n) = read_compact_size(&[254, 0, 0, 1, 0]).unwrap();
        assert_eq!((v, n), (1 << 16, 5));
        let mut u64b = vec![255u8];
        u64b.extend_from_slice(&u64::MAX.to_le_bytes());
        let (v, n) = read_compact_size(&u64b).unwrap();
        assert_eq!((v, n), (u64::MAX, 9));
    }
}
