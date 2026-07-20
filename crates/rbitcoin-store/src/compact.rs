//! Compact integer and flag encoding for schema v2+ records.
//!
//! Bitcoin-style CompactSize for lengths/counts; LEB128 for large positive
//! magnitudes when useful. Flags collapse common constant cases (final
//! sequence, empty script/witness).

use crate::error::StoreError;

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

/// Input record flags (schema v3).
pub mod input_flags {
    /// `sequence == 0xffff_ffff`
    pub const SEQ_FINAL: u8 = 1 << 0;
    /// Empty `script_sig`
    pub const EMPTY_SCRIPT: u8 = 1 << 1;
    /// Empty witness stack
    pub const EMPTY_WITNESS: u8 = 1 << 2;
    /// Null prevout (coinbase): skip 32-byte txid + use prev_index = 0xffff_ffff
    pub const NULL_PREV: u8 = 1 << 3;
    /// Deprecated: was CompactSize prev_tx_fk + vout. Class A no longer uses this.
    pub const LOCAL_PREV: u8 = 1 << 4;
}

/// Output record flags (schema v3).
pub mod output_flags {
    /// Empty scriptPubKey
    pub const EMPTY_SCRIPT: u8 = 1 << 0;
    /// Script is exactly `OP_TRUE` (0x51) — anyone-can-spend fixture
    pub const OP_TRUE: u8 = 1 << 1;
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
}
