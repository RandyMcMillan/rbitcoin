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

/// Schema-17 `txout` script-kind nibble (bits 0–3). Production still uses
/// [`output_flags`] until Class A cutover.
pub const SCRIPT_KIND_V17_RAW: u8 = 0;
pub const SCRIPT_KIND_V17_EMPTY: u8 = 1;
pub const SCRIPT_KIND_V17_OP_TRUE: u8 = 2;
pub const SCRIPT_KIND_V17_P2PKH: u8 = 3;
pub const SCRIPT_KIND_V17_P2SH: u8 = 4;
pub const SCRIPT_KIND_V17_P2WPKH: u8 = 5;
pub const SCRIPT_KIND_V17_P2WSH: u8 = 6;
pub const SCRIPT_KIND_V17_P2TR: u8 = 7;
pub const SCRIPT_KIND_V17_OP_RETURN_PUSH: u8 = 8;
pub const SCRIPT_KIND_V17_P2A: u8 = 9;

const SCRIPT_KIND_V17_MAX: u8 = SCRIPT_KIND_V17_P2A;

/// Classify a wire scriptPubKey into a v17 kind and template payload (no CompactSize).
pub fn classify_script(script: &[u8]) -> (u8, &[u8]) {
    if script.is_empty() {
        return (SCRIPT_KIND_V17_EMPTY, &[]);
    }
    if script == [0x51] {
        return (SCRIPT_KIND_V17_OP_TRUE, &[]);
    }
    if script == [0x51, 0x02, 0x4e, 0x73] {
        return (SCRIPT_KIND_V17_P2A, &[]);
    }
    if script.len() == 25
        && script[0] == 0x76
        && script[1] == 0xa9
        && script[2] == 0x14
        && script[23] == 0x88
        && script[24] == 0xac
    {
        return (SCRIPT_KIND_V17_P2PKH, &script[3..23]);
    }
    if script.len() == 23 && script[0] == 0xa9 && script[1] == 0x14 && script[22] == 0x87 {
        return (SCRIPT_KIND_V17_P2SH, &script[2..22]);
    }
    if script.len() == 22 && script[0] == 0x00 && script[1] == 0x14 {
        return (SCRIPT_KIND_V17_P2WPKH, &script[2..]);
    }
    if script.len() == 34 && script[0] == 0x00 && script[1] == 0x20 {
        return (SCRIPT_KIND_V17_P2WSH, &script[2..]);
    }
    if script.len() == 34 && script[0] == 0x51 && script[1] == 0x20 {
        return (SCRIPT_KIND_V17_P2TR, &script[2..]);
    }
    if let Some(data) = canonical_op_return_push(script) {
        return (SCRIPT_KIND_V17_OP_RETURN_PUSH, data);
    }
    (SCRIPT_KIND_V17_RAW, script)
}

/// Expand a v17 kind + classify payload to the wire scriptPubKey.
pub fn expand_script_kind(kind: u8, payload: &[u8]) -> Result<Vec<u8>, StoreError> {
    match kind {
        SCRIPT_KIND_V17_RAW => Ok(payload.to_vec()),
        SCRIPT_KIND_V17_EMPTY => {
            if !payload.is_empty() {
                return Err(StoreError::Corrupt("v17 empty script kind has payload"));
            }
            Ok(Vec::new())
        }
        SCRIPT_KIND_V17_OP_TRUE => {
            if !payload.is_empty() {
                return Err(StoreError::Corrupt("v17 OP_TRUE kind has payload"));
            }
            Ok(vec![0x51])
        }
        SCRIPT_KIND_V17_P2PKH => {
            let h = hash160_payload(payload)?;
            let mut s = vec![0x76, 0xa9, 0x14];
            s.extend_from_slice(h);
            s.extend_from_slice(&[0x88, 0xac]);
            Ok(s)
        }
        SCRIPT_KIND_V17_P2SH => {
            let h = hash160_payload(payload)?;
            let mut s = vec![0xa9, 0x14];
            s.extend_from_slice(h);
            s.push(0x87);
            Ok(s)
        }
        SCRIPT_KIND_V17_P2WPKH => {
            let h = hash160_payload(payload)?;
            let mut s = vec![0x00, 0x14];
            s.extend_from_slice(h);
            Ok(s)
        }
        SCRIPT_KIND_V17_P2WSH => {
            let h = hash256_payload(payload)?;
            let mut s = vec![0x00, 0x20];
            s.extend_from_slice(h);
            Ok(s)
        }
        SCRIPT_KIND_V17_P2TR => {
            let h = hash256_payload(payload)?;
            let mut s = vec![0x51, 0x20];
            s.extend_from_slice(h);
            Ok(s)
        }
        SCRIPT_KIND_V17_OP_RETURN_PUSH => Ok(encode_op_return_push(payload)?),
        SCRIPT_KIND_V17_P2A => {
            if !payload.is_empty() {
                return Err(StoreError::Corrupt("v17 P2A kind has payload"));
            }
            Ok(vec![0x51, 0x02, 0x4e, 0x73])
        }
        _ => Err(StoreError::Corrupt("v17 reserved script kind")),
    }
}

/// Write the on-disk v17 script payload (after the output value). Returns the kind.
pub fn encode_script_kind_v17(script: &[u8], out: &mut Vec<u8>) -> u8 {
    let (kind, payload) = classify_script(script);
    match kind {
        SCRIPT_KIND_V17_RAW | SCRIPT_KIND_V17_OP_RETURN_PUSH => {
            write_compact_size(out, payload.len() as u64);
            out.extend_from_slice(payload);
        }
        _ => out.extend_from_slice(payload),
    }
    kind
}

/// Read an on-disk v17 script payload and expand it to the wire scriptPubKey.
pub fn decode_script_kind_v17(kind: u8, buf: &[u8]) -> Result<(Vec<u8>, usize), StoreError> {
    if kind > SCRIPT_KIND_V17_MAX {
        return Err(StoreError::Corrupt("v17 reserved script kind"));
    }
    match kind {
        SCRIPT_KIND_V17_EMPTY | SCRIPT_KIND_V17_OP_TRUE | SCRIPT_KIND_V17_P2A => {
            Ok((expand_script_kind(kind, &[])?, 0))
        }
        SCRIPT_KIND_V17_P2PKH | SCRIPT_KIND_V17_P2SH | SCRIPT_KIND_V17_P2WPKH => {
            if buf.len() < 20 {
                return Err(StoreError::Corrupt("short v17 hash160 script payload"));
            }
            Ok((expand_script_kind(kind, &buf[..20])?, 20))
        }
        SCRIPT_KIND_V17_P2WSH | SCRIPT_KIND_V17_P2TR => {
            if buf.len() < 32 {
                return Err(StoreError::Corrupt("short v17 hash256 script payload"));
            }
            Ok((expand_script_kind(kind, &buf[..32])?, 32))
        }
        SCRIPT_KIND_V17_RAW | SCRIPT_KIND_V17_OP_RETURN_PUSH => {
            let (slen, n) = read_compact_size(buf)?;
            let slen = slen as usize;
            if buf.len() < n + slen {
                return Err(StoreError::Corrupt("v17 script payload truncated"));
            }
            Ok((expand_script_kind(kind, &buf[n..n + slen])?, n + slen))
        }
        _ => Err(StoreError::Corrupt("v17 reserved script kind")),
    }
}

fn hash160_payload(payload: &[u8]) -> Result<&[u8], StoreError> {
    if payload.len() != 20 {
        return Err(StoreError::Corrupt("v17 script kind expects 20-byte hash"));
    }
    Ok(payload)
}

fn hash256_payload(payload: &[u8]) -> Result<&[u8], StoreError> {
    if payload.len() != 32 {
        return Err(StoreError::Corrupt("v17 script kind expects 32-byte hash"));
    }
    Ok(payload)
}

/// Canonical single-push `OP_RETURN`: direct push 1..=75, or PUSHDATA1 only when n≥76.
fn canonical_op_return_push(script: &[u8]) -> Option<&[u8]> {
    if script.first() != Some(&0x6a) {
        return None;
    }
    let rest = &script[1..];
    let n0 = *rest.first()?;
    if (1..=75).contains(&n0) {
        let n = n0 as usize;
        if rest.len() == 1 + n {
            return Some(&rest[1..]);
        }
        return None;
    }
    if n0 == 0x4c && rest.len() >= 2 {
        let n = rest[1] as usize;
        if n >= 76 && rest.len() == 2 + n {
            return Some(&rest[2..]);
        }
    }
    None
}

fn encode_op_return_push(data: &[u8]) -> Result<Vec<u8>, StoreError> {
    if data.is_empty() || data.len() > 255 {
        return Err(StoreError::Corrupt("v17 OP_RETURN push length"));
    }
    let mut s = vec![0x6a];
    if data.len() <= 75 {
        s.push(data.len() as u8);
    } else {
        s.push(0x4c);
        s.push(data.len() as u8);
    }
    s.extend_from_slice(data);
    Ok(s)
}

// Keep the landing codecs compiled on the lib target until production cutover.
const _: () = {
    let _: fn(&[u8]) -> (u8, &[u8]) = classify_script;
    let _: fn(u8, &[u8]) -> Result<Vec<u8>, StoreError> = expand_script_kind;
    let _: fn(&[u8], &mut Vec<u8>) -> u8 = encode_script_kind_v17;
    let _: fn(u8, &[u8]) -> Result<(Vec<u8>, usize), StoreError> = decode_script_kind_v17;
};

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
            assert_eq!(compact_size_len(n), buf.len());
        }
        assert_eq!(compact_size_len(252), 1);
        assert_eq!(compact_size_len(253), 3);
        assert_eq!(compact_size_len(u16::MAX as u64), 3);
        assert_eq!(compact_size_len(u16::MAX as u64 + 1), 5);
        assert_eq!(compact_size_len(u32::MAX as u64), 5);
        assert_eq!(compact_size_len(u32::MAX as u64 + 1), 9);
        assert_eq!(uleb128_len(0), 1);
        assert_eq!(uleb128_len(127), 1);
        assert_eq!(uleb128_len(128), 2);
        assert!(uleb128_len(u64::MAX) >= 9);
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
