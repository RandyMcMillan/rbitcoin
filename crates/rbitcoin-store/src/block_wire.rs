//! Bitcoin **block** wire walk: Σ `tx.input` without a `Block` decode.
//!
//! Peer enqueue stamps this so IBD lookup can pack/hold waves from the BQ
//! index. Invalid / truncated payloads yield `0`.

use crate::compact::read_compact_size;

const HEADER_LEN: usize = 80;

/// Σ input count over every tx in a serialized block (header + tx vector).
pub fn block_wire_input_count(payload: &[u8]) -> u32 {
    if payload.len() < HEADER_LEN {
        return 0;
    }
    let mut off = HEADER_LEN;
    let Some(n_tx) = read_count(payload, &mut off) else {
        return 0;
    };
    // Empty tx is ≥10 bytes; a huge CompactSize is truncated junk.
    if n_tx as usize > payload.len().saturating_sub(off) {
        return 0;
    }
    let mut inputs = 0u32;
    for _ in 0..n_tx {
        let Some(n_in) = skip_tx(payload, &mut off) else {
            return 0;
        };
        inputs = inputs.saturating_add(n_in);
    }
    inputs
}

fn read_count(buf: &[u8], off: &mut usize) -> Option<u32> {
    let (v, n) = read_compact_size(&buf[*off..]).ok()?;
    if v > u32::MAX as u64 {
        return None;
    }
    *off = off.checked_add(n)?;
    if *off > buf.len() {
        return None;
    }
    Some(v as u32)
}

fn skip_compact_blob(buf: &[u8], off: &mut usize) -> Option<()> {
    let (len, n) = read_compact_size(&buf[*off..]).ok()?;
    let len = usize::try_from(len).ok()?;
    *off = off.checked_add(n)?.checked_add(len)?;
    if *off > buf.len() {
        return None;
    }
    Some(())
}

fn skip_input(buf: &[u8], off: &mut usize) -> Option<()> {
    *off = off.checked_add(36)?;
    if *off > buf.len() {
        return None;
    }
    skip_compact_blob(buf, off)?;
    *off = off.checked_add(4)?;
    if *off > buf.len() {
        return None;
    }
    Some(())
}

fn skip_output(buf: &[u8], off: &mut usize) -> Option<()> {
    *off = off.checked_add(8)?;
    if *off > buf.len() {
        return None;
    }
    skip_compact_blob(buf, off)
}

/// Skip one tx; return its `nIn`.
fn skip_tx(buf: &[u8], off: &mut usize) -> Option<u32> {
    *off = off.checked_add(4)?;
    if *off > buf.len() {
        return None;
    }
    let mut witness = false;
    if *off + 1 < buf.len() && buf[*off] == 0 && buf[*off + 1] == 1 {
        witness = true;
        *off += 2;
    }
    let n_in = read_count(buf, off)?;
    for _ in 0..n_in {
        skip_input(buf, off)?;
    }
    let n_out = read_count(buf, off)?;
    for _ in 0..n_out {
        skip_output(buf, off)?;
    }
    if witness {
        for _ in 0..n_in {
            let n_stack = read_count(buf, off)?;
            for _ in 0..n_stack {
                skip_compact_blob(buf, off)?;
            }
        }
    }
    *off = off.checked_add(4)?;
    if *off > buf.len() {
        return None;
    }
    Some(n_in)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header() -> Vec<u8> {
        vec![0u8; HEADER_LEN]
    }

    fn compact(n: u64) -> Vec<u8> {
        let mut o = Vec::new();
        crate::compact::write_compact_size(&mut o, n);
        o
    }

    fn coinbase_tx(n_in: u32) -> Vec<u8> {
        let mut t = Vec::new();
        t.extend_from_slice(&1u32.to_le_bytes());
        t.extend_from_slice(&compact(u64::from(n_in)));
        for _ in 0..n_in {
            t.extend_from_slice(&[0u8; 32]);
            t.extend_from_slice(&u32::MAX.to_le_bytes());
            t.extend_from_slice(&compact(2));
            t.extend_from_slice(&[0, 0]);
            t.extend_from_slice(&u32::MAX.to_le_bytes());
        }
        t.extend_from_slice(&compact(1));
        t.extend_from_slice(&0u64.to_le_bytes());
        t.extend_from_slice(&compact(1));
        t.push(0x51);
        t.extend_from_slice(&0u32.to_le_bytes());
        t
    }

    fn witness_tx(n_in: u32) -> Vec<u8> {
        let mut t = Vec::new();
        t.extend_from_slice(&1u32.to_le_bytes());
        t.push(0);
        t.push(1);
        t.extend_from_slice(&compact(u64::from(n_in)));
        for _ in 0..n_in {
            t.extend_from_slice(&[0u8; 32]);
            t.extend_from_slice(&0u32.to_le_bytes());
            t.extend_from_slice(&compact(0));
            t.extend_from_slice(&u32::MAX.to_le_bytes());
        }
        t.extend_from_slice(&compact(1));
        t.extend_from_slice(&1u64.to_le_bytes());
        t.extend_from_slice(&compact(1));
        t.push(0x51);
        for _ in 0..n_in {
            t.extend_from_slice(&compact(0));
        }
        t.extend_from_slice(&0u32.to_le_bytes());
        t
    }

    fn block(txs: &[Vec<u8>]) -> Vec<u8> {
        let mut p = header();
        p.extend_from_slice(&compact(txs.len() as u64));
        for t in txs {
            p.extend_from_slice(t);
        }
        p
    }

    #[test]
    fn header_only_and_truncated_are_zero() {
        assert_eq!(block_wire_input_count(&[]), 0);
        assert_eq!(block_wire_input_count(&header()), 0);
        let mut p = block(&[coinbase_tx(1)]);
        p.pop();
        assert_eq!(block_wire_input_count(&p), 0);
    }

    #[test]
    fn coinbase_and_multi_input() {
        assert_eq!(block_wire_input_count(&block(&[coinbase_tx(1)])), 1);
        assert_eq!(
            block_wire_input_count(&block(&[coinbase_tx(1), coinbase_tx(3)])),
            4
        );
    }

    #[test]
    fn witness_flag_counts_vin_not_stack() {
        assert_eq!(block_wire_input_count(&block(&[witness_tx(2)])), 2);
        assert_eq!(
            block_wire_input_count(&block(&[coinbase_tx(1), witness_tx(2)])),
            3
        );
    }

    #[test]
    fn huge_ntx_compactsize_is_zero() {
        let mut p = header();
        p.extend_from_slice(&compact(u64::from(u32::MAX)));
        assert_eq!(block_wire_input_count(&p), 0);
    }
}
