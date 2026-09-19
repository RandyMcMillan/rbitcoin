//! MEM_SCHEMA 2 packed live records (`fee‖weight‖txid‖wtxid‖tx‖vin aux`).
//!
//! Vin codec mirrors Class A packed flags/witness but writes `prev_txid` when
//! there is no Class A `create_fk`. Outputs reuse [`OutputRecord`] unspent
//! encode. Packed decode → `Transaction` → consensus serialize must equal the
//! admitted wire.

use crate::error::MempoolError;
use bitcoin::hashes::Hash;
use bitcoin::{Amount, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Txid, Witness, Wtxid};
use rbitcoin_primitives::Fk;
use rbitcoin_store::OutputRecord;

const VIN_SEQ_FINAL: u8 = 1 << 0;
const VIN_EMPTY_SCRIPT: u8 = 1 << 1;
const VIN_EMPTY_WITNESS: u8 = 1 << 2;
const VIN_HAS_CREATE_FK: u8 = 1 << 3;
const VIN_HAS_SCRIPT_HASH: u8 = 1 << 4;

/// Per-vin aux persisted with the packed body (SH index / tip-entry purge).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VinAux {
    pub prev_txid: Txid,
    pub vout: u32,
    pub script_hash: Option<[u8; 32]>,
    pub create_fk: Option<Fk>,
}

/// One LIVE body payload (schema 2).
#[derive(Debug, Clone)]
pub struct PackedLive {
    pub fee_sat: u64,
    pub weight: u64,
    pub txid: Txid,
    pub wtxid: Wtxid,
    pub tx: Transaction,
    pub vins: Vec<VinAux>,
}

pub fn encode_packed_live(
    tx: &Transaction,
    txid: &Txid,
    wtxid: &Wtxid,
    fee_sat: u64,
    weight: u64,
    vins: &[VinAux],
) -> Result<Vec<u8>, MempoolError> {
    if vins.len() != tx.input.len() && !vins.is_empty() {
        return Err(MempoolError::Corrupt("vin aux count"));
    }
    let mut out = Vec::with_capacity(80 + tx.input.len() * 80 + tx.output.len() * 40);
    out.extend_from_slice(&fee_sat.to_le_bytes());
    out.extend_from_slice(&weight.to_le_bytes());
    out.extend_from_slice(txid.as_byte_array());
    out.extend_from_slice(wtxid.as_byte_array());
    out.extend_from_slice(&tx.version.0.to_le_bytes());
    out.extend_from_slice(&tx.lock_time.to_consensus_u32().to_le_bytes());
    write_compact_size(&mut out, tx.input.len() as u64);
    write_compact_size(&mut out, tx.output.len() as u64);
    for (i, inp) in tx.input.iter().enumerate() {
        let aux = vins.get(i);
        encode_vin(&mut out, inp, aux);
    }
    for o in &tx.output {
        let v =
            i64::try_from(o.value.to_sat()).map_err(|_| MempoolError::Corrupt("txout amount"))?;
        OutputRecord::encode_unspent_into(v, o.script_pubkey.as_bytes(), &mut out);
    }
    Ok(out)
}

pub fn decode_packed_live(buf: &[u8]) -> Result<PackedLive, MempoolError> {
    if buf.len() < 16 + 32 + 32 + 8 {
        return Err(MempoolError::Corrupt("packed live short"));
    }
    let fee_sat = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    let weight = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    let txid = Txid::from_byte_array(buf[16..48].try_into().unwrap());
    let wtxid = Wtxid::from_byte_array(buf[48..80].try_into().unwrap());
    let mut off = 80usize;
    let version = i32::from_le_bytes(take_arr(buf, &mut off)?);
    let lock_time = u32::from_le_bytes(take_arr(buf, &mut off)?);
    let (n_in, n) = read_compact_size(&buf[off..])?;
    off += n;
    let (n_out, n) = read_compact_size(&buf[off..])?;
    off += n;
    if n_in > 100_000 || n_out > 100_000 {
        return Err(MempoolError::Corrupt("packed live counts"));
    }
    let mut input = Vec::with_capacity(n_in as usize);
    let mut vins = Vec::with_capacity(n_in as usize);
    for _ in 0..n_in {
        let (txin, aux, n) = decode_vin(&buf[off..])?;
        off += n;
        input.push(txin);
        vins.push(aux);
    }
    let mut output = Vec::with_capacity(n_out as usize);
    for _ in 0..n_out {
        let (rec, n) = OutputRecord::decode_at(&buf[off..])
            .map_err(|_| MempoolError::Corrupt("packed vout"))?;
        off += n;
        let value = if rec.value < 0 { Amount::ZERO } else { Amount::from_sat(rec.value as u64) };
        output.push(TxOut { value, script_pubkey: ScriptBuf::from_bytes(rec.script) });
    }
    if off != buf.len() {
        return Err(MempoolError::Corrupt("packed live trailing"));
    }
    let tx = Transaction {
        version: bitcoin::transaction::Version(version),
        lock_time: bitcoin::absolute::LockTime::from_consensus(lock_time),
        input,
        output,
    };
    Ok(PackedLive { fee_sat, weight, txid, wtxid, tx, vins })
}

fn encode_vin(out: &mut Vec<u8>, inp: &TxIn, aux: Option<&VinAux>) {
    let mut flags = 0u8;
    if inp.sequence == Sequence::MAX {
        flags |= VIN_SEQ_FINAL;
    }
    if inp.script_sig.is_empty() {
        flags |= VIN_EMPTY_SCRIPT;
    }
    if inp.witness.is_empty() {
        flags |= VIN_EMPTY_WITNESS;
    }
    if aux.is_some_and(|a| a.create_fk.is_some()) {
        flags |= VIN_HAS_CREATE_FK;
    }
    if aux.is_some_and(|a| a.script_hash.is_some()) {
        flags |= VIN_HAS_SCRIPT_HASH;
    }
    out.push(flags);
    out.extend_from_slice(inp.previous_output.txid.as_byte_array());
    write_compact_size(out, u64::from(inp.previous_output.vout));
    if flags & VIN_SEQ_FINAL == 0 {
        out.extend_from_slice(&inp.sequence.to_consensus_u32().to_le_bytes());
    }
    if flags & VIN_EMPTY_SCRIPT == 0 {
        let s = inp.script_sig.as_bytes();
        write_compact_size(out, s.len() as u64);
        out.extend_from_slice(s);
    }
    if flags & VIN_EMPTY_WITNESS == 0 {
        write_compact_size(out, inp.witness.len() as u64);
        for item in inp.witness.iter() {
            write_compact_size(out, item.len() as u64);
            out.extend_from_slice(item);
        }
    }
    if let Some(a) = aux {
        if let Some(fk) = a.create_fk {
            out.extend_from_slice(&fk.0.to_le_bytes());
        }
        if let Some(sh) = a.script_hash {
            out.extend_from_slice(&sh);
        }
    }
}

fn decode_vin(buf: &[u8]) -> Result<(TxIn, VinAux, usize), MempoolError> {
    if buf.is_empty() {
        return Err(MempoolError::Corrupt("packed vin short"));
    }
    let flags = buf[0];
    let mut off = 1usize;
    if flags & 0xE0 != 0 {
        return Err(MempoolError::Corrupt("packed vin flags"));
    }
    let prev_txid = Txid::from_byte_array(take_arr(buf, &mut off)?);
    let (vout64, n) = read_compact_size(&buf[off..])?;
    off += n;
    if vout64 > u64::from(u32::MAX) {
        return Err(MempoolError::Corrupt("packed vout"));
    }
    let vout = vout64 as u32;
    let sequence = if flags & VIN_SEQ_FINAL != 0 {
        Sequence::MAX
    } else {
        Sequence::from_consensus(u32::from_le_bytes(take_arr(buf, &mut off)?))
    };
    let script_sig = if flags & VIN_EMPTY_SCRIPT != 0 {
        ScriptBuf::new()
    } else {
        let (len, n) = read_compact_size(&buf[off..])?;
        off += n;
        let end = off.checked_add(len as usize).ok_or(MempoolError::Corrupt("packed script"))?;
        if end > buf.len() {
            return Err(MempoolError::Corrupt("packed script"));
        }
        let s = ScriptBuf::from_bytes(buf[off..end].to_vec());
        off = end;
        s
    };
    let witness = if flags & VIN_EMPTY_WITNESS != 0 {
        Witness::new()
    } else {
        let (n_items, n) = read_compact_size(&buf[off..])?;
        off += n;
        let mut items = Vec::with_capacity(n_items as usize);
        for _ in 0..n_items {
            let (len, n) = read_compact_size(&buf[off..])?;
            off += n;
            let end =
                off.checked_add(len as usize).ok_or(MempoolError::Corrupt("packed witness"))?;
            if end > buf.len() {
                return Err(MempoolError::Corrupt("packed witness"));
            }
            items.push(buf[off..end].to_vec());
            off = end;
        }
        Witness::from_slice(&items)
    };
    let create_fk = if flags & VIN_HAS_CREATE_FK != 0 {
        Some(Fk(u64::from_le_bytes(take_arr(buf, &mut off)?)))
    } else {
        None
    };
    let script_hash =
        if flags & VIN_HAS_SCRIPT_HASH != 0 { Some(take_arr::<32>(buf, &mut off)?) } else { None };
    let txin = TxIn {
        previous_output: bitcoin::OutPoint { txid: prev_txid, vout },
        script_sig,
        sequence,
        witness,
    };
    let aux = VinAux { prev_txid, vout, script_hash, create_fk };
    Ok((txin, aux, off))
}

fn take_arr<const N: usize>(buf: &[u8], off: &mut usize) -> Result<[u8; N], MempoolError> {
    let end = off.checked_add(N).ok_or(MempoolError::Corrupt("packed short"))?;
    if end > buf.len() {
        return Err(MempoolError::Corrupt("packed short"));
    }
    let arr = buf[*off..end].try_into().unwrap();
    *off = end;
    Ok(arr)
}

fn write_compact_size(out: &mut Vec<u8>, n: u64) {
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

fn read_compact_size(buf: &[u8]) -> Result<(u64, usize), MempoolError> {
    if buf.is_empty() {
        return Err(MempoolError::Corrupt("compact size empty"));
    }
    match buf[0] {
        n @ 0..=252 => Ok((u64::from(n), 1)),
        253 => {
            if buf.len() < 3 {
                return Err(MempoolError::Corrupt("compact size u16 truncated"));
            }
            let v = u16::from_le_bytes([buf[1], buf[2]]);
            Ok((u64::from(v), 3))
        }
        254 => {
            if buf.len() < 5 {
                return Err(MempoolError::Corrupt("compact size u32 truncated"));
            }
            let v = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            Ok((u64::from(v), 5))
        }
        255 => {
            if buf.len() < 9 {
                return Err(MempoolError::Corrupt("compact size u64 truncated"));
            }
            let v = u64::from_le_bytes(buf[1..9].try_into().unwrap());
            Ok((v, 9))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::absolute::LockTime;
    use bitcoin::consensus::encode::serialize;
    use bitcoin::transaction::Version;

    fn sample_tx(annex: bool) -> Transaction {
        let mut witness = Witness::new();
        witness.push([0x01, 0x02]);
        if annex {
            witness.push([0x50, 0x00, 0xaa]);
        }
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: bitcoin::OutPoint {
                    txid: Txid::from_byte_array([0xab; 32]),
                    vout: 1,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness,
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        }
    }

    #[test]
    fn packed_roundtrip_serialize_equals_admitted_raw() {
        let tx = sample_tx(true);
        let raw = serialize(&tx);
        let txid = tx.compute_txid();
        let wtxid = tx.compute_wtxid();
        let sh = [0x11u8; 32];
        let aux = [VinAux {
            prev_txid: tx.input[0].previous_output.txid,
            vout: 1,
            script_hash: Some(sh),
            create_fk: Some(Fk(42)),
        }];
        let packed = encode_packed_live(&tx, &txid, &wtxid, 123, 400, &aux).unwrap();
        let got = decode_packed_live(&packed).unwrap();
        assert_eq!(got.fee_sat, 123);
        assert_eq!(got.weight, 400);
        assert_eq!(got.txid, txid);
        assert_eq!(got.wtxid, wtxid);
        assert_eq!(serialize(&got.tx), raw, "consensus serialize pin");
        assert_eq!(got.vins[0].script_hash, Some(sh));
        assert_eq!(got.vins[0].create_fk, Some(Fk(42)));
    }
}
