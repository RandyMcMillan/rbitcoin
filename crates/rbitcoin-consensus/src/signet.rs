//! BIP325 signet consensus: every non-genesis tip block must satisfy the network challenge.
//!
//! Solution is embedded in the coinbase witness-commitment push after magic `ecc7daa2`.
//!
//! **When:** tip confirm / connect only — not Class A archive structure (ECDSA is too
//! expensive for the IBD prep path).

use bitcoin::absolute::LockTime;
use bitcoin::consensus::Encodable;
use bitcoin::hashes::{sha256d, Hash};
use bitcoin::script::{Script, ScriptBuf};
use bitcoin::{Amount, Block, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness};

use crate::error::ConsensusError;
use crate::script::interpreter::{self, EvalContext, SigVersion};

/// BIP325 magic prefix inside the witness commitment push.
const SIGNET_HEADER: [u8; 4] = [0xec, 0xc7, 0xda, 0xa2];

/// Default global signet challenge (Bitcoin Core `SigNetParams` without `-signetchallenge`).
///
/// `OP_1 <pubkey1> <pubkey2> OP_2 OP_CHECKMULTISIG` (1-of-2).
pub fn default_signet_challenge() -> ScriptBuf {
    ScriptBuf::from_bytes(hex_decode(
        "512103ad5e0edad18cb1f0fc0d28a3d4f1f3e445640337489abb10404f2d1e086be430210359ef5021964fe22d6f8e05b2463c9540ce96883fe3b278760f048f5189f2e6c452ae",
    ))
}

fn hex_decode(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

/// Validate BIP325 signet block solution against `challenge`.
pub fn validate_signet_block_solution(
    block: &Block,
    challenge: &Script,
) -> Result<(), ConsensusError> {
    // Genesis (null prev) is always valid.
    if block.header.prev_blockhash.to_byte_array() == [0u8; 32] {
        return Ok(());
    }

    let (to_spend, to_sign) = build_signet_txs(block, challenge)?;
    verify_challenge_spend(&to_spend, &to_sign, challenge)
}

fn build_signet_txs(
    block: &Block,
    challenge: &Script,
) -> Result<(Transaction, Transaction), ConsensusError> {
    if block.txdata.is_empty() {
        return Err(ConsensusError::BadBlock("signet: no coinbase"));
    }

    let mut modified_cb = block.txdata[0].clone();
    let cidx = witness_commitment_index(&modified_cb)
        .ok_or(ConsensusError::BadBlock("signet: no witness commitment"))?;

    let commitment_spk = modified_cb.output[cidx].script_pubkey.as_bytes().to_vec();
    // Core: no SIGNET_HEADER section is allowed only for trivial OP_TRUE challenges.
    let (solution, stripped) = match fetch_and_clear_signet_section(&commitment_spk) {
        Some(x) => x,
        None if challenge.as_bytes() == [0x51] => (Vec::new(), commitment_spk.clone()),
        None => return Err(ConsensusError::BadBlock("signet: no solution section")),
    };
    modified_cb.output[cidx].script_pubkey = ScriptBuf::from_bytes(stripped);

    let (script_sig, witness) = if solution.is_empty() {
        (ScriptBuf::new(), Witness::new())
    } else {
        parse_signet_solution(&solution)?
    };

    let signet_merkle = modified_merkle_root(&modified_cb, block)?;

    let mut block_data = Vec::new();
    block
        .header
        .version
        .to_consensus()
        .consensus_encode(&mut block_data)
        .map_err(|_| ConsensusError::BadBlock("signet encode"))?;
    block
        .header
        .prev_blockhash
        .consensus_encode(&mut block_data)
        .map_err(|_| ConsensusError::BadBlock("signet encode"))?;
    signet_merkle
        .consensus_encode(&mut block_data)
        .map_err(|_| ConsensusError::BadBlock("signet encode"))?;
    block
        .header
        .time
        .consensus_encode(&mut block_data)
        .map_err(|_| ConsensusError::BadBlock("signet encode"))?;

    // Core: `vin.emplace_back(COutPoint(), CScript(OP_0), 0)` then `scriptSig << block_data`.
    // scriptSig must be OP_0 + push(block_data) or the to_spend txid (and sighash) is wrong.
    let mut ss = vec![0x00]; // OP_0
    push_data(&mut ss, &block_data);
    let to_spend = Transaction {
        version: bitcoin::transaction::Version::non_standard(0),
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::null(),
            script_sig: ScriptBuf::from_bytes(ss),
            sequence: Sequence::ZERO,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_bytes(challenge.as_bytes().to_vec()),
        }],
    };
    let to_spend_txid = to_spend.compute_txid();

    let to_sign = Transaction {
        version: bitcoin::transaction::Version::non_standard(0),
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint {
                txid: to_spend_txid,
                vout: 0,
            },
            script_sig,
            sequence: Sequence::ZERO,
            witness,
        }],
        output: vec![TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_bytes(vec![0x6a]), // OP_RETURN
        }],
    };

    Ok((to_spend, to_sign))
}

fn push_data(out: &mut Vec<u8>, data: &[u8]) {
    if data.len() < 0x4c {
        out.push(data.len() as u8);
        out.extend_from_slice(data);
    } else if data.len() <= 0xff {
        out.push(0x4c);
        out.push(data.len() as u8);
        out.extend_from_slice(data);
    } else {
        out.push(0x4d);
        out.extend_from_slice(&(data.len() as u16).to_le_bytes());
        out.extend_from_slice(data);
    }
}

fn witness_commitment_index(coinbase: &Transaction) -> Option<usize> {
    for (i, out) in coinbase.output.iter().enumerate() {
        let b = out.script_pubkey.as_bytes();
        if b.len() >= 6 && b[0] == 0x6a {
            if find_subslice(b, &[0xaa, 0x21, 0xa9, 0xed]).is_some() {
                return Some(i);
            }
        }
    }
    None
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Extract signet solution after SIGNET_HEADER; return (solution, rewritten script).
fn fetch_and_clear_signet_section(spk: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut pc = 0usize;
    let mut solution: Option<Vec<u8>> = None;
    let mut replacement = Vec::new();

    while pc < spk.len() {
        let op = spk[pc];
        pc += 1;
        if op == 0x00 {
            replacement.push(0x00);
            continue;
        }
        if (1..=75).contains(&op) {
            let n = op as usize;
            if pc + n > spk.len() {
                return None;
            }
            let data = &spk[pc..pc + n];
            pc += n;
            if solution.is_none() {
                if let Some(sol) = extract_header_payload(data) {
                    solution = Some(sol);
                    // Keep only the 4-byte header in the rewritten push (Core behaviour).
                    push_data(&mut replacement, &SIGNET_HEADER);
                    continue;
                }
            }
            push_data(&mut replacement, data);
            continue;
        }
        if op == 0x4c && pc < spk.len() {
            let n = spk[pc] as usize;
            pc += 1;
            if pc + n > spk.len() {
                return None;
            }
            let data = &spk[pc..pc + n];
            pc += n;
            if solution.is_none() {
                if let Some(sol) = extract_header_payload(data) {
                    solution = Some(sol);
                    push_data(&mut replacement, &SIGNET_HEADER);
                    continue;
                }
            }
            replacement.push(0x4c);
            replacement.push(n as u8);
            replacement.extend_from_slice(data);
            continue;
        }
        if op == 0x4d && pc + 1 < spk.len() {
            let n = u16::from_le_bytes([spk[pc], spk[pc + 1]]) as usize;
            pc += 2;
            if pc + n > spk.len() {
                return None;
            }
            let data = &spk[pc..pc + n];
            pc += n;
            if solution.is_none() {
                if let Some(sol) = extract_header_payload(data) {
                    solution = Some(sol);
                    push_data(&mut replacement, &SIGNET_HEADER);
                    continue;
                }
            }
            replacement.push(0x4d);
            replacement.extend_from_slice(&(n as u16).to_le_bytes());
            replacement.extend_from_slice(data);
            continue;
        }
        replacement.push(op);
    }

    solution.map(|sol| (sol, replacement))
}

fn extract_header_payload(data: &[u8]) -> Option<Vec<u8>> {
    if data.len() > SIGNET_HEADER.len() && data.starts_with(&SIGNET_HEADER) {
        Some(data[SIGNET_HEADER.len()..].to_vec())
    } else {
        None
    }
}

fn parse_signet_solution(solution: &[u8]) -> Result<(ScriptBuf, Witness), ConsensusError> {
    let mut rdr = solution;
    let script_sig = read_script(&mut rdr)?;
    let witness = read_witness_stack(&mut rdr)?;
    if !rdr.is_empty() {
        return Err(ConsensusError::BadBlock("signet: extraneous solution data"));
    }
    Ok((script_sig, witness))
}

fn read_compact_size(rdr: &mut &[u8]) -> Result<u64, ConsensusError> {
    if rdr.is_empty() {
        return Err(ConsensusError::BadBlock("signet: compact size"));
    }
    let first = rdr[0];
    *rdr = &rdr[1..];
    match first {
        n @ 0..=252 => Ok(n as u64),
        253 => {
            if rdr.len() < 2 {
                return Err(ConsensusError::BadBlock("signet: compact size"));
            }
            let v = u16::from_le_bytes([rdr[0], rdr[1]]) as u64;
            *rdr = &rdr[2..];
            Ok(v)
        }
        254 => {
            if rdr.len() < 4 {
                return Err(ConsensusError::BadBlock("signet: compact size"));
            }
            let v = u32::from_le_bytes(rdr[..4].try_into().unwrap()) as u64;
            *rdr = &rdr[4..];
            Ok(v)
        }
        255 => {
            if rdr.len() < 8 {
                return Err(ConsensusError::BadBlock("signet: compact size"));
            }
            let v = u64::from_le_bytes(rdr[..8].try_into().unwrap());
            *rdr = &rdr[8..];
            Ok(v)
        }
    }
}

fn read_script(rdr: &mut &[u8]) -> Result<ScriptBuf, ConsensusError> {
    let n = read_compact_size(rdr)? as usize;
    if rdr.len() < n {
        return Err(ConsensusError::BadBlock("signet: scriptSig short"));
    }
    let s = ScriptBuf::from_bytes(rdr[..n].to_vec());
    *rdr = &rdr[n..];
    Ok(s)
}

fn read_witness_stack(rdr: &mut &[u8]) -> Result<Witness, ConsensusError> {
    let count = read_compact_size(rdr)? as usize;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let n = read_compact_size(rdr)? as usize;
        if rdr.len() < n {
            return Err(ConsensusError::BadBlock("signet: witness short"));
        }
        items.push(rdr[..n].to_vec());
        *rdr = &rdr[n..];
    }
    let refs: Vec<&[u8]> = items.iter().map(|v| v.as_slice()).collect();
    Ok(Witness::from_slice(&refs))
}

fn modified_merkle_root(
    modified_cb: &Transaction,
    block: &Block,
) -> Result<bitcoin::TxMerkleNode, ConsensusError> {
    let mut leaves: Vec<[u8; 32]> = Vec::with_capacity(block.txdata.len());
    leaves.push(modified_cb.compute_txid().to_byte_array());
    for tx in block.txdata.iter().skip(1) {
        leaves.push(tx.compute_txid().to_byte_array());
    }
    while leaves.len() > 1 {
        if leaves.len() % 2 == 1 {
            let last = *leaves.last().unwrap();
            leaves.push(last);
        }
        let mut next = Vec::with_capacity(leaves.len() / 2);
        for pair in leaves.chunks(2) {
            let mut buf = [0u8; 64];
            buf[..32].copy_from_slice(&pair[0]);
            buf[32..].copy_from_slice(&pair[1]);
            next.push(*sha256d::Hash::hash(&buf).as_byte_array());
        }
        leaves = next;
    }
    Ok(bitcoin::TxMerkleNode::from_byte_array(leaves[0]))
}

fn verify_challenge_spend(
    to_spend: &Transaction,
    to_sign: &Transaction,
    challenge: &Script,
) -> Result<(), ConsensusError> {
    let prevout = &to_spend.output[0];
    let mut stack: Vec<Vec<u8>> = Vec::new();
    interpreter::eval_script_sig_pushes(to_sign.input[0].script_sig.as_script(), &mut stack)
        .map_err(|_| ConsensusError::BadBlock("signet solution invalid"))?;
    let wit = &to_sign.input[0].witness;
    for i in 0..wit.len() {
        if let Some(item) = wit.nth(i) {
            stack.push(item.to_vec());
        }
    }

    let prevouts = [prevout.clone()];
    let ctx = EvalContext::new(
        to_sign,
        0,
        prevout.value,
        &prevouts,
        challenge,
        SigVersion::Base,
    );
    match interpreter::eval_script(challenge, &mut stack, &ctx) {
        Ok(need_clean) => {
            if need_clean {
                interpreter::require_clean_true(&stack)
                    .map_err(|_| ConsensusError::BadBlock("signet solution invalid"))?;
            }
            Ok(())
        }
        Err(_) => Err(ConsensusError::BadBlock("signet solution invalid")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::consensus::encode::deserialize;

    #[test]
    fn default_challenge_parses() {
        let c = default_signet_challenge();
        assert!(c.as_bytes().len() > 70);
        assert_eq!(c.as_bytes()[0], 0x51); // OP_1
    }

    #[test]
    fn extract_signet_header_payload() {
        let mut push = SIGNET_HEADER.to_vec();
        push.extend_from_slice(&[0x01, 0x02, 0x03]);
        let sol = extract_header_payload(&push).unwrap();
        assert_eq!(sol, vec![0x01, 0x02, 0x03]);
    }

    #[test]
    fn fetch_and_clear_rewrites_push() {
        // OP_RETURN + push(header||0xab)
        let mut spk = vec![0x6a, 0x05];
        spk.extend_from_slice(&SIGNET_HEADER);
        spk.push(0xab);
        let (sol, stripped) = fetch_and_clear_signet_section(&spk).unwrap();
        assert_eq!(sol, vec![0xab]);
        // stripped should still start with OP_RETURN
        assert_eq!(stripped[0], 0x6a);
    }

    /// Regression: height-1 global signet block must accept under BIP325.
    ///
    /// Bug class: `to_spend.scriptSig` missing leading `OP_0` before block_data push
    /// produced a wrong txid → CHECKMULTISIG failed → tip stuck at 0.
    #[test]
    fn signet_block_1_solution_valid() {
        let raw = include_bytes!("../tests/fixtures/signet_block_1.bin");
        let block: Block = deserialize(raw).expect("decode signet block 1");
        assert_eq!(
            block.header.block_hash().to_string(),
            "00000086d6b2636cb2a392d45edc4ec544a10024d30141c9adf4bfd9de533b53"
        );
        let challenge = default_signet_challenge();
        validate_signet_block_solution(&block, challenge.as_script())
            .expect("BIP325 solution for real signet height 1");
    }

    #[test]
    fn signet_block_1_rejects_mutated_solution() {
        let raw = include_bytes!("../tests/fixtures/signet_block_1.bin");
        let mut block: Block = deserialize(raw).expect("decode");
        // Flip a byte in the witness-commitment output (destroys signature).
        let spk = block.txdata[0].output[1].script_pubkey.as_bytes().to_vec();
        let mut bad = spk.clone();
        *bad.last_mut().unwrap() ^= 0xff;
        block.txdata[0].output[1].script_pubkey = ScriptBuf::from_bytes(bad);
        let challenge = default_signet_challenge();
        assert!(validate_signet_block_solution(&block, challenge.as_script()).is_err());
    }

    #[test]
    fn to_spend_script_sig_starts_with_op_0() {
        let raw = include_bytes!("../tests/fixtures/signet_block_1.bin");
        let block: Block = deserialize(raw).unwrap();
        let challenge = default_signet_challenge();
        let (to_spend, _) = build_signet_txs(&block, challenge.as_script()).unwrap();
        let ss = to_spend.input[0].script_sig.as_bytes();
        assert_eq!(ss[0], 0x00, "Core CScript(OP_0) then push(block_data)");
    }

    #[test]
    fn genesis_prev_skips_challenge() {
        let raw = include_bytes!("../tests/fixtures/signet_block_1.bin");
        let mut block: Block = deserialize(raw).unwrap();
        block.header.prev_blockhash = bitcoin::BlockHash::from_byte_array([0u8; 32]);
        // Empty txs would fail for non-genesis; genesis path returns Ok first.
        block.txdata.clear();
        validate_signet_block_solution(&block, Script::from_bytes(&[0x51])).unwrap();
    }

    #[test]
    fn empty_block_and_missing_commitment_errors() {
        let challenge = default_signet_challenge();
        let raw = include_bytes!("../tests/fixtures/signet_block_1.bin");
        let mut block: Block = deserialize(raw).unwrap();
        block.txdata.clear();
        assert!(matches!(
            build_signet_txs(&block, challenge.as_script()),
            Err(ConsensusError::BadBlock(_))
        ));
        let mut block: Block = deserialize(raw).unwrap();
        // Strip commitment-looking outputs from coinbase.
        block.txdata[0].output.clear();
        block.txdata[0].output.push(TxOut {
            value: Amount::from_sat(1),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        });
        assert!(matches!(
            build_signet_txs(&block, challenge.as_script()),
            Err(ConsensusError::BadBlock(_))
        ));
    }

    #[test]
    fn op_true_challenge_allows_missing_signet_section() {
        // Coinbase with bare witness commitment (no SIGNET_HEADER) + OP_TRUE challenge.
        let cb = Transaction {
            version: bitcoin::transaction::Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(vec![0x00, 0x01]),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::ZERO,
                script_pubkey: ScriptBuf::from_bytes({
                    let mut v = vec![0x6a, 0x24, 0xaa, 0x21, 0xa9, 0xed];
                    v.extend([0u8; 32]);
                    v
                }),
            }],
        };
        let block = Block {
            header: bitcoin::block::Header {
                version: bitcoin::block::Version::ONE,
                prev_blockhash: bitcoin::BlockHash::from_byte_array([1u8; 32]),
                merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
                time: 1,
                bits: bitcoin::CompactTarget::from_consensus(0x207f_ffff),
                nonce: 0,
            },
            txdata: vec![cb.clone()],
        };
        // No SIGNET_HEADER → error for real challenge.
        assert!(build_signet_txs(&block, default_signet_challenge().as_script()).is_err());
        // OP_TRUE challenge allows empty solution.
        let challenge = ScriptBuf::from_bytes(vec![0x51]);
        let (to_spend, to_sign) = build_signet_txs(&block, challenge.as_script()).unwrap();
        assert!(to_sign.input[0].script_sig.is_empty());
        assert_eq!(to_spend.output[0].script_pubkey.as_bytes(), &[0x51]);
        let _ = cb;
    }

    #[test]
    fn push_data_all_length_encodings() {
        let mut short = Vec::new();
        push_data(&mut short, &[0xab]);
        assert_eq!(short, vec![0x01, 0xab]);

        let mid = vec![0u8; 0x4c];
        let mut out = Vec::new();
        push_data(&mut out, &mid);
        assert_eq!(out[0], 0x4c);
        assert_eq!(out[1], 0x4c);
        assert_eq!(&out[2..], mid.as_slice());

        let long = vec![0x11u8; 300];
        let mut out2 = Vec::new();
        push_data(&mut out2, &long);
        assert_eq!(out2[0], 0x4d);
        assert_eq!(u16::from_le_bytes([out2[1], out2[2]]), 300);
        assert_eq!(&out2[3..], long.as_slice());
    }

    #[test]
    fn fetch_and_clear_pushdata1_and_pushdata2() {
        // OP_RETURN + PUSHDATA1(header||payload)
        let mut payload = SIGNET_HEADER.to_vec();
        payload.extend_from_slice(&[0xde, 0xad]);
        let mut spk = vec![0x6a, 0x4c, payload.len() as u8];
        spk.extend_from_slice(&payload);
        let (sol, stripped) = fetch_and_clear_signet_section(&spk).unwrap();
        assert_eq!(sol, vec![0xde, 0xad]);
        assert_eq!(stripped[0], 0x6a);

        // PUSHDATA2 path
        let mut payload2 = SIGNET_HEADER.to_vec();
        payload2.push(0xbe);
        let mut spk2 = vec![0x6a, 0x4d];
        spk2.extend_from_slice(&(payload2.len() as u16).to_le_bytes());
        spk2.extend_from_slice(&payload2);
        let (sol2, _) = fetch_and_clear_signet_section(&spk2).unwrap();
        assert_eq!(sol2, vec![0xbe]);

        // Truncated push → None
        assert!(fetch_and_clear_signet_section(&[0x05, 0x01]).is_none());
        assert!(fetch_and_clear_signet_section(&[0x4c, 0x05, 0x01]).is_none());
        assert!(fetch_and_clear_signet_section(&[0x4d, 0x05, 0x00, 0x01]).is_none());
        // OP_0 pass-through without solution
        assert!(fetch_and_clear_signet_section(&[0x00, 0x51]).is_none());
        // Non-header push retained
        let no_hdr = vec![0x03, 0xaa, 0xbb, 0xcc];
        assert!(fetch_and_clear_signet_section(&no_hdr).is_none());
        let _ = no_hdr;
    }

    #[test]
    fn compact_size_and_solution_parse_errors() {
        assert_eq!(read_compact_size(&mut (&[0u8][..])).unwrap(), 0);
        assert_eq!(read_compact_size(&mut (&[252u8][..])).unwrap(), 252);
        // 253 + u16 LE
        let mut r = &[253u8, 0x01, 0x00][..];
        assert_eq!(read_compact_size(&mut r).unwrap(), 1);
        // 254 + u32
        let mut r = &[254u8, 0x02, 0x00, 0x00, 0x00][..];
        assert_eq!(read_compact_size(&mut r).unwrap(), 2);
        // 255 + u64
        let mut r = &[255u8, 0x03, 0, 0, 0, 0, 0, 0, 0][..];
        assert_eq!(read_compact_size(&mut r).unwrap(), 3);

        assert!(read_compact_size(&mut (&[][..])).is_err());
        assert!(read_compact_size(&mut (&[253u8][..])).is_err());
        assert!(read_compact_size(&mut (&[254u8, 0][..])).is_err());
        assert!(read_compact_size(&mut (&[255u8, 0, 0, 0][..])).is_err());

        // Empty solution: scriptSig compact 0 + witness count 0.
        let (ss, wit) = parse_signet_solution(&[0x00, 0x00]).unwrap();
        assert!(ss.is_empty());
        assert_eq!(wit.len(), 0);
        // Extraneous tail.
        assert!(parse_signet_solution(&[0x00, 0x00, 0xff]).is_err());
        // Short script.
        assert!(parse_signet_solution(&[0x02, 0xaa]).is_err());
        // Short witness item.
        assert!(parse_signet_solution(&[0x00, 0x01, 0x02, 0xaa]).is_err());
    }

    #[test]
    fn modified_merkle_odd_leaf_count() {
        let raw = include_bytes!("../tests/fixtures/signet_block_1.bin");
        let block: Block = deserialize(raw).unwrap();
        // Two non-coinbase leaves force at least one pairing; invent a second dummy tx leaf
        // by cloning block with an extra empty-ish tx for merkle shape.
        let cb = block.txdata[0].clone();
        let root = modified_merkle_root(&cb, &block).unwrap();
        assert_ne!(root.to_byte_array(), [0u8; 32]);
        // Odd: only coinbase
        let solo = Block {
            header: block.header,
            txdata: vec![cb.clone()],
        };
        let r2 = modified_merkle_root(&cb, &solo).unwrap();
        assert_eq!(r2.to_byte_array(), cb.compute_txid().to_byte_array());
        let _ = cb;
    }

    #[test]
    fn witness_commitment_index_finds_magic() {
        let raw = include_bytes!("../tests/fixtures/signet_block_1.bin");
        let block: Block = deserialize(raw).unwrap();
        assert!(witness_commitment_index(&block.txdata[0]).is_some());
        assert!(find_subslice(b"hello world", b"world").is_some());
        assert!(find_subslice(b"abc", b"xyz").is_none());
    }

    #[test]
    fn push_data_mid_and_unknown_op_passthrough() {
        // push_data uses PUSHDATA2 for len > 0xff (no PUSHDATA4 branch).
        let mid = vec![0u8; 300];
        let mut out = Vec::new();
        push_data(&mut out, &mid);
        assert_eq!(out[0], 0x4d);
        assert_eq!(u16::from_le_bytes([out[1], out[2]]), 300);
        // Bare 0x4e is treated as a non-push opcode (no solution).
        assert!(fetch_and_clear_signet_section(&[0x4e, 0x05, 0x00, 0x00, 0x00, 0x01]).is_none());
        // OP_RETURN + OP_TRUE: no signet header → None.
        assert!(fetch_and_clear_signet_section(&[0x6a, 0x51]).is_none());
    }

    #[test]
    fn modified_merkle_three_leaves() {
        let raw = include_bytes!("../tests/fixtures/signet_block_1.bin");
        let block: Block = deserialize(raw).unwrap();
        let cb = block.txdata[0].clone();
        // Build block with coinbase + 2 dummy non-cb (odd non-cb count after strip).
        let dummy = |n: u8| Transaction {
            version: bitcoin::transaction::Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array([n; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let multi = Block {
            header: block.header,
            txdata: vec![cb.clone(), dummy(1), dummy(2)],
        };
        let root = modified_merkle_root(&cb, &multi).unwrap();
        assert_ne!(root.to_byte_array(), [0u8; 32]);
        let _ = cb;
    }
}
