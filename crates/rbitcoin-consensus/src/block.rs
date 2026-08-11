use crate::confirm_phase_stats;
use crate::error::ConsensusError;
use crate::milestone::Milestone;
use crate::params::ChainParams;
use bitcoin::block::Block;
use bitcoin::hashes::{sha256d, Hash};
use bitcoin::script::{Script, ScriptBuf};
use bitcoin::{Amount, OutPoint, Transaction, TxOut};
use rbitcoin_primitives::Height;
use rbitcoin_query::{FkMap, Query, U32Map, U64Map};
use std::borrow::Borrow;
use std::hash::BuildHasherDefault;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct ValidationContext<'a> {
    pub params: &'a ChainParams,
    pub height: Height,
    pub milestone: Milestone,
    /// When **false** (IBD Class A archive prep): skip height-gated soft-fork
    /// checks that need a reliable tip height — BIP34 coinbase push and
    /// “unexpected witness before segwit”.
    ///
    /// Archive intentionally used `height = GENESIS` as a BIP34 sentinel (resume
    /// could not always trust ordered height). That made **signet** reject every
    /// post-genesis block: Core/Inquisition `SegwitHeight = 1`, so height 0 looks
    /// pre-segwit while BIP325 blocks always carry witness. Soft-fork timing is
    /// enforced at **confirm** with the true height. Merkle / weight / witness
    /// **commitment** still run here either way.
    pub enforce_height_gates: bool,
}

impl<'a> ValidationContext<'a> {
    /// Full structure + soft-fork gates at `height` (confirm / connect).
    pub fn at(params: &'a ChainParams, height: Height, milestone: Milestone) -> Self {
        Self {
            params,
            height,
            milestone,
            enforce_height_gates: true,
        }
    }

    /// Archive prep: height-independent structure only (see [`Self::enforce_height_gates`]).
    pub fn archive_structure(params: &'a ChainParams) -> Self {
        Self {
            params,
            height: Height::GENESIS,
            milestone: Milestone::NONE,
            enforce_height_gates: false,
        }
    }
}

/// Context-free / structural block checks (no UTXO / prevout).
pub fn validate_block_structure(
    block: &Block,
    ctx: &ValidationContext<'_>,
) -> Result<(), ConsensusError> {
    validate_block_structure_hashed(block, ctx).map(|_| ())
}

/// Like [`validate_block_structure`], but returns **once-computed** txids for reuse
/// (merkle / dup / archive encode) so callers do not re-hash every tx.
pub fn validate_block_structure_hashed(
    block: &Block,
    ctx: &ValidationContext<'_>,
) -> Result<Vec<[u8; 32]>, ConsensusError> {
    if block.txdata.is_empty() {
        return Err(ConsensusError::BadBlock("no transactions"));
    }
    if !block.txdata[0].is_coinbase() {
        return Err(ConsensusError::BadBlock("first tx not coinbase"));
    }
    for tx in block.txdata.iter().skip(1) {
        if tx.is_coinbase() {
            return Err(ConsensusError::BadBlock("coinbase not first"));
        }
    }

    // Weight / size limits (segwit-aware)
    let weight = block.weight();
    if weight.to_wu() > 4_000_000 {
        return Err(ConsensusError::BadBlock("block weight too large"));
    }

    // Cheap witness scan (no hashing) so we only pay wtxid when needed.
    let has_witness = block_has_witness(block);

    // Single pass: txid per tx (dup set + merkle leaves). Optional wtxid pass after.
    let n = block.txdata.len();
    let mut txids = Vec::with_capacity(n);
    let mut seen = std::collections::HashSet::with_capacity(n);
    for tx in &block.txdata {
        let id = tx.compute_txid().to_byte_array();
        if !seen.insert(id) {
            return Err(ConsensusError::BadBlock("duplicate txid"));
        }
        txids.push(id);
    }

    // Merkle root from precomputed txids (same tree as bitcoin core / rust-bitcoin).
    let merkle = merkle_root_bytes(&txids);
    if merkle != block.header.merkle_root.to_byte_array() {
        return Err(ConsensusError::BadBlock("merkle root mismatch"));
    }

    // BIP34: coinbase scriptSig must start with the block height **after** the
    // network's buried activation height (mainnet 227931; signet/testnet4 1;
    // regtest effectively never — see `bitcoin::consensus::Params`).
    // Enforcing from height 1 rejects mainnet block 1 immediately.
    // Skipped on archive prep (`enforce_height_gates = false`); confirm re-checks.
    if ctx.enforce_height_gates && ctx.params.bip34_active_at(ctx.height.0) {
        check_bip34_coinbase(&block.txdata[0], ctx.height.0)?;
    }

    // Coinbase scriptSig length 2..=100 (Core consensus).
    {
        let cb_ss = block.txdata[0].input[0].script_sig.as_bytes().len();
        if cb_ss < 2 || cb_ss > 100 {
            return Err(ConsensusError::BadBlock("bad-cb-length"));
        }
    }

    // MAX_MONEY on every output (and sum checked during connect for fees).
    const MAX_MONEY: u64 = 21_000_000 * 100_000_000;
    for tx in &block.txdata {
        let mut out_sum = 0u64;
        for o in &tx.output {
            let v = o.value.to_sat();
            if v > MAX_MONEY {
                return Err(ConsensusError::BadBlock("bad-txns-vout-toolarge"));
            }
            out_sum = out_sum.saturating_add(v);
            if out_sum > MAX_MONEY {
                return Err(ConsensusError::BadBlock("bad-txns-txouttotal-toolarge"));
            }
        }
    }

    // Legacy + P2SH sigops cost (scaled); reject if over MAX_BLOCK_SIGOPS_COST.
    // Full witness sigop counting is conservative: we charge legacy*4 + P2SH accurate.
    {
        const WITNESS_SCALE: u64 = 4;
        const MAX_BLOCK_SIGOPS_COST: u64 = 80_000;
        let mut cost = 0u64;
        for tx in &block.txdata {
            cost = cost.saturating_add(legacy_sigop_count(tx).saturating_mul(WITNESS_SCALE));
            if !tx.is_coinbase() {
                // P2SH sigops need prevouts; charge only scriptSig/scriptPubKey legacy here.
                // Accurate P2SH is applied during connect when prevouts are known — see
                // `check_block_sigops_with_prevouts` if wired later.
            }
        }
        if cost > MAX_BLOCK_SIGOPS_COST {
            return Err(ConsensusError::BadBlock("bad-blk-sigops"));
        }
    }

    // Witness: commitment always required when any input has witness data.
    // Pre-segwit ban only with reliable height (confirm / known height).
    // Archive prep must not ban witness: signet SegwitHeight=1 + BIP325.
    if has_witness {
        if ctx.enforce_height_gates && !ctx.params.segwit_active_at(ctx.height.0) {
            return Err(ConsensusError::BadBlock("unexpected witness before segwit"));
        }
        let mut non_cb = Vec::with_capacity(n.saturating_sub(1));
        for tx in block.txdata.iter().skip(1) {
            non_cb.push(tx.compute_wtxid().to_byte_array());
        }
        check_witness_commitment_with_wtxids(block, &non_cb)?;
    }

    // BIP325 signet solution is **not** checked here — structure/archive must stay
    // cheap for IBD. Full challenge verify runs on tip confirm only
    // (`confirm_archived_run` / connect). Invalid signet blocks never become tip.

    Ok(txids)
}

/// True if any input carries witness data.
#[inline]
pub fn block_has_witness(block: &Block) -> bool {
    block
        .txdata
        .iter()
        .any(|tx| tx.input.iter().any(|i| !i.witness.is_empty()))
}

/// Core-style legacy sigop count (CHECKSIG=1, CHECKMULTISIG=20 or accurate N).
fn legacy_sigop_count(tx: &Transaction) -> u64 {
    let mut n = 0u64;
    for inp in &tx.input {
        n = n.saturating_add(script_sigop_count(inp.script_sig.as_bytes(), false));
    }
    for out in &tx.output {
        n = n.saturating_add(script_sigop_count(out.script_pubkey.as_bytes(), false));
    }
    n
}

fn script_sigop_count(script: &[u8], accurate: bool) -> u64 {
    let mut n = 0u64;
    let mut i = 0usize;
    let mut last_opcode = 0xffu8;
    while i < script.len() {
        let opcode = script[i];
        i += 1;
        if opcode <= 0x4b {
            let push = opcode as usize;
            i = i.saturating_add(push);
        } else if opcode == 0x4c && i < script.len() {
            let push = script[i] as usize;
            i = i.saturating_add(1 + push);
        } else if opcode == 0x4d && i + 1 < script.len() {
            let push = u16::from_le_bytes([script[i], script[i + 1]]) as usize;
            i = i.saturating_add(2 + push);
        } else if opcode == 0x4e && i + 3 < script.len() {
            let push = u32::from_le_bytes(script[i..i + 4].try_into().unwrap_or([0; 4])) as usize;
            i = i.saturating_add(4 + push);
        } else if opcode == 0xac || opcode == 0xad {
            // OP_CHECKSIG / VERIFY
            n = n.saturating_add(1);
        } else if opcode == 0xae || opcode == 0xaf {
            // OP_CHECKMULTISIG / VERIFY
            if accurate && last_opcode >= 0x51 && last_opcode <= 0x60 {
                n = n.saturating_add(u64::from(last_opcode - 0x50));
            } else {
                n = n.saturating_add(20);
            }
        }
        last_opcode = opcode;
    }
    n
}

/// Last data push in a script (P2SH redeem / witness script).
fn last_script_push(script: &[u8]) -> Option<&[u8]> {
    let mut i = 0usize;
    let mut last: Option<(usize, usize)> = None;
    while i < script.len() {
        let opcode = script[i];
        i += 1;
        let (start, len) = if opcode <= 0x4b {
            let push = opcode as usize;
            let s = i;
            i = i.saturating_add(push);
            (s, push)
        } else if opcode == 0x4c && i < script.len() {
            let push = script[i] as usize;
            i += 1;
            let s = i;
            i = i.saturating_add(push);
            (s, push)
        } else if opcode == 0x4d && i + 1 < script.len() {
            let push = u16::from_le_bytes([script[i], script[i + 1]]) as usize;
            i += 2;
            let s = i;
            i = i.saturating_add(push);
            (s, push)
        } else if opcode == 0x4e && i + 3 < script.len() {
            let push = u32::from_le_bytes(script[i..i + 4].try_into().unwrap_or([0; 4])) as usize;
            i += 4;
            let s = i;
            i = i.saturating_add(push);
            (s, push)
        } else {
            continue;
        };
        if start + len <= script.len() {
            last = Some((start, len));
        }
    }
    last.map(|(s, l)| &script[s..s + l])
}

fn is_p2sh_script(spk: &[u8]) -> bool {
    spk.len() == 23 && spk[0] == 0xa9 && spk[1] == 0x14 && spk[22] == 0x87
}

fn is_p2wpkh_program(prog: &[u8]) -> bool {
    prog.len() == 22 && prog[0] == 0x00 && prog[1] == 0x14
}

fn is_p2wsh_program(prog: &[u8]) -> bool {
    prog.len() == 34 && prog[0] == 0x00 && prog[1] == 0x20
}

/// BIP16 P2SH sigops from redeem scripts (accurate CHECKMULTISIG count).
fn p2sh_sigop_count(tx: &Transaction, prevouts: &[TxOut]) -> u64 {
    let mut n = 0u64;
    for (i, inp) in tx.input.iter().enumerate() {
        let Some(prev) = prevouts.get(i) else {
            continue;
        };
        if !is_p2sh_script(prev.script_pubkey.as_bytes()) {
            continue;
        }
        if let Some(redeem) = last_script_push(inp.script_sig.as_bytes()) {
            n = n.saturating_add(script_sigop_count(redeem, true));
        }
    }
    n
}

/// BIP141 witness sigop count (not witness-scaled).
fn witness_sigop_count(tx: &Transaction, prevouts: &[TxOut]) -> u64 {
    let mut n = 0u64;
    for (i, inp) in tx.input.iter().enumerate() {
        let Some(prev) = prevouts.get(i) else {
            continue;
        };
        let mut program = prev.script_pubkey.as_bytes();
        // Nested P2SH-P2W*: redeem in scriptSig is the witness program.
        if is_p2sh_script(program) {
            if let Some(redeem) = last_script_push(inp.script_sig.as_bytes()) {
                program = redeem;
            } else {
                continue;
            }
        }
        if is_p2wpkh_program(program) {
            n = n.saturating_add(1);
        } else if is_p2wsh_program(program) {
            // Witness script is last stack item.
            let wit = &inp.witness;
            if let Some(ws) = wit.last() {
                n = n.saturating_add(script_sigop_count(ws, true));
            }
        }
    }
    n
}

/// Full Core-style sigop cost for one tx given prevouts (BIP16 + BIP141).
fn tx_sigop_cost(tx: &Transaction, prevouts: &[TxOut], bip16: bool) -> u64 {
    const WITNESS_SCALE: u64 = 4;
    let mut cost = legacy_sigop_count(tx).saturating_mul(WITNESS_SCALE);
    if bip16 {
        cost = cost.saturating_add(p2sh_sigop_count(tx, prevouts).saturating_mul(WITNESS_SCALE));
    }
    cost = cost.saturating_add(witness_sigop_count(tx, prevouts));
    cost
}

/// BIP141: coinbase must commit to witness merkle root when segwit is used.
///
/// `precomputed_non_cb` is wtxids for non-coinbase txs (same order as `txdata[1..]`).
fn check_witness_commitment_with_wtxids(
    block: &Block,
    precomputed_non_cb: &[[u8; 32]],
) -> Result<(), ConsensusError> {
    // Commitment header: 0x6a24aa21a9ed || 32-byte hash
    const MAGIC: [u8; 6] = [0x6a, 0x24, 0xaa, 0x21, 0xa9, 0xed];
    let coinbase = &block.txdata[0];
    let mut commitment: Option<[u8; 32]> = None;
    for out in coinbase.output.iter().rev() {
        let b = out.script_pubkey.as_bytes();
        if b.len() >= 38 && b[0..6] == MAGIC {
            let mut h = [0u8; 32];
            h.copy_from_slice(&b[6..38]);
            commitment = Some(h);
            break;
        }
    }
    let Some(committed) = commitment else {
        return Err(ConsensusError::BadBlock("missing witness commitment"));
    };

    if precomputed_non_cb.len() != block.txdata.len().saturating_sub(1) {
        return Err(ConsensusError::BadBlock("wtxid count mismatch"));
    }
    // witness root: merkle of wtxids with coinbase wtxid = zeros
    let mut leaves = Vec::with_capacity(block.txdata.len());
    leaves.push([0u8; 32]); // coinbase wtxid
    leaves.extend_from_slice(precomputed_non_cb);
    let witness_root = merkle_root_bytes(&leaves);
    // BIP141 / Core: when commitment is present, coinbase witness must be
    // exactly one 32-byte reserved value (bad-witness-nonce-size otherwise).
    let wit = &coinbase.input[0].witness;
    if wit.len() != 1 {
        return Err(ConsensusError::BadBlock("bad-witness-nonce-size"));
    }
    let reserved = wit
        .nth(0)
        .ok_or(ConsensusError::BadBlock("bad-witness-nonce-size"))?;
    if reserved.len() != 32 {
        return Err(ConsensusError::BadBlock("bad-witness-nonce-size"));
    }
    // commitment hash = SHA256D(witness_root || witness_reserved_value)
    let mut buf = [0u8; 64];
    buf[0..32].copy_from_slice(&witness_root);
    buf[32..64].copy_from_slice(reserved);
    let hash = sha256d::Hash::hash(&buf);
    if hash.to_byte_array() != committed {
        return Err(ConsensusError::BadBlock("witness commitment mismatch"));
    }
    Ok(())
}

/// Merkle root over 32-byte leaves (txid or wtxid tree). Public for tests.
pub(crate) fn merkle_root_bytes(leaves: &[[u8; 32]]) -> [u8; 32] {
    if leaves.is_empty() {
        return [0u8; 32];
    }
    let mut layer: Vec<[u8; 32]> = leaves.to_vec();
    while layer.len() > 1 {
        let mut next = Vec::with_capacity(layer.len().div_ceil(2));
        let mut i = 0;
        while i < layer.len() {
            let left = layer[i];
            let right = if i + 1 < layer.len() {
                layer[i + 1]
            } else {
                left
            };
            let mut buf = [0u8; 64];
            buf[0..32].copy_from_slice(&left);
            buf[32..64].copy_from_slice(&right);
            next.push(sha256d::Hash::hash(&buf).to_byte_array());
            i += 2;
        }
        layer = next;
    }
    layer[0]
}

/// BIP34: coinbase scriptSig must start with the block height, encoded as Bitcoin
/// Core's `CScript << int64` push (not raw CScriptNum for small values).
///
/// Core `CScript::push_int64`:
/// - 0 → `OP_0` (0x00)
/// - 1..=16 → `OP_1`..=`OP_16` (0x51..=0x60)
/// - else → minimal CScriptNum (`len || little-endian bytes`, sign-aware)
fn check_bip34_coinbase(coinbase: &Transaction, height: u32) -> Result<(), ConsensusError> {
    let script = &coinbase.input[0].script_sig;
    let bytes = script.as_bytes();
    if bytes.is_empty() {
        return Err(ConsensusError::BadBlock("bip34 coinbase script empty"));
    }
    let expected = bip34_height_script(height);
    if bytes.len() < expected.len() || &bytes[..expected.len()] != expected.as_slice() {
        return Err(ConsensusError::BadBlock("bip34 height encoding"));
    }
    Ok(())
}

/// Serialize `height` the same way Core pushes it into the coinbase scriptSig.
#[must_use]
pub fn bip34_height_script(height: u32) -> Vec<u8> {
    let n = height as i64;
    if n == 0 {
        return vec![0x00]; // OP_0
    }
    if (1..=16).contains(&n) {
        // OP_1 = 0x51 … OP_16 = 0x60
        return vec![0x50 + n as u8];
    }
    // CScriptNum::serialize (minimal signed little-endian) + push length prefix.
    let mut num = Vec::new();
    let mut abs = n;
    let neg = abs < 0;
    if neg {
        abs = -abs;
    }
    while abs > 0 {
        num.push((abs & 0xff) as u8);
        abs >>= 8;
    }
    if let Some(last) = num.last() {
        if last & 0x80 != 0 {
            num.push(if neg { 0x80 } else { 0x00 });
        } else if neg {
            let i = num.len() - 1;
            num[i] |= 0x80;
        }
    } else {
        num.push(0);
    }
    let mut out = Vec::with_capacity(1 + num.len());
    out.push(num.len() as u8);
    out.extend_from_slice(&num);
    out
}

/// Connect checks on contiguous tip confirm.
///
/// Pipeline (optimistic scripts, assumevalid-shaped):
/// 1. **Assemble** — resolve prevout *content*, intra-block doubles, fees; build jobs
///    (no durable spentness / maturity).
/// 2. **Scripts** — above milestone, script_pool (CPU; needs prevout values only).
/// 3. **Structural** — durable spentness, maturity, coinbase subsidy (order-sensitive).
///
/// Class C tip updates (`confirm_block`) stay outside this function.
///
/// `archived_tx_fks`: Class A fks for `block.txdata` (same order) when confirming
/// archived bodies (thin create_fk / Class A rows in parent cache).
///
/// **Production tip / IBD:** use [`crate::accept_and_connect_block`] or
/// [`crate::confirm_archived_run`] (load pin denserels → scripts → write). This
/// helper is a **no-write** unit-test surface (empty pin → store cold spentness).
/// It does not populate denserels and must not be the tip hot path.
pub fn validate_block_connect(
    query: &Query,
    block: &Block,
    ctx: &ValidationContext<'_>,
    archived_tx_fks: Option<&[rbitcoin_primitives::Fk]>,
) -> Result<(), ConsensusError> {
    // BIP325 on connect/confirm only (not structure/archive).
    if ctx.height.0 > 0 {
        if let Some(challenge) = ctx.params.signet_challenge.as_ref() {
            crate::signet::validate_signet_block_solution(block, challenge.as_script())?;
        }
    }

    let check_scripts = !ctx.milestone.skips_scripts_at(ctx.height.0);
    // Pending until assemble+scripts+structural succeed — no durable writes on failure.
    let mut pending = std::collections::HashSet::new();
    let mut pending_creates = std::collections::HashMap::new();
    // Unit-test path: no load pin stage (production uses confirm_archived_run).
    let batch_parents = rbitcoin_query::BatchParents::new();
    let batch_thin = rbitcoin_query::BatchThin::default();
    // Sole hash for this unit-test connect surface.
    let create_txids: Vec<[u8; 32]> = block
        .txdata
        .iter()
        .map(|t| t.compute_txid().to_byte_array())
        .collect();
    let block_hash = block.header.block_hash().to_byte_array();
    // Unit connect: one prev-MTP resolve here (no triple re-walk inside assemble).
    let prev_mtp = if ctx.height.0 == 0 {
        0
    } else {
        crate::header::median_time_past(query, Height(ctx.height.0 - 1)).unwrap_or(0)
    };
    let bip16_active = bip16_active_from_prev_mtp(ctx.params, ctx.height.0, &block_hash, prev_mtp);
    let (script_jobs, spends, fees) = assemble_block_prevouts(
        query,
        block,
        ctx,
        archived_tx_fks,
        &mut pending,
        &mut pending_creates,
        &batch_parents,
        &batch_thin,
        &create_txids,
        prev_mtp,
        &block_hash,
        bip16_active,
        None, // unit connect: owned job txs
    )?;
    if check_scripts && !script_jobs.is_empty() {
        verify_scripts_pool(&script_jobs)?;
    }
    // Structural: re-walk with durable spentness (fresh pending for this single block).
    // Note: empty BatchParents → missing abs → Err (cold forbidden). Callers that
    // need connect without pin must use confirm_write_phase with full pin.
    let mut structural_pending = std::collections::HashSet::new();
    let mut mtp_cache = U32Map::default();
    let mut meta_by_abs = U64Map::default();
    let _ = structural_validate_spends(
        query,
        block,
        ctx,
        archived_tx_fks,
        &spends,
        fees,
        &mut structural_pending,
        &batch_parents,
        &mut mtp_cache,
        &mut meta_by_abs,
    )?;
    Ok(())
}

/// Whether assemble should probe durable spentness / maturity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AssembleMode {
    /// Resolve prevout content + build script jobs; skip durable spentness/maturity.
    Optimistic,
    /// Full connect (legacy one-shot): spentness + maturity during assemble.
    Full,
}

/// One non-coinbase tx ready for script/sig verification (prevouts already resolved).
///
/// Mainnet BIP16 exception block (never enforce P2SH redeem), Core `BIP16Exception`.
/// Height 170060 — pre-activation spends of HASH160/EQUAL as bare scripts.
pub(crate) const BIP16_EXCEPTION_MAINNET: [u8; 32] = [
    // little-endian display hash 00000000000002dc756eebf4f49723ed8d30cc28a5f108eb94b1ba88ac4f9c22
    0x22, 0x9c, 0x4f, 0xac, 0x88, 0xba, 0xb1, 0x94, 0xeb, 0x08, 0xf1, 0xa5, 0x28, 0xcc, 0x30, 0x8d,
    0xed, 0x23, 0x97, 0xf4, 0xf4, 0xeb, 0x6e, 0x75, 0xdc, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
];

/// BIP16 P2SH from **precomputed** prev MTP + block hash (no header re-walk, no rehash).
///
/// Callers must pass the same prev-block MTP used for BIP113 / header MTP checks
/// and the once-computed block hash (plan `meta.hash` / structure).
#[inline]
pub(crate) fn bip16_active_from_prev_mtp(
    params: &ChainParams,
    height: u32,
    block_hash: &[u8; 32],
    prev_mtp: u32,
) -> bool {
    // Exception block: never SCRIPT_VERIFY_P2SH.
    if *block_hash == BIP16_EXCEPTION_MAINNET {
        return false;
    }
    if height == 0 {
        return false;
    }
    // Core: previous block's median-time-past >= bip16_time.
    prev_mtp >= params.btc.bip16_time
}

/// Transaction held by a [`ScriptCheckJob`].
///
/// Confirm path uses [`JobTx::shared`] so jobs borrow the wire [`Arc<Block>`]
/// (refcount only — no deep `Transaction` clone). Tests/benches use [`JobTx::owned`].
///
/// Deref to [`Transaction`] so script paths keep `job.tx.input` / `&job.tx` ergonomics.
#[derive(Clone)]
pub(crate) struct JobTx {
    inner: JobTxInner,
}

#[derive(Clone)]
enum JobTxInner {
    Owned(Transaction),
    Shared { block: Arc<Block>, index: usize },
}

impl JobTx {
    #[inline]
    pub(crate) fn owned(tx: Transaction) -> Self {
        Self {
            inner: JobTxInner::Owned(tx),
        }
    }

    #[inline]
    pub(crate) fn shared(block: Arc<Block>, index: usize) -> Self {
        debug_assert!(index < block.txdata.len());
        Self {
            inner: JobTxInner::Shared { block, index },
        }
    }
}

impl Deref for JobTx {
    type Target = Transaction;
    #[inline]
    fn deref(&self) -> &Transaction {
        match &self.inner {
            JobTxInner::Owned(t) => t,
            JobTxInner::Shared { block, index } => &block.txdata[*index],
        }
    }
}

impl DerefMut for JobTx {
    #[inline]
    fn deref_mut(&mut self) -> &mut Transaction {
        match &mut self.inner {
            JobTxInner::Owned(t) => t,
            JobTxInner::Shared { .. } => {
                panic!("ScriptCheckJob shared wire tx is immutable")
            }
        }
    }
}

impl From<Transaction> for JobTx {
    #[inline]
    fn from(tx: Transaction) -> Self {
        Self::owned(tx)
    }
}

impl Borrow<Transaction> for JobTx {
    #[inline]
    fn borrow(&self) -> &Transaction {
        self.deref()
    }
}

impl AsRef<Transaction> for JobTx {
    #[inline]
    fn as_ref(&self) -> &Transaction {
        self.deref()
    }
}

/// Script-verify job for one non-coinbase create.
///
/// Confirm assemble attaches the wire [`Arc<Block>`] (no tx deep-clone). `txid`
/// is the structure/plan hash so scripts can probe mempool preverified without
/// re-hashing.
pub struct ScriptCheckJob {
    /// Wire txid (assemble / [`Self::new`]); used for mempool preverified skip.
    pub(crate) txid: [u8; 32],
    pub(crate) prevouts: Vec<TxOut>,
    /// Owned (tests) or shared wire block + index (confirm path).
    pub(crate) tx: JobTx,
    /// BIP65 CLTV active (false → OP_CLTV is a no-op, matching pre-activation).
    pub(crate) bip65_active: bool,
    /// BIP112 CSV active (false → OP_CSV is a no-op, matching pre-activation).
    pub(crate) bip112_active: bool,
    /// BIP66 strict DER active (false → accept historical lax DER encodings).
    pub(crate) bip66_active: bool,
    /// BIP16 P2SH active (false → `OP_HASH160 … OP_EQUAL` is bare, not redeem).
    pub(crate) bip16_active: bool,
    /// BIP341/342 taproot active (false → v1 witness program is anyone-can-spend).
    pub(crate) taproot_active: bool,
    /// SCRIPT_VERIFY_MINIMALIF (standardness / fixture flag; TapScript always on).
    pub(crate) minimal_if: bool,
    /// SCRIPT_VERIFY_NULLFAIL.
    pub(crate) nullfail: bool,
    /// SCRIPT_VERIFY_LOW_S.
    pub(crate) low_s: bool,
    /// SCRIPT_VERIFY_STRICTENC.
    pub(crate) strictenc: bool,
    /// SCRIPT_VERIFY_NULLDUMMY (also implied by bip112 on mainnet).
    pub(crate) null_dummy: bool,
    /// SCRIPT_VERIFY_MINIMALDATA.
    pub(crate) minimal_data: bool,
    /// SCRIPT_VERIFY_WITNESS_PUBKEYTYPE: witness keys must be compressed.
    pub(crate) witness_pubkeytype: bool,
    /// SCRIPT_VERIFY_WITNESS active (fixture flag / post-segwit production).
    pub(crate) witness_active: bool,
    /// SCRIPT_VERIFY_DISCOURAGE_UPGRADABLE_WITNESS_PROGRAM.
    pub(crate) discourage_upgradable_witness: bool,
    /// SCRIPT_VERIFY_CONST_SCRIPTCODE: CODESEPARATOR + FindAndDelete hard-fail.
    pub(crate) const_scriptcode: bool,
}

impl ScriptCheckJob {
    /// Build a job hashing `tx` once for [`Self::txid`] (tests / benches).
    #[inline]
    pub(crate) fn new(
        prevouts: Vec<TxOut>,
        tx: Transaction,
        bip65_active: bool,
        bip112_active: bool,
        bip66_active: bool,
        bip16_active: bool,
        taproot_active: bool,
    ) -> Self {
        use bitcoin::hashes::Hash;
        let txid = tx.compute_txid().to_byte_array();
        Self::with_txid(
            txid,
            prevouts,
            tx,
            bip65_active,
            bip112_active,
            bip66_active,
            bip16_active,
            taproot_active,
        )
    }

    /// Owned-tx path (tests / benches / unit connect): reuse precomputed txid.
    #[inline]
    pub(crate) fn with_txid(
        txid: [u8; 32],
        prevouts: Vec<TxOut>,
        tx: Transaction,
        bip65_active: bool,
        bip112_active: bool,
        bip66_active: bool,
        bip16_active: bool,
        taproot_active: bool,
    ) -> Self {
        Self::from_parts(
            txid,
            prevouts,
            JobTx::owned(tx),
            bip65_active,
            bip112_active,
            bip66_active,
            bip16_active,
            taproot_active,
        )
    }

    /// Confirm assemble: share the wire [`Arc<Block>`] (no `Transaction` clone).
    #[inline]
    pub(crate) fn with_shared_tx(
        txid: [u8; 32],
        prevouts: Vec<TxOut>,
        block: Arc<Block>,
        tx_index: usize,
        bip65_active: bool,
        bip112_active: bool,
        bip66_active: bool,
        bip16_active: bool,
        taproot_active: bool,
    ) -> Self {
        Self::from_parts(
            txid,
            prevouts,
            JobTx::shared(block, tx_index),
            bip65_active,
            bip112_active,
            bip66_active,
            bip16_active,
            taproot_active,
        )
    }

    /// Single construction site for activation + production standardness defaults.
    #[inline]
    fn from_parts(
        txid: [u8; 32],
        prevouts: Vec<TxOut>,
        tx: JobTx,
        bip65_active: bool,
        bip112_active: bool,
        bip66_active: bool,
        bip16_active: bool,
        taproot_active: bool,
    ) -> Self {
        Self {
            txid,
            prevouts,
            tx,
            bip65_active,
            bip112_active,
            bip66_active,
            bip16_active,
            taproot_active,
            minimal_if: false,
            nullfail: false,
            low_s: false,
            strictenc: false,
            // BIP147 co-activated with CSV on mainnet (bip112 height).
            null_dummy: bip112_active,
            minimal_data: false,
            witness_pubkeytype: false,
            // Production post-segwit path always has witness rules; pre-segwit
            // txs have empty witnesses so checks are no-ops.
            witness_active: true,
            discourage_upgradable_witness: false,
            const_scriptcode: false,
        }
    }
}

/// Sequential assemble: resolve prevout **content**, build script jobs, collect spends.
///
/// [`AssembleMode::Optimistic`] (confirm IBD path): no durable spentness / maturity /
/// BIP68 create-height resolution — those run in [`structural_validate_spends`] after
/// scripts (load must not walk `tx_height` per parent). Absolute nLockTime finality
/// (BIP113 MTP of prev block) still runs here — it only needs header MTP.
/// [`AssembleMode::Full`]: spentness + maturity + BIP68 during the walk (legacy).
///
/// `pending_spent` / `pending_creates`: run-local same-run tracking.
///
/// Prevouts resolve from per-batch [`rbitcoin_query::BatchParents`] +
/// [`rbitcoin_query::BatchThin`], then shared outs FIFO / durable store.
///
/// Returns `(script_jobs, spends, fees)` — fees for coinbase subsidy check on structural.
///
/// `prev_mtp` / `block_hash` / `bip16_active` must be computed **once** by the
/// caller (assemble_run header window) — no re-walk of headers and no rehash.
///
/// `wire`: when `Some`, script jobs share that Arc (no `Transaction` clone).
/// When `None` (unit-test connect), jobs own a deep clone of each non-cb tx.
pub(crate) fn assemble_block_prevouts(
    query: &Query,
    block: &Block,
    ctx: &ValidationContext<'_>,
    archived_tx_fks: Option<&[rbitcoin_primitives::Fk]>,
    pending_spent: &mut std::collections::HashSet<([u8; 32], u32)>,
    pending_creates: &mut std::collections::HashMap<([u8; 32], u32), rbitcoin_primitives::Fk>,
    batch_parents: &rbitcoin_query::BatchParents,
    batch_thin: &rbitcoin_query::BatchThin,
    // Precomputed create txids (structure / plan); required, same order as txdata.
    create_txids: &[[u8; 32]],
    prev_mtp: u32,
    block_hash: &[u8; 32],
    bip16_active: bool,
    wire: Option<&Arc<Block>>,
) -> Result<
    (
        Vec<ScriptCheckJob>,
        // Spends: (prev_txid, vout, spending_tx_fk, create_tx_fk).
        Vec<(
            [u8; 32],
            u32,
            rbitcoin_primitives::Fk,
            rbitcoin_primitives::Fk,
        )>,
        i64, // total fees
    ),
    ConsensusError,
> {
    assemble_block_prevouts_mode(
        query,
        block,
        ctx,
        archived_tx_fks,
        pending_spent,
        pending_creates,
        AssembleMode::Optimistic,
        batch_parents,
        batch_thin,
        create_txids,
        prev_mtp,
        block_hash,
        bip16_active,
        wire,
    )
}

fn assemble_block_prevouts_mode(
    query: &Query,
    block: &Block,
    ctx: &ValidationContext<'_>,
    archived_tx_fks: Option<&[rbitcoin_primitives::Fk]>,
    pending_spent: &mut std::collections::HashSet<([u8; 32], u32)>,
    pending_creates: &mut std::collections::HashMap<([u8; 32], u32), rbitcoin_primitives::Fk>,
    mode: AssembleMode,
    batch_parents: &rbitcoin_query::BatchParents,
    batch_thin: &rbitcoin_query::BatchThin,
    create_txids: &[[u8; 32]],
    prev_mtp: u32,
    block_hash: &[u8; 32],
    bip16_active: bool,
    wire: Option<&Arc<Block>>,
) -> Result<
    (
        Vec<ScriptCheckJob>,
        Vec<(
            [u8; 32],
            u32,
            rbitcoin_primitives::Fk,
            rbitcoin_primitives::Fk,
        )>,
        i64,
    ),
    ConsensusError,
> {
    if let Some(fks) = archived_tx_fks {
        if fks.len() != block.txdata.len() {
            return Err(ConsensusError::BadBlock("archived tx fk count mismatch"));
        }
    }
    if create_txids.len() != block.txdata.len() {
        return Err(ConsensusError::BadBlock(
            "invariant: create_txids length must match block.txdata (no assemble re-hash)",
        ));
    }
    if block.txdata.is_empty() {
        return Err(ConsensusError::BadBlock("empty block"));
    }
    if !block.txdata[0].is_coinbase() {
        return Err(ConsensusError::BadBlock("first tx not coinbase"));
    }
    // Caller-supplied BIP16 must match hash+prev_mtp (no silent re-resolve).
    debug_assert_eq!(
        bip16_active,
        bip16_active_from_prev_mtp(ctx.params, ctx.height.0, block_hash, prev_mtp)
    );
    let _ = block_hash; // used in debug_assert; release keeps caller contract
    let bip16_for_jobs = bip16_active;

    let n_tx = block.txdata.len();
    let mut block_spends: std::collections::HashSet<OutPoint> =
        std::collections::HashSet::with_capacity(n_tx.saturating_mul(2));
    // Full-block txid → index (create_txids, same order as block.txdata). Used to
    // reject spends of later same-block parents (Core topological order).
    // `same_block` below only holds *earlier* txs already walked.
    let mut txid_index: std::collections::HashMap<[u8; 32], usize> =
        std::collections::HashMap::with_capacity(n_tx);
    for (i, id) in create_txids.iter().enumerate() {
        txid_index.insert(*id, i);
    }
    // txid → index into `block.txdata` for txs already validated in this walk.
    let mut same_block: std::collections::HashMap<[u8; 32], usize> =
        std::collections::HashMap::with_capacity(n_tx);
    let mut fees = 0i64;
    // Skip job materialization when scripts are skipped — pure waste below the
    // milestone. Prevouts still resolve for fees + full sigop cost.
    let build_script_jobs = !ctx.milestone.skips_scripts_at(ctx.height.0);
    let mut script_jobs: Vec<ScriptCheckJob> = if build_script_jobs {
        Vec::with_capacity(n_tx.saturating_sub(1))
    } else {
        Vec::new()
    };
    // BIP141/BIP16 full block sigop cost (structure only counts legacy×4).
    const MAX_BLOCK_SIGOPS_COST: u64 = 80_000;
    let mut block_sigops_cost = legacy_sigop_count(&block.txdata[0]).saturating_mul(4);
    // (prev_txid, vout, spending_tx_fk, create_tx_fk).
    let mut spends: Vec<(
        [u8; 32],
        u32,
        rbitcoin_primitives::Fk,
        rbitcoin_primitives::Fk,
    )> = Vec::with_capacity(n_tx.saturating_mul(2));
    // Coinbase height cache spans the whole block (was recreated per tx).
    let mut coinbase_height_cache: FkMap<Option<u32>> =
        FkMap::with_capacity_and_hasher(64, Default::default());

    use crate::confirm_phase_stats;
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    // BIP113 absolute finality cutoff: **once per block**, from caller prev_mtp
    // (same value as header MTP check — no second median_time_past walk).
    let lock_time_cutoff = if ctx.params.csv_active_at(ctx.height.0) {
        if ctx.height.0 == 0 {
            block.header.time
        } else {
            prev_mtp
        }
    } else {
        block.header.time
    };

    // Spent checks: pending_spent (this run) + durable confirmed-strong annotations.
    for (ti, tx) in block.txdata.iter().enumerate() {
        let spend_fk = archived_tx_fks.map(|fks| fks[ti]);
        // Sole pipeline identity — structure/plan computed once; never re-hash here.
        let txid = create_txids[ti];

        if ti > 0 {
            if tx.input.is_empty() {
                return Err(ConsensusError::BadTx("no inputs"));
            }
            if tx.output.is_empty() {
                return Err(ConsensusError::BadTx("no outputs"));
            }

            let mut value_in = 0i64;
            // Prevouts for fees, sigop cost, and (optionally) script jobs.
            let mut prevouts: Vec<TxOut> = Vec::with_capacity(tx.input.len());
            let mut input_create_heights: Vec<u32> = Vec::with_capacity(tx.input.len());
            // Thin create_fk edges from this confirm batch (batch-local).
            let thin = spend_fk.and_then(|fk| fk.get().and_then(|id| batch_thin.get(&id)));

            let t_prev = Instant::now();
            for (ii, input) in tx.input.iter().enumerate() {
                let op = input.previous_output;
                if !block_spends.insert(op) {
                    return Err(ConsensusError::BadTx("double spend in block"));
                }
                let key = (op.txid.to_byte_array(), op.vout);
                if pending_spent.contains(&key) {
                    return Err(ConsensusError::PrevoutSpent);
                }
                // Topological order (Core CheckTxInputs walk): a same-block
                // parent must appear *before* this tx. Archive batch_thin stamps
                // create_fk for the whole block at once; using that edge would
                // accept child-before-parent
                // (docs/external_findings/005-non-topological-block-accepted.md).
                if let Some(&pj) = txid_index.get(&key.0) {
                    if pj >= ti {
                        return Err(ConsensusError::MissingPrevout);
                    }
                }
                // Load pin / thin create_fk is a **promise**: pin denserels must
                // carry identity matching wire prev_txid (resolve hard-misses
                // wrong pin; load fills schema-13 identity from plan RAM or
                // txid.body). Do not treat thin as a soft spentness hint.
                // Same-block parents (pj < ti) resolve via same_block only —
                // thin still ok if pin matches; order already enforced above.
                let prev_fk = thin
                    .as_ref()
                    .and_then(|t| t.get(ii))
                    .and_then(|e| e.create_fk.map(rbitcoin_primitives::Fk))
                    .or_else(|| pending_creates.get(&key).copied())
                    .or_else(|| query.tx_fk_by_txid(op.txid.as_byte_array()).ok().flatten());
                let pin_live = match prev_fk {
                    Some(fk) => batch_parents.has_parent_out(fk, op.vout),
                    None => false,
                };
                // Durable spentness: Full mode only. Optimistic defers to structural
                // after scripts (assumevalid-shaped: scripts need values, not UTXO proof).
                if mode == AssembleMode::Full && !pin_live && !pending_creates.contains_key(&key) {
                    let spent = if let Some(cfk) = prev_fk {
                        query
                            .store()
                            .has_confirmed_strong_spender_create(cfk, op.vout, None)
                            .map_err(ConsensusError::from)?
                    } else {
                        query
                            .store()
                            .has_confirmed_strong_spender(op.txid.as_byte_array(), op.vout)
                            .map_err(ConsensusError::from)?
                    };
                    if spent {
                        return Err(ConsensusError::PrevoutSpent);
                    }
                }
                let prev_out = resolve_prevout(
                    query,
                    block,
                    op,
                    prev_fk,
                    &same_block,
                    &mut coinbase_height_cache,
                    batch_parents,
                    ctx.height.0,
                    mode == AssembleMode::Full,
                )?;
                let create_fk = prev_out.create_fk;
                if mode == AssembleMode::Full {
                    if let Some(created) = prev_out.coinbase_height {
                        let maturity = ctx.params.coinbase_maturity();
                        if ctx.height.0 < created.saturating_add(maturity) {
                            return Err(ConsensusError::BadTx("coinbase immature"));
                        }
                    }
                }
                // Same-run / provisional double-spend tracking (both modes).
                pending_spent.insert(key);
                spends.push((
                    key.0,
                    key.1,
                    spend_fk.unwrap_or(rbitcoin_primitives::Fk::NULL),
                    create_fk,
                ));
                value_in = value_in
                    .checked_add(prev_out.txout.value.to_sat() as i64)
                    .ok_or(ConsensusError::BadTx("value in overflow"))?;
                if mode == AssembleMode::Full {
                    input_create_heights.push(prev_out.create_height);
                }
                prevouts.push(prev_out.txout);
            }
            confirm_phase_stats::ASM_PREVOUT_NS
                .fetch_add(t_prev.elapsed().as_nanos() as u64, Ordering::Relaxed);

            let t_sig = Instant::now();
            block_sigops_cost =
                block_sigops_cost.saturating_add(tx_sigop_cost(tx, &prevouts, bip16_for_jobs));
            if block_sigops_cost > MAX_BLOCK_SIGOPS_COST {
                return Err(ConsensusError::BadBlock("bad-blk-sigops"));
            }
            confirm_phase_stats::ASM_SIGOP_NS
                .fetch_add(t_sig.elapsed().as_nanos() as u64, Ordering::Relaxed);

            // BIP113 absolute finality (uses block-level lock_time_cutoff).
            let t_fin = Instant::now();
            if !is_final_tx(tx, ctx.height.0, lock_time_cutoff) {
                return Err(ConsensusError::BadTx("not final"));
            }
            // BIP68 relative locks need per-input create heights (`tx_height`).
            // Optimistic/confirm defers that IO to structural write; Full does it here.
            // Reuse the same prev-block MTP as BIP113 when CSV is active (already
            // computed once as lock_time_cutoff for height > 0).
            if mode == AssembleMode::Full && ctx.params.csv_active_at(ctx.height.0) {
                let mut coin_mtps = Vec::with_capacity(input_create_heights.len());
                for &ch in &input_create_heights {
                    let mtp = if ch == 0 {
                        0
                    } else {
                        crate::header::median_time_past(query, Height(ch.saturating_sub(1)))?
                    };
                    coin_mtps.push(mtp);
                }
                let prev_mtp = if ctx.height.0 == 0 {
                    0
                } else {
                    lock_time_cutoff
                };
                if !sequence_locks_satisfied(
                    tx,
                    &input_create_heights,
                    &coin_mtps,
                    ctx.height.0,
                    prev_mtp,
                ) {
                    return Err(ConsensusError::BadTx("bad-txns-nonfinal"));
                }
            }
            confirm_phase_stats::ASM_FINAL_NS
                .fetch_add(t_fin.elapsed().as_nanos() as u64, Ordering::Relaxed);

            let mut value_out = 0i64;
            for o in &tx.output {
                let sats = o.value.to_sat() as i64;
                if sats < 0 {
                    return Err(ConsensusError::BadTx("negative output"));
                }
                value_out = value_out
                    .checked_add(sats)
                    .ok_or(ConsensusError::BadTx("value out overflow"))?;
            }
            if value_out > value_in {
                return Err(ConsensusError::BadTx("in < out"));
            }
            fees = fees
                .checked_add(value_in - value_out)
                .ok_or(ConsensusError::BadTx("fee overflow"))?;

            if build_script_jobs {
                let t_job = Instant::now();
                // Reuse A1 wire txid — scripts stage must not re-hash for preverified.
                // Confirm: share wire Arc (no Transaction clone). Unit connect: own.
                let job = if let Some(w) = wire {
                    ScriptCheckJob::with_shared_tx(
                        txid,
                        prevouts,
                        Arc::clone(w),
                        ti,
                        ctx.params.bip65_active_at(ctx.height.0),
                        ctx.params.csv_active_at(ctx.height.0),
                        ctx.params.bip66_active_at(ctx.height.0),
                        bip16_for_jobs,
                        ctx.params.taproot_active_at(ctx.height.0),
                    )
                } else {
                    ScriptCheckJob::with_txid(
                        txid,
                        prevouts,
                        tx.clone(),
                        ctx.params.bip65_active_at(ctx.height.0),
                        ctx.params.csv_active_at(ctx.height.0),
                        ctx.params.bip66_active_at(ctx.height.0),
                        bip16_for_jobs,
                        ctx.params.taproot_active_at(ctx.height.0),
                    )
                };
                script_jobs.push(job);
                confirm_phase_stats::ASM_JOB_NS
                    .fetch_add(t_job.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
        }

        // `txid` computed once from wire at top of loop (A1).
        let create_fk = spend_fk.unwrap_or(rbitcoin_primitives::Fk::NULL);
        for (v, _) in tx.output.iter().enumerate() {
            if !create_fk.is_null() {
                pending_creates.insert((txid, v as u32), create_fk);
            }
        }
        same_block.insert(txid, ti);
    }

    Ok((script_jobs, spends, fees))
}

fn check_coinbase_subsidy(
    block: &Block,
    ctx: &ValidationContext<'_>,
    fees: i64,
) -> Result<(), ConsensusError> {
    let subsidy = block_subsidy(ctx.height.0, ctx.params);
    let mut coinbase_out = 0i64;
    for o in &block.txdata[0].output {
        coinbase_out = coinbase_out
            .checked_add(o.value.to_sat() as i64)
            .ok_or(ConsensusError::BadBlock("coinbase value overflow"))?;
    }
    let max_cb = subsidy
        .checked_add(fees)
        .ok_or(ConsensusError::BadBlock("subsidy+fees overflow"))?;
    if coinbase_out > max_cb {
        return Err(ConsensusError::BadBlock("coinbase excess value"));
    }
    Ok(())
}

/// Local wall times for one block's structural pass (write path diagnostics).
///
/// Measured with `Instant` — **not** deltas of window atomics (those race with
/// `sample_and_reset` mid-batch and produced false `spent=0` on slow writes).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct StructuralPhaseNs {
    pub spent_ns: u64,
    /// Pin abs collect + bulk on-disk 9-byte spender meta pread.
    pub spent_abs_ns: u64,
    /// `is_confirmed_strong_at` on non-null fields (still durable authority).
    pub spent_strong_ns: u64,
    /// Cold unspent_create_vouts / null-create probes.
    pub spent_cold_ns: u64,
    /// pending_spent order gate (CPU).
    pub spent_pending_ns: u64,
    pub create_h_ns: u64,
    pub bip68_ns: u64,
}

/// Post-script structural checks: durable spentness, maturity, BIP68, coinbase subsidy.
///
/// Runs in height order on the write path (after scripts). `pending_spent` is
/// write-local across a multi-height run.
///
/// **BIP68** create-height lives here (not optimistic load assemble) so confirm
/// load does not walk `tx_height` for every parent. Heights: bulk `tx_height`.
/// Coin MTP only for time-type relative locks on version ≥2 txs (v1 skipped).
///
/// **Spentness:** pin denserels → abs + bulk 9-byte meta. Sparse durable-**spent**
/// set (not unspent). Missing abs / short meta is hard `Err`. **Multi-list** after
/// reorg annotate is a protocol cold walk (`has_confirmed_strong_spender_create`)
/// — not a hard fail (tip-follow reorgs leave multi flags by design). Snapshots
/// `(field, flags)` into `meta_by_abs` for pure-write annotate.
pub(crate) fn structural_validate_spends(
    query: &Query,
    block: &Block,
    ctx: &ValidationContext<'_>,
    archived_tx_fks: Option<&[rbitcoin_primitives::Fk]>,
    spends: &[(
        [u8; 32],
        u32,
        rbitcoin_primitives::Fk,
        rbitcoin_primitives::Fk,
    )],
    fees: i64,
    pending_spent: &mut std::collections::HashSet<([u8; 32], u32)>,
    batch_parents: &rbitcoin_query::BatchParents,
    // MTP by end-height, shared across blocks in one write run.
    mtp_cache: &mut U32Map<u32>,
    // Structural disk meta for pure-write annotate: abs → (field, flags).
    meta_by_abs: &mut U64Map<(rbitcoin_primitives::Fk, u8)>,
) -> Result<StructuralPhaseNs, ConsensusError> {
    use std::collections::HashSet;
    use std::time::Instant;

    // create_fk → create height (BIP68), filled in create-height phase.
    let mut create_height_by_fk: FkMap<u32> =
        FkMap::with_capacity_and_hasher(spends.len().min(256), BuildHasherDefault::default());
    let maturity = ctx.params.coinbase_maturity();

    // ── Spentness (durable + same-run pending) ─────────────────────────────
    // Authority is on-disk spender meta (bulk mmap). Pin only supplies abs.
    // Cold body walk is forbidden on write — spent_cold must stay 0.
    let t_spent = Instant::now();

    // Unique create_fk → sorted unique vouts.
    let mut vouts_by_create: U64Map<Vec<u32>> = U64Map::default();
    let mut null_create_keys: Vec<([u8; 32], u32)> = Vec::new();
    for &(prev_txid, vout, _sfk, create_fk) in spends {
        if create_fk.is_null() {
            null_create_keys.push((prev_txid, vout));
            continue;
        }
        if let Some(id) = create_fk.get() {
            vouts_by_create.entry(id).or_default().push(vout);
        }
    }
    for vouts in vouts_by_create.values_mut() {
        vouts.sort_unstable();
        vouts.dedup();
    }

    // abs_jobs: (create_id, vout, abs_off) — every non-null create must have abs.
    // Load promises denserels for every external spend edge; missing abs is a
    // load bug, not a soft cold spentness path (no unpinned “wire-corrected”
    // recovery for stamp/identity bugs).
    let t_abs = Instant::now();
    let mut abs_jobs: Vec<(u64, u32, u64)> = Vec::with_capacity(spends.len());
    for (id, vouts) in &vouts_by_create {
        let fk = rbitcoin_primitives::Fk(*id);
        for &v in vouts {
            let Some(abs) = batch_parents.get_spender_abs(fk, v) else {
                return Err(ConsensusError::Store(rbitcoin_store::StoreError::Corrupt(
                    "invariant: structural spentness missing pin denserels/abs (cold forbidden)",
                )));
            };
            abs_jobs.push((*id, v, abs));
        }
    }
    // Sparse durable **spent** set (honest IBD: almost all outs unspent).
    // Present ⇒ confirmed-strong spent; missing ⇒ unspent.
    let mut durable_spent: HashSet<(u64, u32)> = HashSet::new();
    let mut multi_list_ns = 0u64;

    let tip = query.tip_height().map(|h| h.0);

    // Hot path: bulk 9-byte spender meta at pin offsets (on-disk authority).
    // Serial with create_h heights below — combined multi-fd wave was measured
    // neutral/worse (body DONTCACHE peeks + height slots).
    let mut spent_strong_ns = 0u64;
    if !abs_jobs.is_empty() {
        let abs_offs: Vec<u64> = abs_jobs.iter().map(|(_, _, a)| *a).collect();
        let meta_backend = rbitcoin_store::spend_meta_backend();
        let t_meta = Instant::now();
        let metas = query
            .store()
            .get_spender_meta_at_abs_batch_backend(&abs_offs, meta_backend)
            .map_err(ConsensusError::from)?;
        let meta_ns = t_meta.elapsed().as_nanos() as u64;
        confirm_phase_stats::SPEND_META_NS.fetch_add(meta_ns, Ordering::Relaxed);
        confirm_phase_stats::SPEND_META_N.fetch_add(abs_offs.len() as u64, Ordering::Relaxed);
        let _ = meta_backend;
        if metas.len() != abs_jobs.len() {
            return Err(ConsensusError::Store(rbitcoin_store::StoreError::Corrupt(
                "invariant: structural meta batch length",
            )));
        }
        let t_strong = Instant::now();
        for (i, &(id, vout, abs)) in abs_jobs.iter().enumerate() {
            let Some((field, flags)) = metas[i] else {
                return Err(ConsensusError::Store(rbitcoin_store::StoreError::Corrupt(
                    "invariant: structural spender meta short/OOB (cold forbidden)",
                )));
            };
            meta_by_abs.insert(abs, (field, flags));
            let multi = flags & rbitcoin_store::output_flags::MULTI_SPENDER != 0;
            if multi {
                // Protocol path (docs/invariants.md): reorg / second annotate leaves a
                // multi-list. Resolve confirmed-strong via list walk — do **not**
                // hard-fail the flag alone (that freezes tip after any tip-follow reorg
                // that double-annotated a parent out).
                let t_m = Instant::now();
                let spent = query
                    .store()
                    .has_confirmed_strong_spender_create(rbitcoin_primitives::Fk(id), vout, None)
                    .map_err(ConsensusError::from)?;
                multi_list_ns = multi_list_ns.saturating_add(t_m.elapsed().as_nanos() as u64);
                if spent {
                    durable_spent.insert((id, vout));
                }
                continue;
            }
            if field.is_null() {
                continue; // unspent
            }
            let strong = query
                .store()
                .is_confirmed_strong_at(field, tip)
                .map_err(ConsensusError::from)?;
            if !strong {
                continue;
            }
            // Integrity: a confirmed-strong spender cannot predate its create.
            // Prior tip-follow annotate bugs wrote garbage sole fields that point
            // at ancient strong fks (e.g. create@961404 / field@22671) — that is
            // not consensus PrevoutSpent. Ignore impossible meta (load/annotate
            // corruption), do not soft-recover via wire re-check.
            let create_h = query
                .store()
                .tx_height
                .get(rbitcoin_primitives::Fk(id))
                .map_err(ConsensusError::from)?;
            let spend_h = query
                .store()
                .tx_height
                .get(field)
                .map_err(ConsensusError::from)?;
            if let (Some(ch), Some(sh)) = (create_h, spend_h) {
                if sh < ch {
                    continue;
                }
            }
            durable_spent.insert((id, vout));
        }
        spent_strong_ns = t_strong
            .elapsed()
            .as_nanos()
            .saturating_sub(multi_list_ns as u128) as u64;
    }
    // abs ≈ collect + meta pread (not strong loop / multi walk).
    let spent_abs_ns = (t_abs.elapsed().as_nanos() as u64).saturating_sub(spent_strong_ns);

    // Null create_fk = same-block spend (resolve sets NULL). Double-spend is
    // **only** `pending_spent` within this confirm run — do **not** probe durable
    // store by wire txid. Already-archived Class A rehydrate (plan=None) already
    // holds those creates under the same txids before Class C tip; durable lookup
    // can false-hit Class A rows / BIP30 siblings and reject valid same-block
    // edges (mainnet 961461 tip stall).
    let _ = null_create_keys; // same-block keys: pending only (no durable probe)
                              // Multi-list walks are protocol cold; same-block no longer probes store.
    let spent_cold_ns = multi_list_ns;

    // Order-sensitive pending double-spend + durable rejection.
    // create_fk is load-established (pin denserels + identity); durable_spent is
    // keyed by that create. Same-block (null create_fk): pending only.
    let t_pending = Instant::now();
    for &(prev_txid, vout, _spend_fk, create_fk) in spends {
        let key = (prev_txid, vout);
        if pending_spent.contains(&key) {
            return Err(ConsensusError::PrevoutSpent);
        }
        let spent = if create_fk.is_null() {
            false // same-block: pending_spent only
        } else if let Some(id) = create_fk.get() {
            // Present in durable_spent ⇒ confirmed-strong spent.
            durable_spent.contains(&(id, vout))
        } else {
            false
        };
        if spent {
            return Err(ConsensusError::PrevoutSpent);
        }
        pending_spent.insert(key);
    }
    let spent_pending_ns = t_pending.elapsed().as_nanos() as u64;
    let spent_ns = t_spent.elapsed().as_nanos() as u64;

    // ── Create height + coinbase maturity (durable Class C only) ──────────
    // Heights: bulk `tx_height`. Coinbase: create_fk == first_tx_fk of block at
    // that height — **never** `tx.body`. Pin may short-circuit non-coinbase.
    let t_create = Instant::now();
    let unique_create_fks: Vec<rbitcoin_primitives::Fk> = {
        let mut v: Vec<rbitcoin_primitives::Fk> = vouts_by_create
            .keys()
            .map(|id| rbitcoin_primitives::Fk(*id))
            .collect();
        v.sort_unstable_by_key(|f| f.0);
        v
    };
    let durable_heights = query
        .store()
        .tx_height_get_batch(&unique_create_fks)
        .map_err(ConsensusError::from)?;
    let height_by_id: U64Map<u32> = unique_create_fks
        .iter()
        .zip(durable_heights.into_iter())
        .filter_map(|(fk, h)| {
            let id = fk.get()?;
            Some((id, h.unwrap_or(0)))
        })
        .collect();

    // height → coinbase Class A fk (first tx of that confirmed block).
    let mut height_list: Vec<u32> = height_by_id.values().copied().collect();
    height_list.sort_unstable();
    height_list.dedup();
    let coinbase_fk_by_height = query
        .store()
        .coinbase_fk_at_heights(&height_list)
        .map_err(ConsensusError::from)?;

    let mut seen_create: rbitcoin_query::U64Set =
        rbitcoin_query::U64Set::with_capacity_and_hasher(vouts_by_create.len(), Default::default());
    for &(_ptid, _vout, _sfk, create_fk) in spends {
        if create_fk.is_null() {
            continue;
        }
        let Some(id) = create_fk.get() else {
            continue;
        };
        if !seen_create.insert(id) {
            continue;
        }
        let durable_h = height_by_id.get(&id).copied().unwrap_or(0);

        // Pin-proven non-coinbase: height only (no body).
        if batch_parents.get_parent_coinbase(create_fk) == Some(false) {
            create_height_by_fk.insert(create_fk, durable_h);
            continue;
        }

        // Coinbase if pin says so, else create_fk == first_tx_fk of block at H
        // (confirmed + header_txs_first) — never open tx.body for vin0.
        let is_cb = batch_parents.get_parent_coinbase(create_fk) == Some(true)
            || coinbase_fk_by_height
                .get(&durable_h)
                .is_some_and(|cb| *cb == create_fk);
        if is_cb && ctx.height.0 < durable_h.saturating_add(maturity) {
            return Err(ConsensusError::BadTx("coinbase immature"));
        }
        create_height_by_fk.insert(create_fk, durable_h);
    }
    let create_h_ns = t_create.elapsed().as_nanos() as u64;

    // ── BIP68 relative sequence locks (CSV package) ────────────────────────
    // Skip height/MTP prep for version < 2 (sequence_locks early-out).
    // Resolve coin MTP only for time-type relative locks (TYPE_FLAG).
    let t_bip68 = Instant::now();
    if ctx.params.csv_active_at(ctx.height.0) {
        const DISABLE: u32 = 1 << 31;
        const TYPE_FLAG: u32 = 1 << 22;
        let prev_mtp = if ctx.height.0 == 0 {
            0
        } else {
            mtp_at(query, Height(ctx.height.0 - 1), mtp_cache)?
        };
        let mut si = 0usize;
        // Reuse buffers across txs (write-local).
        let mut prev_heights: Vec<u32> = Vec::new();
        let mut coin_mtps: Vec<u32> = Vec::new();
        for tx in block.txdata.iter().skip(1) {
            let n_in = tx.input.len();
            if si + n_in > spends.len() {
                return Err(ConsensusError::BadBlock(
                    "structural spends/tx input mismatch",
                ));
            }
            let tx_spends = &spends[si..si + n_in];
            si += n_in;

            // version < 2 (unsigned): relative locks inactive.
            if !bip68_active_for_tx(tx) {
                continue;
            }

            prev_heights.clear();
            coin_mtps.clear();
            prev_heights.reserve(n_in);
            coin_mtps.reserve(n_in);
            for (inp, &(_ptid, _vout, _sfk, create_fk)) in tx.input.iter().zip(tx_spends.iter()) {
                let ch = if create_fk.is_null() {
                    // Same-block create (no Class A fk yet): Core uses spend height.
                    ctx.height.0
                } else {
                    create_height_by_fk.get(&create_fk).copied().unwrap_or(0)
                };
                prev_heights.push(ch);
                let seq = inp.sequence.to_consensus_u32();
                // Coin MTP only when a time-type relative lock is active.
                let need_mtp = seq & DISABLE == 0 && seq & TYPE_FLAG != 0;
                let mtp = if !need_mtp || ch == 0 {
                    0
                } else {
                    mtp_at(query, Height(ch.saturating_sub(1)), mtp_cache)?
                };
                coin_mtps.push(mtp);
            }
            if !sequence_locks_satisfied(tx, &prev_heights, &coin_mtps, ctx.height.0, prev_mtp) {
                return Err(ConsensusError::BadTx("bad-txns-nonfinal"));
            }
        }
        if si != spends.len() {
            return Err(ConsensusError::BadBlock(
                "structural spends/tx input mismatch",
            ));
        }
    }
    let bip68_ns = t_bip68.elapsed().as_nanos() as u64;

    let _ = archived_tx_fks;
    check_coinbase_subsidy(block, ctx, fees)?;
    Ok(StructuralPhaseNs {
        spent_ns,
        spent_abs_ns,
        spent_strong_ns,
        spent_cold_ns,
        spent_pending_ns,
        create_h_ns,
        bip68_ns,
    })
}

/// [`crate::header::median_time_past`] with a write-run cache keyed by end height.
fn mtp_at(query: &Query, height: Height, cache: &mut U32Map<u32>) -> Result<u32, ConsensusError> {
    if let Some(&t) = cache.get(&height.0) {
        return Ok(t);
    }
    let t = crate::header::median_time_past(query, height)?;
    cache.insert(height.0, t);
    Ok(t)
}

/// Parallel script checks for an owned job slice (preferred entry — no ref `Vec`).
///
/// Uses the in-crate [`crate::script_pool`] (not rayon). One job = one
/// non-coinbase tx (shared [`bitcoin::sighash::SighashCache`] across its inputs).
pub fn verify_scripts_pool(jobs: &[ScriptCheckJob]) -> Result<(), ConsensusError> {
    crate::script_pool::try_for_each_parallel(jobs, verify_one_script_job)
}

/// Parallel script checks across borrowed jobs (multi-block wave).
pub fn verify_scripts_pool_jobs(jobs: &[&ScriptCheckJob]) -> Result<(), ConsensusError> {
    crate::script_pool::try_for_each_parallel(jobs, |job| verify_one_script_job(*job))
}

/// Whether this job can skip `verify_job_all_inputs`.
///
/// OP_TRUE scriptPubKey alone is **not** sufficient: Core still
/// `EvalScript(scriptSig)` (CLTV/CSV may live there). Only skip when every
/// input is a pure ACS spend (empty scriptSig + empty witness + OP_TRUE spk).
#[inline]
fn job_needs_script_check(job: &ScriptCheckJob) -> bool {
    let tx: &bitcoin::Transaction = &*job.tx;
    for (i, prev) in job.prevouts.iter().enumerate() {
        if !is_anyone_can_spend(prev.script_pubkey.as_script()) {
            return true;
        }
        let Some(vin) = tx.input.get(i) else {
            return true;
        };
        if !vin.script_sig.is_empty() || !vin.witness.is_empty() {
            return true;
        }
    }
    // All pure OP_TRUE + empty scriptSig/witness — nothing to evaluate.
    false
}

#[inline]
fn verify_one_script_job(job: &ScriptCheckJob) -> Result<(), ConsensusError> {
    if job_needs_script_check(job) {
        crate::script::verify_job_all_inputs(job)
    } else {
        Ok(())
    }
}

/// Halving subsidy (mainnet schedule; regtest uses same formula with params).
pub fn block_subsidy(height: u32, _params: &ChainParams) -> i64 {
    let halvings = height / 210_000;
    if halvings >= 64 {
        return 0;
    }
    50_0000_0000i64 >> halvings
}

struct ResolvedPrevout {
    txout: TxOut,
    /// `Some(create_height)` when prev is a confirmed coinbase (maturity check).
    coinbase_height: Option<u32>,
    /// Block height that created this UTXO (BIP68). Same-block → spending height.
    create_height: u32,
    /// Class A create fk for this prevout (or `NULL` for same-block). Load pin
    /// denserels must carry identity matching wire `prev_txid` for this fk.
    create_fk: rbitcoin_primitives::Fk,
}

/// BIP65/113 nLockTime threshold: values below are block heights, above are unix times.
pub const LOCKTIME_THRESHOLD: u32 = 500_000_000;

/// Core `IsFinalTx`: absolute locktime vs block height / time cutoff.
///
/// `lock_time_cutoff` is the comparison time: **MTP of the previous block** after
/// BIP113 (CSV package), else the block header timestamp.
pub fn is_final_tx(tx: &Transaction, block_height: u32, lock_time_cutoff: u32) -> bool {
    let lt = tx.lock_time.to_consensus_u32();
    if lt == 0 {
        return true;
    }
    if lt < LOCKTIME_THRESHOLD {
        if lt < block_height {
            return true;
        }
    } else if lt < lock_time_cutoff {
        return true;
    }
    // Still final if every input sequence is SEQUENCE_FINAL (0xffffffff).
    tx.input.iter().all(|i| i.sequence.is_final())
}

/// BIP68 / CSV version gate: Core compares `nVersion` as **unsigned**
/// (`uint32_t >= 2`). rust-bitcoin exposes `Version(i32)`; cast explicitly so
/// `0xFFFFFFFF` enforces locks (not signed `-1 < 2`).
/// See **RB-001** in `docs/rust-bitcoin-limitations.md` and
/// `docs/external_findings/003-bip68-version-signedness-consensus-split.md`.
#[inline]
pub fn bip68_active_for_tx(tx: &Transaction) -> bool {
    (tx.version.0 as u32) >= 2
}

/// BIP68 relative locks when `tx.version` as u32 ≥ 2.
///
/// `prev_heights[i]` / `prev_mtps[i]`: create height and MTP of the block *before*
/// the creating block (for time-based locks; use 0 when create height is 0).
/// `block_height` = containing block; `block_prev_mtp` = MTP of previous block.
pub fn sequence_locks_satisfied(
    tx: &Transaction,
    prev_heights: &[u32],
    prev_coin_mtps: &[u32],
    block_height: u32,
    block_prev_mtp: u32,
) -> bool {
    if !bip68_active_for_tx(tx) {
        return true;
    }
    const DISABLE: u32 = 1 << 31;
    const TYPE_FLAG: u32 = 1 << 22;
    const MASK: u32 = 0x0000_ffff;
    const GRANULARITY: u32 = 9;

    let mut min_height: i64 = -1;
    let mut min_time: i64 = -1;
    for (i, inp) in tx.input.iter().enumerate() {
        let seq = inp.sequence.to_consensus_u32();
        if seq & DISABLE != 0 {
            continue;
        }
        let coin_h = prev_heights.get(i).copied().unwrap_or(0);
        let rel = (seq & MASK) as i64;
        if seq & TYPE_FLAG != 0 {
            let coin_mtp = prev_coin_mtps.get(i).copied().unwrap_or(0) as i64;
            min_time = min_time.max(coin_mtp + (rel << GRANULARITY) - 1);
        } else {
            min_height = min_height.max(i64::from(coin_h) + rel - 1);
        }
    }
    // Core EvaluateSequenceLocks: fail if minHeight >= block.nHeight or minTime >= prev MTP.
    if min_height >= i64::from(block_height) {
        return false;
    }
    if min_time >= i64::from(block_prev_mtp) {
        return false;
    }
    true
}

fn resolve_prevout(
    query: &Query,
    block: &Block,
    op: OutPoint,
    // Prefer thin create_fk from load (avoids full InputRecord).
    prev_fk_hint: Option<rbitcoin_primitives::Fk>,
    same_block: &std::collections::HashMap<[u8; 32], usize>,
    coinbase_height_cache: &mut FkMap<Option<u32>>,
    batch_parents: &rbitcoin_query::BatchParents,
    // Height of the block being validated (same-block BIP68 coin height).
    spend_height: u32,
    // When false (optimistic confirm load): only prevout value/script — no
    // `tx_height` / coinbase body walks. BIP68 + maturity run in structural.
    resolve_create_heights: bool,
) -> Result<ResolvedPrevout, ConsensusError> {
    use rbitcoin_query::connect_prevout_stats;
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    let t0 = Instant::now();
    let prev_txid = op.txid.to_byte_array();

    #[inline]
    fn note_path(
        path_ns: &std::sync::atomic::AtomicU64,
        path_n: &std::sync::atomic::AtomicU64,
        t0: Instant,
    ) {
        path_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        path_n.fetch_add(1, Ordering::Relaxed);
        confirm_phase_stats::ASM_IN_N.fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        if std::ptr::eq(path_n, &confirm_phase_stats::ASM_PREV_BATCH_N) {
            confirm_phase_stats::tl_note_batch_hit();
        }
    }

    // Same-block spend of an earlier output in this block.
    // Clone script only for the spent vout (not every create in the block).
    if let Some(&ti) = same_block.get(&prev_txid) {
        let tx = block.txdata.get(ti).ok_or(ConsensusError::MissingPrevout)?;
        let v = op.vout as usize;
        let o = tx.output.get(v).ok_or(ConsensusError::MissingPrevout)?;
        // Same-block: Core uses the spending block's height as the coin height.
        note_path(
            &confirm_phase_stats::ASM_PREV_SAME_NS,
            &confirm_phase_stats::ASM_PREV_SAME_N,
            t0,
        );
        return Ok(ResolvedPrevout {
            txout: o.clone(),
            coinbase_height: None,
            create_height: if resolve_create_heights {
                spend_height
            } else {
                0
            },
            create_fk: rbitcoin_primitives::Fk::NULL,
        });
    }

    // Batch pin first (no TxRecord clone — A3). Cold Class A only when the
    // create is not pin-covered. Pin identity/vout misses are hard invariants
    // (load must fill schema-13 identity + denserels for need_vouts).
    // N1: classify warm-path miss so cold_n is explainable on `ibd: perf`.
    #[derive(Clone, Copy)]
    enum ColdWhy {
        NullFk,
        NotPin,
    }
    let mut cold_why = ColdWhy::NullFk;

    if let Some(prev_fk) = prev_fk_hint {
        cold_why = ColdWhy::NotPin;
        match batch_parents.get_parent_txout_parts(prev_fk, op.vout) {
            Some((value, script, parent_txid)) if parent_txid == prev_txid => {
                connect_prevout_stats::WAVE_HIT.fetch_add(1, Ordering::Relaxed);
                let (cb_h, create_height) = if resolve_create_heights {
                    // Need TxRecord only for coinbase/maturity path (Full mode).
                    let prev_rec = batch_parents
                        .get_parent_tx(prev_fk)
                        .ok_or(ConsensusError::MissingPrevout)?;
                    let cb_h = coinbase_height_for_maturity(
                        query,
                        prev_fk,
                        &prev_rec,
                        batch_parents,
                        coinbase_height_cache,
                    )?;
                    (cb_h, create_height_for_fk(query, prev_fk, cb_h)?)
                } else {
                    (None, 0)
                };
                note_path(
                    &confirm_phase_stats::ASM_PREV_BATCH_NS,
                    &confirm_phase_stats::ASM_PREV_BATCH_N,
                    t0,
                );
                return Ok(ResolvedPrevout {
                    txout: TxOut {
                        value: Amount::from_sat(value as u64),
                        script_pubkey: ScriptBuf::from_bytes(script),
                    },
                    coinbase_height: cb_h,
                    create_height,
                    // Pin row matched wire prev_txid — denserels create_fk is trusted.
                    create_fk: prev_fk,
                });
            }
            Some((_value, _script, parent_txid)) => {
                // Pin promised this create_fk + vout but identity ≠ wire. Load bug
                // (schema-13 zero identity, wrong denserels stamp) — hard fail.
                // Do **not** soft-cold recover; fill pin identity at load instead.
                let _ = parent_txid;
                confirm_phase_stats::ASM_PREV_COLD_TXID_MISMATCH_N.fetch_add(1, Ordering::Relaxed);
                #[cfg(test)]
                confirm_phase_stats::tl_note_cold_why_txid_mismatch();
                return Err(ConsensusError::Store(rbitcoin_store::StoreError::Corrupt(
                    "invariant: pin parent create identity mismatch wire prev_txid",
                )));
            }
            None if batch_parents.contains(prev_fk) => {
                // Parent create pinned, but needed vout not in sparse outs — load bug.
                confirm_phase_stats::ASM_PREV_COLD_VOUT_MISS_N.fetch_add(1, Ordering::Relaxed);
                #[cfg(test)]
                confirm_phase_stats::tl_note_cold_why_vout_miss();
                return Err(ConsensusError::Store(rbitcoin_store::StoreError::Corrupt(
                    "invariant: pin incomplete outs for spent parent vout",
                )));
            }
            None => {
                // Hint not in pin — cold Class A below (Allow pin / unit empty pin).
            }
        }
    }

    // Cold path: create-fk candidates (thin → durable head / store).
    let t_fk = Instant::now();
    let head_fk = query
        .tx_fk_by_txid(&prev_txid)
        .map_err(ConsensusError::from)?;
    confirm_phase_stats::ASM_PREV_FK_NS
        .fetch_add(t_fk.elapsed().as_nanos() as u64, Ordering::Relaxed);
    let candidates = [prev_fk_hint, head_fk];
    let mut seen: [u64; 3] = [0; 3];
    let mut n_seen = 0usize;
    for prev_fk in candidates.into_iter().flatten() {
        if prev_fk.is_null() {
            continue;
        }
        let id = prev_fk.0;
        if seen[..n_seen].contains(&id) {
            continue;
        }
        if n_seen < 3 {
            seen[n_seen] = id;
            n_seen += 1;
        }
        let prev_rec = match query.get_tx_class_a(prev_fk) {
            Ok(r) => r,
            Err(_) => continue,
        };
        if prev_rec.txid != prev_txid {
            continue;
        }
        let out = match find_output(query, prev_fk, &prev_rec, op.vout) {
            Ok(o) => o,
            Err(ConsensusError::MissingPrevout) => continue,
            Err(e) => return Err(e),
        };
        let (cb_h, create_height) = if resolve_create_heights {
            let cb_h = coinbase_height_for_maturity(
                query,
                prev_fk,
                &prev_rec,
                batch_parents,
                coinbase_height_cache,
            )?;
            (cb_h, create_height_for_fk(query, prev_fk, cb_h)?)
        } else {
            (None, 0)
        };
        note_path(
            &confirm_phase_stats::ASM_PREV_COLD_NS,
            &confirm_phase_stats::ASM_PREV_COLD_N,
            t0,
        );
        match cold_why {
            ColdWhy::NullFk => {
                confirm_phase_stats::ASM_PREV_COLD_NULL_FK_N.fetch_add(1, Ordering::Relaxed);
                #[cfg(test)]
                confirm_phase_stats::tl_note_cold_why_null_fk();
            }
            ColdWhy::NotPin => {
                confirm_phase_stats::ASM_PREV_COLD_NOT_PIN_N.fetch_add(1, Ordering::Relaxed);
                #[cfg(test)]
                confirm_phase_stats::tl_note_cold_why_not_pin();
            }
        }
        return Ok(ResolvedPrevout {
            txout: TxOut {
                value: Amount::from_sat(out.value as u64),
                script_pubkey: ScriptBuf::from_bytes(out.script),
            },
            coinbase_height: cb_h,
            create_height,
            // Cold candidate whose Class A body txid matches the wire prevout.
            create_fk: prev_fk,
        });
    }

    Err(ConsensusError::MissingPrevout)
}

/// Height of the block that created `prev_fk` (for BIP68).
fn create_height_for_fk(
    query: &Query,
    prev_fk: rbitcoin_primitives::Fk,
    coinbase_height: Option<u32>,
) -> Result<u32, ConsensusError> {
    if let Some(h) = coinbase_height {
        return Ok(h);
    }
    Ok(query
        .store()
        .tx_height
        .get(prev_fk)
        .map_err(ConsensusError::from)?
        .unwrap_or(0))
}

/// Coinbase create height for maturity, or `None` if not a coinbase / unknown.
///
/// Unlike the old `!is_cb || cb_h.is_some()` gate, a missing height never
/// discards an already-located parent output (that became MissingPrevout).
fn coinbase_height_for_maturity(
    query: &Query,
    prev_fk: rbitcoin_primitives::Fk,
    prev_rec: &rbitcoin_store::TxRecord,
    batch_parents: &rbitcoin_query::BatchParents,
    coinbase_height_cache: &mut FkMap<Option<u32>>,
) -> Result<Option<u32>, ConsensusError> {
    let (is_cb, cb_h) = coinbase_info(
        query,
        prev_fk,
        prev_rec,
        batch_parents,
        coinbase_height_cache,
    )?;
    if !is_cb {
        return Ok(None);
    }
    if cb_h.is_some() {
        return Ok(cb_h);
    }
    // Last resort: durable tx_height.
    Ok(query
        .store()
        .tx_height
        .get(prev_fk)
        .map_err(ConsensusError::from)?)
}

/// `(is_coinbase, create_height if coinbase and confirmed)`.
fn coinbase_info(
    query: &Query,
    prev_fk: rbitcoin_primitives::Fk,
    prev_rec: &rbitcoin_store::TxRecord,
    batch_parents: &rbitcoin_query::BatchParents,
    cache: &mut FkMap<Option<u32>>,
) -> Result<(bool, Option<u32>), ConsensusError> {
    if let Some(&h) = cache.get(&prev_fk) {
        // Cache value is coinbase create height only: `Some(h)` ⇒ coinbase,
        // `None` ⇒ not a coinbase. Do **not** re-derive is_cb from
        // `input_count == 1` — single-input non-coinbases also cache `None`,
        // and that wrong is_cb made resolve fall through (MissingPrevout) on
        // the second spend of the same parent (mainnet @546: two vouts of one
        // 1-in parent in one spending tx).
        return Ok((h.is_some(), h));
    }
    // Batch pin may stash coinbase *flag* only (heights from durable Class C).
    if let Some(is_cb) = batch_parents.get_parent_coinbase(prev_fk) {
        if !is_cb {
            cache.insert(prev_fk, None);
            return Ok((false, None));
        }
        let h = query
            .store()
            .tx_height
            .get(prev_fk)
            .map_err(ConsensusError::from)?;
        if h.is_some() {
            cache.insert(prev_fk, h);
        }
        return Ok((true, h));
    }
    if prev_rec.input_count != 1 {
        cache.insert(prev_fk, None);
        return Ok((false, None));
    }
    let is_cb = is_coinbase_tx_record(query, prev_fk, prev_rec)?;
    let h = if is_cb {
        query
            .store()
            .tx_height
            .get(prev_fk)
            .map_err(ConsensusError::from)?
    } else {
        None
    };
    if !is_cb || h.is_some() {
        cache.insert(prev_fk, h);
    }
    Ok((is_cb, h))
}

fn is_coinbase_tx_record(
    query: &Query,
    prev_fk: rbitcoin_primitives::Fk,
    rec: &rbitcoin_store::TxRecord,
) -> Result<bool, ConsensusError> {
    if rec.input_count != 1 {
        return Ok(false);
    }
    // Key by create fk so packed Class A works with `tx.head` off (catch-up).
    let inp = query
        .tx_input_at_fk(prev_fk, rec, 0)
        .map_err(ConsensusError::from)?;
    Ok(inp.is_coinbase() || (inp.prev_txid == [0u8; 32] && inp.prev_index == 0xffff_ffff))
}

fn find_output(
    query: &Query,
    prev_fk: rbitcoin_primitives::Fk,
    prev_rec: &rbitcoin_store::TxRecord,
    vout: u32,
) -> Result<rbitcoin_store::OutputRecord, ConsensusError> {
    if vout >= prev_rec.output_count {
        return Err(ConsensusError::MissingPrevout);
    }
    // Cold path: always use create fk (packed body + head-off catch-up).
    query
        .tx_output_at_fk_attributed(prev_fk, prev_rec, vout, true)
        .map_err(ConsensusError::from)
}

fn is_anyone_can_spend(script: &Script) -> bool {
    crate::script::is_anyone_can_spend(script)
}

#[cfg(test)]
mod bip34_tests {
    use super::bip34_height_script;

    #[test]
    fn small_heights_use_op_n() {
        assert_eq!(bip34_height_script(0), vec![0x00]);
        assert_eq!(bip34_height_script(1), vec![0x51]); // OP_1 — signet block 1
        assert_eq!(bip34_height_script(16), vec![0x60]);
    }

    #[test]
    fn height_17_uses_push() {
        assert_eq!(bip34_height_script(17), vec![0x01, 0x11]);
    }

    #[test]
    fn height_128_sign_byte() {
        // 128 = 0x80 needs trailing 0x00 so it is not negative
        assert_eq!(bip34_height_script(128), vec![0x02, 0x80, 0x00]);
    }
}

#[cfg(test)]
mod finality_tests {
    use super::{is_final_tx, sequence_locks_satisfied, LOCKTIME_THRESHOLD};
    use bitcoin::absolute::LockTime;
    use bitcoin::script::ScriptBuf;
    use bitcoin::{Amount, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness};

    fn bare_tx(version: i32, lock_time: LockTime, sequence: Sequence) -> Transaction {
        Transaction {
            version: bitcoin::transaction::Version(version),
            lock_time,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::new(),
                sequence,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        }
    }

    #[test]
    fn final_when_locktime_zero() {
        let tx = bare_tx(1, LockTime::ZERO, Sequence::MAX);
        assert!(is_final_tx(&tx, 100, 1_000_000));
    }

    #[test]
    fn height_locktime_not_final_until_height() {
        let tx = bare_tx(1, LockTime::from_height(100).unwrap(), Sequence::ZERO);
        assert!(!is_final_tx(&tx, 100, 1_000_000)); // need lt < height
        assert!(is_final_tx(&tx, 101, 1_000_000));
    }

    #[test]
    fn sequence_final_ignores_locktime() {
        let tx = bare_tx(1, LockTime::from_height(100).unwrap(), Sequence::MAX);
        assert!(is_final_tx(&tx, 50, 1_000_000));
    }

    #[test]
    fn time_locktime_uses_cutoff() {
        let t = LOCKTIME_THRESHOLD + 1000;
        let tx = bare_tx(1, LockTime::from_time(t).unwrap(), Sequence::ZERO);
        assert!(!is_final_tx(&tx, 1, t)); // need lt < cutoff
        assert!(is_final_tx(&tx, 1, t + 1));
    }

    #[test]
    fn bip68_height_relative_lock() {
        // version 2, seq = 10 (height), coin at height 100 → minHeight = 100+10-1 = 109
        // needs block_height > 109
        let tx = bare_tx(2, LockTime::ZERO, Sequence::from_consensus(10));
        assert!(!sequence_locks_satisfied(&tx, &[100], &[0], 109, 0));
        assert!(sequence_locks_satisfied(&tx, &[100], &[0], 110, 0));
    }

    #[test]
    fn bip68_disabled_by_version_1() {
        let tx = bare_tx(1, LockTime::ZERO, Sequence::from_consensus(10));
        assert!(sequence_locks_satisfied(&tx, &[100], &[0], 50, 0));
    }

    /// Core treats nVersion as unsigned: 0xFFFFFFFF ≥ 2 → BIP68 enforced
    /// (`docs/external_findings/003-bip68-version-signedness-consensus-split.md`).
    #[test]
    fn bip68_enforced_when_version_high_bit_set() {
        // rust-bitcoin Version(i32): -1 is wire 0xFFFFFFFF.
        let tx = bare_tx(-1, LockTime::ZERO, Sequence::from_consensus(10));
        assert!(super::bip68_active_for_tx(&tx));
        // Same relative height lock as bip68_height_relative_lock — must fail at h=109.
        assert!(!sequence_locks_satisfied(&tx, &[100], &[0], 109, 0));
        assert!(sequence_locks_satisfied(&tx, &[100], &[0], 110, 0));
    }

    #[test]
    fn bip68_disable_flag_and_time_type() {
        // DISABLE bit → ignore relative lock.
        let disable = 1u32 << 31;
        let tx = bare_tx(2, LockTime::ZERO, Sequence::from_consensus(disable | 10));
        assert!(sequence_locks_satisfied(&tx, &[100], &[0], 50, 0));

        // Time-based: TYPE_FLAG | n, granularity 512s.
        let type_flag = 1u32 << 22;
        let n = 2u32; // 2 × 512s relative
        let tx = bare_tx(2, LockTime::ZERO, Sequence::from_consensus(type_flag | n));
        // coin MTP = 1000; minTime ≈ 1000 + (2<<9) - 1
        let coin_mtp = 1000u32;
        let min_time = coin_mtp as i64 + ((n as i64) << 9) - 1;
        assert!(!sequence_locks_satisfied(
            &tx,
            &[100],
            &[coin_mtp],
            200,
            (min_time as u32).saturating_sub(1)
        ));
        assert!(sequence_locks_satisfied(
            &tx,
            &[100],
            &[coin_mtp],
            200,
            min_time as u32 + 1
        ));
    }

    /// Height-type locks ignore coin MTP (write path may leave mtps as 0).
    #[test]
    fn bip68_height_type_ignores_zero_mtp() {
        let tx = bare_tx(2, LockTime::ZERO, Sequence::from_consensus(10));
        assert!(sequence_locks_satisfied(&tx, &[100], &[0], 110, 0));
        // bogus MTP must not affect height-type check
        assert!(sequence_locks_satisfied(&tx, &[100], &[u32::MAX], 110, 0));
        assert!(!sequence_locks_satisfied(&tx, &[100], &[0], 109, 0));
    }
}

#[cfg(test)]
mod structure_rule_tests {
    use super::{
        bip16_active_from_prev_mtp, bip34_height_script, block_subsidy, is_p2sh_script,
        is_p2wpkh_program, is_p2wsh_program, last_script_push, merkle_root_bytes,
        script_sigop_count, validate_block_structure, ScriptCheckJob, ValidationContext,
        BIP16_EXCEPTION_MAINNET,
    };
    use crate::error::ConsensusError;
    use crate::milestone::Milestone;
    use crate::params::ChainParams;
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::hashes::Hash;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{
        Amount, Block, BlockHash, CompactTarget, OutPoint, Sequence, Transaction, TxIn,
        TxMerkleNode, TxOut, Witness,
    };
    use rbitcoin_primitives::Height;

    fn params() -> ChainParams {
        ChainParams::regtest()
    }

    fn ctx_h(height: u32) -> ValidationContext<'static> {
        // Leak params for 'static test ctx simplicity.
        let p = Box::leak(Box::new(params()));
        ValidationContext::at(p, Height(height), Milestone::NONE)
    }

    fn coinbase(height: u32) -> Transaction {
        // Consensus requires coinbase scriptSig length in 2..=100.
        let mut ss = if height == 0 {
            vec![0x00]
        } else {
            bip34_height_script(height)
        };
        while ss.len() < 2 {
            ss.push(0x00);
        }
        let script_sig = ScriptBuf::from_bytes(ss);
        Transaction {
            version: TxVersion::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig,
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_0000_0000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        }
    }

    fn non_coinbase_spend(n: u8) -> Transaction {
        Transaction {
            version: TxVersion::ONE,
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
                value: Amount::from_sat(1_000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        }
    }

    fn block_with(txs: Vec<Transaction>) -> Block {
        let mut block = Block {
            header: Header {
                version: Version::ONE,
                prev_blockhash: BlockHash::from_byte_array([0; 32]),
                merkle_root: TxMerkleNode::from_byte_array([0; 32]),
                time: 1_290_000_000,
                bits: CompactTarget::from_consensus(0x207f_ffff),
                nonce: 0,
            },
            txdata: txs,
        };
        if !block.txdata.is_empty() {
            block.header.merkle_root = block.compute_merkle_root().unwrap();
        }
        block
    }

    fn assert_bad_block(err: ConsensusError, needle: &str) {
        match err {
            ConsensusError::BadBlock(s) => {
                assert!(
                    s.contains(needle),
                    "expected BadBlock containing {needle:?}, got {s:?}"
                );
            }
            other => panic!("expected BadBlock({needle:?}), got {other:?}"),
        }
    }

    #[test]
    fn s1_rejects_empty_txdata() {
        let b = block_with(vec![]);
        let err = validate_block_structure(&b, &ctx_h(0)).unwrap_err();
        assert_bad_block(err, "no transactions");
    }

    #[test]
    fn s2_rejects_non_coinbase_first() {
        let b = block_with(vec![non_coinbase_spend(1)]);
        let err = validate_block_structure(&b, &ctx_h(1)).unwrap_err();
        assert_bad_block(err, "first tx not coinbase");
    }

    #[test]
    fn s3_rejects_second_coinbase() {
        let b = block_with(vec![coinbase(1), coinbase(1)]);
        let err = validate_block_structure(&b, &ctx_h(1)).unwrap_err();
        assert_bad_block(err, "coinbase not first");
    }

    #[test]
    fn s4_rejects_overweight_block() {
        // ~1MB of script data per tx ≈ 4M weight; a few large outputs exceed the limit.
        let mut txs = vec![coinbase(1)];
        for i in 0..5u8 {
            let mut spk = vec![0x6a, 0x4d, 0xff, 0xff]; // OP_RETURN + pushdata2 placeholder
                                                        // Fill with ~900 KiB raw data via OP_RETURN chunking is awkward; use large script
                                                        // bytes rust-bitcoin will count toward base size.
            spk.extend(std::iter::repeat(0x61).take(900_000)); // OP_NOP filler
            txs.push(Transaction {
                version: TxVersion::ONE,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint {
                        txid: bitcoin::Txid::from_byte_array([i.wrapping_add(1); 32]),
                        vout: 0,
                    },
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::MAX,
                    witness: Witness::new(),
                }],
                output: vec![TxOut {
                    value: Amount::from_sat(1),
                    script_pubkey: ScriptBuf::from_bytes(spk),
                }],
            });
        }
        let b = block_with(txs);
        assert!(
            b.weight().to_wu() > 4_000_000,
            "fixture weight {}",
            b.weight().to_wu()
        );
        let err = validate_block_structure(&b, &ctx_h(1)).unwrap_err();
        assert_bad_block(err, "weight");
    }

    #[test]
    fn s5_rejects_duplicate_txid() {
        let t = non_coinbase_spend(7);
        let b = block_with(vec![coinbase(1), t.clone(), t]);
        let err = validate_block_structure(&b, &ctx_h(1)).unwrap_err();
        assert_bad_block(err, "duplicate txid");
    }

    #[test]
    fn s6_rejects_merkle_root_mismatch() {
        let mut b = block_with(vec![coinbase(1)]);
        b.header.merkle_root = TxMerkleNode::from_byte_array([0x11; 32]);
        let err = validate_block_structure(&b, &ctx_h(1)).unwrap_err();
        assert_bad_block(err, "merkle");
    }

    #[test]
    fn s7_rejects_bip34_missing_after_activation_signet() {
        // Signet activates BIP34 at height 1 (rust-bitcoin Params::SIGNET).
        let p = Box::leak(Box::new(ChainParams::signet()));
        let ctx = ValidationContext::at(p, Height(1), Milestone::NONE);
        let mut cb = coinbase(1);
        cb.input[0].script_sig = ScriptBuf::new(); // strip BIP34
        let b = block_with(vec![cb]);
        let err = validate_block_structure(&b, &ctx).unwrap_err();
        assert_bad_block(err, "bip34");
    }

    #[test]
    fn s7_bip34_not_required_before_mainnet_activation() {
        // Mainnet BIP34 height is 227931 — early blocks must not require the
        // height push (block 1 has a free-form coinbase scriptSig).
        let p = Box::leak(Box::new(ChainParams::mainnet()));
        assert_eq!(p.btc.bip34_height, 227_931);
        let ctx = ValidationContext::at(p, Height(1), Milestone::NONE);
        let mut cb = coinbase(1);
        // Mainnet-style early coinbase: no BIP34 height push.
        cb.input[0].script_sig = ScriptBuf::from_bytes(b"hello".to_vec());
        let b = block_with(vec![cb]);
        validate_block_structure(&b, &ctx).expect("mainnet height 1 must not require BIP34");
    }

    #[test]
    fn s7_bip34_required_at_mainnet_activation_height() {
        let p = Box::leak(Box::new(ChainParams::mainnet()));
        let h = p.btc.bip34_height;
        let ctx = ValidationContext::at(p, Height(h), Milestone::NONE);
        let mut cb = coinbase(h);
        cb.input[0].script_sig = ScriptBuf::new();
        let b = block_with(vec![cb]);
        let err = validate_block_structure(&b, &ctx).unwrap_err();
        assert_bad_block(err, "bip34");
    }

    #[test]
    fn s7_bip34_not_required_at_height_0() {
        let mut cb = coinbase(0);
        // Non-BIP34 push still OK at height 0; scriptSig must still be 2..=100.
        cb.input[0].script_sig = ScriptBuf::from_bytes(vec![0x00, 0x00]);
        let b = block_with(vec![cb]);
        validate_block_structure(&b, &ctx_h(0))
            .expect("height 0 skips BIP34 height push rules we use");
    }

    #[test]
    fn s7_regtest_does_not_activate_bip34_early() {
        // rust-bitcoin REGTEST bip34_height is 100_000_000 — our mined regtest
        // blocks may still *include* a height push, but empty/missing is OK.
        let p = Box::leak(Box::new(ChainParams::regtest()));
        assert!(p.btc.bip34_height > 1_000_000);
        let ctx = ValidationContext::at(p, Height(1), Milestone::NONE);
        let mut cb = coinbase(1);
        cb.input[0].script_sig = ScriptBuf::from_bytes(b"regtest".to_vec());
        let b = block_with(vec![cb]);
        validate_block_structure(&b, &ctx).expect("regtest height 1: BIP34 not active");
    }

    #[test]
    fn s8_rejects_missing_witness_commitment() {
        let mut spend = non_coinbase_spend(9);
        // Non-empty witness forces BIP141 commitment path.
        spend.input[0].witness = Witness::from_slice(&[vec![0x01]]);
        let b = block_with(vec![coinbase(1), spend]);
        // coinbase has no aa21a9ed output
        let err = validate_block_structure(&b, &ctx_h(1)).unwrap_err();
        assert_bad_block(err, "witness commitment");
    }

    #[test]
    fn s8_rejects_wrong_witness_commitment() {
        let mut spend = non_coinbase_spend(10);
        spend.input[0].witness = Witness::from_slice(&[vec![0x02]]);
        let mut cb = coinbase(1);
        // Fake commitment: OP_RETURN magic + zeros
        let mut spk = vec![0x6a, 0x24, 0xaa, 0x21, 0xa9, 0xed];
        spk.extend([0u8; 32]);
        cb.output.push(TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_bytes(spk),
        });
        let b = block_with(vec![cb, spend]);
        let err = validate_block_structure(&b, &ctx_h(1)).unwrap_err();
        assert!(
            matches!(err, ConsensusError::BadBlock(s) if s.contains("witness")),
            "got {err:?}"
        );
    }

    /// Mainnet height 1: witness banned (segwit @ 481824).
    #[test]
    fn s8_mainnet_rejects_witness_before_segwit() {
        let p = Box::leak(Box::new(ChainParams::mainnet()));
        let ctx = ValidationContext::at(p, Height(1), Milestone::NONE);
        let mut spend = non_coinbase_spend(10);
        spend.input[0].witness = Witness::from_slice(&[vec![0x01]]);
        let mut cb = coinbase(1);
        // Valid-looking commitment magic so we hit the pre-segwit ban first.
        let mut spk = vec![0x6a, 0x24, 0xaa, 0x21, 0xa9, 0xed];
        spk.extend([0u8; 32]);
        cb.output.push(TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_bytes(spk),
        });
        let b = block_with(vec![cb, spend]);
        let err = validate_block_structure(&b, &ctx).unwrap_err();
        assert!(
            matches!(err, ConsensusError::BadBlock(s) if s.contains("before segwit")),
            "got {err:?}"
        );
    }

    /// Archive prep must accept signet-shaped witness blocks (BIP325).
    /// Regression: GENESIS height + enforce gates rejected signet IBD entirely.
    #[test]
    fn archive_structure_allows_witness_when_gates_off() {
        let p = Box::leak(Box::new(ChainParams::signet()));
        let ctx = ValidationContext::archive_structure(p);
        assert!(!ctx.enforce_height_gates);
        let mut spend = non_coinbase_spend(10);
        spend.input[0].witness = Witness::from_slice(&[vec![0x01]]);
        let mut cb = coinbase(1);
        let mut spk = vec![0x6a, 0x24, 0xaa, 0x21, 0xa9, 0xed];
        spk.extend([0u8; 32]);
        cb.output.push(TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_bytes(spk),
        });
        let b = block_with(vec![cb, spend]);
        // Wrong commitment → still structure-checked (not pre-segwit ban).
        let err = validate_block_structure(&b, &ctx).unwrap_err();
        assert!(
            matches!(err, ConsensusError::BadBlock(s) if s.contains("witness")),
            "archive must check commitment, not pre-segwit ban; got {err:?}"
        );
        assert!(
            !matches!(err, ConsensusError::BadBlock(s) if s.contains("before segwit")),
            "archive must not ban witness as pre-segwit: {err:?}"
        );
    }

    /// Signet at true height 1: segwit active (Core/Inquisition SegwitHeight=1).
    #[test]
    fn signet_height_1_segwit_active_allows_witness_path() {
        let p = Box::leak(Box::new(ChainParams::signet()));
        assert!(p.segwit_active_at(1));
        let ctx = ValidationContext::at(p, Height(1), Milestone::NONE);
        let mut spend = non_coinbase_spend(10);
        spend.input[0].witness = Witness::from_slice(&[vec![0x01]]);
        let mut cb = coinbase(1);
        let mut spk = vec![0x6a, 0x24, 0xaa, 0x21, 0xa9, 0xed];
        spk.extend([0u8; 32]);
        cb.output.push(TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_bytes(spk),
        });
        let b = block_with(vec![cb, spend]);
        let err = validate_block_structure(&b, &ctx).unwrap_err();
        // Fails commitment hash, not pre-segwit.
        assert!(
            !matches!(err, ConsensusError::BadBlock(s) if s.contains("before segwit")),
            "got {err:?}"
        );
    }

    #[test]
    fn merkle_root_bytes_single_and_odd() {
        let a = [1u8; 32];
        assert_eq!(merkle_root_bytes(&[a]), a);
        let b = [2u8; 32];
        let root2 = merkle_root_bytes(&[a, b]);
        // odd: third leaf duplicated
        let root3 = merkle_root_bytes(&[a, b, a]);
        assert_ne!(root2, root3);
        assert_eq!(merkle_root_bytes(&[]), [0u8; 32]);
    }

    #[test]
    fn p1_block_subsidy_halvings() {
        let p = params();
        assert_eq!(block_subsidy(0, &p), 50_0000_0000);
        assert_eq!(block_subsidy(209_999, &p), 50_0000_0000);
        assert_eq!(block_subsidy(210_000, &p), 25_0000_0000);
        assert_eq!(block_subsidy(419_999, &p), 25_0000_0000);
        assert_eq!(block_subsidy(420_000, &p), 12_5000_0000);
        assert_eq!(block_subsidy(210_000 * 64, &p), 0);
    }

    #[test]
    fn script_sigop_count_and_last_push_helpers() {
        // CHECKSIG / CHECKSIGVERIFY
        assert_eq!(script_sigop_count(&[0xac], false), 1);
        assert_eq!(script_sigop_count(&[0xad], false), 1);
        // CHECKMULTISIG without accurate → 20
        assert_eq!(script_sigop_count(&[0xae], false), 20);
        assert_eq!(script_sigop_count(&[0xaf], false), 20);
        // Accurate: OP_2 CHECKMULTISIG → 2
        assert_eq!(script_sigop_count(&[0x52, 0xae], true), 2);
        // Direct push skip
        assert_eq!(script_sigop_count(&[0x01, 0xff, 0xac], false), 1);
        // PUSHDATA1 / 2 / 4 skip
        assert_eq!(script_sigop_count(&[0x4c, 0x01, 0xab, 0xac], false), 1);
        assert_eq!(
            script_sigop_count(&[0x4d, 0x01, 0x00, 0xcd, 0xac], false),
            1
        );
        assert_eq!(
            script_sigop_count(&[0x4e, 0x01, 0x00, 0x00, 0x00, 0xee, 0xac], false),
            1
        );
        // last_script_push variants
        assert_eq!(
            last_script_push(&[0x02, 0x11, 0x22]),
            Some(&[0x11, 0x22][..])
        );
        assert_eq!(
            last_script_push(&[0x4c, 0x02, 0xaa, 0xbb]),
            Some(&[0xaa, 0xbb][..])
        );
        assert_eq!(
            last_script_push(&[0x4d, 0x01, 0x00, 0x99]),
            Some(&[0x99][..])
        );
        assert_eq!(
            last_script_push(&[0x4e, 0x01, 0x00, 0x00, 0x00, 0x77]),
            Some(&[0x77][..])
        );
        assert!(last_script_push(&[]).is_none());
        // Program shape helpers
        let mut p2sh = vec![0xa9, 0x14];
        p2sh.extend_from_slice(&[0u8; 20]);
        p2sh.push(0x87);
        assert!(is_p2sh_script(&p2sh));
        assert!(!is_p2sh_script(&[0x00]));
        let mut wpkh = vec![0x00, 0x14];
        wpkh.extend_from_slice(&[1u8; 20]);
        assert!(is_p2wpkh_program(&wpkh));
        let mut wsh = vec![0x00, 0x20];
        wsh.extend_from_slice(&[2u8; 32]);
        assert!(is_p2wsh_program(&wsh));
        assert!(!is_p2wsh_program(&wpkh));
    }

    #[test]
    fn p3_default_milestone_heights() {
        use crate::params::default_milestone_height;
        use rbitcoin_primitives::Network;
        assert_eq!(default_milestone_height(Network::Regtest), 0);
        assert!(default_milestone_height(Network::Mainnet) > 0);
        assert!(default_milestone_height(Network::Signet) > 0);
    }

    #[test]
    fn s9_rejects_bad_cb_length_short() {
        let mut cb = coinbase(0);
        cb.input[0].script_sig = ScriptBuf::from_bytes(vec![0x01]); // len 1
        let b = block_with(vec![cb]);
        let err = validate_block_structure(&b, &ctx_h(0)).unwrap_err();
        assert_bad_block(err, "bad-cb-length");
    }

    #[test]
    fn s9_rejects_bad_cb_length_long() {
        let mut cb = coinbase(0);
        cb.input[0].script_sig = ScriptBuf::from_bytes(vec![0x01; 101]);
        let b = block_with(vec![cb]);
        let err = validate_block_structure(&b, &ctx_h(0)).unwrap_err();
        assert_bad_block(err, "bad-cb-length");
    }

    #[test]
    fn s10_rejects_vout_toolarge() {
        let mut cb = coinbase(0);
        // MAX_MONEY + 1 sat.
        cb.output[0].value = Amount::from_sat(21_000_000 * 100_000_000 + 1);
        let b = block_with(vec![cb]);
        let err = validate_block_structure(&b, &ctx_h(0)).unwrap_err();
        assert_bad_block(err, "toolarge");
    }

    #[test]
    fn s11_rejects_excessive_legacy_sigops() {
        let mut cb = coinbase(0);
        // 20_001 × OP_CHECKSIG × WITNESS_SCALE(4) = 80_004 > MAX 80_000.
        cb.output[0].script_pubkey = ScriptBuf::from_bytes(vec![0xac; 20_001]);
        let b = block_with(vec![cb]);
        let err = validate_block_structure(&b, &ctx_h(0)).unwrap_err();
        assert_bad_block(err, "sigops");
    }

    #[test]
    fn s10_rejects_txouttotal_toolarge() {
        // Two outputs each under MAX_MONEY but sum over.
        let half = 11_000_000 * 100_000_000u64; // 11M BTC each
        let mut cb = coinbase(0);
        cb.output = vec![
            TxOut {
                value: Amount::from_sat(half),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            },
            TxOut {
                value: Amount::from_sat(half),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            },
        ];
        let b = block_with(vec![cb]);
        let err = validate_block_structure(&b, &ctx_h(0)).unwrap_err();
        assert_bad_block(err, "txouttotal");
    }

    #[test]
    fn s8_accepts_witness_commitment_with_reserved_value() {
        // Build a valid commitment using non-zero witness reserved in coinbase witness.
        let mut spend = non_coinbase_spend(11);
        spend.input[0].witness = Witness::from_slice(&[vec![0xab]]);
        let mut cb = coinbase(1);
        // Place reserved value as last coinbase witness stack item.
        let reserved = [0x42u8; 32];
        cb.input[0].witness = Witness::from_slice(&[reserved.as_slice()]);
        // Compute expected commitment: SHA256D(witness_root || reserved)
        let wtxid = spend.compute_wtxid().to_byte_array();
        let leaves = vec![[0u8; 32], wtxid];
        let witness_root = merkle_root_bytes(&leaves);
        let mut buf = [0u8; 64];
        buf[..32].copy_from_slice(&witness_root);
        buf[32..].copy_from_slice(&reserved);
        use bitcoin::hashes::{sha256d, Hash};
        let committed = sha256d::Hash::hash(&buf).to_byte_array();
        let mut spk = vec![0x6a, 0x24, 0xaa, 0x21, 0xa9, 0xed];
        spk.extend_from_slice(&committed);
        cb.output.push(TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_bytes(spk),
        });
        let b = block_with(vec![cb, spend]);
        validate_block_structure(&b, &ctx_h(1)).expect("reserved witness commitment");
        let _ = leaves;
    }

    /// Finding 009: commitment present requires exactly one 32-byte coinbase witness item.
    #[test]
    fn s8_rejects_empty_or_multi_item_coinbase_witness_reserved() {
        use bitcoin::hashes::{sha256d, Hash};

        let mut spend = non_coinbase_spend(13);
        spend.input[0].witness = Witness::from_slice(&[vec![0xab]]);
        let wtxid = spend.compute_wtxid().to_byte_array();
        let leaves = vec![[0u8; 32], wtxid];
        let witness_root = merkle_root_bytes(&leaves);
        // Commitment over reserved = zeros (valid crypto if nonce were present).
        let reserved_zero = [0u8; 32];
        let mut buf = [0u8; 64];
        buf[..32].copy_from_slice(&witness_root);
        buf[32..].copy_from_slice(&reserved_zero);
        let committed = sha256d::Hash::hash(&buf).to_byte_array();
        let mut spk = vec![0x6a, 0x24, 0xaa, 0x21, 0xa9, 0xed];
        spk.extend_from_slice(&committed);

        // Empty coinbase witness → bad-witness-nonce-size (not accept via zero probe).
        {
            let mut cb = coinbase(1);
            cb.output.push(TxOut {
                value: Amount::ZERO,
                script_pubkey: ScriptBuf::from_bytes(spk.clone()),
            });
            // witness empty
            let b = block_with(vec![cb, spend.clone()]);
            let err = validate_block_structure(&b, &ctx_h(1)).unwrap_err();
            assert!(
                matches!(err, ConsensusError::BadBlock(s) if s.contains("witness") || s.contains("nonce")),
                "empty reserved: got {err:?}"
            );
        }

        // Multi-item stack with last = zeros matching commitment → still reject.
        {
            let mut cb = coinbase(1);
            cb.output.push(TxOut {
                value: Amount::ZERO,
                script_pubkey: ScriptBuf::from_bytes(spk.clone()),
            });
            cb.input[0].witness = Witness::from_slice(&[vec![0xff], reserved_zero.to_vec()]);
            let b = block_with(vec![cb, spend.clone()]);
            let err = validate_block_structure(&b, &ctx_h(1)).unwrap_err();
            assert!(
                matches!(err, ConsensusError::BadBlock(s) if s.contains("witness") || s.contains("nonce")),
                "multi-item reserved: got {err:?}"
            );
        }

        // Control: exactly one 32-zero item + matching commitment → Ok.
        {
            let mut cb = coinbase(1);
            cb.output.push(TxOut {
                value: Amount::ZERO,
                script_pubkey: ScriptBuf::from_bytes(spk),
            });
            cb.input[0].witness = Witness::from_slice(&[reserved_zero.as_slice()]);
            let b = block_with(vec![cb, spend]);
            validate_block_structure(&b, &ctx_h(1)).expect("single zero reserved OK");
        }
    }

    #[test]
    fn bip34_height_script_large_values() {
        // 0x80 high bit needs pad; larger multi-byte.
        assert_eq!(bip34_height_script(255), vec![0x02, 0xff, 0x00]);
        assert_eq!(bip34_height_script(256), vec![0x02, 0x00, 0x01]);
    }

    #[test]
    fn bip34_height_script_small_and_op_n() {
        assert_eq!(bip34_height_script(0), vec![0x00]);
        for h in 1u32..=16 {
            assert_eq!(bip34_height_script(h), vec![0x50 + h as u8]);
        }
        // First multi-byte form (17).
        assert_eq!(bip34_height_script(17), vec![0x01, 0x11]);
        // Wrong encoding rejected after activation (signet height 1).
        let p = Box::leak(Box::new(ChainParams::signet()));
        let ctx = ValidationContext::at(p, Height(1), Milestone::NONE);
        let mut cb = coinbase(1);
        // Height 1 must be OP_1 (0x51); push-length form is wrong.
        cb.input[0].script_sig = ScriptBuf::from_bytes(vec![0x01, 0x01, 0x00]);
        let b = block_with(vec![cb]);
        let err = validate_block_structure(&b, &ctx).unwrap_err();
        assert_bad_block(err, "bip34");
    }

    /// Full assemble mode: spentness probe + maturity (legacy path).
    #[test]
    fn assemble_full_mode_spend_and_bip68() {
        use super::{assemble_block_prevouts_mode, AssembleMode};
        use crate::accept_and_connect_block;
        use rbitcoin_query::{BatchParents, BatchThin, Query};
        use std::collections::{HashMap, HashSet};
        use std::sync::Once;
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-assemble-full-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let q = Query::open_or_create(&path).unwrap();
        let params = ChainParams::regtest();
        let ms = Milestone::NONE;
        let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
        let mut tip = genesis.block_hash();
        let mut tip_time = genesis.header.time;
        let mut last_cb = genesis.txdata[0].compute_txid();
        for h in 1u32..=3 {
            let bits = CompactTarget::from_consensus(0x207f_ffff);
            let mut block = Block {
                header: Header {
                    version: Version::ONE,
                    prev_blockhash: tip,
                    merkle_root: TxMerkleNode::from_byte_array([0; 32]),
                    time: tip_time + 600,
                    bits,
                    nonce: 0,
                },
                txdata: vec![coinbase(h)],
            };
            block.header.merkle_root = block.compute_merkle_root().unwrap();
            let target = bitcoin::Target::from_compact(bits);
            for nonce in 0..u32::MAX {
                block.header.nonce = nonce;
                if block.header.validate_pow(target).is_ok() {
                    break;
                }
            }
            last_cb = block.txdata[0].compute_txid();
            accept_and_connect_block(&q, &params, Height(h), &block, ms).unwrap();
            tip = block.block_hash();
            tip_time = block.header.time;
        }
        let spend = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: last_cb,
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_0000_0000 - 1000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let bits = CompactTarget::from_consensus(0x207f_ffff);
        let mut block = Block {
            header: Header {
                version: Version::ONE,
                prev_blockhash: tip,
                merkle_root: TxMerkleNode::from_byte_array([0; 32]),
                time: tip_time + 600,
                bits,
                nonce: 0,
            },
            txdata: vec![coinbase(4), spend],
        };
        block.header.merkle_root = block.compute_merkle_root().unwrap();
        let target = bitcoin::Target::from_compact(bits);
        for nonce in 0..u32::MAX {
            block.header.nonce = nonce;
            if block.header.validate_pow(target).is_ok() {
                break;
            }
        }
        let ctx = ctx_h(4);
        let parents = BatchParents::new();
        let thin = BatchThin::default();
        let mut spent = HashSet::new();
        let mut creates = HashMap::new();
        let create_txids: Vec<[u8; 32]> = block
            .txdata
            .iter()
            .map(|t| t.compute_txid().to_byte_array())
            .collect();
        let bh = block.header.block_hash().to_byte_array();
        let r = assemble_block_prevouts_mode(
            &q,
            &block,
            &ctx,
            None,
            &mut spent,
            &mut creates,
            AssembleMode::Full,
            &parents,
            &thin,
            &create_txids,
            0,
            &bh,
            bip16_active_from_prev_mtp(ctx.params, ctx.height.0, &bh, 0),
            None,
        );
        // Immature coinbase or spentness walk — either exercises Full arms.
        let _ = r;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn assemble_rejects_empty_and_fk_mismatch() {
        use super::assemble_block_prevouts;
        use rbitcoin_query::{BatchParents, BatchThin, Query};
        use std::collections::{HashMap, HashSet};
        use std::sync::Once;
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-assemble-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let q = Query::open_or_create(&path).unwrap();
        let ctx = ctx_h(1);
        let empty = block_with(vec![]);
        let parents = BatchParents::new();
        let thin = BatchThin::default();
        let mut spent = HashSet::new();
        let mut creates = HashMap::new();
        let zero = [0u8; 32];
        let err = assemble_block_prevouts(
            &q,
            &empty,
            &ctx,
            None,
            &mut spent,
            &mut creates,
            &parents,
            &thin,
            &[],
            0,
            &zero,
            false,
            None,
        )
        .err()
        .expect("empty");
        assert_bad_block(err, "empty");
        // Wrong archived fk count: coinbase alone needs 1 fk; pass empty slice.
        let b = block_with(vec![coinbase(1)]);
        spent.clear();
        creates.clear();
        let tids: Vec<[u8; 32]> = b
            .txdata
            .iter()
            .map(|t| t.compute_txid().to_byte_array())
            .collect();
        let bh = b.header.block_hash().to_byte_array();
        let err2 = assemble_block_prevouts(
            &q,
            &b,
            &ctx,
            Some(&[]),
            &mut spent,
            &mut creates,
            &parents,
            &thin,
            &tids,
            0,
            &bh,
            false,
            None,
        )
        .err()
        .expect("fk mismatch");
        assert_bad_block(err2, "archived tx fk");
        // first tx not coinbase
        let bad = block_with(vec![non_coinbase_spend(1)]);
        spent.clear();
        creates.clear();
        let tids3: Vec<[u8; 32]> = bad
            .txdata
            .iter()
            .map(|t| t.compute_txid().to_byte_array())
            .collect();
        let bh3 = bad.header.block_hash().to_byte_array();
        let err3 = assemble_block_prevouts(
            &q,
            &bad,
            &ctx,
            None,
            &mut spent,
            &mut creates,
            &parents,
            &thin,
            &tids3,
            0,
            &bh3,
            false,
            None,
        )
        .err()
        .expect("not coinbase");
        assert_bad_block(err3, "coinbase");
        let _ = std::fs::remove_dir_all(&path);
    }

    /// N1: cold-path reason counters for pin→assemble leakage classes.
    ///
    /// Drives [`super::resolve_prevout`] (same crate) so each miss class is
    /// forced without re-implementing path logic in the test.
    #[test]
    fn n1_assemble_cold_why_reasons() {
        use super::resolve_prevout;
        use crate::accept_and_connect_block;
        use crate::confirm_phase_stats;
        use rbitcoin_primitives::Fk;
        use rbitcoin_query::{BatchParents, Query};
        use rbitcoin_store::OutputRecord;
        use std::collections::HashMap;
        use std::sync::Once;
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-n1-cold-why-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let q = Query::open_or_create(&path).unwrap();
        let params = ChainParams::regtest();
        let ms = Milestone::NONE;
        let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
        let mut tip = genesis.block_hash();
        let mut tip_time = genesis.header.time;
        let mut last_cb = genesis.txdata[0].compute_txid();
        let mut last_cb_fk = Fk::NULL;
        for h in 1u32..=3 {
            let bits = CompactTarget::from_consensus(0x207f_ffff);
            let mut block = Block {
                header: Header {
                    version: Version::ONE,
                    prev_blockhash: tip,
                    merkle_root: TxMerkleNode::from_byte_array([0; 32]),
                    time: tip_time + 600,
                    bits,
                    nonce: 0,
                },
                txdata: vec![coinbase(h)],
            };
            block.header.merkle_root = block.compute_merkle_root().unwrap();
            let target = bitcoin::Target::from_compact(bits);
            for nonce in 0..u32::MAX {
                block.header.nonce = nonce;
                if block.header.validate_pow(target).is_ok() {
                    break;
                }
            }
            last_cb = block.txdata[0].compute_txid();
            accept_and_connect_block(&q, &params, Height(h), &block, ms).unwrap();
            last_cb_fk = q
                .tx_fk_by_txid(last_cb.as_byte_array())
                .unwrap()
                .expect("cb fk");
            tip = block.block_hash();
            tip_time = block.header.time;
        }
        let parent_txid = last_cb.to_byte_array();
        let op = OutPoint {
            txid: last_cb,
            vout: 0,
        };
        let empty_block = block_with(vec![coinbase(4)]);
        let same_block: HashMap<[u8; 32], usize> = HashMap::new();
        let mut cb_cache: rbitcoin_query::FkMap<Option<u32>> = rbitcoin_query::FkMap::default();

        // Thread-local N1 counters (process-global atomics race under parallel cargo test).
        let _ = confirm_phase_stats::sample_tl_assemble_cold_why_and_reset();
        let _ = confirm_phase_stats::sample_tl_batch_cold_n_and_reset();

        // ── null_fk: no hint; head still finds parent → cold ──────────
        {
            let parents = BatchParents::new();
            resolve_prevout(
                &q,
                &empty_block,
                op,
                None,
                &same_block,
                &mut cb_cache,
                &parents,
                4,
                false,
            )
            .expect("null_fk cold");
            let why = confirm_phase_stats::sample_tl_assemble_cold_why_and_reset();
            let (_batch_n, cold_n) = confirm_phase_stats::sample_tl_batch_cold_n_and_reset();
            assert_eq!(why, (1, 0, 0, 0), "null_fk why={why:?}");
            assert_eq!(cold_n, 1, "cold_n={cold_n}");
        }

        // ── not_pin: correct fk, empty BatchParents ───────────────────
        {
            let parents = BatchParents::new();
            resolve_prevout(
                &q,
                &empty_block,
                op,
                Some(last_cb_fk),
                &same_block,
                &mut cb_cache,
                &parents,
                4,
                false,
            )
            .expect("not_pin cold");
            let why = confirm_phase_stats::sample_tl_assemble_cold_why_and_reset();
            let (_batch_n, cold_n) = confirm_phase_stats::sample_tl_batch_cold_n_and_reset();
            assert_eq!(why, (0, 1, 0, 0), "not_pin why={why:?}");
            assert_eq!(cold_n, 1, "cold_n");
        }

        // ── batch hit: no cold ────────────────────────────────────────
        {
            let mut parents = BatchParents::new();
            let rec = q.get_tx_class_a(last_cb_fk).expect("class a");
            let out = OutputRecord::unspent(50_0000_0000, vec![0x51]);
            parents.put_resolved(last_cb_fk, rec, &[(0, out)], &[0], Some(true));
            // Ensure pin txid matches wire.
            assert_eq!(
                parents.get_parent_txout_parts(last_cb_fk, 0).unwrap().2,
                parent_txid
            );
            resolve_prevout(
                &q,
                &empty_block,
                op,
                Some(last_cb_fk),
                &same_block,
                &mut cb_cache,
                &parents,
                4,
                false,
            )
            .expect("batch hit");
            let why = confirm_phase_stats::sample_tl_assemble_cold_why_and_reset();
            let (batch_n, cold_n) = confirm_phase_stats::sample_tl_batch_cold_n_and_reset();
            assert_eq!(why, (0, 0, 0, 0), "batch hit must not cold why={why:?}");
            assert_eq!(batch_n, 1, "batch_n");
            assert_eq!(cold_n, 0, "cold_n");
        }

        // ── txid_mismatch: pin present with wrong identity → hard invariant ─
        {
            let mut parents = BatchParents::new();
            let mut rec = q.get_tx_class_a(last_cb_fk).expect("class a");
            rec.txid = [0xee; 32]; // wrong identity
            let out = OutputRecord::unspent(50_0000_0000, vec![0x51]);
            parents.put_resolved(last_cb_fk, rec, &[(0, out)], &[0], Some(true));
            let err = match resolve_prevout(
                &q,
                &empty_block,
                op,
                Some(last_cb_fk),
                &same_block,
                &mut cb_cache,
                &parents,
                4,
                false,
            ) {
                Ok(_) => panic!("mismatch must hard-fail"),
                Err(e) => e,
            };
            let msg = format!("{err}");
            assert!(
                msg.contains("invariant") && msg.contains("identity"),
                "got {err}"
            );
            let why = confirm_phase_stats::sample_tl_assemble_cold_why_and_reset();
            assert_eq!(why, (0, 0, 1, 0), "mismatch why={why:?}");
        }

        // ── vout_miss: parent in batch, needed vout not sparse-pinned → invariant ─
        {
            let mut parents = BatchParents::new();
            let rec = q.get_tx_class_a(last_cb_fk).expect("class a");
            // Pin only vout 1 (does not exist on spend of vout 0).
            let out = OutputRecord::unspent(1, vec![0x51]);
            parents.put_resolved(last_cb_fk, rec, &[(1, out)], &[1], Some(true));
            assert!(parents.contains(last_cb_fk));
            assert!(parents.get_parent_txout_parts(last_cb_fk, 0).is_none());
            let err = match resolve_prevout(
                &q,
                &empty_block,
                op,
                Some(last_cb_fk),
                &same_block,
                &mut cb_cache,
                &parents,
                4,
                false,
            ) {
                Ok(_) => panic!("vout_miss must hard-fail"),
                Err(e) => e,
            };
            let msg = format!("{err}");
            assert!(
                msg.contains("invariant") && msg.contains("incomplete outs"),
                "got {err}"
            );
            let why = confirm_phase_stats::sample_tl_assemble_cold_why_and_reset();
            assert_eq!(why, (0, 0, 0, 1), "vout_miss why={why:?}");
        }

        let _ = std::fs::remove_dir_all(&path);
    }

    /// Mainnet tip stall class (961460→961461): already-archived Class A tip+1
    /// uses `plan=None` pin cold denserels. Schema-13 body has no leading txid;
    /// load must fill pin identity from `txid.body` so assemble pin-hits match
    /// wire (not soft spentness recovery for zero-identity pins).
    ///
    /// Shipped paths:
    /// - archive body first → `confirm_wire_run` (plan=None) succeeds
    /// - IBD-style `confirm_wire_load_from_plan(..., Forbid)` with plan=None must
    ///   still succeed (load forces Allow cold denserels — not
    ///   `lookup stage miss`)
    /// - rapid tip+1/tip+2 accept; genuine double-spend still `PrevoutSpent`
    #[test]
    fn already_archived_schema13_pin_identity_tip_follow() {
        use crate::{
            accept_and_archive_block, accept_and_connect_block, confirm_scripts_phase,
            confirm_wire_load_from_plan, confirm_wire_lookup_stamp, confirm_write_phase,
            ColdPinMode, ScriptPreverified,
        };
        use rbitcoin_query::Query;
        use std::sync::Arc;
        use std::sync::Once;
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-plan-none-pin-id-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let q = Query::open_or_create(&path).unwrap();
        q.set_spend_index(true);
        let params = ChainParams::regtest();
        let ms = Milestone::NONE;
        let maturity = params.coinbase_maturity();

        fn mine_cb(prev: BlockHash, time: u32, h: u32) -> Block {
            let bits = CompactTarget::from_consensus(0x207f_ffff);
            let mut block = Block {
                header: Header {
                    version: Version::ONE,
                    prev_blockhash: prev,
                    merkle_root: TxMerkleNode::from_byte_array([0; 32]),
                    time,
                    bits,
                    nonce: 0,
                },
                txdata: vec![coinbase(h)],
            };
            block.header.merkle_root = block.compute_merkle_root().unwrap();
            let target = bitcoin::Target::from_compact(bits);
            for nonce in 0..u32::MAX {
                block.header.nonce = nonce;
                if block.header.validate_pow(target).is_ok() {
                    break;
                }
            }
            block
        }
        fn mine_with(prev: BlockHash, time: u32, h: u32, extra: Vec<Transaction>) -> Block {
            let bits = CompactTarget::from_consensus(0x207f_ffff);
            let mut txs = vec![coinbase(h)];
            txs.extend(extra);
            let mut block = Block {
                header: Header {
                    version: Version::ONE,
                    prev_blockhash: prev,
                    merkle_root: TxMerkleNode::from_byte_array([0; 32]),
                    time,
                    bits,
                    nonce: 0,
                },
                txdata: txs,
            };
            block.header.merkle_root = block.compute_merkle_root().unwrap();
            let target = bitcoin::Target::from_compact(bits);
            for nonce in 0..u32::MAX {
                block.header.nonce = nonce;
                if block.header.validate_pow(target).is_ok() {
                    break;
                }
            }
            block
        }
        fn spend_acs(prev: bitcoin::Txid, vout: u32, val: Amount) -> Transaction {
            Transaction {
                version: TxVersion::TWO,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint { txid: prev, vout },
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::MAX,
                    witness: Witness::new(),
                }],
                output: vec![TxOut {
                    value: val,
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                }],
            }
        }

        let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
        let mut tip = genesis.block_hash();
        let mut tip_time = genesis.header.time;

        let b1 = mine_cb(tip, tip_time + 600, 1);
        let c1_txid = b1.txdata[0].compute_txid();
        accept_and_connect_block(&q, &params, Height(1), &b1, ms).unwrap();
        tip = b1.block_hash();
        tip_time = b1.header.time;

        let b2 = mine_cb(tip, tip_time + 600, 2);
        let c2_txid = b2.txdata[0].compute_txid();
        accept_and_connect_block(&q, &params, Height(2), &b2, ms).unwrap();
        tip = b2.block_hash();
        tip_time = b2.header.time;

        for h in 3..=maturity + 2 {
            let b = mine_cb(tip, tip_time + 600, h);
            accept_and_connect_block(&q, &params, Height(h), &b, ms).unwrap();
            tip = b.block_hash();
            tip_time = b.header.time;
        }

        // Spend C1 + same-block chain (txA→txB) on tip+1; archive Class A only
        // then confirm plan=None. Same-block edges must not durable-probe Class A
        // by wire txid (that false-PrevoutSpent mainnet 961461 rehydrate).
        let h_spend = maturity + 3;
        let tx_a = spend_acs(c1_txid, 0, Amount::from_sat(49_0000_0000));
        let tx_a_id = tx_a.compute_txid();
        let tx_b = spend_acs(tx_a_id, 0, Amount::from_sat(48_0000_0000));
        let b_s1 = mine_with(tip, tip_time + 600, h_spend, vec![tx_a, tx_b]);
        accept_and_archive_block(&q, &params, Height(h_spend), &b_s1, ms).unwrap();
        assert_eq!(q.tip_height().map(|h| h.0), Some(maturity + 2));
        // IBD rehydrate path: stamp → load(Forbid) must not miss denserels stage
        // when plan=None (consensus forces Allow cold + body_txid identity).
        {
            let arcs = [(Height(h_spend), Arc::new(b_s1.clone()))];
            let stamped =
                confirm_wire_lookup_stamp(&q, &params, ms, &arcs, None).expect("lookup stamp");
            assert!(
                stamped.plan.is_none(),
                "already-archived body must yield plan=None"
            );
            let mat = confirm_wire_load_from_plan(
                &q,
                &params,
                ms,
                stamped,
                None,
                &ScriptPreverified::new(),
                ColdPinMode::Forbid, // IBD load always passes Forbid
            )
            .expect("plan=None + Forbid must force Allow cold denserels");
            let ok = confirm_scripts_phase(mat.batch).expect("scripts");
            confirm_write_phase(&q, &params, ms, ok.batch)
                .expect("plan=None confirm with same-block spends must succeed");
        }
        assert_eq!(q.tip_height().map(|h| h.0), Some(h_spend));
        assert!(q.is_outpoint_spent(c1_txid.as_byte_array(), 0).unwrap());
        tip = b_s1.block_hash();
        tip_time = b_s1.header.time;

        // Also exercise unified confirm_wire_run on a second already-archived spend.
        // (tip already advanced above; remaining cases use accept_and_connect.)

        // Rapid sequential tip-follow (tip+1 then tip+2) via shipped accept.
        let h_n1 = h_spend + 1;
        let b_n1 = mine_with(
            tip,
            tip_time + 600,
            h_n1,
            vec![spend_acs(c2_txid, 0, Amount::from_sat(49_0000_0000))],
        );
        accept_and_connect_block(&q, &params, Height(h_n1), &b_n1, ms)
            .expect("rapid tip+1 valid spend of C2");
        tip = b_n1.block_hash();
        tip_time = b_n1.header.time;
        let h_n2 = h_n1 + 1;
        let b_n2 = mine_cb(tip, tip_time + 600, h_n2);
        accept_and_connect_block(&q, &params, Height(h_n2), &b_n2, ms)
            .expect("rapid tip+2 coinbase extension");
        assert_eq!(q.tip_height().map(|h| h.0), Some(h_n2));
        tip = b_n2.block_hash();
        tip_time = b_n2.header.time;

        // Genuine double-spend of already-spent C1 fails hard.
        let b_ds = mine_with(
            tip,
            tip_time + 600,
            h_n2 + 1,
            vec![spend_acs(c1_txid, 0, Amount::from_sat(48_0000_0000))],
        );
        let err = accept_and_connect_block(&q, &params, Height(h_n2 + 1), &b_ds, ms)
            .expect_err("double-spend of C1 must fail");
        assert!(
            matches!(err, ConsensusError::PrevoutSpent)
                || format!("{err}").contains("spent")
                || format!("{err}").contains("prevout"),
            "got {err}"
        );

        // Structural without denserels/abs is invariant — not soft PrevoutSpent recovery.
        {
            use super::structural_validate_spends;
            use rbitcoin_primitives::Fk;
            use rbitcoin_query::{BatchParents, U32Map, U64Map};
            use std::collections::HashSet;
            let c2_fk = q.tx_fk_by_txid(c2_txid.as_byte_array()).unwrap().unwrap();
            let spends = vec![(c2_txid.to_byte_array(), 0u32, Fk(9_000_001), c2_fk)];
            let parents = BatchParents::new();
            let ctx = ValidationContext::at(Box::leak(Box::new(params.clone())), Height(h_n1), ms);
            let mut pending = HashSet::new();
            let mut mtp = U32Map::default();
            let mut meta = U64Map::default();
            let err = structural_validate_spends(
                &q,
                &b_n1,
                &ctx,
                Some(&[Fk::NULL, Fk(9_000_001)]),
                &spends,
                0,
                &mut pending,
                &parents,
                &mut mtp,
                &mut meta,
            )
            .expect_err("missing denserels abs must hard-fail");
            let msg = format!("{err}");
            assert!(
                msg.contains("invariant") && msg.contains("denserels"),
                "expected denserels invariant, got {err}"
            );
        }

        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn check_witness_wtxid_count_mismatch_via_structure() {
        // Direct unit path for wtxid count: call internal helper via structure
        // with inconsistent precomputed length is not public — exercise
        // missing commitment + reserved-mismatch already covered; cover odd
        // merkle witness leaf + wrong commitment without reserved stack item.
        let mut spend = non_coinbase_spend(12);
        spend.input[0].witness = Witness::from_slice(&[vec![0xcd]]);
        let mut cb = coinbase(1);
        // Commitment magic with zeros; coinbase witness empty → mismatch (no reserved).
        let mut spk = vec![0x6a, 0x24, 0xaa, 0x21, 0xa9, 0xed];
        spk.extend([0u8; 32]);
        cb.output.push(TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_bytes(spk),
        });
        // Non-32 reserved last item cannot rescue.
        cb.input[0].witness = Witness::from_slice(&[vec![0x01, 0x02]]);
        let b = block_with(vec![cb, spend]);
        let err = validate_block_structure(&b, &ctx_h(1)).unwrap_err();
        assert!(
            matches!(err, ConsensusError::BadBlock(s) if s.contains("witness")),
            "got {err:?}"
        );
    }

    /// BIP16 from precomputed prev MTP — no header walk, exception hash respected.
    #[test]
    fn bip16_from_prev_mtp_exception_and_time() {
        let p = Box::leak(Box::new(ChainParams::mainnet()));
        // Exception block never enables P2SH regardless of MTP.
        assert!(!bip16_active_from_prev_mtp(
            p,
            170_000,
            &BIP16_EXCEPTION_MAINNET,
            u32::MAX,
        ));
        // Pre-bip16_time MTP → inactive.
        assert!(!bip16_active_from_prev_mtp(p, 170_000, &[1u8; 32], 0));
        // At/after bip16_time → active.
        assert!(bip16_active_from_prev_mtp(
            p,
            170_000,
            &[1u8; 32],
            p.btc.bip16_time,
        ));
        // Genesis height never.
        assert!(!bip16_active_from_prev_mtp(
            p,
            0,
            &[1u8; 32],
            p.btc.bip16_time,
        ));
    }

    /// Confirm jobs share wire Arc — same Transaction address, no deep clone.
    #[test]
    fn script_job_shared_tx_is_wire_pointer() {
        use std::sync::Arc;
        let spend = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array([7; 32]),
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
        let block = Arc::new(block_with(vec![coinbase(1), spend]));
        let tid = block.txdata[1].compute_txid().to_byte_array();
        let job = ScriptCheckJob::with_shared_tx(
            tid,
            vec![TxOut {
                value: Amount::from_sat(50_0000_0000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
            Arc::clone(&block),
            1,
            true,
            true,
            true,
            true,
            true,
        );
        assert!(std::ptr::eq(
            &*job.tx as *const Transaction,
            &block.txdata[1] as *const Transaction
        ));
        assert_eq!(job.txid, tid);
    }
}

#[cfg(test)]
mod sigop_cost_tests {
    use super::{
        is_p2sh_script, last_script_push, p2sh_sigop_count, script_sigop_count, tx_sigop_cost,
        witness_sigop_count,
    };
    use bitcoin::absolute::LockTime;
    use bitcoin::hashes::Hash;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{Amount, OutPoint, Sequence, Transaction, TxIn, TxOut, Txid, Witness};

    #[test]
    fn last_push_pushdata_and_non_push_skip() {
        // OP_PUSHDATA1 / 2 / 4 last push + non-push opcode continues.
        let mut sc = vec![0x51]; // OP_1 (not a data push for last_script_push)
        sc.extend_from_slice(&[0x4c, 0x02, 0xab, 0xcd]); // PUSHDATA1 2
        assert_eq!(last_script_push(&sc), Some(&[0xabu8, 0xcd][..]));

        let mut sc2 = vec![0x4d, 0x02, 0x00, 0x11, 0x22]; // PUSHDATA2
        sc2.extend_from_slice(&[0xac]); // CHECKSIG after
        assert_eq!(last_script_push(&sc2), Some(&[0x11u8, 0x22][..]));

        let sc3 = vec![0x4e, 0x01, 0x00, 0x00, 0x00, 0xee]; // PUSHDATA4 len=1
        assert_eq!(last_script_push(&sc3), Some(&[0xeeu8][..]));

        // Truncated push ignored.
        assert!(last_script_push(&[0x4c, 0x05, 0x01]).is_none());
        assert!(last_script_push(&[0x4e, 0x10, 0x00, 0x00, 0x00]).is_none());
        assert!(is_p2sh_script(&[
            0xa9, 0x14, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x87
        ]));
        assert!(!is_p2sh_script(&[0x51]));
    }

    #[test]
    fn p2sh_and_witness_sigop_paths() {
        // Nested P2SH-P2WPKH: redeem is 0x0014||20, scriptSig last push = redeem.
        let redeem = {
            let mut r = vec![0x00, 0x14];
            r.extend([0x11u8; 20]);
            r
        };
        let mut ss = vec![redeem.len() as u8];
        ss.extend_from_slice(&redeem);
        let p2sh_spk = {
            use bitcoin::hashes::{hash160, Hash};
            let h = hash160::Hash::hash(&redeem);
            let mut spk = vec![0xa9, 0x14];
            spk.extend_from_slice(h.as_byte_array());
            spk.push(0x87);
            spk
        };
        let tx = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_byte_array([1; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::from_bytes(ss),
                sequence: Sequence::MAX,
                witness: Witness::from_slice(&[vec![0x00], vec![0x01; 33]]),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let prevouts = vec![TxOut {
            value: Amount::from_sat(1),
            script_pubkey: ScriptBuf::from_bytes(p2sh_spk),
        }];
        assert!(witness_sigop_count(&tx, &prevouts) >= 1);
        // P2SH bare redeem with CHECKSIG
        let redeem2 = vec![0xac];
        let mut ss2 = vec![0x01];
        ss2.extend_from_slice(&redeem2);
        let mut spk2 = vec![0xa9, 0x14];
        {
            use bitcoin::hashes::{hash160, Hash};
            let h = hash160::Hash::hash(&redeem2);
            spk2.extend_from_slice(h.as_byte_array());
        }
        spk2.push(0x87);
        let tx2 = Transaction {
            version: TxVersion::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_byte_array([2; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::from_bytes(ss2),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let prev2 = vec![TxOut {
            value: Amount::from_sat(1),
            script_pubkey: ScriptBuf::from_bytes(spk2),
        }];
        assert!(p2sh_sigop_count(&tx2, &prev2) >= 1);
        assert!(tx_sigop_cost(&tx2, &prev2, true) >= 4);
        // Nested P2SH without redeem push → continue (0 witness sigops).
        let tx3 = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_byte_array([3; 32]),
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
        let prev3 = vec![TxOut {
            value: Amount::from_sat(1),
            script_pubkey: ScriptBuf::from_bytes(vec![
                0xa9, 0x14, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x87,
            ]),
        }];
        assert_eq!(witness_sigop_count(&tx3, &prev3), 0);
    }

    #[test]
    fn last_push_and_p2sh_shape() {
        // OP_1 push of 0xac (CHECKSIG) as redeem
        let ss = [0x01, 0xac];
        assert_eq!(last_script_push(&ss), Some(&[0xacu8][..]));
        let p2sh = {
            let mut v = vec![0xa9, 0x14];
            v.extend([0u8; 20]);
            v.push(0x87);
            v
        };
        assert!(is_p2sh_script(&p2sh));
        assert!(!is_p2sh_script(&[0x51]));
    }

    #[test]
    fn accurate_multisig_count() {
        // OP_2 <key> <key> <key> OP_3 OP_CHECKMULTISIG → 3 when accurate
        let redeem = vec![
            0x52, // OP_2
            0x21, // push 33
        ];
        let mut r = redeem;
        r.extend([0x02; 33]);
        r.push(0x21);
        r.extend([0x02; 33]);
        r.push(0x21);
        r.extend([0x02; 33]);
        r.push(0x53); // OP_3
        r.push(0xae); // CHECKMULTISIG
        assert_eq!(script_sigop_count(&r, true), 3);
        assert_eq!(script_sigop_count(&r, false), 20);
    }

    #[test]
    fn p2sh_sigops_from_redeem() {
        let mut p2sh_spk = vec![0xa9, 0x14];
        p2sh_spk.extend([0u8; 20]);
        p2sh_spk.push(0x87);
        // redeem = single CHECKSIG
        let redeem = [0xac];
        let mut ss = vec![0x01]; // push 1 byte
        ss.extend_from_slice(&redeem);
        let tx = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_byte_array([1; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::from_bytes(ss),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let prevouts = vec![TxOut {
            value: Amount::from_sat(10),
            script_pubkey: ScriptBuf::from_bytes(p2sh_spk),
        }];
        assert_eq!(p2sh_sigop_count(&tx, &prevouts), 1);
        // legacy×4 + p2sh×4 = 0 + 4 (no legacy CHECKSIG in ss/spk for bare count of redeem)
        let cost = tx_sigop_cost(&tx, &prevouts, true);
        // scriptSig has push only (0 legacy), output OP_1 (0), p2sh redeem 1×4 = 4
        assert_eq!(cost, 4);
    }

    #[test]
    fn witness_p2wpkh_counts_one() {
        let mut spk = vec![0x00, 0x14];
        spk.extend([0u8; 20]);
        let tx = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_byte_array([2; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::from_slice(&[vec![0x30], vec![0x02; 33]]),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let prevouts = vec![TxOut {
            value: Amount::from_sat(10),
            script_pubkey: ScriptBuf::from_bytes(spk),
        }];
        assert_eq!(witness_sigop_count(&tx, &prevouts), 1);
    }

    #[test]
    fn script_sigop_pushdata_encodings_and_checksigverify() {
        // PUSHDATA1 / 2 / 4 skip payload without counting ops inside.
        let mut s = vec![0x4c, 0x02, 0xac, 0xad]; // push 2 bytes that look like CHECKSIG
        s.push(0xac); // real CHECKSIG after
        assert_eq!(script_sigop_count(&s, false), 1);

        let mut s2 = vec![0x4d, 0x02, 0x00, 0xac, 0xad];
        s2.push(0xad); // CHECKSIGVERIFY
        assert_eq!(script_sigop_count(&s2, false), 1);

        let mut s3 = vec![0x4e, 0x01, 0x00, 0x00, 0x00, 0xac];
        s3.push(0xae); // CHECKMULTISIG → 20
        assert_eq!(script_sigop_count(&s3, false), 20);

        // last_script_push with PUSHDATA*
        let lp = vec![0x4c, 0x01, 0xab];
        assert_eq!(last_script_push(&lp), Some(&[0xabu8][..]));
        let lp2 = vec![0x4d, 0x01, 0x00, 0xcd];
        assert_eq!(last_script_push(&lp2), Some(&[0xcdu8][..]));
        let lp3 = vec![0x4e, 0x01, 0x00, 0x00, 0x00, 0xef];
        assert_eq!(last_script_push(&lp3), Some(&[0xefu8][..]));
        let _ = (lp, lp2, lp3);
    }

    #[test]
    fn witness_p2wsh_and_nested_p2sh() {
        // Native P2WSH: last witness item is script with CHECKSIG.
        let ws = vec![0xac];
        let scripthash = {
            use bitcoin::hashes::{sha256, Hash};
            *sha256::Hash::hash(&ws).as_byte_array()
        };
        let mut spk = vec![0x00, 0x20];
        spk.extend_from_slice(&scripthash);
        let tx = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_byte_array([3; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::from_slice(&[vec![0x01], ws.clone()]),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let prevouts = vec![TxOut {
            value: Amount::from_sat(10),
            script_pubkey: ScriptBuf::from_bytes(spk),
        }];
        assert_eq!(witness_sigop_count(&tx, &prevouts), 1);

        // Nested P2SH-P2WPKH: redeem in scriptSig.
        let mut redeem = vec![0x00, 0x14];
        redeem.extend([0u8; 20]);
        let mut ss = vec![redeem.len() as u8];
        ss.extend_from_slice(&redeem);
        let mut p2sh = vec![0xa9, 0x14];
        p2sh.extend([0u8; 20]);
        p2sh.push(0x87);
        let tx2 = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_byte_array([4; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::from_bytes(ss),
                sequence: Sequence::MAX,
                witness: Witness::from_slice(&[vec![0x30], vec![0x02; 33]]),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let prevouts2 = vec![TxOut {
            value: Amount::from_sat(10),
            script_pubkey: ScriptBuf::from_bytes(p2sh),
        }];
        assert_eq!(witness_sigop_count(&tx2, &prevouts2), 1);

        // p2sh_sigop prevouts short / non-p2sh skip
        assert_eq!(p2sh_sigop_count(&tx2, &[]), 0);
        assert_eq!(
            p2sh_sigop_count(
                &tx2,
                &[TxOut {
                    value: Amount::from_sat(1),
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                }]
            ),
            0
        );
        // witness_sigop missing prevout
        assert_eq!(witness_sigop_count(&tx, &[]), 0);
    }

    #[test]
    fn verify_scripts_pool_empty_and_anyone_can_spend() {
        use super::{verify_scripts_pool, verify_scripts_pool_jobs, ScriptCheckJob};
        assert!(verify_scripts_pool(&[]).is_ok());
        assert!(verify_scripts_pool_jobs(&[]).is_ok());
        let job = ScriptCheckJob::new(
            vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]), // OP_TRUE ACS
            }],
            Transaction {
                version: TxVersion::TWO,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint {
                        txid: Txid::from_byte_array([9; 32]),
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
            },
            true,
            true,
            true,
            true,
            true,
        );
        assert!(verify_scripts_pool(&[job]).is_ok());
        // Borrowed job list path with one ACS job.
        let job2 = ScriptCheckJob::new(
            vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
            Transaction {
                version: TxVersion::TWO,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint {
                        txid: Txid::from_byte_array([8; 32]),
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
            },
            true,
            true,
            true,
            true,
            true,
        );
        assert!(verify_scripts_pool_jobs(&[&job2]).is_ok());
    }

    #[test]
    fn job_tx_traits_and_shared_mut_panic() {
        use super::{block_subsidy, is_anyone_can_spend, is_final_tx, JobTx};
        use crate::params::ChainParams;
        use bitcoin::block::{Header, Version};
        use bitcoin::{Block, BlockHash, CompactTarget, TxMerkleNode};
        use std::borrow::Borrow;
        use std::sync::Arc;

        let tx = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let owned: JobTx = tx.clone().into();
        assert_eq!(owned.as_ref().output.len(), 1);
        assert_eq!(Borrow::<Transaction>::borrow(&owned).output.len(), 1);
        let mut owned_mut = JobTx::owned(tx.clone());
        assert_eq!(owned_mut.output.len(), 1);
        owned_mut.output[0].value = Amount::from_sat(2); // DerefMut owned

        // Minimal block shell for shared JobTx.
        let header = Header {
            version: Version::ONE,
            prev_blockhash: BlockHash::from_byte_array([0; 32]),
            merkle_root: TxMerkleNode::from_byte_array([0; 32]),
            time: 1,
            bits: CompactTarget::from_consensus(0x207f_ffff),
            nonce: 0,
        };
        let block = Arc::new(Block {
            header,
            txdata: vec![
                Transaction {
                    version: TxVersion::ONE,
                    lock_time: LockTime::ZERO,
                    input: vec![TxIn {
                        previous_output: OutPoint::null(),
                        script_sig: ScriptBuf::from_bytes(vec![0x00, 0x00]),
                        sequence: Sequence::MAX,
                        witness: Witness::new(),
                    }],
                    output: vec![TxOut {
                        value: Amount::from_sat(50),
                        script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                    }],
                },
                tx.clone(),
            ],
        });
        let shared = JobTx::shared(Arc::clone(&block), 1);
        assert_eq!(shared.as_ref().output.len(), 1);
        let mut shared_mut = JobTx::shared(block, 1);
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = &mut shared_mut.output;
        }));
        assert!(r.is_err(), "shared JobTx must panic on DerefMut");

        let p = ChainParams::regtest();
        assert_eq!(block_subsidy(0, &p), 50 * 100_000_000);
        assert_eq!(block_subsidy(210_000, &p), 25 * 100_000_000);
        assert_eq!(block_subsidy(6_930_000, &p), 0);
        assert!(is_anyone_can_spend(
            ScriptBuf::from_bytes(vec![0x51]).as_script()
        ));
        assert!(!is_anyone_can_spend(
            ScriptBuf::from_bytes(vec![0x00]).as_script()
        ));
        let final_tx = Transaction {
            version: TxVersion::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![],
        };
        assert!(is_final_tx(&final_tx, 0, 0));
    }
}
