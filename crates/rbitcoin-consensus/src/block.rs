use crate::error::ConsensusError;
use crate::milestone::Milestone;
use crate::params::ChainParams;
use bitcoin::block::Block;
use bitcoin::hashes::{sha256d, Hash};
use bitcoin::script::{Script, ScriptBuf};
use bitcoin::{Amount, OutPoint, Transaction, TxOut};
use rbitcoin_primitives::Height;
use rbitcoin_query::Query;

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
    block.txdata.iter().any(|tx| {
        tx.input
            .iter()
            .any(|i| !i.witness.is_empty())
    })
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
    // commitment hash = SHA256D(witness_root || witness_reserved_value)
    // Standard reserved value is 32 zero bytes when not using commitment nonce.
    let reserved = [0u8; 32];
    let mut buf = [0u8; 64];
    buf[0..32].copy_from_slice(&witness_root);
    buf[32..64].copy_from_slice(&reserved);
    let hash = sha256d::Hash::hash(&buf);
    if hash.to_byte_array() != committed {
        // Also accept if witness reserved is in coinbase witness stack (BIP141)
        if coinbase.input[0].witness.len() >= 1 {
            let wr = coinbase.input[0].witness.last().unwrap();
            if wr.len() == 32 {
                let mut buf2 = [0u8; 64];
                buf2[0..32].copy_from_slice(&witness_root);
                buf2[32..64].copy_from_slice(wr);
                let hash2 = sha256d::Hash::hash(&buf2);
                if hash2.to_byte_array() == committed {
                    return Ok(());
                }
            }
        }
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
/// 2. **Scripts** — above milestone, rayon pool (CPU; needs prevout values only).
/// 3. **Structural** — durable spentness, maturity, coinbase subsidy (order-sensitive).
///
/// Class C tip updates (`confirm_block`) stay outside this function.
///
/// `archived_tx_fks`: Class A fks for `block.txdata` (same order) when confirming
/// archived bodies (thin create_fk / Class A rows in parent cache).
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
    // Tip/connect path: no separate pin stage; resolve from outs FIFO + store.
    let batch_parents = rbitcoin_query::BatchParents::new();
    let batch_thin = rbitcoin_query::BatchThin::new();
    let (script_jobs, spends, fees) = assemble_block_prevouts(
        query,
        block,
        ctx,
        archived_tx_fks,
        &mut pending,
        &mut pending_creates,
        &batch_parents,
        &batch_thin,
    )?;
    if check_scripts && !script_jobs.is_empty() {
        verify_scripts_pool(&script_jobs)?;
    }
    // Structural: re-walk with durable spentness (fresh pending for this single block).
    let mut structural_pending = std::collections::HashSet::new();
    let mut mtp_cache = std::collections::HashMap::new();
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
const BIP16_EXCEPTION_MAINNET: [u8; 32] = [
    // little-endian display hash 00000000000002dc756eebf4f49723ed8d30cc28a5f108eb94b1ba88ac4f9c22
    0x22, 0x9c, 0x4f, 0xac, 0x88, 0xba, 0xb1, 0x94, 0xeb, 0x08, 0xf1, 0xa5, 0x28, 0xcc, 0x30,
    0x8d, 0xed, 0x23, 0x97, 0xf4, 0xf4, 0xeb, 0x6e, 0x75, 0xdc, 0x02, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00,
];

/// BIP16 P2SH flag for scripts in `block` at `ctx.height`.
fn bip16_active_for_block(
    query: &Query,
    ctx: &ValidationContext<'_>,
    block: &Block,
) -> bool {
    use bitcoin::hashes::Hash;
    // Exception block: never SCRIPT_VERIFY_P2SH.
    if block.block_hash().to_byte_array() == BIP16_EXCEPTION_MAINNET {
        return false;
    }
    if ctx.height.0 == 0 {
        return false;
    }
    // Core: previous block's median-time-past >= bip16_time.
    match crate::header::median_time_past(query, Height(ctx.height.0 - 1)) {
        Ok(mtp) => mtp >= ctx.params.btc.bip16_time,
        // Store missing headers (tests): fall back to known mainnet height gate.
        Err(_) => ctx.height.0 >= 173_805,
    }
}

/// Owns a [`Transaction`] clone so the reconstructed block can be dropped before the
/// parallel script wave, without a wasteful encode→deserialize round-trip.
pub struct ScriptCheckJob {
    pub(crate) prevouts: Vec<TxOut>,
    pub(crate) tx: Transaction,
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
pub(crate) fn assemble_block_prevouts(
    query: &Query,
    block: &Block,
    ctx: &ValidationContext<'_>,
    archived_tx_fks: Option<&[rbitcoin_primitives::Fk]>,
    pending_spent: &mut std::collections::HashSet<([u8; 32], u32)>,
    pending_creates: &mut std::collections::HashMap<([u8; 32], u32), rbitcoin_primitives::Fk>,
    batch_parents: &rbitcoin_query::BatchParents,
    batch_thin: &rbitcoin_query::BatchThin,
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
    if block.txdata.is_empty() {
        return Err(ConsensusError::BadBlock("empty block"));
    }
    if !block.txdata[0].is_coinbase() {
        return Err(ConsensusError::BadBlock("first tx not coinbase"));
    }

    // BIP16 (P2SH): Core uses previous block's median-time-past vs bip16_time,
    // with one mainnet exception block that never enforces P2SH redeem rules.
    let bip16_for_jobs = bip16_active_for_block(query, ctx, block);

    let n_tx = block.txdata.len();
    let mut block_spends: std::collections::HashSet<OutPoint> =
        std::collections::HashSet::with_capacity(n_tx.saturating_mul(2));
    // txid → index into `block.txdata` (no script clones until a same-block spend).
    let mut same_block: std::collections::HashMap<[u8; 32], usize> =
        std::collections::HashMap::with_capacity(n_tx);
    let mut fees = 0i64;
    // Skip job materialization (tx clone) when scripts are skipped — pure waste
    // below the milestone. Prevouts still resolve for fees + full sigop cost.
    let build_script_jobs = !ctx.milestone.skips_scripts_at(ctx.height.0);
    let mut script_jobs: Vec<ScriptCheckJob> = if build_script_jobs {
        Vec::with_capacity(n_tx.saturating_sub(1))
    } else {
        Vec::new()
    };
    // BIP141/BIP16 full block sigop cost (structure only counts legacy×4).
    const MAX_BLOCK_SIGOPS_COST: u64 = 80_000;
    let mut block_sigops_cost =
        legacy_sigop_count(&block.txdata[0]).saturating_mul(4);
    // (prev_txid, vout, spending_tx_fk, create_tx_fk).
    let mut spends: Vec<(
        [u8; 32],
        u32,
        rbitcoin_primitives::Fk,
        rbitcoin_primitives::Fk,
    )> = Vec::with_capacity(n_tx.saturating_mul(2));
    // Coinbase height cache spans the whole block (was recreated per tx).
    let mut coinbase_height_cache: std::collections::HashMap<
        rbitcoin_primitives::Fk,
        Option<u32>,
    > = std::collections::HashMap::with_capacity(64);

    // Spent checks: pending_spent (this run) + durable confirmed-strong annotations.
    let cache = query.confirm_parent_cache();
    for (ti, tx) in block.txdata.iter().enumerate() {
        let spend_fk = archived_tx_fks.map(|fks| fks[ti]);
        // Txid only — prefer cache body meta (no full Class A re-decode).
        let archived_txid: Option<[u8; 32]> = if let Some(fk) = spend_fk {
            cache.get_parent_txid(fk).or_else(|| {
                query
                    .get_tx_class_a(fk)
                    .ok()
                    .map(|r| r.txid)
            })
        } else {
            None
        };

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

            for (ii, input) in tx.input.iter().enumerate() {
                let op = input.previous_output;
                if !block_spends.insert(op) {
                    return Err(ConsensusError::BadTx("double spend in block"));
                }
                let key = (op.txid.to_byte_array(), op.vout);
                if pending_spent.contains(&key) {
                    return Err(ConsensusError::PrevoutSpent);
                }
                // Load pin spent-filtered sparse outs / body creates: skip durable
                // probes when the parent out is already in the batch pin map or body.
                let prev_fk = thin
                    .as_ref()
                    .and_then(|t| t.get(ii))
                    .and_then(|e| e.create_fk.map(rbitcoin_primitives::Fk))
                    .or_else(|| pending_creates.get(&key).copied())
                    .or_else(|| {
                        query
                            .tx_fk_by_txid(op.txid.as_byte_array())
                            .ok()
                            .flatten()
                    });
                let pin_live = prev_fk
                    .map(|fk| {
                        batch_parents.has_parent_out(fk, op.vout)
                            || cache.has_parent_out(fk, op.vout)
                    })
                    .unwrap_or(false);
                // Durable spentness: Full mode only. Optimistic defers to structural
                // after scripts (assumevalid-shaped: scripts need values, not UTXO proof).
                if mode == AssembleMode::Full
                    && !pin_live
                    && !pending_creates.contains_key(&key)
                {
                    let spent = if let Some(cfk) = prev_fk {
                        // `None` range → store resolves via tx.idx.
                        query
                            .store()
                            .has_confirmed_strong_spender_create(cfk, op.vout, None)
                            .map_err(ConsensusError::Store)?
                    } else {
                        query
                            .store()
                            .has_confirmed_strong_spender(op.txid.as_byte_array(), op.vout)
                            .map_err(ConsensusError::Store)?
                    };
                    if spent {
                        return Err(ConsensusError::PrevoutSpent);
                    }
                }
                // Optimistic: skip create_height / coinbase store lookups (BIP68 +
                // maturity run in structural). Full: resolve for in-walk BIP68.
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
                let create_fk = prev_fk.unwrap_or(rbitcoin_primitives::Fk::NULL);
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

            block_sigops_cost = block_sigops_cost.saturating_add(tx_sigop_cost(
                tx,
                &prevouts,
                bip16_for_jobs,
            ));
            if block_sigops_cost > MAX_BLOCK_SIGOPS_COST {
                return Err(ConsensusError::BadBlock("bad-blk-sigops"));
            }

            // BIP113 absolute finality (prev-block MTP only — cheap; stays on load).
            let lock_time_cutoff = if ctx.params.csv_active_at(ctx.height.0) {
                if ctx.height.0 == 0 {
                    block.header.time
                } else {
                    crate::header::median_time_past(query, Height(ctx.height.0 - 1))?
                }
            } else {
                block.header.time
            };
            if !is_final_tx(tx, ctx.height.0, lock_time_cutoff) {
                return Err(ConsensusError::BadTx("not final"));
            }
            // BIP68 relative locks need per-input create heights (`tx_height`).
            // Optimistic/confirm defers that IO to structural write; Full does it here.
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
                    crate::header::median_time_past(query, Height(ctx.height.0 - 1))?
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
                script_jobs.push(ScriptCheckJob {
                    prevouts,
                    // One deep clone beats encode-at-connect + deserialize-per-worker.
                    tx: tx.clone(),
                    bip65_active: ctx.params.bip65_active_at(ctx.height.0),
                    bip112_active: ctx.params.csv_active_at(ctx.height.0),
                    bip66_active: ctx.params.bip66_active_at(ctx.height.0),
                    bip16_active: bip16_for_jobs,
                    taproot_active: ctx.params.taproot_active_at(ctx.height.0),
                });
            }
        }

        let txid = if let Some(t) = archived_txid {
            t
        } else {
            tx.compute_txid().to_byte_array()
        };
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
    pub create_h_ns: u64,
    pub bip68_ns: u64,
}

/// Post-script structural checks: durable spentness, maturity, BIP68, coinbase subsidy.
///
/// Runs in height order on the write path (after scripts). `pending_spent` is
/// write-local across a multi-height run.
///
/// **BIP68** create-height / coin-MTP IO lives here (not optimistic load assemble)
/// so confirm load does not walk `tx_height` for every parent.
///
/// **Spentness:** prefer pin-stashed absolute 9-byte spender offsets + one
/// bulk io_uring pread; fall back to body-range / idx for cold parents.
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
    mtp_cache: &mut std::collections::HashMap<u32, u32>,
) -> Result<StructuralPhaseNs, ConsensusError> {
    use std::collections::{HashMap, HashSet};
    use std::time::Instant;

    let mut coinbase_height_cache: HashMap<rbitcoin_primitives::Fk, Option<u32>> =
        HashMap::with_capacity(64);
    // create_fk → create height (BIP68), filled in create-height phase.
    let mut create_height_by_fk: HashMap<rbitcoin_primitives::Fk, u32> =
        HashMap::with_capacity(spends.len().min(256));
    let maturity = ctx.params.coinbase_maturity();
    let cache = query.confirm_parent_cache();

    // ── Spentness (durable + same-run pending) ─────────────────────────────
    let t_spent = Instant::now();

    // Unique create_fk → sorted unique vouts.
    let mut vouts_by_create: HashMap<u64, Vec<u32>> = HashMap::new();
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

    // Partition: pin-layout absolute offsets (bulk 9-byte pread) vs cold fallback.
    // abs_jobs: (create_id, vout, abs_off)
    let mut abs_jobs: Vec<(u64, u32, u64)> = Vec::with_capacity(spends.len());
    let mut fallback_by_create: HashMap<u64, Vec<u32>> = HashMap::new();
    for (id, vouts) in &vouts_by_create {
        let fk = rbitcoin_primitives::Fk(*id);
        let mut cold: Vec<u32> = Vec::new();
        for &v in vouts {
            if let Some(abs) = batch_parents.get_spender_abs(fk, v) {
                abs_jobs.push((*id, v, abs));
            } else {
                cold.push(v);
            }
        }
        if !cold.is_empty() {
            fallback_by_create.insert(*id, cold);
        }
    }

    // Durable unspent: (create_id, vout) present ⇒ not confirmed-strong spent.
    let mut durable_unspent: HashSet<(u64, u32)> = HashSet::with_capacity(spends.len());
    let tip = query.tip_height().map(|h| h.0);

    // Hot path: bulk 9-byte spender meta at pin offsets.
    if !abs_jobs.is_empty() {
        let abs_offs: Vec<u64> = abs_jobs.iter().map(|(_, _, a)| *a).collect();
        let metas = query
            .store()
            .get_spender_meta_at_abs_batch(&abs_offs)
            .map_err(ConsensusError::Store)?;
        for (i, &(id, vout, _)) in abs_jobs.iter().enumerate() {
            let Some((multi, field)) = metas[i] else {
                // Short/failed pread: treat as not live (fail closed).
                continue;
            };
            if field.is_null() {
                durable_unspent.insert((id, vout));
                continue;
            }
            if multi {
                // Rare multi-list: cold path for this one out.
                fallback_by_create.entry(id).or_default().push(vout);
                continue;
            }
            let strong = query
                .store()
                .is_confirmed_strong_at(field, tip)
                .map_err(ConsensusError::Store)?;
            if !strong {
                durable_unspent.insert((id, vout));
            }
        }
    }

    // Cold: bulk idx ranges once, then unspent_create_vouts per create.
    if !fallback_by_create.is_empty() {
        let need_range: Vec<rbitcoin_primitives::Fk> = fallback_by_create
            .keys()
            .map(|id| rbitcoin_primitives::Fk(*id))
            .collect();
        let ranges = query
            .store()
            .tx_body_range_batch(&need_range)
            .map_err(ConsensusError::Store)?;
        let mut range_by_create: HashMap<u64, (u64, u64)> =
            HashMap::with_capacity(need_range.len());
        for (fk, opt) in need_range.iter().zip(ranges.into_iter()) {
            if let (Some(id), Some(r)) = (fk.get(), opt) {
                range_by_create.insert(id, r);
            }
        }
        for (id, mut vouts) in fallback_by_create {
            vouts.sort_unstable();
            vouts.dedup();
            let range = range_by_create.get(&id).copied();
            let unspent = query
                .store()
                .unspent_create_vouts(rbitcoin_primitives::Fk(id), &vouts, range)
                .map_err(ConsensusError::Store)?;
            for v in unspent {
                durable_unspent.insert((id, v));
            }
        }
    }

    // Null create_fk (rare): txid path, one probe each.
    let mut durable_null_spent: HashSet<([u8; 32], u32)> = HashSet::new();
    null_create_keys.sort_unstable();
    null_create_keys.dedup();
    for &(prev_txid, vout) in &null_create_keys {
        if query
            .store()
            .has_confirmed_strong_spender(&prev_txid, vout)
            .map_err(ConsensusError::Store)?
        {
            durable_null_spent.insert((prev_txid, vout));
        }
    }

    // Order-sensitive pending double-spend + durable rejection.
    for &(prev_txid, vout, _spend_fk, create_fk) in spends {
        let key = (prev_txid, vout);
        if pending_spent.contains(&key) {
            return Err(ConsensusError::PrevoutSpent);
        }
        let spent = if create_fk.is_null() {
            durable_null_spent.contains(&key)
        } else if let Some(id) = create_fk.get() {
            // Missing from durable_unspent ⇒ confirmed-strong spent (or OOB).
            !durable_unspent.contains(&(id, vout))
        } else {
            false
        };
        if spent {
            return Err(ConsensusError::PrevoutSpent);
        }
        pending_spent.insert(key);
    }
    let spent_ns = t_spent.elapsed().as_nanos() as u64;

    // ── Create height + coinbase maturity (prefer pin stashes) ─────────────
    let t_create = Instant::now();
    let mut seen_create: HashSet<u64> = HashSet::with_capacity(vouts_by_create.len());
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
        // Already resolved (should not happen with seen_create).
        if create_height_by_fk.contains_key(&create_fk) {
            continue;
        }

        // Pin coinbase flag + height: skip store maturity / input walk.
        if let Some(cb_opt) = batch_parents.get_parent_coinbase_height(create_fk) {
            match cb_opt {
                Some(ch) => {
                    if ctx.height.0 < ch.saturating_add(maturity) {
                        return Err(ConsensusError::BadTx("coinbase immature"));
                    }
                    create_height_by_fk.insert(create_fk, ch);
                    continue;
                }
                None => {
                    // Not a coinbase — BIP68 height from pin or tx_height only.
                    let ch = if let Some(h) = batch_parents.get_create_height(create_fk) {
                        h
                    } else {
                        query
                            .store()
                            .tx_height
                            .get(create_fk)
                            .map_err(ConsensusError::Store)?
                            .unwrap_or(0)
                    };
                    create_height_by_fk.insert(create_fk, ch);
                    continue;
                }
            }
        }

        // Pin create height without coinbase flag (pin_new): still need maturity.
        let pin_ch = batch_parents.get_create_height(create_fk);
        let prev_rec = match batch_parents
            .get_parent_tx(create_fk)
            .or_else(|| cache.get_parent_tx(create_fk))
        {
            Some(r) => r,
            None => query
                .get_tx_class_a(create_fk)
                .map_err(ConsensusError::Store)?,
        };
        let created = coinbase_height_for_maturity(
            query,
            create_fk,
            &prev_rec,
            batch_parents,
            &mut coinbase_height_cache,
        )?;
        if let Some(ch) = created {
            if ctx.height.0 < ch.saturating_add(maturity) {
                return Err(ConsensusError::BadTx("coinbase immature"));
            }
        }
        let ch = if let Some(h) = pin_ch {
            h
        } else {
            create_height_for_fk(query, create_fk, created)?
        };
        create_height_by_fk.insert(create_fk, ch);
    }
    let create_h_ns = t_create.elapsed().as_nanos() as u64;

    // ── BIP68 relative sequence locks (CSV package) ────────────────────────
    let t_bip68 = Instant::now();
    if ctx.params.csv_active_at(ctx.height.0) {
        let prev_mtp = if ctx.height.0 == 0 {
            0
        } else {
            mtp_at(query, Height(ctx.height.0 - 1), mtp_cache)?
        };
        let mut si = 0usize;
        for tx in block.txdata.iter().skip(1) {
            let n_in = tx.input.len();
            if si + n_in > spends.len() {
                return Err(ConsensusError::BadBlock("structural spends/tx input mismatch"));
            }
            let tx_spends = &spends[si..si + n_in];
            si += n_in;

            let mut prev_heights = Vec::with_capacity(n_in);
            let mut coin_mtps = Vec::with_capacity(n_in);
            for &(_ptid, _vout, _sfk, create_fk) in tx_spends {
                let ch = if create_fk.is_null() {
                    // Same-block create (no Class A fk yet): Core uses spend height.
                    ctx.height.0
                } else {
                    create_height_by_fk
                        .get(&create_fk)
                        .copied()
                        .unwrap_or(0)
                };
                prev_heights.push(ch);
                let mtp = if ch == 0 {
                    0
                } else {
                    mtp_at(query, Height(ch.saturating_sub(1)), mtp_cache)?
                };
                coin_mtps.push(mtp);
            }
            if !sequence_locks_satisfied(
                tx,
                &prev_heights,
                &coin_mtps,
                ctx.height.0,
                prev_mtp,
            ) {
                return Err(ConsensusError::BadTx("bad-txns-nonfinal"));
            }
        }
        if si != spends.len() {
            return Err(ConsensusError::BadBlock("structural spends/tx input mismatch"));
        }
    }
    let bip68_ns = t_bip68.elapsed().as_nanos() as u64;

    let _ = archived_tx_fks;
    check_coinbase_subsidy(block, ctx, fees)?;
    Ok(StructuralPhaseNs {
        spent_ns,
        create_h_ns,
        bip68_ns,
    })
}

/// [`crate::header::median_time_past`] with a write-run cache keyed by end height.
fn mtp_at(
    query: &Query,
    height: Height,
    cache: &mut std::collections::HashMap<u32, u32>,
) -> Result<u32, ConsensusError> {
    if let Some(&t) = cache.get(&height.0) {
        return Ok(t);
    }
    let t = crate::header::median_time_past(query, height)?;
    cache.insert(height.0, t);
    Ok(t)
}

/// Parallel script checks for an owned job slice (preferred entry — no ref `Vec`).
pub fn verify_scripts_pool(jobs: &[ScriptCheckJob]) -> Result<(), ConsensusError> {
    verify_script_jobs(jobs)
}

/// Parallel script/sig checks across jobs (possibly from multiple blocks).
///
/// Uses the **rayon global pool** for all non-empty waves (including a single
/// job — one code path). One job = one non-coinbase tx (shared
/// [`bitcoin::sighash::SighashCache`] across its inputs).
///
/// **Why rayon (not a custom tokio queue):** script verify is CPU-bound and
/// runs on the confirm OS thread outside the async runtime; rayon’s work-stealing
/// pool is built for that. Wire rebuild stays sequential (see `confirm_run`).
pub fn verify_scripts_pool_jobs(jobs: &[&ScriptCheckJob]) -> Result<(), ConsensusError> {
    verify_script_job_refs(jobs)
}

fn job_needs_script_check(job: &ScriptCheckJob) -> bool {
    job.prevouts
        .iter()
        .any(|p| !is_anyone_can_spend(p.script_pubkey.as_script()))
}

/// Direct slice path (no intermediate `Vec<&_>`). Always rayon when non-empty.
fn verify_script_jobs(jobs: &[ScriptCheckJob]) -> Result<(), ConsensusError> {
    if jobs.is_empty() {
        return Ok(());
    }
    use rayon::prelude::*;
    jobs.par_iter().try_for_each(|job| {
        if job_needs_script_check(job) {
            verify_job_all_inputs(job)
        } else {
            Ok(())
        }
    })
}

fn verify_script_job_refs(jobs: &[&ScriptCheckJob]) -> Result<(), ConsensusError> {
    if jobs.is_empty() {
        return Ok(());
    }
    use rayon::prelude::*;
    jobs.par_iter().try_for_each(|job| {
        if job_needs_script_check(job) {
            verify_job_all_inputs(job)
        } else {
            Ok(())
        }
    })
}

fn verify_job_all_inputs(job: &ScriptCheckJob) -> Result<(), ConsensusError> {
    crate::script::verify_job_all_inputs(job)
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

/// BIP68 relative locks when `tx.version >= 2`.
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
    if tx.version.0 < 2 {
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
    coinbase_height_cache: &mut std::collections::HashMap<rbitcoin_primitives::Fk, Option<u32>>,
    batch_parents: &rbitcoin_query::BatchParents,
    // Height of the block being validated (same-block BIP68 coin height).
    spend_height: u32,
    // When false (optimistic confirm load): only prevout value/script — no
    // `tx_height` / coinbase body walks. BIP68 + maturity run in structural.
    resolve_create_heights: bool,
) -> Result<ResolvedPrevout, ConsensusError> {
    use rbitcoin_query::connect_prevout_stats;
    use std::sync::atomic::Ordering;

    let prev_txid = op.txid.to_byte_array();

    // Same-block spend of an earlier output in this block.
    // Clone script only for the spent vout (not every create in the block).
    if let Some(&ti) = same_block.get(&prev_txid) {
        let tx = block
            .txdata
            .get(ti)
            .ok_or(ConsensusError::MissingPrevout)?;
        let v = op.vout as usize;
        let o = tx.output.get(v).ok_or(ConsensusError::MissingPrevout)?;
        // Same-block: Core uses the spending block's height as the coin height.
        return Ok(ResolvedPrevout {
            txout: o.clone(),
            coinbase_height: None,
            create_height: if resolve_create_heights {
                spend_height
            } else {
                0
            },
        });
    }

    let cache = query.confirm_parent_cache();

    // Batch pin map first, then shared body cache. Wire prev_txid is
    // authoritative — reject wrong create_fk hits.
    if let Some(prev_fk) = prev_fk_hint {
        let hit = batch_parents
            .get_parent_out(prev_fk, op.vout)
            .or_else(|| cache.get_parent_out(prev_fk, op.vout));
        if let Some((prev_rec, out)) = hit {
            if prev_rec.txid == prev_txid {
                connect_prevout_stats::WAVE_HIT.fetch_add(1, Ordering::Relaxed);
                let (cb_h, create_height) = if resolve_create_heights {
                    let cb_h = coinbase_height_for_maturity(
                        query,
                        prev_fk,
                        &prev_rec,
                        batch_parents,
                        coinbase_height_cache,
                    )?;
                    (
                        cb_h,
                        create_height_for_fk(query, prev_fk, cb_h)?,
                    )
                } else {
                    (None, 0)
                };
                return Ok(ResolvedPrevout {
                    txout: TxOut {
                        value: Amount::from_sat(out.value as u64),
                        script_pubkey: ScriptBuf::from_bytes(out.script),
                    },
                    coinbase_height: cb_h,
                    create_height,
                });
            }
        }
    }

    // Cold path: create-fk candidates (thin → durable head / store).
    let head_fk = query
        .tx_fk_by_txid(&prev_txid)
        .map_err(ConsensusError::Store)?;
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
            (
                cb_h,
                create_height_for_fk(query, prev_fk, cb_h)?,
            )
        } else {
            (None, 0)
        };
        return Ok(ResolvedPrevout {
            txout: TxOut {
                value: Amount::from_sat(out.value as u64),
                script_pubkey: ScriptBuf::from_bytes(out.script),
            },
            coinbase_height: cb_h,
            create_height,
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
        .map_err(ConsensusError::Store)?
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
    coinbase_height_cache: &mut std::collections::HashMap<rbitcoin_primitives::Fk, Option<u32>>,
) -> Result<Option<u32>, ConsensusError> {
    let (is_cb, cb_h) =
        coinbase_info(query, prev_fk, prev_rec, batch_parents, coinbase_height_cache)?;
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
        .map_err(ConsensusError::Store)?)
}

/// `(is_coinbase, create_height if coinbase and confirmed)`.
fn coinbase_info(
    query: &Query,
    prev_fk: rbitcoin_primitives::Fk,
    prev_rec: &rbitcoin_store::TxRecord,
    batch_parents: &rbitcoin_query::BatchParents,
    cache: &mut std::collections::HashMap<rbitcoin_primitives::Fk, Option<u32>>,
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
    // Batch pin may stash coinbase height (no tx_height / input-run disk).
    if let Some(cached) = batch_parents.get_parent_coinbase_height(prev_fk) {
        cache.insert(prev_fk, cached);
        return Ok((cached.is_some(), cached));
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
            .map_err(ConsensusError::Store)?
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
        .map_err(ConsensusError::Store)?;
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
        .map_err(ConsensusError::Store)
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
    use bitcoin::{
        Amount, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness,
    };

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
        assert!(!sequence_locks_satisfied(
            &tx,
            &[100],
            &[0],
            109,
            0
        ));
        assert!(sequence_locks_satisfied(&tx, &[100], &[0], 110, 0));
    }

    #[test]
    fn bip68_disabled_by_version_1() {
        let tx = bare_tx(1, LockTime::ZERO, Sequence::from_consensus(10));
        assert!(sequence_locks_satisfied(&tx, &[100], &[0], 50, 0));
    }
}

#[cfg(test)]
mod structure_rule_tests {
    use super::{
        block_subsidy, merkle_root_bytes, validate_block_structure, bip34_height_script,
        ValidationContext,
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
        Amount, Block, BlockHash, CompactTarget, OutPoint, Sequence, Transaction, TxIn, TxOut,
        TxMerkleNode, Witness,
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
        assert!(b.weight().to_wu() > 4_000_000, "fixture weight {}", b.weight().to_wu());
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
        validate_block_structure(&b, &ctx_h(0)).expect("height 0 skips BIP34 height push rules we use");
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
    use bitcoin::{
        Amount, OutPoint, Sequence, Transaction, TxIn, TxOut, Txid, Witness,
    };

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
}


