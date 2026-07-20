use crate::error::ConsensusError;
use crate::milestone::Milestone;
use crate::params::ChainParams;
use bitcoin::absolute::LockTime;
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
/// Always: prevouts, double-spend (point+strong), maturity, fees/subsidy, same-block map.
/// Connect + optional script/sig checks.
///
/// Split into:
/// 1. **Connect** — sequential prevouts / spentness / maturity / fees (store-bound;
///    parallelizing these only contended on table `Mutex`es).
/// 2. **Scripts** — above milestone, each non-coinbase tx is checked with its
///    resolved prevouts on a small worker pool (CPU-bound, no store).
///
/// Class C tip updates (`confirm_block`) stay outside this function.
///
/// `archived_tx_fks`: Class A fks for `block.txdata` (same order) when confirming
/// archived bodies (wave thin create_fk / Class A rows).
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
    // Pending until connect+scripts succeed — do not apply UTXO / points on failure.
    let mut pending = std::collections::HashSet::new();
    let mut pending_creates = std::collections::HashMap::new();
    let (script_jobs, spends, _creates) = connect_block_prevouts(
        query,
        block,
        ctx,
        archived_tx_fks,
        None,
        &mut pending,
        &mut pending_creates,
    )?;
    if check_scripts && !script_jobs.is_empty() {
        verify_scripts_pool(&script_jobs)?;
    }
    // Catch-up UTXO is applied only after Class C succeeds (see confirm path).
    let _ = spends;
    Ok(())
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
    /// Index into `block.txdata` (debug / future per-tx hooks).
    #[allow(dead_code)]
    pub(crate) tx_index: usize,
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
}



/// Sequential connect: resolve prevouts and fee checks; build script jobs.
///
/// Spent outpoints go into `pending_spent` (run-local only) so a failed
/// script check can drop them without poisoning the next attempt.
///
/// `pending_creates` maps outpoints created earlier in this confirm run →
/// create Class A fk. Required for mmap UTXO spentness + parent resolve until
/// Class C applies UTXO.
pub(crate) fn connect_block_prevouts(
    query: &Query,
    block: &Block,
    ctx: &ValidationContext<'_>,
    archived_tx_fks: Option<&[rbitcoin_primitives::Fk]>,
    wave_prevouts: Option<&rbitcoin_query::WavePrevoutCache>,
    pending_spent: &mut std::collections::HashSet<([u8; 32], u32)>,
    pending_creates: &mut std::collections::HashMap<([u8; 32], u32), rbitcoin_primitives::Fk>,
) -> Result<
    (
        Vec<ScriptCheckJob>,
        Vec<([u8; 32], u32)>,
        // Creates for light UTXO apply: (txid, vout, create_fk).
        Vec<([u8; 32], u32, rbitcoin_primitives::Fk)>,
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
    let mut same_block: std::collections::HashMap<[u8; 32], Vec<TxOut>> =
        std::collections::HashMap::with_capacity(n_tx);
    let mut fees = 0i64;
    // Skip job materialization (tx clone + prevout retention) when scripts are
    // skipped — pure waste below the milestone.
    let build_script_jobs = !ctx.milestone.skips_scripts_at(ctx.height.0);
    let mut script_jobs: Vec<ScriptCheckJob> = if build_script_jobs {
        Vec::with_capacity(n_tx.saturating_sub(1))
    } else {
        Vec::new()
    };
    let mut spends: Vec<([u8; 32], u32)> = Vec::with_capacity(n_tx.saturating_mul(2));
    let mut creates: Vec<([u8; 32], u32, rbitcoin_primitives::Fk)> =
        Vec::with_capacity(n_tx.saturating_mul(2));
    // Coinbase height cache spans the whole block (was recreated per tx).
    let mut coinbase_height_cache: std::collections::HashMap<
        rbitcoin_primitives::Fk,
        Option<u32>,
    > = std::collections::HashMap::with_capacity(64);

    // Spent checks:
    // - pending_spent: this confirm run (ephemeral)
    // - catch-up: light UTXO via catchup_is_spent
    // - tip: durable has_confirmed_strong_spender (source of truth; no tip-live
    //   short-circuit — that needed spent_local as a safety net)
    let spend_index_on = query.spend_index_enabled();

    for (ti, tx) in block.txdata.iter().enumerate() {
        let spend_fk = archived_tx_fks.map(|fks| fks[ti]);
        // Wave-local tx row first (no disk/class_a); else Class A.
        let archived_rec = if let Some(fk) = spend_fk {
            if let Some(w) = wave_prevouts {
                if let Some(rec) = w.get_tx(fk) {
                    Some(rec.clone())
                } else {
                    Some(query.get_tx_class_a(fk).map_err(ConsensusError::Store)?)
                }
            } else {
                Some(query.get_tx_class_a(fk).map_err(ConsensusError::Store)?)
            }
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
            // Only retain prevouts when scripts will run; otherwise drop after fee add.
            let mut prevouts: Vec<TxOut> = if build_script_jobs {
                Vec::with_capacity(tx.input.len())
            } else {
                Vec::new()
            };
            // Thin create_fk hints from wave (no full input-run clone).
            let thin = spend_fk.and_then(|fk| {
                wave_prevouts.and_then(|w| w.thin_inputs(fk).map(|s| s.to_vec()))
            });

            for (ii, input) in tx.input.iter().enumerate() {
                let op = input.previous_output;
                if !block_spends.insert(op) {
                    return Err(ConsensusError::BadTx("double spend in block"));
                }
                let key = (op.txid.to_byte_array(), op.vout);
                if pending_spent.contains(&key) {
                    return Err(ConsensusError::PrevoutSpent);
                }
                // Wave already filtered spent outs at fill time (same UTXO tip).
                // Skip a second light-UTXO probe on the ~98% wave-hit path.
                let wave_live = wave_prevouts
                    .and_then(|w| w.get_by_txid(op.txid.as_byte_array(), op.vout))
                    .is_some();
                if spend_index_on {
                    // Tip mode: durable confirmed-strong points only.
                    if query
                        .store()
                        .has_confirmed_strong_spender(op.txid.as_byte_array(), op.vout)
                        .map_err(ConsensusError::Store)?
                    {
                        return Err(ConsensusError::PrevoutSpent);
                    }
                } else if !wave_live
                    && !pending_creates.contains_key(&key)
                    && query
                        .catchup_is_spent(op.txid.as_byte_array(), op.vout)
                        .map_err(ConsensusError::Store)?
                {
                    // Catch-up cold path only: light UTXO miss (not same-run create).
                    return Err(ConsensusError::PrevoutSpent);
                }
                let prev_fk = thin
                    .as_ref()
                    .and_then(|t| t.get(ii))
                    .and_then(|e| e.create_fk.map(rbitcoin_primitives::Fk))
                    .or_else(|| pending_creates.get(&key).copied())
                    .or_else(|| {
                        // Only probe UTXO for create_fk when wave/thin missed.
                        if wave_live {
                            None
                        } else {
                            query
                                .ibd_utxo_create_fk(op.txid.as_byte_array(), op.vout)
                                .ok()
                                .flatten()
                        }
                    });
                let prev_out = resolve_prevout(
                    query,
                    op,
                    prev_fk,
                    &same_block,
                    wave_prevouts,
                    &mut coinbase_height_cache,
                )?;
                if let Some(created) = prev_out.coinbase_height {
                    let maturity = ctx.params.coinbase_maturity();
                    if ctx.height.0 < created.saturating_add(maturity) {
                        return Err(ConsensusError::BadTx("coinbase immature"));
                    }
                }
                pending_spent.insert(key);
                spends.push(key);
                value_in = value_in
                    .checked_add(prev_out.txout.value.to_sat() as i64)
                    .ok_or(ConsensusError::BadTx("value in overflow"))?;
                if build_script_jobs {
                    prevouts.push(prev_out.txout);
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
                    tx_index: ti,
                    prevouts,
                    // One deep clone beats encode-at-connect + deserialize-per-worker.
                    tx: tx.clone(),
                    bip65_active: ctx.params.bip65_active_at(ctx.height.0),
                    bip112_active: ctx.params.csv_active_at(ctx.height.0),
                    bip66_active: ctx.params.bip66_active_at(ctx.height.0),
                    bip16_active: bip16_for_jobs,
                });
            }
        }

        let txid = if let Some(ref rec) = archived_rec {
            rec.txid
        } else {
            tx.compute_txid().to_byte_array()
        };
        let create_fk = spend_fk.unwrap_or(rbitcoin_primitives::Fk::NULL);
        let mut outs = Vec::with_capacity(tx.output.len());
        for (v, o) in tx.output.iter().enumerate() {
            outs.push(TxOut {
                value: o.value,
                script_pubkey: o.script_pubkey.clone(),
            });
            if !create_fk.is_null() {
                pending_creates.insert((txid, v as u32), create_fk);
                creates.push((txid, v as u32, create_fk));
            }
        }
        same_block.insert(txid, outs);
    }

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

    let _ = LockTime::ZERO;
    Ok((script_jobs, spends, creates))
}

/// Parallel script checks for an owned job slice (preferred entry — no ref `Vec`).
pub fn verify_scripts_pool(jobs: &[ScriptCheckJob]) -> Result<(), ConsensusError> {
    verify_script_jobs(jobs)
}

/// Parallel script/sig checks across jobs (possibly from multiple blocks).
///
/// Uses the **rayon global pool** for multi-tx waves. One job = one non-coinbase
/// tx (shared [`bitcoin::sighash::SighashCache`] across its inputs).
///
/// **Why rayon (not a custom tokio queue):** script verify is CPU-bound and
/// runs on the confirm OS thread outside the async runtime; rayon’s work-stealing
/// pool is built for that. A tokio blocking queue would add channel/join overhead
/// without better load balance for ~30–50 µs crypto jobs. Wire rebuild stays
/// sequential (see `confirm_run` + `rayon_audit`).
///
/// Single-job waves skip the pool. IBD always has many scripts per batch, so
/// multi-job is the hot path.
pub fn verify_scripts_pool_jobs(jobs: &[&ScriptCheckJob]) -> Result<(), ConsensusError> {
    verify_script_job_refs(jobs)
}

fn job_needs_script_check(job: &ScriptCheckJob) -> bool {
    job.prevouts
        .iter()
        .any(|p| !is_anyone_can_spend(p.script_pubkey.as_script()))
}

/// Direct slice path (no intermediate `Vec<&_>`).
fn verify_script_jobs(jobs: &[ScriptCheckJob]) -> Result<(), ConsensusError> {
    match jobs.len() {
        0 => Ok(()),
        1 => {
            if job_needs_script_check(&jobs[0]) {
                verify_job_all_inputs(&jobs[0])
            } else {
                Ok(())
            }
        }
        _ => {
            use rayon::prelude::*;
            // Fine-grained: one stealable unit per tx. Filter inside the task so
            // we do not allocate a second pointer vector on the hot path.
            jobs.par_iter().try_for_each(|job| {
                if job_needs_script_check(job) {
                    verify_job_all_inputs(job)
                } else {
                    Ok(())
                }
            })
        }
    }
}

fn verify_script_job_refs(jobs: &[&ScriptCheckJob]) -> Result<(), ConsensusError> {
    match jobs.len() {
        0 => Ok(()),
        1 => {
            if job_needs_script_check(jobs[0]) {
                verify_job_all_inputs(jobs[0])
            } else {
                Ok(())
            }
        }
        _ => {
            use rayon::prelude::*;
            jobs.par_iter().try_for_each(|job| {
                if job_needs_script_check(job) {
                    verify_job_all_inputs(job)
                } else {
                    Ok(())
                }
            })
        }
    }
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
}

fn resolve_prevout(
    query: &Query,
    op: OutPoint,
    // Prefer thin create_fk from wave (avoids full InputRecord).
    prev_fk_hint: Option<rbitcoin_primitives::Fk>,
    same_block: &std::collections::HashMap<[u8; 32], Vec<TxOut>>,
    wave_prevouts: Option<&rbitcoin_query::WavePrevoutCache>,
    coinbase_height_cache: &mut std::collections::HashMap<rbitcoin_primitives::Fk, Option<u32>>,
) -> Result<ResolvedPrevout, ConsensusError> {
    use rbitcoin_query::connect_prevout_stats;
    use std::sync::atomic::Ordering;

    let prev_txid = op.txid.to_byte_array();

    // Same-block spend of an earlier output in this block.
    if let Some(outs) = same_block.get(&prev_txid) {
        let v = op.vout as usize;
        if v >= outs.len() {
            return Err(ConsensusError::MissingPrevout);
        }
        return Ok(ResolvedPrevout {
            txout: outs[v].clone(),
            coinbase_height: None,
        });
    }

    let prev_fk = prev_fk_hint;

    // Wave-local map first (no mutex; built during parent prefetch).
    //
    // **Wire `prev_txid` is authoritative.** Prefer by-txid; only accept an fk
    // hit when the cached parent's txid matches. Otherwise a wrong create_fk
    // hint can hit another wave entry (every wave-body create is a live parent)
    // and feed the wrong scriptPubKey into script checks.
    if let Some(wave) = wave_prevouts {
        let wave_hit = wave.get_by_txid(&prev_txid, op.vout).or_else(|| {
            prev_fk.and_then(|fk| {
                wave.get_by_fk(fk, op.vout)
                    .filter(|(_, rec, _)| rec.txid == prev_txid)
            })
        });
        if let Some((prev_fk, prev_rec, out)) = wave_hit {
            connect_prevout_stats::WAVE_HIT.fetch_add(1, Ordering::Relaxed);
            let cb_h =
                coinbase_height_for_maturity(query, prev_fk, prev_rec, wave_prevouts, coinbase_height_cache)?;
            // Found a live parent out with matching wire txid — never MissingPrevout.
            return Ok(ResolvedPrevout {
                txout: TxOut {
                    value: Amount::from_sat(out.value as u64),
                    script_pubkey: ScriptBuf::from_bytes(out.script.clone()),
                },
                coinbase_height: cb_h,
            });
        }
    }

    // Cold path: create-fk candidates (thin → UTXO → durable head / store).
    // Tip-follow without a wave (or wave miss) uses this path.
    // Wrong thin hints must not block UTXO / head after a txid mismatch.
    let utxo_fk = query
        .ibd_utxo_create_fk(&prev_txid, op.vout)
        .map_err(ConsensusError::Store)?;
    let head_fk = query
        .tx_fk_by_txid(&prev_txid)
        .map_err(ConsensusError::Store)?;
    let candidates = [prev_fk, utxo_fk, head_fk];
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
        let cb_h = coinbase_height_for_maturity(
            query,
            prev_fk,
            &prev_rec,
            wave_prevouts,
            coinbase_height_cache,
        )?;
        return Ok(ResolvedPrevout {
            txout: TxOut {
                value: Amount::from_sat(out.value as u64),
                script_pubkey: ScriptBuf::from_bytes(out.script),
            },
            coinbase_height: cb_h,
        });
    }

    Err(ConsensusError::MissingPrevout)
}

/// Coinbase create height for maturity, or `None` if not a coinbase / unknown.
///
/// Unlike the old `!is_cb || cb_h.is_some()` gate, a missing height never
/// discards an already-located parent output (that became MissingPrevout).
fn coinbase_height_for_maturity(
    query: &Query,
    prev_fk: rbitcoin_primitives::Fk,
    prev_rec: &rbitcoin_store::TxRecord,
    wave_prevouts: Option<&rbitcoin_query::WavePrevoutCache>,
    coinbase_height_cache: &mut std::collections::HashMap<rbitcoin_primitives::Fk, Option<u32>>,
) -> Result<Option<u32>, ConsensusError> {
    let (is_cb, cb_h) =
        coinbase_info(query, prev_fk, prev_rec, wave_prevouts, coinbase_height_cache)?;
    if !is_cb {
        return Ok(None);
    }
    if cb_h.is_some() {
        return Ok(cb_h);
    }
    // Last resort: durable tx_height (wave may have been filled pre-Class-C).
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
    wave_prevouts: Option<&rbitcoin_query::WavePrevoutCache>,
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
    // Wave-prefetched coinbase height (no tx_height / input-run disk).
    if let Some(wave) = wave_prevouts {
        if let Some(cached) = wave.coinbase_height_fk(prev_fk) {
            cache.insert(prev_fk, cached);
            return Ok((cached.is_some(), cached));
        }
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
        let script_sig = if height == 0 {
            ScriptBuf::from_bytes(vec![0x00])
        } else {
            ScriptBuf::from_bytes(bip34_height_script(height))
        };
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
        cb.input[0].script_sig = ScriptBuf::from_bytes(vec![0x00]);
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
}


