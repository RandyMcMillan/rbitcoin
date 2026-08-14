//! BQ-ahead TipOnly parent resolve (lookup wave).
//!
//! One [`Store::get_fk_by_txid_batch`] (TipOnly) across a short ready-height
//! wave. Hits live on the BQ record. Does not claim, structure, or stamp.

use super::*;
use bitcoin::consensus::Decodable;
use rbitcoin_store::BqParentHits;
use std::collections::{HashMap, HashSet};
use std::io::Cursor;

/// Heights per TipOnly wave. Start conservative so load sees complete BQ
/// slices often instead of waiting out one huge machine.
///
/// Claim pack is **not** ~32 blocks. Claim stops at Σ `tx.input` **8000**
/// (typically **1–3** dense mainnet blocks) or hard 144 thin early blocks.
/// `32` was 8000/250 (mid-chain average) and must not be reused as pack size.
/// Eight heights is a few claim packs at fat-era density, not a `bq soft` dump.
pub const BQ_RESOLVE_WAVE_MAX_BLOCKS: usize = 8;
/// Safety cap so one megablock run cannot stall the wave (~8 × 8000 inputs).
pub const BQ_RESOLVE_WAVE_MAX_KEYS: usize = 64_000;

/// Outcome of one TipOnly wave over BQ-ready heights.
#[derive(Debug, Default, Clone, Copy)]
pub struct BqResolveWaveStats {
    pub heights: u32,
    pub keys: u32,
    pub hits: u32,
    pub work_ns: u64,
}

/// Collect unique external prev_txids (+ pre-BIP34 create txids) from a wire block.
fn collect_resolve_keys(params: &ChainParams, height: u32, block: &Block) -> Vec<[u8; 32]> {
    let mut same_block: HashSet<[u8; 32]> = HashSet::with_capacity(block.txdata.len());
    for tx in &block.txdata {
        same_block.insert(tx.compute_txid().to_byte_array());
    }
    let mut need: Vec<[u8; 32]> = Vec::new();
    for tx in &block.txdata {
        for inp in &tx.input {
            if inp.previous_output.is_null() {
                continue;
            }
            let prev = inp.previous_output.txid.to_byte_array();
            if prev == [0u8; 32] || same_block.contains(&prev) {
                continue;
            }
            need.push(prev);
        }
        if !params.bip34_active_at(height) {
            need.push(tx.compute_txid().to_byte_array());
        }
    }
    need.sort_unstable();
    need.dedup();
    need
}

fn decode_bq_block(payload: &[u8]) -> Option<Block> {
    let mut cur = Cursor::new(payload);
    Block::consensus_decode(&mut cur).ok()
}

/// TipOnly-resolve external parents for `heights` still on the BQ.
///
/// Skips missing / already-complete / undecodable heights. Marks each
/// processed height resolve-complete even when some keys miss (same-batch /
/// in-flight remainder is load's job). Connected-only (fence) resolve.
pub fn confirm_bq_resolve_wave(
    query: &Query,
    params: &ChainParams,
    heights: &[u32],
) -> Result<BqResolveWaveStats, ConsensusError> {
    let t0 = Instant::now();
    let mut stats = BqResolveWaveStats::default();
    let mut per_height: Vec<(u32, Vec<[u8; 32]>)> = Vec::new();
    let mut all_keys: HashSet<[u8; 32]> = HashSet::new();

    for &h in heights {
        if query.block_queue_is_resolve_complete(h) {
            continue;
        }
        let Some(payload) = query.block_queue_payload(h).map_err(ConsensusError::from)? else {
            continue;
        };
        let Some(block) = decode_bq_block(&payload) else {
            continue;
        };
        let need = collect_resolve_keys(params, h, &block);
        if all_keys.len().saturating_add(need.len()) > BQ_RESOLVE_WAVE_MAX_KEYS
            && !per_height.is_empty()
        {
            break;
        }
        for k in &need {
            all_keys.insert(*k);
        }
        per_height.push((h, need));
        if per_height.len() >= BQ_RESOLVE_WAVE_MAX_BLOCKS {
            break;
        }
    }

    let mut keys: Vec<[u8; 32]> = all_keys.into_iter().collect();
    stats.keys = keys.len() as u32;
    keys.sort_unstable_by_key(|txid| query.store().txs.head_primary_slot(txid));

    let mut hit_map: BqParentHits = HashMap::new();
    if !keys.is_empty() {
        let rows = query
            .store()
            .get_fk_by_txid_batch(&keys)
            .map_err(ConsensusError::from)?;
        for (txid, row) in rows {
            if let Some((fk, range)) = row {
                hit_map.insert(txid, (fk, range));
            }
        }
    }
    stats.hits = hit_map.len() as u32;

    for (h, need) in &per_height {
        let attach: Vec<([u8; 32], rbitcoin_primitives::Fk, (u64, u64))> = need
            .iter()
            .filter_map(|t| hit_map.get(t).map(|(fk, range)| (*t, *fk, *range)))
            .collect();
        query
            .block_queue_attach_parent_hits(*h, attach)
            .map_err(ConsensusError::from)?;
        query
            .block_queue_mark_resolve_complete(*h)
            .map_err(ConsensusError::from)?;
        stats.heights = stats.heights.saturating_add(1);
    }
    stats.work_ns = t0.elapsed().as_nanos() as u64;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accept_and_connect_block;
    use crate::regtest_pad::mine_empty_regtest;
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::consensus::encode::serialize;
    use bitcoin::hashes::Hash;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{
        Amount, BlockHash, CompactTarget, OutPoint, ScriptBuf, Sequence, Target, Transaction, TxIn,
        TxMerkleNode, TxOut, Txid, Witness,
    };
    use std::sync::Once;

    fn head_tiny() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
    }

    fn tmp_query() -> (std::path::PathBuf, Query) {
        head_tiny();
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-bq-resolve-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let q = Query::open_or_create(&path).unwrap();
        (path, q)
    }

    fn spend_op_true(prev: Txid, vout: u32, value: Amount) -> Transaction {
        Transaction {
            version: TxVersion::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint { txid: prev, vout },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value,
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        }
    }

    fn coinbase_tx(height: u32) -> Transaction {
        let mut ss = if height == 0 {
            vec![0x00]
        } else {
            crate::bip34_height_script(height)
        };
        while ss.len() < 2 {
            ss.push(0x00);
        }
        Transaction {
            version: TxVersion::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(ss),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_0000_0000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        }
    }

    fn mine_with_txs(prev: BlockHash, time: u32, height: u32, extra: Vec<Transaction>) -> Block {
        let bits = CompactTarget::from_consensus(0x207f_ffff);
        let header = Header {
            version: Version::ONE,
            prev_blockhash: prev,
            merkle_root: TxMerkleNode::from_byte_array([0u8; 32]),
            time,
            bits,
            nonce: 0,
        };
        let mut txdata = vec![coinbase_tx(height)];
        txdata.extend(extra);
        let mut block = Block { header, txdata };
        block.header.merkle_root = block.compute_merkle_root().unwrap();
        let target = Target::from_compact(bits);
        for nonce in 0..u32::MAX {
            block.header.nonce = nonce;
            if block.header.validate_pow(target).is_ok() {
                break;
            }
        }
        block
    }

    #[test]
    fn bq_resolve_wave_source_is_tiponly_batch_only() {
        let src = include_str!("bq_resolve.rs");
        let prod = src.split("#[cfg(test)]").next().unwrap_or(src);
        assert!(
            prod.contains("get_fk_by_txid_batch(&keys)"),
            "wave must use TipOnly 2-wave batch API (not a separate full-depth probe)"
        );
        assert!(
            !prod.contains("TxidResolveMode::TipThenAny"),
            "confirm wave must not pass TipThenAny"
        );
        assert!(
            !prod.contains("get_fk_by_txid_batch_mode"),
            "do not pick an explicit TipThenAny mode"
        );
    }

    #[test]
    fn bq_resolve_wave_attaches_tiponly_hits_multi_height() {
        let (path, q) = tmp_query();
        let params = ChainParams::regtest();
        let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, Milestone::NONE).unwrap();
        let g_cb = genesis.txdata[0].compute_txid();
        let b1 = mine_with_txs(
            genesis.block_hash(),
            genesis.header.time + 600,
            1,
            vec![spend_op_true(g_cb, 0, Amount::from_sat(49_0000_0000))],
        );
        let b2 = mine_with_txs(
            b1.block_hash(),
            b1.header.time + 600,
            2,
            vec![spend_op_true(g_cb, 0, Amount::from_sat(48_0000_0000))],
        );
        q.block_queue_enqueue(1, b1.block_hash().to_byte_array(), 1, &serialize(&b1))
            .unwrap();
        q.block_queue_enqueue(2, b2.block_hash().to_byte_array(), 2, &serialize(&b2))
            .unwrap();

        let st = confirm_bq_resolve_wave(&q, &params, &[1, 2]).unwrap();
        assert_eq!(st.heights, 2);
        assert!(st.keys >= 1);
        assert!(st.hits >= 1);
        let hits1 = q.block_queue_parent_hits(1).expect("h1");
        let hits2 = q.block_queue_parent_hits(2).expect("h2");
        assert!(
            hits1.contains_key(&g_cb.to_byte_array()),
            "genesis coinbase must be a TipOnly hit"
        );
        assert!(hits2.contains_key(&g_cb.to_byte_array()));
        assert!(q.block_queue_is_resolve_complete(1));
        assert!(q.block_queue_is_resolve_complete(2));
        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn bq_resolve_wave_skips_claimed_height() {
        let (path, q) = tmp_query();
        let params = ChainParams::regtest();
        let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, Milestone::NONE).unwrap();
        let g_cb = genesis.txdata[0].compute_txid();
        let b1 = mine_with_txs(
            genesis.block_hash(),
            genesis.header.time + 600,
            1,
            vec![spend_op_true(g_cb, 0, Amount::from_sat(49_0000_0000))],
        );
        let b2 = mine_with_txs(
            b1.block_hash(),
            b1.header.time + 600,
            2,
            vec![spend_op_true(g_cb, 0, Amount::from_sat(48_0000_0000))],
        );
        q.block_queue_enqueue(1, b1.block_hash().to_byte_array(), 1, &serialize(&b1))
            .unwrap();
        q.block_queue_enqueue(2, b2.block_hash().to_byte_array(), 2, &serialize(&b2))
            .unwrap();
        // Caller skipped height 2 (claimed / inflight) — only resolve 1.
        let st = confirm_bq_resolve_wave(&q, &params, &[1]).unwrap();
        assert_eq!(st.heights, 1);
        assert!(q.block_queue_is_resolve_complete(1));
        assert!(!q.block_queue_is_resolve_complete(2));
        assert!(q.block_queue_parent_hits(2).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn bq_resolve_wave_tiponly_after_disconnect() {
        let (path, q) = tmp_query();
        let params = ChainParams::regtest();
        let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, Milestone::NONE).unwrap();
        let b1 = mine_empty_regtest(genesis.block_hash(), genesis.header.time + 600, 1);
        accept_and_connect_block(&q, &params, Height(1), &b1, Milestone::NONE).unwrap();
        let cb1 = b1.txdata[0].compute_txid();
        let child = mine_with_txs(
            b1.block_hash(),
            b1.header.time + 600,
            2,
            vec![spend_op_true(cb1, 0, Amount::from_sat(49_0000_0000))],
        );
        q.block_queue_enqueue(2, child.block_hash().to_byte_array(), 2, &serialize(&child))
            .unwrap();

        q.disconnect_tip().unwrap();
        assert_eq!(q.tip_height().map(|h| h.0), Some(0));

        let st = confirm_bq_resolve_wave(&q, &params, &[2]).unwrap();
        assert_eq!(st.heights, 1);
        let hits = q.block_queue_parent_hits(2).expect("child still queued");
        assert!(
            !hits.contains_key(&cb1.to_byte_array()),
            "abandoned-fork coinbase must not be a TipOnly hit (TipThenAny would attach it)"
        );
        assert!(q.block_queue_is_resolve_complete(2));
        let _ = std::fs::remove_dir_all(&path);
    }

    /// Head occupied may already cover the parent fk; prune until the parent
    /// height is confirmed so stamp does not MissingPrevout (931147 / 933474).
    #[test]
    fn stamp_uses_inflight_until_tip_covers_parent_height() {
        use rbitcoin_query::{InFlightLayer, InFlightLog};
        use rbitcoin_store::{OutputRecord, TxRecord};
        let (path, q) = tmp_query();
        let params = ChainParams::regtest();
        let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, Milestone::NONE).unwrap();
        let parent_txid = [0x22u8; 32];
        let parent_fk = rbitcoin_primitives::Fk(99);
        let pin = std::sync::Arc::new((
            TxRecord {
                txid: parent_txid,
                version: 1,
                locktime: 0,
                input_start_fk: rbitcoin_primitives::Fk::NULL,
                input_count: 1,
                output_start_fk: rbitcoin_primitives::Fk::NULL,
                output_count: 1,
            },
            vec![OutputRecord::unspent(1, vec![0x51])],
        ));
        let mut log = InFlightLog::new();
        log.note_layer(InFlightLayer::from_plan_pins([(parent_fk, &pin)]).with_max_height(1));
        // Production prune_committed: tip still genesis; occupied already 99.
        log.prune_through_tip(Some(0));
        let view = log.snapshot();
        assert!(
            view.get_create_fk(&parent_txid).is_some(),
            "in-flight must survive drain while parent height is unconfirmed"
        );
        let b1 = mine_with_txs(
            genesis.block_hash(),
            genesis.header.time + 600,
            1,
            vec![spend_op_true(
                Txid::from_byte_array(parent_txid),
                0,
                Amount::from_sat(49_0000_0000),
            )],
        );
        let pipe = crate::WireLoadPipeline {
            path_lo: 1,
            parent_hash: None,
            next_tx_start: q.tx_body_count().saturating_add(1).max(1),
            in_flight: view,
            parent_store: std::sync::Arc::new(rbitcoin_query::PipelineParentStore::new()),
        };
        let empty = rbitcoin_store::BqParentHits::default();
        let items = [(Height(1), std::sync::Arc::new(b1))];
        let stamped = crate::confirm_wire_lookup_stamp_with_hits(
            &q,
            &params,
            Milestone::NONE,
            &items,
            Some(&pipe),
            Some(&empty),
        )
        .expect("in-flight parent must stamp until tip covers the parent height");
        let plan = stamped.plan.expect("plan");
        let spend = plan
            .packed
            .iter()
            .find(|(_, ins)| ins.iter().any(|i| !i.is_coinbase()))
            .expect("spend");
        let inp = spend.1.iter().find(|i| !i.is_coinbase()).expect("in");
        assert_eq!(inp.create_fk, parent_fk);
        log.prune_through_tip(Some(1));
        assert!(
            log.snapshot().get_create_fk(&parent_txid).is_none(),
            "confirmed height is leftover TipOnly's job"
        );
        let _ = std::fs::remove_dir_all(&path);
    }

    /// Wave may miss a parent that is already connected in `tx.head`.
    /// Load stamp must TipOnly-head the leftover — not Corrupt-as-invariant.
    #[test]
    fn load_stamp_leftover_parent_via_tiponly_head() {
        let (path, q) = tmp_query();
        let params = ChainParams::regtest();
        let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, Milestone::NONE).unwrap();
        let g_cb = genesis.txdata[0].compute_txid();
        let expect_fk = q
            .store()
            .get_fk_by_txid_tip(&g_cb.to_byte_array())
            .unwrap()
            .expect("genesis coinbase is connected");
        let b1 = mine_with_txs(
            genesis.block_hash(),
            genesis.header.time + 600,
            1,
            vec![spend_op_true(g_cb, 0, Amount::from_sat(49_0000_0000))],
        );
        let empty = rbitcoin_store::BqParentHits::default();
        let items = [(Height(1), std::sync::Arc::new(b1))];
        let stamped = crate::confirm_wire_lookup_stamp_with_hits(
            &q,
            &params,
            Milestone::NONE,
            &items,
            None,
            Some(&empty),
        )
        .expect("leftover connected parent must TipOnly-head, not invariant");
        let plan = stamped.plan.expect("new body needs a plan");
        let spend = plan
            .packed
            .iter()
            .find(|(_, ins)| ins.iter().any(|i| !i.is_coinbase()))
            .expect("spend tx");
        let inp = spend
            .1
            .iter()
            .find(|i| !i.is_coinbase())
            .expect("spend input");
        assert_eq!(inp.prev_txid, g_cb.to_byte_array());
        assert_eq!(inp.create_fk, expect_fk);
        let _ = std::fs::remove_dir_all(&path);
    }

    /// Leftover TipOnly must not resurrect an abandoned (disconnected) Class A row.
    #[test]
    fn load_leftover_disconnected_parent_is_not_tipthenany() {
        use rbitcoin_query::TxApply;
        use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
        let (path, q) = tmp_query();
        let params = ChainParams::regtest();
        let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, Milestone::NONE).unwrap();
        let b1 = mine_empty_regtest(genesis.block_hash(), genesis.header.time + 600, 1);
        accept_and_connect_block(&q, &params, Height(1), &b1, Milestone::NONE).unwrap();
        let cb1 = b1.txdata[0].compute_txid().to_byte_array();
        q.disconnect_tip().unwrap();
        let _ = params;
        let child = TxApply {
            tx: TxRecord {
                txid: [0x22; 32],
                version: 1,
                locktime: 0,
                input_start_fk: rbitcoin_primitives::Fk::NULL,
                input_count: 1,
                output_start_fk: rbitcoin_primitives::Fk::NULL,
                output_count: 1,
            },
            inputs: vec![InputRecord {
                prev_txid: cb1,
                create_fk: rbitcoin_primitives::Fk::NULL,
                prev_index: 0,
                sequence: u32::MAX,
                script_sig: vec![],
                witness: vec![],
            }],
            outputs: vec![OutputRecord::unspent(1, vec![0x51])],
        };
        let mut need = vec![(rbitcoin_primitives::Fk(1), vec![child])];
        let err = q
            .archive_plan_batch_from_store(
                &mut need,
                1,
                &rbitcoin_query::InFlightView::empty(),
                None,
                Some(&rbitcoin_store::BqParentHits::default()),
            )
            .expect_err("disconnected leftover must not TipThenAny-fill");
        let msg = err.to_string();
        assert!(msg.contains("parent create_fk unresolved"), "got: {msg}");
        assert!(
            !msg.contains("invariant: external parent missing BQ TipOnly hit"),
            "leftover miss is unresolved, not the old forbid-head invariant: {msg}"
        );
        let last = rbitcoin_query::archive_phase_stats::last_plan_batch();
        assert!(
            last.head_need > 0,
            "fail pack leftover_n must be metered before stamp: {last:?}"
        );
        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn bq_wave_then_stamp_confirms_empty_block() {
        let (path, q) = tmp_query();
        let params = ChainParams::regtest();
        let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, Milestone::NONE).unwrap();
        let b1 = mine_empty_regtest(genesis.block_hash(), genesis.header.time + 600, 1);
        q.block_queue_enqueue(1, b1.block_hash().to_byte_array(), 1, &serialize(&b1))
            .unwrap();
        confirm_bq_resolve_wave(&q, &params, &[1]).unwrap();
        assert!(q.block_queue_is_resolve_complete(1));
        let hits = q.block_queue_parent_hits(1).unwrap();
        let items = [(Height(1), std::sync::Arc::new(b1))];
        let stamped = crate::confirm_wire_lookup_stamp_with_hits(
            &q,
            &params,
            Milestone::NONE,
            &items,
            None,
            Some(&hits),
        )
        .expect("coinbase-only block needs no external head");
        let mat = crate::confirm_wire_load_from_plan(
            &q,
            &params,
            Milestone::NONE,
            stamped,
            None,
            &ScriptPreverified::new(),
        )
        .expect("load");
        let ok = crate::confirm_scripts_phase(mat.batch).expect("scripts");
        crate::confirm_write_phase(&q, &params, Milestone::NONE, ok.batch).expect("write");
        assert_eq!(q.tip_height().map(|h| h.0), Some(1));
        let _ = std::fs::remove_dir_all(&path);
    }
}
