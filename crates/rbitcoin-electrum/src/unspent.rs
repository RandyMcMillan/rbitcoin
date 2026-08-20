//! Shared Electrum / Esplora scripthash UTXO + mempool overlay.

use bitcoin::hashes::Hash;
use bitcoin::OutPoint;
use rbitcoin_net::MempoolHub;
use rbitcoin_primitives::Fk;
use rbitcoin_query::{Query, QueryError, ScriptHashUtxo, ShJoinSlot};
use rbitcoin_store::script_hash;

/// Confirmed SH UTXOs plus mempool funding, minus mempool spends (Electrum rules).
pub fn scripthash_utxos_with_mempool(
    query: &Query,
    mempool: Option<&MempoolHub>,
    sh: &[u8; 32],
) -> Result<Vec<ScriptHashUtxo>, QueryError> {
    let mut slot = None;
    scripthash_utxos_with_mempool_slot(query, mempool, sh, &mut slot)
}

/// [`scripthash_utxos_with_mempool`] using a connection-local join slot.
pub(crate) fn scripthash_utxos_with_mempool_slot(
    query: &Query,
    mempool: Option<&MempoolHub>,
    sh: &[u8; 32],
    slot: &mut Option<ShJoinSlot>,
) -> Result<Vec<ScriptHashUtxo>, QueryError> {
    let mut out = query.scripthash_listunspent_slot(sh, slot)?;
    let Some(mp) = mempool else {
        return Ok(out);
    };
    out.retain(|x| {
        let op = OutPoint {
            txid: bitcoin::Txid::from_byte_array(x.tx_hash),
            vout: x.tx_pos,
        };
        !mp.spends_outpoint(&op)
    });
    for item in mp.scripthash_mempool(sh) {
        let tid = bitcoin::Txid::from_byte_array(item.txid);
        let Some(tx) = mp.get_tx(&tid) else {
            continue;
        };
        for (vout, o) in tx.output.iter().enumerate() {
            if script_hash(o.script_pubkey.as_bytes()) != *sh {
                continue;
            }
            let op = OutPoint {
                txid: tid,
                vout: vout as u32,
            };
            if mp.spends_outpoint(&op) {
                continue;
            }
            out.push(ScriptHashUtxo {
                tx_hash: item.txid,
                tx_pos: vout as u32,
                height: 0,
                value: o.value.to_sat() as i64,
                create_tx_fk: Fk::NULL,
            });
        }
    }
    Ok(out)
}

/// Esplora `mempool_stats` for a scripthash (same funding/spend loops as listunspent).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MempoolShStats {
    pub tx_count: u32,
    pub funded_txo_count: u32,
    pub funded_txo_sum: i64,
    pub spent_txo_count: u32,
    pub spent_txo_sum: i64,
}

pub fn scripthash_mempool_stats(
    query: &Query,
    mp: &MempoolHub,
    sh: &[u8; 32],
) -> Result<MempoolShStats, QueryError> {
    let items = mp.scripthash_mempool(sh);
    let mut stats = MempoolShStats {
        tx_count: items.len() as u32,
        ..MempoolShStats::default()
    };
    for u in query.scripthash_listunspent(sh)? {
        let op = OutPoint {
            txid: bitcoin::Txid::from_byte_array(u.tx_hash),
            vout: u.tx_pos,
        };
        if mp.spends_outpoint(&op) {
            stats.spent_txo_count = stats.spent_txo_count.saturating_add(1);
            stats.spent_txo_sum = stats.spent_txo_sum.saturating_add(u.value);
        }
    }
    for item in items {
        let tid = bitcoin::Txid::from_byte_array(item.txid);
        let Some(tx) = mp.get_tx(&tid) else {
            continue;
        };
        for (vout, o) in tx.output.iter().enumerate() {
            if script_hash(o.script_pubkey.as_bytes()) != *sh {
                continue;
            }
            let op = OutPoint {
                txid: tid,
                vout: vout as u32,
            };
            if mp.spends_outpoint(&op) {
                continue;
            }
            stats.funded_txo_count = stats.funded_txo_count.saturating_add(1);
            stats.funded_txo_sum = stats.funded_txo_sum.saturating_add(o.value.to_sat() as i64);
        }
    }
    Ok(stats)
}
