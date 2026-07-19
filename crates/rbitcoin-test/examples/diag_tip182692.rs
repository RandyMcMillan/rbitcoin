//! Diagnose tip stall / PrevoutSpent at tip+1 on a signet datadir.
use bitcoin::hashes::Hash;
use rbitcoin_consensus::{confirm_archived_at, ChainParams, Milestone};
use rbitcoin_primitives::{hex_encode, Height};
use rbitcoin_query::Query;

fn main() {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "./datadir-signet/store".into());
    eprintln!("opening {path} …");
    let t_open = std::time::Instant::now();
    let q = Query::open_or_create(&path).expect("open");
    eprintln!("open+repair in {:?}", t_open.elapsed());
    let tip = q.tip_height().expect("tip");
    eprintln!(
        "tip={tip:?} spend_index={} scripthash_rows={}",
        q.spend_index_enabled(),
        q.scripthash_entry_count()
    );
    let tip_hash = q.header_at_height(tip).unwrap().unwrap().1.hash;
    eprintln!("tip_hash={}", hex_encode(tip_hash));

    let path_entries = q
        .resume_work_path_after_tip(tip_hash, 0, 4)
        .expect("resume");
    eprintln!("resume entries={}", path_entries.len());
    for e in &path_entries {
        eprintln!(
            "  h={} body={} hash={}",
            e.height,
            e.has_body,
            hex_encode(e.hash)
        );
    }
    if path_entries.is_empty() {
        return;
    }
    let e0 = &path_entries[0];
    assert_eq!(e0.height, tip.0 + 1);

    let block = q
        .reconstruct_archived_block(&e0.hash)
        .unwrap()
        .expect("body");
    eprintln!("block txs={}", block.txdata.len());

    let mut spent_hits = 0usize;
    let mut raw_only = 0usize;
    for (ti, tx) in block.txdata.iter().enumerate() {
        for (ii, inp) in tx.input.iter().enumerate() {
            if inp.previous_output.is_null() {
                continue;
            }
            let txid = inp.previous_output.txid.to_byte_array();
            let vout = inp.previous_output.vout;
            let strong = q.spenders(&txid, vout).unwrap();
            let raw = q.spenders_raw(&txid, vout).unwrap();
            if !strong.is_empty() {
                spent_hits += 1;
                if spent_hits <= 12 {
                    eprintln!(
                        "SPENT confirmed-strong tx#{ti} in#{ii} prev={}:{}",
                        hex_encode(txid),
                        vout
                    );
                    for p in &strong {
                        let th = q.store().tx_height.get(p.spending_tx_fk).unwrap();
                        let is = q.store().strong_tx.is_strong(p.spending_tx_fk).unwrap();
                        let cs = q.store().is_confirmed_strong(p.spending_tx_fk).unwrap();
                        let stx = q.store().get_tx(p.spending_tx_fk).ok();
                        eprintln!(
                            "  spender fk={} height={:?} is_strong={} conf_strong={} txid={}",
                            p.spending_tx_fk.0,
                            th,
                            is,
                            cs,
                            stx.map(|t| hex_encode(t.txid)).unwrap_or_default()
                        );
                    }
                }
            } else if !raw.is_empty() {
                raw_only += 1;
                if raw_only <= 3 {
                    eprintln!(
                        "raw-only tx#{ti} in#{ii} prev={}:{} raw_n={}",
                        hex_encode(txid),
                        vout,
                        raw.len()
                    );
                    for p in raw.iter().take(4) {
                        let th = q.store().tx_height.get(p.spending_tx_fk).unwrap();
                        let is = q.store().strong_tx.is_strong(p.spending_tx_fk).unwrap();
                        let cs = q.store().is_confirmed_strong(p.spending_tx_fk).unwrap();
                        eprintln!(
                            "  raw fk={} height={:?} is_strong={} conf_strong={}",
                            p.spending_tx_fk.0, th, is, cs
                        );
                    }
                }
            }
        }
    }
    eprintln!("confirmed-strong hits={spent_hits} raw_only_sample={raw_only}");

    // Count strong-above-tip remaining
    let tip_h = tip.0;
    let mut above = 0u64;
    q.store()
        .tx_height
        .for_each_set(|fk, h| {
            if h > tip_h {
                above += 1;
                if above <= 5 {
                    let is = q.store().strong_tx.is_strong(fk).unwrap();
                    eprintln!("  above-tip fk={} h={h} is_strong={is}", fk.0);
                }
            }
            Ok(())
        })
        .unwrap();
    eprintln!("tx_height entries with h > tip: {above}");

    let params = ChainParams::signet();
    let ms = Milestone::NONE;
    eprintln!("confirming height {} …", e0.height);
    let t0 = std::time::Instant::now();
    match confirm_archived_at(&q, &params, Height(e0.height), &e0.hash, ms) {
        Ok(_) => eprintln!("CONFIRM OK in {:?}", t0.elapsed()),
        Err(e) => eprintln!("CONFIRM ERR in {:?}: {e}", t0.elapsed()),
    }
    eprintln!("tip after={:?}", q.tip_height());
}
