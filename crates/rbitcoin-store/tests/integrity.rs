use rbitcoin_primitives::{Fk, Height};
use rbitcoin_store::header_table::block_header_hash;
use rbitcoin_store::header_table::HeaderRecord;
use rbitcoin_store::integrity::*;
use rbitcoin_store::store::Store;
use rbitcoin_store::tx_table::{InputRecord, OutputRecord, TxRecord};
use std::path::PathBuf;

fn tmp() -> PathBuf {
    let p = rbitcoin_store::testutil::fresh_path("integrity");
    std::fs::create_dir_all(&p).unwrap();
    p
}

#[test]
fn one_clock_tick_does_not_share_an_integrity_dir() {
    let a = rbitcoin_store::testutil::TempDir::labeled_at("integrity", 1).unwrap();
    let b = rbitcoin_store::testutil::TempDir::labeled_at("integrity", 1).unwrap();
    assert_ne!(
        a.path(),
        b.path(),
        "two integrity stores from one clock sample"
    );
    for dir in [&a, &b] {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dir.join("meta"))
            .unwrap_or_else(|e| panic!("meta in {}: {e}", dir.display()));
    }
}

fn hdr(prev: Fk, parent_hash: [u8; 32], salt: u8) -> HeaderRecord {
    let mut merkle = [0u8; 32];
    merkle[0] = salt;
    hdr_merkle(prev, parent_hash, salt, merkle)
}

fn hdr_merkle(prev: Fk, parent_hash: [u8; 32], salt: u8, merkle: [u8; 32]) -> HeaderRecord {
    let version = 1i32;
    let timestamp = u32::from(salt) + 1;
    let bits = 0x207fffff;
    let nonce = u32::from(salt);
    let hash = if prev.is_null() {
        block_header_hash(version, &[0u8; 32], &merkle, timestamp, bits, nonce)
    } else {
        block_header_hash(version, &parent_hash, &merkle, timestamp, bits, nonce)
    };
    HeaderRecord {
        prev_fk: prev,
        version,
        timestamp,
        bits,
        nonce,
        merkle_root: merkle,
        hash,
        size: 0,
        weight: 0,
    }
}

fn put_coinbase(s: &Store, salt: u8) -> Fk {
    let rec = TxRecord {
        txid: [salt; 32],
        version: 1,
        locktime: 0,
        input_count: 1,
        output_count: 1,
    };
    let ins = vec![InputRecord::coinbase(u32::MAX, vec![salt], vec![])];
    let outs = vec![OutputRecord::unspent(50, vec![0x51])];
    s.put_tx_full_batch_indexed(&[(rec, ins, outs)], true)
        .unwrap()[0]
}

#[test]
fn clean_header_chain_revalidate_no_shrink() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let mut parent_hash = [0u8; 32];
    let mut prev = Fk::NULL;
    for h in 0u32..4 {
        let rec = hdr(prev, parent_hash, h as u8);
        parent_hash = rec.hash;
        let fk = s.put_header(&rec).unwrap();
        prev = fk;
        s.confirmed
            .set(&rbitcoin_store::ClassCWrite::for_test(), Height(h), fk)
            .unwrap();
    }
    s.flush_class_c_tip().unwrap();
    s.headers.flush().unwrap();

    rbitcoin_log::capture_logs(true);
    let r = s.revalidate_tip_window_n(6).unwrap();
    let logs = rbitcoin_log::take_logs();
    rbitcoin_log::capture_logs(false);
    assert!(r.is_clean(), "clean chain should not shrink: {r:?}");
    assert_eq!(s.confirmed.tip_height(), Some(Height(3)));
    let text = logs
        .iter()
        .map(|(_, line)| line.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        text.contains("store: tip revalidate 0..=3 (4 heights)"),
        "{text}"
    );
    assert!(
        text.contains("store: tip revalidate done height=3"),
        "{text}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn poison_prev_edge_shrinks_tip() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let g = hdr(Fk::NULL, [0u8; 32], 0);
    let g_fk = s.put_header(&g).unwrap();
    s.confirmed
        .set(&rbitcoin_store::ClassCWrite::for_test(), Height(0), g_fk)
        .unwrap();
    let a = hdr(g_fk, g.hash, 1);
    let a_fk = s.put_header(&a).unwrap();
    s.confirmed
        .set(&rbitcoin_store::ClassCWrite::for_test(), Height(1), a_fk)
        .unwrap();
    let b = hdr(a_fk, a.hash, 2);
    let b_fk = s.put_header(&b).unwrap();
    s.confirmed
        .set(&rbitcoin_store::ClassCWrite::for_test(), Height(2), b_fk)
        .unwrap();
    // Steal tip height 2 to point at G (false conf edge).
    s.confirmed
        .set(&rbitcoin_store::ClassCWrite::for_test(), Height(2), g_fk)
        .unwrap();
    s.flush_class_c_tip().unwrap();
    rbitcoin_store::spend_durable::SpendDurable::new(5, 5)
        .store(s.path())
        .unwrap();

    let r = s.revalidate_tip_window_n(6).unwrap();
    assert!(r.tip_shrunk, "must shrink: {r:?}");
    assert_eq!(r.first_bad_reason, Some("prev_fk != confirmed parent"));
    assert_eq!(s.confirmed.tip_height(), Some(Height(1)));
    assert_eq!(r.tip_after, Some(1));
    let m = rbitcoin_store::spend_durable::SpendDurable::load(s.path())
        .unwrap()
        .unwrap();
    assert_eq!(m.annotated_through(), 1);
    assert_eq!(
        m.durable_through(),
        1,
        "a marker above the shrunk tip is clamped"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn tip_seal_roundtrip_keeps_each_field() {
    let dir = tmp();
    let seal = TipSeal {
        tip_height: 0x0403_0201,
        tip_hash: std::array::from_fn(|i| i as u8),
        confirmed_len: 0x0807_0605_0403_0201,
        generation: 0x100f_0e0d_0c0b_0a09,
    };
    seal.store(&dir).unwrap();
    let back = TipSeal::load(&dir).unwrap().expect("seal");
    assert_eq!(back.tip_height, seal.tip_height);
    assert_eq!(back.tip_hash, seal.tip_hash);
    assert_eq!(back.confirmed_len, seal.confirmed_len);
    assert_eq!(back.generation, seal.generation);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn tip_seal_clamps_unsealed_extension() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let g = hdr(Fk::NULL, [0u8; 32], 0);
    let g_fk = s.put_header(&g).unwrap();
    s.confirmed
        .set(&rbitcoin_store::ClassCWrite::for_test(), Height(0), g_fk)
        .unwrap();
    s.flush_class_c_tip().unwrap();
    let seal = TipSeal::load(s.path())
        .unwrap()
        .expect("seal after tip barrier");
    assert_eq!(seal.tip_height, 0);
    assert_eq!(seal.tip_hash, g.hash);
    assert_eq!(seal.confirmed_len, 1);
    assert_eq!(seal.generation, 1);

    // Extend tip in RAM + flush confirmed only *without* updating seal:
    // simulate by writing a higher tip then restoring old seal file.
    let a = hdr(g_fk, g.hash, 1);
    let a_fk = s.put_header(&a).unwrap();
    s.confirmed
        .set(&rbitcoin_store::ClassCWrite::for_test(), Height(1), a_fk)
        .unwrap();
    s.confirmed.flush().unwrap();
    // Overwrite seal with the old tip-0 seal (as if barrier never finished).
    seal.store(s.path()).unwrap();
    assert_eq!(s.confirmed.tip_height(), Some(Height(1)));

    let r = s.revalidate_tip_window_n(6).unwrap();
    assert!(r.tip_shrunk, "seal clamp must shrink: {r:?}");
    assert_eq!(s.confirmed.tip_height(), Some(Height(0)));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn merkle_mismatch_clears_body_and_shrinks() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let g = hdr(Fk::NULL, [0u8; 32], 0);
    let g_fk = s.put_header(&g).unwrap();
    s.confirmed
        .set(&rbitcoin_store::ClassCWrite::for_test(), Height(0), g_fk)
        .unwrap();
    let tx_fk = put_coinbase(&s, 9);
    let txid = s.txs.body_txid(tx_fk).unwrap();
    // Header merkle is salt-based, not txid → S3 fail.
    s.header_txs
        .put_range(&rbitcoin_store::ClassCWrite::for_test(), g_fk, tx_fk, 1)
        .unwrap();
    // Ensure genesis hash still consistent with its merkle field (no rewrite).
    let _ = txid;
    s.flush_class_c_tip().unwrap();
    s.header_txs.flush().unwrap();
    s.txs.flush().unwrap();

    rbitcoin_log::capture_logs(true);
    let r = s.revalidate_tip_window_n(6).unwrap();
    let logs = rbitcoin_log::take_logs();
    rbitcoin_log::capture_logs(false);
    assert_eq!(r.first_bad_reason, Some("merkle root mismatch"));
    assert!(r.bodies_cleared >= 1);
    assert!(r.tip_shrunk);
    assert!(s.confirmed.tip_height().is_none());
    let text = logs
        .iter()
        .map(|(_, line)| line.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        text.contains("store: tip revalidate done height=none"),
        "{text}"
    );
    assert!(text.contains("bad=0 reason=merkle root mismatch"), "{text}");
    assert!(!s.header_txs.has_body(g_fk).unwrap());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn open_restores_strong_bits_from_fence() {
    let dir = tmp();
    let first_tx;
    {
        let s = Store::create_tiny(&dir).unwrap();
        let write = rbitcoin_store::ClassCWrite::for_test();
        let mut parent_hash = [0u8; 32];
        let mut prev = Fk::NULL;
        let mut txs = Vec::new();
        for h in 0u32..8 {
            let tx_fk = put_coinbase(&s, h as u8 + 1);
            let txid = s.txs.body_txid(tx_fk).unwrap();
            let rec = hdr_merkle(prev, parent_hash, h as u8, txid);
            parent_hash = rec.hash;
            let hfk = s.put_header(&rec).unwrap();
            s.header_txs.put_range(&write, hfk, tx_fk, 1).unwrap();
            s.strong_tx.set_strong(&write, tx_fk, hfk).unwrap();
            s.confirmed.set(&write, Height(h), hfk).unwrap();
            prev = hfk;
            txs.push(tx_fk);
        }
        s.rebuild_height_fence().unwrap();
        s.flush_class_c_tip().unwrap();
        first_tx = txs[0];
        s.strong_tx.set_unstrong(&write, first_tx).unwrap();
        s.strong_tx.flush().unwrap();
        assert!(!s.strong_tx.is_strong(first_tx).unwrap());
    }

    let s = Store::open_tiny(&dir).unwrap();
    s.repair_strong_not_on_fence().unwrap();
    assert!(s.restore_strong_from_fence().unwrap() > 0);
    s.strong_tx.flush().unwrap();
    assert!(
        s.strong_tx.is_strong(first_tx).unwrap(),
        "restore must set a fence bit a torn strong flush cleared"
    );
    assert!(s.is_confirmed_strong(first_tx).unwrap());
    let _ = std::fs::remove_dir_all(&dir);
}

/// 20 trailing null `confirmed[]` slots must become the last real tip in
/// one revalidate (the 6-height window alone only walks the tail).
#[test]
fn trailing_null_confirmed_slots_trimmed_in_one_open() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let mut parent_hash = [0u8; 32];
    let mut prev = Fk::NULL;
    for h in 0u32..4 {
        let rec = hdr(prev, parent_hash, h as u8);
        parent_hash = rec.hash;
        let fk = s.put_header(&rec).unwrap();
        prev = fk;
        s.confirmed
            .set(&rbitcoin_store::ClassCWrite::for_test(), Height(h), fk)
            .unwrap();
    }
    s.headers.flush().unwrap();
    s.confirmed.flush().unwrap();
    // No tip seal — seal clamp must not mask the suffix-null case.
    drop(s);

    let path = dir.join("confirmed.body");
    let f = rbitcoin_store::file::TableFile::open(&path, rbitcoin_primitives::TableKind::Confirmed)
        .unwrap();
    let n = (f.logical_len() - rbitcoin_store::file::FILE_HEADER_LEN as u64) / 8;
    assert_eq!(n, 4, "four real confirmed heights");
    let extra = 20u64;
    let zeros = vec![0u8; (extra * 8) as usize];
    f.write_at(rbitcoin_store::file::FILE_HEADER_LEN as u64 + n * 8, &zeros)
        .unwrap();
    f.flush().unwrap();
    drop(f);

    let s = Store::open_tiny(&dir).unwrap();
    assert_eq!(
        s.confirmed.tip_height(),
        Some(Height(23)),
        "HWM includes 20 trailing nulls"
    );
    let r = s.revalidate_tip_window_n(6).unwrap();
    assert_eq!(
        s.confirmed.tip_height(),
        Some(Height(3)),
        "one open must drop the whole null suffix; report={r:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A torn previous-tip block under a later page that landed: the seal clamp
/// leaves the zeroed run on top, and the tip must drop below it, not onto it.
#[test]
fn seal_clamp_exposes_null_run_and_tip_drops_below_it() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    let mut parent_hash = [0u8; 32];
    let mut prev = Fk::NULL;
    for h in 0u32..20 {
        let rec = hdr(prev, parent_hash, h as u8);
        parent_hash = rec.hash;
        let fk = s.put_header(&rec).unwrap();
        prev = fk;
        s.confirmed.set(&write, Height(h), fk).unwrap();
        if h == 14 {
            s.headers.flush().unwrap();
            s.flush_class_c_tip().unwrap();
        }
    }
    let seal = TipSeal::load(s.path()).unwrap().expect("seal at 14");
    assert_eq!(seal.tip_height, 14);
    for h in 5u32..=14 {
        s.confirmed.set_raw_for_test(&write, Height(h), 0).unwrap();
    }
    s.headers.flush().unwrap();
    s.confirmed.flush().unwrap();
    seal.store(s.path()).unwrap();
    rbitcoin_store::spend_durable::SpendDurable::new(12, 12)
        .store(s.path())
        .unwrap();
    drop(s);

    // Reopen: the hole check must measure from the sealed tip and let the
    // null run above height 4 through to the clamp and trim.
    let s = Store::open_tiny(&dir).unwrap();
    let r = s.revalidate_tip_window_n(6).unwrap();
    let tip = s.confirmed.tip_height().expect("a tip below the null run");
    assert_eq!(tip, Height(4), "{r:?}");
    assert!(
        s.confirmed.get(tip).unwrap().is_some(),
        "tip is a null slot"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A clamp down to the sealed height must still compare the sealed hash: a
/// different header there is a block the barrier never sealed.
#[test]
fn seal_clamp_to_a_different_block_at_the_sealed_height_drops_it() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    let mut parent_hash = [0u8; 32];
    let mut prev = Fk::NULL;
    let mut parent_at_10 = ([0u8; 32], Fk::NULL);
    for h in 0u32..=10 {
        if h == 10 {
            parent_at_10 = (parent_hash, prev);
        }
        let rec = hdr(prev, parent_hash, h as u8);
        parent_hash = rec.hash;
        let fk = s.put_header(&rec).unwrap();
        prev = fk;
        s.confirmed.set(&write, Height(h), fk).unwrap();
    }
    s.headers.flush().unwrap();
    s.flush_class_c_tip().unwrap();
    let seal = TipSeal::load(s.path()).unwrap().expect("seal at 10");
    let other = hdr(parent_at_10.1, parent_at_10.0, 200);
    let other_fk = s.put_header(&other).unwrap();
    s.confirmed.set(&write, Height(10), other_fk).unwrap();
    for h in 11u32..=13 {
        s.confirmed.set_raw_for_test(&write, Height(h), 0).unwrap();
    }
    s.headers.flush().unwrap();
    s.confirmed.flush().unwrap();
    seal.store(s.path()).unwrap();

    let r = s.revalidate_tip_window_n(6).unwrap();
    assert_eq!(s.confirmed.tip_height(), Some(Height(9)), "{r:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Heights `0..len` sealed at the tip, then `confirmed[hole]` nulled and
/// flushed, with the spend marker three below the tip.
fn sealed_chain_with_hole(dir: &std::path::Path, len: u32, holes: &[u32]) {
    let s = Store::create_tiny(dir).unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    let mut parent_hash = [0u8; 32];
    let mut prev = Fk::NULL;
    for h in 0..len {
        let rec = hdr(prev, parent_hash, h as u8);
        parent_hash = rec.hash;
        let fk = s.put_header(&rec).unwrap();
        prev = fk;
        s.confirmed.set(&write, Height(h), fk).unwrap();
    }
    s.headers.flush().unwrap();
    s.flush_class_c_tip().unwrap();
    for &hole in holes {
        s.confirmed
            .set_raw_for_test(&write, Height(hole), 0)
            .unwrap();
    }
    s.confirmed.flush().unwrap();
    rbitcoin_store::spend_durable::SpendDurable::new(len - 3, len - 3)
        .store(s.path())
        .unwrap();
}

/// An older binary rewrote the block that holds the sealed tip with no
/// image. A hole there is that crash: open cuts the tip below it.
#[test]
fn null_slot_in_the_sealed_tip_block_cuts_the_tip_at_open() {
    let dir = tmp();
    sealed_chain_with_hole(&dir, 20, &[16]);
    let s = Store::open_tiny(&dir).expect("a hole in the tip block is crash damage");
    assert_eq!(s.confirmed.tip_height(), Some(Height(15)));
    let _ = std::fs::remove_dir_all(&dir);
}

/// The write-ahead image restores a torn `confirmed[]` page, so a null slot
/// outside the block that holds the sealed tip is not crash damage. Open
/// refuses it instead of building a fence without those heights and
/// clearing their strong bits.
#[test]
fn null_confirmed_slot_below_the_tip_refuses_open() {
    let dir = tmp();
    // Height 519 is in the second 4 KiB block; height 3 is in page 0.
    sealed_chain_with_hole(&dir, 520, &[3]);
    match Store::open_tiny(&dir) {
        Err(rbitcoin_store::StoreError::Corrupt(reason)) => {
            assert!(reason.contains("confirmed"), "{reason}");
        }
        Err(e) => panic!("wrong error: {e}"),
        Ok(_) => panic!("open accepted a null confirmed[] slot below the tip"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// The block that holds the sealed tip bounds the cut, not the top non-null
/// slot. With the sealed tip at 520, a torn block nulls 510..=520 and the
/// top slot left is 509, in the block before. A null at 508 is outside the
/// sealed tip's block, so open refuses it.
#[test]
fn null_below_the_sealed_tip_block_refuses_open_after_a_trailing_run() {
    let dir = tmp();
    let mut holes: Vec<u32> = (510..=520).collect();
    holes.push(508);
    sealed_chain_with_hole(&dir, 521, &holes);
    match Store::open_tiny(&dir) {
        Err(rbitcoin_store::StoreError::Corrupt(reason)) => {
            assert!(reason.contains("confirmed"), "{reason}");
        }
        Err(e) => panic!("wrong error: {e}"),
        Ok(s) => panic!(
            "open cut a hole outside the sealed tip block; tip {:?}",
            s.confirmed.tip_height()
        ),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn revalidate_n_zero_and_empty_store() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let r = s.revalidate_tip_window_n(6).unwrap();
    assert!(!r.tip_shrunk);
    assert_eq!(r.tip_after, None);
    assert!(s.confirmed.tip_height().is_none());

    let rec = hdr(Fk::NULL, [0u8; 32], 0);
    let fk = s.put_header(&rec).unwrap();
    s.confirmed
        .set(&rbitcoin_store::ClassCWrite::for_test(), Height(0), fk)
        .unwrap();
    s.flush_class_c_tip().unwrap();
    s.headers.flush().unwrap();
    let r = s.revalidate_tip_window_n(0).unwrap();
    assert!(!r.tip_shrunk);
    assert_eq!(r.tip_after, Some(0));
    assert_eq!(s.confirmed.tip_height(), Some(Height(0)));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn genesis_nonzero_prev_fk_and_header_txs_oob_shrink() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let dummy = hdr(Fk::NULL, [0u8; 32], 9);
    let dummy_fk = s.put_header(&dummy).unwrap();
    let g = hdr(dummy_fk, dummy.hash, 0);
    let g_fk = s.put_header(&g).unwrap();
    s.confirmed
        .set(&rbitcoin_store::ClassCWrite::for_test(), Height(0), g_fk)
        .unwrap();
    s.flush_class_c_tip().unwrap();
    s.headers.flush().unwrap();
    let r = s.revalidate_tip_window_n(6).unwrap();
    assert_eq!(r.first_bad_reason, Some("genesis prev_fk non-null"));
    assert!(r.tip_shrunk);
    let _ = std::fs::remove_dir_all(&dir);

    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let g = hdr(Fk::NULL, [0u8; 32], 0);
    let g_fk = s.put_header(&g).unwrap();
    s.confirmed
        .set(&rbitcoin_store::ClassCWrite::for_test(), Height(0), g_fk)
        .unwrap();
    s.header_txs
        .put_range(
            &rbitcoin_store::ClassCWrite::for_test(),
            g_fk,
            Fk(1_000_000),
            1,
        )
        .unwrap();
    s.flush_class_c_tip().unwrap();
    s.headers.flush().unwrap();
    s.header_txs.flush().unwrap();
    let r = s.revalidate_tip_window_n(6).unwrap();
    assert_eq!(r.first_bad_reason, Some("header_txs range OOB"));
    assert!(r.bodies_cleared >= 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn tip_seal_hash_mismatch_drops_tip_block() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let g = hdr(Fk::NULL, [0u8; 32], 0);
    let g_fk = s.put_header(&g).unwrap();
    s.confirmed
        .set(&rbitcoin_store::ClassCWrite::for_test(), Height(0), g_fk)
        .unwrap();
    s.flush_class_c_tip().unwrap();
    let mut seal = TipSeal::load(s.path())
        .unwrap()
        .expect("seal after tip barrier");
    seal.tip_hash = [0xff; 32];
    seal.store(s.path()).unwrap();
    let r = s.revalidate_tip_window_n(6).unwrap();
    assert!(r.tip_shrunk, "seal hash mismatch must shrink: {r:?}");
    assert!(s.confirmed.tip_height().is_none());
    let _ = std::fs::remove_dir_all(&dir);
}

/// Core `-checkblocks=0` / `-1` means the whole chain, not “skip checks”.
#[test]
fn checkblocks_zero_walks_below_default_window() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let mut parent_hash = [0u8; 32];
    let mut prev = Fk::NULL;
    let mut genesis_fk = Fk::NULL;
    for h in 0u32..10 {
        let rec = hdr(prev, parent_hash, h as u8);
        parent_hash = rec.hash;
        let fk = s.put_header(&rec).unwrap();
        if h == 0 {
            genesis_fk = fk;
        }
        prev = fk;
        s.confirmed
            .set(&rbitcoin_store::ClassCWrite::for_test(), Height(h), fk)
            .unwrap();
    }
    s.confirmed
        .set(
            &rbitcoin_store::ClassCWrite::for_test(),
            Height(1),
            genesis_fk,
        )
        .unwrap();
    s.flush_class_c_tip().unwrap();
    s.headers.flush().unwrap();
    // A marker at the tip keeps the default window short. Without one,
    // `n == 6` is the whole chain.
    rbitcoin_store::spend_durable::SpendDurable::new(9, 9)
        .store(s.path())
        .unwrap();

    let r6 = s.revalidate_tip_window_n(6).unwrap();
    assert!(
        r6.is_clean(),
        "a tip marker keeps the default window off height-1 poison: {r6:?}"
    );
    assert_eq!(s.confirmed.tip_height(), Some(Height(9)));

    let r0 = s.revalidate_tip_window_n(0).unwrap();
    assert!(r0.tip_shrunk, "n=0 must walk from genesis: {r0:?}");
    assert_eq!(r0.first_bad_reason, Some("prev_fk != confirmed parent"));
    assert_eq!(r0.first_bad_height, Some(1));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn durable_through_widens_past_the_six_block_window() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let mut parent_hash = [0u8; 32];
    let mut prev = Fk::NULL;
    let mut genesis_fk = Fk::NULL;
    for h in 0u32..10 {
        let rec = hdr(prev, parent_hash, h as u8);
        parent_hash = rec.hash;
        let fk = s.put_header(&rec).unwrap();
        if h == 0 {
            genesis_fk = fk;
        }
        prev = fk;
        s.confirmed
            .set(&rbitcoin_store::ClassCWrite::for_test(), Height(h), fk)
            .unwrap();
    }
    s.confirmed
        .set(
            &rbitcoin_store::ClassCWrite::for_test(),
            Height(1),
            genesis_fk,
        )
        .unwrap();
    s.flush_class_c_tip().unwrap();
    s.headers.flush().unwrap();
    rbitcoin_store::spend_durable::SpendDurable::new(0, 0)
        .store(s.path())
        .unwrap();

    let r = s.revalidate_tip_window_n(6).unwrap();
    assert_eq!(r.first_bad_height, Some(1), "{r:?}");
    assert!(r.tip_shrunk, "marker at 0 must include height 1: {r:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// No `spend_durable` means nothing has been device-flushed. A fault older
/// than the last 6 heights is still in the window.
#[test]
fn missing_marker_revalidates_from_genesis() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let mut parent_hash = [0u8; 32];
    let mut prev = Fk::NULL;
    let mut genesis_fk = Fk::NULL;
    for h in 0u32..10 {
        let rec = hdr(prev, parent_hash, h as u8);
        parent_hash = rec.hash;
        let fk = s.put_header(&rec).unwrap();
        if h == 0 {
            genesis_fk = fk;
        }
        prev = fk;
        s.confirmed
            .set(&rbitcoin_store::ClassCWrite::for_test(), Height(h), fk)
            .unwrap();
    }
    s.confirmed
        .set(
            &rbitcoin_store::ClassCWrite::for_test(),
            Height(1),
            genesis_fk,
        )
        .unwrap();
    s.flush_class_c_tip().unwrap();
    s.headers.flush().unwrap();
    assert!(rbitcoin_store::spend_durable::SpendDurable::load(s.path())
        .unwrap()
        .is_none());

    let r = s.revalidate_tip_window_n(6).unwrap();
    assert_eq!(r.first_bad_height, Some(1), "{r:?}");
    assert!(
        r.tip_shrunk,
        "a missing marker must walk from genesis, not the last 6: {r:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

struct Disarm(rbitcoin_store::io_handle::IoHandle);

impl Drop for Disarm {
    fn drop(&mut self) {
        rbitcoin_store::io_handle::disarm_pwrite(self.0);
    }
}

fn coinbase(salt: u8) -> (TxRecord, Vec<InputRecord>, Vec<OutputRecord>) {
    (
        TxRecord {
            txid: [salt; 32],
            version: 1,
            locktime: 0,
            input_count: 1,
            output_count: 1,
        },
        vec![InputRecord::coinbase(u32::MAX, vec![salt], vec![])],
        vec![OutputRecord::unspent(50, vec![0x51])],
    )
}

fn connect_coinbase(
    s: &Store,
    write: &rbitcoin_store::ClassCWrite,
    h: u8,
    prev: Fk,
    parent_hash: [u8; 32],
) -> (Fk, [u8; 32]) {
    let txid = [h; 32];
    let (rec, ins, outs) = coinbase(h);
    let tx_fk = s
        .put_tx_full_batch_indexed(&[(rec, ins, outs)], false)
        .unwrap()[0];
    let merkle = rbitcoin_primitives::merkle_root_from_txids(&[txid]);
    let header = hdr_merkle(prev, parent_hash, h, merkle);
    let hash = header.hash;
    let header_fk = s.put_header(&header).unwrap();
    s.header_txs.put_range(write, header_fk, tx_fk, 1).unwrap();
    s.strong_tx.set_strong(write, tx_fk, header_fk).unwrap();
    s.confirmed
        .set(write, Height(u32::from(h)), header_fk)
        .unwrap();
    (header_fk, hash)
}

fn on_disk_hwm(path: &std::path::Path) -> u64 {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(path).unwrap();
    f.seek(SeekFrom::Start(8)).unwrap();
    let mut buf = [0u8; 8];
    f.read_exact(&mut buf).unwrap();
    u64::from_le_bytes(buf)
}

/// A delta commit syncs Class A and names its creates, so open keeps them. An
/// older binary's datadir has no sync mark: open cuts a create no block range
/// names, though a committed delta spend of its output, by a spender still on
/// the chain, may remain. The delta is clipped to the cut `spent.body` end,
/// so the next create at the same offsets reads unspent.
#[test]
fn a_spend_of_a_cut_create_does_not_mark_the_create_that_reuses_its_slot() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    connect_coinbase(&s, &write, 0, Fk::NULL, [0u8; 32]);
    s.flush_class_c_tip().unwrap();
    let (rec, ins, outs) = coinbase(8);
    let unranged = s
        .put_tx_full_batch_indexed(&[(rec, ins, outs)], false)
        .unwrap()[0];
    assert_eq!(unranged, Fk(2));
    let cut = s.txs.spent_range(unranged).unwrap();
    s.put_spend_create(unranged, 0, Fk(1), 0).unwrap();
    s.note_spend_snapshot(0);
    s.checkpoint_spend_through(0).unwrap();
    assert_eq!(s.txs.get_output_spender_meta(unranged, 0).unwrap().1, Fk(1));
    drop(s);
    std::fs::remove_file(dir.join(rbitcoin_store::tx_table::CLASS_A_SYNCED_NAME)).unwrap();

    let s = Store::open_tiny(&dir).unwrap();
    assert_eq!(s.txs.count(), 1, "open cuts the create");
    let (rec, ins, outs) = coinbase(9);
    let fk = s
        .put_tx_full_batch_indexed(&[(rec, ins, outs)], false)
        .unwrap()[0];
    assert_eq!(fk, Fk(2));
    assert_eq!(s.txs.spent_range(fk).unwrap(), cut, "the same slot again");
    let (_, field, _) = s.txs.get_output_spender_meta(fk, 0).unwrap();
    assert!(
        field.is_null(),
        "a new create's output reads spent by {field:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

fn fat_witness(salt: u8) -> (TxRecord, Vec<InputRecord>, Vec<OutputRecord>) {
    let (rec, mut ins, outs) = coinbase(salt);
    // Strides at or above 65536 spill to seqsigwit.loc.ovf.
    ins[0].witness = vec![vec![0u8; 65536 * 8]];
    (rec, ins, outs)
}

fn wide_coinbase(salt: u8) -> (TxRecord, Vec<InputRecord>, Vec<OutputRecord>) {
    let rec = TxRecord {
        txid: [salt; 32],
        version: 1,
        locktime: 0,
        input_count: 1,
        output_count: 256,
    };
    let ins = vec![InputRecord::coinbase(u32::MAX, vec![salt], vec![])];
    (rec, ins, vec![OutputRecord::unspent(1, vec![0x51]); 256])
}

/// Genesis is sealed and named by a spend commit. The next append's header
/// write on `file` fails. The append returns an error and latches Class A:
/// the hook fires, and no later barrier or mark runs, so nothing durable
/// names a create the device may not hold. Reopen keeps genesis and needs
/// no reindex.
fn failed_header_write_stops_class_a(
    file: fn(&Store) -> rbitcoin_store::io_handle::IoHandle,
    tx: (TxRecord, Vec<InputRecord>, Vec<OutputRecord>),
    prune: bool,
) {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    let (genesis, _) = connect_coinbase(&s, &write, 0, Fk::NULL, [0u8; 32]);
    assert_eq!(s.flush_class_c_tip().unwrap(), Some(Height(0)));
    s.note_spend_snapshot(0);
    s.checkpoint_spend_through(0).unwrap();
    s.set_prune_seqsigwit_mode(prune);
    let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writers_at_fail = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = std::sync::Arc::clone(&fired);
    let counted = std::sync::Arc::clone(&writers_at_fail);
    s.txs.on_class_a_failure(move || {
        counted.store(
            rbitcoin_store::testutil::class_a_writers_at_last_failure(),
            std::sync::atomic::Ordering::SeqCst,
        );
        seen.store(true, std::sync::atomic::Ordering::SeqCst);
    });

    let fd = file(&s);
    rbitcoin_store::io_handle::arm_fail_hwm_pwrites(fd, 8);
    let disarm = Disarm(fd);
    assert!(
        s.put_tx_full_batch_indexed(&[tx], false).is_err(),
        "an append whose header write failed returned Ok"
    );
    drop(disarm);
    assert!(fired.load(std::sync::atomic::Ordering::SeqCst));
    assert!(
        writers_at_fail.load(std::sync::atomic::Ordering::SeqCst) > 0,
        "the failure hook ran after the writer count dropped, so a sync can clear the mark"
    );
    assert!(
        s.flush_class_c_tip().is_err(),
        "the barrier ran after a failed write"
    );
    assert!(s.txs.sync_class_a_named(1).is_err());
    assert!(
        s.put_tx_full_batch_indexed(&[coinbase(9)], false).is_err(),
        "an append ran on top of a failed one"
    );
    drop(s);

    let s = Store::open_tiny(&dir).expect("reopen after a failed header write");
    assert_eq!(s.confirmed.tip_height(), Some(Height(0)));
    assert_eq!(s.txs.count(), 1);
    assert!(s.header_txs.has_body(genesis).unwrap());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn failed_locator_header_write_stops_class_a() {
    failed_header_write_stops_class_a(|s| s.txs.create_loc.loc.read_fd(), coinbase(1), false);
}

#[test]
fn failed_create_overflow_header_write_stops_class_a() {
    failed_header_write_stops_class_a(|s| s.txs.create_loc.ovf.read_fd(), wide_coinbase(1), true);
}

#[test]
fn failed_witness_overflow_header_write_stops_class_a() {
    failed_header_write_stops_class_a(|s| s.txs.seqsigwit_loc.ovf.read_fd(), fat_witness(1), false);
}

#[test]
fn failed_txid_header_write_stops_class_a() {
    failed_header_write_stops_class_a(|s| s.txs.txids.file.read_fd(), coinbase(1), false);
}

#[test]
fn failed_input_header_write_stops_class_a() {
    failed_header_write_stops_class_a(|s| s.txs.input.loc.read_fd(), coinbase(1), false);
}

#[test]
fn failed_txstat_header_write_stops_class_a() {
    failed_header_write_stops_class_a(|s| s.txs.txstat.body.read_fd(), coinbase(1), false);
}

/// An append publishes its bodies and locator before it writes `txid.body`,
/// `txstat`, and `input`. A spend commit in that window names only finished
/// creates: a later open keeps create 1 and cuts the unnamed create 2.
#[cfg(debug_assertions)]
#[test]
fn spend_commit_does_not_name_an_unfinished_append() {
    let dir = tmp();
    let s = std::sync::Arc::new(Store::create_tiny(&dir).unwrap());
    let write = rbitcoin_store::ClassCWrite::for_test();
    let (genesis, _) = connect_coinbase(&s, &write, 0, Fk::NULL, [0u8; 32]);
    s.height_fence_extend(Height(0), genesis).unwrap();
    assert_eq!(s.flush_class_c_tip().unwrap(), Some(Height(0)));
    let inside = std::sync::Arc::clone(&s);
    rbitcoin_store::tx_table::set_after_bodies_for_test(Some(Box::new(move || {
        assert_eq!(
            inside.txs.count(),
            2,
            "the bodies published before txid.body"
        );
        inside.note_spend_snapshot(0);
        inside.checkpoint_spend_through(0).unwrap();
    })));
    s.put_tx_full_batch_indexed(&[coinbase(1)], false).unwrap();
    rbitcoin_store::tx_table::set_after_bodies_for_test(None);
    drop(s);
    let s = Store::open_tiny(&dir).unwrap();
    assert_eq!(
        s.txs.count(),
        1,
        "the spend commit named an unfinished create"
    );
    assert_eq!(s.confirmed.tip_height(), Some(Height(0)));
    let _ = std::fs::remove_dir_all(&dir);
}

/// Prune mode already knows the short input is the crash tail. Repair must
/// not also read the witness locator: that read can fail, and open would
/// refuse a store it can already truncate.
#[test]
fn pruned_short_input_skips_the_witness_locator() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    let (prev, parent) = connect_coinbase(&s, &write, 0, Fk::NULL, [0u8; 32]);
    let (prev, parent) = connect_coinbase(&s, &write, 1, prev, parent);
    connect_coinbase(&s, &write, 2, prev, parent);
    s.txs.flush().unwrap();
    assert_eq!(s.txs.count(), 3);
    s.txs.input.truncate_to_count(1).unwrap();
    s.txs.seqsigwit_loc.truncate_to_count(2).unwrap();
    let fd = s.txs.seqsigwit_loc.loc.read_fd();
    rbitcoin_store::io_handle::arm_fail_preads(fd, 8);
    let disarm = Disarm(fd);
    let stems = rbitcoin_store::tx_table::ClassASkewStems {
        create_loc: &s.txs.create_loc,
        seqsigwit_loc: &s.txs.seqsigwit_loc,
        body: &s.txs.body,
        spent: &s.txs.spent,
        seqsigwit: &s.txs.seqsigwit,
        txids: &s.txs.txids,
        txstat: &s.txs.txstat,
        input: &s.txs.input,
    };
    rbitcoin_store::tx_table::repair_class_a_count_skew(stems, true, true, None).unwrap();
    drop(disarm);
    assert_eq!(s.txs.count(), 1, "repair stops at the short input");
    assert_eq!(s.txs.input.count(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

fn write_hwm(path: &std::path::Path, logical: u64) {
    use std::io::{Seek, SeekFrom, Write};
    let mut f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.seek(SeekFrom::Start(8)).unwrap();
    f.write_all(&logical.to_le_bytes()).unwrap();
}

/// The loc header was shortened across a 1024-create window below a sealed
/// tip. Off and the overflow file were not. Open refuses rather than free
/// numbers the sealed block names.
#[test]
fn loc_cut_across_a_window_below_a_sealed_tip_refuses() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    let (prev, parent_hash) = connect_coinbase(&s, &write, 0, Fk::NULL, [0u8; 32]);
    let mut batch = Vec::with_capacity(1024);
    let mut txids = Vec::with_capacity(1024);
    for i in 0..1024u16 {
        let mut txid = [2u8; 32];
        txid[0] = (i & 0xff) as u8;
        txid[1] = (i >> 8) as u8;
        txids.push(txid);
        let wide = i == 1022 || i == 1023;
        let n_out = if wide { 256 } else { 1 };
        let rec = TxRecord {
            txid,
            version: 1,
            locktime: 0,
            input_count: 1,
            output_count: n_out,
        };
        let ins = vec![InputRecord::coinbase(u32::MAX, vec![txid[0]], vec![])];
        let outs = vec![OutputRecord::unspent(1, vec![0x51]); n_out as usize];
        batch.push((rec, ins, outs));
    }
    let fks = s.put_tx_full_batch_indexed(&batch, false).unwrap();
    let merkle = rbitcoin_primitives::merkle_root_from_txids(&txids);
    let header = hdr_merkle(prev, parent_hash, 2, merkle);
    let header_fk = s.put_header(&header).unwrap();
    s.header_txs
        .put_range(&write, header_fk, fks[0], 1024)
        .unwrap();
    for fk in &fks {
        s.strong_tx.set_strong(&write, *fk, header_fk).unwrap();
    }
    s.confirmed.set(&write, Height(1), header_fk).unwrap();
    s.flush_class_c_tip().unwrap();
    s.note_spend_snapshot(1);
    s.checkpoint_spend_through(1).unwrap();
    assert_eq!(s.txs.count(), 1025);
    assert_eq!(s.confirmed.tip_height(), Some(Height(1)));
    drop(s);

    let short =
        rbitcoin_store::file::FILE_HEADER_LEN as u64 + 1023 * rbitcoin_store::create_loc::SLOT;
    write_hwm(&dir.join("create.loc"), short);

    // Height 1 names creates 1024 and 1025: only an older binary cuts them.
    assert_refuses_committed_cut(&dir, 0);
    let _ = std::fs::remove_dir_all(&dir);
}

fn shorten_creates(s: &Store, n: u64) {
    let txs = &s.txs;
    let stems = rbitcoin_store::tx_table::ClassASkewStems {
        create_loc: &txs.create_loc,
        seqsigwit_loc: &txs.seqsigwit_loc,
        body: &txs.body,
        spent: &txs.spent,
        seqsigwit: &txs.seqsigwit,
        txids: &txs.txids,
        txstat: &txs.txstat,
        input: &txs.input,
    };
    let (tx_end, sp_end, in_end) =
        rbitcoin_store::tx_table::class_a_skew_stem_ends(&stems, n, false).unwrap();
    rbitcoin_store::tx_table::class_a_skew_apply_truncate(
        &stems,
        n,
        txs.txids.count(),
        tx_end,
        sp_end,
        in_end,
        false,
    )
    .unwrap();
}

/// The tail's header range was cleared and flushed. The tip shrink did not
/// land. Reopen still drops that tip. Genesis keeps its body.
#[test]
fn cleared_tail_range_still_drops_the_sealed_tip() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    let (genesis, parent) = connect_coinbase(&s, &write, 0, Fk::NULL, [0u8; 32]);
    let (tail, _) = connect_coinbase(&s, &write, 1, genesis, parent);
    s.flush_class_c_tip().unwrap();
    s.txs.flush().unwrap();
    shorten_creates(&s, 1);
    s.header_txs.clear_body(&write, tail).unwrap();
    s.header_txs.flush().unwrap();
    assert_eq!(s.confirmed.tip_height(), Some(Height(1)));
    drop(s);

    let s = Store::open_tiny(&dir).unwrap();
    assert_eq!(s.confirmed.tip_height(), Some(Height(0)));
    assert!(s.header_txs.has_body(genesis).unwrap());
    assert!(!s.header_txs.has_body(tail).unwrap());
    assert_eq!(s.txs.count(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Every header range was cleared, including genesis, and the empty-tip
/// shrink did not land. Reopen drops the tip and leaves no body association.
#[test]
fn cleared_anchor_range_still_drops_the_tip() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    let (genesis, parent) = connect_coinbase(&s, &write, 0, Fk::NULL, [0u8; 32]);
    let (tail, _) = connect_coinbase(&s, &write, 1, genesis, parent);
    s.flush_class_c_tip().unwrap();
    s.txs.flush().unwrap();
    shorten_creates(&s, 0);
    s.header_txs.clear_body(&write, genesis).unwrap();
    s.header_txs.clear_body(&write, tail).unwrap();
    s.header_txs.flush().unwrap();
    assert_eq!(s.confirmed.tip_height(), Some(Height(1)));
    drop(s);

    let s = Store::open_tiny(&dir).unwrap();
    assert_eq!(s.confirmed.tip_height(), None);
    assert!(!s.header_txs.has_body(genesis).unwrap());
    assert!(!s.header_txs.has_body(tail).unwrap());
    let _ = std::fs::remove_dir_all(&dir);
}

/// Swaps a directory onto one table's read fd until dropped. A pread on it
/// fails with `EISDIR`, a real OS error on the real read path.
#[cfg(unix)]
struct UnreadableFd {
    fd: std::os::fd::RawFd,
    saved: std::os::fd::RawFd,
}

#[cfg(unix)]
impl UnreadableFd {
    fn arm(handle: rbitcoin_store::io_handle::IoHandle, dir: &std::path::Path) -> Self {
        use std::os::fd::IntoRawFd;
        let fd = handle.as_raw_fd();
        // SAFETY: `fd` is open for the store's life. `dup` keeps the file.
        let saved = unsafe { libc::dup(fd) };
        assert!(saved >= 0);
        let dir_fd = std::fs::File::open(dir).unwrap().into_raw_fd();
        // SAFETY: both fds are open. `dup2` replaces `fd` in place.
        assert!(unsafe { libc::dup2(dir_fd, fd) } >= 0);
        // SAFETY: `fd` now holds its own reference to the directory.
        unsafe { libc::close(dir_fd) };
        Self { fd, saved }
    }
}

#[cfg(unix)]
impl Drop for UnreadableFd {
    fn drop(&mut self) {
        // SAFETY: `saved` is the store's file. Put it back, then drop the dup.
        unsafe {
            libc::dup2(self.saved, self.fd);
            libc::close(self.saved);
        }
    }
}

/// A read that fails is not a divergent block. Open stops with that error
/// and leaves the tip and every body association as they were. The next
/// open, with the read working, keeps the whole chain. A record torn past
/// the body end, a create with no locator, or a locator length past the
/// body read cap still drops that block.
#[cfg(unix)]
#[test]
fn failed_read_in_tip_window_shrinks_nothing_but_a_torn_record_does() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    let mut prev = Fk::NULL;
    let mut parent_hash = [0u8; 32];
    let mut headers = Vec::new();
    for h in 0u8..8 {
        (prev, parent_hash) = connect_coinbase(&s, &write, h, prev, parent_hash);
        headers.push(prev);
    }
    s.flush_class_c_tip().unwrap();
    s.txs.flush().unwrap();
    s.note_spend_snapshot(7);
    s.checkpoint_spend_through(7).unwrap();
    assert!(s.revalidate_tip_window_n(6).unwrap().is_clean());

    let fault = UnreadableFd::arm(s.txs.seqsigwit.body.read_fd(), &dir);
    let err = s.revalidate_tip_window_n(6).unwrap_err();
    drop(fault);
    assert!(
        matches!(err, rbitcoin_store::StoreError::Io { .. }),
        "the read error, not a verdict: {err}"
    );
    assert_eq!(s.confirmed.tip_height(), Some(Height(7)));
    for fk in &headers {
        assert!(s.header_txs.has_body(*fk).unwrap());
    }

    let r = s.revalidate_tip_window_n(6).unwrap();
    assert!(r.is_clean(), "{r:?}");
    assert_eq!(r.tip_after, Some(7));

    let body = &s.txs.seqsigwit.body;
    body.set_logical_len(body.logical_len() - 1).unwrap();
    let r = s.revalidate_tip_window_n(6).unwrap();
    assert_eq!(r.first_bad_height, Some(7), "{r:?}");
    assert_eq!(r.first_bad_reason, Some("seqsigwit read"));
    assert_eq!(r.tip_after, Some(6), "a torn record still shrinks");
    assert!(!s.header_txs.has_body(headers[7]).unwrap());

    s.txs.seqsigwit_loc.truncate_to_count(6).unwrap();
    let r = s.revalidate_tip_window_n(6).unwrap();
    assert_eq!(r.first_bad_reason, Some("seqsigwit range"), "{r:?}");
    assert_eq!(r.tip_after, Some(5), "a missing locator still shrinks");

    let loc = &s.txs.seqsigwit_loc;
    let (start, _) = s.txs.seqsigwit_range(Fk(6)).unwrap();
    loc.truncate_to_count(5).unwrap();
    let huge = rbitcoin_store::var_table::VarTable::BODY_READ_MAX + 8;
    loc.append(&[start], &[huge]).unwrap();
    let r = s.revalidate_tip_window_n(6).unwrap();
    assert_eq!(r.first_bad_reason, Some("seqsigwit read"), "{r:?}");
    assert_eq!(
        r.tip_after,
        Some(4),
        "a locator past the read cap still shrinks"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Every Class A file a commit names is device-synced before Class C.
///
/// A failed `sync_data` on any of them fails the barrier, and the on-disk
/// tip stays where it was. Power loss then cannot leave `confirmed[]` or a
/// `header_txs` range naming creates that never reached the device. The
/// failure latches: a retried fsync can succeed after the kernel dropped
/// the dirty pages, so every later barrier refuses until a restart.
#[test]
fn class_a_sync_failure_stops_the_class_c_commit() {
    for (name, pick) in CLASS_A_FILES {
        let dir = tmp();
        let s = Store::create_tiny(&dir).unwrap();
        let write = rbitcoin_store::ClassCWrite::for_test();
        let (genesis, parent) = connect_coinbase(&s, &write, 0, Fk::NULL, [0u8; 32]);
        s.flush_class_c_tip().unwrap();
        let sealed = on_disk_hwm(&dir.join("confirmed.body"));
        connect_coinbase(&s, &write, 1, genesis, parent);
        rbitcoin_store::io_handle::arm_fail_sync(pick(&s), 1);
        assert!(
            s.flush_class_c_tip().is_err(),
            "{name}: the barrier committed without syncing it"
        );
        assert!(
            s.flush_class_c_tip().is_err(),
            "{name}: a retried barrier committed over a failed sync"
        );
        assert_eq!(
            on_disk_hwm(&dir.join("confirmed.body")),
            sealed,
            "{name}: confirmed[] reached disk ahead of Class A"
        );
        drop(s);
        let s = Store::open_tiny(&dir).unwrap();
        assert_eq!(s.confirmed.tip_height(), Some(Height(0)), "{name}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// A failed step of the shutdown flush does not skip the steps after it. A
/// dirty scripthash index costs a full rebuild at the next start, so it syncs
/// and clears its mark first, even when the tip seal then fails. The seal is
/// skipped over a failed header sync: a retried fsync can return 0 over
/// dropped pages (#2138).
#[test]
fn a_failed_shutdown_step_still_syncs_a_dirty_scripthash_index() {
    use rbitcoin_store::io_handle::{arm_fail_flush, arm_fail_sync, IoHandle};
    type Arm = fn(IoHandle, u32);
    let header_body: ClassAPick = |s| s.headers.body.io_handle();
    for (name, pick, arm, seal) in [
        ("headers.body", header_body, arm_fail_flush as Arm, true),
        (
            CLASS_A_FILES[0].0,
            CLASS_A_FILES[0].1,
            arm_fail_sync as Arm,
            true,
        ),
        ("no seal", header_body, arm_fail_flush as Arm, false),
    ] {
        let dir = tmp();
        let s = Store::create_tiny(&dir).unwrap();
        let write = rbitcoin_store::ClassCWrite::for_test();
        let (genesis, parent) = connect_coinbase(&s, &write, 0, Fk::NULL, [0u8; 32]);
        s.flush_class_c_tip().unwrap();
        let sealed = on_disk_hwm(&dir.join("confirmed.body"));
        connect_coinbase(&s, &write, 1, genesis, parent);
        drop(s.scripthash.begin_dirty_write().unwrap());
        let marker = dir.join(rbitcoin_store::scripthash::SH_DIRTY_NAME);
        assert!(marker.exists(), "{name}");
        if seal {
            if name == "headers.body" {
                // A put leaves the body synced. Republish its length so the
                // shutdown flush has a sync to fail, as after a failed put.
                let body = &s.headers.body;
                body.set_logical_len(body.logical_len()).unwrap();
            }
            arm(pick(&s), 1);
        }
        rbitcoin_log::capture_logs(true);
        let flushed = s.flush_for_shutdown(seal);
        let lines = rbitcoin_log::take_logs();
        rbitcoin_log::capture_logs(false);
        assert_eq!(flushed.is_err(), seal, "{name}: only a failed step errs");
        assert!(!marker.exists(), "{name}: the scripthash sync was skipped");
        assert_eq!(
            on_disk_hwm(&dir.join("confirmed.body")),
            sealed,
            "{name}: the tip was sealed past a failed prerequisite"
        );
        let step = |what: &str| {
            lines
                .iter()
                .position(|(_, l)| l.contains("shutdown flush") && l.contains(what))
                .unwrap_or_else(|| panic!("{name}: no `{what}` step in {lines:?}"))
        };
        assert!(step("scripthash") < step("tip seal"), "{name}: {lines:?}");
        assert!(step("tip seal") < step("Class A writeback"), "{name}");
        assert!(
            step("Class A writeback") < step("tx.head snapshot"),
            "{name}"
        );
        drop(s);
        let s = Store::open_tiny(&dir).unwrap();
        assert_eq!(s.confirmed.tip_height(), Some(Height(0)), "{name}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// A barrier in flight when another caller's Class A sync fails must not
/// commit after it. The failed fsync took the writeback error for the shared
/// file description, so the in-flight one can return 0 over dropped pages.
/// The tip barrier and the spend checkpoint can run together.
#[test]
fn class_a_sync_in_flight_does_not_pass_a_latched_failure() {
    use std::time::{Duration, Instant};
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    connect_coinbase(&s, &write, 0, Fk::NULL, [0u8; 32]);
    // The pre-check walks the files in order: hold the first caller on the
    // last one, and fail the second on the first, which the first has passed.
    let last = CLASS_A_FILES[CLASS_A_FILES.len() - 1].1(&s);
    let first = CLASS_A_FILES[0].1(&s);
    let hold = Duration::from_millis(400);
    let start = Instant::now();
    rbitcoin_store::io_handle::arm_sync_delay(last, hold);
    let (a_ok, b_ok, b_done) = std::thread::scope(|sc| {
        let a = sc.spawn(|| s.txs.sync_class_a_data().is_ok());
        std::thread::sleep(Duration::from_millis(100));
        rbitcoin_store::io_handle::arm_fail_sync(first, 1);
        let b_ok = s.txs.sync_class_a_data().is_ok();
        let b_done = start.elapsed();
        (a.join().unwrap(), b_ok, b_done)
    });
    assert!(!b_ok, "the armed sync did not fail");
    // A succeeded only if its sync ran before the failure: the failing
    // caller then returned after A's held sync, not during it. (If A started
    // late, the failure latched first and A refused.)
    assert!(
        !a_ok || b_done >= hold,
        "a sync in flight succeeded while another failed beside it ({b_done:?})"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The spend fold syncs `spent.body` on the fd the barrier syncs. If that
/// sync fails it may have taken the writeback error, so the Class A latch
/// closes: a later barrier sync on the fd can return 0.
#[test]
fn failed_fold_sync_latches_class_a() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let spent = s.txs.spent.body.io_handle();
    rbitcoin_store::io_handle::arm_fail_sync(spent, 1);
    assert!(s.txs.sync_folded_spent().is_err());
    rbitcoin_store::io_handle::disarm_sync(spent);
    assert!(
        s.txs.sync_class_a_data().is_err(),
        "a Class A sync passed after the fold's sync of the same fd failed"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The table flush (shutdown, input backfill, prune release) syncs Class A
/// fds too. After a failed Class A sync it refuses: `TableFile::flush`
/// retries, and a retry can return 0 over pages the kernel dropped.
#[test]
fn failed_class_a_sync_refuses_the_table_flush() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    connect_coinbase(&s, &write, 0, Fk::NULL, [0u8; 32]);
    let spent = s.txs.spent.body.io_handle();
    rbitcoin_store::io_handle::arm_fail_sync(spent, 1);
    assert!(s.txs.sync_class_a_data().is_err());
    rbitcoin_store::io_handle::disarm_sync(spent);
    assert!(
        s.txs.flush().is_err(),
        "the table flush synced Class A after a failed sync"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A failed Class A sync tells the store's watcher once, so the node can
/// stop instead of failing every later commit.
#[test]
fn failed_class_a_sync_notifies_the_watcher_once() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let hits = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let seen = std::sync::Arc::clone(&hits);
    s.txs.on_class_a_failure(move || {
        seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    });
    assert!(s.txs.sync_class_a_data().is_ok());
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 0);
    let spent = s.txs.spent.body.io_handle();
    rbitcoin_store::io_handle::arm_fail_sync(spent, 1);
    assert!(s.txs.sync_class_a_data().is_err());
    rbitcoin_store::io_handle::disarm_sync(spent);
    assert!(s.txs.sync_class_a_data().is_err());
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

type ClassAPick = fn(&Store) -> rbitcoin_store::io_handle::IoHandle;

/// Every Class A file the barrier syncs.
const CLASS_A_FILES: [(&str, ClassAPick); 16] = [
    ("txout.body", |s| s.txs.body.body.io_handle()),
    ("seqsigwit.body", |s| s.txs.seqsigwit.body.io_handle()),
    ("spent.body", |s| s.txs.spent.body.io_handle()),
    ("create.loc", |s| s.txs.create_loc.loc.io_handle()),
    ("create.loc.ovf", |s| s.txs.create_loc.ovf.io_handle()),
    ("create.off", |s| s.txs.create_loc.off.io_handle()),
    ("seqsigwit.loc", |s| s.txs.seqsigwit_loc.loc.io_handle()),
    ("seqsigwit.loc.ovf", |s| s.txs.seqsigwit_loc.ovf.io_handle()),
    ("seqsigwit.off", |s| s.txs.seqsigwit_loc.off.io_handle()),
    ("txid.body", |s| s.txs.txids.file.io_handle()),
    ("txstat.body", |s| s.txs.txstat.body.io_handle()),
    ("txstat.ovf", |s| s.txs.txstat.ovf.io_handle()),
    ("txstat.blk", |s| s.txs.txstat.blk.io_handle()),
    ("input.loc", |s| s.txs.input.loc.io_handle()),
    ("input.off", |s| s.txs.input.off.io_handle()),
    ("input.body", |s| s.txs.input.body.io_handle()),
];

/// Create 3 of the archive models: a spend of create 1 with a witness, so
/// the input stem holds a parent edge and the witness stem a real row.
fn spend_of_create_1() -> (TxRecord, Vec<InputRecord>, Vec<OutputRecord>) {
    (
        TxRecord {
            txid: [2u8; 32],
            version: 2,
            locktime: 0,
            input_count: 1,
            output_count: 1,
        },
        vec![InputRecord::prevout(
            [0u8; 32],
            Fk(1),
            0,
            7,
            vec![],
            vec![vec![9; 3]],
        )],
        vec![OutputRecord::unspent(40, vec![0x51])],
    )
}

/// Every kept create reads back whole: outputs, txid, stats, and each
/// input's parent edge, sequence, and witness.
fn assert_creates_read_back(
    s: &Store,
    written: &[(TxRecord, Vec<InputRecord>, Vec<OutputRecord>)],
    case: &str,
) {
    for fk in 1..=s.txs.count() {
        let (rec, ins, _) = &written[fk as usize - 1];
        let (_, got, _) = s
            .get_tx_full(Fk(fk))
            .unwrap_or_else(|e| panic!("{case}: kept create {fk} full read: {e}"));
        let edges = |ins: &[InputRecord]| -> Vec<_> {
            ins.iter()
                .map(|i| (i.parent(), i.sequence, i.witness.clone()))
                .collect()
        };
        assert_eq!(edges(&got), edges(ins), "{case}: kept create {fk} inputs");
        assert_eq!(
            s.txs.body_txid(Fk(fk)).unwrap(),
            rec.txid,
            "{case}: create {fk} txid"
        );
        if let Err(e) = s.txstat_row(Fk(fk)) {
            panic!("{case}: kept create {fk} stats: {e}");
        }
    }
}

fn store_image(dir: &std::path::Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
    let mut out = std::collections::BTreeMap::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for ent in std::fs::read_dir(&d).unwrap().flatten() {
            let p = ent.path();
            if p.is_dir() {
                stack.push(p);
            } else {
                let rel = p.strip_prefix(dir).unwrap().to_path_buf();
                out.insert(rel, std::fs::read(&p).unwrap());
            }
        }
    }
    out
}

fn write_image(dir: &std::path::Path, image: &std::collections::BTreeMap<PathBuf, Vec<u8>>) {
    let _ = std::fs::remove_dir_all(dir);
    for (rel, bytes) in image {
        let p = dir.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, bytes).unwrap();
    }
}

/// Power-loss model for the connect barrier.
///
/// Height 0 is sealed. The next block's archive is written with no barrier.
/// Each archive file then keeps either every write since the seal or none of
/// them. Every single dropped file, and every single kept file, is one
/// image. A file can also keep its header page and lose the rows behind it.
/// Each reopens at the sealed tip with exactly the sealed creates: nothing
/// durable names the rest, so open cuts it without reading it. Every kept
/// create reads back whole, and the next block seals on top. `tx.head`
/// keeps what it wrote: its `meta` is synced on insert and open repairs a
/// head that leads.
#[test]
fn each_unsynced_archive_file_reopens_at_the_sealed_tip() {
    archive_power_loss_model(false);
}

/// The same model after a spend commit named spender 3, whose block is still
/// in flight: no `header_txs` range covers creates 2 and 3. Create 4 is the
/// unsynced write. Open keeps exactly creates 1 to 3, whole.
#[test]
fn each_unsynced_archive_file_past_a_sync_mark_reopens_at_the_mark() {
    archive_power_loss_model(true);
}

fn archive_power_loss_model(marked: bool) {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    let (genesis, parent) = connect_coinbase(&s, &write, 0, Fk::NULL, [0u8; 32]);
    assert_eq!(s.flush_class_c_tip().unwrap(), Some(Height(0)));
    s.txs.head.flush().unwrap();
    let mut written = vec![coinbase(0), coinbase(1), spend_of_create_1()];
    let kept = if marked {
        s.put_tx_full_batch_indexed(&written[1..], false).unwrap();
        s.put_spend_create(Fk(1), 0, Fk(3), 0).unwrap();
        s.note_spend_snapshot(0);
        s.checkpoint_spend_through(0).unwrap();
        s.txs.head.flush().unwrap();
        3
    } else {
        1
    };
    let sealed = store_image(&dir);
    let mut later = spend_of_create_1();
    later.0.txid = [3u8; 32];
    let tail = if marked {
        written.push(later.clone());
        vec![later]
    } else {
        written[1..].to_vec()
    };
    s.put_tx_full_batch_indexed(&tail, false).unwrap();
    drop(s);
    let torn = store_image(&dir);
    let changed: Vec<&PathBuf> = torn
        .iter()
        .filter(|(rel, bytes)| sealed.get(*rel) != Some(*bytes))
        .filter(|(rel, _)| !rel.starts_with("tx.head"))
        .map(|(rel, _)| rel)
        .collect();
    assert!(changed.len() >= 8, "archive files written: {changed:?}");
    let n = changed.len();
    let all = (1u32 << n) - 1;
    // (dropped files, file whose header page landed without its new rows)
    let mut cases = vec![(0, None), (all, None)];
    for bit in 0..n {
        cases.extend([(1u32 << bit, None), (all & !(1u32 << bit), None)]);
        cases.extend([(0, Some(bit)), (all & !(1u32 << bit), Some(bit))]);
    }
    let header = rbitcoin_store::file::FILE_HEADER_LEN;
    let img = tmp();
    for (mask, header_only) in cases {
        let mut image = torn.clone();
        let mut dropped = Vec::new();
        for (bit, rel) in changed.iter().enumerate() {
            if header_only == Some(bit) {
                dropped.push(format!("{} rows", rel.display()));
                let new = &torn[*rel];
                let old = sealed.get(*rel).cloned().unwrap_or_default();
                let mut bytes = vec![0u8; new.len()];
                let keep = old.len().min(new.len());
                bytes[..keep].copy_from_slice(&old[..keep]);
                let head = header.min(new.len());
                bytes[..head].copy_from_slice(&new[..head]);
                image.insert((*rel).clone(), bytes);
                continue;
            }
            if mask & (1 << bit) == 0 {
                continue;
            }
            dropped.push(rel.display().to_string());
            match sealed.get(*rel) {
                Some(bytes) => image.insert((*rel).clone(), bytes.clone()),
                None => image.remove(*rel),
            };
        }
        let case = format!("marked={marked} dropped {dropped:?}");
        write_image(&img, &image);
        let s = Store::open_tiny(&img).unwrap_or_else(|e| panic!("{case}: open failed: {e}"));
        assert_eq!(s.confirmed.tip_height(), Some(Height(0)), "{case}");
        assert_eq!(
            s.txs.count(),
            kept,
            "{case}: open keeps exactly the creates a durable row names"
        );
        assert_creates_read_back(&s, &written, &case);
        connect_coinbase(&s, &write, 1, genesis, parent);
        assert_eq!(
            s.flush_class_c_tip()
                .unwrap_or_else(|e| panic!("{case}: next seal: {e}")),
            Some(Height(1)),
            "{case}"
        );
    }
    let _ = std::fs::remove_dir_all(&img);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A row big enough to need an overflow tail: fee, base, and witness ULEBs
/// past the 8-byte cell.
fn wide_txstat() -> rbitcoin_store::TxStatRow {
    rbitcoin_store::TxStatRow {
        fee_sat: u64::MAX >> 1,
        base: u32::MAX,
        wit_extra: 7,
    }
}

/// A stats stamp after the barrier (`getblockstats` fills unstamped rows)
/// writes the cells, then the overflow blob, then its slot in `txstat.blk`,
/// with no sync until the next barrier. Power loss can keep any of them.
/// Each image reopens and reads the row as unstamped or as the stamp, never
/// an error, so the RPC falls back and restamps; the restamp reads back.
#[test]
fn every_torn_txstat_stamp_reads_unstamped_and_restamps() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    let (genesis, parent) = connect_coinbase(&s, &write, 0, Fk::NULL, [0u8; 32]);
    let (h1, _) = connect_coinbase(&s, &write, 1, genesis, parent);
    assert_eq!(s.flush_class_c_tip().unwrap(), Some(Height(1)));
    let fk = s.header_txs.get_list(h1).unwrap().unwrap()[0];
    drop(s);
    let sealed = store_image(&dir);
    let wide = wide_txstat();
    assert!(rbitcoin_store::txstat::pack_cell(&wide)
        .unwrap()
        .1
        .is_some());
    let s = Store::open_tiny(&dir).unwrap();
    s.write_txstat_block(h1, fk.0, &[wide]).unwrap();
    assert_eq!(s.txstat_row(fk).unwrap(), Some(wide), "control");
    drop(s);
    let stamped = store_image(&dir);
    let changed: Vec<PathBuf> = stamped
        .iter()
        .filter(|(rel, bytes)| sealed.get(*rel) != Some(*bytes))
        .filter(|(rel, _)| {
            let name = rel.to_string_lossy();
            name.starts_with("txstat.")
        })
        .map(|(rel, _)| rel.clone())
        .collect();
    assert_eq!(changed.len(), 3, "stamp writes: {changed:?}");
    let header = rbitcoin_store::file::FILE_HEADER_LEN;
    let mut cases: Vec<(u32, Option<usize>)> = (0..8).map(|mask| (mask, None)).collect();
    cases.extend((0..3).map(|bit| (0, Some(bit))));
    let img = tmp();
    for (dropped, header_only) in cases {
        let mut image = stamped.clone();
        let mut case = Vec::new();
        for (bit, rel) in changed.iter().enumerate() {
            let old = sealed.get(rel).cloned().unwrap_or_default();
            if header_only == Some(bit) {
                let new = &stamped[rel];
                let mut bytes = vec![0u8; new.len()];
                let keep = old.len().min(new.len());
                bytes[..keep].copy_from_slice(&old[..keep]);
                let head = header.min(new.len());
                bytes[..head].copy_from_slice(&new[..head]);
                image.insert(rel.clone(), bytes);
                case.push(format!("{} rows", rel.display()));
            } else if dropped & (1 << bit) != 0 {
                image.insert(rel.clone(), old);
                case.push(rel.display().to_string());
            }
        }
        write_image(&img, &image);
        let s = Store::open_tiny(&img).unwrap_or_else(|e| panic!("lost {case:?}: open: {e}"));
        let row = s
            .txstat_row(fk)
            .unwrap_or_else(|e| panic!("lost {case:?}: row read: {e}"));
        assert!(row.is_none() || row == Some(wide), "lost {case:?}: {row:?}");
        let range = s
            .txstat_range(h1, fk.0, fk.0)
            .unwrap_or_else(|e| panic!("lost {case:?}: range read: {e}"));
        assert_eq!(range, vec![row], "lost {case:?}");
        s.write_txstat_block(h1, fk.0, &[wide]).unwrap();
        assert_eq!(
            s.txstat_row(fk).unwrap(),
            Some(wide),
            "lost {case:?}: restamp"
        );
        assert_eq!(s.txstat_range(h1, fk.0, fk.0).unwrap(), vec![Some(wide)]);
    }
    let _ = std::fs::remove_dir_all(&img);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A stats stamp syncs `txstat` files the Class A barrier syncs. A failed
/// sync there latches Class A as a barrier's does: a retried fsync can
/// succeed after the kernel dropped the dirty pages, and the failed one may
/// have taken an append's writeback error. A stamp the store refuses (a
/// create past the count) is not a failed sync and does not latch.
#[test]
fn failed_txstat_stamp_sync_latches_class_a() {
    let picks: [(&str, ClassAPick); 3] = [
        ("txstat.body", |s| s.txs.txstat.body.io_handle()),
        ("txstat.ovf", |s| s.txs.txstat.ovf.io_handle()),
        ("txstat.blk", |s| s.txs.txstat.blk.io_handle()),
    ];
    for (name, pick) in picks {
        let dir = tmp();
        let s = Store::create_tiny(&dir).unwrap();
        let write = rbitcoin_store::ClassCWrite::for_test();
        let (genesis, parent) = connect_coinbase(&s, &write, 0, Fk::NULL, [0u8; 32]);
        let (h1, _) = connect_coinbase(&s, &write, 1, genesis, parent);
        assert_eq!(s.flush_class_c_tip().unwrap(), Some(Height(1)));
        let fk = s.header_txs.get_list(h1).unwrap().unwrap()[0];
        let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen = std::sync::Arc::clone(&fired);
        s.txs.on_class_a_failure(move || {
            seen.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        let wide = wide_txstat();
        assert!(s.write_txstat_block(h1, fk.0 + 10, &[wide]).is_err());
        assert!(!fired.load(std::sync::atomic::Ordering::SeqCst), "{name}");
        s.txs.sync_class_a_data().unwrap();

        rbitcoin_store::io_handle::arm_fail_sync(pick(&s), 1);
        assert!(
            s.write_txstat_block(h1, fk.0, &[wide]).is_err(),
            "{name}: the stamp did not sync it"
        );
        assert!(fired.load(std::sync::atomic::Ordering::SeqCst), "{name}");
        assert!(
            s.txs.sync_class_a_data().is_err(),
            "{name}: a barrier synced over a failed stamp sync"
        );
        assert!(s.write_txstat_block(h1, fk.0, &[wide]).is_err(), "{name}");
        drop(s);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// A create open keeps past the committed floor can carry a stamped cell
/// whose tail was in a block that never committed. It has no confirmed
/// height to find that tail by, so its row reads as unstamped.
#[test]
fn stamped_txstat_of_an_unconfirmed_create_reads_unstamped() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    connect_coinbase(&s, &write, 0, Fk::NULL, [0u8; 32]);
    let (rec, ins, outs) = coinbase(1);
    let fk = s
        .put_tx_full_batch_indexed(&[(rec, ins, outs)], false)
        .unwrap()[0];
    let (cell, tail) = rbitcoin_store::txstat::pack_cell(&wide_txstat()).unwrap();
    assert!(tail.is_some());
    let off = rbitcoin_store::txstat::TxStat::entry_offset(fk.0).unwrap();
    s.txs.txstat.body.write_at_pwrite(off, &cell).unwrap();
    assert_eq!(s.txstat_row(fk).unwrap(), None);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Power loss during the Class C flush, after the Class A barrier.
///
/// `header_txs.first`, `header_txs.count`, `confirmed[]` and the strong
/// bits are separate files flushed one after another. Each one's new block
/// row may land, not land, or land as a length header over zeroed rows.
/// Every mix must reopen at tip 0 or 1, keep create 2 exactly when a
/// durable row still names it, leave no strong bit past what it keeps, and
/// seal the next blocks.
#[test]
fn every_class_c_subset_after_the_barrier_reopens_and_seals() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    let (genesis, parent) = connect_coinbase(&s, &write, 0, Fk::NULL, [0u8; 32]);
    assert_eq!(s.flush_class_c_tip().unwrap(), Some(Height(0)));
    s.txs.head.flush().unwrap();
    let sealed = store_image(&dir);
    let (one, one_hash) = connect_coinbase(&s, &write, 1, genesis, parent);
    assert_eq!(s.flush_class_c_tip().unwrap(), Some(Height(1)));
    drop(s);
    let committed = store_image(&dir);
    let class_c = [
        "confirmed.body",
        "header_txs_first.body",
        "header_txs_count.body",
        "strong_tx.body",
        "tip_seal",
    ];
    let changed: Vec<&PathBuf> = committed
        .iter()
        .filter(|(rel, bytes)| sealed.get(*rel) != Some(*bytes))
        .filter(|(rel, _)| class_c.iter().any(|c| rel.ends_with(c)))
        .map(|(rel, _)| rel)
        .collect();
    assert_eq!(
        changed.len(),
        class_c.len(),
        "Class C files written: {changed:?}"
    );
    let n = changed.len();
    let all = (1u32 << n) - 1;
    let header = rbitcoin_store::file::FILE_HEADER_LEN;
    let img = tmp();
    for mask in 0..=all {
        for header_only in std::iter::once(None).chain((0..n).map(Some)) {
            if header_only.is_some_and(|b| mask & (1 << b) != 0) {
                continue;
            }
            let mut image = committed.clone();
            let mut torn = Vec::new();
            for (bit, rel) in changed.iter().enumerate() {
                let old = sealed.get(*rel).cloned().unwrap_or_default();
                if header_only == Some(bit) {
                    torn.push(format!("{} rows", rel.display()));
                    let new = &committed[*rel];
                    let mut bytes = vec![0u8; new.len()];
                    let keep = old.len().min(new.len());
                    bytes[..keep].copy_from_slice(&old[..keep]);
                    let head = header.min(new.len());
                    bytes[..head].copy_from_slice(&new[..head]);
                    image.insert((*rel).clone(), bytes);
                } else if mask & (1 << bit) != 0 {
                    torn.push(rel.display().to_string());
                    image.insert((*rel).clone(), old);
                }
            }
            write_image(&img, &image);
            let s = Store::open_tiny(&img)
                .unwrap_or_else(|e| panic!("lost {torn:?}: open failed: {e}"));
            let tip = s.confirmed.tip_height();
            assert!(
                tip == Some(Height(0)) || tip == Some(Height(1)),
                "lost {torn:?}: tip {tip:?}"
            );
            // Create 2 stays exactly when a durable row names it. A cut
            // number leaves no strong bit for the next create to inherit.
            let named = !torn.iter().any(|t| t.starts_with("header_txs"));
            assert_eq!(s.txs.count(), if named { 2 } else { 1 }, "lost {torn:?}");
            assert!(
                !s.is_confirmed_strong(Fk(s.txs.count() + 1)).unwrap(),
                "lost {torn:?}: a strong bit past the kept creates"
            );
            let (two_prev, two_parent) = if tip == Some(Height(0)) {
                let (fk, hash) = connect_coinbase(&s, &write, 1, genesis, parent);
                assert_eq!(
                    s.flush_class_c_tip()
                        .unwrap_or_else(|e| panic!("lost {torn:?}: reseal 1: {e}")),
                    Some(Height(1)),
                    "lost {torn:?}"
                );
                (fk, hash)
            } else {
                (one, one_hash)
            };
            connect_coinbase(&s, &write, 2, two_prev, two_parent);
            assert_eq!(
                s.flush_class_c_tip()
                    .unwrap_or_else(|e| panic!("lost {torn:?}: seal 2: {e}")),
                Some(Height(2)),
                "lost {torn:?}"
            );
        }
    }
    let _ = std::fs::remove_dir_all(&img);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A spend delta commit never names a spender whose archive is not synced.
///
/// In IBD the spender is strong on the RAM tip while its barrier is still in
/// flight. If its archive bytes are lost, open cuts that create and another
/// block can reuse the number; the reopen filter then sees a strong spender
/// and keeps the stale spend. The checkpoint syncs Class A after it freezes
/// the notes and before it renames the commit.
#[test]
fn spend_commit_waits_for_the_spender_archive_sync() {
    for fail in [false, true] {
        let dir = tmp();
        let s = Store::create_tiny(&dir).unwrap();
        let write = rbitcoin_store::ClassCWrite::for_test();
        let (genesis, parent_hash) = connect_coinbase(&s, &write, 0, Fk::NULL, [0u8; 32]);
        s.height_fence_extend(Height(0), genesis).unwrap();
        assert_eq!(s.flush_class_c_tip().unwrap(), Some(Height(0)));
        let parent = s.header_txs.get_list(genesis).unwrap().unwrap()[0];
        let (h1, _) = connect_coinbase(&s, &write, 1, genesis, parent_hash);
        s.height_fence_extend(Height(1), h1).unwrap();
        let spender = s.header_txs.get_list(h1).unwrap().unwrap()[0];
        assert!(s.is_confirmed_strong(spender).unwrap());
        s.put_spend_create(parent, 0, spender, 0).unwrap();
        s.note_spend_snapshot(1);
        let commit = dir
            .join(rbitcoin_store::spend_delta::SPEND_DELTA_DIR)
            .join(rbitcoin_store::spend_delta::SPEND_DELTA_COMMIT_NAME);
        let before = std::fs::read(&commit).ok();
        if fail {
            rbitcoin_store::io_handle::arm_fail_sync(s.txs.txids.file.io_handle(), 1);
            assert!(s.checkpoint_spend_through(1).is_err());
            assert_eq!(
                std::fs::read(&commit).ok(),
                before,
                "the delta commit named spender {spender:?} before its archive was synced"
            );
        } else {
            s.checkpoint_spend_through(1).unwrap();
            assert_ne!(
                std::fs::read(&commit).ok(),
                before,
                "control: the checkpoint wrote no commit"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Class C sealed height 1, then its body headers fell back to height 0.
///
/// The barrier rules this out for this binary, so an older one tore it.
/// Its committed rows name create 2: cutting it would let a later
/// transaction reuse that number under stale spends and postings. Open
/// refuses and asks for a reindex, and keeps refusing on the next try.
#[test]
fn body_header_short_of_a_committed_create_refuses() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    let (genesis, parent) = connect_coinbase(&s, &write, 0, Fk::NULL, [0u8; 32]);
    s.flush_class_c_tip().unwrap();
    let tx_end = on_disk_hwm(&dir.join("txout.body"));
    let sp_end = on_disk_hwm(&dir.join("spent.body"));
    let in_end = on_disk_hwm(&dir.join("seqsigwit.body"));
    connect_coinbase(&s, &write, 1, genesis, parent);
    assert_eq!(s.flush_class_c_tip().unwrap(), Some(Height(1)));
    drop(s);
    write_hwm(&dir.join("txout.body"), tx_end);
    write_hwm(&dir.join("spent.body"), sp_end);
    write_hwm(&dir.join("seqsigwit.body"), in_end);
    for attempt in 0..2 {
        assert_refuses_committed_cut(&dir, attempt);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Open refuses before it changes any Class A file: on this binary the
/// image is the evidence of a device that lost synced writes.
fn assert_refuses_committed_cut(dir: &std::path::Path, attempt: u32) {
    let class_a = |dir: &std::path::Path| {
        let mut image = store_image(dir);
        image.retain(|rel, _| {
            CLASS_A_FILES
                .iter()
                .any(|(name, _)| rel == std::path::Path::new(name))
        });
        image
    };
    let before = class_a(dir);
    assert_eq!(
        before.len(),
        CLASS_A_FILES.len(),
        "Class A files: {:?}",
        before.keys()
    );
    let opened = Store::open_tiny(dir);
    for (rel, bytes) in class_a(dir) {
        assert!(
            before.get(&rel) == Some(&bytes),
            "open {attempt} changed {} before it refused",
            rel.display()
        );
    }
    match opened {
        Err(rbitcoin_store::StoreError::Corrupt(m)) if m.contains("reindex") => {}
        Err(e) => panic!("open {attempt} returned {e}"),
        Ok(s) => panic!(
            "open {attempt} cut committed creates: count {} tip {:?}",
            s.txs.count(),
            s.confirmed.tip_height()
        ),
    }
}
