//! Class A appends under the sector rule: no page images, and open keeps
//! only the creates a durable record names.
use rbitcoin_primitives::Fk;
use rbitcoin_store::store::Store;
use rbitcoin_store::tx_table::{InputRecord, OutputRecord, TxRecord, TxTable};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

const PAGE: usize = 4096;

fn tmp() -> std::path::PathBuf {
    let p = rbitcoin_store::testutil::fresh_path("class-a-tail");
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn coinbase(txid: [u8; 32]) -> (TxRecord, Vec<InputRecord>, Vec<OutputRecord>) {
    (
        TxRecord {
            txid,
            version: 1,
            locktime: 0,
            input_count: 1,
            output_count: 1,
        },
        vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])],
        vec![OutputRecord::unspent(50, vec![0x51])],
    )
}

fn logical_of(path: &Path) -> u64 {
    let mut header = [0u8; 16];
    let mut file = std::fs::File::open(path).unwrap();
    std::io::Read::read_exact(&mut file, &mut header).unwrap();
    u64::from_le_bytes(header[8..16].try_into().unwrap())
}

fn read_at(path: &Path, offset: u64, buf: &mut [u8]) {
    let mut file = std::fs::File::open(path).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    std::io::Read::read_exact(&mut file, buf).unwrap();
}

/// A Class A barrier syncs the stems and writes no page image: an append
/// leaves committed sectors as they were, and open keeps only the creates
/// a durable record names.
#[test]
fn a_class_a_barrier_writes_no_page_image() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    let batch: Vec<_> = (1..=130u64).map(|i| coinbase([i as u8; 32])).collect();
    s.put_tx_full_batch_indexed(&batch, false).unwrap();
    s.txs.sync_class_a_named(130).unwrap();
    s.put_tx_full_batch_indexed(&[coinbase([0xf1u8; 32])], false)
        .unwrap();
    s.txs.sync_class_a_data().unwrap();
    drop(s);
    assert!(
        !dir.join("tear_guard").exists(),
        "a barrier wrote tear_guard/"
    );
    let s = Store::open_tiny(&dir).unwrap();
    assert_eq!(s.txs.txids.get(Fk(130)).unwrap(), [130u8; 32]);
    drop(s);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Sync-array order of the build that imaged Class A pages.
const LEGACY_STEMS: [&str; 16] = [
    "txout.body",
    "seqsigwit.body",
    "spent.body",
    "create.loc",
    "create.loc.ovf",
    "create.off",
    "seqsigwit.loc",
    "seqsigwit.loc.ovf",
    "seqsigwit.off",
    "txid.body",
    "txstat.body",
    "txstat.ovf",
    "txstat.blk",
    "input.loc",
    "input.off",
    "input.body",
];
const LEGACY_TAIL_BYTES: usize = 17 * PAGE;

fn sha(parts: &[&[u8]]) -> [u8; 32] {
    use bitcoin_hashes::{sha256, Hash, HashEngine};
    let mut eng = sha256::HashEngine::default();
    for part in parts {
        eng.input(part);
    }
    sha256::Hash::from_engine(eng).to_byte_array()
}

fn legacy_tail_span(published: u64, file_len: u64) -> (u64, usize) {
    let page = PAGE as u64;
    if published == 0 || published.is_multiple_of(page) || published > file_len {
        return (0, 0);
    }
    let off = published & !(page - 1);
    (off, (published - off) as usize)
}

fn legacy_seal_tail(buf: &mut [u8]) {
    let sum = sha(&[&buf[..24], &buf[56..]]);
    buf[24..56].copy_from_slice(&sum);
}

/// `tail.N` as the imaging build's barrier wrote it: each stem's published
/// length and the partial last block below it, read from disk.
fn legacy_tail_slot(dir: &Path, generation: u64) -> Vec<u8> {
    let mut buf = vec![0u8; LEGACY_TAIL_BYTES];
    buf[0..4].copy_from_slice(b"CAPT");
    buf[4..6].copy_from_slice(&1u16.to_le_bytes());
    buf[6..8].copy_from_slice(&16u16.to_le_bytes());
    buf[8..16].copy_from_slice(&generation.to_le_bytes());
    for (i, name) in LEGACY_STEMS.iter().enumerate() {
        let path = dir.join(name);
        let Ok(meta) = std::fs::metadata(&path) else {
            continue;
        };
        let file_len = meta.len();
        let published = if file_len >= 16 { logical_of(&path) } else { 0 };
        let (off, n) = legacy_tail_span(published, file_len);
        let head = 56 + i * 16;
        buf[head..head + 8].copy_from_slice(&published.to_le_bytes());
        buf[head + 8..head + 16].copy_from_slice(&off.to_le_bytes());
        buf[312 + i * 2..314 + i * 2].copy_from_slice(&(n as u16).to_le_bytes());
        if n > 0 {
            let at = PAGE + i * PAGE;
            read_at(&path, off, &mut buf[at..at + n]);
            if off == 0 && n >= 16 {
                buf[at + 8..at + 16].copy_from_slice(&published.to_le_bytes());
            }
        }
    }
    legacy_seal_tail(&mut buf);
    buf
}

/// A `tail.N` the imaging build created and had not filled yet.
fn legacy_vacant_tail() -> Vec<u8> {
    let mut buf = vec![0u8; LEGACY_TAIL_BYTES];
    buf[0..4].copy_from_slice(b"CAPT");
    buf[4..6].copy_from_slice(&1u16.to_le_bytes());
    buf[6..8].copy_from_slice(&16u16.to_le_bytes());
    legacy_seal_tail(&mut buf);
    buf
}

/// `first` with page 0 of every stem past one page sealed, as the imaging
/// build's open left it.
fn legacy_first(dir: &Path) -> Vec<u8> {
    let mut first = vec![0u8; 16 * 2 * PAGE];
    for (i, name) in LEGACY_STEMS.iter().enumerate() {
        let path = dir.join(name);
        let Ok(meta) = std::fs::metadata(&path) else {
            continue;
        };
        if meta.len() < 16 || logical_of(&path) < PAGE as u64 {
            continue;
        }
        let at = i * 2 * PAGE;
        let n = (meta.len() as usize).min(PAGE);
        read_at(&path, 0, &mut first[at..at + n]);
        let image_sum = sha(&[&first[at..at + PAGE]]);
        let commit = &mut first[at + PAGE..at + 2 * PAGE];
        commit[0..4].copy_from_slice(b"CAPF");
        commit[4..6].copy_from_slice(&(i as u16).to_le_bytes());
        commit[6..8].copy_from_slice(&(PAGE as u16).to_le_bytes());
        commit[8..40].copy_from_slice(&image_sum);
        let sum = sha(&[&commit[..40]]);
        commit[40..72].copy_from_slice(&sum);
    }
    first
}

fn write_legacy_guard(dir: &Path) {
    let guard = dir.join("tear_guard");
    std::fs::create_dir_all(&guard).unwrap();
    std::fs::write(guard.join("first"), legacy_first(dir)).unwrap();
    std::fs::write(guard.join("tail.0"), legacy_tail_slot(dir, 1)).unwrap();
    std::fs::write(guard.join("tail.1"), legacy_vacant_tail()).unwrap();
}

fn zero_at(path: &Path, offset: u64, len: usize) {
    let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&vec![0u8; len]).unwrap();
    file.sync_all().unwrap();
}

fn store_130(dir: &Path) {
    let s = Store::create_tiny(dir).unwrap();
    let batch: Vec<_> = (1..=130u64).map(|i| coinbase([i as u8; 32])).collect();
    s.put_tx_full_batch_indexed(&batch, false).unwrap();
    s.txs.sync_class_a_named(130).unwrap();
}

fn is_corrupt(r: Result<Store, rbitcoin_store::StoreError>) -> bool {
    matches!(r, Err(rbitcoin_store::StoreError::Corrupt(_)))
}

/// A crash under the build that imaged Class A pages can leave a committed
/// page zeroed, with its only synced copy in `tear_guard/`. Open writes
/// page 0 back from `first` and the partial last block from the newer tail
/// slot, then removes the guard.
#[test]
fn open_restores_a_torn_page_from_a_legacy_tear_guard() {
    let dir = tmp();
    store_130(&dir);
    let txid = dir.join("txid.body");
    let before = std::fs::read(&txid).unwrap();
    let published = logical_of(&txid);
    let (off, n) = legacy_tail_span(published, before.len() as u64);
    assert!(off >= PAGE as u64 && n > 0, "txid.body has no partial tail");
    write_legacy_guard(&dir);
    zero_at(&txid, 0, PAGE);
    zero_at(&txid, off, n);
    let s = Store::open_tiny(&dir).unwrap();
    assert_eq!(s.txs.txids.get(Fk(1)).unwrap(), [1u8; 32]);
    assert_eq!(s.txs.txids.get(Fk(130)).unwrap(), [130u8; 32]);
    drop(s);
    assert!(!dir.join("tear_guard").exists(), "open left tear_guard/");
    assert_eq!(std::fs::read(&txid).unwrap(), before);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A guard the imaging build left after a clean stop, and a `.kept` over a
/// range that is not zeroes, change no stem. Open removes them.
#[test]
fn open_drops_a_clean_legacy_tear_guard() {
    let dir = tmp();
    store_130(&dir);
    let before = std::fs::read(dir.join("txid.body")).unwrap();
    write_legacy_guard(&dir);
    let mut kept = Vec::new();
    kept.extend_from_slice(b"CATK");
    kept.extend_from_slice(&0u64.to_le_bytes());
    kept.extend_from_slice(&(PAGE as u32).to_le_bytes());
    kept.extend_from_slice(&[0x5au8; PAGE]);
    std::fs::write(dir.join("txid.body.kept"), kept).unwrap();
    let s = Store::open_tiny(&dir).unwrap();
    assert_eq!(s.txs.txids.get(Fk(130)).unwrap(), [130u8; 32]);
    drop(s);
    assert!(!dir.join("tear_guard").exists(), "open left tear_guard/");
    assert!(
        !dir.join("txid.body.kept").exists(),
        "open left the .kept sidecar"
    );
    assert_eq!(std::fs::read(dir.join("txid.body")).unwrap(), before);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Two tail slots that both fail to decode, a `.kept` that is not a `CATK`
/// image, or a sealed page 0 whose commit fails its checksum may hold the
/// only copy of a page. Open refuses and leaves them on disk.
#[test]
fn open_refuses_an_unparsable_legacy_image() {
    let dir = tmp();
    store_130(&dir);
    let guard = dir.join("tear_guard");
    std::fs::create_dir_all(&guard).unwrap();
    std::fs::write(guard.join("tail.0"), vec![0x5au8; LEGACY_TAIL_BYTES]).unwrap();
    std::fs::write(guard.join("tail.1"), vec![0x5au8; LEGACY_TAIL_BYTES]).unwrap();
    assert!(
        is_corrupt(Store::open_tiny(&dir)),
        "open took two bad slots"
    );
    assert!(guard.join("tail.0").exists(), "open removed a bad slot");
    std::fs::remove_dir_all(&guard).unwrap();
    std::fs::write(dir.join("txid.body.kept"), b"CATK").unwrap();
    assert!(is_corrupt(Store::open_tiny(&dir)), "open took a bad .kept");
    assert!(
        dir.join("txid.body.kept").exists(),
        "open removed a bad .kept"
    );
    std::fs::remove_file(dir.join("txid.body.kept")).unwrap();
    write_legacy_guard(&dir);
    let first = guard.join("first");
    let mut raw = std::fs::read(&first).unwrap();
    raw[9 * 2 * PAGE + PAGE + 8] ^= 1;
    std::fs::write(&first, raw).unwrap();
    assert!(is_corrupt(Store::open_tiny(&dir)), "open took a bad seal");
    assert!(first.exists(), "open removed a bad seal");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A published tail of zeros is data: a coinbase edge packs as eight zero
/// bytes, and an unstamped txstat cell is zeros. Open keeps those bytes.
#[test]
fn open_keeps_a_zero_coinbase_tail() {
    let dir = tmp();
    let n = 511u64;
    {
        let s = Store::create_tiny(&dir).unwrap();
        let batch: Vec<_> = (1..=n).map(|i| coinbase([i as u8; 32])).collect();
        s.put_tx_full_batch_indexed(&batch, false).unwrap();
        s.txs.sync_class_a_named(n).unwrap();
    }
    let input = dir.join("input.body");
    let txstat = dir.join("txstat.body");
    assert!(logical_of(&input) > 4096);
    assert!(logical_of(&txstat) > 4096);
    let s = Store::open_tiny(&dir).unwrap();
    assert_eq!(
        s.input_edges(Fk(n)).unwrap(),
        Some(vec![rbitcoin_store::input::InputEdge::coinbase()])
    );
    assert_eq!(s.txstat_row(Fk(n)).unwrap(), None);
    let _ = std::fs::remove_dir_all(&dir);
}

/// An empty seqsigwit is an 8-byte zero pad so loc strides stay monotone.
/// Open keeps that zero tail as data.
#[test]
fn open_keeps_a_zero_empty_seqsigwit_tail() {
    let dir = tmp();
    let n = 520u64;
    {
        let t = TxTable::create_tiny(&dir).unwrap();
        let batch: Vec<_> = (1..=n)
            .map(|i| {
                let mut txid = [0u8; 32];
                txid[0] = i as u8;
                txid[1] = (i >> 8) as u8;
                (
                    TxRecord {
                        txid,
                        version: 1,
                        locktime: 0,
                        input_count: 0,
                        output_count: 1,
                    },
                    Vec::new(),
                    vec![OutputRecord::unspent(0, vec![0x51])],
                )
            })
            .collect();
        t.put_full_batch_indexed(&batch, false).unwrap();
        t.flush().unwrap();
        assert!(logical_of(&dir.join("seqsigwit.body")) > 4096);
    }
    let t = TxTable::open_tiny(&dir).unwrap();
    assert_eq!(t.count(), n);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Unspent slots are zero bytes. A pre-schema-30 `spent.body` is filled out
/// to its published end, so the last page is those zeros. Open keeps them.
#[test]
fn open_keeps_a_zero_unspent_spent_tail() {
    let dir = tmp();
    let n_out = 600u32;
    {
        let t = TxTable::create_tiny(&dir).unwrap();
        t.put_full_batch_indexed(
            &[(
                TxRecord {
                    txid: [0x11; 32],
                    version: 1,
                    locktime: 0,
                    input_count: 1,
                    output_count: n_out,
                },
                vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])],
                vec![OutputRecord::unspent(1, vec![0x51]); n_out as usize],
            )],
            false,
        )
        .unwrap();
        let end = t.spent.body_published_len();
        assert!(end > 4096);
        drop(t);
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(dir.join("spent.body"))
            .unwrap();
        if file.metadata().unwrap().len() < end {
            file.set_len(end).unwrap();
        }
    }
    let t = TxTable::open_tiny(&dir).unwrap();
    assert_eq!(t.count(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A panic inside a Class A append still releases its writer count and
/// latches the failure. A sync on another thread then refuses instead of
/// waiting forever for a writer that is gone.
#[cfg(debug_assertions)]
#[test]
fn a_panicking_append_releases_its_writer_count() {
    let dir = tmp();
    let s = std::sync::Arc::new(Store::create_tiny(&dir).unwrap());
    rbitcoin_store::tx_table::set_after_bodies_for_test(Some(Box::new(|| {
        panic!("injected panic inside a Class A append");
    })));
    let appending = std::sync::Arc::clone(&s);
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        appending.put_tx_full_batch_indexed(&[coinbase([0x55u8; 32])], false)
    }));
    assert!(unwound.is_err(), "the hook did not run inside the append");
    assert_eq!(
        rbitcoin_store::testutil::class_a_writers(&s.txs),
        0,
        "the unwound append kept its writer count"
    );
    let (tx, rx) = std::sync::mpsc::channel();
    let syncing = std::sync::Arc::clone(&s);
    std::thread::spawn(move || {
        let _ = tx.send(syncing.txs.sync_class_a_data().is_err());
    });
    let refused = rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("a sync waited for a writer that unwound");
    assert!(refused, "a sync sealed over an append that panicked");
    let _ = std::fs::remove_dir_all(&dir);
}

/// An append can latch its failure after a sync passed the latch check at
/// entry and before the sync reads a zero writer count. The sync refuses
/// to seal `class_a.synced` over it.
#[cfg(debug_assertions)]
#[test]
fn sync_refuses_a_failure_latched_before_the_writer_count() {
    let dir = tmp();
    let s = Store::create_tiny(&dir).unwrap();
    s.put_tx_full_batch_indexed(&[coinbase([0x44u8; 32])], false)
        .unwrap();
    let synced = rbitcoin_store::tx_table::read_class_a_synced(&dir).unwrap();
    rbitcoin_store::testutil::set_before_class_a_seal(Some(Box::new(|txs| {
        rbitcoin_store::testutil::latch_class_a_failure(txs);
    })));
    // Naming the create would raise class_a.synced if the seal ran.
    assert!(
        s.txs.sync_class_a_named(1).is_err(),
        "a sync sealed over a latched append failure"
    );
    assert_eq!(
        rbitcoin_store::tx_table::read_class_a_synced(&dir).unwrap(),
        synced,
        "a sync moved class_a.synced over a failed append"
    );
    drop(s);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A mixed `spent.body` page beside a present `spent.delta/fold` comes back
/// as the fold image. The fold replay, not a page mark, repairs this file.
#[test]
fn mixed_spent_body_page_under_fold_reopens_as_the_fold_image() {
    let dir = tmp();
    {
        let s = Store::create_tiny(&dir).unwrap();
        let outs = vec![OutputRecord::unspent(50, vec![0x51]); 512];
        s.put_tx_full_batch_indexed(
            &[(
                TxRecord {
                    txid: [0x62u8; 32],
                    version: 1,
                    locktime: 0,
                    input_count: 1,
                    output_count: 512,
                },
                vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])],
                outs,
            )],
            false,
        )
        .unwrap();
        s.txs.sync_class_a_named(1).unwrap();
    }
    let body = dir.join("spent.body");
    let published = logical_of(&body);
    assert!(published > 4096, "spent.body published {published}");
    let keep = (published - 4096) as usize;
    {
        let mut file = std::fs::OpenOptions::new().write(true).open(&body).unwrap();
        file.seek(SeekFrom::Start(4096)).unwrap();
        file.write_all(&vec![0xabu8; keep]).unwrap();
        file.sync_all().unwrap();
    }
    let delta = dir.join(rbitcoin_store::spend_delta::SPEND_DELTA_DIR);
    let mut fold = Vec::new();
    fold.extend_from_slice(&rbitcoin_primitives::STORE_MAGIC);
    fold.extend_from_slice(&rbitcoin_primitives::FILE_SCHEMA_VERSION.to_le_bytes());
    fold.extend_from_slice(&0u16.to_le_bytes());
    fold.extend_from_slice(&1u32.to_le_bytes());
    fold.extend_from_slice(&4096u64.to_le_bytes());
    fold.extend_from_slice(&vec![0x5au8; PAGE]);
    std::fs::write(
        delta.join(rbitcoin_store::spend_delta::SPEND_DELTA_FOLD_NAME),
        &fold,
    )
    .unwrap();
    let mut commit =
        std::fs::read(delta.join(rbitcoin_store::spend_delta::SPEND_DELTA_COMMIT_NAME)).unwrap();
    commit[8..12].copy_from_slice(&1u32.to_le_bytes());
    std::fs::write(
        delta.join(rbitcoin_store::spend_delta::SPEND_DELTA_COMMIT_NAME),
        &commit,
    )
    .unwrap();
    let s = Store::open_tiny(&dir).unwrap();
    drop(s);
    let mut got = vec![0u8; keep];
    read_at(&body, 4096, &mut got);
    assert_eq!(got, vec![0x5au8; keep], "fold image lost");
    assert_ne!(got, vec![0xabu8; keep], "the mixed page survived");
    let _ = std::fs::remove_dir_all(&dir);
}
