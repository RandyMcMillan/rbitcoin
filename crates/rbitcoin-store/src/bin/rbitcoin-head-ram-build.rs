//! Offline: build a complete sized `tx.head` **in RAM** from Class A bodies and
//! write it to a side file (default `tx.head.test` next to the live head).
//!
//! Does **not** touch live `tx.head` / online resize. Use to time pure RAM fill
//! + sequential write vs background io_uring shadow resize.
//!
//! ```text
//! rbitcoin-head-ram-build <store_dir> [out_path]
//! ```
//!
//! `store_dir` is the store directory (contains `tx.body`, `tx.idx`, …).
//! Default `out_path` = `<store_dir>/tx.head.test`.
//!
//! Layout sizing uses the same [`layout_for_count`] policy as open-time recreate
//! (`RBITCOIN_TX_HEAD_BITS` / `RBITCOIN_HEAD_SCALE` apply).

use rbitcoin_store::{layout_for_count, Store};
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let Some(store_dir) = args.next() else {
        eprintln!(
            "usage: rbitcoin-head-ram-build <store_dir> [out_path]\n\
             \n\
             Build a full tx.head in RAM from Class A bodies and write it out.\n\
             Default out_path: <store_dir>/tx.head.test\n\
             Does not modify the live primary/shadow head."
        );
        return ExitCode::from(2);
    };
    let store_dir = PathBuf::from(store_dir);
    let out = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| store_dir.join("tx.head.test"));

    if !store_dir.is_dir() {
        eprintln!("error: store_dir is not a directory: {}", store_dir.display());
        return ExitCode::from(1);
    }

    let t_open = Instant::now();
    let store = match Store::open(&store_dir) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: open store {}: {e}", store_dir.display());
            return ExitCode::from(1);
        }
    };
    let open_ms = t_open.elapsed().as_millis();
    let n = store.txs.count();
    let layout = layout_for_count(n);
    let live_bits = store.txs.head_bits();
    let live_occ = store.txs.head_occupied();
    eprintln!(
        "store open_ms={open_ms} bodies={n} live_head bits={live_bits} occupied~{live_occ} \
         (occupied may be 0 when open skips full scan on large heads)"
    );
    eprintln!(
        "RAM build layout bits={} slots={} entry={}B body={:.2} GiB → {}",
        layout.bits,
        layout.slots(),
        layout.entry_bytes,
        layout.body_bytes() as f64 / (1024.0 * 1024.0 * 1024.0),
        out.display()
    );

    let t0 = Instant::now();
    let mut last_log = Instant::now();
    match store.txs.build_head_file_in_ram(&out, |done, total, occ| {
        if last_log.elapsed().as_secs() >= 5 || done == total {
            let pct = if total > 0 {
                (100 * done) / total
            } else {
                100
            };
            eprintln!(
                "progress {done}/{total} ({pct}%) occupied={occ} elapsed_ms={}",
                t0.elapsed().as_millis()
            );
            last_log = Instant::now();
        }
    }) {
        Ok(ram) => {
            eprintln!(
                "ok path={} occupied={} bits={} total_ms={}",
                out.display(),
                ram.occupied(),
                ram.layout().bits,
                t0.elapsed().as_millis()
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: RAM build failed: {e}");
            ExitCode::from(1)
        }
    }
}
