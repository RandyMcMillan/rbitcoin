//! Offline: build a complete sized `tx.head` **in RAM** from Class A bodies and
//! write it to a side file (default `tx.head.test` next to the live head).
//!
//! Does **not** touch live `tx.head` / online resize. Use to time pure RAM fill
//! + sequential write, or force an early widen before the node hits 80% load.
//!
//! ```text
//! rbitcoin-head-ram-build <store_dir> [out_path] [--bits N]
//! ```
//!
//! `store_dir` is the store directory (contains `tx.body`, `tx.idx`, …).
//! Default `out_path` = `<store_dir>/tx.head.test`.
//!
//! `--bits N` forces target address width (must be in layout range and ≥ the
//! auto `layout_for_count` width). Without it, sizing matches open-time recreate.

use rbitcoin_store::{layout_for_count, HeadLayout, Store, MAX_BITS, MIN_BITS};
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

fn usage() {
    eprintln!(
        "usage: rbitcoin-head-ram-build <store_dir> [out_path] [--bits N]\n\
         \n\
         Build a full tx.head in RAM from Class A bodies and write it out.\n\
         Default out_path: <store_dir>/tx.head.test\n\
         --bits N  force target bits (e.g. early 31 before load hits 80%)\n\
         Does not modify the live primary/shadow head."
    );
}

fn main() -> ExitCode {
    let mut store_dir: Option<PathBuf> = None;
    let mut out: Option<PathBuf> = None;
    let mut bits: Option<u32> = None;
    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        if a == "--bits" {
            let Some(v) = args.next() else {
                eprintln!("error: --bits requires a value");
                usage();
                return ExitCode::from(2);
            };
            match v.parse::<u32>() {
                Ok(n) if (MIN_BITS..=MAX_BITS).contains(&n) => bits = Some(n),
                Ok(n) => {
                    eprintln!("error: --bits {n} out of {MIN_BITS}..={MAX_BITS}");
                    return ExitCode::from(2);
                }
                Err(_) => {
                    eprintln!("error: invalid --bits value {v:?}");
                    return ExitCode::from(2);
                }
            }
        } else if a == "-h" || a == "--help" {
            usage();
            return ExitCode::SUCCESS;
        } else if store_dir.is_none() {
            store_dir = Some(PathBuf::from(a));
        } else if out.is_none() {
            out = Some(PathBuf::from(a));
        } else {
            eprintln!("error: unexpected argument {a:?}");
            usage();
            return ExitCode::from(2);
        }
    }
    let Some(store_dir) = store_dir else {
        usage();
        return ExitCode::from(2);
    };
    let out = out.unwrap_or_else(|| store_dir.join("tx.head.test"));

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
    let auto = layout_for_count(n);
    let layout = match bits {
        Some(b) => {
            if b < auto.bits {
                eprintln!(
                    "error: --bits {b} is narrower than auto layout_for_count bits={} \
                     for n={n} (would exceed load threshold immediately)",
                    auto.bits
                );
                return ExitCode::from(2);
            }
            match HeadLayout::new(b) {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("error: layout bits={b}: {e}");
                    return ExitCode::from(2);
                }
            }
        }
        None => auto,
    };
    let live_bits = store.txs.head_bits();
    let live_occ = store.txs.head_occupied();
    eprintln!(
        "store open_ms={open_ms} bodies={n} live_head bits={live_bits} occupied~{live_occ} \
         (occupied may be 0 when open skips full scan on large heads)"
    );
    eprintln!(
        "RAM build layout bits={} slots={} entry={}B body={:.2} GiB → {}{}",
        layout.bits,
        layout.slots(),
        layout.entry_bytes,
        layout.body_bytes() as f64 / (1024.0 * 1024.0 * 1024.0),
        out.display(),
        if bits.is_some() {
            " (--bits forced)"
        } else {
            ""
        }
    );

    let t0 = Instant::now();
    let mut last_log = Instant::now();
    match store
        .txs
        .build_head_file_in_ram_with_layout(&out, Some(layout), |done, total, occ| {
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
