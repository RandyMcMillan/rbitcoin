//! Host microbench: address-head insert_many MapFull vs FdOnly.
//!
//! **Operator only** (local disk). Not valid on agent 9p workspace.
//!
//! Build (musl static preferred for operator boxes):
//! ```text
//! nix build .#rbitcoin-musl --out-link result
//! # if bin is packaged: result/bin/rbitcoin-store-bench
//! # interim:
//! cargo build -p rbitcoin-store --release --bin rbitcoin-store-bench
//! ```
//!
//! Run:
//! ```text
//! ./target/release/rbitcoin-store-bench --n 200000 --bits 16
//! ./target/release/rbitcoin-store-bench --n 200000 --bits 16 --access map
//! ./target/release/rbitcoin-store-bench --n 200000 --bits 16 --access fd
//! ```
//!
//! Compares page-coalesced insert_many + probe round-trip. Prints ns/key and
//! wall ms. Record numbers in `docs/io-modality.md` phase 2.

use rbitcoin_primitives::Fk;
use rbitcoin_store::{AddressHead, HeadLayout, TableAccess};
use std::env;
use std::path::PathBuf;
use std::time::Instant;

fn usage() {
    eprintln!(
        "usage: rbitcoin-store-bench [--n KEYS] [--bits BITS] [--access map|fd|both] [--dir DIR]\n\
         defaults: n=100000 bits=16 access=both dir=$TMPDIR/rbitcoin-store-bench-$$"
    );
}

fn parse_args() -> (usize, u32, String, PathBuf) {
    let mut n = 100_000usize;
    let mut bits = 16u32;
    let mut access = "both".to_string();
    let mut dir = env::temp_dir().join(format!(
        "rbitcoin-store-bench-{}",
        std::process::id()
    ));
    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "-h" | "--help" => {
                usage();
                std::process::exit(0);
            }
            "--n" => {
                n = args
                    .next()
                    .and_then(|s| s.parse().ok())
                    .expect("--n needs integer");
            }
            "--bits" => {
                bits = args
                    .next()
                    .and_then(|s| s.parse().ok())
                    .expect("--bits needs integer");
            }
            "--access" => {
                access = args.next().expect("--access needs map|fd|both");
            }
            "--dir" => {
                dir = PathBuf::from(args.next().expect("--dir needs path"));
            }
            other => {
                eprintln!("unknown arg: {other}");
                usage();
                std::process::exit(2);
            }
        }
    }
    (n, bits, access, dir)
}

fn mixed_key(i: u64) -> [u8; 32] {
    let mut k = [0u8; 32];
    k[0..8].copy_from_slice(&i.to_le_bytes());
    // scatter bits so pages are not all sequential
    k[8] = (i.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 56) as u8;
    k[16..24].copy_from_slice(&(i.wrapping_mul(0xc2b2_ae3d_27d4_eb4f)).to_le_bytes());
    k
}

fn run_mode(access: TableAccess, n: usize, bits: u32, base: &std::path::Path) -> Result<(), String> {
    let _ = std::fs::remove_dir_all(base);
    std::fs::create_dir_all(base).map_err(|e| e.to_string())?;
    let path = base.join("head");
    let layout = HeadLayout::with_entry_bytes(bits, 4).map_err(|e| format!("{e}"))?;
    let head = AddressHead::create_with_table_access(&path, layout, access)
        .map_err(|e| format!("create: {e}"))?;
    assert_eq!(head.table_access(), access);

    let mut batch: Vec<([u8; 32], Fk)> = Vec::with_capacity(n);
    for i in 0..n as u64 {
        batch.push((mixed_key(i + 1), Fk(i + 1)));
    }

    let t0 = Instant::now();
    head.insert_many(&batch)
        .map_err(|e| format!("insert_many: {e}"))?;
    let insert_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let insert_ns = t0.elapsed().as_nanos() / n.max(1) as u128;

    let t1 = Instant::now();
    let mut hits = 0usize;
    for i in 0..n as u64 {
        let fks = head
            .probe_fks(&mixed_key(i + 1))
            .map_err(|e| format!("probe: {e}"))?;
        if fks.iter().any(|f| f.0 == i + 1) {
            hits += 1;
        }
    }
    let probe_ms = t1.elapsed().as_secs_f64() * 1000.0;
    let probe_ns = t1.elapsed().as_nanos() / n.max(1) as u128;

    println!(
        "access={access:?} bits={bits} n={n} occupied={} insert_ms={insert_ms:.2} insert_ns/key={insert_ns} \
         probe_ms={probe_ms:.2} probe_ns/key={probe_ns} hits={hits}/{n}",
        head.occupied()
    );
    if hits != n {
        return Err(format!("probe hits {hits} != n {n}"));
    }
    let _ = std::fs::remove_dir_all(base);
    Ok(())
}

fn main() {
    let (n, bits, access, dir) = parse_args();
    println!(
        "rbitcoin-store-bench n={n} bits={bits} access={access} dir={}",
        dir.display()
    );
    if bits < 12 || bits > 28 {
        eprintln!("bits should be 12..=28 for a useful microbench (got {bits})");
    }

    let modes: Vec<TableAccess> = match access.to_ascii_lowercase().as_str() {
        "map" | "mmap" | "mapfull" => vec![TableAccess::MapFull],
        "fd" | "fdonly" | "pread" => vec![TableAccess::FdOnly],
        "both" | "ab" | "a/b" => vec![TableAccess::MapFull, TableAccess::FdOnly],
        other => {
            eprintln!("unknown --access {other}");
            usage();
            std::process::exit(2);
        }
    };

    for mode in modes {
        let sub = dir.join(format!("{mode:?}"));
        if let Err(e) = run_mode(mode, n, bits, &sub) {
            eprintln!("FAIL access={mode:?}: {e}");
            std::process::exit(1);
        }
    }
    println!("ok");
}
