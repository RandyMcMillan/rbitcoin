//! Offline header-graph repair for a datadir `store/` directory.
//!
//! Usage:
//! ```text
//! rbitcoin-rebuild-headers --store ./datadir-mainnet/store --dry-run
//! rbitcoin-rebuild-headers --store ./datadir-mainnet/store --write
//! ```
//!
//! Stop the node before `--write`. See `rbitcoin_store::rebuild_headers`.

use rbitcoin_store::rebuild_headers::rebuild_headers;
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut store: Option<PathBuf> = None;
    let mut write = false;
    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--store" | "--datadir-store" => {
                store = args.next().map(PathBuf::from);
            }
            "--datadir" => {
                // Accept datadir root; append store/
                if let Some(d) = args.next() {
                    store = Some(PathBuf::from(d).join("store"));
                }
            }
            "--write" => write = true,
            "--dry-run" => write = false,
            "-h" | "--help" => {
                eprintln!(
                    "Usage: rbitcoin-rebuild-headers --store <path/to/store> [--dry-run|--write]\n\
                     \n\
                     Or:    rbitcoin-rebuild-headers --datadir <datadir> [--dry-run|--write]\n\
                     \n\
                     Repairs false header prev_fk edges and re-links the confirmed chain.\n\
                     Node must be stopped for --write. Prefer --dry-run first (default)."
                );
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("unknown arg: {other}");
                return ExitCode::FAILURE;
            }
        }
    }

    let Some(store_path) = store else {
        eprintln!("missing --store or --datadir");
        return ExitCode::FAILURE;
    };

    match rebuild_headers(&store_path, write) {
        Ok(r) => {
            println!("store={}", store_path.display());
            println!("wrote={}", r.wrote);
            println!("header_rows={}", r.header_rows);
            println!("tip_height={:?}", r.tip_height);
            println!("null_prev_rows={}", r.null_prev_rows);
            println!("confirmed_relinked={}", r.confirmed_relinked);
            println!("false_prev_nulled={}", r.false_prev_nulled);
            println!(
                "confirmed_tip_plus_one_scrubbed={}",
                r.confirmed_tip_plus_one_scrubbed
            );
            println!("resume_walk_before={}", r.resume_walk_before);
            println!("resume_walk_after={}", r.resume_walk_after);
            if !r.wrote {
                println!("(dry-run: re-run with --write to apply)");
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("rebuild_headers failed: {e}");
            ExitCode::FAILURE
        }
    }
}
