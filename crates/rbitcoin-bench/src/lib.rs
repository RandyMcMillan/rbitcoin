//! Client-side Electrum / Esplora benchmark.
//!
//! Suites follow public methodologies:
//! - **casa**: Lopp/Casa 2020–2022 — sequential `get_balance` / `get_history` /
//!   `listunspent` per scripthash; discard warmup; median of remaining passes.
//! - **sparrow**: Sparrow Wallet 2022 — batched `subscribe` (wallet load) then
//!   batched `get_history` (refresh); optional `transaction.get`.
//! - **hot**: fat-history keys (e.g. well-known high-fanout scripts).
//!
//! Not a product binary. Build: `cargo run -p rbitcoin-bench --features cli --release`.

mod electrum;
mod esplora;
mod hex;
mod jsonrpc;
mod stats;
mod suite;
mod targets;

use crate::electrum::ElectrumClient;
use crate::esplora::EsploraClient;
use crate::stats::format_report;
use crate::suite::{electrum_casa, electrum_hot, electrum_sparrow, esplora_casa, CasaOpts, Suite};
use crate::targets::load_targets;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

pub fn cli_main(args: impl IntoIterator<Item = OsString>) -> ExitCode {
    match run(args.into_iter().collect()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(1)
        }
    }
}

fn usage() -> String {
    format!(
        "rbitcoin-bench {} — Electrum/Esplora client benchmark (not built by default)\n\
         \n\
         Usage:\n\
           cargo run -p rbitcoin-bench --features cli --release -- [OPTIONS]\n\
         \n\
         Required:\n\
           --targets FILE         scripthash hex (64) or addresses, one per line\n\
           --electrum HOST:PORT   Electrum TCP (e.g. 127.0.0.1:50001)\n\
           --esplora http://HOST:PORT\n\
         \n\
         Options:\n\
           --suite casa|sparrow|hot   default casa (casa=Lopp/Casa; sparrow=wallet\n\
                                      load/refresh; hot=fat get_history/listunspent)\n\
           --warmup N                 discarded first passes (casa, default 1)\n\
           --passes N                 counted passes after warmup (casa, default 9)\n\
           --batch N                  sparrow page size (default 50)\n\
           --fetch-txs                sparrow: also blockchain.transaction.get\n\
           --timeout-secs N           per request (default 30)\n\
           -h, --help\n\
           -V, --version\n\
         \n\
         Casa: sequential balance/history/utxo; throw away warmup; median of passes.\n\
         Sparrow: subscribe all (load) then get_history all (refresh), batch 50.\n\
         Compare the same --targets file against rbitcoin, Fulcrum, electrs, ElectrumX.",
        env!("CARGO_PKG_VERSION")
    )
}

#[derive(Debug)]
struct Cfg {
    targets: Option<PathBuf>,
    electrum: Option<String>,
    esplora: Option<String>,
    suite: Suite,
    warmup: u32,
    passes: u32,
    batch: usize,
    fetch_txs: bool,
    timeout: Duration,
}

impl Default for Cfg {
    fn default() -> Self {
        Self {
            targets: None,
            electrum: None,
            esplora: None,
            suite: Suite::Casa,
            warmup: 1,
            passes: 9,
            batch: 50,
            fetch_txs: false,
            timeout: Duration::from_secs(30),
        }
    }
}

fn take(args: &[OsString], i: &mut usize, flag: &str) -> Result<String, String> {
    *i += 1;
    args.get(*i)
        .map(|s| s.to_string_lossy().into_owned())
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_args(args: &[OsString]) -> Result<Cfg, String> {
    let mut cfg = Cfg::default();
    let mut i = 1usize;
    while i < args.len() {
        let a = args[i].to_string_lossy();
        match a.as_ref() {
            "--help" | "-h" => return Err(usage()),
            "--version" | "-V" => {
                return Err(format!("rbitcoin-bench {}", env!("CARGO_PKG_VERSION")));
            }
            "--targets" => cfg.targets = Some(PathBuf::from(take(args, &mut i, "--targets")?)),
            "--electrum" => cfg.electrum = Some(take(args, &mut i, "--electrum")?),
            "--esplora" => cfg.esplora = Some(take(args, &mut i, "--esplora")?),
            "--suite" => cfg.suite = Suite::parse(&take(args, &mut i, "--suite")?)?,
            "--warmup" => {
                cfg.warmup = take(args, &mut i, "--warmup")?
                    .parse()
                    .map_err(|_| "bad --warmup".to_string())?;
            }
            "--passes" => {
                cfg.passes = take(args, &mut i, "--passes")?
                    .parse()
                    .map_err(|_| "bad --passes".to_string())?;
            }
            "--batch" => {
                cfg.batch = take(args, &mut i, "--batch")?
                    .parse()
                    .map_err(|_| "bad --batch".to_string())?;
            }
            "--timeout-secs" => {
                let n: u64 = take(args, &mut i, "--timeout-secs")?
                    .parse()
                    .map_err(|_| "bad --timeout-secs".to_string())?;
                cfg.timeout = Duration::from_secs(n.max(1));
            }
            "--fetch-txs" => cfg.fetch_txs = true,
            other => return Err(format!("unknown argument {other}")),
        }
        i += 1;
    }
    Ok(cfg)
}

fn run(args: Vec<OsString>) -> Result<(), String> {
    let cfg = match parse_args(&args) {
        Ok(c) => c,
        Err(msg) if msg.starts_with("rbitcoin-bench ") || msg.starts_with("Usage:") => {
            println!("{msg}");
            return Ok(());
        }
        Err(e) if e.contains("unknown suite") || e.starts_with("unknown argument") => {
            return Err(e);
        }
        Err(e) if e.contains("requires a value") || e.starts_with("bad --") => return Err(e),
        Err(msg) => {
            println!("{msg}");
            return Ok(());
        }
    };
    if args.iter().any(|a| {
        let s = a.to_string_lossy();
        s == "--help" || s == "-h" || s == "--version" || s == "-V"
    }) {
        return Ok(());
    }
    let path = cfg
        .targets
        .clone()
        .ok_or_else(|| "need --targets FILE (see --help)".to_string())?;
    let targets = load_targets(&path)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    rt.block_on(run_async(cfg, targets))
}

async fn run_async(cfg: Cfg, targets: Vec<String>) -> Result<(), String> {
    let samples;
    let backend;
    match (&cfg.electrum, &cfg.esplora) {
        (Some(addr), None) => {
            backend = "electrum";
            let mut c = ElectrumClient::connect(addr, cfg.timeout).await?;
            samples = match cfg.suite {
                Suite::Casa => {
                    electrum_casa(
                        &mut c,
                        &targets,
                        &CasaOpts {
                            warmup: cfg.warmup,
                            passes: cfg.passes,
                        },
                    )
                    .await?
                }
                Suite::Sparrow => {
                    electrum_sparrow(&mut c, &targets, cfg.batch, cfg.fetch_txs).await?
                }
                Suite::Hot => electrum_hot(&mut c, &targets, cfg.timeout).await?,
            };
        }
        (None, Some(url)) => {
            backend = "esplora";
            if cfg.suite == Suite::Sparrow {
                return Err("sparrow suite is Electrum-only (subscribe)".into());
            }
            let mut c = EsploraClient::connect(url, cfg.timeout).await?;
            samples = match cfg.suite {
                Suite::Casa | Suite::Hot => {
                    esplora_casa(
                        &mut c,
                        &targets,
                        &CasaOpts {
                            warmup: if cfg.suite == Suite::Hot {
                                0
                            } else {
                                cfg.warmup
                            },
                            passes: if cfg.suite == Suite::Hot {
                                1
                            } else {
                                cfg.passes
                            },
                        },
                    )
                    .await?
                }
                Suite::Sparrow => unreachable!(),
            };
        }
        (Some(_), Some(_)) => return Err("use one of --electrum or --esplora".into()),
        (None, None) => {
            return Err("need --electrum HOST:PORT or --esplora http://HOST:PORT".into())
        }
    }
    let name = match cfg.suite {
        Suite::Casa => "casa",
        Suite::Sparrow => "sparrow",
        Suite::Hot => "hot",
    };
    println!("{}", format_report(name, backend, &samples));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_help_and_flags() {
        let args = vec![OsString::from("rbitcoin-bench"), OsString::from("--help")];
        assert!(parse_args(&args).is_err());
        let args = vec![
            OsString::from("rbitcoin-bench"),
            OsString::from("--suite"),
            OsString::from("sparrow"),
            OsString::from("--batch"),
            OsString::from("25"),
            OsString::from("--targets"),
            OsString::from("/tmp/x"),
            OsString::from("--electrum"),
            OsString::from("127.0.0.1:1"),
        ];
        let c = parse_args(&args).unwrap();
        assert_eq!(c.batch, 25);
        assert_eq!(c.suite, Suite::Sparrow);
    }

    #[test]
    fn parse_unknown() {
        let args = vec![OsString::from("b"), OsString::from("--nope")];
        assert!(parse_args(&args).unwrap_err().contains("unknown"));
    }

    #[test]
    fn cli_help_exits_ok() {
        let code = cli_main([OsString::from("rbitcoin-bench"), OsString::from("-h")]);
        assert_eq!(code, ExitCode::SUCCESS);
    }
}
