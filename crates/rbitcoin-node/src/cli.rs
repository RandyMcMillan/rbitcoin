use crate::config::NodeConfig;
use crate::run::{run_node, run_p2p};
use rbitcoin_consensus::default_milestone_height;
use rbitcoin_log::{self, error, info, Level};
use rbitcoin_primitives::Network;
use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

/// Process entry used by `main` and high-level scenarios.
pub fn cli_main<I, T>(args: I) -> ExitCode
where
    I: IntoIterator<Item = T>,
    T: Into<OsString>,
{
    let args: Vec<OsString> = args.into_iter().map(Into::into).collect();
    let mut i = 1usize;
    let mut datadir = PathBuf::from("./datadir");
    let mut network = Network::Mainnet;
    let mut smoke = false;
    let mut listen: Option<SocketAddr> = None;
    let mut electrum_listen: Option<SocketAddr> = None;
    let mut connect: Vec<SocketAddr> = Vec::new();
    let mut use_seeds = true;
    let mut milestone_height = 0u32;
    let mut milestone_set = false;
    let mut max_outbound = 16u32;
    let mut max_run_secs: Option<u64> = None;
    let mut scripthash_index = true;
    // None = env/default; Some(None) = off; Some(Some(level)) = explicit level.
    let mut log_level_cli: Option<Option<Level>> = None;

    while i < args.len() {
        let a = args[i].to_string_lossy();
        match a.as_ref() {
            "--help" | "-h" => {
                eprintln!(
                    "rbitcoin-node {} — usage:\n  rbitcoin-node [--datadir PATH] [--network NET] \\\n    [--listen ADDR] [--connect ADDR]... [--electrum-listen ADDR] \\\n    [--milestone HEIGHT] [--max-outbound N] [--max-run-secs N] \\\n    [--log-level LEVEL] [--no-scripthash-index] [--no-seeds] [--smoke]\n\nNetworks: mainnet|testnet|signet|regtest\nLog level: error|warn|info|debug|trace (default info; or RBITCOIN_LOG / RUST_LOG).\nMilestone: skip script/prevout at/below HEIGHT (IBD assumevalid-style).\n  Default when omitted: mainnet 840000, signet 2000000, testnet 2500000, regtest 0.\n  Use --milestone 0 for full validation. Disables scripthash index under milestone.\nParallel IBD: tip-ahead window 1024, max 16 blocks in transit per peer (Core-like).",
                    env!("CARGO_PKG_VERSION")
                );
                return ExitCode::SUCCESS;
            }
            "--version" | "-V" => {
                eprintln!("rbitcoin-node {}", env!("CARGO_PKG_VERSION"));
                return ExitCode::SUCCESS;
            }
            "--smoke" => {
                smoke = true;
                i += 1;
            }
            "--no-seeds" => {
                use_seeds = false;
                i += 1;
            }
            "--no-scripthash-index" => {
                scripthash_index = false;
                i += 1;
            }
            "--datadir" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("error: --datadir requires a value");
                    return ExitCode::from(2);
                }
                datadir = PathBuf::from(&args[i]);
                i += 1;
            }
            "--network" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("error: --network requires a value");
                    return ExitCode::from(2);
                }
                match Network::parse(&args[i].to_string_lossy()) {
                    Ok(n) => network = n,
                    Err(e) => {
                        eprintln!("error: {e}");
                        return ExitCode::from(2);
                    }
                }
                i += 1;
            }
            "--listen" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("error: --listen requires a value");
                    return ExitCode::from(2);
                }
                match args[i].to_string_lossy().parse::<SocketAddr>() {
                    Ok(a) => listen = Some(a),
                    Err(e) => {
                        eprintln!("error: bad --listen: {e}");
                        return ExitCode::from(2);
                    }
                }
                i += 1;
            }
            "--connect" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("error: --connect requires a value");
                    return ExitCode::from(2);
                }
                match args[i].to_string_lossy().parse::<SocketAddr>() {
                    Ok(a) => connect.push(a),
                    Err(e) => {
                        eprintln!("error: bad --connect: {e}");
                        return ExitCode::from(2);
                    }
                }
                i += 1;
            }
            "--electrum-listen" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("error: --electrum-listen requires a value");
                    return ExitCode::from(2);
                }
                match args[i].to_string_lossy().parse::<SocketAddr>() {
                    Ok(a) => electrum_listen = Some(a),
                    Err(e) => {
                        eprintln!("error: bad --electrum-listen: {e}");
                        return ExitCode::from(2);
                    }
                }
                i += 1;
            }
            "--milestone" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("error: --milestone requires a height");
                    return ExitCode::from(2);
                }
                match args[i].to_string_lossy().parse::<u32>() {
                    Ok(h) => {
                        milestone_height = h;
                        milestone_set = true;
                    }
                    Err(e) => {
                        eprintln!("error: bad --milestone: {e}");
                        return ExitCode::from(2);
                    }
                }
                i += 1;
            }
            "--max-outbound" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("error: --max-outbound requires a number");
                    return ExitCode::from(2);
                }
                match args[i].to_string_lossy().parse::<u32>() {
                    Ok(n) if n > 0 => max_outbound = n,
                    Ok(_) => {
                        eprintln!("error: --max-outbound must be >= 1");
                        return ExitCode::from(2);
                    }
                    Err(e) => {
                        eprintln!("error: bad --max-outbound: {e}");
                        return ExitCode::from(2);
                    }
                }
                i += 1;
            }
            "--max-run-secs" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("error: --max-run-secs requires a number");
                    return ExitCode::from(2);
                }
                match args[i].to_string_lossy().parse::<u64>() {
                    Ok(n) => max_run_secs = Some(n),
                    Err(e) => {
                        eprintln!("error: bad --max-run-secs: {e}");
                        return ExitCode::from(2);
                    }
                }
                i += 1;
            }
            "--log-level" => {
                i += 1;
                if i >= args.len() {
                    eprintln!(
                        "error: --log-level requires a value (error|warn|info|debug|trace|off)"
                    );
                    return ExitCode::from(2);
                }
                let raw = args[i].to_string_lossy();
                if raw.eq_ignore_ascii_case("off") || raw.eq_ignore_ascii_case("none") {
                    log_level_cli = Some(None);
                } else if let Some(l) = Level::parse(&raw) {
                    log_level_cli = Some(Some(l));
                } else {
                    eprintln!(
                        "error: bad --log-level `{raw}` (use error|warn|info|debug|trace|off)"
                    );
                    return ExitCode::from(2);
                }
                i += 1;
            }
            other => {
                eprintln!("error: unknown argument `{other}`");
                return ExitCode::from(2);
            }
        }
    }

    // Logging: CLI --log-level wins; else RBITCOIN_LOG / RUST_LOG; else Info.
    match log_level_cli {
        Some(Some(level)) => rbitcoin_log::init(level),
        Some(None) => rbitcoin_log::init_off(),
        None => {
            if !rbitcoin_log::init_from_env() {
                rbitcoin_log::init(Level::Info);
            }
        }
    }

    let mut config = NodeConfig::default()
        .with_datadir(datadir)
        .with_network(network);
    config.smoke = smoke;
    config.p2p_listen = listen;
    config.electrum_listen = electrum_listen;
    config.connect = connect;
    config.use_seeds = use_seeds;
    // Network default assumevalid-style milestone unless operator set --milestone.
    config.milestone_height = if milestone_set {
        milestone_height
    } else {
        default_milestone_height(network)
    };
    config.max_outbound = max_outbound;
    config.scripthash_index = scripthash_index;
    if max_run_secs.is_some() {
        config.max_run_secs = max_run_secs;
    }

    if smoke {
        match run_node(config) {
            Ok(handle) => {
                info!(
                    "rbitcoin-node {} on {} datadir={}",
                    env!("CARGO_PKG_VERSION"),
                    handle.network_name(),
                    handle.config.datadir.display()
                );
                if std::env::var_os("RBITCOIN_TEST_DROP_STORE").is_some() {
                    let _ = std::fs::remove_dir_all(handle.config.store_path());
                }
                match handle.shutdown() {
                    Ok(()) => ExitCode::SUCCESS,
                    Err(e) => {
                        error!("shutdown error: {e}");
                        ExitCode::FAILURE
                    }
                }
            }
            Err(e) => {
                error!("{e}");
                ExitCode::FAILURE
            }
        }
    } else {
        let rt = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(e) => {
                error!("runtime: {e}");
                return ExitCode::FAILURE;
            }
        };
        match rt.block_on(run_p2p(config)) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                error!("{e}");
                ExitCode::FAILURE
            }
        }
    }
}
