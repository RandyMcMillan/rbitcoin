use crate::config::NodeConfig;
use crate::inhibit::SuspendInhibit;
use crate::run::{run_node, run_p2p};
use rbitcoin_consensus::default_milestone_height;
use rbitcoin_log::{self, error, info, warn, Level};
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
    let mut electrum_tls_listen: Option<SocketAddr> = None;
    let mut electrum_tls_cert: Option<PathBuf> = None;
    let mut electrum_tls_key: Option<PathBuf> = None;
    let mut connect: Vec<SocketAddr> = Vec::new();
    let mut use_seeds = true;
    let mut milestone_height = 0u32;
    let mut milestone_set = false;
    let mut max_outbound = 16u32;
    let mut max_run_secs: Option<u64> = None;
    let mut mempool_size_mb: Option<u64> = None;
    let mut inhibit_suspend = false;
    // None = env/default; Some(None) = off; Some(Some(level)) = explicit level.
    let mut log_level_cli: Option<Option<Level>> = None;

    while i < args.len() {
        let a = args[i].to_string_lossy();
        match a.as_ref() {
            "--help" | "-h" => {
                eprintln!(
                    "rbitcoin-node {} — usage:\n  rbitcoin-node [--datadir PATH] [--network NET] \\\n    [--listen ADDR] [--connect ADDR]... [--electrum-listen ADDR] \\\n    [--electrum-tls-listen ADDR --electrum-tls-cert PEM --electrum-tls-key PEM] \\\n    [--milestone HEIGHT] [--max-outbound N] [--mempool-size-mb N] \\\n    [--max-run-secs N] [--log-level LEVEL] [--no-seeds] [--smoke] \\\n    [--inhibit-suspend]\n\nNetworks: mainnet|testnet|signet|regtest\nLog level: error|warn|info|debug|trace (default info; or RBITCOIN_LOG / RUST_LOG).\nMilestone: skip script/sig checks at/below HEIGHT (assumevalid-style).\n  Default when omitted: mainnet 840000, signet 2000000, testnet 2500000, regtest 0.\n  Use --milestone 0 for full script validation.\nMempool: --mempool-size-mb sets weight budget (default ~300; eviction by worst chunk).\n  Libre-relay-class: 0.1 sat/vB min, no dust ban, full RBF. See OPERATOR.md.\n--inhibit-suspend: ask systemd to block auto sleep/idle while the node runs (off by default).\nElectrum: TCP and/or TLS; banner states libre-relay-class. Memory envs:\n  RBITCOIN_ARCHIVE_QUEUE_MB, RBITCOIN_CLASS_A_CACHE_MB (default 256 each).\nParallel IBD: up to 1024 concurrent block downloads, max 16 in transit per peer.",
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
            "--inhibit-suspend" => {
                inhibit_suspend = true;
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
            "--electrum-tls-listen" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("error: --electrum-tls-listen requires a value");
                    return ExitCode::from(2);
                }
                match args[i].to_string_lossy().parse::<SocketAddr>() {
                    Ok(a) => electrum_tls_listen = Some(a),
                    Err(e) => {
                        eprintln!("error: bad --electrum-tls-listen: {e}");
                        return ExitCode::from(2);
                    }
                }
                i += 1;
            }
            "--electrum-tls-cert" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("error: --electrum-tls-cert requires a path");
                    return ExitCode::from(2);
                }
                electrum_tls_cert = Some(PathBuf::from(&args[i]));
                i += 1;
            }
            "--electrum-tls-key" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("error: --electrum-tls-key requires a path");
                    return ExitCode::from(2);
                }
                electrum_tls_key = Some(PathBuf::from(&args[i]));
                i += 1;
            }
            "--mempool-size-mb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("error: --mempool-size-mb requires a number");
                    return ExitCode::from(2);
                }
                match args[i].to_string_lossy().parse::<u64>() {
                    Ok(n) if n > 0 => mempool_size_mb = Some(n),
                    Ok(_) => {
                        eprintln!("error: --mempool-size-mb must be >= 1");
                        return ExitCode::from(2);
                    }
                    Err(e) => {
                        eprintln!("error: bad --mempool-size-mb: {e}");
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

    // 256-way sharded heads need 1k+ FDs; raise soft NOFILE before store open.
    let (soft, hard) = rbitcoin_store::ensure_nofile_budget();
    if soft > 0 {
        rbitcoin_log::debug!("node: RLIMIT_NOFILE soft={soft} hard={hard}");
    }

    let mut config = NodeConfig::default()
        .with_datadir(datadir)
        .with_network(network);
    config.smoke = smoke;
    config.p2p_listen = listen;
    config.electrum_listen = electrum_listen;
    config.electrum_tls_listen = electrum_tls_listen;
    config.electrum_tls_cert = electrum_tls_cert;
    config.electrum_tls_key = electrum_tls_key;
    config.connect = connect;
    config.use_seeds = use_seeds;
    // Network default assumevalid-style milestone unless operator set --milestone.
    config.milestone_height = if milestone_set {
        milestone_height
    } else {
        default_milestone_height(network)
    };
    config.max_outbound = max_outbound;
    config.inhibit_suspend = inhibit_suspend;
    // Map MiB → weight units (1 MiB ≈ 1e6 WU for budget purposes).
    if let Some(mb) = mempool_size_mb {
        config.mempool_max_weight = mb.saturating_mul(1_000_000);
    }

    // Hold for process lifetime; drop after run_node / run_p2p returns.
    let _suspend_inhibit = if config.inhibit_suspend {
        match SuspendInhibit::try_start("rbitcoin-node running (IBD / tip follow)") {
            Some(g) => Some(g),
            None => {
                warn!(
                    "node: --inhibit-suspend requested but systemd-inhibit unavailable; continuing without inhibit"
                );
                None
            }
        }
    } else {
        None
    };
    if config.electrum_tls_listen.is_some()
        && (config.electrum_tls_cert.is_none() || config.electrum_tls_key.is_none())
    {
        eprintln!("error: --electrum-tls-listen requires --electrum-tls-cert and --electrum-tls-key");
        return ExitCode::from(2);
    }
    if max_run_secs.is_some() {
        config.max_run_secs = max_run_secs;
    }

    // Create --datadir (and store/mempool/wire) before opening the store.
    if let Err(e) = config.ensure_datadir() {
        error!("{e}");
        return ExitCode::FAILURE;
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
