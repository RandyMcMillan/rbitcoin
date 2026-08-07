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
    let mut datadir_set = false;
    let mut network = Network::Mainnet;
    let mut network_set = false;
    let mut smoke = false;
    let mut listen: Option<SocketAddr> = None;
    let mut electrum_listen: Option<SocketAddr> = None;
    let mut connect: Vec<SocketAddr> = Vec::new();
    let mut use_seeds = true;
    let mut seeds_set = false;
    let mut milestone_height = 0u32;
    let mut milestone_set = false;
    let mut max_outbound = 16u32;
    let mut max_outbound_set = false;
    let mut max_inbound = crate::config::DEFAULT_MAX_INBOUND;
    let mut max_inbound_set = false;
    let mut archive_queue_mb = crate::config::DEFAULT_ARCHIVE_QUEUE_MB;
    let mut archive_queue_set = false;
    let mut max_run_secs: Option<u64> = None;
    let mut mempool_size_mb: Option<u64> = None;
    let mut inhibit_suspend = false;
    let mut conf_path: Option<PathBuf> = None;
    // None = env/default; Some(None) = off; Some(Some(level)) = explicit level.
    let mut log_level_cli: Option<Option<Level>> = None;

    while i < args.len() {
        let a = args[i].to_string_lossy();
        match a.as_ref() {
            "--help" | "-h" => {
                eprintln!(
                    "rbitcoin-node {} — usage:\n\
  rbitcoin-node [--conf FILE] [--datadir PATH] [--network NET] \\\n\
    [--listen ADDR] [--connect ADDR]... [--electrum-listen ADDR] \\\n\
    [--milestone|--assumevalid-height HEIGHT] \\\n\
    [--maxoutbound|--max-outbound N] [--maxinbound|--maxconnections N] \\\n\
    [--mempool-size-mb|--maxmempool N] [--archive-queue-mb N] \\\n\
    [--max-run-secs N] [--log-level LEVEL] [--no-seeds] [--smoke] [--inhibit-suspend]\n\n\
Networks: mainnet|testnet|signet|regtest\n\
Log level: error|warn|info|debug|trace|off (CLI > conf log_level > RBITCOIN_LOG / RUST_LOG).\n\
Milestone / assumevalid-height: skip script/sig checks at/below HEIGHT.\n\
  Defaults: mainnet 840000, signet 2000000, testnet 2500000, regtest 0. Use 0 for full scripts.\n\
Mempool: --mempool-size-mb / --maxmempool (default ~300 MiB weight budget).\n\
Peers: --maxoutbound (default 16 live download), --maxinbound/--maxconnections (default 125).\n\
Memory: --archive-queue-mb (default 512 soft densify meter).\n\
Conf: --conf FILE (key=value; CLI overrides conf). See OPERATOR.md.\n\
Advanced debug/IO knobs remain RBITCOIN_* env (not required for normal sync; preserved if CLI omits).\n\
IBD: up to 1024 concurrent getdata, max 16 in transit per peer.",
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
            "--no-seeds" | "--noseeds" => {
                use_seeds = false;
                seeds_set = true;
                i += 1;
            }
            "--inhibit-suspend" => {
                inhibit_suspend = true;
                i += 1;
            }
            "--conf" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("error: --conf requires a path");
                    return ExitCode::from(2);
                }
                conf_path = Some(PathBuf::from(&args[i]));
                i += 1;
            }
            "--datadir" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("error: --datadir requires a value");
                    return ExitCode::from(2);
                }
                datadir = PathBuf::from(&args[i]);
                datadir_set = true;
                i += 1;
            }
            "--network" | "--chain" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("error: --network requires a value");
                    return ExitCode::from(2);
                }
                match Network::parse(&args[i].to_string_lossy()) {
                    Ok(n) => {
                        network = n;
                        network_set = true;
                    }
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
            "--mempool-size-mb" | "--maxmempool" => {
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
            "--milestone" | "--assumevalid-height" => {
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
            "--max-outbound" | "--maxoutbound" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("error: --max-outbound requires a number");
                    return ExitCode::from(2);
                }
                match args[i].to_string_lossy().parse::<u32>() {
                    Ok(n) if n > 0 => {
                        max_outbound = n;
                        max_outbound_set = true;
                    }
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
            "--max-inbound" | "--maxinbound" | "--maxconnections" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("error: --maxinbound requires a number");
                    return ExitCode::from(2);
                }
                match args[i].to_string_lossy().parse::<u32>() {
                    Ok(n) if n > 0 => {
                        max_inbound = n;
                        max_inbound_set = true;
                    }
                    Ok(_) => {
                        eprintln!("error: --maxinbound must be >= 1");
                        return ExitCode::from(2);
                    }
                    Err(e) => {
                        eprintln!("error: bad --maxinbound: {e}");
                        return ExitCode::from(2);
                    }
                }
                i += 1;
            }
            "--archive-queue-mb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("error: --archive-queue-mb requires a number");
                    return ExitCode::from(2);
                }
                match args[i].to_string_lossy().parse::<u64>() {
                    Ok(n) if n > 0 => {
                        archive_queue_mb = n;
                        archive_queue_set = true;
                    }
                    Ok(_) => {
                        eprintln!("error: --archive-queue-mb must be >= 1");
                        return ExitCode::from(2);
                    }
                    Err(e) => {
                        eprintln!("error: bad --archive-queue-mb: {e}");
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

    // Conf file first (if any); CLI flags below override.
    let mut config = NodeConfig::default();
    if let Some(ref cp) = conf_path {
        if let Err(e) = config.merge_conf_file(cp) {
            // Logging not ready; stderr is fine.
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    }

    // Logging: CLI --log-level > conf log_level > RBITCOIN_LOG / RUST_LOG > Info.
    match log_level_cli {
        Some(Some(level)) => rbitcoin_log::init(level),
        Some(None) => rbitcoin_log::init_off(),
        None => {
            if let Some(ref raw) = config.conf_log_level {
                if raw.eq_ignore_ascii_case("off") || raw.eq_ignore_ascii_case("none") {
                    rbitcoin_log::init_off();
                } else if let Some(l) = Level::parse(raw) {
                    rbitcoin_log::init(l);
                } else {
                    eprintln!(
                        "error: conf log_level `{raw}` invalid (use error|warn|info|debug|trace|off)"
                    );
                    return ExitCode::from(2);
                }
            } else if !rbitcoin_log::init_from_env() {
                rbitcoin_log::init(Level::Info);
            }
        }
    }

    // 256-way sharded heads need 1k+ FDs; raise soft NOFILE before store open.
    let (soft, hard) = rbitcoin_store::ensure_nofile_budget();
    if soft > 0 {
        rbitcoin_log::debug!("node: RLIMIT_NOFILE soft={soft} hard={hard}");
    }

    // CLI overrides conf (Core-style).
    if datadir_set {
        config.datadir = datadir;
    }
    if network_set {
        config.network = network;
    }
    if let Some(a) = listen {
        config.p2p_listen = Some(a);
    }
    if let Some(a) = electrum_listen {
        config.electrum_listen = Some(a);
    }
    if !connect.is_empty() {
        config.connect = connect;
    }
    if seeds_set {
        config.use_seeds = use_seeds;
    }
    config.smoke = smoke;
    // Milestone: CLI > conf > network default (assumevalid-style).
    if milestone_set {
        config.milestone_height = milestone_height;
    } else if config.milestone_height == 0 {
        config.milestone_height = default_milestone_height(config.network);
    }
    if max_outbound_set {
        config.max_outbound = max_outbound;
    }
    if max_inbound_set {
        config.max_inbound = max_inbound;
        config.max_inbound_explicit = true;
    }
    if archive_queue_set {
        config.archive_queue_mb = archive_queue_mb;
        config.archive_queue_mb_explicit = true;
    }
    config.inhibit_suspend = inhibit_suspend;
    // Map MiB → weight units (1 MiB ≈ 1e6 WU for budget purposes).
    if let Some(mb) = mempool_size_mb {
        config.mempool_max_weight = mb.saturating_mul(1_000_000);
    }

    // Publish only explicit CLI/conf knobs (preserves pre-set advanced envs).
    config.apply_operator_env();

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OPERATOR_ENV_TEST_LOCK;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_datadir() -> PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("rbitcoin-cli-{n}"))
    }

    /// `ExitCode` is not `PartialEq`; compare via `Debug` (stable, sufficient for tests).
    fn assert_exit(got: ExitCode, want: ExitCode) {
        assert_eq!(
            format!("{got:?}"),
            format!("{want:?}"),
            "exit code mismatch"
        );
    }

    #[test]
    fn help_and_version_exit_success() {
        assert_exit(cli_main(["rbitcoin-node", "--help"]), ExitCode::SUCCESS);
        assert_exit(cli_main(["rbitcoin-node", "-V"]), ExitCode::SUCCESS);
    }

    #[test]
    fn unknown_and_missing_value_errors() {
        assert_exit(cli_main(["rbitcoin-node", "--nope"]), ExitCode::from(2));
        assert_exit(cli_main(["rbitcoin-node", "--network"]), ExitCode::from(2));
        assert_exit(
            cli_main(["rbitcoin-node", "--network", "bogus"]),
            ExitCode::from(2),
        );
        assert_exit(cli_main(["rbitcoin-node", "--datadir"]), ExitCode::from(2));
        assert_exit(
            cli_main(["rbitcoin-node", "--listen", "not-an-addr"]),
            ExitCode::from(2),
        );
        assert_exit(
            cli_main(["rbitcoin-node", "--log-level", "wat"]),
            ExitCode::from(2),
        );
        assert_exit(
            cli_main(["rbitcoin-node", "--max-outbound", "0"]),
            ExitCode::from(2),
        );
        assert_exit(
            cli_main(["rbitcoin-node", "--mempool-size-mb", "0"]),
            ExitCode::from(2),
        );
        // Missing values / parse rejects for advanced knobs.
        assert_exit(cli_main(["rbitcoin-node", "--conf"]), ExitCode::from(2));
        assert_exit(
            cli_main(["rbitcoin-node", "--maxinbound"]),
            ExitCode::from(2),
        );
        assert_exit(
            cli_main(["rbitcoin-node", "--maxinbound", "0"]),
            ExitCode::from(2),
        );
        assert_exit(
            cli_main(["rbitcoin-node", "--maxinbound", "nope"]),
            ExitCode::from(2),
        );
        assert_exit(
            cli_main(["rbitcoin-node", "--archive-queue-mb"]),
            ExitCode::from(2),
        );
        assert_exit(
            cli_main(["rbitcoin-node", "--archive-queue-mb", "0"]),
            ExitCode::from(2),
        );
        assert_exit(
            cli_main(["rbitcoin-node", "--archive-queue-mb", "x"]),
            ExitCode::from(2),
        );
        // Bad conf path / invalid conf log_level.
        let dir = tmp_datadir();
        std::fs::create_dir_all(&dir).unwrap();
        assert_exit(
            cli_main([
                "rbitcoin-node",
                "--conf",
                dir.join("missing.conf").to_str().unwrap(),
                "--datadir",
                dir.join("d").to_str().unwrap(),
            ]),
            ExitCode::from(2),
        );
        let conf = dir.join("badlog.conf");
        std::fs::write(&conf, "log_level=notalevel\nnetwork=regtest\n").unwrap();
        assert_exit(
            cli_main([
                "rbitcoin-node",
                "--smoke",
                "--conf",
                conf.to_str().unwrap(),
                "--datadir",
                dir.join("d2").to_str().unwrap(),
                "--no-seeds",
            ]),
            ExitCode::from(2),
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn smoke_open_and_shutdown() {
        let _g = OPERATOR_ENV_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = tmp_datadir();
        let code = cli_main([
            "rbitcoin-node",
            "--smoke",
            "--network",
            "regtest",
            "--datadir",
            dir.to_str().unwrap(),
            "--no-seeds",
            "--log-level",
            "error",
            "--milestone",
            "0",
            "--max-outbound",
            "2",
            "--maxinbound",
            "10",
            "--mempool-size-mb",
            "10",
            "--archive-queue-mb",
            "64",
        ]);
        assert_exit(code, ExitCode::SUCCESS);
        assert!(dir.join("store").is_dir());
        // CLI published operator envs for library readers.
        assert_eq!(std::env::var("RBITCOIN_P2P_MAX_INBOUND").unwrap(), "10");
        assert_eq!(std::env::var("RBITCOIN_ARCHIVE_QUEUE_MB").unwrap(), "64");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn help_lists_coreish_flags_not_only_env() {
        let _g = OPERATOR_ENV_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // Parse accepts Core-like aliases (not env-only).
        let dir = tmp_datadir();
        let code = cli_main([
            "rbitcoin-node",
            "--smoke",
            "--chain",
            "regtest",
            "--datadir",
            dir.to_str().unwrap(),
            "--assumevalid-height",
            "0",
            "--maxconnections",
            "5",
            "--maxmempool",
            "8",
            "--log-level",
            "error",
            "--noseeds",
        ]);
        assert_exit(code, ExitCode::SUCCESS);
        assert_eq!(std::env::var("RBITCOIN_P2P_MAX_INBOUND").unwrap(), "5");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn conf_file_then_cli_override() {
        let _g = OPERATOR_ENV_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = tmp_datadir();
        std::fs::create_dir_all(&dir).unwrap();
        let conf = dir.join("node.conf");
        std::fs::write(
            &conf,
            "network=signet\nmaxinbound=33\narchive_queue_mb=77\n",
        )
        .unwrap();
        let data = dir.join("data");
        let code = cli_main([
            "rbitcoin-node",
            "--smoke",
            "--conf",
            conf.to_str().unwrap(),
            "--datadir",
            data.to_str().unwrap(),
            "--network",
            "regtest", // CLI overrides conf network
            "--log-level",
            "error",
            "--no-seeds",
            "--milestone",
            "0",
        ]);
        assert_exit(code, ExitCode::SUCCESS);
        // conf maxinbound applied (CLI did not override inbound)
        assert_eq!(std::env::var("RBITCOIN_P2P_MAX_INBOUND").unwrap(), "33");
        assert_eq!(std::env::var("RBITCOIN_ARCHIVE_QUEUE_MB").unwrap(), "77");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CLI omit of inbound/archive must not clobber pre-set advanced envs.
    #[test]
    fn cli_omit_preserves_advanced_env() {
        let _g = OPERATOR_ENV_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        std::env::set_var("RBITCOIN_P2P_MAX_INBOUND", "91");
        std::env::set_var("RBITCOIN_ARCHIVE_QUEUE_MB", "44");
        let dir = tmp_datadir();
        let code = cli_main([
            "rbitcoin-node",
            "--smoke",
            "--network",
            "regtest",
            "--datadir",
            dir.to_str().unwrap(),
            "--log-level",
            "error",
            "--no-seeds",
            "--milestone",
            "0",
            // no --maxinbound / --archive-queue-mb
        ]);
        assert_exit(code, ExitCode::SUCCESS);
        assert_eq!(
            std::env::var("RBITCOIN_P2P_MAX_INBOUND").as_deref(),
            Ok("91")
        );
        assert_eq!(
            std::env::var("RBITCOIN_ARCHIVE_QUEUE_MB").as_deref(),
            Ok("44")
        );
        std::env::remove_var("RBITCOIN_P2P_MAX_INBOUND");
        std::env::remove_var("RBITCOIN_ARCHIVE_QUEUE_MB");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn conf_log_level_applied_when_cli_omits() {
        let _g = OPERATOR_ENV_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = tmp_datadir();
        std::fs::create_dir_all(&dir).unwrap();
        let conf = dir.join("log.conf");
        std::fs::write(&conf, "log_level=warn\nnetwork=regtest\n").unwrap();
        let data = dir.join("data");
        // No --log-level: conf warn must init without error.
        let code = cli_main([
            "rbitcoin-node",
            "--smoke",
            "--conf",
            conf.to_str().unwrap(),
            "--datadir",
            data.to_str().unwrap(),
            "--no-seeds",
            "--milestone",
            "0",
        ]);
        assert_exit(code, ExitCode::SUCCESS);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
