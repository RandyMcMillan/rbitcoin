use crate::config::NodeConfig;
use crate::run::run_node;
use rbitcoin_primitives::Network;
use std::ffi::OsString;
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

    while i < args.len() {
        let a = args[i].to_string_lossy();
        match a.as_ref() {
            "--help" | "-h" => {
                eprintln!(
                    "rbitcoin-node {} — usage: rbitcoin-node [--datadir PATH] [--network NET] [--smoke]",
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
            other => {
                eprintln!("error: unknown argument `{other}`");
                return ExitCode::from(2);
            }
        }
    }

    let _ = smoke;
    let config = NodeConfig::default()
        .with_datadir(datadir)
        .with_network(network);

    match run_node(config) {
        Ok(handle) => {
            eprintln!(
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
                    eprintln!("shutdown error: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
