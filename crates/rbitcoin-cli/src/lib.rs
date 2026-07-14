//! CLI library helpers (RPC client lands with Phase 7).

use std::ffi::OsString;
use std::process::ExitCode;

pub fn crate_name() -> &'static str {
    "rbitcoin-cli"
}

/// Build a wallet-scoped RPC path (mirrors Core `-rpcwallet` routing).
pub fn rpc_wallet_path(wallet: Option<&str>) -> String {
    match wallet {
        None | Some("") => "/".to_string(),
        Some(name) => format!("/wallet/{name}"),
    }
}

/// Process entry used by `main` and high-level scenarios.
pub fn cli_main<I, T>(args: I) -> ExitCode
where
    I: IntoIterator<Item = T>,
    T: Into<OsString>,
{
    let args: Vec<OsString> = args.into_iter().map(Into::into).collect();
    // Skip argv[0]
    let mut i = 1usize;
    let mut rpcwallet: Option<String> = None;
    let mut command: Option<String> = None;

    while i < args.len() {
        let a = args[i].as_os_str();
        if a == "--help" || a == "-h" {
            eprintln!(
                "rbitcoin-cli {} — usage: rbitcoin-cli [--rpcwallet NAME] [COMMAND]",
                env!("CARGO_PKG_VERSION")
            );
            return ExitCode::SUCCESS;
        }
        if a == "--version" || a == "-V" {
            eprintln!("rbitcoin-cli {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        if a == "--rpcwallet" {
            i += 1;
            if i >= args.len() {
                eprintln!("error: --rpcwallet requires a value");
                return ExitCode::from(2);
            }
            rpcwallet = Some(args[i].to_string_lossy().into_owned());
            i += 1;
            continue;
        }
        if command.is_none() {
            command = Some(args[i].to_string_lossy().into_owned());
            i += 1;
            continue;
        }
        eprintln!("error: unexpected argument {}", args[i].to_string_lossy());
        return ExitCode::from(2);
    }

    dispatch(rpcwallet.as_deref(), command.as_deref())
}

fn dispatch(rpcwallet: Option<&str>, command: Option<&str>) -> ExitCode {
    let path = rpc_wallet_path(rpcwallet);
    match command {
        None => {
            eprintln!(
                "rbitcoin-cli {} — no command; RPC not yet connected",
                env!("CARGO_PKG_VERSION")
            );
            eprintln!("wallet path: {path}");
            ExitCode::from(1)
        }
        Some("help") => {
            eprintln!("rbitcoin-cli: RPC methods not yet implemented (Phase 7)");
            ExitCode::SUCCESS
        }
        Some(cmd) => {
            eprintln!("error: RPC command `{cmd}` not implemented (path={path})");
            ExitCode::from(1)
        }
    }
}
