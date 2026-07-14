//! CLI library helpers (RPC client lands with node RPC phase).

use std::ffi::OsString;
use std::process::ExitCode;

pub fn crate_name() -> &'static str {
    "rbitcoin-cli"
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
    let mut command: Option<String> = None;

    while i < args.len() {
        let a = args[i].as_os_str();
        if a == "--help" || a == "-h" {
            eprintln!(
                "rbitcoin-cli {} — usage: rbitcoin-cli [COMMAND]",
                env!("CARGO_PKG_VERSION")
            );
            return ExitCode::SUCCESS;
        }
        if a == "--version" || a == "-V" {
            eprintln!("rbitcoin-cli {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        if command.is_none() {
            command = Some(args[i].to_string_lossy().into_owned());
            i += 1;
            continue;
        }
        eprintln!("error: unexpected argument {}", args[i].to_string_lossy());
        return ExitCode::from(2);
    }

    dispatch(command.as_deref())
}

fn dispatch(command: Option<&str>) -> ExitCode {
    match command {
        None => {
            eprintln!(
                "rbitcoin-cli {} — no command; RPC not yet connected",
                env!("CARGO_PKG_VERSION")
            );
            ExitCode::from(1)
        }
        Some("help") => {
            eprintln!("rbitcoin-cli: node RPC methods not yet implemented");
            ExitCode::SUCCESS
        }
        Some(cmd) => {
            eprintln!("error: RPC command `{cmd}` not implemented");
            ExitCode::from(1)
        }
    }
}
