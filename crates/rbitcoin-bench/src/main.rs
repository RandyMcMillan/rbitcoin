use rbitcoin_bench::cli_main;
use std::process::ExitCode;

fn main() -> ExitCode {
    cli_main(std::env::args_os())
}
