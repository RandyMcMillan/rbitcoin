use rbitcoin_node::cli_main;
use std::process::ExitCode;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> ExitCode {
    cli_main(std::env::args_os())
}
