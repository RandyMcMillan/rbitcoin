use rbitcoin_tui::tui_main;
use std::process::ExitCode;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> ExitCode {
    tui_main(std::env::args_os())
}
