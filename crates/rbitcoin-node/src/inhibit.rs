//! Optional suspend/idle inhibit via `systemd-inhibit` (Linux + systemd).
//!
//! Holds a block-mode inhibit for the process lifetime by running
//! `systemd-inhibit … cat` with stdin kept open. Dropping this guard closes
//! stdin and reaps the child, releasing the inhibit.
//!
//! No-op when the binary is missing or spawn fails (non-systemd hosts).

use rbitcoin_log::{info, warn};
use std::process::{Child, Command, Stdio};

/// RAII guard: system will not auto-suspend/idle while this is alive (if acquired).
pub struct SuspendInhibit {
    child: Child,
}

impl SuspendInhibit {
    /// Try to acquire a systemd sleep/idle inhibit. Returns `None` if unavailable.
    pub fn try_start(why: &str) -> Option<Self> {
        // `--what`: block automatic sleep and idle; leave lid/power-key alone
        // so the operator can still force power-off intentionally.
        let mut child = match Command::new("systemd-inhibit")
            .args([
                "--what=sleep:idle",
                "--who=rbitcoin-node",
                "--why",
                why,
                "--mode=block",
                "cat",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(c) => c,
            Err(_) => return None, // no binary / not Linux
        };

        // Keep child.stdin open so `cat` blocks until Drop.
        if child.stdin.is_none() {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }

        // Inhibit fails fast if logind is unavailable.
        std::thread::sleep(std::time::Duration::from_millis(50));
        match child.try_wait() {
            Ok(Some(status)) => {
                warn!("node: systemd-inhibit exited early ({status}); suspend not inhibited");
                return None;
            }
            Ok(None) => {}
            Err(e) => {
                warn!("node: systemd-inhibit status: {e}; suspend not inhibited");
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }

        info!("node: suspend/idle inhibited via systemd-inhibit ({why})");
        Some(Self { child })
    }
}

impl Drop for SuspendInhibit {
    fn drop(&mut self) {
        // Closing stdin ends `cat` → systemd-inhibit releases the lock.
        drop(self.child.stdin.take());
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_start_and_drop_is_safe() {
        // On hosts without systemd-inhibit this returns None; with it, Drop reaps.
        let g = SuspendInhibit::try_start("rbitcoin unit test");
        drop(g);
        // Second call still safe.
        let _ = SuspendInhibit::try_start("rbitcoin unit test 2");
    }
}
