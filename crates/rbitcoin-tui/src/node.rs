use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::Receiver;

/// Try to find the `rbitcoin-node` binary.
///
/// 1. Use an explicit path if provided.
/// 2. Try a sibling of the current executable.
/// 3. Fall back to `rbitcoin-node` in `PATH`.
pub fn find_node_binary(explicit: Option<&Path>) -> PathBuf {
    if let Some(p) = explicit {
        return p.to_path_buf();
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let sibling = dir.join("rbitcoin-node");
            if sibling.exists() {
                return sibling;
            }
        }
    }
    PathBuf::from("rbitcoin-node")
}

/// Spawn the node as a child process and return a channel that receives stderr lines.
pub fn spawn_node(datadir: &Path, binary: Option<&Path>) -> Result<(Child, Receiver<String>), String> {
    let bin = find_node_binary(binary);
    let mut cmd = Command::new(&bin);
    cmd.arg("--datadir").arg(datadir);
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::null());
    cmd.stderr(Stdio::piped());
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("spawn {}: {e}", bin.display()))?;

    let stderr = child.stderr.take().expect("piped stderr");
    let (tx, rx) = std::sync::mpsc::channel();

    std::thread::spawn(move || {
        let reader = std::io::BufReader::new(stderr);
        let mut lines = std::io::BufRead::lines(reader);
        while let Some(Ok(l)) = lines.next() {
            if tx.send(l).is_err() {
                break;
            }
        }
    });

    Ok((child, rx))
}

/// Poll whether the child is still alive. If it exited, return the exit code.
pub fn check_child(child: &mut Child) -> Option<i32> {
    match child.try_wait() {
        Ok(Some(status)) => status.code(),
        _ => None,
    }
}

/// Kill the child process and reap it.
pub fn kill_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_node_binary_uses_explicit_path() {
        let explicit = PathBuf::from("/opt/rbitcoin-node");
        let found = find_node_binary(Some(&explicit));
        assert_eq!(found, explicit);
    }

    #[test]
    fn find_node_binary_fallback_name() {
        let found = find_node_binary(None);
        assert_eq!(found, PathBuf::from("rbitcoin-node"));
    }
}
