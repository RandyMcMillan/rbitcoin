//! RPC auth: datadir token file (Bearer).

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Bearer token accepted by the RPC server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RpcAuth {
    pub token: String,
}

impl RpcAuth {
    pub fn new(token: impl Into<String>) -> Self {
        Self { token: token.into() }
    }

    pub fn matches_token(&self, token: &str) -> bool {
        self.token == token
    }
}

pub fn default_token_path(datadir: &Path) -> PathBuf {
    datadir.join("rpc.token")
}

pub fn default_socket_path(datadir: &Path) -> PathBuf {
    datadir.join("rpc.sock")
}

/// Read an existing token or write a new CSPRNG token (mode 0600).
pub fn resolve_rpc_auth(
    datadir: &Path,
    token_path: Option<&Path>,
) -> Result<(RpcAuth, PathBuf), String> {
    let path = token_path.map(|p| p.to_path_buf()).unwrap_or_else(|| default_token_path(datadir));
    if path.is_file() {
        let auth = read_token_file(&path)?;
        return Ok((auth, path));
    }
    let auth = write_token_file(&path)?;
    Ok((auth, path))
}

pub fn read_token_file(path: &Path) -> Result<RpcAuth, String> {
    let line =
        fs::read_to_string(path).map_err(|e| format!("read token {}: {e}", path.display()))?;
    let token = line.trim();
    if token.is_empty() {
        return Err(format!("token file {}: empty", path.display()));
    }
    Ok(RpcAuth::new(token))
}

pub fn write_token_file(path: &Path) -> Result<RpcAuth, String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("token parent: {e}"))?;
    }
    let auth = RpcAuth::new(random_token());
    let mut f = fs::File::create(path).map_err(|e| format!("token create: {e}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
    }
    f.write_all(auth.token.as_bytes()).map_err(|e| format!("token write: {e}"))?;
    f.sync_all().map_err(|e| format!("token sync: {e}"))?;
    Ok(auth)
}

/// Parse `Authorization: Bearer …`.
pub fn parse_bearer_auth(header: &str) -> Option<&str> {
    let header = header.trim();
    header
        .strip_prefix("Bearer ")
        .or_else(|| header.strip_prefix("bearer "))
        .map(str::trim)
        .filter(|t| !t.is_empty())
}

fn random_token() -> String {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).expect("CSPRNG for RPC token");
    let mut out = String::with_capacity(64);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp() -> PathBuf {
        let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        std::env::temp_dir().join(format!("rbitcoin-rpc-auth-{n}"))
    }

    #[test]
    fn token_roundtrip() {
        let dir = tmp();
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("rpc.token");
        let a = write_token_file(&path).unwrap();
        let b = read_token_file(&path).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.token.len(), 64);
        assert!(a.token.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_reuses_existing_token() {
        let dir = tmp();
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("rpc.token");
        fs::write(&path, "fixed-token\n").unwrap();
        let (a, p) = resolve_rpc_auth(&dir, None).unwrap();
        assert_eq!(a.token, "fixed-token");
        assert_eq!(p, path);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn token_is_csprng_hex() {
        let dir = tmp();
        fs::create_dir_all(&dir).unwrap();
        let mut seen = std::collections::HashSet::new();
        for i in 0..32 {
            let path = dir.join(format!("rpc.token-{i}"));
            let a = write_token_file(&path).unwrap();
            assert_eq!(a.token.len(), 64);
            assert!(seen.insert(a.token.clone()));
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn bearer_parse_and_token_paths() {
        assert_eq!(parse_bearer_auth("Bearer abc"), Some("abc"));
        assert_eq!(parse_bearer_auth("bearer xyz"), Some("xyz"));
        assert!(parse_bearer_auth("Bearer ").is_none());
        assert!(parse_bearer_auth("Basic abc").is_none());
        let a = RpcAuth::new("s3cret");
        assert!(a.matches_token("s3cret"));
        assert!(!a.matches_token("nope"));
        assert_eq!(default_socket_path(Path::new("/d")), PathBuf::from("/d/rpc.sock"));
        assert_eq!(default_token_path(Path::new("/d")), PathBuf::from("/d/rpc.token"));
    }

    #[test]
    fn empty_token_file_errors() {
        let dir = tmp();
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("rpc.token");
        fs::write(&path, "  \n").unwrap();
        assert!(read_token_file(&path).is_err());
        let _ = fs::remove_dir_all(&dir);
    }
}
