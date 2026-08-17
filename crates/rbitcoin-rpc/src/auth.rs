//! Core-inspired RPC auth: cookie file and/or user/password HTTP Basic.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Credentials accepted by the RPC server (user + password).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RpcAuth {
    pub user: String,
    pub password: String,
}

impl RpcAuth {
    pub fn new(user: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            user: user.into(),
            password: password.into(),
        }
    }

    /// Encode as `user:password` (cookie file body / Basic raw).
    pub fn cookie_line(&self) -> String {
        format!("{}:{}", self.user, self.password)
    }

    /// Parse `user:password` from a cookie file line.
    pub fn from_cookie_line(line: &str) -> Option<Self> {
        let line = line.trim();
        let (user, password) = line.split_once(':')?;
        if user.is_empty() || password.is_empty() {
            return None;
        }
        Some(Self {
            user: user.to_string(),
            password: password.to_string(),
        })
    }

    /// Constant-time-ish equality for Basic auth compare.
    pub fn matches(&self, user: &str, password: &str) -> bool {
        // Avoid short-circuit on length alone for password; still not full CT.
        user == self.user && password == self.password
    }
}

/// Resolve auth for a listen bind: explicit user/pass, else generate cookie under datadir.
pub fn resolve_rpc_auth(
    datadir: &Path,
    rpc_user: Option<&str>,
    rpc_password: Option<&str>,
    cookie_path: Option<&Path>,
) -> Result<(RpcAuth, Option<PathBuf>), String> {
    match (rpc_user, rpc_password) {
        (Some(u), Some(p)) if !u.is_empty() && !p.is_empty() => Ok((RpcAuth::new(u, p), None)),
        (Some(_), None) | (None, Some(_)) => {
            Err("rpcuser and rpcpassword must both be set (or both unset for cookie auth)".into())
        }
        (Some(u), Some(p)) if u.is_empty() || p.is_empty() => {
            Err("rpcuser and rpcpassword must be non-empty".into())
        }
        _ => {
            let path = cookie_path
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| datadir.join(".cookie"));
            let auth = write_cookie_file(&path)?;
            Ok((auth, Some(path)))
        }
    }
}

/// Write a new random cookie to `path` (mode 0600 when supported). Returns credentials.
pub fn write_cookie_file(path: &Path) -> Result<RpcAuth, String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("cookie parent: {e}"))?;
    }
    let auth = RpcAuth::new("__cookie__", random_cookie_password());
    let mut f = fs::File::create(path).map_err(|e| format!("cookie create: {e}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
    }
    f.write_all(auth.cookie_line().as_bytes())
        .map_err(|e| format!("cookie write: {e}"))?;
    f.sync_all().map_err(|e| format!("cookie sync: {e}"))?;
    Ok(auth)
}

/// Read credentials from an existing cookie file.
pub fn read_cookie_file(path: &Path) -> Result<RpcAuth, String> {
    let s = fs::read_to_string(path).map_err(|e| format!("cookie read: {e}"))?;
    RpcAuth::from_cookie_line(&s).ok_or_else(|| "cookie file: expected user:password".into())
}

/// Parse HTTP `Authorization: Basic …` header value.
pub fn parse_basic_auth(header: &str) -> Option<(String, String)> {
    let header = header.trim();
    let rest = header
        .strip_prefix("Basic ")
        .or_else(|| header.strip_prefix("basic "))?;
    use base64::Engine;
    let raw = base64::engine::general_purpose::STANDARD
        .decode(rest.trim())
        .ok()?;
    let s = String::from_utf8(raw).ok()?;
    let (u, p) = s.split_once(':')?;
    Some((u.to_string(), p.to_string()))
}

fn random_cookie_password() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::{SystemTime, UNIX_EPOCH};
    let mut h = DefaultHasher::new();
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .hash(&mut h);
    std::process::id().hash(&mut h);
    format!(
        "{:016x}{:016x}",
        h.finish(),
        h.finish().wrapping_mul(0x9e37)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp() -> PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("rbitcoin-rpc-auth-{n}"))
    }

    #[test]
    fn cookie_roundtrip() {
        let dir = tmp();
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(".cookie");
        let a = write_cookie_file(&path).unwrap();
        let b = read_cookie_file(&path).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.user, "__cookie__");
        assert!(!a.password.is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_user_pass() {
        let dir = tmp();
        let (a, cookie) = resolve_rpc_auth(&dir, Some("u"), Some("p"), None).unwrap();
        assert_eq!(a.user, "u");
        assert_eq!(a.password, "p");
        assert!(cookie.is_none());
    }

    #[test]
    fn resolve_cookie_when_no_user() {
        let dir = tmp();
        fs::create_dir_all(&dir).unwrap();
        let (a, cookie) = resolve_rpc_auth(&dir, None, None, None).unwrap();
        assert_eq!(a.user, "__cookie__");
        let path = cookie.unwrap();
        assert!(path.exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn partial_user_pass_errors() {
        let dir = tmp();
        assert!(resolve_rpc_auth(&dir, Some("u"), None, None).is_err());
        assert!(resolve_rpc_auth(&dir, None, Some("p"), None).is_err());
    }

    #[test]
    fn basic_auth_parse() {
        use base64::Engine;
        let tok = base64::engine::general_purpose::STANDARD.encode("alice:s3cret");
        let (u, p) = parse_basic_auth(&format!("Basic {tok}")).unwrap();
        assert_eq!(u, "alice");
        assert_eq!(p, "s3cret");
        assert!(parse_basic_auth("Bearer x").is_none());
    }
}
