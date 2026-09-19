//! Datadir socket / bearer-token JSON-RPC client for the documented node subset.

use rbitcoin_primitives::Network;
use serde_json::Value;
use std::ffi::OsString;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

fn usage() -> String {
    format!(
        "rbitcoin-cli {} — usage: rbitcoin-cli [OPTIONS] <COMMAND> [PARAMS...]\n\
         \n\
         Options:\n\
           --datadir PATH         node datadir (default ./datadir); unix socket PATH/rpc.sock\n\
           --network NET          mainnet|testnet|signet|regtest (default TCP port)\n\
           --rpc-url URL          HTTP JSON-RPC (default http://127.0.0.1:<network port>)\n\
           --rpc-token-file PATH  Bearer token (default PATH/rpc.token from --datadir)\n\
           -h, --help             this message\n\
           -V, --version          print version\n\
         \n\
         Local: --datadir talks to {{datadir}}/rpc.sock (no token). TCP uses Bearer\n\
         from {{datadir}}/rpc.token. Plain HTTP, same as the node.",
        env!("CARGO_PKG_VERSION")
    )
}

struct CliConfig {
    datadir: PathBuf,
    network: Network,
    rpc_url: Option<String>,
    token_file: Option<PathBuf>,
    command: Option<String>,
    params: Vec<String>,
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            datadir: PathBuf::from(".").join("datadir"),
            network: Network::Mainnet,
            rpc_url: None,
            token_file: None,
            command: None,
            params: Vec::new(),
        }
    }
}

enum Action {
    Help,
    Version,
    Call(CliConfig),
}

fn take_value(args: &[OsString], i: &mut usize, flag: &str) -> Result<String, String> {
    *i += 1;
    args.get(*i)
        .map(|s| s.to_string_lossy().into_owned())
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn flag_name_and_eq(raw: &str) -> Option<(&str, Option<&str>)> {
    let rest = raw.strip_prefix("--")?;
    match rest.split_once('=') {
        Some((name, val)) => Some((name, Some(val))),
        None => Some((rest, None)),
    }
}

fn parse_args(args: &[OsString]) -> Result<Action, String> {
    let mut cfg = CliConfig::default();
    let mut i = 1usize;
    while i < args.len() {
        let a = args[i].to_string_lossy();
        if a == "--help" || a == "-h" {
            return Ok(Action::Help);
        }
        if a == "--version" || a == "-V" {
            return Ok(Action::Version);
        }
        if a.starts_with('-') && !a.starts_with("--") {
            return Err(format!("unknown argument `{a}`"));
        }
        if let Some((name, eq)) = flag_name_and_eq(a.as_ref()) {
            let val = match eq {
                Some(v) => {
                    if v.is_empty() {
                        return Err(format!("--{name} requires a value"));
                    }
                    v.to_string()
                }
                None => take_value(args, &mut i, &format!("--{name}"))?,
            };
            match name {
                "datadir" => cfg.datadir = PathBuf::from(val),
                "network" => {
                    cfg.network =
                        Network::parse(&val).map_err(|e| format!("invalid --network: {e}"))?;
                }
                "rpc-url" => cfg.rpc_url = Some(val),
                "rpc-token-file" => cfg.token_file = Some(PathBuf::from(val)),
                other => return Err(format!("unknown argument `--{other}`")),
            }
            i += 1;
            continue;
        }
        if cfg.command.is_none() {
            cfg.command = Some(a.into_owned());
        } else {
            cfg.params.push(a.into_owned());
        }
        i += 1;
    }
    Ok(Action::Call(cfg))
}

fn socket_path(cfg: &CliConfig) -> PathBuf {
    cfg.datadir.join("rpc.sock")
}

fn token_path(cfg: &CliConfig) -> PathBuf {
    cfg.token_file.clone().unwrap_or_else(|| cfg.datadir.join("rpc.token"))
}

fn parse_http_url(url: &str) -> Result<(String, u16), String> {
    let rest = url.strip_prefix("http://").or_else(|| url.strip_prefix("https://")).unwrap_or(url);
    let (host, port) =
        rest.rsplit_once(':').ok_or_else(|| format!("--rpc-url needs host:port (got {url})"))?;
    let port: u16 = port.parse().map_err(|_| format!("invalid --rpc-url port in {url}"))?;
    if host.is_empty() {
        return Err(format!("invalid --rpc-url host in {url}"));
    }
    Ok((host.to_string(), port))
}

fn read_token(path: &Path) -> Result<String, String> {
    let line =
        std::fs::read_to_string(path).map_err(|e| format!("read token {}: {e}", path.display()))?;
    let t = line.trim();
    if t.is_empty() {
        return Err(format!("token file {}: empty", path.display()));
    }
    Ok(t.to_string())
}

fn param_value(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_string()))
}

fn rpc_http(
    mut stream: impl Read + Write,
    host: &str,
    port: u16,
    auth: Option<&str>,
    method: &str,
    params: &[Value],
) -> Result<Value, String> {
    let body = serde_json::json!({
        "jsonrpc": "1.0",
        "id": "1",
        "method": method,
        "params": params,
    });
    let body = serde_json::to_vec(&body).map_err(|e| e.to_string())?;
    let auth_h = match auth {
        Some(t) => format!("Authorization: Bearer {t}\r\n"),
        None => String::new(),
    };
    let req = format!(
        "POST / HTTP/1.1\r\n\
         Host: {host}:{port}\r\n\
         {auth_h}\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        body.len()
    );
    let mut wire = req.into_bytes();
    wire.extend_from_slice(&body);
    stream.write_all(&wire).map_err(|e| format!("write: {e}"))?;
    let (status, resp_body) = read_http(&mut stream)?;
    if status == 401 {
        return Err("RPC unauthorized (check rpc.token or --rpc-token-file)".into());
    }
    if !(200..300).contains(&status) {
        return Err(format!("HTTP {status}: {resp_body}"));
    }
    let v: Value = serde_json::from_str(&resp_body).map_err(|e| format!("rpc json: {e}"))?;
    if let Some(err) = v.get("error").filter(|e| !e.is_null()) {
        let msg = err.get("message").and_then(|m| m.as_str()).unwrap_or("rpc error");
        let code = err.get("code").and_then(|c| c.as_i64()).unwrap_or(0);
        return Err(format!("RPC error {code}: {msg}"));
    }
    Ok(v.get("result").cloned().unwrap_or(Value::Null))
}

fn connect_unix(path: &Path) -> Result<socket2::Socket, String> {
    let sock = socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None)
        .map_err(|e| format!("unix socket: {e}"))?;
    let addr = socket2::SockAddr::unix(path).map_err(|e| format!("unix addr: {e}"))?;
    sock.connect(&addr).map_err(|e| format!("connect {}: {e}", path.display()))?;
    Ok(sock)
}

fn read_http(stream: &mut impl Read) -> Result<(u16, String), String> {
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).map_err(|e| format!("read: {e}"))?;
    let text = String::from_utf8_lossy(&buf);
    let (head, body) = text
        .split_once("\r\n\r\n")
        .or_else(|| text.split_once("\n\n"))
        .ok_or("invalid HTTP response")?;
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    Ok((status, body.to_string()))
}

fn format_result(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn dispatch_call(cfg: &CliConfig) -> Result<String, String> {
    let cmd = cfg.command.as_deref().ok_or_else(usage)?;
    if cmd == "help" {
        return Ok(usage());
    }
    let params: Vec<Value> = cfg.params.iter().map(|p| param_value(p)).collect();
    let sock = socket_path(cfg);
    let result = if cfg.rpc_url.is_none() && sock.exists() {
        let stream = connect_unix(&sock)?;
        rpc_http(stream, "localhost", 0, None, cmd, &params)?
    } else {
        let (host, port) = match cfg.rpc_url.as_deref() {
            Some(u) => parse_http_url(u)?,
            None => ("127.0.0.1".into(), cfg.network.default_rpc_port()),
        };
        let stream = TcpStream::connect((host.as_str(), port))
            .map_err(|e| format!("connect {host}:{port}: {e}"))?;
        stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
        stream.set_write_timeout(Some(Duration::from_secs(30))).ok();
        let token = read_token(&token_path(cfg))?;
        rpc_http(stream, &host, port, Some(&token), cmd, &params)?
    };
    Ok(format_result(&result))
}

/// Process entry used by `main` and high-level scenarios.
pub fn cli_main<I, T>(args: I) -> ExitCode
where
    I: IntoIterator<Item = T>,
    T: Into<OsString>,
{
    let args: Vec<OsString> = args.into_iter().map(Into::into).collect();
    let action = match parse_args(&args) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };
    match action {
        Action::Help => {
            eprintln!("{}", usage());
            ExitCode::SUCCESS
        }
        Action::Version => {
            eprintln!("rbitcoin-cli {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Action::Call(cfg) => match dispatch_call(&cfg) {
            Ok(out) => {
                println!("{out}");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: {e}");
                ExitCode::from(1)
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn exit_ok(c: ExitCode) -> bool {
        format!("{c:?}") == format!("{:?}", ExitCode::SUCCESS)
    }

    fn tmp_datadir() -> PathBuf {
        let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let p = std::env::temp_dir().join(format!("rbitcoin-cli-{n}"));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn spawn_rpc_mock(token: &str, result_json: &str) -> (u16, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let expect = format!("Bearer {token}");
        let result = result_json.to_string();
        let h = thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut raw = Vec::new();
            let mut tmp = [0u8; 1024];
            while !raw.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = s.read(&mut tmp).unwrap();
                if n == 0 {
                    break;
                }
                raw.extend_from_slice(&tmp[..n]);
            }
            let req = String::from_utf8_lossy(&raw);
            let authorized = req.lines().any(|line| {
                line.trim()
                    .strip_prefix("Authorization:")
                    .map(|r| r.trim().eq_ignore_ascii_case(&expect))
                    .unwrap_or(false)
            });
            let body = if authorized {
                format!("{{\"jsonrpc\":\"1.0\",\"id\":\"1\",\"result\":{result},\"error\":null}}\n")
            } else {
                String::new()
            };
            if authorized {
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                s.write_all(resp.as_bytes()).unwrap();
            } else {
                s.write_all(b"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Bearer realm=\"jsonrpc\"\r\nContent-Length: 13\r\nConnection: close\r\n\r\nUnauthorized\n").unwrap();
            }
        });
        (port, h)
    }

    #[test]
    fn param_tokens_json_or_string() {
        assert_eq!(param_value("0"), serde_json::json!(0));
        assert_eq!(param_value("true"), serde_json::json!(true));
        assert_eq!(param_value("abc"), serde_json::json!("abc"));
    }

    #[test]
    fn help_and_version_do_not_dial() {
        assert!(exit_ok(cli_main(["rbitcoin-cli", "--help"])));
        assert!(exit_ok(cli_main(["rbitcoin-cli", "-V"])));
        assert!(exit_ok(cli_main(["rbitcoin-cli", "help"])));
    }

    #[test]
    fn equals_form_datadir_is_accepted() {
        let dir = tmp_datadir();
        std::fs::write(dir.join("rpc.token"), "s3cret").unwrap();
        let (port, h) = spawn_rpc_mock("s3cret", "0");
        let flag = format!("--datadir={}", dir.display());
        let code = cli_main([
            "rbitcoin-cli",
            flag.as_str(),
            &format!("--rpc-url=http://127.0.0.1:{port}"),
            "getblockcount",
        ]);
        let _ = std::fs::remove_dir_all(&dir);
        assert!(exit_ok(code), "--foo=bar form must parse, got {code:?}");
        let _ = h.join();
    }

    #[test]
    fn usage_describes_every_option() {
        let u = usage();
        for needle in [
            "--datadir PATH",
            "--network NET",
            "--rpc-url URL",
            "--rpc-token-file PATH",
            "-h, --help",
            "-V, --version",
        ] {
            assert!(u.contains(needle), "usage must list {needle}");
        }
        assert!(
            !u.contains("--rpcuser") && !u.contains("--rpcport") && !u.contains("--rpcconnect"),
            "dropped Core names must not be advertised: {u}"
        );
        assert!(
            u.contains("-V, --version") && u.contains("print version"),
            "version must have a description"
        );
    }

    #[test]
    fn getblockcount_token_against_mock() {
        let dir = tmp_datadir();
        std::fs::write(dir.join("rpc.token"), "s3cret").unwrap();
        let (port, h) = spawn_rpc_mock("s3cret", "0");
        let code = cli_main([
            "rbitcoin-cli",
            "--datadir",
            dir.to_str().unwrap(),
            "--rpc-url",
            &format!("http://127.0.0.1:{port}"),
            "getblockcount",
        ]);
        let _ = std::fs::remove_dir_all(&dir);
        assert!(
            exit_ok(code),
            "token getblockcount must succeed against a live RPC mock, got {code:?}"
        );
        let _ = h.join();
    }

    #[test]
    fn missing_auth_is_unauthorized() {
        let (port, _h) = spawn_rpc_mock("alice", "0");
        let dir = tmp_datadir();
        let code = cli_main([
            "rbitcoin-cli",
            "--datadir",
            dir.to_str().unwrap(),
            "--rpc-url",
            &format!("http://127.0.0.1:{port}"),
            "getblockcount",
        ]);
        let _ = std::fs::remove_dir_all(&dir);
        assert!(!exit_ok(code));
    }

    #[test]
    fn default_rpc_port_follows_network() {
        assert_eq!(Network::Mainnet.default_rpc_port(), 8332);
        assert_eq!(Network::Regtest.default_rpc_port(), 18443);
    }

    #[cfg(unix)]
    #[test]
    fn unix_socket_needs_no_token() {
        use std::os::unix::net::UnixListener;
        let dir = tmp_datadir();
        let sock = dir.join("rpc.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        let h = thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut raw = Vec::new();
            let mut tmp = [0u8; 1024];
            while !raw.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = s.read(&mut tmp).unwrap();
                if n == 0 {
                    break;
                }
                raw.extend_from_slice(&tmp[..n]);
            }
            let body =
                "{\"jsonrpc\":\"1.0\",\"id\":\"1\",\"result\":7,\"error\":null}\n".to_string();
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            s.write_all(resp.as_bytes()).unwrap();
        });
        let code = cli_main(["rbitcoin-cli", "--datadir", dir.to_str().unwrap(), "getblockcount"]);
        let _ = std::fs::remove_dir_all(&dir);
        assert!(exit_ok(code), "unix getblockcount must succeed, got {code:?}");
        let _ = h.join();
    }
}
