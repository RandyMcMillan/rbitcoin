use serde_json::Value;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Clone, Debug)]
pub enum RpcEndpoint {
    Unix(PathBuf),
    Tcp {
        host: String,
        port: u16,
        token_file: PathBuf,
    },
}

impl RpcEndpoint {
    pub fn label(&self) -> String {
        match self {
            Self::Unix(path) => format!("unix {}", path.display()),
            Self::Tcp { host, port, .. } => format!("tcp {host}:{port}"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct BlockchainInfo {
    pub chain: String,
    pub blocks: u64,
    pub headers: u64,
    pub verification_progress: f64,
    pub initial_block_download: bool,
    pub bestblockhash: String,
    pub difficulty: f64,
    pub mediantime: u64,
    pub chainwork: String,
    pub pruned: bool,
    pub size_on_disk: u64,
    pub warnings: Option<String>,
}

impl BlockchainInfo {
    pub fn from_value(v: Value) -> Result<Self, String> {
        Ok(Self {
            chain: string_field(&v, "chain")?,
            blocks: u64_field(&v, "blocks")?,
            headers: u64_field(&v, "headers")?,
            verification_progress: f64_field(&v, "verificationprogress")?,
            initial_block_download: bool_field(&v, "initialblockdownload")?,
            bestblockhash: string_field(&v, "bestblockhash").unwrap_or_default(),
            difficulty: f64_field(&v, "difficulty").unwrap_or(0.0),
            mediantime: u64_field(&v, "mediantime").unwrap_or(0),
            chainwork: string_field(&v, "chainwork").unwrap_or_default(),
            pruned: bool_field(&v, "pruned").unwrap_or(false),
            size_on_disk: u64_field(&v, "size_on_disk").unwrap_or(0),
            warnings: optional_string_field(&v, "warnings")?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct NetworkInfo {
    pub version: u64,
    pub subversion: String,
    pub protocolversion: u64,
    pub connections_in: u64,
    pub connections_out: u64,
    pub timeoffset: i64,
    pub relayfee: f64,
    pub networkactive: bool,
    pub localaddresses: Vec<String>,
    pub warnings: Option<String>,
}

impl NetworkInfo {
    pub fn from_value(v: Value) -> Result<Self, String> {
        let localaddresses = v
            .get("localaddresses")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|a| {
                        a.get("address")
                            .and_then(Value::as_str)
                            .map(|s| s.to_string())
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(Self {
            version: u64_field(&v, "version").unwrap_or(0),
            subversion: string_field(&v, "subversion").unwrap_or_default(),
            protocolversion: u64_field(&v, "protocolversion").unwrap_or(0),
            connections_in: u64_field(&v, "connections_in")?,
            connections_out: u64_field(&v, "connections_out")?,
            timeoffset: i64_field(&v, "timeoffset")?,
            relayfee: f64_field(&v, "relayfee").unwrap_or(0.0),
            networkactive: bool_field(&v, "networkactive").unwrap_or(true),
            localaddresses,
            warnings: optional_string_field(&v, "warnings")?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct MempoolInfo {
    pub transactions: u64,
    pub bytes: u64,
    pub usage: u64,
    pub maxmempool: u64,
    pub min_fee_sat_vb: f64,
    pub total_fee: f64,
    pub unbroadcast: u64,
    pub ancestorlimit: u64,
    pub descendantlimit: u64,
}

impl MempoolInfo {
    pub fn from_value(v: Value) -> Result<Self, String> {
        let min_fee_btc_kvb = f64_field(&v, "mempoolminfee")?;
        Ok(Self {
            transactions: u64_field(&v, "size")?,
            bytes: u64_field(&v, "bytes")?,
            usage: u64_field(&v, "usage").unwrap_or(0),
            maxmempool: u64_field(&v, "maxmempool")?,
            min_fee_sat_vb: min_fee_btc_kvb * 100_000_000.0 / 1000.0,
            total_fee: f64_field(&v, "totalfee").unwrap_or(0.0),
            unbroadcast: u64_field(&v, "unbroadcastcount")?,
            ancestorlimit: u64_field(&v, "ancestorlimit").unwrap_or(0),
            descendantlimit: u64_field(&v, "descendantlimit").unwrap_or(0),
        })
    }
}

#[derive(Clone, Debug)]
pub struct PeerInfo {
    pub id: u64,
    pub addr: String,
    pub conn_type: String,
    pub inbound: bool,
    pub height: u64,
    pub ping_ms: Option<u64>,
    pub minping_ms: Option<u64>,
    pub version: u64,
    pub subver: String,
    pub synched_headers: i64,
    pub synched_blocks: i64,
    pub bytesrecv: u64,
    pub bytessent: u64,
    pub banscore: i64,
    pub addnode: bool,
}

impl PeerInfo {
    pub fn from_value(v: Value) -> Result<Self, String> {
        let ping = f64_field(&v, "pingtime").ok().map(|f| (f * 1000.0) as u64);
        let minping = f64_field(&v, "minping").ok().map(|f| (f * 1000.0) as u64);
        Ok(Self {
            id: u64_field(&v, "id")?,
            addr: string_field(&v, "addr")?,
            conn_type: string_field(&v, "connection_type").unwrap_or_default(),
            inbound: bool_field(&v, "inbound")?,
            height: u64_field(&v, "startingheight").unwrap_or(0),
            ping_ms: ping,
            minping_ms: minping,
            version: u64_field(&v, "version").unwrap_or(0),
            subver: string_field(&v, "subver").unwrap_or_default(),
            synched_headers: i64_field(&v, "synced_headers").unwrap_or(-1),
            synched_blocks: i64_field(&v, "synced_blocks").unwrap_or(-1),
            bytesrecv: u64_field(&v, "bytesrecv").unwrap_or(0),
            bytessent: u64_field(&v, "bytessent").unwrap_or(0),
            banscore: i64_field(&v, "banscore").unwrap_or(0),
            addnode: bool_field(&v, "addnode").unwrap_or(false),
        })
    }
}

pub fn parse_http_url(url: &str) -> Result<(String, u16), String> {
    let rest = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .unwrap_or(url);
    let (host, port) = rest
        .rsplit_once(':')
        .ok_or_else(|| format!("--rpc-url needs host:port (got {url})"))?;
    let port = port
        .parse::<u16>()
        .map_err(|_| format!("invalid --rpc-url port in {url}"))?;
    if host.is_empty() {
        return Err(format!("invalid --rpc-url host in {url}"));
    }
    Ok((host.to_string(), port))
}

pub fn rpc_call(
    endpoint: &RpcEndpoint,
    method: &str,
    params: &[Value],
    timeout: Duration,
) -> Result<Value, String> {
    let body = serde_json::json!({
        "jsonrpc": "1.0",
        "id": "1",
        "method": method,
        "params": params,
    });
    let body = serde_json::to_vec(&body).map_err(|e| e.to_string())?;
    let result = match endpoint {
        RpcEndpoint::Unix(path) => {
            #[cfg(unix)]
            {
                let stream = UnixStream::connect(path)
                    .map_err(|e| format!("connect {}: {e}", path.display()))?;
                stream.set_read_timeout(Some(timeout)).ok();
                stream.set_write_timeout(Some(timeout)).ok();
                rpc_http(stream, "localhost", 0, None, &body)
            }
            #[cfg(not(unix))]
            {
                Err("unix sockets are not supported on this platform".into())
            }
        }
        RpcEndpoint::Tcp {
            host,
            port,
            token_file,
        } => {
            let stream = TcpStream::connect((host.as_str(), *port))
                .map_err(|e| format!("connect {host}:{port}: {e}"))?;
            stream.set_read_timeout(Some(timeout)).ok();
            stream.set_write_timeout(Some(timeout)).ok();
            let token = read_token(token_file)?;
            rpc_http(stream, host, *port, Some(&token), &body)
        }
    }?;
    let v: Value = serde_json::from_str(&result).map_err(|e| format!("rpc json: {e}"))?;
    if let Some(err) = v.get("error").filter(|e| !e.is_null()) {
        let msg = err
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("rpc error");
        let code = err.get("code").and_then(|c| c.as_i64()).unwrap_or(0);
        return Err(format!("RPC error {code}: {msg}"));
    }
    Ok(v.get("result").cloned().unwrap_or(Value::Null))
}

fn rpc_http(
    mut stream: impl Read + Write,
    host: &str,
    port: u16,
    auth: Option<&str>,
    body: &[u8],
) -> Result<String, String> {
    let auth_h = match auth {
        Some(token) => {
            let req = format!(
                "POST / HTTP/1.1\r\nHost: {host}:{port}\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let mut wire = req.into_bytes();
            wire.extend_from_slice(body);
            stream.write_all(&wire).map_err(|e| format!("write: {e}"))?;
            let mut buf = Vec::new();
            stream
                .read_to_end(&mut buf)
                .map_err(|e| format!("read: {e}"))?;
            let text = String::from_utf8(buf).map_err(|e| format!("utf-8: {e}"))?;
            return parse_http_response(&text);
        }
        None => "",
    };
    let req = format!(
        "POST / HTTP/1.1\r\nHost: {host}:{port}\r\n{auth_h}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let mut wire = req.into_bytes();
    wire.extend_from_slice(body);
    stream.write_all(&wire).map_err(|e| format!("write: {e}"))?;
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .map_err(|e| format!("read: {e}"))?;
    let text = String::from_utf8(buf).map_err(|e| format!("utf-8: {e}"))?;
    parse_http_response(&text)
}

fn parse_http_response(text: &str) -> Result<String, String> {
    let (head, body) = text
        .split_once("\r\n\r\n")
        .or_else(|| text.split_once("\n\n"))
        .ok_or_else(|| "invalid HTTP response".to_string())?;
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    if status == 401 {
        return Err("RPC unauthorized (check rpc.token or --rpc-token-file)".into());
    }
    if !(200..300).contains(&status) {
        return Err(format!("HTTP {status}: {body}"));
    }
    Ok(body.to_string())
}

fn read_token(path: &Path) -> Result<String, String> {
    let line =
        std::fs::read_to_string(path).map_err(|e| format!("read token {}: {e}", path.display()))?;
    let token = line.trim();
    if token.is_empty() {
        return Err(format!("token file {}: empty", path.display()));
    }
    Ok(token.to_string())
}

fn string_field(v: &Value, key: &str) -> Result<String, String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| format!("RPC response missing string field `{key}`"))
}

fn optional_string_field(v: &Value, key: &str) -> Result<Option<String>, String> {
    match v.get(key) {
        Some(val) if val.is_null() => Ok(None),
        Some(val) => val
            .as_str()
            .map(|s| Some(s.to_owned()))
            .ok_or_else(|| format!("RPC field `{key}` is not a string or null")),
        None => Ok(None),
    }
}

fn u64_field(v: &Value, key: &str) -> Result<u64, String> {
    v.get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("RPC response missing integer field `{key}`"))
}

fn i64_field(v: &Value, key: &str) -> Result<i64, String> {
    v.get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| format!("RPC response missing integer field `{key}`"))
}

fn f64_field(v: &Value, key: &str) -> Result<f64, String> {
    v.get(key)
        .and_then(Value::as_f64)
        .ok_or_else(|| format!("RPC response missing numeric field `{key}`"))
}

fn bool_field(v: &Value, key: &str) -> Result<bool, String> {
    v.get(key)
        .and_then(Value::as_bool)
        .ok_or_else(|| format!("RPC response missing bool field `{key}`"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_http_url_accepts_plain_or_schemed_urls() {
        assert_eq!(
            parse_http_url("http://127.0.0.1:8332").expect("url"),
            ("127.0.0.1".into(), 8332)
        );
        assert_eq!(
            parse_http_url("127.0.0.1:18332").expect("url"),
            ("127.0.0.1".into(), 18332)
        );
    }

    #[test]
    fn parse_http_response_accepts_success_body() {
        let body =
            parse_http_response("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello").expect("body");
        assert_eq!(body, "hello");
    }
}
