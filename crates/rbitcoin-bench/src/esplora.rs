//! HTTP/1.1 GET client for Esplora REST (plain HTTP, keep-alive).

use serde_json::Value;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

pub struct EsploraClient {
    host: String,
    port: u16,
    stream: BufReader<TcpStream>,
    timeout: Duration,
}

impl EsploraClient {
    pub fn parse_url(url: &str) -> Result<(String, u16), String> {
        let u = url.trim();
        let rest = u.strip_prefix("http://").ok_or_else(|| {
            "esplora url must be http://host:port (no TLS in this client)".to_string()
        })?;
        if rest.contains('/') && rest.find('/').unwrap_or(0) > 0 {
            let hostport = rest.split('/').next().unwrap_or(rest);
            return split_host_port(hostport);
        }
        split_host_port(rest)
    }

    pub async fn connect(url: &str, timeout: Duration) -> Result<Self, String> {
        let (host, port) = Self::parse_url(url)?;
        let stream = tokio::time::timeout(timeout, TcpStream::connect((host.as_str(), port)))
            .await
            .map_err(|_| format!("connect timeout {host}:{port}"))?
            .map_err(|e| e.to_string())?;
        stream.set_nodelay(true).map_err(|e| e.to_string())?;
        Ok(Self {
            host,
            port,
            stream: BufReader::new(stream),
            timeout,
        })
    }

    pub async fn get_json(&mut self, path: &str) -> Result<(Value, u64), String> {
        let req = format!(
            "GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: keep-alive\r\nAccept: application/json\r\n\r\n",
            host = self.host,
            port = self.port
        );
        let t0 = Instant::now();
        self.stream
            .get_mut()
            .write_all(req.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        let mut headers = Vec::new();
        loop {
            let mut line = String::new();
            tokio::time::timeout(self.timeout, self.stream.read_line(&mut line))
                .await
                .map_err(|_| "read timeout".to_string())?
                .map_err(|e| e.to_string())?;
            if line == "\r\n" || line == "\n" {
                break;
            }
            headers.push(line);
        }
        if headers
            .first()
            .is_none_or(|s| !s.contains(" 200 ") && !s.starts_with("HTTP/1.1 200"))
        {
            return Err(format!(
                "http {}",
                headers.first().map(|s| s.trim()).unwrap_or("no status")
            ));
        }
        let mut len: Option<usize> = None;
        for h in &headers {
            let l = h.to_ascii_lowercase();
            if let Some(rest) = l.strip_prefix("content-length:") {
                len = Some(
                    rest.trim()
                        .parse()
                        .map_err(|_| "bad content-length".to_string())?,
                );
            }
        }
        let body = if let Some(n) = len {
            let mut buf = vec![0u8; n];
            tokio::time::timeout(self.timeout, self.stream.read_exact(&mut buf))
                .await
                .map_err(|_| "read timeout".to_string())?
                .map_err(|e| e.to_string())?;
            buf
        } else {
            return Err("response missing content-length".into());
        };
        let nanos = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let v: Value = serde_json::from_slice(&body).map_err(|e| e.to_string())?;
        Ok((v, nanos))
    }
}

fn split_host_port(hostport: &str) -> Result<(String, u16), String> {
    let hostport = hostport.trim().trim_end_matches('/');
    if let Some((h, p)) = hostport.rsplit_once(':') {
        let port: u16 = p.parse().map_err(|_| format!("bad port {p}"))?;
        Ok((h.trim_matches('[').trim_matches(']').to_string(), port))
    } else {
        Ok((hostport.to_string(), 80))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn parse_url_http() {
        let (h, p) = EsploraClient::parse_url("http://127.0.0.1:3000").unwrap();
        assert_eq!((h, p), ("127.0.0.1".into(), 3000));
        assert!(EsploraClient::parse_url("https://x").is_err());
    }

    #[tokio::test]
    async fn get_json_ok() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            let mut buf = [0u8; 512];
            let _ = s.read(&mut buf).await;
            let body = b"{\"chain_stats\":{\"tx_count\":3}}";
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
                body.len()
            );
            s.write_all(resp.as_bytes()).await.unwrap();
            s.write_all(body).await.unwrap();
        });
        let mut c = EsploraClient::connect(
            &format!("http://127.0.0.1:{}", addr.port()),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let (v, ns) = c.get_json("/scripthash/aa").await.unwrap();
        assert_eq!(v["chain_stats"]["tx_count"], 3);
        assert!(ns > 0);
    }
}
