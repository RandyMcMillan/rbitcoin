//! Newline JSON-RPC Electrum client (TCP). Pipelines N requests per write.

use crate::jsonrpc;
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

pub struct ElectrumClient {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
    next_id: u64,
    timeout: Duration,
}

impl ElectrumClient {
    pub async fn connect(addr: &str, timeout: Duration) -> Result<Self, String> {
        let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| format!("connect timeout {addr}"))?
            .map_err(|e| e.to_string())?;
        stream.set_nodelay(true).map_err(|e| e.to_string())?;
        let (r, w) = stream.into_split();
        let mut c = Self {
            reader: BufReader::new(r),
            writer: w,
            next_id: 1,
            timeout,
        };
        let _ = c
            .call("server.version", json!(["rbitcoin-bench", "1.4"]))
            .await?;
        Ok(c)
    }

    pub async fn call(&mut self, method: &str, params: Value) -> Result<(Value, u64), String> {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        let line = jsonrpc::request(id, method, params);
        let t0 = Instant::now();
        self.writer
            .write_all(line.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        self.writer
            .write_all(b"\n")
            .await
            .map_err(|e| e.to_string())?;
        self.writer.flush().await.map_err(|e| e.to_string())?;
        let mut resp = String::new();
        tokio::time::timeout(self.timeout, self.reader.read_line(&mut resp))
            .await
            .map_err(|_| "read timeout".to_string())?
            .map_err(|e| e.to_string())?;
        let nanos = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
        Ok((jsonrpc::result_ok(&resp)?, nanos))
    }

    /// Write `reqs` lines then read that many responses. Wall is start-write to last-read.
    pub async fn call_batch(
        &mut self,
        methods: &[(&str, Value)],
    ) -> Result<(Vec<Value>, u64), String> {
        if methods.is_empty() {
            return Ok((Vec::new(), 0));
        }
        let t0 = Instant::now();
        let mut ids = Vec::with_capacity(methods.len());
        let mut buf = String::new();
        for (method, params) in methods {
            let id = self.next_id;
            self.next_id = self.next_id.saturating_add(1);
            ids.push(id);
            buf.push_str(&jsonrpc::request(id, method, params.clone()));
            buf.push('\n');
        }
        self.writer
            .write_all(buf.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        self.writer.flush().await.map_err(|e| e.to_string())?;
        let mut out = Vec::with_capacity(methods.len());
        for _ in ids {
            let mut resp = String::new();
            tokio::time::timeout(self.timeout, self.reader.read_line(&mut resp))
                .await
                .map_err(|_| "read timeout".to_string())?
                .map_err(|e| e.to_string())?;
            out.push(jsonrpc::result_ok(&resp)?);
        }
        let nanos = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
        Ok((out, nanos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    async fn serve_one_line(reply: &'static str) -> String {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            let (r, mut w) = s.into_split();
            let mut br = BufReader::new(r);
            let mut line = String::new();
            let _ = br.read_line(&mut line).await;
            // handshake
            w.write_all(b"{\"id\":1,\"result\":[\"ok\",\"1.4\"]}\n")
                .await
                .unwrap();
            let mut line2 = String::new();
            let _ = br.read_line(&mut line2).await;
            w.write_all(reply.as_bytes()).await.unwrap();
            w.write_all(b"\n").await.unwrap();
        });
        addr
    }

    #[tokio::test]
    async fn call_balance() {
        let addr = serve_one_line(r#"{"id":2,"result":{"confirmed":1,"unconfirmed":0}}"#).await;
        let mut c = ElectrumClient::connect(&addr, Duration::from_secs(2))
            .await
            .unwrap();
        let (v, ns) = c
            .call(
                "blockchain.scripthash.get_balance",
                json!(["ab".repeat(32)]),
            )
            .await
            .unwrap();
        assert_eq!(v["confirmed"], 1);
        assert!(ns > 0);
    }
}
