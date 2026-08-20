//! Casa sequential medians and Sparrow batched wallet load/refresh.

use crate::electrum::ElectrumClient;
use crate::esplora::EsploraClient;
use crate::jsonrpc;
use crate::progress::Progress;
use crate::stats::Sample;
use serde_json::{json, Value};
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Suite {
    Casa,
    Sparrow,
    Hot,
}

impl Suite {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "casa" => Ok(Self::Casa),
            "sparrow" => Ok(Self::Sparrow),
            "hot" => Ok(Self::Hot),
            other => Err(format!("unknown suite {other} (casa|sparrow|hot)")),
        }
    }
}

pub struct CasaOpts {
    pub warmup: u32,
    pub passes: u32,
}

impl Default for CasaOpts {
    fn default() -> Self {
        Self {
            warmup: 1,
            passes: 9,
        }
    }
}

pub async fn electrum_casa(
    client: &mut ElectrumClient,
    targets: &[String],
    opts: &CasaOpts,
    progress: &mut Progress,
) -> Result<Vec<Sample>, String> {
    let keep = opts.passes.max(1);
    let total = opts.warmup.saturating_add(keep);
    let mut samples = Vec::new();
    for sh in targets {
        let params = json!([sh]);
        for pass in 0..total {
            let (bal, bns) = client
                .call("blockchain.scripthash.get_balance", params.clone())
                .await?;
            let (hist, hns) = client
                .call("blockchain.scripthash.get_history", params.clone())
                .await?;
            let (utxo, uns) = client
                .call("blockchain.scripthash.listunspent", params.clone())
                .await?;
            let history_n = jsonrpc::history_len(&hist);
            let utxo_n = jsonrpc::utxo_len(&utxo);
            let _ = bal;
            if pass >= opts.warmup {
                samples.push(Sample {
                    query: "get_balance",
                    nanos: bns,
                    history_n,
                    utxo_n,
                });
                samples.push(Sample {
                    query: "get_history",
                    nanos: hns,
                    history_n,
                    utxo_n,
                });
                samples.push(Sample {
                    query: "listunspent",
                    nanos: uns,
                    history_n,
                    utxo_n,
                });
            }
            progress.tick();
        }
    }
    progress.finish();
    Ok(samples)
}

pub async fn esplora_casa(
    client: &mut EsploraClient,
    targets: &[String],
    opts: &CasaOpts,
    progress: &mut Progress,
) -> Result<Vec<Sample>, String> {
    let keep = opts.passes.max(1);
    let total = opts.warmup.saturating_add(keep);
    let mut samples = Vec::new();
    for sh in targets {
        for pass in 0..total {
            let (info, ins) = client.get_json(&format!("/scripthash/{sh}")).await?;
            let (txs, tns) = client.get_json(&format!("/scripthash/{sh}/txs")).await?;
            let (utxo, uns) = client.get_json(&format!("/scripthash/{sh}/utxo")).await?;
            let history_n = info
                .pointer("/chain_stats/tx_count")
                .and_then(|v| v.as_u64())
                .unwrap_or_else(|| jsonrpc::history_len(&txs));
            let utxo_n = jsonrpc::utxo_len(&utxo);
            if pass >= opts.warmup {
                samples.push(Sample {
                    query: "get_balance",
                    nanos: ins,
                    history_n,
                    utxo_n,
                });
                samples.push(Sample {
                    query: "get_history",
                    nanos: tns,
                    history_n,
                    utxo_n,
                });
                samples.push(Sample {
                    query: "listunspent",
                    nanos: uns,
                    history_n,
                    utxo_n,
                });
            }
            progress.tick();
        }
    }
    progress.finish();
    Ok(samples)
}

pub async fn electrum_sparrow(
    client: &mut ElectrumClient,
    targets: &[String],
    batch: usize,
    fetch_txs: bool,
    progress: &mut Progress,
) -> Result<Vec<Sample>, String> {
    let batch = batch.max(1);
    let mut samples = Vec::new();
    let mut i = 0;
    while i < targets.len() {
        let end = (i + batch).min(targets.len());
        let chunk = &targets[i..end];
        let reqs: Vec<(&str, Value)> = chunk
            .iter()
            .map(|sh| ("blockchain.scripthash.subscribe", json!([sh])))
            .collect();
        let (_vals, ns) = client.call_batch(&reqs).await?;
        samples.push(Sample {
            query: "subscribe_batch",
            nanos: ns,
            history_n: chunk.len() as u64,
            utxo_n: 0,
        });
        progress.tick();
        i = end;
    }
    progress.set_label("sparrow refresh");
    i = 0;
    let mut txids: Vec<String> = Vec::new();
    while i < targets.len() {
        let end = (i + batch).min(targets.len());
        let chunk = &targets[i..end];
        let reqs: Vec<(&str, Value)> = chunk
            .iter()
            .map(|sh| ("blockchain.scripthash.get_history", json!([sh])))
            .collect();
        let (vals, ns) = client.call_batch(&reqs).await?;
        samples.push(Sample {
            query: "get_history_batch",
            nanos: ns,
            history_n: chunk.len() as u64,
            utxo_n: 0,
        });
        progress.tick();
        if fetch_txs {
            for v in &vals {
                if let Some(arr) = v.as_array() {
                    for item in arr {
                        if let Some(h) = item.get("tx_hash").and_then(|x| x.as_str()) {
                            txids.push(h.to_string());
                        }
                    }
                }
            }
        }
        i = end;
    }
    if fetch_txs {
        txids.sort();
        txids.dedup();
        let n_tx_batches = txids.len().div_ceil(batch) as u64;
        progress.add_work(n_tx_batches);
        progress.set_label("sparrow txs");
        i = 0;
        while i < txids.len() {
            let end = (i + batch).min(txids.len());
            let reqs: Vec<(&str, Value)> = txids[i..end]
                .iter()
                .map(|h| ("blockchain.transaction.get", json!([h])))
                .collect();
            let (_vals, ns) = client.call_batch(&reqs).await?;
            samples.push(Sample {
                query: "transaction_get_batch",
                nanos: ns,
                history_n: (end - i) as u64,
                utxo_n: 0,
            });
            progress.tick();
            i = end;
        }
    }
    progress.finish();
    Ok(samples)
}

pub async fn electrum_hot(
    client: &mut ElectrumClient,
    targets: &[String],
    timeout: Duration,
    progress: &mut Progress,
) -> Result<Vec<Sample>, String> {
    let _ = timeout;
    let mut samples = Vec::new();
    for sh in targets {
        let params = json!([sh]);
        let (hist, hns) = client
            .call("blockchain.scripthash.get_history", params.clone())
            .await?;
        let history_n = jsonrpc::history_len(&hist);
        samples.push(Sample {
            query: "get_history",
            nanos: hns,
            history_n,
            utxo_n: 0,
        });
        let (utxo, uns) = client
            .call("blockchain.scripthash.listunspent", params)
            .await?;
        samples.push(Sample {
            query: "listunspent",
            nanos: uns,
            history_n,
            utxo_n: jsonrpc::utxo_len(&utxo),
        });
        progress.tick();
    }
    progress.finish();
    Ok(samples)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suite_parse() {
        assert_eq!(Suite::parse("casa").unwrap(), Suite::Casa);
        assert!(Suite::parse("lopp").is_err());
    }

    #[tokio::test]
    async fn electrum_casa_drops_warmup() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            let (r, mut w) = s.into_split();
            let mut br = BufReader::new(r);
            loop {
                let mut line = String::new();
                if br.read_line(&mut line).await.unwrap() == 0 {
                    break;
                }
                let v: Value = serde_json::from_str(line.trim()).unwrap();
                let id = v["id"].clone();
                let method = v["method"].as_str().unwrap_or("");
                let result = match method {
                    "server.version" => json!(["ok", "1.4"]),
                    "blockchain.scripthash.get_balance" => {
                        json!({"confirmed": 1, "unconfirmed": 0})
                    }
                    "blockchain.scripthash.get_history" => json!([{"height":1,"tx_hash":"aa"}]),
                    "blockchain.scripthash.listunspent" => {
                        json!([{"tx_hash":"aa","tx_pos":0,"height":1,"value":1}])
                    }
                    _ => json!(null),
                };
                let resp = json!({"id": id, "result": result});
                w.write_all(resp.to_string().as_bytes()).await.unwrap();
                w.write_all(b"\n").await.unwrap();
            }
        });
        let mut c = ElectrumClient::connect(&addr, Duration::from_secs(2))
            .await
            .unwrap();
        let sh = "ab".repeat(32);
        let mut progress = Progress::start("casa test", 3);
        let samples = electrum_casa(
            &mut c,
            &[sh],
            &CasaOpts {
                warmup: 1,
                passes: 2,
            },
            &mut progress,
        )
        .await
        .unwrap();
        assert_eq!(samples.len(), 6);
        assert_eq!(samples[0].query, "get_balance");
        assert_eq!(samples[0].history_n, 1);
    }

    #[tokio::test]
    async fn electrum_sparrow_batches() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            let (r, mut w) = s.into_split();
            let mut br = BufReader::new(r);
            loop {
                let mut line = String::new();
                if br.read_line(&mut line).await.unwrap() == 0 {
                    break;
                }
                let v: Value = serde_json::from_str(line.trim()).unwrap();
                let resp = json!({"id": v["id"], "result": []});
                w.write_all(resp.to_string().as_bytes()).await.unwrap();
                w.write_all(b"\n").await.unwrap();
            }
        });
        let mut c = ElectrumClient::connect(&addr, Duration::from_secs(2))
            .await
            .unwrap();
        let a = "ab".repeat(32);
        let b = "cd".repeat(32);
        let mut progress = Progress::start("sparrow test", 2);
        let samples = electrum_sparrow(&mut c, &[a, b], 2, false, &mut progress)
            .await
            .unwrap();
        assert!(samples.iter().any(|s| s.query == "subscribe_batch"));
        assert!(samples.iter().any(|s| s.query == "get_history_batch"));
    }
}
