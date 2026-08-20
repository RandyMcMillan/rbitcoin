//! Latency samples and Casa-style median / percentile summary.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sample {
    pub query: &'static str,
    pub nanos: u64,
    pub history_n: u64,
    pub utxo_n: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QuerySummary {
    pub query: &'static str,
    pub n: usize,
    pub p50_us: u64,
    pub p95_us: u64,
    pub max_us: u64,
}

pub fn median_us(nanos: &mut [u64]) -> u64 {
    percentile_us(nanos, 50.0)
}

pub fn percentile_us(nanos: &mut [u64], p: f64) -> u64 {
    if nanos.is_empty() {
        return 0;
    }
    nanos.sort_unstable();
    let n = nanos.len();
    let idx = ((p / 100.0) * (n.saturating_sub(1) as f64)).round() as usize;
    nanos[idx.min(n - 1)] / 1000
}

pub fn summarize(samples: &[Sample]) -> Vec<QuerySummary> {
    let mut names: Vec<&'static str> = samples.iter().map(|s| s.query).collect();
    names.sort_unstable();
    names.dedup();
    let mut out = Vec::with_capacity(names.len());
    for query in names {
        let mut ns: Vec<u64> = samples
            .iter()
            .filter(|s| s.query == query)
            .map(|s| s.nanos)
            .collect();
        let n = ns.len();
        let max_us = ns.iter().copied().max().unwrap_or(0) / 1000;
        let p50_us = median_us(&mut ns);
        let p95_us = percentile_us(&mut ns, 95.0);
        out.push(QuerySummary {
            query,
            n,
            p50_us,
            p95_us,
            max_us,
        });
    }
    out
}

/// History-size buckets used in the Casa charts (10–999 interesting range).
pub fn history_bucket(history_n: u64) -> &'static str {
    match history_n {
        0..=9 => "0-9",
        10..=49 => "10-49",
        50..=99 => "50-99",
        100..=249 => "100-249",
        250..=999 => "250-999",
        _ => "1000+",
    }
}

pub fn format_report(suite: &str, backend: &str, samples: &[Sample]) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "suite={suite} backend={backend} samples={}",
        samples.len()
    ));
    lines.push("query\tn\tp50_us\tp95_us\tmax_us".to_string());
    for q in summarize(samples) {
        lines.push(format!(
            "{}\t{}\t{}\t{}\t{}",
            q.query, q.n, q.p50_us, q.p95_us, q.max_us
        ));
    }
    lines.push("query\tbucket\tn\tp50_us".to_string());
    let mut keys: Vec<(&'static str, &'static str)> = samples
        .iter()
        .map(|s| (s.query, history_bucket(s.history_n)))
        .collect();
    keys.sort_unstable();
    keys.dedup();
    for (query, bucket) in keys {
        let mut ns: Vec<u64> = samples
            .iter()
            .filter(|s| s.query == query && history_bucket(s.history_n) == bucket)
            .map(|s| s.nanos)
            .collect();
        let n = ns.len();
        let p50_us = percentile_us(&mut ns, 50.0);
        lines.push(format!("{query}\t{bucket}\t{n}\t{p50_us}"));
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_odd_and_empty() {
        assert_eq!(median_us(&mut []), 0);
        assert_eq!(median_us(&mut [5_000, 1_000, 9_000]), 5);
        assert_eq!(percentile_us(&mut [1_000, 2_000, 3_000, 4_000], 95.0), 4);
    }

    #[test]
    fn summarize_groups_queries() {
        let samples = vec![
            Sample {
                query: "get_balance",
                nanos: 2_000,
                history_n: 12,
                utxo_n: 1,
            },
            Sample {
                query: "get_balance",
                nanos: 4_000,
                history_n: 12,
                utxo_n: 1,
            },
            Sample {
                query: "listunspent",
                nanos: 10_000,
                history_n: 12,
                utxo_n: 3,
            },
        ];
        let s = summarize(&samples);
        assert_eq!(s.len(), 2);
        assert_eq!(s[0].query, "get_balance");
        assert_eq!(s[0].n, 2);
        assert_eq!(s[0].p50_us, 4);
        let report = format_report("casa", "electrum", &samples);
        assert!(report.contains("get_balance\t10-49\t2\t"));
        assert!(report.contains("listunspent"));
    }

    #[test]
    fn history_bucket_edges() {
        assert_eq!(history_bucket(0), "0-9");
        assert_eq!(history_bucket(9), "0-9");
        assert_eq!(history_bucket(10), "10-49");
        assert_eq!(history_bucket(100), "100-249");
        assert_eq!(history_bucket(1000), "1000+");
    }
}
