//! Per-key CSV for `--out` (casa/hot sequential runs).

use std::path::Path;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KeyRow {
    pub scripthash: String,
    pub oldest_tx: Option<u32>,
    pub newest_tx: Option<u32>,
    pub oldest_utxo: Option<u32>,
    pub newest_utxo: Option<u32>,
    pub txs: u64,
    pub utxos: u64,
    pub get_balance_us: Vec<u64>,
    pub get_history_us: Vec<u64>,
    pub listunspent_us: Vec<u64>,
}

fn opt_u32(v: Option<u32>) -> String {
    v.map(|n| n.to_string()).unwrap_or_default()
}

fn pad_times(times: &[u64], passes: usize) -> Vec<String> {
    let mut out = Vec::with_capacity(passes);
    for i in 0..passes {
        out.push(times.get(i).map(|n| n.to_string()).unwrap_or_default());
    }
    out
}

pub fn csv_header(passes: usize) -> String {
    let passes = passes.max(1);
    let mut cols = vec![
        "scripthash".to_string(),
        "oldest_tx".to_string(),
        "newest_tx".to_string(),
        "oldest_utxo".to_string(),
        "newest_utxo".to_string(),
        "txs".to_string(),
        "utxos".to_string(),
    ];
    for name in ["get_balance_us", "get_history_us", "listunspent_us"] {
        for i in 1..=passes {
            cols.push(format!("{name}_{i}"));
        }
    }
    cols.join(",")
}

pub fn csv_row(row: &KeyRow, passes: usize) -> String {
    let passes = passes.max(1);
    let mut cols = vec![
        row.scripthash.clone(),
        opt_u32(row.oldest_tx),
        opt_u32(row.newest_tx),
        opt_u32(row.oldest_utxo),
        opt_u32(row.newest_utxo),
        row.txs.to_string(),
        row.utxos.to_string(),
    ];
    cols.extend(pad_times(&row.get_balance_us, passes));
    cols.extend(pad_times(&row.get_history_us, passes));
    cols.extend(pad_times(&row.listunspent_us, passes));
    cols.join(",")
}

pub fn format_csv(rows: &[KeyRow], passes: usize) -> String {
    let mut out = csv_header(passes);
    out.push('\n');
    for row in rows {
        out.push_str(&csv_row(row, passes));
        out.push('\n');
    }
    out
}

pub fn write_csv(path: &Path, rows: &[KeyRow], passes: usize) -> Result<(), String> {
    write_body(path, &format_csv(rows, passes))
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClientRow {
    pub client: u32,
    pub n_keys: usize,
    pub txs: u64,
    pub utxos: u64,
    pub wallet_load_us: Vec<u64>,
}

pub fn clients_csv_header(passes: usize) -> String {
    let passes = passes.max(1);
    let mut cols = vec![
        "client".to_string(),
        "n_keys".to_string(),
        "txs".to_string(),
        "utxos".to_string(),
    ];
    for i in 1..=passes {
        cols.push(format!("wallet_load_us_{i}"));
    }
    cols.join(",")
}

pub fn clients_csv_row(row: &ClientRow, passes: usize) -> String {
    let mut cols = vec![
        row.client.to_string(),
        row.n_keys.to_string(),
        row.txs.to_string(),
        row.utxos.to_string(),
    ];
    cols.extend(pad_times(&row.wallet_load_us, passes));
    cols.join(",")
}

pub fn format_clients_csv(rows: &[ClientRow], passes: usize) -> String {
    let mut out = clients_csv_header(passes);
    out.push('\n');
    for row in rows {
        out.push_str(&clients_csv_row(row, passes));
        out.push('\n');
    }
    out
}

pub fn write_clients_csv(path: &Path, rows: &[ClientRow], passes: usize) -> Result<(), String> {
    write_body(path, &format_clients_csv(rows, passes))
}

fn write_body(path: &Path, body: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
    }
    std::fs::write(path, body).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_has_nine_warm_columns_by_default() {
        let h = csv_header(9);
        assert!(h.starts_with(
            "scripthash,oldest_tx,newest_tx,oldest_utxo,newest_utxo,txs,utxos,get_balance_us_1"
        ));
        assert!(h.contains("get_balance_us_9"));
        assert!(h.contains("get_history_us_9"));
        assert!(h.contains("listunspent_us_9"));
        assert!(!h.contains("get_balance_us_10"));
        assert_eq!(h.split(',').count(), 7 + 9 * 3);
    }

    #[test]
    fn row_encodes_heights_counts_and_times() {
        let row = KeyRow {
            scripthash: "ab".repeat(32),
            oldest_tx: Some(100),
            newest_tx: Some(800_000),
            oldest_utxo: Some(700_000),
            newest_utxo: Some(800_000),
            txs: 42,
            utxos: 3,
            get_balance_us: vec![10, 11],
            get_history_us: vec![20, 21],
            listunspent_us: vec![30, 31],
        };
        let line = csv_row(&row, 2);
        assert!(line.starts_with(&format!(
            "{},100,800000,700000,800000,42,3,10,11,20,21,30,31",
            "ab".repeat(32)
        )));
        let wide = csv_row(&row, 9);
        assert!(wide.ends_with(",30,31,,,,,,,"));
        let csv = format_csv(&[row], 2);
        assert_eq!(csv.lines().count(), 2);
    }

    #[test]
    fn empty_heights_are_blank() {
        let row = KeyRow {
            scripthash: "cd".repeat(32),
            ..KeyRow::default()
        };
        let line = csv_row(&row, 1);
        assert!(line.contains(",,,,,0,0,"));
    }

    #[test]
    fn write_csv_roundtrip() {
        let dir = std::env::temp_dir().join(format!("rbtc-bench-out-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("k.csv");
        let row = KeyRow {
            scripthash: "ab".repeat(32),
            oldest_tx: Some(1),
            newest_tx: Some(2),
            txs: 2,
            get_history_us: vec![9],
            ..KeyRow::default()
        };
        write_csv(&path, &[row], 1).unwrap();
        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.contains("oldest_tx"));
        assert!(body.contains(",1,2,,,2,0,"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn clients_csv_has_load_times() {
        let row = ClientRow {
            client: 3,
            n_keys: 8,
            txs: 12,
            utxos: 4,
            wallet_load_us: vec![100, 90],
        };
        let h = clients_csv_header(2);
        assert_eq!(
            h,
            "client,n_keys,txs,utxos,wallet_load_us_1,wallet_load_us_2"
        );
        assert_eq!(clients_csv_row(&row, 2), "3,8,12,4,100,90");
        let csv = format_clients_csv(&[row], 2);
        assert_eq!(csv.lines().count(), 2);
    }
}
