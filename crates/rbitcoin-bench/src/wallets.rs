//! Split a corpus into many small wallets (no key reuse across clients).

pub const MIXED_WALLET_KEYS: &[usize] = &[8, 16, 32];

pub fn keys_needed(n_wallets: usize, wallet_keys: Option<usize>) -> usize {
    match wallet_keys {
        Some(k) => n_wallets.saturating_mul(k),
        None => MIXED_WALLET_KEYS
            .iter()
            .copied()
            .cycle()
            .take(n_wallets)
            .sum(),
    }
}

pub fn pack_wallets(
    keys: &[String],
    n_wallets: usize,
    wallet_keys: Option<usize>,
) -> Result<Vec<Vec<String>>, String> {
    if n_wallets == 0 {
        return Err("--clients must be >= 1".into());
    }
    if matches!(wallet_keys, Some(0)) {
        return Err("--wallet-keys must be >= 1".into());
    }
    let need = keys_needed(n_wallets, wallet_keys);
    if keys.len() < need {
        return Err(format!(
            "need {need} keys for {n_wallets} clients (have {})",
            keys.len()
        ));
    }
    let mut out = Vec::with_capacity(n_wallets);
    let mut off = 0usize;
    for i in 0..n_wallets {
        let n = wallet_keys.unwrap_or(MIXED_WALLET_KEYS[i % MIXED_WALLET_KEYS.len()]);
        out.push(keys[off..off + n].to_vec());
        off += n;
    }
    Ok(out)
}

/// Keep keys that fit a small-wallet cap. A key that itself exceeds `max_txs`
/// or `max_utxos` is dropped; remaining keys fill the wallet until a cap.
pub fn keep_small(items: &[(String, u64, u64)], max_txs: u64, max_utxos: u64) -> Vec<String> {
    let mut out = Vec::new();
    let mut txs = 0u64;
    let mut utxos = 0u64;
    for (sh, t, u) in items {
        if *t > max_txs || *u > max_utxos {
            continue;
        }
        if txs.saturating_add(*t) > max_txs || utxos.saturating_add(*u) > max_utxos {
            continue;
        }
        txs = txs.saturating_add(*t);
        utxos = utxos.saturating_add(*u);
        out.push(sh.clone());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("{i:064x}")).collect()
    }

    #[test]
    fn mixed_sizes_are_disjoint_slices() {
        let k = keys(56);
        let w = pack_wallets(&k, 3, None).unwrap();
        assert_eq!(w.len(), 3);
        assert_eq!(w[0].len(), 8);
        assert_eq!(w[1].len(), 16);
        assert_eq!(w[2].len(), 32);
        assert_eq!(w[0][0], k[0]);
        assert_eq!(w[1][0], k[8]);
        assert_eq!(w[2][0], k[24]);
        let mut seen = std::collections::HashSet::new();
        for wallet in &w {
            for sh in wallet {
                assert!(seen.insert(sh), "overlap {sh}");
            }
        }
    }

    #[test]
    fn fixed_size_and_too_few_keys() {
        let k = keys(40);
        let w = pack_wallets(&k, 4, Some(10)).unwrap();
        assert_eq!(w.len(), 4);
        assert!(w.iter().all(|x| x.len() == 10));
        assert_eq!(keys_needed(4, Some(10)), 40);
        assert!(pack_wallets(&k, 5, Some(10))
            .unwrap_err()
            .contains("need 50 keys"));
        assert!(pack_wallets(&k, 0, None)
            .unwrap_err()
            .contains("--clients must be >= 1"));
        assert!(pack_wallets(&k, 1, Some(0))
            .unwrap_err()
            .contains("--wallet-keys must be >= 1"));
    }

    #[test]
    fn keep_small_drops_fat_and_fills_cap() {
        let items = vec![
            ("aa".repeat(32), 40, 40),
            ("bb".repeat(32), 2000, 1),
            ("cc".repeat(32), 40, 40),
            ("dd".repeat(32), 40, 40),
            ("ee".repeat(32), 1, 1),
        ];
        let kept = keep_small(&items, 1000, 100);
        assert_eq!(
            kept,
            vec!["aa".repeat(32), "cc".repeat(32), "ee".repeat(32)]
        );
        assert!(keep_small(&[("ff".repeat(32), 5, 200)], 1000, 100).is_empty());
    }

    #[test]
    fn sparrow_corpus_covers_default_eight_clients() {
        let got = crate::targets::load_corpus("sparrow").unwrap();
        let need = keys_needed(8, None);
        assert!(got.len() >= need, "sparrow {} need {need}", got.len());
        let w = pack_wallets(&got, 8, None).unwrap();
        assert_eq!(w.len(), 8);
        assert_eq!(w.iter().map(|x| x.len()).sum::<usize>(), need);
    }
}
