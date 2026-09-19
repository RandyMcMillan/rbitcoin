//! Schema v5 spend annotations on create outputs (replaces v4 point multimap).
//!
//! - Sole spender: `output.spender_field = spending_tx_fk` (MULTI clear).
//! - Multi: MULTI set, `spender_field` = head of [`crate::spender_table::SpenderTable`].
//!
//! Best-chain views filter with `is_confirmed_strong` (leave annotations on reorg).

use crate::error::StoreError;
use crate::spender_table::SpenderTable;
use crate::tx_table::TxTable;
use rbitcoin_primitives::Fk;

/// Query-facing spend edge (outpoint filled by caller args).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PointRecord {
    pub out_txid: [u8; 32],
    pub out_index: u32,
    pub spending_tx_fk: Fk,
    pub spending_vin: u32,
    pub next: Fk,
}

/// Mark create outpoint spent by `spending_tx_fk` at `spending_vin` (promote to multi-list if needed).
pub fn put_spend_on_create(
    txs: &TxTable,
    spenders: &SpenderTable,
    create_tx_fk: Fk,
    vout: u32,
    spending_tx_fk: Fk,
    spending_vin: u32,
) -> Result<(), StoreError> {
    put_spend_on_create_at(txs, spenders, create_tx_fk, vout, spending_tx_fk, spending_vin, None)
}

/// Like [`put_spend_on_create`] with optional cache-held body `(offset, len)` — **no idx**.
pub fn put_spend_on_create_at(
    txs: &TxTable,
    spenders: &SpenderTable,
    create_tx_fk: Fk,
    vout: u32,
    spending_tx_fk: Fk,
    spending_vin: u32,
    body_range: Option<(u64, u64)>,
) -> Result<(), StoreError> {
    if create_tx_fk.is_null() || spending_tx_fk.is_null() {
        return Err(StoreError::InvalidFk);
    }
    let (multi, field, field_vin) = match body_range {
        Some((off, len)) => txs.get_output_spender_meta_at(off, len, vout)?,
        None => txs.get_output_spender_meta(create_tx_fk, vout)?,
    };

    let set = |multi: bool, field: Fk, vin: u32| -> Result<(), StoreError> {
        match body_range {
            Some((off, len)) => txs.set_output_spender_meta_at(off, len, vout, multi, field, vin),
            None => txs.set_output_spender_meta(create_tx_fk, vout, multi, field, vin),
        }
    };

    if !multi && field.is_null() {
        return set(false, spending_tx_fk, spending_vin);
    }
    if !multi && field == spending_tx_fk && field_vin == spending_vin {
        return Ok(());
    }
    if !multi {
        // IBD first-spend path is sole-only; multi is rare (reorg / double annotate).
        let e1 = spenders.append(field, field_vin, Fk::NULL)?;
        let e2 = spenders.append(spending_tx_fk, spending_vin, e1)?;
        return set(true, e2, 0);
    }
    let cap = spenders.count();
    let mut cur = Some(field);
    let mut steps = 0u64;
    while let Some(fk) = cur {
        steps = steps.saturating_add(1);
        if steps > cap {
            return Err(StoreError::Corrupt("invariant: spender multi-list cycle"));
        }
        let (spend_tx, spend_vin, next) = spenders.get(fk)?;
        if spend_tx == spending_tx_fk && spend_vin == spending_vin {
            return Ok(());
        }
        cur = if next.is_null() { None } else { Some(next) };
    }
    let e = spenders.append(spending_tx_fk, spending_vin, field)?;
    set(true, e, 0)
}

/// Visit `(spending_tx_fk, vin)` for a create outpoint (no Class C filter).
pub fn for_each_spender_create<F>(
    txs: &TxTable,
    spenders: &SpenderTable,
    create_tx_fk: Fk,
    vout: u32,
    mut visit: F,
) -> Result<(), StoreError>
where
    F: FnMut(Fk, u32) -> Result<bool, StoreError>,
{
    if create_tx_fk.is_null() {
        return Ok(());
    }
    let (multi, field, vin) = match txs.get_output_spender_meta(create_tx_fk, vout) {
        Ok(m) => m,
        Err(StoreError::NotFound) => return Ok(()),
        Err(e) => return Err(e),
    };
    if field.is_null() {
        return Ok(());
    }
    if !multi {
        let _ = visit(field, vin)?;
        return Ok(());
    }
    let cap = spenders.count();
    let mut cur = Some(field);
    let mut steps = 0u64;
    while let Some(fk) = cur {
        steps = steps.saturating_add(1);
        if steps > cap {
            return Err(StoreError::Corrupt("invariant: spender multi-list cycle"));
        }
        let (spend_tx, spend_vin, next) = spenders.get(fk)?;
        if !visit(spend_tx, spend_vin)? {
            return Ok(());
        }
        cur = if next.is_null() { None } else { Some(next) };
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::address_head::HeadLayout;
    use crate::spender_table::SpenderTable;
    use crate::tx_table::{InputRecord, OutputRecord, TxRecord, TxTable};

    fn tmp() -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "rbitcoin-point-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn put_create(txs: &TxTable, txid: [u8; 32], n_out: u32) -> Fk {
        let outs: Vec<_> =
            (0..n_out).map(|i| OutputRecord::unspent(i as i64 + 1, vec![0x51])).collect();
        let item = (
            TxRecord {
                txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: n_out,
            },
            vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])],
            outs,
        );
        txs.put_full_batch_indexed(&[item], true).unwrap()[0]
    }

    #[test]
    fn spend_annotate_sole_multi_and_visit() {
        let dir = tmp();
        let layout = HeadLayout::new(crate::address_head::TINY_BITS).unwrap();
        let txs = TxTable::create_with_head_layout(&dir, layout).unwrap();
        let spenders = SpenderTable::create(&dir).unwrap();
        let create = put_create(&txs, [1u8; 32], 2);
        let s1 = put_create(&txs, [2u8; 32], 1);
        let s2 = put_create(&txs, [3u8; 32], 1);
        let s3 = put_create(&txs, [4u8; 32], 1);

        assert!(matches!(
            put_spend_on_create(&txs, &spenders, Fk::NULL, 0, s1, 0),
            Err(StoreError::InvalidFk)
        ));
        assert!(matches!(
            put_spend_on_create(&txs, &spenders, create, 0, Fk::NULL, 0),
            Err(StoreError::InvalidFk)
        ));

        // Sole first spend.
        put_spend_on_create(&txs, &spenders, create, 0, s1, 0).unwrap();
        // Idempotent.
        put_spend_on_create(&txs, &spenders, create, 0, s1, 0).unwrap();
        // Promote to multi.
        put_spend_on_create(&txs, &spenders, create, 0, s2, 1).unwrap();
        // Prepend multi.
        put_spend_on_create(&txs, &spenders, create, 0, s3, 2).unwrap();

        let mut visited = Vec::new();
        for_each_spender_create(&txs, &spenders, create, 0, |fk, vin| {
            visited.push((fk, vin));
            Ok(true)
        })
        .unwrap();
        assert_eq!(visited.len(), 3);
        assert_eq!(visited[0], (s3, 2)); // newest head
        assert_eq!(visited[1], (s2, 1));
        assert_eq!(visited[2], (s1, 0));
        // Early stop.
        let mut n = 0;
        for_each_spender_create(&txs, &spenders, create, 0, |_, _| {
            n += 1;
            Ok(false)
        })
        .unwrap();
        assert_eq!(n, 1);
        // Null create / missing / unspent.
        for_each_spender_create(&txs, &spenders, Fk::NULL, 0, |_, _| unreachable!()).unwrap();
        for_each_spender_create(&txs, &spenders, Fk(9999), 0, |_, _| unreachable!()).unwrap();
        for_each_spender_create(&txs, &spenders, create, 1, |_, _| unreachable!()).unwrap();

        // spent.body range path
        let (off, len) = txs.spent_range(create).unwrap();
        put_spend_on_create_at(&txs, &spenders, create, 1, s1, 7, Some((off, len))).unwrap();
        let mut one = None;
        for_each_spender_create(&txs, &spenders, create, 1, |fk, vin| {
            one = Some((fk, vin));
            Ok(true)
        })
        .unwrap();
        assert_eq!(one, Some((s1, 7)));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn for_each_spender_create_cycle_is_corrupt() {
        let dir = tmp();
        let layout = HeadLayout::new(crate::address_head::TINY_BITS).unwrap();
        let txs = TxTable::create_with_head_layout(&dir, layout).unwrap();
        let spenders = SpenderTable::create(&dir).unwrap();
        let create = put_create(&txs, [1u8; 32], 1);
        let s1 = put_create(&txs, [2u8; 32], 1);
        let s2 = put_create(&txs, [3u8; 32], 1);
        put_spend_on_create(&txs, &spenders, create, 0, s1, 0).unwrap();
        put_spend_on_create(&txs, &spenders, create, 0, s2, 0).unwrap();
        let (_multi, head, _vin) = txs.get_output_spender_meta(create, 0).unwrap();
        let (_sfk, _vin, older) = spenders.get(head).unwrap();
        let older_id = older.get().unwrap();
        let off = crate::file::FILE_HEADER_LEN as u64
            + (older_id - 1) * crate::spender_table::SPENDER_RECORD_LEN as u64;
        let ovf = crate::file::TableFile::open(
            dir.join(crate::spender_table::SPENT_OVF_NAME),
            rbitcoin_primitives::TableKind::Spender,
        )
        .unwrap();
        let mut rec = [0u8; crate::spender_table::SPENDER_RECORD_LEN];
        ovf.read_at(off, &mut rec).unwrap();
        rec[8..16].copy_from_slice(&head.0.to_le_bytes());
        ovf.write_at(off, &rec).unwrap();
        match for_each_spender_create(&txs, &spenders, create, 0, |_, _| Ok(true)) {
            Err(StoreError::Corrupt(m)) => {
                assert!(m.contains("cycle"), "{m}");
            }
            other => panic!("expected cycle Corrupt, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
