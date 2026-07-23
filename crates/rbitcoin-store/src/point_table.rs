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
    /// Always 0 in v5 (input index no longer stored).
    pub spending_input_index: u32,
    pub next: Fk,
}

/// Mark create outpoint spent by `spending_tx_fk` (promote to multi-list if needed).
pub fn put_spend_on_create(
    txs: &TxTable,
    spenders: &SpenderTable,
    create_tx_fk: Fk,
    vout: u32,
    spending_tx_fk: Fk,
) -> Result<(), StoreError> {
    put_spend_on_create_at(txs, spenders, create_tx_fk, vout, spending_tx_fk, None)
}

/// Like [`put_spend_on_create`] with optional runway-cached body `(offset, len)` — **no idx**.
pub fn put_spend_on_create_at(
    txs: &TxTable,
    spenders: &SpenderTable,
    create_tx_fk: Fk,
    vout: u32,
    spending_tx_fk: Fk,
    body_range: Option<(u64, u64)>,
) -> Result<(), StoreError> {
    if create_tx_fk.is_null() || spending_tx_fk.is_null() {
        return Err(StoreError::InvalidFk);
    }
    let (multi, field) = match body_range {
        Some((off, len)) => txs.get_output_spender_meta_at(off, len, vout)?,
        None => txs.get_output_spender_meta(create_tx_fk, vout)?,
    };

    let set = |multi: bool, field: Fk| -> Result<(), StoreError> {
        match body_range {
            Some((off, len)) => txs.set_output_spender_meta_at(off, len, vout, multi, field),
            None => txs.set_output_spender_meta(create_tx_fk, vout, multi, field),
        }
    };

    if !multi && field.is_null() {
        return set(false, spending_tx_fk);
    }
    if !multi && field == spending_tx_fk {
        return Ok(());
    }
    if !multi {
        // Promote sole → multi list (field was previous spending_tx_fk).
        // IBD first-spend path is sole-only; multi is rare (reorg / double annotate).
        let e1 = spenders.append(field, Fk::NULL)?;
        let e2 = spenders.append(spending_tx_fk, e1)?;
        return set(true, e2);
    }
    // Already multi: prepend.
    let e = spenders.append(spending_tx_fk, field)?;
    set(true, e)
}

/// Visit spending_tx_fks for a create outpoint (no Class C filter).
pub fn for_each_spender_create<F>(
    txs: &TxTable,
    spenders: &SpenderTable,
    create_tx_fk: Fk,
    vout: u32,
    mut visit: F,
) -> Result<(), StoreError>
where
    F: FnMut(Fk) -> Result<bool, StoreError>,
{
    if create_tx_fk.is_null() {
        return Ok(());
    }
    let (multi, field) = match txs.get_output_spender_meta(create_tx_fk, vout) {
        Ok(m) => m,
        Err(StoreError::NotFound) => return Ok(()),
        Err(e) => return Err(e),
    };
    if field.is_null() {
        return Ok(());
    }
    if !multi {
        let _ = visit(field)?;
        return Ok(());
    }
    let mut cur = Some(field);
    while let Some(fk) = cur {
        let (spend_tx, next) = spenders.get(fk)?;
        if !visit(spend_tx)? {
            return Ok(());
        }
        cur = if next.is_null() { None } else { Some(next) };
    }
    Ok(())
}
