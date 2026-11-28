//! One height-wave and one commit for filter and tweak rows.
//!
//! The confirm batch and the post-IBD window both produce [`IndexRows`].
//! A commit that finds the watermark or `confirmed[h]` moved returns
//! [`IndexCommit::moved`]. The live path treats that as corrupt. The
//! materialize treats it as a re-plan.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::Query;
use bitcoin::bip158::BlockFilter;
use rbitcoin_primitives::{Fk, Height};

use crate::ConfirmError;
use rbitcoin_consensus::script_pool::start_for_each_owned_chunk;
use rbitcoin_consensus::ConsensusError;

/// Per-tx tweak (`None` = ineligible), block order.
pub type HeightTweaks = Vec<Option<[u8; 33]>>;

/// Filter bytes and tweak vecs for a run of heights. Heights travel with the rows.
#[derive(Default)]
pub struct IndexRows {
    pub filters: Vec<(Height, BlockFilter, Fk)>,
    pub tweaks: Vec<(Height, Fk, HeightTweaks)>,
}

pub struct IndexHeightOut {
    pub filter: Option<(Height, BlockFilter, Fk)>,
    pub tweaks: Option<(Height, Fk, HeightTweaks)>,
}

/// Result of [`commit_index_rows`].
pub struct IndexCommit {
    pub put_ns: u64,
    /// Watermark or `confirmed[h]` moved, or the rows leave a hole at `next`.
    pub moved: bool,
}

/// Claim one height per job. `f` writes its [`IndexHeightOut`] into the job.
pub fn finish_wave<T: Sync>(
    jobs: Vec<T>,
    f: fn(&T) -> Result<(), ConsensusError>,
) -> Result<(), ConfirmError> {
    if let Some(wave) = start_for_each_owned_chunk(jobs, f, 1)? {
        wave.finish()?;
    }
    Ok(())
}

pub fn rows_from_slots(slots: &[Arc<Mutex<Option<IndexHeightOut>>>]) -> IndexRows {
    let mut rows = IndexRows::default();
    for slot in slots {
        let Some(done) = slot.lock().unwrap_or_else(|e| e.into_inner()).take() else {
            continue;
        };
        if let Some(filter) = done.filter {
            rows.filters.push(filter);
        }
        if let Some(tweaks) = done.tweaks {
            rows.tweaks.push(tweaks);
        }
    }
    rows
}

/// Commit each index from its watermark. Empty rows are a no-op.
///
/// When both indexes have a contiguous run, one append group syncs them:
/// every payload, then both body lengths, then both index lengths. A hole
/// in the filter rows still skips the tweak commit.
pub fn commit_index_rows(query: &Query, rows: IndexRows) -> Result<IndexCommit, ConfirmError> {
    let t0 = Instant::now();
    let filters = classify_filters(query, rows.filters)?;
    if matches!(filters, Ready::Hole) {
        return Ok(IndexCommit {
            put_ns: t0.elapsed().as_nanos() as u64,
            moved: true,
        });
    }
    let tweaks = classify_tweaks(query, rows.tweaks)?;
    if let (Ready::Go(filters), Ready::Go(tweaks)) = (&filters, &tweaks) {
        let moved = seal_both(query, filters, tweaks)?;
        return Ok(IndexCommit {
            put_ns: t0.elapsed().as_nanos() as u64,
            moved,
        });
    }
    let mut moved = false;
    if let Ready::Go(filters) = filters {
        if query.commit_window_filters(filters.start, &filters.items)? == 0 {
            return Ok(IndexCommit {
                put_ns: t0.elapsed().as_nanos() as u64,
                moved: true,
            });
        }
    }
    match tweaks {
        Ready::Hole => moved = true,
        Ready::Go(tweaks) => moved = query.commit_window_tweaks(&tweaks)? == 0,
        Ready::Skip => {}
    }
    Ok(IndexCommit {
        put_ns: t0.elapsed().as_nanos() as u64,
        moved,
    })
}

enum Ready<T> {
    Skip,
    Hole,
    Go(T),
}

struct FilterRun {
    start: u32,
    items: Vec<(BlockFilter, Fk)>,
}

type TweakRows = Vec<(Height, Fk, HeightTweaks)>;

fn classify_filters(
    query: &Query,
    filters: Vec<(Height, BlockFilter, Fk)>,
) -> Result<Ready<FilterRun>, ConfirmError> {
    let Some(next) = query.filter_index_next() else {
        return Ok(Ready::Skip);
    };
    let Some(start) = filters.iter().position(|(h, _, _)| h.0 >= next) else {
        return Ok(Ready::Skip);
    };
    if filters[start..]
        .iter()
        .enumerate()
        .any(|(i, (h, _, _))| h.0 != next.saturating_add(i as u32))
    {
        return Ok(Ready::Hole);
    }
    let items = filters
        .into_iter()
        .skip(start)
        .map(|(_, filter, fk)| (filter, fk))
        .collect();
    Ok(Ready::Go(FilterRun { start: next, items }))
}

fn classify_tweaks(query: &Query, tweaks: TweakRows) -> Result<Ready<TweakRows>, ConfirmError> {
    let Some(next) = query.tweak_index_next() else {
        return Ok(Ready::Skip);
    };
    let Some(start) = tweaks.iter().position(|(h, _, _)| h.0 >= next) else {
        return Ok(Ready::Skip);
    };
    let slice = tweaks.into_iter().skip(start).collect::<Vec<_>>();
    if slice
        .iter()
        .enumerate()
        .any(|(i, (h, _, _))| h.0 != next.saturating_add(i as u32))
    {
        return Ok(Ready::Hole);
    }
    Ok(Ready::Go(slice))
}

/// Both runs looked contiguous. Recheck under the appender lock, then one
/// append group. Tweaks that moved commit the filters alone.
fn seal_both(
    query: &Query,
    filters: &FilterRun,
    tweaks: &[(Height, Fk, HeightTweaks)],
) -> Result<bool, ConfirmError> {
    let _appender = query.bf_wb.lock_appender();
    let Some(table) = query.block_filter_table() else {
        return Ok(false);
    };
    let guard = query
        .sp_tweaks
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    let Some(tweak_table) = guard.as_ref() else {
        drop(guard);
        query.write_basic_filters(table, filters.start, &filters.items, |recs| table.put(recs))?;
        return Ok(true);
    };
    let mut tweaks_fit = tweaks
        .first()
        .is_some_and(|item| item.0 == tweak_table.next_height());
    if tweaks_fit {
        for (height, header_fk, _) in tweaks {
            if query.store.confirmed.get(*height)? != Some(*header_fk) {
                tweaks_fit = false;
                break;
            }
        }
    }
    if !tweaks_fit {
        drop(guard);
        query.write_basic_filters(table, filters.start, &filters.items, |recs| table.put(recs))?;
        return Ok(true);
    }
    let refs: Vec<(Height, &[Option<[u8; 33]>])> = tweaks
        .iter()
        .map(|(height, _, records)| (*height, records.as_slice()))
        .collect();
    let staged = tweak_table.stage_blocks(&refs)?;
    let n = query.write_basic_filters(table, filters.start, &filters.items, |recs| {
        rbitcoin_store::block_filter::commit_filters_with_tweaks(table, recs, &staged)
    })?;
    if n == 0 {
        return Ok(true);
    }
    staged.committed();
    Ok(false)
}
