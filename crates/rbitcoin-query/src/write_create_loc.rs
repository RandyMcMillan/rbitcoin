//! Write-thread loc pairs from Class A append (just-written parents).
//!
//! Lookup TipOnly reads `create.loc` after [`crate::Query::note_lookup_tiponly_start`].
//! At note, `keep_until` is the last height whose TipOnly had started
//! (`lookup_started_hi`, at least the noting pack). That value is never bumped.
//! Prune drops a pack when that height has finished write (`written_hi ≥
//! keep_until`) and the pack is not the one just written (`pack_height <
//! written_hi`). Fill of that write runs first. Later-wave spends take TipOnly
//! loc on the in-flight identity; RAM loc is only for same-wave holes.
//!
//! This window is **write-thread TLS** (one writer per thread — same ownership
//! as load's [`crate::InFlight`], not a `Query` mutex). Disconnect is polled
//! via [`crate::Query::take_disconnect`] on the write thread.

use rbitcoin_primitives::Fk;
use rbitcoin_store::CreateLocPair;
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

use crate::Query;

const PAIR_BYTES: u64 = std::mem::size_of::<CreateLocPair>() as u64;

/// Last TipOnly'd height at note, floored at `pack_height`.
pub(crate) fn keep_until_at_note(pack_height: u32, started_hi: Option<u32>) -> u32 {
    started_hi.unwrap_or(pack_height).max(pack_height)
}

#[derive(Debug)]
struct LocPack {
    keep_until: u32,
    base: u64,
    pairs: Vec<CreateLocPair>,
    approx_bytes: u64,
}

impl LocPack {
    fn get(&self, id: u64) -> Option<CreateLocPair> {
        let off = id.checked_sub(self.base)?;
        self.pairs.get(off as usize).copied()
    }
}

/// Write-owned loc window. Note after append; prune after that write batch
/// (and later overlapping batches) finish.
#[derive(Debug, Default)]
pub(crate) struct WriteCreateLocRam {
    by_height: BTreeMap<u32, LocPack>,
    approx_bytes: u64,
}

impl WriteCreateLocRam {
    pub(crate) fn note(
        &mut self,
        pack_height: u32,
        keep_until: u32,
        fks: &[Fk],
        loc: &[CreateLocPair],
    ) {
        if fks.is_empty() || loc.len() != fks.len() {
            return;
        }
        let Some(start) = fks[0].get() else {
            return;
        };
        let bytes = (loc.len() as u64).saturating_mul(PAIR_BYTES);
        if let Some(old) = self.by_height.insert(
            pack_height,
            LocPack { keep_until, base: start, pairs: loc.to_vec(), approx_bytes: bytes },
        ) {
            self.approx_bytes = self.approx_bytes.saturating_sub(old.approx_bytes);
        }
        self.approx_bytes = self.approx_bytes.saturating_add(bytes);
    }

    pub(crate) fn get(&self, fk: Fk) -> Option<CreateLocPair> {
        let id = fk.get()?;
        for pack in self.by_height.values() {
            if let Some(p) = pack.get(id) {
                return Some(p);
            }
        }
        None
    }

    /// Drop packs whose last-started-at-note height has finished write.
    ///
    /// `keep_until` is fixed at note. Equality drops once `written_hi` covers
    /// that height, except the noting pack (`pack_height < written_hi`).
    pub(crate) fn prune_written_through(&mut self, written_hi: u32) {
        let drop: Vec<u32> = self
            .by_height
            .iter()
            .filter(|(h, p)| **h < written_hi && p.keep_until <= written_hi)
            .map(|(h, _)| *h)
            .collect();
        for h in drop {
            if let Some(p) = self.by_height.remove(&h) {
                self.approx_bytes = self.approx_bytes.saturating_sub(p.approx_bytes);
            }
        }
    }

    /// Disconnect: drop packs at/above `height`. Remaining keep-until cannot
    /// wait for disconnected lookup batches.
    pub(crate) fn drop_from_height(&mut self, height: u32) {
        let drop = self.by_height.split_off(&height);
        for p in drop.into_values() {
            self.approx_bytes = self.approx_bytes.saturating_sub(p.approx_bytes);
        }
        let cap = height.saturating_sub(1);
        for p in self.by_height.values_mut() {
            if p.keep_until > cap {
                p.keep_until = cap;
            }
        }
    }

    /// Packs, pair count, approx bytes (IBD `wloc=`).
    pub(crate) fn size_snapshot(&self) -> (usize, usize, u64) {
        let pairs: usize = self.by_height.values().map(|p| p.pairs.len()).sum();
        (self.by_height.len(), pairs, self.approx_bytes)
    }
}

thread_local! {
    static RAM: RefCell<WriteCreateLocRam> = RefCell::new(WriteCreateLocRam::default());
    static DISCONNECT_SEEN: Cell<u64> = const { Cell::new(0) };
}

fn publish(ram: &WriteCreateLocRam) {
    let (packs, pairs, bytes) = ram.size_snapshot();
    crate::process_mem_stats::note_wloc(packs, pairs, bytes);
}

/// Drop this thread's loc window (Query open / test isolation). Does not publish
/// zeros — `wloc=` atomics are process-wide like `iflight=`.
pub(crate) fn clear() {
    RAM.with(|c| {
        *c.borrow_mut() = WriteCreateLocRam::default();
    });
    DISCONNECT_SEEN.with(|c| c.set(0));
}

pub(crate) fn with_ram<R>(query: &Query, f: impl FnOnce(&mut WriteCreateLocRam) -> R) -> R {
    RAM.with(|c| {
        let mut ram = c.borrow_mut();
        poll_disconnect(query, &mut ram);
        let r = f(&mut ram);
        publish(&ram);
        r
    })
}

fn poll_disconnect(query: &Query, ram: &mut WriteCreateLocRam) {
    let mut seen = DISCONNECT_SEEN.with(|c| c.get());
    if let Some(h) = query.take_disconnect(&mut seen) {
        ram.drop_from_height(h);
        DISCONNECT_SEEN.with(|c| c.set(seen));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_primitives::Fk;

    fn pair(n: u64) -> CreateLocPair {
        CreateLocPair { txout: (n * 10, 8), spent: (n * 9, 8), n_out: 1 }
    }

    fn fks(ids: &[u64]) -> Vec<Fk> {
        ids.iter().copied().map(Fk).collect()
    }

    fn loc(ids: &[u64]) -> Vec<CreateLocPair> {
        ids.iter().copied().map(pair).collect()
    }

    fn prune(m: &mut WriteCreateLocRam, written_hi: u32) {
        m.prune_written_through(written_hi);
    }

    #[test]
    fn note_get_by_fk() {
        let mut m = WriteCreateLocRam::default();
        m.note(1, 4, &fks(&[10, 11]), &loc(&[10, 11]));
        assert_eq!(m.get(Fk(10)).unwrap().txout, (100, 8));
        assert_eq!(m.get(Fk(11)).unwrap().spent, (99, 8));
        assert!(m.get(Fk(12)).is_none());
        assert!(m.get(Fk(9)).is_none());
    }

    #[test]
    fn prune_written_through_drops_at_keep_until() {
        let mut m = WriteCreateLocRam::default();
        m.note(1, 4, &fks(&[10]), &loc(&[10]));
        prune(&mut m, 3);
        assert!(m.get(Fk(10)).is_some(), "written_hi < keep_until keeps");
        prune(&mut m, 4);
        assert!(
            m.get(Fk(10)).is_none(),
            "written_hi == keep_until drops (last overlapping batch finished write)"
        );
        assert_eq!(m.size_snapshot(), (0, 0, 0));
    }

    #[test]
    fn prune_written_through_is_per_pack_keep_until() {
        let mut m = WriteCreateLocRam::default();
        m.note(1, 4, &fks(&[10]), &loc(&[10]));
        m.note(2, 8, &fks(&[11]), &loc(&[11]));
        prune(&mut m, 4);
        assert!(m.get(Fk(10)).is_none());
        assert!(m.get(Fk(11)).is_some(), "later pack waits for keep_until 8");
        prune(&mut m, 8);
        assert!(m.get(Fk(11)).is_none());
    }

    #[test]
    fn keep_until_at_note_is_last_started() {
        assert_eq!(keep_until_at_note(360, Some(432)), 432);
        assert_eq!(keep_until_at_note(360, Some(1080)), 1080);
        assert_eq!(keep_until_at_note(360, None), 360);
        assert_eq!(keep_until_at_note(5, Some(3)), 5);
    }

    #[test]
    fn prune_keeps_noting_pack_until_later_write() {
        let mut m = WriteCreateLocRam::default();
        m.note(3, 3, &fks(&[10]), &loc(&[10]));
        prune(&mut m, 3);
        assert!(m.get(Fk(10)).is_some(), "same-write prune must not drop the noting pack");
        prune(&mut m, 4);
        assert!(m.get(Fk(10)).is_none());
        assert_eq!(m.size_snapshot(), (0, 0, 0));
    }

    #[test]
    fn prune_drops_when_last_started_height_has_written() {
        let mut m = WriteCreateLocRam::default();
        m.note(360, keep_until_at_note(360, Some(432)), &fks(&[10]), &loc(&[10]));
        prune(&mut m, 431);
        assert!(m.get(Fk(10)).is_some(), "intervening writes below last started keep");
        prune(&mut m, 432);
        assert!(
            m.get(Fk(10)).is_none(),
            "drop after last started write (fill of that write already ran)"
        );
    }

    #[test]
    fn prune_does_not_bump_keep_until() {
        let mut m = WriteCreateLocRam::default();
        m.note(360, keep_until_at_note(360, Some(432)), &fks(&[10]), &loc(&[10]));
        prune(&mut m, 433);
        assert!(
            m.get(Fk(10)).is_none(),
            "keep_until is fixed at note; later lookup_started_hi is not applied"
        );
    }

    #[test]
    fn no_count_cap_while_keep_until_open() {
        let mut m = WriteCreateLocRam::default();
        for i in 0..8u64 {
            m.note((i + 1) as u32, 100, &[Fk(1000 + i)], &[pair(1000 + i)]);
        }
        let (packs, pairs, bytes) = m.size_snapshot();
        assert_eq!(packs, 8);
        assert_eq!(pairs, 8);
        assert_eq!(bytes, 8 * PAIR_BYTES);
        assert!(m.get(Fk(1000)).is_some());
        assert!(m.get(Fk(1007)).is_some());
        prune(&mut m, 99);
        assert_eq!(m.size_snapshot().0, 8, "no silent FIFO drop");
        prune(&mut m, 100);
        assert_eq!(m.size_snapshot(), (0, 0, 0));
    }

    #[test]
    fn drop_from_height_drops_suffix_and_clamps() {
        let mut m = WriteCreateLocRam::default();
        m.note(5, 20, &fks(&[10]), &loc(&[10]));
        m.note(8, 20, &fks(&[11]), &loc(&[11]));
        m.drop_from_height(8);
        assert!(m.get(Fk(10)).is_some());
        assert!(m.get(Fk(11)).is_none());
        prune(&mut m, 7);
        assert!(
            m.get(Fk(10)).is_none(),
            "keep_until clamped to disconnect-1 so remaining pipeline can retire the pack"
        );
    }

    #[test]
    fn drop_from_height_zero_clears() {
        let mut m = WriteCreateLocRam::default();
        m.note(1, 4, &fks(&[10]), &loc(&[10]));
        m.drop_from_height(0);
        assert!(m.get(Fk(10)).is_none());
        assert_eq!(m.size_snapshot(), (0, 0, 0));
    }

    #[test]
    fn empty_or_len_mismatch_is_noop() {
        let mut m = WriteCreateLocRam::default();
        m.note(1, 4, &[], &[]);
        m.note(1, 4, &fks(&[10]), &loc(&[10, 11]));
        assert_eq!(m.size_snapshot(), (0, 0, 0));
        m.note(1, 4, &fks(&[0]), &loc(&[0]));
        assert!(m.get(Fk(0)).is_none(), "null fk does not note");
    }

    #[test]
    fn same_pack_height_replace_does_not_double_count_bytes() {
        let mut m = WriteCreateLocRam::default();
        m.note(1, 4, &fks(&[10, 11]), &loc(&[10, 11]));
        let (_, _, once) = m.size_snapshot();
        m.note(1, 8, &fks(&[10]), &loc(&[10]));
        let (packs, pairs, twice) = m.size_snapshot();
        assert_eq!(packs, 1);
        assert_eq!(pairs, 1);
        assert_eq!(twice, PAIR_BYTES);
        assert!(twice < once);
        assert!(m.get(Fk(11)).is_none());
        assert!(m.get(Fk(10)).is_some());
    }
}
