//! Schema-14 SH overflow: stack of mono OA segments sized to **one main shard**.
//!
//! After main seals, new keys land on the open overflow segment
//! (`scripthash.ovf/NNNNNN`). When load ≥ ~0.80 or a new key cannot place
//! (no rehash), the open segment is sealed with a real BF8R fuse and a new
//! same-size empty segment is opened. Lookups: open → sealed (fuse-gated).

use crate::error::StoreError;
use crate::fuse8_filter::{fuse_key_from_mixed, SealedFuse8};
use crate::scripthash_head::{ScriptHashHead, ShardedScriptHashHead};
use crate::scripthash_layout::ShHeadValue;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// Directory under the store root for overflow segment files.
pub const OVERFLOW_DIR: &str = "scripthash.ovf";
/// Legacy interim full-size overflow path (wiped on open).
pub const LEGACY_OVERFLOW_HEAD: &str = "scripthash.ovf.head";
/// Legacy placeholder fuse next to single-file ovf (removed with legacy wipe).
pub const LEGACY_OVERFLOW_FUSE: &str = "scripthash.ovf.head.fuse8";

/// Slot count for one overflow segment (= one main shard).
#[inline]
pub fn ovf_segment_slots(main: &ShardedScriptHashHead) -> u64 {
    let per = main.slots_per_shard();
    debug_assert!(
        main.shard_count() <= 1 || main.total_slots() / main.shard_count() as u64 == per,
        "ovf geometry: total/n_shards must equal per-shard slots"
    );
    per
}

#[inline]
pub fn ovf_dir(store_dir: &Path) -> PathBuf {
    store_dir.join(OVERFLOW_DIR)
}

#[inline]
pub fn ovf_seg_path(store_dir: &Path, id: u32) -> PathBuf {
    ovf_dir(store_dir).join(format!("{id:06}"))
}

#[inline]
pub fn ovf_fuse_path(store_dir: &Path, id: u32) -> PathBuf {
    ovf_dir(store_dir).join(format!("{id:06}.fuse8"))
}

fn file_is_shsr(path: &Path) -> bool {
    let Ok(mut f) = std::fs::File::open(path) else {
        return false;
    };
    let mut magic = [0u8; 4];
    use std::io::Read;
    matches!(f.read_exact(&mut magic), Ok(())) && magic == *b"SHSR"
}

/// Fuse key from full Electrum scripthash (16 B head prefix + zero pad).
#[inline]
pub fn sh_ovf_fuse_key(full: &[u8; 32]) -> u64 {
    let mut pad = [0u8; 32];
    pad[..16].copy_from_slice(&full[..16]);
    fuse_key_from_mixed(&pad)
}

/// One mono OA segment (open if `fuse` is None).
pub struct OvfSegment {
    pub id: u32,
    pub head: ScriptHashHead,
    /// Set after seal (BF8R on disk). Open segment has None.
    pub fuse: Option<SealedFuse8>,
}

impl OvfSegment {
    pub fn is_open(&self) -> bool {
        self.fuse.is_none()
    }

    pub fn slots(&self) -> u64 {
        self.head.slots()
    }
}

/// Stack of mono overflow segments under `scripthash.ovf/`.
pub struct ShOverflowStack {
    store_dir: PathBuf,
    segs: Vec<OvfSegment>,
}

impl ShOverflowStack {
    pub fn empty(store_dir: &Path) -> Self {
        Self {
            store_dir: store_dir.to_path_buf(),
            segs: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.segs.is_empty()
    }

    pub fn segs(&self) -> &[OvfSegment] {
        &self.segs
    }

    /// Number of mono overflow segments (0 if none).
    #[cfg(test)]
    pub fn segment_count(&self) -> usize {
        self.segs.len()
    }

    /// Slot count of the open (last) segment, if any.
    #[cfg(test)]
    pub fn open_segment_slots(&self) -> Option<u64> {
        self.segs.last().filter(|s| s.is_open()).map(|s| s.slots())
    }

    /// Open existing `scripthash.ovf/` segments (if any). Wipes legacy full-size ovf.
    pub fn open(store_dir: &Path) -> Result<Self, StoreError> {
        wipe_legacy_fullsize_overflow(store_dir)?;
        let dir = ovf_dir(store_dir);
        if !dir.is_dir() {
            return Ok(Self::empty(store_dir));
        }
        let mut ids: Vec<u32> = std::fs::read_dir(&dir)
            .map_err(|e| StoreError::io(&dir, e))?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                // Segment files: six decimal digits only (not `.fuse8` / `.occ`).
                if name.len() == 6 && name.chars().all(|c| c.is_ascii_digit()) {
                    name.parse::<u32>().ok()
                } else {
                    None
                }
            })
            .collect();
        ids.sort_unstable();
        ids.dedup();
        // Sorted sealed ovf (SHSR + .idx + .fuse8) is ScriptHashTable::sealed_ovf.
        ids.retain(|id| !file_is_shsr(&ovf_seg_path(store_dir, *id)));
        if ids.is_empty() {
            return Ok(Self::empty(store_dir));
        }
        let mut segs = Vec::with_capacity(ids.len());
        for (i, id) in ids.iter().enumerate() {
            if i > 0 && *id != ids[i - 1] + 1 {
                return Err(StoreError::Corrupt(
                    "scripthash.ovf: non-contiguous segment ids",
                ));
            }
            let path = ovf_seg_path(store_dir, *id);
            let head = ScriptHashHead::open(path)?;
            let fuse_path = ovf_fuse_path(store_dir, *id);
            let is_last = i + 1 == ids.len();
            let fuse = if fuse_path.is_file() {
                match crate::fuse8_filter::open_file(&fuse_path) {
                    Ok(crate::fuse8_filter::FuseFileOpen::Ready(f)) => Some(f),
                    Ok(crate::fuse8_filter::FuseFileOpen::NeedsRewrite { reason, .. }) => {
                        // Legacy v1 fuse: drop and treat as open only on last segment;
                        // sealed non-last must still fail hard (cannot rewrite SH fuse here).
                        if is_last {
                            rbitcoin_log::warn!(
                                "store: scripthash.ovf fuse migrate id={id} ({reason}) — \
                                 treating last segment as open (fuse removed)"
                            );
                            let _ = std::fs::remove_file(&fuse_path);
                            None
                        } else {
                            return Err(StoreError::Corrupt(
                                "scripthash.ovf: sealed segment fuse needs rewrite (re-seal ovf)",
                            ));
                        }
                    }
                    Err(_) if is_last => {
                        // Corrupt fuse on open (last) segment: treat as open.
                        let _ = std::fs::remove_file(&fuse_path);
                        None
                    }
                    Err(e) => return Err(e),
                }
            } else {
                None
            };
            // Only the last segment may be open (no fuse).
            if !is_last && fuse.is_none() {
                return Err(StoreError::Corrupt(
                    "scripthash.ovf: sealed segment missing fuse",
                ));
            }
            segs.push(OvfSegment {
                id: *id,
                head,
                fuse,
            });
        }
        Ok(Self {
            store_dir: store_dir.to_path_buf(),
            segs,
        })
    }

    /// Ensure an open mono segment of `slots` exists (creates `000000` first).
    pub fn ensure_open(&mut self, slots: u64) -> Result<(), StoreError> {
        if self.segs.last().map(|s| s.is_open()).unwrap_or(false) {
            return Ok(());
        }
        let id = self.segs.last().map(|s| s.id + 1).unwrap_or(0);
        let dir = ovf_dir(&self.store_dir);
        std::fs::create_dir_all(&dir).map_err(|e| StoreError::io(&dir, e))?;
        let path = ovf_seg_path(&self.store_dir, id);
        if path.exists() {
            return Err(StoreError::Corrupt(
                "scripthash.ovf: open segment path already exists",
            ));
        }
        let head = ScriptHashHead::create_with_slots(path, slots)?;
        self.segs.push(OvfSegment {
            id,
            head,
            fuse: None,
        });
        Ok(())
    }

    /// Probe open then sealed newest→oldest (fuse skip when present).
    #[cfg(test)]
    pub fn get(&self, key: &[u8; 32]) -> Result<Option<ShHeadValue>, StoreError> {
        for seg in self.segs.iter().rev() {
            if let Some(ref f) = seg.fuse {
                if !f.contains(sh_ovf_fuse_key(key)) {
                    continue;
                }
            }
            if let Some(v) = seg.head.get(key)? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    /// Like [`get`] but also returns the home segment id.
    pub fn get_with_home(&self, key: &[u8; 32]) -> Result<Option<(u32, ShHeadValue)>, StoreError> {
        for seg in self.segs.iter().rev() {
            if let Some(ref f) = seg.fuse {
                if !f.contains(sh_ovf_fuse_key(key)) {
                    continue;
                }
            }
            if let Some(v) = seg.head.get(key)? {
                return Ok(Some((seg.id, v)));
            }
        }
        Ok(None)
    }

    /// Visit every live head value on all segments (oldest → newest).
    pub fn for_each_occupied(
        &self,
        mut f: impl FnMut([u8; 32], ShHeadValue) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        for seg in &self.segs {
            seg.head.for_each_occupied(&mut f)?;
        }
        Ok(())
    }

    /// Insert known-home updates on `seg_id` (update-only if sealed).
    pub fn insert_on_segment(
        &self,
        seg_id: u32,
        entries: &[([u8; 32], ShHeadValue)],
    ) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        let seg = self
            .segs
            .iter()
            .find(|s| s.id == seg_id)
            .ok_or(StoreError::Corrupt("scripthash.ovf: missing home segment"))?;
        let allow_new = seg.is_open();
        let rem = seg.head.insert_many_full_no_rehash(entries, allow_new)?;
        if !rem.is_empty() {
            // Sealed home must always accept updates; open NeedSlot is handled
            // by the caller via seal+roll for **new** keys only.
            return Err(StoreError::Corrupt(
                "scripthash.ovf: home segment refused update (invariant)",
            ));
        }
        Ok(())
    }

    /// Place new keys on the open segment (no rehash). Remainder needs seal+roll.
    pub fn insert_new_on_open(
        &mut self,
        entries: &[([u8; 32], ShHeadValue)],
    ) -> Result<Vec<([u8; 32], ShHeadValue)>, StoreError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let open = self
            .segs
            .last()
            .filter(|s| s.is_open())
            .ok_or(StoreError::Corrupt("scripthash.ovf: no open segment"))?;
        open.head.insert_many_full_no_rehash(entries, true)
    }

    /// Seal open segment if load ≥ seal threshold (real BF8R + next empty segment).
    pub fn maybe_seal_at_load(&mut self, seal_load: f64) -> Result<(), StoreError> {
        let Some(open) = self.segs.last() else {
            return Ok(());
        };
        if !open.is_open() {
            return Ok(());
        }
        let Some(r) = open.head.load_ratio() else {
            return Ok(());
        };
        if r + f64::EPSILON < seal_load {
            return Ok(());
        }
        self.seal_open_and_roll()
    }

    /// Seal open with real fuse, open next same-size segment.
    pub fn seal_open_and_roll(&mut self) -> Result<(), StoreError> {
        let (id, slots) = {
            let open = self
                .segs
                .last()
                .filter(|s| s.is_open())
                .ok_or(StoreError::Corrupt("scripthash.ovf: seal without open"))?;
            (open.id, open.slots())
        };

        let mut set: HashSet<u64> = HashSet::new();
        {
            let open = self.segs.last().unwrap();
            open.head.for_each_occupied(|full, _val| {
                set.insert(sh_ovf_fuse_key(&full));
                Ok(())
            })?;
        }
        let mut keys: Vec<u64> = set.into_iter().collect();
        keys.sort_unstable();
        let fuse = SealedFuse8::build(&keys)?;
        let fuse_path = ovf_fuse_path(&self.store_dir, id);
        // Publish fuse first, then mark sealed in memory and open next.
        fuse.write_to(&fuse_path)?;
        {
            let open = self.segs.last_mut().unwrap();
            open.fuse = Some(fuse);
        }

        let next_id = id + 1;
        let dir = ovf_dir(&self.store_dir);
        std::fs::create_dir_all(&dir).map_err(|e| StoreError::io(&dir, e))?;
        let path = ovf_seg_path(&self.store_dir, next_id);
        let head = ScriptHashHead::create_with_slots(path, slots)?;
        self.segs.push(OvfSegment {
            id: next_id,
            head,
            fuse: None,
        });
        Ok(())
    }

    /// Insert new keys with no_rehash; seal+roll on NeedSlot or load gate until done.
    pub fn insert_new_with_roll(
        &mut self,
        entries: &[([u8; 32], ShHeadValue)],
        seal_load: f64,
    ) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut pending = entries.to_vec();
        // Bound rolls: each seal frees a full empty segment (≥1 key).
        for _ in 0..64 {
            if pending.is_empty() {
                self.maybe_seal_at_load(seal_load)?;
                return Ok(());
            }
            let rem = self.insert_new_on_open(&pending)?;
            if rem.is_empty() {
                self.maybe_seal_at_load(seal_load)?;
                return Ok(());
            }

            self.seal_open_and_roll()?;
            pending = rem;
        }
        Err(StoreError::Corrupt(
            "scripthash.ovf: seal+roll could not place keys",
        ))
    }

    pub fn clear_key(&self, key: &[u8; 32]) -> Result<bool, StoreError> {
        for seg in self.segs.iter().rev() {
            if let Some(ref f) = seg.fuse {
                if !f.contains(sh_ovf_fuse_key(key)) {
                    continue;
                }
            }
            if seg.head.clear_key(key)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn insert(&self, key: &[u8; 32], val: &ShHeadValue) -> Result<(), StoreError> {
        if let Some((id, _)) = self.get_with_home(key)? {
            return self.insert_on_segment(id, &[(*key, val.clone())]);
        }
        // New key: only open segment (caller should use insert_new_with_roll).
        let open = self
            .segs
            .last()
            .filter(|s| s.is_open())
            .ok_or(StoreError::Corrupt("scripthash.ovf: insert without open"))?;
        let rem = open
            .head
            .insert_many_full_no_rehash(&[(*key, val.clone())], true)?;
        if !rem.is_empty() {
            return Err(StoreError::Corrupt(
                "scripthash.ovf: open full (use insert_new_with_roll)",
            ));
        }
        Ok(())
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        for s in &self.segs {
            s.head.flush()?;
        }
        Ok(())
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        for s in &self.segs {
            s.head.flush_async()?;
        }
        Ok(())
    }
}

/// Remove interim full-size `scripthash.ovf.head` (+ fuse) so segmented ovf can start clean.
pub fn wipe_legacy_fullsize_overflow(store_dir: &Path) -> Result<(), StoreError> {
    let legacy = store_dir.join(LEGACY_OVERFLOW_HEAD);
    let legacy_fuse = store_dir.join(LEGACY_OVERFLOW_FUSE);
    if legacy.exists() {
        rbitcoin_log::info!(
            "store: removing legacy full-size {} (schema-14 uses mono segment stack under {})",
            LEGACY_OVERFLOW_HEAD,
            OVERFLOW_DIR
        );
        if legacy.is_dir() {
            std::fs::remove_dir_all(&legacy).map_err(|e| StoreError::io(&legacy, e))?;
        } else {
            std::fs::remove_file(&legacy).map_err(|e| StoreError::io(&legacy, e))?;
        }
    }
    if legacy_fuse.exists() {
        let _ = std::fs::remove_file(&legacy_fuse);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hashhead::HeadRole;
    use crate::scripthash_layout::{ShEntry, ShHeadValue};
    use rbitcoin_primitives::Fk;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "rbitcoin-ovf-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn ovf_segment_slots_equals_one_main_shard() {
        let dir = tmp();
        // Tiny default SH: often 1 shard × 64 slots (or multi-shard under scale).
        let main =
            ShardedScriptHashHead::create_for_role(dir.join("main"), HeadRole::ScriptHash).unwrap();
        let ovf_slots = ovf_segment_slots(&main);
        assert_eq!(ovf_slots, main.slots_per_shard());
        if main.shard_count() > 1 {
            assert_eq!(ovf_slots, main.total_slots() / main.shard_count() as u64);
        }
        // Multi-shard pin: 4 × 64 → ovf 64 (not 256).
        let multi = ShardedScriptHashHead::create_sharded(dir.join("multi"), 4, 64).unwrap();
        assert_eq!(multi.shard_count(), 4);
        assert_eq!(multi.slots_per_shard(), 64);
        assert_eq!(ovf_segment_slots(&multi), 64);
        // Pure geometry: slots == one main shard (not total).
        assert_eq!(multi.total_slots() / multi.shard_count() as u64, 64);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ovf_path_helpers_format() {
        let root = Path::new("/data/store");
        assert_eq!(ovf_dir(root), PathBuf::from("/data/store/scripthash.ovf"));
        assert_eq!(
            ovf_seg_path(root, 0),
            PathBuf::from("/data/store/scripthash.ovf/000000")
        );
        assert_eq!(
            ovf_seg_path(root, 12),
            PathBuf::from("/data/store/scripthash.ovf/000012")
        );
        assert_eq!(
            ovf_fuse_path(root, 0),
            PathBuf::from("/data/store/scripthash.ovf/000000.fuse8")
        );
    }

    #[test]
    fn ensure_open_creates_mono_not_sharded() {
        let dir = tmp();
        let mut stack = ShOverflowStack::empty(&dir);
        stack.ensure_open(64).unwrap();
        assert_eq!(stack.segment_count(), 1);
        assert_eq!(stack.open_segment_slots(), Some(64));
        let seg_path = ovf_seg_path(&dir, 0);
        assert!(seg_path.is_file(), "mono file expected");
        assert!(!seg_path.is_dir());
        // No 64-way shard directory under ovf.
        let entries: Vec<_> = std::fs::read_dir(ovf_dir(&dir))
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(entries.iter().any(|n| n == "000000"));
        assert!(!entries.iter().any(|n| n.len() == 2)); // no "00".."3f" shards
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// One tmp stack: insert/clear/seal/roll/fuse-v1 migrate/reject/non-contiguous.
    ///
    /// Collapses prior per-path opens (each re-sealed 8-slot segments) into a single
    /// journey so default-suite wall time drops without losing contracts.
    #[test]
    fn overflow_seal_migrate_clear_and_open_errors_journey() {
        // --- Path A: empty + insert/clear/update + seal+roll ---
        let dir = tmp();
        {
            let mut stack = ShOverflowStack::empty(&dir);
            assert!(stack.is_empty());
            stack.flush().unwrap();
            stack.flush_async().unwrap();
            assert!(stack.insert_new_on_open(&[]).unwrap().is_empty());
            stack.insert_new_with_roll(&[], 0.8).unwrap();
            stack.ensure_open(8).unwrap();
            stack.maybe_seal_at_load(0.99).unwrap();

            let mut k1 = [0u8; 32];
            k1[0] = 0x71;
            let v1 = ShHeadValue::inline_one(ShEntry::new(Fk(7)));
            stack.insert(&k1, &v1).unwrap();
            assert!(stack.get(&k1).unwrap().is_some());
            stack
                .insert(&k1, &ShHeadValue::inline_one(ShEntry::new(Fk(8))))
                .unwrap();
            assert_eq!(stack.get(&k1).unwrap().unwrap().inline_fks(), vec![Fk(8)]);
            stack.insert_on_segment(0, &[]).unwrap();
            match stack.insert_on_segment(99, &[(k1, v1.clone())]) {
                Ok(_) => panic!("missing segment"),
                Err(e) => assert!(format!("{e}").contains("missing home"), "{e}"),
            }
            assert!(stack.clear_key(&k1).unwrap());
            assert!(stack.get(&k1).unwrap().is_none());

            // Seal+roll with real BF8R (tiny 8-slot open).
            let mut placed = Vec::new();
            for i in 0..8u32 {
                let mut key = [0u8; 32];
                key[0] = 0x10;
                key[1] = i as u8;
                stack
                    .insert_new_with_roll(
                        &[(
                            key,
                            ShHeadValue::inline_one(ShEntry::new(Fk(u64::from(i) + 1))),
                        )],
                        ShardedScriptHashHead::SH_SEAL_LOAD,
                    )
                    .unwrap();
                placed.push(key);
            }
            assert!(
                stack.segment_count() >= 2,
                "expected seal+roll, segs={}",
                stack.segment_count()
            );
            let fuse0 = ovf_fuse_path(&dir, 0);
            assert!(fuse0.is_file());
            let f = SealedFuse8::read_from(&fuse0).expect("BF8R");
            assert!(placed.iter().any(|k| f.contains(sh_ovf_fuse_key(k))));
            for k in &placed {
                assert!(stack.get(k).unwrap().is_some());
            }
            stack.flush().unwrap();
            // ensure_open no-op when already open.
            let n = stack.segment_count();
            stack.ensure_open(8).unwrap();
            assert_eq!(stack.segment_count(), n);

            // Sealed non-last + v1 fuse → hard fail on open.
            let mut raw = Vec::from(*b"BF8R");
            raw.extend_from_slice(&1u32.to_le_bytes());
            raw.extend_from_slice(&0u64.to_le_bytes());
            std::fs::write(&fuse0, &raw).unwrap();
        }
        match ShOverflowStack::open(&dir) {
            Ok(_) => panic!("expected corrupt sealed fuse"),
            Err(e) => {
                let msg = format!("{e}");
                assert!(
                    msg.contains("needs rewrite")
                        || msg.contains("re-seal")
                        || msg.contains("fuse"),
                    "{msg}"
                );
            }
        }
        let _ = std::fs::remove_dir_all(&dir);

        // --- Path B: v1 fuse on last segment soft-migrates ---
        let dir = tmp();
        {
            let mut stack = ShOverflowStack::empty(&dir);
            stack.ensure_open(8).unwrap();
            let mut key = [0u8; 32];
            key[0] = 0xab;
            stack
                .insert_new_with_roll(&[(key, ShHeadValue::inline_one(ShEntry::new(Fk(1))))], 0.99)
                .unwrap();
            let fuse = ovf_fuse_path(&dir, 0);
            let mut raw = Vec::from(*b"BF8R");
            raw.extend_from_slice(&1u32.to_le_bytes());
            raw.extend_from_slice(&0u64.to_le_bytes());
            std::fs::write(&fuse, &raw).unwrap();
        }
        {
            let stack = ShOverflowStack::open(&dir).unwrap();
            assert_eq!(stack.segment_count(), 1);
            assert!(stack.segs[0].is_open());
            assert!(!ovf_fuse_path(&dir, 0).exists());
            let mut key = [0u8; 32];
            key[0] = 0xab;
            assert!(stack.get(&key).unwrap().is_some());
        }
        let _ = std::fs::remove_dir_all(&dir);

        // --- Path B2: corrupt (non-BF8R) fuse on last segment → drop + open ---
        let dir = tmp();
        {
            let mut stack = ShOverflowStack::empty(&dir);
            stack.ensure_open(8).unwrap();
            let mut key = [0u8; 32];
            key[0] = 0xcd;
            stack
                .insert_new_with_roll(&[(key, ShHeadValue::inline_one(ShEntry::new(Fk(2))))], 0.99)
                .unwrap();
            std::fs::write(ovf_fuse_path(&dir, 0), b"XXXX garbage").unwrap();
        }
        {
            let stack = ShOverflowStack::open(&dir).unwrap();
            assert!(stack.segs[0].is_open());
            assert!(!ovf_fuse_path(&dir, 0).exists());
            let mut key = [0u8; 32];
            key[0] = 0xcd;
            assert!(stack.get(&key).unwrap().is_some());
        }
        let _ = std::fs::remove_dir_all(&dir);

        // --- Path B3: ensure_open when path already exists → corrupt ---
        let dir = tmp();
        {
            let mut stack = ShOverflowStack::empty(&dir);
            let path = ovf_seg_path(&dir, 0);
            std::fs::create_dir_all(ovf_dir(&dir)).unwrap();
            std::fs::write(&path, b"decoy").unwrap();
            match stack.ensure_open(8) {
                Ok(_) => panic!("expected path already exists"),
                Err(e) => assert!(format!("{e}").contains("already exists"), "{e}"),
            }
        }
        let _ = std::fs::remove_dir_all(&dir);

        // --- Path E: insert_new_with_roll remainder path (NeedSlot → seal → place) ---
        let dir = tmp();
        {
            let mut stack = ShOverflowStack::empty(&dir);
            stack.ensure_open(4).unwrap(); // very small
                                           // Batch larger than open capacity forces rem + seal_open_and_roll.
            let batch: Vec<_> = (0..8u32)
                .map(|i| {
                    let mut key = [0u8; 32];
                    key[0] = 0x55;
                    key[1] = i as u8;
                    (
                        key,
                        ShHeadValue::inline_one(ShEntry::new(Fk(u64::from(i) + 1))),
                    )
                })
                .collect();
            stack
                .insert_new_with_roll(&batch, ShardedScriptHashHead::SH_SEAL_LOAD)
                .unwrap();
            assert!(stack.segment_count() >= 2);
            for (k, _) in &batch {
                assert!(stack.get(k).unwrap().is_some());
            }
            // clear_key through sealed fuse gate (skip when fuse says no)
            let mut absent = [0u8; 32];
            absent[0] = 0xfe;
            assert!(!stack.clear_key(&absent).unwrap());
            // insert_on_segment home refuse path: try update on sealed with new key? skip
            stack.flush_async().unwrap();
        }
        let _ = std::fs::remove_dir_all(&dir);

        // --- Path C: non-contiguous segment ids ---
        let dir = tmp();
        {
            let mut stack = ShOverflowStack::empty(&dir);
            stack.ensure_open(8).unwrap();
            for i in 0..10u32 {
                let mut key = [0u8; 32];
                key[0] = 0x41;
                key[1] = i as u8;
                stack
                    .insert_new_with_roll(
                        &[(
                            key,
                            ShHeadValue::inline_one(ShEntry::new(Fk(u64::from(i) + 1))),
                        )],
                        ShardedScriptHashHead::SH_SEAL_LOAD,
                    )
                    .unwrap();
            }
            assert!(stack.segment_count() >= 2);
            let open_id = stack.segs.last().unwrap().id;
            let _ = std::fs::remove_file(ovf_seg_path(&dir, open_id));
            let _ = std::fs::remove_file(ovf_fuse_path(&dir, open_id));
            ScriptHashHead::create_with_slots(ovf_seg_path(&dir, open_id + 1), 8).unwrap();
        }
        match ShOverflowStack::open(&dir) {
            Ok(_) => panic!("expected non-contiguous"),
            Err(e) => assert!(format!("{e}").contains("non-contiguous"), "{e}"),
        }
        let _ = std::fs::remove_dir_all(&dir);

        // --- Path D: wipe legacy full-size ovf on open ---
        let dir = tmp();
        std::fs::write(dir.join(LEGACY_OVERFLOW_HEAD), b"decoy").unwrap();
        std::fs::write(dir.join(LEGACY_OVERFLOW_FUSE), b"SHFUSE01").unwrap();
        let stack = ShOverflowStack::open(&dir).unwrap();
        assert!(stack.is_empty());
        assert!(!dir.join(LEGACY_OVERFLOW_HEAD).exists());
        assert!(!dir.join(LEGACY_OVERFLOW_FUSE).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
