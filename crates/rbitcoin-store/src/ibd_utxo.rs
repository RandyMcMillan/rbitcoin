//! IBD light UTXO: unspent outpoint → create Class A fk (mmap).
//!
//! Slot (24 B, 8-byte aligned):
//! ```text
//!   prefix[12] || pack(state:u8, vout:u24) || create_fk:u64 LE
//! ```
//! Membership + parent resolve without a global txid→fk process map.
//! Not a full coins cache (no value/script).
//!
//! Optional RAM pin (`mlock`): keep the map resident so materialize / Class A
//! page-cache pressure cannot major-fault UTXO probes. Enabled via node
//! `--mlock-utxo` (see [`IbdUtxo::open_or_create`]). Requires raised
//! `RLIMIT_MEMLOCK`. Multi‑GiB pin is tight on 8 GiB hosts.

use crate::error::StoreError;
use memmap2::MmapMut;
use rbitcoin_primitives::Fk;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

const MAGIC: &[u8; 8] = b"RBUXTO03";
const HEADER_LEN: usize = 4096;
/// 12 prefix + 4 packed + 8 fk = 24 (divisible by 8).
const SLOT_LEN: usize = 24;
const PREFIX_LEN: usize = 12;
const OFF_PREFIX: usize = 0;
const OFF_PACKED: usize = 12;
const OFF_FK: usize = 16;

const STATE_EMPTY: u8 = 0;
const STATE_LIVE: u8 = 1;
const STATE_TOMB: u8 = 2;

/// Product constraint: vout fits in 24 bits.
pub const VOUT_MAX: u32 = (1 << 24) - 1;

const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 8;
const OFF_TIP: usize = 12;
const OFF_NUM_SLOTS: usize = 16;
const OFF_LIVE: usize = 24;
const OFF_PREFIX_BITS: usize = 32;
const OFF_VOUT_BITS: usize = 34;
const OFF_FLAGS: usize = 36;
const OFF_SLOT_LEN: usize = 40;
const VERSION: u32 = 2;

/// Default initial slots (2^22 → 96 MiB of 24 B slots). Grows as needed.
/// Production: `RBITCOIN_IBD_UTXO_SLOTS=268435456` (~6 GiB @ 24 B).
pub const DEFAULT_NUM_SLOTS: u64 = 1 << 22;
const LOAD_GROW: f64 = 0.80;

/// Raise soft `RLIMIT_MEMLOCK` toward `want_bytes` (capped by hard). Best-effort.
#[cfg(unix)]
fn ensure_memlock_budget(want_bytes: u64) {
    let mut rlim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit with valid pointer.
    if unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut rlim) } != 0 {
        rbitcoin_log::warn!(
            "store: getrlimit(MEMLOCK) failed: {}",
            std::io::Error::last_os_error()
        );
        return;
    }
    let hard = rlim.rlim_max as u64;
    let soft = rlim.rlim_cur as u64;
    let hard_cap = if hard == u64::MAX || rlim.rlim_max == libc::RLIM_INFINITY {
        want_bytes.max(soft)
    } else {
        hard
    };
    let target = want_bytes.min(hard_cap).max(soft);
    if target > soft {
        rlim.rlim_cur = target as libc::rlim_t;
        // SAFETY: setrlimit soft ≤ hard.
        if unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &rlim) } != 0 {
            rbitcoin_log::warn!(
                "store: setrlimit(MEMLOCK) soft {soft}→{target} failed (hard={hard}): {}; \
                 UTXO mlock may fail — raise LimitMEMLOCK / ulimit -l",
                std::io::Error::last_os_error()
            );
            return;
        }
        rbitcoin_log::debug!(
            "store: raised RLIMIT_MEMLOCK soft {soft}→{target} (hard={hard})"
        );
    } else if soft < want_bytes
        && hard != u64::MAX
        && rlim.rlim_max != libc::RLIM_INFINITY
        && hard < want_bytes
    {
        rbitcoin_log::warn!(
            "store: RLIMIT_MEMLOCK hard={hard} < UTXO map {want_bytes} bytes; \
             mlock needs higher LimitMEMLOCK / ulimit -l"
        );
    }
}

/// Best-effort pin of the UTXO mmap when `enabled`. Never fails the open path.
///
/// Linux: prefer `mlock2(..., MLOCK_ONFAULT)` so empty slots lock lazily.
/// Fallback: [`MmapMut::lock`] (full `mlock`).
fn try_mlock_map(map: &MmapMut, path: &Path, enabled: bool) {
    if !enabled {
        return;
    }
    let len = map.len();
    if len == 0 {
        return;
    }
    #[cfg(unix)]
    ensure_memlock_budget(len as u64);

    #[cfg(target_os = "linux")]
    {
        // linux/mman.h MLOCK_ONFAULT = 1
        const MLOCK_ONFAULT: libc::c_uint = 1;
        let ptr = map.as_ptr() as *const libc::c_void;
        // SAFETY: map is live for `len` bytes.
        let rc = unsafe { libc::mlock2(ptr, len, MLOCK_ONFAULT) };
        if rc == 0 {
            static LOGGED: AtomicBool = AtomicBool::new(false);
            if !LOGGED.swap(true, Ordering::Relaxed) {
                rbitcoin_log::info!(
                    "store: ibd_utxo mlock2(ONFAULT) ok path={} size≈{:.2} GiB (--mlock-utxo)",
                    path.display(),
                    len as f64 / (1024.0 * 1024.0 * 1024.0)
                );
            } else {
                rbitcoin_log::debug!(
                    "store: ibd_utxo mlock2(ONFAULT) ok path={} size={}",
                    path.display(),
                    len
                );
            }
            return;
        }
        rbitcoin_log::debug!(
            "store: ibd_utxo mlock2(ONFAULT) failed ({}), trying mlock whole map",
            std::io::Error::last_os_error()
        );
    }

    match map.lock() {
        Ok(()) => {
            static LOGGED: AtomicBool = AtomicBool::new(false);
            if !LOGGED.swap(true, Ordering::Relaxed) {
                rbitcoin_log::info!(
                    "store: ibd_utxo mlock ok path={} size≈{:.2} GiB (--mlock-utxo)",
                    path.display(),
                    len as f64 / (1024.0 * 1024.0 * 1024.0)
                );
            } else {
                rbitcoin_log::debug!(
                    "store: ibd_utxo mlock ok path={} size={}",
                    path.display(),
                    len
                );
            }
        }
        Err(e) => {
            rbitcoin_log::warn!(
                "store: ibd_utxo mlock failed path={} size={} err={e} \
                 (need RLIMIT_MEMLOCK ≥ map; node continues unlocked)",
                path.display(),
                len
            );
        }
    }
}

#[inline]
fn pack(state: u8, vout: u32) -> u32 {
    debug_assert!(vout <= VOUT_MAX);
    ((state as u32) << 24) | (vout & 0x00FF_FFFF)
}

#[inline]
fn unpack(p: u32) -> (u8, u32) {
    ((p >> 24) as u8, p & 0x00FF_FFFF)
}

#[inline]
fn prefix_of(txid: &[u8; 32]) -> [u8; PREFIX_LEN] {
    let mut p = [0u8; PREFIX_LEN];
    p.copy_from_slice(&txid[0..PREFIX_LEN]);
    p
}

fn hash0(prefix: &[u8; PREFIX_LEN], vout: u32) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for &b in prefix {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x100000001b3);
    }
    h ^= u64::from(vout);
    h = h.wrapping_mul(0x100000001b3);
    h
}

fn io(path: &Path, e: std::io::Error) -> StoreError {
    StoreError::io(path, e)
}

/// One LIVE overflow entry (prefix collision): full txid + create fk.
type OverflowEntry = ([u8; 32], u64);

/// Persistent IBD UTXO: unspent outpoint → create_tx_fk.
pub struct IbdUtxo {
    path: PathBuf,
    /// Kept open so the mmap stays valid for the table lifetime.
    _file: File,
    map: MmapMut,
    num_slots: u64,
    live: u64,
    tip: Option<u32>,
    /// Rare (prefix,vout) collisions with full txid + fk.
    overflow: HashMap<([u8; PREFIX_LEN], u32), Vec<OverflowEntry>>,
    /// Re-applied after [`Self::grow`] (same process policy as open).
    mlock: bool,
}

impl IbdUtxo {
    fn slots_offset() -> usize {
        HEADER_LEN
    }

    fn file_len(num_slots: u64) -> u64 {
        Self::slots_offset() as u64 + num_slots * SLOT_LEN as u64
    }

    fn slot_ptr(map: &MmapMut, num_slots: u64, i: u64) -> (*const u8, *mut u8) {
        debug_assert!(i < num_slots);
        let off = Self::slots_offset() + (i as usize) * SLOT_LEN;
        let p = map.as_ptr().wrapping_add(off);
        (p, p as *mut u8)
    }

    fn read_slot(map: &MmapMut, num_slots: u64, i: u64) -> ([u8; PREFIX_LEN], u8, u32, u64) {
        let (p, _) = Self::slot_ptr(map, num_slots, i);
        unsafe {
            let mut prefix = [0u8; PREFIX_LEN];
            std::ptr::copy_nonoverlapping(p.add(OFF_PREFIX), prefix.as_mut_ptr(), PREFIX_LEN);
            let mut pb = [0u8; 4];
            std::ptr::copy_nonoverlapping(p.add(OFF_PACKED), pb.as_mut_ptr(), 4);
            let packed = u32::from_le_bytes(pb);
            let (state, vout) = unpack(packed);
            let mut fb = [0u8; 8];
            std::ptr::copy_nonoverlapping(p.add(OFF_FK), fb.as_mut_ptr(), 8);
            let fk = u64::from_le_bytes(fb);
            (prefix, state, vout, fk)
        }
    }

    fn write_slot(
        map: &mut MmapMut,
        num_slots: u64,
        i: u64,
        prefix: &[u8; PREFIX_LEN],
        state: u8,
        vout: u32,
        create_fk: u64,
    ) {
        let (_, p) = Self::slot_ptr(map, num_slots, i);
        let packed = pack(state, vout).to_le_bytes();
        let fb = create_fk.to_le_bytes();
        unsafe {
            std::ptr::copy_nonoverlapping(prefix.as_ptr(), p.add(OFF_PREFIX), PREFIX_LEN);
            std::ptr::copy_nonoverlapping(packed.as_ptr(), p.add(OFF_PACKED), 4);
            std::ptr::copy_nonoverlapping(fb.as_ptr(), p.add(OFF_FK), 8);
        }
    }

    fn write_header(map: &mut MmapMut, tip: Option<u32>, num_slots: u64, live: u64) {
        let buf = &mut map[..HEADER_LEN];
        buf[OFF_MAGIC..OFF_MAGIC + 8].copy_from_slice(MAGIC);
        buf[OFF_VERSION..OFF_VERSION + 4].copy_from_slice(&VERSION.to_le_bytes());
        let tip_raw = tip.map(|t| t).unwrap_or(u32::MAX);
        buf[OFF_TIP..OFF_TIP + 4].copy_from_slice(&tip_raw.to_le_bytes());
        buf[OFF_NUM_SLOTS..OFF_NUM_SLOTS + 8].copy_from_slice(&num_slots.to_le_bytes());
        buf[OFF_LIVE..OFF_LIVE + 8].copy_from_slice(&live.to_le_bytes());
        buf[OFF_PREFIX_BITS..OFF_PREFIX_BITS + 2].copy_from_slice(&96u16.to_le_bytes());
        buf[OFF_VOUT_BITS..OFF_VOUT_BITS + 2].copy_from_slice(&24u16.to_le_bytes());
        buf[OFF_FLAGS..OFF_FLAGS + 4].copy_from_slice(&0u32.to_le_bytes());
        buf[OFF_SLOT_LEN..OFF_SLOT_LEN + 4].copy_from_slice(&(SLOT_LEN as u32).to_le_bytes());
    }

    fn read_header(map: &MmapMut) -> Result<(Option<u32>, u64, u64), StoreError> {
        if map.len() < HEADER_LEN {
            return Err(StoreError::Corrupt("ibd utxo header short"));
        }
        if &map[OFF_MAGIC..OFF_MAGIC + 8] != MAGIC {
            return Err(StoreError::Corrupt(
                "ibd utxo bad magic (delete ibd_utxo.map to rebuild)",
            ));
        }
        let ver = u32::from_le_bytes(map[OFF_VERSION..OFF_VERSION + 4].try_into().unwrap());
        if ver != VERSION {
            return Err(StoreError::Corrupt(
                "ibd utxo unsupported version (delete ibd_utxo.map to rebuild)",
            ));
        }
        let tip_raw = u32::from_le_bytes(map[OFF_TIP..OFF_TIP + 4].try_into().unwrap());
        let tip = if tip_raw == u32::MAX {
            None
        } else {
            Some(tip_raw)
        };
        let num_slots =
            u64::from_le_bytes(map[OFF_NUM_SLOTS..OFF_NUM_SLOTS + 8].try_into().unwrap());
        let live = u64::from_le_bytes(map[OFF_LIVE..OFF_LIVE + 8].try_into().unwrap());
        if !num_slots.is_power_of_two() || num_slots < 16 {
            return Err(StoreError::Corrupt("ibd utxo bad num_slots"));
        }
        Ok((tip, num_slots, live))
    }

    pub fn create(path: impl Into<PathBuf>, num_slots: u64) -> Result<Self, StoreError> {
        Self::create_with_mlock(path, num_slots, false)
    }

    pub fn create_with_mlock(
        path: impl Into<PathBuf>,
        num_slots: u64,
        mlock: bool,
    ) -> Result<Self, StoreError> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| io(parent, e))?;
        }
        let num_slots = num_slots.max(16).next_power_of_two();
        let flen = Self::file_len(num_slots);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| io(&path, e))?;
        file.set_len(flen).map_err(|e| io(&path, e))?;
        let mut map = unsafe { MmapMut::map_mut(&file) }.map_err(|e| io(&path, e))?;
        map[HEADER_LEN..].fill(0);
        Self::write_header(&mut map, None, num_slots, 0);
        map.flush().map_err(|e| io(&path, e))?;
        try_mlock_map(&map, &path, mlock);
        Ok(Self {
            path,
            _file: file,
            map,
            num_slots,
            live: 0,
            tip: None,
            overflow: HashMap::new(),
            mlock,
        })
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StoreError> {
        Self::open_with_mlock(path, false)
    }

    pub fn open_with_mlock(path: impl Into<PathBuf>, mlock: bool) -> Result<Self, StoreError> {
        let path = path.into();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| io(&path, e))?;
        let map = unsafe { MmapMut::map_mut(&file) }.map_err(|e| io(&path, e))?;
        let (tip, num_slots, live) = Self::read_header(&map)?;
        let expect = Self::file_len(num_slots);
        if (map.len() as u64) < expect {
            return Err(StoreError::Corrupt("ibd utxo file truncated"));
        }
        try_mlock_map(&map, &path, mlock);
        Ok(Self {
            path,
            _file: file,
            map,
            num_slots,
            live,
            tip,
            overflow: HashMap::new(),
            mlock,
        })
    }

    pub fn open_or_create(dir: &Path) -> Result<Self, StoreError> {
        Self::open_or_create_with_mlock(dir, false)
    }

    /// Open/create under `dir/ibd_utxo.map`. When `mlock`, pin the map in RAM
    /// (see module docs). Grow reuses the same policy.
    pub fn open_or_create_with_mlock(dir: &Path, mlock: bool) -> Result<Self, StoreError> {
        let path = dir.join("ibd_utxo.map");
        if path.exists() {
            match Self::open_with_mlock(&path, mlock) {
                Ok(u) => Ok(u),
                Err(StoreError::Corrupt(_)) => {
                    // Schema bump / poison: recreate empty (caller rebuilds to tip).
                    let _ = std::fs::remove_file(&path);
                    let n = std::env::var("RBITCOIN_IBD_UTXO_SLOTS")
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(DEFAULT_NUM_SLOTS);
                    Self::create_with_mlock(path, n, mlock)
                }
                Err(e) => Err(e),
            }
        } else {
            let n = std::env::var("RBITCOIN_IBD_UTXO_SLOTS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_NUM_SLOTS);
            Self::create_with_mlock(path, n, mlock)
        }
    }

    /// Whether this handle requested mlock on open (may still be unlocked if pin failed).
    pub fn mlock_requested(&self) -> bool {
        self.mlock
    }

    pub fn tip(&self) -> Option<u32> {
        self.tip
    }

    pub fn live_count(&self) -> u64 {
        self.live
    }

    pub fn num_slots(&self) -> u64 {
        self.num_slots
    }

    pub fn load_factor(&self) -> f64 {
        if self.num_slots == 0 {
            return 0.0;
        }
        self.live as f64 / self.num_slots as f64
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn ensure_vout(vout: u32) -> Result<(), StoreError> {
        if vout > VOUT_MAX {
            return Err(StoreError::Corrupt("ibd utxo vout >= 2^24"));
        }
        Ok(())
    }

    /// Create-tx Class A fk for an unspent outpoint, if present.
    ///
    /// Slot keys are 12-byte txid prefix + vout (birthday collision on full
    /// txid is negligible at chain UTXO scale). Overflow holds full-txid
    /// extras when a compressed key is already live.
    pub fn get_create_fk(&self, txid: &[u8; 32], vout: u32) -> Result<Option<Fk>, StoreError> {
        Self::ensure_vout(vout)?;
        let prefix = prefix_of(txid);
        if let Some(list) = self.overflow.get(&(prefix, vout)) {
            if let Some(fk) = list
                .iter()
                .find(|(t, _)| t == txid)
                .and_then(|(_, raw)| Fk::new(*raw))
            {
                return Ok(Some(fk));
            }
            // Miss on overflow: first occupant may still live in the slot.
        }
        let mask = self.num_slots - 1;
        let mut i = hash0(&prefix, vout) & mask;
        for step in 0..self.num_slots {
            let (sp, state, sv, fk_raw) = Self::read_slot(&self.map, self.num_slots, i);
            match state {
                STATE_EMPTY => return Ok(None),
                STATE_LIVE if sp == prefix && sv == vout => {
                    return Ok(Fk::new(fk_raw));
                }
                STATE_LIVE | STATE_TOMB => {
                    i = (i + step + 1) & mask;
                }
                _ => return Err(StoreError::Corrupt("ibd utxo bad state")),
            }
        }
        Ok(None)
    }

    pub fn contains(&self, txid: &[u8; 32], vout: u32) -> Result<bool, StoreError> {
        Ok(self.get_create_fk(txid, vout)?.is_some())
    }

    /// Insert created outpoint with its create-tx Class A fk.
    ///
    /// Idempotent if already live (partial apply retry). **Single** open-address
    /// walk (no separate `contains` + insert probes — confirm UTXO apply was
    /// paying 2–3 probes per create).
    pub fn insert_create(
        &mut self,
        txid: &[u8; 32],
        vout: u32,
        create_fk: Fk,
    ) -> Result<(), StoreError> {
        Self::ensure_vout(vout)?;
        if create_fk.is_null() {
            return Err(StoreError::Corrupt("ibd utxo null create_fk"));
        }
        if self.load_factor() >= LOAD_GROW {
            self.grow()?;
        }
        let prefix = prefix_of(txid);
        // Exact full-txid already in overflow → idempotent.
        if let Some(list) = self.overflow.get(&(prefix, vout)) {
            if list.iter().any(|(t, _)| t == txid) {
                return Ok(());
            }
            // Prefix collision: another full txid occupies overflow. New create
            // with same compressed key goes to overflow (birthday on 12 bytes).
            let list = self
                .overflow
                .entry((prefix, vout))
                .or_insert_with(Vec::new);
            if !list.iter().any(|(t, _)| t == txid) {
                list.push((*txid, create_fk.0));
            }
            return Ok(());
        }
        let mask = self.num_slots - 1;
        let mut i = hash0(&prefix, vout) & mask;
        for step in 0..self.num_slots {
            let (sp, state, sv, _fk) = Self::read_slot(&self.map, self.num_slots, i);
            match state {
                STATE_EMPTY | STATE_TOMB => {
                    Self::write_slot(
                        &mut self.map,
                        self.num_slots,
                        i,
                        &prefix,
                        STATE_LIVE,
                        vout,
                        create_fk.0,
                    );
                    self.live = self.live.saturating_add(1);
                    return Ok(());
                }
                STATE_LIVE if sp == prefix && sv == vout => {
                    // Same compressed key already live (idempotent re-apply, or
                    // 12-byte birthday). First occupant stays in the slot; a
                    // distinct full txid would need overflow — treat birthday
                    // as negligible (same as prior contains_compressed path).
                    return Ok(());
                }
                STATE_LIVE => {
                    i = (i + step + 1) & mask;
                }
                _ => return Err(StoreError::Corrupt("ibd utxo bad state")),
            }
        }
        Err(StoreError::Corrupt("ibd utxo table full"))
    }

    /// Remove a spent outpoint. Returns false if it was not unspent.
    pub fn take_spend(&mut self, txid: &[u8; 32], vout: u32) -> Result<bool, StoreError> {
        Self::ensure_vout(vout)?;
        let prefix = prefix_of(txid);
        if let Some(list) = self.overflow.get_mut(&(prefix, vout)) {
            if let Some(pos) = list.iter().position(|(t, _)| t == txid) {
                list.swap_remove(pos);
                if list.is_empty() {
                    self.overflow.remove(&(prefix, vout));
                    // First occupant may still be live in the slot — do not
                    // tombstone just because overflow drained.
                }
                return Ok(true);
            }
            // Not in overflow: fall through (slot may hold the first occupant).
        }
        let mask = self.num_slots - 1;
        let mut i = hash0(&prefix, vout) & mask;
        for step in 0..self.num_slots {
            let (sp, state, sv, _) = Self::read_slot(&self.map, self.num_slots, i);
            match state {
                STATE_EMPTY => return Ok(false),
                STATE_LIVE if sp == prefix && sv == vout => {
                    Self::write_slot(
                        &mut self.map,
                        self.num_slots,
                        i,
                        &prefix,
                        STATE_TOMB,
                        vout,
                        0,
                    );
                    self.live = self.live.saturating_sub(1);
                    return Ok(true);
                }
                STATE_LIVE | STATE_TOMB => i = (i + step + 1) & mask,
                _ => return Err(StoreError::Corrupt("ibd utxo bad state")),
            }
        }
        Ok(false)
    }

    /// Update tip + header in the mmap (process-local; no msync).
    pub fn set_tip(&mut self, tip: Option<u32>) {
        self.tip = tip;
        Self::write_header(&mut self.map, self.tip, self.num_slots, self.live);
    }

    /// No-op durability: light UTXO is a rebuildable catch-up cache.
    ///
    /// Slot/header updates are visible in-process via the live mmap. We intentionally
    /// skip `msync` / file flush on the confirm hot path (and everywhere else) — kill
    /// may leave a torn or lagging map; open/heal rebuilds from Class A + tip.
    pub fn flush(&mut self) -> Result<(), StoreError> {
        Self::write_header(&mut self.map, self.tip, self.num_slots, self.live);
        Ok(())
    }

    /// Set tip in the mmap (no msync). Name kept for call sites / tests.
    pub fn commit_tip(&mut self, tip: Option<u32>) -> Result<(), StoreError> {
        self.set_tip(tip);
        Ok(())
    }

    pub fn grow(&mut self) -> Result<(), StoreError> {
        let new_slots = self.num_slots.saturating_mul(2).max(16);
        let mlock = self.mlock;
        let tmp = self.path.with_extension("map.tmp");
        let mut neu = Self::create_with_mlock(&tmp, new_slots, mlock)?;
        for i in 0..self.num_slots {
            let (prefix, state, vout, fk_raw) = Self::read_slot(&self.map, self.num_slots, i);
            if state != STATE_LIVE {
                continue;
            }
            neu.insert_live_prefix_fk(&prefix, vout, fk_raw)?;
        }
        for ((prefix, vout), entries) in &self.overflow {
            for (txid, fk_raw) in entries {
                let mut t = *txid;
                t[0..PREFIX_LEN].copy_from_slice(prefix);
                let fk = Fk::new(*fk_raw).unwrap_or(Fk::NULL);
                if !fk.is_null() {
                    neu.insert_create(&t, *vout, fk)?;
                }
            }
        }
        let mut live = 0u64;
        for i in 0..neu.num_slots {
            let (_, state, _, _) = Self::read_slot(&neu.map, neu.num_slots, i);
            if state == STATE_LIVE {
                live += 1;
            }
        }
        for list in neu.overflow.values() {
            if list.len() > 1 {
                live += (list.len() - 1) as u64;
            }
        }
        neu.live = live;
        neu.tip = self.tip;
        neu.commit_tip(self.tip)?;
        drop(neu);
        std::fs::rename(&tmp, &self.path).map_err(|e| io(&self.path, e))?;
        *self = Self::open_with_mlock(&self.path, mlock)?;
        Ok(())
    }

    fn insert_live_prefix_fk(
        &mut self,
        prefix: &[u8; PREFIX_LEN],
        vout: u32,
        create_fk: u64,
    ) -> Result<(), StoreError> {
        Self::ensure_vout(vout)?;
        let mask = self.num_slots - 1;
        let mut i = hash0(prefix, vout) & mask;
        for step in 0..self.num_slots {
            let (_, state, _, _) = Self::read_slot(&self.map, self.num_slots, i);
            if state == STATE_EMPTY || state == STATE_TOMB {
                Self::write_slot(
                    &mut self.map,
                    self.num_slots,
                    i,
                    prefix,
                    STATE_LIVE,
                    vout,
                    create_fk,
                );
                self.live = self.live.saturating_add(1);
                return Ok(());
            }
            i = (i + step + 1) & mask;
        }
        Err(StoreError::Corrupt("ibd utxo grow full"))
    }

    pub fn clear(&mut self) -> Result<(), StoreError> {
        self.map[HEADER_LEN..].fill(0);
        self.live = 0;
        self.tip = None;
        self.overflow.clear();
        self.commit_tip(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp() -> PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let d = std::env::temp_dir().join(format!("rbitcoin-ibd-utxo-{n}"));
        std::fs::create_dir_all(&d).unwrap();
        d.join("ibd_utxo.map")
    }

    fn txid(seed: u8) -> [u8; 32] {
        let mut t = [0u8; 32];
        t[0] = seed;
        t[1] = seed.wrapping_add(1);
        t[11] = seed.wrapping_add(3);
        t[31] = seed.wrapping_add(9);
        t
    }

    #[test]
    fn pack_unpack_state_vout() {
        let p = pack(STATE_LIVE, 42);
        let (s, v) = unpack(p);
        assert_eq!(s, STATE_LIVE);
        assert_eq!(v, 42);
    }

    #[test]
    fn insert_get_fk_take() {
        let path = tmp();
        let mut u = IbdUtxo::create(&path, 1024).unwrap();
        let t = txid(7);
        let fk = Fk(99);
        u.insert_create(&t, 0, fk).unwrap();
        assert_eq!(u.get_create_fk(&t, 0).unwrap(), Some(fk));
        assert!(u.contains(&t, 0).unwrap());
        u.insert_create(&t, 0, fk).unwrap(); // idempotent
        assert!(u.take_spend(&t, 0).unwrap());
        assert!(u.get_create_fk(&t, 0).unwrap().is_none());
        assert!(!u.take_spend(&t, 0).unwrap());
        let _ = std::fs::remove_file(&path);
    }

    /// Overflow miss must not hide the first (slot) occupant after a later
    /// full-txid was recorded under the same compressed key.
    #[test]
    fn overflow_miss_still_returns_slot_occupant() {
        let path = tmp();
        let mut u = IbdUtxo::create(&path, 1024).unwrap();
        let mut a = [0u8; 32];
        a[0..12].copy_from_slice(&[1u8; 12]);
        a[31] = 1;
        let mut b = a;
        b[31] = 2;
        u.insert_create(&a, 0, Fk(10)).unwrap();
        // Force overflow entry without going through insert's prefix-only
        // contains short-circuit (birthday path is not fully precise).
        u.overflow
            .entry((prefix_of(&a), 0))
            .or_default()
            .push((b, 20));
        assert_eq!(u.get_create_fk(&a, 0).unwrap(), Some(Fk(10)));
        assert_eq!(u.get_create_fk(&b, 0).unwrap(), Some(Fk(20)));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn many_inserts_and_roundtrip_file() {
        let path = tmp();
        let mut u = IbdUtxo::create(&path, 4096).unwrap();
        for i in 0..500u32 {
            let mut t = txid((i % 250) as u8);
            t[2] = (i >> 8) as u8;
            t[3] = i as u8;
            u.insert_create(&t, i % 10, Fk(u64::from(i) + 1)).unwrap();
        }
        u.commit_tip(Some(100)).unwrap();
        drop(u);
        let u2 = IbdUtxo::open(&path).unwrap();
        assert_eq!(u2.tip(), Some(100));
        assert!(u2.live_count() >= 500);
        let mut t = txid(0);
        t[2] = 0;
        t[3] = 0;
        assert_eq!(u2.get_create_fk(&t, 0).unwrap(), Some(Fk(1)));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn mlock_flag_open_does_not_fail() {
        // Even if mlock fails (low RLIMIT_MEMLOCK), open must succeed.
        let path = tmp();
        let u = IbdUtxo::create_with_mlock(&path, 64, true).unwrap();
        assert!(u.mlock_requested());
        drop(u);
        let u2 = IbdUtxo::open_with_mlock(&path, true).unwrap();
        assert!(u2.mlock_requested());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn rejects_huge_vout() {
        let path = tmp();
        let mut u = IbdUtxo::create(&path, 64).unwrap();
        let t = txid(1);
        assert!(u.insert_create(&t, VOUT_MAX + 1, Fk(1)).is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn slot_len_aligned() {
        assert_eq!(SLOT_LEN % 8, 0);
        assert_eq!(HEADER_LEN % 8, 0);
    }
}
