//! Drop-cleaning Tiny fixtures for tests. Not an operator API.
//!
//! Production constructors stay Mainnet (`StoreLayout::single`). Tests that
//! need an on-disk store use [`tiny_store`] / [`TempDir`] instead of rolling
//! `std::env::temp_dir()` + `create_dir_all`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::Store;

#[doc(hidden)]
pub static SEQ: AtomicU64 = AtomicU64::new(0);

fn sanitize_label(label: &str) -> String {
    label
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

fn clock_tick() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// Path no other call in this process shares, including two calls in one clock tick.
fn unique_path(label: &str, tick: u128) -> PathBuf {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let safe = sanitize_label(label);
    std::env::temp_dir().join(format!("rbitcoin-{safe}-{}-{tick}-{n}", std::process::id(),))
}

/// Skip every file flush in this process from now on
/// ([`crate::durability`]). Only [`crate::skip_flushes_before_main!`] calls it.
#[cfg(debug_assertions)]
pub fn skip_flushes_in_this_process() {
    crate::durability::skip_in_this_process();
}

/// Flushes sent to the OS while this flushes one file through each path:
/// [`Durable::durable_sync_all`], [`Durable::durable_sync_data`], and
/// [`TableFile::sync_data_all`] (io_uring, the pool, or `Durable`). Other
/// threads' flushes count too. Only [`crate::skip_flushes_before_main!`]'s
/// test calls it.
///
/// [`Durable::durable_sync_all`]: crate::durability::Durable::durable_sync_all
/// [`Durable::durable_sync_data`]: crate::durability::Durable::durable_sync_data
/// [`TableFile::sync_data_all`]: crate::file::TableFile::sync_data_all
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn flushes_issued_by_probe() -> Result<u64, crate::StoreError> {
    use crate::durability::Durable as _;

    let dir = TempDir::labeled("flush-probe")
        .map_err(|e| crate::StoreError::io(Path::new("flush-probe"), e))?;
    let path = dir.path().join("f");
    let before = crate::durability::flushes_issued();
    let file = std::fs::File::create(&path).map_err(|e| crate::StoreError::io(&path, e))?;
    file.durable_sync_all()
        .map_err(|e| crate::StoreError::io(&path, e))?;
    file.durable_sync_data()
        .map_err(|e| crate::StoreError::io(&path, e))?;
    let table = crate::file::TableFile::create(
        dir.path().join("t"),
        rbitcoin_primitives::TableKind::ArrayLink,
    )?;
    crate::file::TableFile::sync_data_all(&[&table])?;
    Ok(crate::durability::flushes_issued() - before)
}

/// Turn file flushes off for this test binary before any test runs, under
/// the calling crate's `skip-flushes` feature ([`crate::durability`]). Call it
/// once, at the top of a `tests/suite.rs`, in a crate that declares that
/// feature. It also adds a test that the switch matches the feature and that
/// a flush through `Durable` or a table batch sync obeys it. A test
/// that checks a flush belongs in a suite that does not call it.
#[macro_export]
macro_rules! skip_flushes_before_main {
    () => {
        #[cfg(all(feature = "skip-flushes", not(debug_assertions)))]
        compile_error!("skip-flushes needs a debug build");

        // Before `main`, so before any test thread (the net suite's lane gate
        // uses the same sections).
        #[cfg(all(feature = "skip-flushes", debug_assertions))]
        #[used]
        #[cfg_attr(
            any(target_os = "linux", target_os = "android", target_os = "freebsd"),
            link_section = ".init_array"
        )]
        #[cfg_attr(target_os = "macos", link_section = "__DATA,__mod_init_func")]
        #[cfg_attr(target_os = "windows", link_section = ".CRT$XCU")]
        static SKIP_FLUSHES_BEFORE_MAIN: extern "C" fn() = {
            extern "C" fn skip() {
                $crate::testutil::skip_flushes_in_this_process();
            }
            skip
        };

        #[cfg(debug_assertions)]
        #[test]
        fn flushes_skipped_matches_the_skip_flushes_feature(
        ) -> ::std::result::Result<(), $crate::StoreError> {
            let skip = cfg!(feature = "skip-flushes");
            assert_eq!($crate::durability::flushes_skipped(), skip);
            let issued = $crate::testutil::flushes_issued_by_probe()?;
            // Other tests run beside this one, so an unskipped count is a floor.
            if skip {
                assert_eq!(issued, 0);
            } else {
                assert!(issued >= 3, "issued {issued}");
            }
            Ok(())
        }
    };
}

/// Unique path. The file or directory is not created.
pub fn fresh_path(label: &str) -> PathBuf {
    unique_path(label, clock_tick())
}

/// Unique directory removed on drop.
pub struct TempDir {
    pub path: PathBuf,
}

impl TempDir {
    pub fn new() -> std::io::Result<Self> {
        Self::labeled("tmp")
    }

    pub fn labeled(label: &str) -> std::io::Result<Self> {
        Self::labeled_at(label, clock_tick())
    }

    /// `tick` is the clock sample. Two calls with the same sample still differ.
    #[doc(hidden)]
    pub fn labeled_at(label: &str, tick: u128) -> std::io::Result<Self> {
        let path = unique_path(label, tick);
        std::fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    /// Unique directory short enough to bind a Unix socket in. macOS
    /// `sun_path` holds 103 bytes and its default `TMPDIR` already takes 49,
    /// so the name has no label or timestamp. `create_dir` refusing a
    /// leftover from an earlier process with the same PID keeps it unique.
    pub fn for_socket() -> std::io::Result<Self> {
        loop {
            let n = SEQ.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("rbitcoin-sock-{}-{n}", std::process::id()));
            match std::fs::create_dir(&path) {
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                created => return created.map(|()| Self { path }),
            }
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn as_path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

impl AsRef<Path> for TempDir {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<std::ffi::OsStr> for TempDir {
    fn as_ref(&self) -> &std::ffi::OsStr {
        self.path.as_os_str()
    }
}

impl std::ops::Deref for TempDir {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.path
    }
}

/// Recursive copy of a store directory. Creates `dst` first.
pub fn copy_store_tree(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create store copy");
    for ent in std::fs::read_dir(src).expect("read store") {
        let ent = ent.expect("store entry");
        let to = dst.join(ent.file_name());
        let ty = ent.file_type().expect("file type");
        if ty.is_dir() {
            copy_store_tree(&ent.path(), &to);
        } else {
            std::fs::copy(ent.path(), &to).expect("copy store file");
        }
    }
}

/// Tiny-head store in a drop-cleaning directory.
pub fn tiny_store() -> (TempDir, Store) {
    tiny_store_labeled("store")
}

pub fn tiny_store_labeled(label: &str) -> (TempDir, Store) {
    let dir = TempDir::labeled(label).expect("create temp dir");
    let store = Store::create_tiny(dir.path()).expect("create tiny store");
    (dir, store)
}

/// Tiny store at `dir` whose `tx.head` segments have `2^bits` slots, so a
/// few hundred creates fill one and roll it.
pub fn create_tiny_with_head_bits(dir: &Path, bits: u32) -> Store {
    let head = crate::address_head::HeadLayout::new(bits).expect("head bits in range");
    Store::create_layout_with_head(crate::StoreLayout::tiny(dir.to_path_buf()), head)
        .expect("create tiny store")
}

/// Zero the slot pages of every unsealed `tx.head` segment under `root`,
/// keeping each file's length, its trailing layout footer, and the synced
/// `meta`. A power loss leaves this when `meta` was synced after an insert
/// and the page writes were not.
pub fn drop_unsynced_tx_head_pages(root: &Path) {
    fn find_head(dir: &Path) -> Option<PathBuf> {
        std::fs::read_dir(dir).ok()?.flatten().find_map(|e| {
            let p = e.path();
            if !p.is_dir() {
                return None;
            }
            if p.file_name()? == "tx.head" {
                Some(p)
            } else {
                find_head(&p)
            }
        })
    }
    let head = find_head(root).expect("tx.head under root");
    for e in std::fs::read_dir(&head).expect("read tx.head").flatten() {
        let p = e.path();
        if p.extension().is_some() || p.file_name().is_some_and(|n| n == "meta") {
            continue;
        }
        let len = std::fs::metadata(&p).expect("segment len").len();
        let slots = len.saturating_sub(crate::file::TRAILING_FOOTER_LEN as u64);
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&p)
            .expect("open segment");
        std::io::Write::write_all(&mut f, &vec![0u8; slots as usize]).expect("zero slot pages");
    }
}

/// Checkpoint an explicit height, running `between` after `sync_data`.
#[cfg(debug_assertions)]
pub fn checkpoint_spend_through_between(
    store: &Store,
    height: u32,
    between: impl FnOnce(&Store),
) -> Result<(), crate::StoreError> {
    store.checkpoint_spend_through_between(height, between)
}

/// Disconnect-generation sample, then snapshot height, with a hook between
/// them and a hook during `sync_data`.
#[cfg(debug_assertions)]
pub fn checkpoint_observed_spend_gap(
    store: &Store,
    after_first_read: impl FnOnce(&Store),
    during_sync: impl FnOnce(&Store),
) -> Result<(), crate::StoreError> {
    store.checkpoint_observed_spend_gap(after_first_read, during_sync)
}

/// Class A writer count sampled when the failure hook last ran on this
/// thread.
pub fn class_a_writers_at_last_failure() -> usize {
    crate::tx_table::WRITERS_AT_LATCH.with(|slot| slot.get())
}

/// One-shot hook run inside the next Class A seal on this thread, after the
/// latch check at entry and before the writer count is read. A test latches
/// an append failure there, the way a racing append thread can.
#[cfg(debug_assertions)]
pub fn set_before_class_a_seal(hook: Option<crate::tx_table::SealHook>) {
    crate::tx_table::BEFORE_SEAL.with(|c| *c.borrow_mut() = hook);
}

/// Latch a Class A failure on `txs`, as a failed append write does.
pub fn latch_class_a_failure(txs: &crate::tx_table::TxTable) {
    txs.latch_class_a_failure();
}
