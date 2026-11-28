//! Flush exclusion. A store that returns Ok is the slot on disk when that
//! flush left the mark clean. `get_sample` does not cover this handshake.

use rbitcoin_primitives::TableKind;
use rbitcoin_store::array_table::{ArrayTable, FlushStep};
use rbitcoin_store::ClassCWrite;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

#[test]
fn a_store_that_passes_the_flush_is_the_slot_on_disk() {
    super::engine::check_at_most(3, || {
        let dir = std::env::temp_dir().join("rbtc-flush-model");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let table =
            Arc::new(ArrayTable::create(dir.join("slots.body"), TableKind::ArrayLink).unwrap());
        table.set(&ClassCWrite::for_test(), 0, 5).unwrap();
        let entered = Arc::new(AtomicBool::new(false));
        let sampled = Arc::new(AtomicU64::new(u64::MAX));
        let writing = Arc::clone(&table);
        let entered_w = Arc::clone(&entered);
        let writer = loom::thread::spawn(move || {
            if writing.set(&ClassCWrite::for_test(), 0, 99).is_ok() {
                entered_w.store(true, Ordering::Relaxed);
            }
        });
        let flushing = Arc::clone(&table);
        let sampled_f = Arc::clone(&sampled);
        let flusher = loom::thread::spawn(move || {
            flushing
                .flush_dirty_between(|t, step| {
                    if step == FlushStep::Write {
                        sampled_f.store(t.live_slot(0), Ordering::Relaxed);
                    }
                })
                .unwrap();
        });
        writer.join().unwrap();
        flusher.join().unwrap();
        if entered.load(Ordering::Relaxed)
            && table.unstored_from.load(Ordering::Relaxed) == u64::MAX
        {
            let mut buf = [0u8; 8];
            table.file.read_at(ArrayTable::offset(0), &mut buf).unwrap();
            let disk = u64::from_le_bytes(buf);
            let seen = sampled.load(Ordering::Relaxed);
            if disk != 99 {
                panic!("committed store lost: disk={disk} mark=CLEAN entered=true sampled={seen}");
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    });
}

/// The grouped Class C flush publishes clean after its mark is cleared,
/// outside the store exclusion. A store noted in that window must stay dirty,
/// so the next flush writes it.
#[test]
fn a_note_during_the_grouped_publish_stays_dirty() {
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_store::chain::ConfirmedTable;
    // Page images are 4 KiB on the stack. The model coroutine stack is 8 KiB.
    fn big<T: Send + 'static>(
        f: impl FnOnce() -> T + Send + 'static,
    ) -> loom::thread::JoinHandle<T> {
        loom::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(f)
            .unwrap()
    }
    super::engine::check_at_most(3, || {
        big(|| {
            let dir = std::env::temp_dir()
                .join(format!("rbtc-group-publish-model-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            let table = Arc::new(ConfirmedTable::create(&dir).unwrap());
            let write = ClassCWrite::for_test();
            table.set(&write, Height(0), Fk(5)).unwrap();
            table.flush().unwrap();
            table.set(&write, Height(0), Fk(6)).unwrap();
            let writing = Arc::clone(&table);
            let writer = big(move || {
                writing
                    .set(&ClassCWrite::for_test(), Height(0), Fk(99))
                    .is_ok()
            });
            let flushing = Arc::clone(&table);
            let flusher = big(move || flushing.flush().unwrap());
            let stored = writer.join().unwrap();
            flusher.join().unwrap();
            table.flush().unwrap();
            if !stored {
                let _ = std::fs::remove_dir_all(&dir);
                return;
            }
            let mut buf = [0u8; 8];
            table
                .arr
                .file
                .read_at(ArrayTable::offset(0), &mut buf)
                .unwrap();
            let disk = u64::from_le_bytes(buf);
            if disk != 99 {
                panic!("a note during the grouped publish was marked clean: disk={disk}");
            }
            let _ = std::fs::remove_dir_all(&dir);
        })
        .join()
        .unwrap();
    });
}
