//! Completion-ring facade over a bounded worker pool.
//!
//! Same harvest contract as io_uring (`user_data`, drain, in-flight cap).
//! Darwin's file AIO is a thread pool; this is the honest session there and
//! the Linux CI pin (`RBITCOIN_IO=pool`).

use crate::error::StoreError;
use crate::io_handle::IoHandle;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

struct Job {
    handle: IoHandle,
    offset: u64,
    ptr: *mut u8,
    len: usize,
    user_data: u64,
    write: bool,
}

// SAFETY: the machine thread keeps `ptr` live until the matching CQE is harvested.
unsafe impl Send for Job {}

struct Shared {
    jobs: Mutex<VecDeque<Job>>,
    job_cv: Condvar,
    cqes: Mutex<VecDeque<(u64, i32)>>,
    cqe_cv: Condvar,
    shutdown: AtomicBool,
    /// Jobs popped by a worker and not yet pushed to `cqes`.
    running: AtomicUsize,
}

pub(crate) struct PoolEngine {
    shared: Arc<Shared>,
    workers: Vec<JoinHandle<()>>,
}

impl PoolEngine {
    pub(crate) fn open(n_workers: usize) -> Self {
        let n = n_workers.clamp(1, 16);
        let shared = Arc::new(Shared {
            jobs: Mutex::new(VecDeque::new()),
            job_cv: Condvar::new(),
            cqes: Mutex::new(VecDeque::new()),
            cqe_cv: Condvar::new(),
            shutdown: AtomicBool::new(false),
            running: AtomicUsize::new(0),
        });
        let mut workers = Vec::with_capacity(n);
        for _ in 0..n {
            let sh = Arc::clone(&shared);
            workers.push(
                thread::Builder::new()
                    .name("rbtc-io-pool".into())
                    .spawn(move || worker_loop(sh))
                    .expect("spawn io pool worker"),
            );
        }
        Self { shared, workers }
    }

    pub(crate) fn push_pread(
        &self,
        handle: IoHandle,
        offset: u64,
        buf: &mut [u8],
        user_data: u64,
    ) -> Result<(), StoreError> {
        self.push(
            handle,
            offset,
            buf.as_mut_ptr(),
            buf.len(),
            user_data,
            false,
        )
    }

    pub(crate) fn push_pwrite(
        &self,
        handle: IoHandle,
        offset: u64,
        buf: &[u8],
        user_data: u64,
    ) -> Result<(), StoreError> {
        self.push(
            handle,
            offset,
            buf.as_ptr() as *mut u8,
            buf.len(),
            user_data,
            true,
        )
    }

    fn push(
        &self,
        handle: IoHandle,
        offset: u64,
        ptr: *mut u8,
        len: usize,
        user_data: u64,
        write: bool,
    ) -> Result<(), StoreError> {
        if len == 0 {
            return Ok(());
        }
        {
            let mut q = self.shared.jobs.lock().unwrap_or_else(|e| e.into_inner());
            q.push_back(Job {
                handle,
                offset,
                ptr,
                len,
                user_data,
                write,
            });
        }
        self.shared.job_cv.notify_one();
        Ok(())
    }

    pub(crate) fn harvest_ready(&self) -> Vec<(u64, i32)> {
        let mut q = self.shared.cqes.lock().unwrap_or_else(|e| e.into_inner());
        q.drain(..).collect()
    }

    pub(crate) fn wait_one_cqe(&self) {
        let mut q = self.shared.cqes.lock().unwrap_or_else(|e| e.into_inner());
        while q.is_empty() && !self.idle() {
            q = self
                .shared
                .cqe_cv
                .wait(q)
                .unwrap_or_else(|e| e.into_inner());
        }
    }

    fn idle(&self) -> bool {
        let jobs = self
            .shared
            .jobs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty();
        jobs && self.shared.running.load(Ordering::Acquire) == 0
    }

    pub(crate) fn wait_idle(&self) {
        let mut q = self.shared.cqes.lock().unwrap_or_else(|e| e.into_inner());
        while !self.idle() {
            q = self
                .shared
                .cqe_cv
                .wait(q)
                .unwrap_or_else(|e| e.into_inner());
        }
    }
}

impl Drop for PoolEngine {
    fn drop(&mut self) {
        self.shared.shutdown.store(true, Ordering::Release);
        self.shared.job_cv.notify_all();
        self.shared.cqe_cv.notify_all();
        for h in self.workers.drain(..) {
            let _ = h.join();
        }
    }
}

fn worker_loop(shared: Arc<Shared>) {
    loop {
        if shared.shutdown.load(Ordering::Acquire) {
            return;
        }
        let job = {
            let mut q = shared.jobs.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                if shared.shutdown.load(Ordering::Acquire) {
                    return;
                }
                if let Some(j) = q.pop_front() {
                    // Count as in-flight *before* releasing jobs so idle()
                    // cannot see empty+running==0 between pop and the old
                    // post-unlock increment (drain would Corrupt / UAF).
                    shared.running.fetch_add(1, Ordering::AcqRel);
                    break j;
                }
                q = shared.job_cv.wait(q).unwrap_or_else(|e| e.into_inner());
            }
        };
        let buf = unsafe { std::slice::from_raw_parts_mut(job.ptr, job.len) };
        let res = if job.write {
            job.handle.pwrite(job.offset, buf)
        } else {
            job.handle.pread(job.offset, buf)
        };
        {
            let mut cq = shared.cqes.lock().unwrap_or_else(|e| e.into_inner());
            cq.push_back((job.user_data, res));
        }
        shared.running.fetch_sub(1, Ordering::AcqRel);
        shared.cqe_cv.notify_all();
    }
}
