//! Confirm-cache page pinning: identify and unlock mmap ranges across tables.
//!
//! Runway `mlock`s every Class A / Class C / head page confirm will touch for
//! heights in the parent cache, then releases via tip GC when heights leave
//! `(tip, tip+depth]`.

/// Which store mmap a locked page range belongs to (for unlock routing).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MlockTable {
    TxBody = 0,
    TxIdx = 1,
    TxHead = 2,
    HeaderBody = 3,
    HeaderHead = 4,
    HeaderTxsFirst = 5,
    HeaderTxsCount = 6,
    Spenders = 7,
    StrongTx = 8,
    TxHeight = 9,
    Confirmed = 10,
}

impl MlockTable {
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Page-aligned range previously returned from [`crate::file::TableFile::mlock_range`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MlockRange {
    pub table: MlockTable,
    pub page_start: u64,
    pub page_len: u64,
}

impl MlockRange {
    pub fn empty(table: MlockTable) -> Self {
        Self {
            table,
            page_start: 0,
            page_len: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.page_len == 0
    }
}
