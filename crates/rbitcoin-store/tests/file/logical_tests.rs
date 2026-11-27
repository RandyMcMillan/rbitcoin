use rbitcoin_primitives::TableKind;
use rbitcoin_store::error::StoreError;
use rbitcoin_store::file::{TableFile, FILE_HEADER_LEN, TRAILING_FOOTER_LEN};
use rbitcoin_store::testutil::TempDir;

/// The leading header is 16 bytes. Reopen reads the logical length from it.
/// A file shorter than that header still fails before any field is read.
#[test]
fn leading_logical_survives_open() {
    let dir = TempDir::labeled("leading-logical").unwrap();
    let path = dir.path().join("txout");
    let logical = FILE_HEADER_LEN as u64 + 24;
    {
        let f = TableFile::create(&path, TableKind::TxOut).unwrap();
        f.set_logical_len(logical).unwrap();
        assert_eq!(f.logical_len(), logical);
    }
    let opened = TableFile::open(&path, TableKind::TxOut).unwrap();
    assert_eq!(opened.logical_len(), logical);

    let short = dir.path().join("short");
    std::fs::write(&short, [0u8; 8]).unwrap();
    assert!(matches!(
        TableFile::open(&short, TableKind::TxOut),
        Err(StoreError::Io { .. })
    ));
}

/// The trailer is 32 bytes at the published end. The data length is the
/// logical field minus that trailer. A short file stays corrupt.
#[test]
fn trailing_footer_names_the_data_len() {
    let dir = TempDir::labeled("trailing-logical").unwrap();
    let path = dir.path().join("txout");
    let data = 1000u64;
    let logical = data + TRAILING_FOOTER_LEN as u64;
    {
        let f = TableFile::create_trailing_header(&path, TableKind::TxOut).unwrap();
        f.ensure_capacity(logical).unwrap();
        f.set_logical_len(logical).unwrap();
    }
    let (_path, got) = TableFile::trailing_footer_data_bytes(&path, TableKind::TxOut).unwrap();
    assert_eq!(got, data);
    let (opened, _) = TableFile::open_trailing_header_from_end(&path, TableKind::TxOut).unwrap();
    assert_eq!(opened.logical_len(), logical);

    let short = dir.path().join("short");
    std::fs::write(&short, b"tiny").unwrap();
    assert!(matches!(
        TableFile::trailing_footer_data_bytes(&short, TableKind::TxOut),
        Err(StoreError::Corrupt("trailing-header table short"))
    ));
}

/// A store table: an empty `meta` beside it, so the write probe reports
/// the class it computes instead of `FIXTURE`.
fn store_dir(label: &str) -> TempDir {
    let dir = TempDir::labeled(label).unwrap();
    std::fs::write(dir.path().join("meta"), b"").unwrap();
    dir
}

/// A synced trailing table of `data` bytes at `path`.
fn synced_trailing(path: &std::path::Path, data: u64) -> TableFile {
    let f = TableFile::create_trailing_header(path, TableKind::HashHead).unwrap();
    let logical = data + TRAILING_FOOTER_LEN as u64;
    f.ensure_capacity(logical).unwrap();
    f.set_logical_len(logical).unwrap();
    f.write_at(0, &[0x11; 8]).unwrap();
    f.flush().unwrap();
    f
}

/// A trailing table built elsewhere and renamed into place is new to this
/// process. Its footer is its durable end, so an in-place write below it
/// is one the write probe classifies (`ONE_SECTOR` here), not an append
/// past zero.
#[test]
fn reopened_trailing_table_writes_below_its_footer() {
    let dir = store_dir("trailing-reopen");
    let built = dir.path().join("built");
    let path = dir.path().join("probe-reopen.tail");
    drop(synced_trailing(&built, 4096));
    std::fs::rename(&built, &path).unwrap();
    let (f, _) = TableFile::open_trailing_header_from_end(&path, TableKind::HashHead).unwrap();
    f.write_at(16, &[0x5a; 32]).unwrap();
    let mut got = [0u8; 32];
    f.pread_all(16, &mut got).unwrap();
    assert_eq!(got, [0x5a; 32]);
}

/// `set_trailing_ext` rewrites a synced footer at the same length. The
/// write probe classifies that rewrite by its sectors (`ONE_SECTOR` here)
/// instead of skipping it as a length that did not grow.
#[test]
fn trailing_ext_rewrites_the_footer_in_place() {
    let dir = store_dir("trailing-ext");
    let path = dir.path().join("probe-ext.tail");
    let mut f = synced_trailing(&path, 4096);
    let ext = [0x7e; 16];
    f.set_trailing_ext(ext).unwrap();
    f.flush().unwrap();
    drop(f);
    let (_, got) = TableFile::open_trailing_header_from_end(&path, TableKind::HashHead).unwrap();
    assert_eq!(got, ext);
}
