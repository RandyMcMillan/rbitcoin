//! Archive mode and finalization epoch (durable-archive soft/hard zones).

use crate::error::StoreError;
use rbitcoin_primitives::{schema_file_openable, SCHEMA_VERSION, STORE_MAGIC};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::Path;

/// On-disk epoch record under `store/archive_epoch`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchiveEpoch {
    /// When true, tip wire ring + incremental finalize are active.
    pub archive_mode: bool,
    /// Last height fully fsynced / sealed (None = nothing finalized yet).
    pub finalized_height: Option<u32>,
    /// Soft-zone depth (wire ring window).
    pub wire_depth: u32,
}

impl Default for ArchiveEpoch {
    fn default() -> Self {
        Self {
            archive_mode: false,
            finalized_height: None,
            wire_depth: 100,
        }
    }
}

impl ArchiveEpoch {
    const FILE: &'static str = "archive_epoch";

    pub fn path(dir: &Path) -> std::path::PathBuf {
        dir.join(Self::FILE)
    }

    pub fn load(dir: &Path) -> Result<Self, StoreError> {
        let p = Self::path(dir);
        if !p.exists() {
            return Ok(Self::default());
        }
        let mut f = OpenOptions::new()
            .read(true)
            .open(&p)
            .map_err(|e| StoreError::io(&p, e))?;
        let mut buf = [0u8; 32];
        f.read_exact(&mut buf).map_err(|e| StoreError::io(&p, e))?;
        if buf[0..4] != STORE_MAGIC {
            return Err(StoreError::BadMagic);
        }
        let ver = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        if !schema_file_openable(ver) {
            return Err(StoreError::BadSchema(ver));
        }
        let mode = buf[6] != 0;
        let has_fin = buf[7] != 0;
        let fin = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let wire_depth = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        Ok(Self {
            archive_mode: mode,
            finalized_height: if has_fin { Some(fin) } else { None },
            wire_depth,
        })
    }

    pub fn store(&self, dir: &Path) -> Result<(), StoreError> {
        let p = Self::path(dir);
        let mut buf = [0u8; 32];
        buf[0..4].copy_from_slice(&STORE_MAGIC);
        buf[4..6].copy_from_slice(&SCHEMA_VERSION.to_le_bytes());
        buf[6] = if self.archive_mode { 1 } else { 0 };
        match self.finalized_height {
            Some(h) => {
                buf[7] = 1;
                buf[8..12].copy_from_slice(&h.to_le_bytes());
            }
            None => {
                buf[7] = 0;
            }
        }
        buf[12..16].copy_from_slice(&self.wire_depth.to_le_bytes());
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&p)
            .map_err(|e| StoreError::io(&p, e))?;
        f.write_all(&buf).map_err(|e| StoreError::io(&p, e))?;
        f.sync_all().map_err(|e| StoreError::io(&p, e))?;
        Ok(())
    }

    /// Soft zone: heights strictly above finalized (if any).
    pub fn is_soft_zone(&self, height: u32) -> bool {
        match self.finalized_height {
            None => true,
            Some(f) => height > f,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_primitives::{SCHEMA_VERSION, STORE_MAGIC};
    use std::io::Write;

    fn tmp_dir() -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "rbitcoin-epoch-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn epoch_load_store_soft_zone_and_errors() {
        let dir = tmp_dir();
        // Missing file → default.
        let d = ArchiveEpoch::load(&dir).unwrap();
        assert_eq!(d, ArchiveEpoch::default());
        assert!(d.is_soft_zone(0));
        assert!(d.is_soft_zone(999));

        let e = ArchiveEpoch {
            archive_mode: true,
            finalized_height: Some(10),
            wire_depth: 50,
        };
        e.store(&dir).unwrap();
        let loaded = ArchiveEpoch::load(&dir).unwrap();
        assert_eq!(loaded, e);
        assert!(!loaded.is_soft_zone(10));
        assert!(loaded.is_soft_zone(11));
        assert!(!loaded.is_soft_zone(0));

        // finalized None branch on store
        let e2 = ArchiveEpoch {
            archive_mode: false,
            finalized_height: None,
            wire_depth: 7,
        };
        e2.store(&dir).unwrap();
        assert_eq!(ArchiveEpoch::load(&dir).unwrap(), e2);

        // Bad magic
        {
            let p = ArchiveEpoch::path(&dir);
            let mut f = std::fs::File::create(&p).unwrap();
            f.write_all(&[0u8; 32]).unwrap();
        }
        assert!(matches!(
            ArchiveEpoch::load(&dir),
            Err(StoreError::BadMagic)
        ));

        // Bad schema
        {
            let p = ArchiveEpoch::path(&dir);
            let mut buf = [0u8; 32];
            buf[0..4].copy_from_slice(&STORE_MAGIC);
            buf[4..6].copy_from_slice(&(SCHEMA_VERSION.wrapping_add(99)).to_le_bytes());
            std::fs::write(&p, buf).unwrap();
        }
        assert!(matches!(
            ArchiveEpoch::load(&dir),
            Err(StoreError::BadSchema(_))
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
