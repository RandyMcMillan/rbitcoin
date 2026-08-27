//! In-tree BDZ 3-graph MPHF (`u64` keys → `[0, n)`).
//!
//! A key **not** in the set still maps into `[0, n)`. Callers verify identity.

use crate::error::StoreError;
use crate::io_handle::IoHandle;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const MAGIC: &[u8; 4] = b"BDZ1";
const MAGIC2: &[u8; 4] = b"BDZ2";
const VERSION: u32 = 1;
const GAMMA_NUM: u64 = 123;
const GAMMA_DEN: u64 = 100;
const MAX_SEED: u32 = 256;
const HEADER_LEN: u64 = 24;
const HEADER_LEN2: u64 = 32;
const G_PAGE_BYTES: usize = 4096;
const G_PAGE_WORDS: u32 = (G_PAGE_BYTES / 4) as u32;
const G_BITS_WORDS: u32 = 32;

#[derive(Debug)]
enum GStore {
    Ram(Box<[u32]>),
    Fd {
        file: File,
        path: PathBuf,
        off: u64,
        n_bytes: u64,
        g_bits: u32,
        page_preads: AtomicU64,
    },
}

#[derive(Debug)]
pub struct BdzMphf {
    n: u32,
    m: u32,
    seed: u64,
    modulus: u32,
    g: GStore,
}

#[inline]
fn splitmix64(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *seed;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[inline]
fn mix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

#[inline]
fn hash3(key: u64, seed: u64, m: u32) -> [u32; 3] {
    let h = mix64(key.wrapping_add(seed));
    let m = m as u64;
    let a = ((h as u128 * m as u128) >> 64) as u32;
    let b = ((h.rotate_left(21) as u128 * m as u128) >> 64) as u32;
    let c = ((h.rotate_left(42) as u128 * m as u128) >> 64) as u32;
    let mut out = [a, b, c];
    if out[1] == out[0] {
        out[1] = (out[1] + 1) % (m as u32);
    }
    if out[2] == out[0] || out[2] == out[1] {
        out[2] = (out[2] + 1) % (m as u32);
        if out[2] == out[0] || out[2] == out[1] {
            out[2] = (out[2] + 1) % (m as u32);
        }
    }
    out
}

impl BdzMphf {
    pub fn n(&self) -> u32 {
        self.n
    }

    #[cfg(test)]
    pub fn modulus(&self) -> u32 {
        self.modulus
    }

    pub fn g_bytes(&self) -> usize {
        match &self.g {
            GStore::Ram(g) => g.len() * 4,
            GStore::Fd { n_bytes, .. } => *n_bytes as usize,
        }
    }

    pub fn g_bytes_resident(&self) -> usize {
        match &self.g {
            GStore::Ram(g) => g.len() * 4,
            GStore::Fd { .. } => 0,
        }
    }

    #[cfg(test)]
    pub fn vertices(&self, key: u64) -> [u32; 3] {
        if self.n <= 1 {
            return [0, 0, 0];
        }
        hash3(key, self.seed, self.m)
    }

    pub fn index(&self, key: u64) -> Result<u32, StoreError> {
        Ok(self.index_batch(&[key], &mut crate::IoCtx::none())?[0])
    }

    #[cfg(test)]
    pub fn take_g_page_preads(&self) -> u64 {
        match &self.g {
            GStore::Fd { page_preads, .. } => page_preads.swap(0, Ordering::Relaxed),
            GStore::Ram(_) => 0,
        }
    }

    pub fn index_batch(
        &self,
        keys: &[u64],
        ctx: &mut crate::IoCtx<'_>,
    ) -> Result<Vec<u32>, StoreError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        if self.n == 0 {
            return Ok(vec![0; keys.len()]);
        }
        if self.n == 1 {
            return Ok(vec![self.one_key_index()?; keys.len()]);
        }
        match &self.g {
            GStore::Ram(g) => Ok(keys
                .iter()
                .map(|&k| {
                    let [a, b, c] = hash3(k, self.seed, self.m);
                    g[a as usize]
                        .wrapping_add(g[b as usize])
                        .wrapping_add(g[c as usize])
                        % self.modulus
                })
                .collect()),
            GStore::Fd { .. } => self.index_batch_fd(keys, ctx),
        }
    }

    fn index_batch_fd(
        &self,
        keys: &[u64],
        ctx: &mut crate::IoCtx<'_>,
    ) -> Result<Vec<u32>, StoreError> {
        let GStore::Fd {
            file,
            path,
            off,
            n_bytes,
            g_bits,
            page_preads,
            ..
        } = &self.g
        else {
            return Err(StoreError::Corrupt("bdz mphf: fd batch"));
        };
        let verts: Vec<[u32; 3]> = keys.iter().map(|&k| hash3(k, self.seed, self.m)).collect();
        if *g_bits == G_BITS_WORDS {
            let mut page_ids: Vec<u32> = verts
                .iter()
                .flat_map(|v| v.iter().map(|&vert| vert / G_PAGE_WORDS))
                .collect();
            page_ids.sort_unstable();
            page_ids.dedup();
            let mut words = vec![[0u32; 3]; keys.len()];
            let mut fill = |page: u32, buf: &[u8]| {
                let page_base = page * G_PAGE_WORDS;
                for (ki, v) in verts.iter().enumerate() {
                    for (j, &vert) in v.iter().enumerate() {
                        if vert / G_PAGE_WORDS != page {
                            continue;
                        }
                        let rel = ((vert - page_base) as usize) * 4;
                        words[ki][j] = u32::from_le_bytes(buf[rel..rel + 4].try_into().unwrap());
                    }
                }
            };
            stream_or_load_pages(
                file,
                path,
                *off,
                *n_bytes,
                &page_ids,
                page_preads,
                ctx,
                &mut fill,
            )?;
            return Ok(words
                .iter()
                .map(|[a, b, c]| a.wrapping_add(*b).wrapping_add(*c) % self.modulus)
                .collect());
        }
        let mut page_ids: Vec<u32> = verts
            .iter()
            .flat_map(|v| {
                v.iter()
                    .copied()
                    .flat_map(|vert| vertex_pages(vert, *g_bits))
            })
            .collect();
        page_ids.sort_unstable();
        page_ids.dedup();
        let mut loaded: Vec<(u32, Vec<u8>)> = Vec::with_capacity(page_ids.len());
        let mut fill = |page: u32, buf: &[u8]| {
            loaded.push((page, buf.to_vec()));
        };
        stream_or_load_pages(
            file,
            path,
            *off,
            *n_bytes,
            &page_ids,
            page_preads,
            ctx,
            &mut fill,
        )?;
        Ok(verts
            .iter()
            .map(|[a, b, c]| {
                unpack_g_from_pages(&loaded, *a, *g_bits)
                    .wrapping_add(unpack_g_from_pages(&loaded, *b, *g_bits))
                    .wrapping_add(unpack_g_from_pages(&loaded, *c, *g_bits))
                    % self.modulus
            })
            .collect())
    }

    fn one_key_index(&self) -> Result<u32, StoreError> {
        match &self.g {
            GStore::Ram(g) => Ok(g.first().copied().unwrap_or(0)),
            GStore::Fd {
                file,
                path,
                off,
                g_bits,
                ..
            } => {
                if *g_bits == G_BITS_WORDS {
                    let mut buf = [0u8; 4];
                    pread_exact(file, path, *off, &mut buf)?;
                    return Ok(u32::from_le_bytes(buf));
                }
                let nbytes = ((*g_bits as usize) + 7) / 8;
                let mut buf = vec![0u8; nbytes.max(1)];
                pread_exact(file, path, *off, &mut buf)?;
                Ok(unpack_g_at(&buf, 0, *g_bits))
            }
        }
    }

    pub fn build(keys: &[u64]) -> Result<Self, StoreError> {
        let n = keys.len() as u32;
        let ranks: Vec<u32> = (0..n).collect();
        Self::build_from_ranks(keys, &ranks, n)
    }

    pub fn build_assigned(keys: &[u64], values: &[u32], modulus: u32) -> Result<Self, StoreError> {
        if keys.len() != values.len() {
            return Err(StoreError::Corrupt("bdz mphf: assigned len"));
        }
        if keys.is_empty() {
            return Self::build_from_ranks(keys, values, modulus);
        }
        if modulus == 0 {
            return Err(StoreError::Corrupt("bdz mphf: assigned modulus"));
        }
        let mut seen = vec![false; modulus as usize];
        for &v in values {
            if v >= modulus {
                return Err(StoreError::Corrupt("bdz mphf: assigned value"));
            }
            if seen[v as usize] {
                return Err(StoreError::Corrupt("bdz mphf: assigned duplicate"));
            }
            seen[v as usize] = true;
        }
        Self::build_from_ranks(keys, values, modulus)
    }

    fn build_from_ranks(keys: &[u64], ranks: &[u32], modulus: u32) -> Result<Self, StoreError> {
        let n = keys.len() as u32;
        if n == 0 {
            return Ok(Self {
                n: 0,
                m: 0,
                seed: 0,
                modulus,
                g: GStore::Ram(Box::new([])),
            });
        }
        if n == 1 {
            return Ok(Self {
                n: 1,
                m: 1,
                seed: 1,
                modulus,
                g: GStore::Ram(vec![ranks[0]].into_boxed_slice()),
            });
        }
        let m = ((u64::from(n) * GAMMA_NUM + GAMMA_DEN - 1) / GAMMA_DEN)
            .max(u64::from(n) + 3)
            .max(3) as u32;
        let mut rng = 0x9e37_79b9_7f4a_7c15u64;
        for _try in 0..MAX_SEED {
            let seed = splitmix64(&mut rng);
            if let Some(g) = try_peel(keys, ranks, seed, m, modulus) {
                return Ok(Self {
                    n,
                    m,
                    seed,
                    modulus,
                    g: GStore::Ram(g.into_boxed_slice()),
                });
            }
        }
        Err(StoreError::Corrupt("bdz mphf: graph did not peel"))
    }

    pub fn write_to(&self, path: &Path) -> Result<(), StoreError> {
        let GStore::Ram(g) = &self.g else {
            return Err(StoreError::Corrupt("bdz mphf: write requires RAM g"));
        };
        let mut buf = Vec::with_capacity(HEADER_LEN as usize + g.len() * 4);
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&VERSION.to_le_bytes());
        buf.extend_from_slice(&self.n.to_le_bytes());
        buf.extend_from_slice(&self.m.to_le_bytes());
        buf.extend_from_slice(&self.seed.to_le_bytes());
        for &x in g.iter() {
            buf.extend_from_slice(&x.to_le_bytes());
        }
        std::fs::write(path, &buf).map_err(|e| StoreError::io(path, e))?;
        Ok(())
    }

    pub fn write_packed_to(&self, path: &Path) -> Result<(), StoreError> {
        let GStore::Ram(g) = &self.g else {
            return Err(StoreError::Corrupt("bdz mphf: write requires RAM g"));
        };
        let g_bits = g_bits_for_modulus(self.modulus);
        let packed = pack_g(g, g_bits);
        let mut buf = Vec::with_capacity(HEADER_LEN2 as usize + packed.len());
        buf.extend_from_slice(MAGIC2);
        buf.extend_from_slice(&VERSION.to_le_bytes());
        buf.extend_from_slice(&self.n.to_le_bytes());
        buf.extend_from_slice(&self.m.to_le_bytes());
        buf.extend_from_slice(&self.seed.to_le_bytes());
        buf.extend_from_slice(&self.modulus.to_le_bytes());
        buf.extend_from_slice(&g_bits.to_le_bytes());
        buf.extend_from_slice(&packed);
        std::fs::write(path, &buf).map_err(|e| StoreError::io(path, e))?;
        Ok(())
    }

    pub fn read_from(path: &Path) -> Result<Self, StoreError> {
        let file = File::open(path).map_err(|e| StoreError::io(path, e))?;
        let mut hdr = [0u8; HEADER_LEN as usize];
        pread_exact(&file, path, 0, &mut hdr)?;
        if &hdr[0..4] != MAGIC {
            return Err(StoreError::Corrupt("bdz mphf: bad magic"));
        }
        let ver = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
        if ver != VERSION {
            return Err(StoreError::Corrupt("bdz mphf: bad version"));
        }
        let n = u32::from_le_bytes(hdr[8..12].try_into().unwrap());
        let m = u32::from_le_bytes(hdr[12..16].try_into().unwrap());
        let seed = u64::from_le_bytes(hdr[16..24].try_into().unwrap());
        if n == 0 {
            return Ok(Self {
                n: 0,
                m: 0,
                seed: 0,
                modulus: 0,
                g: GStore::Ram(Box::new([])),
            });
        }
        let n_words = if n == 1 { 1 } else { m };
        let g_bytes = n_words as u64 * 4;
        let meta = file.metadata().map_err(|e| StoreError::io(path, e))?;
        if meta.len() < HEADER_LEN + g_bytes {
            return Err(StoreError::Corrupt("bdz mphf: g length"));
        }
        Ok(Self {
            n,
            m: n_words,
            seed,
            modulus: n,
            g: GStore::Fd {
                file,
                path: path.to_path_buf(),
                off: HEADER_LEN,
                n_bytes: g_bytes,
                g_bits: G_BITS_WORDS,
                page_preads: AtomicU64::new(0),
            },
        })
    }

    pub fn read_packed_from(path: &Path) -> Result<Self, StoreError> {
        let file = File::open(path).map_err(|e| StoreError::io(path, e))?;
        let mut hdr = [0u8; HEADER_LEN2 as usize];
        pread_exact(&file, path, 0, &mut hdr)?;
        if &hdr[0..4] != MAGIC2 {
            return Err(StoreError::Corrupt("bdz mphf: bad magic"));
        }
        let ver = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
        if ver != VERSION {
            return Err(StoreError::Corrupt("bdz mphf: bad version"));
        }
        let n = u32::from_le_bytes(hdr[8..12].try_into().unwrap());
        let m = u32::from_le_bytes(hdr[12..16].try_into().unwrap());
        let seed = u64::from_le_bytes(hdr[16..24].try_into().unwrap());
        let modulus = u32::from_le_bytes(hdr[24..28].try_into().unwrap());
        let g_bits = u32::from_le_bytes(hdr[28..32].try_into().unwrap());
        if g_bits == 0 || g_bits > 32 {
            return Err(StoreError::Corrupt("bdz mphf: g_bits"));
        }
        if n == 0 {
            return Ok(Self {
                n: 0,
                m: 0,
                seed: 0,
                modulus,
                g: GStore::Ram(Box::new([])),
            });
        }
        let n_verts = if n == 1 { 1 } else { m };
        let n_bytes = packed_g_bytes(n_verts, g_bits);
        let meta = file.metadata().map_err(|e| StoreError::io(path, e))?;
        if meta.len() < HEADER_LEN2 + n_bytes {
            return Err(StoreError::Corrupt("bdz mphf: g length"));
        }
        Ok(Self {
            n,
            m: n_verts,
            seed,
            modulus,
            g: GStore::Fd {
                file,
                path: path.to_path_buf(),
                off: HEADER_LEN2,
                n_bytes,
                g_bits,
                page_preads: AtomicU64::new(0),
            },
        })
    }
}

fn pread_exact(file: &File, path: &Path, offset: u64, buf: &mut [u8]) -> Result<(), StoreError> {
    let h = IoHandle::from_file(file);
    let mut done = 0usize;
    while done < buf.len() {
        let n = h.pread(offset + done as u64, &mut buf[done..]);
        if n < 0 {
            return Err(StoreError::io(path, std::io::Error::from_raw_os_error(-n)));
        }
        if n == 0 {
            return Err(StoreError::Corrupt("bdz mphf: short pread"));
        }
        done += n as usize;
    }
    Ok(())
}

fn g_page_need(n_bytes: u64, page: u32) -> usize {
    let page_base = u64::from(page) * G_PAGE_BYTES as u64;
    if page_base >= n_bytes {
        return 0;
    }
    ((n_bytes - page_base) as usize).min(G_PAGE_BYTES)
}

fn load_g_page(
    file: &File,
    path: &Path,
    g_off: u64,
    n_bytes: u64,
    page: u32,
    buf: &mut [u8; G_PAGE_BYTES],
) -> Result<usize, StoreError> {
    let need = g_page_need(n_bytes, page);
    if need == 0 {
        return Ok(0);
    }
    pread_exact(
        file,
        path,
        g_off + u64::from(page) * G_PAGE_BYTES as u64,
        &mut buf[..need],
    )?;
    Ok(need)
}

fn stream_or_load_pages(
    file: &File,
    path: &Path,
    off: u64,
    n_bytes: u64,
    page_ids: &[u32],
    page_preads: &AtomicU64,
    ctx: &mut crate::IoCtx<'_>,
    fill: &mut impl FnMut(u32, &[u8]),
) -> Result<(), StoreError> {
    match ctx.session() {
        Some(session) => stream_g_pages(
            session,
            file,
            path,
            off,
            n_bytes,
            page_ids,
            page_preads,
            fill,
        ),
        None => {
            for &page in page_ids {
                let mut buf = [0u8; G_PAGE_BYTES];
                let n = load_g_page(file, path, off, n_bytes, page, &mut buf)?;
                page_preads.fetch_add(1, Ordering::Relaxed);
                fill(page, &buf[..n]);
            }
            Ok(())
        }
    }
}

fn stream_g_pages(
    session: &mut crate::uring_session::UringSession,
    file: &File,
    path: &Path,
    g_off: u64,
    n_bytes: u64,
    page_ids: &[u32],
    page_preads: &AtomicU64,
    fill: &mut impl FnMut(u32, &[u8]),
) -> Result<(), StoreError> {
    let n_pages = page_ids.len();
    if n_pages == 0 {
        return Ok(());
    }
    let fd = IoHandle::from_file(file);
    let pool_n = (crate::uring_session::DEFAULT_ENTRIES as usize)
        .min(n_pages)
        .max(1);
    let mut bufs: Vec<[u8; G_PAGE_BYTES]> = vec![[0u8; G_PAGE_BYTES]; pool_n];
    let mut slot_page: Vec<Option<usize>> = vec![None; pool_n];
    let mut free_slots: Vec<usize> = (0..pool_n).collect();
    let mut next_page = 0usize;
    let mut in_flight = 0usize;
    session.begin_batch()?;
    let epoch = session.epoch();
    let run = (|| -> Result<(), StoreError> {
        loop {
            while next_page < n_pages
                && !free_slots.is_empty()
                && session.free_sq() > 0
                && in_flight < pool_n
            {
                let slot = free_slots.pop().unwrap();
                let pi = next_page;
                next_page += 1;
                let page = page_ids[pi];
                let need = g_page_need(n_bytes, page);
                if need == 0 {
                    free_slots.push(slot);
                    continue;
                }
                let off = g_off + u64::from(page) * G_PAGE_BYTES as u64;
                let buf = &mut bufs[slot][..need];
                buf.fill(0);
                let ud = crate::uring_session::pack_ud(
                    crate::uring_session::KIND_MPHF_G,
                    epoch,
                    slot as u32,
                );
                session.push_pread_flags(fd, off, buf, ud, 0)?;
                slot_page[slot] = Some(pi);
                in_flight += 1;
            }
            session.sync_submission();
            let _ = session.submit();
            if in_flight == 0 {
                break;
            }
            let mut cqes = session.harvest_ready()?;
            if cqes.is_empty() {
                session.submit_and_wait_one()?;
                cqes = session.harvest_ready()?;
                if cqes.is_empty() {
                    session.poison();
                    return Err(StoreError::Corrupt("invariant: io_uring wait timeout"));
                }
            }
            for (ud, res) in cqes {
                let (kind, ep, slot) = crate::uring_session::unpack_ud(ud);
                let slot = slot as usize;
                if kind != crate::uring_session::KIND_MPHF_G
                    || ep != epoch
                    || slot >= pool_n
                    || slot_page[slot].is_none()
                {
                    session.poison();
                    return Err(StoreError::Corrupt("invariant: io_uring leftover cqe"));
                }
                in_flight = in_flight.saturating_sub(1);
                let pi = slot_page[slot].take().unwrap();
                let page = page_ids[pi];
                let need = g_page_need(n_bytes, page);
                if res < 0 {
                    return Err(StoreError::io(
                        path,
                        std::io::Error::from_raw_os_error(-res),
                    ));
                }
                let mut n = res as usize;
                if n < need {
                    load_g_page(file, path, g_off, n_bytes, page, &mut bufs[slot])?;
                    n = need;
                }
                n = n.min(need);
                page_preads.fetch_add(1, Ordering::Relaxed);
                fill(page, &bufs[slot][..n]);
                free_slots.push(slot);
            }
        }
        Ok(())
    })();
    session.drain_all()?;
    run
}

fn g_bits_for_modulus(modulus: u32) -> u32 {
    if modulus <= 1 {
        return 1;
    }
    32 - (modulus - 1).leading_zeros()
}

fn packed_g_bytes(n_verts: u32, g_bits: u32) -> u64 {
    let bits = u64::from(n_verts) * u64::from(g_bits);
    bits.div_ceil(8)
}

fn pack_g(g: &[u32], g_bits: u32) -> Vec<u8> {
    let mut out = vec![0u8; packed_g_bytes(g.len() as u32, g_bits) as usize];
    let mask = if g_bits == 32 {
        u32::MAX
    } else {
        (1u32 << g_bits) - 1
    };
    for (v, &val) in g.iter().enumerate() {
        let start = u64::from(v as u32) * u64::from(g_bits);
        let bits = val & mask;
        for i in 0..g_bits {
            if bits & (1 << i) != 0 {
                let bit = start + u64::from(i);
                let byte = (bit / 8) as usize;
                let rem = (bit % 8) as u32;
                out[byte] |= 1 << rem;
            }
        }
    }
    out
}

fn unpack_g_at(buf: &[u8], v: u32, g_bits: u32) -> u32 {
    let start = u64::from(v) * u64::from(g_bits);
    let mut val = 0u32;
    for i in 0..g_bits {
        let bit = start + u64::from(i);
        let byte = (bit / 8) as usize;
        if byte >= buf.len() {
            break;
        }
        let rem = (bit % 8) as u32;
        if buf[byte] & (1 << rem) != 0 {
            val |= 1 << i;
        }
    }
    val
}

fn unpack_g_from_pages(pages: &[(u32, Vec<u8>)], v: u32, g_bits: u32) -> u32 {
    let start = u64::from(v) * u64::from(g_bits);
    let mut val = 0u32;
    for i in 0..g_bits {
        let bit = start + u64::from(i);
        let byte_i = bit / 8;
        let page = (byte_i / G_PAGE_BYTES as u64) as u32;
        let off = (byte_i % G_PAGE_BYTES as u64) as usize;
        let rem = (bit % 8) as u32;
        let Some((_, buf)) = pages.iter().find(|(p, _)| *p == page) else {
            continue;
        };
        if off < buf.len() && buf[off] & (1 << rem) != 0 {
            val |= 1 << i;
        }
    }
    val
}

fn vertex_pages(v: u32, g_bits: u32) -> impl Iterator<Item = u32> {
    let start = u64::from(v) * u64::from(g_bits);
    let end = start + u64::from(g_bits.saturating_sub(1));
    let p0 = (start / 8 / G_PAGE_BYTES as u64) as u32;
    let p1 = (end / 8 / G_PAGE_BYTES as u64) as u32;
    p0..=p1
}

#[cfg(test)]
fn vertex_straddles_page(v: u32, g_bits: u32) -> bool {
    let mut it = vertex_pages(v, g_bits);
    let a = it.next();
    let b = it.next();
    a.is_some() && b.is_some()
}

fn try_peel(keys: &[u64], ranks: &[u32], seed: u64, m: u32, modulus: u32) -> Option<Vec<u32>> {
    let m_us = m as usize;
    let mut edges = vec![[0u32; 3]; keys.len()];
    let mut deg = vec![0u32; m_us];
    for (i, &k) in keys.iter().enumerate() {
        let h = hash3(k, seed, m);
        edges[i] = h;
        for v in h {
            deg[v as usize] = deg[v as usize].saturating_add(1);
        }
    }
    let mut off = vec![0u32; m_us + 1];
    for i in 0..m_us {
        off[i + 1] = off[i].saturating_add(deg[i]);
    }
    let mut adj = vec![0u32; off[m_us] as usize];
    let mut wr = off[..m_us].to_vec();
    for (i, h) in edges.iter().enumerate() {
        for &v in h {
            let slot = wr[v as usize] as usize;
            adj[slot] = i as u32;
            wr[v as usize] = wr[v as usize].saturating_add(1);
        }
    }
    let mut live = vec![true; keys.len()];
    let mut q: Vec<u32> = Vec::new();
    for (v, &d) in deg.iter().enumerate() {
        if d == 1 {
            q.push(v as u32);
        }
    }
    let mut order: Vec<(u32, u32)> = Vec::with_capacity(keys.len());
    let mut qi = 0usize;
    while qi < q.len() {
        let v = q[qi];
        qi += 1;
        if deg[v as usize] != 1 {
            continue;
        }
        let lo = off[v as usize] as usize;
        let hi = off[v as usize + 1] as usize;
        let Some(&ei) = adj[lo..hi].iter().find(|&&e| live[e as usize]) else {
            continue;
        };
        live[ei as usize] = false;
        order.push((ei, v));
        for u in edges[ei as usize] {
            deg[u as usize] = deg[u as usize].saturating_sub(1);
            if deg[u as usize] == 1 {
                q.push(u);
            }
        }
    }
    if order.len() != keys.len() {
        return None;
    }
    let mut g = vec![0u32; m as usize];
    let mut assigned = vec![false; m as usize];
    for &(ei, v) in order.iter().rev() {
        let h = edges[ei as usize];
        let mut s = 0u32;
        for u in h {
            if assigned[u as usize] {
                s = s.wrapping_add(g[u as usize]);
            }
        }
        let rank = ranks[ei as usize];
        g[v as usize] = (rank + modulus - (s % modulus)) % modulus;
        assigned[v as usize] = true;
    }
    Some(g)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bdz_injective_10k() {
        let keys: Vec<u64> = (0..10_000u64)
            .map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(7))
            .collect();
        let f = BdzMphf::build(&keys).unwrap();
        let mut seen = vec![false; keys.len()];
        for &k in &keys {
            let i = f.index(k).unwrap() as usize;
            assert!(i < keys.len());
            assert!(!seen[i], "collision at {i}");
            seen[i] = true;
        }
        assert!(seen.iter().all(|&b| b));
        let miss = f.index(0xDEAD_BEEF_u64).unwrap();
        assert!(miss < keys.len() as u32);
    }

    #[test]
    fn bdz_injective_2k() {
        let keys: Vec<u64> = (0..2_000u64)
            .map(|i| i.wrapping_mul(0xD1B5_4A32_D192_ED03).wrapping_add(11))
            .collect();
        let f = BdzMphf::build(&keys).unwrap();
        let mut seen = vec![false; keys.len()];
        for &k in &keys {
            let i = f.index(k).unwrap() as usize;
            assert!(i < keys.len());
            assert!(!seen[i], "collision at {i}");
            seen[i] = true;
        }
        assert!(seen.iter().all(|&b| b));
    }

    #[test]
    fn bdz_empty_and_one() {
        let z = BdzMphf::build(&[]).unwrap();
        assert_eq!(z.n(), 0);
        let one = BdzMphf::build(&[42]).unwrap();
        assert_eq!(one.index(42).unwrap(), 0);
        assert_eq!(one.index(99).unwrap(), 0);
    }

    #[test]
    fn bdz_roundtrip_file() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-bdz-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let keys: Vec<u64> = (0..200u64).map(|i| i * 17 + 3).collect();
        let f = BdzMphf::build(&keys).unwrap();
        let p = dir.join("t.mphf");
        f.write_to(&p).unwrap();
        assert_eq!(&std::fs::read(&p).unwrap()[0..4], b"BDZ1");
        let g = BdzMphf::read_from(&p).unwrap();
        for &k in &keys {
            assert_eq!(f.index(k).unwrap(), g.index(k).unwrap());
        }
        assert_eq!(g.g_bytes_resident(), 0, "open must not retain the g array");
        assert_eq!(g.g_bytes(), f.g_bytes());
        let miss = g.index(0xDEAD_BEEF_u64).unwrap();
        assert!(miss < keys.len() as u32);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn bdz_open_matches_ram_index_without_g_heap() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-bdz-fd-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let keys: Vec<u64> = (0..10_000u64)
            .map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(7))
            .collect();
        let ram = BdzMphf::build(&keys).unwrap();
        assert!(ram.g_bytes_resident() > 0);
        let p = dir.join("t.mphf");
        ram.write_to(&p).unwrap();
        let fd = BdzMphf::read_from(&p).unwrap();
        assert_eq!(fd.g_bytes_resident(), 0);
        for &k in &keys {
            assert_eq!(ram.index(k).unwrap(), fd.index(k).unwrap());
        }
        let miss_k = 0xDEAD_BEEF_u64;
        assert_eq!(ram.index(miss_k).unwrap(), fd.index(miss_k).unwrap());
        assert!(fd.index(miss_k).unwrap() < keys.len() as u32);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn assigned_peel_index_is_rel_minus_one() {
        let keys = [10u64, 20, 30, 40];
        let rels = [3u32, 1, 4, 2];
        let values: Vec<u32> = rels.iter().map(|r| r - 1).collect();
        let f = BdzMphf::build_assigned(&keys, &values, 4).unwrap();
        assert_eq!(f.modulus(), 4);
        for (&k, &rel) in keys.iter().zip(rels.iter()) {
            assert_eq!(f.index(k).unwrap(), rel - 1);
        }
        let miss = f.index(0xDEAD_BEEF_u64).unwrap();
        assert!(miss < 4);
    }

    #[test]
    fn assigned_peel_modulus_hole_and_bip30_newest() {
        let keys = [1u64, 2, 3];
        let values = [0u32, 2, 3];
        let modulus = 4u32;
        let f = BdzMphf::build_assigned(&keys, &values, modulus).unwrap();
        assert_eq!(f.n(), 3);
        assert_eq!(f.modulus(), modulus);
        assert_eq!(f.index(1).unwrap(), 0);
        assert_eq!(f.index(2).unwrap(), 2);
        assert_eq!(f.index(3).unwrap(), 3);
        let miss = f.index(99).unwrap();
        assert!(miss < modulus);

        let newest_key = 7u64;
        let newest_rel = 5u32;
        let f2 = BdzMphf::build_assigned(&[newest_key, 8], &[newest_rel - 1, 0], 5).unwrap();
        assert_eq!(f2.index(newest_key).unwrap(), newest_rel - 1);
        assert_eq!(f2.index(8).unwrap(), 0);
        assert!(f2.index(123).unwrap() < 5);
    }

    #[test]
    fn g_bits_for_modulus_width() {
        assert_eq!(g_bits_for_modulus(1), 1);
        assert_eq!(g_bits_for_modulus(2), 1);
        assert_eq!(g_bits_for_modulus(1 << 24), 24);
        assert_eq!(g_bits_for_modulus(1 << 25), 25);
    }

    #[test]
    fn pack_g_roundtrip_bits() {
        for g_bits in [1u32, 2, 24, 25] {
            let g: Vec<u32> = (0..64).map(|i| i % (1u32 << (g_bits.min(5)))).collect();
            let packed = pack_g(&g, g_bits);
            for (v, &want) in g.iter().enumerate() {
                assert_eq!(
                    unpack_g_at(&packed, v as u32, g_bits),
                    want,
                    "g_bits={g_bits} v={v}"
                );
            }
        }
    }

    #[test]
    fn assigned_packed_fd_matches_ram_and_straddles() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-bdz2-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let n = 1_200u32;
        let modulus = 1u32 << 25;
        let keys: Vec<u64> = (0..n as u64)
            .map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(13))
            .collect();
        let values: Vec<u32> = (0..n).map(|i| i * 17 + 3).collect();
        let ram = BdzMphf::build_assigned(&keys, &values, modulus).unwrap();
        let p = dir.join("t.mphf");
        ram.write_packed_to(&p).unwrap();
        let raw = std::fs::read(&p).unwrap();
        assert_eq!(&raw[0..4], b"BDZ2");
        let fd = BdzMphf::read_packed_from(&p).unwrap();
        assert_eq!(fd.g_bytes_resident(), 0);
        assert_eq!(fd.modulus(), modulus);
        for &k in &keys {
            assert_eq!(ram.index(k).unwrap(), fd.index(k).unwrap());
        }
        let miss = 0xDEAD_BEEF_u64;
        assert_eq!(ram.index(miss).unwrap(), fd.index(miss).unwrap());
        assert!(fd.index(miss).unwrap() < modulus);

        let mut straddle_key = None;
        for &k in &keys {
            let verts = ram.vertices(k);
            let g_bits = g_bits_for_modulus(modulus);
            if verts.iter().any(|&v| vertex_straddles_page(v, g_bits)) {
                straddle_key = Some(k);
                break;
            }
        }
        let k = straddle_key.expect("expected a page-straddling vertex at 25-bit width");
        let verts = fd.vertices(k);
        let g_bits = g_bits_for_modulus(modulus);
        let mut pages: Vec<u32> = verts
            .iter()
            .copied()
            .flat_map(|v| vertex_pages(v, g_bits))
            .collect();
        pages.sort_unstable();
        pages.dedup();
        let _ = fd.take_g_page_preads();
        assert_eq!(fd.index(k).unwrap(), ram.index(k).unwrap());
        assert_eq!(fd.take_g_page_preads(), pages.len() as u64);
        assert!(
            pages.len() >= 2,
            "straddle vertex must include both page sides"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
