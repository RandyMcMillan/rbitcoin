//! In-tree BDZ 3-graph MPHF (`u64` keys → `[0, n)`).
//!
//! A key **not** in the set still maps into `[0, n)`. Callers verify identity.

use crate::error::StoreError;
use std::path::Path;

const MAGIC: &[u8; 4] = b"BDZ1";
const VERSION: u32 = 1;
const GAMMA_NUM: u64 = 123;
const GAMMA_DEN: u64 = 100;
const MAX_SEED: u32 = 256;

#[derive(Debug, Clone)]
pub struct BdzMphf {
    n: u32,
    m: u32,
    seed: u64,
    g: Box<[u32]>,
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

    pub fn g_bytes(&self) -> usize {
        self.g.len() * 4
    }

    pub fn index(&self, key: u64) -> u32 {
        if self.n == 0 {
            return 0;
        }
        if self.n == 1 {
            return 0;
        }
        let [a, b, c] = hash3(key, self.seed, self.m);
        (self.g[a as usize]
            .wrapping_add(self.g[b as usize])
            .wrapping_add(self.g[c as usize]))
            % self.n
    }

    pub fn build(keys: &[u64]) -> Result<Self, StoreError> {
        let n = keys.len() as u32;
        if n == 0 {
            return Ok(Self {
                n: 0,
                m: 0,
                seed: 0,
                g: Box::new([]),
            });
        }
        if n == 1 {
            return Ok(Self {
                n: 1,
                m: 1,
                seed: 1,
                g: vec![0u32].into_boxed_slice(),
            });
        }
        let m = ((u64::from(n) * GAMMA_NUM + GAMMA_DEN - 1) / GAMMA_DEN)
            .max(u64::from(n) + 3)
            .max(3) as u32;
        let mut rng = 0x9e37_79b9_7f4a_7c15u64;
        for _try in 0..MAX_SEED {
            let seed = splitmix64(&mut rng);
            if let Some(g) = try_peel(keys, seed, m, n) {
                return Ok(Self {
                    n,
                    m,
                    seed,
                    g: g.into_boxed_slice(),
                });
            }
        }
        Err(StoreError::Corrupt("bdz mphf: graph did not peel"))
    }

    pub fn write_to(&self, path: &Path) -> Result<(), StoreError> {
        let mut buf = Vec::with_capacity(24 + self.g.len() * 4);
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&VERSION.to_le_bytes());
        buf.extend_from_slice(&self.n.to_le_bytes());
        buf.extend_from_slice(&self.m.to_le_bytes());
        buf.extend_from_slice(&self.seed.to_le_bytes());
        for &x in self.g.iter() {
            buf.extend_from_slice(&x.to_le_bytes());
        }
        std::fs::write(path, &buf).map_err(|e| StoreError::io(path, e))?;
        Ok(())
    }

    pub fn read_from(path: &Path) -> Result<Self, StoreError> {
        let buf = std::fs::read(path).map_err(|e| StoreError::io(path, e))?;
        if buf.len() < 24 || &buf[0..4] != MAGIC {
            return Err(StoreError::Corrupt("bdz mphf: bad magic"));
        }
        let ver = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if ver != VERSION {
            return Err(StoreError::Corrupt("bdz mphf: bad version"));
        }
        let n = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let m = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let seed = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        let rest = &buf[24..];
        if n == 0 {
            return Ok(Self {
                n: 0,
                m: 0,
                seed: 0,
                g: Box::new([]),
            });
        }
        let g_bytes = if n == 1 { 4 } else { m as usize * 4 };
        if rest.len() < g_bytes {
            return Err(StoreError::Corrupt("bdz mphf: g length"));
        }
        let mut g = vec![0u32; g_bytes / 4];
        for (i, chunk) in rest[..g_bytes].chunks_exact(4).enumerate() {
            g[i] = u32::from_le_bytes(chunk.try_into().unwrap());
        }
        Ok(Self {
            n,
            m: g.len() as u32,
            seed,
            g: g.into_boxed_slice(),
        })
    }
}

fn try_peel(keys: &[u64], seed: u64, m: u32, n: u32) -> Option<Vec<u32>> {
    let mut edges = vec![[0u32; 3]; keys.len()];
    let mut deg = vec![0u32; m as usize];
    let mut adj: Vec<Vec<u32>> = vec![Vec::new(); m as usize];
    for (i, &k) in keys.iter().enumerate() {
        let h = hash3(k, seed, m);
        edges[i] = h;
        for v in h {
            deg[v as usize] = deg[v as usize].saturating_add(1);
            adj[v as usize].push(i as u32);
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
        let Some(&ei) = adj[v as usize].iter().find(|&&e| live[e as usize]) else {
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
        let rank = ei % n;
        g[v as usize] = (rank + n - (s % n)) % n;
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
            let i = f.index(k) as usize;
            assert!(i < keys.len());
            assert!(!seen[i], "collision at {i}");
            seen[i] = true;
        }
        assert!(seen.iter().all(|&b| b));
        let miss = f.index(0xDEAD_BEEF_u64);
        assert!(miss < keys.len() as u32);
    }

    #[test]
    fn bdz_empty_and_one() {
        let z = BdzMphf::build(&[]).unwrap();
        assert_eq!(z.n(), 0);
        let one = BdzMphf::build(&[42]).unwrap();
        assert_eq!(one.index(42), 0);
        assert_eq!(one.index(99), 0);
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
        let g = BdzMphf::read_from(&p).unwrap();
        for &k in &keys {
            assert_eq!(f.index(k), g.index(k));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
