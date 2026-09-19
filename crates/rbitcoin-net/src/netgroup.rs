//! Outbound / inbound netgroup key: asmap ASN, or IPv4 `/16` / IPv6 `/32`.

use crate::asmap::AsMap;
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};

/// Stable neighborhood key. With asmap, the ASN (IPv4 and IPv6 that map to
/// the same ASN share a key). Without, Core-style prefix groups.
pub fn netgroup(addr: SocketAddr, asmap: Option<&AsMap>) -> u64 {
    if let Some(m) = asmap {
        return u64::from(m.mapped_as(addr.ip()));
    }
    prefix_group(addr.ip())
}

/// Walk `ranked` and prefer addrs whose group is not in `occupied` or already
/// picked. Once every unused-group candidate is taken, fill remaining slots
/// in ranked order (duplicates / occupied groups).
pub fn select_diverse(
    ranked: &[SocketAddr],
    max: usize,
    occupied_groups: &HashSet<u64>,
    mut group_of: impl FnMut(SocketAddr) -> u64,
) -> Vec<SocketAddr> {
    if max == 0 || ranked.is_empty() {
        return Vec::new();
    }
    let mut picked = Vec::new();
    let mut used = occupied_groups.clone();
    for &addr in ranked {
        if picked.len() >= max {
            break;
        }
        let g = group_of(addr);
        if used.contains(&g) {
            continue;
        }
        used.insert(g);
        picked.push(addr);
    }
    if picked.len() < max {
        for &addr in ranked {
            if picked.len() >= max {
                break;
            }
            if picked.contains(&addr) {
                continue;
            }
            picked.push(addr);
        }
    }
    picked
}

fn prefix_group(ip: IpAddr) -> u64 {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            (u64::from(o[0]) << 8) | u64::from(o[1])
        }
        IpAddr::V6(v6) => {
            let o = v6.octets();
            u64::from(u32::from_be_bytes([o[0], o[1], o[2], o[3]]))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asmap::{two_prefix_asmap_bytes, AsMap};
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn v4(a: u8, b: u8, c: u8, d: u8) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(a, b, c, d)), 8333)
    }

    #[test]
    fn netgroup_prefix_ipv4_slash16() {
        assert_eq!(netgroup(v4(1, 2, 3, 4), None), netgroup(v4(1, 2, 9, 9), None));
        assert_ne!(netgroup(v4(1, 2, 3, 4), None), netgroup(v4(1, 3, 0, 1), None));
    }

    #[test]
    fn netgroup_prefix_ipv6_slash32() {
        let a = SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)), 1);
        let b = SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 1, 0, 0, 0, 0, 1)), 1);
        let c = SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb9, 0, 0, 0, 0, 0, 1)), 1);
        assert_eq!(netgroup(a, None), netgroup(b, None));
        assert_ne!(netgroup(a, None), netgroup(c, None));
    }

    #[test]
    fn netgroup_asmap_two_asns() {
        let m = AsMap::from_bytes(two_prefix_asmap_bytes()).unwrap();
        let a = v4(1, 2, 3, 4);
        let b = v4(1, 2, 9, 9);
        let c = v4(3, 4, 0, 1);
        assert_eq!(netgroup(a, Some(&m)), netgroup(b, Some(&m)));
        assert_ne!(netgroup(a, Some(&m)), netgroup(c, Some(&m)));
        let v6 =
            SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0x0102, 0x0304)), 1);
        assert_eq!(netgroup(a, Some(&m)), netgroup(v6, Some(&m)));
    }

    fn group_octet0(a: SocketAddr) -> u64 {
        match a.ip() {
            IpAddr::V4(v) => u64::from(v.octets()[0]),
            IpAddr::V6(_) => 0,
        }
    }

    #[test]
    fn select_diverse_skips_occupied_asn() {
        use std::collections::HashSet;
        let m = AsMap::from_bytes(two_prefix_asmap_bytes()).unwrap();
        let ranked = vec![v4(1, 2, 0, 1), v4(3, 4, 0, 1), v4(1, 2, 0, 2)];
        let mut occupied = HashSet::new();
        occupied.insert(netgroup(v4(1, 2, 0, 9), Some(&m)));
        let got = select_diverse(&ranked, 2, &occupied, |a| netgroup(a, Some(&m)));
        assert_eq!(got[0], v4(3, 4, 0, 1));
        assert_eq!(got[1], v4(1, 2, 0, 1));
    }

    #[test]
    fn select_diverse_skips_occupied_then_fills() {
        use std::collections::HashSet;
        let ranked: Vec<SocketAddr> = (1..=4).map(|i| v4(i, 0, 0, 1)).collect();
        let mut occupied = HashSet::new();
        occupied.insert(1);
        let got = select_diverse(&ranked, 3, &occupied, group_octet0);
        assert_eq!(got[0], v4(2, 0, 0, 1));
        assert_eq!(got[1], v4(3, 0, 0, 1));
        assert_eq!(got[2], v4(4, 0, 0, 1));
        assert!(!got.contains(&v4(1, 0, 0, 1)));
    }

    #[test]
    fn select_diverse_thin_book_still_fills_max() {
        use std::collections::HashSet;
        let ranked = vec![v4(1, 0, 0, 1), v4(1, 0, 0, 2), v4(1, 0, 0, 3)];
        let occupied = HashSet::new();
        let got = select_diverse(&ranked, 3, &occupied, group_octet0);
        assert_eq!(got.len(), 3);
        assert_eq!(got, ranked);
    }

    #[test]
    fn select_diverse_four_groups_max_eight() {
        use std::collections::HashSet;
        let mut ranked = Vec::new();
        for g in 1u8..=4 {
            ranked.push(v4(g, 0, 0, 1));
            ranked.push(v4(g, 0, 0, 2));
        }
        let got = select_diverse(&ranked, 8, &HashSet::new(), group_octet0);
        assert_eq!(got.len(), 8);
        let first4: Vec<u8> = got[..4]
            .iter()
            .map(|a| match a.ip() {
                IpAddr::V4(v) => v.octets()[0],
                IpAddr::V6(_) => 0,
            })
            .collect();
        assert_eq!(first4, vec![1, 2, 3, 4]);
    }
}
