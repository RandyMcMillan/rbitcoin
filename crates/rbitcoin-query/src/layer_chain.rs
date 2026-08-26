//! Newest-first Arc layer list. live_union and RecentCreates share splice/prepend.

use arc_swap::{ArcSwapOption, Guard};
use std::sync::Arc;

/// One immutable layer plus the older chain. `hits` is never cloned on splice.
#[derive(Debug)]
pub struct ChainLayer<Meta, Hits> {
    pub lo: u32,
    pub hi: u32,
    pub meta: Meta,
    pub hits: Arc<Hits>,
    pub older: Option<Arc<ChainLayer<Meta, Hits>>>,
}

impl<Meta, Hits> ChainLayer<Meta, Hits> {
    pub fn prepend(
        older: Option<Arc<Self>>,
        lo: u32,
        hi: u32,
        meta: Meta,
        hits: Arc<Hits>,
    ) -> Arc<Self> {
        Arc::new(Self {
            lo,
            hi,
            meta,
            hits,
            older,
        })
    }

    /// Newest-first. `f` returns Some to stop.
    pub fn walk<R>(&self, mut f: impl FnMut(&Self) -> Option<R>) -> Option<R> {
        let mut layer = self;
        loop {
            if let Some(v) = f(layer) {
                return Some(v);
            }
            match layer.older.as_deref() {
                Some(older) => layer = older,
                None => return None,
            }
        }
    }
}

/// Keep nodes where `keep` is true. Reuse the node Arc when `older` is unchanged;
/// otherwise a new node with the **same** `hits` Arc.
pub fn splice_kept<Meta: Clone, Hits>(
    head: Option<Arc<ChainLayer<Meta, Hits>>>,
    keep: impl Fn(&ChainLayer<Meta, Hits>) -> bool,
) -> Option<Arc<ChainLayer<Meta, Hits>>> {
    let mut nodes = Vec::new();
    let mut cur = head;
    while let Some(n) = cur {
        let older = n.older.clone();
        nodes.push(n);
        cur = older;
    }
    let mut new_head: Option<Arc<ChainLayer<Meta, Hits>>> = None;
    for n in nodes.into_iter().rev() {
        if !keep(&n) {
            continue;
        }
        let older_ok = match (n.older.as_ref(), new_head.as_ref()) {
            (None, None) => true,
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            _ => false,
        };
        if older_ok {
            new_head = Some(n);
        } else {
            new_head = Some(ChainLayer::prepend(
                new_head,
                n.lo,
                n.hi,
                n.meta.clone(),
                Arc::clone(&n.hits),
            ));
        }
    }
    new_head
}

fn option_arc_eq<T>(a: &Option<Arc<T>>, b: &Option<Arc<T>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => Arc::ptr_eq(x, y),
        _ => false,
    }
}

/// Store `next` when `slot` still holds `expected`. Lost CAS returns the live head.
pub fn cas_head<Meta, Hits>(
    slot: &ArcSwapOption<ChainLayer<Meta, Hits>>,
    expected: &Option<Arc<ChainLayer<Meta, Hits>>>,
    next: Option<Arc<ChainLayer<Meta, Hits>>>,
) -> Result<(), Option<Arc<ChainLayer<Meta, Hits>>>> {
    let prev = Guard::into_inner(slot.compare_and_swap(expected, next));
    if option_arc_eq(expected, &prev) {
        Ok(())
    } else {
        Err(prev)
    }
}

/// Apply `f` to the live head until [`cas_head`] lands.
pub fn rcu_head<Meta, Hits>(
    slot: &ArcSwapOption<ChainLayer<Meta, Hits>>,
    mut f: impl FnMut(&Option<Arc<ChainLayer<Meta, Hits>>>) -> Option<Arc<ChainLayer<Meta, Hits>>>,
) {
    let mut cur = slot.load_full();
    loop {
        let next = f(&cur);
        match cas_head(slot, &cur, next) {
            Ok(()) => return,
            Err(live) => cur = live,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arc_swap::ArcSwapOption;

    #[test]
    fn cas_head_rejects_stale_expected() {
        let slot: ArcSwapOption<ChainLayer<u32, u8>> = ArcSwapOption::empty();
        let a = ChainLayer::prepend(None, 10, 10, 10, Arc::new(1u8));
        assert!(cas_head(&slot, &None, Some(Arc::clone(&a))).is_ok());
        let stale = Some(Arc::clone(&a));
        rcu_head(&slot, |cur| splice_kept(cur.clone(), |l| l.lo < 10));
        assert!(slot.load_full().is_none());
        let c = ChainLayer::prepend(stale.clone(), 12, 12, 12, Arc::new(2u8));
        assert!(
            cas_head(&slot, &stale, Some(c)).is_err(),
            "CAS against a dropped head must fail"
        );
        assert!(slot.load_full().is_none(), "stale prepend must not restore");
        rcu_head(&slot, |cur| {
            Some(ChainLayer::prepend(cur.clone(), 12, 12, 12, Arc::new(2u8)))
        });
        let head = slot.load_full().expect("retry");
        assert_eq!(*head.hits, 2);
        assert!(head.older.is_none());
    }
}
