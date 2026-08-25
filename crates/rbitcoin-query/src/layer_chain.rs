//! Newest-first Arc layer list. live_union and RecentCreates share splice/prepend.

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
