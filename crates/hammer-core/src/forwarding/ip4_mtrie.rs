use std::net::Ipv4Addr;

use ipnet::Ipv4Net;

pub use crate::ds::MtrieValue as Ip4MtrieValue;
use crate::ds::{Mtrie, MtrieRoute};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ip4MtrieRoute<V> {
    pub prefix: Ipv4Net,
    pub value: V,
}

impl<V> Ip4MtrieRoute<V> {
    #[inline(always)]
    pub fn new(prefix: Ipv4Net, value: V) -> Self {
        Self { prefix, value }
    }
}

#[derive(Debug, Clone)]
pub struct Ip4Mtrie<V: Ip4MtrieValue> {
    inner: Mtrie<V>,
}

impl<V: Ip4MtrieValue> Ip4Mtrie<V> {
    #[inline]
    pub fn empty() -> Self {
        Self {
            inner: Mtrie::empty(),
        }
    }

    #[inline]
    pub fn from_routes(routes: impl IntoIterator<Item = Ip4MtrieRoute<V>>) -> Self {
        Self {
            inner: Mtrie::from_routes(routes.into_iter().map(|route| {
                MtrieRoute::new(
                    ip4_to_key(route.prefix.addr()),
                    route.prefix.prefix_len(),
                    route.value,
                )
            })),
        }
    }

    #[inline]
    pub fn insert(&mut self, prefix: Ipv4Net, value: V) {
        self.inner
            .insert(ip4_to_key(prefix.addr()), prefix.prefix_len(), value);
    }

    #[inline(always)]
    pub fn lookup(&self, destination: Ipv4Addr) -> Option<V> {
        self.inner.lookup(ip4_to_key(destination))
    }

    #[inline(always)]
    pub fn prefetch(&self, destination: Ipv4Addr) {
        self.inner.prefetch(ip4_to_key(destination));
    }

    #[inline(always)]
    pub fn ply_count(&self) -> usize {
        self.inner.ply_count()
    }
}

#[inline(always)]
fn ip4_to_key(addr: Ipv4Addr) -> u32 {
    u32::from(addr)
}
