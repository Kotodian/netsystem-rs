use std::collections::BTreeMap;
use std::net::{Ipv4Addr, Ipv6Addr};

use hammer_service::net::{DpoId, DpoType, FibEntry, FibSource, FibTable, FibTableBackend};
use ipnet::{Ipv4Net, Ipv6Net};

use crate::ip::IpPathFlags;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpFibError {
    PrefixExists,
    PrefixMissing,
    ForwardingDpoRequired,
}

#[derive(Debug, Clone, Copy)]
struct Ip4TrieNode {
    child: [Option<u32>; 2],
    forwarding: Option<DpoId>,
}

impl Default for Ip4TrieNode {
    fn default() -> Self {
        Self {
            child: [None, None],
            forwarding: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Ip4ForwardingTrie {
    nodes: Vec<Ip4TrieNode>,
}

impl Ip4ForwardingTrie {
    fn new() -> Self {
        Self {
            nodes: vec![Ip4TrieNode::default()],
        }
    }

    fn insert(&mut self, prefix: Ipv4Net, forwarding: DpoId) {
        if self.nodes.is_empty() {
            self.nodes.push(Ip4TrieNode::default());
        }
        let mut node = 0usize;
        let address = u32::from(prefix.network());
        for bit in 0..prefix.prefix_len() {
            let branch = ((address >> (31 - bit)) & 1) as usize;
            let child = if let Some(child) = self.nodes[node].child[branch] {
                child as usize
            } else {
                let child = self.nodes.len() as u32;
                self.nodes[node].child[branch] = Some(child);
                self.nodes.push(Ip4TrieNode::default());
                child as usize
            };
            node = child;
        }
        self.nodes[node].forwarding = Some(forwarding);
    }

    fn remove(&mut self, prefix: Ipv4Net) {
        let address = u32::from(prefix.network());
        let mut node = 0usize;
        for bit in 0..prefix.prefix_len() {
            let branch = ((address >> (31 - bit)) & 1) as usize;
            let Some(child) = self.nodes[node].child[branch] else {
                return;
            };
            node = child as usize;
        }
        self.nodes[node].forwarding = None;
    }

    #[inline(always)]
    fn lookup(&self, address: Ipv4Addr) -> Option<DpoId> {
        let mut node = 0usize;
        let mut result = self.nodes.first()?.forwarding;
        let value = u32::from(address);
        for bit in 0..32 {
            let branch = ((value >> (31 - bit)) & 1) as usize;
            let Some(child) = self.nodes[node].child[branch] else {
                break;
            };
            let child = child as usize;
            node = child;
            if self.nodes[node].forwarding.is_some() {
                result = self.nodes[node].forwarding;
            }
        }
        result
    }
}

#[derive(Debug, Clone)]
pub struct Ip4FibBackend {
    entries: BTreeMap<Ipv4Net, u32>,
    forwarding: Ip4ForwardingTrie,
}

impl Default for Ip4FibBackend {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            forwarding: Ip4ForwardingTrie::new(),
        }
    }
}

impl FibTableBackend for Ip4FibBackend {
    type Prefix = Ipv4Net;
    type PacketAddress = Ipv4Addr;
    type NextHop = Ipv4Addr;
    type PathFlags = IpPathFlags;
    type Error = IpFibError;

    fn lookup(&self, prefix: Self::Prefix) -> Option<u32> {
        self.entries.get(&prefix).copied()
    }

    fn lookup_exact(&self, prefix: Self::Prefix) -> Option<u32> {
        self.lookup(prefix)
    }

    fn less_specific(&self, prefix: Self::Prefix) -> Option<(Self::Prefix, u32)> {
        self.entries
            .iter()
            .filter(|(candidate, _)| {
                candidate.prefix_len() < prefix.prefix_len()
                    && candidate.contains(&prefix.network())
            })
            .max_by_key(|(candidate, _)| candidate.prefix_len())
            .map(|(candidate, entry)| (*candidate, *entry))
    }

    fn insert_entry(&mut self, prefix: Self::Prefix, entry: u32) -> Result<(), Self::Error> {
        if self.entries.insert(prefix, entry).is_some() {
            return Err(IpFibError::PrefixExists);
        }
        Ok(())
    }

    fn remove_entry(&mut self, prefix: Self::Prefix, _: u32) -> Result<(), Self::Error> {
        self.entries
            .remove(&prefix)
            .ok_or(IpFibError::PrefixMissing)?;
        self.forwarding.remove(prefix);
        Ok(())
    }

    fn forwarding_lookup(&self, address: Self::PacketAddress) -> Option<DpoId> {
        self.forwarding.lookup(address)
    }

    fn forwarding_update(&mut self, prefix: Self::Prefix, dpo: DpoId) -> Result<(), Self::Error> {
        if dpo.class() != DpoType::LOAD_BALANCE {
            return Err(IpFibError::ForwardingDpoRequired);
        }
        if !self.entries.contains_key(&prefix) {
            return Err(IpFibError::PrefixMissing);
        }
        self.forwarding.insert(prefix, dpo);
        Ok(())
    }

    fn forwarding_remove(
        &mut self,
        prefix: Self::Prefix,
        _old: DpoId,
        cover: Option<(Self::Prefix, DpoId)>,
    ) -> Result<(), Self::Error> {
        self.forwarding.remove(prefix);
        if let Some((cover_prefix, cover_dpo)) = cover {
            if self.entries.contains_key(&cover_prefix) {
                self.forwarding.insert(cover_prefix, cover_dpo);
            }
        }
        Ok(())
    }

    fn project_forwarding(
        &mut self,
        entry: &FibEntry,
        _: FibSource,
    ) -> Result<Option<DpoId>, Self::Error> {
        Ok(entry.forwarding)
    }
}

#[derive(Debug, Clone, Copy)]
struct Ip6TrieNode {
    child: [Option<u32>; 2],
    forwarding: Option<DpoId>,
}

impl Default for Ip6TrieNode {
    fn default() -> Self {
        Self {
            child: [None, None],
            forwarding: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Ip6ForwardingTrie {
    nodes: Vec<Ip6TrieNode>,
}

impl Ip6ForwardingTrie {
    fn new() -> Self {
        Self {
            nodes: vec![Ip6TrieNode::default()],
        }
    }

    fn insert(&mut self, prefix: Ipv6Net, forwarding: DpoId) {
        if self.nodes.is_empty() {
            self.nodes.push(Ip6TrieNode::default());
        }
        let mut node = 0usize;
        let address = u128::from_be_bytes(prefix.network().octets());
        for bit in 0..prefix.prefix_len() {
            let branch = ((address >> (127 - bit)) & 1) as usize;
            let child = if let Some(child) = self.nodes[node].child[branch] {
                child as usize
            } else {
                let child = self.nodes.len() as u32;
                self.nodes[node].child[branch] = Some(child);
                self.nodes.push(Ip6TrieNode::default());
                child as usize
            };
            node = child;
        }
        self.nodes[node].forwarding = Some(forwarding);
    }

    fn remove(&mut self, prefix: Ipv6Net) {
        let address = u128::from_be_bytes(prefix.network().octets());
        let mut node = 0usize;
        for bit in 0..prefix.prefix_len() {
            let branch = ((address >> (127 - bit)) & 1) as usize;
            let Some(child) = self.nodes[node].child[branch] else {
                return;
            };
            node = child as usize;
        }
        self.nodes[node].forwarding = None;
    }

    #[inline(always)]
    fn lookup(&self, address: Ipv6Addr) -> Option<DpoId> {
        let mut node = 0usize;
        let mut result = self.nodes.first()?.forwarding;
        let value = u128::from_be_bytes(address.octets());
        for bit in 0..128 {
            let branch = ((value >> (127 - bit)) & 1) as usize;
            let Some(child) = self.nodes[node].child[branch] else {
                break;
            };
            let child = child as usize;
            node = child;
            if self.nodes[node].forwarding.is_some() {
                result = self.nodes[node].forwarding;
            }
        }
        result
    }
}

#[derive(Debug, Clone)]
pub struct Ip6FibBackend {
    entries: BTreeMap<Ipv6Net, u32>,
    forwarding: Ip6ForwardingTrie,
}

impl Default for Ip6FibBackend {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            forwarding: Ip6ForwardingTrie::new(),
        }
    }
}

impl FibTableBackend for Ip6FibBackend {
    type Prefix = Ipv6Net;
    type PacketAddress = Ipv6Addr;
    type NextHop = Ipv6Addr;
    type PathFlags = IpPathFlags;
    type Error = IpFibError;

    fn lookup(&self, prefix: Self::Prefix) -> Option<u32> {
        self.entries.get(&prefix).copied()
    }
    fn lookup_exact(&self, prefix: Self::Prefix) -> Option<u32> {
        self.lookup(prefix)
    }
    fn less_specific(&self, prefix: Self::Prefix) -> Option<(Self::Prefix, u32)> {
        self.entries
            .iter()
            .filter(|(candidate, _)| {
                candidate.prefix_len() < prefix.prefix_len()
                    && candidate.contains(&prefix.network())
            })
            .max_by_key(|(candidate, _)| candidate.prefix_len())
            .map(|(candidate, entry)| (*candidate, *entry))
    }
    fn insert_entry(&mut self, prefix: Self::Prefix, entry: u32) -> Result<(), Self::Error> {
        if self.entries.insert(prefix, entry).is_some() {
            return Err(IpFibError::PrefixExists);
        }
        Ok(())
    }
    fn remove_entry(&mut self, prefix: Self::Prefix, _: u32) -> Result<(), Self::Error> {
        self.entries
            .remove(&prefix)
            .ok_or(IpFibError::PrefixMissing)?;
        self.forwarding.remove(prefix);
        Ok(())
    }
    fn forwarding_lookup(&self, address: Self::PacketAddress) -> Option<DpoId> {
        self.forwarding.lookup(address)
    }
    fn forwarding_update(&mut self, prefix: Self::Prefix, dpo: DpoId) -> Result<(), Self::Error> {
        if dpo.class() != DpoType::LOAD_BALANCE {
            return Err(IpFibError::ForwardingDpoRequired);
        }
        if !self.entries.contains_key(&prefix) {
            return Err(IpFibError::PrefixMissing);
        }
        self.forwarding.insert(prefix, dpo);
        Ok(())
    }
    fn forwarding_remove(
        &mut self,
        prefix: Self::Prefix,
        _old: DpoId,
        cover: Option<(Self::Prefix, DpoId)>,
    ) -> Result<(), Self::Error> {
        self.forwarding.remove(prefix);
        if let Some((cover_prefix, cover_dpo)) = cover {
            if self.entries.contains_key(&cover_prefix) {
                self.forwarding.insert(cover_prefix, cover_dpo);
            }
        }
        Ok(())
    }
    fn project_forwarding(
        &mut self,
        entry: &FibEntry,
        _: FibSource,
    ) -> Result<Option<DpoId>, Self::Error> {
        Ok(entry.forwarding)
    }
}

pub type Ip4FibTable = FibTable<Ipv4Net, Ip4FibBackend>;
pub type Ip6FibTable = FibTable<Ipv6Net, Ip6FibBackend>;

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_service::net::{DpoProto, DpoType};

    fn dpo(index: u32) -> DpoId {
        DpoId::new(DpoType::LOAD_BALANCE, DpoProto::IP4, index, 1)
    }

    #[test]
    fn ipv4_lpm_selects_longest_prefix_and_restores_cover() {
        let mut table = Ip4FibTable::new(Ip4FibBackend::default());
        let cover = Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 0), 8).unwrap();
        let specific = Ipv4Net::new(Ipv4Addr::new(10, 1, 0, 0), 16).unwrap();
        table.add_route(cover, FibSource::API, dpo(7)).unwrap();
        table.add_route(specific, FibSource::API, dpo(9)).unwrap();
        assert_eq!(
            table.forwarding_lookup(Ipv4Addr::new(10, 1, 2, 3)),
            Some(dpo(9))
        );
        table.remove_route(specific, FibSource::API).unwrap();
        assert_eq!(
            table.forwarding_lookup(Ipv4Addr::new(10, 1, 2, 3)),
            Some(dpo(7))
        );
    }

    #[test]
    fn ipv6_lpm_keeps_masked_prefixes_distinct() {
        let mut table = Ip6FibTable::new(Ip6FibBackend::default());
        let first = Ipv6Net::new(Ipv6Addr::LOCALHOST, 128).unwrap();
        let second = Ipv6Net::new(Ipv6Addr::from([0, 0, 0, 0, 0, 0, 0, 1]), 64).unwrap();
        table.add_route(second, FibSource::API, dpo(3)).unwrap();
        table.add_route(first, FibSource::API, dpo(4)).unwrap();
        assert_eq!(table.forwarding_lookup(Ipv6Addr::LOCALHOST), Some(dpo(4)));
        assert_eq!(table.lookup_exact(second), Some(0));
    }

    #[test]
    fn forwarding_trie_handles_default_and_host_routes() {
        let mut table = Ip4FibTable::new(Ip4FibBackend::default());
        let default = Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).unwrap();
        let host = Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 7), 32).unwrap();
        table.add_route(default, FibSource::API, dpo(1)).unwrap();
        table.add_route(host, FibSource::API, dpo(2)).unwrap();
        assert_eq!(
            table.forwarding_lookup(Ipv4Addr::new(198, 51, 100, 9)),
            Some(dpo(1))
        );
        assert_eq!(
            table.forwarding_lookup(Ipv4Addr::new(192, 0, 2, 7)),
            Some(dpo(2))
        );
    }
}
