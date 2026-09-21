use hammer_infra::bihash::{Bihash16x8, Bihash48x8};
use hammer_service::session::SessionTable;

use crate::config::IpSessionTableConfig;

pub(crate) type Ip4SessionTable = SessionTable<Bihash16x8, Bihash16x8>;
pub(crate) type Ip6SessionTable = SessionTable<Bihash48x8, Bihash48x8>;

pub(crate) struct LocalTable {
    ip4: Ip4SessionTable,
    ip6: Ip6SessionTable,
}

pub(crate) enum IpSessionTableHashes {
    Ip4(Ip4SessionTable),
    Ip6(Ip6SessionTable),
    Local(LocalTable),
}

pub(crate) struct IpSessionTable {
    hashes: IpSessionTableHashes,
    appns_indices: Vec<u32>,
}

impl IpSessionTable {
    pub(crate) fn ip4(config: IpSessionTableConfig) -> Self {
        Self {
            hashes: IpSessionTableHashes::Ip4(ip4_hashes(config)),
            appns_indices: Vec::new(),
        }
    }

    pub(crate) fn ip6(config: IpSessionTableConfig) -> Self {
        Self {
            hashes: IpSessionTableHashes::Ip6(ip6_hashes(config)),
            appns_indices: Vec::new(),
        }
    }

    pub(crate) fn local(config: IpSessionTableConfig) -> Self {
        Self {
            hashes: IpSessionTableHashes::Local(LocalTable {
                ip4: ip4_hashes(config),
                ip6: ip6_hashes(config),
            }),
            appns_indices: Vec::new(),
        }
    }

    pub(crate) fn ip4_hashes(&self) -> Option<&Ip4SessionTable> {
        match &self.hashes {
            IpSessionTableHashes::Ip4(table) => Some(table),
            IpSessionTableHashes::Ip6(_) => None,
            IpSessionTableHashes::Local(table) => Some(&table.ip4),
        }
    }

    pub(crate) fn ip6_hashes(&self) -> Option<&Ip6SessionTable> {
        match &self.hashes {
            IpSessionTableHashes::Ip4(_) => None,
            IpSessionTableHashes::Ip6(table) => Some(table),
            IpSessionTableHashes::Local(table) => Some(&table.ip6),
        }
    }

    pub(crate) fn family_matches(&self, ip4: bool) -> bool {
        matches!(
            (&self.hashes, ip4),
            (IpSessionTableHashes::Ip4(_), true) | (IpSessionTableHashes::Ip6(_), false)
        )
    }

    pub(crate) fn is_local(&self) -> bool {
        matches!(&self.hashes, IpSessionTableHashes::Local(_))
    }

    pub(crate) fn appns_indices(&self) -> &[u32] {
        &self.appns_indices
    }

    pub(crate) fn appns_indices_mut(&mut self) -> &mut Vec<u32> {
        &mut self.appns_indices
    }

    pub(crate) fn memory_size(&self) -> u64 {
        match &self.hashes {
            IpSessionTableHashes::Ip4(table) => {
                table.sessions().memory_size() as u64 + table.half_open().memory_size() as u64
            }
            IpSessionTableHashes::Ip6(table) => {
                table.sessions().memory_size() as u64 + table.half_open().memory_size() as u64
            }
            IpSessionTableHashes::Local(table) => {
                table.ip4.sessions().memory_size() as u64
                    + table.ip4.half_open().memory_size() as u64
                    + table.ip6.sessions().memory_size() as u64
                    + table.ip6.half_open().memory_size() as u64
            }
        }
    }
}

fn ip4_hashes(config: IpSessionTableConfig) -> Ip4SessionTable {
    SessionTable::new(
        Bihash16x8::with_memory_size(config.v4_session_buckets(), config.v4_session_memory()),
        Bihash16x8::with_memory_size(config.v4_halfopen_buckets(), config.v4_halfopen_memory()),
    )
}

fn ip6_hashes(config: IpSessionTableConfig) -> Ip6SessionTable {
    SessionTable::new(
        Bihash48x8::with_memory_size(config.v6_session_buckets(), config.v6_session_memory()),
        Bihash48x8::with_memory_size(config.v6_halfopen_buckets(), config.v6_halfopen_memory()),
    )
}
