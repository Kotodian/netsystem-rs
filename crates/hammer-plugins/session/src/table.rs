use hammer_infra::bihash::{Bihash16x8, Bihash48x8};
use hammer_service::session::SessionTable;

use crate::config::IpSessionTableConfig;

pub(crate) type Ip4SessionTable = SessionTable<Bihash16x8, Bihash16x8>;
pub(crate) type Ip6SessionTable = SessionTable<Bihash48x8, Bihash48x8>;

pub(crate) enum IpSessionTable {
    Ip4(Ip4SessionTable),
    Ip6(Ip6SessionTable),
}

impl IpSessionTable {
    pub(crate) fn ip4(config: IpSessionTableConfig) -> Self {
        Self::Ip4(SessionTable::new(
            Bihash16x8::with_memory_size(config.v4_session_buckets(), config.v4_session_memory()),
            Bihash16x8::with_memory_size(config.v4_halfopen_buckets(), config.v4_halfopen_memory()),
        ))
    }

    pub(crate) fn ip6(config: IpSessionTableConfig) -> Self {
        Self::Ip6(SessionTable::new(
            Bihash48x8::with_memory_size(config.v6_session_buckets(), config.v6_session_memory()),
            Bihash48x8::with_memory_size(config.v6_halfopen_buckets(), config.v6_halfopen_memory()),
        ))
    }

    pub(crate) fn memory_size(&self) -> u64 {
        match self {
            Self::Ip4(table) => {
                table.sessions().memory_size() as u64 + table.half_open().memory_size() as u64
            }
            Self::Ip6(table) => {
                table.sessions().memory_size() as u64 + table.half_open().memory_size() as u64
            }
        }
    }
}
