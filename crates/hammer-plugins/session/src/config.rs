const DEFAULT_SESSION_TABLE_BUCKETS: u32 = 20_000;
const DEFAULT_SESSION_TABLE_MEMORY: u32 = 64 << 20;

#[derive(Debug, Clone, Copy, Default, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct IpSessionConfig {
    pub v4_session_table_buckets: u32,
    pub v4_session_table_memory: u32,
    pub v4_halfopen_table_buckets: u32,
    pub v4_halfopen_table_memory: u32,
    pub v6_session_table_buckets: u32,
    pub v6_session_table_memory: u32,
    pub v6_halfopen_table_buckets: u32,
    pub v6_halfopen_table_memory: u32,
    #[serde(flatten)]
    pub transport: crate::transport::IpTransportConfig,
}

pub type IpSessionTableConfig = IpSessionConfig;

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
pub(crate) struct NetworkSessionConfig {
    pub session: Option<IpSessionTableConfig>,
}

impl IpSessionTableConfig {
    #[inline]
    pub(crate) const fn v4_session_buckets(self) -> u32 {
        configured_or_default(self.v4_session_table_buckets, DEFAULT_SESSION_TABLE_BUCKETS)
    }

    #[inline]
    pub(crate) const fn v4_session_memory(self) -> u32 {
        configured_or_default(self.v4_session_table_memory, DEFAULT_SESSION_TABLE_MEMORY)
    }

    #[inline]
    pub(crate) const fn v4_halfopen_buckets(self) -> u32 {
        configured_or_default(
            self.v4_halfopen_table_buckets,
            DEFAULT_SESSION_TABLE_BUCKETS,
        )
    }

    #[inline]
    pub(crate) const fn v4_halfopen_memory(self) -> u32 {
        configured_or_default(self.v4_halfopen_table_memory, DEFAULT_SESSION_TABLE_MEMORY)
    }

    #[inline]
    pub(crate) const fn v6_session_buckets(self) -> u32 {
        configured_or_default(self.v6_session_table_buckets, DEFAULT_SESSION_TABLE_BUCKETS)
    }

    #[inline]
    pub(crate) const fn v6_session_memory(self) -> u32 {
        configured_or_default(self.v6_session_table_memory, DEFAULT_SESSION_TABLE_MEMORY)
    }

    #[inline]
    pub(crate) const fn v6_halfopen_buckets(self) -> u32 {
        configured_or_default(
            self.v6_halfopen_table_buckets,
            DEFAULT_SESSION_TABLE_BUCKETS,
        )
    }

    #[inline]
    pub(crate) const fn v6_halfopen_memory(self) -> u32 {
        configured_or_default(self.v6_halfopen_table_memory, DEFAULT_SESSION_TABLE_MEMORY)
    }
}

#[inline]
const fn configured_or_default(configured: u32, default: u32) -> u32 {
    if configured == 0 { default } else { configured }
}
