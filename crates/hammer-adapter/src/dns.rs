use std::sync::Arc;

use async_trait::async_trait;
use hammer_core::config::DomainStrategy;
use hammer_core::error::CoreError;
use hammer_core::lifecycle::Lifecycle;
use hickory_proto::op::Message;
use std::net::IpAddr;

/// `adapter.DNSRouter` — picks a `DnsTransport` for each query and caches
/// results.
#[async_trait]
pub trait DnsRouter: Lifecycle {
    async fn exchange(
        &self,
        message: Message,
        options: DnsQueryOptions,
    ) -> Result<Message, CoreError>;
    async fn lookup(
        &self,
        domain: &str,
        options: DnsQueryOptions,
    ) -> Result<Vec<IpAddr>, CoreError>;
    fn clear_cache(&self);
    fn lookup_reverse_mapping(&self, ip: IpAddr) -> Option<String>;
    fn reset_network(&self);
}

/// `adapter.DNSTransport` — single upstream resolver (UDP / TCP / HTTPS / hosts
/// / local).
#[async_trait]
pub trait DnsTransport: Lifecycle {
    fn type_name(&self) -> &str;
    fn tag(&self) -> &str;
    fn dependencies(&self) -> &[String];
    fn reset(&self);
    async fn exchange(&self, message: Message) -> Result<Message, CoreError>;
}

pub trait DnsTransportManager: Lifecycle {
    fn list(&self) -> Vec<Arc<dyn DnsTransport>>;
    fn get(&self, tag: &str) -> Option<Arc<dyn DnsTransport>>;
    fn default(&self) -> Option<Arc<dyn DnsTransport>>;
    fn remove(&self, tag: &str) -> Result<(), CoreError>;
}

#[derive(Clone, Default)]
pub struct DnsQueryOptions {
    pub transport: Option<Arc<dyn DnsTransport>>,
    pub strategy: DomainStrategy,
    pub lookup_strategy: DomainStrategy,
    pub disable_cache: bool,
    pub rewrite_ttl: Option<u32>,
}
