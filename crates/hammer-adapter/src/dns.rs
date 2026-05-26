use async_trait::async_trait;
use hammer_core::config::DomainStrategy;
use hammer_core::error::CoreResult;
use hammer_core::lifecycle::Lifecycle;
use hickory_proto::op::Message;
use std::net::IpAddr;

use crate::RuntimeComponent;

pub type DnsTransportComponent = RuntimeComponent<dyn DnsTransport>;

/// `adapter.DNSRouter` — picks a `DnsTransport` for each query and caches
/// results.
#[async_trait(?Send)]
pub trait DnsRouter: Lifecycle {
    async fn exchange(&self, message: Message, options: DnsQueryOptions) -> CoreResult<Message>;
    async fn lookup(&self, domain: &str, options: DnsQueryOptions) -> CoreResult<Vec<IpAddr>>;
    fn try_exchange_fast(
        &self,
        message: &Message,
        options: DnsQueryOptions,
    ) -> CoreResult<Option<Message>>;
    fn clear_cache(&self);
    fn lookup_reverse_mapping(&self, ip: IpAddr) -> Option<String>;
    fn reset_network(&self);
}

/// `adapter.DNSTransport` — single upstream resolver (UDP / TCP / HTTPS / hosts
/// / local).
#[async_trait(?Send)]
pub trait DnsTransport: Lifecycle {
    fn reset(&self);
    async fn exchange(&self, message: Message) -> CoreResult<Message>;
}

pub trait DnsTransportManager: Lifecycle {
    fn list(&self) -> Vec<DnsTransportComponent>;
    fn get(&self, id: &str) -> Option<DnsTransportComponent>;
    fn default(&self) -> Option<DnsTransportComponent>;
    fn remove(&self, id: &str) -> CoreResult<()>;
}

#[derive(Clone, Default)]
pub struct DnsQueryOptions {
    pub transport: Option<DnsTransportComponent>,
    pub strategy: DomainStrategy,
    pub lookup_strategy: DomainStrategy,
    pub disable_cache: bool,
    pub rewrite_ttl: Option<u32>,
}
