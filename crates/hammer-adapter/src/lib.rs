pub mod certificate;
pub mod connection;
pub mod dialer;
pub mod dns;
pub mod endpoint;
pub mod handler;
pub mod http;
pub mod inbound;
pub mod network;
pub mod outbound;
pub mod registry;
pub mod router;
pub mod rule;
pub mod service;

pub use hammer_core::lifecycle::{
    ALL_STAGES, LIFECYCLE_ORDER, Lifecycle, LifecycleService, StartStage,
};

// Re-exports used by the runtime crate so it doesn't have to know which
// sub-module each trait lives in.
pub use certificate::{CertificateProviderManager, CertificateProviderService, CertificateStore};
pub use connection::ConnectionManager;
pub use dialer::{Dialer, Network};
pub use dns::{DnsRouter, DnsTransport, DnsTransportManager};
pub use endpoint::{Endpoint, EndpointManager};
pub use handler::{ConnectionHandler, PacketConnectionHandler};
pub use http::{HttpClientManager, HttpTransport};
pub use inbound::{Inbound, InboundManager};
pub use network::NetworkManager;
pub use outbound::{Outbound, OutboundManager};
pub use registry::{Constructor, Registry, RegistryContext};
pub use router::Router;
pub use rule::{HeadlessRule, Rule};
pub use service::ServiceManager;
