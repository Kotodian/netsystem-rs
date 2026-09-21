use std::net::IpAddr;
use std::sync::OnceLock;

use hammer_infra::bihash::{Bihash, BihashKey, hash_words};
use hammer_runtime::{RuntimeError, RuntimeResult};
use hammer_service::session::SessionEndpoint;
use hammer_service::transport::{Config, TransportMain};

use crate::endpoint::{IpSessionEndpoint, IpTransportConnectionId, IpTransportEndpoint};

type IpLocalEndpoint = SessionEndpoint<IpTransportEndpoint>;

#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct IpLocalEndpointKey([u64; 3]);

impl BihashKey for IpLocalEndpointKey {
    #[inline(always)]
    fn hash(self) -> u64 {
        hash_words(&self.0)
    }
}

impl<'a> From<&'a IpLocalEndpoint> for IpLocalEndpointKey {
    #[inline(always)]
    fn from(endpoint: &'a IpLocalEndpoint) -> Self {
        let (first, second) = match endpoint.transport().address {
            IpAddr::V4(address) => (u64::from(u32::from(address)), 0),
            IpAddr::V6(address) => {
                let octets = address.octets();
                let mut first = [0; 8];
                let mut second = [0; 8];
                first.copy_from_slice(&octets[..8]);
                second.copy_from_slice(&octets[8..]);
                (
                    u64::from_be_bytes(first),
                    u64::from_be_bytes(second),
                )
            }
        };
        Self([
            first,
            second,
            (u64::from(endpoint.transport().fib_index) << 32)
                | (u64::from(endpoint.transport().port) << 8)
                | u64::from(endpoint.transport_protocol()),
        ])
    }
}

struct AlpnProtocolTable {
    by_name: Bihash<u64, 7>,
}

impl AlpnProtocolTable {
    fn new() -> Self {
        Self {
            by_name: Bihash::new(64),
        }
    }
}

#[hammer_component_macros::runtime_error(subsystem = "transport")]
#[derive(Debug, thiserror::Error)]
pub enum LocalEndpointError {
    #[error("FIB {fib_index} has no route to {remote}")]
    NoRoute { fib_index: u32, remote: IpAddr },
    #[error("FIB {fib_index} route to {remote} has no resolving interface")]
    NoResolvingInterface { fib_index: u32, remote: IpAddr },
    #[error("interface {sw_if_index} has no local address for {remote}")]
    NoLocalAddress { sw_if_index: u32, remote: IpAddr },
    #[error("transport {protocol} has no available local port")]
    NoLocalPort { protocol: u8 },
    #[error("local endpoint {endpoint:?} is already in use")]
    LocalPortInUse { endpoint: IpSessionEndpoint },
}

pub struct IpTransportMain {
    main: TransportMain<IpTransportEndpoint, IpLocalEndpointKey, AlpnProtocolTable>,
}

static TRANSPORT_MAIN: OnceLock<IpTransportMain> = OnceLock::new();

impl IpTransportMain {
    pub fn init(config: Config) -> RuntimeResult<()> {
        let main = Self {
            main: TransportMain::new(config, AlpnProtocolTable::new()),
        };
        assert!(
            TRANSPORT_MAIN.set(main).is_ok(),
            "IP TransportMain initialization callback executes once"
        );
        Ok(())
    }

    pub fn global() -> RuntimeResult<&'static Self> {
        TRANSPORT_MAIN
            .get()
            .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "session" })
    }

    pub fn mark_used(&self, endpoint: &IpSessionEndpoint) -> Result<(), LocalEndpointError> {
        let local = local_endpoint(endpoint);
        if self.main.mark_used(local) {
            Ok(())
        } else {
            Err(LocalEndpointError::LocalPortInUse { endpoint: *endpoint })
        }
    }

    pub fn share(&self, endpoint: &IpSessionEndpoint) {
        self.main.share(&local_endpoint(endpoint));
    }

    pub fn release(&self, endpoint: &IpSessionEndpoint) -> bool {
        self.main.release(&local_endpoint(endpoint))
    }

    pub fn allocate_local(
        &self,
        mut endpoint: IpSessionEndpoint,
    ) -> Result<IpSessionEndpoint, LocalEndpointError> {
        let mut config = *endpoint.transport();
        if config.local.address.is_unspecified() {
            return Err(LocalEndpointError::NoLocalAddress {
                sw_if_index: config.local.sw_if_index,
                remote: config.peer.address,
            });
        }
        if config.local.port == 0 {
            let port = self.main.next_source_port();
            if port == 0 {
                return Err(LocalEndpointError::NoLocalPort {
                    protocol: endpoint.transport_protocol(),
                });
            }
            config.local.port = port;
            endpoint = SessionEndpoint::new(config, endpoint.transport_protocol());
        }
        if !self.main.mark_used(local_endpoint(&endpoint)) {
            return Err(LocalEndpointError::LocalPortInUse { endpoint });
        }
        Ok(endpoint)
    }

    pub fn local_endpoints_in_use(&self) -> u32 {
        self.main.local_endpoints_in_use()
    }

    pub fn max_port_allocation_tries(&self) -> u16 {
        self.main.max_port_allocation_tries()
    }

    pub fn clear_port_allocation_stats(&self) {
        self.main.clear_port_allocation_stats();
    }
}

fn local_endpoint(endpoint: &IpSessionEndpoint) -> IpLocalEndpoint {
    SessionEndpoint::new(
        endpoint.transport().local,
        endpoint.transport_protocol(),
    )
}

impl From<IpSessionEndpoint> for IpTransportConnectionId {
    #[inline(always)]
    fn from(endpoint: IpSessionEndpoint) -> Self {
        let config = *endpoint.transport();
        match (config.local.address, config.peer.address) {
            (IpAddr::V4(local_address), IpAddr::V4(remote_address)) => Self::Ip4 {
                remote_address,
                local_address,
                fib_index: config.local.fib_index,
                remote_port: config.peer.port,
                local_port: config.local.port,
                dscp: config.dscp,
                transport_protocol: endpoint.transport_protocol(),
            },
            (IpAddr::V6(local_address), IpAddr::V6(remote_address)) => Self::Ip6 {
                remote_address,
                local_address,
                fib_index: config.local.fib_index,
                remote_port: config.peer.port,
                local_port: config.local.port,
                dscp: config.dscp,
                transport_protocol: endpoint.transport_protocol(),
            },
            _ => panic!("IP transport endpoint family mismatch"),
        }
    }
}

#[hammer_component_macros::init_function(
    name = "ip_transport_main_init",
    runs_after = ["session_table_config"]
)]
fn init_ip_transport_main() -> RuntimeResult<()> {
    let config = crate::ip_session_config()
        .map(|config| config.transport)
        .unwrap_or_default();
    IpTransportMain::init(config)
}
