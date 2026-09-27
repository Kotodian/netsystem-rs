//! Concrete IP Session endpoint and lookup-table plugin.

use std::sync::OnceLock;

use hammer_runtime::RuntimeResult;

mod api;
mod config;
mod endpoint;
mod lookup;
mod main;
mod namespace;
mod table;
mod transport;

pub use api::{
    AppNamespaceAddDel, AppNamespaceAddDelReply, AppNamespaceAddDelRetval, InterfaceIndex,
};
pub use config::{IpSessionConfig, IpSessionTableConfig};
pub use endpoint::{
    ENDPOINT_INVALID_INDEX, IpHalfOpenHandle, IpSessionEndpoint, IpSessionEndpointConfig,
    IpTransportConnectionId,
    IpTransportEndpoint, IpTransportEndpointConfig,
};
pub use lookup::{IpSessionFamily, IpSessionLookup, IpSessionLookupKey, SessionTableIterator};
pub use main::IpSessionMain;
pub use namespace::{IpNamespaceBinding, IpNamespaceMain, namespaces};
pub use transport::{
    IpTransportConfig, IpTransportConnection, IpTransportMain, LocalEndpointCleanupState,
};

static SESSION_TABLE_CONFIG: OnceLock<IpSessionTableConfig> = OnceLock::new();

pub(crate) fn ip_session_config() -> Option<IpSessionConfig> {
    SESSION_TABLE_CONFIG.get().copied()
}

#[hammer_component_macros::config_function(
    name = "session_table_config",
    section = "network",
    early = true,
    runs_after = ["session_config"]
)]
fn configure_session_tables(config: config::NetworkSessionConfig) -> RuntimeResult<()> {
    let config = config.session.unwrap_or_default();
    assert!(
        SESSION_TABLE_CONFIG.set(config).is_ok(),
        "Session table configuration callback executes once"
    );
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "ip_transport_main_init",
    runs_after = ["session_init"]
)]
fn init_ip_session_main() -> RuntimeResult<()> {
    let table_config = SESSION_TABLE_CONFIG.get().copied().unwrap_or_default();
    IpSessionMain::init(table_config, table_config.transport)
    .map_err(|source| IpSessionInitError::SessionCore { source })?;
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "session_lookup_init",
    runs_after = ["ip_transport_main_init"]
)]
fn session_lookup_init() -> RuntimeResult<()> {
    IpSessionMain::global()
        .map(|_| ())
        .map_err(|source| IpSessionInitError::SessionCore { source }.into())
}

#[inline]
pub fn session_lookup() -> &'static IpSessionLookup {
    IpSessionMain::global()
        .expect("IP Session Main is initialized before transport use")
        .lookup_main()
}

#[hammer_component_macros::runtime_error(subsystem = "session")]
#[derive(Debug, thiserror::Error)]
enum IpSessionInitError {
    #[error("initialize Session core")]
    SessionCore {
        #[source]
        source: hammer_service::session::SessionError,
    },
}

hammer_component_macros::declare_plugin!(
    name = "session",
    load_after = ["ip"],
    init_functions = [
        __INIT_FN_IP_TRANSPORT_MAIN_INIT,
        __INIT_FN_SESSION_LOOKUP_INIT,
        namespace::__INIT_FN_IP_NAMESPACE_INIT,
    ],
    config_functions = [__CONFIG_FN_SESSION_TABLE_CONFIG],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [],
    worker_init_functions = [],
    api_init_functions = [api::__INIT_FN_SESSION_API_HOOKUP],
    graph_nodes = [],
    node_functions = [],
    process_nodes = [],
);
