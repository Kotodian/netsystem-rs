//! Concrete IP Session endpoint and lookup-table plugin.

use std::sync::OnceLock;

use hammer_runtime::RuntimeResult;

mod config;
mod endpoint;
mod lookup;
mod table;

pub use config::IpSessionTableConfig;
pub use endpoint::{
    ENDPOINT_INVALID_INDEX, IpHalfOpenHandle, IpSessionEndpoint, IpTransportConnectionId,
    IpTransportEndpoint, IpTransportEndpointConfig,
};
pub use lookup::{IpSessionFamily, IpSessionLookup};

static SESSION_TABLE_CONFIG: OnceLock<IpSessionTableConfig> = OnceLock::new();
static SESSION_LOOKUP: OnceLock<IpSessionLookup> = OnceLock::new();

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
    name = "session_lookup_init",
    runs_after = ["session_init"]
)]
fn init_session_lookup() -> RuntimeResult<()> {
    let config = SESSION_TABLE_CONFIG.get().copied().unwrap_or_default();
    assert!(
        SESSION_LOOKUP.set(IpSessionLookup::new(config)).is_ok(),
        "Session lookup initialization callback executes once"
    );
    Ok(())
}

#[inline]
pub fn session_lookup() -> &'static IpSessionLookup {
    SESSION_LOOKUP
        .get()
        .expect("IP Session lookup is initialized before transport use")
}

hammer_component_macros::declare_plugin!(
    name = "session",
    load_after = [],
    init_functions = [__INIT_FN_SESSION_LOOKUP_INIT],
    config_functions = [__CONFIG_FN_SESSION_TABLE_CONFIG],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [],
    worker_init_functions = [],
    graph_nodes = [],
    node_functions = [],
    process_nodes = [],
);
