//! QUIC transport plugin skeleton.
//!
//! The first slice owns only QUIC's listener relationship and Session App
//! callback table. QUIC protocol state, packet processing, and worker timers
//! remain plugin-owned follow-up work; the service layer exposes only the
//! generic Session listener and callback contracts.

mod config;
mod listener;
mod session_app;
mod worker;

hammer_component_macros::declare_plugin!(
    name = "quic",
    load_after = ["udp"],
    init_functions = [listener::__INIT_FN_QUIC_INIT],
    config_functions = [],
    early_config_functions = [],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [],
    worker_init_functions = [],
    graph_nodes = [],
    node_functions = [],
    process_nodes = [],
    session_transports = [worker::__SESSION_TRANSPORT_QUIC_WORKER],
    session_apps = [session_app::QUIC_SESSION_APP],
    binary_api_methods = [
        config::__BINARY_API_REGISTER_SERVER_CONFIG_API,
        config::__BINARY_API_REGISTER_CLIENT_CONFIG_API,
        config::__BINARY_API_REMOVE_CONFIG_API,
    ],
);

pub use config::{
    ClientConfig, ConfigError, ConfigId, ServerConfig, TransportConfig, register_client_config,
    register_server_config, remove_config,
};
pub use listener::QuicMain;
