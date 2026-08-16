extern crate self as hammer_service;

hammer_runtime::__declare_registration_image!(
    init_functions = [
        binary_api::__INIT_FN_BINARY_API_INIT,
        session::__INIT_FN_APPLICATION_INIT,
        device::__INIT_FN_DEVICE_INIT,
        session::__INIT_FN_SESSION_INIT,
        session::__INIT_FN_SESSION_ATTACH_SERVER,
        transport::__INIT_FN_TRANSPORT_INIT,
        stats::__INIT_FN_STATS_INIT,
    ];
    config_functions = [];
    early_config_functions = [
        binary_api::__CONFIG_FN_BINARY_API_CONFIG,
        session::__CONFIG_FN_SESSION_CONFIG,
        stats::__CONFIG_FN_STATS_CONFIG,
    ];
    main_loop_enter_functions = [];
    main_loop_exit_functions = [session::__INIT_FN_EXIT_SESSION];
    worker_init_functions = [
        session::__INIT_FN_SESSION_WORKER_INIT,
    ];
    graph_nodes = [
        data_plane::__SERVICE_GRAPH_NODE_DROP_NODE,
        data_plane::__SERVICE_GRAPH_NODE_HANDOFF_NODE,
        device::__SERVICE_GRAPH_NODE_DEVICE_INPUT_NODE,
        interface::__SERVICE_GRAPH_NODE_INTERFACE_OUTPUT_NODE,
        session::node::__SESSION_GRAPH_NODE_APP_SESSION_INPUT_NODE,
        session::node::__SESSION_GRAPH_NODE_SESSION_QUEUE_NODE,
    ];
    node_functions = [];
    process_nodes = [
        binary_api::__PROCESS_NODE_BINARY_API,
        stats::__PROCESS_NODE_STATS_COLLECTOR,
    ];
    session_transports = [];
    session_apps = [];
    binary_api_methods = [
        stats::__BINARY_API_STATS_LIST,
        stats::__BINARY_API_STATS_DUMP,
    ];
);

#[doc(hidden)]
pub fn registration_image() -> &'static hammer_runtime::__private::RegistrationImage {
    &__HAMMER_REGISTRATION_IMAGE
}

pub mod app;
pub mod binary_api;
pub mod data_plane;
/// Device-class abstraction. Concrete drivers live under `hammer-plugins/device/`.
pub mod device;
pub mod feature_arc;
/// Interface / adjacency control plane — shared infrastructure, not a plugin.
pub mod interface;
pub mod net;
pub mod opaque;
/// Session layer — shared infrastructure, not a plugin.
pub mod session;
// Stats segment capability, collector Process Node, and `stats.*` Binary
// API methods. Crate-internal: the public surface is the Binary API wire.
mod stats;
/// Transport-neutral helpers. Protocol plugins live under `hammer-plugins/transport/`.
pub mod transport;

pub use hammer_runtime::{AttachError, RuntimeError, RuntimeResult};

#[cfg(test)]
pub fn reset_subsystem_mains_for_test() {
    reset_subsystem_mains_for_plugin_test();
}

/// Test helper for plugin crates that cannot see `#[cfg(test)]` items on this crate.
pub fn reset_subsystem_mains_for_plugin_test() {
    crate::net::pmtu::reset_path_mtu_cache_for_test();
    crate::interface::reset_interface_main_for_test();
}
