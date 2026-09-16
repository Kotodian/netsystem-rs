extern crate self as hammer_service;

hammer_runtime::__declare_registration_image!(
    init_functions = [
        binary_api::__INIT_FN_BINARY_API_INIT,
        session::__INIT_FN_APPLICATION_INIT,
        interface_model::__INIT_FN_INTERFACE_MAIN_INIT,
        interface_model::feature::__INIT_FN_INTERFACE_FEATURE_INIT,
        net::__INIT_FN_NET_MAIN_INIT,
        device::__INIT_FN_DEVICE_MAIN_INIT,
        session::__INIT_FN_SESSION_INIT,
        session::__INIT_FN_SESSION_ATTACH_SERVER,
        transport::__INIT_FN_TRANSPORT_MAIN_INIT,
        vpe_api::__INIT_FN_VPE_API_INIT,
    ];
    config_functions = [
        binary_api::__CONFIG_FN_BINARY_API_CONFIG,
        binary_api::__CONFIG_FN_API_SEGMENT_CONFIG,
        session::__CONFIG_FN_SESSION_CONFIG,
    ];
    main_loop_enter_functions = [];
    main_loop_exit_functions = [
        binary_api::__INIT_FN_EXIT_BINARY_API,
        session::__INIT_FN_EXIT_SESSION,
    ];
    worker_init_functions = [
        session::__INIT_FN_SESSION_WORKER_INIT,
    ];
    num_workers_change_functions = [];
    api_init_functions = [vpe_api::__INIT_FN_VPE_API_HOOKUP];
    graph_nodes = [
        data_plane::__SERVICE_GRAPH_NODE_DROP_NODE,
        data_plane::__SERVICE_GRAPH_NODE_PUNT_NODE,
        interface::__SERVICE_GRAPH_NODE_INTERFACE_OUTPUT_NODE,
        session::node::__SESSION_GRAPH_NODE_APP_SESSION_INPUT_NODE,
        session::node::__SESSION_GRAPH_NODE_SESSION_QUEUE_NODE,
    ];
    node_functions = [];
    process_nodes = [
        binary_api::__PROCESS_NODE_BINARY_API,
    ];
    binary_api_methods = [];
    stats_registrations = [
        hammer_ipc::binary_api::memory_shared::__STATS_REGISTRATION_REGISTER_ROOT_REGION_PVT_HEAP,
        hammer_ipc::binary_api::memory_shared::__STATS_REGISTRATION_REGISTER_API_REGION_PVT_HEAP,
        hammer_ipc::binary_api::memory_shared::__STATS_REGISTRATION_REGISTER_API_REGION_DATA_HEAP,
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
/// Interface / adjacency control plane — shared infrastructure, not a plugin.
pub mod interface;
mod interface_model;
pub use interface_model::InterfaceRegistrationImage;
pub mod net;
pub mod opaque;
/// Session layer — shared infrastructure, not a plugin.
pub mod session;
/// Transport-neutral helpers. Protocol plugins live under `hammer-plugins/transport/`.
pub mod transport;
pub mod vpe_api;

pub use hammer_runtime::{AttachError, RuntimeError, RuntimeResult};

#[cfg(test)]
static BUFFER_MAIN_INIT: std::sync::Once = std::sync::Once::new();
