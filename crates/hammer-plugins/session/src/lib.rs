//! Concrete IP Session endpoint and lookup-table plugin.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use hammer_infra::svm::fifo_segment::{FifoSegmentError, SvmFifoSegment, SvmFifoSegmentConfig};
use hammer_infra::svm::ssvm::{SsvmConfig, SsvmError, SsvmPrivate, SsvmSegmentBackend};
use hammer_runtime::RuntimeResult;
use hammer_service::session::SessionConfig;

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
    ENDPOINT_INVALID_INDEX, IpHalfOpenHandle, IpSessionEndpoint, IpTransportConnectionId,
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
    let settings = hammer_service::session::session_config();
    let worker_count = hammer_runtime::config::worker::worker_count();
    let session_capacity = u32::try_from(settings.pool_capacity).map_err(|_| {
        IpSessionInitError::SessionCapacityOverflow {
            capacity: settings.pool_capacity,
        }
    })?;
    let event_ring_capacity = u32::try_from(settings.app_mq_capacity).map_err(|_| {
        IpSessionInitError::EventQueueCapacityOverflow {
            capacity: settings.app_mq_capacity,
        }
    })?;
    let mut session_config = SessionConfig::default();
    session_config.worker_count = worker_count as u32;
    session_config.configured_worker_mq_length = event_ring_capacity;
    session_config.event_ring_capacity = event_ring_capacity;
    session_config.session_capacity = session_capacity;
    session_config.session_enable_asap = true;

    let mapping = Arc::new(
        SsvmPrivate::server_init_fifo_segment(&SsvmConfig {
            backend: SsvmSegmentBackend::Private,
            name: "hammer-session-worker-mq".to_owned(),
            size: session_config.worker_mq_segment_size,
            requested_va: 0,
            huge_page: false,
            attach_timeout: Duration::from_secs(1),
        })
        .map_err(IpSessionInitError::WorkerMessageQueueMapping)?,
    );
    let segment = SvmFifoSegment::new(
        mapping,
        SvmFifoSegmentConfig {
            slices: session_config.worker_count,
            ..SvmFifoSegmentConfig::default()
        },
    )
    .map_err(IpSessionInitError::WorkerMessageQueueSegment)?;
    let main = IpSessionMain::init(
        session_config,
        table_config,
        table_config.transport,
        segment,
    )
    .map_err(|retval| IpSessionInitError::SessionCore { retval })?;
    IpSessionMain::publish_global(main);
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "session_lookup_init",
    runs_after = ["ip_transport_main_init"]
)]
fn session_lookup_init() -> RuntimeResult<()> {
    IpSessionMain::global()
        .map(|_| ())
        .map_err(|retval| IpSessionInitError::SessionCore { retval }.into())
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
    #[error("Session capacity {capacity} does not fit u32")]
    SessionCapacityOverflow { capacity: usize },
    #[error("Session event queue capacity {capacity} does not fit u32")]
    EventQueueCapacityOverflow { capacity: usize },
    #[error("create Session worker message-queue mapping")]
    WorkerMessageQueueMapping(#[source] SsvmError),
    #[error("initialize Session worker message-queue segment")]
    WorkerMessageQueueSegment(#[source] FifoSegmentError),
    #[error("initialize Session core returned retval {retval}")]
    SessionCore { retval: i32 },
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
