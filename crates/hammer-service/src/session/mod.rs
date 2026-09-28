//! Session layer — shared in `hammer-service` (not a loadable plugin).

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use hammer_core::data_plane::{NodeId, NodeState};
use hammer_infra::svm::fifo_segment::{FifoSegmentError, SvmFifoSegment, SvmFifoSegmentConfig};
use hammer_infra::svm::ssvm::{SsvmConfig, SsvmPrivate, SsvmSegmentBackend};
use hammer_runtime::attach::AppServer;
use hammer_runtime::{DataPlaneMain, RuntimeResult};

pub mod app;
#[deprecated(note = "Legacy Application path; new Application ownership belongs in session::app")]
pub mod application;
pub mod config;
mod control;
pub mod core;
pub mod endpoint;
pub mod error;
#[deprecated(note = "Legacy external App Session path; use session::app")]
pub mod legacy_app;
pub mod lookup;
pub mod node;
pub mod protocol;
pub mod runtime;
pub mod segment_manager;
pub mod state;
pub mod table;

pub use app::{ApplicationConfig, ApplicationEventResult, ApplicationFlags, SessionCleanup};
pub use application::{
    APPLICATION_MAIN, ApplicationError, ApplicationMain, ApplicationMqResources, application_main,
};
pub use config::Session as SessionSettings;
pub use core::{
    PoolReallocationState, RxDelivery, SESSION_E_ALLOC, SESSION_E_INVALID, SESSION_E_MQ_MSG_ALLOC,
    SESSION_E_NOINTF, SESSION_E_NOIP, SESSION_E_NONE, SESSION_E_NOPORT, SESSION_E_NOROUTE,
    SESSION_E_NOSESSION, SESSION_E_NOSUPPORT, SESSION_E_PORTINUSE, SESSION_E_SEG_CREATE,
    SESSION_E_SEG_NO_SPACE, SESSION_E_TRANSPORT_NO_REG, SESSION_E_UNKNOWN,
    SESSION_EVENT_QUEUE_FULL, SESSION_EVENT_QUEUE_LOCK_FAILED, SESSION_INDEX_INVALID, AppSession, Session,
    SessionConfig, SessionControlData, SessionDmaTransfer, SessionEvent, SessionEventElement,
    SessionEventEnqueue, SessionEventType, SessionFlags, SessionHandle, SessionMain,
    SessionMigrationRequest, SessionMigrationState, SessionRxSegment, SessionState,
    SessionTxContext, SessionWorker, SessionWorkerFlags, SessionWorkerState,
    enqueue_notify, program_transport_io_event, program_tx_io_event,
};
pub use endpoint::{SessionEndpoint, SessionEndpointConfig, SessionEndpointFlags};
pub use error::{SessionConnectError, SessionError, SessionQueueError};
pub use legacy_app::AppWorker;
pub use lookup::{SessionLookup, SessionLookupResult};
pub use node::{
    AppSessionInputNode, SESSION_QUEUE_IO_BUDGET, SessionInputNode, SessionQueueNext,
    SessionQueueNode,
};
pub use protocol::{SessionAppVft, register_session_app};
pub use runtime::{SESSION_MAIN, SessionAcceptMetadata, SessionEndpointRole, session_main};
pub use segment_manager::{
    SegmentManager, SegmentManagerError, SegmentManagerFlags, SegmentManagerMain,
    SegmentManagerProperties,
};
pub use table::SessionTable;

static SESSION_CONFIG: OnceLock<config::Session> = OnceLock::new();
static APP_SERVER: OnceLock<Arc<AppServer>> = OnceLock::new();
static APP_SESSION_INPUT_NODE: OnceLock<NodeId> = OnceLock::new();

pub fn app_server() -> Option<Arc<AppServer>> {
    APP_SERVER.get().map(Arc::clone)
}

pub fn session_config() -> &'static config::Session {
    SESSION_CONFIG
        .get()
        .expect("Session configuration is installed before Session initialization")
}

#[hammer_component_macros::config_function(
    name = "session_config",
    section = "network",
    early = true,
    runs_after = ["runtime_worker_config"]
)]
fn configure_session(config: config::NetworkSessionConfig) -> RuntimeResult<()> {
    let session = config.session.unwrap_or_default();
    session.validate()?;
    assert!(
        SESSION_CONFIG.set(session).is_ok(),
        "Session configuration callback executes once"
    );
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "session_init",
    runs_after = ["transport_main_init", "application_init", "session_attach_server"]
)]
fn init_session() -> RuntimeResult<()> {
    let settings = session_config();
    let worker_count = hammer_runtime::config::worker::worker_count();
    let mut config = core::SessionConfig::default();
    config.worker_count = worker_count as u32;
    config.session_capacity = u32::try_from(settings.pool_capacity).map_err(|_| {
        SessionQueueError::SessionCapacityOverflow {
            capacity: settings.pool_capacity,
        }
    })?;
    let event_capacity = u32::try_from(settings.app_mq_capacity).map_err(|_| {
        SessionQueueError::EventQueueCapacityOverflow {
            capacity: settings.app_mq_capacity,
        }
    })?;
    config.configured_worker_mq_length = event_capacity;
    config.event_ring_capacity = event_capacity;
    config.session_enable_asap = false;
    let mapping = Arc::new(
        SsvmPrivate::server_init_fifo_segment(&SsvmConfig {
            backend: SsvmSegmentBackend::Private,
            name: "hammer-session-worker-mq".to_owned(),
            size: config.worker_mq_segment_size,
            requested_va: 0,
            huge_page: false,
            attach_timeout: Duration::from_secs(1),
        })
        .map_err(|source| SessionQueueError::SegmentCreate {
            source: FifoSegmentError::Ssvm { source },
        })?,
    );
    let segment = SvmFifoSegment::new(
        mapping,
        SvmFifoSegmentConfig {
            slices: config.worker_count,
            ..SvmFifoSegmentConfig::default()
        },
    )
    .map_err(|source| SessionQueueError::SegmentCreate { source })?;
    core::SessionMain::init(config, segment)?;
    runtime::SessionMain::init(hammer_runtime::config::worker::worker_count())
}

#[hammer_component_macros::main_loop_exit_function]
fn exit_session() -> RuntimeResult<()> {
    if let Ok(session) = runtime::SessionMain::global() {
        session.begin_session_migration_shutdown();
    }
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "application_init",
    runs_after = ["transport_main_init"]
)]
fn init_application() -> RuntimeResult<()> {
    SegmentManagerMain::init(SegmentManagerProperties::default());
    app::ApplicationMain::init(
        u32::try_from(hammer_runtime::config::worker::worker_count())
            .expect("configured worker count fits u32"),
    )?;
    ApplicationMain::init()
}

#[hammer_component_macros::worker_init_function(name = "session_worker_init")]
fn init_session_worker(engine: &mut DataPlaneMain) -> RuntimeResult<()> {
    let session_main = core::SessionMain::global()
        .expect("Session Main initializes before session worker graph setup");
    let enabled = session_main.is_enabled();
    let session_input = engine
        .node_by_name("session-input")
        .ok_or(error::SessionQueueError::NodeMissing)?;
    engine.nodes().set_node_state(
        session_input,
        if enabled {
            NodeState::Interrupt
        } else {
            NodeState::Disabled
        },
    )?;
    let worker = unsafe { session_main.worker_mut(engine) }
        .expect("session worker graph setup runs on a configured Data Worker");
    worker.install_input_node(session_input);
    let session_queue = engine
        .node_by_name("session-queue")
        .ok_or(error::SessionQueueError::NodeMissing)?;
    worker.install_queue_node(session_queue);
    engine.nodes().set_node_state(
        session_queue,
        if enabled {
            NodeState::Polling
        } else {
            NodeState::Disabled
        },
    )?;
    let app_session_input = engine
        .node_by_name("appsl-rx-mqs-input")
        .ok_or(error::SessionQueueError::NodeMissing)?;
    if let Some(installed) = APP_SESSION_INPUT_NODE.get() {
        assert_eq!(
            *installed, app_session_input,
            "Session workers share graph node identities"
        );
    } else {
        APP_SESSION_INPUT_NODE
            .set(app_session_input)
            .expect("Session input node identity is installed once");
    }
    engine
        .nodes()
        .set_node_state(app_session_input, NodeState::Disabled)?;
    runtime::install_session_worker(engine, app_session_input, session_queue)?;
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "session_attach_server",
    runs_after = ["application_init"]
)]
fn configure_attach_server() -> RuntimeResult<()> {
    let session = session_config();
    let Some(path) = session.attach_socket_path.as_deref() else {
        return Ok(());
    };
    let server = Arc::new(AppServer::bind(path, session.app_session_capacity)?);
    assert!(
        APP_SERVER.set(server).is_ok(),
        "Session attach server configuration callback executes once"
    );
    Ok(())
}
