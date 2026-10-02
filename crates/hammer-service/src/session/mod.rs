//! Session layer — shared in `hammer-service` (not a loadable plugin).

use std::sync::Arc;
use std::time::Duration;

use hammer_infra::svm::fifo_segment::{FifoSegmentError, SvmFifoSegment, SvmFifoSegmentConfig};
use hammer_infra::svm::msg_queue::{SvmMsgQ, SvmMsgQConfig, SvmMsgQError, SvmMsgQRingConfig};
use hammer_infra::svm::ssvm::{SsvmConfig, SsvmPrivate, SsvmSegmentBackend};
use hammer_runtime::RuntimeResult;

pub mod app;
pub mod core;
pub mod endpoint;
pub mod error;
pub mod lookup;
pub mod namespace;
pub mod node;
pub mod segment_manager;
pub mod table;

pub use app::{AppWorker, ApplicationError, ApplicationListener, ApplicationMain};
pub use app::{ApplicationConfig, ApplicationEventResult, ApplicationFlags, SessionCleanup};
pub use core::{
    AppSession, PoolReallocationState, RxDelivery, SESSION_INDEX_INVALID, Session, SessionConfig,
    SessionControlData, SessionDmaTransfer, SessionEvent, SessionEventElement, SessionEventEnqueue,
    SessionEventType, SessionFlags, SessionHandle, SessionMain, SessionMigrationRequest,
    SessionMigrationState, SessionRxSegment, SessionState, SessionTxContext, SessionTxDispatch,
    SessionTxOutcome, SessionWorker, SessionWorkerFlags, SessionWorkerState, enqueue_notify,
    program_transport_io_event, program_tx_io_event, send_control_event, send_rpc_event,
    send_rpc_event_force,
};
pub use endpoint::{SessionEndpoint, SessionEndpointConfig, SessionEndpointFlags};
pub use error::{SessionError, SessionQueueError};
pub use lookup::{SessionLookup, SessionLookupResult};
pub use node::{SESSION_QUEUE_IO_BUDGET, SessionInputNode, SessionQueueNext, SessionQueueNode};
pub use segment_manager::{
    SegmentManager, SegmentManagerError, SegmentManagerFlags, SegmentManagerMain,
    SegmentManagerProperties,
};
pub use table::SessionTable;

#[hammer_component_macros::init_function(name = "session_init")]
fn init_session() -> RuntimeResult<()> {
    let worker_count = hammer_runtime::config::worker::worker_count();
    let mut config = core::SessionConfig::default();
    config.worker_count = u32::try_from(worker_count).expect("configured worker count fits u32");
    let queue_length = config.configured_worker_mq_length.max(2_048);
    let rings = [
        SvmMsgQRingConfig::new(
            queue_length,
            std::mem::size_of::<core::SessionEvent>() as u32,
        ),
        SvmMsgQRingConfig::new(queue_length >> 1, 256),
    ];
    let queue_config = SvmMsgQConfig {
        consumer_pid: 0,
        q_nitems: queue_length,
        rings: &rings,
    };
    // VPP session.c:1737-1765: one two-ring MQ per worker, plus 1 MiB
    // for extended configuration messages outside the rings.
    let segment_size = SvmMsgQ::size_to_alloc(&queue_config)
        .and_then(|size| {
            size.checked_mul(worker_count)
                .ok_or(SvmMsgQError::LayoutOverflow)
        })
        .and_then(|size| {
            size.checked_add(1 << 20)
                .ok_or(SvmMsgQError::LayoutOverflow)
        })
        .map_err(|source| SessionQueueError::SegmentCreate {
            source: FifoSegmentError::from(source),
        })?
        .max(config.worker_mq_segment_size);
    let mapping = Arc::new(
        SsvmPrivate::server_init_fifo_segment(&SsvmConfig {
            backend: SsvmSegmentBackend::Memfd,
            name: "hammer-session-worker-mq".to_owned(),
            size: segment_size,
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
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "application_init",
    runs_after = ["session_init"]
)]
fn init_application() -> RuntimeResult<()> {
    SegmentManagerMain::init(SegmentManagerProperties::default());
    app::ApplicationMain::init(
        u32::try_from(hammer_runtime::config::worker::worker_count())
            .expect("configured worker count fits u32"),
    )?;
    Ok(())
}
