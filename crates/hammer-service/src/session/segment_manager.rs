use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use hammer_infra::pool::Pool;
use hammer_infra::svm::fifo_segment::{SvmFifoSegment, SvmFifoSegmentConfig};
use hammer_infra::svm::msg_queue::{SvmMsgQ, SvmMsgQConfig, SvmMsgQRingConfig};
use hammer_infra::svm::ssvm::{SsvmConfig, SsvmPrivate, SsvmSegmentBackend};
use hammer_runtime::RuntimeError;

use super::application::ApplicationError;
use super::core::SESSION_EVENT_RECORD_SIZE;

const DEFAULT_FIFO_SIZE: u32 = 1 << 12;
const DEFAULT_SEGMENT_SIZE: usize = 1 << 20;
const DEFAULT_EVENT_QUEUE_SIZE: u32 = 128;
const DEFAULT_MAX_FIFO_SIZE: u32 = 4 << 20;
const EVENT_CONTROL_SIZE: u32 = 256;
static SEGMENT_NAME_COUNTER: AtomicU32 = AtomicU32::new(0);

// VPP: segment_manager_props_t, segment_manager.h:15-37;
// segment_manager_main_init, segment_manager.c:1067-1082.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentManagerProperties {
    pub rx_fifo_size: u32,
    pub tx_fifo_size: u32,
    pub event_queue_size: u32,
    pub preallocated_fifos: u32,
    pub preallocated_fifo_headers: u32,
    pub segment_size: usize,
    pub add_segment_size: usize,
    pub add_segment: bool,
    pub use_mq_eventfd: bool,
    pub use_huge_page: bool,
    pub no_dump_segments: bool,
    pub n_slices: u8,
    pub segment_backend: SsvmSegmentBackend,
    pub max_fifo_size: u32,
    pub high_watermark: u8,
    pub low_watermark: u8,
    pub first_allocation_percent: u8,
    pub max_segments: u32,
}

impl SegmentManagerProperties {
    pub fn new(
        worker_count: u32,
        segment_backend: SsvmSegmentBackend,
    ) -> Result<Self, ApplicationError> {
        let n_slices = u8::try_from(worker_count)
            .ok()
            .filter(|count| *count != 0)
            .ok_or(ApplicationError::SegmentSliceCount { worker_count })?;
        Ok(Self {
            rx_fifo_size: DEFAULT_FIFO_SIZE,
            tx_fifo_size: DEFAULT_FIFO_SIZE,
            event_queue_size: DEFAULT_EVENT_QUEUE_SIZE,
            preallocated_fifos: 0,
            preallocated_fifo_headers: 0,
            segment_size: DEFAULT_SEGMENT_SIZE,
            add_segment_size: DEFAULT_SEGMENT_SIZE,
            add_segment: false,
            use_mq_eventfd: false,
            use_huge_page: false,
            no_dump_segments: false,
            n_slices,
            segment_backend,
            max_fifo_size: DEFAULT_MAX_FIFO_SIZE,
            high_watermark: 80,
            low_watermark: 50,
            first_allocation_percent: 100,
            max_segments: 0,
        })
    }
}

// VPP: seg_manager_flag_t, segment_manager.h:39-50.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SegmentManagerFlags {
    pub detached: bool,
    pub detached_listener: bool,
    pub listener: bool,
    pub connects: bool,
}

// VPP: segment_manager_t, segment_manager.h:52-80. The first segment owns
// the event queue; the manager retains its index rather than a self-reference.
pub struct SegmentManager {
    segments: Pool<SvmFifoSegment>,
    owner_app_worker: u32,
    first_segment_protected: bool,
    event_queue_index: u32,
    flags: SegmentManagerFlags,
    properties: SegmentManagerProperties,
}

impl SegmentManager {
    // VPP: segment_manager_init_first/segment_manager_alloc_queue,
    // segment_manager.c:421-509,990-1034.
    pub(crate) fn new(properties: SegmentManagerProperties) -> Result<Self, ApplicationError> {
        if properties.n_slices == 0 {
            return Err(ApplicationError::SegmentSliceCount { worker_count: 0 });
        }
        if properties.event_queue_size == 0
            || properties.preallocated_fifos != 0 && properties.preallocated_fifo_headers != 0
        {
            return Err(ApplicationError::SegmentConfigInvalid);
        }

        let mut segments = Pool::new();
        let mut remaining_pairs = properties.preallocated_fifos;
        loop {
            let first = segments.is_empty();
            let size = if first {
                properties.segment_size.max(DEFAULT_SEGMENT_SIZE)
            } else {
                properties.add_segment_size.max(DEFAULT_SEGMENT_SIZE)
            };
            let name = format!(
                "hammer-app-{}-{}",
                std::process::id(),
                SEGMENT_NAME_COUNTER.fetch_add(1, Ordering::Relaxed)
            );
            let mapping = Arc::new(
                SsvmPrivate::server_init_fifo_segment(&SsvmConfig {
                    backend: properties.segment_backend,
                    name,
                    size,
                    requested_va: 0,
                    huge_page: properties.use_huge_page,
                    attach_timeout: Duration::from_secs(1),
                })
                .map_err(|source| ApplicationError::SegmentMapping { source })?,
            );
            let mut segment = SvmFifoSegment::new(
                mapping,
                SvmFifoSegmentConfig {
                    slices: u32::from(properties.n_slices),
                    max_fifo_size: properties.max_fifo_size.max(DEFAULT_FIFO_SIZE) as usize,
                    first_allocation_percent: properties.first_allocation_percent,
                    low_watermark: properties.low_watermark,
                    high_watermark: properties.high_watermark,
                },
            )
            .map_err(|source| ApplicationError::SegmentInit { source })?;

            if first {
                let queue_len = properties.event_queue_size;
                let control_len = (queue_len >> 4).max(16);
                let rings = [
                    SvmMsgQRingConfig::new(queue_len, SESSION_EVENT_RECORD_SIZE),
                    SvmMsgQRingConfig::new(control_len, EVENT_CONTROL_SIZE),
                ];
                segment
                    .allocate_message_queue(
                        0,
                        &SvmMsgQConfig {
                            consumer_pid: 0,
                            q_nitems: queue_len,
                            rings: &rings,
                        },
                    )
                    .map_err(|source| ApplicationError::SegmentInit { source })?;
                for slice in 0..u32::from(properties.n_slices) {
                    let count = properties.preallocated_fifo_headers
                        / u32::from(properties.n_slices)
                        + u32::from(
                            slice
                                < properties.preallocated_fifo_headers
                                    % u32::from(properties.n_slices),
                        );
                    segment
                        .preallocate_fifo_headers(slice, count)
                        .map_err(|source| ApplicationError::SegmentInit { source })?;
                }
            }

            if remaining_pairs != 0 {
                let before = remaining_pairs;
                for slice in 0..u32::from(properties.n_slices) {
                    let slices_left = u32::from(properties.n_slices) - slice;
                    let requested = remaining_pairs.div_ceil(slices_left);
                    let leftover = segment
                        .preallocate_fifo_pairs(
                            slice,
                            properties.rx_fifo_size as usize,
                            properties.tx_fifo_size as usize,
                            requested,
                        )
                        .map_err(|source| ApplicationError::SegmentInit { source })?;
                    remaining_pairs -= requested - leftover;
                }
                if remaining_pairs == before && !first {
                    return Err(ApplicationError::SegmentNoSpace);
                }
            }
            segments.insert(segment);
            if remaining_pairs == 0 {
                break;
            }
            if properties.max_segments != 0 && segments.len() >= properties.max_segments as usize {
                return Err(ApplicationError::SegmentNoSpace);
            }
        }

        Ok(Self {
            segments,
            owner_app_worker: u32::MAX,
            first_segment_protected: true,
            event_queue_index: 0,
            flags: SegmentManagerFlags {
                connects: true,
                ..SegmentManagerFlags::default()
            },
            properties,
        })
    }

    #[inline]
    pub fn event_queue(&self) -> Option<&SvmMsgQ> {
        self.segments.get(0)?.message_queue(self.event_queue_index)
    }

    #[inline(always)]
    pub const fn owner_app_worker(&self) -> u32 {
        self.owner_app_worker
    }

    #[inline]
    pub fn active_fifo_count(&self) -> u32 {
        self.segments
            .iter()
            .map(|(_, segment)| segment.active_fifo_count())
            .sum()
    }

    pub(crate) fn set_owner(&mut self, owner_app_worker: u32) {
        assert_eq!(
            self.owner_app_worker,
            u32::MAX,
            "SegmentManager owner is assigned once"
        );
        self.owner_app_worker = owner_app_worker;
    }

    // VPP: app_worker_alloc_listener_segment_manager,
    // application_worker.c:147-158.
    pub(crate) fn mark_listener(&mut self) {
        self.flags.listener = true;
        self.first_segment_protected = true;
    }

    // VPP: app_worker_stop_listen_session, application_worker.c:363-408.
    pub(crate) fn mark_detached_listener(&mut self) {
        self.flags.detached_listener = true;
        self.flags.listener = false;
        self.first_segment_protected = false;
    }
}

// VPP: segment_manager_main_t, segment_manager.c:50-69. Control-plane
// mutation is under the main thread's WorkerBarrier; worker readers use only
// a borrowed manager while their own graph execution is active.
pub struct SegmentManagerMain {
    managers: UnsafeCell<Pool<SegmentManager>>,
}

static SEGMENT_MANAGER_MAIN: OnceLock<SegmentManagerMain> = OnceLock::new();

// SAFETY: manager pool mutation is main-thread work under WorkerBarrier.
unsafe impl Send for SegmentManagerMain {}
// SAFETY: no manager is removed while a Data Worker may retain a borrow.
unsafe impl Sync for SegmentManagerMain {}

impl SegmentManagerMain {
    pub fn init() {
        assert!(
            SEGMENT_MANAGER_MAIN
                .set(Self {
                    managers: UnsafeCell::new(Pool::new())
                })
                .is_ok(),
            "SegmentManagerMain initializes once"
        );
    }

    #[inline(always)]
    pub fn global() -> Result<&'static Self, RuntimeError> {
        SEGMENT_MANAGER_MAIN
            .get()
            .ok_or(RuntimeError::PluginStateNotInitialized {
                plugin: "segment manager",
            })
    }

    #[inline(always)]
    pub fn get(&self, manager: u32) -> Option<&SegmentManager> {
        unsafe { &*self.managers.get() }.get(manager)
    }

    pub(crate) fn insert(&self, manager: SegmentManager) -> u32 {
        // SAFETY: the caller holds the main-thread WorkerBarrier.
        unsafe { &mut *self.managers.get() }.insert(manager)
    }

    pub(crate) fn remove(&self, manager: u32) -> Result<(), ApplicationError> {
        // SAFETY: the caller holds the main-thread WorkerBarrier.
        let managers = unsafe { &mut *self.managers.get() };
        let entry = managers
            .get(manager)
            .ok_or(ApplicationError::SegmentMissing { manager })?;
        if entry.active_fifo_count() != 0 {
            return Err(ApplicationError::SegmentBusy { manager });
        }
        managers.remove(manager);
        Ok(())
    }
}
