use std::cell::UnsafeCell;
use std::mem::size_of;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use hammer_infra::pool::Pool;
use hammer_infra::svm::fifo::{Fifo, FifoError};
use hammer_infra::svm::fifo_segment::{
    FifoSegmentError, FifoSegmentFtype, SvmFifoSegment, SvmFifoSegmentConfig,
};
use hammer_infra::svm::msg_queue::{SvmMsgQ, SvmMsgQConfig, SvmMsgQRingConfig};
use hammer_infra::svm::ssvm::{SsvmConfig, SsvmPrivate, SsvmSegmentBackend};

use super::core::SessionEvent;

/// Per-application FIFO segment policy.
/// VPP: `segment_manager_props_t`, vnet/session/segment_manager.h:15-37.
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
    pub max_fifo_size: u32,
    pub high_watermark: u8,
    pub low_watermark: u8,
    pub max_segments: u32,
}

impl Default for SegmentManagerProperties {
    fn default() -> Self {
        // VPP: segment_manager_main_init/segment_manager_props_init,
        // segment_manager.c:87-100,1067-1080.
        Self {
            rx_fifo_size: 1 << 12,
            tx_fifo_size: 1 << 12,
            event_queue_size: 128,
            preallocated_fifos: 0,
            preallocated_fifo_headers: 0,
            segment_size: 1 << 20,
            add_segment_size: 1 << 20,
            add_segment: false,
            use_mq_eventfd: false,
            max_fifo_size: 4 << 20,
            high_watermark: 80,
            low_watermark: 50,
            max_segments: 0,
        }
    }
}

bitflags::bitflags! {
    /// VPP: `seg_manager_flag_t`, vnet/session/segment_manager.h:39-50.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct SegmentManagerFlags: u8 {
        const DETACHED = 1 << 0;
        const DETACHED_LISTENER = 1 << 1;
        const LISTENER = 1 << 2;
        const CONNECTS = 1 << 3;
    }
}

/// VPP: `SESSION_E_SEG_CREATE`, `SESSION_E_SEG_NO_SPACE`, and
/// `SESSION_E_ALLOC`, session_types.h:519-561; segment_manager.c:744-892.
#[hammer_component_macros::runtime_error(subsystem = "session segment")]
#[derive(Debug, thiserror::Error)]
pub enum SegmentManagerError {
    #[error("Session segment creation failed")]
    SegmentCreate {
        #[source]
        source: FifoSegmentError,
    },
    #[error("Session segment has no space for a FIFO pair")]
    SegmentNoSpace,
    #[error("Session FIFO allocation failed")]
    FifoAllocation {
        #[source]
        source: FifoSegmentError,
    },
    #[error("Session event queue allocation failed")]
    EventQueueAllocation {
        #[source]
        source: FifoSegmentError,
    },
    #[error("Session segment manager has been detached")]
    Detached,
    #[error("Session segment worker {worker} is invalid")]
    InvalidWorker { worker: u32 },
    #[error("Session event queue capacity {capacity} is invalid")]
    InvalidEventQueueCapacity { capacity: u32 },
    #[error("Session segment manager still owns active FIFOs")]
    ActiveFifos,
}

/// VPP: `segment_manager_t`, segment_manager.h:52-80. The first segment owns
/// the event MQ. No queue storage is copied into this record.
pub struct SegmentManager<'segment> {
    event_queue: &'segment SvmMsgQ,
    segments: RwLock<Pool<SvmFifoSegment>>,
    owner_app_worker: u32,
    first_segment_protected: bool,
    flags: SegmentManagerFlags,
    properties: SegmentManagerProperties,
    max_fifo_size: u32,
    high_watermark: u8,
    low_watermark: u8,
    index: u32,
}

// SAFETY: the event queue is stable in the first segment's completed MQ Vec.
// Segment pool access is serialized by segments; the first segment is never
// removed before all AppWorkers borrowing its queue are detached.
unsafe impl Sync for SegmentManager<'_> {}

impl<'segment> SegmentManager<'segment> {
    /// VPP: `segment_manager_init_first`, segment_manager.c:421-488.
    pub fn init(
        owner_app_worker: u32,
        properties: SegmentManagerProperties,
        worker_count: u32,
        backend: SsvmSegmentBackend,
        huge_page: bool,
        flags: SegmentManagerFlags,
        name: String,
    ) -> Result<Self, SegmentManagerError> {
        let max_fifo_size = properties.max_fifo_size.max(4096);
        let mapping = Arc::new(
            SsvmPrivate::server_init_fifo_segment(&SsvmConfig {
                backend,
                name,
                size: properties.segment_size,
                requested_va: 0,
                huge_page,
                attach_timeout: Duration::from_secs(1),
            })
            .map_err(|source| SegmentManagerError::SegmentCreate {
                source: FifoSegmentError::Ssvm { source },
            })?,
        );
        let mut first = SvmFifoSegment::new(
            mapping,
            SvmFifoSegmentConfig {
                slices: worker_count,
                max_fifo_size: max_fifo_size as usize,
                high_watermark: properties.high_watermark,
                low_watermark: properties.low_watermark,
                ..SvmFifoSegmentConfig::default()
            },
        )
        .map_err(|source| SegmentManagerError::SegmentCreate { source })?;
        let q_nitems = properties
            .event_queue_size
            .checked_next_power_of_two()
            .filter(|&n| n != 0)
            .ok_or(SegmentManagerError::InvalidEventQueueCapacity {
                capacity: properties.event_queue_size,
            })?;
        // VPP: segment_manager_alloc_queue, segment_manager.c:1008-1035.
        let rings = [
            SvmMsgQRingConfig::new(
                properties.event_queue_size,
                size_of::<SessionEvent>() as u32,
            ),
            SvmMsgQRingConfig::new((properties.event_queue_size >> 4).max(16), 256),
        ];
        let queue = first
            .allocate_message_queue(
                0,
                &SvmMsgQConfig {
                    consumer_pid: 0,
                    q_nitems,
                    rings: &rings,
                },
            )
            .map_err(|source| SegmentManagerError::EventQueueAllocation { source })?;
        if properties.use_mq_eventfd {
            queue.allocate_eventfd().map_err(|source| {
                SegmentManagerError::EventQueueAllocation {
                    source: FifoSegmentError::MessageQueue { source },
                }
            })?;
        }
        let queue = queue as *const SvmMsgQ;
        if properties.preallocated_fifo_headers != 0 {
            let per_worker = properties.preallocated_fifo_headers / worker_count.max(1);
            for worker in 0..worker_count {
                first
                    .preallocate_fifo_headers(worker, per_worker)
                    .map_err(|source| SegmentManagerError::FifoAllocation { source })?;
            }
        }
        if properties.preallocated_fifos != 0 {
            let pairs_per_worker = properties.preallocated_fifos / worker_count.max(1);
            let extra = properties.preallocated_fifos % worker_count.max(1);
            for worker in 0..worker_count {
                let pairs = pairs_per_worker + u32::from(worker < extra);
                let remaining = first
                    .preallocate_fifo_pairs(
                        worker,
                        properties.rx_fifo_size as usize,
                        properties.tx_fifo_size as usize,
                        pairs,
                    )
                    .map_err(|source| SegmentManagerError::FifoAllocation { source })?;
                if remaining != 0 {
                    return Err(SegmentManagerError::SegmentNoSpace);
                }
            }
        }
        let mut segments = Pool::new();
        let index = segments.insert(first);
        assert_eq!(index, 0, "the first segment owns the event queue");
        // SAFETY: `SvmMsgQ` is stored in the completed first segment MQ Vec;
        // this manager never appends another MQ there and drops the borrow
        // before the segment pool. Manager removal requires AppWorker detach.
        let event_queue = unsafe { &*queue };
        Ok(Self {
            event_queue,
            segments: RwLock::new(segments),
            owner_app_worker,
            first_segment_protected: true,
            flags,
            properties,
            max_fifo_size,
            high_watermark: properties.high_watermark,
            low_watermark: properties.low_watermark,
            index: u32::MAX,
        })
    }

    /// VPP: `segment_manager_event_queue`, segment_manager.h:155-159.
    #[inline(always)]
    pub fn event_queue(&self) -> &SvmMsgQ {
        self.event_queue
    }

    #[inline(always)]
    pub const fn owner_app_worker(&self) -> u32 {
        self.owner_app_worker
    }

    #[inline(always)]
    pub const fn index(&self) -> u32 {
        self.index
    }

    /// VPP: `segment_manager_alloc_session_fifos`, segment_manager.c:744-892.
    /// The segment pool retains the resource; Session receives a private FIFO
    /// value referring to the same SVM header and chunk chain.
    pub fn allocate_session_fifos(&self, worker: u32) -> Result<(Fifo, Fifo), SegmentManagerError> {
        if self.flags.contains(SegmentManagerFlags::DETACHED) {
            return Err(SegmentManagerError::Detached);
        }
        let mut segments = self
            .segments
            .write()
            .expect("Segment Manager lock is not poisoned");
        let (segment_index, _) = segments
            .iter()
            .filter(|(_, segment)| worker < segment.n_slices as u32)
            .max_by_key(|(_, segment)| segment.available_bytes())
            .ok_or(SegmentManagerError::InvalidWorker { worker })?;
        let segment = segments
            .get_mut(segment_index)
            .expect("selected segment is occupied");
        let required =
            self.properties.rx_fifo_size as usize + self.properties.tx_fifo_size as usize;
        if segment.available_bytes() < required {
            return Err(SegmentManagerError::SegmentNoSpace);
        }
        let rx_index = segment
            .allocate_fifo(
                worker,
                self.properties.rx_fifo_size as usize,
                FifoSegmentFtype::RxFifo,
            )
            .map_err(|source| match source {
                FifoSegmentError::Fifo {
                    source: FifoError::SegmentExhausted,
                } => SegmentManagerError::SegmentNoSpace,
                source => SegmentManagerError::FifoAllocation { source },
            })?;
        let tx_index = match segment.allocate_fifo(
            worker,
            self.properties.tx_fifo_size as usize,
            FifoSegmentFtype::TxFifo,
        ) {
            Ok(index) => index,
            Err(source) => {
                segment
                    .free_server_fifo(worker, rx_index)
                    .expect("a just-allocated RX FIFO can be rolled back");
                return Err(match source {
                    FifoSegmentError::Fifo {
                        source: FifoError::SegmentExhausted,
                    } => SegmentManagerError::SegmentNoSpace,
                    source => SegmentManagerError::FifoAllocation { source },
                });
            }
        };
        let rx = segment
            .fifo(worker, rx_index)
            .expect("allocated RX FIFO remains in pool");
        // SAFETY: the session is the sole RX consumer and the segment retains
        // the allocation until Session cleanup returns both pool entries.
        let mut rx = unsafe { rx.duplicate() };
        let tx = segment
            .fifo(worker, tx_index)
            .expect("allocated TX FIFO remains in pool");
        // SAFETY: the session is the sole TX producer under worker ownership.
        let mut tx = unsafe { tx.duplicate() };
        rx.segment_manager = self.index;
        tx.segment_manager = self.index;
        rx.segment_index = segment_index;
        tx.segment_index = segment_index;
        Ok((rx, tx))
    }

    /// VPP: `segment_manager_dealloc_fifos`, segment_manager.c:892-949;
    /// called by the owning Session's Drop after removal from its Rust pool.
    pub(crate) fn release_session_fifos(&self, rx: &Fifo, tx: &Fifo) -> bool {
        assert_eq!(
            rx.segment_manager, self.index,
            "RX FIFO Segment Manager owner"
        );
        assert_eq!(
            tx.segment_manager, self.index,
            "TX FIFO Segment Manager owner"
        );
        assert_eq!(
            rx.segment_index, tx.segment_index,
            "FIFO pair segment identity"
        );
        let worker = unsafe { (*rx.shr).slice_index as u32 };
        assert_eq!(
            worker,
            unsafe { (*tx.shr).slice_index as u32 },
            "FIFO pair worker identity"
        );
        let mut segments = self
            .segments
            .write()
            .expect("Segment Manager lock is not poisoned");
        let segment = segments
            .get_mut(rx.segment_index)
            .expect("Session FIFO segment remains allocated until cleanup");
        let slice = segment
            .slices
            .get(worker as usize)
            .expect("Session FIFO slice remains allocated");
        let rx_index = slice
            .fifos
            .iter()
            .find_map(|(index, fifo)| (fifo.shr == rx.shr).then_some(index))
            .expect("Session RX FIFO remains allocated until cleanup");
        let tx_index = slice
            .fifos
            .iter()
            .find_map(|(index, fifo)| (fifo.shr == tx.shr).then_some(index))
            .expect("Session TX FIFO remains allocated until cleanup");
        assert_ne!(rx_index, tx_index, "RX and TX FIFO pool entries differ");
        segment
            .free_server_fifo(worker, rx_index)
            .expect("validated RX FIFO returns to its segment");
        segment
            .free_server_fifo(worker, tx_index)
            .expect("validated TX FIFO returns to its segment");
        self.flags.contains(SegmentManagerFlags::DETACHED)
            && segments
                .iter()
                .all(|(_, segment)| segment.active_fifo_count() == 0)
    }

    /// VPP: `segment_manager_init_free`, segment_manager.c:566-584.
    pub fn mark_detached(&mut self) {
        self.flags.insert(SegmentManagerFlags::DETACHED);
        self.first_segment_protected = false;
    }

    #[inline(always)]
    pub fn has_fifos(&self) -> bool {
        self.segments
            .read()
            .expect("Segment Manager lock is not poisoned")
            .iter()
            .any(|(_, segment)| segment.active_fifo_count() != 0)
    }
}

/// VPP: `segment_manager_main_t`, segment_manager.c:52-80,1880-1898.
pub struct SegmentManagerMain<'segment> {
    segment_managers: UnsafeCell<Pool<SegmentManager<'segment>>>,
    segment_name_counter: AtomicU32,
    defaults: SegmentManagerProperties,
}

// SAFETY: manager pool mutation belongs to Main Thread under WorkerBarrier;
// workers borrow only existing managers and never retain them across detach.
unsafe impl Sync for SegmentManagerMain<'_> {}

static SEGMENT_MAIN: OnceLock<SegmentManagerMain<'static>> = OnceLock::new();

impl SegmentManagerMain<'static> {
    /// VPP: `segment_manager_main_init`, segment_manager.c:1880-1898.
    pub fn init(defaults: SegmentManagerProperties) {
        assert!(
            SEGMENT_MAIN
                .set(Self {
                    segment_managers: UnsafeCell::new(Pool::new()),
                    segment_name_counter: AtomicU32::new(0),
                    defaults,
                })
                .is_ok(),
            "Segment Manager Main initializes once"
        );
    }

    #[inline(always)]
    pub fn global() -> Option<&'static Self> {
        SEGMENT_MAIN.get()
    }

    /// VPP: `segment_manager_free_safe`, segment_manager.c:566-579. The
    /// thread-zero main loop polls the queued removal under WorkerBarrier.
    pub(crate) fn remove_detached(index: u32) {
        hammer_runtime::enqueue_main_thread_future(async move {
            assert!(
                hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
                "detached Segment Manager removal requires Main Thread and stopped Data Workers"
            );
            let main = Self::global()
                .expect("Segment Manager Main remains initialized while Sessions exist");
            // SAFETY: the Main Thread holds WorkerBarrier before changing the pool.
            let managers = unsafe { &mut *main.segment_managers.get() };
            if managers.get(index).is_some_and(|manager| {
                manager.flags.contains(SegmentManagerFlags::DETACHED) && !manager.has_fifos()
            }) {
                assert!(
                    managers.remove(index).is_some(),
                    "detached Segment Manager remains allocated until removal"
                );
            }
        });
    }
}

impl<'segment> SegmentManagerMain<'segment> {
    /// VPP: `segment_manager_alloc`/`segment_manager_init_first`,
    /// segment_manager.c:389-488. Publication occurs only after all resources
    /// are ready, so allocation failure leaves the manager pool unchanged.
    pub fn allocate(
        &self,
        owner_app_worker: u32,
        properties: SegmentManagerProperties,
        worker_count: u32,
        backend: SsvmSegmentBackend,
        huge_page: bool,
        flags: SegmentManagerFlags,
    ) -> Result<u32, SegmentManagerError> {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "Segment Manager allocation requires Main Thread and stopped Data Workers"
        );
        let name = format!(
            "hammer-app-segment-{:x}",
            self.segment_name_counter.fetch_add(1, Ordering::Relaxed)
        );
        let manager = SegmentManager::init(
            owner_app_worker,
            properties,
            worker_count,
            backend,
            huge_page,
            flags,
            name,
        )?;
        // SAFETY: the Main Thread owns this pool while workers are stopped.
        let managers = unsafe { &mut *self.segment_managers.get() };
        let index = managers.insert(manager);
        managers.get_mut(index).expect("inserted manager").index = index;
        Ok(index)
    }

    /// # Safety
    /// Caller owns the worker execution or holds WorkerBarrier; the manager
    /// must not be detached until the returned borrow ends.
    #[inline(always)]
    pub unsafe fn get(&self, manager: u32) -> Option<&SegmentManager<'segment>> {
        // SAFETY: the caller preserves the pool entry lifetime.
        unsafe { &*self.segment_managers.get() }.get(manager)
    }

    /// VPP: `app_worker_alloc`, application_worker.c:15-34, followed by
    /// `application_alloc_worker_and_init`, application.c:986-1020.
    pub fn assign_owner(&self, manager: u32, worker: u32) {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "Segment Manager owner assignment requires Main Thread and stopped Data Workers"
        );
        // SAFETY: the new manager is not visible to Data Workers until the
        // application worker has been fully initialized.
        unsafe { &mut *self.segment_managers.get() }
            .get_mut(manager)
            .expect("new Segment Manager remains allocated")
            .owner_app_worker = worker;
    }

    /// VPP: `segment_manager_init_free`, segment_manager.c:566-584.
    pub fn detach(&self, manager: u32) -> Result<Option<()>, SegmentManagerError> {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "Segment Manager detach requires Main Thread and stopped Data Workers"
        );
        // SAFETY: Main Thread owns pool mutation while workers are stopped.
        let managers = unsafe { &mut *self.segment_managers.get() };
        let Some(entry) = managers.get_mut(manager) else {
            return Ok(None);
        };
        entry.mark_detached();
        if !entry.has_fifos() {
            assert!(
                managers.remove(manager).is_some(),
                "detached Segment Manager remains allocated until removal"
            );
        }
        Ok(Some(()))
    }

    #[inline(always)]
    pub const fn defaults(&self) -> SegmentManagerProperties {
        self.defaults
    }
}
