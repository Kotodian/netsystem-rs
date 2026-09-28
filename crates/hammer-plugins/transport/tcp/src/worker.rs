use std::time::Instant;

use hammer_infra::align::{CACHE_LINE, CacheLineAlignMark};
use hammer_infra::fifo_queue::FifoQueue;
use hammer_infra::pool::Pool;
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;
use hammer_runtime::DataWorkerId;
use hammer_service::session::SessionQueueNext;

use super::lookup::TcpLookupState;
use super::timers::{TCP_TIMER_EXPIRY_BUDGET, TCP_TIMER_KIND_COUNT, TcpTimerToken};
use super::TcpConnection;

const DEFAULT_TCP_CONNECTION_CAPACITY: usize = 1024;

#[repr(C)]
pub struct TcpWorker {
    cacheline0: CacheLineAlignMark,
    pub(crate) connections: Pool<TcpConnection>,
    pending_deq_acked: Vec<u32>,
    pending_disconnects: Vec<u32>,
    pending_resets: Vec<u32>,
    pub(crate) time_us: f64,
    pub(crate) time_tstamp: u32,
    pub(crate) max_timers_per_loop: u32,
    pub(super) pending_timers: FifoQueue<TcpTimerToken>,
    cacheline1: CacheLineAlignMark,
    cached_opts: [u8; 40],
    tx_buffers: Vec<u32>,
    pending_cleanups: FifoQueue<TcpCleanupRequest>,
    pub(crate) tco_next_node: [SessionQueueNext; 2],
    pub(super) timer_wheel: TimerWheel1t2w2048sl<u32>,
    pub(super) expired_timers: Vec<u32>,
    pub(super) last_timer_update: Instant,
    pub(crate) time_origin: Instant,
    pub(crate) time_origin_seconds: Option<f64>,
    cacheline2: CacheLineAlignMark,
    pub(crate) lookup: TcpLookupState,
    pub(crate) protocol: u8,
}

struct TcpCleanupRequest {
    free_time: Instant,
    connection_index: u32,
}

const _: () = {
    assert!(core::mem::align_of::<TcpWorker>() == CACHE_LINE);
    assert!(core::mem::offset_of!(TcpWorker, cacheline0) == 0);
    assert!(core::mem::offset_of!(TcpWorker, cacheline1) % CACHE_LINE == 0);
    assert!(core::mem::offset_of!(TcpWorker, cacheline2) % CACHE_LINE == 0);
};

impl TcpWorker {
    #[inline]
    pub fn new(worker: DataWorkerId, protocol: u8) -> Self {
        let time_origin = Instant::now();
        Self {
            cacheline0: CacheLineAlignMark,
            connections: Pool::with_capacity(DEFAULT_TCP_CONNECTION_CAPACITY),
            pending_deq_acked: Vec::with_capacity(256),
            pending_disconnects: Vec::with_capacity(256),
            pending_resets: Vec::with_capacity(256),
            time_us: 0.0,
            time_tstamp: 0,
            max_timers_per_loop: 10,
            pending_timers: FifoQueue::new(),
            cacheline1: CacheLineAlignMark,
            cached_opts: [0; 40],
            tx_buffers: Vec::new(),
            pending_cleanups: FifoQueue::new(),
            tco_next_node: [SessionQueueNext::from_slot(0); 2],
            timer_wheel: TimerWheel1t2w2048sl::with_timer_ids(
                TCP_TIMER_EXPIRY_BUDGET,
                TCP_TIMER_KIND_COUNT,
            ),
            expired_timers: Vec::new(),
            last_timer_update: time_origin,
            time_origin,
            time_origin_seconds: None,
            cacheline2: CacheLineAlignMark,
            lookup: TcpLookupState::new(worker),
            protocol,
        }
    }

    #[inline]
    pub(crate) fn has_connection_capacity(&self) -> bool {
        self.connections.len() < self.connections.capacity()
    }

    #[inline]
    pub(crate) fn insert_connection(&mut self, connection: TcpConnection) -> u32 {
        let index = self.connections.insert(connection);
        self.connections
            .get_mut(index)
            .expect("inserted TCP connection remains in its pool")
            .base
            .connection_index = index;
        index
    }

    #[inline]
    pub(crate) fn connection(&self, index: u32) -> Option<&TcpConnection> {
        self.connections.get(index)
    }

    #[inline]
    pub(crate) fn connection_mut(&mut self, index: u32) -> Option<&mut TcpConnection> {
        self.connections.get_mut(index)
    }

    #[inline]
    pub(crate) fn remove_connection(&mut self, index: u32) -> TcpConnection {
        self.connections
            .remove(index)
            .expect("TCP connection index remains live during removal")
    }
}
