use crate::session::error::SessionError;

pub mod congestion;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportTxMode {
    Peek,
    Dequeue,
    Internal,
    Datagram,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportServiceType {
    VirtualCircuit,
    Connectionless,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportOptions {
    pub tx_mode: TransportTxMode,
    pub service_type: TransportServiceType,
}

impl TransportOptions {
    #[inline(always)]
    pub const fn new(tx_mode: TransportTxMode, service_type: TransportServiceType) -> Self {
        Self {
            tx_mode,
            service_type,
        }
    }

    #[inline(always)]
    pub const fn tx_mode(self) -> TransportTxMode {
        self.tx_mode
    }

    #[inline(always)]
    pub const fn service_type(self) -> TransportServiceType {
        self.service_type
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransportConnectionFlags {
    pub tx_paced: bool,
    pub no_lookup: bool,
    pub descheduled: bool,
    pub connectionless: bool,
    pub error: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransportSendFlags {
    pub deschedule: bool,
    pub postpone: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransportSendParams {
    pub send_space: u32,
    pub tx_offset: u32,
    pub send_mss: u16,
    pub max_burst_size: u32,
    pub bytes_dequeued: u32,
    pub flags: TransportSendFlags,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Pacer {
    pub bytes_per_second: u64,
    pub bucket: i64,
    pub last_update: u64,
    pub tokens_per_period: f32,
    pub min_burst: u32,
    pub max_burst: u32,
}

impl Pacer {
    pub const MIN_MSS: u32 = 1_460;
    pub const MIN_BURST: u32 = Self::MIN_MSS;
    pub const MAX_BURST_PACKETS: u32 = 43;
    pub const MAX_BURST: u32 = Self::MAX_BURST_PACKETS * Self::MIN_MSS;

    pub const fn new() -> Self {
        Self {
            bytes_per_second: 0,
            bucket: 0,
            last_update: 0,
            tokens_per_period: 0.0,
            min_burst: Self::MIN_BURST,
            max_burst: Self::MAX_BURST,
        }
    }

    #[inline(always)]
    pub const fn is_enabled(&self) -> bool {
        self.bytes_per_second != 0
    }

    #[inline(always)]
    pub fn update(&mut self, now: u64) -> u32 {
        let periods = now.saturating_sub(self.last_update);
        let increment = periods as f32 * self.tokens_per_period;
        if increment > 10.0 {
            self.last_update = now;
            self.bucket = (self.bucket + increment as i64).min(i64::from(self.max_burst));
        }
        if self.bucket >= 0 { self.max_burst } else { 0 }
    }

    #[inline(always)]
    pub fn update_bytes(&mut self, bytes: u32) {
        self.bucket = self.bucket.saturating_sub(i64::from(bytes));
    }
}

impl Default for Pacer {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct TransportConnection<E, O = u32> {
    pub session: crate::session::SessionHandle,
    pub connection_index: u32,
    pub worker_index: u32,
    pub flags: TransportConnectionFlags,
    pub endpoint: E,
    pub pacer: Pacer,
    pub opaque: O,
    pub cacheline_end: hammer_infra::align::CacheLineAlignMark,
}

impl<E, O> TransportConnection<E, O> {
    #[inline]
    pub fn is_descheduled(&self) -> bool {
        self.flags.descheduled
    }

    #[inline]
    pub fn is_connectionless(&self) -> bool {
        self.flags.connectionless
    }

    #[inline(always)]
    pub fn is_tx_paced(&self) -> bool {
        self.flags.tx_paced
    }

    #[inline(always)]
    pub fn clear_descheduled(&mut self, now: u64) {
        self.flags.descheduled = false;
        if self.is_tx_paced() {
            self.pacer.last_update = now;
            self.pacer.bucket = 0;
        }
    }
}

/// Static transport capability contract. The VFT below is retained only as a
/// migration shim and is not part of the ADR-0038 target path.
pub trait Transport<T> {
    type Connection;
    type Attribute;

    fn options(&self) -> TransportOptions;
    fn connect(
        &self,
        endpoint: &T,
        session: crate::session::SessionHandle,
    ) -> Result<u32, SessionError>;
    fn connect_stream(
        &self,
        endpoint: &T,
        session: crate::session::SessionHandle,
    ) -> Result<u32, SessionError>;
    fn start_listen(
        &self,
        endpoint: &T,
        session: crate::session::SessionHandle,
    ) -> Result<u32, SessionError>;
    fn stop_listen(&self, connection_index: u32) -> Result<u32, SessionError>;
    fn half_close(&self, connection_index: u32, worker_index: u32);
    fn close(&self, connection_index: u32, worker_index: u32);
    fn reset(&self, connection_index: u32, worker_index: u32);
    fn cleanup(&self, connection_index: u32, worker_index: u32);
    fn cleanup_half_open(&self, connection_index: u32);
    fn push_header(
        &self,
        connection_index: u32,
        worker_index: u32,
        buffers: &mut [u32],
        available_bytes: u32,
    ) -> u32;
    fn send_params(&self, connection_index: u32, worker_index: u32) -> TransportSendParams;
    fn update_time(&self, now: f64, worker_index: u32);
    fn flush_data(&self, connection_index: u32, worker_index: u32);
    fn custom_tx(
        &self,
        session: crate::session::SessionHandle,
        params: &mut TransportSendParams,
    ) -> usize;
    fn app_rx_event(&self, connection_index: u32, worker_index: u32) -> Result<(), SessionError>;
    fn connection(&self, connection_index: u32, worker_index: u32) -> Option<&Self::Connection>;
    fn listener(&self, connection_index: u32) -> Option<&Self::Connection>;
    fn half_open(&self, connection_index: u32) -> Option<&Self::Connection>;
    fn endpoint(&self, connection_index: u32, worker_index: u32) -> (T, T);
    fn listener_endpoint(&self, connection_index: u32) -> (T, T);
    fn attribute(
        &self,
        connection_index: u32,
        worker_index: u32,
        attribute: &mut Self::Attribute,
    ) -> Result<(), SessionError>;
}

pub trait TransportMain: Sized {
    type Config;
    type Endpoint;
    type LocalEndpoint;

    fn init(config: Self::Config) -> Result<Self, SessionError>;
    fn global() -> Result<&'static Self, SessionError>;
    fn mark_used(&self, endpoint: &Self::Endpoint) -> Result<(), SessionError>;
    fn share(&self, endpoint: &Self::Endpoint);
    fn release(&self, endpoint: &Self::Endpoint) -> Result<(), SessionError>;
    fn allocate_local(&self, endpoint: Self::Endpoint) -> Result<Self::Endpoint, SessionError>;
}
