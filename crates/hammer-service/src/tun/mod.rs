use std::mem::transmute;
use std::sync::{Arc, Mutex, OnceLock};

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, DriverNode, FrameIndex, NetworkOpaque, Node, NodeId,
    NodeProcessFn, NodeRegistration, NodeResult, NodeRuntimeData, PacketTrace, SecondaryOpaque,
    TapEthernetMetadata, TraceFormatter, add_packet_trace, unlikely,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::vec::Vec;

use crate::net::ip::parse_ip_packet;

use crate::interface::InterfaceControlHandle;

const DEFAULT_TUN_RECV_BATCH: usize = 256;
const ETHERNET_HEADER_LEN: usize = 14;
const ETHERTYPE_IP4: u16 = 0x0800;
const ETHERTYPE_IP6: u16 = 0x86dd;

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct TunOpaque {
    tap_ethernet: Option<TapEthernetMetadata>,
    reserved: [u64; 4],
}

const _: () = assert!(core::mem::size_of::<TunOpaque>() <= core::mem::size_of::<SecondaryOpaque>());

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriverScheduleMode {
    Poll,
    Interrupt,
    Adaptive,
}

#[derive(Clone)]
pub struct DeviceMain {
    inner: Arc<Mutex<DeviceMainInner>>,
}

#[derive(Default)]
struct DeviceMainInner {
    rx_queues: Vec<RxQueue>,
    tx_queues: Vec<TxQueue>,
}

#[derive(Debug, Clone, Copy)]
struct RxQueue {
    input_node: NodeId,
    mode: DriverScheduleMode,
    interrupt_pending: bool,
}

#[derive(Debug, Clone, Copy)]
struct TxQueue {
    output_node: NodeId,
}

impl Default for DeviceMain {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl DeviceMain {
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(DeviceMainInner::default())),
        }
    }

    pub fn register_rx_queue(&self, input_node: NodeId, mode: DriverScheduleMode) -> u32 {
        let mut inner = self.inner.lock().expect("device main poisoned");
        let index = inner.rx_queues.len() as u32;
        inner.rx_queues.push(RxQueue {
            input_node,
            mode,
            interrupt_pending: false,
        });
        index
    }

    pub fn register_tx_queue(&self, output_node: NodeId) -> u32 {
        let mut inner = self.inner.lock().expect("device main poisoned");
        let index = inner.tx_queues.len() as u32;
        inner.tx_queues.push(TxQueue { output_node });
        index
    }

    pub fn mark_rx_interrupt_pending(&self, rx_queue: u32) -> CoreResult<NodeId> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| CoreError::internal("device main poisoned"))?;
        let queue = inner
            .rx_queues
            .get_mut(rx_queue as usize)
            .ok_or_else(|| CoreError::internal("device RX queue is not registered"))?;
        queue.interrupt_pending = true;
        Ok(queue.input_node)
    }

    pub fn consume_rx_interrupt_pending(&self, rx_queue: u32) -> CoreResult<bool> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| CoreError::internal("device main poisoned"))?;
        let queue = inner
            .rx_queues
            .get_mut(rx_queue as usize)
            .ok_or_else(|| CoreError::internal("device RX queue is not registered"))?;
        match queue.mode {
            DriverScheduleMode::Poll => Ok(true),
            DriverScheduleMode::Interrupt | DriverScheduleMode::Adaptive => {
                let pending = queue.interrupt_pending;
                queue.interrupt_pending = false;
                Ok(pending)
            }
        }
    }

    pub fn tx_node(&self, tx_queue: u32) -> CoreResult<NodeId> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| CoreError::internal("device main poisoned"))?;
        inner
            .tx_queues
            .get(tx_queue as usize)
            .map(|queue| queue.output_node)
            .ok_or_else(|| CoreError::internal("device TX queue is not registered"))
    }
}

pub trait TunPacketSource {
    fn recv_frame(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        interface_id: &str,
        mode: TunDriverMode,
        max: usize,
    ) -> CoreResult<usize>;
}

pub trait TunPacketSink {
    fn send_frame(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        mode: TunDriverMode,
    ) -> CoreResult<usize>;
}

#[doc(hidden)]
pub enum TunInputBackend {
    Memory(MemoryTunInput),
    Scripted(RealTunInput<ScriptedTunIo>),
}

#[doc(hidden)]
pub enum TunOutputBackend {
    Memory(MemoryTunOutput),
    Scripted(RealTunOutput<ScriptedTunIo>),
}

#[doc(hidden)]
pub trait IntoTunInputBackend {
    fn into_tun_input_backend(self) -> TunInputBackend;
}

#[doc(hidden)]
pub trait IntoTunOutputBackend {
    fn into_tun_output_backend(self) -> TunOutputBackend;
}

impl TunInputBackend {
    #[inline]
    fn recv_frame(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        interface_id: &str,
        mode: TunDriverMode,
        max: usize,
    ) -> CoreResult<usize> {
        match self {
            Self::Memory(input) => input.recv_frame(runtime, frame, interface_id, mode, max),
            Self::Scripted(input) => input.recv_frame(runtime, frame, interface_id, mode, max),
        }
    }
}

impl TunOutputBackend {
    #[inline]
    fn send_frame(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        mode: TunDriverMode,
    ) -> CoreResult<usize> {
        match self {
            Self::Memory(output) => output.send_frame(runtime, frame, mode),
            Self::Scripted(output) => output.send_frame(runtime, frame, mode),
        }
    }
}

impl IntoTunInputBackend for MemoryTunInput {
    #[inline]
    fn into_tun_input_backend(self) -> TunInputBackend {
        TunInputBackend::Memory(self)
    }
}

impl IntoTunOutputBackend for MemoryTunOutput {
    #[inline]
    fn into_tun_output_backend(self) -> TunOutputBackend {
        TunOutputBackend::Memory(self)
    }
}

impl IntoTunInputBackend for RealTunInput<ScriptedTunIo> {
    #[inline]
    fn into_tun_input_backend(self) -> TunInputBackend {
        TunInputBackend::Scripted(self)
    }
}

impl IntoTunOutputBackend for RealTunOutput<ScriptedTunIo> {
    #[inline]
    fn into_tun_output_backend(self) -> TunOutputBackend {
        TunOutputBackend::Scripted(self)
    }
}

struct TunInputRuntime {
    input: TunInputBackend,
    interface_id: String,
    interface_index: Option<u32>,
    interface_control: Option<InterfaceControlHandle>,
    device_main: Option<DeviceMain>,
    rx_queue: Option<u32>,
    next: NodeId,
    max_batch: usize,
    mode: TunDriverMode,
}

struct TunOutputRuntime {
    output: TunOutputBackend,
    mode: TunDriverMode,
}

impl TunInputRuntime {
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        if let (Some(device_main), Some(rx_queue)) = (&self.device_main, self.rx_queue)
            && !device_main.consume_rx_interrupt_pending(rx_queue)?
        {
            return Ok(NodeResult::drop());
        }
        let max_batch = self.max_batch.min(frame.remaining_capacity());
        let first_new = frame.pending_len();
        let received =
            self.input
                .recv_frame(runtime, frame, &self.interface_id, self.mode, max_batch)?;
        let interface_index = self.ingress_interface_index()?;
        if let Some(interface_index) = interface_index {
            for index in frame.pending_indices()[first_new..].iter().copied() {
                let mut buffer = runtime.get_buffer_mut(index)?;
                let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
                network.sw_if_index[0] = interface_index;
            }
        }
        if let Some(current_node) = runtime.current_node()
            && unlikely(runtime.may_mark_trace(current_node))
        {
            for index in frame.pending_indices()[first_new..].iter().copied() {
                runtime.try_mark_trace(current_node, index)?;
                add_packet_trace!(
                    runtime,
                    index,
                    TunInputTrace {
                        interface_index,
                        mode: self.mode,
                        received,
                    },
                )?;
            }
        }
        if frame.has_pending() {
            Ok(NodeResult::next_current(self.next))
        } else {
            Ok(NodeResult::drop())
        }
    }

    #[inline]
    fn ingress_interface_index(&self) -> CoreResult<Option<u32>> {
        if let Some(interface_control) = &self.interface_control {
            return interface_control
                .interface_index(&self.interface_id)
                .map(Some)
                .ok_or_else(|| {
                    CoreError::internal(format!(
                        "interface {} is not registered",
                        self.interface_id
                    ))
                });
        }
        Ok(self.interface_index)
    }
}

impl TunOutputRuntime {
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let pending = frame.pending_len();
        for index in frame.pending_indices().iter().copied() {
            add_packet_trace!(
                runtime,
                index,
                TunOutputTrace {
                    mode: self.mode,
                    pending,
                },
            )?;
        }
        self.output.send_frame(runtime, frame, self.mode)?;
        Ok(NodeResult::drop())
    }
}

#[derive(Clone)]
pub struct TunMain {
    id: usize,
    inner: Arc<Mutex<TunMainInner>>,
}

struct TunMainInner {
    inputs: Vec<TunInputRuntime>,
    outputs: Vec<TunOutputRuntime>,
}

impl Default for TunMain {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl TunMain {
    #[inline]
    pub fn new() -> Self {
        let inner = Arc::new(Mutex::new(TunMainInner {
            inputs: Vec::new(),
            outputs: Vec::new(),
        }));
        let mut mains = tun_mains().lock().expect("tun main registry poisoned");
        let id = mains.len();
        mains.push(Arc::clone(&inner));
        Self { id, inner }
    }

    #[inline]
    fn default_main() -> Self {
        static DEFAULT: OnceLock<TunMain> = OnceLock::new();
        DEFAULT.get_or_init(TunMain::new).clone()
    }

    #[inline]
    fn register_input(&self, input: TunInputRuntime) -> usize {
        let mut inner = self.inner.lock().expect("tun main poisoned");
        let slot = inner.inputs.len();
        inner.inputs.push(input);
        slot
    }

    #[inline]
    fn register_output(&self, output: TunOutputRuntime) -> usize {
        let mut inner = self.inner.lock().expect("tun main poisoned");
        let slot = inner.outputs.len();
        inner.outputs.push(output);
        slot
    }

    fn inner_for_runtime_data(data: NodeRuntimeData) -> CoreResult<Arc<Mutex<TunMainInner>>> {
        let main_id = data.usize_word(0)?;
        let mains = tun_mains()
            .lock()
            .map_err(|_| CoreError::internal("tun main registry poisoned"))?;
        mains
            .get(main_id)
            .cloned()
            .ok_or_else(|| CoreError::internal("TUN main runtime data is invalid"))
    }

    #[inline]
    fn runtime_data(&self, slot: usize) -> CoreResult<NodeRuntimeData> {
        Ok(NodeRuntimeData::from_words([
            u64::try_from(self.id).map_err(|_| CoreError::internal("TUN main id overflow"))?,
            u64::try_from(slot).map_err(|_| CoreError::internal("TUN node slot overflow"))?,
            0,
            0,
        ]))
    }
}

fn tun_mains() -> &'static Mutex<Vec<Arc<Mutex<TunMainInner>>>> {
    static MAINS: OnceLock<Mutex<Vec<Arc<Mutex<TunMainInner>>>>> = OnceLock::new();
    MAINS.get_or_init(|| Mutex::new(Vec::new()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TunBufferSendResult {
    Complete,
    Partial(usize),
    Backpressure,
}

pub trait TunBufferIo {
    fn try_recv_buffer(&mut self, buffer: &mut [u8]) -> CoreResult<Option<usize>>;

    fn max_recv_len(&self) -> Option<usize> {
        None
    }

    fn try_send_buffer(&mut self, packet: &[u8], offset: usize) -> CoreResult<TunBufferSendResult>;

    fn try_send_buffers(
        &mut self,
        segments: &[&[u8]],
        offset: usize,
        total_len: usize,
    ) -> CoreResult<TunBufferSendResult> {
        if segments.len() > 1 {
            return Err(CoreError::internal(
                "chained TUN TX requires vectored IO support",
            ));
        }
        if offset >= total_len {
            return Ok(TunBufferSendResult::Complete);
        }
        self.try_send_buffer(segments.first().copied().unwrap_or_default(), offset)
    }
}

pub struct RealTunInput<I> {
    io: I,
}

impl<I> RealTunInput<I> {
    #[inline]
    pub fn new(io: I) -> Self {
        Self { io }
    }

    #[inline]
    pub fn into_inner(self) -> I {
        self.io
    }
}

impl<I> TunPacketSource for RealTunInput<I>
where
    I: TunBufferIo,
{
    #[inline]
    fn recv_frame(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        interface_id: &str,
        mode: TunDriverMode,
        max: usize,
    ) -> CoreResult<usize> {
        if max == 0 {
            return Ok(0);
        }
        if mode.is_tap() {
            return Err(CoreError::internal("real TUN driver only supports L3 TUN"));
        }
        let mut received = 0usize;
        while received < max {
            let index = runtime.alloc_index()?;
            let len = match self.recv_into_buffer(runtime, index) {
                Ok(Some(len)) => len,
                Ok(None) => {
                    runtime.free_index(index);
                    break;
                }
                Err(err) => {
                    runtime.free_index(index);
                    return Err(err);
                }
            };
            if len == 0 {
                runtime.free_index(index);
                break;
            };
            self.set_l3_metadata(runtime, index, interface_id)?;
            if let Err(err) = frame.push_index(index) {
                runtime.free_index(index);
                return Err(err);
            }
            received += 1;
        }
        Ok(received)
    }
}

impl<I> RealTunInput<I>
where
    I: TunBufferIo,
{
    fn recv_into_buffer(
        &mut self,
        runtime: &DataPlaneRuntime,
        index: hammer_adapter::BufferIndex,
    ) -> CoreResult<Option<usize>> {
        let mut buffer = runtime.get_buffer_mut(index)?;
        let dst = buffer.writable_tail_mut();
        let dst_len = dst.len();
        let Some(len) = self.io.try_recv_buffer(dst)? else {
            return Ok(None);
        };
        if len == dst_len && self.io.max_recv_len().is_none_or(|max| dst_len < max) {
            return Err(CoreError::internal(
                "TUN packet filled receive buffer; possible truncation",
            ));
        }
        buffer.commit_writable_tail(len)?;
        Ok(Some(len))
    }

    fn set_l3_metadata(
        &self,
        runtime: &DataPlaneRuntime,
        index: hammer_adapter::BufferIndex,
        _: &str,
    ) -> CoreResult<()> {
        let buffer = runtime.get_buffer(index)?;
        if parse_ip_packet(buffer.current()).is_err() {
            return Err(CoreError::internal(
                "received TUN packet has invalid IP header",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct TunPendingTx {
    index: hammer_adapter::BufferIndex,
    offset: usize,
}

struct TunPendingTxRing {
    slots: Vec<Option<TunPendingTx>>,
    head: usize,
    len: usize,
}

impl TunPendingTxRing {
    fn with_capacity(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(None);
        }
        Self {
            slots,
            head: 0,
            len: 0,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.slots.len()
    }

    fn set_capacity(&mut self, capacity: usize) {
        debug_assert!(
            self.len == 0,
            "TUN TX ring capacity is only changed before use"
        );
        *self = Self::with_capacity(capacity);
    }

    #[inline]
    fn front(&self) -> Option<TunPendingTx> {
        if self.len == 0 {
            return None;
        }
        self.slots[self.head]
    }

    #[inline]
    fn front_mut(&mut self) -> Option<&mut TunPendingTx> {
        if self.len == 0 {
            return None;
        }
        self.slots[self.head].as_mut()
    }

    fn push_back(&mut self, pending: TunPendingTx) -> CoreResult<()> {
        if self.len == self.capacity() {
            return Err(CoreError::internal("real TUN TX ring full"));
        }
        let slot = (self.head + self.len) % self.capacity();
        self.slots[slot] = Some(pending);
        self.len += 1;
        Ok(())
    }

    fn pop_front(&mut self) -> Option<TunPendingTx> {
        if self.len == 0 {
            return None;
        }
        let pending = self.slots[self.head].take();
        self.head = (self.head + 1) % self.capacity();
        self.len -= 1;
        if self.len == 0 {
            self.head = 0;
        }
        pending
    }
}

pub struct RealTunOutput<I> {
    io: I,
    pending_tx: TunPendingTxRing,
    tx_ring_capacity: usize,
}

#[derive(Clone, Default)]
pub struct ScriptedTunIo {
    inner: Arc<Mutex<ScriptedTunIoInner>>,
}

#[derive(Default)]
struct ScriptedTunIoInner {
    rx: Vec<Vec<u8>>,
    rx_head: usize,
    recv_calls: usize,
    send_calls: usize,
    send_results: Vec<TunBufferSendResult>,
    send_result_head: usize,
    sent: Vec<Vec<u8>>,
    sent_segment_counts: Vec<usize>,
}

impl ScriptedTunIo {
    #[inline]
    pub fn inject(&self, packet: Vec<u8>) {
        self.inner
            .lock()
            .expect("scripted TUN IO poisoned")
            .rx
            .push(packet);
    }

    #[inline]
    pub fn push_send_result(&self, result: TunBufferSendResult) {
        self.inner
            .lock()
            .expect("scripted TUN IO poisoned")
            .send_results
            .push(result);
    }

    #[inline]
    pub fn recv_calls(&self) -> usize {
        self.inner
            .lock()
            .expect("scripted TUN IO poisoned")
            .recv_calls
    }

    #[inline]
    pub fn send_calls(&self) -> usize {
        self.inner
            .lock()
            .expect("scripted TUN IO poisoned")
            .send_calls
    }

    #[inline]
    pub fn sent(&self) -> Vec<Vec<u8>> {
        self.inner
            .lock()
            .expect("scripted TUN IO poisoned")
            .sent
            .clone()
    }

    #[inline]
    pub fn sent_segment_counts(&self) -> Vec<usize> {
        self.inner
            .lock()
            .expect("scripted TUN IO poisoned")
            .sent_segment_counts
            .clone()
    }
}

impl TunBufferIo for ScriptedTunIo {
    fn try_recv_buffer(&mut self, buffer: &mut [u8]) -> CoreResult<Option<usize>> {
        let mut inner = self.inner.lock().expect("scripted TUN IO poisoned");
        inner.recv_calls += 1;
        if inner.rx_head >= inner.rx.len() {
            return Ok(None);
        }
        let packet = &inner.rx[inner.rx_head];
        let len = packet.len().min(buffer.len());
        buffer[..len].copy_from_slice(&packet[..len]);
        inner.rx_head += 1;
        Ok(Some(len))
    }

    fn try_send_buffer(&mut self, packet: &[u8], offset: usize) -> CoreResult<TunBufferSendResult> {
        self.record_send(&[packet], offset, packet.len())
    }

    fn try_send_buffers(
        &mut self,
        segments: &[&[u8]],
        offset: usize,
        total_len: usize,
    ) -> CoreResult<TunBufferSendResult> {
        self.record_send(segments, offset, total_len)
    }
}

impl ScriptedTunIo {
    fn record_send(
        &mut self,
        segments: &[&[u8]],
        offset: usize,
        total_len: usize,
    ) -> CoreResult<TunBufferSendResult> {
        let mut inner = self.inner.lock().expect("scripted TUN IO poisoned");
        inner.send_calls += 1;
        let result = if inner.send_result_head < inner.send_results.len() {
            let result = inner.send_results[inner.send_result_head];
            inner.send_result_head += 1;
            result
        } else {
            TunBufferSendResult::Complete
        };
        inner.sent_segment_counts.push(segments.len());
        match result {
            TunBufferSendResult::Complete => {
                let mut sent = Vec::with_capacity(total_len.saturating_sub(offset));
                extend_segments_from_offset(&mut sent, segments, offset, total_len);
                inner.sent.push(sent);
            }
            TunBufferSendResult::Partial(next_offset) => {
                let take = next_offset.saturating_sub(offset).min(total_len - offset);
                let mut sent = Vec::with_capacity(take);
                extend_segments_from_offset(&mut sent, segments, offset, offset + take);
                inner.sent.push(sent);
            }
            TunBufferSendResult::Backpressure => {}
        }
        Ok(result)
    }
}

fn extend_segments_from_offset(
    out: &mut Vec<u8>,
    segments: &[&[u8]],
    offset: usize,
    end_offset: usize,
) {
    let mut base = 0usize;
    for segment in segments {
        let end = base + segment.len();
        if offset < end && base < end_offset {
            let start_in_segment = offset.saturating_sub(base);
            let end_in_segment = (end_offset - base).min(segment.len());
            out.extend_from_slice(&segment[start_in_segment..end_in_segment]);
        }
        base = end;
    }
}

impl<I> RealTunOutput<I> {
    #[inline]
    pub fn new(io: I) -> Self {
        Self {
            io,
            pending_tx: TunPendingTxRing::with_capacity(DEFAULT_TUN_RECV_BATCH),
            tx_ring_capacity: DEFAULT_TUN_RECV_BATCH,
        }
    }

    #[inline]
    pub fn with_tx_ring_capacity(mut self, tx_ring_capacity: usize) -> Self {
        let tx_ring_capacity = tx_ring_capacity.max(1);
        self.tx_ring_capacity = tx_ring_capacity;
        self.pending_tx.set_capacity(tx_ring_capacity);
        self
    }

    #[inline]
    pub fn into_inner(self) -> I {
        self.io
    }
}

impl<I> RealTunOutput<I>
where
    I: TunBufferIo,
{
    fn drain_pending_tx(
        &mut self,
        runtime: &DataPlaneRuntime,
        mode: TunDriverMode,
    ) -> CoreResult<usize> {
        if mode.is_tap() {
            return Err(CoreError::internal("real TUN driver only supports L3 TUN"));
        }
        let mut completed = 0usize;
        while let Some(pending) = self.pending_tx.front() {
            let send_result = self.try_send_pending_tx(runtime, pending);
            match send_result {
                Ok(TunBufferSendResult::Complete) => {
                    self.pending_tx.pop_front();
                    runtime.free_index(pending.index);
                    completed += 1;
                }
                Ok(TunBufferSendResult::Partial(offset)) => {
                    let total_len = packet_total_len(runtime, pending.index)?;
                    validate_tun_tx_partial(pending.offset, offset, total_len)?;
                    if offset == total_len {
                        self.pending_tx.pop_front();
                        runtime.free_index(pending.index);
                        completed += 1;
                        continue;
                    }
                    if let Some(head) = self.pending_tx.front_mut() {
                        head.offset = offset;
                    }
                    break;
                }
                Ok(TunBufferSendResult::Backpressure) => break,
                Err(err) => return Err(err),
            }
        }
        Ok(completed)
    }

    #[inline]
    fn try_send_pending_tx(
        &mut self,
        runtime: &DataPlaneRuntime,
        pending: TunPendingTx,
    ) -> CoreResult<TunBufferSendResult> {
        let packet = runtime.copy_packet(pending.index)?;
        let total_len = packet.len();
        if pending.offset > total_len {
            return Err(CoreError::internal("TUN TX offset exceeds packet length"));
        }
        if pending.offset == total_len {
            return Ok(TunBufferSendResult::Complete);
        }
        self.io.try_send_buffer(&packet, pending.offset)
    }
}

impl<I> TunPacketSink for RealTunOutput<I>
where
    I: TunBufferIo,
{
    #[inline]
    fn send_frame(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        mode: TunDriverMode,
    ) -> CoreResult<usize> {
        if mode.is_tap() {
            runtime.free_frame(frame);
            return Err(CoreError::internal("real TUN driver only supports L3 TUN"));
        }
        let batch_len = frame.pending_len();
        self.drain_pending_tx(runtime, mode)?;
        if self.pending_tx.len().saturating_add(batch_len) > self.tx_ring_capacity {
            runtime.free_frame(frame);
            return Err(CoreError::internal("real TUN TX ring full"));
        }
        for index in frame.drain_pending() {
            self.pending_tx
                .push_back(TunPendingTx { index, offset: 0 })?;
        }
        self.drain_pending_tx(runtime, mode)?;
        Ok(batch_len)
    }
}

#[inline]
fn packet_total_len(
    runtime: &DataPlaneRuntime,
    index: hammer_adapter::BufferIndex,
) -> CoreResult<usize> {
    let packet = runtime.get_buffer(index)?;
    packet
        .current_len()
        .checked_add(packet.total_len_not_including_first())
        .ok_or_else(|| CoreError::internal("TUN TX packet length overflow"))
}

#[inline]
fn validate_tun_tx_partial(previous: usize, next: usize, total_len: usize) -> CoreResult<()> {
    if next <= previous {
        return Err(CoreError::internal(format!(
            "non-advancing TUN TX partial offset: {next} <= {previous}"
        )));
    }
    if next > total_len {
        return Err(CoreError::internal(format!(
            "TUN TX partial offset exceeds packet length: {next} > {total_len}"
        )));
    }
    Ok(())
}

#[derive(Clone)]
pub struct TunDeviceEventSource {
    device_main: DeviceMain,
    rx_queue: Option<u32>,
    tx_queue: Option<u32>,
}

impl TunDeviceEventSource {
    #[inline]
    pub fn new(device_main: DeviceMain, rx_queue: Option<u32>, tx_queue: Option<u32>) -> Self {
        Self {
            device_main,
            rx_queue,
            tx_queue,
        }
    }

    #[inline]
    pub fn input(device_main: DeviceMain, rx_queue: u32) -> Self {
        Self::new(device_main, Some(rx_queue), None)
    }

    #[inline]
    pub fn output(device_main: DeviceMain, tx_queue: u32) -> Self {
        Self::new(device_main, None, Some(tx_queue))
    }

    #[inline]
    pub fn schedule_readable(&self, runtime: &DataPlaneRuntime) -> CoreResult<()> {
        let rx_queue = self
            .rx_queue
            .ok_or_else(|| CoreError::internal("TUN device RX queue is not configured"))?;
        let input = self.device_main.mark_rx_interrupt_pending(rx_queue)?;
        schedule_empty_driver_frame(runtime, input)
    }

    #[inline]
    pub fn schedule_writable(&self, runtime: &DataPlaneRuntime) -> CoreResult<()> {
        let tx_queue = self
            .tx_queue
            .ok_or_else(|| CoreError::internal("TUN device TX queue is not configured"))?;
        let output = self.device_main.tx_node(tx_queue)?;
        schedule_empty_driver_frame(runtime, output)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TunDriverMode {
    Tun,
    Tap,
}

impl TunDriverMode {
    #[inline(always)]
    pub const fn from_tap(tap: bool) -> Self {
        if tap { Self::Tap } else { Self::Tun }
    }

    #[inline(always)]
    pub const fn is_tap(self) -> bool {
        matches!(self, Self::Tap)
    }
}

#[inline]
fn schedule_empty_driver_frame(runtime: &DataPlaneRuntime, node: NodeId) -> CoreResult<()> {
    let frame = runtime.alloc_frame_index()?;
    schedule_allocated_driver_frame(runtime, node, frame)
}

#[inline]
fn schedule_allocated_driver_frame(
    runtime: &DataPlaneRuntime,
    node: NodeId,
    frame: FrameIndex,
) -> CoreResult<()> {
    match runtime.schedule_driver_frame(node, frame) {
        Ok(()) => Ok(()),
        Err(err) => {
            let _ = runtime.free_frame_index(frame);
            Err(err)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TunInputTrace {
    pub interface_index: Option<u32>,
    pub mode: TunDriverMode,
    pub received: usize,
}

impl TunInputTrace {
    pub const ENCODED_LEN: usize = 14;

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::ENCODED_LEN {
            return None;
        }
        let interface_index = match bytes[0] {
            0 => None,
            1 => Some(u32::from_le_bytes(bytes[1..5].try_into().ok()?)),
            _ => return None,
        };
        let mode = decode_tun_driver_mode(bytes[5])?;
        let received = usize::try_from(u64::from_le_bytes(bytes[6..14].try_into().ok()?)).ok()?;
        Some(Self {
            interface_index,
            mode,
            received,
        })
    }
}

impl PacketTrace for TunInputTrace {
    #[inline]
    fn encode_trace(&self, out: &mut hammer_infra::vec::Vec<u8>) {
        out.push(u8::from(self.interface_index.is_some()));
        out.extend_from_slice(&self.interface_index.unwrap_or_default().to_le_bytes());
        out.push(encode_tun_driver_mode(self.mode));
        out.extend_from_slice(&(self.received as u64).to_le_bytes());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TunOutputTrace {
    pub mode: TunDriverMode,
    pub pending: usize,
}

impl TunOutputTrace {
    pub const ENCODED_LEN: usize = 9;

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::ENCODED_LEN {
            return None;
        }
        let mode = decode_tun_driver_mode(bytes[0])?;
        let pending = usize::try_from(u64::from_le_bytes(bytes[1..9].try_into().ok()?)).ok()?;
        Some(Self { mode, pending })
    }
}

impl PacketTrace for TunOutputTrace {
    #[inline]
    fn encode_trace(&self, out: &mut hammer_infra::vec::Vec<u8>) {
        out.push(encode_tun_driver_mode(self.mode));
        out.extend_from_slice(&(self.pending as u64).to_le_bytes());
    }
}

fn format_tun_input_trace(bytes: &[u8]) -> String {
    match TunInputTrace::decode(bytes) {
        Some(trace) => format!("{trace:?}"),
        None => format!("TunInputTrace invalid={bytes:?}"),
    }
}

fn format_tun_output_trace(bytes: &[u8]) -> String {
    match TunOutputTrace::decode(bytes) {
        Some(trace) => format!("{trace:?}"),
        None => format!("TunOutputTrace invalid={bytes:?}"),
    }
}

#[inline]
fn encode_tun_driver_mode(mode: TunDriverMode) -> u8 {
    match mode {
        TunDriverMode::Tun => 0,
        TunDriverMode::Tap => 1,
    }
}

#[inline]
fn decode_tun_driver_mode(value: u8) -> Option<TunDriverMode> {
    match value {
        0 => Some(TunDriverMode::Tun),
        1 => Some(TunDriverMode::Tap),
        _ => None,
    }
}

#[derive(Clone)]
pub struct TunInputDriverNode {
    node_name: &'static str,
    main: TunMain,
    slot: usize,
    runtime_data: NodeRuntimeData,
    next: NodeId,
}

impl TunInputDriverNode {
    #[inline]
    pub fn new<I>(input: I, interface_id: impl Into<String>, next: NodeId) -> Self
    where
        I: IntoTunInputBackend,
    {
        Self::new_with_main(TunMain::default_main(), input, interface_id, next)
    }

    #[inline]
    pub fn new_with_main<I>(
        main: TunMain,
        input: I,
        interface_id: impl Into<String>,
        next: NodeId,
    ) -> Self
    where
        I: IntoTunInputBackend,
    {
        let slot = main.register_input(TunInputRuntime {
            input: input.into_tun_input_backend(),
            interface_id: interface_id.into(),
            interface_index: None,
            interface_control: None,
            device_main: None,
            rx_queue: None,
            next,
            max_batch: DEFAULT_TUN_RECV_BATCH,
            mode: TunDriverMode::Tun,
        });
        let runtime_data = main.runtime_data(slot).expect("TUN input runtime data");
        Self {
            node_name: "tun-input-driver",
            main,
            slot,
            runtime_data,
            next,
        }
    }

    #[inline]
    pub fn with_interface_index(self, interface_index: u32) -> Self {
        let mut inner = self.main.inner.lock().expect("tun main poisoned");
        inner
            .inputs
            .get_mut(self.slot)
            .expect("TUN input runtime slot")
            .interface_index = Some(interface_index);
        drop(inner);
        self
    }

    #[inline]
    pub fn with_interface_control(self, interface_control: InterfaceControlHandle) -> Self {
        let mut inner = self.main.inner.lock().expect("tun main poisoned");
        inner
            .inputs
            .get_mut(self.slot)
            .expect("TUN input runtime slot")
            .interface_control = Some(interface_control);
        drop(inner);
        self
    }

    #[inline]
    pub fn with_rx_queue(self, device_main: DeviceMain, rx_queue: u32) -> Self {
        let mut inner = self.main.inner.lock().expect("tun main poisoned");
        let input = inner
            .inputs
            .get_mut(self.slot)
            .expect("TUN input runtime slot");
        input.device_main = Some(device_main);
        input.rx_queue = Some(rx_queue);
        drop(inner);
        self
    }

    pub fn bind_rx_queue(&self, device_main: DeviceMain, rx_queue: u32) -> CoreResult<()> {
        let mut inner = self
            .main
            .inner
            .lock()
            .map_err(|_| CoreError::internal("tun main poisoned"))?;
        let input = inner
            .inputs
            .get_mut(self.slot)
            .ok_or_else(|| CoreError::internal("TUN input runtime slot is invalid"))?;
        input.device_main = Some(device_main);
        input.rx_queue = Some(rx_queue);
        Ok(())
    }

    #[inline]
    pub fn with_max_batch(self, max_batch: usize) -> Self {
        let mut inner = self.main.inner.lock().expect("tun main poisoned");
        inner
            .inputs
            .get_mut(self.slot)
            .expect("TUN input runtime slot")
            .max_batch = max_batch;
        drop(inner);
        self
    }

    #[inline]
    pub fn with_tap(self, tap: bool) -> Self {
        let mut inner = self.main.inner.lock().expect("tun main poisoned");
        inner
            .inputs
            .get_mut(self.slot)
            .expect("TUN input runtime slot")
            .mode = TunDriverMode::from_tap(tap);
        drop(inner);
        self
    }

    #[inline]
    pub fn with_mode(self, mode: TunDriverMode) -> Self {
        let mut inner = self.main.inner.lock().expect("tun main poisoned");
        inner
            .inputs
            .get_mut(self.slot)
            .expect("TUN input runtime slot")
            .mode = mode;
        drop(inner);
        self
    }

    #[inline]
    pub fn with_node_name(mut self, node_name: &'static str) -> Self {
        self.node_name = node_name;
        self
    }
}

impl Node for TunInputDriverNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Err(CoreError::internal(
            "TUN input driver must run through its descriptor process function",
        ))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tun_input_driver_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_tun_input_trace)
    }
}

impl DriverNode for TunInputDriverNode {
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::next(self.node_name, 1)
    }

    #[inline]
    fn node_initial_nexts(&self) -> &[NodeId] {
        std::slice::from_ref(&self.next)
    }
}

pub struct TunOutputDriverNode {
    node_name: &'static str,
    main: TunMain,
    slot: usize,
    runtime_data: NodeRuntimeData,
}

impl TunOutputDriverNode {
    #[inline]
    pub fn new<O>(output: O) -> Self
    where
        O: IntoTunOutputBackend,
    {
        Self::new_with_main(TunMain::default_main(), output)
    }

    #[inline]
    pub fn new_with_main<O>(main: TunMain, output: O) -> Self
    where
        O: IntoTunOutputBackend,
    {
        let slot = main.register_output(TunOutputRuntime {
            output: output.into_tun_output_backend(),
            mode: TunDriverMode::Tun,
        });
        let runtime_data = main.runtime_data(slot).expect("TUN output runtime data");
        Self {
            node_name: "tun-output-driver",
            main,
            slot,
            runtime_data,
        }
    }

    #[inline]
    pub fn with_tap(self, tap: bool) -> Self {
        let mut inner = self.main.inner.lock().expect("tun main poisoned");
        inner
            .outputs
            .get_mut(self.slot)
            .expect("TUN output runtime slot")
            .mode = TunDriverMode::from_tap(tap);
        drop(inner);
        self
    }

    #[inline]
    pub fn with_mode(self, mode: TunDriverMode) -> Self {
        let mut inner = self.main.inner.lock().expect("tun main poisoned");
        inner
            .outputs
            .get_mut(self.slot)
            .expect("TUN output runtime slot")
            .mode = mode;
        drop(inner);
        self
    }

    #[inline]
    pub fn with_node_name(mut self, node_name: &'static str) -> Self {
        self.node_name = node_name;
        self
    }
}

impl Node for TunOutputDriverNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Err(CoreError::internal(
            "TUN output driver must run through its descriptor process function",
        ))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tun_output_driver_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_tun_output_trace)
    }
}

impl DriverNode for TunOutputDriverNode {
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::next(self.node_name, 0)
    }
}

fn tun_input_driver_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let slot = data.usize_word(1)?;
    let inner = TunMain::inner_for_runtime_data(data)?;
    let mut inner = inner
        .lock()
        .map_err(|_| CoreError::internal("tun main poisoned"))?;
    let input = inner
        .inputs
        .get_mut(slot)
        .ok_or_else(|| CoreError::internal("TUN input runtime slot is invalid"))?;
    input.process(runtime, frame)
}

fn tun_output_driver_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let slot = data.usize_word(1)?;
    let inner = TunMain::inner_for_runtime_data(data)?;
    let mut inner = inner
        .lock()
        .map_err(|_| CoreError::internal("tun main poisoned"))?;
    let output = inner
        .outputs
        .get_mut(slot)
        .ok_or_else(|| CoreError::internal("TUN output runtime slot is invalid"))?;
    output.process(runtime, frame)
}

#[derive(Clone, Default)]
pub struct MemoryTunDevice {
    inner: Arc<Mutex<MemoryTunInner>>,
}

#[derive(Default)]
struct MemoryTunInner {
    input: Vec<Vec<u8>>,
    output: Vec<Vec<u8>>,
    output_batch_sizes: Vec<usize>,
    closed: bool,
}

#[derive(Clone)]
pub struct MemoryTunInput {
    inner: Arc<Mutex<MemoryTunInner>>,
}

#[derive(Clone)]
pub struct MemoryTunOutput {
    inner: Arc<Mutex<MemoryTunInner>>,
}

impl MemoryTunDevice {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn input(&self) -> MemoryTunInput {
        MemoryTunInput {
            inner: Arc::clone(&self.inner),
        }
    }

    #[inline]
    pub fn output(&self) -> MemoryTunOutput {
        MemoryTunOutput {
            inner: Arc::clone(&self.inner),
        }
    }

    #[inline]
    pub fn inject(&self, packet: Vec<u8>) -> CoreResult<()> {
        let mut inner = self.inner.lock().expect("memory TUN poisoned");
        if inner.closed {
            return Err(CoreError::internal("memory TUN is closed"));
        }
        inner.input.push(packet);
        Ok(())
    }

    #[inline]
    pub fn drain_output(&self) -> Vec<Vec<u8>> {
        self.inner
            .lock()
            .expect("memory TUN poisoned")
            .output
            .drain(..)
            .collect()
    }

    #[inline]
    pub fn drain_output_batch_sizes(&self) -> Vec<usize> {
        self.inner
            .lock()
            .expect("memory TUN poisoned")
            .output_batch_sizes
            .drain(..)
            .collect()
    }

    #[inline]
    pub fn close(&self) {
        self.inner.lock().expect("memory TUN poisoned").closed = true;
    }
}

impl TunPacketSource for MemoryTunInput {
    #[inline]
    fn recv_frame(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        interface_id: &str,
        mode: TunDriverMode,
        max: usize,
    ) -> CoreResult<usize> {
        let mut inner = self.inner.lock().expect("memory TUN poisoned");
        if inner.closed {
            return Err(CoreError::internal("memory TUN is closed"));
        }
        let mut attempts = 0usize;
        let mut accepted = 0usize;
        while attempts < max {
            if inner.input.is_empty() {
                break;
            };
            let packet = inner
                .input
                .drain(..1)
                .next()
                .expect("checked non-empty memory TUN input");
            attempts += 1;
            if push_packet_to_frame(runtime, frame, interface_id, mode, &packet)? {
                accepted += 1;
            }
        }
        Ok(accepted)
    }
}

impl TunPacketSink for MemoryTunOutput {
    #[inline]
    fn send_frame(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        mode: TunDriverMode,
    ) -> CoreResult<usize> {
        let mut inner = self.inner.lock().expect("memory TUN poisoned");
        if inner.closed {
            return Err(CoreError::internal("memory TUN is closed"));
        }
        let batch_len = frame.pending_len();
        if batch_len != 0 {
            inner.output_batch_sizes.push(batch_len);
        }
        let mut processed = 0usize;
        let mut send_result = Ok(());
        {
            let mut cursor = frame.batch_cursor(runtime.preferred_frame_batch_width());
            cursor.prefetch_next(runtime);
            'send: while let Some(batch) = cursor.next() {
                cursor.prefetch_next(runtime);
                for index in batch.indices() {
                    let packet = tun_output_packet(runtime, index, mode);
                    runtime.free_index(index);
                    processed += 1;
                    match packet {
                        Ok(packet) => inner.output.push(packet),
                        Err(err) => {
                            send_result = Err(err);
                            break 'send;
                        }
                    }
                }
            }
        }
        if let Err(err) = send_result {
            for index in frame.pending_indices()[processed..].iter().copied() {
                runtime.free_index(index);
            }
            frame.clear();
            return Err(err);
        }
        frame.clear();
        Ok(batch_len)
    }
}

#[inline]
fn push_packet_to_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    _: &str,
    mode: TunDriverMode,
    packet: &[u8],
) -> CoreResult<bool> {
    let Some((packet, tap_ethernet)) = packet_for_mode(mode, packet) else {
        return Ok(false);
    };
    let index = runtime.alloc_index_with_bytes(packet)?;
    let mut buffer = runtime.get_buffer_mut(index)?;
    if parse_ip_packet(packet).is_err() {
        runtime.free_index(index);
        return Ok(false);
    }
    let opaque = unsafe { transmute::<_, &mut TunOpaque>(buffer.opaque2_mut()) };
    opaque.tap_ethernet = tap_ethernet;
    if let Err(err) = frame.push_index(index) {
        runtime.free_index(index);
        return Err(err);
    }
    Ok(true)
}

#[inline]
fn packet_for_mode(
    mode: TunDriverMode,
    packet: &[u8],
) -> Option<(&[u8], Option<TapEthernetMetadata>)> {
    if !mode.is_tap() {
        return Some((packet, None));
    }
    if packet.len() < ETHERNET_HEADER_LEN {
        return None;
    }
    let ethertype = u16::from_be_bytes([packet[12], packet[13]]);
    if ethertype != ETHERTYPE_IP4 && ethertype != ETHERTYPE_IP6 {
        return None;
    }
    let mut destination = [0u8; 6];
    destination.copy_from_slice(&packet[..6]);
    let mut source = [0u8; 6];
    source.copy_from_slice(&packet[6..12]);
    Some((
        &packet[ETHERNET_HEADER_LEN..],
        Some(TapEthernetMetadata::new(destination, source, ethertype)),
    ))
}

#[inline]
fn tun_output_packet(
    runtime: &DataPlaneRuntime,
    index: hammer_adapter::BufferIndex,
    mode: TunDriverMode,
) -> CoreResult<Vec<u8>> {
    let packet = runtime.copy_packet(index)?;
    if !mode.is_tap() {
        return Ok(packet);
    }
    let buffer = runtime.get_buffer(index)?;
    let opaque = unsafe { transmute::<_, &TunOpaque>(buffer.opaque2()) };
    let Some(tap) = opaque.tap_ethernet else {
        return Ok(packet);
    };
    if tap.header_present {
        return Ok(packet);
    }
    let mut frame = Vec::with_capacity(ETHERNET_HEADER_LEN + packet.len());
    frame.extend_from_slice(&tap.header());
    frame.extend_from_slice(&packet);
    Ok(frame)
}
