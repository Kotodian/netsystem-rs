use std::marker::PhantomData;
use std::mem::transmute;
use std::sync::{Arc, Mutex};

use hammer_adapter::{
    BufferFrame, BufferIndex, BufferRef, DataPlaneRuntime, DriverNode, Frame, Next, Node, NodeId,
    NodeProcessFn, NodeRegistration, NodeResult, NodeRuntimeData, PacketTrace, SecondaryOpaque,
    TraceFormatter, add_packet_trace, unlikely,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::vec::Vec;

pub use crate::device::{
    DeviceEventSource as TunDeviceEventSource, DeviceMain, DeviceRuntimeSlot, DriverScheduleMode,
};
use crate::interface::InterfaceControlHandle;
use crate::net::{NetworkOpaque, TapEthernetMetadata};

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

/// Direction marker + dispatch trait for TUN driver nodes. Implemented by the
/// per-direction runtime structs (`TunInputRuntime` / `TunOutputRuntime`), which
/// double as the type parameter on `TunDriverNode<R>` / `TunBackend<R>` /
/// `IntoTunBackend<R>`. This collapses the prior `TunInput*` / `TunOutput*`
/// duplicated type pairs into a single generic family keyed by the runtime type.
pub trait TunDriverDirection: Send {
    /// Node name used in `NodeRegistration::next`.
    const NODE_NAME: &'static str;

    /// Number of initial next-node slots carried on the driver node descriptor
    /// (1 for input, 0 for output).
    const NEXT_COUNT: usize;

    /// Memory-backed backend variant for this direction.
    type MemoryBackend;

    /// Scripted (real-IO) backend variant for this direction.
    type RealBackend;

    /// Trace formatter for this direction's packet trace.
    fn trace_formatter() -> TraceFormatter;

    /// Dataplane process entry point. Recovered from `NodeRuntimeData` by
    /// `tun_driver_process::<R>` and invoked on the per-slot runtime state.
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult;
}

#[doc(hidden)]
pub enum TunBackend<R: TunDriverDirection> {
    Memory(R::MemoryBackend),
    Scripted(R::RealBackend),
}

#[doc(hidden)]
pub trait IntoTunBackend<R: TunDriverDirection> {
    fn into_tun_backend(self) -> TunBackend<R>;
}

impl TunBackend<TunInputRuntime> {
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

impl TunBackend<TunOutputRuntime> {
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

impl IntoTunBackend<TunInputRuntime> for MemoryTunInput {
    #[inline]
    fn into_tun_backend(self) -> TunBackend<TunInputRuntime> {
        TunBackend::Memory(self)
    }
}

impl IntoTunBackend<TunOutputRuntime> for MemoryTunOutput {
    #[inline]
    fn into_tun_backend(self) -> TunBackend<TunOutputRuntime> {
        TunBackend::Memory(self)
    }
}

impl IntoTunBackend<TunInputRuntime> for RealTunInput<ScriptedTunIo> {
    #[inline]
    fn into_tun_backend(self) -> TunBackend<TunInputRuntime> {
        TunBackend::Scripted(self)
    }
}

impl IntoTunBackend<TunOutputRuntime> for RealTunOutput<ScriptedTunIo> {
    #[inline]
    fn into_tun_backend(self) -> TunBackend<TunOutputRuntime> {
        TunBackend::Scripted(self)
    }
}

pub struct TunInputRuntime {
    input: TunBackend<TunInputRuntime>,
    interface_id: String,
    interface_index: Option<u32>,
    interface_control: Option<InterfaceControlHandle>,
    device_main: Option<Arc<DeviceMain>>,
    rx_queue: Option<u32>,
    next: NodeId,
    max_batch: usize,
    mode: TunDriverMode,
}

pub struct TunOutputRuntime {
    output: TunBackend<TunOutputRuntime>,
    mode: TunDriverMode,
}

impl TunDriverDirection for TunInputRuntime {
    const NODE_NAME: &'static str = "tun-input-driver";
    const NEXT_COUNT: usize = 1;
    type MemoryBackend = MemoryTunInput;
    type RealBackend = RealTunInput<ScriptedTunIo>;

    #[inline]
    fn trace_formatter() -> TraceFormatter {
        format_tun_input_trace
    }

    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        if let (Some(device_main), Some(rx_queue)) = (&self.device_main, self.rx_queue)
            && !match device_main.consume_rx_interrupt_pending(rx_queue) {
                Ok(pending) => pending,
                Err(_) => return NodeResult::drop(),
            }
        {
            return NodeResult::drop();
        }
        let max_batch = self.max_batch.min(frame.remaining_capacity());
        let first_new = frame.pending_len();
        let received =
            match self
                .input
                .recv_frame(runtime, frame, &self.interface_id, self.mode, max_batch)
            {
                Ok(received) => received,
                Err(_) => return NodeResult::drop(),
            };
        let interface_index = match self.ingress_interface_index() {
            Ok(index) => index,
            Err(_) => return NodeResult::drop(),
        };
        if let Some(interface_index) = interface_index {
            let indices = frame.pending_indices();
            let mut read = first_new;
            let len = indices.len();
            while read + 4 <= len {
                let index0 = indices[read];
                let index1 = indices[read + 1];
                let index2 = indices[read + 2];
                let index3 = indices[read + 3];
                if let Ok(mut buffer) = runtime.get_buffer_mut(index0) {
                    let network =
                        unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
                    network.sw_if_index[0] = interface_index;
                }
                if let Ok(mut buffer) = runtime.get_buffer_mut(index1) {
                    let network =
                        unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
                    network.sw_if_index[0] = interface_index;
                }
                if let Ok(mut buffer) = runtime.get_buffer_mut(index2) {
                    let network =
                        unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
                    network.sw_if_index[0] = interface_index;
                }
                if let Ok(mut buffer) = runtime.get_buffer_mut(index3) {
                    let network =
                        unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
                    network.sw_if_index[0] = interface_index;
                }
                read += 4;
            }
            if read + 2 <= len {
                let index0 = indices[read];
                let index1 = indices[read + 1];
                if let Ok(mut buffer) = runtime.get_buffer_mut(index0) {
                    let network =
                        unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
                    network.sw_if_index[0] = interface_index;
                }
                if let Ok(mut buffer) = runtime.get_buffer_mut(index1) {
                    let network =
                        unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
                    network.sw_if_index[0] = interface_index;
                }
                read += 2;
            }
            while read < len {
                let index0 = indices[read];
                if let Ok(mut buffer) = runtime.get_buffer_mut(index0) {
                    let network =
                        unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
                    network.sw_if_index[0] = interface_index;
                }
                read += 1;
            }
        }
        if let Some(current_node) = runtime.current_node()
            && unlikely(runtime.may_mark_trace(current_node))
        {
            let indices = frame.pending_indices();
            let mut read = first_new;
            let len = indices.len();
            while read + 4 <= len {
                let index0 = indices[read];
                let index1 = indices[read + 1];
                let index2 = indices[read + 2];
                let index3 = indices[read + 3];
                let _ = runtime.try_mark_trace(current_node, index0);
                let _ = add_packet_trace!(
                    runtime,
                    index0,
                    TunInputTrace {
                        interface_index,
                        mode: self.mode,
                        received
                    }
                );
                let _ = runtime.try_mark_trace(current_node, index1);
                let _ = add_packet_trace!(
                    runtime,
                    index1,
                    TunInputTrace {
                        interface_index,
                        mode: self.mode,
                        received
                    }
                );
                let _ = runtime.try_mark_trace(current_node, index2);
                let _ = add_packet_trace!(
                    runtime,
                    index2,
                    TunInputTrace {
                        interface_index,
                        mode: self.mode,
                        received
                    }
                );
                let _ = runtime.try_mark_trace(current_node, index3);
                let _ = add_packet_trace!(
                    runtime,
                    index3,
                    TunInputTrace {
                        interface_index,
                        mode: self.mode,
                        received
                    }
                );
                read += 4;
            }
            if read + 2 <= len {
                let index0 = indices[read];
                let index1 = indices[read + 1];
                let _ = runtime.try_mark_trace(current_node, index0);
                let _ = add_packet_trace!(
                    runtime,
                    index0,
                    TunInputTrace {
                        interface_index,
                        mode: self.mode,
                        received
                    }
                );
                let _ = runtime.try_mark_trace(current_node, index1);
                let _ = add_packet_trace!(
                    runtime,
                    index1,
                    TunInputTrace {
                        interface_index,
                        mode: self.mode,
                        received
                    }
                );
                read += 2;
            }
            while read < len {
                let index0 = indices[read];
                let _ = runtime.try_mark_trace(current_node, index0);
                let _ = add_packet_trace!(
                    runtime,
                    index0,
                    TunInputTrace {
                        interface_index,
                        mode: self.mode,
                        received,
                    },
                );
                read += 1;
            }
        }
        if !frame.has_pending() {
            return NodeResult::drop();
        }
        let mut next_frame = match runtime.buffers().get_next_frame(self.next) {
            Ok(frame) => frame,
            Err(_) => return NodeResult::drop(),
        };
        let width = runtime.preferred_frame_batch_width();
        if frame
            .retain_indices_batched(width, |index| {
                next_frame.push_index(index)?;
                Ok(false)
            })
            .is_err()
        {
            return NodeResult::drop();
        }
        if runtime.put_next_frame(next_frame).is_err() {
            return NodeResult::drop();
        }
        NodeResult::drop()
    }
}

impl TunInputRuntime {
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

impl TunDriverDirection for TunOutputRuntime {
    const NODE_NAME: &'static str = "tun-output-driver";
    const NEXT_COUNT: usize = 0;
    type MemoryBackend = MemoryTunOutput;
    type RealBackend = RealTunOutput<ScriptedTunIo>;

    #[inline]
    fn trace_formatter() -> TraceFormatter {
        format_tun_output_trace
    }

    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        self.trace_frame(runtime, frame);
        let _ = self.output.send_frame(runtime, frame, self.mode);
        NodeResult::drop()
    }
}

impl TunOutputRuntime {
    fn trace_frame(&self, runtime: &DataPlaneRuntime, frame: &BufferFrame) {
        let pending = frame.pending_len();
        let indices = frame.pending_indices();
        let len = indices.len();
        let mut read = 0usize;
        while read + 4 <= len {
            if read + 4 < len {
                runtime.prefetch_header(indices[read + 4]);
            }
            if read + 5 < len {
                runtime.prefetch_header(indices[read + 5]);
            }
            if read + 6 < len {
                runtime.prefetch_header(indices[read + 6]);
            }
            if read + 7 < len {
                runtime.prefetch_header(indices[read + 7]);
            }
            let _ = add_packet_trace!(
                runtime,
                indices[read],
                TunOutputTrace {
                    mode: self.mode,
                    pending,
                },
            );
            let _ = add_packet_trace!(
                runtime,
                indices[read + 1],
                TunOutputTrace {
                    mode: self.mode,
                    pending,
                },
            );
            let _ = add_packet_trace!(
                runtime,
                indices[read + 2],
                TunOutputTrace {
                    mode: self.mode,
                    pending,
                },
            );
            let _ = add_packet_trace!(
                runtime,
                indices[read + 3],
                TunOutputTrace {
                    mode: self.mode,
                    pending,
                },
            );
            read += 4;
        }
        if read + 2 <= len {
            if read + 2 < len {
                runtime.prefetch_header(indices[read + 2]);
            }
            if read + 3 < len {
                runtime.prefetch_header(indices[read + 3]);
            }
            let _ = add_packet_trace!(
                runtime,
                indices[read],
                TunOutputTrace {
                    mode: self.mode,
                    pending,
                },
            );
            let _ = add_packet_trace!(
                runtime,
                indices[read + 1],
                TunOutputTrace {
                    mode: self.mode,
                    pending,
                },
            );
            read += 2;
        }
        while read < len {
            if read + 1 < len {
                runtime.prefetch_header(indices[read + 1]);
            }
            let _ = add_packet_trace!(
                runtime,
                indices[read],
                TunOutputTrace {
                    mode: self.mode,
                    pending,
                },
            );
            read += 1;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TunBufferSendResult {
    Complete,
    Backpressure,
}

pub trait TunBufferIo {
    fn try_recv_buffer(&mut self, buffer: &mut [u8]) -> CoreResult<Option<usize>>;

    fn max_recv_len(&self) -> Option<usize> {
        None
    }

    fn try_send_buffers(&mut self, segments: &[&[u8]]) -> CoreResult<TunBufferSendResult>;

    #[inline]
    fn try_send_buffer(&mut self, packet: &[u8]) -> CoreResult<TunBufferSendResult> {
        self.try_send_buffers(&[packet])
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
        let mut in_flight = runtime.buffers().get_next_frame(NodeId::new(0))?;
        while received < max {
            let Some(index) = self.recv_into_frame(runtime, frame, &mut in_flight)? else {
                break;
            };
            if let Err(err) = self.set_l3_metadata(runtime, index, interface_id) {
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
    fn recv_into_frame(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
        in_flight: &mut Frame<Next>,
    ) -> CoreResult<Option<BufferIndex>> {
        let index = runtime.alloc_index()?;
        in_flight.push_index(index)?;
        (|| -> CoreResult<Option<BufferIndex>> {
            let mut buffer = runtime.get_buffer_mut(index)?;
            let dst = buffer.writable_tail_mut();
            let dst_len = dst.len();
            let Some(len) = self.io.try_recv_buffer(dst)? else {
                return Ok(None);
            };
            if len == 0 {
                return Ok(None);
            }
            if len == dst_len && self.io.max_recv_len().is_none_or(|max| dst_len < max) {
                return Ok(None);
            }
            buffer.commit_writable_tail(len)?;
            drop(buffer);
            frame.push_index(index)?;
            in_flight.retain_indices(|candidate| Ok(candidate != index))?;
            Ok(Some(index))
        })()
    }

    fn set_l3_metadata(
        &self,
        _: &DataPlaneRuntime,
        _: hammer_adapter::BufferIndex,
        _: &str,
    ) -> CoreResult<()> {
        Ok(())
    }
}

pub struct RealTunOutput<I> {
    io: I,
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

    fn try_send_buffers(&mut self, segments: &[&[u8]]) -> CoreResult<TunBufferSendResult> {
        self.record_send(segments)
    }
}

impl ScriptedTunIo {
    fn record_send(&mut self, segments: &[&[u8]]) -> CoreResult<TunBufferSendResult> {
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
        if matches!(result, TunBufferSendResult::Complete) {
            let total_len: usize = segments.iter().map(|segment| segment.len()).sum();
            let mut sent = Vec::with_capacity(total_len);
            for segment in segments {
                sent.extend_from_slice(segment);
            }
            inner.sent.push(sent);
        }
        Ok(result)
    }
}

impl<I> RealTunOutput<I> {
    #[inline]
    pub fn new(io: I) -> Self {
        Self { io }
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
    #[inline]
    fn try_send_index(
        &mut self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
        mode: TunDriverMode,
    ) -> CoreResult<TunBufferSendResult> {
        if mode.is_tap() {
            return Err(CoreError::internal("real TUN driver only supports L3 TUN"));
        }
        let mut refs: Vec<BufferRef<'_>> = Vec::with_capacity(4);
        let mut chain = runtime.chain(index);
        while let Some(buffer) = chain.next() {
            refs.push(buffer?);
        }
        drop(chain);
        if refs.len() <= 1 {
            return self
                .io
                .try_send_buffer(refs.first().map(|buffer| buffer.current()).unwrap_or(&[]));
        }
        let mut segments: Vec<&[u8]> = Vec::with_capacity(refs.len());
        for buffer in &refs {
            segments.push(buffer.current());
        }
        self.io.try_send_buffers(&segments)
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
            return Err(CoreError::internal("real TUN driver only supports L3 TUN"));
        }
        let mut sent = 0usize;
        let indices = frame.pending_indices();
        let len = indices.len();
        let mut read = 0usize;
        while read < len {
            match self.try_send_index(runtime, indices[read], mode)? {
                TunBufferSendResult::Complete => sent += 1,
                TunBufferSendResult::Backpressure => break,
            }
            read += 1;
        }
        Ok(sent)
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

/// TUN device-class state holder. Bundles the input/output per-instance runtime
/// registries (`DeviceRuntimeSlot<T>`) used by `TunInputDriverNode` /
/// `TunOutputDriverNode` to recover their per-slot state from `NodeRuntimeData`.
///
/// This is the TUN-specific companion to `DeviceMain` (the queue registry): the
/// `DeviceMain` maps RX/TX queue indices to input/output node ids + schedule
/// mode, while `TunMain` holds the per-slot `TunInputRuntime` / `TunOutputRuntime`
/// state that the driver node process functions mutate.
///
/// # Synchronization
///
/// Inherits `DeviceRuntimeSlot`'s lock-free dataplane + barrier-gated control
/// plane contract. `with_input_mut` / `with_output_mut` are control-plane
/// accessors: callers must hold the runtime data-plane barrier if the runtime is
/// dispatching; pre-registration builder chains are single-threaded and need no
/// barrier.
#[derive(Clone)]
pub struct TunMain {
    input_slot: Arc<DeviceRuntimeSlot<TunInputRuntime>>,
    output_slot: Arc<DeviceRuntimeSlot<TunOutputRuntime>>,
}

impl Default for TunMain {
    #[inline]
    fn default() -> Self {
        Self::default_main()
    }
}

impl TunMain {
    #[inline]
    pub fn default_main() -> Self {
        Self {
            input_slot: DeviceRuntimeSlot::new(),
            output_slot: DeviceRuntimeSlot::new(),
        }
    }

    #[inline]
    pub fn register_input(&self, input: TunInputRuntime) -> usize {
        self.input_slot.register(input)
    }

    #[inline]
    pub fn register_output(&self, output: TunOutputRuntime) -> usize {
        self.output_slot.register(output)
    }

    #[inline]
    pub fn input_runtime_data(&self, slot: usize) -> CoreResult<NodeRuntimeData> {
        self.input_slot.runtime_data(slot)
    }

    #[inline]
    pub fn output_runtime_data(&self, slot: usize) -> CoreResult<NodeRuntimeData> {
        self.output_slot.runtime_data(slot)
    }

    #[inline]
    pub fn with_input_mut<R>(&self, slot: usize, f: impl FnOnce(&mut TunInputRuntime) -> R) -> R {
        self.input_slot.with_mut(slot, f)
    }

    #[inline]
    pub fn with_output_mut<R>(&self, slot: usize, f: impl FnOnce(&mut TunOutputRuntime) -> R) -> R {
        self.output_slot.with_mut(slot, f)
    }
}

/// Generic TUN driver node, parameterized by the per-direction runtime type
/// (`TunInputRuntime` or `TunOutputRuntime`), which acts as the
/// `TunDriverDirection` marker. Collapses the prior `TunInputDriverNode` /
/// `TunOutputDriverNode` duplicate pair into one struct + direction-specific
/// constructor/builder impl blocks. The `next: Option<NodeId>` field carries
/// the input's single next node id (`Some`) and is `None` for output.
#[derive(Clone)]
pub struct TunDriverNode<R: TunDriverDirection> {
    node_name: &'static str,
    main: TunMain,
    slot: usize,
    runtime_data: NodeRuntimeData,
    next: Option<NodeId>,
    _dir: PhantomData<R>,
}

impl<R: TunDriverDirection> TunDriverNode<R> {
    #[inline]
    pub fn with_node_name(mut self, node_name: &'static str) -> Self {
        self.node_name = node_name;
        self
    }
}

impl TunDriverNode<TunInputRuntime> {
    #[inline]
    pub fn new<I>(input: I, interface_id: impl Into<String>, next: NodeId) -> Self
    where
        I: IntoTunBackend<TunInputRuntime>,
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
        I: IntoTunBackend<TunInputRuntime>,
    {
        let slot = main.register_input(TunInputRuntime {
            input: input.into_tun_backend(),
            interface_id: interface_id.into(),
            interface_index: None,
            interface_control: None,
            device_main: None,
            rx_queue: None,
            next,
            max_batch: DEFAULT_TUN_RECV_BATCH,
            mode: TunDriverMode::Tun,
        });
        let runtime_data = main
            .input_runtime_data(slot)
            .expect("TUN input runtime data");
        Self {
            node_name: <TunInputRuntime as TunDriverDirection>::NODE_NAME,
            main,
            slot,
            runtime_data,
            next: Some(next),
            _dir: PhantomData,
        }
    }

    #[inline]
    pub fn with_interface_index(self, interface_index: u32) -> Self {
        self.main.with_input_mut(self.slot, |input| {
            input.interface_index = Some(interface_index)
        });
        self
    }

    #[inline]
    pub fn with_interface_control(self, interface_control: InterfaceControlHandle) -> Self {
        self.main.with_input_mut(self.slot, |input| {
            input.interface_control = Some(interface_control)
        });
        self
    }

    #[inline]
    pub fn with_rx_queue(self, device_main: Arc<DeviceMain>, rx_queue: u32) -> Self {
        self.main.with_input_mut(self.slot, |input| {
            input.device_main = Some(device_main);
            input.rx_queue = Some(rx_queue);
        });
        self
    }

    pub fn bind_rx_queue(&self, device_main: Arc<DeviceMain>, rx_queue: u32) -> CoreResult<()> {
        // Control-plane: caller must hold the runtime data-plane barrier if the
        // runtime is dispatching. Pre-registration setup is single-threaded.
        self.main.with_input_mut(self.slot, |input| {
            input.device_main = Some(device_main);
            input.rx_queue = Some(rx_queue);
        });
        Ok(())
    }

    #[inline]
    pub fn with_max_batch(self, max_batch: usize) -> Self {
        self.main
            .with_input_mut(self.slot, |input| input.max_batch = max_batch);
        self
    }

    #[inline]
    pub fn with_tap(self, tap: bool) -> Self {
        self.main
            .with_input_mut(self.slot, |input| input.mode = TunDriverMode::from_tap(tap));
        self
    }

    #[inline]
    pub fn with_mode(self, mode: TunDriverMode) -> Self {
        self.main
            .with_input_mut(self.slot, |input| input.mode = mode);
        self
    }
}

impl TunDriverNode<TunOutputRuntime> {
    #[inline]
    pub fn new<O>(output: O) -> Self
    where
        O: IntoTunBackend<TunOutputRuntime>,
    {
        Self::new_with_main(TunMain::default_main(), output)
    }

    #[inline]
    pub fn new_with_main<O>(main: TunMain, output: O) -> Self
    where
        O: IntoTunBackend<TunOutputRuntime>,
    {
        let slot = main.register_output(TunOutputRuntime {
            output: output.into_tun_backend(),
            mode: TunDriverMode::Tun,
        });
        let runtime_data = main
            .output_runtime_data(slot)
            .expect("TUN output runtime data");
        Self {
            node_name: <TunOutputRuntime as TunDriverDirection>::NODE_NAME,
            main,
            slot,
            runtime_data,
            next: None,
            _dir: PhantomData,
        }
    }

    #[inline]
    pub fn with_tap(self, tap: bool) -> Self {
        self.main.with_output_mut(self.slot, |output| {
            output.mode = TunDriverMode::from_tap(tap)
        });
        self
    }

    #[inline]
    pub fn with_mode(self, mode: TunDriverMode) -> Self {
        self.main
            .with_output_mut(self.slot, |output| output.mode = mode);
        self
    }
}

impl<R: TunDriverDirection> Node for TunDriverNode<R> {
    #[inline(always)]
    fn process(&mut self, _runtime: &DataPlaneRuntime, _frame: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tun_driver_process::<R>
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(R::trace_formatter())
    }
}

impl<R: TunDriverDirection> DriverNode for TunDriverNode<R> {
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::next(self.node_name, R::NEXT_COUNT)
    }

    #[inline]
    fn node_initial_nexts(&self) -> &[NodeId] {
        match self.next.as_ref() {
            Some(next) => std::slice::from_ref(next),
            None => &[],
        }
    }
}

fn tun_driver_process<R: TunDriverDirection>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let r = match DeviceRuntimeSlot::<R>::borrow_for_runtime_data(data) {
        Ok(r) => r,
        Err(_) => return NodeResult::drop(),
    };
    r.process(runtime, frame)
}

pub type TunInputDriverNode = TunDriverNode<TunInputRuntime>;
pub type TunOutputDriverNode = TunDriverNode<TunOutputRuntime>;

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
        let indices = frame.pending_indices();
        let len = indices.len();
        let mut read = 0usize;
        while read + 4 <= len {
            if read + 4 < len {
                runtime.prefetch_header(indices[read + 4]);
            }
            if read + 5 < len {
                runtime.prefetch_header(indices[read + 5]);
            }
            if read + 6 < len {
                runtime.prefetch_header(indices[read + 6]);
            }
            if read + 7 < len {
                runtime.prefetch_header(indices[read + 7]);
            }
            let index0 = indices[read];
            let index1 = indices[read + 1];
            let index2 = indices[read + 2];
            let index3 = indices[read + 3];
            memory_tun_output_index(runtime, &mut inner.output, index0, mode)?;
            memory_tun_output_index(runtime, &mut inner.output, index1, mode)?;
            memory_tun_output_index(runtime, &mut inner.output, index2, mode)?;
            memory_tun_output_index(runtime, &mut inner.output, index3, mode)?;
            read += 4;
        }
        if read + 2 <= len {
            if read + 2 < len {
                runtime.prefetch_header(indices[read + 2]);
            }
            if read + 3 < len {
                runtime.prefetch_header(indices[read + 3]);
            }
            let index0 = indices[read];
            let index1 = indices[read + 1];
            memory_tun_output_index(runtime, &mut inner.output, index0, mode)?;
            memory_tun_output_index(runtime, &mut inner.output, index1, mode)?;
            read += 2;
        }
        while read < len {
            if read + 1 < len {
                runtime.prefetch_header(indices[read + 1]);
            }
            let index0 = indices[read];
            memory_tun_output_index(runtime, &mut inner.output, index0, mode)?;
            read += 1;
        }
        Ok(batch_len)
    }
}

fn memory_tun_output_index(
    runtime: &DataPlaneRuntime,
    output: &mut Vec<Vec<u8>>,
    index: BufferIndex,
    mode: TunDriverMode,
) -> CoreResult<()> {
    let mut tap_header = None;
    {
        let buffer = runtime.get_buffer(index)?;
        if mode.is_tap() {
            let opaque = unsafe { transmute::<_, &TunOpaque>(buffer.opaque2()) };
            if let Some(tap) = opaque.tap_ethernet
                && !tap.header_present
            {
                tap_header = Some(tap.header());
            }
        }
    }
    let payload_len = packet_total_len(runtime, index)?;
    let capacity = payload_len
        .checked_add(if tap_header.is_some() {
            ETHERNET_HEADER_LEN
        } else {
            0
        })
        .ok_or_else(|| CoreError::internal("memory TAP packet length overflow"))?;
    let mut packet = Vec::with_capacity(capacity);
    if let Some(header) = tap_header {
        packet.extend_from_slice(&header);
    }
    let mut chain = runtime.chain(index);
    while let Some(buffer) = chain.next() {
        let buffer = buffer?;
        packet.extend_from_slice(buffer.current());
    }
    drop(chain);
    output.push(packet);
    Ok(())
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
    let index = runtime.alloc_index()?;
    frame.push_index(index)?;
    {
        let mut buffer = runtime.get_buffer_mut(index)?;
        let dst = buffer.writable_tail_mut();
        if packet.len() > dst.len() {
            return Err(CoreError::internal("TUN packet exceeds buffer capacity"));
        }
        dst[..packet.len()].copy_from_slice(packet);
        buffer.commit_writable_tail(packet.len())?;
        let opaque = unsafe { transmute::<_, &mut TunOpaque>(buffer.opaque2_mut()) };
        opaque.tap_ethernet = tap_ethernet;
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
