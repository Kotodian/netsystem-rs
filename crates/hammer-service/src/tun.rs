use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, DriverNode, Node, NodeId, NodeRegistration, NodeResult,
    PacketTrace, RouteMetadata, TapEthernetMetadata, TraceFormatter,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::vec::Vec;

pub use crate::net::packet_route_metadata;

use crate::interface::InterfaceControlHandle;

const DEFAULT_TUN_RECV_BATCH: usize = 256;
const ETHERNET_HEADER_LEN: usize = 14;
const ETHERTYPE_IP4: u16 = 0x0800;
const ETHERTYPE_IP6: u16 = 0x86dd;

pub trait TunPacketSource {
    fn recv_frame<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
        interface_id: &str,
        mode: TunDriverMode,
        max: usize,
    ) -> CoreResult<usize>;
}

pub trait TunPacketSink {
    fn send_frame<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
        mode: TunDriverMode,
    ) -> CoreResult<usize>;
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
    fn encode_trace(&self, out: &mut std::vec::Vec<u8>) {
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
    fn encode_trace(&self, out: &mut std::vec::Vec<u8>) {
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

pub struct TunInputDriverNode<I> {
    node_name: &'static str,
    input: I,
    interface_id: String,
    interface_index: Option<u32>,
    interface_control: Option<InterfaceControlHandle>,
    next: NodeId,
    max_batch: usize,
    mode: TunDriverMode,
}

impl<I> TunInputDriverNode<I> {
    #[inline]
    pub fn new(input: I, interface_id: impl Into<String>, next: NodeId) -> Self {
        Self {
            node_name: "tun-input-driver-node",
            input,
            interface_id: interface_id.into(),
            interface_index: None,
            interface_control: None,
            next,
            max_batch: DEFAULT_TUN_RECV_BATCH,
            mode: TunDriverMode::Tun,
        }
    }

    #[inline]
    pub fn with_interface_index(mut self, interface_index: u32) -> Self {
        self.interface_index = Some(interface_index);
        self
    }

    #[inline]
    pub fn with_interface_control(mut self, interface_control: InterfaceControlHandle) -> Self {
        self.interface_control = Some(interface_control);
        self
    }

    #[inline]
    pub fn with_max_batch(mut self, max_batch: usize) -> Self {
        self.max_batch = max_batch;
        self
    }

    #[inline]
    pub fn with_tap(mut self, tap: bool) -> Self {
        self.mode = TunDriverMode::from_tap(tap);
        self
    }

    #[inline]
    pub fn with_mode(mut self, mode: TunDriverMode) -> Self {
        self.mode = mode;
        self
    }

    #[inline]
    pub fn with_node_name(mut self, node_name: &'static str) -> Self {
        self.node_name = node_name;
        self
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

impl<I, G> Node<G> for TunInputDriverNode<I>
where
    I: TunPacketSource,
{
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let max_batch = self.max_batch.min(frame.remaining_capacity());
        let first_new = frame.pending_len();
        let received =
            self.input
                .recv_frame(runtime, frame, &self.interface_id, self.mode, max_batch)?;
        let interface_index = self.ingress_interface_index()?;
        if let Some(interface_index) = interface_index {
            for index in frame.pending_indices()[first_new..].iter().copied() {
                runtime
                    .get_buffer_mut(index)?
                    .metadata_mut()
                    .ingress_interface = Some(interface_index);
            }
        }
        let Some(current_node) = runtime.current_node() else {
            return Err(CoreError::internal(
                "tun input trace outside node processing",
            ));
        };
        for index in frame.pending_indices()[first_new..].iter().copied() {
            runtime.try_mark_trace(current_node, index)?;
            runtime.add_trace_with(index, || {
                Ok(TunInputTrace {
                    interface_index,
                    mode: self.mode,
                    received,
                })
            })?;
        }
        if frame.has_pending() {
            Ok(NodeResult::next_current(self.next))
        } else {
            Ok(NodeResult::drop())
        }
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_tun_input_trace)
    }
}

impl<I, G> DriverNode<G> for TunInputDriverNode<I>
where
    I: TunPacketSource,
{
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

pub struct TunOutputDriverNode<O> {
    node_name: &'static str,
    output: O,
    mode: TunDriverMode,
}

impl<O> TunOutputDriverNode<O> {
    #[inline]
    pub fn new(output: O) -> Self {
        Self {
            node_name: "tun-output-driver-node",
            output,
            mode: TunDriverMode::Tun,
        }
    }

    #[inline]
    pub fn with_tap(mut self, tap: bool) -> Self {
        self.mode = TunDriverMode::from_tap(tap);
        self
    }

    #[inline]
    pub fn with_mode(mut self, mode: TunDriverMode) -> Self {
        self.mode = mode;
        self
    }

    #[inline]
    pub fn with_node_name(mut self, node_name: &'static str) -> Self {
        self.node_name = node_name;
        self
    }
}

impl<O, G> Node<G> for TunOutputDriverNode<O>
where
    O: TunPacketSink,
{
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let pending = frame.pending_len();
        for index in frame.pending_indices().iter().copied() {
            runtime.add_trace_with(index, || {
                Ok(TunOutputTrace {
                    mode: self.mode,
                    pending,
                })
            })?;
        }
        self.output.send_frame(runtime, frame, self.mode)?;
        Ok(NodeResult::drop())
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_tun_output_trace)
    }
}

impl<O, G> DriverNode<G> for TunOutputDriverNode<O>
where
    O: TunPacketSink,
{
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::next(self.node_name, 0)
    }
}

#[derive(Clone, Default)]
pub struct MemoryTunDevice {
    inner: Rc<RefCell<MemoryTunInner>>,
}

#[derive(Default)]
struct MemoryTunInner {
    input: VecDeque<Vec<u8>>,
    output: VecDeque<Vec<u8>>,
    output_batch_sizes: Vec<usize>,
    closed: bool,
}

#[derive(Clone)]
pub struct MemoryTunInput {
    inner: Rc<RefCell<MemoryTunInner>>,
}

#[derive(Clone)]
pub struct MemoryTunOutput {
    inner: Rc<RefCell<MemoryTunInner>>,
}

impl MemoryTunDevice {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn input(&self) -> MemoryTunInput {
        MemoryTunInput {
            inner: Rc::clone(&self.inner),
        }
    }

    #[inline]
    pub fn output(&self) -> MemoryTunOutput {
        MemoryTunOutput {
            inner: Rc::clone(&self.inner),
        }
    }

    #[inline]
    pub fn inject(&self, packet: Vec<u8>) -> CoreResult<()> {
        let mut inner = self.inner.borrow_mut();
        if inner.closed {
            return Err(CoreError::internal("memory TUN is closed"));
        }
        inner.input.push_back(packet);
        Ok(())
    }

    #[inline]
    pub fn drain_output(&self) -> Vec<Vec<u8>> {
        self.inner.borrow_mut().output.drain(..).collect()
    }

    #[inline]
    pub fn drain_output_batch_sizes(&self) -> Vec<usize> {
        self.inner
            .borrow_mut()
            .output_batch_sizes
            .drain(..)
            .collect()
    }

    #[inline]
    pub fn close(&self) {
        self.inner.borrow_mut().closed = true;
    }
}

impl TunPacketSource for MemoryTunInput {
    #[inline]
    fn recv_frame<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
        interface_id: &str,
        mode: TunDriverMode,
        max: usize,
    ) -> CoreResult<usize> {
        let mut inner = self.inner.borrow_mut();
        if inner.closed {
            return Err(CoreError::internal("memory TUN is closed"));
        }
        let mut attempts = 0usize;
        let mut accepted = 0usize;
        while attempts < max {
            let Some(packet) = inner.input.pop_front() else {
                break;
            };
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
    fn send_frame<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
        mode: TunDriverMode,
    ) -> CoreResult<usize> {
        let mut inner = self.inner.borrow_mut();
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
                        Ok(packet) => inner.output.push_back(packet),
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
fn push_packet_to_frame<G>(
    runtime: &DataPlaneRuntime<G>,
    frame: &mut BufferFrame,
    interface_id: &str,
    mode: TunDriverMode,
    packet: &[u8],
) -> CoreResult<bool> {
    let Some((packet, tap_ethernet)) = packet_for_mode(mode, packet) else {
        return Ok(false);
    };
    let mut metadata =
        packet_route_metadata(interface_id, packet).unwrap_or_else(|_| RouteMetadata {
            inbound: interface_id.to_owned(),
            ..Default::default()
        });
    metadata.tap_ethernet = tap_ethernet;
    let index = runtime.alloc_index_with_bytes(metadata, packet)?;
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
fn tun_output_packet<G>(
    runtime: &DataPlaneRuntime<G>,
    index: hammer_adapter::BufferIndex,
    mode: TunDriverMode,
) -> CoreResult<Vec<u8>> {
    let packet = runtime.copy_current_chain(index)?;
    if !mode.is_tap() {
        return Ok(packet);
    }
    let metadata = runtime.metadata(index)?;
    let Some(tap) = metadata.tap_ethernet else {
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
