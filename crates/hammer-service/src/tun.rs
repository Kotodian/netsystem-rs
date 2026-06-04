use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, DriverNode, Node, NodeId, NodeResult, RouteMetadata,
    TapEthernetMetadata,
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

pub struct TunInputDriverNode<I> {
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
        self.input
            .recv_frame(runtime, frame, &self.interface_id, self.mode, max_batch)?;
        if let Some(interface_index) = self.ingress_interface_index()? {
            for index in frame.pending_indices()[first_new..].iter().copied() {
                runtime
                    .get_buffer_mut(index)?
                    .metadata_mut()
                    .ingress_interface = Some(interface_index);
            }
        }
        if frame.has_pending() {
            Ok(NodeResult::next_current(self.next))
        } else {
            Ok(NodeResult::drop())
        }
    }
}

impl<I, G> DriverNode<G> for TunInputDriverNode<I> where I: TunPacketSource {}

pub struct TunOutputDriverNode<O> {
    output: O,
    mode: TunDriverMode,
}

impl<O> TunOutputDriverNode<O> {
    #[inline]
    pub fn new(output: O) -> Self {
        Self {
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
        self.output.send_frame(runtime, frame, self.mode)?;
        Ok(NodeResult::drop())
    }
}

impl<O, G> DriverNode<G> for TunOutputDriverNode<O> where O: TunPacketSink {}

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
