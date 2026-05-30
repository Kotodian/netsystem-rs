use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, DriverNode, Node, NodeId, NodeResult, RouteMetadata,
};
use hammer_core::error::{CoreError, CoreResult};

pub use crate::packet::packet_route_metadata;

const DEFAULT_TUN_RECV_BATCH: usize = 256;

pub trait TunPacketSource {
    fn recv_frame<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
        interface_id: &str,
        max: usize,
    ) -> CoreResult<usize>;
}

pub trait TunPacketSink {
    fn send_frame<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<usize>;
}

pub struct TunInputDriverNode<I> {
    input: I,
    interface_id: String,
    next: NodeId,
    max_batch: usize,
}

impl<I> TunInputDriverNode<I> {
    #[inline]
    pub fn new(input: I, interface_id: impl Into<String>, next: NodeId) -> Self {
        Self {
            input,
            interface_id: interface_id.into(),
            next,
            max_batch: DEFAULT_TUN_RECV_BATCH,
        }
    }

    #[inline]
    pub fn with_max_batch(mut self, max_batch: usize) -> Self {
        self.max_batch = max_batch;
        self
    }
}

impl<I, G> Node<G> for TunInputDriverNode<I>
where
    I: TunPacketSource,
{
    #[inline]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let max_batch = self.max_batch.min(frame.remaining_capacity());
        self.input
            .recv_frame(runtime, frame, &self.interface_id, max_batch)?;
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
}

impl<O> TunOutputDriverNode<O> {
    #[inline]
    pub fn new(output: O) -> Self {
        Self { output }
    }
}

impl<O, G> Node<G> for TunOutputDriverNode<O>
where
    O: TunPacketSink,
{
    #[inline]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        self.output.send_frame(runtime, frame)?;
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
        max: usize,
    ) -> CoreResult<usize> {
        let mut inner = self.inner.borrow_mut();
        if inner.closed {
            return Err(CoreError::internal("memory TUN is closed"));
        }
        let mut received = 0usize;
        while received < max {
            let Some(packet) = inner.input.pop_front() else {
                break;
            };
            push_packet_to_frame(runtime, frame, interface_id, &packet)?;
            received += 1;
        }
        Ok(received)
    }
}

impl TunPacketSink for MemoryTunOutput {
    #[inline]
    fn send_frame<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
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
            let mut cursor = frame.pair_batch_cursor();
            cursor.prefetch_next_pair(runtime);
            'send: while let Some(batch) = cursor.next() {
                cursor.prefetch_next_pair(runtime);
                for index in batch.indices() {
                    let packet = runtime.copy_current_chain(index);
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
    packet: &[u8],
) -> CoreResult<()> {
    let metadata = RouteMetadata {
        inbound: interface_id.to_owned(),
        ..Default::default()
    };
    let index = runtime.alloc_index_with_bytes(metadata, packet)?;
    if let Err(err) = frame.push_index(index) {
        runtime.free_index(index);
        return Err(err);
    }
    Ok(())
}
