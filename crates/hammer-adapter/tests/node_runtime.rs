use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use hammer_adapter::{
    BufferFrame, BufferNodeError, BufferPoolArena, DataPlaneHandoff, DataPlaneInstructionSet,
    DataPlaneRuntime, DataWorkerId, DriverNode, InternalNode, NextFrame, Node, NodeHandle, NodeId,
    NodeNext, NodeNextEnqueue, NodeNextFrames, NodeNextStorage, NodeNextVectorEnqueue,
    NodeRegistration, NodeResult, PacketNextResolver, PacketTrace, RouteMetadata,
    TraceControlPlane, TraceFormatter, TraceInputPolicy, TracePolicy, add_packet_trace,
    process_cached_rewrite_next, process_cached_speculative_next,
};
use hammer_core::config::{TraceInputOptions, TraceOptions};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::log::{Factory, Level, LogWriter};
use hammer_infra::vec::Vec;
use std::time::Instant;

#[derive(Default)]
struct WakeCounter {
    wakes: AtomicUsize,
}

struct CaptureWriter {
    lines: Mutex<std::vec::Vec<(Level, String)>>,
}

impl LogWriter for CaptureWriter {
    fn write_message(&self, level: Level, message: String) {
        self.lines
            .lock()
            .expect("capture writer poisoned")
            .push((level, message));
    }
}

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

struct ForwardNode {
    next: NodeId,
    trace: Rc<RefCell<Vec<&'static str>>>,
}

impl Node<TestNode> for ForwardNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        self.trace.borrow_mut().push("forward");
        assert_eq!(frame.next_node(), None);
        Ok(NodeResult::next_current(self.next))
    }
}

impl DriverNode<TestNode> for ForwardNode {}

struct SourceDriverNode {
    next: NodeId,
    payload: Vec<u8>,
}

impl Node<TestNode> for SourceDriverNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        assert!(!frame.has_pending());
        let buffer = runtime.alloc_index_with_bytes(RouteMetadata::default(), &self.payload)?;
        frame.push_index(buffer)?;
        Ok(NodeResult::next_current(self.next))
    }
}

impl DriverNode<TestNode> for SourceDriverNode {}

struct SinkNode {
    trace: Rc<RefCell<Vec<&'static str>>>,
    payloads: Rc<RefCell<Vec<Vec<u8>>>>,
}

impl Node<TestNode> for SinkNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        self.trace.borrow_mut().push("sink");
        for buffer in frame.drain_pending() {
            self.payloads
                .borrow_mut()
                .push(runtime.copy_current_chain(buffer)?);
            runtime.free_index(buffer);
        }
        Ok(NodeResult::drop())
    }
}

impl DriverNode<TestNode> for SinkNode {}

struct CountNode {
    count: Rc<Cell<usize>>,
}

impl Node<TestNode> for CountNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        self.count.set(self.count.get() + 1);
        for buffer in frame.drain_pending() {
            runtime.free_index(buffer);
        }
        Ok(NodeResult::drop())
    }
}

impl InternalNode<TestNode> for CountNode {}

struct ErrorNode {
    code: u16,
    errors: Rc<RefCell<Vec<Option<BufferNodeError>>>>,
}

impl Node<TestNode> for ErrorNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for buffer in frame.drain_pending() {
            let mut buffer_ref = runtime.get_buffer_mut(buffer)?;
            let error = runtime.record_current_node_error(self.code)?;
            buffer_ref.set_node_error(error);
            self.errors.borrow_mut().push(buffer_ref.node_error());
            drop(buffer_ref);
            runtime.free_index(buffer);
        }
        Ok(NodeResult::drop())
    }
}

impl InternalNode<TestNode> for ErrorNode {}

struct InvalidNextNode {
    next: NodeId,
}

impl Node<TestNode> for InvalidNextNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime<TestNode>,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Ok(NodeResult::next_current(self.next))
    }
}

impl InternalNode<TestNode> for InvalidNextNode {}

struct ExplicitNextFrameNode {
    next: NodeId,
    payload: Option<&'static [u8]>,
}

impl Node<TestNode> for ExplicitNextFrameNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let split = runtime.alloc_frame_index()?;
        if let Some(payload) = self.payload {
            push_test_packet(runtime, split, payload);
        }
        Ok(NodeResult::next_frame(self.next, split))
    }
}

impl InternalNode<TestNode> for ExplicitNextFrameNode {}

struct MultipleNextFrameNode {
    first_next: NodeId,
    second_next: NodeId,
}

impl Node<TestNode> for MultipleNextFrameNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let first = runtime.alloc_frame_index()?;
        push_test_packet(runtime, first, b"first-next");
        let second = runtime.alloc_frame_index()?;
        push_test_packet(runtime, second, b"second-next");
        NodeResult::try_next_frames([
            NextFrame::Frame {
                node: self.first_next,
                frame: first,
            },
            NextFrame::Frame {
                node: self.second_next,
                frame: second,
            },
        ])
    }
}

impl InternalNode<TestNode> for MultipleNextFrameNode {}

struct HandoffNode {
    target_worker: DataWorkerId,
    target: NodeHandle,
}

impl Node<TestNode> for HandoffNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        runtime.handoff_frame(self.target_worker, self.target, frame)?;
        Ok(NodeResult::drop())
    }
}

impl InternalNode<TestNode> for HandoffNode {}

struct DeclaredNextOwnerNode {
    name: &'static str,
    next: [NodeId; TestNext::COUNT],
}

impl DeclaredNextOwnerNode {
    fn new(next: [NodeId; TestNext::COUNT]) -> Self {
        Self {
            name: "declared-next-owner",
            next,
        }
    }

    fn with_name(mut self, name: &'static str) -> Self {
        self.name = name;
        self
    }
}

impl Node<TestNode> for DeclaredNextOwnerNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let next = runtime.current_node_next(TestNext::Default)?;
        Ok(NodeResult::next_current(next))
    }
}

impl InternalNode<TestNode> for DeclaredNextOwnerNode {
    #[inline(always)]
    fn node_registration(&self) -> NodeRegistration {
        NodeRegistration::next(self.name, TestNext::COUNT)
    }

    #[inline(always)]
    fn node_initial_nexts(&self) -> &[NodeId] {
        &self.next
    }
}

struct DeclaredNextSiblingNode;

impl Node<TestNode> for DeclaredNextSiblingNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let next = runtime.current_node_next(TestNext::Default)?;
        Ok(NodeResult::next_current(next))
    }
}

impl InternalNode<TestNode> for DeclaredNextSiblingNode {
    #[inline(always)]
    fn node_registration(&self) -> NodeRegistration {
        NodeRegistration::sibling_of("declared-next-sibling", "declared-next-owner")
    }
}

struct IllegalSiblingWithNextNode {
    next: [NodeId; TestNext::COUNT],
}

impl Node<TestNode> for IllegalSiblingWithNextNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime<TestNode>,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Ok(NodeResult::drop())
    }
}

impl InternalNode<TestNode> for IllegalSiblingWithNextNode {
    #[inline(always)]
    fn node_registration(&self) -> NodeRegistration {
        NodeRegistration::sibling_of("illegal-sibling-with-next", "declared-next-owner")
    }

    #[inline(always)]
    fn node_initial_nexts(&self) -> &[NodeId] {
        &self.next
    }
}

struct IllegalPlainWithNextNode {
    next: [NodeId; TestNext::COUNT],
}

impl Node<TestNode> for IllegalPlainWithNextNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime<TestNode>,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Ok(NodeResult::drop())
    }
}

impl InternalNode<TestNode> for IllegalPlainWithNextNode {
    #[inline(always)]
    fn node_initial_nexts(&self) -> &[NodeId] {
        &self.next
    }
}

struct IllegalNextCountNode {
    next: [NodeId; TestNext::COUNT],
}

impl Node<TestNode> for IllegalNextCountNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime<TestNode>,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Ok(NodeResult::drop())
    }
}

impl InternalNode<TestNode> for IllegalNextCountNode {
    #[inline(always)]
    fn node_registration(&self) -> NodeRegistration {
        NodeRegistration::next("illegal-next-count", TestNext::COUNT + 1)
    }

    #[inline(always)]
    fn node_initial_nexts(&self) -> &[NodeId] {
        &self.next
    }
}

struct PointerSinkNode {
    payloads: Rc<RefCell<Vec<Vec<u8>>>>,
    current_ptrs: Rc<RefCell<Vec<usize>>>,
}

impl Node<TestNode> for PointerSinkNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for buffer in frame.drain_pending() {
            let current_ptr = runtime.current_ptr(buffer)? as usize;
            self.current_ptrs.borrow_mut().push(current_ptr);
            self.payloads
                .borrow_mut()
                .push(runtime.copy_current_chain(buffer)?);
            runtime.free_index(buffer);
        }
        Ok(NodeResult::drop())
    }
}

impl InternalNode<TestNode> for PointerSinkNode {}

struct SpeculativeSplitNode {
    speculative: NodeId,
    alternate: NodeId,
    frames_in_use_during_process: Rc<Cell<usize>>,
}

impl Node<TestNode> for SpeculativeSplitNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let result =
            NodeNextEnqueue::new(self.speculative).validate_frame(runtime, frame, |index| {
                let payload = runtime.copy_current(index)?;
                if payload == b"alternate" {
                    Ok(self.alternate)
                } else {
                    Ok(self.speculative)
                }
            })?;
        self.frames_in_use_during_process
            .set(runtime.frames_in_use());
        Ok(result)
    }
}

impl InternalNode<TestNode> for SpeculativeSplitNode {}

struct PrecomputedSplitNode {
    speculative: NodeId,
    alternate: NodeId,
    frames_in_use_during_process: Rc<Cell<usize>>,
}

impl Node<TestNode> for PrecomputedSplitNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let mut nexts = Vec::with_capacity(frame.pending_len());
        for index in frame.pending_indices().iter().copied() {
            let payload = runtime.copy_current(index)?;
            if payload == b"alternate" {
                nexts.push(self.alternate);
            } else {
                nexts.push(self.speculative);
            }
        }
        let result = NodeNextEnqueue::new(self.speculative)
            .validate_frame_with_nexts(runtime, frame, &nexts)?;
        self.frames_in_use_during_process
            .set(runtime.frames_in_use());
        Ok(result)
    }
}

impl InternalNode<TestNode> for PrecomputedSplitNode {}

struct BatchSplitNode {
    speculative: NodeId,
    alternate: NodeId,
    frames_in_use_during_process: Rc<Cell<usize>>,
}

impl Node<TestNode> for BatchSplitNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let Some(first) = frame.pending_indices().first().copied() else {
            return Ok(NodeResult::drop());
        };
        let first_next = {
            let payload = runtime.copy_current(first)?;
            if payload == b"alternate" {
                self.alternate
            } else {
                self.speculative
            }
        };
        let result = NodeNextEnqueue::new(first_next)
            .validate_frame_with_first_next_and_buffer_batch_prefetch(
                runtime,
                frame,
                first,
                first_next,
                |batch, index| batch.prefetch_read(index),
                |batch, index| {
                    batch.with_buffer(index, |buffer| {
                        if buffer.current() == b"alternate" {
                            self.alternate
                        } else {
                            self.speculative
                        }
                    })
                },
            )?;
        self.frames_in_use_during_process
            .set(runtime.frames_in_use());
        Ok(result)
    }
}

impl InternalNode<TestNode> for BatchSplitNode {}

struct ChunkedBatchSplitNode {
    speculative: NodeId,
    alternate: NodeId,
    chunk_lens: Rc<RefCell<Vec<usize>>>,
    frames_in_use_during_process: Rc<Cell<usize>>,
}

impl Node<TestNode> for ChunkedBatchSplitNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let Some(first) = frame.pending_indices().first().copied() else {
            return Ok(NodeResult::drop());
        };
        let first_next = {
            let payload = runtime.copy_current(first)?;
            if payload == b"alternate" {
                self.alternate
            } else {
                self.speculative
            }
        };
        let mut first_seen = false;
        let result = NodeNextEnqueue::new(first_next).validate_frame_with_buffer_batch_chunks(
            runtime,
            frame,
            |batch, indices| {
                for index in indices.iter().copied() {
                    batch.prefetch_read(index);
                }
            },
            |batch, indices, nexts| {
                self.chunk_lens.borrow_mut().push(indices.len());
                for (offset, index) in indices.iter().copied().enumerate() {
                    if !first_seen && index == first {
                        first_seen = true;
                        nexts[offset] = first_next;
                        continue;
                    }
                    let buffer = batch.buffer(index)?;
                    nexts[offset] = if buffer.current() == b"alternate" {
                        self.alternate
                    } else {
                        self.speculative
                    };
                }
                Ok(())
            },
        )?;
        self.frames_in_use_during_process
            .set(runtime.frames_in_use());
        Ok(result)
    }
}

impl InternalNode<TestNode> for ChunkedBatchSplitNode {}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TestNext {
    Default,
    Alternate,
}

impl TestNext {
    #[inline(always)]
    fn nodes(default_node: NodeId, alternate_node: NodeId) -> [NodeId; Self::COUNT] {
        [default_node, alternate_node]
    }
}

impl NodeNext for TestNext {
    const COUNT: usize = 2;

    #[inline(always)]
    fn slot(self) -> usize {
        match self {
            Self::Default => 0,
            Self::Alternate => 1,
        }
    }
}

struct PayloadNextResolver {
    default: NodeId,
    alternate: NodeId,
}

impl PacketNextResolver<TestNode> for PayloadNextResolver {
    #[inline(always)]
    fn next_for_index(
        &self,
        runtime: &DataPlaneRuntime<TestNode>,
        index: hammer_adapter::BufferIndex,
    ) -> CoreResult<NodeId> {
        let payload = runtime.copy_current(index)?;
        if payload == b"alternate" {
            Ok(self.alternate)
        } else {
            Ok(self.default)
        }
    }
}

struct CachedSpeculativeSplitNode {
    default: NodeId,
    alternate: NodeId,
    cached_next: Option<NodeId>,
    cached_after_process: Rc<Cell<Option<NodeId>>>,
    frames_in_use_during_process: Rc<Cell<usize>>,
}

impl Node<TestNode> for CachedSpeculativeSplitNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let resolver = PayloadNextResolver {
            default: self.default,
            alternate: self.alternate,
        };
        let result =
            process_cached_speculative_next(runtime, frame, &mut self.cached_next, &resolver)?;
        self.cached_after_process.set(self.cached_next);
        self.frames_in_use_during_process
            .set(runtime.frames_in_use());
        Ok(result)
    }
}

impl InternalNode<TestNode> for CachedSpeculativeSplitNode {}

struct CachedRewriteSplitNode {
    default: NodeId,
    alternate: NodeId,
    cached_next: Option<NodeId>,
    cached_after_process: Rc<Cell<Option<NodeId>>>,
    frames_in_use_during_process: Rc<Cell<usize>>,
}

impl Node<TestNode> for CachedRewriteSplitNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let resolver = PayloadNextResolver {
            default: self.default,
            alternate: self.alternate,
        };
        let result = process_cached_rewrite_next(runtime, frame, &mut self.cached_next, &resolver)?;
        self.cached_after_process.set(self.cached_next);
        self.frames_in_use_during_process
            .set(runtime.frames_in_use());
        Ok(result)
    }
}

impl InternalNode<TestNode> for CachedRewriteSplitNode {}

struct VectorChunkedSplitNode {
    speculative: NodeId,
    alternate: NodeId,
    cached_next: Rc<Cell<Option<NodeId>>>,
    chunk_lens: Rc<RefCell<Vec<usize>>>,
    frames_in_use_during_process: Rc<Cell<usize>>,
}

impl Node<TestNode> for VectorChunkedSplitNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let cached_next = self.cached_next.get().unwrap_or(self.speculative);
        let (result, cached_next) = NodeNextVectorEnqueue::new(cached_next)
            .enqueue_frame_with_buffer_batch_chunks(
                runtime,
                frame,
                |batch, indices| {
                    for index in indices.iter().copied() {
                        batch.prefetch_read(index);
                    }
                },
                |batch, indices, nexts| {
                    self.chunk_lens.borrow_mut().push(indices.len());
                    for (offset, index) in indices.iter().copied().enumerate() {
                        let buffer = batch.buffer(index)?;
                        nexts[offset] = if buffer.current() == b"alternate" {
                            self.alternate
                        } else {
                            self.speculative
                        };
                    }
                    Ok(())
                },
            )?;
        self.cached_next.set(Some(cached_next));
        self.frames_in_use_during_process
            .set(runtime.frames_in_use());
        Ok(result)
    }
}

impl InternalNode<TestNode> for VectorChunkedSplitNode {}

struct VectorErrorAfterEnqueueNode {
    alternate: NodeId,
}

impl Node<TestNode> for VectorErrorAfterEnqueueNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let mut seen = 0usize;
        NodeNextVectorEnqueue::new(self.alternate).enqueue_frame_with_buffer_batch_chunks(
            runtime,
            frame,
            |_batch, _indices| {},
            |_batch, indices, nexts| {
                for offset in 0..indices.len() {
                    seen += 1;
                    if seen == 1 {
                        nexts[offset] = self.alternate;
                    } else {
                        return Err(CoreError::internal("vector validation failed"));
                    }
                }
                Ok(())
            },
        )?;
        unreachable!("vector validation should fail")
    }
}

impl InternalNode<TestNode> for VectorErrorAfterEnqueueNode {}

struct SpeculativeErrorAfterSplitNode {
    speculative: NodeId,
    alternate: NodeId,
}

impl Node<TestNode> for SpeculativeErrorAfterSplitNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let mut seen = 0usize;
        NodeNextEnqueue::new(self.speculative).validate_frame(runtime, frame, |_index| {
            seen += 1;
            if seen == 1 {
                Ok(self.alternate)
            } else {
                Err(CoreError::internal("split validation failed"))
            }
        })
    }
}

impl InternalNode<TestNode> for SpeculativeErrorAfterSplitNode {}

struct SpeculativeErrorAfterSplitAndKeepNode {
    speculative: NodeId,
    alternate: NodeId,
}

impl Node<TestNode> for SpeculativeErrorAfterSplitAndKeepNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let mut seen = 0usize;
        NodeNextEnqueue::new(self.speculative).validate_frame(runtime, frame, |_index| {
            seen += 1;
            match seen {
                1 => Ok(self.alternate),
                2 => Ok(self.speculative),
                _ => Err(CoreError::internal("split validation failed after keep")),
            }
        })
    }
}

impl InternalNode<TestNode> for SpeculativeErrorAfterSplitAndKeepNode {}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestPacketTrace {
    bytes: std::vec::Vec<u8>,
}

impl PacketTrace for TestPacketTrace {
    #[inline]
    fn encode_trace(&self, out: &mut std::vec::Vec<u8>) {
        out.extend_from_slice(&self.bytes);
    }
}

fn format_test_packet_trace(payload: &[u8]) -> String {
    format!("TestPacketTrace bytes={payload:?}")
}

struct TraceNode {
    next: NodeId,
}

impl Node<TestNode> for TraceNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for index in frame.pending_indices().iter().copied() {
            add_packet_trace!(
                runtime,
                index,
                TestPacketTrace {
                    bytes: runtime.copy_current(index)?.to_vec(),
                },
            )?;
        }
        Ok(NodeResult::next_current(self.next))
    }

    #[inline(always)]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_test_packet_trace)
    }
}

impl InternalNode<TestNode> for TraceNode {
    #[inline(always)]
    fn node_registration(&self) -> NodeRegistration {
        NodeRegistration::next("trace-input", 1)
    }

    #[inline(always)]
    fn node_initial_nexts(&self) -> &[NodeId] {
        std::slice::from_ref(&self.next)
    }
}

enum TestNode {
    Forward(ForwardNode),
    SourceDriver(SourceDriverNode),
    Sink(SinkNode),
    Count(CountNode),
    Error(ErrorNode),
    InvalidNext(InvalidNextNode),
    ExplicitNextFrame(ExplicitNextFrameNode),
    MultipleNextFrame(MultipleNextFrameNode),
    Handoff(HandoffNode),
    DeclaredNextOwner(DeclaredNextOwnerNode),
    DeclaredNextSibling(DeclaredNextSiblingNode),
    IllegalSiblingWithNext(IllegalSiblingWithNextNode),
    IllegalPlainWithNext(IllegalPlainWithNextNode),
    IllegalNextCount(IllegalNextCountNode),
    PointerSink(PointerSinkNode),
    SpeculativeSplit(SpeculativeSplitNode),
    PrecomputedSplit(PrecomputedSplitNode),
    BatchSplit(BatchSplitNode),
    ChunkedBatchSplit(ChunkedBatchSplitNode),
    CachedSpeculativeSplit(CachedSpeculativeSplitNode),
    CachedRewriteSplit(CachedRewriteSplitNode),
    VectorChunkedSplit(VectorChunkedSplitNode),
    VectorErrorAfterEnqueue(VectorErrorAfterEnqueueNode),
    SpeculativeErrorAfterSplit(SpeculativeErrorAfterSplitNode),
    SpeculativeErrorAfterSplitAndKeep(SpeculativeErrorAfterSplitAndKeepNode),
    Trace(TraceNode),
}

impl From<ForwardNode> for TestNode {
    fn from(node: ForwardNode) -> Self {
        Self::Forward(node)
    }
}

impl From<SinkNode> for TestNode {
    fn from(node: SinkNode) -> Self {
        Self::Sink(node)
    }
}

impl From<SourceDriverNode> for TestNode {
    fn from(node: SourceDriverNode) -> Self {
        Self::SourceDriver(node)
    }
}

impl From<CountNode> for TestNode {
    fn from(node: CountNode) -> Self {
        Self::Count(node)
    }
}

impl From<ErrorNode> for TestNode {
    fn from(node: ErrorNode) -> Self {
        Self::Error(node)
    }
}

impl From<InvalidNextNode> for TestNode {
    fn from(node: InvalidNextNode) -> Self {
        Self::InvalidNext(node)
    }
}

impl From<ExplicitNextFrameNode> for TestNode {
    fn from(node: ExplicitNextFrameNode) -> Self {
        Self::ExplicitNextFrame(node)
    }
}

impl From<MultipleNextFrameNode> for TestNode {
    fn from(node: MultipleNextFrameNode) -> Self {
        Self::MultipleNextFrame(node)
    }
}

impl From<HandoffNode> for TestNode {
    fn from(node: HandoffNode) -> Self {
        Self::Handoff(node)
    }
}

impl From<DeclaredNextOwnerNode> for TestNode {
    fn from(node: DeclaredNextOwnerNode) -> Self {
        Self::DeclaredNextOwner(node)
    }
}

impl From<DeclaredNextSiblingNode> for TestNode {
    fn from(node: DeclaredNextSiblingNode) -> Self {
        Self::DeclaredNextSibling(node)
    }
}

impl From<IllegalSiblingWithNextNode> for TestNode {
    fn from(node: IllegalSiblingWithNextNode) -> Self {
        Self::IllegalSiblingWithNext(node)
    }
}

impl From<IllegalPlainWithNextNode> for TestNode {
    fn from(node: IllegalPlainWithNextNode) -> Self {
        Self::IllegalPlainWithNext(node)
    }
}

impl From<IllegalNextCountNode> for TestNode {
    fn from(node: IllegalNextCountNode) -> Self {
        Self::IllegalNextCount(node)
    }
}

impl From<PointerSinkNode> for TestNode {
    fn from(node: PointerSinkNode) -> Self {
        Self::PointerSink(node)
    }
}

impl From<SpeculativeSplitNode> for TestNode {
    fn from(node: SpeculativeSplitNode) -> Self {
        Self::SpeculativeSplit(node)
    }
}

impl From<PrecomputedSplitNode> for TestNode {
    fn from(node: PrecomputedSplitNode) -> Self {
        Self::PrecomputedSplit(node)
    }
}

impl From<BatchSplitNode> for TestNode {
    fn from(node: BatchSplitNode) -> Self {
        Self::BatchSplit(node)
    }
}

impl From<ChunkedBatchSplitNode> for TestNode {
    fn from(node: ChunkedBatchSplitNode) -> Self {
        Self::ChunkedBatchSplit(node)
    }
}

impl From<CachedSpeculativeSplitNode> for TestNode {
    fn from(node: CachedSpeculativeSplitNode) -> Self {
        Self::CachedSpeculativeSplit(node)
    }
}

impl From<CachedRewriteSplitNode> for TestNode {
    fn from(node: CachedRewriteSplitNode) -> Self {
        Self::CachedRewriteSplit(node)
    }
}

impl From<VectorChunkedSplitNode> for TestNode {
    fn from(node: VectorChunkedSplitNode) -> Self {
        Self::VectorChunkedSplit(node)
    }
}

impl From<VectorErrorAfterEnqueueNode> for TestNode {
    fn from(node: VectorErrorAfterEnqueueNode) -> Self {
        Self::VectorErrorAfterEnqueue(node)
    }
}

impl From<SpeculativeErrorAfterSplitNode> for TestNode {
    fn from(node: SpeculativeErrorAfterSplitNode) -> Self {
        Self::SpeculativeErrorAfterSplit(node)
    }
}

impl From<SpeculativeErrorAfterSplitAndKeepNode> for TestNode {
    fn from(node: SpeculativeErrorAfterSplitAndKeepNode) -> Self {
        Self::SpeculativeErrorAfterSplitAndKeep(node)
    }
}

impl From<TraceNode> for TestNode {
    fn from(node: TraceNode) -> Self {
        Self::Trace(node)
    }
}

impl Node<TestNode> for TestNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        match self {
            Self::Forward(node) => node.process(runtime, frame),
            Self::SourceDriver(node) => node.process(runtime, frame),
            Self::Sink(node) => node.process(runtime, frame),
            Self::Count(node) => node.process(runtime, frame),
            Self::Error(node) => node.process(runtime, frame),
            Self::InvalidNext(node) => node.process(runtime, frame),
            Self::ExplicitNextFrame(node) => node.process(runtime, frame),
            Self::MultipleNextFrame(node) => node.process(runtime, frame),
            Self::Handoff(node) => node.process(runtime, frame),
            Self::DeclaredNextOwner(node) => node.process(runtime, frame),
            Self::DeclaredNextSibling(node) => node.process(runtime, frame),
            Self::IllegalSiblingWithNext(node) => node.process(runtime, frame),
            Self::IllegalPlainWithNext(node) => node.process(runtime, frame),
            Self::IllegalNextCount(node) => node.process(runtime, frame),
            Self::PointerSink(node) => node.process(runtime, frame),
            Self::SpeculativeSplit(node) => node.process(runtime, frame),
            Self::PrecomputedSplit(node) => node.process(runtime, frame),
            Self::BatchSplit(node) => node.process(runtime, frame),
            Self::ChunkedBatchSplit(node) => node.process(runtime, frame),
            Self::CachedSpeculativeSplit(node) => node.process(runtime, frame),
            Self::CachedRewriteSplit(node) => node.process(runtime, frame),
            Self::VectorChunkedSplit(node) => node.process(runtime, frame),
            Self::VectorErrorAfterEnqueue(node) => node.process(runtime, frame),
            Self::SpeculativeErrorAfterSplit(node) => node.process(runtime, frame),
            Self::SpeculativeErrorAfterSplitAndKeep(node) => node.process(runtime, frame),
            Self::Trace(node) => node.process(runtime, frame),
        }
    }
}

fn assert_internal_node<I>(node: &I)
where
    I: InternalNode<TestNode>,
{
    let _ = node;
}

fn assert_driver_node<I>(node: &I)
where
    I: DriverNode<TestNode>,
{
    let _ = node;
}

#[test]
fn node_runtime_dispatches_pending_frame_to_next_node() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 4, 2, 2);
    let trace = Rc::new(RefCell::new(Vec::new()));
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register(TestNode::Sink(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&payloads),
    }));
    let forward = runtime.nodes().register(TestNode::Forward(ForwardNode {
        next: sink,
        trace: Rc::clone(&trace),
    }));
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"packet")
        .expect("alloc packet");
    runtime
        .with_frame_mut(frame, |frame| frame.push_index(buffer))
        .expect("mutate frame")
        .expect("push packet");

    assert!(
        runtime
            .schedule_frame(forward, frame)
            .expect("schedule frame")
    );

    assert_eq!(runtime.nodes().pending_len(), 1);
    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(&*trace.borrow(), &["forward", "sink"]);
    assert_eq!(&*payloads.borrow(), &[b"packet".to_vec()]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn node_runtime_does_not_schedule_empty_frame() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 2, 1, 1);
    let count = Rc::new(Cell::new(0));
    let node = runtime.nodes().register(TestNode::Count(CountNode {
        count: Rc::clone(&count),
    }));
    let frame = runtime.alloc_frame_index().expect("alloc frame");

    assert!(!runtime.schedule_frame(node, frame).expect("schedule empty"));
    assert_eq!(runtime.nodes().pending_len(), 0);
    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 0);
    assert_eq!(count.get(), 0);

    runtime.free_frame_index(frame).expect("free empty frame");
}

#[test]
fn trace_control_records_marked_packet_entries_when_buffer_is_freed() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 4, 2, 2);
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register(TestNode::Sink(SinkNode {
        trace: Rc::new(RefCell::new(Vec::new())),
        payloads,
    }));
    let trace_node = runtime.nodes().register_internal(TraceNode { next: sink });
    let control = TraceControlPlane::new(8);
    control.publish(TracePolicy {
        enabled: true,
        record_capacity: 8,
        packet_capacity: 4,
        inputs: vec![TraceInputPolicy {
            node: trace_node,
            count: 1,
        }],
    });
    runtime.set_trace_control(Some(control.handle()), 4);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"packet")
        .expect("alloc packet");
    runtime
        .try_mark_trace(trace_node, buffer)
        .expect("mark packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(buffer)
        .expect("push packet");

    assert!(runtime.schedule_frame(trace_node, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(control.drain_completed(), 1);
    let records = control.take_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].input_node, trace_node);
    assert_eq!(records[0].entries.len(), 1);
    assert_eq!(records[0].entries[0].node, trace_node);
    assert_eq!(records[0].entries[0].node_name, Some("trace-input"));
    assert_eq!(records[0].entries[0].payload_bytes, b"packet");
    assert_eq!(
        records[0].entries[0].format_payload(),
        "TestPacketTrace bytes=[112, 97, 99, 107, 101, 116]"
    );
    assert_eq!(runtime.in_use_buffers(), 0);
    assert_eq!(runtime.frames_in_use(), 0);
}

#[test]
fn trace_control_honors_input_quota() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 4, 4, 2);
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register(TestNode::Sink(SinkNode {
        trace: Rc::new(RefCell::new(Vec::new())),
        payloads,
    }));
    let trace_node = runtime.nodes().register_internal(TraceNode { next: sink });
    let control = TraceControlPlane::new(8);
    control.publish(TracePolicy {
        enabled: true,
        record_capacity: 8,
        packet_capacity: 4,
        inputs: vec![TraceInputPolicy {
            node: trace_node,
            count: 1,
        }],
    });
    runtime.set_trace_control(Some(control.handle()), 4);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    for payload in [b"first".as_slice(), b"second".as_slice()] {
        let buffer = runtime
            .alloc_index_with_bytes(RouteMetadata::default(), payload)
            .expect("alloc packet");
        runtime
            .try_mark_trace(trace_node, buffer)
            .expect("mark packet");
        runtime
            .get_frame_mut(frame)
            .expect("mutate frame")
            .push_index(buffer)
            .expect("push packet");
    }

    assert!(runtime.schedule_frame(trace_node, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(control.drain_completed(), 1);
    let records = control.take_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].entries[0].payload_bytes, b"first");
}

#[test]
fn trace_control_publish_updates_packet_capacity_for_new_marks() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 8, 4, 2);
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register(TestNode::Sink(SinkNode {
        trace: Rc::new(RefCell::new(Vec::new())),
        payloads,
    }));
    let trace_node = runtime.nodes().register_internal(TraceNode { next: sink });
    let control = TraceControlPlane::new(8);
    control.publish(TracePolicy {
        enabled: true,
        record_capacity: 8,
        packet_capacity: 1,
        inputs: vec![TraceInputPolicy {
            node: trace_node,
            count: 2,
        }],
    });
    runtime.set_trace_control(Some(control.handle()), 1);
    let first = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"first")
        .expect("alloc first packet");
    let second = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"second")
        .expect("alloc second packet");
    runtime
        .try_mark_trace(trace_node, first)
        .expect("mark first packet");
    runtime
        .try_mark_trace(trace_node, second)
        .expect("packet capacity rejects second mark");
    assert!(
        runtime
            .get_buffer(second)
            .expect("second buffer")
            .trace_mark()
            .is_none()
    );

    control.publish(TracePolicy {
        enabled: true,
        record_capacity: 8,
        packet_capacity: 2,
        inputs: vec![TraceInputPolicy {
            node: trace_node,
            count: 1,
        }],
    });
    runtime
        .try_mark_trace(trace_node, second)
        .expect("new packet capacity allows second mark");

    assert!(
        runtime
            .get_buffer(second)
            .expect("second buffer")
            .trace_mark()
            .is_some()
    );
    runtime.free_index(first);
    runtime.free_index(second);
}

#[test]
fn trace_control_disabled_policy_does_not_mark_packets() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 4, 2, 2);
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register(TestNode::Sink(SinkNode {
        trace: Rc::new(RefCell::new(Vec::new())),
        payloads,
    }));
    let trace_node = runtime.nodes().register_internal(TraceNode { next: sink });
    let control = TraceControlPlane::new(8);
    control.publish(TracePolicy {
        enabled: false,
        record_capacity: 8,
        packet_capacity: 4,
        inputs: vec![TraceInputPolicy {
            node: trace_node,
            count: 1,
        }],
    });
    runtime.set_trace_control(Some(control.handle()), 4);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"packet");

    assert!(runtime.schedule_frame(trace_node, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(control.drain_completed(), 0);
    assert!(control.take_records().is_empty());
}

#[test]
fn trace_control_empty_inputs_do_not_mark_packets() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 4, 2, 2);
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register(TestNode::Sink(SinkNode {
        trace: Rc::new(RefCell::new(Vec::new())),
        payloads,
    }));
    let trace_node = runtime.nodes().register_internal(TraceNode { next: sink });
    let control = TraceControlPlane::new(8);
    control.publish(TracePolicy {
        enabled: true,
        record_capacity: 8,
        packet_capacity: 4,
        inputs: std::vec::Vec::new(),
    });
    runtime.set_trace_control(Some(control.handle()), 4);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"packet");

    assert!(runtime.schedule_frame(trace_node, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(control.drain_completed(), 0);
    assert!(control.take_records().is_empty());
}

#[test]
fn trace_control_replacement_epoch_stops_old_policy_new_quota() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 8, 4, 2);
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register(TestNode::Sink(SinkNode {
        trace: Rc::new(RefCell::new(Vec::new())),
        payloads,
    }));
    let trace_node = runtime.nodes().register_internal(TraceNode { next: sink });
    let control = TraceControlPlane::new(8);
    let first_epoch = control.publish(TracePolicy {
        enabled: true,
        record_capacity: 8,
        packet_capacity: 4,
        inputs: vec![TraceInputPolicy {
            node: trace_node,
            count: 4,
        }],
    });
    runtime.set_trace_control(Some(control.handle()), 4);
    let first = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"old")
        .expect("alloc first packet");
    runtime
        .try_mark_trace(trace_node, first)
        .expect("mark first packet");
    let first_mark = runtime
        .get_buffer(first)
        .expect("first buffer")
        .trace_mark()
        .expect("first packet marked");
    assert_eq!(first_mark.epoch, first_epoch);

    let second_epoch = control.publish(TracePolicy {
        enabled: true,
        record_capacity: 8,
        packet_capacity: 4,
        inputs: std::vec::Vec::new(),
    });
    assert_ne!(first_epoch, second_epoch);
    let second = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"new")
        .expect("alloc second packet");
    runtime
        .try_mark_trace(trace_node, second)
        .expect("new policy must not mark without inputs");

    assert!(
        runtime
            .get_buffer(second)
            .expect("second buffer")
            .trace_mark()
            .is_none()
    );

    runtime.free_index(first);
    runtime.free_index(second);
    assert_eq!(control.drain_completed(), 0);
    assert!(control.take_records().is_empty());
}

#[test]
fn trace_marked_old_epoch_packets_keep_recording_after_policy_replacement() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 8, 4, 2);
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register(TestNode::Sink(SinkNode {
        trace: Rc::new(RefCell::new(Vec::new())),
        payloads,
    }));
    let trace_node = runtime.nodes().register_internal(TraceNode { next: sink });
    let control = TraceControlPlane::new(8);
    let first_epoch = control.publish(TracePolicy {
        enabled: true,
        record_capacity: 8,
        packet_capacity: 4,
        inputs: vec![TraceInputPolicy {
            node: trace_node,
            count: 1,
        }],
    });
    runtime.set_trace_control(Some(control.handle()), 4);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"oldpkt")
        .expect("alloc packet");
    runtime
        .try_mark_trace(trace_node, buffer)
        .expect("mark packet");
    let mark = runtime
        .get_buffer(buffer)
        .expect("buffer")
        .trace_mark()
        .expect("packet marked");
    assert_eq!(mark.epoch, first_epoch);
    control.publish(TracePolicy {
        enabled: true,
        record_capacity: 8,
        packet_capacity: 4,
        inputs: std::vec::Vec::new(),
    });
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(buffer)
        .expect("push packet");

    assert!(runtime.schedule_frame(trace_node, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(control.drain_completed(), 1);
    let records = control.take_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].epoch, first_epoch);
    assert_eq!(records[0].entries.len(), 1);
    assert_eq!(records[0].entries[0].payload_bytes, b"oldpkt");
}

#[test]
fn scheduled_internal_nodes_do_not_start_packet_traces() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 4, 2, 2);
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register(TestNode::Sink(SinkNode {
        trace: Rc::new(RefCell::new(Vec::new())),
        payloads,
    }));
    let trace_node = runtime.nodes().register_internal(TraceNode { next: sink });
    let control = TraceControlPlane::new(8);
    control.publish(TracePolicy {
        enabled: true,
        record_capacity: 8,
        packet_capacity: 4,
        inputs: vec![TraceInputPolicy {
            node: trace_node,
            count: 1,
        }],
    });
    runtime.set_trace_control(Some(control.handle()), 4);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"packet");

    assert!(runtime.schedule_frame(trace_node, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(control.drain_completed(), 0);
    assert!(control.take_records().is_empty());
}

#[test]
fn trace_add_trace_noops_for_unmarked_packets() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 4, 2, 2);
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register(TestNode::Sink(SinkNode {
        trace: Rc::new(RefCell::new(Vec::new())),
        payloads,
    }));
    let trace_node = runtime.nodes().register_internal(TraceNode { next: sink });
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"packet");

    assert!(runtime.schedule_frame(trace_node, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn trace_macro_does_not_build_for_unmarked_packets() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 4, 2, 2);
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register(TestNode::Sink(SinkNode {
        trace: Rc::new(RefCell::new(Vec::new())),
        payloads,
    }));
    let _trace_node = runtime.nodes().register_internal(TraceNode { next: sink });
    let index = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"packet")
        .expect("alloc packet");
    let built = Cell::new(false);

    add_packet_trace!(runtime, index, {
        built.set(true);
        TestPacketTrace {
            bytes: b"should-not-build".to_vec(),
        }
    },)
    .expect("unmarked trace macro noops");

    assert!(!built.get(), "trace payload builder must stay cold");
    runtime.free_index(index);
}

#[test]
fn trace_completed_queue_overflow_drops_records_without_blocking() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 8, 8, 2);
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register(TestNode::Sink(SinkNode {
        trace: Rc::new(RefCell::new(Vec::new())),
        payloads,
    }));
    let trace_node = runtime.nodes().register_internal(TraceNode { next: sink });
    let control = TraceControlPlane::new(1);
    control.publish(TracePolicy {
        enabled: true,
        record_capacity: 1,
        packet_capacity: 8,
        inputs: vec![TraceInputPolicy {
            node: trace_node,
            count: 2,
        }],
    });
    runtime.set_trace_control(Some(control.handle()), 8);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_marked_test_packet(&runtime, frame, trace_node, b"first");
    push_marked_test_packet(&runtime, frame, trace_node, b"second");

    assert!(runtime.schedule_frame(trace_node, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(control.drain_completed(), 1);
    assert_eq!(control.dropped_completed(), 1);
    let records = control.take_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].entries[0].payload_bytes, b"first");
}

#[test]
fn trace_publish_updates_completed_queue_capacity() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 8, 8, 2);
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register(TestNode::Sink(SinkNode {
        trace: Rc::new(RefCell::new(Vec::new())),
        payloads,
    }));
    let trace_node = runtime.nodes().register_internal(TraceNode { next: sink });
    let control = TraceControlPlane::new(1);
    control.publish(TracePolicy {
        enabled: true,
        record_capacity: 3,
        packet_capacity: 8,
        inputs: vec![TraceInputPolicy {
            node: trace_node,
            count: 3,
        }],
    });
    runtime.set_trace_control(Some(control.handle()), 8);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_marked_test_packet(&runtime, frame, trace_node, b"first");
    push_marked_test_packet(&runtime, frame, trace_node, b"second");
    push_marked_test_packet(&runtime, frame, trace_node, b"third");

    assert!(runtime.schedule_frame(trace_node, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(control.drain_completed(), 3);
    assert_eq!(control.dropped_completed(), 0);
    let records = control.take_records();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].entries[0].payload_bytes, b"first");
    assert_eq!(records[1].entries[0].payload_bytes, b"second");
    assert_eq!(records[2].entries[0].payload_bytes, b"third");
}

#[test]
fn trace_publish_preserves_drained_records_when_capacity_grows() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 8, 8, 2);
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register(TestNode::Sink(SinkNode {
        trace: Rc::new(RefCell::new(Vec::new())),
        payloads,
    }));
    let trace_node = runtime.nodes().register_internal(TraceNode { next: sink });
    let control = TraceControlPlane::new(1);
    control.publish(TracePolicy {
        enabled: true,
        record_capacity: 1,
        packet_capacity: 8,
        inputs: vec![TraceInputPolicy {
            node: trace_node,
            count: 1,
        }],
    });
    runtime.set_trace_control(Some(control.handle()), 8);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_marked_test_packet(&runtime, frame, trace_node, b"first");

    assert!(runtime.schedule_frame(trace_node, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(control.drain_completed(), 1);

    control.publish(TracePolicy {
        enabled: true,
        record_capacity: 3,
        packet_capacity: 8,
        inputs: vec![TraceInputPolicy {
            node: trace_node,
            count: 2,
        }],
    });
    let frame = runtime.alloc_frame_index().expect("alloc second frame");
    push_marked_test_packet(&runtime, frame, trace_node, b"second");
    push_marked_test_packet(&runtime, frame, trace_node, b"third");

    assert!(runtime.schedule_frame(trace_node, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(control.drain_completed(), 2);
    let records = control.take_records();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].entries[0].payload_bytes, b"first");
    assert_eq!(records[1].entries[0].payload_bytes, b"second");
    assert_eq!(records[2].entries[0].payload_bytes, b"third");
}

#[test]
fn trace_record_sink_prints_completed_records_through_control_logger() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 4, 2, 2);
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register(TestNode::Sink(SinkNode {
        trace: Rc::new(RefCell::new(Vec::new())),
        payloads,
    }));
    let trace_node = runtime.nodes().register_internal(TraceNode { next: sink });
    let control = TraceControlPlane::new(8);
    control.publish(TracePolicy {
        enabled: true,
        record_capacity: 8,
        packet_capacity: 4,
        inputs: vec![TraceInputPolicy {
            node: trace_node,
            count: 1,
        }],
    });
    runtime.set_trace_control(Some(control.handle()), 4);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_marked_test_packet(&runtime, frame, trace_node, b"packet");
    assert!(runtime.schedule_frame(trace_node, frame).expect("schedule"));
    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);

    let writer = Arc::new(CaptureWriter {
        lines: Mutex::new(std::vec::Vec::new()),
    });
    let logger = Factory::new(Instant::now(), writer.clone()).new_logger("trace-control");

    assert_eq!(control.sink().drain_completed_with_logger(&logger), 1);

    let captured = writer.lines.lock().expect("capture writer poisoned");
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].0, Level::Debug);
    assert!(captured[0].1.contains("trace-control"));
    assert!(
        captured[0]
            .1
            .contains("packet trace epoch=1 input=trace-input")
    );
    assert!(
        captured[0]
            .1
            .contains("trace-input: TestPacketTrace bytes=[112, 97, 99, 107, 101, 116]")
    );
}

#[test]
fn trace_control_publish_options_resolves_declared_node_names() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 2, 1, 1);
    let sink = runtime.nodes().register(TestNode::Count(CountNode {
        count: Rc::new(Cell::new(0)),
    }));
    let trace_node = runtime.nodes().register_internal(TraceNode { next: sink });
    let control = TraceControlPlane::new(4);
    let epoch = control
        .publish_options(
            &TraceOptions {
                enabled: true,
                record_capacity: 4,
                packet_capacity: 2,
                inputs: vec![TraceInputOptions {
                    node: "trace-input".to_owned(),
                    count: 3,
                }],
            },
            |name| runtime.node_by_name(name),
        )
        .expect("publish trace options");

    assert_eq!(epoch, 1);
    assert_eq!(runtime.node_by_name("trace-input"), Some(trace_node));
    let err = control
        .publish_options(
            &TraceOptions {
                enabled: true,
                record_capacity: 4,
                packet_capacity: 2,
                inputs: vec![TraceInputOptions {
                    node: "missing".to_owned(),
                    count: 1,
                }],
            },
            |name| runtime.node_by_name(name),
        )
        .expect_err("missing trace input node must fail");
    assert!(
        err.to_string()
            .contains("trace.inputs node is not a declared packet node: missing"),
        "unexpected error: {err}"
    );
    let epoch = control
        .publish_options(
            &TraceOptions {
                enabled: false,
                record_capacity: 4,
                packet_capacity: 2,
                inputs: vec![TraceInputOptions {
                    node: "missing".to_owned(),
                    count: 1,
                }],
            },
            |_name| panic!("disabled trace inputs should not resolve packet graph nodes"),
        )
        .expect("disabled trace options should publish without input resolution");
    assert_eq!(epoch, 2);
}

#[test]
fn node_runtime_registers_internal_role_node() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 2, 1, 1);
    let count = Rc::new(Cell::new(0));
    let node = CountNode {
        count: Rc::clone(&count),
    };
    assert_internal_node(&node);
    let node = runtime.nodes().register_internal(node);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"packet")
        .expect("alloc packet");
    runtime
        .with_frame_mut(frame, |frame| frame.push_index(buffer))
        .expect("mutate frame")
        .expect("push packet");

    assert!(runtime.schedule_frame(node, frame).expect("schedule frame"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 1);
    assert_eq!(count.get(), 1);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn node_next_storage_reads_static_array_by_key() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 2, 1, 1);
    let default = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let alternate = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let next = [default, alternate];

    assert_eq!(NodeNextStorage::next(&next, TestNext::Default), default);
    assert_eq!(NodeNextStorage::next(&next, TestNext::Alternate), alternate);
}

#[test]
fn node_next_storage_reads_single_next_for_unit_key() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 2, 1, 1);
    let next = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });

    assert_eq!(NodeNextStorage::next(&next, ()), next);
}

#[test]
fn speculative_enqueue_keeps_matching_next_in_current_frame() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 8, 4);
    let default_payloads = Rc::new(RefCell::new(Vec::new()));
    let alternate_payloads = Rc::new(RefCell::new(Vec::new()));
    let trace = Rc::new(RefCell::new(Vec::new()));
    let default = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&default_payloads),
    });
    let alternate = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&alternate_payloads),
    });
    let frames_seen = Rc::new(Cell::new(0));
    let split = runtime.nodes().register_internal(SpeculativeSplitNode {
        speculative: default,
        alternate,
        frames_in_use_during_process: Rc::clone(&frames_seen),
    });
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"default-1");
    push_test_packet(&runtime, frame, b"default-2");
    push_test_packet(&runtime, frame, b"default-3");

    assert!(runtime.schedule_frame(split, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(frames_seen.get(), 1);
    assert_eq!(
        &*default_payloads.borrow(),
        &[
            b"default-1".to_vec(),
            b"default-2".to_vec(),
            b"default-3".to_vec()
        ]
    );
    assert!(alternate_payloads.borrow().is_empty());
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn speculative_enqueue_splits_mismatched_next_to_separate_frame() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 8, 4);
    let default_payloads = Rc::new(RefCell::new(Vec::new()));
    let alternate_payloads = Rc::new(RefCell::new(Vec::new()));
    let trace = Rc::new(RefCell::new(Vec::new()));
    let default = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&default_payloads),
    });
    let alternate = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&alternate_payloads),
    });
    let frames_seen = Rc::new(Cell::new(0));
    let split = runtime.nodes().register_internal(SpeculativeSplitNode {
        speculative: default,
        alternate,
        frames_in_use_during_process: Rc::clone(&frames_seen),
    });
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"default-1");
    push_test_packet(&runtime, frame, b"alternate");
    push_test_packet(&runtime, frame, b"default-2");

    assert!(runtime.schedule_frame(split, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_eq!(frames_seen.get(), 2);
    assert_eq!(
        &*default_payloads.borrow(),
        &[b"default-1".to_vec(), b"default-2".to_vec()]
    );
    assert_eq!(&*alternate_payloads.borrow(), &[b"alternate".to_vec()]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn cached_speculative_next_updates_cache_to_trailing_next() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 8, 4);
    let default_payloads = Rc::new(RefCell::new(Vec::new()));
    let alternate_payloads = Rc::new(RefCell::new(Vec::new()));
    let trace = Rc::new(RefCell::new(Vec::new()));
    let default = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&default_payloads),
    });
    let alternate = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&alternate_payloads),
    });
    let cached_after_process = Rc::new(Cell::new(None));
    let frames_seen = Rc::new(Cell::new(0));
    let split = runtime
        .nodes()
        .register_internal(CachedSpeculativeSplitNode {
            default,
            alternate,
            cached_next: None,
            cached_after_process: Rc::clone(&cached_after_process),
            frames_in_use_during_process: Rc::clone(&frames_seen),
        });
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"default-1");
    push_test_packet(&runtime, frame, b"alternate");
    push_test_packet(&runtime, frame, b"alternate");

    assert!(runtime.schedule_frame(split, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_eq!(cached_after_process.get(), Some(alternate));
    assert_eq!(frames_seen.get(), 2);
    assert_eq!(&*default_payloads.borrow(), &[b"default-1".to_vec()]);
    assert_eq!(
        &*alternate_payloads.borrow(),
        &[b"alternate".to_vec(), b"alternate".to_vec()]
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn cached_rewrite_next_updates_cache_to_trailing_next() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 8, 4);
    let default_payloads = Rc::new(RefCell::new(Vec::new()));
    let alternate_payloads = Rc::new(RefCell::new(Vec::new()));
    let trace = Rc::new(RefCell::new(Vec::new()));
    let default = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&default_payloads),
    });
    let alternate = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&alternate_payloads),
    });
    let cached_after_process = Rc::new(Cell::new(None));
    let frames_seen = Rc::new(Cell::new(0));
    let split = runtime.nodes().register_internal(CachedRewriteSplitNode {
        default,
        alternate,
        cached_next: Some(default),
        cached_after_process: Rc::clone(&cached_after_process),
        frames_in_use_during_process: Rc::clone(&frames_seen),
    });
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"alternate");
    push_test_packet(&runtime, frame, b"alternate");

    assert!(runtime.schedule_frame(split, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(cached_after_process.get(), Some(alternate));
    assert_eq!(frames_seen.get(), 2);
    assert!(default_payloads.borrow().is_empty());
    assert_eq!(
        &*alternate_payloads.borrow(),
        &[b"alternate".to_vec(), b"alternate".to_vec()]
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn precomputed_nexts_split_mismatched_next_to_separate_frame() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 8, 4);
    let default_payloads = Rc::new(RefCell::new(Vec::new()));
    let alternate_payloads = Rc::new(RefCell::new(Vec::new()));
    let trace = Rc::new(RefCell::new(Vec::new()));
    let default = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&default_payloads),
    });
    let alternate = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&alternate_payloads),
    });
    let frames_seen = Rc::new(Cell::new(0));
    let split = runtime.nodes().register_internal(PrecomputedSplitNode {
        speculative: default,
        alternate,
        frames_in_use_during_process: Rc::clone(&frames_seen),
    });
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"default-1");
    push_test_packet(&runtime, frame, b"alternate");
    push_test_packet(&runtime, frame, b"default-2");

    assert!(runtime.schedule_frame(split, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_eq!(frames_seen.get(), 2);
    assert_eq!(
        &*default_payloads.borrow(),
        &[b"default-1".to_vec(), b"default-2".to_vec()]
    );
    assert_eq!(&*alternate_payloads.borrow(), &[b"alternate".to_vec()]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn batch_nexts_split_mismatched_next_to_separate_frame() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 8, 4);
    let default_payloads = Rc::new(RefCell::new(Vec::new()));
    let alternate_payloads = Rc::new(RefCell::new(Vec::new()));
    let trace = Rc::new(RefCell::new(Vec::new()));
    let default = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&default_payloads),
    });
    let alternate = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&alternate_payloads),
    });
    let frames_seen = Rc::new(Cell::new(0));
    let split = runtime.nodes().register_internal(BatchSplitNode {
        speculative: default,
        alternate,
        frames_in_use_during_process: Rc::clone(&frames_seen),
    });
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"default-1");
    push_test_packet(&runtime, frame, b"alternate");
    push_test_packet(&runtime, frame, b"default-2");

    assert!(runtime.schedule_frame(split, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_eq!(frames_seen.get(), 2);
    assert_eq!(
        &*default_payloads.borrow(),
        &[b"default-1".to_vec(), b"default-2".to_vec()]
    );
    assert_eq!(&*alternate_payloads.borrow(), &[b"alternate".to_vec()]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn chunked_batch_nexts_classify_native_width_and_split_mismatches() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities_and_instruction_set(
        32,
        8,
        8,
        4,
        DataPlaneInstructionSet::native(),
    );
    let default_payloads = Rc::new(RefCell::new(Vec::new()));
    let alternate_payloads = Rc::new(RefCell::new(Vec::new()));
    let trace = Rc::new(RefCell::new(Vec::new()));
    let default = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&default_payloads),
    });
    let alternate = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&alternate_payloads),
    });
    let chunk_lens = Rc::new(RefCell::new(Vec::new()));
    let frames_seen = Rc::new(Cell::new(0));
    let split = runtime.nodes().register_internal(ChunkedBatchSplitNode {
        speculative: default,
        alternate,
        chunk_lens: Rc::clone(&chunk_lens),
        frames_in_use_during_process: Rc::clone(&frames_seen),
    });
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"default-1");
    push_test_packet(&runtime, frame, b"default-2");
    push_test_packet(&runtime, frame, b"alternate");
    push_test_packet(&runtime, frame, b"default-3");
    push_test_packet(&runtime, frame, b"default-4");

    assert!(runtime.schedule_frame(split, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_eq!(&*chunk_lens.borrow(), &[4, 1]);
    assert_eq!(frames_seen.get(), 2);
    assert_eq!(
        &*default_payloads.borrow(),
        &[
            b"default-1".to_vec(),
            b"default-2".to_vec(),
            b"default-3".to_vec(),
            b"default-4".to_vec()
        ]
    );
    assert_eq!(&*alternate_payloads.borrow(), &[b"alternate".to_vec()]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn vector_next_enqueue_consumes_input_frame_and_schedules_owned_next_frames() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities_and_instruction_set(
        32,
        8,
        8,
        6,
        DataPlaneInstructionSet::native(),
    );
    let default_payloads = Rc::new(RefCell::new(Vec::new()));
    let alternate_payloads = Rc::new(RefCell::new(Vec::new()));
    let trace = Rc::new(RefCell::new(Vec::new()));
    let default = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&default_payloads),
    });
    let alternate = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&alternate_payloads),
    });
    let cached_next = Rc::new(Cell::new(None));
    let chunk_lens = Rc::new(RefCell::new(Vec::new()));
    let frames_seen = Rc::new(Cell::new(0));
    let split = runtime.nodes().register_internal(VectorChunkedSplitNode {
        speculative: default,
        alternate,
        cached_next: Rc::clone(&cached_next),
        chunk_lens: Rc::clone(&chunk_lens),
        frames_in_use_during_process: Rc::clone(&frames_seen),
    });
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"default-1");
    push_test_packet(&runtime, frame, b"default-2");
    push_test_packet(&runtime, frame, b"alternate");
    push_test_packet(&runtime, frame, b"default-3");
    push_test_packet(&runtime, frame, b"default-4");

    assert!(runtime.schedule_frame(split, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_eq!(&*chunk_lens.borrow(), &[4, 1]);
    assert_eq!(frames_seen.get(), 3);
    assert_eq!(
        &*default_payloads.borrow(),
        &[
            b"default-1".to_vec(),
            b"default-2".to_vec(),
            b"default-3".to_vec(),
            b"default-4".to_vec()
        ]
    );
    assert_eq!(&*alternate_payloads.borrow(), &[b"alternate".to_vec()]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn vector_next_enqueue_caches_trailing_run_as_next_index() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities_and_instruction_set(
        32,
        8,
        8,
        8,
        DataPlaneInstructionSet::native(),
    );
    let default_payloads = Rc::new(RefCell::new(Vec::new()));
    let alternate_payloads = Rc::new(RefCell::new(Vec::new()));
    let trace = Rc::new(RefCell::new(Vec::new()));
    let default = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&default_payloads),
    });
    let alternate = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&alternate_payloads),
    });
    let cached_next = Rc::new(Cell::new(None));
    let chunk_lens = Rc::new(RefCell::new(Vec::new()));
    let frames_seen = Rc::new(Cell::new(0));
    let split = runtime.nodes().register_internal(VectorChunkedSplitNode {
        speculative: default,
        alternate,
        cached_next: Rc::clone(&cached_next),
        chunk_lens,
        frames_in_use_during_process: frames_seen,
    });
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"default");
    push_test_packet(&runtime, frame, b"alternate");
    push_test_packet(&runtime, frame, b"alternate");

    assert!(runtime.schedule_frame(split, frame).expect("schedule"));
    assert_eq!(runtime.run_ready_nodes().expect("run first frame"), 3);
    assert_eq!(cached_next.get(), Some(alternate));

    let frame = runtime.alloc_frame_index().expect("alloc second frame");
    push_test_packet(&runtime, frame, b"alternate");
    push_test_packet(&runtime, frame, b"alternate");

    assert!(
        runtime
            .schedule_frame(split, frame)
            .expect("schedule second")
    );
    assert_eq!(runtime.run_ready_nodes().expect("run second frame"), 2);
    assert_eq!(cached_next.get(), Some(alternate));
    assert_eq!(&*default_payloads.borrow(), &[b"default".to_vec()]);
    assert_eq!(
        &*alternate_payloads.borrow(),
        &[
            b"alternate".to_vec(),
            b"alternate".to_vec(),
            b"alternate".to_vec(),
            b"alternate".to_vec()
        ]
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn vector_next_enqueue_cleans_output_frames_when_classification_fails() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 8, 4);
    let alternate = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let split = runtime
        .nodes()
        .register_internal(VectorErrorAfterEnqueueNode { alternate });
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"alternate");
    push_test_packet(&runtime, frame, b"default");

    assert!(runtime.schedule_frame(split, frame).expect("schedule"));

    let err = runtime
        .run_ready_nodes()
        .expect_err("vector validation failed");
    assert!(
        err.to_string().contains("vector validation failed"),
        "unexpected error: {err}"
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn node_next_frames_enqueue_indices_schedules_batch_to_one_frame() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 8, 4);
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let trace = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register_driver(SinkNode {
        trace,
        payloads: Rc::clone(&payloads),
    });
    let indices = [
        runtime
            .alloc_index_with_bytes(RouteMetadata::default(), b"one")
            .expect("alloc first packet"),
        runtime
            .alloc_index_with_bytes(RouteMetadata::default(), b"two")
            .expect("alloc second packet"),
        runtime
            .alloc_index_with_bytes(RouteMetadata::default(), b"three")
            .expect("alloc third packet"),
    ];
    let mut next_frames = NodeNextFrames::default();

    next_frames
        .enqueue_indices(&runtime, sink, indices.iter().copied())
        .expect("enqueue batch");
    assert_eq!(runtime.frames_in_use(), 1);

    next_frames.schedule(&runtime).expect("schedule batch");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 1);
    assert_eq!(
        &*payloads.borrow(),
        &[b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn node_next_frames_enqueue_indices_cleans_new_frame_when_batch_exceeds_capacity() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 1, 1);
    let sink = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let first = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"one")
        .expect("alloc first packet");
    let second = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"two")
        .expect("alloc second packet");
    let mut next_frames = NodeNextFrames::default();

    let err = next_frames
        .enqueue_indices(&runtime, sink, [first, second])
        .expect_err("batch exceeds frame capacity");

    assert!(
        err.to_string().contains("buffer frame capacity exceeded"),
        "unexpected error: {err}"
    );
    assert_eq!(runtime.frames_in_use(), 0);
    runtime.free_index(first);
    runtime.free_index(second);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn speculative_enqueue_cleans_up_split_frame_when_validation_fails() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 4, 2);
    let default = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let alternate = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let split = runtime
        .nodes()
        .register_internal(SpeculativeErrorAfterSplitNode {
            speculative: default,
            alternate,
        });
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"alternate");
    push_test_packet(&runtime, frame, b"default");

    assert!(runtime.schedule_frame(split, frame).expect("schedule"));

    let err = runtime
        .run_ready_nodes()
        .expect_err("split validation failed");
    assert!(
        err.to_string().contains("split validation failed"),
        "unexpected error: {err}"
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn speculative_enqueue_cleans_up_when_validation_fails_after_split_and_keep() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 4, 2);
    let default = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let alternate = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let split = runtime
        .nodes()
        .register_internal(SpeculativeErrorAfterSplitAndKeepNode {
            speculative: default,
            alternate,
        });
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"alternate");
    push_test_packet(&runtime, frame, b"default");
    push_test_packet(&runtime, frame, b"error");

    assert!(runtime.schedule_frame(split, frame).expect("schedule"));

    let err = runtime
        .run_ready_nodes()
        .expect_err("split validation failed");
    assert!(
        err.to_string()
            .contains("split validation failed after keep"),
        "unexpected error: {err}"
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn node_runtime_releases_current_frame_when_next_current_node_is_invalid() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 4, 4, 2);
    let other_runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 1, 4, 1);
    other_runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let invalid = other_runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let node = runtime
        .nodes()
        .register_internal(InvalidNextNode { next: invalid });
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"packet");

    assert!(runtime.schedule_frame(node, frame).expect("schedule"));

    let err = runtime.run_ready_nodes().expect_err("invalid next node");
    assert!(
        err.to_string().contains("node id out of bounds"),
        "unexpected error: {err}"
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn node_runtime_releases_current_and_next_frame_when_next_frame_node_is_invalid() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 4, 4, 3);
    let other_runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 1, 4, 1);
    other_runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    other_runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let invalid = other_runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let node = runtime.nodes().register_internal(ExplicitNextFrameNode {
        next: invalid,
        payload: Some(b"split"),
    });
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"current");

    assert!(runtime.schedule_frame(node, frame).expect("schedule"));

    let err = runtime.run_ready_nodes().expect_err("invalid next node");
    assert!(
        err.to_string().contains("node id out of bounds"),
        "unexpected error: {err}"
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn node_runtime_releases_unprocessed_next_frames_when_dispatch_fails() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 4, 4);
    let other_runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 1, 4, 1);
    other_runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    other_runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let invalid = other_runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let valid = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let node = runtime.nodes().register_internal(MultipleNextFrameNode {
        first_next: invalid,
        second_next: valid,
    });
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"current");

    assert!(runtime.schedule_frame(node, frame).expect("schedule"));

    let err = runtime.run_ready_nodes().expect_err("invalid first next");
    assert!(
        err.to_string().contains("node id out of bounds"),
        "unexpected error: {err}"
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn node_runtime_releases_empty_next_frame() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 4, 4, 3);
    let count = Rc::new(Cell::new(0));
    let sink = runtime.nodes().register_internal(CountNode {
        count: Rc::clone(&count),
    });
    let node = runtime.nodes().register_internal(ExplicitNextFrameNode {
        next: sink,
        payload: None,
    });
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"current");

    assert!(runtime.schedule_frame(node, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 1);
    assert_eq!(count.get(), 0);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn node_runtime_registers_declared_sibling_with_inherited_next_slots() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 8, 4);
    let default = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let alternate = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });

    let owner = runtime
        .nodes()
        .register_internal(DeclaredNextOwnerNode::new(TestNext::nodes(
            default, alternate,
        )));
    let sibling = runtime.nodes().register_internal(DeclaredNextSiblingNode);

    assert_eq!(
        runtime.nodes().node_next(owner, TestNext::Default).unwrap(),
        default
    );
    assert_eq!(
        runtime
            .nodes()
            .node_next(sibling, TestNext::Default)
            .unwrap(),
        default
    );
    assert_eq!(
        runtime
            .nodes()
            .node_next(sibling, TestNext::Alternate)
            .unwrap(),
        alternate
    );
    assert_eq!(runtime.nodes().node_siblings(owner).unwrap(), vec![sibling]);
    assert_eq!(runtime.nodes().node_siblings(sibling).unwrap(), vec![owner]);
}

#[test]
fn node_runtime_allows_multiple_declared_next_nodes_with_distinct_names() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 8, 4);
    let first = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let second = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });

    let owner_a = runtime
        .nodes()
        .register_internal(DeclaredNextOwnerNode::new(TestNext::nodes(first, second)));
    let owner_b = runtime.nodes().register_internal(
        DeclaredNextOwnerNode::new(TestNext::nodes(second, first))
            .with_name("declared-next-owner-b"),
    );

    assert_eq!(
        runtime
            .nodes()
            .node_next(owner_a, TestNext::Default)
            .unwrap(),
        first
    );
    assert_eq!(
        runtime
            .nodes()
            .node_next(owner_b, TestNext::Default)
            .unwrap(),
        second
    );
}

#[test]
fn node_runtime_propagates_next_slot_updates_across_declared_siblings() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 8, 4);
    let first = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let second = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let replacement = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let owner = runtime
        .nodes()
        .register_internal(DeclaredNextOwnerNode::new(TestNext::nodes(first, second)));
    let sibling = runtime.nodes().register_internal(DeclaredNextSiblingNode);

    runtime
        .nodes()
        .set_node_next(sibling, TestNext::Default, replacement)
        .expect("update sibling next");

    assert_eq!(
        runtime.nodes().node_next(owner, TestNext::Default).unwrap(),
        replacement
    );
    assert_eq!(
        runtime
            .nodes()
            .node_next(sibling, TestNext::Default)
            .unwrap(),
        replacement
    );
    assert_eq!(
        runtime
            .nodes()
            .node_next(owner, TestNext::Alternate)
            .unwrap(),
        second
    );
}

#[test]
fn current_node_next_resolves_declared_sibling_slot_during_processing() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 8, 4);
    let first_count = Rc::new(Cell::new(0));
    let replacement_count = Rc::new(Cell::new(0));
    let first = runtime.nodes().register_internal(CountNode {
        count: Rc::clone(&first_count),
    });
    let alternate = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let replacement = runtime.nodes().register_internal(CountNode {
        count: Rc::clone(&replacement_count),
    });
    runtime
        .nodes()
        .register_internal(DeclaredNextOwnerNode::new(TestNext::nodes(
            first, alternate,
        )));
    let sibling = runtime.nodes().register_internal(DeclaredNextSiblingNode);
    runtime
        .nodes()
        .set_node_next(sibling, TestNext::Default, replacement)
        .expect("update sibling next");

    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"packet");
    assert!(runtime.schedule_frame(sibling, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(first_count.get(), 0);
    assert_eq!(replacement_count.get(), 1);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn node_runtime_rejects_declared_sibling_before_owner_registration() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 8, 4);

    let err = runtime
        .nodes()
        .try_register_internal(DeclaredNextSiblingNode)
        .expect_err("sibling owner missing");

    assert!(
        err.to_string()
            .contains("node sibling owner is not registered"),
        "unexpected error: {err}"
    );
}

#[test]
fn node_runtime_rejects_declared_sibling_with_initial_next_nodes() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 8, 4);
    let first = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let alternate = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    runtime
        .nodes()
        .register_internal(DeclaredNextOwnerNode::new(TestNext::nodes(
            first, alternate,
        )));

    let err = runtime
        .nodes()
        .try_register_internal(IllegalSiblingWithNextNode {
            next: TestNext::nodes(first, alternate),
        })
        .expect_err("sibling initial nexts rejected");

    assert!(
        err.to_string()
            .contains("node sibling cannot declare initial next nodes"),
        "unexpected error: {err}"
    );
}

#[test]
fn node_runtime_rejects_plain_node_with_initial_next_nodes() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 8, 4);
    let first = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let alternate = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });

    let err = runtime
        .nodes()
        .try_register_internal(IllegalPlainWithNextNode {
            next: TestNext::nodes(first, alternate),
        })
        .expect_err("plain initial nexts rejected");

    assert!(
        err.to_string()
            .contains("plain node cannot declare initial next nodes"),
        "unexpected error: {err}"
    );
}

#[test]
fn node_runtime_rejects_declared_next_count_mismatch() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 8, 4);
    let first = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let alternate = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });

    let err = runtime
        .nodes()
        .try_register_internal(IllegalNextCountNode {
            next: TestNext::nodes(first, alternate),
        })
        .expect_err("next count mismatch rejected");

    assert!(
        err.to_string().contains("node initial next count mismatch"),
        "unexpected error: {err}"
    );
}

#[test]
fn node_runtime_rejects_out_of_range_sibling_next_slot_update() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(32, 8, 8, 4);
    let first = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let alternate = runtime.nodes().register_internal(CountNode {
        count: Rc::new(Cell::new(0)),
    });
    let owner = runtime
        .nodes()
        .register_internal(DeclaredNextOwnerNode::new(TestNext::nodes(
            first, alternate,
        )));
    runtime.nodes().register_internal(DeclaredNextSiblingNode);

    let err = runtime
        .nodes()
        .set_node_next_slot(owner, TestNext::COUNT, first)
        .expect_err("slot out of range");

    assert!(
        err.to_string().contains("node next slot out of range"),
        "unexpected error: {err}"
    );
}

#[test]
fn node_runtime_counts_errors_per_runtime_node() {
    let first_runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 2, 1, 1);
    let second_runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 2, 1, 1);
    let first_errors = Rc::new(RefCell::new(Vec::new()));
    let second_errors = Rc::new(RefCell::new(Vec::new()));
    let first_node = first_runtime.nodes().register_internal(ErrorNode {
        code: 7,
        errors: Rc::clone(&first_errors),
    });
    let second_node = second_runtime.nodes().register_internal(ErrorNode {
        code: 7,
        errors: Rc::clone(&second_errors),
    });
    let first_frame = first_runtime.alloc_frame_index().expect("alloc frame");
    let first_buffer = first_runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"packet")
        .expect("alloc packet");
    first_runtime
        .get_frame_mut(first_frame)
        .expect("mutate frame")
        .push_index(first_buffer)
        .expect("push packet");

    assert!(
        first_runtime
            .schedule_frame(first_node, first_frame)
            .expect("schedule frame")
    );

    assert_eq!(first_runtime.run_ready_nodes().expect("run nodes"), 1);
    assert_eq!(
        first_runtime
            .node_error_count(first_node, 7)
            .expect("first counter"),
        1
    );
    assert_eq!(
        second_runtime
            .node_error_count(second_node, 7)
            .expect("second counter"),
        0
    );
    assert_eq!(
        &*first_errors.borrow(),
        &[Some(BufferNodeError::new(first_node, 7))]
    );
    assert!(second_errors.borrow().is_empty());
}

fn push_test_packet(
    runtime: &DataPlaneRuntime<TestNode>,
    frame: hammer_adapter::FrameIndex,
    payload: &[u8],
) {
    let buffer = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), payload)
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(buffer)
        .expect("push packet");
}

fn push_marked_test_packet(
    runtime: &DataPlaneRuntime<TestNode>,
    frame: hammer_adapter::FrameIndex,
    trace_input: NodeId,
    payload: &[u8],
) {
    let buffer = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), payload)
        .expect("alloc packet");
    runtime
        .try_mark_trace(trace_input, buffer)
        .expect("mark packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(buffer)
        .expect("push packet");
}

#[test]
fn data_plane_handoff_moves_frame_between_workers_without_copying_payload_storage() {
    const SINK_HANDLE: NodeHandle = NodeHandle::new(1);
    let handoff = DataPlaneHandoff::with_buffer_arena(2, 8, BufferPoolArena::with_capacity(64, 8));
    let first_runtime = DataPlaneRuntime::<TestNode>::with_handoff(
        DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
            handoff.buffer_arena(),
            4,
            4,
            hammer_adapter::DataPlaneInstructionSet::Scalar,
        ),
        DataWorkerId::new(0),
        handoff.worker(DataWorkerId::new(0)),
    );
    let second_runtime = DataPlaneRuntime::<TestNode>::with_handoff(
        DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
            handoff.buffer_arena(),
            4,
            4,
            hammer_adapter::DataPlaneInstructionSet::Scalar,
        ),
        DataWorkerId::new(1),
        handoff.worker(DataWorkerId::new(1)),
    );
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let current_ptrs = Rc::new(RefCell::new(Vec::new()));
    let sink = second_runtime
        .nodes()
        .register_internal_with_handle(
            SINK_HANDLE,
            PointerSinkNode {
                payloads: Rc::clone(&payloads),
                current_ptrs: Rc::clone(&current_ptrs),
            },
        )
        .expect("register sink handle");
    let handoff_node = first_runtime.nodes().register_internal(HandoffNode {
        target_worker: DataWorkerId::new(1),
        target: SINK_HANDLE,
    });
    let frame = first_runtime.alloc_frame_index().expect("alloc frame");
    let buffer = first_runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"packet")
        .expect("alloc packet");
    let original_ptr = first_runtime
        .get_buffer(buffer)
        .expect("buffer ref")
        .current_ptr() as usize;
    first_runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(buffer)
        .expect("push packet");

    assert!(
        first_runtime
            .schedule_frame(handoff_node, frame)
            .expect("schedule handoff")
    );

    assert_eq!(first_runtime.run_ready_nodes().expect("run source"), 1);
    assert_eq!(first_runtime.in_use_buffers(), 1);
    assert_eq!(second_runtime.run_ready_nodes().expect("run target"), 1);
    assert_eq!(
        sink,
        second_runtime.nodes().node_for_handle(SINK_HANDLE).unwrap()
    );
    assert_eq!(&*payloads.borrow(), &[b"packet".to_vec()]);
    assert_eq!(&*current_ptrs.borrow(), &[original_ptr]);
    assert_eq!(first_runtime.in_use_buffers(), 0);
    assert_eq!(second_runtime.in_use_buffers(), 0);
}

#[test]
fn traced_packet_handoff_records_entries_on_target_worker() {
    const SINK_HANDLE: NodeHandle = NodeHandle::new(3);
    let handoff = DataPlaneHandoff::with_buffer_arena(2, 8, BufferPoolArena::with_capacity(64, 8));
    let first_runtime = DataPlaneRuntime::<TestNode>::with_handoff(
        DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
            handoff.buffer_arena(),
            4,
            4,
            hammer_adapter::DataPlaneInstructionSet::Scalar,
        ),
        DataWorkerId::new(0),
        handoff.worker(DataWorkerId::new(0)),
    );
    let second_runtime = DataPlaneRuntime::<TestNode>::with_handoff(
        DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
            handoff.buffer_arena(),
            4,
            4,
            hammer_adapter::DataPlaneInstructionSet::Scalar,
        ),
        DataWorkerId::new(1),
        handoff.worker(DataWorkerId::new(1)),
    );
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = second_runtime.nodes().register(TestNode::Sink(SinkNode {
        trace: Rc::new(RefCell::new(Vec::new())),
        payloads,
    }));
    assert_eq!(sink, NodeId::new(0));
    let target_trace = second_runtime
        .nodes()
        .register_internal_with_handle(SINK_HANDLE, TraceNode { next: sink })
        .expect("register target trace node");
    let source_sink = first_runtime.nodes().register(TestNode::Count(CountNode {
        count: Rc::new(Cell::new(0)),
    }));
    let source_trace = first_runtime
        .nodes()
        .register_internal(TraceNode { next: source_sink });
    let handoff_node = first_runtime.nodes().register_internal(HandoffNode {
        target_worker: DataWorkerId::new(1),
        target: SINK_HANDLE,
    });
    let control = TraceControlPlane::new(8);
    control.publish(TracePolicy {
        enabled: true,
        record_capacity: 8,
        packet_capacity: 2,
        inputs: vec![TraceInputPolicy {
            node: source_trace,
            count: 1,
        }],
    });
    let trace_handle = control.handle();
    first_runtime.set_trace_control(Some(trace_handle.clone()), 2);
    second_runtime.set_trace_control(Some(trace_handle), 2);
    let frame = first_runtime.alloc_frame_index().expect("alloc frame");
    let buffer = first_runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"packet")
        .expect("alloc packet");
    first_runtime
        .try_mark_trace(source_trace, buffer)
        .expect("mark packet on source worker");
    first_runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(buffer)
        .expect("push packet");

    assert!(
        first_runtime
            .schedule_frame(handoff_node, frame)
            .expect("schedule handoff")
    );

    assert_eq!(first_runtime.run_ready_nodes().expect("run source"), 1);
    assert_eq!(second_runtime.run_ready_nodes().expect("run target"), 2);
    assert_eq!(control.drain_completed(), 1);
    let records = control.take_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].input_node, source_trace);
    assert_eq!(records[0].entries.len(), 1);
    assert_eq!(records[0].entries[0].node, target_trace);
    assert_eq!(records[0].entries[0].node_name, Some("trace-input"));
    assert_eq!(records[0].entries[0].payload_bytes, b"packet");
}

#[test]
fn data_plane_handoff_indices_moves_batch_between_workers_without_copying_payload_storage() {
    const SINK_HANDLE: NodeHandle = NodeHandle::new(8);
    let handoff = DataPlaneHandoff::with_buffer_arena(2, 8, BufferPoolArena::with_capacity(64, 8));
    let first_runtime = DataPlaneRuntime::<TestNode>::with_handoff(
        DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
            handoff.buffer_arena(),
            4,
            4,
            hammer_adapter::DataPlaneInstructionSet::Scalar,
        ),
        DataWorkerId::new(0),
        handoff.worker(DataWorkerId::new(0)),
    );
    let second_runtime = DataPlaneRuntime::<TestNode>::with_handoff(
        DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
            handoff.buffer_arena(),
            4,
            4,
            hammer_adapter::DataPlaneInstructionSet::Scalar,
        ),
        DataWorkerId::new(1),
        handoff.worker(DataWorkerId::new(1)),
    );
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let current_ptrs = Rc::new(RefCell::new(Vec::new()));
    second_runtime
        .nodes()
        .register_internal_with_handle(
            SINK_HANDLE,
            PointerSinkNode {
                payloads: Rc::clone(&payloads),
                current_ptrs: Rc::clone(&current_ptrs),
            },
        )
        .expect("register sink handle");
    let first = first_runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"first")
        .expect("alloc first packet");
    let second = first_runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"second")
        .expect("alloc second packet");
    let first_ptr = first_runtime
        .get_buffer(first)
        .expect("first buffer ref")
        .current_ptr() as usize;
    let second_ptr = first_runtime
        .get_buffer(second)
        .expect("second buffer ref")
        .current_ptr() as usize;

    first_runtime
        .handoff_indices(DataWorkerId::new(1), SINK_HANDLE, [first, second])
        .expect("handoff batch");

    assert_eq!(first_runtime.in_use_buffers(), 2);
    assert_eq!(second_runtime.run_ready_nodes().expect("run target"), 1);
    assert_eq!(
        &*payloads.borrow(),
        &[b"first".to_vec(), b"second".to_vec()]
    );
    assert_eq!(&*current_ptrs.borrow(), &[first_ptr, second_ptr]);
    assert_eq!(first_runtime.in_use_buffers(), 0);
    assert_eq!(second_runtime.in_use_buffers(), 0);
}

#[test]
fn data_plane_handoff_cleans_up_when_target_frame_capacity_is_exceeded() {
    const SINK_HANDLE: NodeHandle = NodeHandle::new(2);
    let handoff = DataPlaneHandoff::with_buffer_arena(2, 8, BufferPoolArena::with_capacity(64, 8));
    let first_runtime = DataPlaneRuntime::<TestNode>::with_handoff(
        DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
            handoff.buffer_arena(),
            4,
            4,
            hammer_adapter::DataPlaneInstructionSet::Scalar,
        ),
        DataWorkerId::new(0),
        handoff.worker(DataWorkerId::new(0)),
    );
    let second_runtime = DataPlaneRuntime::<TestNode>::with_handoff(
        DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
            handoff.buffer_arena(),
            1,
            4,
            hammer_adapter::DataPlaneInstructionSet::Scalar,
        ),
        DataWorkerId::new(1),
        handoff.worker(DataWorkerId::new(1)),
    );
    second_runtime
        .nodes()
        .register_internal_with_handle(
            SINK_HANDLE,
            CountNode {
                count: Rc::new(Cell::new(0)),
            },
        )
        .expect("register sink handle");
    let handoff_node = first_runtime.nodes().register_internal(HandoffNode {
        target_worker: DataWorkerId::new(1),
        target: SINK_HANDLE,
    });
    let frame = first_runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&first_runtime, frame, b"first");
    push_test_packet(&first_runtime, frame, b"second");

    assert!(
        first_runtime
            .schedule_frame(handoff_node, frame)
            .expect("schedule handoff")
    );
    assert_eq!(first_runtime.run_ready_nodes().expect("run source"), 1);

    let err = second_runtime
        .run_ready_nodes()
        .expect_err("target frame capacity exceeded");
    assert!(
        err.to_string().contains("buffer frame capacity exceeded"),
        "unexpected error: {err}"
    );
    assert_eq!(first_runtime.frames_in_use(), 0);
    assert_eq!(second_runtime.frames_in_use(), 0);
    assert_eq!(first_runtime.in_use_buffers(), 0);
    assert_eq!(second_runtime.in_use_buffers(), 0);
}

#[test]
fn data_plane_handoff_cleans_up_when_target_frame_pool_is_exhausted() {
    const SINK_HANDLE: NodeHandle = NodeHandle::new(3);
    let handoff = DataPlaneHandoff::with_buffer_arena(2, 8, BufferPoolArena::with_capacity(64, 8));
    let first_runtime = DataPlaneRuntime::<TestNode>::with_handoff(
        DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
            handoff.buffer_arena(),
            4,
            4,
            hammer_adapter::DataPlaneInstructionSet::Scalar,
        ),
        DataWorkerId::new(0),
        handoff.worker(DataWorkerId::new(0)),
    );
    let second_runtime = DataPlaneRuntime::<TestNode>::with_handoff(
        DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
            handoff.buffer_arena(),
            1,
            0,
            hammer_adapter::DataPlaneInstructionSet::Scalar,
        ),
        DataWorkerId::new(1),
        handoff.worker(DataWorkerId::new(1)),
    );
    second_runtime
        .nodes()
        .register_internal_with_handle(
            SINK_HANDLE,
            CountNode {
                count: Rc::new(Cell::new(0)),
            },
        )
        .expect("register sink handle");
    let handoff_node = first_runtime.nodes().register_internal(HandoffNode {
        target_worker: DataWorkerId::new(1),
        target: SINK_HANDLE,
    });
    let frame = first_runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&first_runtime, frame, b"packet");

    assert!(
        first_runtime
            .schedule_frame(handoff_node, frame)
            .expect("schedule handoff")
    );
    assert_eq!(first_runtime.run_ready_nodes().expect("run source"), 1);

    let err = second_runtime
        .run_ready_nodes()
        .expect_err("target frame pool exhausted");
    assert!(
        err.to_string().contains("frame pool exhausted"),
        "unexpected error: {err}"
    );
    assert_eq!(first_runtime.frames_in_use(), 0);
    assert_eq!(second_runtime.frames_in_use(), 0);
    assert_eq!(first_runtime.in_use_buffers(), 0);
    assert_eq!(second_runtime.in_use_buffers(), 0);
}

#[test]
fn data_plane_handoff_preserves_source_frame_until_marking_succeeds() {
    let handoff = DataPlaneHandoff::with_buffer_arena(2, 8, BufferPoolArena::with_capacity(64, 8));
    let runtime = DataPlaneRuntime::<TestNode>::with_handoff(
        DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
            handoff.buffer_arena(),
            4,
            4,
            hammer_adapter::DataPlaneInstructionSet::Scalar,
        ),
        DataWorkerId::new(0),
        handoff.worker(DataWorkerId::new(0)),
    );
    let foreign_runtime = DataPlaneRuntime::<TestNode>::with_capacities(64, 1, 4, 1);
    let handoff_node = runtime.nodes().register_internal(HandoffNode {
        target_worker: DataWorkerId::new(1),
        target: NodeHandle::new(4),
    });
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let local = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"local")
        .expect("alloc local packet");
    let foreign = foreign_runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"foreign")
        .expect("alloc foreign packet");
    {
        let mut frame = runtime.get_frame_mut(frame).expect("mutate frame");
        frame.push_index(local).expect("push local packet");
        frame.push_index(foreign).expect("push foreign packet");
    }

    assert!(
        runtime
            .schedule_frame(handoff_node, frame)
            .expect("schedule handoff")
    );
    let err = runtime
        .run_ready_nodes()
        .expect_err("foreign buffer cannot be marked");
    assert!(
        err.to_string()
            .contains("buffer index belongs to another pool"),
        "unexpected error: {err}"
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
    assert_eq!(foreign_runtime.in_use_buffers(), 1);

    foreign_runtime.free_index(foreign);
}

#[test]
fn data_plane_handoff_preserves_source_frame_when_target_queue_is_full() {
    const SINK_HANDLE: NodeHandle = NodeHandle::new(5);
    let handoff = DataPlaneHandoff::with_buffer_arena(2, 1, BufferPoolArena::with_capacity(64, 8));
    let runtime = DataPlaneRuntime::<TestNode>::with_handoff(
        DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
            handoff.buffer_arena(),
            4,
            4,
            hammer_adapter::DataPlaneInstructionSet::Scalar,
        ),
        DataWorkerId::new(0),
        handoff.worker(DataWorkerId::new(0)),
    );
    let target_runtime = DataPlaneRuntime::<TestNode>::with_handoff(
        DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
            handoff.buffer_arena(),
            4,
            4,
            hammer_adapter::DataPlaneInstructionSet::Scalar,
        ),
        DataWorkerId::new(1),
        handoff.worker(DataWorkerId::new(1)),
    );
    let received = Rc::new(Cell::new(0));
    target_runtime
        .nodes()
        .register_internal_with_handle(
            SINK_HANDLE,
            CountNode {
                count: Rc::clone(&received),
            },
        )
        .expect("register sink handle");
    let queued = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"queued")
        .expect("alloc queued packet");
    runtime
        .handoff_index(DataWorkerId::new(1), SINK_HANDLE, queued)
        .expect("fill handoff queue");
    let handoff_node = runtime.nodes().register_internal(HandoffNode {
        target_worker: DataWorkerId::new(1),
        target: SINK_HANDLE,
    });
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_test_packet(&runtime, frame, b"blocked");

    assert!(
        runtime
            .schedule_frame(handoff_node, frame)
            .expect("schedule handoff")
    );
    let err = runtime
        .run_ready_nodes()
        .expect_err("handoff queue exhausted");
    assert!(
        err.to_string().contains("handoff queue exhausted"),
        "unexpected error: {err}"
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 1);

    assert_eq!(target_runtime.run_ready_nodes().expect("drain queued"), 1);
    assert_eq!(received.get(), 1);
    assert_eq!(runtime.in_use_buffers(), 0);
    assert_eq!(target_runtime.in_use_buffers(), 0);
}

#[test]
fn node_runtime_registers_driver_role_node() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 4, 2, 2);
    let trace = Rc::new(RefCell::new(Vec::new()));
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&payloads),
    });
    let driver = ForwardNode {
        next: sink,
        trace: Rc::clone(&trace),
    };
    assert_driver_node(&driver);
    let driver = runtime.nodes().register_driver(driver);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"packet")
        .expect("alloc packet");
    runtime
        .with_frame_mut(frame, |frame| frame.push_index(buffer))
        .expect("mutate frame")
        .expect("push packet");

    assert!(
        runtime
            .schedule_frame(driver, frame)
            .expect("schedule frame")
    );

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(&*trace.borrow(), &["forward", "sink"]);
    assert_eq!(&*payloads.borrow(), &[b"packet".to_vec()]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn node_runtime_schedules_empty_driver_frame() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 4, 2, 2);
    let trace = Rc::new(RefCell::new(Vec::new()));
    let payloads = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register_driver(SinkNode {
        trace: Rc::clone(&trace),
        payloads: Rc::clone(&payloads),
    });
    let driver = runtime.nodes().register_driver(SourceDriverNode {
        next: sink,
        payload: b"packet".to_vec().into(),
    });
    let frame = runtime.alloc_frame_index().expect("alloc frame");

    runtime
        .schedule_driver_frame(driver, frame)
        .expect("schedule driver");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(&*trace.borrow(), &["sink"]);
    assert_eq!(&*payloads.borrow(), &[b"packet".to_vec()]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn node_runtime_ready_future_wakes_on_local_schedule() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(8, 2, 1, 1);
    let count = Rc::new(Cell::new(0));
    let node = runtime.nodes().register(TestNode::Count(CountNode {
        count: Rc::clone(&count),
    }));
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"packet")
        .expect("alloc packet");
    runtime
        .with_frame_mut(frame, |frame| frame.push_index(buffer))
        .expect("mutate frame")
        .expect("push packet");
    let wake_counter = Arc::new(WakeCounter::default());
    let waker = Waker::from(Arc::clone(&wake_counter));
    let mut context = Context::from_waker(&waker);
    let mut ready = runtime.nodes().ready();

    assert!(matches!(
        Pin::new(&mut ready).poll(&mut context),
        Poll::Pending
    ));
    assert_eq!(wake_counter.wakes.load(Ordering::SeqCst), 0);

    assert!(runtime.schedule_frame(node, frame).expect("schedule frame"));

    assert_eq!(wake_counter.wakes.load(Ordering::SeqCst), 1);
    assert!(matches!(
        Pin::new(&mut ready).poll(&mut context),
        Poll::Ready(())
    ));
    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 1);
    assert_eq!(count.get(), 1);
}
