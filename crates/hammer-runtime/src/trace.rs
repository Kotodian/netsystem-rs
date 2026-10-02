//! Per-main packet traces following VPP's trace pool and Node count model.

use hammer_core::data_plane::{Frame, NodeId, NodeRegistration};
use hammer_infra::pool::Pool;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::node::{
    InternalNode, Node, NodeErrorCode, NodeErrorDescriptor, NodeErrorSeverity, NodeRuntime,
};
use crate::{DataPlaneMain, RuntimeResult};

pub mod cli;

pub type TraceFormatter = fn(&[u8]) -> String;

pub(crate) const TRACE_THREAD_SHIFT: u32 = 24;
pub(crate) const TRACE_THREAD_LIMIT: u32 = 0xff;
pub(crate) const TRACE_INDEX_LIMIT: u32 = 0x00ff_ffff;

/// VPP `vlib_trace_header_t`; payload occupies `n_data` further headers.
#[repr(C, align(16))]
#[derive(Debug, Clone, Copy, KnownLayout, FromBytes, IntoBytes, Immutable)]
pub(crate) struct TraceHeader {
    pub(crate) time: u64,
    pub(crate) node_index: u32,
    pub(crate) n_data: u32,
}

const _: () = {
    assert!(core::mem::size_of::<TraceHeader>() == 16);
    assert!(core::mem::align_of::<TraceHeader>() == 16);
};

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct TraceNode {
    pub(crate) count: u32,
    pub(crate) limit: u32,
}

#[derive(Debug, Clone, Copy, Default)]
pub enum TraceTimestampFormat {
    #[default]
    Relative,
    Unix,
    Datetime,
}

impl core::fmt::Display for TraceTimestampFormat {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let name = match self {
            Self::Relative => "relative",
            Self::Unix => "unix",
            Self::Datetime => "datetime",
        };
        writeln!(formatter, "trace timestamp format: {name}")
    }
}

#[derive(Debug, Default)]
pub(crate) struct TraceMain {
    pub(crate) trace_buffer_pool: Pool<Vec<TraceHeader>>,
    pub(crate) nodes: Vec<TraceNode>,
    pub(crate) trace_enable: bool,
    pub(crate) verbose: bool,
    pub(crate) timestamp_format: TraceTimestampFormat,
}

impl TraceMain {
    /// VPP `trace_update_capture_options`: zero resets a Node's quota but
    /// does not delete existing packet records.
    pub(crate) fn add_count(&mut self, node: NodeId, count: u32, verbose: bool) {
        let index = node.slot() as usize;
        if self.nodes.len() <= index {
            self.nodes.resize(index + 1, TraceNode::default());
        }
        let trace_node = &mut self.nodes[index];
        if count == 0 {
            trace_node.count = 0;
            trace_node.limit = 0;
        } else {
            trace_node.limit = trace_node
                .limit
                .checked_add(count)
                .expect("trace add prevalidated every main's quota");
        }
        self.verbose = verbose;
        self.trace_enable = true;
    }

    /// VPP `clear_trace_buffer`: called only after all mains disable tracing.
    pub(crate) fn clear(&mut self) {
        self.nodes.clear();
        self.trace_buffer_pool.clear();
    }
}

/// VPP `handoff_trace_t`; a new owner keeps the preceding thread/pool index.
#[repr(C)]
#[derive(Debug, KnownLayout, FromBytes, IntoBytes, Immutable)]
pub(crate) struct HandoffTrace {
    pub(crate) prev_thread: u32,
    pub(crate) prev_trace_index: u32,
}

#[repr(u16)]
#[derive(Clone, Copy)]
enum HandoffTraceError {
    UnexpectedDispatch,
}

impl NodeErrorCode for HandoffTraceError {
    fn local_code(self) -> u16 {
        self as u16
    }
}

pub(crate) const HANDOFF_TRACE_ERRORS: [NodeErrorDescriptor; 1] = [NodeErrorDescriptor::new(
    "unexpected-dispatch",
    NodeErrorSeverity::Warn,
    "Packets sent to the handoff trace node",
)];

pub(crate) struct HandoffTraceNode;

impl InternalNode for HandoffTraceNode {
    fn node_registration(&self) -> Option<NodeRegistration> {
        Some(NodeRegistration::next("handoff-trace", 1))
    }
}

impl Node for HandoffTraceNode {
    fn process(runtime: &mut DataPlaneMain, _: &mut NodeRuntime, frame: &mut Frame) -> usize {
        let count = frame.len();
        runtime.buffer_free(frame.vector_args());
        runtime
            .record_current_node_error_count(HandoffTraceError::UnexpectedDispatch, count as u64)
            .expect("handoff trace error column was registered");
        count
    }

    fn trace_supported(&self) -> bool {
        true
    }

    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_handoff_trace)
    }
}

fn format_handoff_trace(bytes: &[u8]) -> String {
    let (trace, _) = HandoffTrace::ref_from_prefix(bytes)
        .expect("handoff trace record has its registered layout");
    format!(
        "HANDED-OFF: from thread {} trace index {}",
        trace.prev_thread, trace.prev_trace_index
    )
}
