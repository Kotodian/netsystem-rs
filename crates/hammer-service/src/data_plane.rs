use hammer_core::data_plane::{Frame, NodeId, NodeRegistration};
use hammer_runtime::RuntimeResult;
use hammer_runtime::{
    DataPlaneMain, InternalNode, Node, NodeErrorCode, NodeProcessFn, add_packet_trace,
};

use crate::net::{DpoProto, DpoType, NetMain};

/// Record a generated node-local error and store its preinstalled global
/// index in the packet buffer identified by `index`.
#[inline(always)]
pub fn set_index_node_error<E>(
    runtime: &mut DataPlaneMain,
    index: u32,
    error: E,
) -> RuntimeResult<()>
where
    E: NodeErrorCode,
{
    let error = runtime.record_current_node_error(error)?;
    let buffer = runtime.buffer_mut(index);
    buffer.set_node_error_index(error);
    Ok(())
}

#[hammer_component_macros::graph_node(
    graph = service,
    init = crate::data_plane::register_drop,
)]
#[derive(Debug, Clone, Copy, Default)]
pub struct DropNode;

#[hammer_component_macros::graph_node(graph = service, kind = internal, name = "punt")]
pub struct PuntNode;

impl Node for PuntNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        // vlib/drop.c uses the ordinary drop path when no OS punt consumer
        // is installed. Frame storage is recycled separately by dispatch.
        drop_node_process(runtime, node_runtime, frame)
    }
}

impl DropNode {
    pub const NODE_NAME: &'static str = "drop";

    #[inline]
    pub fn new() -> Self {
        Self
    }
}

pub fn register_drop(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime.nodes().try_register_internal(DropNode)?;
    NetMain::global()?
        .register_dpo(
            Some(DpoType::DROP),
            &[(DpoProto::IP4, &[node][..]), (DpoProto::IP6, &[node][..])],
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .map_err(
            |source| hammer_runtime::RuntimeError::GraphNodeInitialization {
                node: DropNode::NODE_NAME,
                source: Box::new(source),
            },
        )?;
    Ok(node)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct DropTrace {
    pub dropped: usize,
}

impl Node for DropNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = drop_node_process;
        process(runtime, node_runtime, frame)
    }
}

fn drop_node_process(
    runtime: &mut DataPlaneMain,
    _: &mut hammer_runtime::node::NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let processed_vectors = frame.len();
    (|| {
        let dropped = frame.len();
        let indices = frame.vector_args();
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
            let _ = add_packet_trace!(runtime, index0, DropTrace { dropped });
            let _ = add_packet_trace!(runtime, index1, DropTrace { dropped });
            let _ = add_packet_trace!(runtime, index2, DropTrace { dropped });
            let _ = add_packet_trace!(runtime, index3, DropTrace { dropped });
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
            let _ = add_packet_trace!(runtime, index0, DropTrace { dropped });
            let _ = add_packet_trace!(runtime, index1, DropTrace { dropped });
            read += 2;
        }
        while read < len {
            if read + 1 < len {
                runtime.prefetch_header(indices[read + 1]);
            }
            let index0 = indices[read];
            let _ = add_packet_trace!(runtime, index0, DropTrace { dropped });
            read += 1;
        }
        ()
    })();
    runtime.buffer_free(frame.vector_args());
    processed_vectors
}

impl InternalNode for DropNode {
    #[inline]
    fn node_registration(&self) -> Option<NodeRegistration>
    where
        Self: Sized,
    {
        Some(NodeRegistration::next(Self::NODE_NAME, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_runtime::{DataPlaneBufferConfig, NodeRuntime};

    #[test]
    fn terminal_nodes_release_buffer_chains() {
        hammer_infra::main_heap::init_default().unwrap();
        crate::BUFFER_MAIN_INIT.call_once(|| {
            hammer_core::buffer::BufferMain::new(
                64,
                1024,
                &[0],
                2,
                hammer_infra::PageSize::Default,
            )
            .unwrap();
        });
        let mut runtime = DataPlaneMain::new(DataPlaneBufferConfig {
            thread_index: 1,
            ..Default::default()
        });
        // vlib/drop.c::process_drop_punt releases complete chains for Drop
        // and for Punt without an OS consumer. Neither operation drains vectors.
        for process in [DropNode::process as NodeProcessFn, PuntNode::process] {
            let mut indices = [0; 3];
            assert_eq!(runtime.buffer_alloc(&mut indices), 3);
            runtime.buffer_chain_buffer(indices[0], indices[1]);
            let cached_free = runtime.cached_free_buffers();
            let mut frame = Frame::<(), u32, ()>::new(0);
            frame.set_vector_count(2);
            frame
                .vector_args_mut()
                .copy_from_slice(&[indices[0], indices[2]]);
            assert_eq!(
                process(&mut runtime, &mut NodeRuntime::empty(), &mut frame),
                2
            );
            assert_eq!(frame.vector_args(), &[indices[0], indices[2]]);
            assert_eq!(runtime.cached_free_buffers(), cached_free + 3);
            drop(frame);
            assert_eq!(runtime.cached_free_buffers(), cached_free + 3);
        }
    }
}
