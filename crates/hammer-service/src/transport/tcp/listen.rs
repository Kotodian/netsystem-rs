use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeNextFrames, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::TcpConnectionId;

use crate::session::SessionId;
use crate::transport::congestion::CongestionController;
use super::connection::TcpConnection;
use super::segment::parse_tcp_packet;
use super::TcpQueueHandle;
use super::state_machine::Listen;

#[hammer_component_macros::node_next]
pub enum TcpListenNext {
    Output,
    Drop,
}

#[hammer_component_macros::node(role = internal, next = TcpListenNext)]
pub struct TcpListenNode<C: CongestionController + 'static> {
    session_queue: TcpQueueHandle<C>,
}

impl<C> Node for TcpListenNode<C>
where
    C: CongestionController + 'static,
{
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let next = Self::runtime_nexts(runtime)?;
        tcp_listen_process_frame(runtime, frame, self.session_queue, next)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_listen_process::<C>
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.session_queue.runtime_data())
    }
}

fn tcp_listen_process<C>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult>
where
    C: CongestionController + 'static,
{
    let next = TcpListenNode::<C>::runtime_nexts(runtime)?;
    tcp_listen_process_frame::<C>(runtime, frame, TcpQueueHandle::new(data), next)
}

#[inline]
fn tcp_listen_process_frame<C>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: TcpQueueHandle<C>,
    next: [NodeId; TcpListenNext::COUNT],
) -> CoreResult<NodeResult>
where
    C: CongestionController + 'static,
{
    let tcp_output = next[TcpListenNext::Output as usize];
    let drop_next = next[TcpListenNext::Drop as usize];
    let mut next_frames = NodeNextFrames::default();
    frame.rewrite_indices_batched(runtime.preferred_frame_batch_width(), |index| {
        match tcp_listen_index(runtime, index, session_queue, tcp_output, &mut next_frames) {
            Ok(()) => Ok(None),
            Err(_) => {
                next_frames.enqueue(runtime, drop_next, index)?;
                Ok(None)
            }
        }
    })?;
    next_frames.schedule(runtime)?;
    Ok(NodeResult::drop())
}

fn tcp_listen_index<C>(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    session_queue: TcpQueueHandle<C>,
    tcp_output: NodeId,
    next_frames: &mut NodeNextFrames,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let packet = parse_tcp_packet(runtime, index)?;
    let mut tx_index = None;
    let result = {
        let mut queue = session_queue.borrow_mut()?;
        let worker = queue.worker();
        let session_id = queue.insert_session_with_id(|session_id: SessionId| {
            let connection_id = TcpConnectionId::new(session_id.get());
            let connection: TcpConnection<Listen, _> = TcpConnection::new(
                Some(connection_id),
                worker,
                packet.local.port(),
                Some(packet.local),
                packet.remote,
            );
            connection.into()
        });
        let state = queue
            .session(session_id)
            .ok_or_else(|| CoreError::internal("tcp listen session is missing"))?
            .clone();
        let connection: TcpConnection<Listen, _> = state.try_into()?;
        let (next_state, control) = connection.receive_syn(&packet)?;
        let state = queue
            .session_mut(session_id)
            .ok_or_else(|| CoreError::internal("tcp listen session is missing"))?;
        *state = next_state;
        if let Some(segment) = control {
            let allocated = runtime.packet_buffers().alloc_index(Default::default())?;
            if let Err(error) = segment.write_to_buffer(runtime, allocated) {
                runtime.free_index(allocated);
                return Err(error);
            }
            tx_index = Some(allocated);
        }
        Ok(())
    };
    if let Err(error) = result {
        if let Some(tx_index) = tx_index.take() {
            runtime.free_index(tx_index);
        }
        return Err(error);
    }

    if let Some(tx_index) = tx_index.take() {
        if let Err(error) = next_frames.enqueue(runtime, tcp_output, tx_index) {
            runtime.free_index(tx_index);
            return Err(error);
        }
    }
    runtime.free_index(index);
    Ok(())
}
