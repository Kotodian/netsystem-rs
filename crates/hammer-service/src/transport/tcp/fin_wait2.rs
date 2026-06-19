use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeNextFrames, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};

use crate::transport::congestion::CongestionController;
use super::TcpSessionProtocol;
use super::connection::TcpConnection;
use super::segment::parse_tcp_packet;
use super::session::TcpSessionQueueHandle;
use super::state_machine::FinWait2;

#[hammer_component_macros::node_next]
pub enum TcpFinWait2Next {
    Output,
    Drop,
}

#[hammer_component_macros::node(role = internal, next = TcpFinWait2Next)]
pub struct TcpFinWait2Node<C: CongestionController + 'static> {
    #[node(default)]
    session_queue: Option<TcpSessionQueueHandle<C>>,
}

impl<C> TcpFinWait2Node<C>
where
    C: CongestionController + 'static,
{
    #[inline]
    pub(crate) fn with_session_queue(mut self, handle: TcpSessionQueueHandle<C>) -> Self {
        self.session_queue = Some(handle);
        self
    }
}

impl<C> Node for TcpFinWait2Node<C>
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
        tcp_fin_wait2_frame(runtime, frame, self.session_queue, next)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_fin_wait2_process::<C>
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        self.session_queue
            .map(TcpSessionQueueHandle::runtime_data)
            .ok_or_else(|| CoreError::internal("tcp fin-wait2 node missing session queue"))
    }
}

fn tcp_fin_wait2_process<C>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult>
where
    C: CongestionController + 'static,
{
    let next = TcpFinWait2Node::<C>::runtime_nexts(runtime)?;
    tcp_fin_wait2_frame::<C>(runtime, frame, Some(TcpSessionQueueHandle::new(data)), next)
}

fn tcp_fin_wait2_frame<C>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: Option<TcpSessionQueueHandle<C>>,
    next: [NodeId; TcpFinWait2Next::COUNT],
) -> CoreResult<NodeResult>
where
    C: CongestionController + 'static,
{
    let session_queue = session_queue
        .ok_or_else(|| CoreError::internal("tcp fin-wait2 node missing session queue"))?;
    let tcp_output = next[TcpFinWait2Next::Output as usize];
    let drop_next = next[TcpFinWait2Next::Drop as usize];
    let mut next_frames = NodeNextFrames::default();
    frame.rewrite_indices_batched(runtime.preferred_frame_batch_width(), |index| {
        match tcp_fin_wait2_index(runtime, index, session_queue, tcp_output, &mut next_frames) {
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

fn tcp_fin_wait2_index<C>(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    session_queue: TcpSessionQueueHandle<C>,
    tcp_output: NodeId,
    next_frames: &mut NodeNextFrames,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let packet = parse_tcp_packet(runtime, index)?;
    let mut tx_index = None;
    let result = TcpSessionProtocol::with_queue(session_queue, |queue: &mut super::session::TcpSessionQueue<C>| {
        let (session_id, _, _) = queue
            .session_route_by_tuple(packet.local, packet.remote)
            .ok_or_else(|| CoreError::internal("tcp fin-wait2 session is missing"))?;
        let connection: TcpConnection<FinWait2, _> = queue.take_connection(session_id)?;
        let control = connection.receive_fin_wait2(queue, session_id, &packet)?;
        if let Some(segment) = control {
            let allocated = runtime.packet_buffers().alloc_index(Default::default())?;
            if let Err(error) = queue.protocol.insert_segment(allocated, segment) {
                runtime.free_index(allocated);
                return Err(error);
            }
            tx_index = Some(allocated);
        }
        Ok(())
    });
    if let Err(error) = result {
        if let Some(tx_index) = tx_index.take() {
            TcpSessionProtocol::with_queue(
                session_queue,
                |queue: &mut super::session::TcpSessionQueue<C>| {
                queue.protocol.remove_segment(tx_index);
                Ok(())
                },
            )?;
            runtime.free_index(tx_index);
        }
        return Err(error);
    }
    if let Some(tx_index) = tx_index.take()
        && let Err(error) = next_frames.enqueue(runtime, tcp_output, tx_index)
    {
        TcpSessionProtocol::with_queue(
            session_queue,
            |queue: &mut super::session::TcpSessionQueue<C>| {
            queue.protocol.remove_segment(tx_index);
            Ok(())
            },
        )?;
        runtime.free_index(tx_index);
        return Err(error);
    }
    runtime.free_index(index);
    Ok(())
}
