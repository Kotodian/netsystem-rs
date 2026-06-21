use std::cell::RefCell;

use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeNextFrames, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::TcpConnectionId;
use hammer_infra::map::FlatHashTable;
use hammer_infra::vec::Vec;

use crate::session::SessionId;
use crate::transport::congestion::CongestionController;
use super::connection::TcpConnection;
use super::input::TcpInputControlPlane;
use super::segment::parse_tcp_packet;
use super::TcpQueueHandle;

#[hammer_component_macros::node_next]
pub enum TcpListenNext {
    Output,
    Drop,
}

#[hammer_component_macros::node(role = internal, next = TcpListenNext)]
pub struct TcpListenNode<C: CongestionController + 'static> {
    control: TcpInputControlPlane,
    session_queue: TcpQueueHandle<C>,
}

thread_local! {
    static TCP_LISTEN_CONTROLS: RefCell<Vec<TcpInputControlPlane>> =
        const { RefCell::new(Vec::new()) };
    static TCP_LISTEN_CONTROL_INDEX: RefCell<FlatHashTable<u64, usize>> =
        RefCell::new(FlatHashTable::new());
}

fn sync_tcp_listen_control<C>(
    session_queue: TcpQueueHandle<C>,
    control: TcpInputControlPlane,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let key = session_queue.runtime_data().word(0);
    TCP_LISTEN_CONTROL_INDEX.with(|index| {
        let mut index = index.borrow_mut();
        if let Some(slot) = index.lookup(&key) {
            TCP_LISTEN_CONTROLS.with(|controls| {
                let mut controls = controls.borrow_mut();
                let Some(current) = controls.get_mut(slot) else {
                    return Err(CoreError::internal("tcp listen control slot is invalid"));
                };
                *current = control;
                Ok(())
            })
        } else {
            TCP_LISTEN_CONTROLS.with(|controls| {
                let mut controls = controls.borrow_mut();
                let slot = controls.len();
                controls.push(control);
                index.insert(key, slot);
                Ok(())
            })
        }
    })
}

fn tcp_listen_control(data: NodeRuntimeData) -> CoreResult<TcpInputControlPlane> {
    let key = data.word(0);
    TCP_LISTEN_CONTROL_INDEX.with(|index| {
        let slot = index
            .borrow()
            .lookup(&key)
            .ok_or_else(|| CoreError::internal("tcp listen control index is missing"))?;
        TCP_LISTEN_CONTROLS.with(|controls| {
            controls
                .borrow()
                .get(slot)
                .cloned()
                .ok_or_else(|| CoreError::internal("tcp listen control is missing"))
        })
    })
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
        sync_tcp_listen_control(self.session_queue, self.control.clone())?;
        let next = Self::runtime_nexts(runtime)?;
        tcp_listen_process_frame(runtime, frame, self.control.clone(), self.session_queue, next)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_listen_process::<C>
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        sync_tcp_listen_control(self.session_queue, self.control.clone())?;
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
    tcp_listen_process_frame::<C>(
        runtime,
        frame,
        tcp_listen_control(data)?,
        TcpQueueHandle::new(data),
        next,
    )
}

#[inline]
fn tcp_listen_process_frame<C>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    control: TcpInputControlPlane,
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
        match tcp_listen_index(
            runtime,
            index,
            &control,
            session_queue,
            tcp_output,
            &mut next_frames,
        ) {
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
    control: &TcpInputControlPlane,
    session_queue: TcpQueueHandle<C>,
    tcp_output: NodeId,
    next_frames: &mut NodeNextFrames,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let packet = parse_tcp_packet(runtime, index)?;
    let capabilities = control
        .lookup_listener(packet.local)
        .map(|value| value.capabilities)
        .unwrap_or_default();
    let mut tx_index = None;
    let result = {
        let mut queue = session_queue.borrow_mut()?;
        let worker = queue.worker();
        let session_id = queue.insert_session_with_id(|session_id: SessionId| {
            let connection_id = TcpConnectionId::new(session_id.get());
            let mut connection = TcpConnection::new(
                Some(connection_id),
                worker,
                packet.local.port(),
                Some(packet.local),
                packet.remote,
            );
            let _ = connection.set_local_capabilities(capabilities);
            connection
        });
        let control = queue
            .session_mut(session_id)
            .ok_or_else(|| CoreError::internal("tcp listen session is missing"))?
            .receive_syn(&packet)?;
        let present = queue.session(session_id).is_some();
        if present {
            queue.refresh_session_route(session_id)?;
        }
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
