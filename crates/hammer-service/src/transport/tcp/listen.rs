use std::cell::RefCell;
use std::net::SocketAddr;
use std::sync::Arc;

use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeProcessFn, NodeResult,
    NodeRuntimeData, NodeVectorDispatch,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{TcpCapabilities, TcpHandshakeObservation, TcpSeq};

use super::TcpLookupId;
use super::input::take_pending_tcp_accept;
use super::options::tcp_capabilities_from_options;

#[hammer_component_macros::node_next]
pub enum TcpListenNext {
    Accept,
}

pub trait TcpListenBackend: Send + Sync {
    #[inline]
    fn observe_passive_open(&self, _observation: TcpPassiveOpenObservation) -> CoreResult<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpPassiveOpenObservation {
    pub listener_id: TcpLookupId,
    pub local: SocketAddr,
    pub remote: SocketAddr,
    pub sequence: u32,
    pub next_sequence: u32,
    pub advertised_window: u32,
    pub capabilities: TcpCapabilities,
}

struct TcpListenBackendHandle {
    raw: *const (),
    clone_raw: fn(*const ()) -> *const (),
    drop_raw: fn(*const ()),
    observe_passive_open: fn(*const (), TcpPassiveOpenObservation) -> CoreResult<()>,
}

unsafe impl Send for TcpListenBackendHandle {}
unsafe impl Sync for TcpListenBackendHandle {}

impl Default for TcpListenBackendHandle {
    #[inline]
    fn default() -> Self {
        Self::noop()
    }
}

impl Clone for TcpListenBackendHandle {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            raw: (self.clone_raw)(self.raw),
            clone_raw: self.clone_raw,
            drop_raw: self.drop_raw,
            observe_passive_open: self.observe_passive_open,
        }
    }
}

impl Drop for TcpListenBackendHandle {
    #[inline]
    fn drop(&mut self) {
        (self.drop_raw)(self.raw);
    }
}

impl TcpListenBackendHandle {
    #[inline]
    fn noop() -> Self {
        Self {
            raw: std::ptr::null(),
            clone_raw: clone_noop_listen_backend,
            drop_raw: drop_noop_listen_backend,
            observe_passive_open: observe_noop_passive_open,
        }
    }

    #[inline]
    fn new<O>(backend: Arc<O>) -> Self
    where
        O: TcpListenBackend + 'static,
    {
        Self {
            raw: Arc::into_raw(backend) as *const (),
            clone_raw: clone_listen_backend_arc::<O>,
            drop_raw: drop_listen_backend_arc::<O>,
            observe_passive_open: observe_passive_open_with::<O>,
        }
    }

    #[inline]
    fn observe_passive_open(&self, observation: TcpPassiveOpenObservation) -> CoreResult<()> {
        (self.observe_passive_open)(self.raw, observation)
    }
}

#[inline]
fn clone_noop_listen_backend(_raw: *const ()) -> *const () {
    std::ptr::null()
}

#[inline]
fn drop_noop_listen_backend(_raw: *const ()) {}

#[inline]
fn observe_noop_passive_open(
    _raw: *const (),
    _observation: TcpPassiveOpenObservation,
) -> CoreResult<()> {
    Ok(())
}

#[inline]
fn clone_listen_backend_arc<O>(raw: *const ()) -> *const ()
where
    O: TcpListenBackend + 'static,
{
    let raw = raw.cast::<O>();
    if !raw.is_null() {
        unsafe {
            Arc::increment_strong_count(raw);
        }
    }
    raw.cast()
}

#[inline]
fn drop_listen_backend_arc<O>(raw: *const ())
where
    O: TcpListenBackend + 'static,
{
    let raw = raw.cast::<O>();
    if !raw.is_null() {
        unsafe {
            drop(Arc::from_raw(raw));
        }
    }
}

#[inline]
fn observe_passive_open_with<O>(
    raw: *const (),
    observation: TcpPassiveOpenObservation,
) -> CoreResult<()>
where
    O: TcpListenBackend + 'static,
{
    let raw = raw.cast::<O>();
    if raw.is_null() {
        return Ok(());
    }
    unsafe { (&*raw).observe_passive_open(observation) }
}

#[derive(Clone)]
struct TcpListenRuntime {
    backend: TcpListenBackendHandle,
}

thread_local! {
    static TCP_LISTEN_RUNTIMES: RefCell<hammer_infra::vec::Vec<TcpListenRuntime>> =
        const { RefCell::new(hammer_infra::vec::Vec::new()) };
}

#[inline]
fn has_tcp_listen_runtime(data: NodeRuntimeData) -> bool {
    data.word(1) != 0
}

fn register_tcp_listen_runtime(backend: TcpListenBackendHandle) -> CoreResult<NodeRuntimeData> {
    TCP_LISTEN_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let slot = runtimes.len();
        runtimes.push(TcpListenRuntime { backend });
        Ok(NodeRuntimeData::from_words([
            u64::try_from(slot)
                .map_err(|_| CoreError::internal("TCP listen runtime slot overflow"))?,
            1,
            0,
            0,
        ]))
    })
}

fn tcp_listen_runtime(data: NodeRuntimeData) -> CoreResult<TcpListenRuntime> {
    if !has_tcp_listen_runtime(data) {
        return Ok(TcpListenRuntime {
            backend: TcpListenBackendHandle::default(),
        });
    }
    let slot = data.usize_word(0)?;
    TCP_LISTEN_RUNTIMES.with(|runtimes| {
        runtimes
            .borrow()
            .get(slot)
            .cloned()
            .ok_or_else(|| CoreError::internal("TCP listen runtime slot is invalid"))
    })
}

fn sync_tcp_listen_runtime(
    data: NodeRuntimeData,
    backend: TcpListenBackendHandle,
) -> CoreResult<()> {
    if !has_tcp_listen_runtime(data) {
        return Ok(());
    }
    let slot = data.usize_word(0)?;
    TCP_LISTEN_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let runtime = runtimes
            .get_mut(slot)
            .ok_or_else(|| CoreError::internal("TCP listen runtime slot is invalid"))?;
        runtime.backend = backend;
        Ok(())
    })
}

#[hammer_component_macros::node(role = internal, next = TcpListenNext)]
pub struct TcpListenNode {
    #[node(default)]
    runtime_data: NodeRuntimeData,
    #[node(default)]
    backend: TcpListenBackendHandle,
    #[node(default)]
    cached_next: Option<hammer_adapter::NodeId>,
}

impl TcpListenNode {
    #[inline]
    pub fn with_backend<O>(mut self, backend: Arc<O>) -> Self
    where
        O: TcpListenBackend + 'static,
    {
        let backend = TcpListenBackendHandle::new(backend);
        if has_tcp_listen_runtime(self.runtime_data) {
            let _ = sync_tcp_listen_runtime(self.runtime_data, backend.clone());
        } else if let Ok(runtime_data) = register_tcp_listen_runtime(backend.clone()) {
            self.runtime_data = runtime_data;
        }
        self.backend = backend;
        self
    }
}

impl Node for TcpListenNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        sync_tcp_listen_runtime(self.runtime_data, self.backend.clone())?;
        let next = Self::runtime_nexts(runtime)?;
        let accept = next[TcpListenNext::Accept as usize];
        let (result, cached_next) = NodeVectorDispatch::new(self.cached_next).route_frame_index(
            runtime,
            frame,
            |index| tcp_listen_next_for_index(runtime, index, accept, &self.backend),
        )?;
        self.cached_next = cached_next;
        Ok(result)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_listen_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        sync_tcp_listen_runtime(self.runtime_data, self.backend.clone())?;
        Ok(self.runtime_data)
    }
}

fn tcp_listen_process(
    runtime: &DataPlaneRuntime,
    data: hammer_adapter::NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let state = tcp_listen_runtime(data)?;
    let next = TcpListenNode::runtime_nexts(runtime)?;
    let accept = next[TcpListenNext::Accept as usize];
    let (result, _) = NodeVectorDispatch::new(None).route_frame_index(runtime, frame, |index| {
        tcp_listen_next_for_index(runtime, index, accept, &state.backend)
    })?;
    Ok(result)
}

fn tcp_listen_next_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    accept: NodeId,
    backend: &TcpListenBackendHandle,
) -> CoreResult<Option<NodeId>> {
    let Some(listener_id) = take_pending_tcp_accept(index)? else {
        return Ok(Some(accept));
    };
    let observation = tcp_listen_handshake_observation(runtime, index)?;
    if tcp_listen_is_pure_syn(observation) {
        let Some((remote, local)) = tcp_listen_socket_addrs(runtime, index)? else {
            return Ok(Some(accept));
        };
        backend.observe_passive_open(TcpPassiveOpenObservation {
            listener_id,
            local,
            remote,
            sequence: observation.sequence,
            next_sequence: observation.next_sequence,
            advertised_window: observation.advertised_window,
            capabilities: observation.capabilities,
        })?;
        runtime.free_index(index);
        return Ok(None);
    }
    super::input::mark_pending_tcp_accept(index, listener_id)?;
    Ok(Some(accept))
}

#[inline]
fn tcp_listen_is_pure_syn(observation: TcpHandshakeObservation) -> bool {
    observation.syn()
        && !observation.ack()
        && !observation.fin()
        && observation.flags & 0x04 == 0
        && observation.next_sequence == TcpSeq::new(observation.sequence).advance(1).raw()
}

fn tcp_listen_handshake_observation(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
) -> CoreResult<TcpHandshakeObservation> {
    let cursor = runtime.get_buffer(index)?.packet_cursor();
    let packet: std::vec::Vec<u8> = runtime.copy_current_chain(index)?.into_iter().collect();
    let sequence_offset = cursor.transport_header_offset() + 4;
    let acknowledgment_offset = cursor.transport_header_offset() + 8;
    let flags_offset = cursor.transport_header_offset() + 13;
    let window_offset = cursor.transport_header_offset() + 14;
    let flags = packet.get(flags_offset).copied().unwrap_or_default();
    let sequence = packet
        .get(sequence_offset..sequence_offset + 4)
        .map(|bytes| u32::from_be_bytes(bytes.try_into().expect("sequence bytes")))
        .unwrap_or_default();
    let acknowledgment = packet
        .get(acknowledgment_offset..acknowledgment_offset + 4)
        .map(|bytes| u32::from_be_bytes(bytes.try_into().expect("ack bytes")))
        .filter(|_| flags & 0x10 != 0);
    let advertised_window = packet
        .get(window_offset..window_offset + 2)
        .map(|bytes| u16::from_be_bytes(bytes.try_into().expect("window bytes")) as u32)
        .unwrap_or_default();
    let payload_len = (cursor
        .packet_len()
        .saturating_sub(cursor.transport_payload_offset())) as u32;
    let next_sequence = TcpSeq::new(sequence)
        .advance(payload_len + u32::from(flags & 0x02 != 0) + u32::from(flags & 0x01 != 0))
        .raw();
    let capabilities = packet
        .get(cursor.transport_header_offset() + 20..cursor.transport_payload_offset())
        .map(tcp_capabilities_from_options)
        .unwrap_or_default();
    Ok(TcpHandshakeObservation::new(
        flags,
        sequence,
        acknowledgment,
        advertised_window,
        next_sequence,
    )
    .with_capabilities(capabilities))
}

fn tcp_listen_socket_addrs(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
) -> CoreResult<Option<(SocketAddr, SocketAddr)>> {
    let metadata = runtime.metadata(index)?;
    let Some(remote) = metadata
        .source
        .as_ref()
        .map(|source| SocketAddr::new(source.host, source.port))
    else {
        return Ok(None);
    };
    let Some(local) = metadata
        .destination
        .as_ref()
        .map(|destination| SocketAddr::new(destination.host, destination.port))
    else {
        return Ok(None);
    };
    Ok(Some((remote, local)))
}
