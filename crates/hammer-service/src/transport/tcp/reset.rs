use std::cell::RefCell;
use std::net::SocketAddr;
use std::sync::Arc;

use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeProcessFn, NodeResult,
    NodeRuntimeData, NodeVectorDispatch, SocksAddr,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::vec::Vec as InfraVec;

use super::TcpInputError;

#[hammer_component_macros::node_next]
pub enum TcpResetNext {
    Drop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpResetReason {
    AckInvalid,
    ConnectionClosed,
    Other(u16),
    MissingNodeError,
}

impl TcpResetReason {
    #[inline]
    fn from_node_error_code(code: Option<u16>) -> Self {
        match code {
            Some(code) if code == TcpInputError::AckInvalid.code() => Self::AckInvalid,
            Some(code) if code == TcpInputError::ConnectionClosed.code() => Self::ConnectionClosed,
            Some(code) => Self::Other(code),
            None => Self::MissingNodeError,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpResetObservation {
    pub local: SocketAddr,
    pub remote: SocketAddr,
    pub reason: TcpResetReason,
}

pub trait TcpResetObserver: Send + Sync {
    fn observe_reset(&self, observation: TcpResetObservation) -> CoreResult<()>;
}

struct TcpResetObserverHandle {
    raw: *const (),
    clone_raw: fn(*const ()) -> *const (),
    drop_raw: fn(*const ()),
    observe: fn(*const (), TcpResetObservation) -> CoreResult<()>,
}

unsafe impl Send for TcpResetObserverHandle {}
unsafe impl Sync for TcpResetObserverHandle {}

impl Default for TcpResetObserverHandle {
    #[inline]
    fn default() -> Self {
        Self::noop()
    }
}

impl Clone for TcpResetObserverHandle {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            raw: (self.clone_raw)(self.raw),
            clone_raw: self.clone_raw,
            drop_raw: self.drop_raw,
            observe: self.observe,
        }
    }
}

impl Drop for TcpResetObserverHandle {
    #[inline]
    fn drop(&mut self) {
        (self.drop_raw)(self.raw);
    }
}

impl TcpResetObserverHandle {
    #[inline]
    fn noop() -> Self {
        Self {
            raw: std::ptr::null(),
            clone_raw: clone_noop_handle,
            drop_raw: drop_noop_handle,
            observe: observe_noop_reset,
        }
    }

    #[inline]
    fn new<O>(observer: Arc<O>) -> Self
    where
        O: TcpResetObserver + 'static,
    {
        Self {
            raw: Arc::into_raw(observer) as *const (),
            clone_raw: clone_arc_handle::<O>,
            drop_raw: drop_arc_handle::<O>,
            observe: observe_reset_with::<O>,
        }
    }

    #[inline]
    fn is_registered(&self) -> bool {
        !self.raw.is_null()
    }

    #[inline]
    fn observe_reset(&self, observation: TcpResetObservation) -> CoreResult<()> {
        (self.observe)(self.raw, observation)
    }
}

#[inline]
fn clone_noop_handle(_raw: *const ()) -> *const () {
    std::ptr::null()
}

#[inline]
fn drop_noop_handle(_raw: *const ()) {}

#[inline]
fn observe_noop_reset(_raw: *const (), _observation: TcpResetObservation) -> CoreResult<()> {
    Ok(())
}

#[inline]
fn clone_arc_handle<O>(raw: *const ()) -> *const ()
where
    O: TcpResetObserver + 'static,
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
fn drop_arc_handle<O>(raw: *const ())
where
    O: TcpResetObserver + 'static,
{
    let raw = raw.cast::<O>();
    if !raw.is_null() {
        unsafe {
            drop(Arc::from_raw(raw));
        }
    }
}

#[inline]
fn observe_reset_with<O>(raw: *const (), observation: TcpResetObservation) -> CoreResult<()>
where
    O: TcpResetObserver + 'static,
{
    let raw = raw.cast::<O>();
    if raw.is_null() {
        return Ok(());
    }
    unsafe { (&*raw).observe_reset(observation) }
}

#[derive(Clone, Default)]
struct TcpResetRuntime {
    observer: TcpResetObserverHandle,
}

thread_local! {
    static TCP_RESET_RUNTIMES: RefCell<InfraVec<TcpResetRuntime>> =
        const { RefCell::new(InfraVec::new()) };
}

#[inline]
fn has_tcp_reset_runtime(data: NodeRuntimeData) -> bool {
    data.word(1) != 0
}

fn register_tcp_reset_runtime(observer: TcpResetObserverHandle) -> CoreResult<NodeRuntimeData> {
    TCP_RESET_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let slot = runtimes.len();
        runtimes.push(TcpResetRuntime { observer });
        Ok(NodeRuntimeData::from_words([
            u64::try_from(slot)
                .map_err(|_| CoreError::internal("TCP reset runtime slot overflow"))?,
            1,
            0,
            0,
        ]))
    })
}

fn tcp_reset_runtime(data: NodeRuntimeData) -> CoreResult<TcpResetRuntime> {
    if !has_tcp_reset_runtime(data) {
        return Ok(TcpResetRuntime::default());
    }
    let slot = data.usize_word(0)?;
    TCP_RESET_RUNTIMES.with(|runtimes| {
        runtimes
            .borrow()
            .get(slot)
            .cloned()
            .ok_or_else(|| CoreError::internal("TCP reset runtime slot is invalid"))
    })
}

fn sync_tcp_reset_runtime(
    data: NodeRuntimeData,
    observer: TcpResetObserverHandle,
) -> CoreResult<()> {
    if !has_tcp_reset_runtime(data) {
        return Ok(());
    }
    let slot = data.usize_word(0)?;
    TCP_RESET_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let runtime = runtimes
            .get_mut(slot)
            .ok_or_else(|| CoreError::internal("TCP reset runtime slot is invalid"))?;
        runtime.observer = observer;
        Ok(())
    })
}

#[hammer_component_macros::node(role = internal, next = TcpResetNext)]
pub struct TcpResetNode {
    #[node(default)]
    runtime_data: NodeRuntimeData,
    #[node(default)]
    observer: TcpResetObserverHandle,
    #[node(default)]
    cached_next: Option<NodeId>,
}

impl TcpResetNode {
    #[inline]
    pub fn with_observer<O>(mut self, observer: Arc<O>) -> CoreResult<Self>
    where
        O: TcpResetObserver + 'static,
    {
        let observer = TcpResetObserverHandle::new(observer);
        if has_tcp_reset_runtime(self.runtime_data) {
            sync_tcp_reset_runtime(self.runtime_data, observer.clone())?;
        } else {
            self.runtime_data = register_tcp_reset_runtime(observer.clone())?;
        }
        self.observer = observer;
        Ok(self)
    }
}

impl Node for TcpResetNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        sync_tcp_reset_runtime(self.runtime_data, self.observer.clone())?;
        let next = Self::runtime_nexts(runtime)?;
        let drop_next = next[TcpResetNext::Drop as usize];
        let (result, cached_next) = NodeVectorDispatch::new(self.cached_next).route_frame_index(
            runtime,
            frame,
            |index| tcp_reset_next_for_index(runtime, index, drop_next, &self.observer),
        )?;
        self.cached_next = cached_next;
        Ok(result)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_reset_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        sync_tcp_reset_runtime(self.runtime_data, self.observer.clone())?;
        Ok(self.runtime_data)
    }
}

fn tcp_reset_process(
    runtime: &DataPlaneRuntime,
    data: hammer_adapter::NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let state = tcp_reset_runtime(data)?;
    let next = TcpResetNode::runtime_nexts(runtime)?;
    let drop_next = next[TcpResetNext::Drop as usize];
    let (result, _) = NodeVectorDispatch::new(None).route_frame_index(runtime, frame, |index| {
        tcp_reset_next_for_index(runtime, index, drop_next, &state.observer)
    })?;
    Ok(result)
}

fn tcp_reset_next_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    drop_next: NodeId,
    observer: &TcpResetObserverHandle,
) -> CoreResult<Option<NodeId>> {
    if observer.is_registered() {
        observer.observe_reset(tcp_reset_observation(runtime, index)?)?;
    }
    Ok(Some(drop_next))
}

fn tcp_reset_observation(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
) -> CoreResult<TcpResetObservation> {
    let metadata = runtime.metadata(index)?;
    let remote = socket_addr(
        metadata.source,
        "tcp reset observer requires remote source metadata",
    )?;
    let local = socket_addr(
        metadata.destination,
        "tcp reset observer requires local destination metadata",
    )?;
    let reason =
        TcpResetReason::from_node_error_code(runtime.node_error(index)?.map(|error| error.code()));
    Ok(TcpResetObservation {
        local,
        remote,
        reason,
    })
}

fn socket_addr(value: Option<SocksAddr>, missing: &'static str) -> CoreResult<SocketAddr> {
    let value = value.ok_or_else(|| CoreError::internal(missing))?;
    Ok(SocketAddr::new(value.host, value.port))
}
