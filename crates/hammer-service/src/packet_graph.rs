//! Service graph: linkme [`SERVICE_GRAPH_NODES`] + per-worker boot in TLS.

use std::cell::{RefCell, UnsafeCell};
use std::sync::Arc;

use hammer_adapter::{
    DataPlaneBuffers, DataWorkerId, NodeHandle, NodeId, NodeRegistration, NodeRuntime,
    NodeRuntimeData, NodeState,
};
use hammer_core::config::Route;
use hammer_core::config::network::CongestionController;
use hammer_core::error::{CoreError, CoreResult, HammerError, HammerResult};
use hammer_runtime::graph::Graph;
use hammer_runtime::spawn::DataRuntimeContext;

use crate::session::node::SessionQueueNode;
use crate::session::runtime::dispatch_registered_session_queue_once_at;
use crate::transport::congestion::CongestionController as CongestionControllerTrait;
use crate::transport::tcp::lookup::{set_tcp_worker_state, TcpWorkerOwnedState};
use crate::transport::tcp::{TcpConnection, TcpInputControlPlane, TcpQueue, TcpSessionDriver};

/// Config → CC type. Only place that maps config enum to controller type.
#[macro_export]
macro_rules! with_tcp_cc {
    ($boot:expr, |$cc:ident| $body:expr) => {{
        match $boot.congestion {
            ::hammer_core::config::network::CongestionController::Bbr => {{
                type $cc = $crate::transport::congestion::BbrController;
                $body
            }}
        }
    }};
}

pub(crate) struct Boot {
    pub(crate) congestion: CongestionController,
    mss: usize,
    pub(crate) tcp_control: TcpInputControlPlane,
    pub(crate) handoff_handle: NodeHandle,
    pub(crate) routes: Arc<[Route]>,
    buffers: DataPlaneBuffers,
    worker: UnsafeCell<WorkerState>,
}

#[derive(Default)]
struct WorkerState {
    queue_runtime_data: Option<NodeRuntimeData>,
    session_queue_node: Option<SessionQueueNode>,
}

thread_local! {
    static BOOT: RefCell<Option<Boot>> = const { RefCell::new(None) };
}

pub(crate) fn with_boot<R>(f: impl FnOnce(&Boot) -> CoreResult<R>) -> CoreResult<R> {
    BOOT.with(|cell| {
        let boot = cell.borrow();
        let boot = boot
            .as_ref()
            .ok_or_else(|| CoreError::internal("graph boot missing"))?;
        f(boot)
    })
}

#[linkme::distributed_slice]
pub static SERVICE_GRAPH_NODES: [(
    NodeRegistration,
    hammer_adapter::NodeKind,
    fn(&NodeRuntime, usize, &()) -> CoreResult<NodeId>,
    Option<fn(&NodeRuntime, usize, &()) -> CoreResult<()>>,
)] = [..];

fn service_graph() -> Graph<()> {
    Graph::new(&*SERVICE_GRAPH_NODES)
}

fn graph_node_slot(name: &str) -> Option<NodeId> {
    SERVICE_GRAPH_NODES
        .iter()
        .position(|(registration, ..)| node_registration_name(*registration) == Some(name))
        .and_then(|slot| u32::try_from(slot).ok())
        .map(NodeId::new)
}

fn node_registration_name(registration: NodeRegistration) -> Option<&'static str> {
    match registration {
        NodeRegistration::Plain => None,
        NodeRegistration::Next { name, .. } | NodeRegistration::Sibling { name, .. } => Some(name),
    }
}

impl Boot {
    fn new(
        congestion: CongestionController,
        mss: usize,
        tcp_control: TcpInputControlPlane,
        handoff_handle: NodeHandle,
        routes: Arc<[Route]>,
        buffers: DataPlaneBuffers,
    ) -> Self {
        Self {
            congestion,
            mss,
            tcp_control,
            handoff_handle,
            routes,
            buffers,
            worker: UnsafeCell::new(WorkerState::default()),
        }
    }

    pub(crate) fn ensure_tcp_session(&self, worker: usize) -> CoreResult<NodeRuntimeData> {
        let worker_state = unsafe { &mut *self.worker.get() };
        if let Some(runtime_data) = worker_state.queue_runtime_data {
            return Ok(runtime_data);
        }
        crate::with_tcp_cc!(self, |C| self.register_tcp_session::<C>(worker, worker_state))
    }

    pub(crate) fn set_session_queue_node(&self, node: SessionQueueNode) {
        unsafe {
            (*self.worker.get()).session_queue_node = Some(node);
        }
    }

    fn register_tcp_session<C: CongestionControllerTrait + 'static>(
        &self,
        worker: usize,
        worker_state: &mut WorkerState,
    ) -> CoreResult<NodeRuntimeData> {
        let worker = DataWorkerId::new(
            u32::try_from(worker)
                .map_err(|_| CoreError::internal("worker index does not fit into u32"))?,
        );
        let mut worker_state_tls = TcpWorkerOwnedState::new(worker);
        set_tcp_worker_state(&mut worker_state_tls);
        let queue = crate::session::node::register_session_queue(TcpSessionDriver::<C>::new(
            worker,
            self.buffers.clone(),
        ))?;
        let runtime_data = queue.runtime_data();
        worker_state.queue_runtime_data = Some(runtime_data);
        Ok(runtime_data)
    }
}

pub(crate) fn graph_node(runtime: &NodeRuntime, name: &str) -> CoreResult<NodeId> {
    runtime
        .node_by_name(name)
        .ok_or_else(|| CoreError::internal(format!("node `{name}` is not registered")))
}

pub(crate) fn ensure_tcp_session(worker_id: usize) -> CoreResult<NodeRuntimeData> {
    with_boot(|boot| boot.ensure_tcp_session(worker_id))
}

pub(crate) fn wire_session_queue(
    runtime: &NodeRuntime,
    worker_id: usize,
    _: &(),
) -> CoreResult<()> {
    with_boot(|boot| {
        crate::with_tcp_cc!(boot, |C| {
            let queue = TcpQueue::<C>::new(boot.ensure_tcp_session(worker_id)?);
            let tcp_output = graph_node(runtime, "tcp-output")?;
            let worker = unsafe { &*boot.worker.get() };
            let session_queue_node = worker
                .session_queue_node
                .as_ref()
                .ok_or_else(|| CoreError::internal("session queue node missing"))?;
            let node = graph_node(runtime, "session-queue")?;
            session_queue_node.attach_queue(
                queue,
                tcp_output.into(),
                dispatch_registered_session_queue_once_at::<TcpConnection<C>>,
            )?;
            runtime.set_node_state(node, NodeState::Polling)?;
            Ok(())
        })
    })
}

#[inline]
pub fn resolve_graph_node(name: &str) -> Option<NodeId> {
    graph_node_slot(name)
}

pub fn install_service_graph(
    data_context: &DataRuntimeContext,
    congestion: CongestionController,
    mss: usize,
    tcp_control: TcpInputControlPlane,
    handoff_handle: NodeHandle,
    routes: Arc<[Route]>,
) -> HammerResult<()> {
    data_context
        .install_on_workers(move |worker, runtime| {
            BOOT.with(|cell| {
                *cell.borrow_mut() = Some(Boot::new(
                    congestion,
                    mss,
                    tcp_control.clone(),
                    handoff_handle,
                    Arc::clone(&routes),
                    runtime.packet_buffers().clone(),
                ));
            });
            service_graph()
                .init(runtime.nodes(), worker, &())
                .map_err(HammerError::from)
        })
        .and_then(|results| {
            for result in results {
                result?;
            }
            Ok(())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_adapter::NodeKind;

    #[test]
    fn service_graph_contains_tcp_nodes() {
        for name in [
            "drop",
            "handoff",
            "ip-lookup",
            "tcp-input",
            "tcp-listen",
            "session-queue",
        ] {
            assert!(resolve_graph_node(name).is_some(), "missing {name}");
        }
    }

    #[test]
    fn session_queue_registers_as_driver_node() {
        let (_, kind, ..) = SERVICE_GRAPH_NODES
            .iter()
            .find(|(registration, ..)| {
                matches!(registration, NodeRegistration::Next { name, .. } if *name == "session-queue")
            })
            .expect("session-queue");
        assert_eq!(*kind, NodeKind::Driver);
    }
}
