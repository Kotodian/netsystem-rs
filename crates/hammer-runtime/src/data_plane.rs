use std::fmt;
use std::sync::Arc;

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, Node, NodeId, NodeResult, RouteDispatchNode, RouterNode,
};
use hammer_core::error::CoreResult;

use crate::route::Router;

pub(crate) type RuntimeDataPlaneRuntime = DataPlaneRuntime<RuntimeDataPlaneNode>;

pub(crate) fn new_worker_runtime(slot_capacity: usize, slots: usize) -> RuntimeDataPlaneRuntime {
    RuntimeDataPlaneRuntime::with_buffer_capacity(slot_capacity, slots)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RuntimeDataPlaneGraph {
    pub(crate) ingress: NodeId,
    dispatch: NodeId,
    drop: NodeId,
}

impl RuntimeDataPlaneGraph {
    pub(crate) fn has_same_layout(self, other: Self) -> bool {
        self.ingress == other.ingress && self.dispatch == other.dispatch && self.drop == other.drop
    }
}

pub(crate) fn install_route_graph(
    runtime: &RuntimeDataPlaneRuntime,
    router: Arc<Router>,
    outbound_ids: impl IntoIterator<Item = String>,
    endpoint_ids: impl IntoIterator<Item = String>,
) -> CoreResult<RuntimeDataPlaneGraph> {
    let drop = runtime
        .nodes()
        .register(RuntimeDataPlaneNode::Drop(RuntimeDropNode));
    let mut dispatch_node = RouteDispatchNode::new()
        .with_reject(drop)
        .with_hijack_dns(drop);
    for outbound_id in outbound_ids {
        dispatch_node.register_outbound(outbound_id, drop);
    }
    for endpoint_id in endpoint_ids {
        dispatch_node.register_endpoint(endpoint_id, drop);
    }
    let dispatch = runtime
        .nodes()
        .register(RuntimeDataPlaneNode::RouteDispatch(dispatch_node));
    let ingress = runtime
        .nodes()
        .register(RuntimeDataPlaneNode::Router(RouterNode::new(
            router, dispatch,
        )));
    Ok(RuntimeDataPlaneGraph {
        ingress,
        dispatch,
        drop,
    })
}

pub(crate) enum RuntimeDataPlaneNode {
    Drop(RuntimeDropNode),
    Router(RouterNode<Arc<Router>>),
    RouteDispatch(RouteDispatchNode),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RuntimeDropNode;

impl RuntimeDropNode {
    fn process<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for index in frame.drain_pending() {
            runtime.free_index(index);
        }
        Ok(NodeResult::drop())
    }
}

impl fmt::Debug for RuntimeDataPlaneNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Drop(_) => f.write_str("RuntimeDataPlaneNode::Drop"),
            Self::Router(_) => f.write_str("RuntimeDataPlaneNode::Router"),
            Self::RouteDispatch(_) => f.write_str("RuntimeDataPlaneNode::RouteDispatch"),
        }
    }
}

impl Node<RuntimeDataPlaneNode> for RuntimeDataPlaneNode {
    fn process(
        &mut self,
        runtime: &RuntimeDataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        match self {
            Self::Drop(node) => node.process(runtime, frame),
            Self::Router(node) => node.process(runtime, frame),
            Self::RouteDispatch(node) => node.process(runtime, frame),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use hammer_adapter::{
        ComponentMeta, Network, Outbound, OutboundManager as _, ProxyPacketConn, ProxyStream,
        RuntimeComponent, SocksAddr,
    };
    use hammer_core::config::RouteOptions;
    use hammer_core::error::{CoreError, CoreResult};
    use hammer_core::log::{DiscardWriter, Factory, Logger};

    use crate::{MetricsRegistry, OutboundManager};

    fn logger(id: &str) -> Logger {
        Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
    }

    struct DummyOutbound;

    #[async_trait]
    impl Outbound for DummyOutbound {
        async fn dial(
            &self,
            _network: Network,
            _destination: SocksAddr,
            _initial_payload: &[u8],
        ) -> CoreResult<Box<dyn ProxyStream>> {
            Err(CoreError::internal("dummy outbound does not dial"))
        }

        async fn listen_packet(&self) -> CoreResult<Box<dyn ProxyPacketConn>> {
            Err(CoreError::internal("dummy outbound has no packet conn"))
        }

        async fn probe_latency(&self, _protocol: &str, _timeout: Duration) -> CoreResult<Duration> {
            Err(CoreError::internal("dummy outbound has no probe"))
        }
    }

    fn dummy_outbound(id: &str) -> RuntimeComponent<dyn Outbound> {
        RuntimeComponent::new(
            ComponentMeta::new(
                "outbound",
                "dummy",
                id,
                vec![Network::Udp],
                Vec::new(),
                None,
            ),
            Arc::new(DummyOutbound),
        )
    }

    #[test]
    fn route_graph_routes_default_outbound_to_terminal_drop() {
        let outbound = Arc::new(OutboundManager::new(logger("outbound"), "direct"));
        outbound
            .register_outbound(dummy_outbound("direct"))
            .expect("register direct outbound");
        let router = Arc::new(
            crate::Router::from_options_with_metrics(
                logger("router"),
                RouteOptions {
                    final_: "direct".to_owned(),
                    auto_detect_interface: false,
                    rules: Vec::new(),
                    default_domain_resolver: None,
                },
                Arc::clone(&outbound),
                MetricsRegistry::new(),
            )
            .expect("build router"),
        );
        let runtime = new_worker_runtime(128, 8);
        let graph = install_route_graph(
            &runtime,
            router,
            outbound
                .list()
                .into_iter()
                .map(|outbound| outbound.id().to_owned()),
            std::iter::empty::<String>(),
        )
        .expect("install route graph");
        let frame = runtime.alloc_frame_index().expect("alloc frame");
        let buffer = runtime
            .alloc_index_with_bytes(
                hammer_adapter::RouteMetadata {
                    inbound: "tun".to_owned(),
                    network: Network::Udp,
                    destination: Some(SocksAddr::ip(
                        IpAddr::V4(std::net::Ipv4Addr::new(198, 51, 100, 1)),
                        443,
                    )),
                    ..Default::default()
                },
                b"packet",
            )
            .expect("alloc packet");
        runtime
            .with_frame_mut(frame, |frame| frame.push_index(buffer))
            .expect("mutate frame")
            .expect("push packet");

        assert!(
            runtime
                .schedule_frame(graph.ingress, frame)
                .expect("schedule")
        );

        assert_eq!(runtime.run_ready_nodes().expect("run graph"), 3);
        assert_eq!(runtime.in_use_buffers(), 0);
        assert_eq!(runtime.frames_in_use(), 0);
    }
}
