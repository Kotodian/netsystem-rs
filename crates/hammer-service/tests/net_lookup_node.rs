use std::cell::RefCell;
use std::rc::Rc;

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, InternalNode, Node, NodeResult, RouteDecision, RouteMetadata,
    RouteTarget,
};
use hammer_core::error::CoreResult;
use hammer_service::net::RouteLookupNode;

struct SinkNode {
    payloads: Rc<RefCell<Vec<Vec<u8>>>>,
}

impl Node<TestNode> for SinkNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for buffer in frame.drain_pending() {
            self.payloads
                .borrow_mut()
                .push(runtime.copy_current_chain(buffer)?);
            runtime.free_index(buffer);
        }
        Ok(NodeResult::drop())
    }
}

enum TestNode {
    Sink(SinkNode),
    RouteLookup(RouteLookupNode),
}

impl From<RouteLookupNode> for TestNode {
    fn from(node: RouteLookupNode) -> Self {
        Self::RouteLookup(node)
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
            Self::Sink(node) => node.process(runtime, frame),
            Self::RouteLookup(node) => node.process(runtime, frame),
        }
    }
}

fn assert_internal_node<I>(node: &I)
where
    I: InternalNode<TestNode>,
{
    let _ = node;
}

#[test]
fn route_lookup_node_uses_explicit_reject_output() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(16, 8, 4, 4);
    let direct_payloads = Rc::new(RefCell::new(Vec::new()));
    let block_payloads = Rc::new(RefCell::new(Vec::new()));
    let drop_payloads = Rc::new(RefCell::new(Vec::new()));
    let direct = runtime.nodes().register(TestNode::Sink(SinkNode {
        payloads: Rc::clone(&direct_payloads),
    }));
    let block = runtime.nodes().register(TestNode::Sink(SinkNode {
        payloads: Rc::clone(&block_payloads),
    }));
    let drop = runtime.nodes().register(TestNode::Sink(SinkNode {
        payloads: Rc::clone(&drop_payloads),
    }));
    let lookup = RouteLookupNode::new()
        .with_outbound("direct", direct)
        .with_outbound("block", block)
        .with_reject(drop);
    assert_internal_node(&lookup);
    let lookup = runtime.nodes().register_internal(lookup);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    for (decision, payload) in [
        (
            RouteDecision::Route {
                target: RouteTarget::Outbound("direct".to_owned()),
            },
            b"first".as_slice(),
        ),
        (
            RouteDecision::Route {
                target: RouteTarget::Outbound("block".to_owned()),
            },
            b"second".as_slice(),
        ),
        (
            RouteDecision::Reject {
                method: "drop".to_owned(),
            },
            b"reject".as_slice(),
        ),
    ] {
        let buffer = runtime
            .alloc_index_with_bytes(
                RouteMetadata {
                    route_decision: Some(decision),
                    ..Default::default()
                },
                payload,
            )
            .expect("alloc packet");
        runtime
            .get_frame_mut(frame)
            .expect("mutate frame")
            .push_index(buffer)
            .expect("push packet");
    }

    assert!(runtime.schedule_frame(lookup, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 4);
    assert_eq!(&*direct_payloads.borrow(), &[b"first".to_vec()]);
    assert_eq!(&*block_payloads.borrow(), &[b"second".to_vec()]);
    assert_eq!(&*drop_payloads.borrow(), &[b"reject".to_vec()]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}
