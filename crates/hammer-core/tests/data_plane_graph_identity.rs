use hammer_core::data_plane::{
    NodeHandle, NodeId, NodeKind, NodeNext, NodeRegistration, NodeState,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExampleNext {
    Drop,
    Punt,
}

impl ExampleNext {
    const COUNT: usize = 2;
}

impl NodeNext for ExampleNext {
    fn slot(self) -> u16 {
        match self {
            Self::Drop => 0,
            Self::Punt => 1,
        }
    }
}

#[test]
fn core_data_plane_graph_identity_items_have_expected_behavior() {
    let node = NodeId::new(7);
    assert_eq!(node.slot(), 7);

    let handle = NodeHandle::new(42);
    assert_eq!(handle, NodeHandle::new(42));

    let registration = NodeRegistration::next("ip-input", ExampleNext::COUNT);
    assert_eq!(registration.name(), Some("ip-input"));
    assert!(matches!(
        registration,
        NodeRegistration::Next {
            name: "ip-input",
            next_count: 2,
        }
    ));

    let sibling = NodeRegistration::sibling_of("ip-input-ipv6", "ip-input");
    assert_eq!(sibling.name(), Some("ip-input-ipv6"));
    assert!(matches!(
        sibling,
        NodeRegistration::Sibling {
            name: "ip-input-ipv6",
            sibling_of: "ip-input",
        }
    ));

    let nexts = [NodeId::new(3), NodeId::new(9)];
    assert_eq!(
        nexts[usize::from(NodeNext::slot(ExampleNext::Drop))],
        NodeId::new(3)
    );
    assert_eq!(
        nexts[usize::from(NodeNext::slot(ExampleNext::Punt))],
        NodeId::new(9)
    );

    assert_eq!(NodeKind::Driver, NodeKind::Driver);
    assert_eq!(NodeKind::Internal, NodeKind::Internal);
    assert_eq!(NodeState::Disabled, NodeState::Disabled);
    assert_eq!(NodeState::default(), NodeState::Polling);
    assert_eq!(NodeNext::slot(ExampleNext::Drop), 0u16);
    assert_eq!(NodeNext::slot(42u16), 42u16);
}
