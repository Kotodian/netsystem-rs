use hammer_core::data_plane::{NodeId, NodeNext};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExampleNext {
    Drop,
    Punt,
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
fn local_next_slots_index_registered_targets() {
    let nexts = [NodeId::new(3), NodeId::new(9)];
    assert_eq!(
        nexts[usize::from(NodeNext::slot(ExampleNext::Drop))],
        NodeId::new(3)
    );
    assert_eq!(nexts[usize::from(NodeNext::slot(1u16))], NodeId::new(9));
}
