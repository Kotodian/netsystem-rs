use hammer_core::data_plane::{NodeId, NodeNext, NodeNextStorage};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExampleNext {
    Drop,
    Punt,
}

impl ExampleNext {
    const COUNT: usize = 2;

    const fn index(self) -> usize {
        match self {
            Self::Drop => 0,
            Self::Punt => 1,
        }
    }
}

impl NodeNext for ExampleNext {
    fn slot(self) -> u16 {
        self.index() as u16
    }
}

#[test]
fn node_next_is_local_u16_slot_and_u16_implements_it() {
    assert_eq!(NodeNext::slot(ExampleNext::Drop), 0u16);
    assert_eq!(NodeNext::slot(ExampleNext::Punt), 1u16);
    assert_eq!(NodeNext::slot(42u16), 42u16);
    assert_eq!(NodeNext::slot(u16::MAX), u16::MAX);

    let nexts = [NodeId::new(3), NodeId::new(9)];
    assert_eq!(
        NodeNextStorage::next(&nexts, ExampleNext::Drop),
        NodeId::new(3)
    );
    assert_eq!(NodeNextStorage::next(&nexts, 1u16), NodeId::new(9));
    assert_eq!(ExampleNext::COUNT, 2);
}
