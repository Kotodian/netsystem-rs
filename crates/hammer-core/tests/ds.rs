use hammer_core::ds::{MtrieEntry, PackedMtrie, PackedMtrieValue, PrefixLengthSearchOrder};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Next {
    Drop,
    Forward,
}

impl PackedMtrieValue for Next {
    #[inline(always)]
    fn into_leaf_value(self) -> u32 {
        match self {
            Self::Drop => 0,
            Self::Forward => 1,
        }
    }

    #[inline(always)]
    fn from_leaf_value(value: u32) -> Self {
        match value {
            0 => Self::Drop,
            1 => Self::Forward,
            other => panic!("unexpected mtrie value: {other}"),
        }
    }
}

#[test]
fn packed_mtrie_stores_leaf_encoded_values_for_hot_paths() {
    let trie = PackedMtrie::from_entries([
        MtrieEntry::new(0, 0, Next::Drop),
        MtrieEntry::new(u32::from_be_bytes([198, 51, 100, 0]), 24, Next::Forward),
    ]);

    assert_eq!(
        trie.lookup(u32::from_be_bytes([203, 0, 113, 7])),
        Some(Next::Drop)
    );
    assert_eq!(
        trie.lookup(u32::from_be_bytes([198, 51, 100, 42])),
        Some(Next::Forward)
    );
    assert_eq!(trie.root_alignment(), 64);
    assert_eq!(trie.root_addr() % 64, 0);
    assert_eq!(trie.ply_addr(0).expect("ply") % 64, 0);
}

#[test]
fn prefix_length_search_order_keeps_longest_prefix_first() {
    let mut order = PrefixLengthSearchOrder::empty();

    order.insert(0);
    order.insert(64);
    order.insert(128);
    order.insert(64);

    assert_eq!(order.as_slice(), &[128, 64, 0]);
}
