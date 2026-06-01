use hammer_core::ds::{
    FlatHashTable, Mtrie, MtrieEntry, PackedMtrie, PackedMtrieValue, PrefixLengthSearchOrder,
};

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
fn mtrie_is_generic_over_u32_keys_and_cacheline_aligned() {
    let trie = Mtrie::from_entries([
        MtrieEntry::new(0, 0, [0x11; 32]),
        MtrieEntry::new(u32::from_be_bytes([198, 51, 100, 0]), 24, [0x22; 32]),
    ]);

    assert_eq!(
        trie.lookup(u32::from_be_bytes([203, 0, 113, 7])),
        Some([0x11; 32])
    );
    assert_eq!(
        trie.lookup(u32::from_be_bytes([198, 51, 100, 42])),
        Some([0x22; 32])
    );
    assert_eq!(trie.root_alignment(), 64);
    assert_eq!(trie.root_addr() % 64, 0);
    assert_eq!(trie.ply_addr(0).expect("ply") % 64, 0);
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
}

#[test]
fn flat_hash_table_is_generic_and_exposes_bucket_prefetch() {
    let table = FlatHashTable::from_entries([(10u128, Next::Drop), (42u128, Next::Forward)]);

    table.prefetch_key(&42);

    assert_eq!(table.lookup(&10), Some(Next::Drop));
    assert_eq!(table.lookup(&42), Some(Next::Forward));
    assert_eq!(table.lookup(&99), None);
    assert!(table.bucket_count().is_power_of_two());
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
