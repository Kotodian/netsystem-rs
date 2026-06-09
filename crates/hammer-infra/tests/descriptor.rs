use hammer_infra::descriptor::{Descriptor, DescriptorTable};

enum TestTag {}

type TestDescriptor = Descriptor<TestTag>;

#[test]
fn descriptor_table_reuses_lowest_free_slot_and_invalidates_stale_handles() {
    let mut table = DescriptorTable::<u64, TestTag>::new();
    let first = table.insert(10);
    let second = table.insert(20);
    let third = table.insert(30);

    assert_eq!(first.slot(), 0);
    assert_eq!(second.slot(), 1);
    assert_eq!(third.slot(), 2);
    assert_eq!(table.remove(second), Some(20));
    assert!(!table.contains(second));
    assert_eq!(table.remove(first), Some(10));
    assert!(!table.contains(first));

    let reused_first = table.insert(40);
    let reused_second = table.insert(50);
    assert_eq!(reused_first.slot(), 0);
    assert_eq!(reused_second.slot(), 1);
    assert_ne!(reused_first.generation(), first.generation());
    assert_ne!(reused_second.generation(), second.generation());
    assert_eq!(table.get(reused_first), Some(&40));
    assert_eq!(table.get(reused_second), Some(&50));
    assert_eq!(table.get(third), Some(&30));
    assert_eq!(table.get(first), None);
    assert_eq!(table.get(second), None);
}

#[test]
fn descriptor_raw_value_round_trips_slot_and_generation() {
    let descriptor = TestDescriptor::from_parts(0x1122_3344, 0xaabb_ccdd);

    assert_eq!(descriptor.slot(), 0x1122_3344);
    assert_eq!(descriptor.generation(), 0xaabb_ccdd);
    assert_eq!(
        TestDescriptor::new(descriptor.value()).value(),
        descriptor.value()
    );
}
