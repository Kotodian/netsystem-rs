use hammer_infra::boxed::Box;
use hammer_infra::vec::Vec;

#[test]
fn vec_uses_the_rust_collection_interface() {
    let mut values = Vec::from([1, 2, 2, 3]);
    values.dedup();
    values.insert(1, 9);
    assert_eq!(values.swap_remove(1), 9);

    values.resize(5, 4);
    values.retain(|value| *value != 4);
    let mut tail = values.split_off(1);
    values.append(&mut tail);
    assert!(tail.is_empty());

    let available = values.capacity() - values.len();
    assert_eq!(values.spare_capacity_mut().len(), available);

    let boxed: Box<[i32]> = values.into_boxed_slice();
    assert_eq!(&*boxed, &[1, 3, 2]);

    let repeated: Vec<_> = hammer_infra::vec![7; 3];
    assert_eq!(repeated, [7, 7, 7]);
}
