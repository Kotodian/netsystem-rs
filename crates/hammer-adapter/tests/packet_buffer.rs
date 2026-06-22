use std::mem::{align_of, size_of, transmute};

use hammer_adapter::{
    NetworkOpaque, PRIMARY_OPAQUE_ALIGN, PRIMARY_OPAQUE_BYTES, PacketBufferCacheline0,
    PacketBufferCacheline1, PrimaryOpaque, SecondaryOpaque,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct TestPrimaryPayload {
    a: u64,
    b: u64,
    c: u64,
    d: u64,
    e: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
struct TestSecondaryPayload {
    words: [u64; 7],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, align(8))]
struct TestNetworkPayload {
    left: u64,
    middle: u64,
    right: u64,
}

#[test]
fn packet_buffer_layout_matches_cacheline_budget() {
    assert_eq!(size_of::<PacketBufferCacheline0>(), 64);
    assert_eq!(align_of::<PacketBufferCacheline0>(), 64);
    assert_eq!(size_of::<PacketBufferCacheline1>(), 64);
    assert_eq!(align_of::<PacketBufferCacheline1>(), 64);
    assert_eq!(size_of::<PrimaryOpaque>(), 40);
    assert_eq!(align_of::<PrimaryOpaque>(), 8);
    assert_eq!(PRIMARY_OPAQUE_BYTES, size_of::<PrimaryOpaque>());
    assert_eq!(PRIMARY_OPAQUE_ALIGN, align_of::<PrimaryOpaque>());
    assert!(size_of::<NetworkOpaque>() <= PRIMARY_OPAQUE_BYTES);
    assert!(align_of::<NetworkOpaque>() <= PRIMARY_OPAQUE_ALIGN);
    assert_eq!(size_of::<SecondaryOpaque>(), 56);
    assert_eq!(align_of::<SecondaryOpaque>(), 8);
}

#[test]
fn primary_and_secondary_payloads_round_trip_safely() {
    let primary_payload = TestPrimaryPayload {
        a: 1,
        b: 2,
        c: 3,
        d: 4,
        e: 5,
    };
    let mut primary = PrimaryOpaque::default();
    primary.write(primary_payload);
    assert_eq!(primary.read::<TestPrimaryPayload>(), primary_payload);
    primary.clear();
    assert_eq!(
        primary.read::<TestPrimaryPayload>(),
        TestPrimaryPayload::default()
    );

    let secondary_payload = TestSecondaryPayload {
        words: [11, 12, 13, 14, 15, 16, 17],
    };
    let mut secondary = SecondaryOpaque::default();
    secondary.write(secondary_payload);
    assert_eq!(secondary.read::<TestSecondaryPayload>(), secondary_payload);
    secondary.clear();
    assert_eq!(
        secondary.read::<TestSecondaryPayload>(),
        TestSecondaryPayload { words: [0; 7] }
    );
}

#[test]
fn network_overlay_updates_common_fields_and_payload() {
    let mut header = PacketBufferCacheline0::default();
    {
        let network =
            unsafe { transmute::<&mut PrimaryOpaque, &mut NetworkOpaque>(&mut header.opaque) };
        network.sw_if_index = [7, 9001];
        network.l2_hdr_offset = 0;
        network.l3_hdr_offset = 14;
        network.l4_hdr_offset = 34;
        network.feature_arc_index = 3;
        network.oflags = 0b1010_0101;
        network.payload_mut().write(TestNetworkPayload {
            left: 0xaa,
            middle: 0xbb,
            right: 0xcc,
        });
    }

    let network = unsafe { transmute::<&PrimaryOpaque, &NetworkOpaque>(&header.opaque) };
    assert_eq!(network.sw_if_index, [7, 9001]);
    assert_eq!(network.l2_hdr_offset, 0);
    assert_eq!(network.l3_hdr_offset, 14);
    assert_eq!(network.l4_hdr_offset, 34);
    assert_eq!(network.feature_arc_index, 3);
    assert_eq!(network.oflags, 0b1010_0101);
    assert_eq!(
        network.payload().read::<TestNetworkPayload>(),
        TestNetworkPayload {
            left: 0xaa,
            middle: 0xbb,
            right: 0xcc,
        }
    );
}
