use std::mem::{align_of, size_of};

use hammer_adapter::{
    NetworkOpaque, NetworkOpaquePayload, PRIMARY_OPAQUE_ALIGN, PRIMARY_OPAQUE_BYTES,
    PacketBufferHeader, PacketBufferHeaderExt, PrimaryOpaque, PrimaryOpaquePayload,
    SecondaryOpaque, SecondaryOpaquePayload,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TestPrimaryPayload {
    a: u64,
    b: u64,
    c: u64,
    d: u64,
    e: u64,
}

impl PrimaryOpaquePayload for TestPrimaryPayload {
    fn encode_primary(&self) -> [u64; 5] {
        [self.a, self.b, self.c, self.d, self.e]
    }

    fn decode_primary(words: [u64; 5]) -> Self {
        Self {
            a: words[0],
            b: words[1],
            c: words[2],
            d: words[3],
            e: words[4],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TestSecondaryPayload {
    words: [u64; 7],
}

impl SecondaryOpaquePayload for TestSecondaryPayload {
    fn encode_secondary(&self) -> [u64; 7] {
        self.words
    }

    fn decode_secondary(words: [u64; 7]) -> Self {
        Self { words }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TestNetworkPayload {
    left: u64,
    middle: u64,
    right: u64,
}

impl NetworkOpaquePayload for TestNetworkPayload {
    fn encode_network(&self) -> [u64; 3] {
        [self.left, self.middle, self.right]
    }

    fn decode_network(words: [u64; 3]) -> Self {
        Self {
            left: words[0],
            middle: words[1],
            right: words[2],
        }
    }
}

#[test]
fn packet_buffer_layout_matches_cacheline_budget() {
    assert_eq!(size_of::<PacketBufferHeader>(), 64);
    assert_eq!(align_of::<PacketBufferHeader>(), 64);
    assert_eq!(size_of::<PacketBufferHeaderExt>(), 64);
    assert_eq!(align_of::<PacketBufferHeaderExt>(), 64);
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
    primary.write(&primary_payload);
    assert_eq!(primary.read::<TestPrimaryPayload>(), primary_payload);
    primary.clear();
    assert_eq!(
        primary.read::<TestPrimaryPayload>().encode_primary(),
        [0; 5]
    );

    let secondary_payload = TestSecondaryPayload {
        words: [11, 12, 13, 14, 15, 16, 17],
    };
    let mut secondary = SecondaryOpaque::default();
    secondary.write(&secondary_payload);
    assert_eq!(secondary.read::<TestSecondaryPayload>(), secondary_payload);
    secondary.clear();
    assert_eq!(
        secondary.read::<TestSecondaryPayload>().encode_secondary(),
        [0; 7]
    );
}

#[test]
fn network_overlay_updates_common_fields_and_payload() {
    let mut header = PacketBufferHeader::default();
    {
        let network = header.network_mut();
        network.sw_if_index = [7, 9001];
        network.l2_hdr_offset = 0;
        network.l3_hdr_offset = 14;
        network.l4_hdr_offset = 34;
        network.feature_arc_index = 3;
        network.oflags = 0b1010_0101;
        network.payload.write(&TestNetworkPayload {
            left: 0xaa,
            middle: 0xbb,
            right: 0xcc,
        });
    }

    let network = header.network();
    assert_eq!(network.sw_if_index, [7, 9001]);
    assert_eq!(network.l2_hdr_offset, 0);
    assert_eq!(network.l3_hdr_offset, 14);
    assert_eq!(network.l4_hdr_offset, 34);
    assert_eq!(network.feature_arc_index, 3);
    assert_eq!(network.oflags, 0b1010_0101);
    assert_eq!(
        network.payload.read::<TestNetworkPayload>(),
        TestNetworkPayload {
            left: 0xaa,
            middle: 0xbb,
            right: 0xcc,
        }
    );
}
