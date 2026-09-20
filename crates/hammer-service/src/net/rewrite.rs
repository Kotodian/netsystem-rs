use std::mem::{offset_of, size_of};

use hammer_core::data_plane::NodeId;
use hammer_runtime::DataPlaneMain;

use super::{DpoId, DpoProto, NetMain};

#[repr(C)]
#[derive(Debug, Clone)]
pub struct AdjacencyRewrite {
    pub sw_if_index: u32,
    pub next_index: u16,
    pub data_bytes: u16,
    pub max_l3_packet_bytes: u16,
    pub flags: u8,
    pub dst_mcast_offset: u8,
    pub data: [u8; 116],
}

impl AdjacencyRewrite {
    pub fn new(sw_if_index: u32, mtu: u16) -> Self {
        Self {
            sw_if_index,
            next_index: 0,
            data_bytes: 0,
            max_l3_packet_bytes: mtu,
            flags: 0,
            dst_mcast_offset: 0,
            data: [0xfe; 116],
        }
    }

    pub fn init(&mut self, main: &mut DataPlaneMain, adjacency_node: NodeId, proto: DpoProto) {
        let net = NetMain::global().expect("rewrite requires initialized network owner");
        let output = DpoId::interface_tx(proto, self.sw_if_index);
        self.next_index = net
            .dpo_main_mut()
            .stack_from_node(main, adjacency_node, output)
            .expect("interface TX node must be registered before adjacency rewrite")
            .next();
    }

    pub fn set_data(&mut self, bytes: &[u8]) {
        assert!(
            bytes.len() <= self.data.len(),
            "adjacency rewrite exceeds 116 bytes"
        );
        self.data[..bytes.len()].copy_from_slice(bytes);
        self.data[bytes.len()..].fill(0xfe);
        self.data_bytes = bytes.len() as u16;
    }

    pub fn clear_data(&mut self) {
        self.data.fill(0xfe);
        self.data_bytes = 0;
    }

    pub fn update_mtu(&mut self, mtu: u16) {
        self.max_l3_packet_bytes = mtu;
    }
}

const _: () = assert!(size_of::<AdjacencyRewrite>() == 128);
const _: () = assert!(offset_of!(AdjacencyRewrite, data) == 12);
