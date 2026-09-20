use std::mem::size_of;

use hammer_core::data_plane::NodeId;
use hammer_runtime::DataPlaneMain;

use crate::interface::{InterfaceMain, InterfaceMtuKind};

pub const REWRITE_HAS_FEATURES: u8 = 1 << 0;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct RewriteHeader {
    pub sw_if_index: u32,
    pub next_index: u16,
    pub data_bytes: u16,
    pub max_l3_packet_bytes: u16,
    pub flags: u8,
    pub dst_mcast_offset: u8,
}

impl RewriteHeader {
    pub const fn poisoned() -> Self {
        Self {
            sw_if_index: u32::MAX,
            next_index: 0xfefe,
            data_bytes: 0xfefe,
            max_l3_packet_bytes: 0xfefe,
            flags: 0,
            dst_mcast_offset: 0xfe,
        }
    }

    pub fn init(
        &mut self,
        main: &mut DataPlaneMain,
        sw_if_index: u32,
        mtu_kind: InterfaceMtuKind,
        source_node: NodeId,
        target_node: NodeId,
    ) {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("rewrite initialization requires publication ownership");
        let interfaces = super::NetMain::global()
            .expect("rewrite initialization requires the network Main")
            .interface_main();
        let interface = interfaces
            .software_interface(sw_if_index)
            .expect("rewrite interface must remain live during publication");
        let mtu = interface.mtu.get(mtu_kind).min(u32::from(u16::MAX)) as u16;
        let next_index = main
            .nodes()
            .add_node_next_slot(source_node, target_node)
            .expect("rewrite target must be a registered graph node");
        self.sw_if_index = sw_if_index;
        self.next_index = next_index;
        self.max_l3_packet_bytes = mtu;
    }

    pub fn set_data(&mut self, storage: &mut [u8; 116], bytes: &[u8]) {
        assert!(
            bytes.len() <= storage.len(),
            "adjacency rewrite exceeds 116 bytes"
        );
        storage[..bytes.len()].copy_from_slice(bytes);
        storage[bytes.len()..].fill(0xfe);
        self.data_bytes = bytes.len() as u16;
    }

    pub fn clear_data(&mut self, storage: &mut [u8; 116]) {
        storage.fill(0xfe);
        self.data_bytes = 0;
    }

    pub fn update_mtu(&mut self, interfaces: &InterfaceMain, mtu_kind: InterfaceMtuKind) {
        self.max_l3_packet_bytes = interfaces
            .software_interface(self.sw_if_index)
            .expect("rewrite interface must remain live during MTU publication")
            .mtu
            .get(mtu_kind)
            .min(u32::from(u16::MAX)) as u16;
    }
}

const _: () = assert!(size_of::<RewriteHeader>() == 12);
