use std::cell::UnsafeCell;
use std::sync::OnceLock;

use hammer_core::data_plane::{Frame, NodeId, NodeNext};
use hammer_infra::sparse_vec::SparseVec;
use hammer_runtime::node::{NodeErrorCode, NodeErrorDescriptor, NodeErrorSeverity};
use hammer_runtime::{DataPlaneMain, Node, NodeProcessFn, RuntimeError, RuntimeResult};
use zerocopy::FromBytes;

use crate::device::DeviceInputNode;
use crate::opaque::NetworkOpaque;

const ETHERNET_HEADER_LEN: usize = 14;
const ETHERNET_VLAN_HEADER_LEN: usize = 4;
const ETHERNET_TYPE_LENGTH_BOUNDARY: u16 = 0x0600;
const ETHERNET_TYPE_VLAN: u16 = 0x8100;
const ETHERNET_TYPE_DOT1AD: u16 = 0x88a8;
const ETHERNET_TYPE_VLAN_9100: u16 = 0x9100;
const ETHERNET_TYPE_VLAN_9200: u16 = 0x9200;

#[derive(zerocopy::FromBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
#[allow(dead_code)]
#[repr(C)]
struct EthernetHeader {
    destination: [u8; 6],
    source: [u8; 6],
    ether_type: [u8; 2],
}

#[derive(zerocopy::FromBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
#[allow(dead_code)]
#[repr(C)]
struct EthernetVlanHeader {
    priority_cfi_and_id: [u8; 2],
    ether_type: [u8; 2],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum EthernetInputError {
    HeaderTooShort,
    UnknownType,
    UnknownVlan,
}

impl NodeErrorCode for EthernetInputError {
    #[inline(always)]
    fn local_code(self) -> u16 {
        self as u16
    }
}

impl EthernetInputError {
    const DESCRIPTORS: [NodeErrorDescriptor; 3] = [
        NodeErrorDescriptor::new(
            "header-too-short",
            NodeErrorSeverity::Error,
            "Ethernet header is too short",
        ),
        NodeErrorDescriptor::new(
            "unknown-type",
            NodeErrorSeverity::Warn,
            "Unknown Ethernet type",
        ),
        NodeErrorDescriptor::new("unknown-vlan", NodeErrorSeverity::Error, "Unknown VLAN"),
    ];
}

#[hammer_component_macros::node_next]
pub enum EthernetInputNext {
    #[next("punt")]
    Punt,
    #[next("drop")]
    Drop,
}

#[hammer_component_macros::feature(arc = DeviceInputNode)]
#[hammer_component_macros::graph_node(
    graph = service,
    init = register_ethernet_input,
    role = internal,
    name = "ethernet-input",
    next = EthernetInputNext
)]
pub struct EthernetInputNode;

fn register_ethernet_input(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime.nodes().try_register_internal_with_next_names(
        EthernetInputNode::new(),
        &EthernetInputNext::NEXT_NAMES,
    )?;
    runtime.register_node_errors(node, &EthernetInputError::DESCRIPTORS)?;
    Ok(node)
}

impl Node for EthernetInputNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = |runtime, node_runtime, frame| {
            let processed = frame.len();
            let ethernet =
                EthernetMain::global().expect("EthernetMain exists before ethernet-input executes");
            hammer_runtime::process_frame!(runtime, node_runtime, frame, |index| {
                next_for_index(runtime, index, ethernet)
            });
            processed
        };
        process(runtime, node_runtime, frame)
    }
}

pub struct EthernetMain {
    next_by_ethertype: UnsafeCell<SparseVec<u16>>,
}

// SAFETY: startup callbacks mutate the table on the main thread before Data
// Workers start. Packet processing only reads it after startup publication.
unsafe impl Send for EthernetMain {}
// SAFETY: no borrow from the UnsafeCell escapes an owner operation, and this
// ADR does not permit registration after Data Workers start.
unsafe impl Sync for EthernetMain {}

static ETHERNET_MAIN: OnceLock<EthernetMain> = OnceLock::new();

impl EthernetMain {
    fn new() -> Self {
        Self {
            next_by_ethertype: UnsafeCell::new(SparseVec::with_index_bits(u16::BITS as u8)),
        }
    }

    pub fn init() -> RuntimeResult<()> {
        assert!(
            ETHERNET_MAIN.set(Self::new()).is_ok(),
            "Ethernet initialization callback executes once"
        );
        Ok(())
    }

    pub fn global() -> RuntimeResult<&'static Self> {
        ETHERNET_MAIN
            .get()
            .ok_or(RuntimeError::RuntimeCapabilityMissing {
                type_name: "hammer_service::ethernet::EthernetMain",
            })
    }

    pub fn register_input_type(
        &self,
        nodes: &hammer_runtime::NodeMain,
        ether_type: u16,
        consumer: NodeId,
    ) -> RuntimeResult<u16> {
        hammer_runtime::ensure_main_thread()
            .expect("EtherType registration belongs to the main thread");
        assert!(
            !hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0),
            "EtherType registration precedes Data Worker startup"
        );
        let input = nodes
            .node_by_name(EthernetInputNode::NODE_NAME)
            .expect("ethernet-input exists before EtherType registration");
        let slot = nodes.add_node_next_slot(input, consumer)?;
        // SAFETY: registration is serialized on the main thread before Data
        // Workers start. SparseVec replacement is infallible for a u16 key.
        unsafe {
            (&mut *self.next_by_ethertype.get()).insert(usize::from(ether_type), slot);
        }
        Ok(slot)
    }

    #[inline(always)]
    fn next_for_ethertype(&self, ether_type: u16) -> Option<u16> {
        // SAFETY: the table is immutable after startup publication.
        unsafe {
            (&*self.next_by_ethertype.get())
                .get(usize::from(ether_type))
                .copied()
        }
    }
}

#[hammer_component_macros::init_function(
    name = "ethernet_main_init",
    runs_after = ["net_main_init"]
)]
fn ethernet_main_init() -> RuntimeResult<()> {
    EthernetMain::init()
}

#[inline(always)]
fn next_for_index(runtime: &mut DataPlaneMain, index: u32, ethernet: &EthernetMain) -> u16 {
    let parsed = parse_header(runtime.buffer(index).current());
    let (next, error, header_len) = match parsed {
        Err(error) => (NodeNext::slot(EthernetInputNext::Drop), Some(error), None),
        Ok((ether_type, header_len)) if ether_type < ETHERNET_TYPE_LENGTH_BOUNDARY => (
            NodeNext::slot(EthernetInputNext::Punt),
            Some(EthernetInputError::UnknownType),
            Some(header_len),
        ),
        Ok((ether_type, header_len)) => match ethernet.next_for_ethertype(ether_type) {
            Some(next) => (next, None, Some(header_len)),
            None => (
                NodeNext::slot(EthernetInputNext::Punt),
                Some(EthernetInputError::UnknownType),
                Some(header_len),
            ),
        },
    };
    let error_index = error.map(|error| {
        runtime
            .record_current_node_error(error)
            .expect("ethernet-input errors are registered before dispatch")
    });
    let buffer = runtime.buffer_mut(index);
    if let Some(header_len) = header_len {
        buffer.advance(isize::from(header_len));
        hammer_core::buffer_opaque!(mut buffer => NetworkOpaque).l3_hdr_offset = 0;
    }
    if let Some(error_index) = error_index {
        buffer.set_node_error_index(error_index);
    } else {
        buffer.clear_node_error();
    }
    next
}

#[inline(always)]
fn parse_header(bytes: &[u8]) -> Result<(u16, u8), EthernetInputError> {
    let (header, _) =
        EthernetHeader::ref_from_prefix(bytes).map_err(|_| EthernetInputError::HeaderTooShort)?;
    let mut ether_type = u16::from_be_bytes(header.ether_type);
    let mut header_len = ETHERNET_HEADER_LEN;
    if !is_vlan(ether_type) {
        return Ok((ether_type, header_len as u8));
    }

    let (vlan, _) = EthernetVlanHeader::ref_from_prefix(&bytes[header_len..])
        .map_err(|_| EthernetInputError::HeaderTooShort)?;
    ether_type = u16::from_be_bytes(vlan.ether_type);
    header_len += ETHERNET_VLAN_HEADER_LEN;
    if ether_type != ETHERNET_TYPE_VLAN {
        return Ok((ether_type, header_len as u8));
    }

    let (vlan, _) = EthernetVlanHeader::ref_from_prefix(&bytes[header_len..])
        .map_err(|_| EthernetInputError::HeaderTooShort)?;
    ether_type = u16::from_be_bytes(vlan.ether_type);
    header_len += ETHERNET_VLAN_HEADER_LEN;
    if ether_type == ETHERNET_TYPE_VLAN {
        return Err(EthernetInputError::UnknownVlan);
    }
    Ok((ether_type, header_len as u8))
}

#[inline(always)]
fn is_vlan(ether_type: u16) -> bool {
    matches!(
        ether_type,
        ETHERNET_TYPE_VLAN
            | ETHERNET_TYPE_DOT1AD
            | ETHERNET_TYPE_VLAN_9100
            | ETHERNET_TYPE_VLAN_9200
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_untagged_and_vlan_headers_without_copy() {
        let untagged = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 0x08, 0x00];
        assert_eq!(parse_header(&untagged), Ok((0x0800, 14)));

        let single = [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 0x81, 0x00, 0, 1, 0x86, 0xdd,
        ];
        assert_eq!(parse_header(&single), Ok((0x86dd, 18)));

        let double = [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 0x88, 0xa8, 0, 1, 0x81, 0x00, 0, 2, 0x08, 0x00,
        ];
        assert_eq!(parse_header(&double), Ok((0x0800, 22)));
    }

    #[test]
    fn rejects_truncated_and_deep_vlan_headers() {
        let base = [0u8; ETHERNET_HEADER_LEN - 1];
        assert_eq!(parse_header(&base), Err(EthernetInputError::HeaderTooShort));

        let single = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 0x81, 0x00, 0, 1, 0x81];
        assert_eq!(
            parse_header(&single),
            Err(EthernetInputError::HeaderTooShort)
        );

        let deep = [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 0x88, 0xa8, 0, 1, 0x81, 0x00, 0, 2, 0x81, 0x00,
        ];
        assert_eq!(parse_header(&deep), Err(EthernetInputError::UnknownVlan));
    }
}
