#[path = "input.rs"]
pub mod input;
#[path = "local.rs"]
pub mod local;
#[path = "reassembly.rs"]
pub mod reassembly;

#[derive(Debug, Clone, Copy, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum IpRoutePathBehavior {
    Normal = 0,
    Local = 1,
    Drop = 2,
    UdpEncap = 3,
    IcmpUnreachable = 4,
    IcmpProhibit = 5,
    SourceLookup = 6,
    Dvr = 7,
    InterfaceRx = 8,
    Classify = 9,
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    #[repr(transparent)]
    pub struct IpPathFlags: u32 {
        const RESOLVE_VIA_HOST = 1 << 0;
        const RESOLVE_VIA_ATTACHED = 1 << 1;
        const LOCAL = 1 << 2;
        const ATTACHED = 1 << 3;
        const DROP = 1 << 4;
        const EXCLUSIVE = 1 << 5;
        const INTF_RX = 1 << 6;
        const RPF_ID = 1 << 7;
        const SOURCE_LOOKUP = 1 << 8;
        const UDP_ENCAP = 1 << 9;
        const DEAG = 1 << 13;
        const DVR = 1 << 14;
        const ICMP_UNREACH = 1 << 15;
        const ICMP_PROHIBIT = 1 << 16;
        const CLASSIFY = 1 << 17;
        const GLEAN = 1 << 19;
    }
}

use crate::protocol::ip::{IpFragmentKey, IpInputError, IpInputTarget, IpProtocol, IpVersion};

/// Runtime registries owned by the IP plugin. Mirrors VPP's per-node error
/// enumeration style: the registry identity is a typed discriminant, not a
/// string payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IpRuntimeRegistry {
    IpInput,
    IpLocal,
}

impl std::fmt::Display for IpRuntimeRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::IpInput => "ip-input",
            Self::IpLocal => "ip-local",
        })
    }
}

/// Control-plane operations that require IP plugin runtime state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IpControlOperation {
    IpProtocolRegistration,
}

impl std::fmt::Display for IpControlOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::IpProtocolRegistration => "ip protocol registration",
        })
    }
}

/// Recoverable control-plane failures shared by IP graph-node registration,
/// worker sync, and per-node runtime registry access.
#[hammer_component_macros::runtime_error(subsystem = "ip")]
#[derive(Debug, thiserror::Error)]
pub(crate) enum IpControlError {
    #[error("{registry} runtime registry is poisoned")]
    RuntimeRegistryPoisoned { registry: IpRuntimeRegistry },
    #[error("{registry} runtime slot {slot} is not registered")]
    RuntimeSlotInvalid {
        registry: IpRuntimeRegistry,
        slot: usize,
    },
    #[error("{operation} requires a node runtime")]
    NodeRuntimeUnavailable { operation: IpControlOperation },
}

pub use input::{Ip4InputNext, Ip4InputNode, Ip6InputNext, Ip6InputNode, IpInputTrace};
pub use local::{
    Ip4LocalNext, Ip4LocalNode, Ip4ReceiveNode, Ip6LocalNext, Ip6LocalNode, Ip6ReceiveNode,
    IpLocalError, IpLocalTrace, IpLocalTraceStage,
};
pub use reassembly::{
    Ip4ReassemblyNext, Ip4ReassemblyNode, Ip6ReassemblyNext, Ip6ReassemblyNode,
    IpReassemblyDirectory, IpReassemblyHandoff, IpReassemblyTrace, IpReassemblyTraceAction,
    pack_fragment_owner_value, unpack_fragment_owner_value,
};
