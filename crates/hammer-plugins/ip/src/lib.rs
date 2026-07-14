//! Dynamic `ip` plugin (`libhammer_plugin_ip`).

use hammer_core::plugin::PluginRegistration;

static LOAD_AFTER: &[&str] = &[];

static REGISTRATION: PluginRegistration = PluginRegistration {
    name: "ip",
    version: env!("CARGO_PKG_VERSION"),
    version_required: env!("CARGO_PKG_VERSION"),
    load_after: LOAD_AFTER,
};

#[unsafe(no_mangle)]
pub extern "C" fn hammer_plugin_registration() -> *const PluginRegistration {
    &REGISTRATION
}

pub fn registration() -> &'static PluginRegistration {
    &REGISTRATION
}

hammer_component_macros::declare_plugin!(name = "ip", load_after = []);

pub mod ip;
mod lookup;

pub use ip::{
    IcmpEchoRequestNext, IcmpEchoRequestNode, IcmpEchoRequestTrace, IcmpErrorNext, IcmpErrorNode,
    IcmpErrorSourceTable, IcmpErrorSourceTableHandle, IcmpErrorTrace, IcmpInputControlPlane,
    IcmpInputError, IcmpInputNode, IcmpInputTrace, IcmpNodeError, IcmpPathMtuNode, IpInputNext,
    IpInputNode, IpInputTrace, IpLocalArc, IpLocalControlPlane, IpLocalError, IpLocalNext,
    IpLocalNode, IpLocalSourceCheck, IpLocalTrace, IpLocalTraceStage, IpReassemblyDirectory,
    IpReassemblyExpireWalk, IpReassemblyHandoff, IpReassemblyNext, IpReassemblyNode,
    IpReassemblyTrace, IpReassemblyTraceAction, IpReceiveNode, IpUnicastArc,
    pack_fragment_owner_value, unpack_fragment_owner_value,
};
pub use lookup::{
    AdjacencyRewriteNode, AdjacencyRewriteNodeError, AdjacencyRewriteTrace, IpLookupControlPlane,
    IpLookupNode, IpLookupTrace,
};
pub fn reset_ip_main_for_test() {
    lookup::reset_for_test();
    hammer_service::net::pmtu::reset_path_mtu_cache_for_test();
}
