use super::DpoType;

/// Per-packet IP forwarding facts written by lookup and consumed by IP
/// adjacency rewrite. This is IP-plugin-owned secondary opaque state.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForwardingMetadata {
    pub fib_index: u32,
    pub route_dpo_type: DpoType,
    pub route_dpo_index: u32,
    pub load_balance_index: u32,
    pub bucket_index: u16,
    pub dpo_type: DpoType,
    pub dpo_index: u32,
}
