//! On-mapped directory entries, mirroring `vlib_stats_entry_t`.
//!
//! VPP's entry packs a type tag, a union (index/value/data) and a 128-byte
//! name. Hammer splits the union into explicit link and offset fields,
//! records the generation that makes `EntryId` reuse-safe, and keeps the
//! VPP directory type alongside the Prometheus metric kind. No Rust enums
//! live in mapped bytes: states and types are raw `u8` values converted via
//! checked `TryFrom`.

use crate::error::StatsError;
use crate::offset::Offset;

/// Size of one directory slot in bytes.
pub(crate) const SLOT_SIZE: usize = 256;
/// Maximum directory name length including the NUL terminator.
pub(crate) const ENTRY_NAME_LEN: usize = 128;
/// Sentinel for an empty free-list head.
pub(crate) const NULL_INDEX: u64 = u64::MAX;

/// Slot lifecycle states.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum EntryState {
    /// Free slot, chained on the free list.
    Free,
    /// Live metric, indexed by `EntryId`.
    Active,
    /// Hidden slot state reserved for mapped-state validation.
    Removed,
}

impl EntryState {
    pub(crate) const FREE: u8 = 0;
    pub(crate) const ACTIVE: u8 = 1;
    pub(crate) const REMOVED: u8 = 2;

    pub(crate) fn as_u8(self) -> u8 {
        match self {
            EntryState::Free => EntryState::FREE,
            EntryState::Active => EntryState::ACTIVE,
            EntryState::Removed => EntryState::REMOVED,
        }
    }
}

impl TryFrom<u8> for EntryState {
    type Error = StatsError;

    fn try_from(value: u8) -> Result<EntryState, StatsError> {
        match value {
            EntryState::FREE => Ok(EntryState::Free),
            EntryState::ACTIVE => Ok(EntryState::Active),
            EntryState::REMOVED => Ok(EntryState::Removed),
            other => Err(StatsError::InvalidState(other)),
        }
    }
}

/// Directory types mirrored from VPP's `stat_directory_type_t`
/// (shared.h:8-20).
///
/// These byte discriminants are stored in the shared segment and are part
/// of the mapped format: they must never change. `TryFrom<u8>` accepts
/// every real discriminant so a reader can decode any entry a peer may
/// have published; unknown bytes are rejected.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DirectoryType {
    /// `STAT_DIR_TYPE_ILLEGAL`; never valid on a live entry.
    Illegal,
    /// `STAT_DIR_TYPE_SCALAR_INDEX`; a single integer value, as VPP's `/sys`
    /// heartbeat, boottime, and last-stats-clear metrics (stats.h:29-31).
    ScalarIndex,
    /// `STAT_DIR_TYPE_COUNTER_VECTOR_SIMPLE`.
    CounterVectorSimple,
    /// `STAT_DIR_TYPE_COUNTER_VECTOR_COMBINED`.
    CounterVectorCombined,
    /// `STAT_DIR_TYPE_NAME_VECTOR`.
    NameVector,
    /// `STAT_DIR_TYPE_EMPTY`; a removed slot.
    Empty,
    /// `STAT_DIR_TYPE_SYMLINK`.
    Symlink,
    /// `STAT_DIR_TYPE_HISTOGRAM_LOG2`.
    HistogramLog2,
    /// `STAT_DIR_TYPE_RING_BUFFER`.
    RingBuffer,
    /// `STAT_DIR_TYPE_GAUGE`; a single `f64` gauge value.
    Gauge,
}

impl DirectoryType {
    pub(crate) const ILLEGAL: u8 = 0;
    pub(crate) const SCALAR_INDEX: u8 = 1;
    pub(crate) const COUNTER_VECTOR_SIMPLE: u8 = 2;
    pub(crate) const COUNTER_VECTOR_COMBINED: u8 = 3;
    pub(crate) const NAME_VECTOR: u8 = 4;
    pub(crate) const EMPTY: u8 = 5;
    pub(crate) const SYMLINK: u8 = 6;
    pub(crate) const HISTOGRAM_LOG2: u8 = 7;
    pub(crate) const RING_BUFFER: u8 = 8;
    pub(crate) const GAUGE: u8 = 9;

    pub(crate) fn as_u8(self) -> u8 {
        match self {
            DirectoryType::Illegal => DirectoryType::ILLEGAL,
            DirectoryType::ScalarIndex => DirectoryType::SCALAR_INDEX,
            DirectoryType::CounterVectorSimple => DirectoryType::COUNTER_VECTOR_SIMPLE,
            DirectoryType::CounterVectorCombined => DirectoryType::COUNTER_VECTOR_COMBINED,
            DirectoryType::NameVector => DirectoryType::NAME_VECTOR,
            DirectoryType::Empty => DirectoryType::EMPTY,
            DirectoryType::Symlink => DirectoryType::SYMLINK,
            DirectoryType::HistogramLog2 => DirectoryType::HISTOGRAM_LOG2,
            DirectoryType::RingBuffer => DirectoryType::RING_BUFFER,
            DirectoryType::Gauge => DirectoryType::GAUGE,
        }
    }
}

impl TryFrom<u8> for DirectoryType {
    type Error = StatsError;

    fn try_from(value: u8) -> Result<DirectoryType, StatsError> {
        match value {
            DirectoryType::ILLEGAL => Ok(DirectoryType::Illegal),
            DirectoryType::SCALAR_INDEX => Ok(DirectoryType::ScalarIndex),
            DirectoryType::COUNTER_VECTOR_SIMPLE => Ok(DirectoryType::CounterVectorSimple),
            DirectoryType::COUNTER_VECTOR_COMBINED => Ok(DirectoryType::CounterVectorCombined),
            DirectoryType::NAME_VECTOR => Ok(DirectoryType::NameVector),
            DirectoryType::EMPTY => Ok(DirectoryType::Empty),
            DirectoryType::SYMLINK => Ok(DirectoryType::Symlink),
            DirectoryType::HISTOGRAM_LOG2 => Ok(DirectoryType::HistogramLog2),
            DirectoryType::RING_BUFFER => Ok(DirectoryType::RingBuffer),
            DirectoryType::GAUGE => Ok(DirectoryType::Gauge),
            other => Err(StatsError::InvalidState(other)),
        }
    }
}

/// Prometheus metric kinds recorded in the entry.
///
/// Same stable-discriminant rule as [`DirectoryType`]: these bytes are part
/// of the mapped format.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PrometheusType {
    Counter,
    Gauge,
}

impl PrometheusType {
    pub(crate) const COUNTER: u8 = 1;
    pub(crate) const GAUGE: u8 = 2;

    pub(crate) fn as_u8(self) -> u8 {
        match self {
            PrometheusType::Counter => PrometheusType::COUNTER,
            PrometheusType::Gauge => PrometheusType::GAUGE,
        }
    }
}

impl TryFrom<u8> for PrometheusType {
    type Error = StatsError;

    fn try_from(value: u8) -> Result<PrometheusType, StatsError> {
        match value {
            PrometheusType::COUNTER => Ok(PrometheusType::Counter),
            PrometheusType::GAUGE => Ok(PrometheusType::Gauge),
            other => Err(StatsError::InvalidState(other)),
        }
    }
}

/// Encodes `name` as a NUL-terminated 128-byte directory name field.
pub(crate) fn encode_name(name: &str) -> Result<[u8; ENTRY_NAME_LEN], StatsError> {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() >= ENTRY_NAME_LEN || bytes.contains(&0) {
        return Err(StatsError::InvalidPath(name.to_owned()));
    }
    let mut out = [0u8; ENTRY_NAME_LEN];
    out[..bytes.len()].copy_from_slice(bytes);
    Ok(out)
}

/// One on-mapped directory slot. Kept 64-byte-aligned; a fixed multiple of
/// 64 bytes.
#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct DirectorySlot {
    /// NUL-terminated metric path, as in `vlib_stats_entry_t.name`.
    name: [u8; ENTRY_NAME_LEN],
    /// Slot generation; advanced once at removal, published as-is on
    /// free-list reuse.
    generation: u64,
    /// Raw `EntryState` byte.
    state: u8,
    /// Raw `DirectoryType` byte.
    vpp_type: u8,
    /// Raw `PrometheusType` byte.
    prometheus_type: u8,
    reserved: [u8; 5],
    /// Next slot index on the free list (`NULL_INDEX` tail).
    link: u64,
    /// Mapping-relative offset of the metric block (never zero).
    descriptor_offset: u64,
    /// Mapping-relative offset of the cache-line-aligned value (never zero).
    value_offset: u64,
    reserved_words: [u64; 11],
}

const _: () = assert!(std::mem::size_of::<DirectorySlot>() == 256);

impl DirectorySlot {
    pub(crate) fn new_active(
        name: [u8; ENTRY_NAME_LEN],
        generation: u64,
        directory_type: DirectoryType,
        prometheus_type: PrometheusType,
        descriptor_offset: Offset,
        value_offset: Offset,
    ) -> DirectorySlot {
        DirectorySlot {
            name,
            generation,
            state: EntryState::ACTIVE,
            vpp_type: directory_type.as_u8(),
            prometheus_type: prometheus_type.as_u8(),
            reserved: [0; 5],
            link: NULL_INDEX,
            descriptor_offset: descriptor_offset.get(),
            value_offset: value_offset.get(),
            reserved_words: [0; 11],
        }
    }

    pub(crate) fn new_symlink(
        name: [u8; ENTRY_NAME_LEN],
        generation: u64,
        prometheus_type: PrometheusType,
        descriptor_offset: Offset,
        value_offset: Offset,
        target_index: u32,
        target_generation: u64,
        vector_index: u32,
    ) -> DirectorySlot {
        let mut slot = Self::new_active(
            name,
            generation,
            DirectoryType::Symlink,
            prometheus_type,
            descriptor_offset,
            value_offset,
        );
        slot.set_symlink_target(target_index, target_generation, vector_index);
        slot
    }

    pub(crate) fn set_symlink_target(
        &mut self,
        target_index: u32,
        target_generation: u64,
        vector_index: u32,
    ) {
        self.reserved_words[0] = u64::from(target_index);
        self.reserved_words[1] = target_generation;
        self.reserved_words[2] = u64::from(vector_index);
    }

    pub(crate) fn symlink_target_index(&self) -> Result<u32, StatsError> {
        u32::try_from(self.reserved_words[0]).map_err(|_| StatsError::OutOfBounds)
    }

    pub(crate) fn symlink_target_generation(&self) -> u64 {
        self.reserved_words[1]
    }

    pub(crate) fn symlink_vector_index(&self) -> Result<u32, StatsError> {
        u32::try_from(self.reserved_words[2]).map_err(|_| StatsError::OutOfBounds)
    }

    /// The name field up to (excluding) its NUL terminator.
    pub(crate) fn name(&self) -> &[u8] {
        let end = self
            .name
            .iter()
            .position(|&byte| byte == 0)
            .unwrap_or(ENTRY_NAME_LEN);
        &self.name[..end]
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn state(&self) -> Result<EntryState, StatsError> {
        EntryState::try_from(self.state)
    }

    /// The raw state byte, for corruption diagnostics.
    pub(crate) fn state_byte(&self) -> u8 {
        self.state
    }

    /// The directory type, decoded from the raw byte.
    pub(crate) fn directory_type(&self) -> Result<DirectoryType, StatsError> {
        DirectoryType::try_from(self.vpp_type)
    }

    /// The Prometheus metric kind, decoded from the raw byte.
    pub(crate) fn prometheus_type(&self) -> Result<PrometheusType, StatsError> {
        PrometheusType::try_from(self.prometheus_type)
    }

    pub(crate) fn link(&self) -> u64 {
        self.link
    }

    pub(crate) fn set_link(&mut self, link: u64) {
        self.link = link;
    }

    pub(crate) fn set_state(&mut self, state: EntryState) {
        self.state = state.as_u8();
    }

    /// Publishes the advanced generation computed at removal.
    pub(crate) fn set_generation(&mut self, generation: u64) {
        self.generation = generation;
    }

    /// Mapping-relative offset of the metric block, or the null offset.
    pub(crate) fn descriptor_offset(&self) -> Offset {
        Offset::new(self.descriptor_offset)
    }

    /// Mapping-relative offset of the value record, or the null offset.
    pub(crate) fn value_offset(&self) -> Offset {
        Offset::new(self.value_offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every `stat_directory_type_t` discriminant from the vendored
    /// `shared.h:8-20` decodes to exactly its variant; unknown bytes are
    /// rejected.
    #[test]
    fn directory_type_decodes_vendored_discriminants() {
        let vendored = [
            (0, DirectoryType::Illegal),
            (1, DirectoryType::ScalarIndex),
            (2, DirectoryType::CounterVectorSimple),
            (3, DirectoryType::CounterVectorCombined),
            (4, DirectoryType::NameVector),
            (5, DirectoryType::Empty),
            (6, DirectoryType::Symlink),
            (7, DirectoryType::HistogramLog2),
            (8, DirectoryType::RingBuffer),
            (9, DirectoryType::Gauge),
        ];
        for (byte, expected) in vendored {
            assert_eq!(DirectoryType::try_from(byte).ok(), Some(expected));
            assert_eq!(expected.as_u8(), byte);
        }
        for byte in [10, 0x7F, 0xFF] {
            assert!(
                matches!(
                    DirectoryType::try_from(byte),
                    Err(StatsError::InvalidState(_))
                ),
                "byte {byte} must be rejected"
            );
        }
    }

    #[test]
    fn encode_name_rejects_embedded_nul() {
        let name = "/if\0/rx";
        assert!(matches!(
            encode_name(name),
            Err(StatsError::InvalidPath(path)) if path == name
        ));
    }
}
