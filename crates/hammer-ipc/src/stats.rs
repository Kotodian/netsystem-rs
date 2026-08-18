//! Typed, checked client domain for the stats Binary API methods.
//!
//! This crate owns the protocol DTOs and the checked client result types; it
//! has no dependency on `hammer-stats` or any stats implementation, so the
//! CLI transport stays independent of Prometheus, shared-memory/Talc, and the
//! data-plane stats writer. The daemon (`hammer-service`) later converts
//! between `hammer-stats` and this wire at its owner boundary.
//!
//! The wire protocol mirrors the VPP stats client read semantics
//! (`third_party/vpp/src/vpp-api/client/stat_client.c`): `stats.list`
//! (`stat_segment_ls_r`) matches patterns and returns entry descriptions;
//! `stats.dump` (`stat_segment_dump_r`) reads the values of the requested
//! entries in the given order, preserving duplicates. Domain errors are
//! carried in-band under `BinaryApiStatus::Ok` because the existing
//! `BinaryApiClient` discards non-`Ok` payloads and the runtime's non-`Ok`
//! reply constructors carry no payload; a malformed request remains a
//! transport-level `InvalidRequest`.

use std::path::Path;

use prost::Message;

use crate::binary_api::{BinaryApiClient, BinaryApiError};

/// Binary API method that lists stats directory entries matching patterns.
pub const STATS_LIST_METHOD: &str = "stats.list";
/// Binary API method that reads values of requested stats directory entries.
pub const STATS_DUMP_METHOD: &str = "stats.dump";

/// Protobuf wire messages for the stats methods. Kept public so the daemon
/// side (`hammer-service`) can encode handlers later; the checked domain
/// types live at the root of this module.
pub mod wire {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct EntryId {
        #[prost(uint32, tag = "1")]
        pub index: u32,
        #[prost(uint64, tag = "2")]
        pub generation: u64,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ListRequest {
        #[prost(string, repeated, tag = "1")]
        pub patterns: Vec<String>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ConstLabel {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(string, tag = "2")]
        pub value: String,
    }

    /// Directory entry kinds, with the exact stable discriminants from the
    /// vendored VPP `stat_directory_type_t` (`third_party/vpp/src/vlib/stats/shared.h`).
    #[derive(Clone, Copy, Debug, PartialEq, Eq, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum DirectoryType {
        Illegal = 0,
        ScalarIndex = 1,
        CounterVectorSimple = 2,
        CounterVectorCombined = 3,
        NameVector = 4,
        Empty = 5,
        Symlink = 6,
        HistogramLog2 = 7,
        RingBuffer = 8,
        Gauge = 9,
    }

    /// Prometheus representation of an entry. `Unspecified` is the protobuf
    /// zero; the checked root conversion rejects it and every unknown value.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum PrometheusType {
        Unspecified = 0,
        Counter = 1,
        Gauge = 2,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ListEntry {
        #[prost(message, optional, tag = "1")]
        pub id: Option<EntryId>,
        #[prost(string, tag = "2")]
        pub path: String,
        #[prost(enumeration = "DirectoryType", tag = "3")]
        pub directory_type: i32,
        #[prost(enumeration = "PrometheusType", tag = "4")]
        pub prometheus_type: i32,
        #[prost(string, tag = "5")]
        pub fq_name: String,
        #[prost(string, tag = "6")]
        pub help: String,
        #[prost(message, repeated, tag = "7")]
        pub const_labels: Vec<ConstLabel>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ListEntries {
        #[prost(message, repeated, tag = "1")]
        pub entries: Vec<ListEntry>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DumpRequest {
        #[prost(message, repeated, tag = "1")]
        pub ids: Vec<EntryId>,
    }

    /// One value read by `stats.dump`. A message wrapper keeps the scalar
    /// counter/gauge tags stable while allowing the vector-shaped values to
    /// grow additively.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Value {
        #[prost(oneof = "value::Value", tags = "1, 2, 3, 4, 5, 6, 7")]
        pub value: Option<value::Value>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct CounterVectorSimple {
        #[prost(message, repeated, tag = "1")]
        pub rows: Vec<CounterVectorSimpleRow>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct CounterVectorSimpleRow {
        #[prost(uint64, repeated, tag = "1")]
        pub values: Vec<u64>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct CounterVectorCombined {
        #[prost(message, repeated, tag = "1")]
        pub rows: Vec<CounterVectorCombinedRow>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct CounterVectorCombinedRow {
        #[prost(message, repeated, tag = "1")]
        pub values: Vec<CounterVectorCombinedValue>,
    }

    #[derive(Clone, Copy, PartialEq, ::prost::Message)]
    pub struct CounterVectorCombinedValue {
        #[prost(uint64, tag = "1")]
        pub packets: u64,
        #[prost(uint64, tag = "2")]
        pub bytes: u64,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct NameVector {
        #[prost(message, repeated, tag = "1")]
        pub slots: Vec<NameVectorSlot>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct NameVectorSlot {
        #[prost(string, optional, tag = "1")]
        pub name: Option<String>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct HistogramLog2 {
        #[prost(message, repeated, tag = "1")]
        pub rows: Vec<HistogramLog2Row>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct HistogramLog2Row {
        #[prost(uint64, repeated, tag = "1")]
        pub bins: Vec<u64>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct RingBuffer {
        #[prost(message, repeated, tag = "1")]
        pub snapshots: Vec<RingBufferSnapshot>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct RingBufferSnapshot {
        #[prost(uint64, tag = "1")]
        pub sequence: u64,
        #[prost(bytes = "vec", repeated, tag = "2")]
        pub entries: Vec<Vec<u8>>,
    }

    pub mod value {
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum Value {
            #[prost(uint64, tag = "1")]
            Counter(u64),
            #[prost(double, tag = "2")]
            Gauge(f64),
            #[prost(message, tag = "3")]
            CounterVectorSimple(super::CounterVectorSimple),
            #[prost(message, tag = "4")]
            CounterVectorCombined(super::CounterVectorCombined),
            #[prost(message, tag = "5")]
            NameVector(super::NameVector),
            #[prost(message, tag = "6")]
            HistogramLog2(super::HistogramLog2),
            #[prost(message, tag = "7")]
            RingBuffer(super::RingBuffer),
        }
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DumpEntry {
        #[prost(message, optional, tag = "1")]
        pub id: Option<EntryId>,
        #[prost(string, tag = "2")]
        pub path: String,
        #[prost(enumeration = "DirectoryType", tag = "3")]
        pub directory_type: i32,
        #[prost(enumeration = "PrometheusType", tag = "4")]
        pub prometheus_type: i32,
        #[prost(message, optional, tag = "5")]
        pub value: Option<Value>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DumpEntries {
        #[prost(message, repeated, tag = "1")]
        pub entries: Vec<DumpEntry>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct InvalidPatternError {
        #[prost(string, tag = "1")]
        pub pattern: String,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct EntryError {
        #[prost(message, optional, tag = "1")]
        pub id: Option<EntryId>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct IncompatibleTypeError {
        #[prost(message, optional, tag = "1")]
        pub id: Option<EntryId>,
        #[prost(enumeration = "DirectoryType", tag = "2")]
        pub directory_type: i32,
        #[prost(enumeration = "PrometheusType", tag = "3")]
        pub prometheus_type: i32,
    }

    #[derive(Clone, Copy, PartialEq, ::prost::Message)]
    pub struct Empty {}

    /// Typed server-domain error carried in-band in list/dump replies.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ErrorReply {
        #[prost(oneof = "error_oneof::Error", tags = "1, 2, 3, 4, 5, 6")]
        pub error: Option<error_oneof::Error>,
    }

    pub mod error_oneof {
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum Error {
            #[prost(message, tag = "1")]
            InvalidPattern(super::InvalidPatternError),
            #[prost(message, tag = "2")]
            NotFound(super::EntryError),
            #[prost(message, tag = "3")]
            StaleEntry(super::EntryError),
            #[prost(message, tag = "4")]
            ReadBusy(super::Empty),
            #[prost(message, tag = "5")]
            IncompatibleType(super::IncompatibleTypeError),
            #[prost(message, tag = "6")]
            Internal(super::Empty),
        }
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ListReply {
        #[prost(oneof = "list_reply::Result", tags = "1, 2")]
        pub result: Option<list_reply::Result>,
    }

    pub mod list_reply {
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum Result {
            #[prost(message, tag = "1")]
            Entries(super::ListEntries),
            #[prost(message, tag = "2")]
            Error(super::ErrorReply),
        }
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DumpReply {
        #[prost(oneof = "dump_reply::Result", tags = "1, 2")]
        pub result: Option<dump_reply::Result>,
    }

    pub mod dump_reply {
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum Result {
            #[prost(message, tag = "1")]
            Entries(super::DumpEntries),
            #[prost(message, tag = "2")]
            Error(super::ErrorReply),
        }
    }
}

/// Checked stats directory entry identity. Generation 0 is invalid: the
/// daemon's lifecycle model starts appended slots at generation 1 and
/// advances the generation at every removal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EntryId {
    index: u32,
    generation: u64,
}

impl EntryId {
    pub const fn new(index: u32, generation: u64) -> Result<Self, StatsClientError> {
        if generation == 0 {
            return Err(StatsClientError::InvalidEntryIdGeneration { index });
        }
        Ok(Self { index, generation })
    }

    #[inline]
    pub const fn index(self) -> u32 {
        self.index
    }

    #[inline]
    pub const fn generation(self) -> u64 {
        self.generation
    }
}

impl TryFrom<wire::EntryId> for EntryId {
    type Error = StatsClientError;

    fn try_from(wire: wire::EntryId) -> Result<Self, Self::Error> {
        Self::new(wire.index, wire.generation)
    }
}

impl From<EntryId> for wire::EntryId {
    fn from(id: EntryId) -> Self {
        Self {
            index: id.index,
            generation: id.generation,
        }
    }
}

impl std::fmt::Display for EntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "index {} generation {}", self.index, self.generation)
    }
}

/// Checked directory entry kinds with the exact vendored VPP discriminants
/// (`third_party/vpp/src/vlib/stats/shared.h`). Unknown raw values are
/// rejected before any invalid Rust enum can be constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum DirectoryType {
    Illegal = 0,
    ScalarIndex = 1,
    CounterVectorSimple = 2,
    CounterVectorCombined = 3,
    NameVector = 4,
    Empty = 5,
    Symlink = 6,
    HistogramLog2 = 7,
    RingBuffer = 8,
    Gauge = 9,
}

/// Prometheus representation of a stats entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PrometheusType {
    Counter = 1,
    Gauge = 2,
}

impl TryFrom<i32> for DirectoryType {
    type Error = StatsClientError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Illegal),
            1 => Ok(Self::ScalarIndex),
            2 => Ok(Self::CounterVectorSimple),
            3 => Ok(Self::CounterVectorCombined),
            4 => Ok(Self::NameVector),
            5 => Ok(Self::Empty),
            6 => Ok(Self::Symlink),
            7 => Ok(Self::HistogramLog2),
            8 => Ok(Self::RingBuffer),
            9 => Ok(Self::Gauge),
            other => Err(StatsClientError::UnknownEnum {
                field: "stats.directory_type",
                value: other,
            }),
        }
    }
}

impl TryFrom<wire::DirectoryType> for DirectoryType {
    type Error = StatsClientError;

    fn try_from(value: wire::DirectoryType) -> Result<Self, Self::Error> {
        Self::try_from(value as i32)
    }
}

impl From<DirectoryType> for wire::DirectoryType {
    fn from(value: DirectoryType) -> Self {
        match value {
            DirectoryType::Illegal => Self::Illegal,
            DirectoryType::ScalarIndex => Self::ScalarIndex,
            DirectoryType::CounterVectorSimple => Self::CounterVectorSimple,
            DirectoryType::CounterVectorCombined => Self::CounterVectorCombined,
            DirectoryType::NameVector => Self::NameVector,
            DirectoryType::Empty => Self::Empty,
            DirectoryType::Symlink => Self::Symlink,
            DirectoryType::HistogramLog2 => Self::HistogramLog2,
            DirectoryType::RingBuffer => Self::RingBuffer,
            DirectoryType::Gauge => Self::Gauge,
        }
    }
}

impl TryFrom<i32> for PrometheusType {
    type Error = StatsClientError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Counter),
            2 => Ok(Self::Gauge),
            other => Err(StatsClientError::UnknownEnum {
                field: "stats.prometheus_type",
                value: other,
            }),
        }
    }
}

impl TryFrom<wire::PrometheusType> for PrometheusType {
    type Error = StatsClientError;

    fn try_from(value: wire::PrometheusType) -> Result<Self, Self::Error> {
        Self::try_from(value as i32)
    }
}

impl From<PrometheusType> for wire::PrometheusType {
    fn from(value: PrometheusType) -> Self {
        match value {
            PrometheusType::Counter => Self::Counter,
            PrometheusType::Gauge => Self::Gauge,
        }
    }
}

/// One constant label attached to a stats directory entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstLabel {
    pub name: String,
    pub value: String,
}

/// One stats directory entry description from `stats.list`.
#[derive(Debug, Clone, PartialEq)]
pub struct DirectoryEntry {
    pub id: EntryId,
    pub path: String,
    pub directory_type: DirectoryType,
    pub prometheus_type: PrometheusType,
    pub fq_name: String,
    pub help: String,
    pub const_labels: Vec<ConstLabel>,
}

/// One value read by `stats.dump`.
///
/// The vector-shaped variants mirror the owned values returned by the stats
/// segment reader. A dump may retain `directory_type = Symlink` while carrying
/// the resolved target shape, just as VPP's `copy_data` does.
#[derive(Debug, Clone, PartialEq)]
pub enum DumpValue {
    Counter(u64),
    Gauge(f64),
    CounterVectorSimple(Vec<Vec<u64>>),
    CounterVectorCombined(Vec<Vec<(u64, u64)>>),
    NameVector(Vec<Option<String>>),
    HistogramLog2(Vec<Vec<u64>>),
    RingBuffer(Vec<RingBufferSnapshot>),
}

/// One owned ring-buffer row snapshot returned by `stats.dump`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RingBufferSnapshot {
    pub sequence: u64,
    pub entries: Vec<Vec<u8>>,
}

impl From<wire::value::Value> for DumpValue {
    fn from(value: wire::value::Value) -> Self {
        match value {
            wire::value::Value::Counter(value) => Self::Counter(value),
            wire::value::Value::Gauge(value) => Self::Gauge(value),
            wire::value::Value::CounterVectorSimple(value) => {
                Self::CounterVectorSimple(value.rows.into_iter().map(|row| row.values).collect())
            }
            wire::value::Value::CounterVectorCombined(value) => Self::CounterVectorCombined(
                value
                    .rows
                    .into_iter()
                    .map(|row| {
                        row.values
                            .into_iter()
                            .map(|value| (value.packets, value.bytes))
                            .collect()
                    })
                    .collect(),
            ),
            wire::value::Value::NameVector(value) => {
                Self::NameVector(value.slots.into_iter().map(|slot| slot.name).collect())
            }
            wire::value::Value::HistogramLog2(value) => {
                Self::HistogramLog2(value.rows.into_iter().map(|row| row.bins).collect())
            }
            wire::value::Value::RingBuffer(value) => Self::RingBuffer(
                value
                    .snapshots
                    .into_iter()
                    .map(|snapshot| RingBufferSnapshot {
                        sequence: snapshot.sequence,
                        entries: snapshot.entries,
                    })
                    .collect(),
            ),
        }
    }
}

impl From<DumpValue> for wire::Value {
    fn from(value: DumpValue) -> Self {
        let value = match value {
            DumpValue::Counter(value) => wire::value::Value::Counter(value),
            DumpValue::Gauge(value) => wire::value::Value::Gauge(value),
            DumpValue::CounterVectorSimple(rows) => {
                wire::value::Value::CounterVectorSimple(wire::CounterVectorSimple {
                    rows: rows
                        .into_iter()
                        .map(|values| wire::CounterVectorSimpleRow { values })
                        .collect(),
                })
            }
            DumpValue::CounterVectorCombined(rows) => {
                wire::value::Value::CounterVectorCombined(wire::CounterVectorCombined {
                    rows: rows
                        .into_iter()
                        .map(|values| wire::CounterVectorCombinedRow {
                            values: values
                                .into_iter()
                                .map(|(packets, bytes)| wire::CounterVectorCombinedValue {
                                    packets,
                                    bytes,
                                })
                                .collect(),
                        })
                        .collect(),
                })
            }
            DumpValue::NameVector(slots) => wire::value::Value::NameVector(wire::NameVector {
                slots: slots
                    .into_iter()
                    .map(|name| wire::NameVectorSlot { name })
                    .collect(),
            }),
            DumpValue::HistogramLog2(rows) => {
                wire::value::Value::HistogramLog2(wire::HistogramLog2 {
                    rows: rows
                        .into_iter()
                        .map(|bins| wire::HistogramLog2Row { bins })
                        .collect(),
                })
            }
            DumpValue::RingBuffer(snapshots) => wire::value::Value::RingBuffer(wire::RingBuffer {
                snapshots: snapshots
                    .into_iter()
                    .map(|snapshot| wire::RingBufferSnapshot {
                        sequence: snapshot.sequence,
                        entries: snapshot.entries,
                    })
                    .collect(),
            }),
        };
        Self { value: Some(value) }
    }
}

impl TryFrom<wire::Value> for DumpValue {
    type Error = StatsClientError;

    fn try_from(value: wire::Value) -> Result<Self, Self::Error> {
        value
            .value
            .map(Self::from)
            .ok_or(StatsClientError::MissingValue {
                method: STATS_DUMP_METHOD,
            })
    }
}

/// One stats directory entry value from `stats.dump`, in the requested order
/// with duplicates preserved.
#[derive(Debug, Clone, PartialEq)]
pub struct DumpEntry {
    pub id: EntryId,
    pub path: String,
    pub directory_type: DirectoryType,
    pub prometheus_type: PrometheusType,
    pub value: DumpValue,
}

/// Typed server-domain error carried in-band in `stats.list`/`stats.dump`
/// replies under `BinaryApiStatus::Ok`.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum StatsServerError {
    #[error("stats pattern {pattern:?} failed to compile")]
    InvalidPattern { pattern: String },
    #[error("stats entry {id} was not found")]
    NotFound { id: EntryId },
    #[error("stats entry {id} is stale")]
    StaleEntry { id: EntryId },
    #[error("stats segment is busy being written")]
    ReadBusy,
    #[error(
        "stats entry {id} mixes prometheus type {prometheus_type:?} with directory type {directory_type:?}"
    )]
    IncompatibleType {
        id: EntryId,
        directory_type: DirectoryType,
        prometheus_type: PrometheusType,
    },
    #[error("internal stats server error")]
    Internal,
}

/// Client errors raised by the typed stats methods.
#[derive(Debug, thiserror::Error)]
pub enum StatsClientError {
    #[error("Binary API transport failed")]
    Transport {
        #[source]
        source: BinaryApiError,
    },
    #[error("decode stats reply for `{method}`")]
    ReplyDecode {
        method: &'static str,
        #[source]
        source: prost::DecodeError,
    },
    #[error("stats server rejected `{method}`")]
    Server {
        method: &'static str,
        #[source]
        source: StatsServerError,
    },
    #[error("stats reply for `{method}` is missing its result")]
    MissingResult { method: &'static str },
    #[error("stats reply for `{method}` is missing its entry id")]
    MissingId { method: &'static str },
    #[error("stats reply for `{method}` is missing its value")]
    MissingValue { method: &'static str },
    #[error("stats reply for `{method}` claims an error without a discriminant")]
    MissingErrorReply { method: &'static str },
    #[error("stats wire enum `{field}` has unknown discriminant {value}")]
    UnknownEnum { field: &'static str, value: i32 },
    #[error("stats entry id generation must not be zero (index {index})")]
    InvalidEntryIdGeneration { index: u32 },
}

fn checked_entry_id(
    id: Option<wire::EntryId>,
    method: &'static str,
) -> Result<EntryId, StatsClientError> {
    let id = id.ok_or(StatsClientError::MissingId { method })?;
    EntryId::try_from(id)
}

fn convert_list_entries(
    entries: Vec<wire::ListEntry>,
    method: &'static str,
) -> Result<Vec<DirectoryEntry>, StatsClientError> {
    entries
        .into_iter()
        .map(|entry| convert_list_entry(entry, method))
        .collect()
}

fn convert_list_entry(
    entry: wire::ListEntry,
    method: &'static str,
) -> Result<DirectoryEntry, StatsClientError> {
    let directory_type = DirectoryType::try_from(entry.directory_type)?;
    let prometheus_type = PrometheusType::try_from(entry.prometheus_type)?;
    Ok(DirectoryEntry {
        id: checked_entry_id(entry.id, method)?,
        path: entry.path,
        directory_type,
        prometheus_type,
        fq_name: entry.fq_name,
        help: entry.help,
        const_labels: entry
            .const_labels
            .into_iter()
            .map(|label| ConstLabel {
                name: label.name,
                value: label.value,
            })
            .collect(),
    })
}

fn convert_dump_entries(
    entries: Vec<wire::DumpEntry>,
    method: &'static str,
) -> Result<Vec<DumpEntry>, StatsClientError> {
    entries
        .into_iter()
        .map(|entry| convert_dump_entry(entry, method))
        .collect()
}

fn convert_dump_entry(
    entry: wire::DumpEntry,
    method: &'static str,
) -> Result<DumpEntry, StatsClientError> {
    let directory_type = DirectoryType::try_from(entry.directory_type)?;
    let prometheus_type = PrometheusType::try_from(entry.prometheus_type)?;
    let value = entry
        .value
        .ok_or(StatsClientError::MissingValue { method })?;
    let value = DumpValue::try_from(value)?;
    Ok(DumpEntry {
        id: checked_entry_id(entry.id, method)?,
        path: entry.path,
        directory_type,
        prometheus_type,
        value,
    })
}

fn convert_error(
    error: wire::ErrorReply,
    method: &'static str,
) -> Result<StatsServerError, StatsClientError> {
    match error.error {
        Some(wire::error_oneof::Error::InvalidPattern(error)) => {
            Ok(StatsServerError::InvalidPattern {
                pattern: error.pattern,
            })
        }
        Some(wire::error_oneof::Error::NotFound(error)) => Ok(StatsServerError::NotFound {
            id: checked_entry_id(error.id, method)?,
        }),
        Some(wire::error_oneof::Error::StaleEntry(error)) => Ok(StatsServerError::StaleEntry {
            id: checked_entry_id(error.id, method)?,
        }),
        Some(wire::error_oneof::Error::ReadBusy(_)) => Ok(StatsServerError::ReadBusy),
        Some(wire::error_oneof::Error::IncompatibleType(error)) => {
            Ok(StatsServerError::IncompatibleType {
                id: checked_entry_id(error.id, method)?,
                directory_type: DirectoryType::try_from(error.directory_type)?,
                prometheus_type: PrometheusType::try_from(error.prometheus_type)?,
            })
        }
        Some(wire::error_oneof::Error::Internal(_)) => Ok(StatsServerError::Internal),
        None => Err(StatsClientError::MissingErrorReply { method }),
    }
}

fn convert_list_reply(
    reply: wire::ListReply,
    method: &'static str,
) -> Result<Vec<DirectoryEntry>, StatsClientError> {
    match reply.result {
        Some(wire::list_reply::Result::Entries(entries)) => {
            convert_list_entries(entries.entries, method)
        }
        Some(wire::list_reply::Result::Error(error)) => Err(StatsClientError::Server {
            method,
            source: convert_error(error, method)?,
        }),
        None => Err(StatsClientError::MissingResult { method }),
    }
}

fn convert_dump_reply(
    reply: wire::DumpReply,
    method: &'static str,
) -> Result<Vec<DumpEntry>, StatsClientError> {
    match reply.result {
        Some(wire::dump_reply::Result::Entries(entries)) => {
            convert_dump_entries(entries.entries, method)
        }
        Some(wire::dump_reply::Result::Error(error)) => Err(StatsClientError::Server {
            method,
            source: convert_error(error, method)?,
        }),
        None => Err(StatsClientError::MissingResult { method }),
    }
}

/// Typed stats client over the shared Binary API client. One protocol call
/// per method call; encoding and checked conversion happen here, transport
/// stays with `BinaryApiClient`.
pub struct StatsClient {
    client: BinaryApiClient,
}

impl StatsClient {
    pub fn connect(path: impl AsRef<Path>) -> Result<Self, StatsClientError> {
        let client = BinaryApiClient::connect(path)
            .map_err(|source| StatsClientError::Transport { source })?;
        Ok(Self::new(client))
    }

    pub fn new(client: BinaryApiClient) -> Self {
        Self { client }
    }

    pub fn list(&mut self, patterns: &[String]) -> Result<Vec<DirectoryEntry>, StatsClientError> {
        let request = wire::ListRequest {
            patterns: patterns.to_vec(),
        };
        let payload = self
            .client
            .call(STATS_LIST_METHOD, &request.encode_to_vec())
            .map_err(|source| StatsClientError::Transport { source })?;
        let reply = wire::ListReply::decode(payload.as_slice()).map_err(|source| {
            StatsClientError::ReplyDecode {
                method: STATS_LIST_METHOD,
                source,
            }
        })?;
        convert_list_reply(reply, STATS_LIST_METHOD)
    }

    pub fn dump(&mut self, ids: &[EntryId]) -> Result<Vec<DumpEntry>, StatsClientError> {
        let request = wire::DumpRequest {
            ids: ids.iter().map(|&id| id.into()).collect(),
        };
        let payload = self
            .client
            .call(STATS_DUMP_METHOD, &request.encode_to_vec())
            .map_err(|source| StatsClientError::Transport { source })?;
        let reply = wire::DumpReply::decode(payload.as_slice()).map_err(|source| {
            StatsClientError::ReplyDecode {
                method: STATS_DUMP_METHOD,
                source,
            }
        })?;
        convert_dump_reply(reply, STATS_DUMP_METHOD)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::mem::size_of;
    use std::os::unix::net::UnixListener;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    use prost::Message;

    use crate::binary_api::{BinaryApiReply, BinaryApiRequest, BinaryApiStatus};

    use super::wire;
    use super::*;

    #[test]
    fn method_names_are_stable() {
        assert_eq!(STATS_LIST_METHOD, "stats.list");
        assert_eq!(STATS_DUMP_METHOD, "stats.dump");
    }

    #[test]
    fn wire_enums_have_stable_vpp_discriminants() {
        assert_eq!(wire::DirectoryType::Illegal as i32, 0);
        assert_eq!(wire::DirectoryType::ScalarIndex as i32, 1);
        assert_eq!(wire::DirectoryType::CounterVectorSimple as i32, 2);
        assert_eq!(wire::DirectoryType::CounterVectorCombined as i32, 3);
        assert_eq!(wire::DirectoryType::NameVector as i32, 4);
        assert_eq!(wire::DirectoryType::Empty as i32, 5);
        assert_eq!(wire::DirectoryType::Symlink as i32, 6);
        assert_eq!(wire::DirectoryType::HistogramLog2 as i32, 7);
        assert_eq!(wire::DirectoryType::RingBuffer as i32, 8);
        assert_eq!(wire::DirectoryType::Gauge as i32, 9);
        assert_eq!(wire::PrometheusType::Unspecified as i32, 0);
        assert_eq!(wire::PrometheusType::Counter as i32, 1);
        assert_eq!(wire::PrometheusType::Gauge as i32, 2);
        for value in 0..=9 {
            assert!(
                wire::DirectoryType::try_from(value).is_ok(),
                "directory type discriminant {value} must decode"
            );
        }
        assert!(wire::DirectoryType::try_from(10).is_err());
        assert!(wire::PrometheusType::try_from(3).is_err());
    }

    #[test]
    fn list_reply_round_trips_entries() {
        let reply = wire::ListReply {
            result: Some(wire::list_reply::Result::Entries(wire::ListEntries {
                entries: vec![wire::ListEntry {
                    id: Some(wire::EntryId {
                        index: 1,
                        generation: 2,
                    }),
                    path: "if0".to_owned(),
                    directory_type: wire::DirectoryType::ScalarIndex as i32,
                    prometheus_type: wire::PrometheusType::Counter as i32,
                    fq_name: "if0/name".to_owned(),
                    help: "interface name".to_owned(),
                    const_labels: vec![wire::ConstLabel {
                        name: "iface".to_owned(),
                        value: "eth0".to_owned(),
                    }],
                }],
            })),
        };
        let decoded =
            wire::ListReply::decode(reply.encode_to_vec().as_slice()).expect("decode list reply");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn list_reply_round_trips_every_error_oneof() {
        let errors = [
            wire::ErrorReply {
                error: Some(wire::error_oneof::Error::InvalidPattern(
                    wire::InvalidPatternError {
                        pattern: "(".to_owned(),
                    },
                )),
            },
            wire::ErrorReply {
                error: Some(wire::error_oneof::Error::NotFound(wire::EntryError {
                    id: Some(wire::EntryId {
                        index: 7,
                        generation: 3,
                    }),
                })),
            },
            wire::ErrorReply {
                error: Some(wire::error_oneof::Error::StaleEntry(wire::EntryError {
                    id: Some(wire::EntryId {
                        index: 7,
                        generation: 3,
                    }),
                })),
            },
            wire::ErrorReply {
                error: Some(wire::error_oneof::Error::ReadBusy(wire::Empty {})),
            },
            wire::ErrorReply {
                error: Some(wire::error_oneof::Error::IncompatibleType(
                    wire::IncompatibleTypeError {
                        id: Some(wire::EntryId {
                            index: 2,
                            generation: 4,
                        }),
                        directory_type: wire::DirectoryType::Gauge as i32,
                        prometheus_type: wire::PrometheusType::Counter as i32,
                    },
                )),
            },
            wire::ErrorReply {
                error: Some(wire::error_oneof::Error::Internal(wire::Empty {})),
            },
        ];
        for error in errors {
            let reply = wire::ListReply {
                result: Some(wire::list_reply::Result::Error(error)),
            };
            let decoded = wire::ListReply::decode(reply.encode_to_vec().as_slice())
                .expect("decode error reply");
            assert_eq!(decoded, reply);
        }
    }

    #[test]
    fn entry_id_rejects_generation_zero() {
        let error = EntryId::new(0, 0).expect_err("generation zero must be rejected");
        assert!(matches!(
            error,
            StatsClientError::InvalidEntryIdGeneration { index: 0 }
        ));
        let error = EntryId::try_from(wire::EntryId {
            index: 5,
            generation: 0,
        })
        .expect_err("wire generation zero must be rejected");
        assert!(matches!(
            error,
            StatsClientError::InvalidEntryIdGeneration { index: 5 }
        ));
    }

    #[test]
    fn entry_id_round_trips_through_the_wire() {
        let id = EntryId::new(3, 9).expect("valid generation");
        assert_eq!(id.index(), 3);
        assert_eq!(id.generation(), 9);
        let wire_id: wire::EntryId = id.into();
        assert_eq!(
            wire_id,
            wire::EntryId {
                index: 3,
                generation: 9
            }
        );
        assert_eq!(EntryId::try_from(wire_id).expect("decode entry id"), id);
    }

    #[test]
    fn dump_reply_round_trips_counter_and_gauge_values() {
        let reply = wire::DumpReply {
            result: Some(wire::dump_reply::Result::Entries(wire::DumpEntries {
                entries: vec![
                    wire::DumpEntry {
                        id: Some(wire::EntryId {
                            index: 1,
                            generation: 1,
                        }),
                        path: "if0/bytes".to_owned(),
                        directory_type: wire::DirectoryType::CounterVectorSimple as i32,
                        prometheus_type: wire::PrometheusType::Counter as i32,
                        value: Some(wire::Value {
                            value: Some(wire::value::Value::Counter(42)),
                        }),
                    },
                    wire::DumpEntry {
                        id: Some(wire::EntryId {
                            index: 2,
                            generation: 1,
                        }),
                        path: "mem/used".to_owned(),
                        directory_type: wire::DirectoryType::Gauge as i32,
                        prometheus_type: wire::PrometheusType::Gauge as i32,
                        value: Some(wire::Value {
                            value: Some(wire::value::Value::Gauge(3.5)),
                        }),
                    },
                ],
            })),
        };
        let decoded =
            wire::DumpReply::decode(reply.encode_to_vec().as_slice()).expect("decode dump reply");
        assert_eq!(decoded, reply);
    }

    #[test]
    fn dump_value_variants_round_trip_through_wire() {
        let values = [
            DumpValue::Counter(42),
            DumpValue::Gauge(3.5),
            DumpValue::CounterVectorSimple(vec![vec![0, 7, 0], vec![0, 0, 4]]),
            DumpValue::CounterVectorCombined(vec![vec![(0, 0), (3, 42)], vec![(0, 0), (0, 0)]]),
            DumpValue::NameVector(vec![Some("worker-0".to_owned()), None, Some(String::new())]),
            DumpValue::HistogramLog2(vec![vec![1, 2, 3], vec![4, 5, 6]]),
            DumpValue::RingBuffer(vec![RingBufferSnapshot {
                sequence: 9,
                entries: vec![vec![1, 2, 3], Vec::new()],
            }]),
        ];

        for expected in values {
            let wire_value: wire::Value = expected.clone().into();
            let decoded = wire::Value::decode(wire_value.encode_to_vec().as_slice())
                .expect("decode dump value");
            assert_eq!(
                DumpValue::try_from(decoded).expect("convert dump value"),
                expected
            );
        }
    }

    #[test]
    fn dump_reply_preserves_symlink_identity_with_resolved_vector_value() {
        let reply = wire::DumpReply {
            result: Some(wire::dump_reply::Result::Entries(wire::DumpEntries {
                entries: vec![dump_entry_wire(
                    8,
                    "/err/ip/version",
                    wire::DirectoryType::Symlink as i32,
                    wire::PrometheusType::Counter as i32,
                    Some(wire::value::Value::CounterVectorSimple(
                        wire::CounterVectorSimple {
                            rows: vec![wire::CounterVectorSimpleRow { values: vec![17] }],
                        },
                    )),
                )],
            })),
        };

        let entries = convert_dump_reply(reply, STATS_DUMP_METHOD).expect("convert dump reply");
        assert_eq!(entries[0].directory_type, DirectoryType::Symlink);
        assert_eq!(
            entries[0].value,
            DumpValue::CounterVectorSimple(vec![vec![17]])
        );
    }

    fn list_entry_wire(
        id: Option<wire::EntryId>,
        directory_type: i32,
        prometheus_type: i32,
        labels: Vec<wire::ConstLabel>,
    ) -> wire::ListEntry {
        wire::ListEntry {
            id,
            path: "if0".to_owned(),
            directory_type,
            prometheus_type,
            fq_name: "if0/name".to_owned(),
            help: "interface name".to_owned(),
            const_labels: labels,
        }
    }

    #[test]
    fn list_reply_converts_entries_with_labels() {
        let reply = wire::ListReply {
            result: Some(wire::list_reply::Result::Entries(wire::ListEntries {
                entries: vec![list_entry_wire(
                    Some(wire::EntryId {
                        index: 4,
                        generation: 2,
                    }),
                    wire::DirectoryType::CounterVectorSimple as i32,
                    wire::PrometheusType::Counter as i32,
                    vec![
                        wire::ConstLabel {
                            name: "iface".to_owned(),
                            value: "eth0".to_owned(),
                        },
                        wire::ConstLabel {
                            name: "dir".to_owned(),
                            value: "rx".to_owned(),
                        },
                    ],
                )],
            })),
        };
        let entries = convert_list_reply(reply, STATS_LIST_METHOD).expect("convert list reply");
        assert_eq!(
            entries,
            vec![DirectoryEntry {
                id: EntryId::new(4, 2).expect("valid id"),
                path: "if0".to_owned(),
                directory_type: DirectoryType::CounterVectorSimple,
                prometheus_type: PrometheusType::Counter,
                fq_name: "if0/name".to_owned(),
                help: "interface name".to_owned(),
                const_labels: vec![
                    ConstLabel {
                        name: "iface".to_owned(),
                        value: "eth0".to_owned(),
                    },
                    ConstLabel {
                        name: "dir".to_owned(),
                        value: "rx".to_owned(),
                    },
                ],
            }]
        );
    }

    #[test]
    fn list_reply_converts_empty_success() {
        let reply = wire::ListReply {
            result: Some(wire::list_reply::Result::Entries(wire::ListEntries {
                entries: Vec::new(),
            })),
        };
        let entries = convert_list_reply(reply, STATS_LIST_METHOD).expect("convert list reply");
        assert!(entries.is_empty());
    }

    fn error_reply(error: wire::error_oneof::Error) -> wire::ListReply {
        wire::ListReply {
            result: Some(wire::list_reply::Result::Error(wire::ErrorReply {
                error: Some(error),
            })),
        }
    }

    fn entry_error(id: wire::EntryId) -> wire::EntryError {
        wire::EntryError { id: Some(id) }
    }

    #[test]
    fn list_reply_error_is_typed_invalid_pattern_with_exact_pattern() {
        let reply = error_reply(wire::error_oneof::Error::InvalidPattern(
            wire::InvalidPatternError {
                pattern: "(".to_owned(),
            },
        ));
        let error =
            convert_list_reply(reply, STATS_LIST_METHOD).expect_err("invalid pattern must fail");
        assert!(matches!(
            error,
            StatsClientError::Server {
                method,
                source: StatsServerError::InvalidPattern { pattern }
            } if method == STATS_LIST_METHOD && pattern == "("
        ));
    }

    #[test]
    fn list_reply_error_keeps_full_entry_id_for_not_found() {
        let reply = error_reply(wire::error_oneof::Error::NotFound(entry_error(
            wire::EntryId {
                index: 7,
                generation: 3,
            },
        )));
        let error = convert_list_reply(reply, STATS_LIST_METHOD).expect_err("not found must fail");
        assert!(matches!(
            error,
            StatsClientError::Server {
                method,
                source: StatsServerError::NotFound { id }
            } if method == STATS_LIST_METHOD
                && id == EntryId::new(7, 3).expect("valid id")
        ));
    }

    #[test]
    fn list_reply_error_keeps_full_entry_id_for_stale() {
        let reply = error_reply(wire::error_oneof::Error::StaleEntry(entry_error(
            wire::EntryId {
                index: 11,
                generation: 5,
            },
        )));
        let error = convert_list_reply(reply, STATS_LIST_METHOD).expect_err("stale must fail");
        assert!(matches!(
            error,
            StatsClientError::Server {
                method,
                source: StatsServerError::StaleEntry { id }
            } if method == STATS_LIST_METHOD
                && id == EntryId::new(11, 5).expect("valid id")
        ));
    }

    #[test]
    fn list_reply_error_is_typed_read_busy() {
        let reply = error_reply(wire::error_oneof::Error::ReadBusy(wire::Empty {}));
        let error = convert_list_reply(reply, STATS_LIST_METHOD).expect_err("read busy must fail");
        assert!(matches!(
            error,
            StatsClientError::Server {
                method,
                source: StatsServerError::ReadBusy
            } if method == STATS_LIST_METHOD
        ));
    }

    #[test]
    fn list_reply_error_is_typed_incompatible_type() {
        let reply = error_reply(wire::error_oneof::Error::IncompatibleType(
            wire::IncompatibleTypeError {
                id: Some(wire::EntryId {
                    index: 2,
                    generation: 4,
                }),
                directory_type: wire::DirectoryType::Gauge as i32,
                prometheus_type: wire::PrometheusType::Counter as i32,
            },
        ));
        let error =
            convert_list_reply(reply, STATS_LIST_METHOD).expect_err("incompatible type must fail");
        assert!(matches!(
            error,
            StatsClientError::Server {
                method,
                source: StatsServerError::IncompatibleType {
                    id,
                    directory_type,
                    prometheus_type,
                }
            } if method == STATS_LIST_METHOD
                && id == EntryId::new(2, 4).expect("valid id")
                && directory_type == DirectoryType::Gauge
                && prometheus_type == PrometheusType::Counter
        ));
    }

    #[test]
    fn list_reply_error_is_typed_internal() {
        let reply = error_reply(wire::error_oneof::Error::Internal(wire::Empty {}));
        let error = convert_list_reply(reply, STATS_LIST_METHOD).expect_err("internal must fail");
        assert!(matches!(
            error,
            StatsClientError::Server {
                method,
                source: StatsServerError::Internal
            } if method == STATS_LIST_METHOD
        ));
    }

    fn dump_entry_wire(
        index: u32,
        path: &str,
        directory_type: i32,
        prometheus_type: i32,
        value: Option<wire::value::Value>,
    ) -> wire::DumpEntry {
        wire::DumpEntry {
            id: Some(wire::EntryId {
                index,
                generation: 1,
            }),
            path: path.to_owned(),
            directory_type,
            prometheus_type,
            value: Some(wire::Value { value }),
        }
    }

    #[test]
    fn dump_reply_preserves_order_and_duplicates() {
        let reply = wire::DumpReply {
            result: Some(wire::dump_reply::Result::Entries(wire::DumpEntries {
                entries: vec![
                    dump_entry_wire(
                        1,
                        "if0/bytes",
                        wire::DirectoryType::CounterVectorSimple as i32,
                        wire::PrometheusType::Counter as i32,
                        Some(wire::value::Value::Counter(42)),
                    ),
                    dump_entry_wire(
                        1,
                        "if0/bytes",
                        wire::DirectoryType::CounterVectorSimple as i32,
                        wire::PrometheusType::Counter as i32,
                        Some(wire::value::Value::Counter(43)),
                    ),
                    dump_entry_wire(
                        2,
                        "mem/used",
                        wire::DirectoryType::Gauge as i32,
                        wire::PrometheusType::Gauge as i32,
                        Some(wire::value::Value::Gauge(3.5)),
                    ),
                ],
            })),
        };
        let entries = convert_dump_reply(reply, STATS_DUMP_METHOD).expect("convert dump reply");
        assert_eq!(entries.len(), 3, "duplicates are preserved");
        assert_eq!(
            entries[0],
            DumpEntry {
                id: EntryId::new(1, 1).expect("valid id"),
                path: "if0/bytes".to_owned(),
                directory_type: DirectoryType::CounterVectorSimple,
                prometheus_type: PrometheusType::Counter,
                value: DumpValue::Counter(42),
            }
        );
        assert_eq!(entries[1].value, DumpValue::Counter(43));
        assert_eq!(
            entries[2].value,
            DumpValue::Gauge(3.5),
            "gauge values convert exactly"
        );
        assert_eq!(
            entries.iter().map(|entry| entry.id).collect::<Vec<_>>(),
            vec![
                EntryId::new(1, 1).expect("valid id"),
                EntryId::new(1, 1).expect("valid id"),
                EntryId::new(2, 1).expect("valid id"),
            ],
            "request order is preserved"
        );
    }

    #[test]
    fn list_reply_rejects_unknown_directory_type() {
        let reply = wire::ListReply {
            result: Some(wire::list_reply::Result::Entries(wire::ListEntries {
                entries: vec![list_entry_wire(
                    Some(wire::EntryId {
                        index: 1,
                        generation: 1,
                    }),
                    99,
                    wire::PrometheusType::Counter as i32,
                    Vec::new(),
                )],
            })),
        };
        let error = convert_list_reply(reply, STATS_LIST_METHOD)
            .expect_err("unknown directory type must fail");
        assert!(matches!(
            error,
            StatsClientError::UnknownEnum {
                field: "stats.directory_type",
                value: 99
            }
        ));
    }

    #[test]
    fn list_reply_rejects_unknown_and_unspecified_prometheus_type() {
        for (raw, expected) in [(7, 7), (wire::PrometheusType::Unspecified as i32, 0)] {
            let reply = wire::ListReply {
                result: Some(wire::list_reply::Result::Entries(wire::ListEntries {
                    entries: vec![list_entry_wire(
                        Some(wire::EntryId {
                            index: 1,
                            generation: 1,
                        }),
                        wire::DirectoryType::ScalarIndex as i32,
                        raw,
                        Vec::new(),
                    )],
                })),
            };
            let error = convert_list_reply(reply, STATS_LIST_METHOD)
                .expect_err("invalid prometheus type must fail");
            assert!(matches!(
                error,
                StatsClientError::UnknownEnum {
                    field: "stats.prometheus_type",
                    value,
                } if value == expected
            ));
        }
    }

    #[test]
    fn list_reply_rejects_missing_result() {
        let reply = wire::ListReply { result: None };
        let error =
            convert_list_reply(reply, STATS_LIST_METHOD).expect_err("missing result must fail");
        assert!(matches!(
            error,
            StatsClientError::MissingResult { method }
                if method == STATS_LIST_METHOD
        ));
    }

    #[test]
    fn list_reply_rejects_missing_entry_id() {
        let reply = wire::ListReply {
            result: Some(wire::list_reply::Result::Entries(wire::ListEntries {
                entries: vec![list_entry_wire(
                    None,
                    wire::DirectoryType::ScalarIndex as i32,
                    wire::PrometheusType::Counter as i32,
                    Vec::new(),
                )],
            })),
        };
        let error = convert_list_reply(reply, STATS_LIST_METHOD).expect_err("missing id must fail");
        assert!(matches!(
            error,
            StatsClientError::MissingId { method }
                if method == STATS_LIST_METHOD
        ));
    }

    #[test]
    fn list_reply_rejects_zero_generation_entry_id() {
        let reply = wire::ListReply {
            result: Some(wire::list_reply::Result::Entries(wire::ListEntries {
                entries: vec![list_entry_wire(
                    Some(wire::EntryId {
                        index: 5,
                        generation: 0,
                    }),
                    wire::DirectoryType::ScalarIndex as i32,
                    wire::PrometheusType::Counter as i32,
                    Vec::new(),
                )],
            })),
        };
        let error =
            convert_list_reply(reply, STATS_LIST_METHOD).expect_err("generation zero must fail");
        assert!(matches!(
            error,
            StatsClientError::InvalidEntryIdGeneration { index: 5 }
        ));
    }

    #[test]
    fn dump_reply_rejects_missing_value() {
        let reply = wire::DumpReply {
            result: Some(wire::dump_reply::Result::Entries(wire::DumpEntries {
                entries: vec![dump_entry_wire(
                    1,
                    "if0/bytes",
                    wire::DirectoryType::CounterVectorSimple as i32,
                    wire::PrometheusType::Counter as i32,
                    None,
                )],
            })),
        };
        let error =
            convert_dump_reply(reply, STATS_DUMP_METHOD).expect_err("missing value must fail");
        assert!(matches!(
            error,
            StatsClientError::MissingValue { method }
                if method == STATS_DUMP_METHOD
        ));
    }

    #[test]
    fn dump_reply_rejects_missing_dump_entry_id() {
        let reply = wire::DumpReply {
            result: Some(wire::dump_reply::Result::Entries(wire::DumpEntries {
                entries: vec![wire::DumpEntry {
                    id: None,
                    path: "if0/bytes".to_owned(),
                    directory_type: wire::DirectoryType::CounterVectorSimple as i32,
                    prometheus_type: wire::PrometheusType::Counter as i32,
                    value: Some(wire::Value {
                        value: Some(wire::value::Value::Counter(42)),
                    }),
                }],
            })),
        };
        let error =
            convert_dump_reply(reply, STATS_DUMP_METHOD).expect_err("missing dump id must fail");
        assert!(matches!(
            error,
            StatsClientError::MissingId { method }
                if method == STATS_DUMP_METHOD
        ));
    }

    #[test]
    fn error_reply_without_discriminant_is_rejected() {
        let error = convert_error(wire::ErrorReply { error: None }, STATS_LIST_METHOD)
            .expect_err("empty error oneof must fail");
        assert!(matches!(
            error,
            StatsClientError::MissingErrorReply { method }
                if method == STATS_LIST_METHOD
        ));
    }

    #[test]
    fn error_reply_without_entry_id_is_rejected() {
        let error = convert_error(
            wire::ErrorReply {
                error: Some(wire::error_oneof::Error::NotFound(wire::EntryError {
                    id: None,
                })),
            },
            STATS_LIST_METHOD,
        )
        .expect_err("missing error id must fail");
        assert!(matches!(
            error,
            StatsClientError::MissingId { method }
                if method == STATS_LIST_METHOD
        ));
    }

    static SOCKET_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn socket_path() -> PathBuf {
        let sequence = SOCKET_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "hammer-ipc-stats-{}-{sequence}.sock",
            std::process::id()
        ))
    }

    /// Serves one request from a background thread and replies through the
    /// given responder, mirroring the `binary_api` test harness.
    fn spawn_server(
        path: &PathBuf,
        respond: impl Fn(BinaryApiRequest) -> BinaryApiReply + Send + 'static,
    ) -> thread::JoinHandle<()> {
        let listener = UnixListener::bind(path).expect("bind stats test server");
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept stats test client");
            let mut length = [0_u8; size_of::<u32>()];
            stream
                .read_exact(&mut length)
                .expect("read stats request length");
            let mut frame = vec![0; u32::from_be_bytes(length) as usize];
            stream
                .read_exact(&mut frame)
                .expect("read stats request frame");
            let request = BinaryApiRequest::decode(frame.as_slice()).expect("decode stats request");
            let reply = respond(request);
            let frame = reply.encode_to_vec();
            stream
                .write_all(&(frame.len() as u32).to_be_bytes())
                .expect("write stats reply length");
            stream.write_all(&frame).expect("write stats reply frame");
        })
    }

    fn ok_reply(context: u64, payload: Vec<u8>) -> BinaryApiReply {
        BinaryApiReply {
            context,
            status: BinaryApiStatus::Ok as i32,
            payload,
        }
    }

    fn entry_reply_payload() -> Vec<u8> {
        wire::ListReply {
            result: Some(wire::list_reply::Result::Entries(wire::ListEntries {
                entries: vec![list_entry_wire(
                    Some(wire::EntryId {
                        index: 4,
                        generation: 2,
                    }),
                    wire::DirectoryType::CounterVectorSimple as i32,
                    wire::PrometheusType::Counter as i32,
                    Vec::new(),
                )],
            })),
        }
        .encode_to_vec()
    }

    #[test]
    fn stats_client_list_succeeds_over_unix_socket() {
        let path = socket_path();
        let server = spawn_server(&path, |request| {
            assert_eq!(request.method, STATS_LIST_METHOD);
            let list_request =
                wire::ListRequest::decode(request.payload.as_slice()).expect("decode list request");
            assert_eq!(list_request.patterns, vec!["if0"]);
            ok_reply(request.context, entry_reply_payload())
        });

        let mut client = StatsClient::connect(&path).expect("connect stats client");
        let entries = client.list(&["if0".to_owned()]).expect("list succeeds");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, EntryId::new(4, 2).expect("valid id"));
        assert_eq!(entries[0].path, "if0");
        server.join().expect("join stats test server");
    }

    #[test]
    fn stats_client_list_empty_succeeds_over_unix_socket() {
        let path = socket_path();
        let server = spawn_server(&path, |request| {
            assert_eq!(request.method, STATS_LIST_METHOD);
            ok_reply(
                request.context,
                wire::ListReply {
                    result: Some(wire::list_reply::Result::Entries(wire::ListEntries {
                        entries: Vec::new(),
                    })),
                }
                .encode_to_vec(),
            )
        });

        let mut client = StatsClient::connect(&path).expect("connect stats client");
        let entries = client.list(&[]).expect("empty list succeeds");
        assert!(entries.is_empty());
        server.join().expect("join stats test server");
    }

    #[test]
    fn stats_client_dump_succeeds_over_unix_socket() {
        let path = socket_path();
        let server = spawn_server(&path, |request| {
            assert_eq!(request.method, STATS_DUMP_METHOD);
            let dump_request =
                wire::DumpRequest::decode(request.payload.as_slice()).expect("decode dump request");
            assert_eq!(dump_request.ids.len(), 2);
            assert_eq!(dump_request.ids[0].index, 1);
            assert_eq!(dump_request.ids[0].generation, 3);
            assert_eq!(dump_request.ids[1].index, 1);
            assert_eq!(dump_request.ids[1].generation, 3);
            ok_reply(
                request.context,
                wire::DumpReply {
                    result: Some(wire::dump_reply::Result::Entries(wire::DumpEntries {
                        entries: vec![
                            dump_entry_wire(
                                1,
                                "if0/bytes",
                                wire::DirectoryType::CounterVectorSimple as i32,
                                wire::PrometheusType::Counter as i32,
                                Some(wire::value::Value::Counter(42)),
                            ),
                            dump_entry_wire(
                                1,
                                "if0/bytes",
                                wire::DirectoryType::CounterVectorSimple as i32,
                                wire::PrometheusType::Counter as i32,
                                Some(wire::value::Value::Counter(43)),
                            ),
                        ],
                    })),
                }
                .encode_to_vec(),
            )
        });

        let id = EntryId::new(1, 3).expect("valid id");
        let mut client = StatsClient::connect(&path).expect("connect stats client");
        let entries = client.dump(&[id, id]).expect("dump succeeds");
        assert_eq!(entries.len(), 2, "duplicate ids round trip");
        assert_eq!(entries[0].value, DumpValue::Counter(42));
        assert_eq!(entries[1].value, DumpValue::Counter(43));
        server.join().expect("join stats test server");
    }

    #[test]
    fn stats_client_surfaces_non_ok_status_from_binary_api() {
        let path = socket_path();
        let server = spawn_server(&path, |request| BinaryApiReply {
            context: request.context,
            status: BinaryApiStatus::MethodMissing as i32,
            payload: Vec::new(),
        });

        let mut client = StatsClient::connect(&path).expect("connect stats client");
        let error = client.list(&[]).expect_err("non-ok status must fail");
        assert!(matches!(
            error,
            StatsClientError::Transport {
                source: BinaryApiError::ClientRejected { method, status }
            } if method == STATS_LIST_METHOD && status == BinaryApiStatus::MethodMissing
        ));
        server.join().expect("join stats test server");
    }

    #[test]
    fn stats_client_surfaces_context_mismatch_from_binary_api() {
        let path = socket_path();
        let server = spawn_server(&path, |request| {
            ok_reply(request.context.wrapping_add(1), Vec::new())
        });

        let mut client = StatsClient::connect(&path).expect("connect stats client");
        let error = client.list(&[]).expect_err("context mismatch must fail");
        assert!(matches!(
            error,
            StatsClientError::Transport {
                source: BinaryApiError::ClientReplyContext { method, .. }
            } if method == STATS_LIST_METHOD
        ));
        server.join().expect("join stats test server");
    }

    #[test]
    fn stats_client_connect_failure_is_transport() {
        let path = socket_path();
        let error = match StatsClient::connect(&path) {
            Ok(_) => panic!("connect must fail"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            StatsClientError::Transport {
                source: BinaryApiError::ClientConnect { .. }
            }
        ));
    }

    #[test]
    fn stats_client_read_failure_is_transport() {
        let path = socket_path();
        let listener = UnixListener::bind(&path).expect("bind stats test server");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept stats test client");
            let mut length = [0_u8; size_of::<u32>()];
            stream
                .read_exact(&mut length)
                .expect("read stats request length");
            let mut frame = vec![0; u32::from_be_bytes(length) as usize];
            stream
                .read_exact(&mut frame)
                .expect("read stats request frame");
            // Drop the stream without replying: the client must surface ClientRead.
        });

        let mut client = StatsClient::connect(&path).expect("connect stats client");
        let error = client.list(&[]).expect_err("missing reply must fail");
        assert!(matches!(
            error,
            StatsClientError::Transport {
                source: BinaryApiError::ClientRead { method, .. }
            } if method == STATS_LIST_METHOD
        ));
        server.join().expect("join stats test server");
    }
}
