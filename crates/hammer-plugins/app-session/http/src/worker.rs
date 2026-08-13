//! Per-data-worker HTTP/3 connection context ownership.
//!
//! Each data worker owns a bounded, cache-line-aligned pool of connection
//! contexts, mirroring VPP's `http_worker_t` with its per-thread `ctx_pool`
//! (http_private.h:477-482), populated by `http_ctx_alloc_w_thread` through
//! `pool_get_aligned_safe (wrk->ctx_pool, hc, CLIB_CACHE_LINE_BYTES)`
//! (http.c:170-180). Lookups validate liveness before dereferencing
//! (`http_ctx_get_w_thread_if_valid`, http.c:184-189) and free returns the
//! slot to the owning worker's pool (`http_ctx_free`, http.c:198-204). A
//! context binds the exact lower QUIC session: VPP stores the session index
//! in `http_ctx_t::c_s_index` (http.c:752) and resolves it via `session_get`
//! (http.c:876-899); HTTP3 request/stream contexts reference their
//! connection context index and worker (http3.c:35-48).
//!
//! `HttpMain` owns one `HttpWorker` per data worker in a
//! `CacheLine<ThreadOwned<HttpWorker>>` slot, mirroring `QuicMain.workers`
//! (quic listener.rs:58) and VPP's `http_main.wrk`, a fixed array sized by
//! thread count and indexed per thread by `http_worker_get` (http.c:1073,
//! http_private.h:1275-1278). Each worker installs itself once via the
//! `http_worker_init` worker init function, ordered after session/QUIC
//! worker init; `install_worker`/`with_worker` are O(1) slot lookups with
//! typed out-of-range, not-installed, and wrong-thread errors.
//!
//! Stream contexts live in a second, independent generation-checked pool on
//! the same worker, mirroring VPP `http_ts_accept_stream` (http.c:675-721):
//! allocation generation-checks the parent connection context first, then
//! records the child session, parent `ContextId`, and peer direction.
//!
//! Identities follow the Hammer QUIC conventions: `ContextId` packs
//! `slot | generation << 32` exactly like the QUIC `ContextId`
//! (quic worker.rs:178-205) and `SessionId` (hammer-service
//! session/id.rs:6-25), with standard `From` conversions to and from `Index`
//! and the packed `u64`. The underlying `Pool` (hammer-infra pool.rs) keeps
//! allocate/get/remove O(1) and generation-checked, with slots aligned to
//! `CACHE_LINE` by the pool itself. Session App callbacks, HTTP3 engine
//! dispatch, QPACK, request publication, and stop_listen are later slices.

use hammer_infra::fifo::{Fifo, FifoError};
use hammer_infra::pool::{Index, Pool};
use hammer_infra::thread_owned::ThreadOwnedError;
use hammer_runtime::DataWorkerId;
use hammer_runtime::app::SessionAppContext;
use hammer_runtime::error::RuntimeError;
use hammer_runtime::session::SessionStreamDirection;
use hammer_service::session::{SessionEndpointRole, SessionId, SessionWorker};

use crate::http_common::{BodyAccumulator, PublishError, publish_body_chunk};
use crate::http3::preface::encode_control_preface;
use crate::http3::proto::coding::Decode;
use crate::http3::proto::control::{ControlRead, ControlStreamError, ControlStreamReader};
use crate::http3::proto::error::ErrorCode;
use crate::http3::request_frame_reader::{RequestFrameError, RequestFrameRead, RequestFrameReader};
use crate::http3::proto::stream::{StreamCategory, StreamType};

/// Default connection-context capacity of one data worker's pool, matching
/// the QUIC per-worker context capacity (quic worker.rs:42).
pub(crate) const HTTP_CONTEXT_CAPACITY: usize = 4_096;

/// Default per-worker stream-context capacity. HTTP/3 multiplexes many
/// streams over one connection, so the stream pool is deliberately larger
/// than, and independent from, `HTTP_CONTEXT_CAPACITY`: VPP shares one
/// `http_ctx_t` pool between connections and streams (http.c:675-721), which
/// Hammer splits so one cannot starve the other.
pub(crate) const HTTP_STREAM_CAPACITY: usize = 16_384;

/// Exact bytes of the local HTTP/3 control-stream preface: the CONTROL
/// stream type (0x00) followed by a SETTINGS frame (type 0x04, length 0x04)
/// carrying explicit zero QPACK settings — QPACK_MAX_TABLE_CAPACITY=0
/// (0x01 0x00) and QPACK_BLOCKED_STREAMS=0 (0x07 0x00) — the static-only
/// QPACK policy. The zeros are deliberate, not a default-omission artifact:
/// VPP's `http3_frame_settings_write` (frame.c:152-177) drops every setting
/// equal to its default and `http3_conn_init` (http3.c:241-246) normally
/// emits a nonzero SETTINGS_MAX_FIELD_SECTION_SIZE instead, so only the
/// write ordering mirrors VPP (stream type, then SETTINGS, then one
/// post-write event), not byte identity. The `http3::proto` encoders
/// (`StreamType`, `Settings`) target `BufMut`, so a fixed no-heap constant
/// keeps the preface allocation-free on the worker.
pub(crate) const LOCAL_CONTROL_PREFACE: [u8; 7] = [0x00, 0x04, 0x04, 0x01, 0x00, 0x07, 0x00];

/// Generation-checked identity for one HTTP/3 connection context in the
/// owning data worker's pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(crate) struct ContextId(u64);

impl From<u64> for ContextId {
    #[inline]
    fn from(raw: u64) -> Self {
        Self(raw)
    }
}

impl From<ContextId> for u64 {
    #[inline]
    fn from(context: ContextId) -> Self {
        context.0
    }
}

impl From<Index> for ContextId {
    #[inline]
    fn from(index: Index) -> Self {
        Self(u64::from(index.slot()) | (u64::from(index.generation()) << 32))
    }
}

impl From<ContextId> for Index {
    #[inline]
    fn from(context: ContextId) -> Self {
        Self::new(context.0 as u32, (context.0 >> 32) as u32)
    }
}

/// Generation-checked identity for one HTTP/3 stream context in the owning
/// data worker's stream pool, packed `slot | generation << 32` exactly like
/// `ContextId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(crate) struct StreamContextId(u64);

impl From<u64> for StreamContextId {
    #[inline]
    fn from(raw: u64) -> Self {
        Self(raw)
    }
}

impl From<StreamContextId> for u64 {
    #[inline]
    fn from(stream: StreamContextId) -> Self {
        stream.0
    }
}

impl From<Index> for StreamContextId {
    #[inline]
    fn from(index: Index) -> Self {
        Self(u64::from(index.slot()) | (u64::from(index.generation()) << 32))
    }
}

impl From<StreamContextId> for Index {
    #[inline]
    fn from(stream: StreamContextId) -> Self {
        Self::new(stream.0 as u32, (stream.0 >> 32) as u32)
    }
}

/// Cold per-connection state bound to one data-worker context slot.
///
/// Holds the lower QUIC `SessionId` the context was allocated for plus the
/// HTTP/3 bootstrap state of the local control stream; hot HTTP/3 connection
/// state (frames, QPACK, streams) belongs to later slices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ConnectionContext {
    /// Lower QUIC session this connection context is bound to.
    pub(crate) session: SessionId,
    /// Endpoint role of the underlying Session: `Server` for
    /// listener-accepted connections, `Client` for outbound connects;
    /// `None` when accept-time role metadata is absent. Cold, read-only
    /// metadata consumed by the peer uni stream registration policy.
    pub(crate) role: Option<SessionEndpointRole>,
    /// Local control stream child Session, recorded only after the bootstrap
    /// action succeeds; `None` until then. Mirrors VPP recording the opened
    /// control stream in `http_ctx_t::our_ctrl_stream_index` (http3.c:234).
    pub(crate) local_control: Option<SessionId>,
    /// Peer control stream context, registered exactly once when the decoded
    /// peer uni stream type is Control; `None` until then. Mirrors VPP
    /// `http_ctx_t::peer_ctrl_stream_index` (http3.c:1683-1691).
    pub(crate) peer_control: Option<StreamContextId>,
    /// Peer QPACK encoder stream context, registered exactly once; mirrors
    /// VPP `http_ctx_t::peer_encoder_stream_index` (http3.c:1703-1710).
    pub(crate) peer_encoder: Option<StreamContextId>,
    /// Peer QPACK decoder stream context, registered exactly once; mirrors
    /// VPP `http_ctx_t::peer_decoder_stream_index` (http3.c:1693-1700).
    pub(crate) peer_decoder: Option<StreamContextId>,
    /// True until the peer's SETTINGS arrive on its control stream; set at
    /// allocation and consumed by the peer-SETTINGS slice.
    pub(crate) peer_settings_pending: bool,
    /// Generation-checked slot of this connection's SETTINGS reader in the
    /// worker's separate `readers` pool; `None` until the first readable
    /// bytes arrive. The 192-byte `ControlStreamReader` is not embedded in
    /// every connection or stream slot: one pool slot per connection is
    /// enough, since each connection owns at most one peer control stream.
    pub(crate) peer_control_reader: Option<Index>,
}

/// Cold per-stream state bound to one data-worker stream-pool slot.
///
/// Mirrors VPP `http_ts_accept_stream` (http.c:675-721), where the accepted
/// stream context records its child session handle (`hc_tc_session_handle`),
/// the peer direction flag (`SESSION_F_UNIDIRECTIONAL`), and its parent
/// connection index (`hc_http_conn_index`). Hot HTTP/3 stream state (frames,
/// QPACK) belongs to later slices; the peer uni stream type decode state
/// mirrors the varint consumption stage of VPP
/// `http3_stream_transport_rx_unknown_type` (http3.c:1653-1673).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StreamContext {
    /// Child QUIC session this stream context is bound to.
    pub(crate) session: SessionId,
    /// Parent connection context this stream belongs to.
    pub(crate) parent: ContextId,
    /// Peer stream direction: uni- or bidirectional.
    pub(crate) direction: SessionStreamDirection,
    /// Incremental decode state for a peer unidirectional stream's type
    /// varint; stays `Unclassified` for bidi request streams.
    pub(crate) peer_uni_type: PeerUniStreamTypeDecode,
    /// Registered peer uni role, recorded by
    /// [`HttpWorker::register_peer_uni_stream`] only after the parent slot
    /// registration succeeds; `Unclassified` until then. Bidi request
    /// streams never register.
    pub(crate) peer_role: PeerUniStreamRole,
    /// Generation-checked slot of this stream's request-frame reader in the
    /// worker's separate `request_readers` pool; `None` until the first
    /// readable bytes of a bidirectional request stream arrive. Mirrors VPP
    /// keeping the request's frame-header staging (`fh`), phase
    /// (`req_state`), and dispatch callback on the per-request `http_ctx_t`
    /// owned by the data worker (`http3_stream_transport_rx_req`,
    /// http3.c:1732-1799): established once per stream, looked up
    /// generation-checked per readable segment. The reader is not embedded
    /// in every stream slot: one pool slot per stream at most, bounded by
    /// the stream pool capacity.
    pub(crate) request_reader: Option<Index>,
    /// Generation-checked slot of this stream's pending HEADERS field
    /// section in the worker's separate `pending_field_sections` pool;
    /// `None` until the first completed field section is retained. Mirrors
    /// VPP recording the received request on the per-request `http_ctx_t`
    /// owned by the data worker (`req->headers`, set in
    /// `http3_req_state_wait_transport_method`, http3.c:835-899) for the
    /// later app-dispatch stage; the section is not embedded in every stream
    /// slot: one pool slot per stream at most, bounded by the stream pool
    /// capacity, freed with the stream.
    pub(crate) pending_field_section: Option<Index>,
    /// Declared-length body accounting of a bidirectional request stream,
    /// installed from the request HEADERS Content-Length by
    /// [`HttpWorker::install_request_body_length`] and advanced per DATA
    /// frame by [`HttpWorker::process_request_data`]. Mirrors VPP's `to_recv`
    /// on the per-request `http_ctx_t` (`http3_req_state_transport_io_more_data`,
    /// http3.c:1184-1263): `NoBody` until a declared length is installed and
    /// after a body-less HEADERS, so DATA is unexpected before then; a
    /// half-closed stream with a declared but unreceived body is
    /// `REQUEST_INCOMPLETE` ([`HttpWorker::validate_request_finish`]). Peer
    /// uni streams never install or feed a body and keep the `NoBody`
    /// default.
    pub(crate) body: BodyAccumulator,
}

/// A completed HEADERS field section retained for publication: the encoded
/// block and the exact number of lower-FIFO bytes already consumed when the
/// section completed. The two halves are one invariant kept across
/// publication retries — a retry re-publishes the same encoded block and
/// must not re-consume the already-consumed FIFO bytes, mirroring VPP
/// recording the consumed count per frame (`*n_deq = req->fh.length`,
/// http3.c:868, and the total `max_deq - left_deq`, http3.c:1798).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingFieldSection {
    /// Encoded HEADERS field section (QPACK static-table encoding).
    pub(crate) encoded: Vec<u8>,
    /// Exact lower-FIFO bytes consumed when this section completed.
    pub(crate) consumed: usize,
}

/// Classification role of a peer unidirectional stream once its type varint
/// is decoded, mirroring the switch in VPP
/// `http3_stream_transport_rx_unknown_type` (http3.c:1680-1726).
/// `Unclassified` while the varint is incomplete;
/// [`HttpWorker::register_peer_uni_stream`] registers the role with the
/// parent connection once, after the varint completes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum PeerUniStreamRole {
    /// The stream-type varint has not fully arrived.
    #[default]
    Unclassified,
    /// Control stream (0x00): carries SETTINGS, GOAWAY, MAX_PUSH_ID.
    Control,
    /// Push stream (0x01): not supported in this slice.
    Push,
    /// QPACK encoder stream (0x02): drained only.
    QpackEncoder,
    /// QPACK decoder stream (0x03): drained only.
    QpackDecoder,
    /// Any other stream type: ignored.
    Unknown,
}

impl From<StreamCategory> for PeerUniStreamRole {
    fn from(category: StreamCategory) -> Self {
        match category {
            StreamCategory::Control => Self::Control,
            StreamCategory::Push => Self::Push,
            StreamCategory::QpackEncoder => Self::QpackEncoder,
            StreamCategory::QpackDecoder => Self::QpackDecoder,
            StreamCategory::Unknown(_) => Self::Unknown,
        }
    }
}

/// Result of feeding one readable segment to
/// [`PeerUniStreamTypeDecode::feed`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeerUniStreamTypeOutcome {
    /// The stream-type varint has not fully arrived; the partial prefix is
    /// preserved in the decode state for the next feed.
    Incomplete,
    /// The varint completed within this segment: `consumed` bytes of the
    /// segment were consumed, the state reset, and the caller continues
    /// with `segment[consumed..]`.
    Complete {
        stream_type: StreamType,
        category: StreamCategory,
        consumed: usize,
    },
}

/// Incremental decode state for a peer unidirectional stream's type varint
/// (RFC 9114 Section 6.2, RFC 9000 Section 16).
///
/// Mirrors the varint consumption stage of VPP
/// `http3_stream_transport_rx_unknown_type` (http3.c:1653-1673): at most
/// `HTTP_VARINT_MAX_LEN` bytes are read and buffered until the varint is
/// complete, then exactly the encoded varint is drained. Stateful across
/// calls so a type split across FIFO segments decodes without re-reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PeerUniStreamTypeDecode {
    /// Partial varint prefix preserved across feeds, at most
    /// `StreamType::MAX_ENCODED_SIZE` bytes.
    buf: [u8; StreamType::MAX_ENCODED_SIZE],
    /// Number of buffered bytes.
    len: u8,
    /// Decoded classification; `Unclassified` until the varint completes.
    role: PeerUniStreamRole,
}

impl Default for PeerUniStreamTypeDecode {
    fn default() -> Self {
        Self {
            buf: [0; StreamType::MAX_ENCODED_SIZE],
            len: 0,
            role: PeerUniStreamRole::Unclassified,
        }
    }
}

impl PeerUniStreamTypeDecode {
    /// Decoded classification of the peer uni stream, or `Unclassified`
    /// while the varint is incomplete.
    #[inline]
    pub(crate) fn role(&self) -> PeerUniStreamRole {
        self.role
    }

    /// Feeds the next readable bytes of the peer unidirectional stream.
    ///
    /// Buffers at most `StreamType::MAX_ENCODED_SIZE` bytes across calls
    /// and never scans beyond the varint: on `Complete` exactly the encoded
    /// varint is consumed from `segment` and the state resets for the next
    /// stream, so the caller continues with `segment[consumed..]`; on
    /// `Incomplete` the partial prefix is preserved for the next feed.
    pub(crate) fn feed(&mut self, segment: &[u8]) -> PeerUniStreamTypeOutcome {
        let prefix = usize::from(self.len);
        let take = segment.len().min(self.buf.len() - prefix);
        self.buf[prefix..prefix + take].copy_from_slice(&segment[..take]);
        let len = prefix + take;
        self.len = len as u8;

        // All previously buffered bytes are a strict prefix of the varint
        // (it would have completed on the earlier feed), so decode fails
        // only with UnexpectedEnd until enough bytes have arrived.
        let mut buffered = &self.buf[..len];
        let Ok(stream_type) = StreamType::decode(&mut buffered) else {
            return PeerUniStreamTypeOutcome::Incomplete;
        };
        let used = len - buffered.len();
        let category = stream_type.classify();
        self.role = category.into();
        self.len = 0;
        PeerUniStreamTypeOutcome::Complete {
            stream_type,
            category,
            consumed: used - prefix,
        }
    }
}

/// Typed errors for per-worker connection context operations and container
/// install/lookup, mirroring `QuicWorkerError`'s container variants
/// (quic worker.rs:3556-3567).
#[hammer_component_macros::runtime_error(subsystem = "http")]
#[derive(Debug, thiserror::Error)]
pub(crate) enum HttpWorkerError {
    #[error("http connection context pool is full (capacity {capacity})")]
    ContextCapacityExhausted { capacity: usize },
    #[error("http connection context {context:?} is not live")]
    ContextMissing { context: ContextId },
    #[error(
        "http connection context {context:?} is bound to session {actual:?}, expected {expected:?}"
    )]
    SessionMismatch {
        context: ContextId,
        expected: SessionId,
        actual: SessionId,
    },
    #[error("http connection context {context:?} already opened its control stream")]
    ControlStreamAlreadyOpen { context: ContextId },
    #[error(
        "http connection context {context:?} has not opened its local control stream; bootstrap first"
    )]
    ControlStreamNotOpen { context: ContextId },
    #[error(
        "http connection context {context:?} cannot find the Session FIFO of local control child {child:?}"
    )]
    ControlStreamFifoMissing { context: ContextId, child: SessionId },
    #[error("local control preface for connection context {context:?} does not encode")]
    ControlPrefaceEncodeFailed { context: ContextId },
    #[error(
        "local control preface for connection context {context:?} failed to write to the child control stream FIFO: {source}"
    )]
    ControlPrefaceFifo {
        context: ContextId,
        #[source]
        source: FifoError,
    },
    #[error(
        "failed to publish the local control preface TX-enqueue event for connection context {context:?}: {source}"
    )]
    ControlPrefaceEventPublishFailed {
        context: ContextId,
        #[source]
        source: RuntimeError,
    },
    #[error(
        "peer {role:?} stream {stream:?} already registered on connection context {context:?}: HTTP/3 {code}"
    )]
    PeerStreamRoleDuplicate {
        stream: StreamContextId,
        context: ContextId,
        role: PeerUniStreamRole,
        code: ErrorCode,
    },
    #[error(
        "peer push stream {stream:?} on connection context {context:?} is rejected by the local endpoint's push policy: HTTP/3 {code}"
    )]
    PeerPushRejected {
        stream: StreamContextId,
        context: ContextId,
        code: ErrorCode,
    },
    #[error(
        "peer push stream {stream:?} on connection context {context:?} cannot apply the server/client push policy: connection role metadata is missing"
    )]
    PeerPushRoleMissing {
        stream: StreamContextId,
        context: ContextId,
    },
    #[error("peer uni stream {stream:?} on connection context {context:?} has no decoded type to register")]
    PeerStreamRoleUnclassified {
        stream: StreamContextId,
        context: ContextId,
    },
    #[error(
        "peer uni stream {stream:?} on connection context {context:?} is not drain-only (role {role:?})"
    )]
    PeerStreamNotDrainable {
        stream: StreamContextId,
        context: ContextId,
        role: PeerUniStreamRole,
    },
    #[error(
        "peer uni stream {stream:?} on connection context {context:?} is not the registered peer control stream (role {role:?})"
    )]
    PeerControlStreamMismatch {
        stream: StreamContextId,
        context: ContextId,
        role: PeerUniStreamRole,
    },
    #[error(
        "peer uni stream {stream:?} on connection context {context:?} is not the registered peer QPACK encoder/decoder stream (role {role:?})"
    )]
    PeerQpackStreamMismatch {
        stream: StreamContextId,
        context: ContextId,
        role: PeerUniStreamRole,
    },
    #[error(
        "peer uni stream {stream:?} on connection context {context:?} is not an Unknown drain-only stream (role {role:?})"
    )]
    PeerUnknownStreamMismatch {
        stream: StreamContextId,
        context: ContextId,
        role: PeerUniStreamRole,
    },
    #[error(
        "peer control stream {stream:?} on connection context {context:?}: SETTINGS reader pool is full (capacity {capacity})"
    )]
    PeerControlReaderCapacityExhausted {
        stream: StreamContextId,
        context: ContextId,
        capacity: usize,
    },
    #[error("http worker lost the peer control SETTINGS reader slot {index:?} for stream {stream:?}")]
    PeerControlReaderMissing {
        stream: StreamContextId,
        index: Index,
    },
    #[error("http stream {stream:?} request reader pool is full (capacity {capacity})")]
    RequestReaderCapacityExhausted { stream: StreamContextId, capacity: usize },
    #[error("http worker lost the request reader slot {index:?} for stream {stream:?}")]
    RequestReaderMissing { stream: StreamContextId, index: Index },
    #[error("http stream {stream:?} pending field-section pool is full (capacity {capacity})")]
    PendingFieldSectionCapacityExhausted {
        stream: StreamContextId,
        capacity: usize,
    },
    #[error("http worker lost the pending field-section slot {index:?} for stream {stream:?}")]
    PendingFieldSectionMissing { stream: StreamContextId, index: Index },
    #[error(
        "http stream {stream:?} already has a pending HEADERS field section; the newer section is returned unreplaced"
    )]
    PendingFieldSectionOverflow {
        stream: StreamContextId,
        section: PendingFieldSection,
    },
    #[error("http stream context pool is full (capacity {capacity})")]
    StreamCapacityExhausted { capacity: usize },
    #[error("http stream context {stream:?} is not live")]
    StreamMissing { stream: StreamContextId },
    #[error(
        "http stream context {stream:?} is bound to session {actual:?}, expected {expected:?}"
    )]
    StreamSessionMismatch {
        stream: StreamContextId,
        expected: SessionId,
        actual: SessionId,
    },
    #[error(
        "http stream context {stream:?} is not a bidirectional request stream (direction {direction:?})"
    )]
    RequestStreamNotBidi {
        stream: StreamContextId,
        direction: SessionStreamDirection,
    },
    #[error(
        "failed to publish a request body chunk for stream {stream:?} to the upper session FIFO: {error}"
    )]
    BodyChunkPublishFailed {
        stream: StreamContextId,
        error: PublishError,
    },
    #[error("http stream context allocation requires live parent connection context {parent:?}")]
    ParentContextMissing { parent: ContextId },
    #[error(
        "http connection context {context:?} failed to open its control stream through the Session Worker"
    )]
    ControlStreamOpenFailed { context: ContextId },
    #[error("http worker {worker} is outside the configured worker range")]
    WorkerOutOfRange { worker: usize },
    #[error("http worker {worker} is already installed")]
    WorkerAlreadyInstalled { worker: usize },
    #[error("http worker {worker} cannot be accessed")]
    WorkerAccess {
        worker: usize,
        #[source]
        source: ThreadOwnedError,
    },
}

/// Identities returned by
/// [`HttpWorker::classify_peer_uni_stream_reset`]: the reset peer
/// unidirectional stream, its parent connection context, the parent
/// connection's root lower session, and the constant connection-terminating
/// HTTP/3 error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PeerUniStreamReset {
    /// Generation-checked stream context identity of the reset stream.
    pub(crate) stream: StreamContextId,
    /// Parent connection context the reset stream belongs to.
    pub(crate) context: ContextId,
    /// Root QUIC session of the parent connection, which the generic
    /// `SessionWorker::close_connection` action targets.
    pub(crate) session: SessionId,
    /// HTTP/3 error code terminating the connection: always
    /// `ErrorCode::ClosedCriticalStream` (0x0104).
    pub(crate) error_code: ErrorCode,
}

/// Outcome of feeding readable bytes to the registered peer control stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeerControlOutcome {
    /// The SETTINGS frame is not complete yet; the reader buffered all
    /// `consumed` bytes and needs more. The parent connection state is
    /// unchanged.
    Incomplete { consumed: usize },
    /// The SETTINGS frame spanned exactly `consumed` bytes and was valid; the
    /// parent connection's `peer_settings_pending` was cleared. Bytes beyond
    /// `consumed` belong to later control frames and were not read.
    Complete { consumed: usize },
}

/// Errors from [`HttpWorker::process_peer_control_bytes`] and
/// [`HttpWorker::finish_peer_control_stream`].
#[derive(Debug)]
pub(crate) enum PeerControlError {
    /// The stream is not a live, registered peer control stream on a live
    /// parent connection (liveness failure, not a protocol violation).
    Worker(HttpWorkerError),
    /// A SETTINGS protocol error; the caller maps the connection error via
    /// [`ControlStreamError::error_code`] and terminates the connection.
    Protocol(ControlStreamError),
}

impl PeerControlError {
    /// The connection error code for a protocol error; `None` for liveness
    /// errors, which carry no HTTP/3 error code.
    pub(crate) fn error_code(&self) -> Option<ErrorCode> {
        match self {
            PeerControlError::Worker(_) => None,
            PeerControlError::Protocol(error) => Some(error.clone().error_code()),
        }
    }
}

/// Errors from [`HttpWorker::process_request_bytes`].
#[derive(Debug)]
pub(crate) enum RequestReadError {
    /// The stream is not a live bidirectional request stream bound to the
    /// exact session, or its reader slot is missing (liveness failure, not a
    /// protocol violation).
    Worker(HttpWorkerError),
    /// A request-stream protocol error; the caller maps the connection error
    /// via [`RequestFrameError::error_code`] and terminates the connection.
    Protocol(RequestFrameError),
}

impl RequestReadError {
    /// The connection error code for a protocol error; `None` for liveness
    /// errors, which carry no HTTP/3 error code.
    pub(crate) fn error_code(&self) -> Option<ErrorCode> {
        match self {
            RequestReadError::Worker(_) => None,
            RequestReadError::Protocol(error) => Some(error.error_code()),
        }
    }
}

/// Data-worker-owned bounded pools of HTTP/3 connection and stream contexts.
///
/// Owns `ConnectionContext` slots exactly as VPP's `http_worker_t::ctx_pool`
/// does, plus a separate generation-checked `StreamContext` pool mirroring
/// VPP `http_ts_accept_stream` (http.c:675-721); callers resolve identities
/// by `ContextId`/`StreamContextId`, never by raw index. The container
/// (worker installation/attachment) is deferred until Session App callbacks
/// need it.
#[derive(Debug)]
pub(crate) struct HttpWorker {
    contexts: Pool<ConnectionContext>,
    streams: Pool<StreamContext>,
    /// Generation-checked pool of peer control SETTINGS readers, one slot per
    /// connection at most. `ConnectionContext::peer_control_reader` records
    /// the slot, so the 192-byte reader is not embedded in every context or
    /// stream slot; slots are freed when the owning stream or connection
    /// context is released.
    readers: Pool<ControlStreamReader>,
    /// Generation-checked pool of per-stream request-frame readers, one slot
    /// per bidirectional request stream at most. `StreamContext::request_reader`
    /// records the slot, so the reader is not embedded in every stream slot;
    /// the pool is bounded by the stream capacity.
    request_readers: Pool<RequestFrameReader>,
    /// Generation-checked pool of per-stream pending HEADERS field sections,
    /// one slot per bidirectional request stream at most.
    /// `StreamContext::pending_field_section` records the slot; a slot holds
    /// `Some(PendingFieldSection)` — the encoded block plus the exact
    /// lower-FIFO consumed count, one invariant kept across publication
    /// retries — while the section awaits the decode/publish seam and `None`
    /// after it is cleared. The pool is bounded by the stream capacity,
    /// mirroring VPP keeping the received request on the per-request
    /// `http_ctx_t` for the later app-dispatch stage (`req->headers`,
    /// http3.c:835-899).
    pending_field_sections: Pool<Option<PendingFieldSection>>,
}

impl HttpWorker {
    /// Constructs the worker for one data worker id.
    ///
    /// Called once per data worker by the `http_worker_init` worker init
    /// function (listener.rs), mirroring `QuicWorker::new` (quic
    /// worker.rs:684).
    pub(crate) fn new(_worker: DataWorkerId) -> Self {
        Self::with_capacity(HTTP_CONTEXT_CAPACITY)
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self::with_capacities(capacity, HTTP_STREAM_CAPACITY)
    }

    /// Independent connection, stream, and reader pool capacities. The
    /// peer-control reader pool is bounded by the connection capacity: each
    /// connection owns at most one peer control stream and thus one reader.
    /// The request-reader pool is bounded by the stream capacity: each
    /// bidirectional request stream owns at most one request reader.
    pub(crate) fn with_capacities(connections: usize, streams: usize) -> Self {
        Self {
            contexts: Pool::with_capacity(connections),
            streams: Pool::with_capacity(streams),
            readers: Pool::with_capacity(connections),
            request_readers: Pool::with_capacity(streams),
            pending_field_sections: Pool::with_capacity(streams),
        }
    }

    /// Allocates a context slot bound to the exact lower QUIC `session`,
    /// without role metadata (`None`).
    ///
    /// O(1); fails with `ContextCapacityExhausted` when the pool is full.
    pub(crate) fn allocate(&mut self, session: SessionId) -> Result<ContextId, HttpWorkerError> {
        self.allocate_with_role(session, None)
    }

    /// Allocates a context slot bound to the exact lower QUIC `session` and
    /// records its endpoint role, read from accept metadata by the
    /// connection accept path.
    ///
    /// O(1); fails with `ContextCapacityExhausted` when the pool is full.
    pub(crate) fn allocate_with_role(
        &mut self,
        session: SessionId,
        role: Option<SessionEndpointRole>,
    ) -> Result<ContextId, HttpWorkerError> {
        self.contexts
            .insert(ConnectionContext {
                session,
                role,
                local_control: None,
                peer_control: None,
                peer_encoder: None,
                peer_decoder: None,
                peer_settings_pending: true,
                peer_control_reader: None,
            })
            .map(ContextId::from)
            .ok_or(HttpWorkerError::ContextCapacityExhausted {
                capacity: self.contexts.capacity(),
            })
    }

    /// Resolves a live context by its generation-checked identity.
    ///
    /// O(1); rejects stale or out-of-range identities with
    /// `ContextMissing`.
    pub(crate) fn get(&self, context: ContextId) -> Result<&ConnectionContext, HttpWorkerError> {
        self.contexts
            .get(context.into())
            .ok_or(HttpWorkerError::ContextMissing { context })
    }

    /// Resolves a context and verifies it is bound to the exact `session`.
    ///
    /// O(1); rejects mismatched bindings with `SessionMismatch` so a stale
    /// context can never be attributed to a different lower session.
    pub(crate) fn get_for_session(
        &self,
        context: ContextId,
        session: SessionId,
    ) -> Result<&ConnectionContext, HttpWorkerError> {
        let connection = self.get(context)?;
        if connection.session != session {
            return Err(HttpWorkerError::SessionMismatch {
                context,
                expected: session,
                actual: connection.session,
            });
        }
        Ok(connection)
    }

    /// Opens the local HTTP/3 control stream for a live context, exactly once.
    ///
    /// Mirrors VPP `http3_conn_init` (http3.c:216-250), where the control
    /// stream is the first stream opened on a fresh connection. Rejects a
    /// context that already recorded a control stream, invokes
    /// `SessionWorker::open_stream(parent, Uni, app_context)` exactly once,
    /// and records the returned child only after the action succeeds; on
    /// failure the context is left unchanged and the typed error returned
    /// (VPP also reports control-stream open failure without a child,
    /// http3.c:224-230). The control-stream preface bytes are not written
    /// here; that is the FIFO slice.
    pub(crate) fn bootstrap_control_stream(
        &mut self,
        context: ContextId,
        sessions: &mut SessionWorker<Index>,
        app_context: SessionAppContext,
    ) -> Result<SessionId, HttpWorkerError> {
        let connection = self
            .contexts
            .get_mut(context.into())
            .ok_or(HttpWorkerError::ContextMissing { context })?;
        if connection.local_control.is_some() {
            return Err(HttpWorkerError::ControlStreamAlreadyOpen { context });
        }
        let child = sessions
            .open_stream(connection.session, SessionStreamDirection::Uni, app_context)
            .map_err(|_| HttpWorkerError::ControlStreamOpenFailed { context })?;
        connection.local_control = Some(child);
        Ok(child)
    }

    /// Writes the local HTTP/3 control-stream preface into the child control
    /// stream's TX FIFO and publishes a TX-enqueue event.
    ///
    /// Mirrors the ordering of VPP `http3_conn_init` (http3.c:241-246), not
    /// its byte identity (see [`LOCAL_CONTROL_PREFACE`]): after the control
    /// stream is opened, the stream type and SETTINGS frame are written to
    /// the app TX FIFO via `http_io_ts_write`, followed by one
    /// `http_io_ts_after_write (stream, 1)` event. Here the exact 7-byte
    /// preface comes from [`encode_control_preface`], copied in one
    /// `reserve_write` + `copy_from_segments` + `commit` so an
    /// insufficient-capacity shortfall exposes zero bytes, then
    /// [`SessionWorker::publish_tx_enqueue`] raises the child FIFO event
    /// flag and enqueues a TxEnq. O(1): fixed 7-byte stack buffer, one
    /// reservation, one commit, no allocation or copy beyond the preface.
    ///
    /// Event publication is edge-triggered and coalescing:
    /// `publish_tx_enqueue` raises the flag and enqueues one TxEnq only on
    /// the unset→set transition (`Fifo::set_event` returns the transition).
    /// A repeat publish while the flag is still set appends the preface
    /// bytes again but enqueues no second TxEnq until the data worker
    /// consumes the event and clears the flag, so the post-write event
    /// matches VPP's single `http_io_ts_after_write (stream, 1)` even across
    /// repeated calls. Publishing is therefore deliberately not idempotent
    /// as a FIFO write — each call appends bytes — while the single event
    /// follows from the flag edge, not from call counting.
    ///
    /// The caller must have bootstrapped the control stream first
    /// ([`Self::bootstrap_control_stream`]): a context without a recorded
    /// child is a typed error, as is a missing child Session FIFO. An event
    /// publication failure (MQ full) leaves the committed bytes visible with
    /// the FIFO event flag unset (`publish_tx_enqueue` calls `unset_event`
    /// before returning the `RuntimeError`) and never dequeues or drops TX
    /// bytes from the app side.
    pub(crate) fn publish_local_control_preface(
        &self,
        context: ContextId,
        sessions: &SessionWorker<Index>,
    ) -> Result<(), HttpWorkerError> {
        let connection = self.get(context)?;
        let child = connection
            .local_control
            .ok_or(HttpWorkerError::ControlStreamNotOpen { context })?;
        let preface = encode_control_preface()
            .map_err(|_| HttpWorkerError::ControlPrefaceEncodeFailed { context })?;
        let (_, tx_fifo) = sessions
            .fifo_pair(child)
            .ok_or(HttpWorkerError::ControlStreamFifoMissing { context, child })?;
        let mut reservation = tx_fifo
            .reserve_write(preface.len())
            .map_err(|source| HttpWorkerError::ControlPrefaceFifo { context, source })?;
        let copied = reservation
            .copy_from_segments([&preface[..]])
            .map_err(|source| HttpWorkerError::ControlPrefaceFifo { context, source })?;
        let committed = reservation
            .commit(copied)
            .map_err(|source| HttpWorkerError::ControlPrefaceFifo { context, source })?;
        sessions
            .publish_tx_enqueue(child, committed)
            .map_err(|source| HttpWorkerError::ControlPrefaceEventPublishFailed {
                context,
                source,
            })?;
        Ok(())
    }

    /// Releases a context slot back to the pool.
    ///
    /// O(1); the slot's generation advances, so previously issued identities
    /// become stale. Fails with `ContextMissing` for non-live identities. A
    /// released connection frees its SETTINGS reader slot, if one was
    /// allocated.
    pub(crate) fn remove(&mut self, context: ContextId) -> Result<(), HttpWorkerError> {
        let connection = self
            .contexts
            .remove(context.into())
            .ok_or(HttpWorkerError::ContextMissing { context })?;
        if let Some(reader) = connection.peer_control_reader {
            self.readers.remove(reader);
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.contexts.len()
    }

    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.contexts.capacity()
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.contexts.is_empty()
    }

    /// Allocates a stream context bound to child `session` of live connection
    /// context `parent`, with the peer `direction`.
    ///
    /// Mirrors VPP `http_ts_accept_stream` (http.c:675-721), where the stream
    /// context is taken from the same worker's pool, its parent connection
    /// context resolved from the stream session's listener handle, and the
    /// child session/direction/parent recorded on the stream context.
    /// O(1); generation-checks the parent connection before inserting, so a
    /// stale parent identity can never attach a new stream.
    pub(crate) fn allocate_stream(
        &mut self,
        session: SessionId,
        parent: ContextId,
        direction: SessionStreamDirection,
    ) -> Result<StreamContextId, HttpWorkerError> {
        self.contexts
            .get(parent.into())
            .ok_or(HttpWorkerError::ParentContextMissing { parent })?;
        self.streams
            .insert(StreamContext {
                session,
                parent,
                direction,
                peer_uni_type: PeerUniStreamTypeDecode::default(),
                peer_role: PeerUniStreamRole::Unclassified,
                request_reader: None,
                pending_field_section: None,
                body: BodyAccumulator::from(None),
            })
            .map(StreamContextId::from)
            .ok_or(HttpWorkerError::StreamCapacityExhausted {
                capacity: self.streams.capacity(),
            })
    }

    /// Resolves a live stream context by its generation-checked identity.
    ///
    /// O(1); rejects stale or out-of-range identities with `StreamMissing`.
    pub(crate) fn get_stream(
        &self,
        stream: StreamContextId,
    ) -> Result<&StreamContext, HttpWorkerError> {
        self.streams
            .get(stream.into())
            .ok_or(HttpWorkerError::StreamMissing { stream })
    }

    /// Resolves a stream context and verifies it is bound to the exact
    /// `session`.
    ///
    /// O(1); rejects mismatched bindings with `StreamSessionMismatch` so a
    /// stale context can never be attributed to a different lower session.
    pub(crate) fn get_stream_for_session(
        &self,
        stream: StreamContextId,
        session: SessionId,
    ) -> Result<&StreamContext, HttpWorkerError> {
        let stream_context = self.get_stream(stream)?;
        if stream_context.session != session {
            return Err(HttpWorkerError::StreamSessionMismatch {
                stream,
                expected: session,
                actual: stream_context.session,
            });
        }
        Ok(stream_context)
    }

    /// Registers a decoded peer uni stream role with its parent connection,
    /// exactly once per critical owner slot.
    ///
    /// Mirrors the classification switch of VPP
    /// `http3_stream_transport_rx_unknown_type` (http3.c:1680-1726): Control,
    /// QPACK encoder, and QPACK decoder each own one direct
    /// `Option<StreamContextId>` slot on `ConnectionContext`
    /// (`peer_ctrl_stream_index`, `peer_encoder_stream_index`,
    /// `peer_decoder_stream_index`); a second stream claiming an occupied
    /// slot terminates the connection with `HTTP3_ERROR_STREAM_CREATION_ERROR`
    /// (http3.c:1683-1688, 1693-1698, 1703-1708), surfaced here as a typed
    /// error carrying `ErrorCode::StreamCreationError`. `Unknown` stream
    /// types are drained but never registered (http3.c:1723-1725). Push
    /// registration applies VPP's server/client policy (http3.c:1712-1722)
    /// from the connection's endpoint role: rejected with
    /// `StreamCreationError` on a server connection, `IdError` on a client
    /// connection, or `PeerPushRoleMissing` when role metadata is absent —
    /// each typed and recording nothing.
    ///
    /// O(1): resolves the generation-checked stream and parent connection
    /// once each, and records the stream's `peer_role` only after the parent
    /// slot registration succeeds, so a failed duplicate or push leaves both
    /// the current stream role and the parent slots unchanged.
    pub(crate) fn register_peer_uni_stream(
        &mut self,
        stream: StreamContextId,
        role: PeerUniStreamRole,
    ) -> Result<(), HttpWorkerError> {
        let parent = self
            .streams
            .get(stream.into())
            .ok_or(HttpWorkerError::StreamMissing { stream })?
            .parent;
        let connection = self
            .contexts
            .get_mut(parent.into())
            .ok_or(HttpWorkerError::ParentContextMissing { parent })?;
        match role {
            PeerUniStreamRole::Control => {
                if connection.peer_control.is_some() {
                    return Err(HttpWorkerError::PeerStreamRoleDuplicate {
                        stream,
                        context: parent,
                        role,
                        code: ErrorCode::StreamCreationError,
                    });
                }
                connection.peer_control = Some(stream);
            }
            PeerUniStreamRole::QpackEncoder => {
                if connection.peer_encoder.is_some() {
                    return Err(HttpWorkerError::PeerStreamRoleDuplicate {
                        stream,
                        context: parent,
                        role,
                        code: ErrorCode::StreamCreationError,
                    });
                }
                connection.peer_encoder = Some(stream);
            }
            PeerUniStreamRole::QpackDecoder => {
                if connection.peer_decoder.is_some() {
                    return Err(HttpWorkerError::PeerStreamRoleDuplicate {
                        stream,
                        context: parent,
                        role,
                        code: ErrorCode::StreamCreationError,
                    });
                }
                connection.peer_decoder = Some(stream);
            }
            PeerUniStreamRole::Unknown => {
                // Drain-only stream type: VPP's default arm never registers a
                // slot (http3.c:1723-1725).
            }
            PeerUniStreamRole::Push => {
                // VPP's server/client push policy (http3.c:1712-1722)
                // branches on the local endpoint role: a server never
                // accepts a peer push stream, and a client's push is
                // unsupported here; without role metadata there is no policy
                // to apply. Each case is a typed error and records nothing.
                return Err(match connection.role {
                    Some(SessionEndpointRole::Server) => HttpWorkerError::PeerPushRejected {
                        stream,
                        context: parent,
                        code: ErrorCode::StreamCreationError,
                    },
                    Some(SessionEndpointRole::Client) => HttpWorkerError::PeerPushRejected {
                        stream,
                        context: parent,
                        code: ErrorCode::IdError,
                    },
                    None => HttpWorkerError::PeerPushRoleMissing {
                        stream,
                        context: parent,
                    },
                });
            }
            PeerUniStreamRole::Unclassified => {
                return Err(HttpWorkerError::PeerStreamRoleUnclassified {
                    stream,
                    context: parent,
                });
            }
        }
        let recorded = self
            .streams
            .get_mut(stream.into())
            .ok_or(HttpWorkerError::StreamMissing { stream })?;
        recorded.peer_role = role;
        Ok(())
    }

    /// Classifies a reset received on a peer HTTP/3 unidirectional stream.
    ///
    /// Mirrors VPP `http3_transport_stream_reset_callback`, which checks
    /// only unidirectional-ness: Control, QPACK encoder, QPACK decoder,
    /// Unknown, Push, and type-not-yet-decoded (Unclassified) streams all
    /// terminate the connection with `ErrorCode::ClosedCriticalStream`
    /// (0x0104). This method is read-only classification: it generation-
    /// checks the stream context and its parent connection, and returns
    /// copied stream/parent identities plus the parent connection's root
    /// lower session (the `SessionId` the generic `close_connection` action
    /// targets) and the constant error code, mutating nothing. It must not
    /// dispatch the close or clean up yet —
    /// VPP records and dispatches the connection error before any stream
    /// cleanup, which are separate tasks.
    ///
    /// Fails with `StreamMissing` for a stale or out-of-range stream
    /// identity and `ParentContextMissing` for a live stream whose parent
    /// connection is gone; every failure path mutates nothing. O(1): two
    /// generation-checked lookups and a fixed number of copies; no scan,
    /// recursion, allocation, or lock.
    pub(crate) fn classify_peer_uni_stream_reset(
        &self,
        stream: StreamContextId,
    ) -> Result<PeerUniStreamReset, HttpWorkerError> {
        let stream_context = self
            .streams
            .get(stream.into())
            .ok_or(HttpWorkerError::StreamMissing { stream })?;
        let parent = stream_context.parent;
        let connection = self
            .contexts
            .get(parent.into())
            .ok_or(HttpWorkerError::ParentContextMissing { parent })?;
        Ok(PeerUniStreamReset {
            stream,
            context: parent,
            session: connection.session,
            error_code: ErrorCode::ClosedCriticalStream,
        })
    }

    /// Feeds readable bytes of the registered peer control stream into its
    /// SETTINGS reader.
    ///
    /// Mirrors VPP `http3_stream_transport_rx_ctrl` (http3.c:1620-1635) and
    /// `http3_stream_read_settings` (http3.c:1540-1570): the reader enforces
    /// the settings-first rule, accepts an empty SETTINGS frame, and rejects
    /// malformed, semantic, and nonzero-QPACK payloads with a typed
    /// `ControlStreamError`. On `Complete`, exactly the SETTINGS frame bytes
    /// were consumed and the parent connection's `peer_settings_pending` is
    /// cleared; trailing bytes are left for the later control-frame loop. On
    /// `Incomplete` or error, the parent connection state is unchanged; a
    /// call after completion is a second SETTINGS frame (H3_FRAME_UNEXPECTED,
    /// http3.c:1548-1552).
    ///
    /// The SETTINGS reader lives in the worker's `readers` pool, allocated
    /// on the first feed and freed with the stream or connection; feeding
    /// hands over one byte at a time because `ControlStreamReader::push`
    /// does not report how many of the provided bytes it consumed, and the
    /// reader is one-shot. Each push is O(1) (the reader buffers at most 16
    /// header + 128 payload bytes), so the feed is O(frame) with fixed
    /// auxiliary space and never scans trailing bytes.
    pub(crate) fn process_peer_control_bytes(
        &mut self,
        stream: StreamContextId,
        bytes: &[u8],
    ) -> Result<PeerControlOutcome, PeerControlError> {
        let parent = {
            let stream_context = self
                .streams
                .get(stream.into())
                .ok_or(PeerControlError::Worker(HttpWorkerError::StreamMissing {
                    stream,
                }))?;
            if stream_context.peer_role != PeerUniStreamRole::Control {
                return Err(PeerControlError::Worker(
                    HttpWorkerError::PeerControlStreamMismatch {
                        stream,
                        context: stream_context.parent,
                        role: stream_context.peer_role,
                    },
                ));
            }
            stream_context.parent
        };
        let mut connection = *self
            .contexts
            .get(parent.into())
            .ok_or(PeerControlError::Worker(HttpWorkerError::ParentContextMissing {
                parent,
            }))?;
        if !connection.peer_settings_pending {
            return Err(PeerControlError::Protocol(
                ControlStreamError::DuplicateSettings,
            ));
        }
        let reader_index = match connection.peer_control_reader {
            Some(index) => index,
            None => {
                let index = self
                    .readers
                    .insert(ControlStreamReader::new())
                    .ok_or(PeerControlError::Worker(
                        HttpWorkerError::PeerControlReaderCapacityExhausted {
                            stream,
                            context: parent,
                            capacity: self.readers.capacity(),
                        },
                    ))?;
                connection.peer_control_reader = Some(index);
                index
            }
        };
        let reader = self
            .readers
            .get_mut(reader_index)
            .ok_or(PeerControlError::Worker(
                HttpWorkerError::PeerControlReaderMissing {
                    stream,
                    index: reader_index,
                },
            ))?;
        let feed = feed_control_reader(reader, bytes);
        if let Ok((ControlRead::Complete(_), _)) = &feed {
            connection.peer_settings_pending = false;
        }
        // Persist the connection copy: the reader slot on first feed and, on
        // complete SETTINGS, the cleared pending flag. On error the pending
        // flag stays set, so the connection state is unchanged.
        *self
            .contexts
            .get_mut(parent.into())
            .ok_or(PeerControlError::Worker(HttpWorkerError::ParentContextMissing {
                parent,
            }))? = connection;
        match feed {
            Ok((ControlRead::Complete(_), consumed)) => {
                Ok(PeerControlOutcome::Complete { consumed })
            }
            Ok((ControlRead::Incomplete, consumed)) => Ok(PeerControlOutcome::Incomplete {
                consumed,
            }),
            Err(error) => Err(PeerControlError::Protocol(error)),
        }
    }

    /// Feeds readable bytes of a bidirectional request stream into its
    /// request-frame reader.
    ///
    /// Mirrors VPP `http3_stream_transport_rx_req` (http3.c:1732-1799): the
    /// request's frame-header staging (`fh`), phase (`req_state`), and
    /// dispatch callback live on the per-request `http_ctx_t` owned by the
    /// data worker and are looked up generation-checked per readable segment,
    /// and the call returns the exact number of consumed bytes
    /// (`max_deq - left_deq`) so trailing bytes stay with the caller. Here
    /// the [`RequestFrameReader`] is allocated lazily into the worker's
    /// `request_readers` pool on the first feed and recorded on the stream
    /// context; a completed `Headers` field section is returned by value
    /// with the worker-owned retention sink
    /// ([`HttpWorker::retain_pending_field_section`]) as its destination, so
    /// it is never silently dropped.
    ///
    /// Generation/session-checks the stream context and rejects non-bidi
    /// streams with `RequestStreamNotBidi`. Reader protocol errors surface as
    /// `RequestReadError::Protocol` with the HTTP/3 error code; worker
    /// liveness failures as `RequestReadError::Worker`. O(bytes) with O(1)
    /// generation-checked lookups and state beyond the reader's single
    /// bounded HEADERS allocation; no loop over multiple frames, no FIFO
    /// access, no lock.
    pub(crate) fn process_request_bytes<'a>(
        &mut self,
        stream: StreamContextId,
        session: SessionId,
        bytes: &'a [u8],
    ) -> Result<(RequestFrameRead<'a>, usize), RequestReadError> {
        let mut stream_context = *self
            .streams
            .get(stream.into())
            .ok_or(RequestReadError::Worker(HttpWorkerError::StreamMissing {
                stream,
            }))?;
        if stream_context.session != session {
            return Err(RequestReadError::Worker(HttpWorkerError::StreamSessionMismatch {
                stream,
                expected: session,
                actual: stream_context.session,
            }));
        }
        if stream_context.direction != SessionStreamDirection::Bidi {
            return Err(RequestReadError::Worker(
                HttpWorkerError::RequestStreamNotBidi {
                    stream,
                    direction: stream_context.direction,
                },
            ));
        }
        let reader_index = match stream_context.request_reader {
            Some(index) => index,
            None => {
                let index = self
                    .request_readers
                    .insert(RequestFrameReader::new())
                    .ok_or(RequestReadError::Worker(
                        HttpWorkerError::RequestReaderCapacityExhausted {
                            stream,
                            capacity: self.request_readers.capacity(),
                        },
                    ))?;
                stream_context.request_reader = Some(index);
                index
            }
        };
        let reader = self
            .request_readers
            .get_mut(reader_index)
            .ok_or(RequestReadError::Worker(
                HttpWorkerError::RequestReaderMissing {
                    stream,
                    index: reader_index,
                },
            ))?;
        let feed = reader.push(bytes);
        // Persist the stream copy: the reader slot on first feed. On error
        // the slot stays recorded, so the stream dies with its reader, as
        // VPP's per-request state does.
        *self
            .streams
            .get_mut(stream.into())
            .ok_or(RequestReadError::Worker(HttpWorkerError::StreamMissing { stream }))? =
            stream_context;
        feed.map_err(RequestReadError::Protocol)
    }

    /// Installs the declared Content-Length of a bidirectional request
    /// stream into its body accumulator.
    ///
    /// Called once per request when HEADERS completes: `Some(n)` declares a
    /// length (a declared zero is immediately complete), `None` records a
    /// body-less request in which DATA is unexpected (RFC 9114 Section 4.1).
    /// Mirrors VPP installing `req->fh.length`/`to_recv` at the transport
    /// method (`http3_req_state_wait_transport_method`, http3.c:835-899);
    /// the accumulator itself mirrors the `to_recv` accounting of
    /// `http3_req_state_transport_io_more_data` (http3.c:1184-1263).
    /// Generation/session-checks the stream and rejects non-bidi streams
    /// with `RequestStreamNotBidi`; every failure path mutates nothing.
    /// O(1): one generation-checked lookup and write, no lock or allocation.
    pub(crate) fn install_request_body_length(
        &mut self,
        stream: StreamContextId,
        session: SessionId,
        declared: Option<u64>,
    ) -> Result<(), HttpWorkerError> {
        let mut stream_context = *self
            .streams
            .get(stream.into())
            .ok_or(HttpWorkerError::StreamMissing { stream })?;
        if stream_context.session != session {
            return Err(HttpWorkerError::StreamSessionMismatch {
                stream,
                expected: session,
                actual: stream_context.session,
            });
        }
        if stream_context.direction != SessionStreamDirection::Bidi {
            return Err(HttpWorkerError::RequestStreamNotBidi {
                stream,
                direction: stream_context.direction,
            });
        }
        stream_context.body = BodyAccumulator::from(declared);
        *self
            .streams
            .get_mut(stream.into())
            .ok_or(HttpWorkerError::StreamMissing { stream })? = stream_context;
        Ok(())
    }

    /// Publishes one borrowed DATA `chunk` of a bidirectional request stream
    /// to the upper Session FIFO, all-or-nothing.
    ///
    /// Mirrors the bounded app write of VPP
    /// `http3_req_state_transport_io_more_data` (http3.c:1184-1263): the
    /// body accounting rejects a chunk that overruns the declared remaining
    /// length (`GeneralProtocolError`) before any FIFO mutation; a FIFO
    /// that cannot hold the whole chunk returns `Capacity` with the body
    /// unchanged, the dequeue notification armed, and the chunk still with
    /// the caller; only after publication commits is the accumulator
    /// advanced. The lower transport FIFO is never consumed here — the
    /// caller owns the dequeue after success. `RequestReadError::Protocol`
    /// carries the HTTP/3 error code; worker liveness failures (stale
    /// identity, foreign session, non-bidi stream) arrive as
    /// `RequestReadError::Worker` and mutate nothing. O(1): one
    /// generation-checked lookup, one bounded FIFO write, no scan, lock,
    /// or allocation.
    pub(crate) fn process_request_data(
        &mut self,
        stream: StreamContextId,
        session: SessionId,
        upper_rx: &Fifo,
        chunk: &[u8],
    ) -> Result<(), RequestReadError> {
        let mut stream_context = *self
            .streams
            .get(stream.into())
            .ok_or(RequestReadError::Worker(HttpWorkerError::StreamMissing {
                stream,
            }))?;
        if stream_context.session != session {
            return Err(RequestReadError::Worker(HttpWorkerError::StreamSessionMismatch {
                stream,
                expected: session,
                actual: stream_context.session,
            }));
        }
        if stream_context.direction != SessionStreamDirection::Bidi {
            return Err(RequestReadError::Worker(
                HttpWorkerError::RequestStreamNotBidi {
                    stream,
                    direction: stream_context.direction,
                },
            ));
        }
        stream_context
            .body
            .on_data(chunk.len() as u64)
            .map_err(|error| RequestReadError::Protocol(RequestFrameError::Phase(error.into())))?;
        publish_body_chunk(upper_rx, chunk).map_err(|error| {
            RequestReadError::Worker(HttpWorkerError::BodyChunkPublishFailed { stream, error })
        })?;
        // Persist the body advance only after publication committed; on a
        // rejection or capacity/backpressure error the pool slot keeps its
        // exact pre-call accumulator.
        *self
            .streams
            .get_mut(stream.into())
            .ok_or(RequestReadError::Worker(HttpWorkerError::StreamMissing { stream }))? =
            stream_context;
        Ok(())
    }

    /// Validates a bidirectional request stream's FIN against its declared
    /// body: a half-closed stream with a declared but unreceived body is
    /// `RequestIncomplete` (RFC 9114 Section 4.1), mirroring VPP
    /// `http3_req_state_transport_io_more_data` terminating the stream with
    /// `HTTP3_ERROR_REQUEST_INCOMPLETE` when the transport is half-closed,
    /// has no more data, and `to_recv` is still pending (http3.c:1244-1252).
    ///
    /// Validates only — the stream is left live and unreleased on both
    /// outcomes; the caller owns any termination and the
    /// [`HttpWorker::release_request_stream`] removal, preserving existing
    /// release behavior. `RequestReadError::Protocol` carries
    /// `RequestIncomplete`; worker liveness failures (stale identity,
    /// foreign session, non-bidi stream) arrive as `RequestReadError::Worker`
    /// and mutate nothing. O(1): one generation-checked lookup, no lock or
    /// allocation.
    pub(crate) fn validate_request_finish(
        &self,
        stream: StreamContextId,
        session: SessionId,
    ) -> Result<(), RequestReadError> {
        let stream_context = self
            .streams
            .get(stream.into())
            .ok_or(RequestReadError::Worker(HttpWorkerError::StreamMissing {
                stream,
            }))?;
        if stream_context.session != session {
            return Err(RequestReadError::Worker(HttpWorkerError::StreamSessionMismatch {
                stream,
                expected: session,
                actual: stream_context.session,
            }));
        }
        if stream_context.direction != SessionStreamDirection::Bidi {
            return Err(RequestReadError::Worker(
                HttpWorkerError::RequestStreamNotBidi {
                    stream,
                    direction: stream_context.direction,
                },
            ));
        }
        stream_context
            .body
            .finish(true)
            .map_err(|error| RequestReadError::Protocol(RequestFrameError::Phase(error.into())))
    }

    /// Retains a completed HEADERS field section in the worker-local pending
    /// slot of the generation-checked request stream.
    ///
    /// The RX seam packages the encoded block returned by
    /// [`HttpWorker::process_request_bytes`] with the exact lower-FIFO
    /// consumed count into a [`PendingFieldSection`] — one invariant kept
    /// across publication retries — and retains it in the stream's pending
    /// slot, mirroring VPP recording the received request on the per-request
    /// `http_ctx_t` owned by the data worker (`req->headers`, set in
    /// `http3_req_state_wait_transport_method`, http3.c:835-899) for the
    /// later app-dispatch stage. A section already pending (an optional
    /// trailer completing while the initial section was not yet cleared) is
    /// rejected with `PendingFieldSectionOverflow` and the rejected section
    /// is returned in the error — never silently replaced or discarded; the
    /// slot content is replaced only after the previous section was
    /// explicitly cleared. O(1); generation/session-checks the stream, then
    /// at most one pool operation; no lock, no FIFO, no allocation.
    pub(crate) fn retain_pending_field_section(
        &mut self,
        stream: StreamContextId,
        session: SessionId,
        section: PendingFieldSection,
    ) -> Result<(), HttpWorkerError> {
        let mut stream_context = *self
            .streams
            .get(stream.into())
            .ok_or(HttpWorkerError::StreamMissing { stream })?;
        if stream_context.session != session {
            return Err(HttpWorkerError::StreamSessionMismatch {
                stream,
                expected: session,
                actual: stream_context.session,
            });
        }
        match stream_context.pending_field_section {
            None => {
                let slot = self
                    .pending_field_sections
                    .insert(Some(section))
                    .ok_or(HttpWorkerError::PendingFieldSectionCapacityExhausted {
                        stream,
                        capacity: self.pending_field_sections.capacity(),
                    })?;
                stream_context.pending_field_section = Some(slot);
                *self
                    .streams
                    .get_mut(stream.into())
                    .ok_or(HttpWorkerError::StreamMissing { stream })? =
                    stream_context;
            }
            Some(slot) => {
                let pending = self
                    .pending_field_sections
                    .get_mut(slot)
                    .ok_or(HttpWorkerError::PendingFieldSectionMissing {
                        stream,
                        index: slot,
                    })?;
                if pending.is_some() {
                    return Err(HttpWorkerError::PendingFieldSectionOverflow {
                        stream,
                        section,
                    });
                }
                *pending = Some(section);
            }
        }
        Ok(())
    }

    /// Borrows the pending HEADERS field section of a generation-checked
    /// request stream, if any, without consuming it.
    ///
    /// The decode/publish callback seam inspects the retained encoded block
    /// and its exact lower-FIFO consumed count across publication retries
    /// without moving ownership: the same bytes and count stay observable
    /// until the seam explicitly clears the slot. Mirrors the app-dispatch
    /// stage reading the received request off the per-request `http_ctx_t`
    /// in VPP (`http3_req_state_wait_app_method`, http3.c:456-475), whose
    /// state is freed as the stream closes (`http3_stream_free_req`,
    /// http3.c:59-78). `Ok(None)` when nothing is pending; a stale stream
    /// identity is rejected with `StreamMissing`, so a reused slot can never
    /// be observed by an old stream. O(1); generation/session-checks the
    /// stream, then one generation-checked pool access; no copy, no lock, no
    /// FIFO, no allocation.
    pub(crate) fn pending_field_section(
        &self,
        stream: StreamContextId,
        session: SessionId,
    ) -> Result<Option<&PendingFieldSection>, HttpWorkerError> {
        let stream_context = self
            .streams
            .get(stream.into())
            .ok_or(HttpWorkerError::StreamMissing { stream })?;
        if stream_context.session != session {
            return Err(HttpWorkerError::StreamSessionMismatch {
                stream,
                expected: session,
                actual: stream_context.session,
            });
        }
        let Some(slot) = stream_context.pending_field_section else {
            return Ok(None);
        };
        let pending = self
            .pending_field_sections
            .get(slot)
            .ok_or(HttpWorkerError::PendingFieldSectionMissing {
                stream,
                index: slot,
            })?;
        Ok(pending.as_ref())
    }

    /// Clears the pending HEADERS field section of a generation-checked
    /// request stream, if any, keeping the recorded slot for a later trailer
    /// section or a publication retry.
    ///
    /// The decode/publish callback seam clears the slot once the encoded
    /// block and its exact lower-FIFO consumed count are no longer needed;
    /// the slot stays recorded so a later trailer HEADERS (RFC 9114
    /// Section 4.1) refills it, mirroring VPP reusing `req->headers` on the
    /// same per-request `http_ctx_t` for the trailer section. A stale stream
    /// identity or foreign session is rejected before any mutation. O(1);
    /// generation/session-checks the stream, then one generation-checked
    /// pool write; no lock, no FIFO, no allocation.
    pub(crate) fn clear_pending_field_section(
        &mut self,
        stream: StreamContextId,
        session: SessionId,
    ) -> Result<(), HttpWorkerError> {
        let stream_context = self
            .streams
            .get(stream.into())
            .ok_or(HttpWorkerError::StreamMissing { stream })?;
        if stream_context.session != session {
            return Err(HttpWorkerError::StreamSessionMismatch {
                stream,
                expected: session,
                actual: stream_context.session,
            });
        }
        let Some(slot) = stream_context.pending_field_section else {
            return Ok(());
        };
        let pending = self
            .pending_field_sections
            .get_mut(slot)
            .ok_or(HttpWorkerError::PendingFieldSectionMissing {
                stream,
                index: slot,
            })?;
        *pending = None;
        Ok(())
    }

    /// Drains readable bytes of a registered drain-only peer uni stream
    /// (QPACK encoder, QPACK decoder, or Unknown) and returns the number of
    /// bytes drained, always the whole slice.
    ///
    /// Under the static-only QPACK (capacity-zero) policy there is no dynamic
    /// QPACK state, so every provided byte is discarded without inspection,
    /// mirroring VPP `http3_stream_transport_rx_unknown_type`
    /// (http3.c:1723-1725), which drains unknown frame payloads on
    /// drain-only streams. The caller dequeues exactly the returned byte
    /// count from the stream FIFO. O(1); bounded by the provided bytes.
    pub(crate) fn drain_peer_stream_bytes(
        &self,
        stream: StreamContextId,
        bytes: &[u8],
    ) -> Result<usize, HttpWorkerError> {
        let stream_context = self
            .streams
            .get(stream.into())
            .ok_or(HttpWorkerError::StreamMissing { stream })?;
        if !matches!(
            stream_context.peer_role,
            PeerUniStreamRole::QpackEncoder
                | PeerUniStreamRole::QpackDecoder
                | PeerUniStreamRole::Unknown
        ) {
            return Err(HttpWorkerError::PeerStreamNotDrainable {
                stream,
                context: stream_context.parent,
                role: stream_context.peer_role,
            });
        }
        Ok(bytes.len())
    }

    /// Releases a stream context slot back to the pool.
    ///
    /// O(1); the slot's generation advances, so previously issued identities
    /// become stale. Fails with `StreamMissing` for non-live identities. The
    /// stream's lazily allocated request reader slot is freed with the
    /// stream, mirroring VPP `http3_stream_free_req` (http3.c:59-78), which
    /// frees the per-request state as the stream closes and clears the
    /// stream's request index; here the recorded index dies with the removed
    /// context. If the released stream was the registered peer control
    /// stream, its SETTINGS reader slot is freed; if the parent connection
    /// is already released, `remove` freed it.
    pub(crate) fn remove_stream(&mut self, stream: StreamContextId) -> Result<(), HttpWorkerError> {
        let removed = self
            .streams
            .remove(stream.into())
            .ok_or(HttpWorkerError::StreamMissing { stream })?;
        // `Pool::remove` is generation-checked, so a stale reader index can
        // never free a live slot.
        if let Some(reader) = removed.request_reader {
            self.request_readers.remove(reader);
        }
        // The pending field-section slot dies with the stream, mirroring
        // `http3_stream_free_req` freeing the per-request state as the
        // stream closes; the recorded index can never free a live slot.
        if let Some(slot) = removed.pending_field_section {
            self.pending_field_sections.remove(slot);
        }
        if let Some(connection) = self.contexts.get_mut(removed.parent.into()) {
            if connection.peer_control == Some(stream) {
                if let Some(reader) = connection.peer_control_reader.take() {
                    self.readers.remove(reader);
                }
            }
        }
        Ok(())
    }

    /// Releases a bidirectional request stream exactly once, as the
    /// worker-owned boundary for the later FIN/reset/cleanup callback.
    ///
    /// Mirrors VPP `http3_stream_cleanup_callback` (http3.c:2440-2463):
    /// cleanup frees the bidi request stream's per-request state
    /// (`http3_stream_free_req`, http3.c:59-78), while parent-request and
    /// unidirectional cases return early. The request-role guard here is
    /// the generation/session-checked stream context itself: the identity
    /// must resolve to the exact `session` and be bidirectional, after
    /// which the existing [`HttpWorker::remove_stream`] ownership path
    /// frees the request reader and pending field-section slots and
    /// releases the context, advancing the slot's generation.
    ///
    /// Idempotency is pinned like [`HttpWorker::remove_stream`]: a
    /// repeated release of the same identity fails with `StreamMissing`
    /// and mutates nothing, and a live context bound to a different
    /// `session` fails with `StreamSessionMismatch`, also mutating
    /// nothing. A stale identity can never release a reused slot: the
    /// identity dies with the released context, and removal is
    /// generation-checked.
    ///
    /// O(1): one generation-checked stream lookup with the session check,
    /// a constant direction check, and the generation-checked removal
    /// path; no scan, allocation, recursion, or lock.
    pub(crate) fn release_request_stream(
        &mut self,
        stream: StreamContextId,
        session: SessionId,
    ) -> Result<(), HttpWorkerError> {
        let direction = self.get_stream_for_session(stream, session)?.direction;
        if direction != SessionStreamDirection::Bidi {
            return Err(HttpWorkerError::RequestStreamNotBidi { stream, direction });
        }
        self.remove_stream(stream)
    }

    /// Accepts peer EOF on the registered peer control stream.
    ///
    /// Mirrors VPP `http3_transport_stream_close_callback` →
    /// `http3_stream_close`: EOF on the peer control stream is accepted
    /// silently both before and after SETTINGS. The peer control slot and
    /// its SETTINGS reader are freed, and the stream context is released
    /// directly; `peer_settings_pending` is left exactly as it is — an EOF
    /// before SETTINGS keeps the expectation pending, and there is no
    /// MISSING_SETTINGS error and no connection-close action here (the
    /// caller owns any connection policy).
    ///
    /// Fails with `PeerControlError::Worker` for a stale stream identity, a
    /// live non-Control stream, a missing parent, or a stream that is not
    /// the parent's registered `peer_control`; every failure path mutates
    /// nothing. O(1): two generation-checked lookups and a fixed number of
    /// pool operations; no scan, recursion, allocation, or lock.
    pub(crate) fn finish_peer_control_stream(
        &mut self,
        stream: StreamContextId,
    ) -> Result<(), PeerControlError> {
        let parent = {
            let stream_context = self
                .streams
                .get(stream.into())
                .ok_or(PeerControlError::Worker(HttpWorkerError::StreamMissing {
                    stream,
                }))?;
            if stream_context.peer_role != PeerUniStreamRole::Control {
                return Err(PeerControlError::Worker(
                    HttpWorkerError::PeerControlStreamMismatch {
                        stream,
                        context: stream_context.parent,
                        role: stream_context.peer_role,
                    },
                ));
            }
            stream_context.parent
        };
        let connection = self
            .contexts
            .get_mut(parent.into())
            .ok_or(PeerControlError::Worker(HttpWorkerError::ParentContextMissing {
                parent,
            }))?;
        if connection.peer_control != Some(stream) {
            return Err(PeerControlError::Worker(
                HttpWorkerError::PeerControlStreamMismatch {
                    stream,
                    context: parent,
                    role: PeerUniStreamRole::Control,
                },
            ));
        }
        if let Some(reader) = connection.peer_control_reader.take() {
            self.readers.remove(reader);
        }
        connection.peer_control = None;
        self.streams
            .remove(stream.into())
            .ok_or(PeerControlError::Worker(HttpWorkerError::StreamMissing {
                stream,
            }))?;
        Ok(())
    }

    /// Accepts peer EOF on a registered peer QPACK encoder or decoder stream.
    ///
    /// Mirrors VPP `http3_transport_stream_close_callback` → `http3_stream_close`:
    /// plain EOF on either peer QPACK critical stream is silent and non-fatal.
    /// `CLOSED_CRITICAL_STREAM` is raised for a RESET of a critical stream
    /// only, never for EOF, so no error code is produced here; the matching
    /// `peer_encoder` or `peer_decoder` slot is cleared and the stream context
    /// is released directly, leaving the other QPACK slot, the control slot,
    /// and the connection state unchanged. The role comes from the
    /// generation-checked stream context, never from an untrusted caller
    /// argument, and the corresponding slot must exactly equal this stream.
    ///
    /// Fails with `StreamMissing` for a stale stream identity, with
    /// `PeerQpackStreamMismatch` (carrying the actual role) for a live stream
    /// that is not a QPACK encoder or decoder, with `ParentContextMissing`
    /// when the parent is gone, and with `PeerQpackStreamMismatch` when the
    /// parent slot holds a different stream; every failure path mutates
    /// nothing. O(1): two generation-checked lookups and a fixed number of
    /// pool operations; no scan, recursion, allocation, or lock.
    pub(crate) fn finish_peer_qpack_stream(
        &mut self,
        stream: StreamContextId,
    ) -> Result<(), HttpWorkerError> {
        let (parent, role) = {
            let stream_context = self
                .streams
                .get(stream.into())
                .ok_or(HttpWorkerError::StreamMissing { stream })?;
            if !matches!(
                stream_context.peer_role,
                PeerUniStreamRole::QpackEncoder | PeerUniStreamRole::QpackDecoder
            ) {
                return Err(HttpWorkerError::PeerQpackStreamMismatch {
                    stream,
                    context: stream_context.parent,
                    role: stream_context.peer_role,
                });
            }
            (stream_context.parent, stream_context.peer_role)
        };
        let connection = self
            .contexts
            .get_mut(parent.into())
            .ok_or(HttpWorkerError::ParentContextMissing { parent })?;
        let slot = match role {
            PeerUniStreamRole::QpackEncoder => &mut connection.peer_encoder,
            PeerUniStreamRole::QpackDecoder => &mut connection.peer_decoder,
            role => {
                return Err(HttpWorkerError::PeerQpackStreamMismatch {
                    stream,
                    context: parent,
                    role,
                });
            }
        };
        if *slot != Some(stream) {
            return Err(HttpWorkerError::PeerQpackStreamMismatch {
                stream,
                context: parent,
                role,
            });
        }
        *slot = None;
        self.streams
            .remove(stream.into())
            .ok_or(HttpWorkerError::StreamMissing { stream })?;
        Ok(())
    }

    /// Accepts peer EOF on a registered peer Unknown (drain-only) uni stream.
    ///
    /// Mirrors VPP `http3_transport_stream_close_callback` → `http3_stream_close`:
    /// plain EOF on a stream of an unknown type is silent and non-fatal, and
    /// VPP's default arm never registers an `Unknown` slot
    /// (http3.c:1723-1725), so only the stream context is released directly;
    /// the peer control and QPACK slots, the SETTINGS reader, and the
    /// connection state are all unchanged, and no error code is produced. The
    /// role comes from the generation-checked stream context, never from an
    /// untrusted caller argument.
    ///
    /// Fails with `StreamMissing` for a stale stream identity, with
    /// `PeerUnknownStreamMismatch` (carrying the actual role) for a live
    /// stream that is not `Unknown`, and with `ParentContextMissing` when the
    /// parent connection context is gone; every failure path mutates nothing.
    /// O(1): two generation-checked lookups and a fixed number of pool
    /// operations; no scan, recursion, allocation, or lock.
    pub(crate) fn finish_peer_unknown_stream(
        &mut self,
        stream: StreamContextId,
    ) -> Result<(), HttpWorkerError> {
        let parent = {
            let stream_context = self
                .streams
                .get(stream.into())
                .ok_or(HttpWorkerError::StreamMissing { stream })?;
            if stream_context.peer_role != PeerUniStreamRole::Unknown {
                return Err(HttpWorkerError::PeerUnknownStreamMismatch {
                    stream,
                    context: stream_context.parent,
                    role: stream_context.peer_role,
                });
            }
            stream_context.parent
        };
        self.contexts
            .get(parent.into())
            .ok_or(HttpWorkerError::ParentContextMissing { parent })?;
        self.streams
            .remove(stream.into())
            .ok_or(HttpWorkerError::StreamMissing { stream })?;
        Ok(())
    }

    #[inline]
    pub(crate) fn stream_len(&self) -> usize {
        self.streams.len()
    }

    #[inline]
    pub(crate) fn stream_capacity(&self) -> usize {
        self.streams.capacity()
    }

    #[inline]
    pub(crate) fn streams_is_empty(&self) -> bool {
        self.streams.is_empty()
    }
}

/// Feeds the peer control stream's readable bytes into the connection's
/// one-shot SETTINGS reader, one byte at a time, and returns the first
/// non-`Incomplete` result together with the exact number of bytes consumed:
/// `Complete` with the SETTINGS frame's length, `Incomplete` with the whole
/// slice (the reader buffered everything), or the typed error that ended the
/// read.
///
/// `ControlStreamReader::push` is fixed-buffer and one-shot and does not
/// report how many of the bytes it was handed belong to the completed frame,
/// so a single push of the whole slice would silently swallow later control
/// frames on `Complete`. One push per byte makes the consumed count exact:
/// every push before the terminal one returned `Incomplete`, so the frame
/// spans exactly the fed prefix and trailing bytes stay in the stream FIFO
/// for the later control-frame loop (RFC 9114 Section 6.2.1).
fn feed_control_reader(
    reader: &mut ControlStreamReader,
    bytes: &[u8],
) -> Result<(ControlRead, usize), ControlStreamError> {
    for (index, byte) in bytes.iter().enumerate() {
        let read = reader.push(std::slice::from_ref(byte))?;
        if !matches!(read, ControlRead::Incomplete) {
            return Ok((read, index + 1));
        }
    }
    Ok((ControlRead::Incomplete, bytes.len()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(slot: u32, generation: u32) -> SessionId {
        SessionId::from_raw(u64::from(slot) | (u64::from(generation) << 32))
    }

    use std::cell::Cell;

    use hammer_runtime::app::{AppSessionConfig, SessionAppContext};
    use hammer_runtime::session::{SessionApplicationErrorCode, SessionStreamDirection};
    use hammer_runtime::{RuntimeError, RuntimeResult};
    use hammer_service::session::application::ApplicationMain;
    use hammer_service::session::runtime::{SessionTransportId, SessionTransportWorkerActions};

    use crate::http3::proto::frame::FrameType;

    const ACTION_TRANSPORT: SessionTransportId = SessionTransportId::new(0);

    /// Per-thread observation of the fake `open_stream` action: invocation
    /// count, the passed `app_context`, and whether the next call fails.
    thread_local! {
        static OPEN_CALLS: Cell<u32> = const { Cell::new(0) };
        static OPEN_CONTEXT: Cell<u64> = const { Cell::new(0) };
        static OPEN_FAIL: Cell<bool> = const { Cell::new(false) };
    }

    fn fake_open_stream(
        _sessions: &mut SessionWorker<Index>,
        parent: SessionId,
        direction: SessionStreamDirection,
        app_context: SessionAppContext,
    ) -> RuntimeResult<SessionId> {
        OPEN_CALLS.with(|calls| calls.set(calls.get() + 1));
        OPEN_CONTEXT.with(|seen| seen.set(app_context));
        assert_eq!(direction, SessionStreamDirection::Uni);
        if OPEN_FAIL.with(|fail| fail.get()) {
            return Err(RuntimeError::ServiceClosed);
        }
        Ok(SessionId::from_raw(parent.get() + 1))
    }

    fn fake_reset_stream(
        _sessions: &mut SessionWorker<Index>,
        _session_id: SessionId,
        _code: SessionApplicationErrorCode,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn fake_stop_sending(
        _sessions: &mut SessionWorker<Index>,
        _session_id: SessionId,
        _code: SessionApplicationErrorCode,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn fake_close_connection(
        _sessions: &mut SessionWorker<Index>,
        _session_id: SessionId,
        _code: SessionApplicationErrorCode,
        _reason: &[u8],
    ) -> RuntimeResult<()> {
        Ok(())
    }

    /// An `HttpWorker` with one context allocated for a real Session-worker
    /// parent Session whose transport has the fake action table installed.
    fn harness() -> (HttpWorker, SessionWorker<Index>, ContextId, SessionId) {
        let applications = ApplicationMain::new(4);
        let application = applications.attach().expect("attach test Application");
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            64,
            applications,
            None,
        )
        .expect("construct Session worker");
        sessions
            .install_transport_actions(
                ACTION_TRANSPORT,
                SessionTransportWorkerActions::new(
                    fake_open_stream,
                    fake_reset_stream,
                    fake_stop_sending,
                    fake_close_connection,
                ),
            )
            .expect("install transport actions");
        sessions
            .install_application_mq_for_test(application)
            .expect("install Application Rx MQ");
        let parent = sessions
            .construct_transport_session(
                ACTION_TRANSPORT,
                Index::new(3, 1),
                0xCAFE_1234,
                application,
                None,
                None,
                None,
                false,
            )
            .expect("construct parent transport Session");
        let mut worker = HttpWorker::with_capacity(4);
        let context = worker.allocate(parent).expect("allocate context");
        (worker, sessions, context, parent)
    }

    #[test]
    fn allocate_returns_generation_safe_id_bound_to_exact_session() {
        let mut worker = HttpWorker::with_capacity(4);
        let lower = session(3, 1);
        let context = worker.allocate(lower).expect("allocate succeeds");
        assert_eq!(worker.get(context).expect("live context").session, lower);
        assert_eq!(ContextId::from(Index::from(context)), context);
        assert_eq!(ContextId::from(u64::from(context)), context);
        let index = Index::from(context);
        assert_eq!(index.slot(), 0);
        assert_eq!(index.generation(), 1);
        assert_eq!(worker.len(), 1);
        assert_eq!(worker.capacity(), 4);
        assert!(!worker.is_empty());
    }

    #[test]
    fn distinct_allocations_keep_distinct_bindings() {
        let mut worker = HttpWorker::with_capacity(4);
        let first = worker.allocate(session(1, 1)).expect("allocate first");
        let second = worker.allocate(session(2, 1)).expect("allocate second");
        assert_ne!(first, second);
        assert_eq!(
            worker.get(first).expect("first live").session,
            session(1, 1)
        );
        assert_eq!(
            worker.get(second).expect("second live").session,
            session(2, 1)
        );
    }

    #[test]
    fn stale_id_rejected_after_remove_and_reuse() {
        let mut worker = HttpWorker::with_capacity(4);
        let first = worker.allocate(session(5, 1)).expect("allocate");
        worker.remove(first).expect("remove");
        let second = worker.allocate(session(7, 2)).expect("reuse slot");
        assert_ne!(first, second);
        assert_eq!(Index::from(second).slot(), Index::from(first).slot());
        assert_ne!(
            Index::from(second).generation(),
            Index::from(first).generation()
        );
        assert!(matches!(
            worker.get(first),
            Err(HttpWorkerError::ContextMissing { context: c }) if c == first
        ));
        assert_eq!(
            worker.get(second).expect("new identity live").session,
            session(7, 2)
        );
    }

    #[test]
    fn capacity_exhaustion_is_typed_error() {
        let mut worker = HttpWorker::with_capacity(1);
        worker.allocate(session(1, 1)).expect("first slot");
        assert!(matches!(
            worker.allocate(session(2, 1)),
            Err(HttpWorkerError::ContextCapacityExhausted { capacity: 1 })
        ));
        let mut empty = HttpWorker::with_capacity(0);
        assert!(empty.is_empty());
        assert!(matches!(
            empty.allocate(session(3, 1)),
            Err(HttpWorkerError::ContextCapacityExhausted { capacity: 0 })
        ));
    }

    #[test]
    fn session_mismatch_rejected_by_direct_id_lookup() {
        let mut worker = HttpWorker::with_capacity(2);
        let bound = session(1, 1);
        let other = session(2, 1);
        let context = worker.allocate(bound).expect("allocate");
        assert!(matches!(
            worker.get_for_session(context, other),
            Err(HttpWorkerError::SessionMismatch {
                context: c,
                expected: e,
                actual: a,
            }) if c == context && e == other && a == bound
        ));
        assert_eq!(
            worker
                .get_for_session(context, bound)
                .expect("exact session")
                .session,
            bound
        );
    }

    #[test]
    fn remove_missing_or_stale_identity_is_typed_error() {
        let mut worker = HttpWorker::with_capacity(2);
        let bogus = ContextId::from(u64::MAX);
        assert!(matches!(
            worker.remove(bogus),
            Err(HttpWorkerError::ContextMissing { context: c }) if c == bogus
        ));
        let context = worker.allocate(session(1, 1)).expect("allocate");
        worker.remove(context).expect("remove");
        assert!(matches!(
            worker.remove(context),
            Err(HttpWorkerError::ContextMissing { context: c }) if c == context
        ));
    }

    #[test]
    fn fresh_context_has_no_local_control_and_awaits_peer_settings() {
        let mut worker = HttpWorker::with_capacity(4);
        let lower = session(3, 1);
        let context = worker.allocate(lower).expect("allocate context");
        let connection = worker.get(context).expect("live context");
        assert_eq!(connection.local_control, None);
        assert_eq!(connection.peer_control, None);
        assert_eq!(connection.peer_encoder, None);
        assert_eq!(connection.peer_decoder, None);
        assert!(connection.peer_settings_pending);
    }

    #[test]
    fn bootstrap_opens_one_uni_control_stream_and_records_child() {
        let (mut worker, mut sessions, context, parent) = harness();
        OPEN_CALLS.with(|calls| calls.set(0));
        OPEN_CONTEXT.with(|seen| seen.set(0));
        let child = worker
            .bootstrap_control_stream(context, &mut sessions, 0x1234)
            .expect("bootstrap succeeds");
        assert_eq!(
            child,
            SessionId::from_raw(parent.get() + 1),
            "the returned child is the one the action produced"
        );
        assert_eq!(
            worker.get(context).expect("live context").local_control,
            Some(child),
            "the child is recorded after success"
        );
        OPEN_CALLS.with(|calls| assert_eq!(calls.get(), 1, "open_stream invoked exactly once"));
        OPEN_CONTEXT.with(|seen| assert_eq!(seen.get(), 0x1234, "app context passed through"));
    }

    #[test]
    fn duplicate_bootstrap_is_rejected_without_a_second_action() {
        let (mut worker, mut sessions, context, _parent) = harness();
        OPEN_CALLS.with(|calls| calls.set(0));
        worker
            .bootstrap_control_stream(context, &mut sessions, 1)
            .expect("first bootstrap succeeds");
        let duplicate = worker.bootstrap_control_stream(context, &mut sessions, 2);
        assert!(matches!(
            duplicate,
            Err(HttpWorkerError::ControlStreamAlreadyOpen { context: c }) if c == context
        ));
        OPEN_CALLS.with(|calls| assert_eq!(calls.get(), 1, "second bootstrap invokes no action"));
    }

    #[test]
    fn action_failure_leaves_context_unbootstrapped_and_unchanged() {
        let (mut worker, mut sessions, context, _parent) = harness();
        OPEN_CALLS.with(|calls| calls.set(0));
        OPEN_FAIL.with(|fail| fail.set(true));
        let failed = worker.bootstrap_control_stream(context, &mut sessions, 7);
        assert!(matches!(
            failed,
            Err(HttpWorkerError::ControlStreamOpenFailed { context: c }) if c == context
        ));
        OPEN_CALLS.with(|calls| assert_eq!(calls.get(), 1, "failing action still invoked once"));
        let connection = worker.get(context).expect("live context");
        assert_eq!(connection.local_control, None, "failure records no child");
        assert!(
            connection.peer_settings_pending,
            "peer-SETTINGS expectation unchanged"
        );
        // The same context can still bootstrap after the failure.
        OPEN_FAIL.with(|fail| fail.set(false));
        let child = worker
            .bootstrap_control_stream(context, &mut sessions, 7)
            .expect("retry succeeds");
        assert_eq!(
            worker.get(context).expect("live context").local_control,
            Some(child)
        );
        OPEN_CALLS.with(|calls| assert_eq!(calls.get(), 2, "retry invokes the action again"));
    }

    #[test]
    fn local_control_preface_bytes_are_exact() {
        use crate::http3::proto::coding::Encode;
        use crate::http3::proto::frame::Settings;
        use crate::http3::proto::stream::StreamType;

        // Fixed no-heap constant: CONTROL stream type, then a SETTINGS frame
        // (type 0x04, length 0x04) carrying QPACK_MAX_TABLE_CAPACITY=0
        // (0x01 0x00) and QPACK_BLOCKED_STREAMS=0 (0x07 0x00).
        assert_eq!(LOCAL_CONTROL_PREFACE, [0x00, 0x04, 0x04, 0x01, 0x00, 0x07, 0x00]);
        // Cross-check against the proto encoders (test-only allocation is
        // fine): CONTROL stream type, then the static-only QPACK SETTINGS.
        let mut encoded = Vec::new();
        StreamType::CONTROL.encode(&mut encoded);
        Settings::qpack_zero_capacity()
            .expect("static-only QPACK settings")
            .encode(&mut encoded)
            .expect("SETTINGS frame encodes into a Vec");
        assert_eq!(encoded, LOCAL_CONTROL_PREFACE);
    }

    /// A bootstrapped context whose child control stream Session has a real
    /// TX FIFO: the parent at slot 3 and the control child at slot 4, exactly
    /// the child `fake_open_stream` returns (`parent + 1`).
    fn preface_harness() -> (HttpWorker, SessionWorker<Index>, ContextId, SessionId) {
        let applications = ApplicationMain::new(4);
        let application = applications.attach().expect("attach test Application");
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            64,
            applications,
            None,
        )
        .expect("construct Session worker");
        sessions
            .install_transport_actions(
                ACTION_TRANSPORT,
                SessionTransportWorkerActions::new(
                    fake_open_stream,
                    fake_reset_stream,
                    fake_stop_sending,
                    fake_close_connection,
                ),
            )
            .expect("install transport actions");
        sessions
            .install_application_mq_for_test(application)
            .expect("install Application Rx MQ");
        let parent = sessions
            .construct_transport_session(
                ACTION_TRANSPORT,
                Index::new(3, 1),
                0xCAFE_1234,
                application,
                None,
                None,
                None,
                false,
            )
            .expect("construct parent transport Session");
        let child = sessions
            .construct_transport_session(
                ACTION_TRANSPORT,
                Index::new(4, 1),
                0xCAFE_1235,
                application,
                None,
                None,
                None,
                false,
            )
            .expect("construct child control Session");
        let mut worker = HttpWorker::with_capacity(4);
        let context = worker.allocate(parent).expect("allocate context");
        worker
            .bootstrap_control_stream(context, &mut sessions, 1)
            .expect("bootstrap records the child");
        assert_eq!(
            worker.get(context).expect("live context").local_control,
            Some(child),
            "bootstrap records the real child Session"
        );
        (worker, sessions, context, child)
    }

    #[test]
    fn preface_publish_writes_exact_bytes_and_raises_tx_event() {
        let (worker, sessions, context, child) = preface_harness();
        worker
            .publish_local_control_preface(context, &sessions)
            .expect("preface publishes");
        let (_, tx_fifo) = sessions.fifo_pair(child).expect("child TX FIFO");
        let mut bytes = [0u8; LOCAL_CONTROL_PREFACE.len()];
        assert_eq!(
            tx_fifo.peek(0, bytes.len(), &mut bytes),
            LOCAL_CONTROL_PREFACE.len(),
            "the exact preface is visible"
        );
        assert_eq!(bytes, LOCAL_CONTROL_PREFACE, "golden preface bytes");
        assert_eq!(
            tx_fifo.max_dequeue(),
            LOCAL_CONTROL_PREFACE.len(),
            "nothing but the 7 preface bytes is visible"
        );
        // The harness exposes the FIFO event flag, not the exact MQ count
        // behind ApplicationMain, so the success assertion is the flag.
        assert!(tx_fifo.has_event(), "the TX-enqueue event flag is set");
    }

    // Blocked: an MQ-full/error-source test for
    // `ControlPrefaceEventPublishFailed` (its `#[source] RuntimeError`)
    // needs a full application Rx MQ, and no helper that fills that MQ
    // exists in these tests — the fill loop below targets the child TX
    // FIFO, whose full state fails earlier at `reserve_write` and never
    // reaches `publish_tx_enqueue`. Such a test needs an integration seam
    // into the ApplicationMain Rx MQ.
    #[test]
    fn preface_publish_insufficient_capacity_is_atomic() {
        let (worker, sessions, context, child) = preface_harness();
        let (_, tx_fifo) = sessions.fifo_pair(child).expect("child TX FIFO");
        while tx_fifo.max_enqueue() > 0 {
            assert!(tx_fifo.enqueue(&[0xAB; 1024]) > 0, "fill makes progress");
        }
        assert_eq!(tx_fifo.max_enqueue(), 0, "child TX FIFO is full");
        let visible_before = tx_fifo.max_dequeue();
        let failed = worker.publish_local_control_preface(context, &sessions);
        assert!(matches!(
            failed,
            Err(HttpWorkerError::ControlPrefaceFifo {
                context: c,
                source: FifoError::InsufficientCapacity {
                    requested: 7,
                    available: 0,
                },
            }) if c == context
        ));
        assert_eq!(
            tx_fifo.max_dequeue(),
            visible_before,
            "no preface bytes became visible"
        );
        assert!(!tx_fifo.has_event(), "no event was raised");
    }

    #[test]
    fn preface_publish_before_bootstrap_is_typed_error() {
        let (worker, sessions, context, _parent) = harness();
        let failed = worker.publish_local_control_preface(context, &sessions);
        assert!(matches!(
            failed,
            Err(HttpWorkerError::ControlStreamNotOpen { context: c }) if c == context
        ));
    }

    #[test]
    fn preface_publish_missing_child_fifo_is_typed_error() {
        let (mut worker, mut sessions, context, _parent) = harness();
        worker
            .bootstrap_control_stream(context, &mut sessions, 1)
            .expect("bootstrap records a child");
        let child = worker
            .get(context)
            .expect("live context")
            .local_control
            .expect("child recorded");
        let failed = worker.publish_local_control_preface(context, &sessions);
        assert!(matches!(
            failed,
            Err(HttpWorkerError::ControlStreamFifoMissing {
                context: c,
                child: s,
            }) if c == context && s == child
        ));
    }

    #[test]
    fn stream_allocate_binds_parent_session_and_direction() {
        let mut worker = HttpWorker::with_capacities(4, 4);
        let parent = worker.allocate(session(1, 1)).expect("parent context");
        let child = session(2, 1);
        let stream = worker
            .allocate_stream(child, parent, SessionStreamDirection::Bidi)
            .expect("allocate stream");
        let context = worker.get_stream(stream).expect("live stream");
        assert_eq!(context.session, child);
        assert_eq!(context.parent, parent);
        assert_eq!(context.direction, SessionStreamDirection::Bidi);
        assert_eq!(StreamContextId::from(Index::from(stream)), stream);
        assert_eq!(StreamContextId::from(u64::from(stream)), stream);
        let uni = worker
            .allocate_stream(child, parent, SessionStreamDirection::Uni)
            .expect("allocate uni stream");
        assert_eq!(
            worker.get_stream(uni).expect("live stream").direction,
            SessionStreamDirection::Uni
        );
    }

    #[test]
    fn stream_allocation_rejects_stale_or_missing_parent() {
        let mut worker = HttpWorker::with_capacities(4, 4);
        let bogus = ContextId::from(u64::MAX);
        assert!(matches!(
            worker.allocate_stream(session(1, 1), bogus, SessionStreamDirection::Bidi),
            Err(HttpWorkerError::ParentContextMissing { parent: p }) if p == bogus
        ));
        let parent = worker.allocate(session(1, 1)).expect("parent context");
        worker.remove(parent).expect("remove parent");
        assert!(matches!(
            worker.allocate_stream(session(2, 1), parent, SessionStreamDirection::Bidi),
            Err(HttpWorkerError::ParentContextMissing { parent: p }) if p == parent
        ));
        assert_eq!(worker.stream_len(), 0, "failed allocation leaves no stream");
    }

    #[test]
    fn stream_generation_advances_after_remove_and_slot_reuse() {
        let mut worker = HttpWorker::with_capacities(4, 4);
        let parent = worker.allocate(session(1, 1)).expect("parent");
        let first = worker
            .allocate_stream(session(2, 1), parent, SessionStreamDirection::Bidi)
            .expect("allocate first stream");
        worker.remove_stream(first).expect("remove stream");
        let second = worker
            .allocate_stream(session(3, 1), parent, SessionStreamDirection::Uni)
            .expect("reuse slot");
        assert_ne!(first, second);
        assert_eq!(Index::from(second).slot(), Index::from(first).slot());
        assert_ne!(
            Index::from(second).generation(),
            Index::from(first).generation()
        );
        assert!(matches!(
            worker.get_stream(first),
            Err(HttpWorkerError::StreamMissing { stream: s }) if s == first
        ));
        assert_eq!(
            worker.get_stream(second).expect("new identity live").session,
            session(3, 1)
        );
    }

    #[test]
    fn stream_session_mismatch_is_typed_error() {
        let mut worker = HttpWorker::with_capacities(4, 4);
        let parent = worker.allocate(session(1, 1)).expect("parent");
        let child = session(2, 1);
        let other = session(3, 1);
        let stream = worker
            .allocate_stream(child, parent, SessionStreamDirection::Bidi)
            .expect("allocate stream");
        assert!(matches!(
            worker.get_stream_for_session(stream, other),
            Err(HttpWorkerError::StreamSessionMismatch {
                stream: s,
                expected: e,
                actual: a,
            }) if s == stream && e == other && a == child
        ));
        assert_eq!(
            worker
                .get_stream_for_session(stream, child)
                .expect("exact session")
                .session,
            child
        );
    }

    #[test]
    fn stream_and_connection_capacities_are_independent() {
        let mut worker = HttpWorker::with_capacities(1, 2);
        let parent = worker.allocate(session(1, 1)).expect("only connection");
        assert!(matches!(
            worker.allocate(session(2, 1)),
            Err(HttpWorkerError::ContextCapacityExhausted { capacity: 1 })
        ));
        // Stream pool still has headroom despite connection pool exhaustion.
        let first = worker
            .allocate_stream(session(2, 1), parent, SessionStreamDirection::Bidi)
            .expect("first stream");
        let second = worker
            .allocate_stream(session(3, 1), parent, SessionStreamDirection::Uni)
            .expect("second stream");
        assert!(matches!(
            worker.allocate_stream(session(4, 1), parent, SessionStreamDirection::Bidi),
            Err(HttpWorkerError::StreamCapacityExhausted { capacity: 2 })
        ));
        // Removing streams leaves the connection pool untouched.
        worker.remove_stream(first).expect("remove first");
        worker.remove_stream(second).expect("remove second");
        assert_eq!(worker.len(), 1);
        assert_eq!(worker.stream_len(), 0);
        assert_eq!(worker.capacity(), 1);
        assert_eq!(worker.stream_capacity(), 2);
        assert!(worker.streams_is_empty());
    }

    #[test]
    fn remove_stale_or_missing_stream_identity_is_typed_error() {
        let mut worker = HttpWorker::with_capacities(2, 2);
        let bogus = StreamContextId::from(u64::MAX);
        assert!(matches!(
            worker.remove_stream(bogus),
            Err(HttpWorkerError::StreamMissing { stream: s }) if s == bogus
        ));
        let parent = worker.allocate(session(1, 1)).expect("parent");
        let stream = worker
            .allocate_stream(session(2, 1), parent, SessionStreamDirection::Bidi)
            .expect("allocate stream");
        worker.remove_stream(stream).expect("remove stream");
        assert!(matches!(
            worker.remove_stream(stream),
            Err(HttpWorkerError::StreamMissing { stream: s }) if s == stream
        ));
    }

    /// A live parent connection context and one live peer uni stream child of
    /// it.
    fn peer_uni_pair(worker: &mut HttpWorker) -> (ContextId, StreamContextId) {
        let parent = worker.allocate(session(1, 1)).expect("allocate parent");
        let stream = peer_stream(worker, parent);
        (parent, stream)
    }

    /// A live peer uni stream child of `parent`, distinct from any earlier
    /// child of the same parent.
    fn peer_stream(worker: &mut HttpWorker, parent: ContextId) -> StreamContextId {
        worker
            .allocate_stream(session(2, 1), parent, SessionStreamDirection::Uni)
            .expect("allocate peer uni stream")
    }

    /// A rejected push registration must leave the parent slots and the
    /// stream role exactly as before the call.
    fn assert_push_state_unchanged(worker: &HttpWorker, parent: ContextId, stream: StreamContextId) {
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_control, None);
        assert_eq!(connection.peer_encoder, None);
        assert_eq!(connection.peer_decoder, None);
        assert_eq!(
            worker.get_stream(stream).expect("live stream").peer_role,
            PeerUniStreamRole::Unclassified,
            "rejected push records no stream role"
        );
    }

    #[test]
    fn first_control_registration_records_slot_and_stream_role() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, control) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(control, PeerUniStreamRole::Control)
            .expect("first control registration succeeds");
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_control, Some(control));
        assert_eq!(connection.peer_encoder, None);
        assert_eq!(connection.peer_decoder, None);
        assert_eq!(
            worker.get_stream(control).expect("live stream").peer_role,
            PeerUniStreamRole::Control,
            "stream role recorded only after slot registration"
        );
    }

    #[test]
    fn duplicate_control_registration_is_stream_creation_error_and_changes_nothing() {
        let mut worker = HttpWorker::with_capacity(4);
        let parent = worker.allocate(session(1, 1)).expect("allocate parent");
        let first = peer_stream(&mut worker, parent);
        let second = peer_stream(&mut worker, parent);
        worker
            .register_peer_uni_stream(first, PeerUniStreamRole::Control)
            .expect("first registration");
        let duplicate = worker.register_peer_uni_stream(second, PeerUniStreamRole::Control);
        assert!(matches!(
            duplicate,
            Err(HttpWorkerError::PeerStreamRoleDuplicate {
                stream,
                context,
                role: PeerUniStreamRole::Control,
                code: ErrorCode::StreamCreationError,
            }) if stream == second && context == parent
        ));
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_control, Some(first), "slot keeps the first owner");
        assert_eq!(
            worker.get_stream(second).expect("live stream").peer_role,
            PeerUniStreamRole::Unclassified,
            "failed duplicate records no stream role"
        );
    }

    #[test]
    fn first_encoder_and_decoder_registrations_fill_distinct_slots() {
        let mut worker = HttpWorker::with_capacity(4);
        let parent = worker.allocate(session(1, 1)).expect("allocate parent");
        let encoder = peer_stream(&mut worker, parent);
        let decoder = peer_stream(&mut worker, parent);
        worker
            .register_peer_uni_stream(encoder, PeerUniStreamRole::QpackEncoder)
            .expect("first encoder registration");
        worker
            .register_peer_uni_stream(decoder, PeerUniStreamRole::QpackDecoder)
            .expect("first decoder registration");
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_control, None);
        assert_eq!(connection.peer_encoder, Some(encoder));
        assert_eq!(connection.peer_decoder, Some(decoder));
        assert_eq!(
            worker.get_stream(encoder).expect("live stream").peer_role,
            PeerUniStreamRole::QpackEncoder
        );
        assert_eq!(
            worker.get_stream(decoder).expect("live stream").peer_role,
            PeerUniStreamRole::QpackDecoder
        );
    }

    #[test]
    fn duplicate_encoder_registration_is_stream_creation_error_and_changes_nothing() {
        let mut worker = HttpWorker::with_capacity(4);
        let parent = worker.allocate(session(1, 1)).expect("allocate parent");
        let first = peer_stream(&mut worker, parent);
        let second = peer_stream(&mut worker, parent);
        worker
            .register_peer_uni_stream(first, PeerUniStreamRole::QpackEncoder)
            .expect("first registration");
        let duplicate = worker.register_peer_uni_stream(second, PeerUniStreamRole::QpackEncoder);
        assert!(matches!(
            duplicate,
            Err(HttpWorkerError::PeerStreamRoleDuplicate {
                stream,
                context,
                role: PeerUniStreamRole::QpackEncoder,
                code: ErrorCode::StreamCreationError,
            }) if stream == second && context == parent
        ));
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_encoder, Some(first), "slot keeps the first owner");
        assert_eq!(
            worker.get_stream(second).expect("live stream").peer_role,
            PeerUniStreamRole::Unclassified,
            "failed duplicate records no stream role"
        );
    }

    #[test]
    fn duplicate_decoder_registration_is_stream_creation_error_and_changes_nothing() {
        let mut worker = HttpWorker::with_capacity(4);
        let parent = worker.allocate(session(1, 1)).expect("allocate parent");
        let first = peer_stream(&mut worker, parent);
        let second = peer_stream(&mut worker, parent);
        worker
            .register_peer_uni_stream(first, PeerUniStreamRole::QpackDecoder)
            .expect("first registration");
        let duplicate = worker.register_peer_uni_stream(second, PeerUniStreamRole::QpackDecoder);
        assert!(matches!(
            duplicate,
            Err(HttpWorkerError::PeerStreamRoleDuplicate {
                stream,
                context,
                role: PeerUniStreamRole::QpackDecoder,
                code: ErrorCode::StreamCreationError,
            }) if stream == second && context == parent
        ));
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_decoder, Some(first), "slot keeps the first owner");
        assert_eq!(
            worker.get_stream(second).expect("live stream").peer_role,
            PeerUniStreamRole::Unclassified,
            "failed duplicate records no stream role"
        );
    }

    #[test]
    fn unknown_registration_records_drain_role_without_any_slot() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, stream) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(stream, PeerUniStreamRole::Unknown)
            .expect("unknown registration succeeds");
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_control, None);
        assert_eq!(connection.peer_encoder, None);
        assert_eq!(connection.peer_decoder, None);
        assert_eq!(
            worker.get_stream(stream).expect("live stream").peer_role,
            PeerUniStreamRole::Unknown,
            "unknown stream is drained under its recorded role"
        );
    }

    #[test]
    fn allocate_with_role_records_endpoint_role_metadata() {
        let mut worker = HttpWorker::with_capacity(4);
        let server = worker
            .allocate_with_role(session(1, 1), Some(SessionEndpointRole::Server))
            .expect("allocate server connection");
        assert_eq!(
            worker.get(server).expect("live").role,
            Some(SessionEndpointRole::Server)
        );
        let client = worker
            .allocate_with_role(session(2, 1), Some(SessionEndpointRole::Client))
            .expect("allocate client connection");
        assert_eq!(
            worker.get(client).expect("live").role,
            Some(SessionEndpointRole::Client)
        );
        let unclassified = worker.allocate(session(3, 1)).expect("allocate");
        assert_eq!(
            worker.get(unclassified).expect("live").role,
            None,
            "the default one-arg allocate records no role metadata"
        );
    }

    #[test]
    fn push_registration_without_role_metadata_is_typed_error_and_changes_nothing() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, stream) = peer_uni_pair(&mut worker);
        let push = worker.register_peer_uni_stream(stream, PeerUniStreamRole::Push);
        assert!(matches!(
            push,
            Err(HttpWorkerError::PeerPushRoleMissing { stream: s, context: c })
                if s == stream && c == parent
        ));
        assert_push_state_unchanged(&worker, parent, stream);
    }

    #[test]
    fn push_registration_on_server_connection_is_stream_creation_error() {
        let mut worker = HttpWorker::with_capacity(4);
        let parent = worker
            .allocate_with_role(session(1, 1), Some(SessionEndpointRole::Server))
            .expect("allocate server connection");
        let stream = peer_stream(&mut worker, parent);
        let push = worker.register_peer_uni_stream(stream, PeerUniStreamRole::Push);
        assert!(matches!(
            push,
            Err(HttpWorkerError::PeerPushRejected {
                stream: s,
                context: c,
                code: ErrorCode::StreamCreationError,
            }) if s == stream && c == parent
        ));
        assert_push_state_unchanged(&worker, parent, stream);
    }

    #[test]
    fn push_registration_on_client_connection_is_id_error() {
        let mut worker = HttpWorker::with_capacity(4);
        let parent = worker
            .allocate_with_role(session(1, 1), Some(SessionEndpointRole::Client))
            .expect("allocate client connection");
        let stream = peer_stream(&mut worker, parent);
        let push = worker.register_peer_uni_stream(stream, PeerUniStreamRole::Push);
        assert!(matches!(
            push,
            Err(HttpWorkerError::PeerPushRejected {
                stream: s,
                context: c,
                code: ErrorCode::IdError,
            }) if s == stream && c == parent
        ));
        assert_push_state_unchanged(&worker, parent, stream);
    }

    #[test]
    fn unclassified_registration_is_typed_error_and_changes_nothing() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, stream) = peer_uni_pair(&mut worker);
        let unclassified = worker.register_peer_uni_stream(stream, PeerUniStreamRole::Unclassified);
        assert!(matches!(
            unclassified,
            Err(HttpWorkerError::PeerStreamRoleUnclassified { stream: s, context: c })
                if s == stream && c == parent
        ));
        assert_eq!(
            worker.get_stream(stream).expect("live stream").peer_role,
            PeerUniStreamRole::Unclassified
        );
    }

    #[test]
    fn registration_rejects_stale_stream_and_missing_parent() {
        let mut worker = HttpWorker::with_capacity(4);
        let stale = StreamContextId::from(u64::MAX);
        assert!(matches!(
            worker.register_peer_uni_stream(stale, PeerUniStreamRole::Control),
            Err(HttpWorkerError::StreamMissing { stream: s }) if s == stale
        ));
        let (parent, stream) = peer_uni_pair(&mut worker);
        worker.remove(parent).expect("remove parent");
        assert!(matches!(
            worker.register_peer_uni_stream(stream, PeerUniStreamRole::Control),
            Err(HttpWorkerError::ParentContextMissing { parent: p }) if p == parent
        ));
    }

    #[test]
    fn classify_peer_uni_stream_reset_accepts_every_role_with_closed_critical_stream() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, stream) = peer_uni_pair(&mut worker);
        // Registration rejects Push (policy) and Unclassified (no decoded
        // type), so the table writes the role directly: classification must
        // not consult it, since every peer uni role is critical (VPP
        // `http3_transport_stream_reset_callback` checks only
        // unidirectional-ness).
        let expected = PeerUniStreamReset {
            stream,
            context: parent,
            session: session(1, 1),
            error_code: ErrorCode::ClosedCriticalStream,
        };
        assert_eq!(expected.error_code.value(), 0x0104);
        for role in [
            PeerUniStreamRole::Unclassified,
            PeerUniStreamRole::Control,
            PeerUniStreamRole::Push,
            PeerUniStreamRole::QpackEncoder,
            PeerUniStreamRole::QpackDecoder,
            PeerUniStreamRole::Unknown,
        ] {
            worker
                .streams
                .get_mut(stream.into())
                .expect("live stream")
                .peer_role = role;
            assert_eq!(
                worker
                    .classify_peer_uni_stream_reset(stream)
                    .expect("every peer uni role classifies as a critical stream"),
                expected,
            );
        }
    }

    #[test]
    fn classify_peer_uni_stream_reset_copies_identities_and_mutates_nothing() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, stream) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(stream, PeerUniStreamRole::Control)
            .expect("register control");
        let connections = worker.len();
        let streams = worker.stream_len();
        let readers = worker.readers.len();
        let reset = worker
            .classify_peer_uni_stream_reset(stream)
            .expect("live peer control stream classifies");
        assert_eq!(
            reset,
            PeerUniStreamReset {
                stream,
                context: parent,
                session: session(1, 1),
                error_code: ErrorCode::ClosedCriticalStream,
            }
        );
        assert_eq!(reset.error_code.value(), 0x0104);
        // The returned session is the parent connection's root session, not
        // the child stream's own lower session: `close_connection` targets
        // the root connection SessionId.
        assert_ne!(
            worker.get(parent).expect("live connection").session,
            worker.get_stream(stream).expect("live stream").session,
            "child stream SessionId differs from its parent root SessionId"
        );
        // &self classification leaves every pool and registered slot as is.
        assert_eq!(worker.len(), connections);
        assert_eq!(worker.stream_len(), streams);
        assert_eq!(worker.readers.len(), readers);
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_control, Some(stream));
        assert_eq!(connection.peer_encoder, None);
        assert_eq!(connection.peer_decoder, None);
        assert_eq!(connection.peer_control_reader, None);
        assert_eq!(
            worker.get_stream(stream).expect("live stream").peer_role,
            PeerUniStreamRole::Control
        );
    }

    #[test]
    fn classify_peer_uni_stream_reset_rejects_stale_stream_without_mutation() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, stream) = peer_uni_pair(&mut worker);
        worker.remove_stream(stream).expect("remove stream");
        assert!(matches!(
            worker.classify_peer_uni_stream_reset(stream),
            Err(HttpWorkerError::StreamMissing { stream: s }) if s == stream
        ));
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_control, None);
        assert_eq!(connection.peer_encoder, None);
        assert_eq!(connection.peer_decoder, None);
        assert_eq!(worker.stream_len(), 0);
    }

    #[test]
    fn classify_peer_uni_stream_reset_rejects_missing_parent_without_mutation() {
        let mut worker = HttpWorker::with_capacity(4);
        let parent = worker.allocate(session(1, 1)).expect("allocate parent");
        let stream = peer_stream(&mut worker, parent);
        worker.remove(parent).expect("remove parent");
        assert!(matches!(
            worker.classify_peer_uni_stream_reset(stream),
            Err(HttpWorkerError::ParentContextMissing { parent: p }) if p == parent
        ));
        assert_eq!(worker.len(), 0);
        assert_eq!(worker.stream_len(), 1);
    }

    #[test]
    fn peer_uni_type_split_varint_preserves_partial_prefix() {
        // Value 0x7F (127) encodes as a 2-byte varint: [0x40, 0x7F].
        let stream_type = StreamType::from_value(0x7F).expect("in varint range");
        let mut decode = PeerUniStreamTypeDecode::default();
        assert_eq!(decode.role(), PeerUniStreamRole::Unclassified);
        // A lone first byte cannot complete a 2-byte varint; the prefix is
        // preserved for the next feed.
        assert_eq!(
            decode.feed(&[0x40]),
            PeerUniStreamTypeOutcome::Incomplete
        );
        assert_eq!(decode.role(), PeerUniStreamRole::Unclassified);
        // The second byte completes it, consuming exactly that byte.
        assert_eq!(
            decode.feed(&[0x7F]),
            PeerUniStreamTypeOutcome::Complete {
                stream_type,
                category: StreamCategory::Unknown(0x7F),
                consumed: 1,
            }
        );
        assert_eq!(decode.role(), PeerUniStreamRole::Unknown);
    }

    #[test]
    fn peer_uni_type_complete_consumes_exactly_the_encoded_varint() {
        // LOCAL_CONTROL_PREFACE: the 1-byte CONTROL type followed by the
        // SETTINGS frame; only the type varint may be consumed.
        let mut decode = PeerUniStreamTypeDecode::default();
        assert_eq!(
            decode.feed(&LOCAL_CONTROL_PREFACE),
            PeerUniStreamTypeOutcome::Complete {
                stream_type: StreamType::CONTROL,
                category: StreamCategory::Control,
                consumed: 1,
            }
        );
        assert_eq!(decode.role(), PeerUniStreamRole::Control);
        // Consumption is exact: the state resets, so the trailing SETTINGS
        // bytes were neither consumed nor lost, and decode the next type.
        let settings_type = StreamType::from_value(0x04).expect("in varint range");
        assert_eq!(
            decode.feed(&LOCAL_CONTROL_PREFACE[1..]),
            PeerUniStreamTypeOutcome::Complete {
                stream_type: settings_type,
                category: StreamCategory::Unknown(0x04),
                consumed: 1,
            }
        );
    }

    #[test]
    fn peer_uni_type_max_size_varint_completes_at_eight_bytes() {
        // The largest varint (2^62 - 1) encodes as eight 0xFF bytes; any
        // shorter prefix is Incomplete. The existing decoder has no
        // malformed/oversized error (every complete varint is in bounds), so
        // this boundary test stands in for that case.
        let max = (1u64 << 62) - 1;
        let stream_type = StreamType::from_value(max).expect("max varint in range");
        let mut decode = PeerUniStreamTypeDecode::default();
        assert_eq!(
            decode.feed(&[0xFF, 0xFF, 0xFF, 0xFF]),
            PeerUniStreamTypeOutcome::Incomplete
        );
        assert_eq!(
            decode.feed(&[0xFF, 0xFF, 0xFF, 0xFF]),
            PeerUniStreamTypeOutcome::Complete {
                stream_type,
                category: StreamCategory::Unknown(max),
                consumed: 4,
            }
        );
    }

    #[test]
    fn peer_settings_split_across_feeds_reports_consumed_and_pending() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, control) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(control, PeerUniStreamRole::Control)
            .expect("register control stream");
        // SETTINGS, len 2, {QPACK_MAX_TABLE_CAPACITY = 0}.
        let wire = [0x04, 0x02, 0x01, 0x00];
        assert_eq!(
            worker
                .process_peer_control_bytes(control, &wire[..2])
                .expect("no peer control error"),
            PeerControlOutcome::Incomplete { consumed: 2 }
        );
        assert!(
            worker.get(parent).expect("live").peer_settings_pending,
            "pending survives an incomplete SETTINGS frame"
        );
        assert_eq!(
            worker
                .process_peer_control_bytes(control, &wire[2..])
                .expect("no peer control error"),
            PeerControlOutcome::Complete { consumed: 2 }
        );
        assert!(!worker.get(parent).expect("live").peer_settings_pending);
    }

    #[test]
    fn peer_settings_exact_completion_leaves_trailing_bytes_unread() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, control) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(control, PeerUniStreamRole::Control)
            .expect("register control stream");
        // SETTINGS, len 2, {QPACK_MAX_TABLE_CAPACITY = 0}, then the start of a
        // GOAWAY frame: the one-shot reader must stop at the SETTINGS frame
        // and leave the trailing bytes in the FIFO.
        let wire = [0x04, 0x02, 0x01, 0x00, 0x07, 0x00];
        assert_eq!(
            worker
                .process_peer_control_bytes(control, &wire)
                .expect("no peer control error"),
            PeerControlOutcome::Complete { consumed: 4 }
        );
        assert!(!worker.get(parent).expect("live").peer_settings_pending);
    }

    #[test]
    fn empty_settings_frame_is_accepted() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, control) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(control, PeerUniStreamRole::Control)
            .expect("register control stream");
        assert_eq!(
            worker
                .process_peer_control_bytes(control, &[0x04, 0x00])
                .expect("no peer control error"),
            PeerControlOutcome::Complete { consumed: 2 }
        );
        assert!(!worker.get(parent).expect("live").peer_settings_pending);
    }

    #[test]
    fn malformed_settings_is_frame_error_and_leaves_pending() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, control) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(control, PeerUniStreamRole::Control)
            .expect("register control stream");
        // DATA is not in the control-stream frame table, so header validation
        // fails before the settings-first check.
        let error = worker
            .process_peer_control_bytes(control, &[0x00, 0x00])
            .expect_err("DATA cannot start the control stream");
        assert!(
            matches!(error, PeerControlError::Protocol(ControlStreamError::Frame(_))),
            "unexpected error: {error:?}"
        );
        assert_eq!(error.error_code(), Some(ErrorCode::FrameUnexpected));
        assert!(
            worker.get(parent).expect("live").peer_settings_pending,
            "a protocol error leaves the settings pending"
        );
    }

    #[test]
    fn nonzero_qpack_settings_is_settings_error_and_leaves_pending() {
        use crate::http3::proto::frame::SettingId;

        let mut worker = HttpWorker::with_capacity(4);
        let (parent, control) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(control, PeerUniStreamRole::Control)
            .expect("register control stream");
        // SETTINGS, len 2, {QPACK_MAX_TABLE_CAPACITY = 1}.
        let error = worker
            .process_peer_control_bytes(control, &[0x04, 0x02, 0x01, 0x01])
            .expect_err("nonzero QPACK table capacity is unsupported");
        assert!(
            matches!(
                error,
                PeerControlError::Protocol(ControlStreamError::QpackNotSupported(
                    SettingId::QPACK_MAX_TABLE_CAPACITY,
                    1
                ))
            ),
            "unexpected error: {error:?}"
        );
        assert_eq!(error.error_code(), Some(ErrorCode::SettingsError));
        assert!(
            worker.get(parent).expect("live").peer_settings_pending,
            "a protocol error leaves the settings pending"
        );
    }

    #[test]
    fn duplicate_settings_after_completion_is_rejected() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, control) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(control, PeerUniStreamRole::Control)
            .expect("register control stream");
        assert_eq!(
            worker
                .process_peer_control_bytes(control, &[0x04, 0x00])
                .expect("no peer control error"),
            PeerControlOutcome::Complete { consumed: 2 }
        );
        // The connection is settings-complete, so any further control bytes
        // are a second SETTINGS frame (VPP http3.c:1548-1552).
        let error = worker
            .process_peer_control_bytes(control, &[0x04, 0x00])
            .expect_err("second SETTINGS frame");
        assert!(
            matches!(
                error,
                PeerControlError::Protocol(ControlStreamError::DuplicateSettings)
            ),
            "unexpected error: {error:?}"
        );
        assert_eq!(error.error_code(), Some(ErrorCode::FrameUnexpected));
        assert!(
            !worker.get(parent).expect("live").peer_settings_pending,
            "the rejected duplicate does not re-arm settings"
        );
    }

    #[test]
    fn drain_peer_stream_bytes_returns_the_whole_slice_for_drain_only_roles() {
        let mut worker = HttpWorker::with_capacity(4);
        let parent = worker.allocate(session(1, 1)).expect("allocate parent");
        for role in [
            PeerUniStreamRole::QpackEncoder,
            PeerUniStreamRole::QpackDecoder,
            PeerUniStreamRole::Unknown,
        ] {
            let stream = peer_stream(&mut worker, parent);
            worker
                .register_peer_uni_stream(stream, role)
                .expect("register drain-only role");
            assert_eq!(
                worker.drain_peer_stream_bytes(stream, &[0xAA; 5]).expect("drain"),
                5
            );
            assert_eq!(
                worker.drain_peer_stream_bytes(stream, &[]).expect("drain empty"),
                0
            );
        }
    }

    #[test]
    fn drain_peer_stream_bytes_rejects_non_drain_only_roles() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, control) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(control, PeerUniStreamRole::Control)
            .expect("register control stream");
        let error = worker
            .drain_peer_stream_bytes(control, &[0x00])
            .expect_err("control stream is not drain-only");
        assert!(
            matches!(
                error,
                HttpWorkerError::PeerStreamNotDrainable {
                    stream,
                    context,
                    role: PeerUniStreamRole::Control,
                } if stream == control && context == parent
            ),
            "unexpected error: {error:?}"
        );
        // An unregistered stream has no role at all and is also not drainable.
        let unclassified = peer_stream(&mut worker, parent);
        let error = worker
            .drain_peer_stream_bytes(unclassified, &[0x00])
            .expect_err("unclassified stream is not drain-only");
        assert!(matches!(error, HttpWorkerError::PeerStreamNotDrainable { .. }));
        // A released stream id fails the liveness lookup.
        let removed = peer_stream(&mut worker, parent);
        worker.remove_stream(removed).expect("remove stream");
        let error = worker
            .drain_peer_stream_bytes(removed, &[0x00])
            .expect_err("released stream id");
        assert!(matches!(error, HttpWorkerError::StreamMissing { .. }));
    }

    #[test]
    fn releasing_the_connection_frees_the_reader_and_stales_identities() {
        let mut worker = HttpWorker::with_capacity(4);
        let parent = worker.allocate(session(1, 1)).expect("allocate parent");
        let control = peer_stream(&mut worker, parent);
        worker
            .register_peer_uni_stream(control, PeerUniStreamRole::Control)
            .expect("register control stream");
        // The first feed allocates the reader slot.
        assert_eq!(
            worker
                .process_peer_control_bytes(control, &[0x04])
                .expect("no peer control error"),
            PeerControlOutcome::Incomplete { consumed: 1 }
        );
        assert!(worker.get(parent).expect("live").peer_control_reader.is_some());
        // Releasing the connection frees the reader slot; the stream context
        // survives but its parent identity is stale.
        worker.remove(parent).expect("release connection");
        let error = worker
            .process_peer_control_bytes(control, &[0x00])
            .expect_err("parent context released");
        assert!(
            matches!(
                error,
                PeerControlError::Worker(HttpWorkerError::ParentContextMissing { .. })
            ),
            "unexpected error: {error:?}"
        );
        // The next allocation reuses the connection slot at a new generation
        // with no reader, and a fresh reader starts over on the first feed.
        let parent2 = worker.allocate(session(1, 1)).expect("reallocate");
        assert!(worker.get(parent2).expect("live").peer_control_reader.is_none());
        let control2 = peer_stream(&mut worker, parent2);
        worker
            .register_peer_uni_stream(control2, PeerUniStreamRole::Control)
            .expect("register control stream");
        assert_eq!(
            worker
                .process_peer_control_bytes(control2, &[0x04, 0x00])
                .expect("no peer control error"),
            PeerControlOutcome::Complete { consumed: 2 }
        );
    }

    #[test]
    fn removing_the_control_stream_frees_the_reader_slot() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, control) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(control, PeerUniStreamRole::Control)
            .expect("register control stream");
        assert_eq!(
            worker
                .process_peer_control_bytes(control, &[0x04])
                .expect("no peer control error"),
            PeerControlOutcome::Incomplete { consumed: 1 }
        );
        worker.remove_stream(control).expect("remove control stream");
        assert!(
            worker.get(parent).expect("live").peer_control_reader.is_none(),
            "removing the control stream frees its reader slot"
        );
        // The stale stream id now fails the liveness lookup before any feed.
        let error = worker
            .process_peer_control_bytes(control, &[0x00])
            .expect_err("stream released");
        assert!(
            matches!(
                error,
                PeerControlError::Worker(HttpWorkerError::StreamMissing { .. })
            ),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn finish_peer_control_stream_before_settings_clears_slot_and_reader() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, control) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(control, PeerUniStreamRole::Control)
            .expect("register control stream");
        assert_eq!(
            worker
                .process_peer_control_bytes(control, &[0x04])
                .expect("no peer control error"),
            PeerControlOutcome::Incomplete { consumed: 1 }
        );
        worker
            .finish_peer_control_stream(control)
            .expect("finish before SETTINGS succeeds");
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_control, None, "peer control slot cleared");
        assert!(
            connection.peer_control_reader.is_none(),
            "SETTINGS reader freed"
        );
        assert!(
            connection.peer_settings_pending,
            "expectation stays pending when EOF arrives before SETTINGS"
        );
        assert!(matches!(
            worker.get_stream(control),
            Err(HttpWorkerError::StreamMissing { stream: s }) if s == control
        ));
    }

    #[test]
    fn finish_peer_control_stream_after_settings_keeps_expectation_cleared() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, control) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(control, PeerUniStreamRole::Control)
            .expect("register control stream");
        assert_eq!(
            worker
                .process_peer_control_bytes(control, &[0x04, 0x00])
                .expect("no peer control error"),
            PeerControlOutcome::Complete { consumed: 2 }
        );
        assert!(
            !worker.get(parent).expect("live connection").peer_settings_pending,
            "SETTINGS already complete"
        );
        worker
            .finish_peer_control_stream(control)
            .expect("finish after SETTINGS succeeds");
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_control, None, "peer control slot cleared");
        assert!(
            connection.peer_control_reader.is_none(),
            "SETTINGS reader freed"
        );
        assert!(
            !connection.peer_settings_pending,
            "the cleared expectation is left cleared"
        );
    }

    #[test]
    fn finish_peer_control_stream_without_any_bytes_frees_no_reader() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, control) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(control, PeerUniStreamRole::Control)
            .expect("register control stream");
        worker
            .finish_peer_control_stream(control)
            .expect("finish on immediate EOF succeeds");
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_control, None, "peer control slot cleared");
        assert!(
            connection.peer_control_reader.is_none(),
            "no reader was ever allocated"
        );
        assert!(connection.peer_settings_pending, "expectation still pending");
    }

    #[test]
    fn finish_stale_or_non_control_stream_changes_nothing() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, control) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(control, PeerUniStreamRole::Control)
            .expect("register control stream");
        // A stale identity fails the liveness lookup before any state change.
        worker.remove_stream(control).expect("remove control stream");
        assert!(matches!(
            worker.finish_peer_control_stream(control),
            Err(PeerControlError::Worker(HttpWorkerError::StreamMissing { stream: s })) if s == control
        ));
        // A live non-control stream is a typed mismatch that changes nothing.
        let drain = peer_stream(&mut worker, parent);
        worker
            .register_peer_uni_stream(drain, PeerUniStreamRole::QpackEncoder)
            .expect("register encoder stream");
        let error = worker
            .finish_peer_control_stream(drain)
            .expect_err("not a control stream");
        assert!(
            matches!(
                error,
                PeerControlError::Worker(HttpWorkerError::PeerControlStreamMismatch {
                    stream,
                    context,
                    role: PeerUniStreamRole::QpackEncoder,
                }) if stream == drain && context == parent
            ),
            "unexpected error: {error:?}"
        );
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_encoder, Some(drain), "encoder slot untouched");
        assert_eq!(
            worker.get_stream(drain).expect("live stream").peer_role,
            PeerUniStreamRole::QpackEncoder,
            "stream role untouched"
        );
    }

    #[test]
    fn finish_control_stream_mismatched_to_the_registered_slot_changes_nothing() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, control) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(control, PeerUniStreamRole::Control)
            .expect("register control stream");
        assert_eq!(
            worker
                .process_peer_control_bytes(control, &[0x04])
                .expect("no peer control error"),
            PeerControlOutcome::Incomplete { consumed: 1 }
        );
        // Fabricate the otherwise unreachable invariant break: the parent's
        // peer control slot points at a different live stream.
        let other = peer_stream(&mut worker, parent);
        worker
            .contexts
            .get_mut(parent.into())
            .expect("live parent")
            .peer_control = Some(other);
        let error = worker
            .finish_peer_control_stream(control)
            .expect_err("slot mismatch");
        assert!(
            matches!(
                error,
                PeerControlError::Worker(HttpWorkerError::PeerControlStreamMismatch {
                    stream,
                    context,
                    role: PeerUniStreamRole::Control,
                }) if stream == control && context == parent
            ),
            "unexpected error: {error:?}"
        );
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_control, Some(other), "slot untouched");
        assert!(
            connection.peer_control_reader.is_some(),
            "reader slot untouched"
        );
        assert_eq!(
            worker.get_stream(control).expect("live stream").peer_role,
            PeerUniStreamRole::Control,
            "stream role untouched"
        );
    }

    #[test]
    fn finish_then_re_registered_control_stream_still_requires_settings() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, first) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(first, PeerUniStreamRole::Control)
            .expect("register first control stream");
        assert_eq!(
            worker
                .process_peer_control_bytes(first, &[0x04])
                .expect("no peer control error"),
            PeerControlOutcome::Incomplete { consumed: 1 }
        );
        worker
            .finish_peer_control_stream(first)
            .expect("finish first control stream");
        let second = peer_stream(&mut worker, parent);
        worker
            .register_peer_uni_stream(second, PeerUniStreamRole::Control)
            .expect("re-register a fresh control stream");
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_control, Some(second), "fresh slot registered");
        assert!(
            connection.peer_control_reader.is_none(),
            "fresh reader on next feed"
        );
        assert!(
            connection.peer_settings_pending,
            "a re-registered control stream still must deliver SETTINGS"
        );
        assert_eq!(
            worker
                .process_peer_control_bytes(second, &[0x04])
                .expect("no peer control error"),
            PeerControlOutcome::Incomplete { consumed: 1 },
            "the reused reader slot starts fresh, without the old reader's state"
        );
        assert_eq!(
            worker
                .process_peer_control_bytes(second, &[0x00])
                .expect("no peer control error"),
            PeerControlOutcome::Complete { consumed: 1 }
        );
        assert!(
            !worker.get(parent).expect("live connection").peer_settings_pending,
            "the fresh control stream's SETTINGS clears the expectation"
        );
    }

    #[test]
    fn finish_peer_encoder_stream_clears_slot_and_removes_stream() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, encoder) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(encoder, PeerUniStreamRole::QpackEncoder)
            .expect("register encoder stream");
        worker
            .finish_peer_qpack_stream(encoder)
            .expect("finish encoder stream");
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_encoder, None, "encoder slot cleared");
        assert_eq!(connection.peer_decoder, None, "decoder slot untouched");
        assert_eq!(connection.peer_control, None, "control slot untouched");
        assert!(matches!(
            worker.get_stream(encoder),
            Err(HttpWorkerError::StreamMissing { stream: s }) if s == encoder
        ));
    }

    #[test]
    fn finish_peer_decoder_stream_clears_slot_and_removes_stream() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, decoder) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(decoder, PeerUniStreamRole::QpackDecoder)
            .expect("register decoder stream");
        worker
            .finish_peer_qpack_stream(decoder)
            .expect("finish decoder stream");
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_decoder, None, "decoder slot cleared");
        assert_eq!(connection.peer_encoder, None, "encoder slot untouched");
        assert!(matches!(
            worker.get_stream(decoder),
            Err(HttpWorkerError::StreamMissing { stream: s }) if s == decoder
        ));
    }

    #[test]
    fn finish_peer_encoder_stream_leaves_decoder_slot_untouched() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, encoder) = peer_uni_pair(&mut worker);
        let decoder = peer_stream(&mut worker, parent);
        worker
            .register_peer_uni_stream(encoder, PeerUniStreamRole::QpackEncoder)
            .expect("register encoder stream");
        worker
            .register_peer_uni_stream(decoder, PeerUniStreamRole::QpackDecoder)
            .expect("register decoder stream");
        worker
            .finish_peer_qpack_stream(encoder)
            .expect("finish encoder stream");
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_encoder, None, "encoder slot cleared");
        assert_eq!(
            connection.peer_decoder,
            Some(decoder),
            "decoder slot keeps its stream"
        );
        assert!(matches!(
            worker.get_stream(encoder),
            Err(HttpWorkerError::StreamMissing { stream: s }) if s == encoder
        ));
        assert_eq!(
            worker.get_stream(decoder).expect("live decoder").peer_role,
            PeerUniStreamRole::QpackDecoder,
            "decoder stream untouched"
        );
    }

    #[test]
    fn finish_peer_qpack_stream_slot_mismatch_changes_nothing() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, encoder) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(encoder, PeerUniStreamRole::QpackEncoder)
            .expect("register encoder stream");
        // Fabricate the otherwise unreachable invariant break: the parent's
        // peer encoder slot points at a different live stream.
        let other = peer_stream(&mut worker, parent);
        worker
            .contexts
            .get_mut(parent.into())
            .expect("live parent")
            .peer_encoder = Some(other);
        let error = worker
            .finish_peer_qpack_stream(encoder)
            .expect_err("slot mismatch");
        assert!(
            matches!(
                error,
                HttpWorkerError::PeerQpackStreamMismatch {
                    stream,
                    context,
                    role: PeerUniStreamRole::QpackEncoder,
                } if stream == encoder && context == parent
            ),
            "unexpected error: {error:?}"
        );
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(
            connection.peer_encoder,
            Some(other),
            "encoder slot untouched"
        );
        assert_eq!(
            worker.get_stream(encoder).expect("live stream").peer_role,
            PeerUniStreamRole::QpackEncoder,
            "stream role untouched"
        );
    }

    #[test]
    fn finish_stale_or_non_qpack_stream_changes_nothing() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, control) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(control, PeerUniStreamRole::Control)
            .expect("register control stream");
        // A stale identity fails the liveness lookup before any state change.
        worker.remove_stream(control).expect("remove control stream");
        assert!(matches!(
            worker.finish_peer_qpack_stream(control),
            Err(HttpWorkerError::StreamMissing { stream: s }) if s == control
        ));
        // A live non-QPACK stream is a typed mismatch that changes nothing;
        // use a fresh parent so the removed stream's stale control slot does
        // not interfere.
        let other_parent = worker.allocate(session(1, 1)).expect("allocate parent");
        let control = peer_stream(&mut worker, other_parent);
        worker
            .register_peer_uni_stream(control, PeerUniStreamRole::Control)
            .expect("register a control stream");
        let error = worker
            .finish_peer_qpack_stream(control)
            .expect_err("not a QPACK stream");
        assert!(
            matches!(
                error,
                HttpWorkerError::PeerQpackStreamMismatch {
                    stream,
                    context,
                    role: PeerUniStreamRole::Control,
                } if stream == control && context == other_parent
            ),
            "unexpected error: {error:?}"
        );
        let connection = worker.get(other_parent).expect("live connection");
        assert_eq!(
            connection.peer_control,
            Some(control),
            "control slot untouched"
        );
        assert_eq!(
            worker.get_stream(control).expect("live stream").peer_role,
            PeerUniStreamRole::Control,
            "stream role untouched"
        );
    }

    #[test]
    fn finish_peer_unknown_stream_removes_stream_and_preserves_slots() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, control) = peer_uni_pair(&mut worker);
        let encoder = peer_stream(&mut worker, parent);
        let unknown = peer_stream(&mut worker, parent);
        worker
            .register_peer_uni_stream(control, PeerUniStreamRole::Control)
            .expect("register control stream");
        worker
            .register_peer_uni_stream(encoder, PeerUniStreamRole::QpackEncoder)
            .expect("register encoder stream");
        worker
            .register_peer_uni_stream(unknown, PeerUniStreamRole::Unknown)
            .expect("register unknown stream");
        worker
            .finish_peer_unknown_stream(unknown)
            .expect("finish unknown stream");
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(
            connection.peer_control,
            Some(control),
            "control slot untouched"
        );
        assert_eq!(
            connection.peer_encoder,
            Some(encoder),
            "encoder slot untouched"
        );
        assert_eq!(connection.peer_decoder, None, "decoder slot untouched");
        assert!(matches!(
            worker.get_stream(unknown),
            Err(HttpWorkerError::StreamMissing { stream: s }) if s == unknown
        ));
        assert_eq!(
            worker.get_stream(control).expect("live control").peer_role,
            PeerUniStreamRole::Control,
            "control stream untouched"
        );
    }

    #[test]
    fn finish_stale_unknown_stream_changes_nothing() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, unknown) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(unknown, PeerUniStreamRole::Unknown)
            .expect("register unknown stream");
        worker.remove_stream(unknown).expect("remove unknown stream");
        assert!(matches!(
            worker.finish_peer_unknown_stream(unknown),
            Err(HttpWorkerError::StreamMissing { stream: s }) if s == unknown
        ));
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_control, None, "control slot untouched");
        assert_eq!(connection.peer_encoder, None, "encoder slot untouched");
        assert_eq!(connection.peer_decoder, None, "decoder slot untouched");
    }

    #[test]
    fn finish_live_non_unknown_stream_is_typed_mismatch_and_changes_nothing() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, control) = peer_uni_pair(&mut worker);
        worker
            .register_peer_uni_stream(control, PeerUniStreamRole::Control)
            .expect("register control stream");
        let error = worker
            .finish_peer_unknown_stream(control)
            .expect_err("not an unknown stream");
        assert!(
            matches!(
                error,
                HttpWorkerError::PeerUnknownStreamMismatch {
                    stream,
                    context,
                    role: PeerUniStreamRole::Control,
                } if stream == control && context == parent
            ),
            "unexpected error: {error:?}"
        );
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(
            connection.peer_control,
            Some(control),
            "control slot untouched"
        );
        assert_eq!(
            worker.get_stream(control).expect("live stream").peer_role,
            PeerUniStreamRole::Control,
            "stream role untouched"
        );
    }

    #[test]
    fn finish_then_re_registered_unknown_stream_finishes_again() {
        let mut worker = HttpWorker::with_capacity(4);
        let parent = worker.allocate(session(1, 1)).expect("allocate parent");
        let first = peer_stream(&mut worker, parent);
        worker
            .register_peer_uni_stream(first, PeerUniStreamRole::Unknown)
            .expect("register first unknown stream");
        worker
            .finish_peer_unknown_stream(first)
            .expect("finish first unknown stream");
        assert!(matches!(
            worker.get_stream(first),
            Err(HttpWorkerError::StreamMissing { stream: s }) if s == first
        ));
        // Unknown owns no connection slot, so a fresh Unknown stream on the
        // same connection registers and finishes without touching any slot.
        let second = peer_stream(&mut worker, parent);
        worker
            .register_peer_uni_stream(second, PeerUniStreamRole::Unknown)
            .expect("register second unknown stream");
        worker
            .finish_peer_unknown_stream(second)
            .expect("finish second unknown stream");
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_control, None, "no control slot created");
        assert_eq!(connection.peer_encoder, None, "no encoder slot created");
        assert_eq!(connection.peer_decoder, None, "no decoder slot created");
        assert!(matches!(
            worker.get_stream(second),
            Err(HttpWorkerError::StreamMissing { stream: s }) if s == second
        ));
    }

    /// A small worker with one bidirectional request stream bound to
    /// `session(1, 0)` under a live parent connection context.
    fn worker_with_request_stream() -> (HttpWorker, StreamContextId, SessionId) {
        let mut worker = HttpWorker::with_capacities(4, 4);
        let session = session(1, 0);
        let parent = worker.allocate(session).expect("allocate parent context");
        let stream = worker
            .allocate_stream(session, parent, SessionStreamDirection::Bidi)
            .expect("allocate request stream");
        (worker, stream, session)
    }

    #[test]
    fn request_bytes_lazy_allocates_and_reuses_reader_split_headers() {
        let (mut worker, stream, session) = worker_with_request_stream();
        assert!(
            worker.get_stream(stream).expect("live stream").request_reader.is_none(),
            "no reader before the first feed"
        );
        // HEADERS header plus two payload bytes: incomplete, all four bytes
        // consumed into the reader's partial-frame state.
        let (read, consumed) = worker
            .process_request_bytes(stream, session, &[0x01, 0x04, b'a', b'b'])
            .expect("feed partial frame");
        assert_eq!(read, RequestFrameRead::Incomplete);
        assert_eq!(consumed, 4);
        let reader_index = worker
            .get_stream(stream)
            .expect("live stream")
            .request_reader
            .expect("lazily allocated reader");
        // The same slot completes the frame: the split state survived the
        // call boundary and the reader is reused, not reallocated.
        let (read, consumed) = worker
            .process_request_bytes(stream, session, &[b'c', b'd'])
            .expect("complete frame");
        assert_eq!(read, RequestFrameRead::Headers(b"abcd".to_vec()));
        assert_eq!(consumed, 2);
        assert_eq!(
            worker.get_stream(stream).expect("live stream").request_reader,
            Some(reader_index),
            "reader slot persists across feeds"
        );
    }

    #[test]
    fn request_bytes_reader_freed_on_stream_removal_and_slot_reused() {
        let mut worker = HttpWorker::with_capacities(4, 4);
        let parent = worker.allocate(session(1, 0)).expect("allocate parent");
        let first = worker
            .allocate_stream(session(1, 1), parent, SessionStreamDirection::Bidi)
            .expect("allocate first request stream");
        worker
            .process_request_bytes(first, session(1, 1), &[0x01, 0x00])
            .expect("feed headers");
        let first_reader = worker
            .get_stream(first)
            .expect("live stream")
            .request_reader
            .expect("lazily allocated reader");
        assert_eq!(worker.request_readers.len(), 1);
        worker.remove_stream(first).expect("remove first stream");
        assert_eq!(
            worker.request_readers.len(),
            0,
            "removing the stream frees its request reader"
        );
        // The next request stream reuses the freed slot at a fresh
        // generation: feeding it succeeds and the stale first-reader
        // identity no longer resolves.
        let second = worker
            .allocate_stream(session(2, 1), parent, SessionStreamDirection::Bidi)
            .expect("allocate second request stream");
        let (read, consumed) = worker
            .process_request_bytes(second, session(2, 1), &[0x01, 0x02, b'h', b'i'])
            .expect("feed headers on second stream");
        assert_eq!(read, RequestFrameRead::Headers(b"hi".to_vec()));
        assert_eq!(consumed, 4);
        let second_reader = worker
            .get_stream(second)
            .expect("live stream")
            .request_reader
            .expect("reader reallocated for the second stream");
        assert_eq!(worker.request_readers.len(), 1);
        assert_eq!(
            second_reader.slot(),
            first_reader.slot(),
            "the freed reader slot is reused"
        );
        assert!(
            second_reader.generation() > first_reader.generation(),
            "reuse advances the slot generation"
        );
        assert!(
            worker.request_readers.get(first_reader).is_none(),
            "the stale first-reader identity is generation-checked out"
        );
    }

    #[test]
    fn remove_stream_stale_identity_leaves_live_reader_untouched() {
        let mut worker = HttpWorker::with_capacities(4, 4);
        let parent = worker.allocate(session(1, 0)).expect("allocate parent");
        let first = worker
            .allocate_stream(session(1, 1), parent, SessionStreamDirection::Bidi)
            .expect("allocate first request stream");
        worker
            .process_request_bytes(first, session(1, 1), &[0x01, 0x00])
            .expect("feed headers");
        worker.remove_stream(first).expect("remove first stream");
        // A stale removal of the same identity fails before touching the
        // reader pool.
        assert!(matches!(
            worker.remove_stream(first),
            Err(HttpWorkerError::StreamMissing { stream: s }) if s == first
        ));
        assert_eq!(worker.request_readers.len(), 0);
        // A replacement stream reuses the reader slot; removing the stale
        // stream identity must not free the live reader of the replacement.
        let second = worker
            .allocate_stream(session(2, 1), parent, SessionStreamDirection::Bidi)
            .expect("allocate second request stream");
        worker
            .process_request_bytes(second, session(2, 1), &[0x01, 0x00])
            .expect("feed headers on second stream");
        let live_reader = worker
            .get_stream(second)
            .expect("live stream")
            .request_reader
            .expect("reader reallocated for the second stream");
        assert!(matches!(
            worker.remove_stream(first),
            Err(HttpWorkerError::StreamMissing { stream: s }) if s == first
        ));
        assert_eq!(worker.request_readers.len(), 1);
        assert_eq!(
            worker.get_stream(second).expect("live stream").request_reader,
            Some(live_reader),
            "the stale removal left the live reader in place"
        );
    }

    #[test]
    fn request_bytes_exact_consumed_and_trailing_bytes() {
        let (mut worker, stream, session) = worker_with_request_stream();
        // HEADERS payload plus the whole next DATA frame arrive in one call:
        // only the HEADERS frame's bytes are consumed, the DATA bytes trail.
        let (read, consumed) = worker
            .process_request_bytes(
                stream,
                session,
                &[0x01, 0x04, b'a', b'b', b'c', b'd', 0x00, 0x00],
            )
            .expect("feed headers and trailing data");
        assert_eq!(read, RequestFrameRead::Headers(b"abcd".to_vec()));
        assert_eq!(consumed, 6);
        // The trailing bytes are passed back and drained exactly.
        let (read, consumed) = worker
            .process_request_bytes(stream, session, &[0x00, 0x00])
            .expect("feed trailing data");
        assert_eq!(read, RequestFrameRead::Drained(FrameType::DATA, 0));
        assert_eq!(consumed, 2);
        // PUSH_PROMISE is forbidden on a request stream: a typed protocol
        // error carrying the HTTP/3 error code.
        let err = worker
            .process_request_bytes(stream, session, &[0x05, 0x00])
            .expect_err("push promise rejected");
        assert!(matches!(
            err,
            RequestReadError::Protocol(RequestFrameError::Phase(_))
        ));
        assert_eq!(err.error_code(), Some(ErrorCode::FrameUnexpected));
    }

    #[test]
    fn request_bytes_unknown_drain_then_headers() {
        let (mut worker, stream, session) = worker_with_request_stream();
        // An unknown frame (0x2a) completes mid-call; the trailing HEADERS
        // bytes are left unconsumed for the next call.
        let (read, consumed) = worker
            .process_request_bytes(
                stream,
                session,
                &[0x2a, 0x03, b'x', b'y', b'z', 0x01, 0x02, b'h', b'i'],
            )
            .expect("feed unknown frame and trailing headers");
        assert_eq!(
            read,
            RequestFrameRead::Drained(FrameType::from_value(0x2a).unwrap(), 3)
        );
        assert_eq!(consumed, 5);
        let (read, consumed) = worker
            .process_request_bytes(stream, session, &[0x01, 0x02, b'h', b'i'])
            .expect("feed trailing headers");
        assert_eq!(read, RequestFrameRead::Headers(b"hi".to_vec()));
        assert_eq!(consumed, 4);
    }

    #[test]
    fn request_bytes_stale_stream_is_typed_error() {
        let (mut worker, stream, session) = worker_with_request_stream();
        worker.remove_stream(stream).expect("remove stream");
        assert!(matches!(
            worker.process_request_bytes(stream, session, &[0x01, 0x00]),
            Err(RequestReadError::Worker(HttpWorkerError::StreamMissing {
                stream: s,
            })) if s == stream
        ));
    }

    #[test]
    fn request_bytes_foreign_session_is_typed_error() {
        let (mut worker, stream, bound) = worker_with_request_stream();
        let foreign = session(2, 1);
        assert!(matches!(
            worker.process_request_bytes(stream, foreign, &[0x01, 0x00]),
            Err(RequestReadError::Worker(HttpWorkerError::StreamSessionMismatch {
                stream: s,
                expected,
                actual,
            })) if s == stream && expected == foreign && actual == bound
        ));
    }

    #[test]
    fn request_bytes_uni_stream_rejected() {
        let mut worker = HttpWorker::with_capacities(4, 4);
        let parent = worker.allocate(session(1, 0)).expect("parent context");
        let uni = worker
            .allocate_stream(session(2, 0), parent, SessionStreamDirection::Uni)
            .expect("allocate uni stream");
        assert!(matches!(
            worker.process_request_bytes(uni, session(2, 0), &[0x01, 0x00]),
            Err(RequestReadError::Worker(HttpWorkerError::RequestStreamNotBidi {
                stream: s,
                direction,
            })) if s == uni && direction == SessionStreamDirection::Uni
        ));
    }
}
