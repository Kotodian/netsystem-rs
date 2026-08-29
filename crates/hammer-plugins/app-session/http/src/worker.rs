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
//! records the child session, parent `u32`, and peer direction.
//!
//! Identities are direct `u32` indexes returned by the owning worker's
//! `Pool`. The pool keeps allocate/get/remove O(1) and validates that an index
//! is live; connection and stream pools have separate index spaces, while
//! Session metadata distinguishes root and stream roles. Session App callbacks,
//! HTTP3 engine
//! dispatch, QPACK, request publication, and stop_listen are later slices.

use hammer_infra::fifo::{Fifo, FifoError};
use hammer_infra::pool::Pool;
use hammer_infra::thread_owned::ThreadOwnedError;
use hammer_runtime::DataWorkerId;
use hammer_runtime::error::RuntimeError;
use hammer_runtime::session::SessionStreamDirection;
use hammer_service::session::{SessionEndpointRole, SessionWorker};

use crate::http_common::{BodyAccumulator, PublishError, publish_body_chunk};
use crate::http3::preface::encode_control_preface;
use crate::http3::proto::coding::Decode;
use crate::http3::proto::control::{ControlRead, ControlStreamError, ControlStreamReader};
use crate::http3::proto::error::ErrorCode;
use crate::http3::proto::stream::{StreamCategory, StreamType};
use crate::http3::request_frame_reader::{RequestFrameError, RequestFrameRead, RequestFrameReader};

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

/// Cold per-connection state bound to one data-worker context slot.
///
/// Holds the lower QUIC `u32` the context was allocated for plus the
/// HTTP/3 bootstrap state of the local control stream; hot HTTP/3 connection
/// state (frames, QPACK, streams) belongs to later slices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ConnectionContext {
    /// Lower QUIC session this connection context is bound to.
    pub(crate) session: u32,
    /// Endpoint role of the underlying Session: `Server` for
    /// listener-accepted connections, `Client` for outbound connects;
    /// `None` when accept-time role metadata is absent. Cold, read-only
    /// metadata consumed by the peer uni stream registration policy.
    pub(crate) role: Option<SessionEndpointRole>,
    /// Local control stream child Session, recorded only after the bootstrap
    /// action succeeds; `None` until then. Mirrors VPP recording the opened
    /// control stream in `http_ctx_t::our_ctrl_stream_index` (http3.c:234).
    pub(crate) local_control: Option<u32>,
    /// Peer control stream context, registered exactly once when the decoded
    /// peer uni stream type is Control; `None` until then. Mirrors VPP
    /// `http_ctx_t::peer_ctrl_stream_index` (http3.c:1683-1691).
    pub(crate) peer_control: Option<u32>,
    /// Peer QPACK encoder stream context, registered exactly once; mirrors
    /// VPP `http_ctx_t::peer_encoder_stream_index` (http3.c:1703-1710).
    pub(crate) peer_encoder: Option<u32>,
    /// Peer QPACK decoder stream context, registered exactly once; mirrors
    /// VPP `http_ctx_t::peer_decoder_stream_index` (http3.c:1693-1700).
    pub(crate) peer_decoder: Option<u32>,
    /// True until the peer's SETTINGS arrive on its control stream; set at
    /// allocation and consumed by the peer-SETTINGS slice.
    pub(crate) peer_settings_pending: bool,
    /// Generation-checked slot of this connection's SETTINGS reader in the
    /// worker's separate `readers` pool; `None` until the first readable
    /// bytes arrive. The 192-byte `ControlStreamReader` is not embedded in
    /// every connection or stream slot: one pool slot per connection is
    /// enough, since each connection owns at most one peer control stream.
    pub(crate) peer_control_reader: Option<u32>,
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
    pub(crate) session: u32,
    /// Parent connection context this stream belongs to.
    pub(crate) parent: u32,
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
    pub(crate) request_reader: Option<u32>,
    /// Generation-checked slot of this stream's pending HEADERS field
    /// section in the worker's separate `pending_field_sections` pool;
    /// `None` until the first completed field section is retained. Mirrors
    /// VPP recording the received request on the per-request `http_ctx_t`
    /// owned by the data worker (`req->headers`, set in
    /// `http3_req_state_wait_transport_method`, http3.c:835-899) for the
    /// later app-dispatch stage; the section is not embedded in every stream
    /// slot: one pool slot per stream at most, bounded by the stream pool
    /// capacity, freed with the stream.
    pub(crate) pending_field_section: Option<u32>,
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
    /// The app-visible request is terminated and must never be recreated:
    /// after a `ResetStreamAbortRequest` removed the upper request Session,
    /// the stream context stays live so the peer's remaining RX bytes drain
    /// dequeue-only, and a trailing frame — e.g. a trailer HEADERS the
    /// reader's ordering phase accepts (RFC 9114 Section 4.1) — must not
    /// re-enter the ready path and recreate the upper. Mirrors VPP marking
    /// the request terminated and app-closed (`http3_stream_terminate`,
    /// http3.c:140-149): the transport stream resets and the app is never
    /// re-dispatched. Peer uni streams never abort and keep the `false`
    /// default.
    pub(crate) aborted: bool,
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
    ContextMissing { context: u32 },
    #[error(
        "http connection context {context:?} is bound to session {actual:?}, expected {expected:?}"
    )]
    SessionMismatch {
        context: u32,
        expected: u32,
        actual: u32,
    },
    #[error("http connection context {context:?} already opened its control stream")]
    ControlStreamAlreadyOpen { context: u32 },
    #[error(
        "http connection context {context:?} has not opened its local control stream; bootstrap first"
    )]
    ControlStreamNotOpen { context: u32 },
    #[error(
        "http connection context {context:?} cannot find the Session FIFO of local control child {child:?}"
    )]
    ControlStreamFifoMissing { context: u32, child: u32 },
    #[error("local control preface for connection context {context:?} does not encode")]
    ControlPrefaceEncodeFailed { context: u32 },
    #[error(
        "local control preface for connection context {context:?} failed to write to the child control stream FIFO: {source}"
    )]
    ControlPrefaceFifo {
        context: u32,
        #[source]
        source: FifoError,
    },
    #[error(
        "failed to publish the local control preface TX-enqueue event for connection context {context:?}: {source}"
    )]
    ControlPrefaceEventPublishFailed {
        context: u32,
        #[source]
        source: RuntimeError,
    },
    #[error(
        "peer {role:?} stream {stream:?} already registered on connection context {context:?}: HTTP/3 {code}"
    )]
    PeerStreamRoleDuplicate {
        stream: u32,
        context: u32,
        role: PeerUniStreamRole,
        code: ErrorCode,
    },
    #[error(
        "peer push stream {stream:?} on connection context {context:?} is rejected by the local endpoint's push policy: HTTP/3 {code}"
    )]
    PeerPushRejected {
        stream: u32,
        context: u32,
        code: ErrorCode,
    },
    #[error(
        "peer push stream {stream:?} on connection context {context:?} cannot apply the server/client push policy: connection role metadata is missing"
    )]
    PeerPushRoleMissing { stream: u32, context: u32 },
    #[error(
        "peer uni stream {stream:?} on connection context {context:?} has no decoded type to register"
    )]
    PeerStreamRoleUnclassified { stream: u32, context: u32 },
    #[error(
        "peer uni stream {stream:?} on connection context {context:?} is not drain-only (role {role:?})"
    )]
    PeerStreamNotDrainable {
        stream: u32,
        context: u32,
        role: PeerUniStreamRole,
    },
    #[error(
        "peer uni stream {stream:?} on connection context {context:?} is not the registered peer control stream (role {role:?})"
    )]
    PeerControlStreamMismatch {
        stream: u32,
        context: u32,
        role: PeerUniStreamRole,
    },
    #[error(
        "peer uni stream {stream:?} on connection context {context:?} is not the registered peer QPACK encoder/decoder stream (role {role:?})"
    )]
    PeerQpackStreamMismatch {
        stream: u32,
        context: u32,
        role: PeerUniStreamRole,
    },
    #[error(
        "peer uni stream {stream:?} on connection context {context:?} is not an Unknown drain-only stream (role {role:?})"
    )]
    PeerUnknownStreamMismatch {
        stream: u32,
        context: u32,
        role: PeerUniStreamRole,
    },
    #[error(
        "peer control stream {stream:?} on connection context {context:?}: SETTINGS reader pool is full (capacity {capacity})"
    )]
    PeerControlReaderCapacityExhausted {
        stream: u32,
        context: u32,
        capacity: usize,
    },
    #[error(
        "http worker lost the peer control SETTINGS reader slot {index:?} for stream {stream:?}"
    )]
    PeerControlReaderMissing { stream: u32, index: u32 },
    #[error("http stream {stream:?} request reader pool is full (capacity {capacity})")]
    RequestReaderCapacityExhausted { stream: u32, capacity: usize },
    #[error("http worker lost the request reader slot {index:?} for stream {stream:?}")]
    RequestReaderMissing { stream: u32, index: u32 },
    #[error("http stream {stream:?} pending field-section pool is full (capacity {capacity})")]
    PendingFieldSectionCapacityExhausted { stream: u32, capacity: usize },
    #[error("http worker lost the pending field-section slot {index:?} for stream {stream:?}")]
    PendingFieldSectionMissing { stream: u32, index: u32 },
    #[error(
        "http stream {stream:?} already has a pending HEADERS field section; the newer section is returned unreplaced"
    )]
    PendingFieldSectionOverflow {
        stream: u32,
        section: PendingFieldSection,
    },
    #[error("http stream context pool is full (capacity {capacity})")]
    StreamCapacityExhausted { capacity: usize },
    #[error("http stream context {stream:?} is not live")]
    StreamMissing { stream: u32 },
    #[error("http stream context {stream:?} is bound to session {actual:?}, expected {expected:?}")]
    StreamSessionMismatch {
        stream: u32,
        expected: u32,
        actual: u32,
    },
    #[error(
        "http stream context {stream:?} is not a bidirectional request stream (direction {direction:?})"
    )]
    RequestStreamNotBidi {
        stream: u32,
        direction: SessionStreamDirection,
    },
    #[error(
        "failed to publish a request body chunk for stream {stream:?} to the upper session FIFO: {error}"
    )]
    BodyChunkPublishFailed { stream: u32, error: PublishError },
    #[error("http stream context allocation requires live parent connection context {parent:?}")]
    ParentContextMissing { parent: u32 },
    #[error(
        "http connection context {context:?} failed to open its control stream through the Session Worker"
    )]
    ControlStreamOpenFailed { context: u32 },
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
    pub(crate) stream: u32,
    /// Parent connection context the reset stream belongs to.
    pub(crate) context: u32,
    /// Root QUIC session of the parent connection, which the generic
    /// `SessionWorker::close_connection` action targets.
    pub(crate) session: u32,
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
/// by `u32`/`u32`, never by raw index. The container
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
    pub(crate) fn allocate(&mut self, session: u32) -> Result<u32, HttpWorkerError> {
        self.allocate_with_role(session, None)
    }

    /// Allocates a context slot bound to the exact lower QUIC `session` and
    /// records its endpoint role, read from accept metadata by the
    /// connection accept path.
    ///
    /// O(1); fails with `ContextCapacityExhausted` when the pool is full.
    pub(crate) fn allocate_with_role(
        &mut self,
        session: u32,
        role: Option<SessionEndpointRole>,
    ) -> Result<u32, HttpWorkerError> {
        Ok(self.contexts.insert(ConnectionContext {
            session,
            role,
            local_control: None,
            peer_control: None,
            peer_encoder: None,
            peer_decoder: None,
            peer_settings_pending: true,
            peer_control_reader: None,
        }))
    }

    /// Resolves a live context by its generation-checked identity.
    ///
    /// O(1); rejects stale or out-of-range identities with
    /// `ContextMissing`.
    pub(crate) fn get(&self, context: u32) -> Result<&ConnectionContext, HttpWorkerError> {
        if !self.contexts.contains_key(context.into()) {
            return Err(HttpWorkerError::ContextMissing { context });
        }
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
        context: u32,
        session: u32,
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
        context: u32,
        sessions: &mut SessionWorker,
        app_context: u64,
    ) -> Result<u32, HttpWorkerError> {
        if !self.contexts.contains_key(context.into()) {
            return Err(HttpWorkerError::ContextMissing { context });
        }
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
        context: u32,
        sessions: &SessionWorker,
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
    pub(crate) fn remove(&mut self, context: u32) -> Result<(), HttpWorkerError> {
        let connection = self
            .contexts
            .remove(context)
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
        session: u32,
        parent: u32,
        direction: SessionStreamDirection,
    ) -> Result<u32, HttpWorkerError> {
        if !self.contexts.contains_key(parent.into()) {
            return Err(HttpWorkerError::ParentContextMissing { parent });
        }
        Ok(self.streams.insert(StreamContext {
            session,
            parent,
            direction,
            peer_uni_type: PeerUniStreamTypeDecode::default(),
            peer_role: PeerUniStreamRole::Unclassified,
            request_reader: None,
            pending_field_section: None,
            body: BodyAccumulator::from(None),
            aborted: false,
        }))
    }

    /// Resolves a live stream context by its generation-checked identity.
    ///
    /// O(1); rejects stale or out-of-range identities with `StreamMissing`.
    pub(crate) fn get_stream(&self, stream: u32) -> Result<&StreamContext, HttpWorkerError> {
        if !self.streams.contains_key(stream.into()) {
            return Err(HttpWorkerError::StreamMissing { stream });
        }
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
        stream: u32,
        session: u32,
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
    /// `Option<u32>` slot on `ConnectionContext`
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
        stream: u32,
        role: PeerUniStreamRole,
    ) -> Result<(), HttpWorkerError> {
        if !self.streams.contains_key(stream.into()) {
            return Err(HttpWorkerError::StreamMissing { stream });
        }
        let parent = self
            .streams
            .get(stream.into())
            .ok_or(HttpWorkerError::StreamMissing { stream })?
            .parent;
        if !self.contexts.contains_key(parent.into()) {
            return Err(HttpWorkerError::ParentContextMissing { parent });
        }
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
    /// lower session (the `u32` the generic `close_connection` action
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
        stream: u32,
    ) -> Result<PeerUniStreamReset, HttpWorkerError> {
        if !self.streams.contains_key(stream.into()) {
            return Err(HttpWorkerError::StreamMissing { stream });
        }
        let stream_context = self
            .streams
            .get(stream.into())
            .ok_or(HttpWorkerError::StreamMissing { stream })?;
        let parent = stream_context.parent;
        if !self.contexts.contains_key(parent.into()) {
            return Err(HttpWorkerError::ParentContextMissing { parent });
        }
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
        stream: u32,
        bytes: &[u8],
    ) -> Result<PeerControlOutcome, PeerControlError> {
        let parent = {
            if !self.streams.contains_key(stream.into()) {
                return Err(PeerControlError::Worker(HttpWorkerError::StreamMissing {
                    stream,
                }));
            }
            let stream_context =
                self.streams
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
        if !self.contexts.contains_key(parent.into()) {
            return Err(PeerControlError::Worker(
                HttpWorkerError::ParentContextMissing { parent },
            ));
        }
        let mut connection = *self
            .contexts
            .get(parent.into())
            .ok_or(PeerControlError::Worker(
                HttpWorkerError::ParentContextMissing { parent },
            ))?;
        if !connection.peer_settings_pending {
            return Err(PeerControlError::Protocol(
                ControlStreamError::DuplicateSettings,
            ));
        }
        let reader_index = match connection.peer_control_reader {
            Some(index) => index,
            None => {
                let index = self.readers.insert(ControlStreamReader::new());
                connection.peer_control_reader = Some(index);
                index
            }
        };
        if !self.readers.contains_key(reader_index) {
            return Err(PeerControlError::Worker(
                HttpWorkerError::PeerControlReaderMissing {
                    stream,
                    index: reader_index,
                },
            ));
        }
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
            .ok_or(PeerControlError::Worker(
                HttpWorkerError::ParentContextMissing { parent },
            ))? = connection;
        match feed {
            Ok((ControlRead::Complete(_), consumed)) => {
                Ok(PeerControlOutcome::Complete { consumed })
            }
            Ok((ControlRead::Incomplete, consumed)) => {
                Ok(PeerControlOutcome::Incomplete { consumed })
            }
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
        stream: u32,
        session: u32,
        bytes: &'a [u8],
    ) -> Result<(RequestFrameRead<'a>, usize), RequestReadError> {
        if !self.streams.contains_key(stream.into()) {
            return Err(RequestReadError::Worker(HttpWorkerError::StreamMissing {
                stream,
            }));
        }
        let mut stream_context =
            *self
                .streams
                .get(stream.into())
                .ok_or(RequestReadError::Worker(HttpWorkerError::StreamMissing {
                    stream,
                }))?;
        if stream_context.session != session {
            return Err(RequestReadError::Worker(
                HttpWorkerError::StreamSessionMismatch {
                    stream,
                    expected: session,
                    actual: stream_context.session,
                },
            ));
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
                let index = self.request_readers.insert(RequestFrameReader::new());
                stream_context.request_reader = Some(index);
                index
            }
        };
        if !self.request_readers.contains_key(reader_index) {
            return Err(RequestReadError::Worker(
                HttpWorkerError::RequestReaderMissing {
                    stream,
                    index: reader_index,
                },
            ));
        }
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
            .ok_or(RequestReadError::Worker(HttpWorkerError::StreamMissing {
                stream,
            }))? = stream_context;
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
        stream: u32,
        session: u32,
        declared: Option<u64>,
    ) -> Result<(), HttpWorkerError> {
        if !self.streams.contains_key(stream.into()) {
            return Err(HttpWorkerError::StreamMissing { stream });
        }
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

    /// Marks a bidirectional request stream aborted: its app-visible request
    /// ended with the upper request Session removed, but the stream context
    /// stays live so the peer's remaining RX bytes drain dequeue-only, and a
    /// trailing frame must never re-enter the ready path and recreate the
    /// upper. Mirrors VPP setting the terminated/app-closed request state
    /// (`http3_stream_terminate`, http3.c:140-149): the transport stream
    /// resets and the app is never re-dispatched.
    ///
    /// Generation/session-checks the stream and rejects non-bidi streams
    /// with `RequestStreamNotBidi`; every failure path mutates nothing.
    /// O(1): one generation-checked lookup and write, no lock or allocation.
    pub(crate) fn abort_request_stream(
        &mut self,
        stream: u32,
        session: u32,
    ) -> Result<(), HttpWorkerError> {
        if !self.streams.contains_key(stream.into()) {
            return Err(HttpWorkerError::StreamMissing { stream });
        }
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
        stream_context.aborted = true;
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
        stream: u32,
        session: u32,
        upper_rx: &Fifo,
        chunk: &[u8],
    ) -> Result<(), RequestReadError> {
        if !self.streams.contains_key(stream.into()) {
            return Err(RequestReadError::Worker(HttpWorkerError::StreamMissing {
                stream,
            }));
        }
        let mut stream_context =
            *self
                .streams
                .get(stream.into())
                .ok_or(RequestReadError::Worker(HttpWorkerError::StreamMissing {
                    stream,
                }))?;
        if stream_context.session != session {
            return Err(RequestReadError::Worker(
                HttpWorkerError::StreamSessionMismatch {
                    stream,
                    expected: session,
                    actual: stream_context.session,
                },
            ));
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
            .ok_or(RequestReadError::Worker(HttpWorkerError::StreamMissing {
                stream,
            }))? = stream_context;
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
        stream: u32,
        session: u32,
    ) -> Result<(), RequestReadError> {
        if !self.streams.contains_key(stream.into()) {
            return Err(RequestReadError::Worker(HttpWorkerError::StreamMissing {
                stream,
            }));
        }
        let stream_context = self
            .streams
            .get(stream.into())
            .ok_or(RequestReadError::Worker(HttpWorkerError::StreamMissing {
                stream,
            }))?;
        if stream_context.session != session {
            return Err(RequestReadError::Worker(
                HttpWorkerError::StreamSessionMismatch {
                    stream,
                    expected: session,
                    actual: stream_context.session,
                },
            ));
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
        stream: u32,
        session: u32,
        section: PendingFieldSection,
    ) -> Result<(), HttpWorkerError> {
        if !self.streams.contains_key(stream.into()) {
            return Err(HttpWorkerError::StreamMissing { stream });
        }
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
                let slot = self.pending_field_sections.insert(Some(section));
                stream_context.pending_field_section = Some(slot);
                *self
                    .streams
                    .get_mut(stream.into())
                    .ok_or(HttpWorkerError::StreamMissing { stream })? = stream_context;
            }
            Some(slot) => {
                if !self.pending_field_sections.contains_key(slot) {
                    return Err(HttpWorkerError::PendingFieldSectionMissing {
                        stream,
                        index: slot,
                    });
                }
                let pending = self.pending_field_sections.get_mut(slot).ok_or(
                    HttpWorkerError::PendingFieldSectionMissing {
                        stream,
                        index: slot,
                    },
                )?;
                if pending.is_some() {
                    return Err(HttpWorkerError::PendingFieldSectionOverflow { stream, section });
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
        stream: u32,
        session: u32,
    ) -> Result<Option<&PendingFieldSection>, HttpWorkerError> {
        if !self.streams.contains_key(stream.into()) {
            return Err(HttpWorkerError::StreamMissing { stream });
        }
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
        if !self.pending_field_sections.contains_key(slot) {
            return Err(HttpWorkerError::PendingFieldSectionMissing {
                stream,
                index: slot,
            });
        }
        let pending = self.pending_field_sections.get(slot).ok_or(
            HttpWorkerError::PendingFieldSectionMissing {
                stream,
                index: slot,
            },
        )?;
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
        stream: u32,
        session: u32,
    ) -> Result<(), HttpWorkerError> {
        if !self.streams.contains_key(stream.into()) {
            return Err(HttpWorkerError::StreamMissing { stream });
        }
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
        if !self.pending_field_sections.contains_key(slot) {
            return Err(HttpWorkerError::PendingFieldSectionMissing {
                stream,
                index: slot,
            });
        }
        let pending = self.pending_field_sections.get_mut(slot).ok_or(
            HttpWorkerError::PendingFieldSectionMissing {
                stream,
                index: slot,
            },
        )?;
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
        stream: u32,
        bytes: &[u8],
    ) -> Result<usize, HttpWorkerError> {
        if !self.streams.contains_key(stream.into()) {
            return Err(HttpWorkerError::StreamMissing { stream });
        }
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
    pub(crate) fn remove_stream(&mut self, stream: u32) -> Result<(), HttpWorkerError> {
        let removed = self
            .streams
            .remove(stream)
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
                connection.peer_control = None;
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
        stream: u32,
        session: u32,
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
        stream: u32,
    ) -> Result<(), PeerControlError> {
        let parent = {
            if !self.streams.contains_key(stream.into()) {
                return Err(PeerControlError::Worker(HttpWorkerError::StreamMissing {
                    stream,
                }));
            }
            let stream_context =
                self.streams
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
        if !self.contexts.contains_key(parent.into()) {
            return Err(PeerControlError::Worker(
                HttpWorkerError::ParentContextMissing { parent },
            ));
        }
        let connection = self
            .contexts
            .get_mut(parent.into())
            .ok_or(PeerControlError::Worker(
                HttpWorkerError::ParentContextMissing { parent },
            ))?;
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
        self.streams.remove(stream);
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
    pub(crate) fn finish_peer_qpack_stream(&mut self, stream: u32) -> Result<(), HttpWorkerError> {
        let (parent, role) = {
            if !self.streams.contains_key(stream.into()) {
                return Err(HttpWorkerError::StreamMissing { stream });
            }
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
        if !self.contexts.contains_key(parent.into()) {
            return Err(HttpWorkerError::ParentContextMissing { parent });
        }
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
        self.streams.remove(stream);
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
        stream: u32,
    ) -> Result<(), HttpWorkerError> {
        let parent = {
            if !self.streams.contains_key(stream.into()) {
                return Err(HttpWorkerError::StreamMissing { stream });
            }
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
        if !self.contexts.contains_key(parent.into()) {
            return Err(HttpWorkerError::ParentContextMissing { parent });
        }
        self.streams.remove(stream);
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
