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

use hammer_infra::pool::{Index, Pool};
use hammer_infra::thread_owned::ThreadOwnedError;
use hammer_runtime::DataWorkerId;
use hammer_runtime::app::SessionAppContext;
use hammer_runtime::session::SessionStreamDirection;
use hammer_service::session::{SessionId, SessionWorker};

use crate::http3::proto::coding::Decode;
use crate::http3::proto::error::ErrorCode;
use crate::http3::proto::stream::{StreamCategory, StreamType};
use crate::http3::proto::varint::UnexpectedEnd;

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
/// carrying QPACK_MAX_TABLE_CAPACITY=0 (0x01 0x00) and QPACK_BLOCKED_STREAMS=0
/// (0x07 0x00), in the write order of VPP `http3_conn_init` (http3.c:241-246):
/// stream type first, then the SETTINGS frame. The `http3::proto` encoders
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
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
        "peer {role:?} stream {stream:?} already registered on connection context {context:?}: HTTP/3 {code}"
    )]
    PeerStreamRoleDuplicate {
        stream: StreamContextId,
        context: ContextId,
        role: PeerUniStreamRole,
        code: ErrorCode,
    },
    #[error(
        "peer push stream {stream:?} cannot apply the server/client push policy on connection context {context:?}: connection role metadata is unavailable"
    )]
    PeerPushPolicyUnavailable {
        stream: StreamContextId,
        context: ContextId,
    },
    #[error("peer uni stream {stream:?} on connection context {context:?} has no decoded type to register")]
    PeerStreamRoleUnclassified {
        stream: StreamContextId,
        context: ContextId,
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

    /// Independent connection and stream pool capacities.
    pub(crate) fn with_capacities(connections: usize, streams: usize) -> Self {
        Self {
            contexts: Pool::with_capacity(connections),
            streams: Pool::with_capacity(streams),
        }
    }

    /// Allocates a context slot bound to the exact lower QUIC `session`.
    ///
    /// O(1); fails with `ContextCapacityExhausted` when the pool is full.
    pub(crate) fn allocate(&mut self, session: SessionId) -> Result<ContextId, HttpWorkerError> {
        self.contexts
            .insert(ConnectionContext {
                session,
                local_control: None,
                peer_control: None,
                peer_encoder: None,
                peer_decoder: None,
                peer_settings_pending: true,
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

    /// Releases a context slot back to the pool.
    ///
    /// O(1); the slot's generation advances, so previously issued identities
    /// become stale. Fails with `ContextMissing` for non-live identities.
    pub(crate) fn remove(&mut self, context: ContextId) -> Result<(), HttpWorkerError> {
        self.contexts
            .remove(context.into())
            .map(|_| ())
            .ok_or(HttpWorkerError::ContextMissing { context })
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
    /// registration applies VPP's server/client policy (http3.c:1712-1722),
    /// which requires connection role metadata that does not exist yet, so it
    /// is handed off as `PeerPushPolicyUnavailable` and records nothing.
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
                return Err(HttpWorkerError::PeerPushPolicyUnavailable {
                    stream,
                    context: parent,
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

    /// Releases a stream context slot back to the pool.
    ///
    /// O(1); the slot's generation advances, so previously issued identities
    /// become stale. Fails with `StreamMissing` for non-live identities.
    pub(crate) fn remove_stream(&mut self, stream: StreamContextId) -> Result<(), HttpWorkerError> {
        self.streams
            .remove(stream.into())
            .map(|_| ())
            .ok_or(HttpWorkerError::StreamMissing { stream })
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
    fn push_registration_hands_off_without_state_change() {
        let mut worker = HttpWorker::with_capacity(4);
        let (parent, stream) = peer_uni_pair(&mut worker);
        let push = worker.register_peer_uni_stream(stream, PeerUniStreamRole::Push);
        assert!(matches!(
            push,
            Err(HttpWorkerError::PeerPushPolicyUnavailable { stream: s, context: c })
                if s == stream && c == parent
        ));
        let connection = worker.get(parent).expect("live connection");
        assert_eq!(connection.peer_control, None);
        assert_eq!(connection.peer_encoder, None);
        assert_eq!(connection.peer_decoder, None);
        assert_eq!(
            worker.get_stream(stream).expect("live stream").peer_role,
            PeerUniStreamRole::Unclassified,
            "handed-off push records no stream role"
        );
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
}
