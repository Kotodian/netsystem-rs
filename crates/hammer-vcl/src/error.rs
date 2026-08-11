use hammer_app::attach::{AppClientError, ControlReplyKind};
use hammer_runtime::app::{AppSessionError, SessionConnectError};
use thiserror::Error;

use crate::{VclDirection, VclSessionHandle, VclSessionState};

/// Errors owned by the hammer-vcl client layer.
///
/// No numeric status is exposed here: upper-layer failures always preserve
/// the owning layer's typed error through `#[source] source`
/// (`AppClientError`, which in turn preserves `SessionConnectError` /
/// `SessionControlError` / `AppSessionError`).
#[derive(Debug, Error)]
pub enum VclError {
    #[error("Session control failed: {source}")]
    AppClient {
        #[source]
        source: AppClientError,
    },
    #[error("Session data operation failed: {source}")]
    AppSession {
        #[source]
        source: AppSessionError,
    },
    #[error("Session handle {handle:?} is stale or out of range")]
    InvalidHandle { handle: VclSessionHandle },
    #[error("Session pool capacity {capacity} is invalid")]
    PoolCapacityInvalid { capacity: usize },
    #[error("Session pool capacity {capacity} is exhausted")]
    PoolFull { capacity: usize },
    #[error("Session {session:?} cannot connect from state {state:?}")]
    NotConnectable {
        session: VclSessionHandle,
        state: VclSessionState,
    },
    #[error("Session {session:?} cannot be its own parent")]
    SelfParent { session: VclSessionHandle },
    #[error("parent Session {parent:?} is not established")]
    ParentNotEstablished { parent: VclSessionHandle },
    #[error("parent Session {parent:?} is not ready (state {state:?})")]
    ParentNotReady {
        parent: VclSessionHandle,
        state: VclSessionState,
    },
    #[error("Session {session:?} connect failed: {error}")]
    ConnectFailed {
        session: VclSessionHandle,
        #[source]
        error: SessionConnectError,
    },
    #[error("Session {session:?} detached without a connect error")]
    DetachedWithoutError { session: VclSessionHandle },
    #[error("Session {session:?} is not established")]
    SessionNotReady { session: VclSessionHandle },
    #[error("Session {session:?} does not permit {direction:?}")]
    DirectionInvalid {
        session: VclSessionHandle,
        direction: VclDirection,
    },
    #[error("the client inbox produced an unexpected {kind:?} reply")]
    UnexpectedReply { kind: ControlReplyKind },
}
