use crate::attach::{AppClientError, ControlReplyKind};
use hammer_runtime::app::{AppSessionError, SessionConnectError};
use thiserror::Error;

use super::session::{Direction, SessionState};

/// Errors owned by the `hammer-app` VCL client layer.
#[derive(Debug, Error)]
pub enum Error {
    #[error("Session control failed: {source}")]
    AppClient {
        #[from]
        source: AppClientError,
    },
    #[error("Session data operation failed: {source}")]
    AppSession {
        #[from]
        source: AppSessionError,
    },
    #[error("Session handle {handle:?} is stale or out of range")]
    InvalidHandle { handle: u32 },
    #[error("Session {session:?} cannot connect from state {state:?}")]
    NotConnectable { session: u32, state: SessionState },
    #[error("Session {session:?} cannot be its own parent")]
    SelfParent { session: u32 },
    #[error("parent Session {parent:?} is not established")]
    ParentNotEstablished { parent: u32 },
    #[error("parent Session {parent:?} is not ready (state {state:?})")]
    ParentNotReady { parent: u32, state: SessionState },
    #[error("Session {session:?} connect failed: {error}")]
    ConnectFailed {
        session: u32,
        #[source]
        error: SessionConnectError,
    },
    #[error("Session {session:?} detached without a connect error")]
    DetachedWithoutError { session: u32 },
    #[error("Session {session:?} is not established")]
    SessionNotReady { session: u32 },
    #[error("Session {session:?} does not permit {direction:?}")]
    DirectionInvalid { session: u32, direction: Direction },
    #[error("the client inbox produced an unexpected {kind:?} reply")]
    UnexpectedReply { kind: ControlReplyKind },
}
