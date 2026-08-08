use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{DataWorkerId, SessionListenEndpoint, SessionListenerId};

use super::{
    ApplicationConnectionId, SessionAppId, SessionHandle, SessionMsgQueue, SessionMsgQueueError,
};

pub const APPLICATION_SESSION_CONTROL_BYTES: usize = 2048;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplicationSessionRequest {
    Listen {
        context: u64,
        transport: String,
        endpoint: SessionListenEndpoint,
        app: Option<SessionAppId>,
        opaque: Option<u64>,
    },
    Connect {
        context: u64,
        transport: String,
        remote: SocketAddr,
        local: Option<SocketAddr>,
        worker: DataWorkerId,
        server_name: Option<String>,
        app: Option<SessionAppId>,
        opaque: Option<u64>,
    },
    Unlisten {
        context: u64,
        listener: SessionListenerId,
    },
}

impl ApplicationSessionRequest {
    pub const fn context(&self) -> u64 {
        match self {
            Self::Listen { context, .. }
            | Self::Connect { context, .. }
            | Self::Unlisten { context, .. } => *context,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplicationSessionStatus {
    Success,
    InvalidRequest,
    ApplicationMissing,
    ApplicationControlWrongThread,
    TransportMissing,
    TransportDuplicate,
    SessionAppMissing,
    SessionAppDuplicate,
    ListenerCapacityExhausted,
    ListenerMissing,
    ListenerNotOwned,
    ApplicationCapacityExhausted,
    ApplicationMqCapacityInvalid,
    ApplicationMqResourceUnavailable,
    ApplicationMqAlreadyAttached,
    SessionMainUnavailable,
    ConnectionCapacityExhausted,
    ConnectionMissing,
    ConnectionNotOwned,
    ConnectionAlreadyCompleted,
    ConnectionNotCompleted,
    TransportListenUnsupported,
    TransportListenFailed,
    TransportUnlistenFailed,
    TransportConnectUnsupported,
    TransportConnectFailed,
    TlsAlert { alert: u8 },
    QuicVersionUnsupported,
    HandshakeTimedOut,
    ConnectionRefused,
    ConnectionReset,
    PeerClosed { code: u64 },
    QuicTransportError { code: u64 },
    LocalConnectionResourceExhausted,
    LocalConnectionClosed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplicationSessionReply {
    Response {
        context: u64,
        status: ApplicationSessionStatus,
        handle: u64,
    },
    Connected {
        connection: ApplicationConnectionId,
        session: SessionHandle,
    },
    ConnectFailed {
        connection: ApplicationConnectionId,
        status: ApplicationSessionStatus,
    },
}

impl ApplicationSessionReply {
    pub const fn response(context: u64, status: ApplicationSessionStatus, handle: u64) -> Self {
        Self::Response {
            context,
            status,
            handle,
        }
    }

    pub const fn connected(connection: ApplicationConnectionId, session: SessionHandle) -> Self {
        Self::Connected {
            connection,
            session,
        }
    }

    pub const fn connect_failed(
        connection: ApplicationConnectionId,
        status: ApplicationSessionStatus,
    ) -> Self {
        Self::ConnectFailed { connection, status }
    }

    pub const fn context(self) -> u64 {
        match self {
            Self::Response { context, .. } => context,
            Self::Connected { connection, .. } | Self::ConnectFailed { connection, .. } => {
                connection.raw()
            }
        }
    }

    pub const fn status(self) -> ApplicationSessionStatus {
        match self {
            Self::Response { status, .. } | Self::ConnectFailed { status, .. } => status,
            Self::Connected { .. } => ApplicationSessionStatus::Success,
        }
    }

    pub const fn is_response(self) -> bool {
        matches!(self, Self::Response { .. })
    }

    pub const fn listener(self) -> SessionListenerId {
        match self {
            Self::Response { handle, .. } => SessionListenerId::from_raw(handle),
            Self::Connected { .. } | Self::ConnectFailed { .. } => SessionListenerId::from_raw(0),
        }
    }

    pub const fn connection(self) -> ApplicationConnectionId {
        match self {
            Self::Response { handle, .. } => ApplicationConnectionId::from_raw(handle),
            Self::Connected { connection, .. } | Self::ConnectFailed { connection, .. } => {
                connection
            }
        }
    }
}

#[hammer_component_macros::runtime_error(subsystem = "application session MQ")]
#[derive(Debug, Error)]
pub enum ApplicationSessionMqError {
    #[error("failed to encode Application Session request")]
    RequestEncode {
        #[source]
        source: bincode::Error,
    },
    #[error("failed to decode Application Session request")]
    RequestDecode {
        #[source]
        source: bincode::Error,
    },
    #[error("failed to encode Application Session reply")]
    ReplyEncode {
        #[source]
        source: bincode::Error,
    },
    #[error("failed to decode Application Session reply")]
    ReplyDecode {
        #[source]
        source: bincode::Error,
    },
    #[error("failed to enqueue Application Session request")]
    RequestEnqueue {
        #[source]
        source: SessionMsgQueueError,
    },
    #[error("failed to dequeue Application Session request")]
    RequestDequeue {
        #[source]
        source: SessionMsgQueueError,
    },
    #[error("failed to enqueue Application Session reply")]
    ReplyEnqueue {
        #[source]
        source: SessionMsgQueueError,
    },
    #[error("failed to dequeue Application Session reply")]
    ReplyDequeue {
        #[source]
        source: SessionMsgQueueError,
    },
}

pub fn enqueue_application_session_request(
    queue: &SessionMsgQueue,
    request: &ApplicationSessionRequest,
) -> Result<(), ApplicationSessionMqError> {
    let payload = bincode::serialize(request)
        .map_err(|source| ApplicationSessionMqError::RequestEncode { source })?;
    queue
        .enqueue_ctrl_payload(&payload)
        .map_err(|source| ApplicationSessionMqError::RequestEnqueue { source })
}

pub fn dequeue_application_session_request(
    queue: &SessionMsgQueue,
) -> Result<Option<ApplicationSessionRequest>, ApplicationSessionMqError> {
    let mut payload = [0_u8; APPLICATION_SESSION_CONTROL_BYTES - size_of::<u32>()];
    let Some(bytes) = queue
        .dequeue_ctrl_payload(&mut payload)
        .map_err(|source| ApplicationSessionMqError::RequestDequeue { source })?
    else {
        return Ok(None);
    };
    bincode::deserialize(&payload[..bytes])
        .map(Some)
        .map_err(|source| ApplicationSessionMqError::RequestDecode { source })
}

pub fn enqueue_application_session_reply(
    queue: &SessionMsgQueue,
    reply: &ApplicationSessionReply,
) -> Result<(), ApplicationSessionMqError> {
    let payload = bincode::serialize(reply)
        .map_err(|source| ApplicationSessionMqError::ReplyEncode { source })?;
    queue
        .enqueue_ctrl_payload(&payload)
        .map_err(|source| ApplicationSessionMqError::ReplyEnqueue { source })
}

pub fn dequeue_application_session_reply(
    queue: &SessionMsgQueue,
) -> Result<Option<ApplicationSessionReply>, ApplicationSessionMqError> {
    let mut payload = [0_u8; APPLICATION_SESSION_CONTROL_BYTES - size_of::<u32>()];
    let Some(bytes) = queue
        .dequeue_ctrl_payload(&mut payload)
        .map_err(|source| ApplicationSessionMqError::ReplyDequeue { source })?
    else {
        return Ok(None);
    };
    bincode::deserialize(&payload[..bytes])
        .map(Some)
        .map_err(|source| ApplicationSessionMqError::ReplyDecode { source })
}
