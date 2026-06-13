use std::net::Shutdown;

use hammer_adapter::DataWorkerId;
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::TcpConnectionId;
use hammer_runtime::app::AppUserData;

use crate::session::{
    AppSessionId, AppSessionSubmission, AppSessionTimerExpiry, AppSessionTimerToken,
    SessionProtocolContext, SessionProtocolOps,
};
use crate::transport::tcp::{TcpConnectionTable, TcpDataPlaneConnection, TcpLookupId};

#[derive(Debug)]
enum TcpPendingAppSubmission {
    Send {
        connection_id: TcpConnectionId,
        user_data: AppUserData,
    },
    Recv {
        connection_id: TcpConnectionId,
        user_data: AppUserData,
        max_len: u32,
    },
    Close {
        connection_id: TcpConnectionId,
        user_data: AppUserData,
    },
    Shutdown {
        connection_id: TcpConnectionId,
        how: Shutdown,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpSessionTimerKind {
    Retransmit,
    Persist,
    OutputPacing,
}

impl TcpSessionTimerKind {
    const fn token(self) -> AppSessionTimerToken {
        match self {
            Self::Retransmit => AppSessionTimerToken::new(1),
            Self::Persist => AppSessionTimerToken::new(2),
            Self::OutputPacing => AppSessionTimerToken::new(3),
        }
    }

    fn from_token(token: AppSessionTimerToken) -> CoreResult<Self> {
        match token.get() {
            1 => Ok(Self::Retransmit),
            2 => Ok(Self::Persist),
            3 => Ok(Self::OutputPacing),
            other => Err(CoreError::internal(format!(
                "unknown TCP session timer token {other}"
            ))),
        }
    }
}

pub struct TcpSessionProtocol {
    worker: DataWorkerId,
    connections: TcpConnectionTable,
    app_session_to_connection: hammer_infra::map::FlatHashTable<u64, TcpConnectionId>,
    connection_to_app_session: hammer_infra::map::FlatHashTable<u64, AppSessionId>,
    ready_connections: hammer_infra::vec::Vec<TcpConnectionId>,
    ready_slots: hammer_infra::map::FlatHashTable<u64, usize>,
    pending_app_submissions: hammer_infra::vec::Vec<TcpPendingAppSubmission>,
    pending_timers: hammer_infra::vec::Vec<(TcpConnectionId, TcpSessionTimerKind)>,
}

impl TcpSessionProtocol {
    #[inline]
    pub fn new(worker: DataWorkerId) -> Self {
        Self {
            worker,
            connections: TcpConnectionTable::empty(),
            app_session_to_connection: hammer_infra::map::FlatHashTable::new(),
            connection_to_app_session: hammer_infra::map::FlatHashTable::new(),
            ready_connections: hammer_infra::vec::Vec::new(),
            ready_slots: hammer_infra::map::FlatHashTable::new(),
            pending_app_submissions: hammer_infra::vec::Vec::new(),
            pending_timers: hammer_infra::vec::Vec::new(),
        }
    }

    #[inline]
    pub fn worker(&self) -> DataWorkerId {
        self.worker
    }

    pub fn install_connection(&mut self, connection: TcpDataPlaneConnection) -> CoreResult<()> {
        let connection_id = connection
            .connection_id()
            .ok_or_else(|| CoreError::internal("TCP session connection is missing id"))?;
        if connection.owner_worker() != self.worker {
            return Err(CoreError::internal(format!(
                "TCP session worker mismatch: connection owner={} session worker={}",
                connection.owner_worker().slot(),
                self.worker.slot()
            )));
        }
        self.connections.insert(connection);
        self.mark_connection_ready(connection_id);
        Ok(())
    }

    #[inline]
    pub fn connection(&self, connection_id: TcpConnectionId) -> Option<&TcpDataPlaneConnection> {
        self.connections.lookup_by_connection_id(connection_id)
    }

    #[inline]
    pub fn lookup_connection(&self, lookup_id: TcpLookupId) -> Option<&TcpDataPlaneConnection> {
        self.connections.lookup_by_lookup_id(lookup_id)
    }

    pub fn bind_app_session(
        &mut self,
        session_id: AppSessionId,
        connection_id: TcpConnectionId,
    ) -> CoreResult<()> {
        if self.connection(connection_id).is_none() {
            return Err(CoreError::internal(format!(
                "TCP session protocol connection {} is missing",
                connection_id.get()
            )));
        }
        self.app_session_to_connection
            .insert(session_id.get(), connection_id);
        self.connection_to_app_session
            .insert(connection_id.get(), session_id);
        Ok(())
    }

    #[inline]
    pub fn connection_for_session(&self, session_id: AppSessionId) -> Option<TcpConnectionId> {
        self.app_session_to_connection.lookup(&session_id.get())
    }

    #[inline]
    pub fn session_for_connection(&self, connection_id: TcpConnectionId) -> Option<AppSessionId> {
        self.connection_to_app_session.lookup(&connection_id.get())
    }

    #[inline]
    pub fn retransmit_timer_token() -> AppSessionTimerToken {
        TcpSessionTimerKind::Retransmit.token()
    }

    #[inline]
    pub fn persist_timer_token() -> AppSessionTimerToken {
        TcpSessionTimerKind::Persist.token()
    }

    #[inline]
    pub fn output_pacing_timer_token() -> AppSessionTimerToken {
        TcpSessionTimerKind::OutputPacing.token()
    }

    pub fn mark_connection_ready(&mut self, connection_id: TcpConnectionId) {
        if self.ready_slots.lookup(&connection_id.get()).is_some() {
            return;
        }
        let slot = self.ready_connections.len();
        self.ready_connections.push(connection_id);
        self.ready_slots.insert(connection_id.get(), slot);
    }

    pub fn take_ready_connections(&mut self) -> hammer_infra::vec::Vec<TcpConnectionId> {
        let ready = self.ready_connections.iter().copied().collect();
        self.ready_connections.clear();
        self.ready_slots = hammer_infra::map::FlatHashTable::new();
        ready
    }

    pub fn take_pending_app_submissions_for_test(
        &mut self,
    ) -> hammer_infra::vec::Vec<(TcpConnectionId, AppUserData)> {
        self.pending_app_submissions
            .drain(..)
            .map(|submission| match submission {
                TcpPendingAppSubmission::Send {
                    connection_id,
                    user_data,
                }
                | TcpPendingAppSubmission::Close {
                    connection_id,
                    user_data,
                } => (connection_id, user_data),
                TcpPendingAppSubmission::Recv {
                    connection_id,
                    user_data,
                    max_len,
                } => {
                    let _ = max_len;
                    (connection_id, user_data)
                }
                TcpPendingAppSubmission::Shutdown { connection_id, how } => {
                    let _ = how;
                    (connection_id, AppUserData::new(0))
                }
            })
            .collect()
    }

    pub fn take_pending_timers_for_test(
        &mut self,
    ) -> hammer_infra::vec::Vec<(TcpConnectionId, AppSessionTimerToken)> {
        self.pending_timers
            .drain(..)
            .map(|(connection_id, kind)| (connection_id, kind.token()))
            .collect()
    }
}

impl SessionProtocolOps for TcpSessionProtocol {
    fn handle_submission(
        &mut self,
        context: &mut SessionProtocolContext<'_>,
        submission: AppSessionSubmission,
    ) -> CoreResult<()> {
        let session_id = submission.session_id();
        let connection_id = self.connection_for_session(session_id).ok_or_else(|| {
            CoreError::internal(format!(
                "TCP session protocol binding missing for session {}",
                session_id.get()
            ))
        })?;

        match submission {
            AppSessionSubmission::Send(send) => {
                self.pending_app_submissions
                    .push(TcpPendingAppSubmission::Send {
                        connection_id,
                        user_data: send.descriptor().user_data(),
                    });
            }
            AppSessionSubmission::Recv(recv) => {
                self.pending_app_submissions
                    .push(TcpPendingAppSubmission::Recv {
                        connection_id,
                        user_data: recv.descriptor().user_data(),
                        max_len: recv.max_len(),
                    });
            }
            AppSessionSubmission::Close(close) => {
                self.pending_app_submissions
                    .push(TcpPendingAppSubmission::Close {
                        connection_id,
                        user_data: close.descriptor().user_data(),
                    });
            }
            AppSessionSubmission::Shutdown(shutdown) => {
                self.pending_app_submissions
                    .push(TcpPendingAppSubmission::Shutdown {
                        connection_id,
                        how: shutdown.how(),
                    });
            }
        }

        self.mark_connection_ready(connection_id);
        context.mark_ready(session_id);
        Ok(())
    }

    fn handle_timer_expiry(
        &mut self,
        context: &mut SessionProtocolContext<'_>,
        expiry: AppSessionTimerExpiry,
    ) -> CoreResult<()> {
        let connection_id = self
            .connection_for_session(expiry.session_id())
            .ok_or_else(|| {
                CoreError::internal(format!(
                    "TCP session protocol binding missing for session {}",
                    expiry.session_id().get()
                ))
            })?;
        let kind = TcpSessionTimerKind::from_token(expiry.token())?;
        self.pending_timers.push((connection_id, kind));
        self.mark_connection_ready(connection_id);
        context.mark_ready(expiry.session_id());
        Ok(())
    }

    fn handle_ready(
        &mut self,
        _context: &mut SessionProtocolContext<'_>,
        session_id: AppSessionId,
    ) -> CoreResult<()> {
        let connection_id = self.connection_for_session(session_id).ok_or_else(|| {
            CoreError::internal(format!(
                "TCP session protocol binding missing for session {}",
                session_id.get()
            ))
        })?;
        self.mark_connection_ready(connection_id);
        Ok(())
    }
}
