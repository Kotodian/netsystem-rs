#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CreatedState {
    index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PublishedState {
    index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActiveState {
    index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AppClosedState {
    index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TransportClosedState {
    index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ClosedState {
    index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionState {
    Creating,
    Created(CreatedState),
    Published(PublishedState),
    Active(ActiveState),
    AppClosed(AppClosedState),
    TransportClosed(TransportClosedState),
    Closed(ClosedState),
    TransportDeleted,
}

impl SessionState {
    #[inline]
    pub(crate) const fn creating() -> Self {
        Self::Creating
    }

    #[inline]
    pub(crate) const fn finish_creation(self, index: u32) -> Option<Self> {
        match self {
            Self::Creating => Some(Self::Created(CreatedState { index })),
            _ => None,
        }
    }

    #[inline]
    pub(crate) const fn on_connection_published(self) -> Option<(Self, bool)> {
        match self {
            Self::Created(state) => {
                Some((Self::Published(PublishedState { index: state.index }), true))
            }
            Self::Active(_) | Self::AppClosed(_) | Self::TransportClosed(_) | Self::Closed(_) => {
                Some((self, false))
            }
            Self::Creating | Self::Published(_) | Self::TransportDeleted => None,
        }
    }

    #[inline]
    pub(crate) const fn on_connected(self) -> Option<Self> {
        match self {
            Self::Published(state) => Some(Self::Active(ActiveState { index: state.index })),
            _ => None,
        }
    }

    #[inline]
    pub(crate) const fn rollback_index(self) -> Result<Option<u32>, Self> {
        match self {
            Self::Creating => Ok(None),
            Self::Created(state) => Ok(Some(state.index)),
            Self::Published(state) => Ok(Some(state.index)),
            _ => Err(self),
        }
    }

    #[inline]
    pub(crate) const fn transport_index(self) -> Option<u32> {
        match self {
            Self::Creating => None,
            Self::Created(state) => Some(state.index),
            Self::Published(state) => Some(state.index),
            Self::Active(state) => Some(state.index),
            Self::AppClosed(state) => Some(state.index),
            Self::TransportClosed(state) => Some(state.index),
            Self::Closed(state) => Some(state.index),
            Self::TransportDeleted => None,
        }
    }

    /// Returns true when the session entry can be removed.
    pub(crate) fn on_app_close(&mut self) -> bool {
        match *self {
            Self::Active(state) => {
                *self = Self::AppClosed(AppClosedState { index: state.index });
                false
            }
            Self::TransportClosed(state) => {
                *self = Self::Closed(ClosedState { index: state.index });
                false
            }
            Self::TransportDeleted => true,
            Self::Creating
            | Self::Created(_)
            | Self::Published(_)
            | Self::AppClosed(_)
            | Self::Closed(_) => false,
        }
    }

    /// App-close dispatch guard for the worker-local transport action seam;
    /// mirrors VPP `session_transport_close`/`session_transport_reset`
    /// (session.c:1657-1703): sessions at or beyond AppClosed, and sessions
    /// still in Creating (no transport index yet), return false so the
    /// transport is not notified; Created/Published/Active sessions record
    /// AppClosed and return true so the transport action runs with the
    /// close already recorded.
    pub(crate) fn on_app_close_dispatch(&mut self) -> bool {
        if matches!(
            *self,
            Self::Creating
                | Self::AppClosed(_)
                | Self::TransportClosed(_)
                | Self::Closed(_)
                | Self::TransportDeleted
        ) {
            return false;
        }
        // Remaining states with a transport index: Created, Published, Active.
        let Some(index) = self.transport_index() else {
            return false;
        };
        *self = Self::AppClosed(AppClosedState { index });
        true
    }

    /// Returns true when the app must receive its single close notification.
    pub(crate) fn on_transport_close(&mut self, index: u32) -> bool {
        if self.transport_index() != Some(index) {
            return false;
        }
        match *self {
            Self::Active(state) => {
                *self = Self::TransportClosed(TransportClosedState { index: state.index });
                true
            }
            Self::AppClosed(state) => {
                *self = Self::Closed(ClosedState { index: state.index });
                false
            }
            // A published (accepted-waiting or not-yet-connected) Session that
            // the transport closes records the closing state without notifying
            // the Application yet: for accepted children the close is resent
            // when the Application replies to ACCEPTED (VPP
            // `session_mq_accepted_reply_handler`, session_node.c:556-563
            // resends the close when `old_state >= TRANSPORT_CLOSING`).
            Self::Published(state) => {
                *self = Self::TransportClosed(TransportClosedState { index: state.index });
                false
            }
            Self::Creating
            | Self::Created(_)
            | Self::TransportClosed(_)
            | Self::Closed(_)
            | Self::TransportDeleted => false,
        }
    }

    /// Returns true when the session entry can be removed.
    pub(crate) fn on_transport_deleted(&mut self, index: u32) -> bool {
        if self.transport_index() != Some(index) {
            return false;
        }
        match *self {
            Self::Active(_) | Self::TransportClosed(_) => {
                *self = Self::TransportDeleted;
                false
            }
            Self::AppClosed(_) | Self::Closed(_) => true,
            Self::Creating | Self::Created(_) | Self::Published(_) | Self::TransportDeleted => {
                false
            }
        }
    }
}
