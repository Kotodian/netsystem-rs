#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CreatedState<Index> {
    index: Index,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PublishedState<Index> {
    index: Index,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActiveState<Index> {
    index: Index,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AppClosedState<Index> {
    index: Index,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TransportClosedState<Index> {
    index: Index,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ClosedState<Index> {
    index: Index,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionState<Index> {
    Creating,
    Created(CreatedState<Index>),
    Published(PublishedState<Index>),
    Active(ActiveState<Index>),
    AppClosed(AppClosedState<Index>),
    TransportClosed(TransportClosedState<Index>),
    Closed(ClosedState<Index>),
    TransportDeleted,
}

impl<Index: Copy + Eq> SessionState<Index> {
    #[inline]
    pub(crate) const fn creating() -> Self {
        Self::Creating
    }

    #[inline]
    pub(crate) const fn finish_creation(self, index: Index) -> Option<Self> {
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
    pub(crate) const fn rollback_index(self) -> Result<Option<Index>, Self> {
        match self {
            Self::Creating => Ok(None),
            Self::Created(state) => Ok(Some(state.index)),
            Self::Published(state) => Ok(Some(state.index)),
            _ => Err(self),
        }
    }

    #[inline]
    pub(crate) const fn transport_index(self) -> Option<Index> {
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
    pub(crate) fn on_transport_close(&mut self, index: Index) -> bool {
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
    pub(crate) fn on_transport_deleted(&mut self, index: Index) -> bool {
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

#[cfg(test)]
mod tests {
    use super::SessionState;

    #[test]
    fn session_lifecycle_app_first_close_retains_index_until_transport_deleted() {
        let index = 4u32;
        let mut state = active_state(index);

        assert!(!state.on_app_close());
        assert!(matches!(state, SessionState::AppClosed(_)));
        assert_eq!(state.transport_index(), Some(index));
        assert!(!state.on_transport_close(index));
        assert!(matches!(state, SessionState::Closed(_)));
        assert_eq!(state.transport_index(), Some(index));
        assert!(state.on_transport_deleted(index));
    }

    #[test]
    fn session_lifecycle_transport_first_close_retains_index_until_cleanup() {
        let index = 5u32;
        let mut state = active_state(index);
        let mut app_close_notifications = 0;

        if state.on_transport_close(index) {
            app_close_notifications += 1;
        }
        assert!(matches!(state, SessionState::TransportClosed(_)));
        assert_eq!(state.transport_index(), Some(index));
        assert!(!state.on_app_close());
        assert!(matches!(state, SessionState::Closed(_)));
        assert_eq!(state.transport_index(), Some(index));
        if state.on_transport_close(index) {
            app_close_notifications += 1;
        }
        assert_eq!(app_close_notifications, 1);
        assert!(state.on_transport_deleted(index));
    }

    #[test]
    fn stale_transport_deleted_notification_preserves_the_current_index() {
        let stale = 7u32;
        let current = 8u32;
        let mut state = active_state(current);

        assert!(!state.on_transport_deleted(stale));
        assert!(matches!(state, SessionState::Active(_)));
        assert_eq!(state.transport_index(), Some(current));
    }

    #[test]
    fn transport_deleted_then_app_close_removes_session() {
        let index = 3u32;
        let mut state = active_state(index);

        assert!(!state.on_transport_deleted(index));
        assert!(matches!(state, SessionState::TransportDeleted));
        assert!(state.on_app_close());
    }

    #[test]
    fn accepted_session_transitions_create_publish_notify_in_order() {
        let index = 7u32;
        let creating = SessionState::creating();
        assert!(creating.on_connection_published().is_none());
        assert!(creating.on_connected().is_none());

        let created = creating.finish_creation(index).expect("created session");
        assert_eq!(created.rollback_index(), Ok(Some(index)));
        assert!(created.on_connected().is_none());

        let (published, initial) = created
            .on_connection_published()
            .expect("published session");
        assert!(initial);
        assert!(published.on_connection_published().is_none());

        let active = published.on_connected().expect("active session");
        assert!(active.rollback_index().is_err());
        assert_eq!(active.transport_index(), Some(index));
    }

    #[test]
    fn published_session_transport_close_records_closing_state_without_notifying() {
        // An accepted child sits in Published until the Application replies to
        // ACCEPTED. A transport close during that wait is recorded without
        // notifying (VPP session_node.c:556-563 resends it when the reply
        // arrives); a plain Active session still notifies immediately.
        let index = 2u32;
        let mut state = SessionState::creating()
            .finish_creation(index)
            .expect("created session")
            .on_connection_published()
            .expect("published session")
            .0;
        let mut notifications = 0;
        if state.on_transport_close(index) {
            notifications += 1;
        }
        assert_eq!(notifications, 0);
        assert!(matches!(state, SessionState::TransportClosed(_)));
        assert_eq!(state.transport_index(), Some(index));
        assert!(!state.on_app_close());
        assert!(matches!(state, SessionState::Closed(_)));
        assert!(state.on_transport_deleted(index));
    }

    fn active_state(index: u32) -> SessionState<u32> {
        SessionState::creating()
            .finish_creation(index)
            .expect("created session")
            .on_connection_published()
            .expect("published session")
            .0
            .on_connected()
            .expect("active session")
    }
}
