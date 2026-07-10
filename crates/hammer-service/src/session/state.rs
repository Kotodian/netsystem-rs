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
    Active(ActiveState<Index>),
    AppClosed(AppClosedState<Index>),
    TransportClosed(TransportClosedState<Index>),
    Closed(ClosedState<Index>),
    TransportDeleted,
}

impl<Index: Copy + Eq> SessionState<Index> {
    #[inline]
    pub(crate) const fn active(index: Index) -> Self {
        Self::Active(ActiveState { index })
    }

    #[inline]
    pub(crate) const fn transport_index(self) -> Option<Index> {
        match self {
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
            Self::AppClosed(_) | Self::Closed(_) => false,
        }
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
            Self::TransportClosed(_) | Self::Closed(_) | Self::TransportDeleted => false,
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
            Self::TransportDeleted => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use hammer_infra::pool::Index;

    use super::SessionState;

    #[test]
    fn session_lifecycle_app_first_close_retains_index_until_transport_deleted() {
        let index = Index::new(4, 7);
        let mut state = SessionState::active(index);

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
        let index = Index::new(5, 11);
        let mut state = SessionState::active(index);
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
        let stale = Index::new(8, 1);
        let current = Index::new(8, 2);
        let mut state = SessionState::active(current);

        assert!(!state.on_transport_deleted(stale));
        assert!(matches!(state, SessionState::Active(_)));
        assert_eq!(state.transport_index(), Some(current));
    }

    #[test]
    fn transport_deleted_then_app_close_removes_session() {
        let index = Index::new(3, 9);
        let mut state = SessionState::active(index);

        assert!(!state.on_transport_deleted(index));
        assert!(matches!(state, SessionState::TransportDeleted));
        assert!(state.on_app_close());
    }
}
