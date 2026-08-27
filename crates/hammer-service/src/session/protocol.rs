//! VPP-shaped Session App callback surface.

use hammer_runtime::RuntimeResult;
use hammer_runtime::app::{SessionAppContext, SessionHandle};

use super::runtime::SessionWorker;

/// One Session App callback that operates on an exact Session.
pub type SessionAppCallback<Index = u32> =
    fn(&mut SessionWorker<Index>, u32, SessionAppContext) -> RuntimeResult<()>;

/// One Session App callback that carries a segment handle instead of a Session.
pub type SessionAppSegmentCallback<Index = u32> =
    fn(&mut SessionWorker<Index>, u64, SessionAppContext) -> RuntimeResult<()>;

/// VPP `session_cb_vft_t.migrate` callback with the old and new handles.
pub type SessionAppMigrateCallback<Index = u32> =
    fn(&mut SessionWorker<Index>, u32, SessionHandle, SessionAppContext) -> RuntimeResult<()>;

/// Concrete static callback table matching VPP `session_cb_vft_t`.
///
/// All 19 VPP callbacks are present. Unimplemented callbacks remain `None`;
/// the plugin-facing [`SessionApp`] trait supplies no-op defaults for them.
#[derive(Debug, Clone, Copy)]
pub struct SessionAppCallbacks<Index = u32> {
    pub add_segment: Option<SessionAppSegmentCallback<Index>>,
    pub del_segment: Option<SessionAppSegmentCallback<Index>>,
    pub accept: Option<SessionAppCallback<Index>>,
    pub connected: Option<SessionAppCallback<Index>>,
    pub disconnect: Option<SessionAppCallback<Index>>,
    pub reset: Option<SessionAppCallback<Index>>,
    pub transport_closed: Option<SessionAppCallback<Index>>,
    pub cleanup: Option<SessionAppCallback<Index>>,
    pub half_open_cleanup: Option<SessionAppCallback<Index>>,
    pub migrate: Option<SessionAppMigrateCallback<Index>>,
    pub listened: Option<SessionAppCallback<Index>>,
    pub unlistened: Option<SessionAppCallback<Index>>,
    pub builtin_rx: Option<SessionAppCallback<Index>>,
    pub builtin_tx: Option<SessionAppCallback<Index>>,
    pub fifo_tuning: Option<SessionAppCallback<Index>>,
    pub proxy_alloc_fifos: Option<SessionAppCallback<Index>>,
    pub proxy_write_early_data: Option<SessionAppCallback<Index>>,
    pub app_evt: Option<SessionAppCallback<Index>>,
    pub crypto_async: Option<SessionAppCallback<Index>>,
}

impl<Index> SessionAppCallbacks<Index> {
    pub const fn all_none() -> Self {
        Self {
            add_segment: None,
            del_segment: None,
            accept: None,
            connected: None,
            disconnect: None,
            reset: None,
            transport_closed: None,
            cleanup: None,
            half_open_cleanup: None,
            migrate: None,
            listened: None,
            unlistened: None,
            builtin_rx: None,
            builtin_tx: None,
            fifo_tuning: None,
            proxy_alloc_fifos: None,
            proxy_write_early_data: None,
            app_evt: None,
            crypto_async: None,
        }
    }
}

impl<Index> Default for SessionAppCallbacks<Index> {
    fn default() -> Self {
        Self::all_none()
    }
}

/// Plugin-facing Session App with worker-owned concrete state.
pub trait SessionApp: Sized + Send + 'static {
    const CONTEXT_CAPACITY: usize = 1_024;

    fn create(
        _: Option<u32>,
        _: Option<u32>,
        _: Option<u64>,
        _: Option<&str>,
    ) -> RuntimeResult<Self> {
        Err(super::SessionQueueError::SessionAppContextCreateUnsupported.into())
    }

    fn add_segment(&mut self, _: &mut SessionWorker<u32>, _: u64) -> RuntimeResult<()> {
        Ok(())
    }

    fn del_segment(&mut self, _: &mut SessionWorker<u32>, _: u64) -> RuntimeResult<()> {
        Ok(())
    }

    fn accept(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn connected(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn disconnect(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn reset(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn transport_closed(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn cleanup(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn half_open_cleanup(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn migrate(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: SessionHandle,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn listened(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn unlistened(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn builtin_rx(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn builtin_tx(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn fifo_tuning(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn proxy_alloc_fifos(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn proxy_write_early_data(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn app_evt(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn crypto_async(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: SessionAppContext,
    ) -> RuntimeResult<()> {
        Ok(())
    }
}
