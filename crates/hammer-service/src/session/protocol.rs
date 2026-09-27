//! VPP-shaped Session App callback surface.

use hammer_runtime::app::SessionHandle;
use hammer_runtime::{RuntimeError, RuntimeResult};

use super::runtime::SessionWorker;

/// Concrete static callback table matching VPP `session_cb_vft_t`.
///
/// All 19 VPP callbacks are present. Unimplemented callbacks remain `None`.
#[deprecated(note = "Use direct event fields on ApplicationConfig for new applications")]
#[derive(Debug, Clone, Copy)]
pub struct SessionAppVft {
    pub name: &'static str,
    pub add_segment: Option<fn(&mut SessionWorker, u64, u64) -> RuntimeResult<()>>,
    pub del_segment: Option<fn(&mut SessionWorker, u64, u64) -> RuntimeResult<()>>,
    pub accept: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub connected: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub disconnect: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub reset: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub transport_closed: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub cleanup: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub half_open_cleanup: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub migrate: Option<fn(&mut SessionWorker, u32, SessionHandle, u64) -> RuntimeResult<()>>,
    pub listened: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub unlistened: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub builtin_rx: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub builtin_tx: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub fifo_tuning: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub proxy_alloc_fifos: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub proxy_write_early_data: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub app_evt: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
    pub crypto_async: Option<fn(&mut SessionWorker, u32, u64) -> RuntimeResult<()>>,
}

#[allow(deprecated)]
impl SessionAppVft {
    pub const fn all_none(name: &'static str) -> Self {
        Self {
            name,
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

#[allow(deprecated)]
impl Default for SessionAppVft {
    fn default() -> Self {
        Self::all_none("")
    }
}

/// Registers one owner-defined Session App VFT on its owning Application.
#[deprecated(note = "Use direct event fields on ApplicationConfig for new applications")]
#[allow(deprecated)]
pub fn register_session_app(application: u32, vft: SessionAppVft) -> RuntimeResult<u32> {
    super::application_main()
        .register_session_app(application, vft)
        .map_err(RuntimeError::from)
}
