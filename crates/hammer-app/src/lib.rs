//! Application-facing access to Hammer's VPP-shaped app/session boundary.
//!
//! Embedding process entry points must initialize
//! [`hammer_infra::main_heap`] before constructing a runtime or loading DSOs.

pub mod attach;
pub mod echo;
pub mod remote_session;
pub mod tcp;
pub mod udp;

use std::sync::Arc;

use hammer_infra::segment::Local;
use hammer_runtime::RuntimeResult;
use hammer_runtime::app as runtime_app;
pub use hammer_runtime::app::{AppSession, AppSessionConfig, SessionHandle};
pub use hammer_runtime::spawn::DataRuntimeContext;

#[derive(Clone)]
pub struct AppContext {
    inner: runtime_app::AppContext<Local>,
}

impl AppContext {
    #[inline]
    pub fn new(data_context: DataRuntimeContext, app_session_config: AppSessionConfig) -> Self {
        Self {
            inner: runtime_app::AppContext::<Local>::new(data_context, app_session_config),
        }
    }

    #[inline]
    pub fn app_session_config(&self) -> AppSessionConfig {
        self.inner.app_session_config()
    }

    #[inline]
    pub fn worker_count(&self) -> usize {
        self.inner.worker_count()
    }

    #[inline]
    pub fn session(&self, handle: SessionHandle) -> RuntimeResult<Option<Arc<AppSession<Local>>>> {
        self.inner.session(handle)
    }

    #[inline]
    pub async fn session_async(
        &self,
        handle: SessionHandle,
    ) -> RuntimeResult<Option<Arc<AppSession<Local>>>> {
        self.inner.session_async(handle).await
    }
}

#[derive(Clone)]
pub struct App {
    context: AppContext,
}

impl App {
    #[inline]
    pub fn new(data_context: DataRuntimeContext) -> Self {
        Self::with_session_config(data_context, AppSessionConfig::default())
    }

    #[inline]
    pub fn with_session_config(
        data_context: DataRuntimeContext,
        app_session_config: AppSessionConfig,
    ) -> Self {
        Self {
            context: AppContext::new(data_context, app_session_config),
        }
    }

    #[inline]
    pub fn context(&self) -> &AppContext {
        &self.context
    }

    #[inline]
    pub fn session(&self, handle: SessionHandle) -> RuntimeResult<Option<Arc<AppSession<Local>>>> {
        self.context.session(handle)
    }

    #[inline]
    pub async fn session_async(
        &self,
        handle: SessionHandle,
    ) -> RuntimeResult<Option<Arc<AppSession<Local>>>> {
        self.context.session_async(handle).await
    }
}

impl From<AppContext> for App {
    #[inline]
    fn from(context: AppContext) -> Self {
        Self { context }
    }
}
