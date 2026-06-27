use std::sync::Arc;

use hammer_core::error::HammerResult;

use crate::app::application::with_current_app_worker;
use crate::app::handle::SessionHandle;
use crate::app::session::{AppSession, AppSessionConfig};
use crate::spawn::DataRuntimeContext;

#[derive(Clone)]
pub struct AppContext {
    data_context: DataRuntimeContext,
    app_session_config: AppSessionConfig,
}

impl AppContext {
    #[inline]
    pub fn new(data_context: DataRuntimeContext, app_session_config: AppSessionConfig) -> Self {
        Self {
            data_context,
            app_session_config,
        }
    }

    #[inline]
    pub const fn app_session_config(&self) -> AppSessionConfig {
        self.app_session_config
    }

    #[inline]
    pub fn worker_count(&self) -> usize {
        self.data_context.worker_count()
    }

    pub fn session(&self, handle: SessionHandle) -> HammerResult<Option<Arc<AppSession>>> {
        let worker = handle.worker_index() as usize;
        if worker >= self.data_context.worker_count() {
            return Ok(None);
        }
        if self.data_context.current_worker_index() == Some(worker) {
            return Ok(with_current_app_worker(worker, |worker| {
                worker.session(handle)
            }));
        }
        self.data_context.call_blocking_on_worker(worker, move || {
            Ok(with_current_app_worker(worker, |worker| {
                worker.session(handle)
            }))
        })
    }

    pub async fn session_async(
        &self,
        handle: SessionHandle,
    ) -> HammerResult<Option<Arc<AppSession>>> {
        let worker = handle.worker_index() as usize;
        if worker >= self.data_context.worker_count() {
            return Ok(None);
        }
        if self.data_context.current_worker_index() == Some(worker) {
            return Ok(with_current_app_worker(worker, |worker| {
                worker.session(handle)
            }));
        }
        self.data_context
            .call_local_on_worker(worker, move || async move {
                Ok(with_current_app_worker(worker, |worker| {
                    worker.session(handle)
                }))
            })
            .await?
    }
}
