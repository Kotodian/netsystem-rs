use crate::app::session::AppSessionConfig;
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
}
