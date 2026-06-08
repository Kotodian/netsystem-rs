use hammer_adapter::{BufferIndex, DataPlaneRuntime};
use hammer_core::error::CoreResult;
use hammer_runtime::app::{AppContext, AppFlowId};

use super::backend::AppIngressBackend;

#[derive(Clone)]
pub struct AppIngressTarget {
    app: AppContext,
    flow: AppFlowId,
}

impl AppIngressTarget {
    #[inline]
    pub fn new(app: AppContext, flow: AppFlowId) -> Self {
        Self { app, flow }
    }

    #[inline]
    pub fn flow(&self) -> AppFlowId {
        self.flow
    }

    #[inline]
    pub fn app(&self) -> &AppContext {
        &self.app
    }

    #[inline]
    pub fn complete_ingress_descriptor(
        &self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
    ) -> CoreResult<()> {
        AppIngressBackend.complete_ingress(runtime, index, self)
    }

    #[inline]
    pub fn complete_ingress(
        &self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
    ) -> CoreResult<()> {
        self.complete_ingress_descriptor(runtime, index)
    }
}

impl std::fmt::Debug for AppIngressTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppIngressTarget")
            .field("flow", &self.flow.value())
            .finish_non_exhaustive()
    }
}

#[allow(dead_code)]
#[inline]
pub fn deliver_buffer_to_app(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    target: &AppIngressTarget,
) -> CoreResult<()> {
    target.complete_ingress(runtime, index)
}
