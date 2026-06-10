use hammer_adapter::{BufferIndex, DataPlaneRuntime};
use hammer_core::error::CoreResult;
use hammer_runtime::app::{AppContext, AppFlowId, AppSocketId};

use super::backend::AppIngressBackend;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppIngressObject {
    Flow(AppFlowId),
    Socket(AppSocketId),
}

#[derive(Clone)]
pub struct AppIngressTarget {
    app: AppContext,
    object: AppIngressObject,
}

impl AppIngressTarget {
    #[inline]
    pub fn new(app: AppContext, flow: AppFlowId) -> Self {
        Self::flow(app, flow)
    }

    #[inline]
    pub fn flow(app: AppContext, flow: AppFlowId) -> Self {
        Self {
            app,
            object: AppIngressObject::Flow(flow),
        }
    }

    #[inline]
    pub fn socket(app: AppContext, socket: AppSocketId) -> Self {
        Self {
            app,
            object: AppIngressObject::Socket(socket),
        }
    }

    #[inline]
    pub fn object(&self) -> AppIngressObject {
        self.object
    }

    #[inline]
    pub fn flow_id(&self) -> Option<AppFlowId> {
        match self.object {
            AppIngressObject::Flow(flow) => Some(flow),
            AppIngressObject::Socket(_) => None,
        }
    }

    #[inline]
    pub fn socket_id(&self) -> Option<AppSocketId> {
        match self.object {
            AppIngressObject::Flow(_) => None,
            AppIngressObject::Socket(socket) => Some(socket),
        }
    }

    #[inline]
    pub fn app(&self) -> &AppContext {
        &self.app
    }

    #[inline]
    pub fn post_recv_cqe_descriptor(
        &self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
    ) -> CoreResult<()> {
        self.post_recv_cqe_descriptor_with_fin(runtime, index, false)
    }

    #[inline]
    pub fn post_recv_cqe_descriptor_with_fin(
        &self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
        fin: bool,
    ) -> CoreResult<()> {
        AppIngressBackend.post_recv_cqe_with_fin(runtime, index, self, fin)
    }

    #[inline]
    pub fn post_recv_cqe(&self, runtime: &DataPlaneRuntime, index: BufferIndex) -> CoreResult<()> {
        self.post_recv_cqe_descriptor(runtime, index)
    }

    #[inline]
    pub fn post_recv_cqe_with_fin(
        &self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
        fin: bool,
    ) -> CoreResult<()> {
        self.post_recv_cqe_descriptor_with_fin(runtime, index, fin)
    }
}

impl std::fmt::Debug for AppIngressTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppIngressTarget")
            .field("object", &self.object)
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
    target.post_recv_cqe(runtime, index)
}
