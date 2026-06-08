use crate::app::AppIngressTarget;
use hammer_adapter::{BufferIndex, DataPlaneRuntime};
use hammer_core::error::{CoreError, CoreResult};

#[derive(Clone, Debug, Default)]
pub struct AppIngressBackend;

impl AppIngressBackend {
    #[inline]
    pub fn complete_ingress(
        &self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
        target: &AppIngressTarget,
    ) -> CoreResult<()> {
        target
            .app()
            .try_complete_recv_buffer(
                target.flow(),
                runtime.packet_buffers().clone(),
                index,
                false,
            )
            .map_err(|err| CoreError::internal(format!("enqueue app ingress recv cqe: {err}")))?;
        Ok(())
    }

    #[inline]
    pub fn deliver_ingress(
        &self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
        target: &AppIngressTarget,
    ) -> CoreResult<()> {
        self.complete_ingress(runtime, index, target)
    }
}
