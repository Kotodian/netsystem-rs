use std::net::SocketAddr;

use crate::app::{AppIngressObject, AppIngressTarget};
use hammer_adapter::{BufferIndex, DataPlaneRuntime};
use hammer_core::error::{CoreError, CoreResult};

#[derive(Clone, Debug, Default)]
pub struct AppIngressBackend;

impl AppIngressBackend {
    #[inline]
    pub fn post_recv_cqe(
        &self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
        target: &AppIngressTarget,
    ) -> CoreResult<()> {
        self.post_recv_cqe_with_fin(runtime, index, target, false)
    }

    #[inline]
    pub fn post_recv_cqe_with_fin(
        &self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
        target: &AppIngressTarget,
        fin: bool,
    ) -> CoreResult<()> {
        match target.object() {
            AppIngressObject::Flow(flow) => target
                .app()
                .try_complete_recv_buffer(flow, runtime.packet_buffers().clone(), index, fin)
                .map_err(|err| {
                    CoreError::internal(format!("enqueue app ingress recv cqe: {err}"))
                })?,
            AppIngressObject::Socket(socket) => {
                let source = source_socket_addr(runtime, index)?;
                target
                    .app()
                    .try_complete_recv_from_buffer(
                        socket,
                        source,
                        runtime.packet_buffers().clone(),
                        index,
                        false,
                    )
                    .map_err(|err| {
                        CoreError::internal(format!("enqueue app ingress recv_from cqe: {err}"))
                    })?
            }
        }
        Ok(())
    }

    #[inline]
    pub fn deliver_ingress(
        &self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
        target: &AppIngressTarget,
    ) -> CoreResult<()> {
        self.post_recv_cqe(runtime, index, target)
    }
}

#[inline]
fn source_socket_addr(runtime: &DataPlaneRuntime, index: BufferIndex) -> CoreResult<SocketAddr> {
    let metadata = runtime.metadata(index)?;
    let source = metadata
        .source
        .ok_or_else(|| CoreError::internal("app recv_from completion requires source metadata"))?;
    Ok(SocketAddr::new(source.host, source.port))
}
