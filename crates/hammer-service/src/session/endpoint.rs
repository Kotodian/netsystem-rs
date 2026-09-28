use super::core::SessionHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionEndpoint<T> {
    transport: T,
    transport_protocol: u8,
}

impl<T> SessionEndpoint<T> {
    #[inline(always)]
    pub const fn new(transport: T, transport_protocol: u8) -> Self {
        Self {
            transport,
            transport_protocol,
        }
    }

    #[inline(always)]
    pub const fn transport(&self) -> &T {
        &self.transport
    }

    #[inline(always)]
    pub const fn transport_protocol(&self) -> u8 {
        self.transport_protocol
    }
}

bitflags::bitflags! {
    /// Session-level endpoint policy; transport-specific configuration stays in `T`.
    /// VPP: session_types.h:81-110, `session_endpoint_cfg_flags_t`.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct SessionEndpointFlags: u8 {
        const PROXY_LISTEN = 1 << 0;
        const SECURE = 1 << 1;
    }
}

/// Session-side request metadata beyond the transport endpoint and protocol.
/// VPP: session_types.h:100-112, `session_endpoint_cfg_t`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionEndpointConfig<T> {
    pub endpoint: SessionEndpoint<T>,
    pub application_worker: Option<u32>,
    pub opaque: Option<u32>,
    pub namespace: u32,
    pub original_transport_protocol: Option<u8>,
    pub parent: Option<SessionHandle>,
    pub flags: SessionEndpointFlags,
}

impl<T> SessionEndpointConfig<T> {
    /// VPP: session_types.h:138-150, `SESSION_ENDPOINT_CFG_NULL`.
    #[inline]
    pub const fn new(endpoint: SessionEndpoint<T>) -> Self {
        Self {
            endpoint,
            application_worker: None,
            opaque: None,
            namespace: 0,
            original_transport_protocol: None,
            parent: None,
            flags: SessionEndpointFlags::empty(),
        }
    }
}
