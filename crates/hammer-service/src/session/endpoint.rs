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
