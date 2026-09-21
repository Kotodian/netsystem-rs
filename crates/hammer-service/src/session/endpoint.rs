#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionEndpoint<T> {
    transport: T,
    transport_protocol: u8,
}

impl<T> SessionEndpoint<T> {
    #[inline]
    pub const fn new(transport: T, transport_protocol: u8) -> Self {
        Self {
            transport,
            transport_protocol,
        }
    }

    #[inline]
    pub const fn transport(&self) -> &T {
        &self.transport
    }

    #[inline]
    pub const fn transport_protocol(&self) -> u8 {
        self.transport_protocol
    }

    #[inline]
    pub fn into_transport(self) -> T {
        self.transport
    }
}
