#[cfg(feature = "wireguard")]
pub mod wireguard;

#[cfg(feature = "wireguard")]
#[derive(Debug, Clone)]
pub(crate) struct EndpointRuntimeOptions<T> {
    pub(crate) id: String,
    pub(crate) interface: hammer_core::config::EndpointInterfaceOptions,
    pub(crate) protocol: T,
}

#[cfg(feature = "wireguard")]
impl<T> EndpointRuntimeOptions<T> {
    pub(crate) fn from_endpoint(option: &hammer_core::config::Endpoint, protocol: T) -> Self {
        Self {
            id: option.id.clone(),
            interface: option.interface.clone(),
            protocol,
        }
    }
}
