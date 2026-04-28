use hammer_core::lifecycle::Lifecycle;

/// `adapter.HTTPTransport` — single configured HTTP transport. Will likely
/// wrap `hyper::client::conn` in M3 once HTTPS DNS lands.
pub trait HttpTransport: Send + Sync + 'static {
    fn reset(&self);
    fn close_idle_connections(&self);
}

/// `adapter.HTTPClientManager` — gives DNS transports + future modules a
/// shared, route-aware HTTP client. M2 ships an empty placeholder; the
/// `resolve_transport` API arrives in M3.
pub trait HttpClientManager: Lifecycle {
    fn reset_network(&self);
}
