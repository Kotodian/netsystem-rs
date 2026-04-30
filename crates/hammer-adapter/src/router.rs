use hammer_core::lifecycle::Lifecycle;

/// Central decision component that maps incoming connections to route actions.
pub trait Router: Lifecycle {
    fn reset_network(&self);
}
