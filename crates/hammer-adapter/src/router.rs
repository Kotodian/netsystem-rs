use hammer_core::lifecycle::Lifecycle;

/// `adapter.Router` — central decision component that maps incoming connections
/// to outbounds based on `option.Rule` set. M2 keeps only the methods Service
/// orchestration touches (lifecycle + reset_network); the connection routing
/// hot path comes online in M4 alongside the rule engine.
pub trait Router: Lifecycle {
    fn reset_network(&self);
}
