use std::time::Duration;

use hammer_core::error::CoreResult;
use hammer_core::lifecycle::Lifecycle;

use crate::{RouteDecision, RouteMetadata};

/// Central decision component that maps incoming connections to route actions.
pub trait Router: Lifecycle {
    fn reset_network(&self);
    fn match_route(&self, metadata: &mut RouteMetadata) -> CoreResult<RouteDecision>;
    fn prepare_route_metadata(&self, metadata: &mut RouteMetadata) -> CoreResult<()>;
    fn sniff_timeout(&self, metadata: &RouteMetadata) -> Option<Duration>;
    fn should_sniff(&self, metadata: &RouteMetadata) -> bool;
}
