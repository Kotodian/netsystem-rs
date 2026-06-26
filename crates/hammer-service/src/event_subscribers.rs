use std::sync::Arc;

use hammer_core::error::HammerResult;
use hammer_core::log::Logger;
use hammer_runtime::{ControlEventSubscriptionHandle, ControlThreadHandle};

pub(crate) fn build_standard_event_subscribers(
    _logger: Logger,
    _control_handle: Arc<ControlThreadHandle>,
) -> HammerResult<Vec<ControlEventSubscriptionHandle>> {
    Ok(Vec::new())
}
