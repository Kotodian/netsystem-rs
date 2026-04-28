use std::sync::Mutex;

use crate::error::HammerError;
use crate::log::Logger;

use super::lifecycle::{Lifecycle, StartStage};

/// Stand-in lifecycle used in M1 to prove the start/close pipeline runs end-to-end.
/// Each stub logs its own progress so that the macOS demo can visually confirm the
/// 11-element `LIFECYCLE_ORDER` is walked in the correct order.
pub struct StubManager {
    name: &'static str,
    logger: Logger,
}

impl StubManager {
    pub fn new(name: &'static str, logger: Logger) -> Self {
        Self { name, logger }
    }
}

impl Lifecycle for StubManager {
    fn name(&self) -> &str {
        self.name
    }

    fn start(&self, stage: StartStage) -> Result<(), HammerError> {
        self.logger.debug(format!("stage {}", stage.name()));
        Ok(())
    }

    fn close(&self) -> Result<(), HammerError> {
        self.logger.debug("close");
        Ok(())
    }
}

/// Toggle-only `pause` / `wake` book-keeping. M1 is intentionally a no-op beyond
/// recording the bool — M2 promotes this to a real broadcast/notify channel.
pub struct PauseManager {
    paused: Mutex<bool>,
}

impl PauseManager {
    pub fn new() -> Self {
        Self {
            paused: Mutex::new(false),
        }
    }

    pub fn pause(&self) {
        *self.paused.lock().expect("pause poisoned") = true;
    }

    pub fn wake(&self) {
        *self.paused.lock().expect("pause poisoned") = false;
    }

    pub fn is_paused(&self) -> bool {
        *self.paused.lock().expect("pause poisoned")
    }
}

impl Default for PauseManager {
    fn default() -> Self {
        Self::new()
    }
}
