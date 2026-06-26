use hammer_core::error::{HammerError, HammerResult};

use crate::AppOp;

const NOT_WIRED: &str = "vpp app boundary not wired (C2)";

pub async fn echo_once(_op: &AppOp) -> HammerResult<()> {
    Err(HammerError::internal(NOT_WIRED))
}

pub async fn run_echo_loop(_op: &AppOp, _iterations: usize) -> HammerResult<()> {
    Err(HammerError::internal(NOT_WIRED))
}
