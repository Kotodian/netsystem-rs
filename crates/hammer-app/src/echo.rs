use hammer_core::error::HammerResult;

use crate::AppOp;

pub async fn echo_once(op: &AppOp) -> HammerResult<()> {
    let recv = op.recv().await?;
    op.send(recv.into_send()).await
}

pub async fn run_echo_loop(op: &AppOp, iterations: usize) -> HammerResult<()> {
    for _ in 0..iterations {
        echo_once(op).await?;
    }
    Ok(())
}
