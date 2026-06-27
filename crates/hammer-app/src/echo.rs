use hammer_core::error::HammerResult;

use crate::AppSession;

pub async fn echo_once(session: &AppSession, scratch: &mut [u8]) -> HammerResult<usize> {
    let read = session.recv(scratch).await;
    if read == 0 {
        return Ok(0);
    }
    session.send_all(&scratch[..read]).await
}

pub async fn run_echo_loop(
    session: &AppSession,
    scratch: &mut [u8],
    iterations: usize,
) -> HammerResult<usize> {
    let mut total = 0;
    for _ in 0..iterations {
        let wrote = echo_once(session, scratch).await?;
        if wrote == 0 {
            break;
        }
        total += wrote;
    }
    Ok(total)
}
