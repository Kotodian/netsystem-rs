use hammer_core::error::HammerResult;

use crate::AppSession;

pub fn echo_once(session: &AppSession, scratch: &mut [u8]) -> HammerResult<usize> {
    let read = session.recv_bytes(scratch);
    if read == 0 {
        return Ok(0);
    }
    Ok(session.send_bytes(&scratch[..read]))
}

pub fn run_echo_loop(
    session: &AppSession,
    scratch: &mut [u8],
    iterations: usize,
) -> HammerResult<usize> {
    let mut total = 0;
    for _ in 0..iterations {
        let wrote = echo_once(session, scratch)?;
        if wrote == 0 {
            break;
        }
        total += wrote;
    }
    Ok(total)
}
