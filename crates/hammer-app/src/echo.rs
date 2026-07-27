use crate::{AppSession, AppSessionError};

pub fn echo_once(session: &AppSession, scratch: &mut [u8]) -> Result<usize, AppSessionError> {
    let read = session.recv_bytes(scratch);
    if read == 0 {
        return Ok(0);
    }
    session.consume_rx(read);
    session.send_bytes(&scratch[..read])
}

pub fn run_echo_loop(
    session: &AppSession,
    scratch: &mut [u8],
    iterations: usize,
) -> Result<usize, AppSessionError> {
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
