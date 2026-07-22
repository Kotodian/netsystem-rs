use hammer_infra::segment::Local;
use hammer_runtime::RuntimeResult;

use crate::AppSession;

pub fn echo_once(session: &AppSession<Local>, scratch: &mut [u8]) -> RuntimeResult<usize> {
    let read = session.recv_bytes(scratch);
    if read == 0 {
        return Ok(0);
    }
    session.consume_rx(read);
    session.send_bytes(&scratch[..read])
}

pub fn run_echo_loop(
    session: &AppSession<Local>,
    scratch: &mut [u8],
    iterations: usize,
) -> RuntimeResult<usize> {
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
