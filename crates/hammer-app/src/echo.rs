use crate::{AppSession, AppSessionError};

/// Forwards one app-readable span into the Session TX FIFO.
///
/// Only bytes accepted by TX are consumed from RX. When TX is full, this arms
/// its dequeue notification before rechecking capacity; the caller then
/// returns to Session MQ dispatch and invokes the echo again for `TxDeq`.
pub fn echo_once(session: &AppSession, scratch: &mut [u8]) -> Result<usize, AppSessionError> {
    loop {
        let read = session.recv_bytes(scratch);
        if read == 0 {
            return Ok(0);
        }
        let wrote = session.send_bytes(&scratch[..read])?;
        if wrote != 0 {
            session.consume_rx(wrote);
            return Ok(wrote);
        }

        session.want_tx_notification();
        if session.tx_fifo().max_enqueue() == 0 {
            return Ok(0);
        }
        session.clear_tx_notification();
    }
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
