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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use hammer_infra::segment::Segment;
    use hammer_runtime::app::{AppSessionConfig, SessionEvtType, SessionHandle, SessionMsgQueue};

    use super::*;

    fn local_session(fifo_capacity: usize) -> AppSession {
        let app_rx_mq =
            Arc::new(SessionMsgQueue::with_cfg(64, 64).expect("Local Application Rx MQ"));
        AppSession::new_in_segment(
            Segment::default(),
            AppSessionConfig::new(fifo_capacity, 16),
            SessionHandle::new(1, 0),
            app_rx_mq,
        )
        .expect("Local App Session")
    }

    #[test]
    fn local_echo_preserves_rx_bytes_while_tx_is_backpressured() {
        let session = local_session(8);
        session.send_bytes(b"123456").expect("prefill TX FIFO");
        session.enqueue_rx(b"abcdefgh").expect("fill RX FIFO");
        let mut scratch = [0; 8];

        assert_eq!(echo_once(&session, &mut scratch).expect("partial echo"), 2);
        assert_eq!(session.rx_fifo().max_dequeue(), 6);
        assert_eq!(echo_once(&session, &mut scratch).expect("backpressure"), 0);
        assert_eq!(session.rx_fifo().max_dequeue(), 6);

        assert_eq!(session.drop_tx_acked(8).expect("release TX capacity"), 8);
        assert!(
            std::iter::from_fn(|| session.evt_q().dequeue().ok().flatten())
                .any(|event| event.evt_type == SessionEvtType::TxDeq)
        );
        assert_eq!(
            run_echo_loop(&session, &mut scratch, 8).expect("resume echo"),
            6
        );
        assert_eq!(session.rx_fifo().max_dequeue(), 0);

        let mut echoed = [0; 8];
        assert_eq!(session.tx_fifo().peek(0, echoed.len(), &mut echoed), 6);
        assert_eq!(&echoed[..6], b"cdefgh");
    }
}
