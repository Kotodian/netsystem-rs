use std::time::Instant;

use hammer_core::error::CoreResult;
use hammer_core::protocol::tcp::TcpSeq;

use crate::session::protocol::tcp::state::TcpSessionState;

use super::congestion::TcpCongestionAckSample;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpCongestionAckObservation {
    pub accepted_acknowledgment: u32,
    pub now: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpCongestionSendObservation {
    pub bytes_sent: u32,
    pub bytes_in_flight: u32,
    pub now: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpCongestionLossObservation {
    pub bytes_lost: u32,
}

#[derive(Debug, Default)]
pub struct TcpCongestionControlNode;

impl TcpCongestionControlNode {
    pub fn observe_ack(
        session: &mut TcpSessionState,
        observation: TcpCongestionAckObservation,
    ) -> CoreResult<()> {
        let sample = session
            .retransmit_queue_mut()
            .acknowledge_through_with_sample(observation.accepted_acknowledgment, observation.now);
        if let Some(rtt) = sample.latest_rtt {
            let bytes_in_flight =
                tcp_seq_distance(observation.accepted_acknowledgment, session.snd_nxt());
            session.congestion_mut().on_ack(TcpCongestionAckSample {
                bytes_acked: sample.bytes_acked,
                rtt,
                now: observation.now,
                bytes_in_flight,
            });
        }
        Ok(())
    }

    pub fn observe_send(
        session: &mut TcpSessionState,
        observation: TcpCongestionSendObservation,
    ) -> CoreResult<()> {
        session
            .congestion_mut()
            .on_packet_sent(observation.bytes_sent, observation.bytes_in_flight);
        let next_output_at = session
            .congestion()
            .next_send_delay(observation.bytes_sent)
            .map(|delay| observation.now + delay);
        session.set_next_output_at(next_output_at);
        Ok(())
    }

    pub fn observe_loss(
        session: &mut TcpSessionState,
        observation: TcpCongestionLossObservation,
    ) -> CoreResult<()> {
        session.congestion_mut().on_loss(observation.bytes_lost);
        session.set_next_output_at(None);
        Ok(())
    }
}

#[inline]
fn tcp_seq_distance(start: u32, end: u32) -> u32 {
    if start != 0 && end != 0 {
        TcpSeq::new(start).distance_to(TcpSeq::new(end))
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::tcp_seq_distance;

    #[test]
    fn tcp_seq_distance_handles_wraparound() {
        assert_eq!(tcp_seq_distance(u32::MAX - 10, 100), 111);
    }
}
