use std::time::Instant;

use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::TcpSeq;

use crate::session::protocol::tcp::state::TcpSessionTable;

use super::TcpLookupId;
use super::congestion::TcpCongestionAckSample;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpCongestionAckObservation {
    pub lookup_id: TcpLookupId,
    pub accepted_acknowledgment: u32,
    pub now: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpCongestionSendObservation {
    pub lookup_id: TcpLookupId,
    pub bytes_sent: u32,
    pub bytes_in_flight: u32,
    pub now: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpCongestionLossObservation {
    pub lookup_id: TcpLookupId,
    pub bytes_lost: u32,
}

#[derive(Debug, Default)]
pub struct TcpCongestionControlNode;

impl TcpCongestionControlNode {
    pub fn observe_ack(
        sessions: &mut TcpSessionTable,
        observation: TcpCongestionAckObservation,
    ) -> CoreResult<()> {
        let session = sessions
            .lookup_by_lookup_id_mut(observation.lookup_id)
            .ok_or_else(|| CoreError::internal("tcp congestion ack session not found"))?;
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
        sessions: &mut TcpSessionTable,
        observation: TcpCongestionSendObservation,
    ) -> CoreResult<()> {
        let session = sessions
            .lookup_by_lookup_id_mut(observation.lookup_id)
            .ok_or_else(|| CoreError::internal("tcp congestion send session not found"))?;
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
        sessions: &mut TcpSessionTable,
        observation: TcpCongestionLossObservation,
    ) -> CoreResult<()> {
        let session = sessions
            .lookup_by_lookup_id_mut(observation.lookup_id)
            .ok_or_else(|| CoreError::internal("tcp congestion loss session not found"))?;
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
