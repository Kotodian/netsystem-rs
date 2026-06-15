use std::time::{Duration, Instant};

use hammer_core::error::CoreResult;
use hammer_core::protocol::tcp::TcpSeq;

use super::congestion::TcpCongestionAckSample;
use super::connection::TcpConnectionState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpCongestionAckObservation {
    pub accepted_acknowledgment: u32,
    pub bytes_acked: u32,
    pub rtt: Duration,
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
        connection: &mut TcpConnectionState,
        observation: TcpCongestionAckObservation,
    ) -> CoreResult<()> {
        let bytes_in_flight =
            tcp_seq_distance(observation.accepted_acknowledgment, connection.snd_nxt());
        connection.observe_congestion_ack(TcpCongestionAckSample {
            bytes_acked: observation.bytes_acked,
            rtt: observation.rtt,
            now: observation.now,
            bytes_in_flight,
        });
        Ok(())
    }

    pub fn observe_send(
        connection: &mut TcpConnectionState,
        observation: TcpCongestionSendObservation,
    ) -> CoreResult<()> {
        connection.observe_congestion_send(
            observation.bytes_sent,
            observation.bytes_in_flight,
            observation.now,
        );
        Ok(())
    }

    pub fn observe_loss(
        connection: &mut TcpConnectionState,
        observation: TcpCongestionLossObservation,
    ) -> CoreResult<()> {
        connection.observe_congestion_loss(observation.bytes_lost);
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
