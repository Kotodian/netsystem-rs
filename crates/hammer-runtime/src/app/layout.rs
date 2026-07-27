use hammer_infra::align::align_up;
use hammer_infra::fifo::{Fifo, FifoError};
use hammer_infra::segment::Segment;

use crate::app::session_msg_queue::SessionMsgQueue;

/// Offsets for the four session queues within a shared segment.
/// These are filled in by the dataplane when it pre-allocates session
/// resources, then sent to the app process via SCM_RIGHTS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionOffsets {
    pub rx_fifo_off: u64,
    pub tx_fifo_off: u64,
    pub evt_q_off: u64,
    pub tx_evt_q_off: u64,
}

impl SessionOffsets {
    /// Pre-allocate all four session queues in `seg` and return their offsets.
    /// `fifo_capacity` is the byte capacity; `evt_q_capacity` is the desired
    /// usable event count (one extra slot is reserved for the ring protocol).
    pub fn allocate(
        seg: &Segment,
        fifo_capacity: usize,
        evt_q_capacity: usize,
    ) -> Result<Self, FifoError> {
        let fifo_total = align_up(Fifo::layout_bytes(fifo_capacity)?, 64);
        let ring_nitems = evt_q_capacity.max(1) as u32;
        let q_nitems = (evt_q_capacity + 1).next_power_of_two().max(2) as u32;
        let evt_msgq_total = align_up(
            SessionMsgQueue::layout_bytes(q_nitems, ring_nitems).expect("session mq layout"),
            64,
        );
        // Shared worker tx_evt_q uses a fixed 64/64 multi-ring capacity (attach path).
        let tx_msgq_total = align_up(
            SessionMsgQueue::layout_bytes(64, 64).expect("tx session mq layout"),
            64,
        );

        let bytes = fifo_total
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(evt_msgq_total))
            .and_then(|bytes| bytes.checked_add(tx_msgq_total))
            .ok_or(FifoError::CapacityOutOfRange {
                capacity: fifo_capacity,
            })?;
        let rx_fifo_off = seg.alloc(bytes, 64).ok_or(FifoError::SegmentExhausted)?;
        let tx_fifo_off = rx_fifo_off + fifo_total as u64;
        let evt_q_off = tx_fifo_off + fifo_total as u64;
        let tx_evt_q_off = evt_q_off + evt_msgq_total as u64;
        Ok(Self {
            rx_fifo_off,
            tx_fifo_off,
            evt_q_off,
            tx_evt_q_off,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn session_offsets_allocates_all_four_queues() {
        let seg = Segment::local(1024 * 1024);
        let offs = SessionOffsets::allocate(&seg, 32, 16).expect("session layout");
        assert!(offs.rx_fifo_off < offs.tx_fifo_off);
        assert!(offs.tx_fifo_off < offs.evt_q_off);
        assert!(offs.evt_q_off < offs.tx_evt_q_off);
    }

    #[test]
    fn session_offsets_offsets_are_cachelined() {
        let seg = Segment::local(1024 * 1024);
        let offs = SessionOffsets::allocate(&seg, 64, 32).expect("session layout");
        assert_eq!(offs.rx_fifo_off % 64, 0);
        assert_eq!(offs.tx_fifo_off % 64, 0);
        assert_eq!(offs.evt_q_off % 64, 0);
        assert_eq!(offs.tx_evt_q_off % 64, 0);
    }
}
